//! EMA crossover, an example strategy trading one instrument: each time the
//! fast EMA crosses the slow one on that instrument's bars, inside its
//! session if it has one, it opens a trade in the cross direction, a
//! Nautilus bracket (`OrderFactory::bracket`): a market entry, a stop loss
//! and a take profit at given distances from the close of the bar it
//! decided on. A trade not ended by either is closed `hold` after its entry
//! filled (`close_position`, on a time alert), its legs cancelled; the
//! alerts of the trades still open are set again at start. Trades
//! may overlap: on a hedging venue each is a position of its own, on a
//! netting venue they add into one, as the venue keeps them.
//!
//! The config's `instrument_ids` are the instruments read, whose bars it
//! subscribes to and warms up through `strategy::warmup`;
//! `trade_instrument_id` is the one traded. A minimal strategy to drive
//! data, backtest and paper.

mod bracket;
mod config;
mod signal;

pub use bracket::Bps;
pub use config::EmaCrossConfig;

use jiff::Timestamp;
use nautilus_common::{actor::DataActor, messages::data::DataResponseEnvelope, timer::TimeEvent};
use nautilus_core::UUID4;
use nautilus_model::{
    data::{Bar, QuoteTick},
    enums::{ContingencyType, OrderSide},
    events::{OrderEventAny, OrderFilled},
    identifiers::ClientOrderId,
    instruments::Instrument,
    orders::Order,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyCore},
};

use self::{bracket::Exits, signal::Signal};
use crate::{rolls::ContinuousBars, strategy::warmup};

/// The prefix of a trade's time alert, followed by its entry's client order
/// id.
const HOLD_ALERT: &str = "EMA-HOLD:";

#[derive(Debug)]
pub struct EmaCross {
    core: StrategyCore,
    config: EmaCrossConfig,
    signal: Signal,
    /// The request group of the warmup of the last start.
    warmup: UUID4,
}

impl EmaCross {
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn new(config: EmaCrossConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            signal: Signal::new(&config),
            config,
            warmup: UUID4::new(),
        })
    }

    /// Opens a trade on `side`, decided on `bar`: one bracket of
    /// `trade_size`, its stop loss and take profit at their distances from
    /// the bar's close, rounded to the instrument's prices. The size is
    /// rounded to the instrument's size precision: one rounding to zero, as
    /// a trade size off that grid leaves, is an error and no order, logged
    /// by Nautilus.
    fn enter(&mut self, bar: &Bar, side: OrderSide) -> anyhow::Result<()> {
        let instrument_id = self.config.trade_instrument_id;
        let instrument = self
            .cache()
            .instrument(&instrument_id)
            .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} not in cache"))?;
        let size = self.config.trade_size.as_decimal();
        let quantity = instrument
            .try_make_qty_from_decimal(size, None)
            // In the message, not as context: Nautilus logs only the top
            // error of the chain.
            .map_err(|e| anyhow::anyhow!("ordering {size} of {instrument_id}: {e}"))?;
        let exits = Exits::new(
            bar.close.as_decimal(),
            side,
            self.config.stop_loss_bps,
            self.config.take_profit_bps,
        );
        let orders = self
            .order()
            .bracket()
            .instrument_id(instrument_id)
            .order_side(side)
            .quantity(quantity)
            .sl_trigger_price(instrument.try_make_price_from_decimal(exits.stop_loss)?)
            .tp_price(instrument.try_make_price_from_decimal(exits.take_profit)?)
            .call();
        self.submit_order_list(orders, None, None, None)
    }

    /// Ends the trade of `entry` at the end of its hold: cancels its legs
    /// still open, then closes its position if it is open.
    fn exit(&mut self, entry: ClientOrderId) -> anyhow::Result<()> {
        let (legs, position) = {
            let cache = self.cache();
            let entry_order = cache
                .order(&entry)
                .ok_or_else(|| anyhow::anyhow!("entry {entry} not in cache"))?;
            let legs: Vec<ClientOrderId> = entry_order
                .linked_order_ids()
                .unwrap_or_default()
                .iter()
                .filter(|id| cache.order(id).is_some_and(|leg| leg.is_open()))
                .copied()
                .collect();
            (legs, cache.position_for_order(&entry))
        };
        if !legs.is_empty() {
            self.cancel_orders(legs, None, None)?;
        }
        match position {
            Some(position) if position.is_open() => {
                self.close_position(&position, None, None, None, None, None, None)
            }
            _ => Ok(()),
        }
    }

    /// Sets the time alert of each trade of the strategy on the traded
    /// instrument still open, as on its entry's first fill: `hold` after
    /// it, or now if that has passed (the clock's `allow_past`). A trade
    /// is a filled bracket entry; it is still open while the position it
    /// went into is open, not closed since, and no leg of it has filled.
    fn arm_open_trades(&mut self) -> anyhow::Result<()> {
        let open: Vec<(ClientOrderId, Timestamp)> = {
            let strategy_id = self.strategy_id();
            let cache = self.cache();
            cache
                .orders(
                    None,
                    Some(&self.config.trade_instrument_id),
                    strategy_id.as_ref(),
                    None,
                    None,
                )
                .iter()
                .filter(|o| o.contingency_type() == Some(ContingencyType::Oto))
                .filter_map(|entry| {
                    let filled = entry.events().into_iter().find_map(|e| match e {
                        OrderEventAny::Filled(fill) => Some(fill.ts_event),
                        _ => None,
                    })?;
                    let id = entry.client_order_id();
                    let in_position = cache
                        .position_for_order(&id)
                        .is_some_and(|p| p.is_open() && p.ts_opened <= filled);
                    let ended = entry
                        .linked_order_ids()
                        .unwrap_or_default()
                        .iter()
                        .any(|leg| {
                            cache
                                .order(leg)
                                .is_some_and(|l| l.filled_qty().is_positive())
                        });
                    (in_position && !ended).then(|| (id, Timestamp::from(filled)))
                })
                .collect()
        };
        for (entry, filled) in open {
            self.clock().set_time_alert(
                &hold_alert(entry),
                filled + self.config.hold,
                None,
                None,
            )?;
        }
        Ok(())
    }
}

/// The name of the time alert ending the trade of `entry`.
fn hold_alert(entry: ClientOrderId) -> String {
    format!("{HOLD_ALERT}{entry}")
}

nautilus_strategy!(EmaCross, {
    /// The first fill of an entry sets its trade's time alert, `hold`
    /// later; the fill of a leg ends the trade, so cancels it.
    fn on_order_filled(&mut self, event: &OrderFilled) {
        let Some(order) = self.cache().order(&event.client_order_id) else {
            return;
        };
        if let Some(entry) = order.parent_order_id() {
            self.clock().cancel_timer(&hold_alert(entry));
            return;
        }
        let first_fill = order.filled_qty() == event.last_qty;
        if order.contingency_type() == Some(ContingencyType::Oto) && first_fill {
            let at = Timestamp::from(event.ts_event) + self.config.hold;
            let alert = hold_alert(event.client_order_id);
            if let Err(e) = self.clock().set_time_alert(&alert, at, None, None) {
                log::error!("setting the time alert {alert}: {e}");
            }
        }
    }
});

impl DataActor for EmaCross {
    /// Sets the time exit of each trade still open again, as after a
    /// restart, then starts the warmup.
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.arm_open_trades()?;
        let (bar_types, warmup) = (self.config.bars.bar_types(), self.config.bars.warmup);
        self.warmup = UUID4::new();
        // Its bars are built from quotes or source bars, never from a root's
        // trades: no continuous bars.
        warmup::start(
            self,
            &bar_types,
            warmup,
            &ContinuousBars::default(),
            self.warmup,
        )
    }

    /// Logs the end of the warmup; Nautilus logs a failed one as an error.
    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if warmup::finished(self.warmup, response)? {
            log::info!("Warmup complete");
        }
        Ok(())
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, quotes) {
            self.signal.update(&bar);
        }
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, bars) {
            self.signal.update(&bar);
        }
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.signal.update(bar);
        match self.signal.cross(bar) {
            Some(side) => self.enter(bar, side),
            None => Ok(()),
        }
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        match event.name.strip_prefix(HOLD_ALERT) {
            Some(entry) => self.exit(ClientOrderId::new_checked(entry)?),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        timer::TimeEventCallback,
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        enums::{OmsType, OrderType},
        events::OrderEventAny,
        identifiers::{AccountId, PositionId, StrategyId, TradeId, TraderId, VenueOrderId},
        instruments::{
            InstrumentAny,
            stubs::{audusd_sim, gbpusd_sim},
        },
        orders::{OrderAny, OrderTestBuilder, stubs::TestOrderEventStubs},
        position::Position,
        types::{Price, Quantity},
    };
    use nautilus_portfolio::portfolio::Portfolio;

    use super::*;

    /// Trades AUD/USD, reads GBP/USD too.
    const CONFIG: &str = r#"
        instrument_ids = ["AUD/USD.SIM", "GBP/USD.SIM"]
        trade_instrument_id = "AUD/USD.SIM"
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
        trade_size = "100000"
        stop_loss_bps = 50
        take_profit_bps = 50
        hold = "1h"
        warmup = "1h"
    "#;

    const STRATEGY: &str = "EMA_CROSS-001";

    /// 2026-10-26T10:00:00Z, the time of the restart.
    const NOW: u64 = 1_792_922_400_000_000_000;
    const MINUTE: u64 = 60_000_000_000;

    /// Fills the cached `order` into `position` at `ts`, as the execution
    /// engine does: the order, the order's position, the position, kept as
    /// `oms`; a closed netting position reopened under its id.
    fn fill_into(
        cache: &Rc<RefCell<Cache>>,
        instrument: &InstrumentAny,
        order: &OrderAny,
        position: &str,
        ts: u64,
        oms: OmsType,
    ) {
        let account = AccountId::from("SIM-001");
        let filled = TestOrderEventStubs::filled(
            order,
            instrument,
            Some(TradeId::from(order.client_order_id().as_str())),
            Some(PositionId::from(position)),
            Some(Price::from("0.70000")),
            None,
            None,
            None,
            Some(UnixNanos::from(ts)),
            Some(account),
        );
        let OrderEventAny::Filled(fill) = &filled else {
            panic!("not a fill");
        };
        let mut cache = cache.borrow_mut();
        for event in [
            TestOrderEventStubs::submitted(order, account),
            TestOrderEventStubs::accepted(
                order,
                account,
                VenueOrderId::from(order.client_order_id().as_str()),
            ),
            filled.clone(),
        ] {
            cache.update_order(&event).unwrap();
        }
        let id = PositionId::from(position);
        cache
            .add_position_id(
                &id,
                &instrument.id().venue,
                &fill.client_order_id,
                &fill.strategy_id,
            )
            .unwrap();
        match cache.position(&id).map(|p| p.is_open()) {
            Some(true) => {
                cache.update_position_from_fill(id, fill).unwrap();
            }
            _ => cache
                .add_position(&Position::new(instrument, fill.clone()), oms)
                .unwrap(),
        }
    }

    /// Adds to `cache` a bracket of `strategy` on `instrument`, buying its
    /// entry `entry`, its legs `<entry>-SL` and `<entry>-TP`, and fills
    /// the entry into `position` at `filled`.
    fn trade(
        cache: &Rc<RefCell<Cache>>,
        instrument: &InstrumentAny,
        strategy: &str,
        (entry, position): (&str, &str),
        filled: u64,
        oms: OmsType,
    ) {
        let (sl, tp) = (
            ClientOrderId::from(format!("{entry}-SL").as_str()),
            ClientOrderId::from(format!("{entry}-TP").as_str()),
        );
        let entry_order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .strategy_id(StrategyId::from(strategy))
            .client_order_id(ClientOrderId::from(entry))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100_000))
            .contingency_type(ContingencyType::Oto)
            .linked_order_ids(vec![sl, tp])
            .build();
        let leg = |id: ClientOrderId, other: ClientOrderId| {
            OrderTestBuilder::new(OrderType::Market)
                .instrument_id(instrument.id())
                .strategy_id(StrategyId::from(strategy))
                .client_order_id(id)
                .side(OrderSide::Sell)
                .quantity(Quantity::from(100_000))
                .contingency_type(ContingencyType::Oco)
                .linked_order_ids(vec![other])
                .parent_order_id(ClientOrderId::from(entry))
                .reduce_only(true)
                .build()
        };
        for order in [entry_order.clone(), leg(sl, tp), leg(tp, sl)] {
            cache
                .borrow_mut()
                .add_order(order, None, None, false)
                .unwrap();
        }
        fill_into(cache, instrument, &entry_order, position, filled, oms);
    }

    /// Fills the cached order `id` (a leg, a close) into `position` at
    /// `ts`.
    fn fill_order(
        cache: &Rc<RefCell<Cache>>,
        instrument: &InstrumentAny,
        id: &str,
        position: &str,
        ts: u64,
    ) {
        let order = cache
            .borrow()
            .order_owned(&ClientOrderId::from(id))
            .unwrap();
        fill_into(cache, instrument, &order, position, ts, OmsType::Netting);
    }

    /// Adds to `cache` a market order `id` of `strategy` selling 100,000
    /// AUD, as a time exit closing a long position.
    fn close_order(
        cache: &Rc<RefCell<Cache>>,
        instrument: &InstrumentAny,
        strategy: &str,
        id: &str,
    ) {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .strategy_id(StrategyId::from(strategy))
            .client_order_id(ClientOrderId::from(id))
            .side(OrderSide::Sell)
            .quantity(Quantity::from(100_000))
            .reduce_only(true)
            .build();
        cache
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();
    }

    /// The strategy started at `NOW` on `cache`, as after a restart, and
    /// its clock.
    fn started(cache: Rc<RefCell<Cache>>) -> (EmaCross, Rc<RefCell<VirtualClock>>) {
        let mut strategy = EmaCross::new(toml::from_str(CONFIG).unwrap()).unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(UnixNanos::from(NOW), true);
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        strategy
            .core
            .register(TraderId::from("HELIX-001"), clock.clone(), cache, portfolio)
            .unwrap();
        DataActor::on_start(&mut strategy).unwrap();
        (strategy, clock)
    }

    #[test]
    fn a_restart_sets_the_time_exit_of_each_open_trade_again() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-1", "P-1"),
            NOW - 20 * MINUTE,
            OmsType::Hedging,
        );
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-2", "P-2"),
            NOW - 90 * MINUTE,
            OmsType::Hedging,
        );

        let (_strategy, clock) = started(cache);

        let clock = clock.borrow();
        // Opened 20 minutes ago: its hour ends in 40 minutes.
        assert_eq!(
            clock.next_time_ns("EMA-HOLD:O-1"),
            Some(UnixNanos::from(NOW + 40 * MINUTE))
        );
        // Opened 90 minutes ago: its hour is over, so now.
        assert_eq!(
            clock.next_time_ns("EMA-HOLD:O-2"),
            Some(UnixNanos::from(NOW))
        );
    }

    #[test]
    fn a_restart_sets_no_time_exit_for_another_strategy_or_instrument() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        let gbp = InstrumentAny::CurrencyPair(gbpusd_sim());
        trade(
            &cache,
            &aud,
            "OTHER-001",
            ("O-3", "P-3"),
            NOW - 20 * MINUTE,
            OmsType::Hedging,
        );
        trade(
            &cache,
            &gbp,
            STRATEGY,
            ("O-4", "P-4"),
            NOW - 20 * MINUTE,
            OmsType::Hedging,
        );

        let (_strategy, clock) = started(cache);

        let names = clock.borrow().timer_names().join(",");
        assert!(!names.contains(HOLD_ALERT), "{names}");
    }

    /// On a netting venue two crosses, 20 and 50 minutes ago, add into one
    /// position, which opened with the first.
    #[test]
    fn a_restart_sets_the_time_exit_of_each_trade_in_a_netting_position_again() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-1", "P-NET"),
            NOW - 50 * MINUTE,
            OmsType::Netting,
        );
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-2", "P-NET"),
            NOW - 20 * MINUTE,
            OmsType::Netting,
        );

        let (_strategy, clock) = started(cache);

        let clock = clock.borrow();
        assert_eq!(
            clock.next_time_ns("EMA-HOLD:O-1"),
            Some(UnixNanos::from(NOW + 10 * MINUTE))
        );
        assert_eq!(
            clock.next_time_ns("EMA-HOLD:O-2"),
            Some(UnixNanos::from(NOW + 40 * MINUTE))
        );
    }

    /// The first trade's stop loss filled before the restart: its share of
    /// the netting position is gone, the second trade's left open.
    #[test]
    fn a_restart_sets_no_time_exit_for_a_trade_its_leg_ended() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-1", "P-NET"),
            NOW - 50 * MINUTE,
            OmsType::Netting,
        );
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-2", "P-NET"),
            NOW - 20 * MINUTE,
            OmsType::Netting,
        );
        fill_order(&cache, &aud, "O-1-SL", "P-NET", NOW - 10 * MINUTE);

        let (_strategy, clock) = started(cache);

        let clock = clock.borrow();
        assert_eq!(clock.next_time_ns("EMA-HOLD:O-1"), None);
        assert_eq!(
            clock.next_time_ns("EMA-HOLD:O-2"),
            Some(UnixNanos::from(NOW + 40 * MINUTE))
        );
    }

    /// The first trade's time exit closed the netting position; a later
    /// cross opened it again under the same id.
    #[test]
    fn a_restart_sets_no_time_exit_for_a_trade_of_a_netting_position_closed_since() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-1", "P-NET"),
            NOW - 90 * MINUTE,
            OmsType::Netting,
        );
        close_order(&cache, &aud, STRATEGY, "C-1");
        fill_order(&cache, &aud, "C-1", "P-NET", NOW - 30 * MINUTE);
        trade(
            &cache,
            &aud,
            STRATEGY,
            ("O-3", "P-NET"),
            NOW - 20 * MINUTE,
            OmsType::Netting,
        );

        let (_strategy, clock) = started(cache);

        let clock = clock.borrow();
        assert_eq!(clock.next_time_ns("EMA-HOLD:O-1"), None);
        assert_eq!(
            clock.next_time_ns("EMA-HOLD:O-3"),
            Some(UnixNanos::from(NOW + 40 * MINUTE))
        );
    }
}
