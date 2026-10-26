//! EMA crossover, an example strategy trading one instrument: each time the
//! fast EMA crosses the slow one on that instrument's bars, inside its
//! session if it has one, it opens a trade in the cross direction, a
//! Nautilus bracket (`OrderFactory::bracket`): a market entry, a stop loss
//! and a take profit at given distances from the close of the bar it
//! decided on. A trade not ended by either is closed `hold` after its entry
//! filled (`close_position`, on a time alert), its legs cancelled. Trades
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
use nautilus_common::{actor::DataActor, timer::TimeEvent};
use nautilus_model::{
    data::{Bar, QuoteTick},
    enums::{ContingencyType, OrderSide},
    events::OrderFilled,
    identifiers::ClientOrderId,
    instruments::Instrument,
    orders::Order,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyCore},
};

use self::{bracket::Exits, signal::Signal};
use crate::strategy::warmup;

/// The prefix of a trade's time alert, followed by its entry's client order
/// id.
const HOLD_ALERT: &str = "EMA-HOLD:";

#[derive(Debug)]
pub struct EmaCross {
    core: StrategyCore,
    config: EmaCrossConfig,
    signal: Signal,
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
    fn on_start(&mut self) -> anyhow::Result<()> {
        let (bar_types, warmup) = (self.config.bars.bar_types(), self.config.bars.warmup);
        warmup::start(self, &bar_types, warmup)
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
