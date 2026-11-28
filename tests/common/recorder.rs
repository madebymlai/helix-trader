//! A recording strategy for what the EMA crossover cannot show: the open, high and low of bars,
//! fills of a given side and size, funding and positions.
//!
//! It subscribes through `strategy::subscriptions`, as a real strategy does in a backtest, on a
//! plain instrument or, given continuous bars, on a root. It records every bar and fill. On stop,
//! it records the adjustments (funding) to its positions, its orders, denied orders and account
//! margins.
//!
//! It can send market orders on the first bars, or trades at chosen stakes
//! (`strategy::orders::Trade`) on equity converted at chosen rates (`strategy::conversions`). It
//! can close its positions by id on given bars. It leaves `oms_type` unset, so it keeps positions
//! as the venue does.

use std::{cell::RefCell, rc::Rc};

use helix_trader::{
    rolls::ContinuousBars,
    strategy::{
        conversions::{Conversions, Rates},
        orders::{Exits, Trade},
        risk::Stake,
        subscriptions::Subscriptions,
    },
};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    accounts::AccountAny,
    data::{Bar, BarType},
    enums::OrderSide,
    events::{OrderDenied, OrderFilled, PositionAdjusted},
    identifiers::StrategyId,
    orders::{Order, OrderAny},
    position::Position,
    types::{MarginBalance, Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

/// What the recorder got, in order.
///
/// Positions, orders and adjustments are those in the cache when it stops, positions and orders
/// oldest first. Margins are the venue account's, if it is a margin account.
#[derive(Debug, Default)]
pub(crate) struct Record {
    pub bars: Vec<Bar>,

    pub fills: Vec<(OrderSide, Price, UnixNanos)>,

    pub adjustments: Vec<PositionAdjusted>,

    pub positions: Vec<Position>,

    pub orders: Vec<OrderAny>,

    pub denials: Vec<String>,

    pub margins: Vec<MarginBalance>,
}

#[derive(Debug)]
pub(crate) struct Recorder {
    core: StrategyCore,
    bar_type: BarType,

    continuous: ContinuousBars,

    market: Vec<(OrderSide, Quantity)>,

    /// For each first bar, in order: side, one R from the bar's close, stake.
    staked: Vec<(OrderSide, Decimal, Stake)>,

    rates: Rates,

    /// The indexes of bars on which it closes its open positions, before any market order.
    close_on: Vec<usize>,
    record: Rc<RefCell<Record>>,
}

impl Recorder {
    pub(crate) fn new(bar_type: BarType) -> (Self, Rc<RefCell<Record>>) {
        let record = Rc::new(RefCell::new(Record::default()));
        let recorder = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("RECORDER-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            bar_type,
            continuous: ContinuousBars::default(),
            market: vec![],
            staked: vec![],
            rates: Conversions::default().rates(),
            close_on: vec![],
            record: Rc::clone(&record),
        };
        (recorder, record)
    }

    /// On a root, its bars are Nautilus' continuous bars of `continuous`.
    pub(crate) fn continuous(
        bar_type: BarType,
        continuous: ContinuousBars,
    ) -> (Self, Rc<RefCell<Record>>) {
        let (recorder, record) = Self::new(bar_type);
        (
            Self {
                continuous,
                ..recorder
            },
            record,
        )
    }

    pub(crate) fn trading(
        bar_type: BarType,
        market: Vec<(OrderSide, Quantity)>,
    ) -> (Self, Rc<RefCell<Record>>) {
        let (recorder, record) = Self::new(bar_type);
        (Self { market, ..recorder }, record)
    }

    /// Sends the trade of `staked[i]` on the `i`-th bar.
    pub(crate) fn staking(
        bar_type: BarType,
        staked: Vec<(OrderSide, Decimal, Stake)>,
    ) -> (Self, Rc<RefCell<Record>>) {
        let (recorder, record) = Self::new(bar_type);
        (Self { staked, ..recorder }, record)
    }

    pub(crate) fn closing_on(self, close_on: Vec<usize>) -> Self {
        Self { close_on, ..self }
    }

    pub(crate) fn converting(self, rates: Rates) -> Self {
        Self { rates, ..self }
    }
}

nautilus_strategy!(Recorder, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.record
            .borrow_mut()
            .fills
            .push((event.order_side, event.last_px, event.ts_event));
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.record
            .borrow_mut()
            .denials
            .push(event.reason.to_string());
    }
});

impl DataActor for Recorder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let now = self.clock().utc_now();
        Subscriptions::new(vec![self.bar_type], vec![], vec![])
            .continuous(self.continuous.clone())
            .start(self, now);
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        let mut positions = self.cache().positions(None, None, None, None, None);
        positions.sort_by_key(|position| position.ts_opened);
        let venue = self.bar_type.instrument_id().venue;
        let margins = match self.cache().account_for_venue(&venue) {
            Some(AccountAny::Margin(account)) => account.margins.into_values().collect(),
            _ => vec![],
        };
        let mut orders = self.cache().orders(None, None, None, None, None);
        orders.sort_by_key(|order| order.ts_init());
        let mut record = self.record.borrow_mut();
        record.orders = orders;
        record.margins = margins;
        record.adjustments.extend(
            positions
                .iter()
                .flat_map(|position| position.adjustments.iter().copied()),
        );
        record.positions = positions;
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let index = self.record.borrow().bars.len();
        self.record.borrow_mut().bars.push(*bar);
        if self.close_on.contains(&index) {
            let instrument_id = bar.bar_type.instrument_id();
            let open = self
                .cache()
                .positions_open(None, Some(&instrument_id), None, None, None);
            for position in &open {
                self.close_position(position, None, None, None, None, None, None)?;
            }
        }

        if let Some(&(side, quantity)) = self.market.get(index) {
            let order = self.order().market(
                bar.bar_type.instrument_id(),
                side,
                quantity,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            );
            self.submit_order(order, None, None, None)?;
        }

        if let Some(&(side, one_r, stake)) = self.staked.get(index) {
            let exits = Exits {
                bracket: None,
                time_limit: None,
            };
            Trade::new(bar.bar_type.instrument_id(), side, bar.close, one_r, exits)
                .send(self, stake, self.rates)?;
        }
        Ok(())
    }
}
