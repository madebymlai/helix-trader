//! A recording strategy: it subscribes to its bars through
//! `strategy::warmup` as a real strategy does in a backtest, on a plain
//! instrument or, given its continuous bars, on a root, records every bar
//! it gets, every fill, and on stop the adjustments of its positions
//! (funding), the orders denied and its account's margins, and can rest a
//! sell stop on its first bar or send market orders of its choosing on the
//! first bars, close its positions by their ids on given bars, and on stop
//! its positions. It leaves its `oms_type` unset, so it keeps positions as
//! the venue does. For the tests that need what the EMA crossover cannot
//! show: the bars' open, high and low, stop orders, fills of a given side
//! and size, funding, and positions.

use std::{cell::RefCell, rc::Rc};

use helix_trader::{rolls::ContinuousBars, strategy::warmup};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    accounts::AccountAny,
    data::{Bar, BarType},
    enums::OrderSide,
    events::{OrderDenied, OrderFilled, PositionAdjusted},
    identifiers::StrategyId,
    position::Position,
    types::{MarginBalance, Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

/// What the recorder got, in order.
#[derive(Debug, Default)]
pub struct Record {
    /// The bars of `on_bar`.
    pub bars: Vec<Bar>,
    /// Side, price and time of every fill.
    pub fills: Vec<(OrderSide, Price, UnixNanos)>,
    /// The adjustments of its positions in the cache when it stops.
    pub adjustments: Vec<PositionAdjusted>,
    /// Its positions in the cache when it stops, oldest first.
    pub positions: Vec<Position>,
    /// The reasons of the orders the risk engine denied.
    pub denials: Vec<String>,
    /// The margins of its venue's account when it stops, if a margin
    /// account.
    pub margins: Vec<MarginBalance>,
}

#[derive(Debug)]
pub struct Recorder {
    core: StrategyCore,
    bar_type: BarType,
    /// The continuous bars of its bar type, if on a root.
    continuous: ContinuousBars,
    /// The trigger of the sell stop of 10 shares sent on the first bar.
    stop: Option<Price>,
    /// The market order sent on each of the first bars, in order.
    market: Vec<(OrderSide, Quantity)>,
    /// The bars, by index, on which it closes its open positions
    /// (`close_position`), before any market order of the bar.
    close_on: Vec<usize>,
    record: Rc<RefCell<Record>>,
}

impl Recorder {
    /// A recorder of `bar_type`, resting a sell stop at `stop` if given,
    /// and the record it fills.
    pub fn new(bar_type: BarType, stop: Option<Price>) -> (Self, Rc<RefCell<Record>>) {
        let record = Rc::new(RefCell::new(Record::default()));
        let recorder = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("RECORDER-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            bar_type,
            continuous: ContinuousBars::default(),
            stop,
            market: vec![],
            close_on: vec![],
            record: Rc::clone(&record),
        };
        (recorder, record)
    }

    /// A recorder of `bar_type`, on a root, its bars Nautilus' continuous
    /// bars of `continuous`, and the record it fills.
    pub fn continuous(
        bar_type: BarType,
        continuous: ContinuousBars,
    ) -> (Self, Rc<RefCell<Record>>) {
        let (recorder, record) = Self::new(bar_type, None);
        (
            Self {
                continuous,
                ..recorder
            },
            record,
        )
    }

    /// A recorder of `bar_type` sending the market order `market[i]` on
    /// the `i`-th bar, and the record it fills.
    pub fn trading(
        bar_type: BarType,
        market: Vec<(OrderSide, Quantity)>,
    ) -> (Self, Rc<RefCell<Record>>) {
        let (recorder, record) = Self::new(bar_type, None);
        (Self { market, ..recorder }, record)
    }

    /// The recorder closing its open positions, each by its id
    /// (`close_position`), on the bars of `close_on`.
    pub fn closing_on(self, close_on: Vec<usize>) -> Self {
        Self { close_on, ..self }
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
        warmup::subscribe(self, &[self.bar_type], &self.continuous.clone());
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
        let mut record = self.record.borrow_mut();
        record.margins = margins;
        record.adjustments.extend(
            positions
                .iter()
                .flat_map(|position| position.adjustments.iter().cloned()),
        );
        record.positions = positions;
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let index = self.record.borrow().bars.len();
        let first = index == 0;
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
        let Some(trigger) = self.stop.filter(|_| first) else {
            return Ok(());
        };
        let order = self.order().stop_market(
            bar.bar_type.instrument_id(),
            OrderSide::Sell,
            Quantity::from(10),
            trigger,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.submit_order(order, None, None, None)
    }
}
