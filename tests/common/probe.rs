//! A probe strategy: it asks for the fixture's warmup on start, records
//! what it receives and in which order, buys 10 shares at market on the
//! first bar and sells them on the second.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType},
    enums::OrderSide,
    events::OrderFilled,
    identifiers::StrategyId,
    types::{Money, Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

use super::{BAR_TYPE, WARMUP};

/// What the probe received, in order.
#[derive(Debug, PartialEq)]
pub enum Event {
    /// The close times of a warmup response.
    Warmup(Vec<UnixNanos>),
    /// The close time of a replayed bar.
    Bar(UnixNanos),
}

#[derive(Debug, Default)]
pub struct Record {
    pub events: Vec<Event>,
    /// Side, price and commission of every fill.
    pub fills: Vec<(OrderSide, Price, Option<Money>)>,
}

#[derive(Debug)]
pub struct Probe {
    core: StrategyCore,
    record: Rc<RefCell<Record>>,
    bars: usize,
}

impl Probe {
    /// A probe and the record it fills.
    pub fn new() -> (Self, Rc<RefCell<Record>>) {
        let record = Rc::new(RefCell::new(Record::default()));
        let probe = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("PROBE-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            record: Rc::clone(&record),
            bars: 0,
        };
        (probe, record)
    }
}

nautilus_strategy!(Probe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.record
            .borrow_mut()
            .fills
            .push((event.order_side, event.last_px, event.commission));
    }
});

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let bar_type = BarType::from(BAR_TYPE);
        let start = self.clock().utc_now().checked_sub(WARMUP)?;
        self.request_bars(bar_type, Some(start), None, None, None, None)?;
        self.subscribe_bars(bar_type, None, None);
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        let closes = bars.iter().map(|bar| bar.ts_event).collect();
        self.record.borrow_mut().events.push(Event::Warmup(closes));
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.record
            .borrow_mut()
            .events
            .push(Event::Bar(bar.ts_event));
        self.bars += 1;
        let side = match self.bars {
            1 => OrderSide::Buy,
            2 => OrderSide::Sell,
            _ => return Ok(()),
        };
        let order = self.order().market(
            bar.bar_type.instrument_id(),
            side,
            Quantity::from(10),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.submit_order(order, None, None, None)?;
        Ok(())
    }
}

/// What the probe receives on the fixture, from any start after the close
/// of the last bar before the run up to the close of the first replayed
/// bar: one warmup response holding the bars of the warmup before the
/// first replayed one, then every replayed bar.
pub fn fixture_events() -> Vec<Event> {
    let bars = super::bars();
    let before = usize::try_from(super::BARS_BEFORE_START).unwrap();
    let warmup = usize::try_from(WARMUP.as_hours()).unwrap();
    let closes = |bars: &[Bar]| bars.iter().map(|bar| bar.ts_event).collect::<Vec<_>>();
    let mut events = vec![Event::Warmup(closes(&bars[before - warmup..before]))];
    events.extend(closes(&bars[before..]).into_iter().map(Event::Bar));
    events
}
