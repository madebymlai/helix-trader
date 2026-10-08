//! A probe strategy over the fixture catalog: what it receives and in which
//! order, and what its one order costs. One node per process, so one run
//! per file.

mod common;

use std::{cell::RefCell, rc::Rc};

use common::{BAR_TYPE, WARMUP};
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

/// What the probe received, in order.
#[derive(Debug, PartialEq)]
enum Event {
    /// The close times of a warmup response.
    Warmup(Vec<UnixNanos>),
    /// The close time of a replayed bar.
    Bar(UnixNanos),
}

#[derive(Debug, Default)]
struct Record {
    events: Vec<Event>,
    fills: Vec<(Price, Option<Money>)>,
}

/// Asks for the warmup on start, then buys once on the first bar.
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    record: Rc<RefCell<Record>>,
    bought: bool,
}

nautilus_strategy!(Probe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.record
            .borrow_mut()
            .fills
            .push((event.last_px, event.commission));
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
        if !self.bought {
            self.bought = true;
            let order = self.order().market(
                bar.bar_type.instrument_id(),
                OrderSide::Buy,
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
        }
        Ok(())
    }
}

#[test]
fn warmup_arrives_before_the_first_bar_and_the_one_order_pays_its_costs() {
    let catalog = common::catalog();
    let record = Rc::new(RefCell::new(Record::default()));
    let probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        record: Rc::clone(&record),
        bought: false,
    };

    let result = common::backtest(catalog.path(), "3 EUR")
        .run(probe)
        .unwrap();

    let bars = common::bars();
    let before = usize::try_from(common::BARS_BEFORE_START).unwrap();
    let warmup = usize::try_from(WARMUP.as_hours()).unwrap();
    let closes = |bars: &[Bar]| bars.iter().map(|bar| bar.ts_event).collect::<Vec<_>>();
    let mut expected = vec![Event::Warmup(closes(&bars[before - warmup..before]))];
    expected.extend(closes(&bars[before..]).into_iter().map(Event::Bar));
    let record = record.borrow();
    assert_eq!(record.events, expected);

    // A market order fills one tick above the close of the bar it was sent
    // on, and pays the commission once.
    let tick = 0.01;
    let first_close = bars[before].close.as_f64();
    assert_eq!(
        record.fills,
        vec![(
            Price::new(first_close + tick, 2),
            Some(Money::from("3 EUR"))
        )]
    );
    assert_eq!(result.total_orders, 1);
}
