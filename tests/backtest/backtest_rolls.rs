//! A strategy declaring a rolled root (`common::rolls`: GC.GLBX, rolling
//! from GCQ6.GLBX to GCZ6.GLBX at 00:00 UTC on 29 July 2026) over a run
//! from 23:00 to 01:00 across the roll: the run replays each contract's
//! series over its slice only, and a `Roller` switches the strategy's
//! subscriptions from one contract to the next at the roll and tells it.
//! One node per thread, so one run per test.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

use crate::common::rolls::{self, NANOSECOND, POST, PRE, ROOT, roll, tuesday, wednesday};
use helix_trader::{
    backtest::{Backtest, commission_rate, venue_oms_type},
    rolls::{Roller, Transition},
    strategy::{TickData, TickSeries},
};
use jiff::SignedDuration;
use nautilus_common::{actor::DataActor, logging::logger::LoggerConfig, timer::TimeEvent};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, OrderBookDepth, TradeTick},
    enums::BookType,
    identifiers::{InstrumentId, StrategyId},
    types::Money,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

/// What the probe got, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Got {
    /// The times of the clock's alerts once started.
    Alerts(Vec<UnixNanos>),
    /// A tick: its kind, its instrument, its time.
    Tick(TickData, InstrumentId, UnixNanos),
    /// A roll the roller returned, and the clock's time then.
    Roll(Transition, UnixNanos),
}

/// How the probe subscribes.
#[derive(Debug)]
enum Subscribe {
    /// Through a roller of the declared series.
    Roller(Roller),
    /// To the trades and book depth of both contracts, directly.
    Contracts,
}

/// Records each tick it gets, and each roll.
#[derive(Debug)]
struct RollProbe {
    core: StrategyCore,
    subscribe: Subscribe,
    got: Rc<RefCell<Vec<Got>>>,
}

nautilus_strategy!(RollProbe);

impl RollProbe {
    fn record(&self, got: Got) {
        self.got.borrow_mut().push(got);
    }
}

impl DataActor for RollProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        match &self.subscribe {
            Subscribe::Roller(roller) => roller.clone().start(self)?,
            Subscribe::Contracts => {
                for id in [PRE, POST].map(InstrumentId::from) {
                    self.subscribe_trades(id, None, None);
                    self.subscribe_book_depth(
                        id,
                        BookType::L2_MBP,
                        NonZeroUsize::new(10),
                        None,
                        false,
                        None,
                    );
                }
            }
        }
        let clock = self.clock();
        let alerts = clock
            .timer_names()
            .iter()
            .filter_map(|name| clock.next_time_ns(name))
            .collect();
        drop(clock);
        self.record(Got::Alerts(alerts));
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if let Subscribe::Roller(roller) = &self.subscribe
            && let Some(roll) = roller.clone().on_time_event(self, event)?
        {
            let now = self.clock().timestamp_ns();
            self.record(Got::Roll(roll, now));
        }
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.record(Got::Tick(
            TickData::Trades,
            trade.instrument_id,
            trade.ts_init,
        ));
        Ok(())
    }

    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        self.record(Got::Tick(
            TickData::Depth,
            depth.instrument_id,
            depth.ts_init,
        ));
        Ok(())
    }
}

/// The trades and book depth of `instrument_id`.
fn ticks(instrument_id: &str) -> Vec<TickSeries> {
    [TickData::Trades, TickData::Depth]
        .map(|data| TickSeries {
            data,
            instrument_id: InstrumentId::from(instrument_id),
        })
        .to_vec()
}

/// A backtest over `[23:00 Tuesday, 01:00 Wednesday]` on `catalog`,
/// declaring `ticks` and no bar types.
fn backtest(catalog: &tempfile::TempDir, ticks: Vec<TickSeries>) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![],
        ticks,
        start: tuesday("23:00:00"),
        end: wednesday("01:00:00"),
        warmup: SignedDuration::ZERO,
        fee_model: commission_rate(Decimal::ZERO).unwrap(),
        balance: Money::from("1000000 USD"),
        leverage: Decimal::ONE,
        venue_oms: venue_oms_type,
        logging: LoggerConfig::default(),
    }
}

/// Runs a probe subscribing as `subscribe` through `backtest` and returns
/// what it got.
fn run(backtest: &Backtest, subscribe: Subscribe) -> Vec<Got> {
    let got = Rc::new(RefCell::new(Vec::new()));
    let probe = RollProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        subscribe,
        got: Rc::clone(&got),
    };
    backtest.run(probe).unwrap();
    got.take()
}

fn tick(data: TickData, instrument_id: &str, time: &str) -> Got {
    let time = if let Some(time) = time.strip_prefix("thu ") {
        tuesday(time)
    } else {
        wednesday(time)
    };
    Got::Tick(data, InstrumentId::from(instrument_id), time.into())
}

/// The last nanosecond before the roll.
fn before_roll() -> UnixNanos {
    (roll() - NANOSECOND).into()
}

#[test]
fn a_rolled_root_replays_each_contract_only_within_its_slice() {
    let catalog = rolls::catalog();

    let got = run(&backtest(&catalog, ticks(ROOT)), Subscribe::Contracts);

    // Subscribed to both contracts all along: what reaches the probe is
    // what the run replays. August's up to 1 ns before the roll, not its
    // trade at the roll nor after; December's from the roll on, not its
    // trade at 23:15 nor its snapshot at 23:50.
    assert_eq!(
        got,
        [
            Got::Alerts(vec![]),
            tick(TickData::Trades, PRE, "thu 23:30:00"),
            tick(TickData::Depth, PRE, "thu 23:45:00"),
            tick(TickData::Trades, PRE, "thu 23:59:59.999999999"),
            tick(TickData::Trades, POST, "00:00:00"),
            tick(TickData::Depth, POST, "00:20:00"),
            tick(TickData::Trades, POST, "00:45:00"),
        ]
    );
}

#[test]
fn the_roller_switches_the_subscriptions_at_the_roll_and_tells_the_strategy() {
    let catalog = rolls::catalog();
    let backtest = backtest(&catalog, ticks(ROOT));

    let got = run(&backtest, Subscribe::Roller(Roller::new(ticks(ROOT))));

    // The roll is told after August's last tick, at its slice's last
    // nanosecond, and before December's first, at the roll.
    assert_eq!(
        got,
        [
            Got::Alerts(vec![before_roll()]),
            tick(TickData::Trades, PRE, "thu 23:30:00"),
            tick(TickData::Depth, PRE, "thu 23:45:00"),
            tick(TickData::Trades, PRE, "thu 23:59:59.999999999"),
            Got::Roll(
                Transition {
                    time: roll(),
                    pre: InstrumentId::from(PRE),
                    post: InstrumentId::from(POST),
                },
                before_roll(),
            ),
            tick(TickData::Trades, POST, "00:00:00"),
            tick(TickData::Depth, POST, "00:20:00"),
            tick(TickData::Trades, POST, "00:45:00"),
        ]
    );
}

#[test]
fn the_roller_alone_keeps_the_strategy_on_the_contract_standing_for_the_root() {
    let catalog = rolls::catalog();
    // Both contracts declared as themselves: each replayed over the whole
    // run, so what reaches the probe is what the roller subscribes to.
    let backtest = backtest(&catalog, [ticks(PRE), ticks(POST)].concat());

    let got = run(&backtest, Subscribe::Roller(Roller::new(ticks(ROOT))));

    // Not August's trade at the roll nor its ticks after it, not
    // December's trade at 23:15 nor its snapshot at 23:50.
    assert_eq!(
        got,
        [
            Got::Alerts(vec![before_roll()]),
            tick(TickData::Trades, PRE, "thu 23:30:00"),
            tick(TickData::Depth, PRE, "thu 23:45:00"),
            tick(TickData::Trades, PRE, "thu 23:59:59.999999999"),
            Got::Roll(
                Transition {
                    time: roll(),
                    pre: InstrumentId::from(PRE),
                    post: InstrumentId::from(POST),
                },
                before_roll(),
            ),
            tick(TickData::Trades, POST, "00:00:00"),
            tick(TickData::Depth, POST, "00:20:00"),
            tick(TickData::Trades, POST, "00:45:00"),
        ]
    );
}

#[test]
fn the_roller_of_a_plain_instrument_subscribes_once_and_sets_no_alert() {
    let catalog = rolls::catalog();
    let backtest = backtest(&catalog, ticks(POST));

    let got = run(&backtest, Subscribe::Roller(Roller::new(ticks(POST))));

    // December's contract named as itself: all its ticks of the run, no
    // roll.
    assert_eq!(
        got,
        [
            Got::Alerts(vec![]),
            tick(TickData::Trades, POST, "thu 23:15:00"),
            tick(TickData::Depth, POST, "thu 23:50:00"),
            tick(TickData::Trades, POST, "00:00:00"),
            tick(TickData::Depth, POST, "00:20:00"),
            tick(TickData::Trades, POST, "00:45:00"),
        ]
    );
}

/// What `got` holds of `instrument_id`'s ticks, in order.
fn ticks_of(got: &[Got], instrument_id: &str) -> Vec<Got> {
    got.iter()
        .filter(
            |got| matches!(got, Got::Tick(_, id, _) if *id == InstrumentId::from(instrument_id)),
        )
        .cloned()
        .collect()
}

/// August's ticks of the run, read as themselves: each once, the trade at
/// the roll and the ticks after it included.
fn pre_ticks_of_the_run() -> Vec<Got> {
    vec![
        tick(TickData::Trades, PRE, "thu 23:30:00"),
        tick(TickData::Depth, PRE, "thu 23:45:00"),
        tick(TickData::Trades, PRE, "thu 23:59:59.999999999"),
        tick(TickData::Trades, PRE, "00:00:00"),
        tick(TickData::Depth, PRE, "00:10:00"),
        tick(TickData::Trades, PRE, "00:30:00"),
    ]
}

#[test]
fn a_contract_declared_beside_its_root_is_replayed_once() {
    let catalog = rolls::catalog();
    let backtest = backtest(&catalog, [ticks(ROOT), ticks(PRE)].concat());

    let got = run(&backtest, Subscribe::Contracts);

    // August's contract stands for the root up to the roll and is read as
    // itself over the whole run: its ticks before the roll come once.
    assert_eq!(ticks_of(&got, PRE), pre_ticks_of_the_run());
}

#[test]
fn bars_on_a_root_built_from_quotes_are_refused_naming_the_bar_type() {
    let catalog = rolls::catalog();
    let bars = "GC.GLBX-30-MINUTE-MID-INTERNAL";
    let backtest = Backtest {
        bar_types: vec![BarType::from(bars)],
        ..backtest(&catalog, vec![])
    };

    let err = backtest
        .run(RollProbe {
            core: StrategyCore::new(StrategyConfig::default()),
            subscribe: Subscribe::Contracts,
            got: Rc::default(),
        })
        .unwrap_err()
        .to_string();

    assert!(err.contains(bars), "{err}");
}

#[test]
fn a_contract_missing_part_of_its_slice_is_refused_naming_the_slice() {
    let catalog = rolls::catalog_storing(false);

    let err = backtest(&catalog, ticks(ROOT))
        .run(RollProbe {
            core: StrategyCore::new(StrategyConfig::default()),
            subscribe: Subscribe::Contracts,
            got: Rc::default(),
        })
        .unwrap_err()
        .to_string();

    assert!(err.contains(&format!("trades of {POST}")), "{err}");
    assert!(
        err.contains("[2026-07-29T00:00:00Z, 2026-07-29T01:00:00Z] is missing"),
        "{err}"
    );
}
