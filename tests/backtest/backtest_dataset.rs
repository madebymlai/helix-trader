//! The dataset store (`model::dataset`) on a fixture catalog: a probe
//! deciding every hour on the last mid of IC's gold opens the targets of
//! each decision through `Backtest::run_dataset`, and records the target
//! rows published for it; a dataset build replays the days it lacks
//! through it and stores the rows in the catalog. A second build of the
//! same span replays no day, and the rows read back are those the replay
//! settled over the days built. One node per thread: the second build
//! runs none.

use std::{cell::RefCell, rc::Rc};

use crate::common::ic;
use helix_trader::{
    backtest::{Backtest, commission_rate},
    model::{
        dataset::{Dataset, DatasetRow},
        decide::{self, DecisionRule},
        features::FeatureSet,
        targets::{Barrier, Distance, TargetRow, Targets, TargetsConfig},
    },
    strategy::TickSeries,
};
use jiff::{SignedDuration, Timestamp, civil::Date};
use nautilus_common::{
    actor::DataActor,
    logging::logger::LoggerConfig,
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    data::{CustomData, NautilusDataType},
    enums::{OmsType, PriceType},
    events::{OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId},
    types::{Money, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

const STRATEGY: &str = "DATASET-001";

/// Decides every hour on the last mid of IC's gold and opens the targets
/// of each decision; records the rows published for it.
#[derive(Debug)]
struct DatasetProbe {
    core: StrategyCore,
    features: Rc<FeatureSet>,
    targets: Rc<Targets>,
    rows: Rc<RefCell<Vec<TargetRow>>>,
}

nautilus_strategy!(DatasetProbe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.targets.on_order_filled(event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.targets.on_order_denied(&event);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.targets.on_order_rejected(&event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        Rc::clone(&self.targets).on_position_closed(self, &event);
    }
});

impl DataActor for DatasetProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let features = Rc::clone(&self.features);
        features.register(self);
        let rows = Rc::clone(&self.rows);
        let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
            let row = data.data.as_any().downcast_ref::<TargetRow>().unwrap();
            rows.borrow_mut().push(row.clone());
        });
        let topic = get_custom_topic(&TargetRow::data_type(StrategyId::from(STRATEGY)));
        msgbus::subscribe_any(topic.into(), handler, None);
        self.subscribe_quotes(InstrumentId::from(ic::INSTRUMENT_ID), None, None);
        decide::start(self, rule(), SignedDuration::ZERO)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            if let Some(row) = decide::decide(self, &self.features) {
                Rc::clone(&self.targets).open(self, row)?;
            }
            return Ok(());
        }
        Rc::clone(&self.targets).on_time_event(self, event)
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.targets.on_stop();
        Ok(())
    }
}

fn rule() -> DecisionRule {
    DecisionRule::Every(SignedDuration::from_hours(1))
}

/// The probe's one feature, the last mid of IC's gold.
fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(
            InstrumentId::from(ic::INSTRUMENT_ID),
            "mid",
            SimpleMovingAverage::new(1, Some(PriceType::Mid)),
        )
        .build()
        .unwrap()
}

/// A bracket 7.5 bps away on both sides closed after 2 hours, the longest
/// target, and a horizon of 1 hour, of one ounce.
fn targets() -> TargetsConfig {
    TargetsConfig::builder()
        .instrument_id(InstrumentId::from(ic::INSTRUMENT_ID))
        .quantity(Quantity::from(1))
        .barrier(Barrier {
            take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
            stop: Distance::BasisPoints(Decimal::new(75, 1)),
            limit: SignedDuration::from_hours(2),
            atr: None,
        })
        .horizon(SignedDuration::from_hours(1))
        .build()
}

/// A deterministic random walk in [0, 1).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// IC's gold over `[from, to)`, a quote every 5 s, the bid a random walk
/// of up to 0.30 a step, the ask 0.20 above.
fn walk(from: Timestamp, to: Timestamp) -> tempfile::TempDir {
    let mut walk = Lcg(7);
    let mut bid = 4000.0_f64;
    let mut quotes = vec![];
    let mut time = from;
    while time < to {
        bid = ((bid + (walk.next() - 0.5) * 0.6) * 100.0).round() / 100.0;
        let (b, a) = (format!("{bid:.2}"), format!("{:.2}", bid + 0.2));
        quotes.push(ic::quote(time, &b, &a));
        time += SignedDuration::from_secs(5);
    }
    ic::catalog_of(quotes, from, to)
}

/// The replay of a build: the probe over `[from, to]` on IC's hedging
/// venue, 30 USD per million traded, 10M USD; the rows it settled.
fn replay(catalog: &tempfile::TempDir, from: Timestamp, to: Timestamp) -> Vec<TargetRow> {
    let rows = Rc::new(RefCell::new(vec![]));
    let probe = DatasetProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        features: features(),
        targets: Targets::new(targets()).unwrap(),
        rows: Rc::clone(&rows),
    };
    let backtest = Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(ic::INSTRUMENT_ID),
        }],
        start: from,
        end: to,
        warmup: SignedDuration::ZERO,
        fee_model: commission_rate(Decimal::new(3, 5)).unwrap(),
        balance: Money::from("10000000 USD"),
        leverage: Decimal::ONE,
        venue_oms: |_| OmsType::Hedging,
        logging: LoggerConfig::default(),
    };
    backtest.run_dataset(probe).unwrap();
    rows.take()
}

/// Two days, the 21st and 22nd of October 2026, quotes from 23:00 the day
/// before (the engine starts 1 ns before the run) to 03:00 of the 23rd:
/// the replay runs to 02:00, the end of the 22nd plus the bracket's two
/// hours, and every decision of the two days settles by then.
#[test]
fn a_second_build_of_the_same_span_replays_no_day_and_rows_read_back_as_written() {
    let (first, last) = (Date::constant(2026, 10, 21), Date::constant(2026, 10, 22));
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(51),
    );
    let dataset = Dataset::new(
        catalog.path(),
        StrategyId::from(STRATEGY),
        &features(),
        rule(),
        targets(),
    )
    .unwrap();
    let (mut replayed, mut settled) = (vec![], vec![]);

    let built = dataset
        .build(first, last, |from, to| {
            replayed.push((from, to));
            settled = replay(&catalog, from, to);
            Ok(settled.clone())
        })
        .unwrap();
    let rebuilt = dataset
        .build(first, last, |_, _| panic!("the second build replayed"))
        .unwrap();

    assert_eq!(built, [first, last]);
    assert_eq!(
        replayed,
        [(
            ic::at("00:00:00"),
            ic::at("00:00:00") + SignedDuration::from_hours(50)
        )]
    );
    assert!(rebuilt.is_empty());
    let end = ic::at("00:00:00") + SignedDuration::from_hours(48);
    let mut written: Vec<DatasetRow> = settled
        .iter()
        .filter(|row| Timestamp::from(row.decision.ts_event) < end)
        .map(|row| {
            DatasetRow::new(
                row.decision.features.clone(),
                row.values(),
                row.settled_at,
                row.decision.ts_event,
                row.decision.ts_init,
            )
        })
        .collect();
    written.sort_by_key(|row| row.ts_init);
    // A decision every hour of the two days, each settled.
    assert_eq!(written.len(), 48);
    assert_eq!(dataset.read(ic::at("00:00:00"), end).unwrap(), written);
}

/// `hh:mm:ss` UTC on `date`.
fn on(date: Date, time: &str) -> Timestamp {
    format!("{date}T{time}Z").parse().unwrap()
}

/// IC's gold as `walk` from 23:00 on Wednesday 21 October 2026 to 06:00
/// on Tuesday 27th, without quotes over the weekend, from 21:00 on Friday
/// 23rd to 22:00 on Sunday 25th.
fn weekend_walk() -> tempfile::TempDir {
    let (from, to) = (
        on(Date::constant(2026, 10, 21), "23:00:00"),
        on(Date::constant(2026, 10, 27), "06:00:00"),
    );
    let (close, open) = (
        on(Date::constant(2026, 10, 23), "21:00:00"),
        on(Date::constant(2026, 10, 25), "22:00:00"),
    );
    let mut walk = Lcg(7);
    let mut bid = 4000.0_f64;
    let mut quotes = vec![];
    let mut time = from;
    while time < to {
        if time < close || time >= open {
            bid = ((bid + (walk.next() - 0.5) * 0.6) * 100.0).round() / 100.0;
            let (b, a) = (format!("{bid:.2}"), format!("{:.2}", bid + 0.2));
            quotes.push(ic::quote(time, &b, &a));
        }
        time += SignedDuration::from_secs(5);
    }
    ic::catalog_of(quotes, from, to)
}

/// The rows of Friday 23 October 2026 stored by a build from Thursday
/// 22nd to `last`, on a catalog of its own: on a thread of its own, one
/// node per thread.
fn friday_stored_by_a_build_to(last: Date) -> Vec<DatasetRow> {
    std::thread::spawn(move || {
        let catalog = weekend_walk();
        let dataset = Dataset::new(
            catalog.path(),
            StrategyId::from(STRATEGY),
            &features(),
            rule(),
            targets(),
        )
        .unwrap();
        dataset
            .build(Date::constant(2026, 10, 22), last, |from, to| {
                Ok(replay(&catalog, from, to))
            })
            .unwrap();
        let friday = Date::constant(2026, 10, 23);
        dataset
            .read(on(friday, "00:00:00"), on(friday, "23:59:59"))
            .unwrap()
    })
    .join()
    .unwrap()
}

/// A build cut on Friday replays to 02:00 on Saturday, one cut on Monday
/// through the weekend to 02:00 on Tuesday. The decisions after Friday's
/// last quote, 21:00, still settle at their time limits: the venue fills
/// the time close, a market order, on the last quote it has, as it fills
/// any market order between quotes, so neither build waits for Sunday's
/// open, and both store the same Friday.
#[test]
fn a_day_is_stored_the_same_by_a_build_cut_before_a_weekend_and_one_cut_after() {
    let cut_before = friday_stored_by_a_build_to(Date::constant(2026, 10, 23));
    let cut_after = friday_stored_by_a_build_to(Date::constant(2026, 10, 26));

    assert_eq!(cut_before.len(), 24, "{cut_before:#?}");
    assert_eq!(cut_before, cut_after);
}
