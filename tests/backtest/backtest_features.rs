//! A strategy's feature set fed by Nautilus and its decisions on a clock
//! cadence (`model::features`, `model::decide`): the probe declares a
//! moving average of the last three quote mids and one of the last two
//! hour bars' closes, registers them and decides every 30 minutes,
//! recording the rows published. On a ramp of a cent a minute, the rows
//! hold exactly the data up to each decision, the bar closing at a
//! decision included, and none before both features are initialized. One
//! node per thread, so one run per test.

use std::{cell::RefCell, rc::Rc};

use crate::common::{INSTRUMENT_ID, MID_BAR_TYPE, backtest, catalog, minute_quotes, ts};
use helix_trader::model::{
    decide::{self, DecisionRule, FeatureRow},
    features::FeatureSet,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActor,
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    data::{BarType, CustomData},
    enums::PriceType,
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

const STRATEGY: &str = "FEATURES-001";

/// `hh:mm` UTC on the day of the ramp, Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// The mid of the minute closing at `close`: 100 at 09:00, a cent more
/// each minute.
fn mid(close: Timestamp) -> f64 {
    let minutes = close.duration_since(at("09:00")).as_mins();
    100.0 + 0.01 * minutes as f64
}

/// Decides on its features every 30 minutes and records each row
/// published for it.
#[derive(Debug)]
struct FeatureProbe {
    core: StrategyCore,
    features: Rc<FeatureSet>,
    rows: Rc<RefCell<Vec<FeatureRow>>>,
}

impl FeatureProbe {
    fn new() -> (Self, Rc<RefCell<Vec<FeatureRow>>>) {
        let instrument_id = InstrumentId::from(INSTRUMENT_ID);
        let features = FeatureSet::builder()
            .quotes(
                instrument_id,
                "mid_3_quotes",
                SimpleMovingAverage::new(3, Some(PriceType::Mid)),
            )
            .bars(
                BarType::from(MID_BAR_TYPE),
                "close_2_hours",
                SimpleMovingAverage::new(2, None),
            )
            .code(include_str!("backtest_features.rs"))
            .build()
            .unwrap();
        let rows = Rc::new(RefCell::new(vec![]));
        let probe = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(STRATEGY)),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            features,
            rows: Rc::clone(&rows),
        };
        (probe, rows)
    }
}

nautilus_strategy!(FeatureProbe);

impl DataActor for FeatureProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let features = Rc::clone(&self.features);
        features.register(self);
        let rows = Rc::clone(&self.rows);
        let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
            let row = data.data.as_any().downcast_ref::<FeatureRow>().unwrap();
            rows.borrow_mut().push(row.clone());
        });
        let topic = get_custom_topic(&FeatureRow::data_type(StrategyId::from(STRATEGY)));
        msgbus::subscribe_any(topic.into(), handler, None);
        self.subscribe_quotes(InstrumentId::from(INSTRUMENT_ID), None, None);
        self.subscribe_bars(BarType::from(MID_BAR_TYPE), None, None);
        let every = DecisionRule::Every(SignedDuration::from_mins(30));
        decide::start(self, every, SignedDuration::ZERO)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            decide::decide(self, &self.features);
        }
        Ok(())
    }
}

#[test]
fn each_decision_publishes_the_features_of_the_data_up_to_it() {
    let quotes = minute_quotes(at("09:00"), at("13:00"), |close| Some(mid(close)));
    // Stored from 08:00: the run reads from 1 ns before its start.
    let catalog = catalog(&quotes, at("08:00"), at("13:00"));
    let (probe, rows) = FeatureProbe::new();

    backtest(
        catalog.path(),
        at("09:00"),
        at("12:45"),
        SignedDuration::ZERO,
        "0 EUR",
    )
    .run(probe)
    .unwrap();

    // A decision on the half hour fires 10 ms after it. Then the last three
    // quotes close on it and the two minutes before, a mean of the mid a
    // minute before, the next quote closing a minute later; the last two
    // bars close on the hour at or before it, the one closing on it
    // included. The decisions at 09:30, 10:00 and 10:30 come before the
    // second bar.
    let expected = [
        (
            "11:00",
            mid(at("10:59")),
            (mid(at("10:00")) + mid(at("11:00"))) / 2.0,
        ),
        (
            "11:30",
            mid(at("11:29")),
            (mid(at("10:00")) + mid(at("11:00"))) / 2.0,
        ),
        (
            "12:00",
            mid(at("11:59")),
            (mid(at("11:00")) + mid(at("12:00"))) / 2.0,
        ),
        (
            "12:30",
            mid(at("12:29")),
            (mid(at("11:00")) + mid(at("12:00"))) / 2.0,
        ),
    ];
    let rows = rows.borrow();
    let times: Vec<UnixNanos> = rows.iter().map(|row| row.ts_event).collect();
    let expected_times: Vec<UnixNanos> = expected
        .iter()
        .map(|(t, ..)| (at(t) + SignedDuration::from_millis(10)).into())
        .collect();
    assert_eq!(times, expected_times);
    for (row, (time, quotes, bars)) in rows.iter().zip(expected) {
        assert_eq!(row.strategy_id, StrategyId::from(STRATEGY));
        assert!(
            (row.features[0] - quotes).abs() < 1e-9 && (row.features[1] - bars).abs() < 1e-9,
            "at {time}: {:?}, expected [{quotes}, {bars}]",
            row.features
        );
    }
}
