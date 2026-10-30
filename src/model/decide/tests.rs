use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEventCallback,
};
use nautilus_core::UUID4;
use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    data::QuoteTick,
    enums::PriceType,
    identifiers::{InstrumentId, TraderId},
    types::{Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

use super::*;
use nautilus_common::actor::indicators::ActorIndicator;

const STRATEGY: &str = "PROBE-001";
const AUD: &str = "AUD/USD.SIM";
const GBP: &str = "GBP/USD.SIM";

/// 2026-10-26T10:07:30Z.
const NOW: u64 = 1_792_922_850_000_000_000;
/// 2026-10-26T10:15:00Z, the next quarter hour.
const QUARTER: u64 = 1_792_923_300_000_000_000;
const SECOND: u64 = 1_000_000_000;
/// How long after its time a decision fires: past the timer building
/// the bar that closes then.
const MARGIN: u64 = 10_000_000;

#[derive(Debug)]
struct Probe {
    core: StrategyCore,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {}

/// A strategy registered without a node at `now`, as the EMA's tests
/// do, and its clock.
fn registered(now: u64) -> (Probe, Rc<RefCell<VirtualClock>>) {
    let mut probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
    };
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    clock.borrow_mut().advance_time(UnixNanos::from(now), true);
    clock
        .borrow_mut()
        .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let portfolio = Rc::new(RefCell::new(Portfolio::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    probe
        .core
        .register(TraderId::from("HELIX-001"), clock.clone(), cache, portfolio)
        .unwrap();
    (probe, clock)
}

fn aud() -> InstrumentId {
    InstrumentId::from(AUD)
}

fn every(minutes: i64) -> DecisionRule {
    DecisionRule::Every(SignedDuration::from_mins(minutes))
}

/// The mean of the last two mids of AUD/USD.
fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(
            InstrumentId::from(AUD),
            "mid_2",
            SimpleMovingAverage::new(2, Some(PriceType::Mid)),
        )
        .build()
        .unwrap()
}

/// A quote of `instrument` with the mid `mid`, made at `ns`.
fn quote_of(instrument: &str, mid: f64, ns: u64) -> QuoteTick {
    QuoteTick::new(
        InstrumentId::from(instrument),
        Price::new(mid - 0.0001, 5),
        Price::new(mid + 0.0001, 5),
        Quantity::from(1),
        Quantity::from(1),
        UnixNanos::from(ns),
        UnixNanos::from(ns),
    )
}

/// A quote of AUD/USD with the mid `mid`, a second before `NOW`.
fn quote(mid: f64) -> QuoteTick {
    quote_of(AUD, mid, NOW - SECOND)
}

/// The rows published for `STRATEGY` from now on.
fn published() -> Rc<RefCell<Vec<FeatureRow>>> {
    let rows = Rc::new(RefCell::new(vec![]));
    let sink = Rc::clone(&rows);
    let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
        let row = data.data.as_any().downcast_ref::<FeatureRow>().unwrap();
        sink.borrow_mut().push(row.clone());
    });
    let topic = get_custom_topic(&FeatureRow::data_type(StrategyId::from(STRATEGY)));
    msgbus::subscribe_any(topic.into(), handler, None);
    rows
}

#[test]
fn a_rule_reads_from_toml() {
    #[derive(Deserialize)]
    struct Config {
        decision: DecisionRule,
    }
    let read = |toml: &str| toml::from_str::<Config>(toml).unwrap().decision;

    assert_eq!(read(r#"decision = { every = "15m" }"#), every(15));
    assert_eq!(read(r#"decision = "event""#), DecisionRule::Event);
}

#[test]
fn a_cadence_must_be_positive() {
    assert!(every(0).validate().is_err());
    assert!(every(-15).validate().is_err());
    assert!(every(15).validate().is_ok());
    assert!(DecisionRule::Event.validate().is_ok());
}

#[test]
fn a_cadence_decides_just_after_the_grid_of_its_interval() {
    let (probe, clock) = registered(NOW);

    start(&probe, every(15), SignedDuration::ZERO).unwrap();

    assert_eq!(
        clock.borrow().next_time_ns(DECISION_TIMER),
        Some(UnixNanos::from(QUARTER + MARGIN))
    );
}

/// Started on a grid time, before its decision: that decision is still
/// to come.
#[test]
fn a_cadence_started_on_the_grid_keeps_its_decision() {
    let (probe, clock) = registered(QUARTER);

    start(&probe, every(15), SignedDuration::ZERO).unwrap();

    assert_eq!(
        clock.borrow().next_time_ns(DECISION_TIMER),
        Some(UnixNanos::from(QUARTER + MARGIN))
    );
}

#[test]
fn a_delayed_cadence_decides_its_delay_after_the_grid() {
    let (probe, clock) = registered(NOW);

    start(&probe, every(15), SignedDuration::from_secs(5)).unwrap();

    assert_eq!(
        clock.borrow().next_time_ns(DECISION_TIMER),
        Some(UnixNanos::from(QUARTER + 5 * SECOND + MARGIN))
    );
}

/// Started between a grid time and its delayed decision: that decision
/// is still to come.
#[test]
fn a_delayed_cadence_started_inside_its_delay_keeps_the_next_decision() {
    let (probe, clock) = registered(QUARTER + 2 * SECOND);

    start(&probe, every(15), SignedDuration::from_secs(5)).unwrap();

    assert_eq!(
        clock.borrow().next_time_ns(DECISION_TIMER),
        Some(UnixNanos::from(QUARTER + 5 * SECOND + MARGIN))
    );
}

/// Nautilus fires the timer at its start plus whole intervals: the
/// decisions after the first stay on the grid.
#[test]
fn the_cadence_keeps_the_grid_after_its_first_decision() {
    let (probe, clock) = registered(NOW);
    start(&probe, every(15), SignedDuration::ZERO).unwrap();

    let events = clock
        .borrow_mut()
        .advance_time(UnixNanos::from(QUARTER + 15 * 60 * SECOND + MARGIN), true);

    let times: Vec<u64> = events.iter().map(|e| e.ts_event.as_u64()).collect();
    assert_eq!(
        times,
        [QUARTER + MARGIN, QUARTER + 15 * 60 * SECOND + MARGIN]
    );
    assert!(events.iter().all(is_decision));
}

#[test]
fn an_event_rule_sets_no_timer() {
    let (probe, clock) = registered(NOW);

    start(&probe, DecisionRule::Event, SignedDuration::ZERO).unwrap();

    assert_eq!(clock.borrow().timer_count(), 0);
}

#[test]
fn another_timer_is_no_decision() {
    let event = TimeEvent::new(
        "EMA-HOLD:O-1".into(),
        UUID4::new(),
        UnixNanos::from(NOW),
        UnixNanos::from(NOW),
    );

    assert!(!is_decision(&event));
}

#[test]
fn cold_features_give_no_decision_and_publish_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = features();
    features.handle_quote(&quote(0.7)).unwrap();

    assert_eq!(decide(&probe, &features, every(15), aud()), None);
    assert!(rows.borrow().is_empty());
}

#[test]
fn a_decision_publishes_the_features_keyed_by_the_strategy() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = features();
    features.handle_quote(&quote(0.7)).unwrap();
    features.handle_quote(&quote(0.8)).unwrap();

    let row = decide(&probe, &features, every(15), aud()).unwrap();

    assert_eq!(row.strategy_id, StrategyId::from(STRATEGY));
    assert_eq!(row.ts_event, UnixNanos::from(NOW));
    assert_eq!(row.features.len(), 1);
    assert!((row.features[0] - 0.75).abs() < 1e-9, "{row:?}");
    assert_eq!(*rows.borrow(), [row]);
}

/// Warm features whose last quote of AUD/USD was made at `ns`.
fn warm_until(ns: u64) -> Rc<FeatureSet> {
    let features = features();
    features
        .handle_quote(&quote_of(AUD, 0.7, ns - SECOND))
        .unwrap();
    features.handle_quote(&quote_of(AUD, 0.8, ns)).unwrap();
    features
}

/// A market closed for the last 16 minutes: its last quote is older
/// than the decision 15 minutes ago.
#[test]
fn a_cadence_without_data_since_its_previous_decision_skips_and_publishes_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = warm_until(NOW - 16 * 60 * SECOND);

    assert_eq!(decide(&probe, &features, every(15), aud()), None);
    assert!(rows.borrow().is_empty());
}

/// The decision 15 minutes ago saw the data up to it: a quote made then
/// is stale, one a nanosecond later fresh.
#[test]
fn data_since_the_previous_decision_is_fresh_and_data_up_to_it_stale() {
    let (probe, _clock) = registered(NOW);
    let previous = NOW - 15 * 60 * SECOND;

    assert_eq!(
        decide(&probe, &warm_until(previous), every(15), aud()),
        None
    );
    assert!(decide(&probe, &warm_until(previous + 1), every(15), aud()).is_some());
}

/// The traded instrument's market is closed, another's open: the
/// decision would trade on stale prices.
#[test]
fn data_of_another_instrument_makes_no_decision_fresh() {
    let (probe, _clock) = registered(NOW);
    let features = FeatureSet::builder()
        .quotes(
            InstrumentId::from(AUD),
            "aud",
            SimpleMovingAverage::new(1, Some(PriceType::Mid)),
        )
        .quotes(
            InstrumentId::from(GBP),
            "gbp",
            SimpleMovingAverage::new(1, Some(PriceType::Mid)),
        )
        .build()
        .unwrap();
    features
        .handle_quote(&quote_of(AUD, 0.7, NOW - 16 * 60 * SECOND))
        .unwrap();
    features
        .handle_quote(&quote_of(GBP, 1.3, NOW - SECOND))
        .unwrap();

    assert_eq!(decide(&probe, &features, every(15), aud()), None);
    assert!(decide(&probe, &features, every(15), InstrumentId::from(GBP)).is_some());
}

#[test]
fn an_event_decides_on_whatever_data_came_last() {
    let (probe, _clock) = registered(NOW);
    let features = warm_until(NOW - 24 * 60 * 60 * SECOND);

    assert!(decide(&probe, &features, DecisionRule::Event, aud()).is_some());
}

#[test]
fn the_fingerprint_changes_with_the_rule() {
    let features = features();

    assert_eq!(
        fingerprint(&features, every(15)),
        fingerprint(&features, every(15))
    );
    assert_ne!(
        fingerprint(&features, every(15)),
        fingerprint(&features, every(30))
    );
    assert_ne!(
        fingerprint(&features, every(15)),
        fingerprint(&features, DecisionRule::Event)
    );
    assert_ne!(fingerprint(&features, every(15)), features.fingerprint());
}

/// The fingerprint hashes this module's code: its tests live in a
/// file of their own, so editing one changes no fingerprint.
#[test]
fn the_fingerprinted_code_holds_no_test() {
    assert!(!include_str!("../decide.rs").contains("#[test]"));
}
