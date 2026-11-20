use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    actor::indicators::ActorIndicator,
    cache::Cache,
    clock::{Clock, VirtualClock},
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEventCallback,
};
use nautilus_core::UUID4;
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId, TraderId},
    types::{Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;

use super::*;
use crate::strategy::{Declared, EmaArgs, IndicatorsConfig, Series};

const STRATEGY: &str = "PROBE-001";
const AUD: &str = "AUD/USD.SIM";
const GBP: &str = "GBP/USD.SIM";

/// 2026-10-26T10:07:30Z.
const NOW: u64 = 1_792_922_850_000_000_000;
/// 2026-10-26T10:15:00Z, the next quarter hour.
const QUARTER: u64 = 1_792_923_300_000_000_000;
const SECOND: u64 = 1_000_000_000;
/// How long after its time a decision fires: past the timer building the bar that closes then.
const MARGIN: u64 = 10_000_000;

#[derive(Debug)]
struct Probe {
    core: StrategyCore,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {}

/// A strategy registered without a node at `now`, as the EMA's tests do, and its clock.
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

/// The last mid, Nautilus' exponential average of one, `[indicators.mid]`.
const MID: Declared<EmaArgs> = Declared::new("mid");
/// Nautilus' exponential average of two mids, the mean of the first two once both came,
/// `[indicators.mid_2]`.
const MID_2: Declared<EmaArgs> = Declared::new("mid_2");

/// The tables of the features, as `[indicators]` holds them.
fn tables() -> IndicatorsConfig {
    toml::from_str(
        r#"
        mid = { period = 1, price_type = "MID" }
        mid_2 = { period = 2, price_type = "MID" }
        "#,
    )
    .unwrap()
}

/// The average of two mids of AUD/USD.
fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .feature(&[Series::quotes(InstrumentId::from(AUD))], MID_2, &tables())
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

/// A cadence not positive is refused, naming its interval.
#[rstest]
#[case::of_nought(0, Some("0s"))]
#[case::below_nought(-15, Some("15m ago"))]
#[case::positive(15, None)]
fn a_cadence_must_be_positive(#[case] minutes: i64, #[case] refused: Option<&str>) {
    let validated = every(minutes).validate();

    match refused {
        Some(interval) => {
            let err = validated.unwrap_err().to_string();
            assert!(err.contains(interval), "{err}");
        }
        None => validated.unwrap(),
    }
}

/// A cadence of 15 minutes decides at its grid times, plus its delay and the margin; started on a
/// grid time, or between a grid time and its delayed decision, that decision is still to come.
#[rstest]
#[case::started_off_the_grid(NOW, SignedDuration::ZERO, QUARTER + MARGIN)]
#[case::started_on_the_grid(QUARTER, SignedDuration::ZERO, QUARTER + MARGIN)]
#[case::delayed(NOW, SignedDuration::from_secs(5), QUARTER + 5 * SECOND + MARGIN)]
#[case::delayed_started_inside_its_delay(
    QUARTER + 2 * SECOND,
    SignedDuration::from_secs(5),
    QUARTER + 5 * SECOND + MARGIN
)]
fn a_cadence_decides_just_after_the_grid_of_its_interval(
    #[case] now: u64,
    #[case] delay: SignedDuration,
    #[case] next: u64,
) {
    let (probe, clock) = registered(now);

    start(&probe, every(15), delay).unwrap();

    assert_eq!(
        clock.borrow().next_time_ns(DECISION_TIMER),
        Some(UnixNanos::from(next))
    );
}

/// What is made up to a decision's firing, its grid time plus its delay and the margin, is that
/// decision's to read first; what comes after, the next one's. The grid starts at the epoch, and a
/// cadence of 4 ms, shorter than its margin, reads what is made less than an interval before the
/// epoch's decision fires there.
#[rstest]
#[case::before_the_grid_time(every(15), SignedDuration::ZERO, QUARTER - 1, QUARTER)]
#[case::at_the_grid_time(every(15), SignedDuration::ZERO, QUARTER, QUARTER)]
#[case::at_the_firing(every(15), SignedDuration::ZERO, QUARTER + MARGIN, QUARTER)]
#[case::after_the_firing(
    every(15),
    SignedDuration::ZERO,
    QUARTER + MARGIN + 1,
    QUARTER + 900 * SECOND
)]
#[case::at_a_delayed_firing(
    every(15),
    SignedDuration::from_secs(5),
    QUARTER + 5 * SECOND + MARGIN,
    QUARTER
)]
#[case::after_a_delayed_firing(
    every(15),
    SignedDuration::from_secs(5),
    QUARTER + 5 * SECOND + MARGIN + 1,
    QUARTER + 900 * SECOND
)]
#[case::at_the_epoch(every(15), SignedDuration::ZERO, 0, 0)]
#[case::at_the_epochs_firing(every(15), SignedDuration::ZERO, MARGIN, 0)]
#[case::after_the_epochs_firing(every(15), SignedDuration::ZERO, MARGIN + 1, 900 * SECOND)]
#[case::within_an_interval_before_the_epochs_firing(
    DecisionRule::Every(SignedDuration::from_millis(4)),
    SignedDuration::ZERO,
    7_000_000,
    0
)]
fn data_is_read_first_by_the_first_decision_firing_at_or_after_it(
    #[case] rule: DecisionRule,
    #[case] delay: SignedDuration,
    #[case] ns: u64,
    #[case] grid: u64,
) {
    assert_eq!(
        decision_of(rule, delay, UnixNanos::from(ns)),
        UnixNanos::from(grid)
    );
}

/// A cadence of 4 ms: what is made 6 ms after the epoch, a whole interval before the epoch's
/// decision fires, would be read first by a decision before the epoch.
#[rstest]
#[should_panic(expected = "a decision after the epoch")]
fn data_read_first_by_a_decision_before_the_epoch_is_refused() {
    let every_4_ms = DecisionRule::Every(SignedDuration::from_millis(4));

    let _ = decision_of(every_4_ms, SignedDuration::ZERO, UnixNanos::from(6_000_000));
}

#[rstest]
fn another_timer_is_no_decision() {
    let event = TimeEvent::new(
        "EMA-HOLD:O-1".into(),
        UUID4::new(),
        UnixNanos::from(NOW),
        UnixNanos::from(NOW),
    );

    assert!(!is_decision(&event));
}

#[rstest]
fn cold_features_give_no_decision_and_publish_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = features();
    features.handle_quote(&quote(0.7)).unwrap();

    assert_eq!(decide(&probe, &features, every(15), aud()), None);
    assert!(rows.borrow().is_empty());
}

#[rstest]
fn a_decision_publishes_the_features_keyed_by_the_strategy() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = features();
    features.handle_quote(&quote(0.7)).unwrap();
    features.handle_quote(&quote(0.8)).unwrap();

    let row = decide(&probe, &features, every(15), aud()).unwrap();

    assert_eq!(row.strategy_id, StrategyId::from(STRATEGY));
    assert_eq!(row.ts_event, UnixNanos::from(NOW));
    assert_eq!(row.ts_init, UnixNanos::from(NOW));
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

/// A market closed for the last 16 minutes: its last quote is older than the decision 15 minutes
/// ago.
#[rstest]
fn a_cadence_without_data_since_its_previous_decision_skips_and_publishes_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = warm_until(NOW - 16 * 60 * SECOND);

    assert_eq!(decide(&probe, &features, every(15), aud()), None);
    assert!(rows.borrow().is_empty());
}

/// The decision 15 minutes ago saw the data up to it: a quote made then is stale, one a nanosecond
/// later fresh.
#[rstest]
fn data_since_the_previous_decision_is_fresh_and_data_up_to_it_stale() {
    let (probe, _clock) = registered(NOW);
    let previous = NOW - 15 * 60 * SECOND;

    assert_eq!(
        decide(&probe, &warm_until(previous), every(15), aud()),
        None
    );
    assert!(decide(&probe, &warm_until(previous + 1), every(15), aud()).is_some());
}

/// The decision an interval after the epoch has the epoch's before it: a quote made at the epoch is
/// stale, one a nanosecond later fresh.
#[rstest]
fn the_decision_an_interval_after_the_epoch_judges_its_data_from_the_epoch() {
    let (probe, _clock) = registered(900 * SECOND);
    let warm_at = |ns: u64| {
        let features = features();
        features.handle_quote(&quote_of(AUD, 0.7, ns)).unwrap();
        features.handle_quote(&quote_of(AUD, 0.8, ns)).unwrap();
        features
    };

    assert_eq!(decide(&probe, &warm_at(0), every(15), aud()), None);
    assert!(decide(&probe, &warm_at(1), every(15), aud()).is_some());
}

/// A decision less than an interval after the epoch has no previous decision to judge its data
/// from.
#[rstest]
#[should_panic(expected = "a decision an interval after the epoch")]
fn a_decision_within_an_interval_of_the_epoch_is_refused() {
    let (probe, _clock) = registered(600 * SECOND);

    let _ = decide(&probe, &features(), every(15), aud());
}

/// The traded instrument's market is closed, another's open: the decision would trade on stale
/// prices.
#[rstest]
fn data_of_another_instrument_makes_no_decision_fresh() {
    let (probe, _clock) = registered(NOW);
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(InstrumentId::from(AUD))], MID, &tables)
        .feature(&[Series::quotes(InstrumentId::from(GBP))], MID, &tables)
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

/// No feature reads AUD/USD, which the strategy reads as trades alone: its last trade in the cache,
/// made a second ago, makes the decision fresh.
#[rstest]
fn a_trade_of_the_traded_instrument_in_the_cache_makes_the_decision_fresh() {
    let (probe, _clock) = registered(NOW);
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(InstrumentId::from(GBP))], MID, &tables())
        .build()
        .unwrap();
    features
        .handle_quote(&quote_of(GBP, 1.3, NOW - 16 * 60 * SECOND))
        .unwrap();
    assert_eq!(decide(&probe, &features, every(15), aud()), None);

    probe
        .cache_rc()
        .borrow_mut()
        .add_trade(TradeTick::new(
            aud(),
            Price::from("0.70000"),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::from("1"),
            UnixNanos::from(NOW - SECOND),
            UnixNanos::from(NOW - SECOND),
        ))
        .unwrap();

    assert!(decide(&probe, &features, every(15), aud()).is_some());
}

#[rstest]
fn the_fingerprint_changes_with_the_rule() {
    let features = features();

    assert_eq!(
        fingerprint(&features, every(15), SignedDuration::ZERO),
        fingerprint(&features, every(15), SignedDuration::ZERO)
    );
    assert_ne!(
        fingerprint(&features, every(15), SignedDuration::ZERO),
        fingerprint(&features, every(30), SignedDuration::ZERO)
    );
    assert_ne!(
        fingerprint(&features, every(15), SignedDuration::ZERO),
        features.fingerprint()
    );
}

/// A cadence decides its build delay after the grid (`start`): another delay, 5 s for composite
/// bars or none, is another fingerprint.
#[rstest]
fn the_fingerprint_changes_with_the_build_delay() {
    let features = features();
    let delayed = |secs| fingerprint(&features, every(15), SignedDuration::from_secs(secs));

    assert_ne!(delayed(5), delayed(0));
    assert_ne!(delayed(5), delayed(6));
}

/// Without a build delay the rule's part is as it was before the delay was keyed, so the datasets
/// and stores of strategies reading no composite bars keep their fingerprints.
#[rstest]
fn a_fingerprint_without_a_build_delay_keeps_the_rules_part_as_it_was() {
    let features = features();

    assert_eq!(
        fingerprint(&features, every(15), SignedDuration::ZERO),
        features.fingerprint().with(b"every 900000000000 ns")
    );
}
