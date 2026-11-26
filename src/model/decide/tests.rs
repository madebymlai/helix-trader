use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    actor::indicators::ActorIndicator,
    cache::Cache,
    clock::{Clock, VirtualClock},
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEventCallback,
};
use nautilus_core::{UUID4, datetime::NANOSECONDS_IN_MILLISECOND};
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
use crate::{
    model::features::testing::RISE,
    strategy::{Declared, EmaArgs, IndicatorsConfig, Series},
};

const STRATEGY: &str = "PROBE-001";
const AUD: &str = "AUD/USD.SIM";
const GBP: &str = "GBP/USD.SIM";

/// 2026-10-26T10:07:30Z.
const NOW: u64 = 1_792_922_850_000_000_000;
/// 2026-10-26T10:15:00Z, the next quarter hour.
const QUARTER: u64 = 1_792_923_300_000_000_000;
const SECOND: u64 = 1_000_000_000;
const MARGIN: u64 = 10_000_000;

#[derive(Debug)]
struct Probe {
    core: StrategyCore,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {}

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

const MID: Declared<EmaArgs> = Declared::new("mid");
/// An exponential average of two mids. Once both have arrived, it is their mean.
const MID_2: Declared<EmaArgs> = Declared::new("mid_2");

fn tables() -> IndicatorsConfig {
    toml::from_str(
        r#"
        mid = { period = 1, price_type = "MID" }
        mid_2 = { period = 2, price_type = "MID" }
        rise = { step = 0.0 }
        "#,
    )
    .unwrap()
}

fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .feature(&[Series::quotes(InstrumentId::from(AUD))], MID_2, &tables())
        .build()
        .unwrap()
}

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

fn quote(mid: f64) -> QuoteTick {
    quote_of(AUD, mid, NOW - SECOND)
}

/// The decision timer's event scheduled at `ns`.
fn fired(ns: u64) -> TimeEvent {
    TimeEvent::new(
        DECISION_TIMER.into(),
        UUID4::new(),
        UnixNanos::from(ns),
        UnixNanos::from(ns),
    )
}

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

/// A decision reads data made up to its firing, its grid time plus delay and `MARGIN`. A 4 ms
/// cadence, shorter than `MARGIN`, reads data made less than an interval before the epoch's firing.
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

/// Data made at 6 ms, an interval before the epoch's firing at 10 ms, would be read first by a
/// decision before the epoch.
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
#[should_panic(expected = "EMA-HOLD:O-1 is no decision")]
fn another_timers_event_is_refused_as_a_decision() {
    let (probe, _clock) = registered(NOW);
    let hold = TimeEvent::new(
        "EMA-HOLD:O-1".into(),
        UUID4::new(),
        UnixNanos::from(NOW),
        UnixNanos::from(NOW),
    );

    let _ = decide(&probe, &hold, &features(), every(15), aud());
}

#[rstest]
fn cold_features_give_no_decision_and_publish_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = features();
    features.handle_quote(&quote(0.7)).unwrap();

    assert_eq!(
        decide(&probe, &fired(NOW), &features, every(15), aud()),
        None
    );
    assert!(rows.borrow().is_empty());
}

#[rstest]
fn a_decision_publishes_the_features_keyed_by_the_strategy() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = features();
    features.handle_quote(&quote(0.7)).unwrap();
    features.handle_quote(&quote(0.8)).unwrap();

    let row = decide(&probe, &fired(NOW), &features, every(15), aud()).unwrap();

    assert_eq!(row.strategy_id, StrategyId::from(STRATEGY));
    assert_eq!(row.ts_event, UnixNanos::from(NOW));
    assert_eq!(row.ts_init, UnixNanos::from(NOW));
    assert_eq!(row.features.len(), 1);
    assert!((row.features[0] - 0.75).abs() < 1e-9, "{row:?}");
    assert_eq!(*rows.borrow(), [row]);
}

fn warm_until(ns: u64) -> Rc<FeatureSet> {
    let features = features();
    features
        .handle_quote(&quote_of(AUD, 0.7, ns - SECOND))
        .unwrap();
    features.handle_quote(&quote_of(AUD, 0.8, ns)).unwrap();
    features
}

#[rstest]
fn a_cadence_without_data_since_its_previous_decision_skips_and_publishes_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = warm_until(NOW - 16 * 60 * SECOND);

    assert_eq!(
        decide(&probe, &fired(NOW), &features, every(15), aud()),
        None
    );
    assert!(rows.borrow().is_empty());
}

#[rstest]
fn data_since_the_previous_decision_is_fresh_and_data_up_to_it_stale() {
    let (probe, _clock) = registered(NOW);
    let previous = NOW - 15 * 60 * SECOND;

    assert_eq!(
        decide(&probe, &fired(NOW), &warm_until(previous), every(15), aud()),
        None
    );
    assert!(
        decide(
            &probe,
            &fired(NOW),
            &warm_until(previous + 1),
            every(15),
            aud()
        )
        .is_some()
    );
}

#[rstest]
fn the_decision_an_interval_after_the_epoch_judges_its_data_from_the_epoch() {
    let (probe, _clock) = registered(900 * SECOND);
    let warm_at = |ns: u64| {
        let features = features();
        features.handle_quote(&quote_of(AUD, 0.7, ns)).unwrap();
        features.handle_quote(&quote_of(AUD, 0.8, ns)).unwrap();
        features
    };

    let decision = fired(900 * SECOND);

    assert_eq!(
        decide(&probe, &decision, &warm_at(0), every(15), aud()),
        None
    );
    assert!(decide(&probe, &decision, &warm_at(1), every(15), aud()).is_some());
}

#[rstest]
#[should_panic(expected = "a decision an interval after the epoch")]
fn a_decision_within_an_interval_of_the_epoch_is_refused() {
    let (probe, _clock) = registered(600 * SECOND);

    let _ = decide(&probe, &fired(600 * SECOND), &features(), every(15), aud());
}

/// The traded instrument is closed and another is open.
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

    assert_eq!(
        decide(&probe, &fired(NOW), &features, every(15), aud()),
        None
    );
    assert!(
        decide(
            &probe,
            &fired(NOW),
            &features,
            every(15),
            InstrumentId::from(GBP)
        )
        .is_some()
    );
}

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
    assert_eq!(
        decide(&probe, &fired(NOW), &features, every(15), aud()),
        None
    );

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

    assert!(decide(&probe, &fired(NOW), &features, every(15), aud()).is_some());
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

#[rstest]
fn the_fingerprint_changes_with_the_build_delay() {
    let features = features();
    let delayed = |secs| fingerprint(&features, every(15), SignedDuration::from_secs(secs));

    assert_ne!(delayed(5), delayed(0));
    assert_ne!(delayed(5), delayed(6));
}

/// Existing datasets keep their fingerprints.
#[rstest]
fn a_fingerprint_without_a_build_delay_keeps_the_rules_part_as_it_was() {
    let features = features();

    assert_eq!(
        fingerprint(&features, every(15), SignedDuration::ZERO),
        features.fingerprint().with(b"every 900000000000 ns")
    );
}

/// The grid point before the decision at `NOW` on `every(15)`.
const PREVIOUS: u64 = NOW - 15 * 60 * SECOND;

/// The features of `features`, warm and fresh at `NOW`, decided only at a rise of GBP's mid.
fn filtered() -> Rc<FeatureSet> {
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(aud())], MID_2, &tables())
        .event_filter(&[Series::quotes(InstrumentId::from(GBP))], RISE, &tables())
        .build()
        .unwrap();
    features
        .handle_quote(&quote_of(AUD, 0.7, NOW - 2 * SECOND))
        .unwrap();
    features.handle_quote(&quote(0.8)).unwrap();
    features
}

/// Hands `features` GBP's mids, each at its time.
fn gbp(features: &FeatureSet, mids: &[(f64, u64)]) {
    for &(mid, ns) in mids {
        features.handle_quote(&quote_of(GBP, mid, ns)).unwrap();
    }
}

#[rstest]
fn a_cold_event_filter_gives_no_decision_and_publishes_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();

    assert_eq!(
        decide(&probe, &fired(NOW), &filtered(), every(15), aud()),
        None
    );
    assert!(rows.borrow().is_empty());
}

/// GBP's mid rose before the previous grid point and fell since.
#[rstest]
fn a_decision_whose_filter_marks_no_event_since_the_previous_grid_point_publishes_nothing() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = filtered();
    gbp(
        &features,
        &[
            (1.3, PREVIOUS - 2 * SECOND),
            (1.4, PREVIOUS - SECOND),
            (1.2, PREVIOUS + 60 * SECOND),
        ],
    );

    assert_eq!(
        decide(&probe, &fired(NOW), &features, every(15), aud()),
        None
    );
    assert!(rows.borrow().is_empty());
}

/// GBP's mid rose 2 minutes after the previous grid point, 13 minutes before the decision.
#[rstest]
fn an_event_between_grid_points_makes_the_next_decision() {
    let (probe, _clock) = registered(NOW);
    let rows = published();
    let features = filtered();
    gbp(
        &features,
        &[
            (1.3, PREVIOUS + 60 * SECOND),
            (1.4, PREVIOUS + 120 * SECOND),
        ],
    );

    let row = decide(&probe, &fired(NOW), &features, every(15), aud()).unwrap();

    assert_eq!(row.ts_init, UnixNanos::from(NOW));
    assert!((row.features[0] - 0.75).abs() < 1e-9, "{row:?}");
    assert_eq!(*rows.borrow(), [row]);
}

/// An event at the previous grid point is the previous decision's, as data at it is.
#[rstest]
#[case::on_the_previous_grid_point(PREVIOUS, false)]
#[case::just_after_it(PREVIOUS + 1, true)]
fn an_event_is_a_decisions_from_just_after_the_previous_grid_point(
    #[case] risen_at: u64,
    #[case] decided: bool,
) {
    let (probe, _clock) = registered(NOW);
    let features = filtered();
    gbp(&features, &[(1.3, PREVIOUS - SECOND), (1.4, risen_at)]);

    assert_eq!(
        decide(&probe, &fired(NOW), &features, every(15), aud()).is_some(),
        decided
    );
}

/// AUD, the traded instrument, had no data over the interval to the previous grid point, so the
/// decision there was stale. An event in that interval is no event of the next decision, which
/// judges it from the previous grid point, not from the last decision made.
#[rstest]
#[case::in_the_stale_interval(PREVIOUS - 5 * 60 * SECOND, false)]
#[case::after_it(PREVIOUS + 5 * 60 * SECOND, true)]
fn an_event_after_a_stale_interval_makes_a_decision_and_one_in_it_does_not(
    #[case] risen_at: u64,
    #[case] decided: bool,
) {
    let (probe, _clock) = registered(NOW);
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(aud())], MID_2, &tables())
        .event_filter(&[Series::quotes(InstrumentId::from(GBP))], RISE, &tables())
        .build()
        .unwrap();
    features
        .handle_quote(&quote_of(AUD, 0.7, PREVIOUS - 20 * 60 * SECOND))
        .unwrap();
    features.handle_quote(&quote(0.8)).unwrap();
    gbp(&features, &[(1.3, risen_at - SECOND), (1.4, risen_at)]);

    assert_eq!(
        decide(&probe, &fired(NOW), &features, every(15), aud()).is_some(),
        decided
    );
}

/// The grid point after `NOW` on `every(15)`.
const NEXT: u64 = NOW + 15 * 60 * SECOND;

/// A live clock reads past a decision's scheduled time by the delay before its callback runs, a
/// different one at each decision. GBP's mid rises 200 ms after the scheduled time `NOW`: a delay
/// of 300 ms there has the rise read before that decision, one of 100 ms after it. Each decision
/// asks the filter from the scheduled time before it to its own, so the rise is the decision at
/// `NEXT`'s alone, counted once, whatever the delays.
#[rstest]
#[case::read_before_a_late_decision(300, 100)]
#[case::read_after_an_early_decision(100, 300)]
fn an_event_just_after_a_scheduled_time_is_the_next_decisions_alone_whatever_the_clocks_delays(
    #[case] first_delay_ms: u64,
    #[case] second_delay_ms: u64,
) {
    let (probe, clock) = registered(NOW);
    let features = filtered();
    gbp(&features, &[(1.3, PREVIOUS - SECOND)]);
    let risen_at = NOW + 200 * NANOSECONDS_IN_MILLISECOND;
    let first_read = NOW + first_delay_ms * NANOSECONDS_IN_MILLISECOND;

    if risen_at <= first_read {
        gbp(&features, &[(1.4, risen_at)]);
    }
    clock
        .borrow_mut()
        .advance_time(UnixNanos::from(first_read), true);
    let first = decide(&probe, &fired(NOW), &features, every(15), aud());
    if risen_at > first_read {
        gbp(&features, &[(1.4, risen_at)]);
    }
    features
        .handle_quote(&quote_of(AUD, 0.8, NEXT - SECOND))
        .unwrap();
    clock.borrow_mut().advance_time(
        UnixNanos::from(NEXT + second_delay_ms * NANOSECONDS_IN_MILLISECOND),
        true,
    );
    let second = decide(&probe, &fired(NEXT), &features, every(15), aud());

    assert_eq!((first.is_some(), second.is_some()), (false, true));
}

/// A decision is at its scheduled time, which picks its model and keys its dataset row, and the
/// row was read at the clock's time, 300 ms later.
#[rstest]
fn a_decision_read_past_its_scheduled_time_is_at_that_time_and_read_at_the_clocks() {
    let (probe, clock) = registered(NOW - 2 * SECOND);
    let features = features();
    features.handle_quote(&quote(0.7)).unwrap();
    features.handle_quote(&quote(0.8)).unwrap();
    clock.borrow_mut().advance_time(
        UnixNanos::from(NOW + 300 * NANOSECONDS_IN_MILLISECOND),
        true,
    );

    let row = decide(&probe, &fired(NOW), &features, every(15), aud()).unwrap();

    assert_eq!(row.ts_event, UnixNanos::from(NOW));
    assert_eq!(
        row.ts_init,
        UnixNanos::from(NOW + 300 * NANOSECONDS_IN_MILLISECOND)
    );
}
