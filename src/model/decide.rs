//! When a strategy decides, and what it publishes at each decision.
//!
//! A strategy decides on a clock cadence or on an event it chooses
//! (`DecisionRule`, from its config). A cadence is a Nautilus timer,
//! `DECISION_TIMER`, which `start` sets from `on_start` on an
//! epoch-aligned time: Nautilus fires it at that time plus whole intervals,
//! so decisions sit on the fixed UTC grid of the interval's multiples
//! whatever the run start, in a backtest as live. The strategy calls
//! `decide` from `on_time_event` on that timer (`is_decision`), or, on an
//! event rule, from the handler of its event.
//!
//! A live node builds the bars of a composite bar type a delay after their
//! close (`warmup::live_bar_build_delay`), a backtest at the close. So a
//! live strategy starts its cadence with that delay, a backtest with none,
//! and every decision fires `DECISION_MARGIN` later still, past the timer
//! building the bar: each decision then sees the bar closed at its grid
//! time in both.
//!
//! `decide` snapshots the features, if every one is initialized, and
//! publishes them on the message bus as a `FeatureRow`, custom data keyed
//! by the strategy's id (`FeatureRow::data_type`). Cold features give no
//! decision and publish nothing.
//!
//! Nor does a stale decision of a cadence. A decision is fresh if the
//! features read market data of the traded instrument made (`ts_init`)
//! after the previous decision of the grid, an interval before, whether
//! the strategy ran then or not: so the first decision of a run is judged
//! as any other, and a warmup's data makes it fresh only if made within
//! that interval. A closed market, a weekend's or a night's, would
//! otherwise decide on its last quote, its targets the spread and the
//! fees: a stale decision gives no row, no targets and no trade, in a
//! dataset run as in a backtest or live, and is logged at debug.
//!
//! The market data is the quotes, trades, bars, depth and deltas of the
//! traded instrument a feature reads (`FeatureSet::last_update`), whatever
//! their kind or venue, with no calendar of sessions. Not the data of
//! another instrument, which may trade while the traded one is closed, nor
//! custom data, which may come on a weekend. The node builds no time bar
//! for a period without data (`warmup::data_engine_config`), so a bar is
//! data. A decision on an event is fresh: the event is the data.

use std::{any::Any, sync::Arc};

use anyhow::ensure;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    timer::TimeEvent,
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::{
    data::{CustomData, CustomDataTrait, DataType, HasTsInit},
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use serde::{Deserialize, Serialize};

use super::features::{FeatureSet, Fingerprint};

/// The name of the timer of a clock cadence.
pub const DECISION_TIMER: &str = "DECISION";

/// How long after its grid time (and the live node's bar build delay) a
/// decision fires: past the data engine's timer building the bar that
/// closes then, which fires on the same time. Nautilus runs a backtest's
/// timers of one time in the order of their names, a live node's as their
/// tasks wake, so a decision on the grid time itself could come before the
/// bar. 10 ms is well past the 1 ms resolution of a live node's timers;
/// the same margin in a backtest keeps both deciding on the same data.
pub const DECISION_MARGIN: SignedDuration = SignedDuration::from_millis(10);

/// When a strategy decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecisionRule {
    /// On a clock cadence: at every multiple of the interval since the Unix
    /// epoch, such as `{ every = "15m" }`.
    Every(SignedDuration),
    /// On an event the strategy chooses, from any of its handlers:
    /// `"event"`.
    Event,
}

/// The features of one decision, published on the message bus.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeatureRow {
    /// The strategy that decided.
    pub strategy_id: StrategyId,
    /// The value of each feature, in the order of the feature set's names.
    pub features: Vec<f64>,
    /// The time of the decision.
    pub ts_event: UnixNanos,
    /// The time the row was made: the decision's too.
    pub ts_init: UnixNanos,
}

impl DecisionRule {
    /// # Errors
    ///
    /// Returns an error if a cadence's interval is not positive.
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Self::Every(interval) = self {
            ensure!(
                interval.is_positive(),
                "the decision interval {interval:#} is not positive"
            );
        }
        Ok(())
    }

    /// The rule as the fingerprint reads it.
    fn fingerprint_part(self) -> String {
        match self {
            Self::Every(interval) => format!("every {} ns", interval.as_nanos()),
            Self::Event => "event".to_string(),
        }
    }
}

/// Starts the clock cadence of `rule` on `actor`, its decisions `delay`
/// and `DECISION_MARGIN` after their grid times: `delay` is the live
/// node's bar build delay live, none in a backtest. The timer starts on
/// the last decision time not after now, so it first fires on the next
/// one; an event rule sets no timer.
///
/// # Errors
///
/// Returns an error if the rule is invalid or Nautilus refuses the timer.
pub fn start<A: DataActor + DataActorNative>(
    actor: &A,
    rule: DecisionRule,
    delay: SignedDuration,
) -> anyhow::Result<()> {
    rule.validate()?;
    let DecisionRule::Every(interval) = rule else {
        return Ok(());
    };
    let clock = actor.clock();
    let now = i128::from(clock.timestamp_ns().as_u64());
    let (interval_ns, offset_ns) = (interval.as_nanos(), (delay + DECISION_MARGIN).as_nanos());
    let start = (now - offset_ns).div_euclid(interval_ns) * interval_ns + offset_ns;
    clock.set_timer_ns(
        DECISION_TIMER,
        DurationNanos::try_from(interval)?,
        Some(UnixNanos::from(u64::try_from(start)?)),
        None,
        None,
        Some(true),
        Some(false),
    )
}

/// Whether `event` is a decision of the clock cadence.
#[must_use]
pub fn is_decision(event: &TimeEvent) -> bool {
    event.name == DECISION_TIMER
}

/// Decides now, on `rule`, trading `instrument_id`: snapshots `features`
/// if the decision is fresh and every feature is initialized, and
/// publishes the row on the message bus, keyed by `strategy`'s id. A
/// decision of a cadence is fresh if a feature read data of
/// `instrument_id` after the cadence's previous decision (see the module
/// documentation); a stale one is skipped, logged at debug. A decision on
/// an event is fresh.
///
/// # Panics
///
/// Panics if `strategy` is not registered with a trader, or has no id.
pub fn decide<S>(
    strategy: &S,
    features: &FeatureSet,
    rule: DecisionRule,
    instrument_id: InstrumentId,
) -> Option<FeatureRow>
where
    S: Strategy + StrategyNative + DataActor + DataActorNative,
{
    let now = strategy.clock().timestamp_ns();
    if let DecisionRule::Every(interval) = rule {
        let previous = UnixNanos::from(Timestamp::from(now) - interval);
        if features
            .last_update(instrument_id)
            .is_none_or(|last| last <= previous)
        {
            log::debug!(
                "decision at {now} skipped: no data of {instrument_id} since the previous, at {previous}"
            );
            return None;
        }
    }
    let values = features.values()?;
    let strategy_id = strategy
        .strategy_id()
        .expect("a strategy deciding has an id");
    let row = FeatureRow {
        strategy_id,
        features: values,
        ts_event: now,
        ts_init: now,
    };
    let data_type = FeatureRow::data_type(strategy_id);
    let data = CustomData::new(Arc::new(row.clone()), data_type.clone());
    strategy.publish_data(&data_type, &data);
    Some(row)
}

/// The fingerprint of `features` and `rule`, with this module's code,
/// which places the decisions.
#[must_use]
pub fn fingerprint(features: &FeatureSet, rule: DecisionRule) -> Fingerprint {
    features
        .fingerprint()
        .with(rule.fingerprint_part().as_bytes())
        .with(include_str!("decide.rs").as_bytes())
}

impl FeatureRow {
    /// The data type of the rows of `strategy_id`.
    #[must_use]
    pub fn data_type(strategy_id: StrategyId) -> DataType {
        DataType::new(
            Self::type_name_static(),
            None,
            Some(strategy_id.to_string()),
        )
    }
}

impl HasTsInit for FeatureRow {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for FeatureRow {
    fn type_name(&self) -> &'static str {
        Self::type_name_static()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn type_name_static() -> &'static str {
        "FeatureRow"
    }
}

#[cfg(test)]
mod tests {
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
}
