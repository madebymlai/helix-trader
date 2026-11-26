//! Sets when a strategy decides and what it publishes at each decision.
//!
//! A strategy decides on a clock cadence (`DecisionRule`). The cadence is a Nautilus timer,
//! `DECISION_TIMER`. `start` sets it on an epoch-aligned time, so decisions sit on the UTC grid of
//! the interval's multiples, whatever the run start, in a backtest as live. The strategy calls
//! `decide` from `on_time_event` when `is_decision`, with the timer's event. A decision is at the
//! event's scheduled time, which a backtest's clock reads in the callback and a live one passes by
//! however long the callback waited.
//!
//! A node builds the bars of a composite bar type a delay after their close
//! (`warmup::bar_build_delay`). The cadence starts with that delay. Each decision fires
//! `DECISION_MARGIN` later still, past the timer that builds the bar.
//!
//! `decide` publishes the features on the message bus as a `FeatureRow` keyed by the strategy's id
//! (`FeatureRow::data_type`). Cold features give no decision.
//!
//! A stale decision gives none either, because it would trade a closed market on its last quote.
//! A decision is fresh if the features read market data of the traded instrument with a
//! `ts_init` after the previous decision on the grid, an interval before its scheduled time. This
//! holds whether the strategy ran then or not. A stale decision gives no row, targets or trade, and
//! is logged at debug.
//!
//! The market data is the quotes, trades, bars, depth and deltas of the traded instrument that a
//! feature or the event filter reads (`FeatureSet::last_update`), plus its last quote and trade in
//! the cache. There is no calendar of sessions. The quotes added by `StandardConfig::data_requirements` count when no
//! series reads the traded instrument. Other instruments and custom data do not count. The node
//! builds no time bar for a period without data (`warmup::data_engine_config`), so a bar is data.
//!
//! A fresh decision that is no event gives none either (`FeatureSet::event`): a cold event filter,
//! or one that marks no event, gives no row, targets or trade, and is logged at debug. The filter
//! is asked with the previous grid point, the boundary freshness reads, and the decision's
//! scheduled time, so consecutive decisions ask it over windows that tile the grid. An edge event
//! between two grid points makes the next decision, at most an interval later, and one at the
//! previous grid point is the previous decision's. Then the features' values are read.
//!
//! The filter is asked at a stale decision too, before its freshness is judged, so an edge filter
//! drops the events of each window passed while the traded instrument has no data
//! (`EventTimes::in_window`). `ModelHandle::decide` returns before `decide` while its warmup is
//! answering or its target's ATR is cold, so the filter keeps those windows' events until the
//! first decision after.

use std::sync::Arc;

use anyhow::Context;
use jiff::SignedDuration;
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    timer::TimeEvent,
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::{
    custom_data,
    data::{CustomData, CustomDataTrait, DataType},
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use serde::Deserialize;

use super::features::{FeatureSet, Fingerprint};

/// The name of the clock cadence timer.
pub const DECISION_TIMER: &str = "DECISION";

/// How long after its grid time plus the bar build delay a decision fires, past the timer that
/// builds the bar.
///
/// Nautilus runs a backtest's timers for one time in the order of their names. A live node runs
/// them as their tasks wake. 10 ms is well past a live node's 1 ms timer resolution.
pub const DECISION_MARGIN: SignedDuration = SignedDuration::from_millis(10);

/// When a strategy decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecisionRule {
    /// On a clock cadence, at every multiple of the interval since the Unix epoch.
    ///
    /// For example, `{ every = "15m" }`.
    Every(SignedDuration),
}

/// The features of one decision, published on the message bus.
#[custom_data]
pub struct FeatureRow {
    /// The strategy that decided.
    pub strategy_id: StrategyId,
    /// The value of each feature, in the order of the names in the feature set.
    pub features: Vec<f64>,
    /// The time of the decision, its timer's scheduled time.
    pub ts_event: UnixNanos,
    /// The time the features were read, the clock's in the decision's callback. A backtest's clock
    /// reads the decision's time there, and a live one later.
    pub ts_init: UnixNanos,
}

impl DecisionRule {
    /// # Errors
    ///
    /// Returns an error if the cadence interval is not positive.
    pub fn validate(&self) -> anyhow::Result<()> {
        let Self::Every(interval) = self;
        anyhow::ensure!(
            interval.is_positive(),
            "the decision interval {interval:#} is not positive"
        );
        Ok(())
    }

    /// Keys the delay only if there is one, so a rule on its grid reads as before.
    fn fingerprint_part(self, delay: SignedDuration) -> String {
        let Self::Every(interval) = self;
        if delay.is_zero() {
            format!("every {} ns", interval.as_nanos())
        } else {
            format!(
                "every {} ns after {} ns",
                interval.as_nanos(),
                delay.as_nanos()
            )
        }
    }
}

/// Starts the clock cadence of `rule` on `actor`.
///
/// Decisions fire `delay`, the bar build delay, plus `DECISION_MARGIN` after the grid times. The
/// timer starts on the last decision time not after now. The rule must be valid
/// (`DecisionRule::validate`).
///
/// # Errors
///
/// Returns an error if the clock is not yet `delay` plus `DECISION_MARGIN` past the epoch, or
/// Nautilus refuses the timer.
pub fn start<A: DataActor + DataActorNative>(
    actor: &A,
    rule: DecisionRule,
    delay: SignedDuration,
) -> anyhow::Result<()> {
    let DecisionRule::Every(interval) = rule;
    let clock = actor.clock();
    let now = clock.timestamp_ns();
    let offset = firing(delay);
    let (interval_ns, offset_ns) = (
        DurationNanos::try_from(interval)?,
        DurationNanos::try_from(offset)?,
    );
    let start = now
        .checked_sub(offset_ns)
        .with_context(|| format!("the clock, at {now} ns, is not yet {offset:#} past the epoch"))?
        .floor(interval_ns)
        + offset_ns;
    clock.set_timer_ns(
        DECISION_TIMER,
        interval_ns,
        Some(start),
        None,
        None,
        Some(true),
        Some(false),
    )
}

fn firing(delay: SignedDuration) -> SignedDuration {
    delay + DECISION_MARGIN
}

/// Returns the grid time of the first decision on `rule` that fires at or after `ts`.
///
/// That decision reads what is made at `ts`, because the data engine hands it over before the
/// timer.
///
/// # Panics
///
/// Panics if:
/// - The rule is invalid (`DecisionRule::validate`).
/// - The interval is past Nautilus' nanoseconds.
/// - The decisions fire before their grid times (`delay` negative past `DECISION_MARGIN`).
/// - That decision would be before the epoch: `ts` is an interval or more before the epoch's fires.
#[must_use]
pub fn decision_of(rule: DecisionRule, delay: SignedDuration, ts: UnixNanos) -> UnixNanos {
    let DecisionRule::Every(interval) = rule;
    assert!(interval.is_positive(), "a cadence of {interval:#}");
    let interval = DurationNanos::try_from(interval).expect("a cadence in nanoseconds");
    let firing = DurationNanos::try_from(firing(delay)).expect("a firing in nanoseconds");
    match ts.checked_sub(firing) {
        Some(since) => {
            let grid = since.floor(interval);
            if grid == since { grid } else { grid + interval }
        }
        None => {
            assert!(
                firing.as_u64() - ts.as_u64() < interval.as_u64(),
                "a decision after the epoch"
            );
            UnixNanos::default()
        }
    }
}

/// Returns whether `event` is the timer of the clock cadence.
#[must_use]
pub fn is_decision(event: &TimeEvent) -> bool {
    event.name == DECISION_TIMER
}

/// Decides at `event`, the cadence's timer firing, on `rule`, trading `instrument_id`.
///
/// The decision is at the event's scheduled time, its `ts_event`, and its previous grid point an
/// interval before. A backtest's clock reads that time in the callback. A live clock reads later,
/// by however long the callback waited, so its time would leave gaps and overlaps between
/// consecutive decisions. The scheduled times tile the grid exactly.
///
/// Publishes a row of `features` if, judged in this order, the decision is fresh, it is an event
/// (`FeatureSet::event`) and every feature is initialized. The event filter is asked first, at
/// every decision, stale or not, so that it drops the events of the windows passed.
///
/// # Panics
///
/// Panics if:
/// - `event` is not the cadence's timer (`is_decision`).
/// - `strategy` is not registered with a trader, or has no id.
/// - The decision is less than an interval after the epoch, so there is no previous decision.
pub fn decide<S>(
    strategy: &S,
    event: &TimeEvent,
    features: &FeatureSet,
    rule: DecisionRule,
    instrument_id: InstrumentId,
) -> Option<FeatureRow>
where
    S: Strategy + StrategyNative + DataActor + DataActorNative,
{
    assert!(is_decision(event), "{} is no decision", event.name);
    let at = event.ts_event;
    let DecisionRule::Every(interval) = rule;
    let previous = DurationNanos::try_from(interval)
        .ok()
        .and_then(|interval| at.checked_sub(interval))
        .expect("a decision an interval after the epoch");
    let event = features.event(previous, at);
    let cache = strategy.cache();
    let quoted = cache.quote(&instrument_id).map(|quote| quote.ts_init);
    let traded = cache.trade(&instrument_id).map(|trade| trade.ts_init);
    if features
        .last_update(instrument_id)
        .max(quoted)
        .max(traded)
        .is_none_or(|last| last <= previous)
    {
        log::debug!(
            "decision at {at} skipped: no data of {instrument_id} since the previous, at {previous}"
        );
        return None;
    }

    if let Err(no_event) = event {
        log::debug!("decision at {at} skipped: {no_event}, the previous grid point at {previous}");
        return None;
    }
    let values = features.values()?;
    let strategy_id = strategy
        .strategy_id()
        .expect("a strategy deciding has an id");
    let row = FeatureRow {
        strategy_id,
        features: values,
        ts_event: at,
        ts_init: strategy.clock().timestamp_ns(),
    };
    let data_type = FeatureRow::data_type(strategy_id);
    let data = CustomData::new(Arc::new(row.clone()), data_type.clone());
    strategy.publish_data(&data_type, &data);
    Some(row)
}

/// Returns the fingerprint of `features` and `rule`, keyed by the bar build delay `delay`.
#[must_use]
pub fn fingerprint(
    features: &FeatureSet,
    rule: DecisionRule,
    delay: SignedDuration,
) -> Fingerprint {
    features
        .fingerprint()
        .with(rule.fingerprint_part(delay).as_bytes())
}

impl FeatureRow {
    /// Returns the data type of the feature rows of `strategy_id`.
    #[must_use]
    pub fn data_type(strategy_id: StrategyId) -> DataType {
        DataType::new(
            Self::type_name_static(),
            None,
            Some(strategy_id.to_string()),
        )
    }
}

#[cfg(test)]
mod tests;
