//! When a strategy decides, and what it publishes at each decision.
//!
//! A strategy decides on a clock cadence (`DecisionRule`): a Nautilus timer, `DECISION_TIMER`,
//! which `start` sets on an epoch-aligned time, so decisions sit on the UTC grid of the interval's
//! multiples whatever the run start, in a backtest as live. The strategy calls `decide` from
//! `on_time_event` when `is_decision`.
//!
//! A node builds the bars of a composite bar type a delay after their close
//! (`warmup::bar_build_delay`). The cadence starts with that delay and each decision fires
//! `DECISION_MARGIN` later still, past the timer building the bar.
//!
//! `decide` publishes the features on the message bus as a `FeatureRow` keyed by the strategy's id
//! (`FeatureRow::data_type`). Cold features give no decision.
//!
//! Nor does a stale decision, which would trade a closed market on its last quote. A decision is
//! fresh if the features read market data of the traded instrument made (`ts_init`) after the
//! previous decision of the grid, an interval before, whether the strategy ran then or not. A stale
//! decision gives no row, targets or trade, and is logged at debug.
//!
//! The market data is the quotes, trades, bars, depth and deltas of the traded instrument a feature
//! reads (`FeatureSet::last_update`), and its last quote and trade in the cache, with no calendar
//! of sessions. The quotes added by `StandardConfig::data_requirements` count when no series reads
//! the traded instrument. Other instruments and custom data do not. The node builds no time bar
//! for a period without data (`warmup::data_engine_config`), so a bar is data.

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

/// The name of the timer of a clock cadence.
pub const DECISION_TIMER: &str = "DECISION";

/// How long after its grid time and the bar build delay a decision fires, past the timer building
/// the bar. Nautilus runs a backtest's timers of one time in the order of their names, a live
/// node's as their tasks wake. 10 ms is well past a live node's 1 ms timer resolution.
pub const DECISION_MARGIN: SignedDuration = SignedDuration::from_millis(10);

/// When a strategy decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecisionRule {
    /// On a clock cadence: at every multiple of the interval since the Unix epoch, such as `{ every
    /// = "15m" }`.
    Every(SignedDuration),
}

/// The features of one decision, published on the message bus.
#[custom_data]
pub struct FeatureRow {
    /// The strategy that decided.
    pub strategy_id: StrategyId,
    /// The value of each feature, in the order of the feature set's names.
    pub features: Vec<f64>,
    /// The time of the decision.
    pub ts_event: UnixNanos,
    /// The time of the row, the decision's too.
    pub ts_init: UnixNanos,
}

impl DecisionRule {
    /// # Errors
    ///
    /// Returns an error if a cadence's interval is not positive.
    pub fn validate(&self) -> anyhow::Result<()> {
        let Self::Every(interval) = self;
        anyhow::ensure!(
            interval.is_positive(),
            "the decision interval {interval:#} is not positive"
        );
        Ok(())
    }

    /// The delay is keyed only if any, so a rule on its grid reads as before.
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

/// Starts the clock cadence of `rule` on `actor`, deciding `delay`, the bar build delay, and
/// `DECISION_MARGIN` after the grid times. The timer starts on the last decision time not after
/// now. The rule is valid (`DecisionRule::validate`).
///
/// # Errors
///
/// Returns an error if the clock is not yet `delay` and `DECISION_MARGIN` past the epoch, or
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

/// The grid time of the first decision on `rule` to fire at or after `ts`, which reads what is
/// made at `ts`: the data engine hands it over before the timer.
///
/// # Panics
///
/// Panics if:
/// - The rule is invalid (`DecisionRule::validate`), its interval is past Nautilus' nanoseconds, or
///   its decisions fire before their grid times (`delay` negative past `DECISION_MARGIN`).
/// - That decision would be before the epoch: `ts` an interval or more before the epoch's fires.
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

/// Whether `event` is the clock cadence's timer.
#[must_use]
pub fn is_decision(event: &TimeEvent) -> bool {
    event.name == DECISION_TIMER
}

/// Decides now, on `rule`, trading `instrument_id`: publishes a row of `features` if the decision
/// is fresh and every feature is initialized.
///
/// # Panics
///
/// Panics if:
/// - `strategy` is not registered with a trader, or has no id.
/// - Now is less than an interval after the epoch: there is no previous decision.
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
    let DecisionRule::Every(interval) = rule;
    let previous = DurationNanos::try_from(interval)
        .ok()
        .and_then(|interval| now.checked_sub(interval))
        .expect("a decision an interval after the epoch");
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
            "decision at {now} skipped: no data of {instrument_id} since the previous, at {previous}"
        );
        return None;
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

/// The fingerprint of `features` and `rule`, keyed by the bar build delay `delay`.
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

#[cfg(test)]
mod tests;
