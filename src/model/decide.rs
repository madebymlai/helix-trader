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
mod tests;
