//! The latency of the simulated venue.
//!
//! A run's latency (`Latency`), if it has one, builds the Nautilus
//! `StaticLatencyModel` (`nautilus_execution::models::latency`) that
//! `BacktestVenueConfig` takes: each order command reaches the venue that
//! long after it is sent, a submit, a modify and a cancel alike, and is
//! processed at the first settlement point from then on: market data of its
//! instrument, after the venue has taken it in, or a timer
//! (`concepts/backtesting/execution-flow.md`, "Latency-delayed commands").
//! A market order then fills on the first quote of its instrument at or
//! after its arrival, not on the one it was sent on. Without one, a command
//! reaches the venue as it is sent, Nautilus' default.

use std::{fmt, str::FromStr};

use anyhow::ensure;
use jiff::SignedDuration;
use nautilus_core::DurationNanos;
use nautilus_execution::models::latency::{LatencyModelAny, StaticLatencyModel};
use serde::Deserialize;

use super::node::DATASET_PURGE_MINS;

/// How long after it is sent an order command reaches the simulated
/// venue: positive. Written as other durations are, by jiff, such as
/// `"150ms"`, on the command line (`--latency 150ms`) as in a model's config
/// (`latency = "150ms"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "SignedDuration")]
pub struct Latency(DurationNanos);

impl TryFrom<SignedDuration> for Latency {
    type Error = anyhow::Error;

    fn try_from(latency: SignedDuration) -> anyhow::Result<Self> {
        ensure!(
            latency.is_positive(),
            "a latency must be positive, not {latency:#}: leave it out for none"
        );
        Ok(Self(DurationNanos::try_from(latency)?))
    }
}

/// A duration as jiff reads it, such as `"150ms"` or `"PT0.15S"`.
impl FromStr for Latency {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> anyhow::Result<Self> {
        text.parse::<SignedDuration>()?.try_into()
    }
}

/// The duration in jiff's friendly format, its largest units first, such
/// as `150ms` or `1s 500ms`: one text for every spelling of one latency.
impl fmt::Display for Latency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#}", SignedDuration::from(self.0))
    }
}

impl Latency {
    /// The Nautilus latency model delaying every command by the latency:
    /// a `StaticLatencyModel` of that base latency and no extra for a
    /// submit, a modify or a cancel, as Nautilus' guides configure it
    /// (`concepts/backtesting/execution-flow.md`, `bar-execution.md`).
    #[must_use]
    pub fn model(self) -> LatencyModelAny {
        let none = DurationNanos::ZERO;
        LatencyModelAny::Static(StaticLatencyModel::new(self.0, none, none, none))
    }

    /// The longest a command of a dataset run at the latency waits from
    /// its send to its processing: the latency, then until the run's next
    /// settlement point, which is a minute at most, its execution engine
    /// purging the cache every minute by a timer
    /// (`node::DATASET_PURGE_MINS`), which releases every command due
    /// (`concepts/backtesting/execution-flow.md`, "Latency-delayed
    /// commands"). Market data of the command's instrument releases it
    /// sooner.
    #[must_use]
    pub(crate) fn dataset_delay(self) -> SignedDuration {
        SignedDuration::from(self.0) + SignedDuration::from_mins(i64::from(DATASET_PURGE_MINS))
    }
}
