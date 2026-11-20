//! The latency of the simulated venue.
//!
//! The venue processes a command at the first settlement point after the command arrives. A
//! settlement point is market data for the command's instrument, or a timer (see
//! `concepts/backtesting/execution-flow.md`, "Latency-delayed commands"). A market order therefore
//! fills on the first quote at or after its arrival, not on the quote it was sent on.

use std::{fmt::Display, str::FromStr};

use jiff::SignedDuration;
use nautilus_core::DurationNanos;
use nautilus_execution::models::latency::{LatencyModelAny, StaticLatencyModel};
use serde::Deserialize;

use super::node::DATASET_PURGE_MINS;

/// The time an order command takes to reach the simulated venue after it is sent.
///
/// The latency must be positive. It is written as jiff writes durations, such as `"150ms"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "SignedDuration")]
pub struct Latency(DurationNanos);

impl TryFrom<SignedDuration> for Latency {
    type Error = anyhow::Error;

    fn try_from(latency: SignedDuration) -> anyhow::Result<Self> {
        anyhow::ensure!(
            latency.is_positive(),
            "a latency must be positive, not {latency:#}: leave it out for none"
        );
        Ok(Self(DurationNanos::try_from(latency)?))
    }
}

impl FromStr for Latency {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> anyhow::Result<Self> {
        text.parse::<SignedDuration>()?.try_into()
    }
}

/// Formats the duration in jiff's friendly format, such as `1s 500ms`.
///
/// Every spelling of a latency gives the same text.
impl Display for Latency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", SignedDuration::from(self.0))
    }
}

impl Latency {
    /// Returns the Nautilus `StaticLatencyModel` that delays every command by the latency.
    #[must_use]
    pub fn model(self) -> LatencyModelAny {
        let none = DurationNanos::ZERO;
        LatencyModelAny::Static(StaticLatencyModel::new(self.0, none, none, none))
    }

    /// Returns the longest a command waits in a dataset run, from its send to its processing.
    ///
    /// A command first waits out the latency. It then waits up to a minute, until the purge timer
    /// (`node::DATASET_PURGE_MINS`) releases it.
    #[must_use]
    pub(crate) fn dataset_delay(self) -> SignedDuration {
        SignedDuration::from(self.0) + SignedDuration::from_mins(i64::from(DATASET_PURGE_MINS))
    }
}
