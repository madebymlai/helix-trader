//! The latency of the simulated venue.
//!
//! A command is processed at the first settlement point after it arrives: market data of its
//! instrument, or a timer (`concepts/backtesting/execution-flow.md`, "Latency-delayed commands"). A
//! market order then fills on the first quote at or after its arrival, not the one it was sent on.

use std::{fmt::Display, str::FromStr};

use jiff::SignedDuration;
use nautilus_core::DurationNanos;
use nautilus_execution::models::latency::{LatencyModelAny, StaticLatencyModel};
use serde::Deserialize;

use super::node::DATASET_PURGE_MINS;

/// How long after it is sent an order command reaches the simulated venue: positive. Written as
/// jiff writes durations, such as `"150ms"`.
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

/// The duration in jiff's friendly format, such as `1s 500ms`, one text for every spelling.
impl Display for Latency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", SignedDuration::from(self.0))
    }
}

impl Latency {
    /// The Nautilus `StaticLatencyModel` delaying every command by the latency.
    #[must_use]
    pub fn model(self) -> LatencyModelAny {
        let none = DurationNanos::ZERO;
        LatencyModelAny::Static(StaticLatencyModel::new(self.0, none, none, none))
    }

    /// The longest a dataset run's command waits from its send to its processing: the latency,
    /// then up to a minute, until the purge timer (`node::DATASET_PURGE_MINS`) releases it.
    #[must_use]
    pub(crate) fn dataset_delay(self) -> SignedDuration {
        SignedDuration::from(self.0) + SignedDuration::from_mins(i64::from(DATASET_PURGE_MINS))
    }
}
