//! The HTTP client's failures, and which of them a retry may lift.

use std::time::Duration;

use nautilus_network::{http::HttpClientError, retry::RetryError};

use super::models::LseRefusal;

/// How a call to LSE failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LseHttpError {
    /// LSE was not reached, or did not answer in time.
    #[error("{0}")]
    Transport(#[from] HttpClientError),
    /// LSE refused the call, with its HTTP status, and the `Retry-After`
    /// it gave, if any.
    #[error("LSE refused it with {status} {refusal}")]
    Refusal {
        status: u16,
        refusal: Box<LseRefusal>,
        retry_after: Option<Duration>,
    },
    /// An answer LSE's docs do not describe: its status and the start of
    /// its body.
    #[error("LSE answered {status}, not as its docs say: {body}")]
    Unexpected { status: u16, body: String },
    /// An answer whose body is not what LSE's docs say it holds: what it
    /// should have been and why it is not.
    #[error("LSE's {what} is not as its docs say: {source}")]
    Malformed {
        what: &'static str,
        source: serde_json::Error,
    },
    /// A file whose SHA-256 is not the one LSE named: cut short.
    #[error("the file LSE sent, {len} bytes, is not the one its SHA-256 names")]
    Checksum { len: usize },
    /// The retries ended without an answer.
    #[error("{0}")]
    Retry(RetryError),
}

impl LseHttpError {
    /// Whether asking again may get the answer: a transport failure or a
    /// timeout, a cut file, and LSE's refusals for the moment, a 429 of
    /// calls or of the one download at a time and a busy 503, but not a
    /// 429 of a quota, which lasts until it resets.
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            Self::Transport(e) => matches!(
                e,
                HttpClientError::TransportError(_) | HttpClientError::TimeoutError(_)
            ),
            Self::Checksum { .. } => true,
            Self::Refusal {
                status, refusal, ..
            } => (*status == 429 && !refusal.is_quota()) || *status == 503,
            Self::Unexpected { .. } | Self::Malformed { .. } | Self::Retry(_) => false,
        }
    }

    /// How long LSE asked to wait before asking again (`Retry-After`).
    pub(crate) fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Refusal { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}
