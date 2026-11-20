//! The HTTP client's failures, and which a retry may lift.

use std::time::Duration;

use nautilus_network::{http::HttpClientError, retry::RetryError};

use super::models::LseRefusal;

/// How a call to LSE failed.
#[derive(Debug, thiserror::Error)]
pub enum LseHttpError {
    /// LSE was not reached, or did not answer in time.
    #[error("{0}")]
    Transport(#[from] HttpClientError),
    /// LSE refused the call, with its HTTP status, and the `Retry-After` it gave, if any.
    #[error("LSE refused it with {status} {refusal}")]
    Refusal {
        status: u16,
        refusal: Box<LseRefusal>,
        retry_after: Option<Duration>,
    },
    /// An answer LSE's docs do not describe.
    #[error("LSE answered {status}, not as its docs say: {body}")]
    Unexpected { status: u16, body: String },
    /// An answer whose body is not what LSE's docs say it holds.
    #[error("LSE's {what} is not as its docs say: {source}")]
    Malformed {
        what: &'static str,
        source: serde_json::Error,
    },
    /// A whole file whose SHA-256 is not the one LSE gave, with the `x-export-id`, if any.
    #[error(
        "the file of LSE's export {}, {len} bytes, does not have the SHA-256 LSE gave for it",
        export_id.as_deref().unwrap_or("with no id")
    )]
    Checksum {
        len: usize,
        export_id: Option<String>,
    },
    /// The retries ended without an answer.
    #[error("{0}")]
    Retry(RetryError),
}

impl LseHttpError {
    /// Returns whether asking again may get the answer.
    ///
    /// Transient are transport failures and timeouts, a 429 of calls or of the one download at a
    /// time, and a busy 503. A quota 429 lasts until it resets, and a checksum mismatch is
    /// permanent, as LSE builds the file before sending it (LSE's docs, "Files").
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Transport(e) => matches!(
                e,
                HttpClientError::TransportError(_) | HttpClientError::TimeoutError(_)
            ),
            Self::Refusal {
                status, refusal, ..
            } => (*status == 429 && !refusal.is_quota()) || *status == 503,
            Self::Checksum { .. }
            | Self::Unexpected { .. }
            | Self::Malformed { .. }
            | Self::Retry(_) => false,
        }
    }

    /// Returns whether LSE refused the export as too wide: shorter dates may be served.
    pub fn is_too_wide(&self) -> bool {
        matches!(self, Self::Refusal { refusal, .. } if refusal.is_too_wide())
    }

    /// Returns how long LSE asked to wait before asking again (`Retry-After`).
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Refusal { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}
