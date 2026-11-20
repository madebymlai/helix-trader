//! The failures of the HTTP client, and which of them a retry may lift.

use std::time::Duration;

use nautilus_network::{http::HttpClientError, retry::RetryError};

use super::models::LseRefusal;

/// How a call to LSE failed.
#[derive(Debug, thiserror::Error)]
pub enum LseHttpError {
    /// LSE was not reached, or did not answer in time.
    #[error("{0}")]
    Transport(#[from] HttpClientError),
    /// LSE refused the call.
    ///
    /// Carries the HTTP status and the `Retry-After` LSE gave, if any.
    #[error("LSE refused it with {status} {refusal}")]
    Refusal {
        status: u16,
        refusal: Box<LseRefusal>,
        retry_after: Option<Duration>,
    },
    /// LSE gave an answer its docs do not describe.
    #[error("LSE answered {status}, not as its docs say: {body}")]
    Unexpected { status: u16, body: String },
    /// LSE gave an answer whose body differs from what its docs say.
    #[error("LSE's {what} is not as its docs say: {source}")]
    Malformed {
        what: &'static str,
        source: serde_json::Error,
    },
    /// The whole file's SHA-256 differs from the one LSE gave.
    ///
    /// Carries the `x-export-id`, if any.
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
    /// Transient failures are:
    /// - Transport failures and timeouts.
    /// - A 429 for the call limit or the one-download-at-a-time limit.
    /// - A busy 503.
    ///
    /// Permanent failures are:
    /// - A quota 429, which lasts until the quota resets.
    /// - A checksum mismatch, because LSE builds the file before sending it (LSE's docs, "Files").
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

    /// Returns whether LSE refused the export as too wide.
    ///
    /// LSE may serve shorter dates.
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
