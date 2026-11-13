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
    /// A whole file whose SHA-256 is not the one LSE gave with it, with
    /// LSE's id of the export (`x-export-id`), if it gave one.
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
    /// Whether asking again may get the answer: a transport failure or a
    /// timeout, a file cut short among them, and LSE's refusals for the
    /// moment, a 429 of calls or of the one download at a time and a busy
    /// 503, but not a 429 of a quota, which lasts until it resets. A whole
    /// file whose SHA-256 is not LSE's is no transient failure: LSE builds
    /// the file before sending it, so that its checksum is exact (LSE's
    /// docs, "Files").
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            Self::Transport(e) => matches!(
                e,
                HttpClientError::TransportError(_) | HttpClientError::TimeoutError(_)
            ),
            Self::Refusal {
                status, refusal, ..
            } => (*status == 429 && !refusal.is_quota()) || *status == 503,
            Self::Checksum { .. } | Self::Unexpected { .. } | Self::Retry(_) => false,
        }
    }

    /// Whether LSE holds no row of the export: a 404 saying no stored row
    /// is in the selection or no session in its dates. A 400
    /// `data-unavailable` says the market serves no such rows at all.
    pub(crate) fn is_no_rows(&self) -> bool {
        matches!(self, Self::Refusal { status: 404, refusal, .. } if refusal.is_no_rows())
    }

    /// Whether LSE refused the export as too wide: shorter dates may be
    /// served.
    pub(crate) fn is_too_wide(&self) -> bool {
        matches!(self, Self::Refusal { refusal, .. } if refusal.is_too_wide())
    }

    /// How long LSE asked to wait before asking again (`Retry-After`).
    pub(crate) fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Refusal { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}
