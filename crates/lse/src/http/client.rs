//! The HTTP client of LSE's REST API, on Nautilus' `HttpClient`.
//!
//! Every call is a GET with the key in the `X-API-Key` header, kept within the plan's calls a
//! minute. A transient refusal is waited out as long as its `Retry-After` says.

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    num::NonZeroU32,
    time::Duration,
};

use bytes::Bytes;
use jiff::Timestamp;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_network::{
    http::{
        HttpClient, HttpRedirectPolicy, HttpResponse, Method, create_standard_nautilus_headers,
        parse_retry_after,
    },
    ratelimiter::quota::Quota,
    retry::{RetryConfig, RetryManager},
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{
    error::LseHttpError,
    models::{LseFactors, LsePair, LsePairs, LseRefusal, ReferenceCatalogue, ReferenceCollection},
    query::{ExportQuery, FactorsQuery, MacroQuery, PairsQuery, ReferenceQuery},
};
use crate::common::credential::LseCredential;

const EXPORT_PATH: &str = "/v3/export";
const PAIRS_PATH: &str = "/v3/pairs";
const REFERENCE_PATH: &str = "/v3/reference";
const REFERENCE_EXPORT_PATH: &str = "/v3/reference/export";
const MACRO_EXPORT_PATH: &str = "/v3/macro/export";
const FACTORS_PATH: &str = "/v3/factors";
const API_KEY_HEADER: &str = "X-API-Key";
const CHECKSUM_HEADER: &str = "x-checksum-sha256";
const EXPORT_ID_HEADER: &str = "x-export-id";
const RETRY_AFTER_HEADER: &str = "retry-after";
const RATE_KEY: &str = "lse";
const TIMEOUT_SECS: u64 = 300;
const BODY_EXCERPT: usize = 200;

/// LSE's REST API on one key.
pub struct LseHttpClient {
    base_url: String,
    client: HttpClient,
    /// A key has one download at a time (LSE's docs, "One download at a time").
    downloads: tokio::sync::Mutex<()>,
    retries: RetryManager<LseHttpError>,
}

impl Debug for LseHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(LseHttpClient))
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl LseHttpClient {
    /// Creates the client of the API at `base_url` on `credential`'s key.
    ///
    /// Calls are spaced `60 / calls_per_minute` s apart, so that no minute holds more however the
    /// plan counts it. Redirects are refused, so that the key goes nowhere else.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus' client cannot be built.
    pub fn new(
        base_url: String,
        credential: &LseCredential,
        calls_per_minute: NonZeroU32,
    ) -> anyhow::Result<Self> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert(API_KEY_HEADER.to_string(), credential.api_key().to_string());
        let quota = Quota::with_period(Duration::from_secs(60) / calls_per_minute.get())
            .expect("a minute over a positive count is not zero");
        let client = HttpClient::builder()
            .headers(headers)
            .header_keys(vec![
                CHECKSUM_HEADER.to_string(),
                EXPORT_ID_HEADER.to_string(),
                RETRY_AFTER_HEADER.to_string(),
            ])
            .keyed_quotas(vec![(RATE_KEY.to_string(), quota)])
            .timeout_secs(TIMEOUT_SECS)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .build()?;
        let retries = RetryManager::new(RetryConfig {
            max_retries: 10,
            initial_delay_ms: 1_000,
            max_delay_ms: 60_000,

            // The client's own timeout bounds each call
            operation_timeout_ms: None,
            ..RetryConfig::default()
        });
        Ok(Self {
            base_url,
            client,
            downloads: tokio::sync::Mutex::default(),
            retries,
        })
    }

    /// Returns the file of the export `query` selects, or `None` if LSE holds no row of it (see
    /// `LseRefusal::is_no_rows`).
    ///
    /// A transient failure is retried after the `Retry-After`, or a backoff.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE refuses the export, answers as its docs do not say, sends a whole
    /// file whose SHA-256 is not LSE's, or cannot be reached, after the retries a transient failure
    /// gets.
    pub async fn export(&self, query: &ExportQuery) -> Result<Option<Bytes>, LseHttpError> {
        self.download(EXPORT_PATH, query).await
    }

    /// Returns the file of the reference export `query` selects, as `export` does.
    ///
    /// # Errors
    ///
    /// Returns an error if the download fails, as with `export`.
    pub async fn reference_export(
        &self,
        query: &ReferenceQuery,
    ) -> Result<Option<Bytes>, LseHttpError> {
        self.download(REFERENCE_EXPORT_PATH, query).await
    }

    /// Returns the file of the macro export `query` selects, as `export` does.
    ///
    /// # Errors
    ///
    /// Returns an error if the download fails, as with `export`.
    pub async fn macro_export(&self, query: &MacroQuery) -> Result<Option<Bytes>, LseHttpError> {
        self.download(MACRO_EXPORT_PATH, query).await
    }

    /// Returns LSE's reference collections (`/v3/reference`).
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails, as with `export`.
    pub async fn reference_collections(&self) -> Result<Vec<ReferenceCollection>, LseHttpError> {
        self.json::<(), ReferenceCatalogue>("reference catalogue", REFERENCE_PATH, None)
            .await
            .map(|catalogue| catalogue.datasets)
    }

    /// Returns the listings LSE's catalogue holds of `query`'s symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails, as with `export`.
    pub async fn pairs(&self, query: &PairsQuery) -> Result<Vec<LsePair>, LseHttpError> {
        self.json::<_, LsePairs>("catalogue", PAIRS_PATH, Some(query))
            .await
            .map(|page| page.pairs)
    }

    /// Returns the splits and dividends of `query`'s instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails, as with `export`.
    pub async fn factors(&self, query: &FactorsQuery) -> Result<LseFactors, LseHttpError> {
        self.json("factors", FACTORS_PATH, Some(query)).await
    }

    async fn json<Q, T>(
        &self,
        what: &'static str,
        path: &str,
        query: Option<&Q>,
    ) -> Result<T, LseHttpError>
    where
        Q: Serialize + Sync,
        T: serde::de::DeserializeOwned,
    {
        let retry_after = |e: &LseHttpError| e.retry_after();
        let response = self
            .retries
            .invocation(
                what,
                || self.get(path, query),
                LseHttpError::is_transient,
                LseHttpError::Retry,
            )
            .retry_delay(&retry_after)
            .execute()
            .await?;
        serde_json::from_slice(&response.body)
            .map_err(|source| LseHttpError::Malformed { what, source })
    }

    async fn download<Q>(&self, path: &str, query: &Q) -> Result<Option<Bytes>, LseHttpError>
    where
        Q: Serialize + Display + Sync,
    {
        let _download = self.downloads.lock().await;
        let retry_after = |e: &LseHttpError| e.retry_after();
        self.retries
            .invocation(
                "export",
                || self.download_once(path, query),
                LseHttpError::is_transient,
                LseHttpError::Retry,
            )
            .retry_delay(&retry_after)
            .execute()
            .await
    }

    async fn download_once<Q>(&self, path: &str, query: &Q) -> Result<Option<Bytes>, LseHttpError>
    where
        Q: Serialize + Display + Sync,
    {
        let response = match self.get(path, Some(query)).await {
            Err(LseHttpError::Refusal { refusal, .. }) if refusal.is_no_rows() => {
                log::debug!("LSE holds no rows of {query}: {refusal}");
                return Ok(None);
            }
            answer => answer?,
        };

        // LSE's file comes in a 200 beside its SHA-256 (LSE's docs, "Files")
        let (200, Some(expected)) = (
            response.status.as_u16(),
            response.headers.get(CHECKSUM_HEADER),
        ) else {
            return Err(unexpected(&response));
        };
        let actual = Sha256::digest(&response.body);
        if !expected.eq_ignore_ascii_case(&hex(&actual)) {
            return Err(LseHttpError::Checksum {
                len: response.body.len(),
                export_id: response.headers.get(EXPORT_ID_HEADER).cloned(),
            });
        }
        Ok(Some(response.body))
    }

    async fn get<Q>(&self, path: &str, query: Option<&Q>) -> Result<HttpResponse, LseHttpError>
    where
        Q: Serialize + Sync,
    {
        let response = self
            .client
            .request_with_params(
                Method::GET,
                format!("{}{path}", self.base_url),
                query,
                None,
                None,
                None,
                Some(vec![RATE_KEY.to_string()]),
            )
            .await?;
        if response.status.is_success() {
            return Ok(response);
        }
        let Ok(refusal) = serde_json::from_slice::<LseRefusal>(&response.body) else {
            return Err(unexpected(&response));
        };
        let retry_after = response.headers.get(RETRY_AFTER_HEADER).and_then(|value| {
            parse_retry_after(
                value,
                Timestamp::from(get_atomic_clock_realtime().get_time_ns()),
            )
        });
        Err(LseHttpError::Refusal {
            status: response.status.as_u16(),
            refusal: Box::new(refusal),
            retry_after,
        })
    }
}

fn unexpected(response: &HttpResponse) -> LseHttpError {
    let body = String::from_utf8_lossy(&response.body);
    LseHttpError::Unexpected {
        status: response.status.as_u16(),
        body: body.chars().take(BODY_EXCERPT).collect(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
