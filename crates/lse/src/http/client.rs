//! The HTTP client of LSE's REST API, on Nautilus' `HttpClient`: every
//! call a GET with the key in the `X-API-Key` header, kept within the
//! calls a minute of the key's plan, and a refusal for the moment waited
//! out, as long as its `Retry-After` says. It calls a market's exports and
//! the lookups of its catalogue, the reference catalogue and a reference
//! collection's exports, a macro series' exports, and an instrument's
//! splits and dividends.

use std::{collections::HashMap, fmt, num::NonZeroU32, time::Duration};

use bytes::Bytes;
use jiff::Timestamp;
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
use tokio::sync::Mutex;

use super::{
    error::LseHttpError,
    models::{LseFactors, LsePair, LsePairs, LseRefusal, ReferenceCatalogue, ReferenceCollection},
    query::{ExportQuery, FactorsQuery, MacroQuery, PairsQuery, ReferenceQuery},
};
use crate::common::credential::LseCredential;

/// The paths of the calls: a market's export, a lookup of its catalogue,
/// the reference catalogue, a reference collection's export, a macro
/// series' export and an instrument's factors.
const EXPORT_PATH: &str = "/v3/export";
const PAIRS_PATH: &str = "/v3/pairs";
const REFERENCE_PATH: &str = "/v3/reference";
const REFERENCE_EXPORT_PATH: &str = "/v3/reference/export";
const MACRO_EXPORT_PATH: &str = "/v3/macro/export";
const FACTORS_PATH: &str = "/v3/factors";
/// The header carrying the key.
const API_KEY_HEADER: &str = "X-API-Key";
/// The headers read of LSE's answers: a file's SHA-256 and LSE's id of
/// its export, and how long to wait before asking again.
const CHECKSUM_HEADER: &str = "x-checksum-sha256";
const EXPORT_ID_HEADER: &str = "x-export-id";
const RETRY_AFTER_HEADER: &str = "retry-after";
/// The key of the one quota every call counts against.
const RATE_KEY: &str = "lse";
/// How long a call, its file included, may take.
const TIMEOUT_SECS: u64 = 300;
/// The most of an answer's body an error quotes.
const BODY_EXCERPT: usize = 200;

/// LSE's REST API on one key.
pub struct LseHttpClient {
    base_url: String,
    client: HttpClient,
    /// Held over an export: a key has one download open at a time (LSE's
    /// docs, "One download at a time").
    downloads: Mutex<()>,
    retries: RetryManager<LseHttpError>,
}

impl fmt::Debug for LseHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(LseHttpClient))
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl LseHttpClient {
    /// The API at `base_url` on `credential`'s key, one call every
    /// `60 / calls_per_minute` s at most, so that no minute holds more,
    /// however the plan's minute is counted. Redirects are refused, so
    /// that the key goes nowhere else.
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
            // The client's own timeout bounds each call.
            operation_timeout_ms: None,
            ..RetryConfig::default()
        });
        Ok(Self {
            base_url,
            client,
            downloads: Mutex::default(),
            retries,
        })
    }

    /// The client, asking again as `retry` says, for a test that cannot
    /// wait out the backoff.
    #[cfg(feature = "test-support")]
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retries = RetryManager::new(retry);
        self
    }

    /// The file of the export `query` selects, LSE's 200 beside its SHA-256,
    /// or `None` if LSE holds no row of it (see `LseRefusal::is_no_rows`).
    /// One download at a time: a second export waits for the first. A call
    /// LSE refuses for the moment, or a file cut short, is asked again after
    /// the `Retry-After` LSE gave, or a backoff; a whole file whose SHA-256
    /// is not the one LSE gave fails the export.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE refuses the export (its code, message and
    /// required plan in the error), answers as its docs do not say, or
    /// cannot be reached, after the retries a transient failure gets.
    pub async fn export(&self, query: &ExportQuery) -> Result<Option<Bytes>, LseHttpError> {
        self.download(EXPORT_PATH, query).await
    }

    /// The file of the reference export `query` selects, or `None` if LSE
    /// holds no row of it, downloaded as `export` downloads one.
    ///
    /// # Errors
    ///
    /// As `export`.
    pub async fn reference_export(
        &self,
        query: &ReferenceQuery,
    ) -> Result<Option<Bytes>, LseHttpError> {
        self.download(REFERENCE_EXPORT_PATH, query).await
    }

    /// The file of the macro export `query` selects, or `None` if LSE holds
    /// no row of it, downloaded as `export` downloads one.
    ///
    /// # Errors
    ///
    /// As `export`.
    pub async fn macro_export(&self, query: &MacroQuery) -> Result<Option<Bytes>, LseHttpError> {
        self.download(MACRO_EXPORT_PATH, query).await
    }

    /// LSE's reference collections, as its catalogue lists them
    /// (`/v3/reference`), each with the column of its rows' time. A call
    /// LSE refuses for the moment is asked again as `export`'s is.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE refuses the call, answers as its docs do not
    /// say, or cannot be reached, after the retries a transient failure
    /// gets.
    pub async fn reference_collections(&self) -> Result<Vec<ReferenceCollection>, LseHttpError> {
        self.json::<(), ReferenceCatalogue>("reference catalogue", REFERENCE_PATH, None)
            .await
            .map(|catalogue| catalogue.datasets)
    }

    /// The listings LSE's catalogue holds of `query`'s symbol. A call LSE
    /// refuses for the moment is asked again as `export`'s is.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE refuses the lookup (its code, message and
    /// required plan in the error), answers as its docs do not say, or
    /// cannot be reached, after the retries a transient failure gets.
    pub async fn pairs(&self, query: &PairsQuery) -> Result<Vec<LsePair>, LseHttpError> {
        self.json::<_, LsePairs>("catalogue", PAIRS_PATH, Some(query))
            .await
            .map(|page| page.pairs)
    }

    /// The splits and dividends of `query`'s instrument, whole, as LSE's
    /// factors answer them. A call LSE refuses for the moment is asked
    /// again as `export`'s is.
    ///
    /// # Errors
    ///
    /// As `pairs`.
    pub async fn factors(&self, query: &FactorsQuery) -> Result<LseFactors, LseHttpError> {
        self.json("factors", FACTORS_PATH, Some(query)).await
    }

    /// LSE's JSON answer to a GET of `path` with `query`, `what` it is,
    /// asked again for a refusal for the moment as an export is.
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

    /// The file of the export of `path` that `query` selects, or `None`
    /// (see `export`), one download at a time.
    async fn download<Q>(&self, path: &str, query: &Q) -> Result<Option<Bytes>, LseHttpError>
    where
        Q: Serialize + fmt::Display + Sync,
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
        Q: Serialize + fmt::Display + Sync,
    {
        let response = match self.get(path, Some(query)).await {
            Err(LseHttpError::Refusal { refusal, .. }) if refusal.is_no_rows() => {
                log::debug!("LSE holds no rows of {query}: {refusal}");
                return Ok(None);
            }
            answer => answer?,
        };
        // LSE's file comes in a 200 beside its SHA-256 (LSE's docs, "Files").
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

    /// LSE's answer to a GET of `path` with `query`, if it serves it.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE cannot be reached, or does not serve the
    /// call: its refusal, with its HTTP status and the `Retry-After` it
    /// gave, or its answer, if it is not a refusal its docs describe.
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
        let retry_after = response
            .headers
            .get(RETRY_AFTER_HEADER)
            .and_then(|value| parse_retry_after(value, Timestamp::now()));
        Err(LseHttpError::Refusal {
            status: response.status.as_u16(),
            refusal: Box::new(refusal),
            retry_after,
        })
    }
}

/// `response`, an answer LSE's docs do not describe, as an error: its
/// status and the start of its body.
fn unexpected(response: &HttpResponse) -> LseHttpError {
    let body = String::from_utf8_lossy(&response.body);
    LseHttpError::Unexpected {
        status: response.status.as_u16(),
        body: body.chars().take(BODY_EXCERPT).collect(),
    }
}

/// `bytes` in lower-case hex, as LSE writes a SHA-256.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
