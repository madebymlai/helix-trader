//! The HTTP client of LSE's REST API, on Nautilus' `HttpClient`: every
//! call a GET with the key in the `X-API-Key` header, kept within the
//! calls a minute of the key's plan, and a refusal for the moment waited
//! out, as long as its `Retry-After` says. It calls a market's exports,
//! and the reference catalogue and a reference collection's exports.

use std::{collections::HashMap, fmt, num::NonZeroU32, sync::Arc, time::Duration};

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
    models::{LseRefusal, ReferenceCatalogue, ReferenceCollection},
    query::{ExportQuery, ReferenceQuery},
};
use crate::common::credential::LseCredential;

/// The paths of the calls: a market's export, the reference catalogue
/// and a reference collection's export.
const EXPORT_PATH: &str = "/v3/export";
const REFERENCE_PATH: &str = "/v3/reference";
const REFERENCE_EXPORT_PATH: &str = "/v3/reference/export";
/// The header carrying the key.
const API_KEY_HEADER: &str = "X-API-Key";
/// The headers read of LSE's answers: a file's SHA-256, and how long to
/// wait before asking again.
const CHECKSUM_HEADER: &str = "x-checksum-sha256";
const RETRY_AFTER_HEADER: &str = "retry-after";
/// The key of the one quota every call counts against.
const RATE_KEY: &str = "lse";
/// How long a call, its file included, may take.
const TIMEOUT_SECS: u64 = 300;
/// The most of an answer's body an error quotes.
const BODY_EXCERPT: usize = 200;

/// LSE's REST API on one key.
pub(crate) struct LseHttpClient {
    base_url: String,
    client: HttpClient,
    /// Held over an export: a key has one download open at a time (LSE's
    /// docs, "One download at a time").
    downloads: Arc<Mutex<()>>,
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
    pub(crate) fn new(
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
            downloads: Arc::default(),
            retries,
        })
    }

    /// The file of the export `query` selects, or `None` if LSE holds no
    /// row of it (`data-unavailable`, `date-not-trading`). One download at
    /// a time: a second export waits for the first. A call LSE refuses for
    /// the moment, or a file cut short, is asked again after the
    /// `Retry-After` LSE gave, or a backoff.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE refuses the export (its code, message and
    /// required plan in the error), answers as its docs do not say, or
    /// cannot be reached, after the retries a transient failure gets.
    pub(crate) async fn export(&self, query: &ExportQuery) -> Result<Option<Bytes>, LseHttpError> {
        self.download(EXPORT_PATH, query).await
    }

    /// The file of the reference export `query` selects, or `None` if LSE
    /// holds no row of it, downloaded as `export` downloads one.
    ///
    /// # Errors
    ///
    /// As `export`.
    pub(crate) async fn reference_export(
        &self,
        query: &ReferenceQuery,
    ) -> Result<Option<Bytes>, LseHttpError> {
        self.download(REFERENCE_EXPORT_PATH, query).await
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
    pub(crate) async fn reference_collections(
        &self,
    ) -> Result<Vec<ReferenceCollection>, LseHttpError> {
        let retry_after = |e: &LseHttpError| e.retry_after();
        let response = self
            .retries
            .invocation(
                "reference catalogue",
                || self.get(REFERENCE_PATH, None::<&()>),
                LseHttpError::is_transient,
                LseHttpError::Retry,
            )
            .retry_delay(&retry_after)
            .execute()
            .await?;
        serde_json::from_slice::<ReferenceCatalogue>(&response.body)
            .map(|catalogue| catalogue.datasets)
            .map_err(|source| LseHttpError::Malformed {
                what: "reference catalogue",
                source,
            })
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
        if let Some(expected) = response.headers.get(CHECKSUM_HEADER) {
            let actual = Sha256::digest(&response.body);
            if !expected.eq_ignore_ascii_case(&hex(&actual)) {
                return Err(LseHttpError::Checksum {
                    len: response.body.len(),
                });
            }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nautilus_core::string::secret::SecretString;

    use super::*;
    use crate::{
        common::symbol::LseSymbol,
        http::query::ReferenceQuery,
        testing::{
            API_KEY, CALLS_EXHAUSTED, DATA_UNAVAILABLE, DATE_NOT_TRADING, GOLD_TRADES, MockLse,
            PLAN_REQUIRED, REFERENCE_CATALOGUE, REFERENCE_DATA_UNAVAILABLE, ROWS_EXHAUSTED, Reply,
            US10Y_BOND_YIELDS,
        },
    };

    fn client(lse: &MockLse, calls_per_minute: u32) -> LseHttpClient {
        let credential =
            LseCredential::resolve(Some(SecretString::from(API_KEY)), |_| None).unwrap();
        LseHttpClient::new(
            lse.url().to_string(),
            &credential,
            NonZeroU32::new(calls_per_minute).unwrap(),
        )
        .unwrap()
    }

    /// GOLD in `futures` over the captured second.
    fn gold_second() -> ExportQuery {
        let gold = LseSymbol::new("futures", "GOLD").unwrap();
        ExportQuery::trades(
            &gold,
            "2026-10-21T10:00:00Z".parse().unwrap(),
            "2026-10-21T10:00:01Z".parse().unwrap(),
        )
    }

    #[tokio::test]
    async fn an_export_asks_lse_s_selection_with_the_key_and_gets_the_file() {
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        let file = client(&lse, 60).export(&gold_second()).await.unwrap();

        assert_eq!(file.as_deref(), Some(GOLD_TRADES));
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(call.path, "/v3/export");
        assert_eq!(call.api_key.as_deref(), Some(API_KEY));
        let mut query = call.query.clone();
        query.sort();
        let expected = [
            ("adjusted", "0"),
            ("format", "parquet"),
            ("from", "2026-10-21T10:00:00Z"),
            ("market", "futures"),
            ("symbol", "GOLD"),
            ("timeframe", "tick"),
            ("to", "2026-10-21T10:00:01Z"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        assert_eq!(query, expected);
    }

    #[tokio::test]
    async fn a_refusal_is_an_error_with_lse_s_code_message_and_required_plan_asked_once() {
        let lse = MockLse::start(vec![
            Reply::refusal(403, PLAN_REQUIRED),
            Reply::file(GOLD_TRADES),
        ]);

        let err = client(&lse, 60).export(&gold_second()).await.unwrap_err();

        let err = err.to_string();
        for part in [
            "403",
            "plan-required",
            "This feed requires a subscription",
            "CME Group Lite",
        ] {
            assert!(err.contains(part), "{part}: {err}");
        }
        assert_eq!(lse.calls().len(), 1);
    }

    #[tokio::test]
    async fn no_stored_row_and_no_session_are_no_file() {
        for refusal in [DATA_UNAVAILABLE, DATE_NOT_TRADING] {
            let lse = MockLse::start(vec![Reply::refusal(404, refusal)]);

            let file = client(&lse, 60).export(&gold_second()).await.unwrap();

            assert_eq!(file, None, "{refusal}");
        }
    }

    /// LSE's documented 429, its wait cut to a second.
    #[tokio::test]
    async fn a_429_of_calls_is_waited_out_by_its_retry_after_then_asked_again() {
        let lse = MockLse::start(vec![
            Reply::refusal(429, CALLS_EXHAUSTED).retry_after(1),
            Reply::file(GOLD_TRADES),
        ]);

        let file = client(&lse, 6000).export(&gold_second()).await.unwrap();

        assert_eq!(file.as_deref(), Some(GOLD_TRADES));
        let [refused, asked_again] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        let waited = asked_again.received - refused.answered;
        assert!(waited >= Duration::from_secs(1), "{waited:?}");
    }

    /// The rows a day reset at 00:00 UTC: no wait of a run's gets past
    /// them.
    #[tokio::test]
    async fn a_429_of_a_quota_is_an_error_asked_once() {
        let lse = MockLse::start(vec![
            Reply::refusal(429, ROWS_EXHAUSTED).retry_after(18_720),
            Reply::file(GOLD_TRADES),
        ]);

        let err = client(&lse, 6000).export(&gold_second()).await.unwrap_err();

        assert!(err.to_string().contains("rows-exhausted"), "{err}");
        assert_eq!(lse.calls().len(), 1);
    }

    #[tokio::test]
    async fn a_file_cut_short_is_fetched_again() {
        let lse = MockLse::start(vec![Reply::cut_file(GOLD_TRADES), Reply::file(GOLD_TRADES)]);

        let file = client(&lse, 6000).export(&gold_second()).await.unwrap();

        assert_eq!(file.as_deref(), Some(GOLD_TRADES));
        assert_eq!(lse.calls().len(), 2);
    }

    /// 120 calls a minute: one every half second.
    #[tokio::test]
    async fn calls_come_no_faster_than_the_calls_a_minute_allow() {
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES), Reply::file(GOLD_TRADES)]);
        let client = client(&lse, 120);

        client.export(&gold_second()).await.unwrap();
        client.export(&gold_second()).await.unwrap();

        let [first, second] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        let apart = second.received - first.received;
        assert!(apart >= Duration::from_millis(450), "{apart:?}");
    }

    /// Two exports at once, each file taking 300 ms: the second is asked
    /// once the first has come.
    #[tokio::test]
    async fn one_export_downloads_at_a_time() {
        let slow = || Reply::file(GOLD_TRADES).after(Duration::from_millis(300));
        let lse = MockLse::start(vec![slow(), slow()]);
        let client = client(&lse, 6000);
        let query = gold_second();

        let (a, b) = tokio::join!(client.export(&query), client.export(&query));

        a.unwrap();
        b.unwrap();
        let [first, second] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert!(second.received >= first.answered, "{first:?} {second:?}");
    }

    /// US10Y in the daily bond yields from 9 to 12 November.
    fn us10y_days() -> ReferenceQuery {
        ReferenceQuery::new(
            "bond_yields",
            "US10Y",
            "2026-11-09".parse().unwrap(),
            "2026-11-12".parse().unwrap(),
        )
    }

    #[tokio::test]
    async fn the_reference_catalogue_names_the_time_column_of_each_collection() {
        let lse = MockLse::start(vec![Reply::json(REFERENCE_CATALOGUE)]);

        let collections = client(&lse, 60).reference_collections().await.unwrap();

        let time_column = |name: &str| {
            collections
                .iter()
                .find(|collection| collection.dataset == name)
                .map(|collection| collection.time_column.as_str())
        };
        assert_eq!(collections.len(), 21);
        assert_eq!(time_column("bond_yields"), Some("date"));
        assert_eq!(time_column("bond_yields_hourly"), Some("ts"));
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(call.path, "/v3/reference");
        assert_eq!(call.query, []);
        assert_eq!(call.api_key.as_deref(), Some(API_KEY));
    }

    /// A body made up for the test: valid JSON with no collections.
    #[tokio::test]
    async fn a_catalogue_not_as_lse_s_docs_describe_it_is_an_error_saying_so() {
        let lse = MockLse::start(vec![Reply::json(r#"{"collections": []}"#)]);

        let err = client(&lse, 60).reference_collections().await.unwrap_err();

        assert!(err.to_string().contains("reference catalogue"), "{err}");
        assert_eq!(lse.calls().len(), 1);
    }

    #[tokio::test]
    async fn a_reference_export_asks_lse_s_selection_by_its_dates_and_gets_the_file() {
        let lse = MockLse::start(vec![Reply::file(US10Y_BOND_YIELDS)]);

        let file = client(&lse, 60)
            .reference_export(&us10y_days())
            .await
            .unwrap();

        assert_eq!(file.as_deref(), Some(US10Y_BOND_YIELDS));
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(call.path, "/v3/reference/export");
        assert_eq!(call.api_key.as_deref(), Some(API_KEY));
        let mut query = call.query.clone();
        query.sort();
        let expected = [
            ("dataset", "bond_yields"),
            ("format", "parquet"),
            ("from", "2026-11-09"),
            ("symbol", "US10Y"),
            ("to", "2026-11-12"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        assert_eq!(query, expected);
    }

    #[tokio::test]
    async fn a_reference_selection_with_no_stored_row_is_no_file() {
        let lse = MockLse::start(vec![Reply::refusal(404, REFERENCE_DATA_UNAVAILABLE)]);

        let file = client(&lse, 60)
            .reference_export(&us10y_days())
            .await
            .unwrap();

        assert_eq!(file, None);
    }

    #[test]
    fn the_key_is_not_in_the_client_s_debug() {
        let lse = MockLse::start(vec![]);

        assert!(!format!("{:?}", client(&lse, 60)).contains(API_KEY));
    }
}
