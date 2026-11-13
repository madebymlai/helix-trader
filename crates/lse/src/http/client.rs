//! The HTTP client of LSE's REST API, on Nautilus' `HttpClient`: its
//! exports and the lookups of its catalogue, every call a GET with the key
//! in the `X-API-Key` header, kept within the calls a minute of the key's
//! plan, and a refusal for the moment waited out, as long as its
//! `Retry-After` says.

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
    models::{LsePair, LsePairs, LseRefusal},
    query::{ExportQuery, PairsQuery},
};
use crate::common::credential::LseCredential;

/// The calls the client makes: an export's, and a lookup of the
/// catalogue's.
const EXPORT_PATH: &str = "/v3/export";
const PAIRS_PATH: &str = "/v3/pairs";
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
            downloads: Arc::default(),
            retries,
        })
    }

    /// The client, asking again as `retry` says, for a test that cannot
    /// wait out the backoff.
    #[cfg(test)]
    pub(crate) fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retries = RetryManager::new(retry);
        self
    }

    /// The file of the export `query` selects, or `None` if LSE holds no
    /// row of it (`data-unavailable`, `date-not-trading`). One download at
    /// a time: a second export waits for the first. A call LSE refuses for
    /// the moment, or a file cut short, is asked again after the
    /// `Retry-After` LSE gave, or a backoff; a whole file whose SHA-256 is
    /// not the one LSE gave fails the export.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE refuses the export (its code, message and
    /// required plan in the error), answers as its docs do not say, or
    /// cannot be reached, after the retries a transient failure gets.
    pub(crate) async fn export(&self, query: &ExportQuery) -> Result<Option<Bytes>, LseHttpError> {
        let _download = self.downloads.lock().await;
        let retry_after = |e: &LseHttpError| e.retry_after();
        self.retries
            .invocation(
                "export",
                || self.export_once(query),
                LseHttpError::is_transient,
                LseHttpError::Retry,
            )
            .retry_delay(&retry_after)
            .execute()
            .await
    }

    async fn export_once(&self, query: &ExportQuery) -> Result<Option<Bytes>, LseHttpError> {
        let response = match self.get(EXPORT_PATH, query).await {
            Ok(response) => response,
            Err(e) if e.is_no_rows() => {
                log::debug!("LSE holds no rows of {query}: {e}");
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        if let Some(expected) = response.headers.get(CHECKSUM_HEADER) {
            let actual = Sha256::digest(&response.body);
            if !expected.eq_ignore_ascii_case(&hex(&actual)) {
                return Err(LseHttpError::Checksum {
                    len: response.body.len(),
                    export_id: response.headers.get(EXPORT_ID_HEADER).cloned(),
                });
            }
        }
        Ok(Some(response.body))
    }

    /// The listings LSE's catalogue holds of `query`'s symbol. A call LSE
    /// refuses for the moment is asked again as an export is.
    ///
    /// # Errors
    ///
    /// Returns an error if LSE refuses the lookup (its code, message and
    /// required plan in the error), answers as its docs do not say, or
    /// cannot be reached, after the retries a transient failure gets.
    pub(crate) async fn pairs(&self, query: &PairsQuery) -> Result<Vec<LsePair>, LseHttpError> {
        let retry_after = |e: &LseHttpError| e.retry_after();
        self.retries
            .invocation(
                "pairs",
                || self.pairs_once(query),
                LseHttpError::is_transient,
                LseHttpError::Retry,
            )
            .retry_delay(&retry_after)
            .execute()
            .await
    }

    async fn pairs_once(&self, query: &PairsQuery) -> Result<Vec<LsePair>, LseHttpError> {
        let response = self.get(PAIRS_PATH, query).await?;
        serde_json::from_slice::<LsePairs>(&response.body)
            .map(|page| page.pairs)
            .map_err(|_| unexpected(&response))
    }

    /// One call of `path` with `query`: LSE's answer if it is a success,
    /// else its refusal, with the `Retry-After` it gave.
    async fn get(&self, path: &str, query: &impl Serialize) -> Result<HttpResponse, LseHttpError> {
        let response = self
            .client
            .request_with_params(
                Method::GET,
                format!("{}{path}", self.base_url),
                Some(query),
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

/// `response`, an answer LSE's docs do not describe: its status and the
/// start of its body.
fn unexpected(response: &HttpResponse) -> LseHttpError {
    LseHttpError::Unexpected {
        status: response.status.as_u16(),
        body: String::from_utf8_lossy(&response.body)
            .chars()
            .take(BODY_EXCERPT)
            .collect(),
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
        common::{credential::LseProduct, symbol::LseSymbol},
        http::query::{PairsQuery, Rows},
        testing::{
            API_KEY, CALLS_EXHAUSTED, DATA_UNAVAILABLE, DATA_UNAVAILABLE_QUOTES, DATE_NOT_TRADING,
            EXPORT_ID, GCZ26_PAIR_ID, GCZ26_QUOTES, GOLD_TRADES, MockLse, PAIRS_ESZ6,
            PLAN_REQUIRED, ROWS_EXHAUSTED, Reply,
        },
    };

    fn client(lse: &MockLse, calls_per_minute: u32) -> LseHttpClient {
        let credential = LseCredential::resolve(
            LseProduct::Global,
            Some(SecretString::from(API_KEY)),
            |_| None,
        )
        .unwrap();
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
        ExportQuery::series(
            &gold,
            Rows::TradeSlice,
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

    /// LSE's docs, "Files": a download that is cut is requested again from
    /// the start.
    #[tokio::test]
    async fn a_file_cut_short_is_fetched_again() {
        let lse = MockLse::start(vec![Reply::cut_file(GOLD_TRADES), Reply::file(GOLD_TRADES)]);

        let file = client(&lse, 6000).export(&gold_second()).await.unwrap();

        assert_eq!(file.as_deref(), Some(GOLD_TRADES));
        assert_eq!(lse.calls().len(), 2);
    }

    /// A whole file LSE built before sending, whose SHA-256 is not the one
    /// LSE gave: the export is named by LSE's id, for LSE to look into.
    #[tokio::test]
    async fn a_file_whose_sha_256_is_not_lse_s_fails_naming_lse_s_export_asked_once() {
        let lse = MockLse::start(vec![
            Reply::corrupt_file(GOLD_TRADES),
            Reply::file(GOLD_TRADES),
        ]);

        let err = client(&lse, 6000).export(&gold_second()).await.unwrap_err();

        assert!(err.to_string().contains(EXPORT_ID), "{err}");
        assert_eq!(lse.calls().len(), 1);
    }

    /// Two retries, 10 ms apart: three calls, then LSE's last refusal.
    #[tokio::test]
    async fn retries_that_run_out_end_in_lse_s_last_refusal() {
        let lse = MockLse::start(vec![Reply::refusal(429, CALLS_EXHAUSTED); 4]);
        let client = client(&lse, 6000).with_retry(RetryConfig {
            max_retries: 2,
            initial_delay_ms: 10,
            max_delay_ms: 10,
            jitter_ms: 0,
            operation_timeout_ms: None,
            ..RetryConfig::default()
        });

        let err = client.export(&gold_second()).await.unwrap_err();

        assert!(err.to_string().contains("calls-exhausted"), "{err}");
        assert_eq!(lse.calls().len(), 3);
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

    /// LSE's catalogue lists ESZ6's two listings, of 2016 and of 2026.
    #[tokio::test]
    async fn a_lookup_asks_lse_s_catalogue_for_a_symbol_s_listings() {
        let lse = MockLse::start(vec![Reply::answer(PAIRS_ESZ6)]);

        let pairs = client(&lse, 60)
            .pairs(&PairsQuery::new("cme", "ESZ6"))
            .await
            .unwrap();

        let listed: Vec<_> = pairs
            .iter()
            .map(|pair| (pair.pair_id, pair.name.as_str()))
            .collect();
        assert_eq!(
            listed,
            [
                (4_000_000_001, "ES futures 2026-12"),
                (4_000_658_762, "ES futures 2016-12")
            ]
        );
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(call.path, "/v3/pairs");
        assert_eq!(call.api_key.as_deref(), Some(API_KEY));
        let mut query = call.query.clone();
        query.sort();
        assert_eq!(
            query,
            [("market", "cme"), ("symbol", "ESZ6")]
                .map(|(name, value)| (name.to_string(), value.to_string()))
        );
    }

    /// A listing's rows are selected by its number and by `data`, no
    /// symbol or timeframe beside them.
    #[tokio::test]
    async fn an_export_of_a_listing_asks_for_its_rows_by_its_pair_id() {
        let lse = MockLse::start(vec![Reply::file(GCZ26_QUOTES)]);
        let query = ExportQuery::listing(
            "cme",
            GCZ26_PAIR_ID,
            Rows::Quotes,
            "2026-11-12T14:30:00Z".parse().unwrap(),
            "2026-11-12T14:30:01Z".parse().unwrap(),
        );

        let file = client(&lse, 60).export(&query).await.unwrap();

        assert_eq!(file.as_deref(), Some(GCZ26_QUOTES));
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        let mut query = call.query.clone();
        query.sort();
        let expected = [
            ("adjusted", "0"),
            ("data", "quotes"),
            ("format", "parquet"),
            ("from", "2026-11-12T14:30:00Z"),
            ("market", "cme"),
            ("pair_id", "4000000002"),
            ("to", "2026-11-12T14:30:01Z"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        assert_eq!(query, expected);
    }

    /// `data-unavailable` is no rows as a 404 only: as a 400 it says the
    /// market serves no such rows, as a derived market's quotes.
    #[tokio::test]
    async fn a_400_data_unavailable_is_lse_s_refusal_not_an_empty_export() {
        let lse = MockLse::start(vec![Reply::refusal(400, DATA_UNAVAILABLE_QUOTES)]);

        let err = client(&lse, 60).export(&gold_second()).await.unwrap_err();

        let err = err.to_string();
        for part in ["400", "data-unavailable", "This market offers timeframes"] {
            assert!(err.contains(part), "{part}: {err}");
        }
    }

    #[test]
    fn the_key_is_not_in_the_client_s_debug() {
        let lse = MockLse::start(vec![]);

        assert!(!format!("{:?}", client(&lse, 60)).contains(API_KEY));
    }
}
