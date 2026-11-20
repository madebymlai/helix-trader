//! LSE's HTTP client against the mock LSE peer (`lse::testing::MockLse`).

use std::{num::NonZeroU32, time::Duration};

use lse::{
    common::{
        credential::{LseCredential, LseProduct},
        symbol::LseSymbol,
    },
    http::{
        client::LseHttpClient,
        error::LseHttpError,
        query::{Dates, ExportQuery, PairsQuery, ReferenceQuery, Rows},
    },
    testing::{
        API_KEY, CALLS_EXHAUSTED, DATA_UNAVAILABLE, DATA_UNAVAILABLE_QUOTES, DATE_NOT_TRADING,
        EXPORT_ID, GCZ26_PAIR_ID, GCZ26_QUOTES, GOLD_TRADES, MockLse, PAIRS_ESZ6, PLAN_REQUIRED,
        REFERENCE_CATALOGUE, REFERENCE_DATA_UNAVAILABLE, ROWS_EXHAUSTED, Reply, TICKS_NOT_STORED,
        US10Y_BOND_YIELDS,
    },
};
use nautilus_core::string::secret::SecretString;
use rstest::rstest;

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

/// LSE's refusals that asking again cannot lift:
/// - A plan that the key lacks.
/// - `data-unavailable` for a market that stores no trades. Only its message tells it from an
///   empty selection, and the message begins "No stored data for this selection" (LSE's docs,
///   "Refusal codes").
/// - `data-unavailable` as a 400, meaning the market serves no such rows, as with a derived
///   market's quotes. Only a 404 means no rows.
/// - A 429 for a quota, such as the rows a day, which resets at 00:00 UTC.
#[rstest]
#[case::a_plan_the_key_lacks(
    Reply::refusal(403, PLAN_REQUIRED),
    &["403", "plan-required", "This feed requires a subscription", "CME Group Lite"],
)]
#[case::a_market_storing_no_trades(
    Reply::refusal(404, TICKS_NOT_STORED),
    &["404", "data-unavailable", "Tick trades are not stored for this market"],
)]
#[case::rows_the_market_does_not_serve(
    Reply::refusal(400, DATA_UNAVAILABLE_QUOTES),
    &["400", "data-unavailable", "This market offers timeframes"],
)]
#[case::a_quota(
    Reply::refusal(429, ROWS_EXHAUSTED).retry_after(18_720),
    &["429", "rows-exhausted"],
)]
#[tokio::test]
async fn a_refusal_is_an_error_with_lse_s_code_message_and_required_plan_asked_once(
    #[case] refusal: Reply,
    #[case] parts: &[&str],
) {
    let lse = MockLse::start(vec![refusal, Reply::file(GOLD_TRADES)]);

    let err = client(&lse, 60).export(&gold_second()).await.unwrap_err();

    let err = err.to_string();
    for part in parts {
        assert!(err.contains(part), "{part}: {err}");
    }
    assert_eq!(lse.calls().len(), 1);
}

#[rstest]
#[case::no_stored_row(DATA_UNAVAILABLE)]
#[case::no_session(DATE_NOT_TRADING)]
#[tokio::test]
async fn no_stored_row_and_no_session_are_no_file(#[case] refusal: &str) {
    let lse = MockLse::start(vec![Reply::refusal(404, refusal)]);

    let file = client(&lse, 60).export(&gold_second()).await.unwrap();

    assert_eq!(file, None);
}

/// LSE sends a file as a 200 with its SHA-256 (LSE's docs, "Files"). LSE once answered an export
/// still building with a 202.
#[rstest]
#[case::a_202(Reply::file(GOLD_TRADES).with_status(202), 202)]
#[case::a_200_without_lse_s_sha_256(Reply::file(GOLD_TRADES).without_checksum(), 200)]
#[tokio::test]
async fn an_answer_other_than_a_200_beside_lse_s_sha_256_is_no_file_but_an_unexpected_answer_asked_once(
    #[case] answer: Reply,
    #[case] status: u16,
) {
    let lse = MockLse::start(vec![answer, Reply::file(GOLD_TRADES)]);

    let err = client(&lse, 60).export(&gold_second()).await.unwrap_err();

    assert!(
        matches!(err, LseHttpError::Unexpected { status: answered, .. } if answered == status),
        "{err}"
    );
    assert_eq!(lse.calls().len(), 1);
}

#[tokio::test]
async fn a_429_of_calls_is_waited_out_by_its_retry_after_then_asked_again() {
    let lse = MockLse::start(vec![
        Reply::refusal(429, CALLS_EXHAUSTED).retry_after(2),
        Reply::file(GOLD_TRADES),
    ]);

    let file = client(&lse, 6000).export(&gold_second()).await.unwrap();

    assert_eq!(file.as_deref(), Some(GOLD_TRADES));
    let [refused, asked_again] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls())
    };
    let waited = asked_again.received - refused.answered;
    assert!(waited >= Duration::from_secs(2), "{waited:?}");
}

/// LSE's docs ("Files") say to request a download that is cut again from the start.
#[tokio::test]
async fn a_file_cut_short_is_fetched_again() {
    let lse = MockLse::start(vec![Reply::cut_file(GOLD_TRADES), Reply::file(GOLD_TRADES)]);

    let file = client(&lse, 6000).export(&gold_second()).await.unwrap();

    assert_eq!(file.as_deref(), Some(GOLD_TRADES));
    assert_eq!(lse.calls().len(), 2);
}

#[tokio::test]
async fn a_file_whose_sha_256_is_not_lse_s_fails_naming_lse_s_export_asked_once() {
    let lse = MockLse::start(vec![
        Reply::corrupt_file(GOLD_TRADES),
        Reply::file(GOLD_TRADES),
    ]);

    let err = client(&lse, 6000).export(&gold_second()).await.unwrap_err();

    let LseHttpError::Checksum {
        len,
        export_id: Some(export_id),
    } = err
    else {
        panic!("{err}")
    };
    assert_eq!((len, export_id.as_str()), (GOLD_TRADES.len(), EXPORT_ID));
    assert_eq!(lse.calls().len(), 1);
}

/// 120 calls a minute allow one call every half second.
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

fn us10y_days() -> ReferenceQuery {
    ReferenceQuery::new(
        "bond_yields",
        "US10Y",
        Dates {
            from: Some("2026-11-09".parse().unwrap()),
            to: "2026-11-12".parse().unwrap(),
        },
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

/// A made-up body: valid JSON with no collections.
#[tokio::test]
async fn a_catalogue_not_as_lse_s_docs_describe_it_is_an_error_saying_so() {
    let lse = MockLse::start(vec![Reply::json(r#"{"collections": []}"#)]);

    let err = client(&lse, 60).reference_collections().await.unwrap_err();

    assert!(
        matches!(
            err,
            LseHttpError::Malformed {
                what: "reference catalogue",
                ..
            }
        ),
        "{err}"
    );
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

/// LSE's catalogue lists the two listings of ESZ6, for 2016 and for 2026.
#[tokio::test]
async fn a_lookup_asks_lse_s_catalogue_for_a_symbol_s_listings() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6)]);

    let pairs = client(&lse, 60)
        .pairs(&PairsQuery::new("cme", "ESZ6"))
        .await
        .unwrap();

    let listed: Vec<_> = pairs
        .iter()
        .map(|pair| (pair.pair_id, pair.name.as_deref()))
        .collect();
    assert_eq!(
        listed,
        [
            (4_000_000_001, Some("ES futures 2026-12")),
            (4_000_658_762, Some("ES futures 2016-12"))
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

#[rstest]
fn the_key_is_not_in_the_client_s_debug() {
    let lse = MockLse::start(vec![]);

    assert!(!format!("{:?}", client(&lse, 60)).contains(API_KEY));
}
