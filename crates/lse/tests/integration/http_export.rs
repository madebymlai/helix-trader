//! An export over any span against the mock LSE peer: the days and hours
//! it is asked in, halves of one LSE refuses as too wide, checksums, and the
//! span LSE has not loaded.

use std::num::NonZeroU32;

use jiff::Timestamp;
use lse::{
    common::{
        credential::{LseCredential, LseProduct},
        symbol::LseSymbol,
    },
    http::{
        client::LseHttpClient,
        export::{Exported, fetch},
        query::{ExportQuery, Rows},
    },
    testing::{
        API_KEY, DATA_UNAVAILABLE, DATA_UNAVAILABLE_TODAY, EXPORT_ID, EXPORT_TOO_WIDE,
        GCZ26_PAIR_ID, GOLD_TRADES, MockLse, Reply,
    },
};
use nautilus_core::string::secret::SecretString;
use rstest::rstest;

fn client(lse: &MockLse) -> LseHttpClient {
    let credential = LseCredential::resolve(
        LseProduct::Global,
        Some(SecretString::from(API_KEY)),
        |_| None,
    )
    .unwrap();
    LseHttpClient::new(
        lse.url().to_string(),
        &credential,
        NonZeroU32::new(6000).unwrap(),
    )
    .unwrap()
}

fn at(s: &str) -> Timestamp {
    s.parse().unwrap()
}

/// GOLD's trades in `futures` over `[from, to)`.
fn gold(from: &str, to: &str) -> ExportQuery {
    let gold = LseSymbol::new("futures", "GOLD").unwrap();
    ExportQuery::series(&gold, Rows::TradeSlice, at(from), at(to))
}

/// The spans LSE was asked for, in order.
fn asked(lse: &MockLse) -> Vec<(String, String)> {
    lse.calls()
        .iter()
        .map(|call| {
            (
                call.param("from").unwrap().to_string(),
                call.param("to").unwrap().to_string(),
            )
        })
        .collect()
}

fn span(from: &str, to: &str) -> (String, String) {
    (from.to_string(), to.to_string())
}

/// The spans of the files, in order.
fn file_spans(exported: &Exported) -> Vec<(Timestamp, Timestamp)> {
    exported
        .files
        .iter()
        .map(|(query, _)| query.window())
        .collect()
}

/// A day long past when asked.
const LONG_AFTER: &str = "2026-11-13T16:00:00Z";

#[tokio::test]
async fn an_export_is_asked_a_utc_day_at_a_time() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES), Reply::file(GOLD_TRADES)]);
    let query = gold("2026-10-20T23:00:00Z", "2026-10-21T01:00:00Z");

    let exported = fetch(&client(&lse), &query, at(LONG_AFTER)).await.unwrap();

    assert_eq!(
        asked(&lse),
        [
            span("2026-10-20T23:00:00Z", "2026-10-21T00:00:00Z"),
            span("2026-10-21T00:00:00Z", "2026-10-21T01:00:00Z"),
        ]
    );
    assert_eq!(exported.files.len(), 2);
}

/// A closed window ending at a midnight, `[start, end]` asked as
/// `[start, end + 1 ns)`: the midnight's instant is asked in the day's
/// export, LSE serving an export across midnight.
#[tokio::test]
async fn a_span_ending_an_instant_past_a_midnight_is_one_export_through_it() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let query = gold("2026-10-20T00:00:00Z", "2026-10-21T00:00:00.000000001Z");

    let exported = fetch(&client(&lse), &query, at(LONG_AFTER)).await.unwrap();

    assert_eq!(
        asked(&lse),
        [span(
            "2026-10-20T00:00:00Z",
            "2026-10-21T00:00:00.000000001Z"
        )]
    );
    assert_eq!(exported.files.len(), 1);
    assert_eq!(exported.unloaded_from, None);
}

/// LSE refuses the four seconds and their second half; the first half
/// and the two quarters are served, the third quarter holding no row.
#[tokio::test]
async fn an_export_lse_refuses_as_too_wide_is_asked_again_in_halves_until_each_is_served() {
    let lse = MockLse::start(vec![
        Reply::refusal(413, EXPORT_TOO_WIDE),
        Reply::file(GOLD_TRADES),
        Reply::refusal(413, EXPORT_TOO_WIDE),
        Reply::refusal(404, DATA_UNAVAILABLE),
        Reply::file(GOLD_TRADES),
    ]);
    let query = gold("2026-10-21T10:00:00Z", "2026-10-21T10:00:04Z");

    let exported = fetch(&client(&lse), &query, at(LONG_AFTER)).await.unwrap();

    assert_eq!(
        asked(&lse),
        [
            span("2026-10-21T10:00:00Z", "2026-10-21T10:00:04Z"),
            span("2026-10-21T10:00:00Z", "2026-10-21T10:00:02Z"),
            span("2026-10-21T10:00:02Z", "2026-10-21T10:00:04Z"),
            span("2026-10-21T10:00:02Z", "2026-10-21T10:00:03Z"),
            span("2026-10-21T10:00:03Z", "2026-10-21T10:00:04Z"),
        ]
    );
    assert_eq!(
        file_spans(&exported),
        [
            (at("2026-10-21T10:00:00Z"), at("2026-10-21T10:00:02Z")),
            (at("2026-10-21T10:00:03Z"), at("2026-10-21T10:00:04Z")),
        ]
    );
    assert_eq!(exported.unloaded_from, None);
}

#[tokio::test]
async fn a_second_lse_still_refuses_as_too_wide_fails_with_its_refusal_naming_it() {
    let lse = MockLse::start(vec![Reply::refusal(413, EXPORT_TOO_WIDE)]);
    let query = gold("2026-10-21T10:00:00Z", "2026-10-21T10:00:01Z");

    let err = fetch(&client(&lse), &query, at(LONG_AFTER))
        .await
        .unwrap_err();

    let err = format!("{err:#}");
    for part in [
        "export-too-wide",
        "2,500,000 rows an export",
        "GOLD",
        "2026-10-21T10:00:00Z, 2026-10-21T10:00:01Z",
    ] {
        assert!(err.contains(part), "{part}: {err}");
    }
    assert_eq!(lse.calls().len(), 1);
}

#[tokio::test]
async fn a_file_whose_sha_256_is_not_lse_s_fails_naming_the_export() {
    let lse = MockLse::start(vec![Reply::corrupt_file(GOLD_TRADES)]);
    let query = gold("2026-10-21T10:00:00Z", "2026-10-21T10:00:01Z");

    let err = fetch(&client(&lse), &query, at(LONG_AFTER))
        .await
        .unwrap_err();

    let err = format!("{err:#}");
    for part in [
        "GOLD",
        "2026-10-21T10:00:00Z, 2026-10-21T10:00:01Z",
        EXPORT_ID,
    ] {
        assert!(err.contains(part), "{part}: {err}");
    }
}

// LSE stores a UTC day after it ends, the morning after. Asked at 16:00
// on 22 October, the 22nd is the current day and the 21st the day
// before.
const ON_THE_22ND: &str = "2026-10-22T16:00:00Z";

#[tokio::test]
async fn the_current_day_is_not_loaded_whatever_lse_answers() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES), Reply::file(GOLD_TRADES)]);
    let query = gold("2026-10-21T10:00:00Z", "2026-10-22T01:00:00Z");

    let exported = fetch(&client(&lse), &query, at(ON_THE_22ND)).await.unwrap();

    assert_eq!(exported.files.len(), 2);
    assert_eq!(exported.unloaded_from, Some(at("2026-10-22T00:00:00Z")));
}

#[tokio::test]
async fn the_day_before_is_loaded_once_lse_answers_rows_of_it() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let query = gold("2026-10-21T10:00:00Z", "2026-10-22T00:00:00Z");

    let exported = fetch(&client(&lse), &query, at(ON_THE_22ND)).await.unwrap();

    assert_eq!(exported.files.len(), 1);
    assert_eq!(exported.unloaded_from, None);
}

/// The 21st's export takes the 22nd's first instant, which LSE has not
/// loaded, rows of the 21st or not.
#[tokio::test]
async fn a_day_whose_export_takes_the_current_day_s_first_instant_reaches_into_it() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let query = gold("2026-10-21T00:00:00Z", "2026-10-22T00:00:00.000000001Z");

    let exported = fetch(&client(&lse), &query, at(ON_THE_22ND)).await.unwrap();

    assert_eq!(lse.calls().len(), 1);
    assert_eq!(exported.unloaded_from, Some(at("2026-10-22T00:00:00Z")));
}

/// LSE's answer cannot tell a day not loaded from one with no trading.
#[tokio::test]
async fn the_day_before_is_not_loaded_while_lse_answers_no_rows_of_it() {
    let lse = MockLse::start(vec![Reply::refusal(404, DATA_UNAVAILABLE_TODAY)]);
    let query = gold("2026-10-21T10:00:00Z", "2026-10-22T00:00:00Z");

    let exported = fetch(&client(&lse), &query, at(ON_THE_22ND)).await.unwrap();

    assert!(exported.files.is_empty());
    assert_eq!(exported.unloaded_from, Some(at("2026-10-21T10:00:00Z")));
}

/// GCZ6's `rows` over `[from, to)`, by its listing.
fn gcz6(rows: Rows, from: &str, to: &str) -> ExportQuery {
    ExportQuery::listing("cme", GCZ26_PAIR_ID, rows, at(from), at(to))
}

/// LSE's docs ask for an hour or less of a busy contract's book or
/// orders at a time ("The market: cme").
#[rstest]
#[case::book(Rows::Book)]
#[case::orders(Rows::Orders)]
#[tokio::test]
async fn a_book_or_orders_export_is_asked_a_utc_hour_at_a_time(#[case] rows: Rows) {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES); 3]);
    let query = gcz6(rows, "2026-10-21T10:30:00Z", "2026-10-21T12:15:00Z");

    let exported = fetch(&client(&lse), &query, at(LONG_AFTER)).await.unwrap();

    assert_eq!(
        asked(&lse),
        [
            span("2026-10-21T10:30:00Z", "2026-10-21T11:00:00Z"),
            span("2026-10-21T11:00:00Z", "2026-10-21T12:00:00Z"),
            span("2026-10-21T12:00:00Z", "2026-10-21T12:15:00Z"),
        ]
    );
    assert_eq!(exported.files.len(), 3);
}

/// A closed window ending at an hour, `[start, end]` asked as `[start,
/// end + 1 ns)`: the hour's instant is asked in the hour before's
/// export.
#[tokio::test]
async fn a_book_span_ending_an_instant_past_an_hour_is_one_export_through_it() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let query = gcz6(
        Rows::Book,
        "2026-10-21T10:00:00Z",
        "2026-10-21T11:00:00.000000001Z",
    );

    fetch(&client(&lse), &query, at(LONG_AFTER)).await.unwrap();

    assert_eq!(
        asked(&lse),
        [span(
            "2026-10-21T10:00:00Z",
            "2026-10-21T11:00:00.000000001Z"
        )]
    );
}

/// The day before is loaded once LSE answers rows of it, in any of its
/// hours: the first hour asked holds none, the second some.
#[tokio::test]
async fn the_day_before_is_loaded_once_lse_answers_rows_of_any_hour_of_it() {
    let lse = MockLse::start(vec![
        Reply::refusal(404, DATA_UNAVAILABLE),
        Reply::file(GOLD_TRADES),
    ]);
    let query = gcz6(Rows::Book, "2026-10-21T10:00:00Z", "2026-10-21T12:00:00Z");

    let exported = fetch(&client(&lse), &query, at(ON_THE_22ND)).await.unwrap();

    assert_eq!(lse.calls().len(), 2);
    assert_eq!(exported.unloaded_from, None);
}

#[tokio::test]
async fn the_day_before_is_not_loaded_while_lse_answers_no_rows_of_any_hour_of_it() {
    let lse = MockLse::start(vec![Reply::refusal(404, DATA_UNAVAILABLE); 2]);
    let query = gcz6(Rows::Orders, "2026-10-21T10:00:00Z", "2026-10-21T12:00:00Z");

    let exported = fetch(&client(&lse), &query, at(ON_THE_22ND)).await.unwrap();

    assert_eq!(exported.unloaded_from, Some(at("2026-10-21T10:00:00Z")));
}

#[tokio::test]
async fn a_day_before_the_day_before_is_loaded_rows_or_not() {
    let lse = MockLse::start(vec![Reply::refusal(404, DATA_UNAVAILABLE)]);
    let query = gold("2026-10-21T10:00:00Z", "2026-10-22T00:00:00Z");

    let exported = fetch(&client(&lse), &query, at("2026-10-23T00:00:00Z"))
        .await
        .unwrap();

    assert!(exported.files.is_empty());
    assert_eq!(exported.unloaded_from, None);
}
