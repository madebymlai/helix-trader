//! Tests exports against the mock LSE peer.
//!
//! They cover the days and hours asked, halving, checksums and the span not loaded.

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

fn gold(from: &str, to: &str) -> ExportQuery {
    let gold = LseSymbol::new("futures", "GOLD").unwrap();
    ExportQuery::series(&gold, Rows::TradeSlice, at(from), at(to))
}

fn gcz6(rows: Rows, from: &str, to: &str) -> ExportQuery {
    ExportQuery::listing("cme", GCZ26_PAIR_ID, rows, at(from), at(to))
}

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

fn file_spans(exported: &Exported) -> Vec<(Timestamp, Timestamp)> {
    exported
        .files
        .iter()
        .map(|(query, _)| query.window())
        .collect()
}

const LONG_AFTER: &str = "2026-11-13T16:00:00Z";

/// LSE's docs ask for a busy contract's book or orders an hour or less at a time ("The market:
/// cme").
#[rstest]
#[case::trades(
    gold("2026-10-20T23:00:00Z", "2026-10-21T01:00:00Z"),
    &[
        ("2026-10-20T23:00:00Z", "2026-10-21T00:00:00Z"),
        ("2026-10-21T00:00:00Z", "2026-10-21T01:00:00Z"),
    ],
)]
#[case::book(
    gcz6(Rows::Book, "2026-10-21T10:30:00Z", "2026-10-21T12:15:00Z"),
    &[
        ("2026-10-21T10:30:00Z", "2026-10-21T11:00:00Z"),
        ("2026-10-21T11:00:00Z", "2026-10-21T12:00:00Z"),
        ("2026-10-21T12:00:00Z", "2026-10-21T12:15:00Z"),
    ],
)]
#[case::orders(
    gcz6(Rows::Orders, "2026-10-21T10:30:00Z", "2026-10-21T12:15:00Z"),
    &[
        ("2026-10-21T10:30:00Z", "2026-10-21T11:00:00Z"),
        ("2026-10-21T11:00:00Z", "2026-10-21T12:00:00Z"),
        ("2026-10-21T12:00:00Z", "2026-10-21T12:15:00Z"),
    ],
)]
#[tokio::test]
async fn an_export_is_asked_a_utc_day_and_a_book_or_orders_a_utc_hour_at_a_time(
    #[case] query: ExportQuery,
    #[case] spans: &[(&str, &str)],
) {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES); spans.len()]);

    let exported = fetch(&client(&lse), &query, at(LONG_AFTER)).await.unwrap();

    let spans: Vec<_> = spans.iter().map(|&(from, to)| span(from, to)).collect();
    assert_eq!(asked(&lse), spans);
    assert_eq!(exported.files.len(), spans.len());
    assert_eq!(exported.unloaded_from, None);
}

/// A closed `[start, end]` that ends at a midnight, or at an hour for a book, is asked as
/// `[start, end + 1 ns)`. LSE serves an export across either.
#[rstest]
#[case::a_midnight(gold, "2026-10-20T00:00:00Z", "2026-10-21T00:00:00.000000001Z")]
#[case::a_book_s_hour(
    |from: &str, to: &str| gcz6(Rows::Book, from, to),
    "2026-10-21T10:00:00Z",
    "2026-10-21T11:00:00.000000001Z"
)]
#[tokio::test]
async fn a_span_ending_an_instant_past_a_midnight_or_a_book_s_hour_is_one_export_through_it(
    #[case] query: fn(&str, &str) -> ExportQuery,
    #[case] from: &str,
    #[case] to: &str,
) {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

    let exported = fetch(&client(&lse), &query(from, to), at(LONG_AFTER))
        .await
        .unwrap();

    assert_eq!(asked(&lse), [span(from, to)]);
    assert_eq!(exported.files.len(), 1);
    assert_eq!(exported.unloaded_from, None);
}

/// LSE refuses the four seconds and the second half of them, and holds no row in the third
/// quarter.
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

// LSE stores a UTC day the morning after it ends, so at 16:00 on 22 October the 22nd is current and
// the 21st is the day before.
const ON_THE_22ND: &str = "2026-10-22T16:00:00Z";

/// Checks where the span not loaded starts as of `now`:
/// - The current day is not loaded, whatever LSE answers.
/// - The day before is loaded once LSE answers rows of any hour of it, because its answer cannot
///   tell a day not loaded from one with no trading.
/// - An earlier day is loaded, whether or not it has rows.
///
/// Each case gives the calls, the files and where the span not loaded starts.
#[rstest]
#[case::the_current_day_whatever_lse_answers(
    vec![Reply::file(GOLD_TRADES), Reply::file(GOLD_TRADES)],
    gold("2026-10-21T10:00:00Z", "2026-10-22T01:00:00Z"),
    ON_THE_22ND,
    (2, 2, Some("2026-10-22T00:00:00Z")),
)]
#[case::the_day_before_once_lse_answers_rows_of_it(
    vec![Reply::file(GOLD_TRADES)],
    gold("2026-10-21T10:00:00Z", "2026-10-22T00:00:00Z"),
    ON_THE_22ND,
    (1, 1, None),
)]
// The 21st's export takes the 22nd's first instant, which is not loaded whatever rows the 21st has
#[case::a_day_whose_export_takes_the_current_day_s_first_instant(
    vec![Reply::file(GOLD_TRADES)],
    gold("2026-10-21T00:00:00Z", "2026-10-22T00:00:00.000000001Z"),
    ON_THE_22ND,
    (1, 1, Some("2026-10-22T00:00:00Z")),
)]
#[case::the_day_before_while_lse_answers_no_rows_of_it(
    vec![Reply::refusal(404, DATA_UNAVAILABLE_TODAY)],
    gold("2026-10-21T10:00:00Z", "2026-10-22T00:00:00Z"),
    ON_THE_22ND,
    (1, 0, Some("2026-10-21T10:00:00Z")),
)]
// The first hour holds no rows and the second holds some
#[case::the_day_before_once_lse_answers_rows_of_any_hour_of_it(
    vec![Reply::refusal(404, DATA_UNAVAILABLE), Reply::file(GOLD_TRADES)],
    gcz6(Rows::Book, "2026-10-21T10:00:00Z", "2026-10-21T12:00:00Z"),
    ON_THE_22ND,
    (2, 1, None),
)]
#[case::the_day_before_while_lse_answers_no_rows_of_any_hour_of_it(
    vec![Reply::refusal(404, DATA_UNAVAILABLE); 2],
    gcz6(Rows::Orders, "2026-10-21T10:00:00Z", "2026-10-21T12:00:00Z"),
    ON_THE_22ND,
    (2, 0, Some("2026-10-21T10:00:00Z")),
)]
#[case::a_day_before_the_day_before_rows_or_not(
    vec![Reply::refusal(404, DATA_UNAVAILABLE)],
    gold("2026-10-21T10:00:00Z", "2026-10-22T00:00:00Z"),
    "2026-10-23T00:00:00Z",
    (1, 0, None),
)]
#[tokio::test]
async fn the_span_lse_has_not_loaded_is_the_current_day_and_the_day_before_until_lse_answers_rows_of_it(
    #[case] replies: Vec<Reply>,
    #[case] query: ExportQuery,
    #[case] now: &str,
    #[case] (calls, files, unloaded_from): (usize, usize, Option<&str>),
) {
    let lse = MockLse::start(replies);

    let exported = fetch(&client(&lse), &query, at(now)).await.unwrap();

    assert_eq!(
        (
            lse.calls().len(),
            exported.files.len(),
            exported.unloaded_from
        ),
        (calls, files, unloaded_from.map(at))
    );
}
