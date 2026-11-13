//! An export over any span, as LSE serves exports: asked a UTC day an
//! export, an export LSE refuses as too wide asked again in halves, and the
//! span LSE has not loaded yet told apart from one it holds no row of.
//!
//! LSE stores a UTC day after it ends, the morning after (LSE's API docs,
//! "The newest date": the trades, quotes and candles of a day are loaded
//! the morning after it, UTC, and its book and orders after them), and
//! answers a day it has not loaded as one it holds no row of
//! (`data-unavailable`: "the dates asked may not be loaded yet (or nothing
//! traded in them)", `test_data/`). So the current UTC day is not loaded,
//! whatever LSE answers; the day before is once LSE answers rows of it;
//! and an earlier day LSE holds no row of had none.

use std::cmp::Ordering;

use anyhow::Context;
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp};

use super::{client::LseHttpClient, query::ExportQuery};

/// A UTC day.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// What LSE holds of an export.
#[derive(Debug)]
pub(crate) struct Exported {
    /// The files LSE sent, each beside the export it is the file of, in
    /// time order.
    pub(crate) files: Vec<(ExportQuery, Bytes)>,
    /// Where the span LSE has not loaded yet starts, if the export reaches
    /// into it: from there, the files may lack rows LSE will hold.
    pub(crate) unloaded_from: Option<Timestamp>,
}

/// The files of the export `query`, asked a UTC day an export, one at a
/// time; each day LSE refuses as too wide (`export-too-wide`) is asked
/// again in halves, and each half while LSE refuses it, down to a second.
/// With them, where the span LSE has not loaded as of `now` starts, if
/// `query` reaches into it (see the module's doc).
///
/// # Errors
///
/// Returns an error naming the export that failed: LSE refused it, LSE's
/// file of it does not have the SHA-256 LSE gave for it, or LSE could not
/// be reached (see `LseHttpClient::export`).
pub(crate) async fn fetch(
    http: &LseHttpClient,
    query: &ExportQuery,
    now: Timestamp,
) -> anyhow::Result<Exported> {
    let mut files = Vec::new();
    let mut unloaded_from = None;
    for day in utc_days(query) {
        let day_files = fetch_halving(http, day.clone()).await?;
        let (from, _) = day.window();
        if unloaded_from.is_none() && !loaded(from, !day_files.is_empty(), now) {
            unloaded_from = Some(from);
        }
        files.extend(day_files);
    }
    Ok(Exported {
        files,
        unloaded_from,
    })
}

/// The files of `query`, asked again in halves while LSE refuses it as too
/// wide, in time order.
async fn fetch_halving(
    http: &LseHttpClient,
    query: ExportQuery,
) -> anyhow::Result<Vec<(ExportQuery, Bytes)>> {
    let mut files = Vec::new();
    // The exports still to ask, the next one last.
    let mut pending = vec![query];
    while let Some(export) = pending.pop() {
        let file = match http.export(&export).await {
            Err(e) if e.is_too_wide() => {
                if let Some((first, second)) = export.halves() {
                    log::info!("Asking for {export} in halves: {e}");
                    pending.extend([second, first]);
                    continue;
                }
                Err(e)
            }
            answer => answer,
        }
        .with_context(|| format!("exporting {export}"))?;
        files.extend(file.map(|file| (export, file)));
    }
    Ok(files)
}

/// `query` as the UTC days its span crosses, each over its part of the
/// span, in time order.
fn utc_days(query: &ExportQuery) -> Vec<ExportQuery> {
    let (start, end) = query.window();
    let mut days = Vec::new();
    let mut from = start;
    while from < end {
        let to = (midnight(from) + DAY).min(end);
        days.push(query.over(from, to));
        from = to;
    }
    days
}

/// Whether LSE has loaded the UTC day of `time` as of `now`, given whether
/// it answered rows of the day (see the module's doc).
fn loaded(time: Timestamp, rows: bool, now: Timestamp) -> bool {
    match midnight(time).cmp(&(midnight(now) - DAY)) {
        Ordering::Less => true,
        Ordering::Equal => rows,
        Ordering::Greater => false,
    }
}

/// The start of the UTC day of `time`.
fn midnight(time: Timestamp) -> Timestamp {
    Timestamp::from_second(time.as_second().div_euclid(86_400) * 86_400)
        .expect("a midnight of a timestamp is one")
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use nautilus_core::string::secret::SecretString;

    use super::*;
    use crate::{
        common::{credential::LseCredential, symbol::LseSymbol},
        http::client::LseHttpClient,
        testing::{
            API_KEY, DATA_UNAVAILABLE, DATA_UNAVAILABLE_TODAY, EXPORT_ID, EXPORT_TOO_WIDE,
            GOLD_TRADES, MockLse, Reply,
        },
    };

    fn client(lse: &MockLse) -> LseHttpClient {
        let credential =
            LseCredential::resolve(Some(SecretString::from(API_KEY)), |_| None).unwrap();
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
        ExportQuery::trades(&gold, at(from), at(to))
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

    /// LSE's answer cannot tell a day not loaded from one with no trading.
    #[tokio::test]
    async fn the_day_before_is_not_loaded_while_lse_answers_no_rows_of_it() {
        let lse = MockLse::start(vec![Reply::refusal(404, DATA_UNAVAILABLE_TODAY)]);
        let query = gold("2026-10-21T10:00:00Z", "2026-10-22T00:00:00Z");

        let exported = fetch(&client(&lse), &query, at(ON_THE_22ND)).await.unwrap();

        assert!(exported.files.is_empty());
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
}
