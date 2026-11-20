//! An export over any span.
//!
//! An export is asked a UTC day, or for a book or orders a UTC hour (`ExportQuery::longest_span`).
//!
//! LSE loads a UTC day the morning after it ends (LSE's API docs, "The newest date"), and answers
//! an unloaded day as one with no rows (`data-unavailable`, `test_data/`). So:
//! - The current UTC day is not loaded, whatever LSE answers.
//! - The day before is loaded once LSE answers rows of it.
//! - An earlier day LSE holds no row of had none.
//!
//! Other sources follow the same rule (`loaded`, see `rows`): on 13 November 2026 the catalogue
//! held the bond yields of the 12th as of the 13th's 01:34 UTC.

use std::cmp::Ordering;

use anyhow::Context;
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp};

use super::{client::LseHttpClient, query::ExportQuery};

const DAY: SignedDuration = SignedDuration::from_hours(24);
const INSTANT: SignedDuration = SignedDuration::from_nanos(1);

/// What LSE holds of an export.
#[derive(Debug)]
pub struct Exported {
    /// The files LSE sent, each beside its export, in time order.
    pub files: Vec<(ExportQuery, Bytes)>,
    /// Where the unloaded span starts, if the export reaches into it: the files may lack rows from
    /// there.
    pub unloaded_from: Option<Timestamp>,
}

/// Returns the files of the export `query`, halving an export LSE refuses as too wide
/// (`export-too-wide`) down to a second.
///
/// # Errors
///
/// Returns an error if an export fails (see `LseHttpClient::export`).
pub async fn fetch(
    http: &LseHttpClient,
    query: &ExportQuery,
    now: Timestamp,
) -> anyhow::Result<Exported> {
    let mut files = Vec::new();
    let mut unloaded_from = None;
    for day in pieces(query, DAY) {
        let mut day_files = Vec::new();
        for export in pieces(&day, day.longest_span()) {
            day_files.extend(fetch_halving(http, export).await?);
        }
        let (from, to) = day.window();
        let last = to - INSTANT;
        if unloaded_from.is_none() {
            if !loaded(from, !day_files.is_empty(), now) {
                unloaded_from = Some(from);
            } else if midnight(last) != midnight(from) && !loaded(last, false, now) {
                unloaded_from = Some(last);
            }
        }
        files.extend(day_files);
    }
    Ok(Exported {
        files,
        unloaded_from,
    })
}

async fn fetch_halving(
    http: &LseHttpClient,
    query: ExportQuery,
) -> anyhow::Result<Vec<(ExportQuery, Bytes)>> {
    let mut files = Vec::new();

    // The next export is last
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

// A span ending 1 ns past a boundary (a closed window ending at midnight, asked as
// `[start, end + 1 ns)` as Nautilus' Databento client does) keeps that instant in the previous
// export, as LSE serves exports across midnight.
fn pieces(query: &ExportQuery, unit: SignedDuration) -> Vec<ExportQuery> {
    let (start, end) = query.window();
    let mut pieces = Vec::new();
    let mut from = start;
    while from < end {
        let next = floor(from, unit) + unit;
        let to = if end.duration_since(next) <= INSTANT {
            end
        } else {
            next
        };
        pieces.push(query.over(from, to));
        from = to;
    }
    pieces
}

/// Returns whether LSE has loaded the UTC day of `time` as of `now`, given its `rows` of the day.
pub fn loaded(time: Timestamp, rows: bool, now: Timestamp) -> bool {
    match midnight(time).cmp(&(midnight(now) - DAY)) {
        Ordering::Less => true,
        Ordering::Equal => rows,
        Ordering::Greater => false,
    }
}

/// Returns the start of the UTC day of `time`.
pub fn midnight(time: Timestamp) -> Timestamp {
    floor(time, DAY)
}

fn floor(time: Timestamp, unit: SignedDuration) -> Timestamp {
    let (nanos, unit) = (time.as_nanosecond(), unit.as_nanos());
    Timestamp::from_nanosecond(nanos - nanos.rem_euclid(unit))
        .expect("the start of a span at or before a timestamp is one")
}
