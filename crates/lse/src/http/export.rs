//! An export over any span, as LSE serves exports: asked a UTC day an
//! export, a book or orders a UTC hour (`ExportQuery::longest_span`), an
//! export LSE refuses as too wide asked again in halves, and the span LSE
//! has not loaded yet told apart from one it holds no row of.
//!
//! LSE stores a UTC day after it ends, the morning after (LSE's API docs,
//! "The newest date": the trades, quotes and candles of a day are loaded
//! the morning after it, UTC, and its book and orders after them), and
//! answers a day it has not loaded as one it holds no row of
//! (`data-unavailable`: "the dates asked may not be loaded yet (or nothing
//! traded in them)", `test_data/`). So the current UTC day is not loaded,
//! whatever LSE answers; the day before is once LSE answers rows of it;
//! and an earlier day LSE holds no row of had none. A day's export that
//! takes the next day's first instant (see `pieces`) reaches into that
//! day too, LSE answering no rows of it apart. The data client holds the
//! dates of the rows of LSE's other sources to the same rule (`loaded`,
//! see `rows`): LSE stores them the morning after too (its catalogue, read
//! on 13 November 2026: the bond yields of the 12th, as of the 13th's 01:34
//! UTC).

use std::cmp::Ordering;

use anyhow::Context;
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp};

use super::{client::LseHttpClient, query::ExportQuery};

/// A UTC day.
const DAY: SignedDuration = SignedDuration::from_hours(24);
/// The finest step of a time, Nautilus' and LSE's.
const INSTANT: SignedDuration = SignedDuration::from_nanos(1);

/// What LSE holds of an export.
#[derive(Debug)]
pub struct Exported {
    /// The files LSE sent, each beside the export it is the file of, in
    /// time order.
    pub files: Vec<(ExportQuery, Bytes)>,
    /// Where the span LSE has not loaded yet starts, if the export reaches
    /// into it: from there, the files may lack rows LSE will hold.
    pub unloaded_from: Option<Timestamp>,
}

/// The files of the export `query`, asked a UTC day an export, or an hour
/// of a book or orders (`ExportQuery::longest_span`), one at a time; each
/// export LSE refuses as too wide (`export-too-wide`) is asked again in
/// halves, and each half while LSE refuses it, down to a second. With them,
/// where the span LSE has not loaded as of `now` starts, if `query` reaches
/// into it (see the module's doc), a day's rows being those of any of its
/// exports.
///
/// # Errors
///
/// Returns an error naming the export that failed: LSE refused it, LSE's
/// file of it does not have the SHA-256 LSE gave for it, or LSE could not
/// be reached (see `LseHttpClient::export`).
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

/// `query` as the spans of `unit` from 1970 its span crosses, UTC days or
/// hours, each over its part of the span, in time order. A span ending an
/// instant past the end of one, as a closed window of the data engine's
/// ending at a midnight is asked (`[start, end]` as `[start, end + 1 ns)`,
/// as Nautilus' Databento client asks one), has that instant asked in the
/// one before's export, LSE serving exports across midnight, rather than
/// in an export of its own.
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

/// Whether LSE has loaded the UTC day of `time` as of `now`, given whether
/// it answered rows of the day (see the module's doc): the rule of a market
/// export's days and of the dates of the rows of LSE's other sources alike.
pub fn loaded(time: Timestamp, rows: bool, now: Timestamp) -> bool {
    match midnight(time).cmp(&(midnight(now) - DAY)) {
        Ordering::Less => true,
        Ordering::Equal => rows,
        Ordering::Greater => false,
    }
}

/// The start of the UTC day of `time`.
pub fn midnight(time: Timestamp) -> Timestamp {
    floor(time, DAY)
}

/// The start of the span of `unit` from 1970 that `time` falls in.
fn floor(time: Timestamp, unit: SignedDuration) -> Timestamp {
    let (nanos, unit) = (time.as_nanosecond(), unit.as_nanos());
    Timestamp::from_nanosecond(nanos - nanos.rem_euclid(unit))
        .expect("the start of a span at or before a timestamp is one")
}
