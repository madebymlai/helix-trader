//! Fills the catalog with the history of the given instruments: bars per
//! bar type, quotes per instrument.
//!
//! Source-agnostic, no node: the data come from fetch functions the caller
//! passes, backed by an adapter's historical client (`ib.rs`). Per series
//! (a bar type, or an instrument's quotes) it resumes after its stored
//! range (`get_intervals`), checks the response, sorts it by `ts_init` and
//! writes it with `write_instruments` and `write_to_parquet` on a
//! `ParquetDataCatalog` (`nautilus_persistence`). Nautilus does not persist
//! requested data on its own.

use std::{collections::HashSet, fmt::Display};

use anyhow::{Context, bail};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, HasTsInit, QuoteTick},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::HasCatalogDataType,
};
use nautilus_serialization::arrow::EncodeToRecordBatch;

/// Fills `catalog` with `instruments`, then over `[start, end]` with the
/// bars of each bar type, fetched one bar type at a time through
/// `fetch_bars`, and with the quotes of each instrument, fetched one
/// instrument at a time through `fetch_quotes`.
///
/// Instruments already in the catalog are not written again. Each series
/// (a bar type, or an instrument's quotes) is stored over the windows asked
/// for (widened to its oldest and newest item). Once stored, it resumes
/// 1 ns after the end of that range, whatever `start` (no hole between the
/// stored range and `start`): a covered window asks for nothing, and
/// returned items at or before that point are dropped. A `start` before
/// the stored range is an error, raised before fetching: extending history
/// backwards is not supported.
///
/// A fetch over `(from, to]` returns the items with `ts_init` in it. Any
/// error stops the run; series written before it stay stored. An empty
/// response on a series' first fill, or an item of another series, is an
/// error. Once a series is stored, an empty response, or one of only
/// already stored items, means nothing new: nothing is written and the
/// next series run. Items are stored sorted by `ts_init`, duplicates
/// dropped.
pub async fn fill_catalog(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,
    bar_types: &[BarType],
    start: Timestamp,
    end: Timestamp,
    fetch_bars: impl AsyncFnMut(BarType, Timestamp, Timestamp) -> anyhow::Result<Vec<Bar>>,
    fetch_quotes: impl AsyncFnMut(InstrumentId, Timestamp, Timestamp) -> anyhow::Result<Vec<QuoteTick>>,
) -> anyhow::Result<()> {
    let instrument_ids: Vec<InstrumentId> = instruments.iter().map(Instrument::id).collect();
    let ids: Vec<String> = instrument_ids.iter().map(ToString::to_string).collect();
    let stored: HashSet<InstrumentId> = catalog
        .instruments(Some(&ids), None, None)
        .context("reading stored instruments")?
        .iter()
        .map(Instrument::id)
        .collect();
    let new = instruments
        .into_iter()
        .filter(|i| !stored.contains(&i.id()))
        .collect();
    catalog
        .write_instruments(new)
        .context("writing instruments")?;
    fill_series(catalog, bar_types, start, end, fetch_bars).await?;
    fill_series(catalog, &instrument_ids, start, end, fetch_quotes).await
}

/// Data the catalog stores as series: bars per bar type, quotes per
/// instrument.
trait Series: HasTsInit + EncodeToRecordBatch + HasCatalogDataType {
    /// What tells one series from another, and names its files.
    type Key: Copy + PartialEq + Display;
    /// The data, in messages.
    const NAME: &str;

    fn key(&self) -> Self::Key;
}

impl Series for Bar {
    type Key = BarType;
    const NAME: &str = "bars";

    fn key(&self) -> BarType {
        self.bar_type
    }
}

impl Series for QuoteTick {
    type Key = InstrumentId;
    const NAME: &str = "quotes";

    fn key(&self) -> InstrumentId {
        self.instrument_id
    }
}

/// Fills the series of `keys` over `[start, end]`, one at a time, through
/// `fetch` (see `fill_catalog`).
async fn fill_series<S: Series>(
    catalog: &ParquetDataCatalog,
    keys: &[S::Key],
    start: Timestamp,
    end: Timestamp,
    mut fetch: impl AsyncFnMut(S::Key, Timestamp, Timestamp) -> anyhow::Result<Vec<S>>,
) -> anyhow::Result<()> {
    let name = S::NAME;
    let start_ns = to_unix_nanos(start)?;
    let end_ns = to_unix_nanos(end)?;
    for &key in keys {
        let intervals = catalog
            .get_intervals(&S::catalog_data_type().into(), Some(&key.to_string()))
            .with_context(|| format!("reading the stored range of the {name} of {key}"))?;
        if let (Some(&(first, _)), Some(&(_, last))) = (intervals.first(), intervals.last())
            && start_ns < UnixNanos::from(first)
        {
            bail!(
                "the {name} of {key} are stored over [{}, {}] and {start} is before it: \
                 extending history backwards is not supported; use a new catalog",
                UnixNanos::from(first),
                UnixNanos::from(last),
            );
        }
        let resume_point = intervals.last().map(|&(_, last)| UnixNanos::from(last));
        let from = match resume_point {
            Some(last) => just_after(last)?,
            None => start,
        };
        if from >= end {
            continue;
        }
        let mut items = fetch(key, from, end)
            .await
            .with_context(|| format!("fetching the {name} of {key}"))?;
        if items.is_empty() && resume_point.is_none() {
            bail!("empty response for the {name} of {key}");
        }
        if let Some(foreign) = items.iter().find(|item| item.key() != key) {
            bail!(
                "response for the {name} of {key} holds {name} of {}",
                foreign.key()
            );
        }
        if let Some(last) = resume_point {
            items.retain(|item| item.ts_init() > last);
        }
        items.sort_by_key(HasTsInit::ts_init);
        items.dedup_by_key(|item| item.ts_init());
        let (Some(oldest), Some(newest)) = (items.first(), items.last()) else {
            continue;
        };
        let covered_from = to_unix_nanos(from)?.min(oldest.ts_init());
        let covered_to = end_ns.max(newest.ts_init());
        catalog
            .write_to_parquet(&items, Some(covered_from), Some(covered_to), None)
            .with_context(|| format!("writing the {name} of {key}"))?;
    }
    Ok(())
}

pub(crate) fn to_unix_nanos(ts: Timestamp) -> anyhow::Result<UnixNanos> {
    let ns = u64::try_from(ts.as_nanosecond())
        .with_context(|| format!("{ts} is outside the range of the catalog"))?;
    Ok(UnixNanos::from(ns))
}

/// The first nanosecond after `last`.
fn just_after(last: UnixNanos) -> anyhow::Result<Timestamp> {
    Timestamp::from_nanosecond(i128::from(last.as_u64()) + 1)
        .with_context(|| format!("no timestamp after {last}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::{
        instruments::stubs::{equity_aapl, futures_contract_es},
        types::{Price, Quantity},
    };

    fn bar_type() -> BarType {
        "ESZ21.GLBX-1-HOUR-LAST-EXTERNAL".parse().unwrap()
    }

    fn bar_at(bar_type: BarType, ts: u64) -> Bar {
        let p = Price::from("100.00");
        Bar::new(
            bar_type,
            p,
            p,
            p,
            p,
            Quantity::from(1),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    }

    fn instrument() -> InstrumentAny {
        InstrumentAny::FuturesContract(futures_contract_es(None, None))
    }

    async fn fill(
        catalog: &mut ParquetDataCatalog,
        bar_types: &[BarType],
        source: impl FnMut(BarType) -> anyhow::Result<Vec<Bar>>,
    ) -> anyhow::Result<()> {
        fill_window(catalog, vec![instrument()], bar_types, 0, 1_000, source)
            .await
            .0
    }

    fn ns(ns: u64) -> Timestamp {
        Timestamp::from_nanosecond(ns.into()).unwrap()
    }

    /// The bar type and window the source was asked for, in nanoseconds.
    type Asked = (BarType, u64, u64);

    /// Fills over `[start, end]` (nanoseconds) and returns the result with
    /// the windows the source was asked for.
    async fn fill_window(
        catalog: &mut ParquetDataCatalog,
        instruments: Vec<InstrumentAny>,
        bar_types: &[BarType],
        start: u64,
        end: u64,
        mut source: impl FnMut(BarType) -> anyhow::Result<Vec<Bar>>,
    ) -> (anyhow::Result<()>, Vec<Asked>) {
        let mut asked = Vec::new();
        let result = fill_catalog(
            catalog,
            instruments,
            bar_types,
            ns(start),
            ns(end),
            async |bt, s, e| {
                asked.push((bt, as_ns(s), as_ns(e)));
                source(bt)
            },
            epoch_quote,
        )
        .await;
        (result, asked)
    }

    /// A quote of `id` stamped `ts`.
    fn quote_at(id: InstrumentId, ts: u64) -> QuoteTick {
        QuoteTick::new(
            id,
            Price::from("100.00"),
            Price::from("100.25"),
            Quantity::from(1_000_000),
            Quantity::from(1_000_000),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    }

    /// The quote source of the bar tests: a quote at the epoch, so each
    /// instrument's quotes are stored from it on in the first run and
    /// nothing is stored in later runs, whatever the window: quotes never
    /// fail a bar test.
    async fn epoch_quote(
        id: InstrumentId,
        _: Timestamp,
        _: Timestamp,
    ) -> anyhow::Result<Vec<QuoteTick>> {
        Ok(vec![quote_at(id, 0)])
    }

    /// The instrument and window the quote source was asked for, in
    /// nanoseconds.
    type AskedQuotes = (InstrumentId, u64, u64);

    /// Fills `instruments` and their quotes, no bar type, over
    /// `[start, end]` (nanoseconds) and returns the result with the windows
    /// the source was asked for.
    async fn fill_quotes(
        catalog: &mut ParquetDataCatalog,
        instruments: Vec<InstrumentAny>,
        start: u64,
        end: u64,
        mut source: impl FnMut(InstrumentId) -> anyhow::Result<Vec<QuoteTick>>,
    ) -> (anyhow::Result<()>, Vec<AskedQuotes>) {
        let mut asked = Vec::new();
        let result = fill_catalog(
            catalog,
            instruments,
            &[],
            ns(start),
            ns(end),
            async |bt, _, _| bail!("asked for the bars of {bt}"),
            async |id, s, e| {
                asked.push((id, as_ns(s), as_ns(e)));
                source(id)
            },
        )
        .await;
        (result, asked)
    }

    fn es_id() -> InstrumentId {
        instrument().id()
    }

    fn aapl() -> InstrumentAny {
        InstrumentAny::Equity(equity_aapl())
    }

    fn stored_quotes(catalog: &mut ParquetDataCatalog, id: InstrumentId) -> Vec<u64> {
        catalog
            .quote_ticks(Some(vec![id.to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|q| q.ts_init.as_u64())
            .collect()
    }

    fn as_ns(ts: Timestamp) -> u64 {
        ts.as_nanosecond().try_into().unwrap()
    }

    fn stored_ts(catalog: &mut ParquetDataCatalog, bt: BarType) -> Vec<u64> {
        catalog
            .bars(Some(vec![bt.to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|b| b.ts_init.as_u64())
            .collect()
    }

    #[tokio::test]
    async fn bars_are_written_and_read_back_per_bar_type_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let hour = bar_type();
        let day: BarType = "ESZ21.GLBX-1-DAY-LAST-EXTERNAL".parse().unwrap();
        fill(&mut catalog, &[hour, day], |bt| {
            Ok(if bt == hour {
                vec![bar_at(bt, 10), bar_at(bt, 20)]
            } else {
                vec![bar_at(bt, 5)]
            })
        })
        .await
        .unwrap();
        assert_eq!(stored_ts(&mut catalog, hour), vec![10, 20]);
        assert_eq!(stored_ts(&mut catalog, day), vec![5]);
    }

    #[tokio::test]
    async fn unsorted_and_duplicate_bars_are_stored_sorted_and_unique() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        fill(&mut catalog, &[bt], |bt| {
            Ok(vec![
                bar_at(bt, 30),
                bar_at(bt, 10),
                bar_at(bt, 30),
                bar_at(bt, 20),
            ])
        })
        .await
        .unwrap();
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn instruments_are_written_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        fill(&mut catalog, &[bt], |bt| Ok(vec![bar_at(bt, 10)]))
            .await
            .unwrap();
        let ids = vec!["ESZ21.GLBX".to_string()];
        let stored = catalog.instruments(Some(&ids), None, None).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].id(), InstrumentId::from("ESZ21.GLBX"));
    }

    #[tokio::test]
    async fn empty_response_is_an_error_naming_the_bar_type() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let err = fill(&mut catalog, &[bt], |_| Ok(vec![])).await.unwrap_err();
        assert!(err.to_string().contains(&bt.to_string()), "{err}");
    }

    #[tokio::test]
    async fn foreign_bar_type_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let other: BarType = "ESZ21.GLBX-1-DAY-LAST-EXTERNAL".parse().unwrap();
        let err = fill(&mut catalog, &[bt], |_| Ok(vec![bar_at(other, 10)]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains(&other.to_string()), "{err}");
        assert!(stored_ts(&mut catalog, bt).is_empty());
    }

    #[tokio::test]
    async fn each_bar_type_is_fetched_over_the_window_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let hour = bar_type();
        let day: BarType = "ESZ21.GLBX-1-DAY-LAST-EXTERNAL".parse().unwrap();
        let start = Timestamp::from_second(100).unwrap();
        let end = Timestamp::from_second(900).unwrap();
        let mut asked = Vec::new();
        fill_catalog(
            &mut catalog,
            vec![instrument()],
            &[hour, day],
            start,
            end,
            async |bt, s, e| {
                asked.push((bt, s, e));
                Ok(vec![bar_at(bt, 10)])
            },
            epoch_quote,
        )
        .await
        .unwrap();
        assert_eq!(asked, vec![(hour, start, end), (day, start, end)]);
    }
    #[tokio::test]
    async fn second_run_over_the_same_window_asks_for_nothing_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let bars = |bt| Ok(vec![bar_at(bt, 10), bar_at(bt, 20)]);
        let (first, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, bars).await;
        first.unwrap();
        let (second, asked) =
            fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, bars).await;
        second.unwrap();
        assert_eq!(asked, vec![]);
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 20]);
    }
    #[tokio::test]
    async fn later_end_asks_only_after_the_last_stored_bar_and_appends() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (first, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 10), bar_at(bt, 20)])
        })
        .await;
        first.unwrap();
        let (second, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 200, |bt| {
            Ok(vec![bar_at(bt, 150)])
        })
        .await;
        second.unwrap();
        assert_eq!(asked, vec![(bt, 101, 200)]);
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 20, 150]);
    }

    #[tokio::test]
    async fn resume_point_is_that_of_the_same_bar_type() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let hour = bar_type();
        let day: BarType = "ESZ21.GLBX-1-DAY-LAST-EXTERNAL".parse().unwrap();
        let bars = |bt| Ok(vec![bar_at(bt, 10)]);
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[hour], 0, 100, bars).await;
        r.unwrap();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[day], 0, 50, bars).await;
        r.unwrap();
        let (r, asked) = fill_window(
            &mut catalog,
            vec![instrument()],
            &[hour, day],
            0,
            200,
            |bt| Ok(vec![bar_at(bt, 150)]),
        )
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(hour, 101, 200), (day, 51, 200)]);
    }

    #[tokio::test]
    async fn later_start_than_the_stored_end_still_resumes_right_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 10)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 500, 600, |bt| {
            Ok(vec![bar_at(bt, 300), bar_at(bt, 550)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(bt, 101, 600)]);
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 300, 550]);
    }

    #[tokio::test]
    async fn window_ending_right_after_the_stored_end_asks_for_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 10)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 101, |bt| {
            Ok(vec![bar_at(bt, 101)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![]);
        assert_eq!(stored_ts(&mut catalog, bt), vec![10]);
    }

    #[tokio::test]
    async fn bars_at_or_before_the_resume_point_are_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 10), bar_at(bt, 20)])
        })
        .await;
        r.unwrap();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 200, |bt| {
            Ok(vec![bar_at(bt, 20), bar_at(bt, 100), bar_at(bt, 101)])
        })
        .await;
        r.unwrap();
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 20, 101]);
    }
    #[tokio::test]
    async fn response_of_only_stored_bars_is_not_an_error_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let bars = |bt| Ok(vec![bar_at(bt, 10), bar_at(bt, 20)]);
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, bars).await;
        r.unwrap();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 200, |bt| {
            Ok(vec![bar_at(bt, 20), bar_at(bt, 100)])
        })
        .await;
        r.unwrap();
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 20]);
    }
    #[tokio::test]
    async fn instruments_are_written_once_across_two_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let instrument_at = |ts: u64| {
            let mut es = futures_contract_es(None, None);
            es.ts_init = UnixNanos::from(ts);
            InstrumentAny::FuturesContract(es)
        };
        let (r, _) = fill_window(&mut catalog, vec![instrument_at(1)], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 10)])
        })
        .await;
        r.unwrap();
        let (r, _) = fill_window(&mut catalog, vec![instrument_at(2)], &[bt], 0, 200, |bt| {
            Ok(vec![bar_at(bt, 150)])
        })
        .await;
        r.unwrap();
        let ids = vec!["ESZ21.GLBX".to_string()];
        let stored = catalog.instruments(Some(&ids), None, None).unwrap();
        assert_eq!(stored.len(), 1);
    }
    #[tokio::test]
    async fn failure_on_the_second_bar_type_keeps_the_first_and_a_rerun_fetches_only_the_second() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let hour = bar_type();
        let day: BarType = "ESZ21.GLBX-1-DAY-LAST-EXTERNAL".parse().unwrap();
        let (r, _) = fill_window(
            &mut catalog,
            vec![instrument()],
            &[hour, day],
            0,
            100,
            |bt| {
                if bt == day {
                    bail!("source down")
                }
                Ok(vec![bar_at(bt, 10)])
            },
        )
        .await;
        r.unwrap_err();
        assert_eq!(stored_ts(&mut catalog, hour), vec![10]);
        let (r, asked) = fill_window(
            &mut catalog,
            vec![instrument()],
            &[hour, day],
            0,
            100,
            |bt| Ok(vec![bar_at(bt, 20)]),
        )
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(day, 0, 100)]);
        assert_eq!(stored_ts(&mut catalog, day), vec![20]);
    }

    #[tokio::test]
    async fn empty_response_on_a_resumed_window_records_nothing_and_the_next_bar_types_run() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let hour = bar_type();
        let day: BarType = "ESZ21.GLBX-1-DAY-LAST-EXTERNAL".parse().unwrap();
        let (r, _) = fill_window(
            &mut catalog,
            vec![instrument()],
            &[hour, day],
            0,
            100,
            |bt| Ok(vec![bar_at(bt, 10)]),
        )
        .await;
        r.unwrap();
        let (r, asked) = fill_window(
            &mut catalog,
            vec![instrument()],
            &[hour, day],
            0,
            200,
            |bt| {
                Ok(if bt == hour {
                    vec![]
                } else {
                    vec![bar_at(bt, 150)]
                })
            },
        )
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(hour, 101, 200), (day, 101, 200)]);
        assert_eq!(stored_ts(&mut catalog, hour), vec![10]);
        assert_eq!(stored_ts(&mut catalog, day), vec![10, 150]);
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[hour], 0, 200, |bt| {
            Ok(vec![bar_at(bt, 150)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(hour, 101, 200)]);
    }

    #[tokio::test]
    async fn bar_after_the_window_end_moves_the_resume_point_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 10), bar_at(bt, 150)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 200, |bt| {
            Ok(vec![bar_at(bt, 150), bar_at(bt, 180)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(bt, 151, 200)]);
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 150, 180]);
    }
    #[tokio::test]
    async fn earlier_start_than_stored_is_an_error_and_fetches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 100, 200, |bt| {
            Ok(vec![bar_at(bt, 150)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 50, 300, |bt| {
            Ok(vec![bar_at(bt, 60), bar_at(bt, 250)])
        })
        .await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains(&bt.to_string()), "{err}");
        assert!(err.contains("backwards"), "{err}");
        assert_eq!(asked, vec![]);
        assert_eq!(stored_ts(&mut catalog, bt), vec![150]);
    }
    #[tokio::test]
    async fn rerun_with_the_same_start_is_covered_from_the_window_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let bars = |bt| Ok(vec![bar_at(bt, 150)]);
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 100, 200, bars).await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 100, 200, bars).await;
        r.unwrap();
        assert_eq!(asked, vec![]);
    }
    #[tokio::test]
    async fn start_inside_the_stored_range_resumes_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 100, 200, |bt| {
            Ok(vec![bar_at(bt, 150)])
        })
        .await;
        r.unwrap();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 300, 400, |bt| {
            Ok(vec![bar_at(bt, 350)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 150, 500, |bt| {
            Ok(vec![bar_at(bt, 450)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(bt, 401, 500)]);
    }

    #[tokio::test]
    async fn stored_range_includes_bars_returned_before_the_window_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 100, 200, |bt| {
            Ok(vec![bar_at(bt, 50), bar_at(bt, 150)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 50, 200, |bt| {
            Ok(vec![bar_at(bt, 150)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![]);
    }

    #[tokio::test]
    async fn quotes_are_written_and_read_back_per_instrument_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let aapl_id = aapl().id();
        let (r, asked) = fill_quotes(&mut catalog, vec![instrument(), aapl()], 0, 100, |id| {
            Ok(if id == es_id() {
                vec![quote_at(id, 20), quote_at(id, 10), quote_at(id, 20)]
            } else {
                vec![quote_at(id, 5)]
            })
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 0, 100), (aapl_id, 0, 100)]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10, 20]);
        assert_eq!(stored_quotes(&mut catalog, aapl_id), vec![5]);
    }

    #[tokio::test]
    async fn quotes_are_covered_over_the_window_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (r, _) = fill_quotes(&mut catalog, vec![instrument()], 100, 200, |id| {
            Ok(vec![quote_at(id, 149)])
        })
        .await;
        r.unwrap();
        let intervals = catalog
            .get_intervals(
                &QuoteTick::catalog_data_type().into(),
                Some(&es_id().to_string()),
            )
            .unwrap();
        assert_eq!(intervals, vec![(100, 200)]);
    }

    #[tokio::test]
    async fn quotes_resume_asking_only_after_their_stored_range_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let id = es_id();
        let (r, _) = fill_quotes(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(vec![quote_at(id, 9), quote_at(id, 99)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_quotes(&mut catalog, vec![instrument()], 0, 200, |id| {
            Ok(vec![quote_at(id, 99), quote_at(id, 100), quote_at(id, 199)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(id, 101, 200)]);
        assert_eq!(stored_quotes(&mut catalog, id), vec![9, 99, 199]);
    }

    #[tokio::test]
    async fn quotes_rerun_over_the_same_window_asks_for_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let quotes = |id| Ok(vec![quote_at(id, 109), quote_at(id, 149)]);
        let (r, _) = fill_quotes(&mut catalog, vec![instrument()], 100, 200, quotes).await;
        r.unwrap();
        let (r, asked) = fill_quotes(&mut catalog, vec![instrument()], 100, 200, quotes).await;
        r.unwrap();
        assert_eq!(asked, vec![]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![109, 149]);
    }

    #[tokio::test]
    async fn empty_first_quotes_response_is_an_error_naming_the_instrument() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (r, _) = fill_quotes(&mut catalog, vec![instrument()], 0, 100, |_| Ok(vec![])).await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains("quotes of ESZ21.GLBX"), "{err}");
    }

    #[tokio::test]
    async fn empty_resumed_quotes_response_records_nothing_and_the_next_instruments_run() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let aapl_id = aapl().id();
        let both = || vec![instrument(), aapl()];
        let (r, _) = fill_quotes(&mut catalog, both(), 0, 100, |id| {
            Ok(vec![quote_at(id, 50)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_quotes(&mut catalog, both(), 0, 200, |id| {
            Ok(if id == es_id() {
                vec![]
            } else {
                vec![quote_at(id, 150)]
            })
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 101, 200), (aapl_id, 101, 200)]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![50]);
        assert_eq!(stored_quotes(&mut catalog, aapl_id), vec![50, 150]);
        let (r, asked) = fill_quotes(&mut catalog, both(), 0, 200, |id| {
            Ok(vec![quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 101, 200)]);
    }

    #[tokio::test]
    async fn a_quote_of_another_instrument_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let aapl_id = aapl().id();
        let (r, _) = fill_quotes(&mut catalog, vec![instrument()], 0, 100, |_| {
            Ok(vec![quote_at(aapl_id, 50)])
        })
        .await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains(&aapl_id.to_string()), "{err}");
        assert!(stored_quotes(&mut catalog, es_id()).is_empty());
    }

    #[tokio::test]
    async fn earlier_start_than_the_stored_quotes_is_an_error_and_fetches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (r, _) = fill_quotes(&mut catalog, vec![instrument()], 100, 200, |id| {
            Ok(vec![quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_quotes(&mut catalog, vec![instrument()], 50, 300, |id| {
            Ok(vec![quote_at(id, 250)])
        })
        .await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains("quotes of ESZ21.GLBX"), "{err}");
        assert!(err.contains("backwards"), "{err}");
        assert_eq!(asked, vec![]);
    }

    const MINUTE_NS: u64 = 60_000_000_000;

    /// A source of one quote a minute, as IB's are made: the quotes of
    /// `id` stamped 1 ns before each minute's close over `(from, to]`.
    fn minute_quotes(id: InstrumentId, from: u64, to: u64) -> Vec<QuoteTick> {
        (from / MINUTE_NS + 1..=to / MINUTE_NS + 1)
            .map(|minute| quote_at(id, minute * MINUTE_NS - 1))
            .filter(|quote| (from + 1..=to).contains(&quote.ts_init.as_u64()))
            .collect()
    }

    #[tokio::test]
    async fn minute_quotes_are_stored_every_minute_and_a_longer_window_fetches_only_the_new_minutes()
     {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let hour = 60 * MINUTE_NS;
        let mut asked = Vec::new();
        for end in [hour, 2 * hour, 2 * hour] {
            fill_catalog(
                &mut catalog,
                vec![instrument()],
                &[],
                ns(0),
                ns(end),
                async |bt, _, _| bail!("asked for the bars of {bt}"),
                async |id, from, to| {
                    asked.push((as_ns(from), as_ns(to)));
                    Ok(minute_quotes(id, as_ns(from), as_ns(to)))
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(asked, vec![(0, hour), (hour + 1, 2 * hour)]);
        let every_minute: Vec<u64> = (1..=120).map(|m| m * MINUTE_NS - 1).collect();
        assert_eq!(stored_quotes(&mut catalog, es_id()), every_minute);
    }

    #[tokio::test]
    async fn bars_and_quotes_of_an_instrument_resume_each_from_their_own_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_quotes(&mut catalog, vec![instrument()], 0, 50, |id| {
            Ok(vec![quote_at(id, 49)])
        })
        .await;
        r.unwrap();
        let (r, _) = fill_window(&mut catalog, vec![], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 100)])
        })
        .await;
        r.unwrap();
        let mut asked_bars = Vec::new();
        let mut asked_quotes = Vec::new();
        fill_catalog(
            &mut catalog,
            vec![instrument()],
            &[bt],
            ns(0),
            ns(200),
            async |bt, s, e| {
                asked_bars.push((bt, as_ns(s), as_ns(e)));
                Ok(vec![bar_at(bt, 200)])
            },
            async |id, s, e| {
                asked_quotes.push((id, as_ns(s), as_ns(e)));
                Ok(vec![quote_at(id, 199)])
            },
        )
        .await
        .unwrap();
        assert_eq!(asked_bars, vec![(bt, 101, 200)]);
        assert_eq!(asked_quotes, vec![(es_id(), 51, 200)]);
    }
}
