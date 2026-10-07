//! Fills the catalog with the history of the given instruments and bar types.
//!
//! Source-agnostic, no node: the bars come from a fetch function the caller
//! passes, backed by an adapter's historical client (`ib.rs`). Per bar type
//! it resumes after the last stored bar (`query_last_timestamp`), checks the
//! response, sorts it by `ts_init` and writes it with `write_instruments`
//! and `write_to_parquet` on a `ParquetDataCatalog` (`nautilus_persistence`).
//! Nautilus does not persist requested data on its own.

use anyhow::{Context, bail};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, NautilusDataType},
    instruments::InstrumentAny,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

/// Fills `catalog` with `instruments`, then with the bars of each bar type
/// over `[start, end]`, fetched one bar type at a time through `fetch_bars`.
///
/// Any error stops the run. An empty response or a bar of another bar type
/// is an error. Bars are stored sorted by `ts_init`, duplicates dropped.
pub async fn fill_catalog(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,
    bar_types: &[BarType],
    start: Timestamp,
    end: Timestamp,
    mut fetch_bars: impl AsyncFnMut(BarType, Timestamp, Timestamp) -> anyhow::Result<Vec<Bar>>,
) -> anyhow::Result<()> {
    catalog
        .write_instruments(instruments)
        .context("writing instruments")?;
    let end_ns = to_unix_nanos(end)?;
    for &bar_type in bar_types {
        let resume_point = catalog
            .query_last_timestamp(&NautilusDataType::Bar.into(), Some(&bar_type.to_string()))
            .with_context(|| format!("reading the last stored bar of {bar_type}"))?
            .map(UnixNanos::from);
        let from = match resume_point {
            Some(last) if last >= end_ns => continue,
            Some(last) => start.max(just_after(last)?),
            None => start,
        };
        let mut bars = fetch_bars(bar_type, from, end)
            .await
            .with_context(|| format!("fetching bars of {bar_type}"))?;
        if bars.is_empty() {
            bail!("empty response for bar type {bar_type}");
        }
        if let Some(foreign) = bars.iter().find(|bar| bar.bar_type != bar_type) {
            bail!(
                "response for bar type {bar_type} holds a bar of {}",
                foreign.bar_type
            );
        }
        if let Some(last) = resume_point {
            bars.retain(|bar| bar.ts_init > last);
        }
        bars.sort_by_key(|bar| bar.ts_init);
        bars.dedup_by_key(|bar| bar.ts_init);
        let Some(newest) = bars.last() else {
            continue;
        };
        catalog
            .write_to_parquet(&bars, None, Some(end_ns.max(newest.ts_init)), None)
            .with_context(|| format!("writing bars of {bar_type}"))?;
    }
    Ok(())
}

fn to_unix_nanos(ts: Timestamp) -> anyhow::Result<UnixNanos> {
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
        identifiers::InstrumentId,
        instruments::{Instrument, stubs::futures_contract_es},
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
        )
        .await;
        (result, asked)
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
        let (r, asked) =
            fill_window(&mut catalog, vec![instrument()], &[hour, day], 0, 200, |bt| {
                Ok(vec![bar_at(bt, 150)])
            })
            .await;
        r.unwrap();
        assert_eq!(asked, vec![(hour, 101, 200), (day, 51, 200)]);
    }

    #[tokio::test]
    async fn resume_point_before_the_window_asks_from_the_window_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bt = bar_type();
        let (r, _) = fill_window(&mut catalog, vec![instrument()], &[bt], 0, 100, |bt| {
            Ok(vec![bar_at(bt, 10)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill_window(&mut catalog, vec![instrument()], &[bt], 500, 600, |bt| {
            Ok(vec![bar_at(bt, 550)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(bt, 500, 600)]);
        assert_eq!(stored_ts(&mut catalog, bt), vec![10, 550]);
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
}
