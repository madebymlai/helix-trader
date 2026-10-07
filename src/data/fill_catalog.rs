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
use nautilus_model::{
    data::{Bar, BarType},
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
    for &bar_type in bar_types {
        let mut bars = fetch_bars(bar_type, start, end)
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
        bars.sort_by_key(|bar| bar.ts_init);
        bars.dedup_by_key(|bar| bar.ts_init);
        catalog
            .write_to_parquet(&bars, None, None, None)
            .with_context(|| format!("writing bars of {bar_type}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_core::UnixNanos;
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
        mut source: impl FnMut(BarType) -> anyhow::Result<Vec<Bar>>,
    ) -> anyhow::Result<()> {
        let start = Timestamp::from_second(0).unwrap();
        let end = Timestamp::from_second(1_000).unwrap();
        fill_catalog(
            catalog,
            vec![instrument()],
            bar_types,
            start,
            end,
            async |bt, _, _| source(bt),
        )
        .await
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
}
