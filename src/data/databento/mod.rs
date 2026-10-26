//! Databento history: DBN files of one instrument, loaded into the catalog
//! with Nautilus' own Databento loader (`nautilus_databento::loader`).
//!
//! An MBP-10 file becomes `OrderBookDepth` snapshots, a trades file
//! `TradeTick`s, under the instrument id the loader resolves from the
//! file's symbology (see `README.md`, "Instrument ids"). Each file is
//! stored over its request window, read from its DBN metadata, so
//! consecutive daily files cover the catalog without gaps and a file
//! already stored is not decoded again.

use std::path::{Path, PathBuf};

use anyhow::{Context, bail, ensure};
use dbn::decode::{DbnMetadata, dbn::Decoder};
use nautilus_core::UnixNanos;
use nautilus_databento::loader::DatabentoDataLoader;
use nautilus_model::{
    data::{Data, HasTsInit, OrderBookDepth, TradeTick},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog,
    catalog::types::{HasCatalogDataType, data_path_prefix},
};
use nautilus_serialization::arrow::EncodeToRecordBatch;

/// Nautilus' map of Databento publishers to venues, copied from
/// `crates/adapters/databento/publishers.json` at the pinned commit: the
/// loader reads it from a path.
const PUBLISHERS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/data/databento/publishers.json"
);

/// The most rows written to one catalog file: a day of COMEX gold MBP-10
/// is about 7 million snapshots, some 7 GB decoded at once.
const ROWS_PER_FILE: usize = 1_000_000;

/// Loads Databento DBN files into the catalog.
#[derive(Debug)]
pub struct DatabentoFiles {
    loader: DatabentoDataLoader,
    rows_per_file: usize,
}

impl DatabentoFiles {
    /// A loader with Nautilus' publishers.
    ///
    /// # Errors
    ///
    /// Returns an error if the publishers cannot be read.
    pub fn new() -> anyhow::Result<Self> {
        let loader = DatabentoDataLoader::new(Some(PathBuf::from(PUBLISHERS)))
            .context("building Nautilus' Databento loader")?;
        Ok(Self {
            loader,
            rows_per_file: ROWS_PER_FILE,
        })
    }

    /// Loads `files`, each an MBP-10 or trades DBN file of `instrument`,
    /// into `catalog`, with `instrument` written first if not stored.
    ///
    /// Each file is stored over its request window `[start, end - 1 ns]`
    /// from its metadata, in files of at most `ROWS_PER_FILE` rows. A
    /// window already stored is skipped before decoding; one stored up to
    /// a point (a load stopped midway) resumes after it.
    ///
    /// # Errors
    ///
    /// Returns an error, stopping the load, on a file of another schema, a
    /// record of another instrument or outside the file's window, or a
    /// window stored other than as a prefix. Files loaded before it stay
    /// stored.
    pub fn load(
        &mut self,
        catalog: &ParquetDataCatalog,
        instrument: InstrumentAny,
        files: &[PathBuf],
    ) -> anyhow::Result<()> {
        let id = instrument.id();
        // The loader resolves prices' precision by symbol: a record of
        // another symbol fails to decode, naming its id.
        self.loader
            .set_price_precision(id.symbol, instrument.price_precision());
        let stored = catalog
            .instruments(Some(&[id.to_string()]), None, None)
            .context("reading stored instruments")?;
        if stored.is_empty() {
            catalog
                .write_instruments(vec![instrument])
                .context("writing the instrument")?;
        }
        for path in files {
            self.load_file(catalog, id, path)
                .with_context(|| format!("loading {}", path.display()))?;
        }
        Ok(())
    }

    /// Loads the file at `path`, of the instrument `id`, by its schema.
    fn load_file(
        &self,
        catalog: &ParquetDataCatalog,
        id: InstrumentId,
        path: &Path,
    ) -> anyhow::Result<()> {
        let schema = self.loader.schema_from_file(path)?;
        match schema.as_deref() {
            Some("mbp-10") => self.store::<dbn::Mbp10Msg, OrderBookDepth>(catalog, id, path),
            Some("trades") => self.store::<dbn::TradeMsg, TradeTick>(catalog, id, path),
            other => bail!("schema {other:?} is not stored: only mbp-10 and trades are"),
        }
    }

    /// Stores the records of type `R` in the file at `path` as `T`s of the
    /// instrument `id`, over the part of the file's window not yet stored.
    fn store<R, T>(
        &self,
        catalog: &ParquetDataCatalog,
        id: InstrumentId,
        path: &Path,
    ) -> anyhow::Result<()>
    where
        R: dbn::Record + dbn::HasRType + 'static,
        T: TryFrom<Data, Error = ()> + HasTsInit + EncodeToRecordBatch + HasCatalogDataType,
    {
        let (start, end) = file_window(path)?;
        let data_type = T::catalog_data_type();
        let identifier = id.to_string();
        let named = || format!("the {} of {id}", data_path_prefix(&data_type));
        let missing = catalog
            .get_missing_intervals_for_request(
                start,
                end,
                &data_type.clone().into(),
                Some(&identifier),
            )
            .with_context(|| format!("reading the stored range of {}", named()))?;
        let from = match missing.as_slice() {
            [] => {
                log::info!("{}: {} already stored", path.display(), named());
                return Ok(());
            }
            [(from, to)] if *to == end => *from,
            _ => bail!(
                "{} are stored over parts of [{start}, {end}] other than its start \
                 (missing {missing:?})",
                named()
            ),
        };
        let mut chunk: Vec<T> = Vec::new();
        let mut chunk_start = from;
        let mut rows = 0;
        let write = |chunk: &[T], first: u64, last: u64| {
            catalog
                .write_to_parquet(
                    chunk,
                    Some(UnixNanos::from(first)),
                    Some(UnixNanos::from(last)),
                    None,
                )
                .with_context(|| format!("writing {}", named()))
        };
        for record in self
            .loader
            .read_records::<R>(path, None, None, false, None)?
        {
            let (Some(item), _) = record? else {
                continue;
            };
            let ts = item.ts_init().as_u64();
            let record_id = item.instrument_id();
            ensure!(
                record_id == id,
                "a record of {record_id} in a file loaded for {id}"
            );
            ensure!(
                (start..=end).contains(&ts),
                "a record at {ts} outside the file's window [{start}, {end}]"
            );
            if ts < from {
                continue;
            }
            // A catalog file ends 1 ns before the next one starts, and never
            // splits the records of one timestamp.
            if chunk.len() >= self.rows_per_file
                && let Some(last) = chunk.last()
                && ts > last.ts_init().as_u64()
            {
                write(&chunk, chunk_start, ts - 1)?;
                chunk.clear();
                chunk_start = ts;
            }
            let item =
                T::try_from(item).map_err(|()| anyhow::anyhow!("a record not of {}", named()))?;
            chunk.push(item);
            rows += 1;
        }
        write(&chunk, chunk_start, end)?;
        log::info!("{}: {rows} rows of {} stored", path.display(), named());
        Ok(())
    }
}

/// The request window of the DBN file at `path`, from its metadata: the
/// closed interval `[start, end - 1 ns]`, as the catalog stores intervals.
fn file_window(path: &Path) -> anyhow::Result<(u64, u64)> {
    let decoder = Decoder::from_zstd_file(path)?;
    let metadata = decoder.metadata();
    let end = metadata
        .end
        .context("the file's metadata has no end")?
        .get();
    ensure!(
        metadata.start < end,
        "the file's window [{}, {end}) is empty",
        metadata.start
    );
    Ok((metadata.start, end - 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::{identifiers::Symbol, instruments::stubs::futures_contract_es};

    /// The window of the fixtures, from their metadata: 2020-12-28 13:00 to
    /// 2020-12-29 00:00 UTC.
    const START: u64 = 1_609_160_400_000_000_000;
    const END: u64 = 1_609_200_000_000_000_000 - 1;
    /// The two snapshots of the MBP-10 fixture.
    const DEPTH_TS: [u64; 2] = [1_609_160_400_000_704_060, 1_609_160_400_000_750_544];

    fn testdata(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/data/databento/testdata")
            .join(name)
    }

    fn mbp10() -> PathBuf {
        testdata("test_data.mbp-10.dbn.zst")
    }

    fn trades() -> PathBuf {
        testdata("test_data.trades.dbn.zst")
    }

    /// The contract of Nautilus' fixtures (ESH1, prices in cents) under `id`.
    fn instrument(id: &str) -> InstrumentAny {
        let mut contract = futures_contract_es(None, None);
        contract.id = InstrumentId::from(id);
        contract.raw_symbol = Symbol::from("ESH1");
        InstrumentAny::FuturesContract(contract)
    }

    fn esh1() -> InstrumentAny {
        instrument("ESH1.GLBX")
    }

    fn new_catalog() -> (tempfile::TempDir, ParquetDataCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        (dir, catalog)
    }

    fn intervals<T: HasCatalogDataType>(catalog: &ParquetDataCatalog) -> Vec<(u64, u64)> {
        catalog
            .get_intervals(&T::catalog_data_type().into(), Some("ESH1.GLBX"))
            .unwrap()
    }

    /// Every file under the catalog, with its modification time.
    fn snapshot(dir: &Path) -> Vec<(PathBuf, std::time::SystemTime)> {
        fn walk(dir: &Path, out: &mut Vec<(PathBuf, std::time::SystemTime)>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, out);
                } else {
                    let modified = path.metadata().unwrap().modified().unwrap();
                    out.push((path, modified));
                }
            }
        }
        let mut out = Vec::new();
        walk(dir, &mut out);
        out.sort();
        out
    }

    #[test]
    fn stores_the_mbp10_snapshots_and_trades_under_the_instrument() {
        let (_dir, mut catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        databento
            .load(&catalog, esh1(), &[mbp10(), trades()])
            .unwrap();
        let ids = Some(vec!["ESH1.GLBX".to_string()]);
        let depths = catalog.order_book_depths(ids.clone(), None, None).unwrap();
        let stamps: Vec<u64> = depths.iter().map(|d| d.ts_init.as_u64()).collect();
        assert_eq!(stamps, DEPTH_TS);
        assert_eq!(depths[0].bids.len(), 10);
        assert_eq!(depths[0].bids[0].price.precision, 2);
        let trades = catalog.trade_ticks(ids, None, None).unwrap();
        let trade_ids: Vec<String> = trades.iter().map(|t| t.trade_id.to_string()).collect();
        assert_eq!(trade_ids, ["1170380", "1170414"]);
        let stored = catalog.instruments(None, None, None).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].id(), InstrumentId::from("ESH1.GLBX"));
    }

    #[test]
    fn covers_each_file_over_its_request_window() {
        let (_dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        databento
            .load(&catalog, esh1(), &[mbp10(), trades()])
            .unwrap();
        assert_eq!(intervals::<OrderBookDepth>(&catalog), [(START, END)]);
        assert_eq!(intervals::<TradeTick>(&catalog), [(START, END)]);
    }

    #[test]
    fn a_second_load_writes_nothing() {
        let (dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        let paths = [mbp10(), trades()];
        databento.load(&catalog, esh1(), &paths).unwrap();
        let before = snapshot(dir.path());
        databento.load(&catalog, esh1(), &paths).unwrap();
        assert_eq!(snapshot(dir.path()), before);
    }

    #[test]
    fn splits_a_file_into_contiguous_catalog_files() {
        let (_dir, mut catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        databento.rows_per_file = 1;
        databento.load(&catalog, esh1(), &[mbp10()]).unwrap();
        assert_eq!(
            intervals::<OrderBookDepth>(&catalog),
            [(START, DEPTH_TS[1] - 1), (DEPTH_TS[1], END)]
        );
        let depths = catalog
            .order_book_depths(Some(vec!["ESH1.GLBX".to_string()]), None, None)
            .unwrap();
        assert_eq!(depths.len(), 2);
    }

    #[test]
    fn resumes_a_file_stored_up_to_a_point() {
        let (dir, mut catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        databento.rows_per_file = 1;
        databento.load(&catalog, esh1(), &[mbp10()]).unwrap();
        // Keep only the first catalog file, as a load stopped after it.
        // Catalog files are named after their interval, so the later sorts last.
        let (second, _) = snapshot(dir.path())
            .into_iter()
            .filter(|(path, _)| path.to_string_lossy().contains("order_book_depth"))
            .max()
            .unwrap();
        std::fs::remove_file(second).unwrap();
        assert_eq!(
            intervals::<OrderBookDepth>(&catalog),
            [(START, DEPTH_TS[1] - 1)]
        );
        databento.load(&catalog, esh1(), &[mbp10()]).unwrap();
        assert_eq!(
            intervals::<OrderBookDepth>(&catalog),
            [(START, DEPTH_TS[1] - 1), (DEPTH_TS[1], END)]
        );
        let depths = catalog
            .order_book_depths(Some(vec!["ESH1.GLBX".to_string()]), None, None)
            .unwrap();
        let stamps: Vec<u64> = depths.iter().map(|d| d.ts_init.as_u64()).collect();
        assert_eq!(stamps, DEPTH_TS);
    }

    #[test]
    fn refuses_records_of_another_instrument() {
        let (_dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        let err = databento
            .load(&catalog, instrument("ESH1.XCME"), &[trades()])
            .unwrap_err();
        let err = format!("{err:#}");
        assert!(
            err.contains("ESH1.GLBX") && err.contains("ESH1.XCME"),
            "{err}"
        );
        assert!(intervals::<TradeTick>(&catalog).is_empty());
    }

    #[test]
    fn refuses_a_schema_it_does_not_store() {
        let (_dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        let mbp1 = testdata("test_data.mbp-1.dbn.zst");
        let err = databento.load(&catalog, esh1(), &[mbp1]).unwrap_err();
        let err = format!("{err:#}");
        assert!(
            err.contains("mbp-1") && err.contains("test_data.mbp-1"),
            "{err}"
        );
    }
}
