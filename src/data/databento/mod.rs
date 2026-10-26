//! Databento history: downloaded DBN files of `GLBX` instruments, loaded
//! into the catalog with Nautilus' own Databento loader
//! (`nautilus_databento::loader`), with the instrument's definitions from
//! Databento's historical API. Market data is never downloaded here.
//!
//! An MBP-10 file becomes `OrderBookDepth` snapshots, a trades file
//! `TradeTick`s, under the instrument id the loader resolves from the
//! file's symbology (see `README.md`, "Instrument ids"). Each file is
//! stored over its request window, read from its DBN metadata, so
//! consecutive daily files cover the catalog without gaps and a file
//! already stored is not decoded again.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail, ensure};
use dbn::{
    Metadata, Schema,
    decode::{DbnMetadata, dbn::Decoder},
};
use jiff::Timestamp;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_databento::{
    common::Credential,
    historical::{DatabentoHistoricalClient, RangeQueryParams},
    loader::DatabentoDataLoader,
};
use nautilus_model::{
    data::{Data, HasTsInit, OrderBookDepth, TradeTick},
    identifiers::{InstrumentId, Symbol, Venue},
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

    /// Whether `id` is on `GLBX`, the venue Nautilus' loader gives CME
    /// Globex records (`GLBX.MDP3`), so its history comes from here.
    #[must_use]
    pub fn serves(id: InstrumentId) -> bool {
        id.venue == Venue::GLBX()
    }

    /// Fills `catalog` from the DBN files in `dir` for `series`, each the
    /// trades of a `GLBX` instrument: every MBP-10 and trades file of the
    /// instrument's symbol whose window meets `[start, end]` is loaded
    /// whole (see `load`), after the instrument's definitions over the
    /// files' span, fetched from Databento with `api_key`.
    ///
    /// # Errors
    ///
    /// Returns an error if a series is not such trades, checked before any
    /// request; if `dir` has no file for an instrument; if the definitions
    /// are of another instrument or cannot be fetched; or as `load`.
    pub async fn fill(
        &mut self,
        catalog: &ParquetDataCatalog,
        ids: &[InstrumentId],
        dir: &Path,
        start: Timestamp,
        end: Timestamp,
        api_key: &str,
    ) -> anyhow::Result<()> {
        if let Some(id) = ids.iter().find(|&&id| !Self::serves(id)) {
            bail!("Databento files serve GLBX instruments, not {id}");
        }
        for &id in ids {
            let files = files_of(dir, id.symbol, start, end)?;
            ensure!(
                !files.is_empty(),
                "no mbp-10 or trades file of {} in {} meets [{start}, {end}]",
                id.symbol,
                dir.display()
            );
            let definitions = definitions(api_key, &files).await?;
            if let Some(other) = definitions.iter().find(|d| d.id() != id) {
                bail!("Databento defines {} for the files of {id}", other.id());
            }
            self.load(catalog, definitions, &files)?;
        }
        Ok(())
    }

    /// Loads `files`, each an MBP-10 or trades DBN file of one instrument,
    /// into `catalog`, with `definitions`, the instrument's, written first
    /// if the instrument is not stored.
    ///
    /// Each file is stored over its request window `[start, end - 1 ns]`
    /// from its metadata, in files of at most `ROWS_PER_FILE` rows. A
    /// window already stored is skipped before decoding; one stored up to
    /// a point (a load stopped midway) resumes after it.
    ///
    /// # Errors
    ///
    /// Returns an error, before writing, if `definitions` is empty or not of
    /// one instrument and price precision. Otherwise returns an error,
    /// stopping the load, on a file of another schema, a record of another
    /// instrument or outside the file's window, or a window stored other
    /// than as a prefix. Files loaded before it stay stored.
    fn load(
        &mut self,
        catalog: &ParquetDataCatalog,
        mut definitions: Vec<InstrumentAny>,
        files: &[PathBuf],
    ) -> anyhow::Result<()> {
        let first = definitions.first().context("no instrument definition")?;
        let (id, precision) = (first.id(), first.price_precision());
        if let Some(other) = definitions
            .iter()
            .find(|d| d.id() != id || d.price_precision() != precision)
        {
            bail!(
                "definitions of several instruments: {id} with precision {precision}, \
                 {} with precision {}",
                other.id(),
                other.price_precision()
            );
        }
        // The loader resolves prices' precision by symbol: a record of
        // another symbol fails to decode, naming its id.
        self.loader.set_price_precision(id.symbol, precision);
        let stored = catalog
            .instruments(Some(&[id.to_string()]), None, None)
            .context("reading stored instruments")?;
        if stored.is_empty() {
            definitions.sort_by_key(HasTsInit::ts_init);
            catalog
                .write_instruments(definitions)
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
        let metadata = metadata(path)?;
        let window = window(&metadata)?;
        match metadata.schema {
            Some(Schema::Mbp10) => {
                self.store::<dbn::Mbp10Msg, OrderBookDepth>(catalog, id, path, window)
            }
            Some(Schema::Trades) => {
                self.store::<dbn::TradeMsg, TradeTick>(catalog, id, path, window)
            }
            other => bail!(
                "schema {} is not stored: only mbp-10 and trades are",
                other.map_or("none", |schema| schema.as_str())
            ),
        }
    }

    /// Stores the records of type `R` in the file at `path` as `T`s of the
    /// instrument `id`, over the part of the file's window `[start, end]`
    /// not yet stored.
    fn store<R, T>(
        &self,
        catalog: &ParquetDataCatalog,
        id: InstrumentId,
        path: &Path,
        (start, end): (u64, u64),
    ) -> anyhow::Result<()>
    where
        R: dbn::Record + dbn::HasRType + 'static,
        T: TryFrom<Data, Error = ()> + HasTsInit + EncodeToRecordBatch + HasCatalogDataType,
    {
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

/// The definitions Databento holds for the symbol of `files` over their
/// span, from its historical API with `api_key`: the instrument under the id
/// the loader gives the files' records (for a continuous symbol,
/// `GC.v.0.GLBX`), once per definition received.
async fn definitions(api_key: &str, files: &[PathBuf]) -> anyhow::Result<Vec<InstrumentAny>> {
    let request = definitions_request(files)?;
    let client = DatabentoHistoricalClient::new(
        Credential::new(api_key),
        PathBuf::from(PUBLISHERS),
        get_atomic_clock_realtime(),
        false,
    )
    .context("building Databento's historical client")?;
    let described = format!(
        "the definitions of {:?} in {} over [{}, {})",
        request.symbols,
        request.dataset,
        request.start,
        request.end.unwrap_or_default(),
    );
    let definitions = client
        .get_range_instruments(request)
        .await
        .with_context(|| format!("requesting {described}"))?;
    log::info!("{} {described}", definitions.len());
    Ok(definitions)
}

/// The MBP-10 and trades files of `symbol` in `dir` (`*.dbn.zst`, by name)
/// whose window meets `[start, end]`, from their metadata.
fn files_of(
    dir: &Path,
    symbol: Symbol,
    start: Timestamp,
    end: Timestamp,
) -> anyhow::Result<Vec<PathBuf>> {
    let (start, end) = (
        UnixNanos::from(start).as_u64(),
        UnixNanos::from(end).as_u64(),
    );
    let mut files = Vec::new();
    let entries = fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))?;
    for entry in entries {
        let path = entry
            .with_context(|| format!("listing {}", dir.display()))?
            .path();
        if !path.to_string_lossy().ends_with(".dbn.zst") {
            continue;
        }
        let metadata = metadata(&path)?;
        let (first, last) = window(&metadata)
            .with_context(|| format!("reading the window of {}", path.display()))?;
        if matches!(metadata.schema, Some(Schema::Mbp10 | Schema::Trades))
            && metadata.symbols == [symbol.as_str()]
            && first <= end
            && last >= start
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// The request for the definitions of the symbol of `files`, all of one
/// dataset and one symbol, over their span: from the earliest start to the
/// latest end of their metadata.
fn definitions_request(files: &[PathBuf]) -> anyhow::Result<RangeQueryParams> {
    let mut request: Option<RangeQueryParams> = None;
    for path in files {
        let metadata = metadata(path)?;
        let (first, last) = window(&metadata)
            .with_context(|| format!("reading the window of {}", path.display()))?;
        let (start, end) = (UnixNanos::from(first), UnixNanos::from(last + 1));
        match &mut request {
            None => {
                request = Some(RangeQueryParams {
                    dataset: metadata.dataset,
                    symbols: metadata.symbols,
                    start,
                    end: Some(end),
                    limit: None,
                    price_precision: None,
                });
            }
            Some(request) => {
                ensure!(
                    request.dataset == metadata.dataset && request.symbols == metadata.symbols,
                    "{} holds {:?} of {}, not {:?} of {}",
                    path.display(),
                    metadata.symbols,
                    metadata.dataset,
                    request.symbols,
                    request.dataset
                );
                request.start = request.start.min(start);
                request.end = request.end.max(Some(end));
            }
        }
    }
    let request = request.context("no file")?;
    ensure!(
        request.symbols.len() == 1,
        "the files hold several symbols: {:?}",
        request.symbols
    );
    Ok(request)
}

/// The metadata of the DBN file at `path`.
fn metadata(path: &Path) -> anyhow::Result<Metadata> {
    let decoder = Decoder::from_zstd_file(path)
        .with_context(|| format!("reading the metadata of {}", path.display()))?;
    Ok(decoder.metadata().clone())
}

/// The request window of a DBN file, from its `metadata`: the closed
/// interval `[start, end - 1 ns]`, as the catalog stores intervals.
fn window(metadata: &Metadata) -> anyhow::Result<(u64, u64)> {
    let end = metadata.end.context("the metadata has no end")?.get();
    ensure!(
        metadata.start < end,
        "the window [{}, {end}) is empty",
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

    /// The ESH1 definition received at `ts`.
    fn esh1_at(ts: u64) -> InstrumentAny {
        let mut contract = futures_contract_es(None, None);
        contract.id = InstrumentId::from("ESH1.GLBX");
        contract.raw_symbol = Symbol::from("ESH1");
        contract.ts_event = UnixNanos::from(ts);
        contract.ts_init = UnixNanos::from(ts);
        InstrumentAny::FuturesContract(contract)
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
            .load(&catalog, vec![esh1()], &[mbp10(), trades()])
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
            .load(&catalog, vec![esh1()], &[mbp10(), trades()])
            .unwrap();
        assert_eq!(intervals::<OrderBookDepth>(&catalog), [(START, END)]);
        assert_eq!(intervals::<TradeTick>(&catalog), [(START, END)]);
    }

    #[test]
    fn a_second_load_writes_nothing() {
        let (dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        let paths = [mbp10(), trades()];
        databento.load(&catalog, vec![esh1()], &paths).unwrap();
        let before = snapshot(dir.path());
        databento.load(&catalog, vec![esh1()], &paths).unwrap();
        assert_eq!(snapshot(dir.path()), before);
    }

    #[test]
    fn splits_a_file_into_contiguous_catalog_files() {
        let (_dir, mut catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        databento.rows_per_file = 1;
        databento.load(&catalog, vec![esh1()], &[mbp10()]).unwrap();
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
        databento.load(&catalog, vec![esh1()], &[mbp10()]).unwrap();
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
        databento.load(&catalog, vec![esh1()], &[mbp10()]).unwrap();
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
            .load(&catalog, vec![instrument("ESH1.XCME")], &[trades()])
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
        let err = databento.load(&catalog, vec![esh1()], &[mbp1]).unwrap_err();
        let err = format!("{err:#}");
        assert!(
            err.contains("mbp-1") && err.contains("test_data.mbp-1"),
            "{err}"
        );
    }

    #[test]
    fn stores_every_definition_of_the_instrument() {
        let (_dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        let definitions = vec![esh1_at(START + 5), esh1_at(START + 1)];
        databento.load(&catalog, definitions, &[trades()]).unwrap();
        let mut stored: Vec<u64> = catalog
            .instruments(None, None, None)
            .unwrap()
            .iter()
            .map(|i| HasTsInit::ts_init(i).as_u64())
            .collect();
        stored.sort_unstable();
        assert_eq!(stored, [START + 1, START + 5]);
    }

    #[test]
    fn refuses_definitions_of_several_instruments() {
        let (_dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        let definitions = vec![esh1(), instrument("ESH1.XCME")];
        let err = databento
            .load(&catalog, definitions, &[trades()])
            .unwrap_err();
        let err = format!("{err:#}");
        assert!(
            err.contains("ESH1.GLBX") && err.contains("ESH1.XCME"),
            "{err}"
        );
        assert!(catalog.instruments(None, None, None).unwrap().is_empty());
    }

    #[test]
    fn asks_for_the_definitions_of_the_files_symbol_over_their_span() {
        let request = definitions_request(&[mbp10(), trades()]).unwrap();
        assert_eq!(request.dataset, "GLBX.MDP3");
        assert_eq!(request.symbols, ["ESH1"]);
        assert_eq!(request.start, UnixNanos::from(START));
        assert_eq!(request.end, Some(UnixNanos::from(END + 1)));
    }

    #[test]
    fn refuses_to_ask_for_files_of_several_symbols() {
        let msft = testdata("test_data.definition.equity.dbn.zst");
        let err = definitions_request(&[trades(), msft]).unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("ESH1") && err.contains("MSFT"), "{err}");
    }

    fn ns(ns: u64) -> Timestamp {
        Timestamp::from(UnixNanos::from(ns))
    }

    #[test]
    fn serves_glbx_instruments_only() {
        assert!(DatabentoFiles::serves(InstrumentId::from("GC.v.0.GLBX")));
        assert!(!DatabentoFiles::serves(InstrumentId::from("GCZ6.XCEC")));
        assert!(!DatabentoFiles::serves(InstrumentId::from(
            "BTC-USDT-SWAP.OKX"
        )));
    }

    #[test]
    fn picks_the_mbp10_and_trades_files_of_the_symbol_meeting_the_window() {
        let dir = testdata("");
        let files = files_of(&dir, Symbol::from("ESH1"), ns(END), ns(END + 10)).unwrap();
        assert_eq!(files, [mbp10(), trades()]);
        let files = files_of(&dir, Symbol::from("ESH1"), ns(0), ns(START)).unwrap();
        assert_eq!(files, [mbp10(), trades()]);
    }

    #[test]
    fn picks_no_file_outside_the_window_or_of_another_symbol() {
        let dir = testdata("");
        let after = files_of(&dir, Symbol::from("ESH1"), ns(END + 1), ns(END + 10)).unwrap();
        assert!(after.is_empty());
        let before = files_of(&dir, Symbol::from("ESH1"), ns(0), ns(START - 1)).unwrap();
        assert!(before.is_empty());
        let other = files_of(&dir, Symbol::from("ESM1"), ns(START), ns(END)).unwrap();
        assert!(other.is_empty());
    }

    #[tokio::test]
    async fn refuses_an_instrument_it_does_not_serve_before_any_request() {
        let (_dir, catalog) = new_catalog();
        let mut databento = DatabentoFiles::new().unwrap();
        let ids = [InstrumentId::from("ESH1.XCME")];
        let err = databento
            .fill(&catalog, &ids, &testdata(""), ns(START), ns(END), "unused")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ESH1.XCME"), "{err}");
    }
}
