//! Databento history of `GLBX` instruments (CME Globex, `GLBX.MDP3`),
//! fetched with Nautilus' historical client (`DatabentoHistoricalClient`)
//! and stored in the catalog: MBP-10 as `OrderBookDepth` snapshots, trades
//! as `TradeTick`s, with the instrument's definitions.
//!
//! Each series resumes after what the catalog holds and is fetched one UTC
//! day per request, so a day of MBP-10 (about 7 million snapshots) is the
//! most held at once. Databento's quote for the missing days is logged
//! before fetching them. See `README.md`.

use std::path::PathBuf;

use anyhow::{Context, bail, ensure};
use databento::{dbn::Schema, historical::metadata::GetCostParams};
use jiff::{Timestamp, tz::TimeZone};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_databento::{
    common::{Credential, get_date_time_range},
    historical::{DatabentoHistoricalClient, RangeQueryParams},
    symbology::infer_symbology_type,
};
use nautilus_model::{
    data::{HasTsInit, OrderBookDepth, TradeTick},
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog,
    catalog::types::{HasCatalogDataType, data_path_prefix},
};
use nautilus_serialization::arrow::EncodeToRecordBatch;

/// Nautilus' map of Databento publishers to venues, copied from
/// `crates/adapters/databento/publishers.json` at the pinned commit: the
/// client reads it from a path.
const PUBLISHERS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/data/databento/publishers.json"
);

/// The Databento dataset of `GLBX` instruments.
const DATASET: &str = "GLBX.MDP3";

/// The most rows written to one catalog file: a day of COMEX gold MBP-10
/// is about 7 million snapshots.
const ROWS_PER_FILE: usize = 1_000_000;

/// The book depth of Databento's MBP-10 schema.
const DEPTH: usize = 10;

/// A closed interval of Unix nanoseconds, as the catalog stores intervals.
type Window = (u64, u64);

/// Databento's historical API, for the history of `GLBX` instruments.
#[derive(Debug)]
pub struct DatabentoHistory {
    client: DatabentoHistoricalClient,
    /// Databento's own client, for the metadata Nautilus' does not give:
    /// cost quotes and the dataset's end.
    metadata: databento::HistoricalClient,
}

impl DatabentoHistory {
    /// The clients, with `api_key`.
    ///
    /// # Errors
    ///
    /// Returns an error if a client cannot be built (a malformed key).
    pub fn new(api_key: &str) -> anyhow::Result<Self> {
        let client = DatabentoHistoricalClient::new(
            Credential::new(api_key),
            PathBuf::from(PUBLISHERS),
            get_atomic_clock_realtime(),
            false,
        )
        .context("building Nautilus' Databento historical client")?;
        let metadata = databento::HistoricalClient::builder()
            .key(api_key)
            .context("building Databento's client")?
            .build()
            .context("building Databento's client")?;
        Ok(Self { client, metadata })
    }

    /// Whether `id` is on `GLBX`, the venue Nautilus gives CME Globex
    /// records (`publishers.json`), so its history comes from here.
    #[must_use]
    pub fn serves(id: InstrumentId) -> bool {
        id.venue == Venue::GLBX()
    }

    /// Fills `catalog` over `[start, end]` with the MBP-10 snapshots and
    /// trades of `ids`, `GLBX` instruments, and their definitions.
    ///
    /// Each series resumes 1 ns after its stored range, whatever `start`; a
    /// covered window asks for nothing, not even a quote. Otherwise
    /// Databento's quote for the missing days is logged, the definitions
    /// over them later than any stored are written, and each day is one
    /// request, stored over the day. A day with no record (a Saturday, CME
    /// being closed) writes nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument is not on `GLBX`, checked before
    /// any request; if `start` is before a series' stored range (extending
    /// history backwards is not supported); if a request fails; or if a
    /// response holds another instrument or a record outside its day. Days
    /// stored before the error stay stored.
    pub async fn fill(
        &self,
        catalog: &ParquetDataCatalog,
        ids: &[InstrumentId],
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<()> {
        if let Some(id) = ids.iter().find(|&&id| !Self::serves(id)) {
            bail!("Databento serves GLBX instruments here, not {id}");
        }
        let (start, end) = (nanos(start), nanos(end));
        for &id in ids {
            self.fill_instrument(catalog, id, start, end).await?;
        }
        Ok(())
    }

    /// Fills `catalog` with the series of `id` over `[start, end]`.
    async fn fill_instrument(
        &self,
        catalog: &ParquetDataCatalog,
        id: InstrumentId,
        start: u64,
        end: u64,
    ) -> anyhow::Result<()> {
        let depth = missing_window::<OrderBookDepth>(catalog, id, start, end)?;
        let trades = missing_window::<TradeTick>(catalog, id, start, end)?;
        let from = match (depth, trades) {
            (None, None) => {
                log::info!(
                    "{id}: MBP-10 and trades stored up to {}, nothing to fetch",
                    at(end)
                );
                return Ok(());
            }
            (Some((a, _)), Some((b, _))) => a.min(b),
            (Some((from, _)), None) | (None, Some((from, _))) => from,
        };
        let available = self
            .metadata
            .clone()
            .metadata()
            .get_dataset_range(DATASET)
            .await
            .with_context(|| format!("requesting the range of {DATASET}"))?;
        ensure_available(end, nanos_of(available.end.unix_timestamp_nanos())?)?;
        for (schema, window) in [(Schema::Mbp10, depth), (Schema::Trades, trades)] {
            if let Some(window) = window {
                self.log_cost(id, schema, window).await?;
            }
        }
        self.store_definitions(catalog, id, (from, end)).await?;
        for day in depth.map(days).unwrap_or_default() {
            let depths = self
                .client
                .get_range_order_book_depth(request(id, day), Some(DEPTH))
                .await
                .with_context(|| format!("requesting the MBP-10 of {id} over {day:?}"))?;
            write_day(catalog, id, day, &depths, ROWS_PER_FILE)?;
        }
        for day in trades.map(days).unwrap_or_default() {
            let trades = self
                .client
                .get_range_trades(request(id, day), None)
                .await
                .with_context(|| format!("requesting the trades of {id} over {day:?}"))?;
            write_day(catalog, id, day, &trades, ROWS_PER_FILE)?;
        }
        Ok(())
    }

    /// Logs Databento's quote for `schema` of `id` over `window`.
    async fn log_cost(
        &self,
        id: InstrumentId,
        schema: Schema,
        window: Window,
    ) -> anyhow::Result<()> {
        let symbol = id.symbol.to_string();
        let params = GetCostParams::builder()
            .dataset(DATASET)
            .symbols(vec![symbol.as_str()])
            .schema(schema)
            .stype_in(infer_symbology_type(&symbol))
            .date_time_range(range(window)?)
            .build();
        let cost = self
            .metadata
            .clone()
            .metadata()
            .get_cost(&params)
            .await
            .with_context(|| format!("quoting the {schema} of {id} over {window:?}"))?;
        log::info!(
            "{id}: Databento quotes ${cost:.2} for {schema} over [{}, {}] ({} days)",
            at(window.0),
            at(window.1),
            days(window).len(),
        );
        Ok(())
    }

    /// Writes the definitions of `id` received over `window` that are later
    /// than any stored.
    async fn store_definitions(
        &self,
        catalog: &ParquetDataCatalog,
        id: InstrumentId,
        window: Window,
    ) -> anyhow::Result<()> {
        // Also caches the symbol's price precision for the data requests.
        let definitions = self
            .client
            .get_range_instruments(request(id, window))
            .await
            .with_context(|| format!("requesting the definitions of {id}"))?;
        if let Some(other) = definitions.iter().find(|d| d.id() != id) {
            bail!("Databento defines {} for {id}", other.id());
        }
        let stored = catalog
            .instruments(Some(&[id.to_string()]), None, None)
            .context("reading stored instruments")?;
        let latest = stored.iter().map(HasTsInit::ts_init).max();
        let mut new: Vec<InstrumentAny> = definitions
            .into_iter()
            .filter(|d| latest.is_none_or(|latest| HasTsInit::ts_init(d) > latest))
            .collect();
        new.sort_by_key(HasTsInit::ts_init);
        log::info!("{id}: {} new definitions", new.len());
        catalog
            .write_instruments(new)
            .context("writing the definitions")?;
        Ok(())
    }
}

/// Refuses an `end` Databento has no data up to yet: a request over it
/// would fail.
fn ensure_available(end: u64, available_end: u64) -> anyhow::Result<()> {
    ensure!(
        end < available_end,
        "{DATASET} has data up to {} only, not up to {}",
        at(available_end),
        at(end)
    );
    Ok(())
}

/// Signed Unix nanoseconds `ns` as a `u64`.
fn nanos_of(ns: i128) -> anyhow::Result<u64> {
    u64::try_from(ns).with_context(|| format!("{ns} ns is before 1970"))
}

/// The part of `[start, end]` to fetch for the series of type `T` of `id`,
/// after its stored range in `catalog` (see `resume`).
fn missing_window<T: HasCatalogDataType>(
    catalog: &ParquetDataCatalog,
    id: InstrumentId,
    start: u64,
    end: u64,
) -> anyhow::Result<Option<Window>> {
    let data_type = T::catalog_data_type();
    let identifier = id.to_string();
    let named = || format!("the {} of {id}", data_path_prefix(&data_type));
    let catalog_type = data_type.clone().into();
    let reading = || format!("reading the stored range of {}", named());
    let first = catalog
        .query_first_timestamp(&catalog_type, Some(&identifier))
        .with_context(reading)?;
    let last = catalog
        .query_last_timestamp(&catalog_type, Some(&identifier))
        .with_context(reading)?;
    resume(first.zip(last), start, end).with_context(named)
}

/// The window to fetch for a series stored over `stored` (if at all), for
/// `[start, end]`: from 1 ns after the stored range (or `start` if none) to
/// `end`, none when that starts at or after `end`. A `start` before the
/// stored range is an error: extending history backwards is not supported.
fn resume(stored: Option<Window>, start: u64, end: u64) -> anyhow::Result<Option<Window>> {
    let from = match stored {
        Some((first, last)) => {
            ensure!(
                start >= first,
                "stored over [{}, {}] and {} is before it: extending history backwards is \
                 not supported; use a new catalog",
                at(first),
                at(last),
                at(start),
            );
            last + 1
        }
        None => start,
    };
    Ok((from < end).then_some((from, end)))
}

/// `window` cut at each UTC midnight: one window per day it touches.
fn days((from, to): Window) -> Vec<Window> {
    let mut days = Vec::new();
    let mut day_start = from;
    while day_start <= to {
        let next_midnight = nanos(
            Timestamp::from(UnixNanos::from(day_start))
                .to_zoned(TimeZone::UTC)
                .date()
                .tomorrow()
                .expect("a day after any UnixNanos")
                .to_zoned(TimeZone::UTC)
                .expect("UTC midnight exists")
                .timestamp(),
        );
        let day_end = to.min(next_midnight - 1);
        days.push((day_start, day_end));
        day_start = day_end + 1;
    }
    days
}

/// The request of `id`'s symbol in `GLBX.MDP3` over `window`, with the
/// exclusive end Databento takes.
fn request(id: InstrumentId, (from, to): Window) -> RangeQueryParams {
    RangeQueryParams {
        dataset: DATASET.to_string(),
        symbols: vec![id.symbol.to_string()],
        start: UnixNanos::from(from),
        end: Some(UnixNanos::from(to + 1)),
        limit: None,
        price_precision: None,
    }
}

/// `window` as Databento's date-time range, with an exclusive end.
fn range((from, to): Window) -> anyhow::Result<databento::historical::DateTimeRange> {
    get_date_time_range(UnixNanos::from(from), UnixNanos::from(to + 1))
}

/// Writes `items`, the records of `id` over `day` sorted by `ts_init`,
/// stored over `day` in catalog files of at most `rows_per_file` rows, each
/// ending 1 ns before the next starts and never splitting the records of
/// one timestamp. No item writes nothing.
fn write_day<T>(
    catalog: &ParquetDataCatalog,
    id: InstrumentId,
    (from, to): Window,
    items: &[T],
    rows_per_file: usize,
) -> anyhow::Result<()>
where
    T: HasTsInit + EncodeToRecordBatch + HasCatalogDataType + HasInstrumentId,
{
    let named = || format!("the {} of {id}", data_path_prefix(&T::catalog_data_type()));
    if let Some(other) = items.iter().find(|item| item.instrument_id() != id) {
        bail!("{} holds a record of {}", named(), other.instrument_id());
    }
    if let Some(outside) = items
        .iter()
        .map(|item| item.ts_init().as_u64())
        .find(|ts| !(from..=to).contains(ts))
    {
        bail!(
            "{} holds a record at {} outside [{}, {}]",
            named(),
            at(outside),
            at(from),
            at(to)
        );
    }
    let mut chunk_start = 0;
    let mut file_start = from;
    for at in 1..items.len() {
        let ts = items[at].ts_init();
        if at - chunk_start >= rows_per_file && ts > items[at - 1].ts_init() {
            write_file(
                catalog,
                &items[chunk_start..at],
                (file_start, ts.as_u64() - 1),
            )
            .with_context(|| format!("writing {}", named()))?;
            chunk_start = at;
            file_start = ts.as_u64();
        }
    }
    if chunk_start < items.len() {
        write_file(catalog, &items[chunk_start..], (file_start, to))
            .with_context(|| format!("writing {}", named()))?;
    }
    log::info!(
        "{}: {} rows over [{}, {}]",
        named(),
        items.len(),
        at(from),
        at(to)
    );
    Ok(())
}

/// Writes `items` as one catalog file over `window`.
fn write_file<T>(
    catalog: &ParquetDataCatalog,
    items: &[T],
    (from, to): Window,
) -> anyhow::Result<()>
where
    T: HasTsInit + EncodeToRecordBatch + HasCatalogDataType,
{
    catalog.write_to_parquet(
        items,
        Some(UnixNanos::from(from)),
        Some(UnixNanos::from(to)),
        None,
    )?;
    Ok(())
}

/// The instrument a record is of.
trait HasInstrumentId {
    fn instrument_id(&self) -> InstrumentId;
}

impl HasInstrumentId for OrderBookDepth {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl HasInstrumentId for TradeTick {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

/// `ts` in Unix nanoseconds.
fn nanos(ts: Timestamp) -> u64 {
    UnixNanos::from(ts).as_u64()
}

/// Unix nanoseconds `ns` as a time, for messages.
fn at(ns: u64) -> Timestamp {
    Timestamp::from(UnixNanos::from(ns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::{
        enums::AggressorSide,
        identifiers::TradeId,
        types::{Price, Quantity},
    };

    /// 2026-10-26T00:00:00Z.
    const MONDAY: u64 = 1_792_972_800_000_000_000;
    const DAY: u64 = 86_400_000_000_000;

    fn gc() -> InstrumentId {
        InstrumentId::from("GC.v.0.GLBX")
    }

    fn trade(id: InstrumentId, ts: u64, n: u32) -> TradeTick {
        TradeTick::new(
            id,
            Price::from("4294.2"),
            Quantity::from(1),
            AggressorSide::Sell,
            TradeId::new(n.to_string()),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    }

    fn new_catalog() -> (tempfile::TempDir, ParquetDataCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        (dir, catalog)
    }

    fn trade_intervals(catalog: &ParquetDataCatalog) -> Vec<Window> {
        catalog
            .get_intervals(&TradeTick::catalog_data_type().into(), Some("GC.v.0.GLBX"))
            .unwrap()
    }

    #[test]
    fn serves_glbx_instruments_only() {
        assert!(DatabentoHistory::serves(gc()));
        assert!(!DatabentoHistory::serves(InstrumentId::from("GCZ6.XCEC")));
        assert!(!DatabentoHistory::serves(InstrumentId::from(
            "BTC-USDT-SWAP.OKX"
        )));
    }

    #[test]
    fn a_series_not_stored_is_fetched_over_the_whole_window() {
        assert_eq!(resume(None, 10, 20).unwrap(), Some((10, 20)));
    }

    #[test]
    fn a_stored_series_resumes_after_its_range_whatever_the_start() {
        assert_eq!(resume(Some((10, 14)), 12, 20).unwrap(), Some((15, 20)));
        assert_eq!(resume(Some((10, 14)), 10, 20).unwrap(), Some((15, 20)));
    }

    #[test]
    fn a_covered_window_fetches_nothing() {
        // As the four loaded weeks: stored to 1 ns before the end.
        assert_eq!(resume(Some((10, 19)), 10, 20).unwrap(), None);
        assert_eq!(resume(Some((10, 30)), 10, 20).unwrap(), None);
    }

    #[test]
    fn a_start_before_the_stored_range_is_refused() {
        let err = format!("{:#}", resume(Some((10, 14)), 9, 20).unwrap_err());
        assert!(err.contains("backwards"), "{err}");
    }

    #[test]
    fn an_end_databento_has_no_data_up_to_is_refused() {
        assert!(ensure_available(MONDAY - 1, MONDAY).is_ok());
        let err = ensure_available(MONDAY, MONDAY).unwrap_err().to_string();
        assert!(err.contains("2026-10-26"), "{err}");
    }

    #[test]
    fn a_window_is_cut_at_each_utc_midnight() {
        let from = MONDAY + 5;
        let to = MONDAY + 2 * DAY + 7;
        assert_eq!(
            days((from, to)),
            [
                (from, MONDAY + DAY - 1),
                (MONDAY + DAY, MONDAY + 2 * DAY - 1),
                (MONDAY + 2 * DAY, to),
            ]
        );
    }

    #[test]
    fn a_window_within_a_day_is_one_day() {
        assert_eq!(days((MONDAY, MONDAY + 1)), [(MONDAY, MONDAY + 1)]);
        assert_eq!(
            days((MONDAY, MONDAY + DAY - 1)),
            [(MONDAY, MONDAY + DAY - 1)]
        );
        assert_eq!(
            days((MONDAY + DAY - 1, MONDAY + DAY)),
            [
                (MONDAY + DAY - 1, MONDAY + DAY - 1),
                (MONDAY + DAY, MONDAY + DAY)
            ]
        );
    }

    #[test]
    fn a_request_ends_after_its_window() {
        let request = request(gc(), (MONDAY, MONDAY + DAY - 1));
        assert_eq!(request.dataset, "GLBX.MDP3");
        assert_eq!(request.symbols, ["GC.v.0"]);
        assert_eq!(request.start, UnixNanos::from(MONDAY));
        assert_eq!(request.end, Some(UnixNanos::from(MONDAY + DAY)));
    }

    #[test]
    fn a_day_is_stored_over_the_day() {
        let (_dir, mut catalog) = new_catalog();
        let day = (MONDAY, MONDAY + DAY - 1);
        let trades = [trade(gc(), MONDAY + 3, 1), trade(gc(), MONDAY + 9, 2)];
        write_day(&catalog, gc(), day, &trades, ROWS_PER_FILE).unwrap();
        assert_eq!(trade_intervals(&catalog), [day]);
        let stored = catalog
            .trade_ticks(Some(vec!["GC.v.0.GLBX".to_string()]), None, None)
            .unwrap();
        assert_eq!(stored, trades);
    }

    #[test]
    fn a_day_is_split_into_contiguous_files_between_timestamps() {
        let (_dir, mut catalog) = new_catalog();
        let day = (MONDAY, MONDAY + DAY - 1);
        let trades = [
            trade(gc(), MONDAY + 3, 1),
            trade(gc(), MONDAY + 9, 2),
            trade(gc(), MONDAY + 9, 3),
            trade(gc(), MONDAY + 12, 4),
        ];
        write_day(&catalog, gc(), day, &trades, 1).unwrap();
        assert_eq!(
            trade_intervals(&catalog),
            [
                (MONDAY, MONDAY + 8),
                (MONDAY + 9, MONDAY + 11),
                (MONDAY + 12, MONDAY + DAY - 1),
            ]
        );
        let stored = catalog
            .trade_ticks(Some(vec!["GC.v.0.GLBX".to_string()]), None, None)
            .unwrap();
        assert_eq!(stored.len(), 4);
    }

    #[test]
    fn an_empty_day_writes_nothing() {
        let (_dir, catalog) = new_catalog();
        write_day::<TradeTick>(&catalog, gc(), (MONDAY, MONDAY + DAY - 1), &[], 1).unwrap();
        assert!(trade_intervals(&catalog).is_empty());
    }

    #[test]
    fn a_record_of_another_instrument_or_day_is_refused() {
        let (_dir, catalog) = new_catalog();
        let day = (MONDAY, MONDAY + DAY - 1);
        let other = [trade(InstrumentId::from("GCZ6.GLBX"), MONDAY + 3, 1)];
        let err = write_day(&catalog, gc(), day, &other, 1).unwrap_err();
        assert!(err.to_string().contains("GCZ6.GLBX"), "{err}");
        let late = [trade(gc(), MONDAY + DAY, 1)];
        let err = write_day(&catalog, gc(), day, &late, 1).unwrap_err();
        assert!(err.to_string().contains("outside"), "{err}");
        assert!(trade_intervals(&catalog).is_empty());
    }
}
