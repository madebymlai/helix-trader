//! Databento history of `GLBX` instruments (CME Globex, `GLBX.MDP3`),
//! fetched with Nautilus' historical client (`DatabentoHistoricalClient`)
//! and stored in the catalog: MBP-10 as `OrderBookDepth` snapshots, trades
//! as `TradeTick`s, with the instrument's definitions.
//!
//! A fill takes series slices (`strategy::SeriesSlice`), so a rolled root
//! is filled contract by contract, each by its raw symbol over its slice.
//! Each series resumes after what the catalog holds and is fetched one UTC
//! day per request, so a day of MBP-10 (about 7 million snapshots) is the
//! most held at once. Databento's quotes for the missing days of every
//! contract are logged before fetching any. See `README.md`.

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

use crate::strategy::SeriesSlice;

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

    /// Fills `catalog` with the MBP-10 snapshots and trades of the
    /// contracts of `slices`, `GLBX` series, each over its slice, and their
    /// definitions: what `plan` gives, each contract asked for by its own
    /// symbol (raw symbology for a dated contract, `GCZ6`) and stored under
    /// its own id.
    ///
    /// A covered plan asks for nothing, not even a quote. Otherwise
    /// Databento's quotes for every missing window are logged, then their
    /// total, before any data request. Then, contract by contract, the
    /// definitions over its missing span later than any stored are
    /// written, and each day is one request, stored over the day. A day
    /// with no record (a Saturday, CME being closed) writes nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if `plan` does, before any request; if a request
    /// fails; or if a response holds another instrument or a record outside
    /// its day. Days stored before the error stay stored.
    pub async fn fill(
        &self,
        catalog: &ParquetDataCatalog,
        slices: &[SeriesSlice],
    ) -> anyhow::Result<()> {
        let fetches = plan(catalog, slices)?;
        let Some(end) = fetches.iter().map(|fetch| fetch.span().1).max() else {
            return Ok(());
        };
        let available = self
            .metadata
            .clone()
            .metadata()
            .get_dataset_range(DATASET)
            .await
            .with_context(|| format!("requesting the range of {DATASET}"))?;
        ensure_available(end, nanos_of(available.end.unix_timestamp_nanos())?)?;
        let mut total = 0.0;
        for fetch in &fetches {
            for (schema, window) in fetch.series() {
                total += self.log_cost(fetch.id, schema, window).await?;
            }
        }
        log::info!("Databento quotes ${total:.2} in all");
        for fetch in &fetches {
            self.fetch(catalog, fetch).await?;
        }
        Ok(())
    }

    /// Fills `catalog` with the missing windows of `fetch`: the
    /// definitions over their span, then the MBP-10 and the trades, a day
    /// per request.
    async fn fetch(&self, catalog: &ParquetDataCatalog, fetch: &Fetch) -> anyhow::Result<()> {
        let Fetch { id, depth, trades } = *fetch;
        self.store_definitions(catalog, id, fetch.span()).await?;
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

    /// Logs and returns Databento's quote for `schema` of `id` over
    /// `window`.
    async fn log_cost(
        &self,
        id: InstrumentId,
        schema: Schema,
        window: Window,
    ) -> anyhow::Result<f64> {
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
        Ok(cost)
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

/// What the catalog misses of a contract: the window of its MBP-10 and of
/// its trades to fetch, at least one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fetch {
    id: InstrumentId,
    depth: Option<Window>,
    trades: Option<Window>,
}

impl Fetch {
    /// The schemas missing and their windows.
    fn series(&self) -> impl Iterator<Item = (Schema, Window)> {
        [(Schema::Mbp10, self.depth), (Schema::Trades, self.trades)]
            .into_iter()
            .filter_map(|(schema, window)| Some((schema, window?)))
    }

    /// The span of the missing windows.
    fn span(&self) -> Window {
        self.series()
            .map(|(_, window)| window)
            .reduce(|a, b| (a.0.min(b.0), a.1.max(b.1)))
            .expect("at least one missing window")
    }
}

/// What to fetch for `slices`, `GLBX` series: per contract, in the order of
/// `slices`, the MBP-10 and trades missing after what `catalog` stores
/// over the span its slices cover (see `resume`), whatever the series of
/// the slices. A rolled root's contracts come each over its own slice; the
/// slices of one contract from several tables (its root's and its own) are
/// merged into one span. A contract with nothing missing is left out.
///
/// # Errors
///
/// Returns an error if a slice is not on `GLBX`, or if a contract's span
/// starts before a series' stored range (extending history backwards is
/// not supported).
fn plan(catalog: &ParquetDataCatalog, slices: &[SeriesSlice]) -> anyhow::Result<Vec<Fetch>> {
    let mut contracts: Vec<(InstrumentId, Window)> = Vec::new();
    for SeriesSlice { slice, .. } in slices {
        let id = slice.instrument_id;
        if !DatabentoHistory::serves(id) {
            bail!("Databento serves GLBX instruments here, not {id}");
        }
        let (from, to) = (nanos(slice.from), nanos(slice.to));
        match contracts.iter_mut().find(|(contract, _)| *contract == id) {
            Some((_, span)) => *span = (span.0.min(from), span.1.max(to)),
            None => contracts.push((id, (from, to))),
        }
    }
    let mut fetches = Vec::new();
    for (id, (start, end)) in contracts {
        let depth = missing_window::<OrderBookDepth>(catalog, id, start, end)?;
        let trades = missing_window::<TradeTick>(catalog, id, start, end)?;
        if depth.is_none() && trades.is_none() {
            log::info!(
                "{id}: MBP-10 and trades stored up to {}, nothing to fetch",
                at(end)
            );
        } else {
            fetches.push(Fetch { id, depth, trades });
        }
    }
    Ok(fetches)
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
    use crate::strategy::{DataRequirements, TickData, TickSeries};
    use jiff::SignedDuration;
    use nautilus_model::{
        data::stubs::stub_depth10,
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

    fn ns_at(time: &str) -> u64 {
        nanos(time.parse().unwrap())
    }

    /// Requirements reading the trades and book of each of `instrument_ids`.
    fn ticks_of(instrument_ids: &[&str]) -> DataRequirements {
        let ticks = instrument_ids
            .iter()
            .flat_map(|id| {
                [TickData::Trades, TickData::Depth].map(|data| TickSeries {
                    data,
                    instrument_id: InstrumentId::from(*id),
                })
            })
            .collect();
        DataRequirements {
            bar_types: vec![],
            ticks,
            warmup: SignedDuration::ZERO,
        }
    }

    /// The fetches planned for the series `instrument_ids` read over
    /// 2026-07-30 00:00 to 2026-07-31 12:00 UTC, across GC's roll from
    /// GCQ6 to GCZ6 at 2026-07-31 00:00 (the last weekday of July, before
    /// August, Q's delivery month).
    fn plan_across_the_roll(
        catalog: &ParquetDataCatalog,
        instrument_ids: &[&str],
    ) -> anyhow::Result<Vec<Fetch>> {
        let slices = ticks_of(instrument_ids).slices(
            "2026-07-30T00:00Z".parse().unwrap(),
            "2026-07-31T12:00Z".parse().unwrap(),
        );
        plan(catalog, &slices)
    }

    fn fetch(id: &str, depth: Option<Window>, trades: Option<Window>) -> Fetch {
        Fetch {
            id: InstrumentId::from(id),
            depth,
            trades,
        }
    }

    #[test]
    fn a_rolled_root_is_fetched_contract_by_contract_over_each_slice() {
        let (_dir, catalog) = new_catalog();
        let q6 = (ns_at("2026-07-30T00:00Z"), ns_at("2026-07-31T00:00Z") - 1);
        let z6 = (ns_at("2026-07-31T00:00Z"), ns_at("2026-07-31T12:00Z"));
        assert_eq!(
            plan_across_the_roll(&catalog, &["GC.GLBX"]).unwrap(),
            [
                fetch("GCQ6.GLBX", Some(q6), Some(q6)),
                fetch("GCZ6.GLBX", Some(z6), Some(z6)),
            ]
        );
    }

    #[test]
    fn a_contract_is_requested_by_its_raw_symbol() {
        let request = request(InstrumentId::from("GCZ6.GLBX"), (MONDAY, MONDAY + 1));
        assert_eq!(request.symbols, ["GCZ6"]);
    }

    /// GCQ6's trades stored up to 13:00 on the 30th, its book not at all.
    #[test]
    fn each_contract_of_a_rolled_root_resumes_after_its_stored_range() {
        let (_dir, catalog) = new_catalog();
        let gcq6 = InstrumentId::from("GCQ6.GLBX");
        let stored = (ns_at("2026-07-30T00:00Z"), ns_at("2026-07-30T13:00Z"));
        let trades = [trade(gcq6, ns_at("2026-07-30T12:00Z"), 1)];
        write_day(&catalog, gcq6, stored, &trades, ROWS_PER_FILE).unwrap();
        let q6 = (ns_at("2026-07-30T00:00Z"), ns_at("2026-07-31T00:00Z") - 1);
        let q6_after = (ns_at("2026-07-30T13:00Z") + 1, q6.1);
        let z6 = (ns_at("2026-07-31T00:00Z"), ns_at("2026-07-31T12:00Z"));
        assert_eq!(
            plan_across_the_roll(&catalog, &["GC.GLBX"]).unwrap(),
            [
                fetch("GCQ6.GLBX", Some(q6), Some(q6_after)),
                fetch("GCZ6.GLBX", Some(z6), Some(z6)),
            ]
        );
    }

    #[test]
    fn a_contract_stored_over_its_slice_is_not_fetched() {
        let (_dir, catalog) = new_catalog();
        let gcq6 = InstrumentId::from("GCQ6.GLBX");
        let q6 = (ns_at("2026-07-30T00:00Z"), ns_at("2026-07-31T00:00Z") - 1);
        let trades = [trade(gcq6, ns_at("2026-07-30T12:00Z"), 1)];
        write_day(&catalog, gcq6, q6, &trades, ROWS_PER_FILE).unwrap();
        let mut depth = stub_depth10();
        let at_noon = UnixNanos::from(ns_at("2026-07-30T12:00Z"));
        (depth.instrument_id, depth.ts_event, depth.ts_init) = (gcq6, at_noon, at_noon);
        write_day(&catalog, gcq6, q6, &[depth], ROWS_PER_FILE).unwrap();
        let z6 = (ns_at("2026-07-31T00:00Z"), ns_at("2026-07-31T12:00Z"));
        assert_eq!(
            plan_across_the_roll(&catalog, &["GC.GLBX"]).unwrap(),
            [fetch("GCZ6.GLBX", Some(z6), Some(z6))]
        );
    }

    /// GCZ6.GLBX declared as itself is one slice over the window; with
    /// GC.GLBX it is also the root's contract from the roll on.
    #[test]
    fn a_contract_declared_as_itself_and_through_its_root_is_fetched_once_over_both() {
        let (_dir, catalog) = new_catalog();
        let q6 = (ns_at("2026-07-30T00:00Z"), ns_at("2026-07-31T00:00Z") - 1);
        let window = (ns_at("2026-07-30T00:00Z"), ns_at("2026-07-31T12:00Z"));
        assert_eq!(
            plan_across_the_roll(&catalog, &["GC.GLBX", "GCZ6.GLBX"]).unwrap(),
            [
                fetch("GCQ6.GLBX", Some(q6), Some(q6)),
                fetch("GCZ6.GLBX", Some(window), Some(window)),
            ]
        );
    }

    #[test]
    fn a_dated_contract_is_fetched_over_the_whole_window() {
        let (_dir, catalog) = new_catalog();
        let window = (ns_at("2026-07-30T00:00Z"), ns_at("2026-07-31T12:00Z"));
        assert_eq!(
            plan_across_the_roll(&catalog, &["GCZ6.GLBX"]).unwrap(),
            [fetch("GCZ6.GLBX", Some(window), Some(window))]
        );
    }

    #[test]
    fn a_series_not_on_glbx_is_refused() {
        let (_dir, catalog) = new_catalog();
        let err = plan_across_the_roll(&catalog, &["GCZ6.XCEC"]).unwrap_err();
        assert!(err.to_string().contains("GCZ6.XCEC"), "{err}");
    }
}
