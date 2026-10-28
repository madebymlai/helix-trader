//! OKX history: the instruments of OKX swaps and X-Perps, their 1-minute
//! candles and their funding, filled into the catalog one archive month, or
//! day, at a time, then, for funding, from OKX's API.
//!
//! Instruments come from OKX's public REST API through the adapter's
//! `OKXRawHttpClient` (that of its `OKXHttpClient`) and
//! `parse_instrument_any`, with margins of 1 (see `instrument_from`).
//! Candles and a swap's funding come from OKX's archive
//! (see `archive`): one file per swap, or per futures family, and month or
//! day, the months and days of the archive being those of UTC+8. Candles
//! are stored as the bars of the instrument's `1-MINUTE-LAST-EXTERNAL` bar
//! type, the source of the composite bar types strategies trade OKX on;
//! funding as the instrument's `FundingRateUpdate`s, which a backtest's
//! venue settles. The archive serves the funding of swaps only, in monthly
//! files; the funding after its last month, and an X-Perp's, comes from
//! the adapter's `OKXHttpClient::request_funding_rates` (see
//! `fill_funding_from_api`). The archive API, the download and the
//! conversion are ours, to be removed if Nautilus gains an equivalent.
//!
//! The fill (`OkxHistory::fill`) is temporary: OKX's data client answers
//! without a `DataResponseEnvelope`, so Nautilus writes none of its answers
//! back. Once nautilus_trader#5473 is fixed, OKX fills through the node as
//! Databento and IC do (see `node_fill`).

use std::{collections::HashMap, io::Cursor, time::Duration};

use anyhow::{Context, bail, ensure};
use jiff::{
    SignedDuration, Timestamp, ToSpan,
    civil::Date,
    tz::{self, TimeZone},
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{BarSpecification, BarType, Data, FundingRateUpdate, NautilusDataType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{http::HttpClient, ratelimiter::quota::Quota};
use nautilus_okx::{
    common::{
        consts::OKX_VENUE,
        enums::{OKXEnvironment, OKXInstrumentType},
        models::OKXInstrument,
        parse::{is_xperp_rule_type, okx_instrument_type_from_symbol, parse_instrument_any},
    },
    http::{client::OKXHttpClient, query::GetInstrumentsParams},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::fill_catalog;
use crate::strategy::SeriesSlice;

mod archive;

/// OKX's archive of history files (public, no key).
const ARCHIVE_API: &str = "https://www.okx.com/api/v5/public/market-data-history";

/// Where the archive's files are served from; any other URL is refused.
const ARCHIVE_FILES: &str = "https://static.okx.com/";

/// The archive API's limit, 5 requests per 2 seconds, kept as one listing
/// every 600 ms with no burst (every 400 ms still got HTTP 429 now and
/// then). Nautilus' `HttpClient` limits only the requests passing a key,
/// so the listings pass `ARCHIVE_API_KEY`; the downloads, from
/// `static.okx.com`, are not limited.
const ARCHIVE_REQUEST_PERIOD: Duration = Duration::from_millis(600);

/// The rate-limit key of the archive API's requests.
const ARCHIVE_API_KEY: &str = "market-data-history";

/// The timeout of one archive request, a listing or a file (about 1.2 MB
/// a month, 0.04 MB a day, for BTC).
const ARCHIVE_REQUEST_TIMEOUT_SECS: u64 = 60;

/// The archive's months and days are those of UTC+8: the file of
/// 2026-10-19 holds the candles opening from 2026-10-18T16:00Z to
/// 2026-10-19T15:59Z, that of 2026-09 those from 2026-08-31T16:00Z to
/// 2026-09-30T15:59Z.
const ARCHIVE_DAY_OFFSET_HOURS: i8 = 8;

/// The most months, or days, one listing covers (OKX's errors 50077 and
/// 50076 beyond).
const LISTED_PERIODS: i32 = 10;

/// A connection to OKX's public API and archive, for history.
#[derive(Debug)]
pub struct OkxHistory {
    /// Nautilus' client of OKX's API: its parsed funding history, and its
    /// raw client for the instruments.
    client: OKXHttpClient,
    archive: HttpClient,
}

impl OkxHistory {
    /// The clients of OKX's public REST API and of its archive.
    ///
    /// # Errors
    ///
    /// Returns an error if a client cannot be built.
    pub fn new() -> anyhow::Result<Self> {
        let client = OKXHttpClient::new(None, 60, 3, 1000, 10_000, OKXEnvironment::Live, None)
            .context("building the OKX HTTP client")?;
        let quota = Quota::with_period(ARCHIVE_REQUEST_PERIOD)
            .context("the archive request period is zero")?;
        let archive = HttpClient::builder()
            .default_quota(quota)
            .timeout_secs(ARCHIVE_REQUEST_TIMEOUT_SECS)
            .build()
            .context("building the OKX archive client")?;
        Ok(Self { client, archive })
    }

    /// Whether `id` is OKX's, so its history comes from here.
    #[must_use]
    pub fn serves(id: InstrumentId) -> bool {
        id.venue == *OKX_VENUE
    }

    /// Fills `catalog` with `slices` (`DataRequirements::slices`, as the
    /// node fill takes them), each the bars of the `1-MINUTE-LAST-EXTERNAL`
    /// bar type of an OKX swap or X-Perp over its slice, with their
    /// instruments and the funding of each, the funding before the bars
    /// (see `fill_series`). Temporary: OKX's data client answers without a
    /// `DataResponseEnvelope`, so Nautilus writes none of its answers back;
    /// once nautilus_trader#5473 is fixed, OKX fills through the node as
    /// Databento and IC do, and this fill goes.
    ///
    /// # Errors
    ///
    /// Returns an error if a slice is not such bars, checked before any
    /// request, or as `fill_series`.
    pub async fn fill(
        &self,
        catalog: &mut ParquetDataCatalog,
        slices: &[SeriesSlice],
    ) -> anyhow::Result<()> {
        for (bar_type, start, end) in candle_slices(slices)? {
            let id = bar_type.instrument_id();
            let contract = Contract::of(id)?;
            let instrument = self.instrument(id).await?;
            fill_series(
                catalog,
                &instrument,
                bar_type,
                start,
                end,
                async |module, period, first, last| {
                    self.files(contract, module, period, first, last).await
                },
                async |module, file: &ArchiveFile, from, to| match module {
                    ArchiveModule::Candles => {
                        self.file_bars(&instrument, bar_type, file, from, to).await
                    }
                    ArchiveModule::Funding => self.file_funding(id, file, from, to).await,
                },
                async |before| {
                    let funding = self
                        .client
                        .request_funding_rates(id, None, Some(before), None)
                        .await
                        .with_context(|| {
                            format!("requesting the funding of {id} before {before}")
                        })?;
                    log::info!("{id}: {} funding rates before {before}", funding.len());
                    Ok(funding)
                },
            )
            .await?;
        }
        Ok(())
    }

    /// The instrument of `id`, an OKX swap or X-Perp (see
    /// `instrument_from`).
    async fn instrument(&self, id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        let contract = Contract::of(id)?;
        let raw = self
            .client
            .raw_client()
            .get_instruments(GetInstrumentsParams {
                inst_type: contract.inst_type,
                inst_id: Some(id.symbol.to_string()),
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the instrument {id} from OKX"))?;
        let [raw] = raw.as_slice() else {
            bail!("OKX returned {} instruments for {id}, not one", raw.len());
        };
        instrument_from(raw, get_atomic_clock_realtime().get_time_ns())
    }

    /// The bars of `bar_type`, the minute bars of `instrument`, closing in
    /// `[from, to]`, from `file`. Logs the file and count.
    async fn file_bars(
        &self,
        instrument: &InstrumentAny,
        bar_type: BarType,
        file: &ArchiveFile,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        let body = self
            .download(&file.url)
            .await
            .with_context(|| format!("downloading {}", file.filename))?;
        let bars = archive::bars_from_archive(
            Cursor::new(body),
            instrument,
            bar_type,
            UnixNanos::from(from),
            UnixNanos::from(to),
        )
        .with_context(|| format!("reading {}", file.filename))?;
        log::info!(
            "{}: {} bars from {}",
            instrument.id(),
            bars.len(),
            file.filename
        );
        Ok(bars.into_iter().map(Data::Bar).collect())
    }

    /// The funding of the swap `id` at times in `[from, to]`, from `file`.
    /// Logs the file and count.
    async fn file_funding(
        &self,
        id: InstrumentId,
        file: &ArchiveFile,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        let body = self
            .download(&file.url)
            .await
            .with_context(|| format!("downloading {}", file.filename))?;
        let funding = archive::funding_from_archive(
            Cursor::new(body),
            id,
            UnixNanos::from(from),
            UnixNanos::from(to),
        )
        .with_context(|| format!("reading {}", file.filename))?;
        log::info!(
            "{id}: {} funding rates from {}",
            funding.len(),
            file.filename
        );
        Ok(funding.into_iter().map(Data::FundingRate).collect())
    }

    /// The archive's files of `module` of `contract` for the months, or
    /// days, from `first` to `last`, at most `LISTED_PERIODS` of them, keyed
    /// by their first day; one listing request.
    async fn files(
        &self,
        contract: Contract,
        module: ArchiveModule,
        period: ArchivePeriod,
        first: Date,
        last: Date,
    ) -> anyhow::Result<HashMap<Date, ArchiveFile>> {
        let id = contract.id;
        let first_ms = day_start(first)?.as_millisecond().to_string();
        let last_ms = day_start(last)?.as_millisecond().to_string();
        let params: HashMap<String, Vec<String>> = [
            ("module", module.code()),
            ("instType", contract.archive_type()),
            ("instFamilyList", contract.family),
            ("dateAggrType", period.date_aggr_type()),
            ("begin", &first_ms),
            ("end", &last_ms),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), vec![value.to_string()]))
        .collect();
        let asked = || {
            format!(
                "the {} {} files of {id} from {first} to {last}",
                period.date_aggr_type(),
                module.name()
            )
        };
        let response = self
            .archive
            .get(
                ARCHIVE_API.to_string(),
                Some(&params),
                None,
                None,
                Some(vec![ARCHIVE_API_KEY.to_string()]),
            )
            .await
            .with_context(|| format!("asking OKX's archive for {}", asked()))?;
        ensure!(
            response.status.is_success(),
            "OKX's archive answered HTTP {} for {}",
            response.status.as_u16(),
            asked()
        );
        let listing: ArchiveListing = serde_json::from_slice(&response.body)
            .with_context(|| format!("parsing OKX's archive listing of {}", asked()))?;
        listing.files(contract, module)
    }

    /// The bytes of `url`, a file of the archive.
    async fn download(&self, url: &str) -> anyhow::Result<Vec<u8>> {
        ensure!(
            url.starts_with(ARCHIVE_FILES),
            "the archive file {url} is not under {ARCHIVE_FILES}"
        );
        let response = self
            .archive
            .get(url.to_string(), None, None, None, None)
            .await?;
        ensure!(
            response.status.is_success(),
            "HTTP {}",
            response.status.as_u16()
        );
        Ok(response.body.to_vec())
    }
}

/// The bar type and window of each of `slices`, OKX's candles each over
/// its slice; an error for the first slice of another series (see
/// `candle_bar_type`). Temporary until nautilus_trader#5473 is fixed: OKX
/// then fills through the node.
fn candle_slices(slices: &[SeriesSlice]) -> anyhow::Result<Vec<(BarType, Timestamp, Timestamp)>> {
    slices
        .iter()
        .map(
            |SeriesSlice {
                 series: (data_type, identifier),
                 slice,
             }| {
                Ok((
                    candle_bar_type(data_type, identifier)?,
                    slice.from,
                    slice.to,
                ))
            },
        )
        .collect()
}

/// The bar type of a series OKX's archive serves: the bars of the
/// `1-MINUTE-LAST-EXTERNAL` bar type of an OKX instrument, the 1-minute
/// candles of its trades. Any other series is an error saying what to trade
/// OKX on.
fn candle_bar_type(data_type: &NautilusDataType, identifier: &str) -> anyhow::Result<BarType> {
    let served = || {
        format!(
            "OKX's archive serves 1-minute candles only, the bars of \
             `<instrument>-1-MINUTE-LAST-EXTERNAL`: trade OKX on a composite bar type built \
             from them, such as `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`, \
             not on the {data_type:?} of {identifier}"
        )
    };
    ensure!(*data_type == NautilusDataType::Bar, served());
    let bar_type: BarType = identifier.parse().with_context(served)?;
    let minute = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    ensure!(
        bar_type.spec() == minute
            && bar_type.aggregation_source() == AggregationSource::External
            && OkxHistory::serves(bar_type.instrument_id()),
        served()
    );
    Ok(bar_type)
}

/// Fills `catalog` over `[start, end]` with `instrument`, its funding and
/// the bars of `bar_type`, its minute candles. The funding comes first, a
/// swap's from the archive's months (see `fill_from_archive`) then from
/// OKX's API, an X-Perp's from the API only (see `fill_funding_from_api`),
/// so that a failed funding fill stores no new bars and a backtest over
/// them is refused for want of funding; then the bars, from the archive.
/// `list_files` and `file_data` serve the archive's files of a module (as
/// `fill_from_archive`'s), `request_funding` the API's funding (as
/// `fill_funding_from_api`'s `request`).
///
/// # Errors
///
/// As `fill_from_archive` (no funding file for the month of a swap's
/// `start` is an error) and `fill_funding_from_api`.
#[allow(
    clippy::too_many_arguments,
    reason = "the series, its window and its three sources"
)]
async fn fill_series(
    catalog: &mut ParquetDataCatalog,
    instrument: &InstrumentAny,
    bar_type: BarType,
    start: Timestamp,
    end: Timestamp,
    mut list_files: impl AsyncFnMut(
        ArchiveModule,
        ArchivePeriod,
        Date,
        Date,
    ) -> anyhow::Result<HashMap<Date, ArchiveFile>>,
    mut file_data: impl AsyncFnMut(
        ArchiveModule,
        &ArchiveFile,
        Timestamp,
        Timestamp,
    ) -> anyhow::Result<Vec<Data>>,
    request_funding: impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<FundingRateUpdate>>,
) -> anyhow::Result<()> {
    let id = instrument.id();
    if Contract::of(id)?.inst_type == OKXInstrumentType::Swap {
        fill_from_archive(
            catalog,
            instrument,
            ArchiveModule::Funding,
            (NautilusDataType::FundingRateUpdate, id.to_string()),
            start,
            end,
            async |period, first, last| {
                list_files(ArchiveModule::Funding, period, first, last).await
            },
            async |file: &ArchiveFile, from, to| {
                file_data(ArchiveModule::Funding, file, from, to).await
            },
        )
        .await?;
    }
    fill_funding_from_api(catalog, instrument, start, end, request_funding).await?;
    fill_from_archive(
        catalog,
        instrument,
        ArchiveModule::Candles,
        (NautilusDataType::Bar, bar_type.to_string()),
        start,
        end,
        async |period, first, last| list_files(ArchiveModule::Candles, period, first, last).await,
        async |file: &ArchiveFile, from, to| {
            file_data(ArchiveModule::Candles, file, from, to).await
        },
    )
    .await
}

/// Fills `catalog` with `instrument` and `series`, the items of `module`
/// of the instrument, over `[start, end]`, one archive month, or day, at a
/// time: `fill_catalog` over `[start, the period's end]`, so each call
/// resumes where the catalog stopped. A period's file holds the minutes
/// opening in it, so the bars closing in `(its start, its end]` (UTC+8
/// midnights), and the funding at times in `[its start, its end)`: its
/// window ends on the last instant it holds, and the next starts 1 ns after
/// it, contiguous and closed as Nautilus' catalog intervals. The first
/// window starts with the day of `start`, which the files cover whole (a
/// `start` on a day's end would otherwise make a window of one instant,
/// which `fill_catalog` takes as covered). A period starting at `end` is
/// left to the next run. Windows are worked out on closes, a funding time
/// being taken as the close 1 ns after it (`ArchiveModule::lead`).
///
/// From the month it resumes in, the fill reads monthly files, listed
/// `LISTED_PERIODS` months per `list_files` call, up to the first month
/// without one (OKX publishes a month's file after it ends); from there on,
/// for candles, daily files, listed `LISTED_PERIODS` days per call, up to
/// the first day without one (not published yet). It stops at the first
/// period without a file it has no shorter files for. `list_files` answers
/// the files of the months, or days, from its first to its last date, keyed
/// by their first day; `file_data` the items of a file in a window within
/// its period.
///
/// # Errors
///
/// As `fill_catalog`: no file for the first period is an empty first
/// response, an error naming the series.
#[allow(
    clippy::too_many_arguments,
    reason = "the series, its window and its two sources"
)]
async fn fill_from_archive(
    catalog: &mut ParquetDataCatalog,
    instrument: &InstrumentAny,
    module: ArchiveModule,
    series: (NautilusDataType, String),
    start: Timestamp,
    end: Timestamp,
    mut list_files: impl AsyncFnMut(
        ArchivePeriod,
        Date,
        Date,
    ) -> anyhow::Result<HashMap<Date, ArchiveFile>>,
    mut file_data: impl AsyncFnMut(&ArchiveFile, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()> {
    let (data_type, identifier) = series.clone();
    let named = format!("{} {identifier}", module.name());
    let series = [series];
    // Closes from here on.
    let lead = module.lead();
    let end = end + lead;
    let start = day_start(archive_day(start + lead)?)? + SignedDuration::from_nanos(1);
    let last = catalog
        .query_last_timestamp(&data_type.into(), Some(&identifier))
        .with_context(|| format!("reading the stored range of the {named}"))?;
    // `fill_catalog` resumes 1 ns after the stored end.
    let begin = last.map_or(start, |last| {
        Timestamp::from(UnixNanos::from(last)) + SignedDuration::from_nanos(1) + lead
    });
    let mut period = ArchivePeriod::Month;
    let mut first = period.of(begin)?;
    'listings: while day_start(first)? < end {
        let last_listed = period.last_listed(first)?.min(period.of(end)?);
        let files = list_files(period, first, last_listed).await?;
        let mut date = first;
        while date <= last_listed {
            let file = files.get(&date);
            if file.is_none() && period == ArchivePeriod::Month && module.has_daily_files() {
                log::info!("{named}: no monthly file for {date}, reading daily files");
                period = ArchivePeriod::Day;
                first = archive_day(begin)?.max(date);
                continue 'listings;
            }
            fill_catalog(
                catalog,
                vec![instrument.clone()],
                &series,
                start - lead,
                period.end(date)?.min(end) - lead,
                async |_, _, from, to| {
                    let Some(file) = file else {
                        return Ok(Vec::new());
                    };
                    file_data(file, from, to).await
                },
            )
            .await?;
            if file.is_none() {
                log::info!("{named}: stopping at {date}, which OKX has not published");
                return Ok(());
            }
            date = period.next(date)?;
        }
        first = date;
    }
    Ok(())
}

/// Fills `catalog` with `instrument` and its funding at times in `[start,
/// end]` from OKX's API, resuming 1 ns after the stored funding; a window
/// the stored funding already covers asks for nothing.
/// `request(before)` answers the funding at times before `before`, as
/// Nautilus' `request_funding_rates` with `before` as its end (OKX's
/// `after`, exclusive, in milliseconds): up to a page of OKX's newest, oldest
/// first, the oldest without an interval, which Nautilus takes from the time
/// before it. Pages go backwards from `end`, each ending 1 ms after the
/// oldest time of the one before, so that this time comes again with its
/// interval, until a page reaches before the window's first instant or holds
/// only that time (OKX keeps about 3 months). Only updates with an interval
/// are stored, over the window's first instant, or, if OKX's history does
/// not reach it, its first update with an interval, to the newest update:
/// funding not published yet is not covered.
///
/// # Errors
///
/// Returns an error if a request fails, if there is no funding on a first
/// fill (an empty first response, as `fill_catalog`, naming the series), or
/// if OKX's history no longer reaches the stored end, which would leave a
/// hole.
async fn fill_funding_from_api(
    catalog: &mut ParquetDataCatalog,
    instrument: &InstrumentAny,
    start: Timestamp,
    end: Timestamp,
    mut request: impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<FundingRateUpdate>>,
) -> anyhow::Result<()> {
    let id = instrument.id();
    let series = [(NautilusDataType::FundingRateUpdate, id.to_string())];
    let last = catalog
        .query_last_timestamp(&series[0].0.clone().into(), Some(&series[0].1))
        .with_context(|| format!("reading the stored range of the funding of {id}"))?;
    let begin = last.map_or(start, |last| {
        Timestamp::from(UnixNanos::from(last)) + SignedDuration::from_nanos(1)
    });
    // A covered window asks for nothing, as `fill_catalog`.
    if begin > end {
        return Ok(());
    }
    let millisecond = SignedDuration::from_millis(1);
    let in_window = |update: &FundingRateUpdate| {
        let time = Timestamp::from(update.ts_event);
        update.interval.is_some() && begin <= time && time <= end
    };
    let mut funding = Vec::new();
    let mut before = end + millisecond;
    let reached = loop {
        let page = request(before).await?;
        let Some(oldest) = page.first() else {
            break false;
        };
        let oldest = Timestamp::from(oldest.ts_event);
        funding.extend(page.iter().copied().filter(in_window));
        if oldest < begin {
            break true;
        }
        if page.len() == 1 {
            break false;
        }
        before = oldest + millisecond;
    };
    ensure!(
        reached || last.is_none(),
        "OKX's funding history of {id} no longer reaches {begin}, 1 ns after the stored \
         funding: filling it would leave a hole; use a new catalog"
    );
    let times = || {
        funding
            .iter()
            .map(|update| Timestamp::from(update.ts_event))
    };
    let from = if reached {
        begin
    } else {
        times().min().unwrap_or(begin)
    };
    let to = times().max().unwrap_or(end);
    let mut funding: Vec<Data> = funding.into_iter().map(Data::FundingRate).collect();
    fill_catalog(
        catalog,
        vec![instrument.clone()],
        &series,
        from,
        to,
        async |_, _, _, _| Ok(std::mem::take(&mut funding)),
    )
    .await
}

/// The instrument of `raw`, an OKX swap or X-Perp (a future whose rule
/// type is `xperp`), through the adapter's `parse_instrument_any`, with
/// initial and maintenance margins of 1; a dated future is refused. OKX
/// locks notional / leverage on a position, which is Nautilus' default
/// margin model (notional / leverage x the instrument's margin) with
/// margins of 1 and the account's leverage. The adapter would leave them at
/// 0, which locks nothing.
fn instrument_from(raw: &OKXInstrument, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
    let id = raw.inst_id;
    if raw.inst_type == OKXInstrumentType::Futures {
        ensure!(
            is_xperp_rule_type(&raw.rule_type),
            "{id} is a dated future (rule type {:?}); of OKX's futures only X-Perps are served",
            raw.rule_type
        );
    }
    parse_instrument_any(raw, Some(Decimal::ONE), Some(Decimal::ONE), ts_init)?
        .with_context(|| format!("{id} is not an instrument Nautilus supports"))
}

/// An OKX swap (`BTC-USDT-SWAP.OKX`) or future
/// (`BTC-USD_UM_XPERP-310404.OKX`), keyed in OKX's API and archive, and
/// in the live clients' configs, by its instrument type and family.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Contract {
    id: InstrumentId,
    /// `Swap` or `Futures`, from the symbol (the adapter's
    /// `okx_instrument_type_from_symbol`).
    pub(crate) inst_type: OKXInstrumentType,
    /// The symbol without its last part: `BTC-USDT`, `BTC-USD_UM_XPERP`.
    pub(crate) family: &'static str,
}

impl Contract {
    /// The contract of `id`; an error if it is not an OKX swap or future.
    pub(crate) fn of(id: InstrumentId) -> anyhow::Result<Self> {
        ensure!(OkxHistory::serves(id), "{id} is not on {}", *OKX_VENUE);
        let symbol = id.symbol.inner().as_str();
        let inst_type = okx_instrument_type_from_symbol(symbol);
        ensure!(
            matches!(
                inst_type,
                OKXInstrumentType::Swap | OKXInstrumentType::Futures
            ),
            "{id} is not an OKX swap (BTC-USDT-SWAP.OKX) or X-Perp \
             (BTC-USD_UM_XPERP-310404.OKX)"
        );
        let (family, _) = symbol
            .rsplit_once('-')
            .with_context(|| format!("{id} has no instrument family"))?;
        Ok(Self {
            id,
            inst_type,
            family,
        })
    }

    /// The archive's name of the instrument type.
    fn archive_type(self) -> &'static str {
        if self.inst_type == OKXInstrumentType::Swap {
            "SWAP"
        } else {
            "FUTURES"
        }
    }

    /// The start of the name of the contract's file of `module` in the
    /// archive's listing: a swap's own file, or for candles the file of a
    /// future's family's futures chain, which holds the rows of every
    /// contract (see `archive`). An error for the funding of a future, which
    /// the archive does not hold (its funding module refuses `FUTURES`).
    fn listed_prefix(self, module: ArchiveModule) -> anyhow::Result<String> {
        let swap = self.inst_type == OKXInstrumentType::Swap;
        Ok(match module {
            ArchiveModule::Candles if swap => format!("{}-candlesticks-", self.id.symbol),
            ArchiveModule::Candles => format!("{}-futureschain-candlesticks-", self.family),
            ArchiveModule::Funding if swap => format!("{}-fundingrates-", self.id.symbol),
            ArchiveModule::Funding => {
                bail!(
                    "OKX's archive holds the funding of swaps only, not of {}",
                    self.id
                )
            }
        })
    }
}

/// A module of OKX's archive: what its files hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveModule {
    /// 1-minute candles, in monthly and daily files.
    Candles,
    /// The funding of swaps, in monthly files only.
    Funding,
}

impl ArchiveModule {
    /// The archive API's `module`.
    fn code(self) -> &'static str {
        match self {
            Self::Candles => "2",
            Self::Funding => "3",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Candles => "candle",
            Self::Funding => "funding",
        }
    }

    fn has_daily_files(self) -> bool {
        self == Self::Candles
    }

    /// How much earlier than a bar closing at the same point of its period
    /// an item is stamped: 1 ns for funding, since a period's file holds
    /// the bars closing in `(its start, its end]` and the funding at times
    /// in `[its start, its end)`.
    fn lead(self) -> SignedDuration {
        match self {
            Self::Candles => SignedDuration::ZERO,
            Self::Funding => SignedDuration::from_nanos(1),
        }
    }
}

/// The time zone of the archive's months and days.
fn archive_zone() -> TimeZone {
    TimeZone::fixed(tz::offset(ARCHIVE_DAY_OFFSET_HOURS))
}

/// The archive day whose file holds the bar closing at `close`: that of
/// the minute before it.
fn archive_day(close: Timestamp) -> anyhow::Result<Date> {
    let open = close.checked_sub(SignedDuration::from_nanos(1))?;
    Ok(open.to_zoned(archive_zone()).date())
}

/// Midnight UTC+8 at the start of `day`, the open of its first minute.
fn day_start(day: Date) -> anyhow::Result<Timestamp> {
    Ok(day.to_zoned(archive_zone())?.timestamp())
}

/// The period of an archive file, a month or a day of UTC+8, named by its
/// first day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchivePeriod {
    Month,
    Day,
}

impl ArchivePeriod {
    /// The archive's `dateAggrType` of the files of this period.
    fn date_aggr_type(self) -> &'static str {
        match self {
            Self::Month => "monthly",
            Self::Day => "daily",
        }
    }

    /// The first day of the period whose file holds the bar closing at
    /// `close`.
    fn of(self, close: Timestamp) -> anyhow::Result<Date> {
        let day = archive_day(close)?;
        Ok(match self {
            Self::Month => day.first_of_month(),
            Self::Day => day,
        })
    }

    /// The first day of the period after that starting on `first`.
    fn next(self, first: Date) -> anyhow::Result<Date> {
        Ok(match self {
            Self::Month => first.checked_add(1.month())?,
            Self::Day => first.tomorrow()?,
        })
    }

    /// The close of the last minute of the period starting on `first`: the
    /// next period's start.
    fn end(self, first: Date) -> anyhow::Result<Timestamp> {
        day_start(self.next(first)?)
    }

    /// The first day of the last period one listing starting on `first`
    /// covers.
    fn last_listed(self, first: Date) -> anyhow::Result<Date> {
        let more = LISTED_PERIODS - 1;
        Ok(match self {
            Self::Month => first.checked_add(more.months())?,
            Self::Day => first.checked_add(more.days())?,
        })
    }
}

/// The archive API's answer listing the files of a request.
#[derive(Debug, Deserialize)]
struct ArchiveListing {
    code: String,
    msg: String,
    data: Vec<ArchiveData>,
}

#[derive(Debug, Deserialize)]
struct ArchiveData {
    details: Vec<ArchiveDetail>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArchiveDetail {
    group_details: Vec<ArchiveFile>,
}

/// A file of the archive.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArchiveFile {
    /// The start of its period, in milliseconds.
    date_ts: String,
    filename: String,
    #[serde(rename = "sizeMB")]
    size_mb: String,
    url: String,
}

impl ArchiveListing {
    /// The files of `module` of `contract` in this listing, keyed by the
    /// first day of their period; an error if OKX refused the request,
    /// listed two files for one period, or does not hold `module` for
    /// `contract` (see `Contract::listed_prefix`).
    fn files(
        self,
        contract: Contract,
        module: ArchiveModule,
    ) -> anyhow::Result<HashMap<Date, ArchiveFile>> {
        let id = contract.id;
        let prefix = contract.listed_prefix(module)?;
        ensure!(
            self.code == "0",
            "OKX's archive refused the request: code {}, {}",
            self.code,
            self.msg
        );
        let mut files = HashMap::new();
        for file in self
            .data
            .into_iter()
            .flat_map(|data| data.details)
            .flat_map(|detail| detail.group_details)
            .filter(|file| file.filename.starts_with(&prefix))
        {
            let ms: i64 = file
                .date_ts
                .parse()
                .with_context(|| format!("reading the dateTs of {}", file.filename))?;
            let first = Timestamp::from_millisecond(ms)?
                .to_zoned(archive_zone())
                .date();
            if let Some(other) = files.insert(first, file) {
                bail!(
                    "OKX's archive lists more than one {} file of {id} for {first}, one \
                     being {}",
                    module.name(),
                    other.filename
                );
            }
        }
        Ok(files)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
        thread,
    };

    use jiff::civil::date;

    use nautilus_model::data::{Bar, FundingRateUpdate};

    use super::{
        ArchiveModule::{Candles, Funding},
        ArchivePeriod::{Day, Month},
        *,
    };

    const INSTRUMENT: &str = include_str!("testdata/BTC-USDT-SWAP-instrument.json");

    fn raw_instrument() -> OKXInstrument {
        serde_json::from_str(INSTRUMENT).unwrap()
    }

    /// BTC-USDT-SWAP.OKX as OKX's public API defined it on 2026-10-21.
    pub(in crate::data::okx) fn btc_instrument() -> InstrumentAny {
        instrument_from(&raw_instrument(), UnixNanos::default()).unwrap()
    }

    const XPERP_INSTRUMENT: &str = include_str!("testdata/BTC-USD_UM_XPERP-310404-instrument.json");

    fn raw_xperp() -> OKXInstrument {
        serde_json::from_str(XPERP_INSTRUMENT).unwrap()
    }

    /// BTC-USD_UM_XPERP-310404.OKX as OKX's public API defined it on
    /// 2026-10-22.
    pub(in crate::data::okx) fn xperp_instrument() -> InstrumentAny {
        instrument_from(&raw_xperp(), UnixNanos::default()).unwrap()
    }

    #[test]
    fn an_okx_swap_is_a_crypto_perpetual_with_its_contract_value_increments_and_margins_of_one() {
        let InstrumentAny::CryptoPerpetual(btc) = btc_instrument() else {
            panic!("not a CryptoPerpetual");
        };
        assert_eq!(btc.id, InstrumentId::from("BTC-USDT-SWAP.OKX"));
        assert_eq!(btc.multiplier.to_string(), "0.01");
        assert_eq!(btc.price_increment.to_string(), "0.1");
        assert_eq!(btc.size_increment.to_string(), "0.01");
        assert_eq!((btc.price_precision, btc.size_precision), (1, 2));
        assert_eq!(
            (btc.margin_init, btc.margin_maint),
            (Decimal::ONE, Decimal::ONE)
        );
        assert_eq!(btc.settlement_currency.code.as_str(), "USDT");
    }

    #[test]
    fn an_okx_x_perp_is_a_crypto_future_with_margins_of_one() {
        let InstrumentAny::CryptoFuture(btc) = xperp_instrument() else {
            panic!("not a CryptoFuture");
        };
        assert_eq!(btc.id, InstrumentId::from("BTC-USD_UM_XPERP-310404.OKX"));
        assert_eq!(btc.multiplier.to_string(), "0.0001");
        assert_eq!(btc.price_increment.to_string(), "0.1");
        assert_eq!(btc.size_increment.to_string(), "1");
        assert_eq!((btc.price_precision, btc.size_precision), (1, 0));
        assert_eq!(
            (btc.margin_init, btc.margin_maint),
            (Decimal::ONE, Decimal::ONE)
        );
        assert_eq!(btc.settlement_currency.code.as_str(), "USD");
        assert_eq!(btc.quote_currency.code.as_str(), "USD");
        assert!(!btc.is_inverse);
        assert_eq!(
            Timestamp::from(btc.expiration_ns),
            at("2031-04-04T08:00:00Z")
        );
        assert_eq!(
            Timestamp::from(btc.activation_ns),
            at("2026-03-30T04:30:00.428Z")
        );
    }

    #[test]
    fn a_dated_future_is_refused() {
        let mut dated = raw_xperp();
        dated.rule_type = "normal".to_string();
        let err = instrument_from(&dated, UnixNanos::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("X-Perp"), "{err}");
    }

    #[test]
    fn okx_serves_the_instruments_of_its_venue_only() {
        assert!(OkxHistory::serves(btc()));
        for id in ["ESZ6.CME", "SXR8.IBIS", "BTC-USDT-SWAP.BINANCE"] {
            assert!(!OkxHistory::serves(InstrumentId::from(id)), "{id}");
        }
    }

    #[test]
    fn the_type_and_family_of_an_okx_swap_or_future_key_the_api_and_the_archive() {
        let swap = Contract::of(btc()).unwrap();
        assert_eq!(
            (swap.inst_type, swap.family),
            (OKXInstrumentType::Swap, "BTC-USDT")
        );
        let future = Contract::of(xperp()).unwrap();
        assert_eq!(
            (future.inst_type, future.family),
            (OKXInstrumentType::Futures, "BTC-USD_UM_XPERP")
        );
        for id in [
            "BTC-USDT.OKX",
            "BTC-USD-261225-100000-C.OKX",
            "BTC-USDT-SWAP.BINANCE",
        ] {
            assert!(Contract::of(InstrumentId::from(id)).is_err(), "{id}");
        }
    }

    fn btc() -> InstrumentId {
        InstrumentId::from("BTC-USDT-SWAP.OKX")
    }

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310404.OKX")
    }

    const BTC_MINUTES: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";

    #[test]
    fn the_archive_serves_the_minute_candles_of_an_okx_instrument() {
        let bar_type = candle_bar_type(&NautilusDataType::Bar, BTC_MINUTES).unwrap();
        assert_eq!(bar_type, BarType::from(BTC_MINUTES));
        candle_bar_type(
            &NautilusDataType::Bar,
            "BTC-USD_UM_XPERP-310404.OKX-1-MINUTE-LAST-EXTERNAL",
        )
        .unwrap();
    }

    #[test]
    fn each_slice_is_filled_with_its_candles_over_its_window() {
        let requirements = crate::strategy::DataRequirements {
            bar_types: vec![BarType::from(
                "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            )],
            ticks: vec![],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (
            "2026-10-01T00:00:00Z".parse().unwrap(),
            "2026-10-08T00:00:00Z".parse().unwrap(),
        );

        let fills = candle_slices(&requirements.slices(start, end)).unwrap();

        assert_eq!(fills, [(BarType::from(BTC_MINUTES), start, end)]);
    }

    #[test]
    fn a_slice_of_another_series_is_refused() {
        let requirements = crate::strategy::DataRequirements {
            bar_types: vec![BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL")],
            ticks: vec![],
            warmup: SignedDuration::ZERO,
        };
        let start: Timestamp = "2026-10-01T00:00:00Z".parse().unwrap();
        let slices = requirements.slices(start, start + SignedDuration::from_hours(24));

        let err = candle_slices(&slices).unwrap_err();

        assert!(err.to_string().contains("BTC-USDT-SWAP.OKX"), "{err}");
    }

    #[test]
    fn any_other_series_is_refused_naming_the_composite_bar_type_to_trade_on() {
        for (data_type, identifier) in [
            (NautilusDataType::QuoteTick, "BTC-USDT-SWAP.OKX"),
            (NautilusDataType::TradeTick, "BTC-USDT-SWAP.OKX"),
            (
                NautilusDataType::Bar,
                "BTC-USDT-SWAP.OKX-1-HOUR-LAST-EXTERNAL",
            ),
            (
                NautilusDataType::Bar,
                "BTC-USDT-SWAP.OKX-1-MINUTE-MID-EXTERNAL",
            ),
            (
                NautilusDataType::Bar,
                "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-INTERNAL",
            ),
            (NautilusDataType::Bar, "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL"),
        ] {
            let err = candle_bar_type(&data_type, identifier)
                .unwrap_err()
                .to_string();
            assert!(err.contains("@1-MINUTE-EXTERNAL"), "{identifier}: {err}");
        }
    }

    fn listing(group_details: &str) -> ArchiveListing {
        serde_json::from_str(&format!(
            r#"{{"code":"0","msg":"","data":[{{"dateAggrType":"daily","details":[{{"ccy":"","dateRangeEnd":"1792339200000","dateRangeStart":"1792339200000","groupDetails":[{group_details}],"groupSizeMB":"0.04","instFamily":"BTC-USDT","instId":"","instType":"SWAP"}}],"totalSizeMB":"0.04","ts":"1792681198063"}}]}}"#
        ))
        .unwrap()
    }

    /// As OKX's archive listed it on 2026-10-22.
    const BTC_FILE: &str = r#"{"dateTs":"1792339200000","filename":"BTC-USDT-SWAP-candlesticks-2026-10-19.zip","sizeMB":"0.04","url":"https://static.okx.com/cdn/okex/traderecords/candlesticks/daily/20261019/BTC-USDT-SWAP-candlesticks-2026-10-19.zip?v=1001"}"#;

    #[test]
    fn the_listing_of_a_day_gives_the_candle_file_of_the_instrument_keyed_by_its_day() {
        let files = listing(BTC_FILE)
            .files(Contract::of(btc()).unwrap(), Candles)
            .unwrap();
        assert_eq!(files.len(), 1);
        let file = &files[&date(2026, 10, 19)];
        assert_eq!(file.filename, "BTC-USDT-SWAP-candlesticks-2026-10-19.zip");
        assert_eq!(file.size_mb, "0.04");
        assert!(file.url.starts_with(ARCHIVE_FILES));
    }

    /// Two of the files OKX's archive listed on 2026-10-23 for the months
    /// 2025-12 to 2026-09 of BTC-USDT swaps.
    const BTC_MONTHS: &str = r#"{"code":"0","data":[{"dateAggrType":"monthly","details":[{"ccy":"","dateRangeEnd":"1788192000000","dateRangeStart":"1764518400000","groupDetails":[{"dateTs":"1788192000000","filename":"BTC-USDT-SWAP-candlesticks-2026-09.zip","sizeMB":"1.2","url":"https://static.okx.com/cdn/okex/traderecords/candlesticks/monthly/202609/BTC-USDT-SWAP-candlesticks-2026-09.zip?v=1001"},{"dateTs":"1764518400000","filename":"BTC-USDT-SWAP-candlesticks-2025-12.zip","sizeMB":"1.25","url":"https://static.okx.com/cdn/okex/traderecords/candlesticks/monthly/202512/BTC-USDT-SWAP-candlesticks-2025-12.zip?v=1001"}],"groupSizeMB":"12.32","instFamily":"BTC-USDT","instId":"","instType":"SWAP"}],"totalSizeMB":"12.32","ts":"1792708598394"}],"msg":""}"#;

    #[test]
    fn the_listing_of_months_keys_each_file_by_its_month_of_utc_plus_8() {
        let listing: ArchiveListing = serde_json::from_str(BTC_MONTHS).unwrap();
        let files = listing
            .files(Contract::of(btc()).unwrap(), Candles)
            .unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(
            files[&date(2026, 9, 1)].filename,
            "BTC-USDT-SWAP-candlesticks-2026-09.zip"
        );
        assert_eq!(
            files[&date(2025, 12, 1)].filename,
            "BTC-USDT-SWAP-candlesticks-2025-12.zip"
        );
    }

    /// As OKX's archive listed it on 2026-10-22.
    const XPERP_FILE: &str = r#"{"dateTs":"1792339200000","filename":"BTC-USD_UM_XPERP-futureschain-candlesticks-2026-10-19.zip","sizeMB":"0.04","url":"https://static.okx.com/cdn/okex/traderecords/candlesticks/daily/20261019/BTC-USD_UM_XPERP-futureschain-candlesticks-2026-10-19.zip?v=1001"}"#;

    #[test]
    fn the_listing_of_a_day_gives_the_chain_file_of_a_future() {
        let xperp = Contract::of(xperp()).unwrap();
        let files = listing(XPERP_FILE).files(xperp, Candles).unwrap();
        assert_eq!(
            files[&date(2026, 10, 19)].filename,
            "BTC-USD_UM_XPERP-futureschain-candlesticks-2026-10-19.zip"
        );
        let other = XPERP_FILE.replace("BTC-USD_UM_XPERP", "ETH-USD_UM_XPERP");
        assert!(listing(&other).files(xperp, Candles).unwrap().is_empty());
        assert!(listing(BTC_FILE).files(xperp, Candles).unwrap().is_empty());
    }

    #[test]
    fn a_day_not_published_lists_no_file() {
        let empty: ArchiveListing = serde_json::from_str(
            r#"{"code":"0","data":[{"dateAggrType":"daily","details":[],"totalSizeMB":"0","ts":"1792616059979"}],"msg":""}"#,
        )
        .unwrap();
        assert!(
            empty
                .files(Contract::of(btc()).unwrap(), Candles)
                .unwrap()
                .is_empty()
        );
        let other = BTC_FILE.replace("BTC-USDT-SWAP", "BTC-USDT-250101");
        assert!(
            listing(&other)
                .files(Contract::of(btc()).unwrap(), Candles)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_refused_request_or_two_files_for_one_period_is_an_error() {
        let refused: ArchiveListing = serde_json::from_str(
            r#"{"code":"50077","msg":"The time interval between the start date and end date cannot exceed 10 months.","data":[]}"#,
        )
        .unwrap();
        let err = refused
            .files(Contract::of(btc()).unwrap(), Candles)
            .unwrap_err()
            .to_string();
        assert!(err.contains("50077"), "{err}");
        let twice = listing(&format!("{BTC_FILE},{BTC_FILE}"));
        let err = twice
            .files(Contract::of(btc()).unwrap(), Candles)
            .unwrap_err()
            .to_string();
        assert!(err.contains("2026-10-19"), "{err}");
    }

    /// Two of the files OKX's archive listed on 2026-10-23 for the funding
    /// of BTC-USDT swaps over the months 2025-12 to 2026-09.
    const BTC_FUNDING_MONTHS: &str = r#"{"code":"0","data":[{"dateAggrType":"monthly","details":[{"ccy":"","dateRangeEnd":"1788192000000","dateRangeStart":"1764518400000","groupDetails":[{"dateTs":"1788192000000","filename":"BTC-USDT-SWAP-fundingrates-2026-09.zip","sizeMB":"0","url":"https://static.okx.com/cdn/okex/traderecords/swaprates/monthly/202609/BTC-USDT-SWAP-fundingrates-2026-09.zip?v=1001"},{"dateTs":"1785513600000","filename":"BTC-USDT-SWAP-fundingrates-2026-08.zip","sizeMB":"0","url":"https://static.okx.com/cdn/okex/traderecords/swaprates/monthly/202608/BTC-USDT-SWAP-fundingrates-2026-08.zip?v=1001"}],"groupSizeMB":"0","instFamily":"BTC-USDT","instId":"","instType":"SWAP"}],"totalSizeMB":"0","ts":"1792710400893"}],"msg":""}"#;

    #[test]
    fn the_listing_of_funding_months_keys_each_file_by_its_month_of_utc_plus_8() {
        let listing: ArchiveListing = serde_json::from_str(BTC_FUNDING_MONTHS).unwrap();
        let files = listing
            .files(Contract::of(btc()).unwrap(), Funding)
            .unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(
            files[&date(2026, 9, 1)].filename,
            "BTC-USDT-SWAP-fundingrates-2026-09.zip"
        );
        // Funding files are not candle files.
        let listing: ArchiveListing = serde_json::from_str(BTC_FUNDING_MONTHS).unwrap();
        assert!(
            listing
                .files(Contract::of(btc()).unwrap(), Candles)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn the_archive_holds_no_funding_of_an_x_perp() {
        let err = listing(XPERP_FILE)
            .files(Contract::of(xperp()).unwrap(), Funding)
            .unwrap_err()
            .to_string();
        assert!(err.contains("swaps only"), "{err}");
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn an_archive_day_holds_the_minutes_opening_on_it_in_utc_plus_8() {
        let day = date(2026, 10, 19);
        // `dateTs` of the 2026-10-19 file.
        assert_eq!(day_start(day).unwrap().as_millisecond(), 1_792_339_200_000);
        assert_eq!(day_start(day).unwrap(), at("2026-10-18T16:00:00Z"));
        assert_eq!(
            ArchivePeriod::Day.end(day).unwrap(),
            at("2026-10-19T16:00:00Z")
        );
        // A bar is in the day of its open, the minute before its close.
        assert_eq!(
            archive_day(at("2026-10-18T16:00:00Z")).unwrap(),
            date(2026, 10, 18)
        );
        assert_eq!(
            ArchivePeriod::Day
                .of(at("2026-10-18T16:00:00.000000001Z"))
                .unwrap(),
            day
        );
        assert_eq!(archive_day(at("2026-10-19T16:00:00Z")).unwrap(), day);
    }

    #[test]
    fn an_archive_month_holds_the_minutes_opening_in_it_in_utc_plus_8() {
        let month = date(2026, 9, 1);
        // `dateTs` of the 2026-09 file.
        assert_eq!(
            day_start(month).unwrap().as_millisecond(),
            1_788_192_000_000
        );
        assert_eq!(day_start(month).unwrap(), at("2026-08-31T16:00:00Z"));
        assert_eq!(
            ArchivePeriod::Month.end(month).unwrap(),
            at("2026-09-30T16:00:00Z")
        );
        assert_eq!(
            ArchivePeriod::Month.of(at("2026-08-31T16:00:00Z")).unwrap(),
            date(2026, 8, 1)
        );
        for close in ["2026-08-31T16:00:00.000000001Z", "2026-09-30T16:00:00Z"] {
            assert_eq!(
                ArchivePeriod::Month.of(at(close)).unwrap(),
                month,
                "{close}"
            );
        }
    }

    #[test]
    fn a_listing_covers_ten_months_or_ten_days() {
        assert_eq!(
            ArchivePeriod::Month.last_listed(date(2024, 10, 1)).unwrap(),
            date(2025, 7, 1)
        );
        assert_eq!(
            ArchivePeriod::Day.last_listed(date(2026, 9, 21)).unwrap(),
            date(2026, 9, 30)
        );
    }

    /// The minute bar of BTC closing at `close`.
    fn bar_at(close: Timestamp) -> Bar {
        let instrument = btc_instrument();
        let ts_init = UnixNanos::from(close);
        let price = instrument.make_price(100_000.0);
        Bar::new(
            BarType::from(BTC_MINUTES),
            price,
            price,
            price,
            price,
            instrument.make_qty(1.0, None),
            UnixNanos::from(close - SignedDuration::from_mins(1)),
            ts_init,
        )
    }

    /// A file named after its period: `2026-09` for a month, `2026-09-01`
    /// for a day.
    fn file_of(period: ArchivePeriod, first: Date) -> ArchiveFile {
        let filename = match period {
            ArchivePeriod::Month => first.strftime("%Y-%m").to_string(),
            ArchivePeriod::Day => first.to_string(),
        };
        ArchiveFile {
            date_ts: day_start(first).unwrap().as_millisecond().to_string(),
            filename,
            size_mb: "0".to_string(),
            url: String::new(),
        }
    }

    /// An OKX-shaped archive's listings: the monthly files of the months up
    /// to the one starting on `months`, the daily files of the days up to
    /// `days`. Records the listings asked for.
    fn archive_files(
        listed: &mut Vec<(ArchivePeriod, Date, Date)>,
        months: Date,
        days: Date,
    ) -> impl AsyncFnMut(ArchivePeriod, Date, Date) -> anyhow::Result<HashMap<Date, ArchiveFile>>
    {
        async move |period: ArchivePeriod, first: Date, last: Date| {
            listed.push((period, first, last));
            let published = match period {
                ArchivePeriod::Month => months,
                ArchivePeriod::Day => days,
            };
            let mut files = HashMap::new();
            let mut first = first;
            while first <= last.min(published) {
                files.insert(first, file_of(period, first));
                first = period.next(first)?;
            }
            Ok(files)
        }
    }

    /// The bars of an OKX-shaped file closing in `[from, to]`: one closing
    /// at 04:00 UTC (noon in UTC+8) of each archive day. Records the file
    /// and window asked for.
    fn file_bars(
        asked: &mut Vec<(String, Timestamp, Timestamp)>,
    ) -> impl AsyncFnMut(&ArchiveFile, Timestamp, Timestamp) -> anyhow::Result<Vec<Bar>> {
        async move |file: &ArchiveFile, from: Timestamp, to: Timestamp| {
            asked.push((file.filename.clone(), from, to));
            let mut bars = Vec::new();
            let mut day = archive_day(from)?;
            while day_start(day)? < to {
                let noon = day_start(day)? + SignedDuration::from_hours(12);
                if from <= noon && noon <= to {
                    bars.push(bar_at(noon));
                }
                day = day.tomorrow()?;
            }
            Ok(bars)
        }
    }

    /// Whether each window starts 1 ns after the end of the one before.
    fn contiguous(asked: &[(String, Timestamp, Timestamp)]) -> bool {
        asked
            .windows(2)
            .all(|pair| pair[1].1 == pair[0].2 + SignedDuration::from_nanos(1))
    }

    fn stored_closes(catalog: &mut ParquetDataCatalog) -> Vec<Timestamp> {
        catalog
            .bars(Some(vec![BTC_MINUTES.to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|bar| Timestamp::from(bar.ts_init))
            .collect()
    }

    async fn fill(
        catalog: &mut ParquetDataCatalog,
        start: &str,
        end: &str,
        candle_files: impl AsyncFnMut(
            ArchivePeriod,
            Date,
            Date,
        ) -> anyhow::Result<HashMap<Date, ArchiveFile>>,
        file_bars: impl AsyncFnMut(&ArchiveFile, Timestamp, Timestamp) -> anyhow::Result<Vec<Bar>>,
    ) -> anyhow::Result<()> {
        let mut file_bars = file_bars;
        fill_from_archive(
            catalog,
            &btc_instrument(),
            Candles,
            (NautilusDataType::Bar, BTC_MINUTES.to_string()),
            at(start),
            at(end),
            candle_files,
            async |file: &ArchiveFile, from: Timestamp, to: Timestamp| {
                let bars = file_bars(file, from, to).await?;
                Ok(bars.into_iter().map(Data::Bar).collect())
            },
        )
        .await
    }

    fn new_catalog(dir: &tempfile::TempDir) -> ParquetDataCatalog {
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
    }

    #[tokio::test]
    async fn complete_months_come_from_monthly_files_and_the_rest_from_daily_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        // 2026-09 not yet published as a month; days up to 2026-10-02.
        fill(
            &mut catalog,
            "2026-07-10T06:00:00Z",
            "2026-10-03T18:00:00Z",
            archive_files(&mut listed, date(2026, 8, 1), date(2026, 10, 2)),
            file_bars(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(
            listed,
            [
                (Month, date(2026, 7, 1), date(2026, 10, 1)),
                (Day, date(2026, 9, 1), date(2026, 9, 10)),
                (Day, date(2026, 9, 11), date(2026, 9, 20)),
                (Day, date(2026, 9, 21), date(2026, 9, 30)),
                (Day, date(2026, 10, 1), date(2026, 10, 4)),
            ]
        );
        // The first window starts with the archive day of the start.
        assert_eq!(
            asked[..3],
            [
                (
                    "2026-07".to_string(),
                    at("2026-07-09T16:00:00.000000001Z"),
                    at("2026-07-31T16:00:00Z")
                ),
                (
                    "2026-08".to_string(),
                    at("2026-07-31T16:00:00.000000001Z"),
                    at("2026-08-31T16:00:00Z")
                ),
                (
                    "2026-09-01".to_string(),
                    at("2026-08-31T16:00:00.000000001Z"),
                    at("2026-09-01T16:00:00Z")
                ),
            ]
        );
        assert_eq!(asked.len(), 2 + 30 + 2);
        assert_eq!(
            asked.last().unwrap(),
            &(
                "2026-10-02".to_string(),
                at("2026-10-01T16:00:00.000000001Z"),
                at("2026-10-02T16:00:00Z")
            )
        );
        assert!(contiguous(&asked));
        let closes = stored_closes(&mut catalog);
        assert_eq!(closes.len(), 22 + 31 + 30 + 2);
        assert_eq!(closes[0], at("2026-07-10T04:00:00Z"));
        assert_eq!(closes.last().unwrap(), &at("2026-10-02T04:00:00Z"));
        assert_eq!(catalog.instruments(None, None, None).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn two_years_take_one_listing_per_ten_months_and_per_ten_days() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill(
            &mut catalog,
            "2024-10-23T00:00:00Z",
            "2026-10-23T00:00:00Z",
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 21)),
            file_bars(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(
            listed,
            [
                (Month, date(2024, 10, 1), date(2025, 7, 1)),
                (Month, date(2025, 8, 1), date(2026, 5, 1)),
                (Month, date(2026, 6, 1), date(2026, 10, 1)),
                (Day, date(2026, 10, 1), date(2026, 10, 10)),
                (Day, date(2026, 10, 11), date(2026, 10, 20)),
                (Day, date(2026, 10, 21), date(2026, 10, 23)),
            ]
        );
        assert_eq!(asked.len(), 24 + 21);
        assert!(contiguous(&asked));
        let closes = stored_closes(&mut catalog);
        assert_eq!(closes[0], at("2024-10-23T04:00:00Z"));
        assert_eq!(closes.last().unwrap(), &at("2026-10-21T04:00:00Z"));
    }

    #[tokio::test]
    async fn a_fill_resumes_after_an_interruption_and_from_a_month_published_since() {
        let (start, end) = ("2026-08-10T00:00:00Z", "2026-10-05T00:00:00Z");
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        let mut bars = file_bars(&mut asked);
        let result = fill(
            &mut catalog,
            start,
            end,
            archive_files(&mut listed, date(2026, 8, 1), date(2026, 10, 4)),
            async |file: &ArchiveFile, from: Timestamp, to: Timestamp| {
                if file.filename == "2026-09-15" {
                    bail!("download failed");
                }
                bars(file, from, to).await
            },
        )
        .await;
        result.unwrap_err();
        // The days before the failed one stay stored.
        assert_eq!(
            stored_closes(&mut catalog).last().unwrap(),
            &at("2026-09-14T04:00:00Z")
        );
        // 2026-09 is published as a month since.
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill(
            &mut catalog,
            start,
            end,
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 4)),
            file_bars(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(
            listed,
            [
                (Month, date(2026, 9, 1), date(2026, 10, 1)),
                (Day, date(2026, 10, 1), date(2026, 10, 5)),
            ]
        );
        assert_eq!(
            asked[0],
            (
                "2026-09".to_string(),
                at("2026-09-14T16:00:00.000000001Z"),
                at("2026-09-30T16:00:00Z")
            )
        );
        assert_eq!(asked.len(), 1 + 4);
        assert!(contiguous(&asked));
        // As one uninterrupted fill.
        let dir = tempfile::tempdir().unwrap();
        let mut whole = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill(
            &mut whole,
            start,
            end,
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 4)),
            file_bars(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(stored_closes(&mut catalog), stored_closes(&mut whole));
        assert_eq!(stored_closes(&mut whole).len(), 22 + 30 + 4);
    }

    #[tokio::test]
    async fn a_start_on_a_day_start_stores_its_bar_from_the_file_of_the_day_before() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill(
            &mut catalog,
            "2026-10-18T16:00:00Z",
            "2026-10-19T16:00:00Z",
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 19)),
            async |file: &ArchiveFile, from: Timestamp, to: Timestamp| {
                asked.push((file.filename.clone(), from, to));
                // A day's file holds the bars closing after its start up to
                // its end included.
                let day = archive_day(from)?;
                let (first, last) = (
                    day_start(day)? + SignedDuration::from_mins(1),
                    ArchivePeriod::Day.end(day)?,
                );
                Ok([first, last]
                    .into_iter()
                    .filter(|close| from <= *close && *close <= to)
                    .map(bar_at)
                    .collect())
            },
        )
        .await
        .unwrap();
        assert_eq!(
            listed,
            [
                (Month, date(2026, 10, 1), date(2026, 10, 1)),
                (Day, date(2026, 10, 18), date(2026, 10, 19)),
            ]
        );
        assert_eq!(
            asked,
            [
                (
                    "2026-10-18".to_string(),
                    at("2026-10-17T16:00:00.000000001Z"),
                    at("2026-10-18T16:00:00Z")
                ),
                (
                    "2026-10-19".to_string(),
                    at("2026-10-18T16:00:00.000000001Z"),
                    at("2026-10-19T16:00:00Z")
                ),
            ]
        );
        assert_eq!(
            stored_closes(&mut catalog),
            [
                at("2026-10-17T16:01:00Z"),
                at("2026-10-18T16:00:00Z"),
                at("2026-10-18T16:01:00Z"),
                at("2026-10-19T16:00:00Z"),
            ]
        );
    }

    #[tokio::test]
    async fn a_day_not_published_stops_the_fill_and_the_next_run_starts_there() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (start, end) = ("2026-10-16T16:00:00.000000001Z", "2026-10-19T16:00:00Z");
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill(
            &mut catalog,
            start,
            end,
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 17)),
            file_bars(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(asked.len(), 1);
        assert_eq!(stored_closes(&mut catalog), [at("2026-10-17T04:00:00Z")]);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill(
            &mut catalog,
            start,
            end,
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 19)),
            file_bars(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(
            asked,
            [
                (
                    "2026-10-18".to_string(),
                    at("2026-10-17T16:00:00.000000001Z"),
                    at("2026-10-18T16:00:00Z")
                ),
                (
                    "2026-10-19".to_string(),
                    at("2026-10-18T16:00:00.000000001Z"),
                    at("2026-10-19T16:00:00Z")
                ),
            ]
        );
        assert_eq!(stored_closes(&mut catalog).len(), 3);
    }

    #[tokio::test]
    async fn no_file_on_the_first_day_is_an_error_naming_the_bar_type() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        let err = fill(
            &mut catalog,
            "2026-10-16T16:00:00.000000001Z",
            "2026-10-17T16:00:00Z",
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 16)),
            file_bars(&mut asked),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(BTC_MINUTES), "{err}");
    }

    const BTC: &str = "BTC-USDT-SWAP.OKX";

    /// The funding of an OKX-shaped month file at times in `[from, to]`:
    /// one at each 00:00, 08:00 and 16:00 UTC. Records the file and window
    /// asked for.
    fn file_funding(
        asked: &mut Vec<(String, Timestamp, Timestamp)>,
    ) -> impl AsyncFnMut(&ArchiveFile, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>> {
        async move |file: &ArchiveFile, from: Timestamp, to: Timestamp| {
            asked.push((file.filename.clone(), from, to));
            let eight_hours = SignedDuration::from_hours(8);
            let mut time = Timestamp::from_second(from.as_second().div_euclid(28_800) * 28_800)?;
            let mut funding = Vec::new();
            while time <= to {
                if from <= time {
                    let ts = UnixNanos::from(time);
                    funding.push(Data::FundingRate(FundingRateUpdate::new(
                        btc(),
                        Decimal::new(1, 4),
                        Some(480),
                        None,
                        ts,
                        ts,
                    )));
                }
                time += eight_hours;
            }
            Ok(funding)
        }
    }

    async fn fill_funding(
        catalog: &mut ParquetDataCatalog,
        start: &str,
        end: &str,
        list_files: impl AsyncFnMut(
            ArchivePeriod,
            Date,
            Date,
        ) -> anyhow::Result<HashMap<Date, ArchiveFile>>,
        file_data: impl AsyncFnMut(&ArchiveFile, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
    ) -> anyhow::Result<()> {
        fill_from_archive(
            catalog,
            &btc_instrument(),
            Funding,
            (NautilusDataType::FundingRateUpdate, BTC.to_string()),
            at(start),
            at(end),
            list_files,
            file_data,
        )
        .await
    }

    fn stored_funding(catalog: &mut ParquetDataCatalog) -> Vec<Timestamp> {
        catalog
            .funding_rates(Some(vec![BTC.to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|update| Timestamp::from(update.ts_init))
            .collect()
    }

    #[tokio::test]
    async fn funding_comes_from_monthly_files_up_to_the_last_published_month() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        // 2026-10 not published yet.
        fill_funding(
            &mut catalog,
            "2026-07-10T06:00:00Z",
            "2026-10-23T00:00:00Z",
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 21)),
            file_funding(&mut asked),
        )
        .await
        .unwrap();
        // No daily files.
        assert_eq!(listed, [(Month, date(2026, 7, 1), date(2026, 10, 1))]);
        // A month's file holds the funding from its start, midnight in
        // UTC+8, to before its end; the first window starts with the archive
        // day of the start.
        assert_eq!(
            asked,
            [
                (
                    "2026-07".to_string(),
                    at("2026-07-09T16:00:00Z"),
                    at("2026-07-31T15:59:59.999999999Z")
                ),
                (
                    "2026-08".to_string(),
                    at("2026-07-31T16:00:00Z"),
                    at("2026-08-31T15:59:59.999999999Z")
                ),
                (
                    "2026-09".to_string(),
                    at("2026-08-31T16:00:00Z"),
                    at("2026-09-30T15:59:59.999999999Z")
                ),
            ]
        );
        let stored = stored_funding(&mut catalog);
        assert_eq!(stored.len(), 3 * (22 + 31 + 30));
        assert_eq!(stored[0], at("2026-07-09T16:00:00Z"));
        assert_eq!(stored.last().unwrap(), &at("2026-09-30T08:00:00Z"));
        // Covered up to the end of the last month published, as a backtest's
        // coverage check reads it.
        let ns = |s: &str| UnixNanos::from(at(s)).as_u64();
        let missing = |to: &str| {
            catalog
                .get_missing_intervals_for_request(
                    ns("2026-07-09T16:00:00Z"),
                    ns(to),
                    &NautilusDataType::FundingRateUpdate.into(),
                    Some(BTC),
                )
                .unwrap()
        };
        assert!(missing("2026-09-30T15:59:59.999999999Z").is_empty());
        assert_eq!(
            missing("2026-09-30T16:00:00Z"),
            [(ns("2026-09-30T16:00:00Z"), ns("2026-09-30T16:00:00Z"))]
        );
        assert_eq!(catalog.instruments(None, None, None).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_funding_fill_resumes_after_the_stored_end_with_a_month_published_since() {
        let (start, end) = ("2026-07-10T06:00:00Z", "2026-10-23T00:00:00Z");
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill_funding(
            &mut catalog,
            start,
            end,
            archive_files(&mut listed, date(2026, 8, 1), date(2026, 10, 21)),
            file_funding(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(
            stored_funding(&mut catalog).last().unwrap(),
            &at("2026-08-31T08:00:00Z")
        );
        // 2026-09 is published since.
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill_funding(
            &mut catalog,
            start,
            end,
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 21)),
            file_funding(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(listed, [(Month, date(2026, 9, 1), date(2026, 10, 1))]);
        assert_eq!(
            asked,
            [(
                "2026-09".to_string(),
                at("2026-08-31T16:00:00Z"),
                at("2026-09-30T15:59:59.999999999Z")
            )]
        );
        // As one uninterrupted fill.
        let dir = tempfile::tempdir().unwrap();
        let mut whole = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill_funding(
            &mut whole,
            start,
            end,
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 21)),
            file_funding(&mut asked),
        )
        .await
        .unwrap();
        assert_eq!(stored_funding(&mut catalog), stored_funding(&mut whole));
    }

    #[tokio::test]
    async fn no_funding_file_for_the_first_month_is_an_error_naming_the_series() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        let err = fill_funding(
            &mut catalog,
            "2026-10-02T00:00:00Z",
            "2026-10-23T00:00:00Z",
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 21)),
            file_funding(&mut asked),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(&format!("funding_rates of {BTC}")), "{err}");
        assert!(stored_funding(&mut catalog).is_empty());
    }

    /// OKX's funding history of the X-Perp as its API answered it on
    /// 2026-10-23 with `limit=8`: its 8 funding times from 2026-10-20T08:00Z
    /// to 2026-10-22T16:00Z, newest first.
    const XPERP_FUNDING: &str =
        include_str!("testdata/BTC-USD_UM_XPERP-310404-funding-rate-history.json");

    /// A local OKX answering the funding history requests of Nautilus'
    /// client from `XPERP_FUNDING`, as OKX pages it: the newest `page`
    /// funding times before the request's `after`, newest first. Gives the
    /// client and the `after` of each request, in milliseconds.
    fn okx_funding(page: usize) -> (OKXHttpClient, Arc<Mutex<Vec<u64>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let asked = Arc::new(Mutex::new(Vec::new()));
        let afters = asked.clone();
        thread::spawn(move || {
            let recorded: serde_json::Value = serde_json::from_str(XPERP_FUNDING).unwrap();
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut head = String::new();
                for line in BufReader::new(&stream).lines() {
                    let line = line.unwrap();
                    if line.is_empty() {
                        break;
                    }
                    head.push_str(&line);
                }
                let after: u64 = head
                    .split(['?', '&', ' '])
                    .find_map(|param| param.strip_prefix("after="))
                    .unwrap()
                    .parse()
                    .unwrap();
                afters.lock().unwrap().push(after);
                let data: Vec<_> = recorded["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|row| {
                        row["fundingTime"].as_str().unwrap().parse::<u64>().unwrap() < after
                    })
                    .take(page)
                    .collect();
                let body = serde_json::json!({"code": "0", "msg": "", "data": data}).to_string();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let client = OKXHttpClient::new(Some(url), 5, 0, 1, 1, OKXEnvironment::Live, None).unwrap();
        (client, asked)
    }

    const XPERP: &str = "BTC-USD_UM_XPERP-310404.OKX";

    async fn fill_xperp_funding(
        catalog: &mut ParquetDataCatalog,
        client: &OKXHttpClient,
        start: &str,
        end: &str,
    ) -> anyhow::Result<()> {
        fill_funding_from_api(
            catalog,
            &xperp_instrument(),
            at(start),
            at(end),
            async |before| {
                client
                    .request_funding_rates(xperp(), None, Some(before), None)
                    .await
            },
        )
        .await
    }

    fn stored_xperp_funding(catalog: &mut ParquetDataCatalog) -> Vec<FundingRateUpdate> {
        catalog
            .funding_rates(Some(vec![XPERP.to_string()]), None, None)
            .unwrap()
    }

    fn ms(s: &str) -> u64 {
        at(s).as_millisecond().try_into().unwrap()
    }

    /// The stored intervals of the X-Perp's funding.
    fn xperp_intervals(catalog: &ParquetDataCatalog) -> Vec<(u64, u64)> {
        catalog
            .get_intervals(&NautilusDataType::FundingRateUpdate.into(), Some(XPERP))
            .unwrap()
    }

    fn ns(s: &str) -> u64 {
        UnixNanos::from(at(s)).as_u64()
    }

    #[tokio::test]
    async fn api_funding_is_paged_back_to_the_window_start_each_update_with_its_interval() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (client, asked) = okx_funding(3);
        fill_xperp_funding(
            &mut catalog,
            &client,
            "2026-10-21T00:00:00Z",
            "2026-10-23T00:00:00Z",
        )
        .await
        .unwrap();
        // Each page ends 1 ms after the oldest time of the one before, until
        // one reaches before the start.
        assert_eq!(
            *asked.lock().unwrap(),
            [
                ms("2026-10-23T00:00:00.001Z"),
                ms("2026-10-22T00:00:00.001Z"),
                ms("2026-10-21T08:00:00.001Z"),
            ]
        );
        let stored = stored_xperp_funding(&mut catalog);
        let times: Vec<_> = stored
            .iter()
            .map(|update| Timestamp::from(update.ts_event))
            .collect();
        assert_eq!(
            times,
            [
                "2026-10-21T00:00:00Z",
                "2026-10-21T08:00:00Z",
                "2026-10-21T16:00:00Z",
                "2026-10-22T00:00:00Z",
                "2026-10-22T08:00:00Z",
                "2026-10-22T16:00:00Z",
            ]
            .map(at)
        );
        // As Nautilus parses them: the rate, the interval from the time
        // before, `ts_init` the funding time, no next funding time.
        for update in &stored {
            assert_eq!(update.instrument_id, xperp());
            assert_eq!(update.interval, Some(480));
            assert_eq!(update.ts_init, update.ts_event);
            assert_eq!(update.next_funding_ns, None);
        }
        assert_eq!(stored[0].rate.to_string(), "0.0001");
        assert_eq!(stored[5].rate.to_string(), "0.0001453767332828");
        // Covered from the start to the newest funding time.
        assert_eq!(
            xperp_intervals(&catalog),
            [(ns("2026-10-21T00:00:00Z"), ns("2026-10-22T16:00:00Z"))]
        );
        assert_eq!(catalog.instruments(None, None, None).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn api_funding_goes_back_as_far_as_okx_keeps_it_with_an_interval() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (client, asked) = okx_funding(3);
        fill_xperp_funding(
            &mut catalog,
            &client,
            "2026-10-01T00:00:00Z",
            "2026-10-23T00:00:00Z",
        )
        .await
        .unwrap();
        // The last page holds only the oldest time OKX keeps.
        assert_eq!(asked.lock().unwrap().len(), 5);
        assert_eq!(
            asked.lock().unwrap().last(),
            Some(&ms("2026-10-20T08:00:00.001Z"))
        );
        // That time has no interval, so the funding starts after it.
        let stored = stored_xperp_funding(&mut catalog);
        assert_eq!(stored.len(), 7);
        assert_eq!(
            xperp_intervals(&catalog),
            [(ns("2026-10-20T16:00:00Z"), ns("2026-10-22T16:00:00Z"))]
        );
    }

    #[tokio::test]
    async fn an_api_funding_fill_resumes_after_the_stored_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (client, _) = okx_funding(3);
        let start = "2026-10-21T00:00:00Z";
        fill_xperp_funding(&mut catalog, &client, start, "2026-10-21T12:00:00Z")
            .await
            .unwrap();
        let (client, asked) = okx_funding(3);
        fill_xperp_funding(&mut catalog, &client, start, "2026-10-23T00:00:00Z")
            .await
            .unwrap();
        assert_eq!(
            *asked.lock().unwrap(),
            [
                ms("2026-10-23T00:00:00.001Z"),
                ms("2026-10-22T00:00:00.001Z"),
            ]
        );
        assert_eq!(
            xperp_intervals(&catalog),
            [
                (ns("2026-10-21T00:00:00Z"), ns("2026-10-21T08:00:00Z")),
                (
                    ns("2026-10-21T08:00:00.000000001Z"),
                    ns("2026-10-22T16:00:00Z")
                ),
            ]
        );
        // As one uninterrupted fill.
        let dir = tempfile::tempdir().unwrap();
        let mut whole = new_catalog(&dir);
        let (client, _) = okx_funding(3);
        fill_xperp_funding(&mut whole, &client, start, "2026-10-23T00:00:00Z")
            .await
            .unwrap();
        assert_eq!(
            stored_xperp_funding(&mut catalog),
            stored_xperp_funding(&mut whole)
        );
    }

    #[tokio::test]
    async fn an_api_funding_fill_no_longer_reaching_the_stored_end_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let ts = UnixNanos::from(at("2026-10-19T00:00:00Z"));
        let stored = FundingRateUpdate::new(xperp(), Decimal::new(1, 4), Some(480), None, ts, ts);
        catalog
            .write_to_parquet(&[stored], None, None, None)
            .unwrap();
        let (client, _) = okx_funding(3);
        let err = fill_xperp_funding(
            &mut catalog,
            &client,
            "2026-10-19T00:00:00Z",
            "2026-10-23T00:00:00Z",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(XPERP), "{err}");
        assert!(err.contains("hole"), "{err}");
        assert_eq!(stored_xperp_funding(&mut catalog).len(), 1);
    }

    #[tokio::test]
    async fn no_api_funding_on_a_first_fill_is_an_error_naming_the_series() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (client, _) = okx_funding(3);
        let err = fill_xperp_funding(
            &mut catalog,
            &client,
            "2026-10-01T00:00:00Z",
            "2026-10-20T00:00:00Z",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(&format!("funding_rates of {XPERP}")), "{err}");
    }

    /// Fills `instrument`'s minute bars and funding over `[start, end]` with
    /// a fake archive whose funding months run to `funding_months` and
    /// candle days to 2026-10-21, and `request_funding` as OKX's API.
    /// Records the module of each listing asked for.
    async fn fill_series_of(
        catalog: &mut ParquetDataCatalog,
        instrument: &InstrumentAny,
        start: &str,
        end: &str,
        funding_months: Date,
        modules: &mut Vec<ArchiveModule>,
        request_funding: impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<FundingRateUpdate>>,
    ) -> anyhow::Result<()> {
        let (mut listed, mut funding_asked, mut bars_asked) = (Vec::new(), Vec::new(), Vec::new());
        let mut funding_files = archive_files(&mut listed, funding_months, date(2026, 10, 21));
        let mut candles_listed = Vec::new();
        let mut candle_files =
            archive_files(&mut candles_listed, date(2026, 9, 1), date(2026, 10, 21));
        let mut file_funding = file_funding(&mut funding_asked);
        let mut file_bars = file_bars(&mut bars_asked);
        let bar_type = BarType::from(format!("{}-1-MINUTE-LAST-EXTERNAL", instrument.id()));
        fill_series(
            catalog,
            instrument,
            bar_type,
            at(start),
            at(end),
            async |module, period, first, last| {
                modules.push(module);
                match module {
                    Funding => funding_files(period, first, last).await,
                    Candles => candle_files(period, first, last).await,
                }
            },
            async |module, file: &ArchiveFile, from, to| match module {
                Funding => file_funding(file, from, to).await,
                Candles => {
                    let bars = file_bars(file, from, to).await?;
                    Ok(bars.into_iter().map(Data::Bar).collect())
                }
            },
            request_funding,
        )
        .await
    }

    fn bar_intervals(catalog: &ParquetDataCatalog, instrument: &InstrumentAny) -> Vec<(u64, u64)> {
        let bar_type = format!("{}-1-MINUTE-LAST-EXTERNAL", instrument.id());
        catalog
            .get_intervals(&NautilusDataType::Bar.into(), Some(&bar_type))
            .unwrap()
    }

    #[tokio::test]
    async fn the_funding_is_filled_before_the_bars_and_a_failed_one_stores_no_bars() {
        // A swap: its funding from the archive's months, then the API, then
        // its bars.
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut modules = Vec::new();
        let mut requests = 0;
        fill_series_of(
            &mut catalog,
            &btc_instrument(),
            "2026-09-02T00:00:00Z",
            "2026-09-03T00:00:00Z",
            date(2026, 9, 1),
            &mut modules,
            async |_| {
                requests += 1;
                Ok(Vec::new())
            },
        )
        .await
        .unwrap();
        assert_eq!(modules, [Funding, Candles]);
        assert_eq!(requests, 0);
        assert!(!stored_funding(&mut catalog).is_empty());
        assert!(!bar_intervals(&catalog, &btc_instrument()).is_empty());
        // A swap starting in a month the archive has not published: no
        // funding, so no bars.
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut modules = Vec::new();
        let err = fill_series_of(
            &mut catalog,
            &btc_instrument(),
            "2026-10-02T00:00:00Z",
            "2026-10-03T00:00:00Z",
            date(2026, 9, 1),
            &mut modules,
            async |_| Ok(Vec::new()),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(&format!("funding_rates of {BTC}")), "{err}");
        assert_eq!(modules, [Funding]);
        assert!(bar_intervals(&catalog, &btc_instrument()).is_empty());
        // An X-Perp over a window older than OKX's funding history: no
        // funding, so no bars.
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut modules = Vec::new();
        let (client, _) = okx_funding(3);
        let err = fill_series_of(
            &mut catalog,
            &xperp_instrument(),
            "2026-09-02T00:00:00Z",
            "2026-09-03T00:00:00Z",
            date(2026, 9, 1),
            &mut modules,
            async |before| {
                client
                    .request_funding_rates(xperp(), None, Some(before), None)
                    .await
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(&format!("funding_rates of {XPERP}")), "{err}");
        assert!(modules.is_empty());
        assert!(bar_intervals(&catalog, &xperp_instrument()).is_empty());
    }

    #[tokio::test]
    async fn api_funding_is_not_asked_for_a_window_the_stored_funding_covers() {
        // The archive's months cover the swap's funding up to `end`, older
        // than OKX's API keeps.
        let (start, end) = ("2026-07-10T06:00:00Z", "2026-08-15T00:00:00Z");
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (mut listed, mut asked) = (Vec::new(), Vec::new());
        fill_funding(
            &mut catalog,
            start,
            end,
            archive_files(&mut listed, date(2026, 9, 1), date(2026, 10, 21)),
            file_funding(&mut asked),
        )
        .await
        .unwrap();
        let stored = stored_funding(&mut catalog);
        let mut requests = 0;
        fill_funding_from_api(
            &mut catalog,
            &btc_instrument(),
            at(start),
            at(end),
            async |_| {
                requests += 1;
                Ok(Vec::new())
            },
        )
        .await
        .unwrap();
        assert_eq!(requests, 0);
        assert_eq!(stored_funding(&mut catalog), stored);
    }
}
