//! OKX history: the instruments of OKX swaps and their quotes, filled into
//! the catalog one UTC day at a time.
//!
//! Instruments come from OKX's public REST API through the adapter's
//! `OKXRawHttpClient` and `parse_instrument_any`, with the margins of
//! OKX's first position tier. OKX has no quote history, so quotes are
//! rebuilt from OKX's order-book archive (see `archive`): one file per
//! instrument and UTC day, downloaded to a temporary file and read as a
//! stream. The archive API, the download and the conversion are ours, to be
//! removed if Nautilus gains an equivalent.

use std::{collections::HashMap, fs::File, io::Write, num::NonZeroU32, time::Duration};

use anyhow::{Context, bail, ensure};
use jiff::{
    SignedDuration, Timestamp,
    civil::{Date, date},
    tz::TimeZone,
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::QuoteTick,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{http::HttpClient, ratelimiter::quota::Quota};
use nautilus_okx::{
    common::{
        consts::OKX_VENUE,
        enums::{OKXEnvironment, OKXInstrumentType, OKXTradeMode},
        models::OKXInstrument,
        parse::parse_instrument_any,
    },
    http::{
        client::OKXRawHttpClient,
        models::OKXPositionTier,
        query::{GetInstrumentsParams, GetPositionTiersParams},
    },
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::HasCatalogDataType,
};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::fill_catalog;

mod archive;

/// OKX's archive of history files (public, no key).
const ARCHIVE_API: &str = "https://www.okx.com/api/v5/public/market-data-history";

/// Where the archive's files are served from; any other URL is refused.
const ARCHIVE_FILES: &str = "https://static.okx.com/";

/// The archive module of the order book with 400 levels a side.
const ORDER_BOOK_MODULE: &str = "4";

/// The first day of the order-book archive (BTC-USDT-SWAP; nothing before).
const ARCHIVE_START: Date = date(2023, 1, 1);

/// The archive API's limit: 5 requests per 2 seconds.
const ARCHIVE_REQUEST_PERIOD: Duration = Duration::from_millis(400);
const ARCHIVE_REQUEST_BURST: NonZeroU32 = NonZeroU32::new(5).expect("non-zero");

/// The timeout of one archive API request.
const ARCHIVE_REQUEST_TIMEOUT_SECS: u64 = 30;

/// The timeout of one day's download, whole body included (up to about
/// 525 MB).
const DOWNLOAD_TIMEOUT_SECS: u64 = 3600;

/// How often a download logs its progress.
const PROGRESS_BYTES: u64 = 100 * 1024 * 1024;

/// A connection to OKX's public API and archive, for history.
#[derive(Debug)]
pub struct OkxHistory {
    client: OKXRawHttpClient,
    archive: HttpClient,
}

impl OkxHistory {
    /// The clients of OKX's public REST API and of its archive.
    ///
    /// # Errors
    ///
    /// Returns an error if a client cannot be built.
    pub fn new() -> anyhow::Result<Self> {
        let client = OKXRawHttpClient::new(None, 60, 3, 1000, 10_000, OKXEnvironment::Live, None)
            .context("building the OKX HTTP client")?;
        let quota = Quota::with_period(ARCHIVE_REQUEST_PERIOD)
            .context("the archive request period is zero")?
            .allow_burst(ARCHIVE_REQUEST_BURST);
        let archive = HttpClient::builder()
            .default_quota(quota)
            .timeout_secs(DOWNLOAD_TIMEOUT_SECS)
            .build()
            .context("building the OKX archive client")?;
        Ok(Self { client, archive })
    }

    /// Whether `id` is OKX's, so its history comes from here.
    #[must_use]
    pub fn serves(id: InstrumentId) -> bool {
        id.venue == *OKX_VENUE
    }

    /// Fills `catalog` with the instruments of `ids`, OKX swaps, and their
    /// quotes over `[start, end]`, one UTC day at a time (see
    /// `fill_by_day`).
    ///
    /// # Errors
    ///
    /// Returns an error if `start` is before the archive's first day,
    /// checked before any request, if an id is not an OKX swap, or as
    /// `fill_by_day`.
    pub async fn fill(
        &self,
        catalog: &mut ParquetDataCatalog,
        ids: &[InstrumentId],
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<()> {
        ensure_in_archive(start)?;
        let mut instruments = Vec::with_capacity(ids.len());
        for &id in ids {
            instruments.push(self.instrument(id).await?);
        }
        fill_by_day(
            catalog,
            instruments,
            start,
            end,
            async |instrument, from, to| self.day_quotes(instrument, from, to).await,
        )
        .await
    }

    /// The instrument of `id`, an OKX swap, with the initial and
    /// maintenance margins of OKX's first position tier (cross margin).
    async fn instrument(&self, id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        let family = swap_family(id)?;
        let symbol = id.symbol.to_string();
        let raw = self
            .client
            .get_instruments(GetInstrumentsParams {
                inst_type: OKXInstrumentType::Swap,
                inst_id: Some(symbol.clone()),
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the instrument {id} from OKX"))?;
        let [raw] = raw.as_slice() else {
            bail!("OKX returned {} instruments for {id}, not one", raw.len());
        };
        let tiers = self
            .client
            .get_position_tiers(GetPositionTiersParams {
                inst_type: OKXInstrumentType::Swap,
                td_mode: OKXTradeMode::Cross,
                inst_family: Some(family.to_string()),
                tier: Some("1".to_string()),
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the position tiers of {id} from OKX"))?;
        instrument_with_margins(raw, &tiers, get_atomic_clock_realtime().get_time_ns())
    }

    /// The quotes of `instrument` over `[from, to]`, a window within one
    /// UTC day, from that day's archive file; `None` if the archive has no
    /// file for it. Logs the download's progress and the day's counts.
    async fn day_quotes(
        &self,
        instrument: &InstrumentAny,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<Option<Vec<QuoteTick>>> {
        let id = instrument.id();
        let day = utc_date(from);
        ensure!(
            to <= day_start(day.tomorrow()?)?,
            "the window [{from}, {to}] of {id} spans more than the UTC day {day}"
        );
        let Some(file) = self.day_file(id, day).await? else {
            log::warn!("{id}: OKX's archive has no order-book file for {day}");
            return Ok(None);
        };
        log::info!(
            "{id} {day}: downloading {} ({} MB)",
            file.filename,
            file.size_mb
        );
        let mut download = tempfile::tempfile().context("creating a temporary file")?;
        self.download(&file.url, &mut download)
            .await
            .with_context(|| format!("downloading {}", file.filename))?;
        let instrument = instrument.clone();
        let (from, through) = (UnixNanos::from(from), UnixNanos::from(to));
        let made = tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom};
            download.seek(SeekFrom::Start(0))?;
            archive::quotes_from_archive(download, &instrument, from, through)
        })
        .await
        .context("reading the archive file")?
        .with_context(|| format!("reading {}", file.filename))?;
        log::info!(
            "{id} {day}: {} quotes; dropped {} crossed and {} one-sided tops",
            made.quotes.len(),
            made.crossed,
            made.one_sided,
        );
        Ok(Some(made.quotes))
    }

    /// The archive's order-book file of `id` for `day`, if any.
    async fn day_file(&self, id: InstrumentId, day: Date) -> anyhow::Result<Option<ArchiveFile>> {
        let day_ms = day_start(day)?.as_millisecond().to_string();
        let params: HashMap<String, Vec<String>> = [
            ("module", ORDER_BOOK_MODULE),
            ("instType", "SWAP"),
            ("instFamilyList", swap_family(id)?),
            ("dateAggrType", "daily"),
            ("begin", &day_ms),
            ("end", &day_ms),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), vec![value.to_string()]))
        .collect();
        let response = self
            .archive
            .get(
                ARCHIVE_API.to_string(),
                Some(&params),
                None,
                Some(ARCHIVE_REQUEST_TIMEOUT_SECS),
                None,
            )
            .await
            .with_context(|| format!("asking OKX's archive for the {day} file of {id}"))?;
        ensure!(
            response.status.is_success(),
            "OKX's archive answered HTTP {} for the {day} file of {id}",
            response.status.as_u16()
        );
        let listing: ArchiveListing = serde_json::from_slice(&response.body)
            .with_context(|| format!("parsing OKX's archive listing for {day}"))?;
        listing.order_book_file(id)
    }

    /// Streams `url`, a file of the archive, into `file`, logging the
    /// progress.
    async fn download(&self, url: &str, file: &mut File) -> anyhow::Result<()> {
        ensure!(
            url.starts_with(ARCHIVE_FILES),
            "the archive file {url} is not under {ARCHIVE_FILES}"
        );
        let mut response = self.archive.get_stream(url.to_string()).await?;
        ensure!(response.status().is_success(), "HTTP {}", response.status());
        let mut written = 0u64;
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk)?;
            let before = written;
            written += chunk.len() as u64;
            if written / PROGRESS_BYTES > before / PROGRESS_BYTES {
                log::info!("downloaded {} MB", written / (1024 * 1024));
            }
        }
        Ok(())
    }
}

/// Fails if `start` is before the first day of OKX's order-book archive.
fn ensure_in_archive(start: Timestamp) -> anyhow::Result<()> {
    let archive_start = day_start(ARCHIVE_START)?;
    ensure!(
        start >= archive_start,
        "start {start} is before OKX's order-book archive, which begins on {ARCHIVE_START}"
    );
    Ok(())
}

/// Fills `catalog` with `instruments` and their quotes over `[start, end]`,
/// one instrument and one UTC day at a time: `fill_catalog` over
/// `[start, the day's end]`, so at most one day of quotes is in memory and
/// each call resumes where the catalog stopped. `day_quotes` answers a
/// window within one UTC day, `None` if the archive has no file for that
/// day. An instrument starts on the day it resumes in and stops at the
/// first day the archive has no file for (not published yet).
///
/// # Errors
///
/// As `fill_catalog`: an instrument with no file on its first day is an
/// empty first response.
async fn fill_by_day(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,
    start: Timestamp,
    end: Timestamp,
    mut day_quotes: impl AsyncFnMut(
        &InstrumentAny,
        Timestamp,
        Timestamp,
    ) -> anyhow::Result<Option<Vec<QuoteTick>>>,
) -> anyhow::Result<()> {
    let quotes_type = QuoteTick::catalog_data_type().into();
    for instrument in instruments {
        let id = instrument.id();
        let last = catalog
            .query_last_timestamp(&quotes_type, Some(&id.to_string()))
            .with_context(|| format!("reading the stored range of the quotes of {id}"))?;
        // `fill_catalog` resumes 1 ns after the stored end.
        let begin = last.map_or(start, |last| {
            Timestamp::from(UnixNanos::from(last)) + SignedDuration::from_nanos(1)
        });
        let mut day = utc_date(begin);
        while day_start(day)? < end {
            let day_end = day_start(day.tomorrow()?)?.min(end);
            let mut published = true;
            fill_catalog(
                catalog,
                vec![instrument.clone()],
                start,
                day_end,
                async |_, from, to| {
                    let quotes = day_quotes(&instrument, from, to).await?;
                    published = quotes.is_some();
                    Ok(quotes.unwrap_or_default())
                },
            )
            .await?;
            if !published {
                log::info!("{id}: stopping at {day}, which OKX has not published");
                break;
            }
            day = day.tomorrow()?;
        }
    }
    Ok(())
}

/// The instrument of `raw` with the initial and maintenance margins
/// (`imr`, `mmr`) of the first of `tiers`, through the adapter's
/// `parse_instrument_any`, which leaves them at 0 otherwise.
fn instrument_with_margins(
    raw: &OKXInstrument,
    tiers: &[OKXPositionTier],
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let id = raw.inst_id;
    let tier = tiers
        .iter()
        .find(|tier| tier.tier == "1")
        .with_context(|| format!("OKX returned no first position tier for {id}"))?;
    let rate = |value: &str, name: &str| {
        value
            .parse::<Decimal>()
            .with_context(|| format!("the {name} {value:?} of the first tier of {id}"))
    };
    let (imr, mmr) = (rate(&tier.imr, "imr")?, rate(&tier.mmr, "mmr")?);
    ensure!(
        imr > Decimal::ZERO && mmr > Decimal::ZERO,
        "the first tier of {id} has margins {imr} and {mmr}, not both positive"
    );
    parse_instrument_any(raw, Some(imr), Some(mmr), ts_init)?
        .with_context(|| format!("{id} is not an instrument Nautilus supports"))
}

/// The instrument family of the OKX swap `id` (`BTC-USDT` of
/// `BTC-USDT-SWAP.OKX`), the archive's and the tiers' key.
fn swap_family(id: InstrumentId) -> anyhow::Result<&'static str> {
    ensure!(OkxHistory::serves(id), "{id} is not on {}", *OKX_VENUE);
    id.symbol
        .inner()
        .as_str()
        .strip_suffix("-SWAP")
        .with_context(|| format!("{id} is not an OKX swap (SYMBOL-SWAP.OKX)"))
}

/// The UTC date of `ts`.
fn utc_date(ts: Timestamp) -> Date {
    ts.to_zoned(TimeZone::UTC).date()
}

/// Midnight UTC at the start of `day`.
fn day_start(day: Date) -> anyhow::Result<Timestamp> {
    Ok(day.to_zoned(TimeZone::UTC)?.timestamp())
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
struct ArchiveFile {
    filename: String,
    #[serde(rename = "sizeMB")]
    size_mb: String,
    url: String,
}

impl ArchiveListing {
    /// The order-book file of `id` in this listing, if any; an error if
    /// OKX refused the request or listed it twice.
    fn order_book_file(self, id: InstrumentId) -> anyhow::Result<Option<ArchiveFile>> {
        ensure!(
            self.code == "0",
            "OKX's archive refused the request: code {}, {}",
            self.code,
            self.msg
        );
        let prefix = format!("{}-L2orderbook-", id.symbol);
        let mut files = self
            .data
            .into_iter()
            .flat_map(|data| data.details)
            .flat_map(|detail| detail.group_details)
            .filter(|file| file.filename.starts_with(&prefix));
        let file = files.next();
        ensure!(
            files.next().is_none(),
            "OKX's archive lists more than one order-book file of {id} for one day"
        );
        Ok(file)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    const INSTRUMENT: &str = include_str!("testdata/BTC-USDT-SWAP-instrument.json");
    const TIERS: &str = include_str!("testdata/BTC-USDT-position-tiers.json");

    fn raw_instrument() -> OKXInstrument {
        serde_json::from_str(INSTRUMENT).unwrap()
    }

    fn tiers() -> Vec<OKXPositionTier> {
        serde_json::from_str(TIERS).unwrap()
    }

    /// BTC-USDT-SWAP.OKX as OKX's public API defined it on 2026-10-21,
    /// with its first tier's margins.
    pub(in crate::data::okx) fn btc_instrument() -> InstrumentAny {
        instrument_with_margins(&raw_instrument(), &tiers(), UnixNanos::default()).unwrap()
    }

    #[test]
    fn an_okx_swap_is_a_crypto_perpetual_with_its_contract_value_increments_and_tier_one_margins() {
        let InstrumentAny::CryptoPerpetual(btc) = btc_instrument() else {
            panic!("not a CryptoPerpetual");
        };
        assert_eq!(btc.id, InstrumentId::from("BTC-USDT-SWAP.OKX"));
        assert_eq!(btc.multiplier.to_string(), "0.01");
        assert_eq!(btc.price_increment.to_string(), "0.1");
        assert_eq!(btc.size_increment.to_string(), "0.01");
        assert_eq!((btc.price_precision, btc.size_precision), (1, 2));
        assert_eq!(btc.margin_init, Decimal::new(1, 2));
        assert_eq!(btc.margin_maint, Decimal::new(4, 3));
        assert_eq!(btc.settlement_currency.code.as_str(), "USDT");
    }

    #[test]
    fn an_instrument_without_a_first_tier_or_with_a_zero_margin_is_an_error() {
        let err = instrument_with_margins(&raw_instrument(), &[], UnixNanos::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("first position tier"), "{err}");
        let mut zero = tiers();
        zero[0].mmr = "0".to_string();
        let err = instrument_with_margins(&raw_instrument(), &zero, UnixNanos::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("not both positive"), "{err}");
    }

    #[test]
    fn okx_serves_the_instruments_of_its_venue_only() {
        assert!(OkxHistory::serves(btc()));
        for id in ["ESZ6.CME", "SXR8.IBIS", "BTC-USDT-SWAP.BINANCE"] {
            assert!(!OkxHistory::serves(InstrumentId::from(id)), "{id}");
        }
    }

    #[test]
    fn the_family_of_an_okx_swap_keys_the_archive() {
        let family = swap_family(InstrumentId::from("BTC-USDT-SWAP.OKX")).unwrap();
        assert_eq!(family, "BTC-USDT");
        for id in ["BTC-USDT.OKX", "BTC-USDT-SWAP.BINANCE"] {
            assert!(swap_family(InstrumentId::from(id)).is_err(), "{id}");
        }
    }

    fn listing(group_details: &str) -> ArchiveListing {
        serde_json::from_str(&format!(
            r#"{{"code":"0","msg":"","data":[{{"dateAggrType":"daily","details":[{{"ccy":"","groupDetails":[{group_details}],"instFamily":"BTC-USDT","instId":"","instType":"SWAP"}}],"totalSizeMB":"374.76","ts":"1792616059675"}}]}}"#
        ))
        .unwrap()
    }

    const BTC_FILE: &str = r#"{"dateTs":"1792368000000","filename":"BTC-USDT-SWAP-L2orderbook-400lv-2026-10-19.tar.gz","sizeMB":"374.76","url":"https://static.okx.com/cdn/okx/match/orderbook/pro/L2/400lv/daily/20261019/BTC-USDT-SWAP-L2orderbook-400lv-2026-10-19.tar.gz"}"#;

    fn btc() -> InstrumentId {
        InstrumentId::from("BTC-USDT-SWAP.OKX")
    }

    #[test]
    fn the_listing_of_a_day_gives_the_order_book_file_of_the_instrument() {
        let file = listing(BTC_FILE).order_book_file(btc()).unwrap().unwrap();
        assert_eq!(
            file.filename,
            "BTC-USDT-SWAP-L2orderbook-400lv-2026-10-19.tar.gz"
        );
        assert_eq!(file.size_mb, "374.76");
        assert!(file.url.starts_with(ARCHIVE_FILES));
    }

    #[test]
    fn a_day_not_published_lists_no_file() {
        let empty: ArchiveListing = serde_json::from_str(
            r#"{"code":"0","data":[{"dateAggrType":"daily","details":[],"totalSizeMB":"0","ts":"1792616059979"}],"msg":""}"#,
        )
        .unwrap();
        assert_eq!(empty.order_book_file(btc()).unwrap(), None);
        let other = BTC_FILE.replace("BTC-USDT-SWAP", "BTC-USDT-250101");
        assert_eq!(listing(&other).order_book_file(btc()).unwrap(), None);
    }

    #[test]
    fn a_refused_request_or_a_file_listed_twice_is_an_error() {
        let refused: ArchiveListing =
            serde_json::from_str(r#"{"code":"50076","msg":"range too wide","data":[]}"#).unwrap();
        let err = refused.order_book_file(btc()).unwrap_err().to_string();
        assert!(err.contains("50076"), "{err}");
        let twice = listing(&format!("{BTC_FILE},{BTC_FILE}"));
        assert!(twice.order_book_file(btc()).is_err());
    }

    #[test]
    fn days_are_utc() {
        let ts: Timestamp = "2026-10-19T23:59:59.999Z".parse().unwrap();
        assert_eq!(utc_date(ts), date(2026, 10, 19));
        assert_eq!(
            day_start(date(2026, 10, 20)).unwrap(),
            "2026-10-20T00:00:00Z".parse::<Timestamp>().unwrap()
        );
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    /// An OKX-shaped day source: the quote of `instrument` at noon of the
    /// window's UTC day, if within `[from, to]`; `None` (no file) for the
    /// days in `unpublished`. Records the windows asked for.
    fn day_source<'a>(
        asked: &'a mut Vec<(Timestamp, Timestamp)>,
        unpublished: &'a [Date],
    ) -> impl AsyncFnMut(
        &InstrumentAny,
        Timestamp,
        Timestamp,
    ) -> anyhow::Result<Option<Vec<QuoteTick>>>
    + 'a {
        async move |instrument: &InstrumentAny, from: Timestamp, to: Timestamp| {
            asked.push((from, to));
            let day = utc_date(from);
            if unpublished.contains(&day) {
                return Ok(None);
            }
            let noon = UnixNanos::from(day_start(day)? + SignedDuration::from_hours(12));
            let quote = QuoteTick::new(
                instrument.id(),
                instrument.make_price(100_000.0),
                instrument.make_price(100_000.1),
                instrument.make_qty(1.0, None),
                instrument.make_qty(2.0, None),
                noon,
                noon,
            );
            let inside = from <= Timestamp::from(noon) && Timestamp::from(noon) <= to;
            Ok(Some(if inside { vec![quote] } else { vec![] }))
        }
    }

    fn stored_noons(catalog: &mut ParquetDataCatalog) -> Vec<Timestamp> {
        catalog
            .quote_ticks(Some(vec![btc().to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|quote| Timestamp::from(quote.ts_init))
            .collect()
    }

    fn just_after(ts: &str) -> Timestamp {
        at(ts) + SignedDuration::from_nanos(1)
    }

    #[tokio::test]
    async fn a_window_is_filled_one_utc_day_per_fetch_each_resuming_after_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let mut asked = Vec::new();
        fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            at("2026-10-17T06:00:00Z"),
            at("2026-10-19T18:00:00Z"),
            day_source(&mut asked, &[]),
        )
        .await
        .unwrap();
        assert_eq!(
            asked,
            [
                (at("2026-10-17T06:00:00Z"), at("2026-10-18T00:00:00Z")),
                (
                    just_after("2026-10-18T00:00:00Z"),
                    at("2026-10-19T00:00:00Z")
                ),
                (
                    just_after("2026-10-19T00:00:00Z"),
                    at("2026-10-19T18:00:00Z")
                ),
            ]
        );
        assert_eq!(
            stored_noons(&mut catalog),
            [
                at("2026-10-17T12:00:00Z"),
                at("2026-10-18T12:00:00Z"),
                at("2026-10-19T12:00:00Z"),
            ]
        );
        assert_eq!(catalog.instruments(None, None, None).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_day_not_published_stops_the_fill_and_the_next_run_starts_there() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (start, end) = (at("2026-10-17T00:00:00Z"), at("2026-10-20T00:00:00Z"));
        let mut asked = Vec::new();
        let unpublished = [date(2026, 10, 18), date(2026, 10, 19)];
        fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            start,
            end,
            day_source(&mut asked, &unpublished),
        )
        .await
        .unwrap();
        assert_eq!(asked.len(), 2);
        assert_eq!(stored_noons(&mut catalog), [at("2026-10-17T12:00:00Z")]);
        let mut asked = Vec::new();
        fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            start,
            end,
            day_source(&mut asked, &[]),
        )
        .await
        .unwrap();
        assert_eq!(
            asked,
            [
                (
                    just_after("2026-10-18T00:00:00Z"),
                    at("2026-10-19T00:00:00Z")
                ),
                (
                    just_after("2026-10-19T00:00:00Z"),
                    at("2026-10-20T00:00:00Z")
                ),
            ]
        );
        assert_eq!(stored_noons(&mut catalog).len(), 3);
    }

    #[tokio::test]
    async fn a_later_start_resumes_from_the_stored_end_day_by_day() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let mut asked = Vec::new();
        let first = (at("2026-10-17T00:00:00Z"), at("2026-10-18T00:00:00Z"));
        fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            first.0,
            first.1,
            day_source(&mut asked, &[]),
        )
        .await
        .unwrap();
        let mut asked = Vec::new();
        fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            at("2026-10-19T00:00:00Z"),
            at("2026-10-20T00:00:00Z"),
            day_source(&mut asked, &[]),
        )
        .await
        .unwrap();
        assert_eq!(
            asked,
            [
                (
                    just_after("2026-10-18T00:00:00Z"),
                    at("2026-10-19T00:00:00Z")
                ),
                (
                    just_after("2026-10-19T00:00:00Z"),
                    at("2026-10-20T00:00:00Z")
                ),
            ]
        );
        assert_eq!(stored_noons(&mut catalog).len(), 3);
    }

    #[tokio::test]
    async fn a_failed_day_keeps_the_days_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let mut asked = Vec::new();
        let mut source = day_source(&mut asked, &[]);
        let result = fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            at("2026-10-17T00:00:00Z"),
            at("2026-10-20T00:00:00Z"),
            async |instrument: &InstrumentAny, from: Timestamp, to: Timestamp| {
                if utc_date(from) == date(2026, 10, 19) {
                    bail!("download failed");
                }
                source(instrument, from, to).await
            },
        )
        .await;
        result.unwrap_err();
        assert_eq!(stored_noons(&mut catalog).len(), 2);
    }

    #[tokio::test]
    async fn no_file_on_an_instruments_first_day_is_an_error_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let mut asked = Vec::new();
        let err = fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            at("2026-10-17T00:00:00Z"),
            at("2026-10-18T00:00:00Z"),
            day_source(&mut asked, &[date(2026, 10, 17)]),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("BTC-USDT-SWAP.OKX"), "{err}");
    }

    #[test]
    fn a_window_before_the_archive_is_refused_with_its_first_day() {
        let err = ensure_in_archive(at("2022-12-31T23:59:59Z"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("2023-01-01"), "{err}");
        ensure_in_archive(at("2023-01-01T00:00:00Z")).unwrap();
    }
}
