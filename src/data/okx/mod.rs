//! OKX history: the instruments of OKX swaps and X-Perps and their quotes,
//! filled into the catalog one UTC day at a time.
//!
//! Instruments come from OKX's public REST API through the adapter's
//! `OKXRawHttpClient` and `parse_instrument_any`, with margins of 1 (see
//! `instrument_from`). OKX has no quote
//! history, so quotes are rebuilt from OKX's order-book archive (see
//! `archive`): one file per swap, or per futures family, and UTC day,
//! downloaded to a temporary file and read as a stream. The archive API,
//! the download and the conversion are ours, to be removed if Nautilus
//! gains an equivalent.

use std::{collections::HashMap, fs::File, io::Write, num::NonZeroU32, time::Duration};

use anyhow::{Context, bail, ensure};
use jiff::{
    SignedDuration, Timestamp,
    civil::{Date, date},
    tz::TimeZone,
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Data, NautilusDataType, QuoteTick},
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
    http::{client::OKXRawHttpClient, query::GetInstrumentsParams},
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

    /// Fills `catalog` with the instruments of `ids`, OKX swaps or X-Perps,
    /// and their quotes over `[start, end]`, one UTC day at a time (see
    /// `fill_by_day`).
    ///
    /// # Errors
    ///
    /// Returns an error if `start` is before the archive's first day,
    /// checked before any request, if an id is not an OKX swap or X-Perp,
    /// or as `fill_by_day`.
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

    /// The instrument of `id`, an OKX swap or X-Perp (see
    /// `instrument_from`).
    async fn instrument(&self, id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        let contract = Contract::of(id)?;
        let raw = self
            .client
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
            to <= day_last_nanosecond(day)?,
            "the window [{from}, {to}] of {id} spans more than the UTC day {day}"
        );
        let Some(file) = self.day_file(Contract::of(id)?, day).await? else {
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

    /// The archive's order-book file of `contract` for `day`, if any.
    async fn day_file(&self, contract: Contract, day: Date) -> anyhow::Result<Option<ArchiveFile>> {
        let id = contract.id;
        let day_ms = day_start(day)?.as_millisecond().to_string();
        let params: HashMap<String, Vec<String>> = [
            ("module", ORDER_BOOK_MODULE),
            ("instType", contract.archive_type()),
            ("instFamilyList", contract.family),
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
        listing.order_book_file(contract)
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
/// each call resumes where the catalog stopped. A day ends 1 ns before the
/// next midnight, whose lines are in the next day's file: closed and
/// contiguous, as Nautilus' catalog files of one UTC day
/// (`…T00-00-00-000000000Z_…T23-59-59-999999999Z.parquet`), and as its data
/// engine asks a venue for each missing closed interval. A day starting at
/// `end` is left to the next run. `day_quotes` answers a
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
        let series = [(NautilusDataType::QuoteTick, id.to_string())];
        let last = catalog
            .query_last_timestamp(&quotes_type, Some(&id.to_string()))
            .with_context(|| format!("reading the stored range of the quotes of {id}"))?;
        // `fill_catalog` resumes 1 ns after the stored end.
        let begin = last.map_or(start, |last| {
            Timestamp::from(UnixNanos::from(last)) + SignedDuration::from_nanos(1)
        });
        let mut day = utc_date(begin);
        while day_start(day)? < end {
            let day_end = day_last_nanosecond(day)?.min(end);
            let mut published = true;
            fill_catalog(
                catalog,
                vec![instrument.clone()],
                &series,
                start,
                day_end,
                async |_, _, from, to| {
                    let quotes = day_quotes(&instrument, from, to).await?;
                    published = quotes.is_some();
                    Ok(quotes.into_iter().flatten().map(Data::Quote).collect())
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

    /// The start of the name of the contract's order-book file in the
    /// archive's listing: a swap's own file, or the file of its family's
    /// futures chain, which holds one file per contract (see `archive`).
    fn listed_prefix(self) -> String {
        if self.inst_type == OKXInstrumentType::Swap {
            format!("{}-L2orderbook-", self.id.symbol)
        } else {
            format!("{}-futureschain-L2orderbook-", self.family)
        }
    }
}

/// The UTC date of `ts`.
fn utc_date(ts: Timestamp) -> Date {
    ts.to_zoned(TimeZone::UTC).date()
}

/// Midnight UTC at the start of `day`.
fn day_start(day: Date) -> anyhow::Result<Timestamp> {
    Ok(day.to_zoned(TimeZone::UTC)?.timestamp())
}

/// The last nanosecond of `day` (UTC), 1 ns before the next midnight.
fn day_last_nanosecond(day: Date) -> anyhow::Result<Timestamp> {
    Ok(day_start(day.tomorrow()?)? - SignedDuration::from_nanos(1))
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
    /// The order-book file of `contract` in this listing, if any; an
    /// error if OKX refused the request or listed it twice.
    fn order_book_file(self, contract: Contract) -> anyhow::Result<Option<ArchiveFile>> {
        let id = contract.id;
        ensure!(
            self.code == "0",
            "OKX's archive refused the request: code {}, {}",
            self.code,
            self.msg
        );
        let prefix = contract.listed_prefix();
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

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310404.OKX")
    }

    #[test]
    fn the_listing_of_a_day_gives_the_order_book_file_of_the_instrument() {
        let file = listing(BTC_FILE)
            .order_book_file(Contract::of(btc()).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            file.filename,
            "BTC-USDT-SWAP-L2orderbook-400lv-2026-10-19.tar.gz"
        );
        assert_eq!(file.size_mb, "374.76");
        assert!(file.url.starts_with(ARCHIVE_FILES));
    }

    const XPERP_FILE: &str = r#"{"dateTs":"1792368000000","filename":"BTC-USD_UM_XPERP-futureschain-L2orderbook-400lv-2026-10-19.tar.gz","sizeMB":"38.54","url":"https://static.okx.com/cdn/okx/match/orderbook/pro/L2/400lv/daily/20261019/BTC-USD_UM_XPERP-futureschain-L2orderbook-400lv-2026-10-19.tar.gz"}"#;

    #[test]
    fn the_listing_of_a_day_gives_the_chain_file_of_a_future() {
        let xperp = Contract::of(xperp()).unwrap();
        let file = listing(XPERP_FILE).order_book_file(xperp).unwrap().unwrap();
        assert_eq!(
            file.filename,
            "BTC-USD_UM_XPERP-futureschain-L2orderbook-400lv-2026-10-19.tar.gz"
        );
        let other = XPERP_FILE.replace("BTC-USD_UM_XPERP", "ETH-USD_UM_XPERP");
        assert_eq!(listing(&other).order_book_file(xperp).unwrap(), None);
        assert_eq!(listing(BTC_FILE).order_book_file(xperp).unwrap(), None);
    }

    #[test]
    fn a_day_not_published_lists_no_file() {
        let empty: ArchiveListing = serde_json::from_str(
            r#"{"code":"0","data":[{"dateAggrType":"daily","details":[],"totalSizeMB":"0","ts":"1792616059979"}],"msg":""}"#,
        )
        .unwrap();
        assert_eq!(
            empty.order_book_file(Contract::of(btc()).unwrap()).unwrap(),
            None
        );
        let other = BTC_FILE.replace("BTC-USDT-SWAP", "BTC-USDT-250101");
        assert_eq!(
            listing(&other)
                .order_book_file(Contract::of(btc()).unwrap())
                .unwrap(),
            None
        );
    }

    #[test]
    fn a_refused_request_or_a_file_listed_twice_is_an_error() {
        let refused: ArchiveListing =
            serde_json::from_str(r#"{"code":"50076","msg":"range too wide","data":[]}"#).unwrap();
        let err = refused
            .order_book_file(Contract::of(btc()).unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("50076"), "{err}");
        let twice = listing(&format!("{BTC_FILE},{BTC_FILE}"));
        assert!(twice.order_book_file(Contract::of(btc()).unwrap()).is_err());
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
                (
                    at("2026-10-17T06:00:00Z"),
                    at("2026-10-17T23:59:59.999999999Z")
                ),
                (
                    at("2026-10-18T00:00:00Z"),
                    at("2026-10-18T23:59:59.999999999Z")
                ),
                (at("2026-10-19T00:00:00Z"), at("2026-10-19T18:00:00Z")),
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
    async fn the_line_at_midnight_is_stored_from_the_file_of_the_day_it_opens() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let mut asked = Vec::new();
        fill_by_day(
            &mut catalog,
            vec![btc_instrument()],
            at("2026-10-18T00:00:00Z"),
            at("2026-10-20T00:00:00Z"),
            async |instrument: &InstrumentAny, from: Timestamp, to: Timestamp| {
                asked.push((from, to));
                // The file of a day holds its lines from its midnight on.
                let midnight = day_start(utc_date(from))?;
                let quote_at = |ts: Timestamp| {
                    let ts = UnixNanos::from(ts);
                    QuoteTick::new(
                        instrument.id(),
                        instrument.make_price(100_000.0),
                        instrument.make_price(100_000.1),
                        instrument.make_qty(1.0, None),
                        instrument.make_qty(2.0, None),
                        ts,
                        ts,
                    )
                };
                Ok(Some(
                    [midnight, midnight + SignedDuration::from_hours(12)]
                        .into_iter()
                        .filter(|ts| from <= *ts && *ts <= to)
                        .map(quote_at)
                        .collect(),
                ))
            },
        )
        .await
        .unwrap();
        assert_eq!(
            asked,
            [
                (
                    at("2026-10-18T00:00:00Z"),
                    at("2026-10-18T23:59:59.999999999Z")
                ),
                (
                    at("2026-10-19T00:00:00Z"),
                    at("2026-10-19T23:59:59.999999999Z")
                ),
            ]
        );
        assert_eq!(
            stored_noons(&mut catalog),
            [
                at("2026-10-18T00:00:00Z"),
                at("2026-10-18T12:00:00Z"),
                at("2026-10-19T00:00:00Z"),
                at("2026-10-19T12:00:00Z"),
            ]
        );
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
                    at("2026-10-18T00:00:00Z"),
                    at("2026-10-18T23:59:59.999999999Z")
                ),
                (
                    at("2026-10-19T00:00:00Z"),
                    at("2026-10-19T23:59:59.999999999Z")
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
                    at("2026-10-18T00:00:00Z"),
                    at("2026-10-18T23:59:59.999999999Z")
                ),
                (
                    at("2026-10-19T00:00:00Z"),
                    at("2026-10-19T23:59:59.999999999Z")
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
