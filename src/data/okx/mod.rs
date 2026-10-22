//! OKX history: the instruments of OKX swaps and X-Perps and their 1-minute
//! candles, filled into the catalog one archive day at a time.
//!
//! Instruments come from OKX's public REST API through the adapter's
//! `OKXRawHttpClient` and `parse_instrument_any`, with margins of 1 (see
//! `instrument_from`). Candles come from OKX's archive (see `archive`):
//! one small file per swap, or per futures family, and day, the days of
//! the archive being those of UTC+8. They are stored as the bars of the
//! instrument's `1-MINUTE-LAST-EXTERNAL` bar type, the source of the
//! composite bar types strategies trade OKX on. The archive API, the
//! download and the conversion are ours, to be removed if Nautilus gains an
//! equivalent.

use std::{collections::HashMap, io::Cursor, time::Duration};

use anyhow::{Context, bail, ensure};
use jiff::{
    SignedDuration, Timestamp,
    civil::Date,
    tz::{self, TimeZone},
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, Data, NautilusDataType},
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
    http::{client::OKXRawHttpClient, query::GetInstrumentsParams},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::fill_catalog;

mod archive;

/// OKX's archive of history files (public, no key).
const ARCHIVE_API: &str = "https://www.okx.com/api/v5/public/market-data-history";

/// Where the archive's files are served from; any other URL is refused.
const ARCHIVE_FILES: &str = "https://static.okx.com/";

/// The archive module of the 1-minute candles.
const CANDLE_MODULE: &str = "2";

/// The archive API's limit, 5 requests per 2 seconds, kept as one listing
/// every 600 ms with no burst (every 400 ms still got HTTP 429 now and
/// then). Nautilus' `HttpClient` limits only the requests passing a key,
/// so the listings pass `ARCHIVE_API_KEY`; the downloads, from
/// `static.okx.com`, are not limited.
const ARCHIVE_REQUEST_PERIOD: Duration = Duration::from_millis(600);

/// The rate-limit key of the archive API's requests.
const ARCHIVE_API_KEY: &str = "market-data-history";

/// The timeout of one archive request, a listing or a day file (about
/// 0.04 MB for BTC).
const ARCHIVE_REQUEST_TIMEOUT_SECS: u64 = 60;

/// The archive's days are those of UTC+8: the file of 2026-10-19 holds the
/// candles opening from 2026-10-18T16:00Z to 2026-10-19T15:59Z.
const ARCHIVE_DAY_OFFSET_HOURS: i8 = 8;

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

    /// Fills `catalog` over `[start, end]` with `series`, each the bars of
    /// the `1-MINUTE-LAST-EXTERNAL` bar type of an OKX swap or X-Perp, and
    /// their instruments, one archive day at a time (see `fill_by_day`).
    ///
    /// # Errors
    ///
    /// Returns an error if a series is not such bars, checked before any
    /// request, or as `fill_by_day`.
    pub async fn fill(
        &self,
        catalog: &mut ParquetDataCatalog,
        series: &[(NautilusDataType, String)],
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<()> {
        let bar_types = series
            .iter()
            .map(|(data_type, identifier)| candle_bar_type(data_type, identifier))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for bar_type in bar_types {
            let instrument = self.instrument(bar_type.instrument_id()).await?;
            fill_by_day(
                catalog,
                &instrument,
                bar_type,
                start,
                end,
                async |from, to| self.day_bars(&instrument, bar_type, from, to).await,
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
    /// `[from, to]`, a window within one archive day, from that day's
    /// file; `None` if the archive has no file for it. Logs the day's
    /// file and count.
    async fn day_bars(
        &self,
        instrument: &InstrumentAny,
        bar_type: BarType,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<Option<Vec<Bar>>> {
        let id = instrument.id();
        let day = archive_day(from)?;
        ensure!(
            to <= day_end(day)?,
            "the window [{from}, {to}] of {id} spans more than the archive day {day}"
        );
        let Some(file) = self.day_file(Contract::of(id)?, day).await? else {
            log::warn!("{id}: OKX's archive has no candle file for {day}");
            return Ok(None);
        };
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
        log::info!("{id} {day}: {} bars from {}", bars.len(), file.filename);
        Ok(Some(bars))
    }

    /// The archive's candle file of `contract` for `day`, if any.
    async fn day_file(&self, contract: Contract, day: Date) -> anyhow::Result<Option<ArchiveFile>> {
        let id = contract.id;
        let day_ms = day_start(day)?.as_millisecond().to_string();
        let params: HashMap<String, Vec<String>> = [
            ("module", CANDLE_MODULE),
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
                None,
                Some(vec![ARCHIVE_API_KEY.to_string()]),
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
        listing.candle_file(contract)
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

/// Fills `catalog` with `instrument` and the bars of `bar_type` over
/// `[start, end]`, one archive day at a time: `fill_catalog` over
/// `[start, the day's end]`, so each call resumes where the catalog
/// stopped. A day's file holds the minutes opening on that day, so the bars
/// closing in `(its start, its end]` (UTC+8 midnights): its window ends on
/// the close of its last minute, and the next starts 1 ns after it,
/// contiguous and closed as Nautilus' catalog intervals. The first window
/// starts with the day of `start`, which its file covers whole (a `start`
/// on a day's end would otherwise make a window of one instant, which
/// `fill_catalog` takes as covered). A day starting at `end` is left to
/// the next run. `day_bars` answers a window within one
/// archive day, `None` if the archive has no file for that day. The fill
/// starts on the day it resumes in and stops at the first day the archive
/// has no file for (not published yet).
///
/// # Errors
///
/// As `fill_catalog`: no file on the first day is an empty first
/// response, an error naming the series.
async fn fill_by_day(
    catalog: &mut ParquetDataCatalog,
    instrument: &InstrumentAny,
    bar_type: BarType,
    start: Timestamp,
    end: Timestamp,
    mut day_bars: impl AsyncFnMut(Timestamp, Timestamp) -> anyhow::Result<Option<Vec<Bar>>>,
) -> anyhow::Result<()> {
    let series = [(NautilusDataType::Bar, bar_type.to_string())];
    let start = day_start(archive_day(start)?)? + SignedDuration::from_nanos(1);
    let last = catalog
        .query_last_timestamp(&NautilusDataType::Bar.into(), Some(&series[0].1))
        .with_context(|| format!("reading the stored range of the bars of {bar_type}"))?;
    // `fill_catalog` resumes 1 ns after the stored end.
    let begin = last.map_or(start, |last| {
        Timestamp::from(UnixNanos::from(last)) + SignedDuration::from_nanos(1)
    });
    let mut day = archive_day(begin)?;
    while day_start(day)? < end {
        let window_end = day_end(day)?.min(end);
        let mut published = true;
        fill_catalog(
            catalog,
            vec![instrument.clone()],
            &series,
            start,
            window_end,
            async |_, _, from, to| {
                let bars = day_bars(from, to).await?;
                published = bars.is_some();
                Ok(bars.into_iter().flatten().map(Data::Bar).collect())
            },
        )
        .await?;
        if !published {
            log::info!("{bar_type}: stopping at {day}, which OKX has not published");
            break;
        }
        day = day.tomorrow()?;
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

    /// The start of the name of the contract's candle file in the
    /// archive's listing: a swap's own file, or the file of its family's
    /// futures chain, which holds the rows of every contract (see
    /// `archive`).
    fn listed_prefix(self) -> String {
        if self.inst_type == OKXInstrumentType::Swap {
            format!("{}-candlesticks-", self.id.symbol)
        } else {
            format!("{}-futureschain-candlesticks-", self.family)
        }
    }
}

/// The time zone of the archive's days.
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

/// The close of the last minute of `day`: the next day's start.
fn day_end(day: Date) -> anyhow::Result<Timestamp> {
    day_start(day.tomorrow()?)
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
    /// The candle file of `contract` in this listing, if any; an error if
    /// OKX refused the request or listed it twice.
    fn candle_file(self, contract: Contract) -> anyhow::Result<Option<ArchiveFile>> {
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
            "OKX's archive lists more than one candle file of {id} for one day"
        );
        Ok(file)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use jiff::civil::date;

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
    fn the_listing_of_a_day_gives_the_candle_file_of_the_instrument() {
        let file = listing(BTC_FILE)
            .candle_file(Contract::of(btc()).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(file.filename, "BTC-USDT-SWAP-candlesticks-2026-10-19.zip");
        assert_eq!(file.size_mb, "0.04");
        assert!(file.url.starts_with(ARCHIVE_FILES));
    }

    /// As OKX's archive listed it on 2026-10-22.
    const XPERP_FILE: &str = r#"{"dateTs":"1792339200000","filename":"BTC-USD_UM_XPERP-futureschain-candlesticks-2026-10-19.zip","sizeMB":"0.04","url":"https://static.okx.com/cdn/okex/traderecords/candlesticks/daily/20261019/BTC-USD_UM_XPERP-futureschain-candlesticks-2026-10-19.zip?v=1001"}"#;

    #[test]
    fn the_listing_of_a_day_gives_the_chain_file_of_a_future() {
        let xperp = Contract::of(xperp()).unwrap();
        let file = listing(XPERP_FILE).candle_file(xperp).unwrap().unwrap();
        assert_eq!(
            file.filename,
            "BTC-USD_UM_XPERP-futureschain-candlesticks-2026-10-19.zip"
        );
        let other = XPERP_FILE.replace("BTC-USD_UM_XPERP", "ETH-USD_UM_XPERP");
        assert_eq!(listing(&other).candle_file(xperp).unwrap(), None);
        assert_eq!(listing(BTC_FILE).candle_file(xperp).unwrap(), None);
    }

    #[test]
    fn a_day_not_published_lists_no_file() {
        let empty: ArchiveListing = serde_json::from_str(
            r#"{"code":"0","data":[{"dateAggrType":"daily","details":[],"totalSizeMB":"0","ts":"1792616059979"}],"msg":""}"#,
        )
        .unwrap();
        assert_eq!(
            empty.candle_file(Contract::of(btc()).unwrap()).unwrap(),
            None
        );
        let other = BTC_FILE.replace("BTC-USDT-SWAP", "BTC-USDT-250101");
        assert_eq!(
            listing(&other)
                .candle_file(Contract::of(btc()).unwrap())
                .unwrap(),
            None
        );
    }

    #[test]
    fn a_refused_request_or_a_file_listed_twice_is_an_error() {
        let refused: ArchiveListing =
            serde_json::from_str(r#"{"code":"50076","msg":"range too wide","data":[]}"#).unwrap();
        let err = refused
            .candle_file(Contract::of(btc()).unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("50076"), "{err}");
        let twice = listing(&format!("{BTC_FILE},{BTC_FILE}"));
        assert!(twice.candle_file(Contract::of(btc()).unwrap()).is_err());
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
        assert_eq!(day_end(day).unwrap(), at("2026-10-19T16:00:00Z"));
        // A bar is in the day of its open, the minute before its close.
        assert_eq!(
            archive_day(at("2026-10-18T16:00:00Z")).unwrap(),
            date(2026, 10, 18)
        );
        assert_eq!(
            archive_day(at("2026-10-18T16:00:00.000000001Z")).unwrap(),
            day
        );
        assert_eq!(archive_day(at("2026-10-19T16:00:00Z")).unwrap(), day);
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

    /// An OKX-shaped day source: the bar closing at 04:00 UTC (noon in
    /// UTC+8) of the window's archive day, if within `[from, to]`; `None`
    /// (no file) for the days in `unpublished`. Records the windows asked
    /// for.
    fn day_source<'a>(
        asked: &'a mut Vec<(Timestamp, Timestamp)>,
        unpublished: &'a [Date],
    ) -> impl AsyncFnMut(Timestamp, Timestamp) -> anyhow::Result<Option<Vec<Bar>>> + 'a {
        async move |from: Timestamp, to: Timestamp| {
            asked.push((from, to));
            let day = archive_day(from)?;
            if unpublished.contains(&day) {
                return Ok(None);
            }
            let noon = day_start(day)? + SignedDuration::from_hours(12);
            let inside = from <= noon && noon <= to;
            Ok(Some(if inside { vec![bar_at(noon)] } else { vec![] }))
        }
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
        day_bars: impl AsyncFnMut(Timestamp, Timestamp) -> anyhow::Result<Option<Vec<Bar>>>,
    ) -> anyhow::Result<()> {
        fill_by_day(
            catalog,
            &btc_instrument(),
            BarType::from(BTC_MINUTES),
            at(start),
            at(end),
            day_bars,
        )
        .await
    }

    fn new_catalog(dir: &tempfile::TempDir) -> ParquetDataCatalog {
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
    }

    #[tokio::test]
    async fn a_window_is_filled_one_archive_day_per_fetch_each_resuming_after_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut asked = Vec::new();
        fill(
            &mut catalog,
            "2026-10-17T06:00:00Z",
            "2026-10-19T18:00:00Z",
            day_source(&mut asked, &[]),
        )
        .await
        .unwrap();
        // The first window starts with the archive day of the start.
        assert_eq!(
            asked,
            [
                (
                    at("2026-10-16T16:00:00.000000001Z"),
                    at("2026-10-17T16:00:00Z")
                ),
                (
                    at("2026-10-17T16:00:00.000000001Z"),
                    at("2026-10-18T16:00:00Z")
                ),
                (
                    at("2026-10-18T16:00:00.000000001Z"),
                    at("2026-10-19T16:00:00Z")
                ),
                (
                    at("2026-10-19T16:00:00.000000001Z"),
                    at("2026-10-19T18:00:00Z")
                ),
            ]
        );
        assert_eq!(
            stored_closes(&mut catalog),
            [
                at("2026-10-17T04:00:00Z"),
                at("2026-10-18T04:00:00Z"),
                at("2026-10-19T04:00:00Z"),
            ]
        );
        assert_eq!(catalog.instruments(None, None, None).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_start_on_a_day_start_stores_its_bar_from_the_file_of_the_day_before() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut asked = Vec::new();
        fill(
            &mut catalog,
            "2026-10-18T16:00:00Z",
            "2026-10-19T16:00:00Z",
            async |from: Timestamp, to: Timestamp| {
                asked.push((from, to));
                // A day's file holds the bars closing after its start up to
                // its end included.
                let day = archive_day(from)?;
                let (first, last) = (
                    day_start(day)? + SignedDuration::from_mins(1),
                    day_end(day)?,
                );
                Ok(Some(
                    [first, last]
                        .into_iter()
                        .filter(|close| from <= *close && *close <= to)
                        .map(bar_at)
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
                    at("2026-10-17T16:00:00.000000001Z"),
                    at("2026-10-18T16:00:00Z")
                ),
                (
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
        let mut asked = Vec::new();
        let unpublished = [date(2026, 10, 18), date(2026, 10, 19)];
        fill(
            &mut catalog,
            start,
            end,
            day_source(&mut asked, &unpublished),
        )
        .await
        .unwrap();
        assert_eq!(asked.len(), 2);
        assert_eq!(stored_closes(&mut catalog), [at("2026-10-17T04:00:00Z")]);
        let mut asked = Vec::new();
        fill(&mut catalog, start, end, day_source(&mut asked, &[]))
            .await
            .unwrap();
        assert_eq!(
            asked,
            [
                (
                    at("2026-10-17T16:00:00.000000001Z"),
                    at("2026-10-18T16:00:00Z")
                ),
                (
                    at("2026-10-18T16:00:00.000000001Z"),
                    at("2026-10-19T16:00:00Z")
                ),
            ]
        );
        assert_eq!(stored_closes(&mut catalog).len(), 3);
    }

    #[tokio::test]
    async fn a_failed_day_keeps_the_days_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut asked = Vec::new();
        let mut source = day_source(&mut asked, &[]);
        let result = fill(
            &mut catalog,
            "2026-10-16T16:00:00.000000001Z",
            "2026-10-19T16:00:00Z",
            async |from: Timestamp, to: Timestamp| {
                if archive_day(from)? == date(2026, 10, 19) {
                    bail!("download failed");
                }
                source(from, to).await
            },
        )
        .await;
        result.unwrap_err();
        assert_eq!(stored_closes(&mut catalog).len(), 2);
    }

    #[tokio::test]
    async fn no_file_on_the_first_day_is_an_error_naming_the_bar_type() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut asked = Vec::new();
        let err = fill(
            &mut catalog,
            "2026-10-16T16:00:00.000000001Z",
            "2026-10-17T16:00:00Z",
            day_source(&mut asked, &[date(2026, 10, 17)]),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(BTC_MINUTES), "{err}");
    }
}
