//! OKX's candle archive turned into bars: a month's or a day's `.zip` holds
//! one CSV of the confirmed 1-minute candles of a swap, or of every contract
//! of a futures chain, one row per candle, stamped at the minute's open. A
//! month's file holds the same rows as its days' files.
//!
//! Each row becomes the adapter's `OKXCandlestick` and the adapter's
//! `parse_candlestick` makes its bar, at the instrument's precisions, so
//! that prices and volume (in contracts) are those of the bars Nautilus'
//! OKX clients make from OKX's API. `ts_event` is the candle's open, as the
//! adapter keeps it; `ts_init` is its close (open + 1 minute), the
//! convention of Nautilus' bar execution for bars stamped at their open,
//! which the adapter applies to its historical bars from the release after
//! 0.66.0 on (nautilus_trader#5339). The backtest replays bars by
//! `ts_init`, so a bar is seen once its minute is over.
//!
//! A swap's month of funding is one CSV too, one row per funding time, from
//! the month's start (midnight UTC+8) to before its end. Each row becomes
//! the adapter's `OKXFundingRateHistory` and the adapter's
//! `parse_funding_rate` makes its `FundingRateUpdate`, as Nautilus' OKX
//! client makes them from OKX's API: `ts_event` = `ts_init` = the funding
//! time, the interval the time since the row before (from the row after
//! for the file's first row, whose row before is in the month before), no
//! `next_funding_ns`. To be removed if Nautilus gains a loader for this
//! archive.

use std::io::{Read, Seek};

use anyhow::{Context, bail, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, FundingRateUpdate, bar::get_bar_interval_ns},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_okx::{
    common::{
        enums::OKXInstrumentType,
        parse::{parse_candlestick, parse_funding_rate},
    },
    http::models::{OKXCandlestick, OKXFundingRateHistory},
};
use serde::{Deserialize, de::DeserializeOwned};

/// One row of the archive's CSV.
#[derive(Debug, Deserialize)]
struct CandleRow {
    instrument_name: String,
    open: String,
    high: String,
    low: String,
    close: String,
    /// In contracts, as the API's `vol`.
    vol: String,
    vol_ccy: String,
    vol_quote: String,
    /// Milliseconds.
    open_time: String,
    confirm: String,
}

impl CandleRow {
    /// The row as the API's candle: `[ts, o, h, l, c, vol, volCcy,
    /// volCcyQuote, confirm]`.
    fn into_candlestick(self) -> OKXCandlestick {
        OKXCandlestick(
            self.open_time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.vol,
            self.vol_ccy,
            self.vol_quote,
            self.confirm,
        )
    }
}

/// The bars of `bar_type`, a 1-minute bar type of `instrument`, whose
/// `ts_init` (close) is in `[from, through]`, from a month or day file of
/// the archive: the rows of its contract; those of other contracts of a
/// chain are skipped.
///
/// # Errors
///
/// Returns an error if the file is not a zip of one CSV, if a row cannot be
/// read or parsed or is not confirmed, or if no row is of the contract.
pub(super) fn bars_from_archive(
    archive: impl Read + Seek,
    instrument: &InstrumentAny,
    bar_type: BarType,
    from: UnixNanos,
    through: UnixNanos,
) -> anyhow::Result<Vec<Bar>> {
    let id = instrument.id();
    let (name, rows) = read_rows::<CandleRow>(archive)?;
    let interval = get_bar_interval_ns(&bar_type);
    let mut bars = Vec::new();
    let mut of_contract = false;
    for (index, row) in rows.into_iter().enumerate() {
        let number = index + 1;
        if row.instrument_name != id.symbol.as_str() {
            continue;
        }
        of_contract = true;
        if row.confirm != "1" {
            bail!("row {number} of {name} is not confirmed");
        }
        let mut bar = parse_candlestick(
            &row.into_candlestick(),
            bar_type,
            instrument.price_precision(),
            instrument.size_precision(),
            UnixNanos::default(),
        )
        .with_context(|| format!("parsing row {number} of {name}"))?;
        bar.ts_init = bar.ts_event + interval;
        if from <= bar.ts_init && bar.ts_init <= through {
            bars.push(bar);
        }
    }
    ensure!(of_contract, "{name} holds no candle of {id}");
    Ok(bars)
}

/// One row of a funding file's CSV.
#[derive(Debug, Deserialize)]
struct FundingRow {
    instrument_name: String,
    funding_rate: String,
    /// Milliseconds.
    funding_time: u64,
}

/// The funding of the swap `id` at times in `[from, through]`, from a month
/// file of the archive, in time order.
///
/// # Errors
///
/// Returns an error if the file is not a zip of one CSV, if a row cannot be
/// read or parsed, if the swap's rows are not in strictly increasing time
/// order, or if it has fewer than two (no interval).
pub(super) fn funding_from_archive(
    archive: impl Read + Seek,
    id: InstrumentId,
    from: UnixNanos,
    through: UnixNanos,
) -> anyhow::Result<Vec<FundingRateUpdate>> {
    let (name, rows) = read_rows::<FundingRow>(archive)?;
    let rows: Vec<(usize, FundingRow)> = rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| (index + 1, row))
        .filter(|(_, row)| row.instrument_name == id.symbol.as_str())
        .collect();
    ensure!(!rows.is_empty(), "{name} holds no funding of {id}");
    ensure!(
        rows.len() > 1,
        "{name} holds one funding row of {id}, no interval"
    );
    let mut intervals_ms = Vec::new();
    for pair in rows.windows(2) {
        let ((_, before), (number, row)) = (&pair[0], &pair[1]);
        let interval_ms = row
            .funding_time
            .checked_sub(before.funding_time)
            .filter(|ms| *ms > 0)
            .with_context(|| format!("row {number} of {name} is not after the row before"))?;
        intervals_ms.push(interval_ms);
    }
    // The first row's row before is in the month before.
    intervals_ms.insert(0, intervals_ms[0]);
    let mut funding = Vec::new();
    for ((number, row), interval_ms) in rows.iter().zip(intervals_ms) {
        let raw = OKXFundingRateHistory {
            inst_type: OKXInstrumentType::Swap,
            inst_id: id.symbol.inner(),
            funding_rate: row.funding_rate.clone(),
            // The archive's one rate, equal to the API's realized rate.
            realized_rate: row.funding_rate.clone(),
            funding_time: row.funding_time,
            method: None,
        };
        let update = parse_funding_rate(&raw, id, Some(interval_ms))
            .with_context(|| format!("parsing row {number} of {name}"))?;
        if from <= update.ts_init && update.ts_init <= through {
            funding.push(update);
        }
    }
    Ok(funding)
}

/// The rows of the one CSV of the zip `archive`, and its name.
fn read_rows<T: DeserializeOwned>(archive: impl Read + Seek) -> anyhow::Result<(String, Vec<T>)> {
    let mut archive = zip::ZipArchive::new(archive).context("reading the zip")?;
    ensure!(
        archive.len() == 1,
        "the zip holds {} files, not one CSV",
        archive.len()
    );
    let csv = archive.by_index(0).context("reading the zip")?;
    let name = csv.name().context("reading the zip")?.to_string();
    let rows = csv::Reader::from_reader(csv)
        .deserialize::<T>()
        .enumerate()
        .map(|(index, row)| row.with_context(|| format!("reading row {} of {name}", index + 1)))
        .collect::<anyhow::Result<_>>()?;
    Ok((name, rows))
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use jiff::Timestamp;
    use zip::{CompressionMethod, write::SimpleFileOptions};

    use super::*;
    use crate::data::okx::tests::{btc_instrument, xperp_instrument};

    const SWAP_DAY: &[u8] =
        include_bytes!("testdata/BTC-USDT-SWAP-candlesticks-2026-10-19-sample.zip");
    const XPERP_DAY: &[u8] =
        include_bytes!("testdata/BTC-USD_UM_XPERP-futureschain-candlesticks-2026-10-19-sample.zip");
    /// The first and last hours of 2026-09, from the month's file and from
    /// the files of its first and last days.
    const SWAP_MONTH: &[u8] =
        include_bytes!("testdata/BTC-USDT-SWAP-candlesticks-2026-09-sample.zip");
    const SWAP_FIRST_DAY: &[u8] =
        include_bytes!("testdata/BTC-USDT-SWAP-candlesticks-2026-09-01-sample.zip");
    const SWAP_LAST_DAY: &[u8] =
        include_bytes!("testdata/BTC-USDT-SWAP-candlesticks-2026-09-30-sample.zip");

    fn ns(s: &str) -> UnixNanos {
        UnixNanos::from(s.parse::<Timestamp>().unwrap())
    }

    fn minute_bars(instrument: &InstrumentAny) -> BarType {
        BarType::from(format!("{}-1-MINUTE-LAST-EXTERNAL", instrument.id()).as_str())
    }

    /// The bars of `day` for `instrument` over the whole day file.
    fn read(day: &[u8], instrument: &InstrumentAny) -> anyhow::Result<Vec<Bar>> {
        bars_from_archive(
            Cursor::new(day),
            instrument,
            minute_bars(instrument),
            UnixNanos::default(),
            UnixNanos::max(),
        )
    }

    #[test]
    fn a_candle_is_a_bar_stamped_at_its_open_and_seen_at_its_close() {
        let btc = btc_instrument();
        let bars = read(SWAP_DAY, &btc).unwrap();
        // The sample: the day's first 61 minutes and its last one.
        assert_eq!(bars.len(), 62);
        let first = bars[0];
        assert_eq!(first.bar_type, minute_bars(&btc));
        // The 2026-10-19 file is a day of UTC+8: it opens at 16:00 UTC.
        assert_eq!(first.ts_event, ns("2026-10-18T16:00:00Z"));
        assert_eq!(first.ts_init, ns("2026-10-18T16:01:00Z"));
        let prices = [first.open, first.high, first.low, first.close].map(|p| p.to_string());
        assert_eq!(prices, ["81135.6", "81135.6", "81118.8", "81118.9"]);
        // Contracts, as the adapter's bars.
        assert_eq!(first.volume.to_string(), "2306.44");
        let last = bars[61];
        assert_eq!(last.ts_event, ns("2026-10-19T15:59:00Z"));
        assert_eq!(last.ts_init, ns("2026-10-19T16:00:00Z"));
    }

    #[test]
    fn a_months_file_makes_the_bars_of_its_days_files() {
        let btc = btc_instrument();
        let month = read(SWAP_MONTH, &btc).unwrap();
        let mut days = read(SWAP_FIRST_DAY, &btc).unwrap();
        days.extend(read(SWAP_LAST_DAY, &btc).unwrap());
        assert_eq!(month.len(), 120);
        assert_eq!(month, days);
    }

    #[test]
    fn a_months_file_holds_the_minutes_opening_in_its_month_of_utc_plus_8() {
        let bars = read(SWAP_MONTH, &btc_instrument()).unwrap();
        assert_eq!(bars[0].ts_event, ns("2026-08-31T16:00:00Z"));
        assert_eq!(bars[119].ts_event, ns("2026-09-30T15:59:00Z"));
        assert_eq!(bars[119].ts_init, ns("2026-09-30T16:00:00Z"));
    }

    #[test]
    fn the_bars_of_an_hour_are_the_minutes_opening_in_it() {
        let btc = btc_instrument();
        let bars = bars_from_archive(
            Cursor::new(SWAP_DAY),
            &btc,
            minute_bars(&btc),
            ns("2026-10-18T16:00:00.000000001Z"),
            ns("2026-10-18T17:00:00Z"),
        )
        .unwrap();
        assert_eq!(bars.len(), 60);
        assert_eq!(bars[0].ts_event, ns("2026-10-18T16:00:00Z"));
        assert_eq!(bars[59].ts_event, ns("2026-10-18T16:59:00Z"));
        assert_eq!(bars[59].ts_init, ns("2026-10-18T17:00:00Z"));
    }

    #[test]
    fn a_future_is_read_from_its_chain_file_in_whole_contracts() {
        let xperp = xperp_instrument();
        let bars = read(XPERP_DAY, &xperp).unwrap();
        assert_eq!(bars.len(), 62);
        assert_eq!(bars[0].ts_event, ns("2026-10-18T16:00:00Z"));
        assert_eq!(bars[0].close.to_string(), "81095.7");
        assert_eq!(bars[0].volume.to_string(), "3768");
    }

    #[test]
    fn a_file_with_no_candle_of_the_contract_is_an_error() {
        let err = read(SWAP_DAY, &xperp_instrument()).unwrap_err().to_string();
        assert!(
            err.contains("no candle of BTC-USD_UM_XPERP-310404.OKX"),
            "{err}"
        );
    }

    /// A zip of one CSV holding `rows` under the archive's header.
    fn zipped(rows: &str) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        zip.start_file("BTC-USDT-SWAP-candlesticks-2026-10-19.csv", options)
            .unwrap();
        let header =
            "instrument_name,open,high,low,close,vol,vol_ccy,vol_quote,open_time,confirm\n";
        zip.write_all(format!("{header}{rows}").as_bytes()).unwrap();
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn an_unconfirmed_candle_is_an_error_naming_its_row() {
        let rows = "\
BTC-USDT-SWAP,81135.6,81135.6,81118.8,81118.9,2306.44,23.0644,1871132.18229,1792339200000,1
BTC-USDT-SWAP,81118.9,81118.9,81091.9,81093.2,36410.05,364.1005,29531376.89478,1792339260000,0
";
        let err = read(&zipped(rows), &btc_instrument())
            .unwrap_err()
            .to_string();
        assert!(err.contains("row 2"), "{err}");
    }

    /// The first two days of 2026-09 from its funding file, whose rows run
    /// from 2026-08-31T16:00Z (midnight in UTC+8) to 2026-09-30T08:00Z.
    const SWAP_FUNDING: &[u8] =
        include_bytes!("testdata/BTC-USDT-SWAP-fundingrates-2026-09-sample.zip");

    fn read_funding(file: &[u8]) -> anyhow::Result<Vec<FundingRateUpdate>> {
        funding_from_archive(
            Cursor::new(file),
            btc_instrument().id(),
            UnixNanos::default(),
            UnixNanos::max(),
        )
    }

    #[test]
    fn a_funding_row_is_an_update_at_its_funding_time_with_the_interval_from_the_row_before() {
        let funding = read_funding(SWAP_FUNDING).unwrap();
        assert_eq!(funding.len(), 6);
        let first = funding[0];
        assert_eq!(first.instrument_id, btc_instrument().id());
        assert_eq!(first.rate.to_string(), "0.0000744588100483");
        assert_eq!(first.ts_event, ns("2026-08-31T16:00:00Z"));
        assert_eq!(first.ts_init, first.ts_event);
        assert_eq!(first.next_funding_ns, None);
        let last = funding[5];
        assert_eq!(last.rate.to_string(), "0.0000740231074545");
        assert_eq!(last.ts_event, ns("2026-09-02T08:00:00Z"));
        assert_eq!(last.ts_init, last.ts_event);
        // 8 hours; the first row's from the row after it.
        assert!(funding.iter().all(|update| update.interval == Some(480)));
    }

    #[test]
    fn the_funding_of_a_window_is_that_at_times_in_it() {
        let funding = funding_from_archive(
            Cursor::new(SWAP_FUNDING),
            btc_instrument().id(),
            ns("2026-09-01T00:00:00Z"),
            ns("2026-09-01T16:00:00Z"),
        )
        .unwrap();
        let times: Vec<_> = funding.iter().map(|update| update.ts_event).collect();
        assert_eq!(
            times,
            [
                ns("2026-09-01T00:00:00Z"),
                ns("2026-09-01T08:00:00Z"),
                ns("2026-09-01T16:00:00Z")
            ]
        );
        // The interval of the window's first update is still from the row
        // before it.
        assert_eq!(funding[0].interval, Some(480));
    }

    /// A zip of one CSV holding funding `rows` under the archive's header.
    fn zipped_funding(rows: &str) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        zip.start_file("BTC-USDT-SWAP-fundingrates-2026-09.csv", options)
            .unwrap();
        let header = "instrument_name,funding_rate,funding_time\n";
        zip.write_all(format!("{header}{rows}").as_bytes()).unwrap();
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn a_funding_interval_follows_the_times_of_the_rows() {
        let rows = "\
BTC-USDT-SWAP,0.0001,1788192000000
BTC-USDT-SWAP,-0.0002,1788206400000
BTC-USDT-SWAP,0.0003,1788220800000
BTC-USDT-SWAP,0.0004,1788249600000
";
        let funding = read_funding(&zipped_funding(rows)).unwrap();
        let intervals: Vec<_> = funding.iter().map(|update| update.interval).collect();
        assert_eq!(intervals, [Some(240), Some(240), Some(240), Some(480)]);
        assert_eq!(funding[1].rate.to_string(), "-0.0002");
    }

    #[test]
    fn funding_rows_out_of_order_or_alone_are_an_error() {
        let rows = "\
BTC-USDT-SWAP,0.0001,1788220800000
BTC-USDT-SWAP,0.0002,1788192000000
";
        let err = read_funding(&zipped_funding(rows)).unwrap_err().to_string();
        assert!(err.contains("row 2"), "{err}");
        let err = read_funding(&zipped_funding("BTC-USDT-SWAP,0.0001,1788192000000\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("interval"), "{err}");
    }

    #[test]
    fn a_file_with_no_funding_of_the_swap_is_an_error() {
        let err = read_funding(&zipped_funding("ETH-USDT-SWAP,0.0001,1788192000000\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("no funding of BTC-USDT-SWAP.OKX"), "{err}");
    }

    #[test]
    fn rows_of_other_contracts_of_a_chain_are_skipped() {
        let rows = "\
BTC-USDT-250101,1.0,1.0,1.0,1.0,1,0.01,1,1792339200000,1
BTC-USDT-SWAP,81135.6,81135.6,81118.8,81118.9,2306.44,23.0644,1871132.18229,1792339200000,1
";
        let bars = read(&zipped(rows), &btc_instrument()).unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].open.to_string(), "81135.6");
    }
}
