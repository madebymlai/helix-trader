//! OKX's candle archive turned into bars: one day's `.zip` holds one CSV of
//! the confirmed 1-minute candles of a swap, or of every contract of a
//! futures chain, one row per candle, stamped at the minute's open.
//!
//! Each row becomes the adapter's `OKXCandlestick` and the adapter's
//! `parse_candlestick` makes its bar, at the instrument's precisions, so
//! that prices and volume (in contracts) are those of the bars Nautilus'
//! OKX clients make from OKX's API. `ts_event` is the candle's open, as the
//! adapter keeps it; `ts_init` is its close (open + 1 minute), the
//! convention of Nautilus' bar execution for bars stamped at their open,
//! which the adapter applies to its historical bars from the release after
//! 0.66.0 on (nautilus_trader#5339). The backtest replays bars by
//! `ts_init`, so a bar is seen once its minute is over. To be removed if
//! Nautilus gains a loader for this archive.

use std::io::{Read, Seek};

use anyhow::{Context, bail, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, bar::get_bar_interval_ns},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_okx::{common::parse::parse_candlestick, http::models::OKXCandlestick};
use serde::Deserialize;

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
/// `ts_init` (close) is in `[from, through]`, from a day file of the
/// archive: the rows of its contract; those of other contracts of a chain
/// are skipped.
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
    let mut archive = zip::ZipArchive::new(archive).context("reading the zip")?;
    ensure!(
        archive.len() == 1,
        "the zip holds {} files, not one CSV",
        archive.len()
    );
    let csv = archive.by_index(0).context("reading the zip")?;
    let name = csv.name().context("reading the zip")?.to_string();
    let interval = get_bar_interval_ns(&bar_type);
    let mut rows = csv::Reader::from_reader(csv);
    let mut bars = Vec::new();
    let mut of_contract = false;
    for (index, row) in rows.deserialize::<CandleRow>().enumerate() {
        let number = index + 1;
        let row = row.with_context(|| format!("reading row {number} of {name}"))?;
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
