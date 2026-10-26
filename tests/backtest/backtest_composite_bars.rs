//! A strategy on a composite bar type, hour bars Nautilus builds from
//! external minute bars: `fill_catalog` stores the minute bars a source
//! returns, as the strategy's bar types ask (`strategy::catalog_series`),
//! and the backtest replays them, so that every hour bar has the open,
//! high, low and close of its minute bars. One node per thread, so one
//! run per test.

use crate::common::{self, instrument, ohlc, recorder::Recorder, ts};
use helix_trader::{data::fill_catalog, strategy::catalog_series};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, Data, NautilusDataType},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const COMPOSITE: &str = "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";
const MINUTE_BARS: &str = "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL";

/// `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// The minute bar closing at `close`, stamped at its close: the k-th minute
/// after 06:00 opens at 600 plus 0.01 times `37 k mod 101`, with its own
/// high, low and close around it.
fn minute_bar(close: Timestamp) -> Bar {
    let k = close.duration_since(at("06:00")).as_mins();
    let open = 600.0 + 0.01 * ((37 * k) % 101) as f64;
    let stamp = UnixNanos::from(close);
    Bar::new(
        BarType::from(MINUTE_BARS),
        Price::new(open, 2),
        Price::new(open + 0.30, 2),
        Price::new(open - 0.20, 2),
        Price::new(open + 0.10, 2),
        Quantity::from(1),
        stamp,
        stamp,
    )
}

/// The minute bars closing in `[from, to]`.
fn minute_bars(from: Timestamp, to: Timestamp) -> Vec<Bar> {
    let minute = SignedDuration::from_mins(1);
    let mut close = from;
    let mut bars = vec![];
    while close <= to {
        if close > at("06:00") {
            bars.push(minute_bar(close));
        }
        close += minute;
    }
    bars
}

/// The hour bar closing at `close`, worked out here from its minute bars,
/// those closing in `(close − 1 h, close]`.
fn hour_bar(close: Timestamp) -> common::Ohlc {
    let minutes = minute_bars(close - SignedDuration::from_mins(59), close);
    let high = minutes.iter().map(|bar| bar.high).max().unwrap();
    let low = minutes.iter().map(|bar| bar.low).min().unwrap();
    let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
    (close, first.open, high, low, last.close)
}

#[test]
fn hour_bars_built_from_stored_minute_bars_have_the_prices_of_their_minutes() {
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let bar_types = [BarType::from(COMPOSITE)];
    let series = catalog_series(&bar_types);
    assert_eq!(
        series,
        [(NautilusDataType::Bar, MINUTE_BARS.to_string())],
        "the minute bars are what the hour bars are built from"
    );
    let fill = fill_catalog(
        &mut catalog,
        vec![instrument()],
        &series,
        at("06:00"),
        at("15:00"),
        async |_, _, from, to| Ok(minute_bars(from, to).into_iter().map(Data::Bar).collect()),
    );
    // The node runs its own runtime: this one is gone before it starts.
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();

    let (recorder, record) = Recorder::without_warmup(BarType::from(COMPOSITE));
    let mut backtest = common::backtest(
        dir.path(),
        at("10:30"),
        at("15:00"),
        SignedDuration::ZERO,
        "0 EUR",
    );
    backtest.bar_types = bar_types.to_vec();
    backtest.run(recorder).unwrap();

    let record = record.borrow();
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    // The engine starts at 10:29:59.999999999: the bar closing 11:00 lacks
    // the minutes before it; those closing 12:00 to 15:00 are whole.
    let closes: Vec<Timestamp> = replay.iter().map(|bar| bar.0).collect();
    assert_eq!(
        closes,
        ["11:00", "12:00", "13:00", "14:00", "15:00"].map(at)
    );
    let whole: Vec<_> = ["12:00", "13:00", "14:00", "15:00"]
        .map(|close| hour_bar(at(close)))
        .to_vec();
    assert_eq!(replay[1..], whole);
}
