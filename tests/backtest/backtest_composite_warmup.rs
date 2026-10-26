//! The warmup of a composite bar type, hour bars Nautilus builds from
//! external minute bars stamped as OKX's candles (`ts_event` at the
//! minute's open, `ts_init` at its close), run starting off an hour: the
//! warmup requests the minute bars and the data engine builds the hour bars
//! from them; the oldest hour, missing the minutes before `now - warmup`,
//! is dropped; the hour spanning the warmup end and the run start comes
//! once, complete, in the run. One node per thread, so one run per test.

use crate::common::{self, Ohlc, ohlc, recorder::Recorder, ts};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const MINUTE_BARS: &str = "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL";
const HOUR_BARS: &str = "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

/// `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// The minute bar opening at `open`, the k-th minute after 08:00: it opens
/// at 600 plus 0.01 times `37 k mod 101`, its high 0.30 above, its low 0.20
/// below, its close 0.10 above.
fn minute_bar(open: Timestamp) -> Bar {
    let k = open.duration_since(at("08:00")).as_mins();
    let first = 600.0 + 0.01 * ((37 * k) % 101) as f64;
    Bar::new(
        BarType::from(MINUTE_BARS),
        Price::new(first, 2),
        Price::new(first + 0.30, 2),
        Price::new(first - 0.20, 2),
        Price::new(first + 0.10, 2),
        Quantity::from(1),
        UnixNanos::from(open),
        UnixNanos::from(open + SignedDuration::from_mins(1)),
    )
}

/// The minute bars opening in `[from, to)`.
fn minute_bars(from: Timestamp, to: Timestamp) -> Vec<Bar> {
    let mut open = from;
    let mut bars = vec![];
    while open < to {
        bars.push(minute_bar(open));
        open += SignedDuration::from_mins(1);
    }
    bars
}

/// The hour bar closing at `close`, worked out here from the minutes
/// opening in `[close − 1 h, close)`.
fn hour_bar(close: Timestamp) -> Ohlc {
    let minutes = minute_bars(close - SignedDuration::from_hours(1), close);
    let high = minutes.iter().map(|bar| bar.high).max().unwrap();
    let low = minutes.iter().map(|bar| bar.low).min().unwrap();
    let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
    (close, first.open, high, low, last.close)
}

#[test]
fn a_composite_bar_type_warms_up_on_the_hours_of_its_source_bars() {
    let dir = common::catalog_without_quotes();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog
        .write_to_parquet(
            &minute_bars(at("07:00"), at("12:00")),
            Some(UnixNanos::from(at("07:00"))),
            Some(UnixNanos::from(at("12:00"))),
            None,
        )
        .unwrap();
    let warmup = SignedDuration::from_hours(2);
    let (recorder, record) = Recorder::new(BarType::from(HOUR_BARS), warmup, None);
    let mut backtest = common::backtest(dir.path(), at("10:30"), at("12:00"), warmup, "0 EUR");
    backtest.bar_types = vec![BarType::from(HOUR_BARS)];

    backtest.run(recorder).unwrap();

    let record = record.borrow();
    // The engine starts at 10:29:59.999999999, the warmup at 08:29:59.999999999:
    // the hour closing 09:00 lacks the minutes before 08:29 and is dropped.
    let warmup: Vec<_> = record.warmup.iter().map(ohlc).collect();
    assert_eq!(warmup, [hour_bar(at("10:00"))]);
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    assert_eq!(replay, [hour_bar(at("11:00")), hour_bar(at("12:00"))]);
}
