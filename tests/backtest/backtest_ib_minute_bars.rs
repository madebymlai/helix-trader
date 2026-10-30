//! A strategy on hour bars of IB's 1-minute TRADES bars, as `fill-catalog`
//! stores them (`EXTERNAL`, priced LAST, stamped at their close, with the
//! traded volume): `fill_catalog` stores the one series
//! `strategy::catalog_series` lists for the composite bar type, the
//! backtest replays it, the hour bars have the prices of their minutes, and
//! market orders fill on the minute bars, as on OKX's candles: the risk
//! engine prices them from the cached LAST bar (`market_order_price`) and
//! the matching engine fills them at the close of the hour's last minute,
//! with no spread. One node per thread, so one run per test.

use crate::common::{self, instrument, ohlc, recorder::Recorder, ts};
use helix_trader::{data::fill_catalog, strategy::catalog_series};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, Data, NautilusDataType},
    enums::OrderSide,
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const COMPOSITE: &str = "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";
const MINUTE_BARS: &str = "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL";

/// `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// The minute bar closing at `close`, stamped at its close as IB's adapter
/// stamps them (`ts_event = ts_init`), with 100 shares traded: the k-th
/// minute after 10:00 opens at 600 + 0.01 k, with a high 0.30 above, a
/// low 0.20 below and a close 0.10 above its open.
fn minute_bar(close: Timestamp) -> Bar {
    let k = close.duration_since(at("10:00")).as_mins();
    let open = 600.0 + 0.01 * k as f64;
    let stamp = UnixNanos::from(close);
    Bar::new(
        BarType::from(MINUTE_BARS),
        Price::new(open, 2),
        Price::new(open + 0.30, 2),
        Price::new(open - 0.20, 2),
        Price::new(open + 0.10, 2),
        Quantity::from(100),
        stamp,
        stamp,
    )
}

/// The minute bars closing in `[from, to]` after 10:00.
fn minute_bars(from: Timestamp, to: Timestamp) -> Vec<Bar> {
    let mut close = from;
    let mut bars = vec![];
    while close <= to {
        if close > at("10:00") {
            bars.push(minute_bar(close));
        }
        close += SignedDuration::from_mins(1);
    }
    bars
}

/// The hour bar closing at `close`, worked out here from its minute bars,
/// those closing in `(close − 1 h, close]`.
fn hour_bar(close: &str) -> common::Ohlc {
    let minutes = minute_bars(at(close) - SignedDuration::from_mins(59), at(close));
    let high = minutes.iter().map(|bar| bar.high).max().unwrap();
    let low = minutes.iter().map(|bar| bar.low).min().unwrap();
    let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
    (at(close), first.open, high, low, last.close)
}

#[test]
fn orders_on_hour_bars_of_ib_minute_bars_fill_at_the_close_of_the_hours_last_minute() {
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let bar_types = [BarType::from(COMPOSITE)];
    let series = catalog_series(&bar_types);
    assert_eq!(series, [(NautilusDataType::Bar, MINUTE_BARS.to_string())]);
    let fill = fill_catalog(
        &mut catalog,
        vec![instrument()],
        &series,
        at("10:00"),
        at("15:00"),
        async |_, _, from, to| Ok(minute_bars(from, to).into_iter().map(Data::Bar).collect()),
    );
    // The node runs its own runtime: this one is gone before it starts.
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();

    let (recorder, record) = Recorder::trading(
        BarType::from(COMPOSITE),
        vec![
            (OrderSide::Buy, Quantity::from(10)),
            (OrderSide::Sell, Quantity::from(10)),
        ],
    );
    let mut backtest = common::backtest(dir.path(), at("11:00"), at("15:00"), "0 EUR");
    backtest.bar_types = bar_types.to_vec();
    let outcome = backtest.run(recorder).unwrap();

    let record = record.borrow();
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    // The run starts at 11:00: the bar closing 11:00 holds the minute bar
    // stamped then alone; those closing 12:00 to 15:00 are whole.
    let closes: Vec<Timestamp> = replay.iter().map(|bar| bar.0).collect();
    assert_eq!(
        closes,
        ["11:00", "12:00", "13:00", "14:00", "15:00"].map(at)
    );
    assert_eq!(
        replay[1..],
        ["12:00", "13:00", "14:00", "15:00"].map(hour_bar)
    );
    // Each order fills in full at the close of the minute closing its
    // decision bar, stamped at the bar's close.
    let close_of = |hh_mm: &str| minute_bar(at(hh_mm)).close;
    assert_eq!(
        record.fills,
        [
            (
                OrderSide::Buy,
                close_of("11:00"),
                UnixNanos::from(at("11:00"))
            ),
            (
                OrderSide::Sell,
                close_of("12:00"),
                UnixNanos::from(at("12:00"))
            ),
        ]
    );
    assert_eq!(outcome.result.total_orders, 2);
    // 10 shares bought at 600.70 and sold at 601.30, no commission.
    assert_eq!(common::balance(&outcome.result), "10006.00 EUR");
}
