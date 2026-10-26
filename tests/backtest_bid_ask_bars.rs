//! A strategy on hour bars of IB's 1-minute MID bars, over a catalog
//! holding them with the BID and ASK minute bars, all as IB gives them:
//! `fill_catalog` stores the three series `strategy::catalog_series` lists
//! for the composite bar type, the backtest replays them, the hour bars
//! are built from the MID bars alone, and the matching engine pairs the
//! BID and ASK bars into the quotes orders fill on
//! (`execution/src/matching_engine/mod.rs`, `process_quote_ticks_from_bar`).
//!
//! IB's BID, ASK and MID bars carry no volume (IB gives -1, the adapter
//! stores 0), and the matching engine takes a bar quote of size zero for
//! an empty side (`process_bar_quote_tick`): every BID and ASK pair clears
//! the book, so a market order is rejected `No market`. Pinned here as
//! observed at the pinned Nautilus commit, not as wanted. One node per
//! process, so one run per file.

mod common;

use common::{instrument, ohlc, recorder::Recorder, ts};
use helix_trader::{data::fill_catalog, strategy::catalog_series};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, Data, NautilusDataType},
    enums::{OrderSide, PriceType},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const COMPOSITE: &str = "SXR8.IBIS-1-HOUR-MID-INTERNAL@1-MINUTE-EXTERNAL";

/// `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// The minute bar of `bar_type` closing at `close`, stamped at its close
/// and without volume, as IB gives BID, ASK and MID bars: the k-th minute
/// after 10:00 has the mid 600 + 0.01 k, the BID bar 0.05 below it, the
/// ASK bar 0.05 above, each with a high 0.03 above and a low 0.03 below.
fn minute_bar(bar_type: BarType, close: Timestamp) -> Bar {
    let k = close.duration_since(at("10:00")).as_mins();
    let offset = match bar_type.spec().price_type {
        PriceType::Bid => -0.05,
        PriceType::Ask => 0.05,
        _ => 0.0,
    };
    let price = 600.0 + 0.01 * k as f64 + offset;
    let stamp = UnixNanos::from(close);
    Bar::new(
        bar_type,
        Price::new(price, 2),
        Price::new(price + 0.03, 2),
        Price::new(price - 0.03, 2),
        Price::new(price, 2),
        Quantity::from(0),
        stamp,
        stamp,
    )
}

/// The minute bars of `bar_type` closing in `[from, to]` after 10:00.
fn minute_bars(bar_type: BarType, from: Timestamp, to: Timestamp) -> Vec<Bar> {
    let mut close = from;
    let mut bars = vec![];
    while close <= to {
        if close > at("10:00") {
            bars.push(minute_bar(bar_type, close));
        }
        close += SignedDuration::from_mins(1);
    }
    bars
}

#[test]
fn hour_bars_come_from_the_mid_bars_and_orders_find_no_market_in_bid_and_ask_bars_without_volume() {
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let bar_types = [BarType::from(COMPOSITE)];
    let series = catalog_series(&bar_types);
    let fill = fill_catalog(
        &mut catalog,
        vec![instrument()],
        &series,
        at("10:00"),
        at("15:00"),
        async |_, identifier, from, to| {
            let bar_type = BarType::from(identifier);
            Ok(minute_bars(bar_type, from, to)
                .into_iter()
                .map(Data::Bar)
                .collect())
        },
    );
    // The node runs its own runtime: this one is gone before it starts.
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();
    for (data_type, identifier) in &series {
        assert_eq!(*data_type, NautilusDataType::Bar);
        let stored = catalog
            .bars(Some(vec![identifier.clone()]), None, None)
            .unwrap();
        assert_eq!(stored.len(), 300, "{identifier}");
    }

    let (recorder, record) = Recorder::trading_without_warmup(
        BarType::from(COMPOSITE),
        vec![
            (OrderSide::Buy, Quantity::from(10)),
            (OrderSide::Sell, Quantity::from(10)),
        ],
    );
    let mut backtest = common::backtest(
        dir.path(),
        at("11:00"),
        at("15:00"),
        SignedDuration::ZERO,
        "0 EUR",
    );
    backtest.bar_types = bar_types.to_vec();
    let outcome = backtest.run(recorder).unwrap();

    let record = record.borrow();
    let mid = BarType::from("SXR8.IBIS-1-MINUTE-MID-EXTERNAL");
    let hour_bar = |close: &str| {
        let minutes = minute_bars(mid, at(close) - SignedDuration::from_mins(59), at(close));
        let high = minutes.iter().map(|bar| bar.high).max().unwrap();
        let low = minutes.iter().map(|bar| bar.low).min().unwrap();
        let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
        (at(close), first.open, high, low, last.close)
    };
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    // The engine starts at 10:59:59.999999999: the bar closing 11:00 holds
    // the minute closing then alone; those closing 12:00 to 15:00 are whole.
    let closes: Vec<Timestamp> = replay.iter().map(|bar| bar.0).collect();
    assert_eq!(
        closes,
        ["11:00", "12:00", "13:00", "14:00", "15:00"].map(at)
    );
    assert_eq!(
        replay[1..],
        ["12:00", "13:00", "14:00", "15:00"].map(hour_bar)
    );
    assert_eq!(outcome.result.total_orders, 2);
    assert!(record.fills.is_empty(), "{:?}", record.fills);
}
