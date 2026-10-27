//! CME gold's book as Databento's MBP-10 fills the catalog with it:
//! GC.v.0.GLBX, the continuous front contract, a `FuturesContract`, and
//! depth snapshots of 10 levels a side keyed by its id, its trades, no
//! quotes. Here a few snapshots and trades on Wednesday 21 October 2026,
//! the day of `common::ic`.

use std::path::Path;

use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{OrderBookDepth, TradeTick, stubs::stub_depth10},
    enums::AggressorSide,
    identifiers::{InstrumentId, Symbol, TradeId},
    instruments::{InstrumentAny, stubs::futures_contract_es},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use ustr::Ustr;

use super::ic::at;

pub const INSTRUMENT_ID: &str = "GC.v.0.GLBX";
/// Bars of gold the catalog holds no source of: left out of a backtest.
pub const MINUTE_BARS: &str = "GC.v.0.GLBX-1-MINUTE-MID-INTERNAL";

/// The continuous gold contract, live through 2026.
pub fn instrument() -> InstrumentAny {
    let mut gold = futures_contract_es(
        Some(UnixNanos::from(
            at("00:00:00") - jiff::SignedDuration::from_hours(24 * 300),
        )),
        Some(UnixNanos::from(
            at("00:00:00") + jiff::SignedDuration::from_hours(24 * 60),
        )),
    );
    gold.id = InstrumentId::from(INSTRUMENT_ID);
    gold.raw_symbol = Symbol::from("GCZ6");
    gold.underlying = Ustr::from("GC");
    InstrumentAny::FuturesContract(gold)
}

/// A snapshot of 10 levels a side at `time`, its best bid `best_bid`, the
/// levels 1.00 apart, the best ask 1.00 above the best bid.
pub fn depth(time: Timestamp, best_bid: &str) -> OrderBookDepth {
    let mut depth = stub_depth10();
    let shift = Price::from(best_bid) - depth.bids[0].price;
    for order in depth.bids.iter_mut().chain(depth.asks.iter_mut()) {
        order.price = order.price + shift;
    }
    depth.instrument_id = InstrumentId::from(INSTRUMENT_ID);
    depth.ts_event = time.into();
    depth.ts_init = time.into();
    depth
}

/// The snapshots stored: one before the backtests' start, then three.
pub fn depths() -> Vec<OrderBookDepth> {
    vec![
        depth(at("08:30:00"), "4010.00"),
        depth(at("09:00:30"), "4011.00"),
        depth(at("10:15:00"), "4012.50"),
        depth(at("11:45:00"), "4009.00"),
    ]
}

/// Writes gold and `depths()` into the catalog in `dir`, the book stored
/// over 08:00 to 12:00.
pub fn store(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    catalog
        .write_to_parquet(
            &depths(),
            Some(at("08:00:00").into()),
            Some(at("12:00:00").into()),
            None,
        )
        .unwrap();
}

/// A trade of one contract at `time` and `price`, bought.
pub fn trade(time: Timestamp, price: &str) -> TradeTick {
    TradeTick::new(
        InstrumentId::from(INSTRUMENT_ID),
        Price::from(price),
        Quantity::from(1),
        AggressorSide::Buy,
        TradeId::from(time.as_millisecond().to_string().as_str()),
        time.into(),
        time.into(),
    )
}

/// The trades stored: one before the backtests' start, then two.
pub fn trades() -> Vec<TradeTick> {
    vec![
        trade(at("08:45:00"), "4010.50"),
        trade(at("09:10:00"), "4011.50"),
        trade(at("11:50:00"), "4009.50"),
    ]
}

/// Writes `trades()` into the catalog in `dir`, stored over 08:00 to
/// 12:00.
pub fn store_trades(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog
        .write_to_parquet(
            &trades(),
            Some(at("08:00:00").into()),
            Some(at("12:00:00").into()),
            None,
        )
        .unwrap();
}
