//! CME gold's book as Databento's MBP-10 fills the catalog with it:
//! GC.v.0.GLBX, the continuous front contract, a `FuturesContract`, and
//! depth snapshots of 10 levels a side keyed by its id, its trades, no
//! quotes. Here a few snapshots and trades on Wednesday 21 October 2026,
//! the day of `common::ic`; and its book deltas, as Databento's MBO fills
//! the catalog with them, and custom data of it, `Imbalance`, a type of
//! the tests'.

use std::{path::Path, sync::Arc};

use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{
        BookOrder, CustomData, DataType, OrderBookDelta, OrderBookDepth, TradeTick,
        stubs::stub_depth10,
    },
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::{InstrumentId, Symbol, TradeId},
    instruments::{InstrumentAny, stubs::futures_contract_es},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_serialization::arrow_custom_data;
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

/// A bid of one contract at `price`, order `order_id`, added to the book at
/// `time`; the last delta of its event if `last`.
pub fn delta(time: Timestamp, price: &str, order_id: u64, last: bool) -> OrderBookDelta {
    OrderBookDelta::new(
        InstrumentId::from(INSTRUMENT_ID),
        BookAction::Add,
        BookOrder::new(
            OrderSide::Buy,
            Price::from(price),
            Quantity::from(1),
            order_id,
        ),
        if last { RecordFlag::F_LAST as u8 } else { 0 },
        order_id,
        time.into(),
        time.into(),
    )
}

/// The deltas stored: an event before the backtests' start, then an event
/// of two deltas and an event of one.
pub fn deltas() -> Vec<OrderBookDelta> {
    vec![
        delta(at("08:40:00"), "4010.00", 1, true),
        delta(at("09:20:00"), "4011.00", 2, false),
        delta(at("09:20:00"), "4010.75", 3, true),
        delta(at("10:40:00"), "4012.00", 4, true),
    ]
}

/// Writes `deltas()` into the catalog in `dir`, stored over 08:00 to 12:00.
pub fn store_deltas(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog
        .write_to_parquet(
            &deltas(),
            Some(at("08:00:00").into()),
            Some(at("12:00:00").into()),
            None,
        )
        .unwrap();
}

/// Custom data of the tests: an imbalance of gold's book.
#[arrow_custom_data]
#[custom_data]
pub struct Imbalance {
    pub value: f64,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

/// The type name `Imbalance` is stored and published under.
pub const IMBALANCE: &str = "Imbalance";

/// The data type of gold's `Imbalance`s, keyed by its id, as Nautilus'
/// Databento client keys its custom data.
pub fn imbalance_type() -> DataType {
    DataType::new(IMBALANCE, None, Some(INSTRUMENT_ID.to_string()))
}

/// The imbalances stored: one before the backtests' start, then two.
pub fn imbalances() -> Vec<Imbalance> {
    [("08:50:00", 0.5), ("09:30:00", -0.25), ("11:00:00", 0.75)]
        .map(|(time, value)| Imbalance::new(value, at(time).into(), at(time).into()))
        .to_vec()
}

/// Writes `imbalances()` into the catalog in `dir`, stored over 08:00 to
/// 12:00. Registers `Imbalance` with Nautilus first, as any reader of it
/// must.
pub fn store_imbalances(dir: &Path) {
    nautilus_serialization::ensure_custom_data_registered::<Imbalance>();
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let data: Vec<CustomData> = imbalances()
        .into_iter()
        .map(|imbalance| CustomData::new(Arc::new(imbalance), imbalance_type()))
        .collect();
    catalog
        .write_custom_data_batch(
            data,
            Some(at("08:00:00").into()),
            Some(at("12:00:00").into()),
            None,
        )
        .unwrap();
}
