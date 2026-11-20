//! CME gold's book as Databento's MBP-10 fills the catalog: GC.v.0.GLBX, the continuous front
//! contract (a `FuturesContract`), its trades, and depth snapshots of 10 levels a side keyed by its
//! id, no quotes, on Wednesday 21 October 2026, the day of `common::ic`. Also its book deltas, as
//! Databento's MBO fills the catalog, custom data under its id (`Imbalance`), and, apart, its
//! quotes, as LSE streams CME Globex's Level 1.

use std::{path::Path, sync::Arc};

use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{
        BookOrder, CustomData, DataType, OrderBookDelta, OrderBookDepth, QuoteTick, TradeTick,
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

pub(crate) const INSTRUMENT_ID: &str = "GC.v.0.GLBX";

/// Bars of gold the catalog holds no source of: left out of a backtest.
pub(crate) const MINUTE_BARS: &str = "GC.v.0.GLBX-1-MINUTE-MID-INTERNAL";

pub(crate) fn instrument() -> InstrumentAny {
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

/// A snapshot of 10 levels a side at `time`, 1.00 apart, the best ask 1.00 above `best_bid`.
pub(crate) fn depth(time: Timestamp, best_bid: &str) -> OrderBookDepth {
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

/// One snapshot before the backtests' start, then three.
pub(crate) fn depths() -> Vec<OrderBookDepth> {
    vec![
        depth(at("08:30:00"), "4010.00"),
        depth(at("09:00:30"), "4011.00"),
        depth(at("10:15:00"), "4012.50"),
        depth(at("11:45:00"), "4009.00"),
    ]
}

pub(crate) fn store(dir: &Path) {
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

/// A quote at `time`, as LSE streams CME Globex's Level 1, sized 10,000 contracts a side.
pub(crate) fn quote(time: Timestamp, bid: &str, ask: &str) -> QuoteTick {
    let size = Quantity::from(10_000);
    QuoteTick::new(
        InstrumentId::from(INSTRUMENT_ID),
        Price::from(bid),
        Price::from(ask),
        size,
        size,
        time.into(),
        time.into(),
    )
}

pub(crate) fn store_quotes(dir: &Path, quotes: &[QuoteTick], from: Timestamp, to: Timestamp) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    catalog
        .write_to_parquet(quotes, Some(from.into()), Some(to.into()), None)
        .unwrap();
}

pub(crate) fn trade(time: Timestamp, price: &str) -> TradeTick {
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

/// One trade before the backtests' start, then two.
pub(crate) fn trades() -> Vec<TradeTick> {
    vec![
        trade(at("08:45:00"), "4010.50"),
        trade(at("09:10:00"), "4011.50"),
        trade(at("11:50:00"), "4009.50"),
    ]
}

pub(crate) fn store_trades(dir: &Path) {
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

/// Marks the last delta of its event if `last`.
pub(crate) fn delta(time: Timestamp, price: &str, order_id: u64, last: bool) -> OrderBookDelta {
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

/// An event before the backtests' start, then an event of two deltas and one of one.
pub(crate) fn deltas() -> Vec<OrderBookDelta> {
    vec![
        delta(at("08:40:00"), "4010.00", 1, true),
        delta(at("09:20:00"), "4011.00", 2, false),
        delta(at("09:20:00"), "4010.75", 3, true),
        delta(at("10:40:00"), "4012.00", 4, true),
    ]
}

pub(crate) fn store_deltas(dir: &Path) {
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

/// An imbalance of gold's book.
#[arrow_custom_data]
#[custom_data]
pub struct Imbalance {
    pub value: f64,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

pub(crate) const IMBALANCE: &str = "Imbalance";

/// The data type of gold's `Imbalance`s, keyed by its id as Nautilus' Databento client keys its
/// custom data.
pub(crate) fn imbalance_type() -> DataType {
    DataType::new(IMBALANCE, None, Some(INSTRUMENT_ID.to_string()))
}

/// One imbalance before the backtests' start, then two.
pub(crate) fn imbalances() -> Vec<Imbalance> {
    [("08:50:00", 0.5), ("09:30:00", -0.25), ("11:00:00", 0.75)]
        .map(|(time, value)| Imbalance::new(value, at(time).into(), at(time).into()))
        .to_vec()
}

/// Registers `Imbalance` with Nautilus before storing, as any reader of it must.
pub(crate) fn store_imbalances(dir: &Path) {
    store_imbalances_as(dir, &imbalance_type());
}

pub(crate) fn store_imbalances_as(dir: &Path, data_type: &DataType) {
    nautilus_serialization::ensure_custom_data_registered::<Imbalance>();
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let data: Vec<CustomData> = imbalances()
        .into_iter()
        .map(|imbalance| CustomData::new(Arc::new(imbalance), data_type.clone()))
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
