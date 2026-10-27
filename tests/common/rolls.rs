//! CME gold rolled: the root GC.GLBX, which the roll calendar rolls from
//! August's contract (GCQ6.GLBX) to December's (GCZ6.GLBX) at 00:00 UTC on
//! Friday 31 July 2026, and a catalog storing both contracts as fill-catalog
//! stores them, each under its own id with its own definition, their trades
//! and book depth over 22:00 on Thursday 30 July to 02:00 on 31 July, on
//! both sides of the roll. The root itself is not stored.

use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{OrderBookDepth, TradeTick, stubs::stub_depth10},
    enums::AggressorSide,
    identifiers::{InstrumentId, Symbol, TradeId},
    instruments::{InstrumentAny, stubs::futures_contract_es},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use tempfile::TempDir;
use ustr::Ustr;

use super::ts;

pub const ROOT: &str = "GC.GLBX";
/// The front contract up to the roll.
pub const PRE: &str = "GCQ6.GLBX";
/// The front contract from the roll on.
pub const POST: &str = "GCZ6.GLBX";

/// `hh:mm:ss.fffffffff` UTC on Thursday 30 July 2026.
pub fn thursday(time: &str) -> Timestamp {
    ts(&format!("2026-07-30T{time}Z"))
}

/// `hh:mm:ss.fffffffff` UTC on Friday 31 July 2026.
pub fn friday(time: &str) -> Timestamp {
    ts(&format!("2026-07-31T{time}Z"))
}

/// One nanosecond.
pub const NANOSECOND: SignedDuration = SignedDuration::from_nanos(1);

/// The roll: 00:00 UTC on Friday 31 July 2026.
pub fn roll() -> Timestamp {
    friday("00:00:00")
}

/// The contract `id`, raw symbol `raw`, expiring at `expiration`.
fn contract(id: &str, raw: &str, expiration: &str) -> InstrumentAny {
    let mut gold = futures_contract_es(
        Some(UnixNanos::from(ts("2024-01-02T00:00:00Z"))),
        Some(UnixNanos::from(ts(expiration))),
    );
    gold.id = InstrumentId::from(id);
    gold.raw_symbol = Symbol::from(raw);
    gold.underlying = Ustr::from("GC");
    InstrumentAny::FuturesContract(gold)
}

/// A trade of `id` at `time` and `price`, bought.
fn trade(id: &str, time: Timestamp, price: &str) -> TradeTick {
    TradeTick::new(
        InstrumentId::from(id),
        Price::from(price),
        Quantity::from(1),
        AggressorSide::Buy,
        TradeId::from(time.as_nanosecond().to_string().as_str()),
        time.into(),
        time.into(),
    )
}

/// A snapshot of 10 levels a side of `id` at `time`.
fn depth(id: &str, time: Timestamp) -> OrderBookDepth {
    let mut depth = stub_depth10();
    depth.instrument_id = InstrumentId::from(id);
    depth.ts_event = time.into();
    depth.ts_init = time.into();
    depth
}

/// The trades of August's contract: one before the backtests' start, two
/// in its slice (the last at its last nanosecond), two after the roll.
pub fn pre_trades() -> Vec<TradeTick> {
    vec![
        trade(PRE, thursday("22:30:00"), "3990.00"),
        trade(PRE, thursday("23:30:00"), "3991.00"),
        trade(PRE, thursday("23:59:59.999999999"), "3992.00"),
        trade(PRE, roll(), "3992.50"),
        trade(PRE, friday("00:30:00"), "3993.00"),
    ]
}

/// The trades of December's contract: one before the roll, two in its
/// slice (the first at the roll), one after the backtests' end.
pub fn post_trades() -> Vec<TradeTick> {
    vec![
        trade(POST, thursday("23:15:00"), "4040.00"),
        trade(POST, roll(), "4041.00"),
        trade(POST, friday("00:45:00"), "4042.00"),
        trade(POST, friday("01:30:00"), "4043.00"),
    ]
}

/// The snapshots of August's contract: one in its slice, one after.
pub fn pre_depths() -> Vec<OrderBookDepth> {
    vec![
        depth(PRE, thursday("23:45:00")),
        depth(PRE, friday("00:10:00")),
    ]
}

/// The snapshots of December's contract: one before its slice, one in it.
pub fn post_depths() -> Vec<OrderBookDepth> {
    vec![
        depth(POST, thursday("23:50:00")),
        depth(POST, friday("00:20:00")),
    ]
}

/// A catalog of both contracts, their trades and their book depth, each
/// stored over 22:00 Thursday to 02:00 Friday; December's trades left out
/// unless `post_trades_stored`.
pub fn catalog_storing(post_trades_stored: bool) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog
        .write_instruments(vec![
            contract(PRE, "GCQ6", "2026-08-27T17:30:00Z"),
            contract(POST, "GCZ6", "2026-12-29T18:30:00Z"),
        ])
        .unwrap();
    let (from, to) = (
        Some(thursday("22:00:00").into()),
        Some(friday("02:00:00").into()),
    );
    let mut trades = vec![pre_trades()];
    if post_trades_stored {
        trades.push(post_trades());
    }
    for trades in trades {
        catalog.write_to_parquet(&trades, from, to, None).unwrap();
    }
    for depths in [pre_depths(), post_depths()] {
        catalog.write_to_parquet(&depths, from, to, None).unwrap();
    }
    dir
}

/// A catalog of both contracts, their trades and their book depth.
pub fn catalog() -> TempDir {
    catalog_storing(true)
}
