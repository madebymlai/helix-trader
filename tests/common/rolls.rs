//! CME gold rolled: the root GC.GLBX, which the roll calendar rolls from
//! August's contract (GCQ26.GLBX) to December's (GCZ26.GLBX) at 00:00 UTC on
//! Wednesday 29 July 2026 (two weekdays before Friday 31 July, the last
//! weekday before August), and a catalog storing both contracts as
//! fill-catalog stores them, each under its own id with its own definition,
//! their trades and book depth over 22:00 on Tuesday 28 July to 02:00 on
//! 29 July, on both sides of the roll. The root itself is not stored.

use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{OrderBookDepth, QuoteTick, TradeTick, stubs::stub_depth10},
    enums::AggressorSide,
    identifiers::{InstrumentId, Symbol, TradeId},
    instruments::{InstrumentAny, stubs::futures_contract_es},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use tempfile::TempDir;
use ustr::Ustr;

use super::ts;

pub(crate) const ROOT: &str = "GC.GLBX";
/// The front contract up to the roll.
pub(crate) const PRE: &str = "GCQ26.GLBX";
/// The front contract from the roll on.
pub(crate) const POST: &str = "GCZ26.GLBX";

/// `hh:mm:ss.fffffffff` UTC on Tuesday 28 July 2026.
pub(crate) fn tuesday(time: &str) -> Timestamp {
    ts(&format!("2026-07-28T{time}Z"))
}

/// `hh:mm:ss.fffffffff` UTC on Wednesday 29 July 2026.
pub(crate) fn wednesday(time: &str) -> Timestamp {
    ts(&format!("2026-07-29T{time}Z"))
}

/// One nanosecond.
pub(crate) const NANOSECOND: SignedDuration = SignedDuration::from_nanos(1);

/// The roll: 00:00 UTC on Wednesday 29 July 2026.
pub(crate) fn roll() -> Timestamp {
    wednesday("00:00:00")
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
pub(crate) fn trade(id: &str, time: Timestamp, price: &str) -> TradeTick {
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

/// A quote of `id` at `time`, its bid and ask 10 cents either side of
/// `mid`, of one contract each.
pub(crate) fn quote(id: &str, time: Timestamp, mid: &str) -> QuoteTick {
    let mid: Price = Price::from(mid);
    let half = Price::from("0.10");
    QuoteTick::new(
        InstrumentId::from(id),
        mid - half,
        mid + half,
        Quantity::from(1),
        Quantity::from(1),
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

/// The trades of August's contract: two before the backtests' start, not
/// replayed, two in its slice (the last at its last nanosecond), two after
/// the roll.
pub(crate) fn pre_trades() -> Vec<TradeTick> {
    vec![
        trade(PRE, tuesday("22:30:00"), "3990.00"),
        trade(PRE, tuesday("22:45:00"), "3989.00"),
        trade(PRE, tuesday("23:30:00"), "3991.00"),
        trade(PRE, tuesday("23:59:59.999999999"), "3992.00"),
        trade(PRE, roll(), "3992.50"),
        trade(PRE, wednesday("00:30:00"), "3993.00"),
    ]
}

/// The trades of December's contract: one before the roll, two in its
/// slice (the first at the roll), one after the backtests' end.
pub(crate) fn post_trades() -> Vec<TradeTick> {
    vec![
        trade(POST, tuesday("23:15:00"), "4040.00"),
        trade(POST, roll(), "4041.00"),
        trade(POST, wednesday("00:45:00"), "4042.00"),
        trade(POST, wednesday("01:30:00"), "4043.00"),
    ]
}

/// The snapshots of August's contract: one in its slice, one after.
pub(crate) fn pre_depths() -> Vec<OrderBookDepth> {
    vec![
        depth(PRE, tuesday("23:45:00")),
        depth(PRE, wednesday("00:10:00")),
    ]
}

/// The snapshots of December's contract: one before its slice, one in it.
pub(crate) fn post_depths() -> Vec<OrderBookDepth> {
    vec![
        depth(POST, tuesday("23:50:00")),
        depth(POST, wednesday("00:20:00")),
    ]
}

/// A catalog of both contracts, their trades and their book depth, each
/// stored over 22:00 Tuesday to 02:00 Wednesday; December's trades left out
/// unless `post_trades_stored`.
pub(crate) fn catalog_storing(post_trades_stored: bool) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog
        .write_instruments(vec![
            contract(PRE, "GCQ6", "2026-08-27T17:30:00Z"),
            contract(POST, "GCZ6", "2026-12-29T18:30:00Z"),
        ])
        .unwrap();
    let (from, to) = (
        Some(tuesday("22:00:00").into()),
        Some(wednesday("02:00:00").into()),
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
pub(crate) fn catalog() -> TempDir {
    catalog_storing(true)
}

/// A catalog of both contracts and `trades`, of either, stored over 22:00
/// Tuesday to 02:00 Wednesday.
pub(crate) fn catalog_of_trades(trades: &[TradeTick]) -> TempDir {
    catalog_of_ticks(trades, &[])
}

/// A catalog of both contracts, `trades` and `quotes`, of either, stored
/// over 22:00 Tuesday to 02:00 Wednesday.
pub(crate) fn catalog_of_ticks(trades: &[TradeTick], quotes: &[QuoteTick]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog
        .write_instruments(vec![
            contract(PRE, "GCQ6", "2026-08-27T17:30:00Z"),
            contract(POST, "GCZ6", "2026-12-29T18:30:00Z"),
        ])
        .unwrap();
    let (from, to) = (
        Some(tuesday("22:00:00").into()),
        Some(wednesday("02:00:00").into()),
    );
    for contract in [PRE, POST] {
        let of: Vec<TradeTick> = trades
            .iter()
            .filter(|trade| trade.instrument_id == InstrumentId::from(contract))
            .copied()
            .collect();
        catalog.write_to_parquet(&of, from, to, None).unwrap();
        let of: Vec<QuoteTick> = quotes
            .iter()
            .filter(|quote| quote.instrument_id == InstrumentId::from(contract))
            .copied()
            .collect();

        if !of.is_empty() {
            catalog.write_to_parquet(&of, from, to, None).unwrap();
        }
    }
    dir
}
