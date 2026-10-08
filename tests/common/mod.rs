//! The fixture of the backtest tests: a temporary catalog holding one ETF,
//! SXR8.IBIS in EUR, synthetic hourly bars whose closes follow a wave, so
//! that a fast and a slow EMA cross several times, and one quote per bar
//! around its close.

#![allow(dead_code, reason = "each test file uses part of the fixture")]

pub mod probe;

use std::path::Path;

use helix_trader::{
    backtest::Backtest,
    strategy::ema::{EmaCross, EmaCrossConfig, Session},
};
use jiff::{SignedDuration, Timestamp, civil::time, tz::TimeZone};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, QuoteTick},
    identifiers::{InstrumentId, Symbol},
    instruments::{InstrumentAny, stubs::equity_aapl},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use tempfile::TempDir;

pub const INSTRUMENT_ID: &str = "SXR8.IBIS";
pub const BAR_TYPE: &str = "SXR8.IBIS-1-HOUR-LAST-EXTERNAL";
/// The bars stored before the run start.
pub const BARS_BEFORE_START: i64 = 72;
/// The bars replayed in the run.
pub const BARS_IN_RUN: i64 = 120;
/// The warmup the strategies ask for: the last 48 bars before the start.
pub const WARMUP: SignedDuration = SignedDuration::from_hours(48);

/// The first stored bar closes here.
fn first_close() -> Timestamp {
    "2026-01-05T00:00:00Z".parse().unwrap()
}

fn hours(n: i64) -> SignedDuration {
    SignedDuration::from_hours(n)
}

/// Half an hour after the last bar before the run, so no bar sits on the
/// boundary between warmup and replay.
pub fn start() -> Timestamp {
    first_close() + hours(BARS_BEFORE_START - 1) + SignedDuration::from_mins(30)
}

pub fn end() -> Timestamp {
    first_close() + hours(BARS_BEFORE_START + BARS_IN_RUN)
}

pub fn instrument() -> InstrumentAny {
    let mut equity = equity_aapl();
    equity.id = InstrumentId::from(INSTRUMENT_ID);
    equity.raw_symbol = Symbol::from("SXR8");
    equity.isin = Some("IE00B5BMR087".into());
    equity.currency = Currency::EUR();
    InstrumentAny::Equity(equity)
}

/// Every stored bar, oldest first: closes on a wave of 30 bars around 600
/// EUR, each bar stamped on its close.
pub fn bars() -> Vec<Bar> {
    let bar_type = BarType::from(BAR_TYPE);
    let close = |i: i64| 600.0 + 5.0 * (std::f64::consts::TAU * i as f64 / 30.0).sin();
    (0..BARS_BEFORE_START + BARS_IN_RUN)
        .map(|i| {
            let (open, close) = (close(i - 1), close(i));
            let ts = (first_close() + hours(i)).into();
            let price = |p: f64| Price::new(p, 2);
            Bar::new(
                bar_type,
                price(open),
                price(open.max(close) + 0.1),
                price(open.min(close) - 0.1),
                price(close),
                Quantity::from(1000),
                ts,
                ts,
            )
        })
        .collect()
}

/// Half the spread of every quote.
pub fn half_spread() -> Price {
    Price::from("0.05")
}

/// One quote per bar, oldest first: the close less (bid) and plus (ask)
/// the half spread, sizes 1,000,000, stamped 1 ns before the bar's close.
pub fn quotes() -> Vec<QuoteTick> {
    let size = Quantity::from(1_000_000);
    bars()
        .iter()
        .map(|bar| {
            let ts = UnixNanos::from(bar.ts_event.as_u64() - 1);
            QuoteTick::new(
                bar.bar_type.instrument_id(),
                bar.close - half_spread(),
                bar.close + half_spread(),
                size,
                size,
                ts,
                ts,
            )
        })
        .collect()
}

/// Writes the instrument and the bars to a new temporary catalog, stored
/// over `[first_close, end]` as a fill up to `end` stores them, but no
/// quotes.
pub fn catalog_without_quotes() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    catalog
        .write_to_parquet(
            &bars(),
            Some(first_close().into()),
            Some(end().into()),
            None,
        )
        .unwrap();
    dir
}

/// The catalog without quotes, plus the quotes stored from the first one
/// to `end`.
pub fn catalog() -> TempDir {
    let dir = catalog_without_quotes();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let quotes = quotes();
    catalog
        .write_to_parquet(&quotes, Some(quotes[0].ts_init), Some(end().into()), None)
        .unwrap();
    dir
}

/// A backtest of the fixture over the run window, with `commission` per
/// order and 10,000 EUR to start.
pub fn backtest(catalog: &Path, commission: &str) -> Backtest {
    Backtest {
        catalog: catalog.to_path_buf(),
        bar_types: vec![BarType::from(BAR_TYPE)],
        start: start(),
        end: end(),
        warmup: WARMUP,
        commission: Money::from(commission),
        balance: Money::from("10000 EUR"),
        logging: LoggerConfig::default(),
    }
}

/// The session of the EMA crossover on the fixture: Xetra's continuous
/// trading, so the bars closing 10:00 to 17:00 Frankfurt time (09:00 to
/// 16:00 UTC in January) count, eight of the 24 a day.
pub fn session() -> Session {
    Session {
        time_zone: TimeZone::get("Europe/Berlin").unwrap(),
        open: time(9, 0, 0, 0),
        close: time(17, 30, 0, 0),
    }
}

/// The EMA crossover on the fixture: 5 against 15 bars, 10 shares long or
/// short, inside the session.
pub fn ema_cross() -> EmaCross {
    let config = EmaCrossConfig::builder()
        .instrument_ids(vec![InstrumentId::from(INSTRUMENT_ID)])
        .bar_spec(BarType::from(BAR_TYPE).spec())
        .fast_period(5)
        .slow_period(15)
        .trade_size(Quantity::from(10))
        .warmup(WARMUP)
        .session(session())
        .build();
    EmaCross::new(config).unwrap()
}

/// The orders the EMA crossover sends on the fixture.
pub const EMA_ORDERS: usize = 4;
/// Its PnL (total) on the fixture without commission: the spread paid
/// included.
pub const EMA_PNL_WITHOUT_COMMISSION: f64 = -234.0;
