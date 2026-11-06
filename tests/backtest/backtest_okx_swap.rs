//! An OKX swap on 1-minute candles: BTC-USDT-SWAP.OKX, a `CryptoPerpetual`
//! of 0.01 BTC a contract settled in USDT, with the margins of 1
//! `fill-catalog` gives it, and its minute bars stamped as `fill-catalog`
//! stores OKX's candles: `ts_event` at the minute's open, `ts_init` at its
//! close. A recorder on the hour bars Nautilus builds from them (a composite
//! bar type) gets each hour bar holding the minutes opening in its hour;
//! a run on the candles alone, with no quote, reports no spread. One node
//! per thread, so one run per test.

use std::path::Path;

use crate::common::{Ohlc, ohlc, recorder::Recorder, ts};
use helix_trader::{
    backtest::{Backtest, commission_rate, report},
    venues,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType},
    enums::OrderSide,
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

const INSTRUMENT_ID: &str = "BTC-USDT-SWAP.OKX";
const MINUTE_BARS: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";
const HOUR_BARS: &str = "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

/// How long after its close the node builds an hour bar of minute bars,
/// and stamps it: 5 s, as a live node waits for the last minute.
const BUILT_AFTER: SignedDuration = SignedDuration::from_secs(5);

/// `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// BTC-USDT-SWAP as OKX defines it: price tick 0.1, lot 0.01 contracts,
/// 0.01 BTC a contract, with initial and maintenance margins of 1.
fn instrument() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(
        CryptoPerpetual::builder()
            .instrument_id(InstrumentId::from(INSTRUMENT_ID))
            .raw_symbol(Symbol::from("BTC-USDT-SWAP"))
            .base_currency(Currency::BTC())
            .quote_currency(Currency::USDT())
            .settlement_currency(Currency::USDT())
            .is_inverse(false)
            .price_precision(1)
            .size_precision(2)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from("0.01"))
            .multiplier(Quantity::from("0.01"))
            .lot_size(Quantity::from("0.01"))
            .min_quantity(Quantity::from("0.01"))
            .margin_init(Decimal::ONE)
            .margin_maint(Decimal::ONE)
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The minute bar opening at `open`, the k-th minute after 08:00: it
/// opens at 100,000 plus k tenths, its high is 2 above, its low 1 below,
/// its close 0.5 above, 500 contracts. The minute opening at 08:59 has its
/// low 50 lower, the one opening at 10:00 its high 50 higher, so that a
/// minute in the wrong hour shows. Stamped as `fill-catalog` stores OKX's
/// candles: `ts_event` at the open, `ts_init` at the close.
fn minute_bar(open: Timestamp) -> Bar {
    let k = open.duration_since(at("08:00")).as_mins();
    let first = 100_000.0 + 0.1 * k as f64;
    let high = first + 2.0 + if open == at("10:00") { 50.0 } else { 0.0 };
    let low = first - 1.0 - if open == at("08:59") { 50.0 } else { 0.0 };
    let price = |value: f64| Price::new(value, 1);
    Bar::new(
        BarType::from(MINUTE_BARS),
        price(first),
        price(high),
        price(low),
        price(first + 0.5),
        Quantity::from("500.00"),
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

/// The hour bar closing at `close`, worked out here: the minutes opening
/// in `[close − 1 h, close)`, or from `from` if later, stamped
/// `BUILT_AFTER` its close.
fn hour_bar(close: Timestamp, from: Timestamp) -> Ohlc {
    let minutes = minute_bars(from.max(close - SignedDuration::from_hours(1)), close);
    let high = minutes.iter().map(|bar| bar.high).max().unwrap();
    let low = minutes.iter().map(|bar| bar.low).min().unwrap();
    let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
    (close + BUILT_AFTER, first.open, high, low, last.close)
}

/// A catalog in `dir` of the instrument and its minute bars opening from
/// 08:00 to 10:59, stored over `[08:00, 11:00]`.
fn catalog(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    catalog
        .write_to_parquet(
            &minute_bars(at("08:00"), at("11:00")),
            Some(UnixNanos::from(at("08:00"))),
            Some(UnixNanos::from(at("11:00"))),
            None,
        )
        .unwrap();
}

/// A run on the hour bars from 08:30 to 11:01 at OKX's taker fee, 0.05%.
fn backtest(dir: &Path) -> Backtest {
    Backtest {
        catalog: dir.to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start: at("08:30"),
        end: at("11:01"),
        ticks: vec![],
        fee_model: commission_rate(Decimal::new(5, 4)).unwrap(),
        balance: Money::from("10000 USDT"),
        leverage: Decimal::ONE,
        venue_oms: venues::oms_type,
        logging: LoggerConfig::default(),
    }
}

#[test]
fn each_hour_bar_of_okx_candles_holds_the_minutes_opening_in_its_hour() {
    let dir = tempfile::tempdir().unwrap();
    catalog(dir.path());
    let (recorder, record) = Recorder::new(BarType::from(HOUR_BARS));

    backtest(dir.path()).run(recorder).unwrap();

    // The run starts at 08:30: the first bar replayed is the minute opening
    // 08:29, stamped (`ts_init`) at its close, 08:30, so the hour bar
    // closing 09:00 holds the minutes opening from 08:29. Each hour bar
    // holds the minutes opening in its hour, built 5 s after its close:
    // 09:00 has the minute opening 08:59, 10:00 has 09:00 to 09:59, not the
    // minute opening at 10:00, and 11:00, built before the run's end, has
    // 10:00 to 10:59.
    let replay: Vec<_> = record.borrow().bars.iter().map(ohlc).collect();
    assert_eq!(
        replay,
        [
            hour_bar(at("09:00"), at("08:29")),
            hour_bar(at("10:00"), at("09:00")),
            hour_bar(at("11:00"), at("10:00")),
        ]
    );
    assert_eq!(replay[1].2, Price::from("100013.9"));
    assert_eq!(replay[2].2, Price::from("100064.0"));
}

/// A buy on the first hour bar fills on a candle, with no quote to measure
/// a spread on.
#[test]
fn a_run_on_candles_alone_reports_its_spread_as_not_applicable() {
    let dir = tempfile::tempdir().unwrap();
    catalog(dir.path());
    let (recorder, _) = Recorder::trading(
        BarType::from(HOUR_BARS),
        vec![(OrderSide::Buy, Quantity::from("2.00"))],
    );

    let outcome = backtest(dir.path()).run(recorder).unwrap();

    let report = report(&outcome);
    assert!(report.contains("Costs (USDT)"), "{report}");
    assert!(report.contains("  Spread: n/a (no quotes)\n"), "{report}");
}
