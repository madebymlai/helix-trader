//! An OKX swap on 1-minute candles: BTC-USDT-SWAP.OKX, a `CryptoPerpetual`
//! of 0.01 BTC a contract settled in USDT, with the margins of 1
//! `fill-catalog` gives it, and its minute bars stamped as `fill-catalog`
//! stores OKX's candles: `ts_event` at the minute's open, `ts_init` at its
//! close. A recorder on the hour bars Nautilus builds from them (a composite
//! bar type) buys on the first hour bar and flips short on the second: each
//! hour bar holds the minutes opening in its hour, and is seen at its
//! close; the orders fill at the close of the hour's last minute (bar
//! execution); the commission rate is charged on each fill's value;
//! without leverage the open short locks its whole value; with no quotes
//! the report has no spread. One node per thread, so one run per test.

use crate::common::{Ohlc, ohlc, recorder::Recorder, ts};
use helix_trader::backtest::{Backtest, commission_rate, report, venue_oms_type};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType},
    enums::OrderSide,
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

const INSTRUMENT_ID: &str = "BTC-USDT-SWAP.OKX";
const MINUTE_BARS: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";
const HOUR_BARS: &str = "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

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
/// in `[close − 1 h, close)`, or from `from` if later.
fn hour_bar(close: Timestamp, from: Timestamp) -> Ohlc {
    let minutes = minute_bars(from.max(close - SignedDuration::from_hours(1)), close);
    let high = minutes.iter().map(|bar| bar.high).max().unwrap();
    let low = minutes.iter().map(|bar| bar.low).min().unwrap();
    let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
    (close, first.open, high, low, last.close)
}

#[test]
fn an_okx_swap_trades_on_hour_bars_of_its_minute_candles_with_margin_and_commission() {
    let (start, end) = (at("09:00"), at("11:00"));
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    let bars = minute_bars(at("08:00"), at("11:00"));
    catalog
        .write_to_parquet(
            &bars,
            Some(UnixNanos::from(at("08:00"))),
            Some(UnixNanos::from(end)),
            None,
        )
        .unwrap();
    let backtest = Backtest {
        catalog: dir.path().to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start,
        end,
        warmup: SignedDuration::ZERO,
        // OKX's taker fee, 0.05%.
        fee_model: commission_rate(Decimal::new(5, 4)).unwrap(),
        balance: Money::from("10000 USDT"),
        leverage: Decimal::ONE,
        oms_type: venue_oms_type(Venue::from("OKX")),
        logging: LoggerConfig::default(),
    };
    let (recorder, record) = Recorder::trading_without_warmup(
        BarType::from(HOUR_BARS),
        vec![
            (OrderSide::Buy, Quantity::from("2.00")),
            (OrderSide::Sell, Quantity::from("3.00")),
        ],
    );

    let outcome = backtest.run(recorder).unwrap();

    let record = record.borrow();
    // The engine starts at 08:59:59.999999999: the first bar seen is the
    // minute opening 08:59, which closes at 09:00, so the hour bar closing
    // 09:00 holds it alone. Each hour bar holds the minutes opening in its
    // hour: 10:00 has 09:00 to 09:59, not the minute opening at 10:00.
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    assert_eq!(
        replay,
        [
            hour_bar(at("09:00"), at("08:59")),
            hour_bar(at("10:00"), at("09:00")),
            hour_bar(at("11:00"), at("10:00")),
        ]
    );
    assert_eq!(replay[1].2, Price::from("100013.9"));
    assert_eq!(replay[2].2, Price::from("100064.0"));
    // Each order fills on the hour's close, at the close of its last
    // minute, which no later minute has moved.
    let fills: Vec<_> = record
        .fills
        .iter()
        .map(|&(side, price, stamp)| (side, price, Timestamp::from(stamp)))
        .collect();
    assert_eq!(
        fills,
        [
            (OrderSide::Buy, Price::from("100006.4"), at("09:00")),
            (OrderSide::Sell, Price::from("100012.4"), at("10:00")),
        ]
    );

    // A contract is 0.01 BTC: the buy is worth 2,000.128 USDT, the sell
    // 3,000.372, charged 0.05%: 1.000064 and 1.500186 USDT. The 2 contracts
    // held long gain 6 USDT a BTC: 0.12 USDT. Without leverage the short
    // contract left open locks its value at entry, 1,000.124 USDT.
    let result = &outcome.result;
    let summary = |key: &str| result.summary[&format!("account.OKX.{key}")].clone();
    assert_eq!(summary("type"), "MARGIN");
    assert_eq!(summary("balance.USDT.total"), "9997.61975000 USDT");
    assert_eq!(summary("balance.USDT.locked"), "1000.12400000 USDT");

    // The run replays no quote: the spread is not measured.
    let costs = "
Costs (USDT)
  Commission: 2.500250
  Cost per trade: 1.250125
  Gross PnL: 0.120000
  Spread: n/a (no quotes)
";
    let report = report(&outcome);
    assert!(report.contains(costs), "{report}");
}
