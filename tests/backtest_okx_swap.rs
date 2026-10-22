//! An OKX swap on tick quotes: BTC-USDT-SWAP.OKX, a `CryptoPerpetual` of
//! 0.01 BTC a contract settled in USDT, with the margins of 1 `fill-catalog`
//! gives it, and several quotes a minute at their exchange times, one
//! exactly on a minute. A recorder on 1-MINUTE-MID internal bars buys on
//! the first replayed bar and flips short on the second: the bars' open,
//! high, low and close, the fills at the ask and the bid of the latest
//! quote, the commission rate on each fill's value and, without leverage,
//! the open short locking its whole value on the simulated OKX venue. One
//! node per process, so one run per file.

mod common;

use common::{
    Ohlc, ohlc,
    recorder::{Record, Recorder},
    ts,
};
use helix_trader::backtest::{Backtest, commission_rate, report};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, QuoteTick},
    enums::OrderSide,
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

const INSTRUMENT_ID: &str = "BTC-USDT-SWAP.OKX";
const BAR_TYPE: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-MID-INTERNAL";
const WARMUP: SignedDuration = SignedDuration::from_mins(2);

/// `hh:mm:ss.fff` UTC on Wednesday 7 January 2026.
fn at(time: &str) -> Timestamp {
    ts(&format!("2026-01-07T{time}Z"))
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

/// The quotes: time, bid, ask. Sizes do not matter (500 contracts).
const QUOTES: &[(&str, &str, &str)] = &[
    // The warmup, flat.
    ("09:57:00.000", "99990.0", "99990.1"),
    ("09:57:20.000", "99990.0", "99990.1"),
    ("09:57:40.000", "99990.0", "99990.1"),
    ("09:58:00.000", "99990.0", "99990.1"),
    ("09:58:20.000", "99990.0", "99990.1"),
    ("09:58:40.000", "99990.0", "99990.1"),
    // The minute closing 10:00, its last quote exactly on the close.
    ("09:59:10.100", "100000.0", "100000.1"),
    ("09:59:25.500", "100003.0", "100003.2"),
    ("09:59:41.007", "99998.0", "99998.4"),
    ("10:00:00.000", "100001.0", "100001.2"),
    // The minute closing 10:01.
    ("10:00:00.400", "100002.0", "100002.1"),
    ("10:00:20.000", "100010.0", "100010.2"),
    ("10:00:45.999", "100000.0", "100000.6"),
    ("10:00:59.998", "100005.0", "100005.4"),
    // The minute closing 10:02.
    ("10:01:00.001", "100004.0", "100004.1"),
    ("10:01:30.000", "99995.0", "99995.1"),
    ("10:01:59.000", "99999.0", "99999.2"),
    // The minute closing 10:03.
    ("10:02:30.000", "100000.0", "100000.1"),
];

fn quotes() -> Vec<QuoteTick> {
    let size = Quantity::from("500.00");
    QUOTES
        .iter()
        .map(|&(time, bid, ask)| {
            let stamp = UnixNanos::from(at(time));
            QuoteTick::new(
                InstrumentId::from(INSTRUMENT_ID),
                Price::from(bid),
                Price::from(ask),
                size,
                size,
                stamp,
                stamp,
            )
        })
        .collect()
}

/// A bar worked out by hand: its close and its open, high, low and close
/// mids.
fn bar(close: &str, prices: [&str; 4]) -> Ohlc {
    let [open, high, low, last] = prices.map(Price::from);
    (at(close), open, high, low, last)
}

/// A bar's prices as decimals, so that a mid's extra digit does not count.
fn decimals(bar: &Ohlc) -> (Timestamp, [Decimal; 4]) {
    let (close, open, high, low, last) = bar;
    let prices = [open, high, low, last].map(|price| price.as_decimal().normalize());
    (*close, prices)
}

#[test]
fn an_okx_swap_trades_on_its_tick_quotes_with_margin_and_commission() {
    let (from, start, end) = (at("09:57:00.000"), at("10:00:00.000"), at("10:03:00.000"));
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    catalog
        .write_to_parquet(&quotes(), Some(from.into()), Some(end.into()), None)
        .unwrap();
    let backtest = Backtest {
        catalog: dir.path().to_path_buf(),
        bar_types: vec![BarType::from(BAR_TYPE)],
        start,
        end,
        warmup: WARMUP,
        // OKX's taker fee, 0.05%.
        fee_model: commission_rate(Decimal::new(5, 4)).unwrap(),
        balance: Money::from("10000 USDT"),
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
    };
    let (recorder, record) = Recorder::trading(
        BarType::from(BAR_TYPE),
        WARMUP,
        vec![
            (OrderSide::Buy, Quantity::from("2.00")),
            (OrderSide::Sell, Quantity::from("3.00")),
        ],
    );

    let outcome = backtest.run(recorder).unwrap();

    let record: &Record = &record.borrow();
    // Each minute bar holds the mids of the quotes stamped after its open
    // up to its close included: the quote at 10:00:00.000 closes the 10:00
    // bar. The warmup from 09:57:59.999999999 drops the partial 09:58 bar.
    let warmup: Vec<_> = record.warmup.iter().map(ohlc).collect();
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    let expected_warmup = [bar("09:59:00", ["99990.05"; 4])];
    let expected_replay = [
        bar("10:00:00", ["100000.05", "100003.1", "99998.2", "100001.1"]),
        bar(
            "10:01:00",
            ["100002.05", "100010.1", "100000.3", "100005.2"],
        ),
        bar(
            "10:02:00",
            ["100004.05", "100004.05", "99995.05", "99999.1"],
        ),
        bar("10:03:00", ["100000.05"; 4]),
    ];
    let decimals_of = |bars: &[Ohlc]| bars.iter().map(decimals).collect::<Vec<_>>();
    assert_eq!(decimals_of(&warmup), decimals_of(&expected_warmup));
    assert_eq!(decimals_of(&replay), decimals_of(&expected_replay));

    // The buy fills at the ask of the quote on the 10:00 close, the sell at
    // the bid of the last quote before 10:01, not at the one after it.
    let fills: Vec<_> = record
        .fills
        .iter()
        .map(|&(side, price, stamp)| (side, price.as_decimal(), Timestamp::from(stamp)))
        .collect();
    let decimal = |s: &str| s.parse::<Decimal>().unwrap();
    assert_eq!(
        fills,
        [
            (OrderSide::Buy, decimal("100001.2"), at("10:00:00.000")),
            (OrderSide::Sell, decimal("100005.0"), at("10:01:00.000")),
        ]
    );

    // A contract is 0.01 BTC: the buy is worth 2,000.024 USDT, the sell
    // 3,000.15, charged 0.05%: 1.000012 and 1.500075 USDT. The 2 contracts
    // held long gain 3.8 USDT a BTC: 0.076 USDT. Without leverage the short
    // contract left open locks its value at entry, 1,000.05 USDT.
    let result = &outcome.result;
    let summary = |key: &str| result.summary[&format!("account.OKX.{key}")].clone();
    assert_eq!(summary("type"), "MARGIN");
    assert_eq!(summary("balance.USDT.total"), "9997.57591300 USDT");
    assert_eq!(summary("balance.USDT.locked"), "1000.05000000 USDT");

    // The half spread paid: 0.1 on the buy's 2 contracts, 0.2 on the sell's
    // 3, times 0.01 BTC: 0.008 USDT.
    let costs = "
Costs (USDT)
  Commission: 2.500087
  Cost per trade: 1.254044
  Gross PnL: 0.084000
  Spread: 0.008000
";
    let report = report(&outcome);
    assert!(report.contains(costs), "{report}");
}
