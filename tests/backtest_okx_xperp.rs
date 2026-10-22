//! An OKX X-Perp on tick quotes: BTC-USD_UM_XPERP-310404.OKX, a
//! `CryptoFuture` of 0.0001 BTC a contract settled in USD, expiring in
//! 2031, with the margins `fill-catalog` gives it (10% initial, 0.65%
//! maintenance), and several quotes a minute at their exchange times, one
//! exactly on a minute. A recorder on 1-MINUTE-MID internal bars buys on
//! the first replayed bar and flips short on the second: the bars, the
//! fills at the ask and the bid of the latest quote, the commission rate
//! on each fill's value and the margin of the open short, all in USD, on
//! the same simulated venue as a swap. One node per process, so one run
//! per file.

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
    instruments::{CryptoFuture, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

const INSTRUMENT_ID: &str = "BTC-USD_UM_XPERP-310404.OKX";
const BAR_TYPE: &str = "BTC-USD_UM_XPERP-310404.OKX-1-MINUTE-MID-INTERNAL";
const WARMUP: SignedDuration = SignedDuration::from_mins(2);

/// `hh:mm:ss.fff` UTC on Wednesday 7 October 2026.
fn at(time: &str) -> Timestamp {
    ts(&format!("2026-10-07T{time}Z"))
}

/// BTC-USD_UM_XPERP-310404 as Nautilus' OKX adapter parses OKX's
/// definition: underlying `BTC-USD`, quoted and settled in USD, price tick
/// 0.1, lot 1 contract, 0.0001 BTC a contract, listed on 2026-03-30 and
/// expiring on 2031-04-04, with an initial margin of 10% (10x) and the
/// first tier's maintenance margin, 0.65%.
fn instrument() -> InstrumentAny {
    InstrumentAny::CryptoFuture(
        CryptoFuture::builder()
            .instrument_id(InstrumentId::from(INSTRUMENT_ID))
            .raw_symbol(Symbol::from("BTC-USD_UM_XPERP-310404"))
            .underlying(Currency::get_or_create_crypto("BTC-USD"))
            .quote_currency(Currency::USD())
            .settlement_currency(Currency::USD())
            .is_inverse(false)
            .activation_ns(UnixNanos::from(ts("2026-03-30T04:30:00.428Z")))
            .expiration_ns(UnixNanos::from(ts("2031-04-04T08:00:00Z")))
            .price_precision(1)
            .size_precision(0)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from("1"))
            .multiplier(Quantity::from("0.0001"))
            .lot_size(Quantity::from("1"))
            .min_quantity(Quantity::from("1"))
            .margin_init(Decimal::new(1, 1))
            .margin_maint(Decimal::new(65, 4))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The quotes: time, bid, ask. Sizes do not matter (50,000 contracts).
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
    let size = Quantity::from("50000");
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
fn an_okx_x_perp_trades_on_its_tick_quotes_with_margin_and_commission_in_usd() {
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
        balance: Money::from("10000 USD"),
        logging: LoggerConfig::default(),
    };
    let (recorder, record) = Recorder::trading(
        BarType::from(BAR_TYPE),
        WARMUP,
        vec![
            (OrderSide::Buy, Quantity::from("200")),
            (OrderSide::Sell, Quantity::from("300")),
        ],
    );

    let outcome = backtest.run(recorder).unwrap();

    let record: &Record = &record.borrow();
    // As for a swap: each minute bar holds the mids of the quotes stamped
    // after its open up to its close included, and the warmup from
    // 09:57:59.999999999 drops the partial 09:58 bar.
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

    // Amounts in USD have two decimals (Nautilus' USD), each rounded half
    // to even. A contract is 0.0001 BTC: the buy of 200 is worth 2,000.02
    // USD (2,000.024), the sell of 300 3,000.15, charged 0.05%: 1.00 and
    // 1.50 USD. The 200 contracts held long gain 3.8 USD a BTC: 0.08 USD
    // (0.076). The 100 contracts left short lock their maintenance margin,
    // 0.65% of their 1,000.05 USD at entry: 6.50 USD.
    let result = &outcome.result;
    let summary = |key: &str| result.summary[&format!("account.OKX.{key}")].clone();
    assert_eq!(summary("type"), "MARGIN");
    assert_eq!(summary("balance.USD.total"), "9997.58 USD");
    assert_eq!(summary("balance.USD.locked"), "6.50 USD");

    // The half spread paid: 0.1 on the buy's 200 contracts, 0.2 on the
    // sell's 300, times 0.0001 BTC: 0.008 USD, unrounded (ours). Realized
    // PnL 0.08 - 2.50 commission = -2.42; before costs 0.088.
    let costs = "
Costs (USD)
  Commission: 2.500000
  Cost per trade: 1.254000
  Gross PnL: 0.088000
  Spread: 0.008000
";
    let report = report(&outcome);
    assert!(report.contains(costs), "{report}");
}
