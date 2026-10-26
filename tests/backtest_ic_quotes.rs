//! IC Markets' gold on its tick quotes: XAUUSD.ICMARKETS, a `CurrencyPair`
//! of ounces quoted in USD as `fill-catalog` loads it from IC (margins of
//! 1, at most 10,000 oz an order), and quotes as IC's fill stores them:
//! stamped at their tick's millisecond, at irregular times, both sides
//! sized 10,000 (the instrument's maximum order), written through
//! `fill_catalog`. A recorder on the hour MID bars Nautilus builds from
//! them buys the maximum on its first bar and sells it on the second: each
//! order fills whole at the ask, or the bid, of the last quote before the
//! bar's close, IC's commission of 30 USD per million is charged on each
//! fill's value, and the report shows the spread paid. One node per
//! process, so one run per file.

mod common;

use common::{recorder::Recorder, ts};
use helix_trader::{
    backtest::{Backtest, commission_rate, report},
    data::fill_catalog,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, Data, NautilusDataType, QuoteTick},
    enums::OrderSide,
    identifiers::{InstrumentId, Symbol},
    instruments::{CurrencyPair, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

const INSTRUMENT_ID: &str = "XAUUSD.ICMARKETS";
const HOUR_BARS: &str = "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL";

/// `hh:mm:ss.fff` UTC on Wednesday 21 October 2026.
fn at(time: &str) -> Timestamp {
    ts(&format!("2026-10-21T{time}Z"))
}

/// XAUUSD as IC defines it (`src/data/ic/README.md`, "Currency pairs").
fn instrument() -> InstrumentAny {
    InstrumentAny::CurrencyPair(
        CurrencyPair::builder()
            .instrument_id(InstrumentId::from(INSTRUMENT_ID))
            .raw_symbol(Symbol::from("XAUUSD"))
            .base_currency(Currency::from("XAU"))
            .quote_currency(Currency::USD())
            .price_precision(2)
            .size_precision(0)
            .price_increment(Price::from("0.01"))
            .size_increment(Quantity::from(1))
            .lot_size(Quantity::from(100))
            .min_quantity(Quantity::from(1))
            .max_quantity(Quantity::from(10_000))
            .margin_init(Decimal::ONE)
            .margin_maint(Decimal::ONE)
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// An IC quote at `time`, sized as IC's fill sizes XAUUSD's.
fn quote(time: Timestamp, bid: &str, ask: &str) -> QuoteTick {
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

/// Quotes from 08:00 to 12:00, two a minute at :07.250 and :37.750, the
/// bid stepping through 4000.00 to 4000.90 by the minute, the ask 0.20
/// above; the last of the hours closing 10:00 and 11:00 set apart.
fn quotes() -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = at("08:00:07.250");
    while time < at("12:00:00") {
        let bid = Decimal::new(400_000 + 10 * (time.as_second() / 60 % 10), 2);
        let ask = bid + Decimal::new(20, 2);
        quotes.push(match time {
            t if t == at("09:59:37.750") => quote(t, "4005.90", "4006.10"),
            t if t == at("10:59:37.750") => quote(t, "4011.90", "4012.15"),
            t => quote(t, &bid.to_string(), &ask.to_string()),
        });
        let step = if time.subsec_millisecond() == 250 {
            30_500
        } else {
            29_500
        };
        time += SignedDuration::from_millis(step);
    }
    quotes
}

#[test]
fn ic_gold_quotes_fill_orders_at_the_ask_and_the_bid_and_report_the_spread() {
    let (start, end) = (at("09:00:00"), at("12:00:00"));
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let series = [(NautilusDataType::QuoteTick, INSTRUMENT_ID.to_owned())];
    let fill = fill_catalog(
        &mut catalog,
        vec![instrument()],
        &series,
        at("08:00:00"),
        end,
        async |_, _, _, _| Ok(quotes().into_iter().map(Data::Quote).collect()),
    );
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();
    let backtest = Backtest {
        catalog: dir.path().to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start,
        end,
        warmup: SignedDuration::ZERO,
        // IC's Raw commission: 30 USD per million USD traded.
        fee_model: commission_rate(Decimal::new(3, 5)).unwrap(),
        balance: Money::from("2000000 USD"),
        // IC's leverage on the demo account, 1:30.
        leverage: Decimal::from(30),
        logging: LoggerConfig::default(),
    };
    let (recorder, record) = Recorder::trading_without_warmup(
        BarType::from(HOUR_BARS),
        vec![
            (OrderSide::Buy, Quantity::from(10_000)),
            (OrderSide::Sell, Quantity::from(10_000)),
        ],
    );

    let outcome = backtest.run(recorder).unwrap();

    let record = record.borrow();
    // The engine starts 1 ns before 09:00, so the first hour bar closes at
    // 10:00. Each order fills whole, in one fill, at its bar's close: the
    // buy at the ask of the last quote before 10:00, the sell at the bid of
    // the last before 11:00.
    let fills: Vec<_> = record
        .fills
        .iter()
        .map(|&(side, price, stamp)| (side, price, Timestamp::from(stamp)))
        .collect();
    assert_eq!(
        fills,
        [
            (OrderSide::Buy, Price::from("4006.10"), at("10:00:00")),
            (OrderSide::Sell, Price::from("4011.90"), at("11:00:00")),
        ]
    );

    // 10,000 oz bought at 4006.10 (40,061,000 USD) and sold at 4011.90
    // (40,119,000 USD), charged 30 per million: 1,201.83 and 1,203.57.
    // Each paid half its quote's spread on 10,000 oz: 0.10 (spread 0.20)
    // and 0.125 (spread 0.25), 1,000 and 1,250 USD. The gross PnL is the
    // mids' move, 4006.00 to 4012.025, on 10,000 oz: 60,250 USD.
    let costs = "
Costs (USD)
  Commission: 2405.400000
  Cost per trade: 2327.700000
  Gross PnL: 60250.000000
  Spread: 2250.000000
";
    let report = report(&outcome);
    assert!(report.contains(costs), "{report}");
    assert!(report.contains("PnL (total): 55594.600000"), "{report}");
}
