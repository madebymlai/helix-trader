//! IC Markets' gold on its tick quotes (`common::ic`). A recorder on the
//! hour MID bars Nautilus builds from them buys the maximum on its first
//! bar and closes that position by its id on the second, as on IC's
//! hedging venue a sell would open a position of its own: each order fills
//! whole at the ask, or the bid, of the last quote before the bar's close,
//! IC's commission of 30 USD per million is charged on each fill's value,
//! and the report shows the spread paid. One node per thread, so one run
//! per test.

use crate::common::{
    ic::{self, at},
    recorder::Recorder,
};
use helix_trader::backtest::report;
use jiff::Timestamp;
use nautilus_model::{
    data::BarType,
    enums::{OmsType, OrderSide},
    identifiers::Venue,
    types::{Price, Quantity},
};

#[test]
fn ic_gold_quotes_fill_orders_at_the_ask_and_the_bid_and_report_the_spread() {
    let catalog = ic::catalog();
    let backtest = ic::backtest(&catalog);
    assert_eq!(
        (backtest.venue_oms)(Venue::from("ICMARKETS")),
        OmsType::Hedging
    );
    let (recorder, record) = Recorder::trading(
        BarType::from(ic::HOUR_BARS),
        vec![(OrderSide::Buy, Quantity::from(10_000))],
    );

    let outcome = backtest.run(recorder.closing_on(vec![1])).unwrap();

    let record = record.borrow();
    // The run starts at 09:00, so the first hour bar closes at 10:00. Each order fills whole, in one fill, at its bar's close: the
    // buy at the ask of the last quote before 10:00, the close at the bid of
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
    let [position] = record.positions.as_slice() else {
        panic!("{:#?}", record.positions)
    };
    assert!(position.is_closed(), "{position:?}");

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
