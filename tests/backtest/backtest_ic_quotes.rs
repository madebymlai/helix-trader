//! IC Markets' gold on its tick quotes (`common::ic`). A recorder on the hour MID bars buys the
//! maximum on the first bar and closes that position by id on the second. Each order fills whole at
//! the ask, or the bid, of the last quote before the bar's close; IC's commission is 30 USD per
//! million of each fill's value, and the report shows the spread paid.

use helix_trader::backtest::report;
use jiff::Timestamp;
use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    types::{Price, Quantity},
};
use rstest::rstest;

use crate::common::{
    ic::{self, at},
    recorder::Recorder,
};

#[rstest]
fn ic_gold_quotes_fill_orders_at_the_ask_and_the_bid_and_report_the_spread() {
    let catalog = ic::catalog();
    let backtest = ic::backtest(&catalog);
    let (recorder, record) = Recorder::trading(
        BarType::from(ic::HOUR_BARS),
        vec![(OrderSide::Buy, Quantity::from(10_000))],
    );

    let outcome = backtest.run(recorder.closing_on(vec![1])).unwrap();

    let record = record.borrow();

    // The buy fills at the last ask before 10:00, the close at the last bid before 11:00
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

    // 10,000 x 4006.10 = 40,061,000 USD and 10,000 x 4011.90 = 40,119,000, at 30 per million:
    // 1,201.83 + 1,203.57. Spread paid, half each quote's: 10,000 x 0.10 + 10,000 x 0.125 = 1,000 +
    // 1,250. Gross PnL, the mids' move: 10,000 x (4012.025 - 4006.00) = 60,250.
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
