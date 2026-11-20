//! Tests that a trade of the EMA crossover is closed at the market by a time alert 4 hours after
//! its send, when neither its stop loss nor its take profit ends it (`common::trades`,
//! `TIME_EXIT`). The crossover warms up on the bars closing 11:00 at 610.60 and 12:00 at 600.60,
//! then crosses up and buys at 13:00.

use rstest::rstest;

use crate::common::{self, trades};

#[rstest]
fn a_trade_is_closed_its_hold_after_its_send() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);

    let result = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    // Buys 10 at 620.65 and sells at the bid 624.85. Closing at 16:00 would sell at 624.75
    // (10041.00 EUR), and at 18:00 at 624.95 (10043.00), so the balance tells the hour.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), "10042.00 EUR");
}
