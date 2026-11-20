//! Tests that the EMA crossover opens one trade per cross, and none on a bar that keeps the side
//! (`common::trades`). Each trade but the last ends at its 50 bps stop loss on the next bar's first
//! quote, which jumps past it. A triggered stop is a market order, so it fills there.
//!
//! The run: 13:00 crosses up, buys 10 at 620.65 and sets the stop loss at 617.50. The 14:00 bar's
//! first quote bids 599.96, so the stop loss sells there, -206.90, and the close crosses down: sell
//! 10 at 600.55, stop loss 603.60. The 15:00 bar's first quote asks 610.06, so the stop loss buys
//! there, -95.10, and the close crosses up: buy 10 at 610.65, stop loss 607.55, take profit 616.71.
//! 16:00 crosses nothing, and its bids touch neither exit.

use rstest::rstest;

use crate::common::{self, trades};

const LEVELS: [f64; 4] = [620.0, 600.0, 610.0, 611.0];

#[rstest]
fn each_cross_opens_one_trade_in_its_direction() {
    let catalog = trades::catalog(&LEVELS);

    let result = trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    // Three brackets of 3 orders: entry and two legs
    assert_eq!(result.total_orders, 9);
    let pnl = &result.stats_pnls["EUR"];
    assert_eq!(pnl["Max Loser"], -206.9);
    assert_eq!(pnl["Min Loser"], -95.1);
    assert_eq!(common::balance(&result), "9698.00 EUR");
}
