//! Tests that the EMA crossover opens one trade per cross, and none on a bar that keeps the side
//! (`common::trades`). Each trade but the last ends at its 50 bps stop loss on the next bar's first
//! quote, which jumps past it. A triggered stop is a market order, so it fills there.

use rstest::rstest;

use crate::common::{self, trades};

/// The run, bar by bar.
///
/// - 13:00 at 620.60 > 605.60: cross up, buy 10 at 620.65, stop loss 617.50 (620.60 less 0.5%, to
///   the cent); slow EMA 615.60.
/// - 14:00 bar's first quote, 13:01, bids 600.00 + 0.01 − 0.05 = 599.96 < 617.50: the stop loss
///   sells 10 there, −206.90. Close 600.60 < 615.60: cross down, sell 10 at 600.55, stop loss
///   603.60 (600.60 plus 0.5%); slow EMA 605.60.
/// - 15:00 bar's first quote, 14:01, asks 610.00 + 0.01 + 0.05 = 610.06 > 603.60: the stop loss
///   buys 10 there, −95.10. Close 610.60 > 605.60: cross up, buy 10 at 610.65, stop loss 607.55,
///   take profit 616.71; slow EMA 608.93.
/// - 16:00 at 611.60 > 608.93: no cross. Bids 610.96 to 611.55 touch neither exit.
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
