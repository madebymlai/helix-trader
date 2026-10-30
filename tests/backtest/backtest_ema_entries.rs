//! The EMA crossover opens one trade per cross, in the cross direction,
//! and none on a bar keeping the side (`common::trades`). Each trade but
//! the last ends at its stop loss, 50 bps away, on the first quote of the
//! next bar, which jumps past it: the stop is a market order once
//! triggered, so it fills at that quote. One node per thread, so one run
//! per test.

use crate::common::{self, trades};

/// - 13:00 at 620.60, above 605.60: cross up, buy 10 at 620.65, its stop
///   loss at 617.50 (620.60 less 0.5%, to the cent); the slow EMA going to
///   615.60.
/// - The 14:00 bar's first quote, at 13:01, bids 600.00 + 0.01 − 0.05 =
///   599.96, below 617.50: the stop loss sells 10 there, −206.90. The bar
///   closes at 600.60, below 615.60: cross down, sell 10 at 600.55, its stop
///   loss at 603.60 (600.60 plus 0.5%); the slow EMA going to 605.60.
/// - The 15:00 bar's first quote, at 14:01, asks 610.00 + 0.01 + 0.05 =
///   610.06, above 603.60: the stop loss buys 10 there, −95.10. The bar
///   closes at 610.60, above 605.60: cross up, buy 10 at 610.65, its stop
///   loss at 607.55 and take profit at 616.71; the slow EMA going to
///   608.93.
/// - 16:00 at 611.60, above 608.93: long, no cross, no trade. Its bids,
///   610.96 to 611.55, touch neither exit, and the run ends with the long
///   open.
const LEVELS: [f64; 4] = [620.0, 600.0, 610.0, 611.0];

#[test]
fn each_cross_opens_one_trade_in_its_direction() {
    let catalog = trades::catalog(&LEVELS);

    let result = trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    // Three brackets of 3 orders each: entry and two legs, no close.
    assert_eq!(result.total_orders, 9);
    let pnl = &result.stats_pnls["EUR"];
    assert_eq!(pnl["Max Loser"], -206.9);
    assert_eq!(pnl["Min Loser"], -95.1);
    assert_eq!(common::balance(&result), "9698.00 EUR");
}
