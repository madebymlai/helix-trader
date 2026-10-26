//! The EMA crossover opens one trade per cross, in the cross direction,
//! and none on a bar keeping the side (`common::trades`). Each trade is
//! held 30 minutes, its stop loss and take profit 10% away, out of reach.
//! One node per process, so one run per file.

mod common;

use common::{Exits, trades};
use jiff::SignedDuration;

/// - 13:00 at 620.60, above 605.60: cross up, buy 10 at 620.65; the slow
///   EMA going to 615.60. Closed at 13:30 at the bid of the 30th minute of
///   the 14:00 bar, 600.00 + 0.30 − 0.05 = 600.25: −204.00.
/// - 14:00 at 600.60, below 615.60: cross down, sell 10 at 600.55; the slow
///   EMA going to 605.60. Closed at 14:30 at the ask, 610.00 + 0.30 + 0.05
///   = 610.35: −98.00.
/// - 15:00 at 610.60, above 605.60: cross up, buy 10 at 610.65; the slow
///   EMA going to 608.93. Closed at 15:30 at 611.00 + 0.30 − 0.05 =
///   611.25: +6.00.
/// - 16:00 at 611.60, above 608.93: long, no cross, no trade.
const LEVELS: [f64; 4] = [620.0, 600.0, 610.0, 611.0];

#[test]
fn each_cross_opens_one_trade_in_its_direction() {
    let catalog = trades::catalog(&LEVELS);
    let exits = Exits::after(SignedDuration::from_mins(30));

    let result = trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
        .run(trades::ema_cross(exits))
        .unwrap()
        .result;

    // Three trades of 4 orders each: entry, two legs, close.
    assert_eq!(result.total_orders, 12);
    let pnl = &result.stats_pnls["EUR"];
    assert_eq!(pnl["Max Winner"], 6.0);
    assert_eq!(pnl["Max Loser"], -204.0);
    assert_eq!(common::balance(&result), "9704.00 EUR");
}
