//! Tests the EMA's brackets (`common::trades`). The stop loss is 50 bps and the take profit 100 bps
//! from the deciding bar's close. The 4-hour hold is longer than the run. A long ends at its take
//! profit and a short at its stop loss, each on the first minute quote that reaches it.
//!
//! - 13:00 at 620.60: cross up, buy 10 at 620.65, take profit 626.81, stop loss 617.50.
//! - The 14:00 bar's bids reach 626.81 at 13:36: the take profit sells 10.
//! - 15:00 at 600.60: cross down, sell 10 at 600.55, stop loss 603.60, take profit 594.59.
//! - The 16:00 bar's asks reach 603.60 at 15:35: the stop loss buys 10.

use rstest::rstest;

use crate::common::{self, trades};

const LEVELS: [f64; 4] = [620.0, 626.5, 600.0, 603.2];

#[rstest]
fn a_long_ends_at_its_take_profit_and_a_short_at_its_stop_loss() {
    let catalog = trades::catalog(&LEVELS);

    let result = trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    // Two brackets of 3 orders, no close
    assert_eq!(result.total_orders, 6);
    let pnl = &result.stats_pnls["EUR"];
    assert_eq!(pnl["Max Winner"], 61.6);
    assert_eq!(pnl["Max Loser"], -30.5);
    assert_eq!(common::balance(&result), "10031.10 EUR");
}
