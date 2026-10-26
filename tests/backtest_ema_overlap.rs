//! Two overlapping trades of the EMA crossover on a hedging venue, as
//! IC's: the second cross opens a short beside the long still held, each
//! its own position closed by its own time alert (`common::trades`). Each
//! trade is held 90 minutes, its stop loss and take profit 10% away. One
//! node per process, so one run per file.

mod common;

use common::{Exits, trades};
use helix_trader::backtest::Backtest;
use jiff::SignedDuration;
use nautilus_model::enums::OmsType;

/// - 13:00 at 620.60, above 605.60: cross up, buy 10 at 620.65; the slow
///   EMA going to 615.60.
/// - 14:00 at 600.60, below 615.60: cross down, sell 10 at 600.55, a short
///   beside the long; the slow EMA going to 605.60.
/// - 14:30, 90 minutes after the buy: the long is closed, a sell of 10 at
///   the bid of the 30th minute of the 15:00 bar, 604.00 + 0.30 − 0.05 =
///   604.25: −164.00.
/// - 15:00 at 604.60, below 605.60: short, no cross; the slow EMA going to
///   604.93. 16:00 at 602.60, below it: no cross.
/// - 15:30: the short is closed, a buy of 10 at the ask of the 30th minute
///   of the 16:00 bar, 602.00 + 0.30 + 0.05 = 602.35: −18.00.
///
/// On a netting venue the sell at 14:00 would have closed the long there.
const LEVELS: [f64; 4] = [620.0, 600.0, 604.0, 602.0];

#[test]
fn on_a_hedging_venue_overlapping_trades_are_two_positions_each_closed_by_its_hold() {
    let catalog = trades::catalog(&LEVELS);
    let backtest = Backtest {
        venue_oms: |_| OmsType::Hedging,
        ..trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
    };
    let exits = Exits::after(SignedDuration::from_mins(90));

    let result = backtest.run(trades::ema_cross(exits)).unwrap().result;

    assert_eq!(result.total_orders, 8);
    assert_eq!(result.total_positions, 2);
    let pnl = &result.stats_pnls["EUR"];
    assert_eq!(pnl["Max Loser"], -164.0);
    assert_eq!(pnl["Min Loser"], -18.0);
    assert_eq!(common::balance(&result), "9818.00 EUR");
}
