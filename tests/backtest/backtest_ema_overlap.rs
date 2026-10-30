//! Two overlapping trades of the EMA crossover on a hedging venue, as
//! IC's: the second cross opens a short beside the long still held, each
//! its own position closed by its own time alert, 4 hours after its entry
//! (`common::trades`). The moves are small, so that neither trade reaches
//! its stop loss (50 bps) or take profit (100 bps). One node per thread,
//! so one run per test.

use crate::common::{self, trades};
use helix_trader::backtest::Backtest;
use nautilus_model::enums::OmsType;

/// - 13:00 at 610.60, above 605.60: cross up, buy 10 at 610.65, its stop
///   loss at 607.55 and take profit at 616.71 (610.60 less 0.5% and plus
///   1%, to the cent); the slow EMA going to 608.93.
/// - 14:00 at 608.60, below 608.93: cross down, sell 10 at 608.55, a short
///   beside the long, its stop loss at 611.64 and take profit at 602.51
///   (608.60 plus 0.5% and less 1%); the slow EMA going to 608.71.
/// - 15:00 and 16:00 at 608.60, below the slow EMA (608.71, then 608.64):
///   short, no cross; 17:00 to 19:00 are out of the session.
/// - The bids from 13:01 to 17:00, 607.96 to 609.55, and the asks from
///   14:01 to 18:00, 607.06 to 609.65, touch no exit.
/// - 17:00, 4 hours after the buy: the long is closed, a sell of 10 at the
///   bid of the quote closing 17:00, 609.00 + 0.60 − 0.05 = 609.55: −11.00.
/// - 18:00, 4 hours after the sell: the short is closed, a buy of 10 at
///   the ask of the quote closing 18:00, 607.00 + 0.60 + 0.05 = 607.65:
///   +9.00.
///
/// On a netting venue the sell at 14:00 would have closed the long there.
const LEVELS: [f64; 7] = [610.0, 608.0, 608.0, 608.0, 609.0, 607.0, 607.0];

#[test]
fn on_a_hedging_venue_overlapping_trades_are_two_positions_each_closed_by_its_hold() {
    let catalog = trades::catalog(&LEVELS);
    let backtest = Backtest {
        venue_oms: |_| OmsType::Hedging,
        ..trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
    };

    let result = backtest.run(trades::ema_cross()).unwrap().result;

    // Two brackets of 3 orders and two closes.
    assert_eq!(result.total_orders, 8);
    assert_eq!(result.total_positions, 2);
    let pnl = &result.stats_pnls["EUR"];
    assert_eq!(pnl["Max Loser"], -11.0);
    assert_eq!(pnl["Max Winner"], 9.0);
    assert_eq!(common::balance(&result), "9998.00 EUR");
}
