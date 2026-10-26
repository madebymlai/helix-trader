//! The stop loss and the take profit of the EMA's brackets
//! (`common::trades`), each 100 bps from the close of the bar the trade
//! was decided on, held 4 hours, longer than the run: a long ends at its
//! take profit, a short at its stop loss, each on the first minute quote
//! reaching it. One node per process, so one run per file.

mod common;

use common::{Exits, trades};
use jiff::SignedDuration;

/// - 13:00 at 620.60, a cross up (above 605.60): buy 10 at 620.65, the
///   take profit at 626.81 (620.60 plus 1%, to the cent), the stop loss at
///   614.39; the slow EMA going to 615.60.
/// - The 14:00 bar's minute m bids 626.50 + 0.01 m − 0.05: 626.81 at the
///   36th, 13:36: the take profit sells 10 at 626.81 and its stop loss is
///   cancelled. The bar closes at 627.10, above 615.60: long, no cross; the
///   slow EMA going to 623.27.
/// - 15:00 at 600.60, a cross down: sell 10 at the bid, 600.55, the stop
///   loss at 606.61 (600.60 plus 1%), the take profit at 594.59; the slow
///   EMA going to 608.16.
/// - The 16:00 bar's minute m asks 606.20 + 0.01 m + 0.05: 606.61 at the
///   36th, 15:36: the stop loss buys 10 at 606.61 and the take profit is
///   cancelled. The bar closes at 606.80, below 608.16: short, no cross.
const LEVELS: [f64; 4] = [620.0, 626.5, 600.0, 606.2];

#[test]
fn a_long_ends_at_its_take_profit_and_a_short_at_its_stop_loss() {
    let catalog = trades::catalog(&LEVELS);
    let exits = Exits {
        stop_loss_bps: 100,
        take_profit_bps: 100,
        hold: SignedDuration::from_hours(4),
    };

    let result = trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
        .run(trades::ema_cross(exits))
        .unwrap()
        .result;

    // Two brackets of 3 orders, no close: the long gains 6.16 a share, the
    // short loses 6.06, on 10.
    assert_eq!(result.total_orders, 6);
    let pnl = &result.stats_pnls["EUR"];
    assert_eq!(pnl["Max Winner"], 61.6);
    assert_eq!(pnl["Max Loser"], -60.6);
    assert_eq!(common::balance(&result), "10001.00 EUR");
}
