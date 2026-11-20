//! A trade of the EMA crossover that neither its stop loss nor its take profit ends is closed by a
//! time alert at the market, 4 hours, its hold, after its send (its entry filling at once)
//! (`common::trades`, `TIME_EXIT`).

use rstest::rstest;

use crate::common::{self, trades};

/// From `trades::start()`, 10:00, the run replays the bars closing 11:00 at 610.60 and 12:00 at
/// 600.60: the crossover warms up on them, the replay alone, initialized at 12:00, short, its first
/// decision, no cross; at 13:00 it crosses up and buys.
#[rstest]
fn a_trade_is_closed_its_hold_after_its_send() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);

    let result = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    // The entry buys 10 at 620.65 at 13:00; 4 hours later, at 17:00, the close sells 10 at the bid
    // of the quote closing 17:00: 624.30 + 0.60 − 0.05 = 624.85, 4.20 a share on 10. The hour bars
    // from 16:00 each close higher, so the price tells the hour: closed at 16:00 it would have sold
    // at 624.75 (10041.00 EUR), at 18:00 at 624.95 (10043.00); never closed, the balance would
    // still be 10000.00.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), "10042.00 EUR");
}
