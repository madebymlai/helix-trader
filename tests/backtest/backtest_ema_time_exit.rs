//! Tests that a trade of the EMA crossover is closed at the market by a time alert 4 hours after
//! its send, when neither its stop loss nor its take profit ends it (`common::trades`,
//! `TIME_EXIT`).

use rstest::rstest;

use crate::common::{self, trades};

/// Starts from `trades::start()`, 10:00.
///
/// The bars closing 11:00 at 610.60 and 12:00 at 600.60 warm the crossover up alone. It is
/// initialized at 12:00, short, with no cross. At 13:00 it crosses up and buys.
#[rstest]
fn a_trade_is_closed_its_hold_after_its_send() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);

    let result = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    // Buys 10 at 620.65 at 13:00, the 17:00 close sells 10 at the bid 624.30 + 0.60 − 0.05 =
    // 624.85, which is 4.20 a share on 10, and hour bars from 16:00 each close higher, so the price
    // tells the hour: closed at 16:00 it would sell at 624.75 (10041.00 EUR), closed at 18:00 at
    // 624.95 (10043.00), and never closed the balance stays 10000.00.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), "10042.00 EUR");
}
