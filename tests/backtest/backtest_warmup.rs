//! Tests that the EMA crossover warms up on the replay alone.
//!
//! A backtest makes no warmup request. The crossover decides once its indicators are initialized
//! (`common::trades`).
//!
//! From 12:30 the run replays the bars closing 13:00 to 16:00. The slow EMA is initialized at 14:00,
//! long, as 624.60 is above 622.60, and the later bars stay long.

use rstest::rstest;

use crate::common::{self, trades};

/// The first decision at 14:00 is long, not a cross, so there is no order.
#[rstest]
fn a_run_started_at_the_trade_is_cold_there_and_does_not_trade() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::at("12:30"), &levels, "0 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    assert_eq!(result.total_orders, 0);
    assert_eq!(common::balance(&result), "10000.00 EUR");
}
