//! Tests that the EMA crossover warms up on the replay alone.
//!
//! A backtest makes no warmup request. The crossover decides once its indicators are initialized
//! (`common::trades`).

use rstest::rstest;

use crate::common::{self, trades};

/// From 12:30 the run replays the bar closing 13:00 from 12:31 on, at 620.60.
///
/// It then replays 14:00 at 624.60, and 15:00 and 16:00 at 624.80.
/// - The slow EMA is initialized at 14:00, long (624.60 above 622.60). This is its first decision.
/// - 15:00 stays long (624.80 above 622.60), with the EMA at 624.07.
/// - 16:00 stays long (624.80 above 624.07).
/// - 17:00 and 18:00 are out of the session.
///
/// No cross, no order.
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
