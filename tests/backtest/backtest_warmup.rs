//! The EMA crossover warms up on the replay alone: a backtest makes no
//! warmup request, and the crossover decides once its indicators are
//! initialized, so a run trades from the bars it replays (`common::trades`).
//! One node per thread, so one run per test.

use crate::common::{self, trades};

/// From 10:00 the run replays the bars closing 11:00 at 610.60 and 12:00
/// at 600.60: the shortest crossover is initialized at 12:00, short, its
/// first decision, no cross; at 13:00 it trades the time exit as worked
/// out in `trades::TIME_EXIT`.
#[test]
fn a_run_started_before_the_trade_warms_the_crossover_on_its_own_bars() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::at("10:00"), &levels, "0 EUR")
        .run(trades::ema_cross(trades::time_exit()))
        .unwrap()
        .result;

    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), trades::TIME_EXIT_BALANCE);
}

/// From 12:30 the run replays the bar closing 13:00 from 12:31 on, at
/// 620.60, then 14:00 at 624.60 and 15:00 at 624.80: the slow EMA is
/// initialized at 14:00, long (624.60 above 622.60), its first decision;
/// 15:00 stays long (624.80 above 622.60). No cross, no order.
#[test]
fn a_run_started_at_the_trade_is_cold_there_and_does_not_trade() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::at("12:30"), &levels, "0 EUR")
        .run(trades::ema_cross(trades::time_exit()))
        .unwrap()
        .result;

    assert_eq!(result.total_orders, 0);
    assert_eq!(common::balance(&result), "10000.00 EUR");
}
