//! The warmup of the EMA crossover on internal bars, run starting off an
//! hour: the bars built from the warmup quotes, but the partial oldest one,
//! warm the EMAs before the run; the bar spanning the warmup end and the
//! run start comes once, complete, in the run, and its cross trades; the
//! orders fill on the quote 1 ns before the bar's close, buys at the ask,
//! sells at the bid (`common::trades`, `TIME_EXIT`). One node per thread,
//! so one run per test.

use crate::common::{self, trades};

#[test]
fn the_crossover_warms_up_on_the_full_warmup_bars_and_trades_from_the_first_bar_of_the_run() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(trades::ema_cross(trades::time_exit()))
        .unwrap()
        .result;

    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), trades::TIME_EXIT_BALANCE);
}
