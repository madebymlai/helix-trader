//! A run starting exactly on a bar's close. The warmup then starts 1 ns
//! before an hour's close too: its oldest bar holds one quote and is
//! dropped. The engine starts 1 ns before the run, so the quote closing the
//! first bar of the run is replayed: that bar comes once, in the run, and
//! the order sent on it fills on that quote. One node per thread, so one
//! run per test.

use crate::common::{self, trades};

#[test]
fn a_run_starting_on_a_bar_close_trades_on_that_bar_and_its_closing_quote() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::at("13:00"), &levels, "0 EUR")
        .run(trades::ema_cross(trades::time_exit()))
        .unwrap()
        .result;

    // The same orders and fills as a run from 12:30 (backtest_warmup.rs).
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), trades::TIME_EXIT_BALANCE);
}
