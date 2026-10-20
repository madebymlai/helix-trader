//! A run starting exactly on a bar's close. The warmup then starts 1 ns
//! before an hour's close too: its oldest bar holds one quote and is
//! dropped. The engine starts 1 ns before the run, so the quote closing the
//! first bar of the run is replayed: that bar comes once, in the run, and
//! the order sent on it fills on that quote. One node per process, so one
//! run per file.

mod common;

use common::steps;

#[test]
fn a_run_starting_on_a_bar_close_trades_on_that_bar_and_its_closing_quote() {
    let catalog = steps::catalog();
    let result = steps::backtest(catalog.path(), steps::at("13:00"))
        .run(common::shortest_ema_cross(steps::WARMUP))
        .unwrap();

    // The same orders and fills as a run from 12:30 (backtest_warmup.rs).
    assert_eq!(result.total_orders, steps::ORDERS);
    assert_eq!(common::balance(&result), steps::BALANCE);
}
