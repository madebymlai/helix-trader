//! The warmup of the EMA crossover on internal bars, run starting off an
//! hour: the bars built from the warmup quotes, but the partial oldest one,
//! warm the EMAs before the run; the bar spanning the warmup end and the
//! run start comes once, complete, in the run; the orders fill on the quote
//! 1 ns before the bar's close, buys at the ask, sells at the bid. One node
//! per process, so one run per file.

mod common;

use common::steps;

#[test]
fn the_crossover_warms_up_on_the_full_warmup_bars_and_trades_from_the_first_bar_of_the_run() {
    let catalog = steps::catalog();
    let result = steps::backtest(catalog.path(), steps::at("12:30"))
        .run(common::shortest_ema_cross(steps::WARMUP))
        .unwrap();

    assert_eq!(result.total_orders, steps::ORDERS);
    assert_eq!(common::balance(&result), steps::BALANCE);
}
