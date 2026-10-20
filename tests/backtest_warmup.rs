//! The warmup of the EMA crossover on internal bars, run starting off an
//! hour: the bars built from the warmup quotes, but the partial oldest one,
//! warm the EMAs before the run; the bar spanning the warmup end and the
//! run start comes once, complete, in the run; the orders fill on the quote
//! 1 ns before the bar's close, buys at the ask, sells at the bid. One node
//! per process, so one run per file.

mod common;

use common::{quote_before, steps};
use nautilus_model::types::Price;

#[test]
fn the_crossover_warms_up_on_the_full_warmup_bars_and_trades_from_the_first_bar_of_the_run() {
    let catalog = steps::catalog();
    let result = steps::backtest(catalog.path(), steps::at("12:30"))
        .run(common::shortest_ema_cross(steps::WARMUP))
        .unwrap();

    // The prices the orders fill at (see `steps`).
    let quotes = steps::quotes();
    assert_eq!(
        quote_before(&quotes, steps::at("13:00")).ask_price,
        Price::from("620.65")
    );
    assert_eq!(
        quote_before(&quotes, steps::at("14:00")).bid_price,
        Price::from("580.55")
    );
    assert_eq!(result.total_orders, steps::ORDERS);
    assert_eq!(common::balance(&result), steps::BALANCE);
}
