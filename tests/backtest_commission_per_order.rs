//! A commission per order on the EMA's time exit (`common::trades`,
//! `TIME_EXIT`): charged on each order that fills, never on the legs
//! cancelled unfilled. One node per process, so one run per file.

mod common;

use common::trades;

#[test]
fn a_commission_per_order_is_charged_on_each_filled_order() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::start(), &levels, "3 EUR")
        .run(trades::ema_cross(trades::time_exit()))
        .unwrap()
        .result;

    // Of the 4 orders, the entry and the close fill: 6 EUR off the 39.00
    // the trade makes without commission (backtest_warmup.rs). PnL (total)
    // is net of commissions.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(result.stats_pnls["EUR"]["PnL (total)"], 33.0);
    assert_eq!(common::balance(&result), "10033.00 EUR");
}
