//! The EMA crossover with a trade size finer than the instrument's whole
//! shares: 10.5 rounds to the nearest even share count, so it trades as 10
//! shares do (`common::trades`, `TIME_EXIT`). One node per thread, so one
//! run per test.

use crate::common::{self, Exits, trades};
use helix_trader::strategy::ema::EmaCross;
use nautilus_model::types::Quantity;

#[test]
fn a_trade_size_off_the_share_grid_trades_the_rounded_size_and_the_run_completes() {
    let exits: Exits = trades::time_exit();
    let config = helix_trader::strategy::ema::EmaCrossConfig {
        trade_size: Quantity::from("10.5"),
        ..common::ema_config(1, 2, Some(common::session()), exits)
    };
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(EmaCross::new(config).unwrap())
        .unwrap()
        .result;

    // The orders and the balance of 10 shares (backtest_warmup.rs).
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), trades::TIME_EXIT_BALANCE);
}
