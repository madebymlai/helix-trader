//! The EMA crossover whose budget buys a share count off the instrument's
//! whole shares: 0.33% of 10,000 EUR, 33 EUR, over the stop of its trade
//! of `common::trades::TIME_EXIT`, 3.10 EUR a share from the close, buys
//! 10.6 shares. Nautilus' sizer rounds down, never adding risk: it trades
//! 10 shares, as `TIME_EXIT` does, not the 11 the nearest count would
//! risk 34.10 EUR on. One node per thread, so one run per test.

use crate::common::{self, trades};
use helix_trader::strategy::ema::EmaCross;

#[test]
fn a_budget_off_the_share_grid_trades_the_shares_it_rounds_down_to() {
    let mut config = common::ema_config(1, 2, Some(common::session()));
    config.trade.risk = Some("0.33%".parse().unwrap());
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
