//! The EMA crossover over the fixture catalog without commission: the
//! reference the commission of `backtest_ema_cross.rs` is measured against.
//! One node per process, so one run per file.

mod common;

use common::{EMA_ORDERS, EMA_PNL_WITHOUT_COMMISSION};

#[test]
fn without_commission_the_ema_crossover_pays_only_the_spread() {
    let catalog = common::catalog();
    let result = common::backtest(catalog.path(), "0 EUR")
        .run(common::ema_cross())
        .unwrap();

    assert_eq!(result.total_orders, EMA_ORDERS);
    assert_eq!(
        result.stats_pnls["EUR"]["PnL (total)"],
        EMA_PNL_WITHOUT_COMMISSION
    );
}
