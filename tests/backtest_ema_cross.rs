//! The EMA crossover over the fixture catalog with a commission of 3 EUR
//! per order. One node per process, so one run per file.

mod common;

use common::{EMA_ORDERS, EMA_PNL_WITHOUT_COMMISSION};

#[test]
fn the_ema_crossover_trades_pays_the_commission_per_order_and_reproduces_its_figures() {
    let catalog = common::catalog();
    let result = common::backtest(catalog.path(), "3 EUR")
        .run(common::ema_cross())
        .unwrap();

    assert_eq!(result.total_orders, EMA_ORDERS);
    assert!(result.total_positions > 0);

    // PnL (total) is net of commissions, so the commission charged is the
    // difference from the run without commission (backtest_ema_cross_free.rs).
    let pnl = &result.stats_pnls["EUR"];
    let commission = EMA_PNL_WITHOUT_COMMISSION - pnl["PnL (total)"];
    assert_eq!(commission, EMA_ORDERS as f64 * 3.0);

    // The figures of the first run inside the session, reproduced by every
    // run since. They replaced those of the run around the clock: the
    // session keeps 8 of the 24 bars a day, so the EMAs see a coarser wave
    // and cross less often.
    assert_eq!(pnl["PnL (total)"], -243.60000000000036);
    assert_eq!(pnl["PnL% (total)"], -2.436000000000004);
    assert_eq!(pnl["Max Loser"], -99.9);
    assert_eq!(pnl["Expectancy"], -60.900000000000006);
    assert_eq!(
        result.stats_returns["Sharpe Ratio (252 days)"],
        -8.386133812602697
    );
    assert_eq!(
        result.summary["account.IBIS.balance.EUR.total"],
        "9756.40 EUR"
    );
}
