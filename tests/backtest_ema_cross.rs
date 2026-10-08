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

    // The figures of the first run filling on quotes, reproduced by every
    // run since. They replaced those of the run filling one tick (0.01 EUR)
    // beyond the bar's close: the same 4 orders now fill at the fixture's
    // ask or bid, 0.05 EUR from the close: 0.04 EUR more a share, 2.40 EUR
    // over the 60 shares of the closed positions (PnL -243.60 then).
    assert_eq!(pnl["PnL (total)"], -246.0);
    assert_eq!(pnl["PnL% (total)"], -2.46);
    assert_eq!(pnl["Max Loser"], -100.7);
    assert_eq!(pnl["Expectancy"], -61.5);
    assert_eq!(
        result.stats_returns["Sharpe Ratio (252 days)"],
        -8.452374688882992
    );
    assert_eq!(
        result.summary["account.IBIS.balance.EUR.total"],
        "9754.00 EUR"
    );
}
