//! The EMA crossover over the wave with a commission of 3 EUR per order.
//! One node per process, so one run per file.

mod common;

use common::wave;

#[test]
fn the_ema_crossover_trades_on_internal_bars_and_pays_the_commission_per_order() {
    let catalog = wave::catalog();
    let result = wave::backtest(catalog.path(), "3 EUR")
        .run(wave::ema_cross())
        .unwrap();

    assert_eq!(result.total_orders, wave::EMA_ORDERS);
    assert!(result.total_positions > 0);

    // PnL (total) is net of commissions, so the commission charged is the
    // difference from the run without commission (backtest_ema_cross_free.rs).
    let pnl = &result.stats_pnls["EUR"];
    let commission = wave::EMA_PNL_WITHOUT_COMMISSION - pnl["PnL (total)"];
    assert_eq!(commission, wave::EMA_ORDERS as f64 * 3.0);

    // The figures of the earlier fixture's run, on external hour bars with
    // one quote each, reproduced on internal bars built from the minute
    // quotes (see `wave::EMA_PNL_WITHOUT_COMMISSION`). All but the Sharpe
    // ratio: its daily returns mark the open position to the day's last
    // quote, now a minute before midnight instead of an hour.
    assert_eq!(pnl["PnL (total)"], -246.0);
    assert_eq!(pnl["PnL% (total)"], -2.46);
    assert_eq!(pnl["Max Loser"], -100.7);
    assert_eq!(pnl["Expectancy"], -61.5);
    assert_eq!(
        result.stats_returns["Sharpe Ratio (252 days)"],
        -8.67512468098641
    );
    assert_eq!(common::balance(&result), "9754.00 EUR");
}
