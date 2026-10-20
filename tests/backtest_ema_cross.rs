//! The EMA crossover over the wave with a commission of 3 EUR per order.
//! One node per process, so one run per file.

mod common;

use common::wave;

#[test]
fn the_ema_crossover_trades_on_internal_bars_and_pays_the_commission_per_order() {
    let catalog = wave::catalog();
    let result = wave::backtest(catalog.path(), "3 EUR")
        .run(wave::ema_cross())
        .unwrap()
        .result;

    assert_eq!(result.total_orders, wave::EMA_ORDERS);
    assert!(result.total_positions > 0);

    // PnL (total) is net of commissions, so the commission charged is the
    // difference from the run without commission (backtest_ema_cross_free.rs).
    let pnl = &result.stats_pnls["EUR"];
    let commission = wave::EMA_PNL_WITHOUT_COMMISSION - pnl["PnL (total)"];
    assert_eq!(commission, wave::EMA_ORDERS as f64 * 3.0);
    // 10,000 EUR, less the 234 of the run without commission and the 12 of
    // commission.
    assert_eq!(common::balance(&result), "9754.00 EUR");
}
