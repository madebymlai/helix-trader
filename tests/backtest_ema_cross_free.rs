//! The EMA crossover over the wave without commission: the reference the
//! commission of `backtest_ema_cross.rs` is measured against. One node per
//! process, so one run per file.

mod common;

use common::wave;

#[test]
fn without_commission_the_ema_crossover_pays_only_the_spread() {
    let catalog = wave::catalog();
    let result = wave::backtest(catalog.path(), "0 EUR")
        .run(wave::ema_cross())
        .unwrap()
        .result;

    assert_eq!(result.total_orders, wave::EMA_ORDERS);
    assert_eq!(
        result.stats_pnls["EUR"]["PnL (total)"],
        wave::EMA_PNL_WITHOUT_COMMISSION
    );
}
