//! The max drawdown is among the result's return statistics: the run
//! registers it with the portfolio, which computes it over the returns
//! series with the default statistics. The EMA crossover over the wave
//! with a commission of 3 EUR per order. One node per process, so one run
//! per file.

mod common;

use common::wave;

#[test]
fn the_result_holds_the_max_drawdown_of_its_returns() {
    let catalog = wave::catalog();
    let result = wave::backtest(catalog.path(), "3 EUR")
        .run(wave::ema_cross())
        .unwrap()
        .result;

    // The largest fall from a peak of the equity compounded from 1 over
    // the returns, as a negative fraction.
    let (mut equity, mut peak, mut drawdown) = (1.0_f64, 1.0_f64, 0.0_f64);
    for ret in result.returns_series.values() {
        equity *= 1.0 + ret;
        peak = peak.max(equity);
        drawdown = drawdown.max((peak - equity) / peak);
    }
    assert!(drawdown > 0.0, "the wave's run loses: {drawdown}");
    let reported = result.stats_returns.get("Max Drawdown").copied();
    assert_eq!(reported, Some(-drawdown), "{:?}", result.stats_returns);
}
