//! The max drawdown is among the result's return statistics: the run
//! registers it with the portfolio, which computes it over the returns
//! series with the default statistics. The EMA crossover over the wave
//! with a commission of 3 EUR per order, a run that loses. One node per
//! thread, so one run per test.

use crate::common::wave;

#[test]
fn the_result_holds_the_max_drawdown_of_its_returns() {
    let catalog = wave::catalog();
    let result = wave::backtest(catalog.path(), "3 EUR")
        .run(wave::ema_cross())
        .unwrap()
        .result;

    // A fall from a peak of the equity, as a negative fraction.
    let reported = result.stats_returns.get("Max Drawdown").copied();
    assert!(
        reported.is_some_and(|drawdown| drawdown < 0.0),
        "{:?}",
        result.stats_returns
    );
}
