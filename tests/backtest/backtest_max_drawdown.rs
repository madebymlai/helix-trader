//! The max drawdown is among the result's return statistics: the portfolio computes it over the
//! returns series with the default statistics. The run is the EMA crossover's time exit
//! (`common::trades`, `TIME_EXIT`) with a commission of 30 EUR per order, so it loses.

use rstest::rstest;

use crate::common::{self, trades};

/// The trade gains 4.20 x 10 = 42 EUR, and its two fills cost 60 EUR: the balance ends at 9982.00
/// EUR, flat. The run is one day, so the returns series holds one return, -18 / 10,000 = -0.18%,
/// a fall from the starting peak: the max drawdown, as a negative fraction.
#[rstest]
fn the_result_holds_the_max_drawdown_of_its_returns() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let result = trades::backtest(catalog.path(), trades::start(), &levels, "30 EUR")
        .run(trades::ema_cross())
        .unwrap()
        .result;

    assert_eq!(common::balance(&result), "9982.00 EUR");
    let reported = result.stats_returns["Max Drawdown"];
    assert!(
        (reported - -0.0018).abs() < 1e-12,
        "{reported}: {:?}",
        result.stats_returns
    );
}
