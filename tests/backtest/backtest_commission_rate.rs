//! A commission rate on the EMA's time exit (backtest_ema_time_exit.rs): each fill is charged the
//! rate on its value, and the report shows the costs.

use helix_trader::backtest::{Commission, report};
use rstest::rstest;
use rust_decimal::Decimal;

use crate::common::{self, trades};

#[rstest]
fn a_commission_rate_charges_the_value_of_every_fill_and_the_report_shows_the_costs() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let mut backtest = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR");

    // 2%, so that each fill's commission is a whole cent
    backtest.commission = Commission::Rate(Decimal::new(2, 2));

    let outcome = backtest.run(trades::ema_cross()).unwrap();
    let result = &outcome.result;

    // 10 bought at 620.65 and sold at 624.85: 6,206.50 and 6,248.50 EUR, charged 124.13 and
    // 124.97, 249.10 in all. Without commission the balance ends at 10042.00 EUR.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(result), "9792.90 EUR");

    // Half spread 0.05 EUR on 20 shares: 1.00 EUR. PnL -207.10 (9792.90 less 10,000), 43.00
    // before the 249.10 of commission and the spread. Cost per trade: 250.10 / 2 fills.
    let costs = "
Costs (EUR)
  Commission: 249.100000
  Cost per trade: 125.050000
  Gross PnL: 43.000000
  Spread: 1.000000
";
    let report = report(&outcome);
    assert!(report.contains(costs), "{report}");
}
