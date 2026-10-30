//! A commission rate on the EMA's time exit: the orders of the run without
//! commission (backtest_warmup.rs), each fill charged the rate on its
//! value, and the report's costs. One node per thread, so one run per
//! test.

use crate::common::{self, trades};
use helix_trader::backtest::{commission_rate, report};
use rust_decimal::Decimal;

#[test]
fn a_commission_rate_charges_the_value_of_every_fill_and_the_report_shows_the_costs() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let mut backtest = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR");
    // 2%, so that the commission of each fill is a whole cent.
    backtest.fee_model = commission_rate(Decimal::new(2, 2)).unwrap();

    let outcome = backtest.run(trades::ema_cross()).unwrap();
    let result = &outcome.result;

    // The fills of the time exit: 10 bought at 620.65, then 10 sold at
    // 624.85: 6,206.50 and 6,248.50 EUR, charged 124.13 and 124.97 EUR,
    // 249.10 in all. Without commission the balance ends at 10042.00 EUR.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(result), "9792.90 EUR");

    // Each fill paid the half spread, 0.05 EUR, on each share: 1.00 EUR
    // on 20. The PnL of the run is -207.10 (9792.90 less 10,000); before
    // the 249.10 of commission and the spread, 43.00. The cost per trade
    // is commission and spread over the 2 fills: 250.10 / 2.
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
