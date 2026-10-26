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

    let outcome = backtest
        .run(trades::ema_cross(trades::time_exit()))
        .unwrap();
    let result = &outcome.result;

    // The fills of the time exit: 10 bought at 620.65, then 10 sold at
    // 624.55: 6,206.50 and 6,245.50 EUR, charged 124.13 and 124.91 EUR,
    // 249.04 in all. Without commission the balance ends at 10039.00 EUR.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(result), "9789.96 EUR");

    // Each fill paid the half spread, 0.05 EUR, on each share: 1.00 EUR
    // on 20. The PnL of the run is -210.04 (9789.96 less 10,000); before
    // the 249.04 of commission and the spread, 40.00. The cost per trade
    // is commission and spread over the 2 fills: 250.04 / 2.
    let costs = "
Costs (EUR)
  Commission: 249.040000
  Cost per trade: 125.020000
  Gross PnL: 40.000000
  Spread: 1.000000
";
    let report = report(&outcome);
    assert!(report.contains(costs), "{report}");
}
