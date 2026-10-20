//! A commission rate on the steps: the orders of the run without
//! commission (backtest_warmup.rs), each fill charged the rate on its
//! value, and the report's costs. One node per process, so one run per
//! file.

mod common;

use common::steps;
use helix_trader::backtest::{commission_rate, report};
use rust_decimal::Decimal;

#[test]
fn a_commission_rate_charges_the_value_of_every_fill_and_the_report_shows_the_costs() {
    let catalog = steps::catalog();
    let mut backtest = steps::backtest(catalog.path(), steps::at("12:30"));
    // 2%, so that the commission of each fill is a whole cent.
    backtest.fee_model = commission_rate(Decimal::new(2, 2)).unwrap();

    let outcome = backtest
        .run(common::shortest_ema_cross(steps::WARMUP))
        .unwrap();
    let result = &outcome.result;

    // The fills of the steps: 10 bought at 620.65 and 20 sold at 580.55,
    // 6,206.50 and 11,611.00 EUR, charged 124.13 and 232.22 EUR. Without
    // commission the balance ends at 9599.00 EUR.
    assert_eq!(result.total_orders, steps::ORDERS);
    assert_eq!(common::balance(result), "9242.65 EUR");

    // Each fill paid the half spread, 0.05 EUR, on each share: 1.50 EUR
    // on 30. The gross PnL is the -757.35 of the run (9242.65 less
    // 10,000) before the 356.35 of commission and the spread; the 10
    // shares still short at the end have paid their 0.50 of it.
    let costs = "
Costs (EUR)
  Commission: 356.350000
  Cost per trade: 178.925000
  Gross PnL: -399.500000
  Spread: 1.500000
";
    let report = report(&outcome);
    assert!(report.contains(costs), "{report}");
}
