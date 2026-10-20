//! A commission rate on the steps: the orders of the run without
//! commission (backtest_warmup.rs), each fill charged the rate on its
//! value. One node per process, so one run per file.

mod common;

use common::steps;
use helix_trader::backtest::commission_rate;
use rust_decimal::Decimal;

#[test]
fn a_commission_rate_charges_the_value_of_every_fill() {
    let catalog = steps::catalog();
    let mut backtest = steps::backtest(catalog.path(), steps::at("12:30"));
    // 2%, so that the commission of each fill is a whole cent.
    backtest.fee_model = commission_rate(Decimal::new(2, 2)).unwrap();

    let result = backtest
        .run(common::shortest_ema_cross(steps::WARMUP))
        .unwrap();

    // The fills of the steps: 10 bought at 620.65 and 20 sold at 580.55,
    // 6,206.50 and 11,611.00 EUR, charged 124.13 and 232.22 EUR. Without
    // commission the balance ends at 9599.00 EUR.
    assert_eq!(result.total_orders, steps::ORDERS);
    assert_eq!(common::balance(&result), "9242.65 EUR");
}
