//! IC Markets' gold carries IC's margin from `common::ic`: 1 / 30, the demo account's 1:30. The
//! backtest runs unleveraged.
//!
//! A recorder starts with 1,000 USD. It buys 5 oz on the 10:00 bar at 4006.10. The margin of 667.68
//! USD fits, so the order fills. It buys 5 on the 11:00 bar at 4012.15, which needs 668.69 USD.
//! Only 331.72 is free (1,000 less margin and 0.60 commission), so Nautilus' risk engine denies
//! the order.

use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    types::{Money, Quantity},
};
use rstest::rstest;

use crate::common::{ic, recorder::Recorder};

#[rstest]
fn an_ic_position_locks_ic_s_margin_and_an_order_beyond_it_is_denied() {
    let catalog = ic::catalog();
    let mut backtest = ic::backtest(&catalog);
    backtest.balance = Money::from("1000 USD");
    let (recorder, record) = Recorder::trading(
        BarType::from(ic::HOUR_BARS),
        vec![
            (OrderSide::Buy, Quantity::from(5)),
            (OrderSide::Buy, Quantity::from(5)),
        ],
    );

    backtest.run(recorder).unwrap();

    let record = record.borrow();
    let [(OrderSide::Buy, price, _)] = record.fills.as_slice() else {
        panic!("{:#?}", record.fills)
    };
    assert_eq!(price.to_string(), "4006.10");
    let [denial] = record.denials.as_slice() else {
        panic!("{:#?}", record.denials)
    };
    assert!(
        denial.contains("331.72 USD") && denial.contains("668.69 USD"),
        "{denial}"
    );
    let [margin] = record.margins.as_slice() else {
        panic!("{:#?}", record.margins)
    };
    assert_eq!(margin.maintenance, Money::from("667.68 USD"));
}
