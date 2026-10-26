//! IC Markets' gold carries IC's margin (`common::ic`: 1 / 30, the demo
//! account's 1:30), and the backtest runs unleveraged, so a position locks
//! what IC locks. On 1,000 USD a recorder buys 5 oz on the 10:00 bar, at
//! 4006.10 (20,030.50 USD, twenty times the balance): IC's margin, 667.68
//! USD, fits, and the order fills. It buys 5 more on the 11:00 bar, at
//! 4012.15: their margin, 668.69 USD, exceeds what the first left free
//! (1,000 less its margin and its commission of 0.60), and Nautilus' risk
//! engine denies it. One node per process, so one run per file.

mod common;

use common::{ic, recorder::Recorder};
use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    types::{Money, Quantity},
};

#[test]
fn an_ic_position_locks_ic_s_margin_and_an_order_beyond_it_is_denied() {
    let catalog = ic::catalog();
    let mut backtest = ic::backtest(&catalog);
    backtest.balance = Money::from("1000 USD");
    let (recorder, record) = Recorder::trading_without_warmup(
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
    assert_eq!(
        record.denials,
        ["INITIAL_MARGIN_EXCEEDS_FREE_BALANCE: free=331.72 USD, margin=668.69 USD"]
    );
    let [margin] = record.margins.as_slice() else {
        panic!("{:#?}", record.margins)
    };
    assert_eq!(margin.maintenance, Money::from("667.68 USD"));
}
