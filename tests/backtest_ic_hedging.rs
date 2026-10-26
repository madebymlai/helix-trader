//! IC Markets' venue keeps positions as its hedging cTrader account does
//! (`venue_oms_type`): on IC's gold quotes (`common::ic`), a recorder, its
//! `oms_type` unset, buys 10 oz on the 10:00 bar, at 4006.10, and 10 more
//! on the 11:00 bar, at 4012.15, and closes neither: two positions, each
//! at its own price. One node per process, so one run per file.

mod common;

use common::{ic, recorder::Recorder};
use nautilus_model::{
    data::BarType,
    enums::{OrderSide, PositionSide},
    types::Quantity,
};

#[test]
fn two_overlapping_ic_trades_are_two_positions_at_their_own_prices() {
    let catalog = ic::catalog();
    let (recorder, record) = Recorder::trading_without_warmup(
        BarType::from(ic::HOUR_BARS),
        vec![
            (OrderSide::Buy, Quantity::from(10)),
            (OrderSide::Buy, Quantity::from(10)),
        ],
    );

    ic::backtest(&catalog).run(recorder).unwrap();

    let record = record.borrow();
    let [first, second] = record.positions.as_slice() else {
        panic!("{:#?}", record.positions)
    };
    for position in [first, second] {
        assert!(position.is_open(), "{position:?}");
        assert_eq!(position.side, PositionSide::Long);
        assert_eq!(position.quantity, Quantity::from(10));
    }
    assert_eq!(first.avg_px_open, 4006.10);
    assert_eq!(second.avg_px_open, 4012.15);
}
