//! The same strategy as `backtest_netting_venue.rs`, its `oms_type` unset,
//! on a hedging venue: two overlapping trades (`common::overlap`) are two
//! positions, each at its own price. One node per process, so one run per
//! file.

mod common;

use common::overlap;
use nautilus_model::enums::{OmsType, PositionSide};
use rust_decimal::Decimal;

#[test]
fn two_overlapping_trades_are_two_positions_at_their_own_prices() {
    let record = overlap::run(OmsType::Hedging);

    let record = record.borrow();
    let [first, second] = record.positions.as_slice() else {
        panic!("{:#?}", record.positions)
    };
    for position in [first, second] {
        assert!(position.is_open(), "{position:?}");
        assert_eq!(position.side, PositionSide::Long);
        assert_eq!(position.quantity.as_f64(), 10.0);
    }
    assert_eq!(first.avg_px_open, Decimal::new(62_065, 2));
    assert_eq!(second.avg_px_open, Decimal::new(58_065, 2));
}
