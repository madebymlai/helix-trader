//! The same strategy as `backtest_hedging_venue.rs`, its `oms_type` unset,
//! on a netting venue, as IB's and OKX's: two overlapping trades
//! (`common::overlap`) add into one position at their average price. One
//! node per process, so one run per file.

mod common;

use common::overlap;
use nautilus_model::enums::{OmsType, PositionSide};
use rust_decimal::Decimal;

#[test]
fn two_overlapping_trades_add_into_one_position_at_their_average_price() {
    let record = overlap::run(|_| OmsType::Netting);

    let record = record.borrow();
    let [position] = record.positions.as_slice() else {
        panic!("{:#?}", record.positions)
    };
    assert!(position.is_open(), "{position:?}");
    assert_eq!(position.side, PositionSide::Long);
    assert_eq!(position.quantity.as_f64(), 20.0);
    assert_eq!(position.avg_px_open, Decimal::new(60_065, 2));
}
