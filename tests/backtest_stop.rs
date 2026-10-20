//! A resting stop is checked on every minute's quote, not on the bars: it
//! fills on the first quote crossing it, within the hour. One node per
//! process, so one run per file.

mod common;

use common::{MID_BAR_TYPE, fall, quote_before, recorder::Recorder};
use nautilus_model::{data::BarType, enums::OrderSide, types::Price};

#[test]
fn a_sell_stop_fills_at_the_bid_of_the_first_minute_quote_crossing_it() {
    let catalog = fall::catalog();
    let stop = Price::from("600.00");
    let (recorder, record) = Recorder::new(BarType::from(MID_BAR_TYPE), fall::WARMUP, Some(stop));

    fall::backtest(catalog.path()).run(recorder).unwrap();

    // Sent on the bar closing 13:00. The bid falls 0.50 a minute from
    // 619.45: 600.45 on the quote closing 13:39, 599.95 on the one closing
    // 13:40, the first at or below the stop.
    let crossing = quote_before(&fall::quotes(), fall::at("13:40"));
    assert_eq!(crossing.bid_price, Price::from("599.95"));
    let record = record.borrow();
    assert_eq!(
        record.fills,
        vec![(OrderSide::Sell, crossing.bid_price, crossing.ts_event)]
    );
}
