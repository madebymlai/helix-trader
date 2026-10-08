//! A run starting exactly on a bar's close. The engine starts 1 ns earlier,
//! so that bar's quote, stamped 1 ns before it, is replayed and the order
//! sent on the bar fills on it; the bar arrives once, in the replay, not
//! also in the warmup. One node per process, so one run per file.

mod common;

use common::probe::{Probe, fixture_events};
use nautilus_model::enums::OrderSide;

#[test]
fn a_run_starting_on_a_bar_close_fills_the_first_order_on_that_bars_quote() {
    let catalog = common::catalog();
    let (probe, record) = Probe::new();
    let before = usize::try_from(common::BARS_BEFORE_START).unwrap();
    let mut backtest = common::backtest(catalog.path(), "3 EUR");
    backtest.start = common::bars()[before].ts_event.into();

    backtest.run(probe).unwrap();

    let record = record.borrow();
    assert_eq!(record.events, fixture_events());
    let (side, price, _) = record.fills[0];
    assert_eq!(
        (side, price),
        (OrderSide::Buy, common::quotes()[before].ask_price)
    );
}
