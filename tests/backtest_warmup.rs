//! A probe strategy over the fixture catalog: what it receives and in which
//! order, and what its orders fill at and cost. One node per process, so
//! one run per file.

mod common;

use common::probe::{Probe, fixture_events};
use nautilus_model::{enums::OrderSide, types::Money};

#[test]
fn warmup_arrives_before_the_bars_and_market_orders_fill_on_the_decision_hours_quote() {
    let catalog = common::catalog();
    let (probe, record) = Probe::new();

    let result = common::backtest(catalog.path(), "3 EUR")
        .run(probe)
        .unwrap();

    let record = record.borrow();
    assert_eq!(record.events, fixture_events());

    // The buy on the first replayed bar fills at that hour's ask, the sell
    // on the second at that hour's bid, each paying the commission once.
    let before = usize::try_from(common::BARS_BEFORE_START).unwrap();
    let quotes = common::quotes();
    let commission = Some(Money::from("3 EUR"));
    assert_eq!(
        record.fills,
        vec![
            (OrderSide::Buy, quotes[before].ask_price, commission),
            (OrderSide::Sell, quotes[before + 1].bid_price, commission),
        ]
    );
    assert_eq!(result.total_orders, 2);
}
