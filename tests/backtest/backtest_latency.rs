//! IC Markets' gold on its tick quotes (`common::ic`) at a latency of 30 s, Nautilus' static
//! latency model (`backtest::Latency`). A recorder on the hour MID bars buys on the first bar and
//! closes on the second. Each command fills on the first quote at or after its arrival, not on the
//! one it was sent on, nor on the first after it: IC's quotes come at :07.250 and :37.750 each
//! minute, so a command sent on the hour fills on the quote at :37.750.

use helix_trader::backtest::{Backtest, Latency};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    types::{Price, Quantity},
};
use rstest::rstest;

use crate::common::{
    ic::{self, at},
    recorder::Recorder,
};

#[rstest]
fn at_a_latency_a_market_order_fills_on_the_first_quote_after_its_arrival() {
    // The 10:00:07.250 quote differs from the 10:00:37.750 one (4000.00 / 4000.20), as at 11:00
    let quotes = ic::quotes()
        .into_iter()
        .map(|quote| match Timestamp::from(quote.ts_event) {
            t if t == at("10:00:07.250") => ic::quote(t, "4003.00", "4003.20"),
            _ => quote,
        })
        .collect();
    let catalog = ic::catalog_of(quotes, at("08:00:00"), at("12:00:00"));
    let backtest = Backtest {
        latency: Some(Latency::try_from(SignedDuration::from_secs(30)).unwrap()),
        ..ic::backtest(&catalog)
    };
    let (recorder, record) = Recorder::trading(
        BarType::from(ic::HOUR_BARS),
        vec![(OrderSide::Buy, Quantity::from(10_000))],
    );

    backtest.run(recorder.closing_on(vec![1])).unwrap();

    // The buy, sent at 10:00 (last ask 4006.10), arrives at 10:00:30, after the 10:00:07.250 quote
    // (ask 4003.20), and fills at the next ask, 4000.20. The close, sent at 11:00, fills at the
    // 11:00:37.750 bid, 4000.00.
    let fills: Vec<_> = record
        .borrow()
        .fills
        .iter()
        .map(|&(side, price, stamp)| (side, price, Timestamp::from(stamp)))
        .collect();
    assert_eq!(
        fills,
        [
            (OrderSide::Buy, Price::from("4000.20"), at("10:00:37.750")),
            (OrderSide::Sell, Price::from("4000.00"), at("11:00:37.750")),
        ]
    );
}
