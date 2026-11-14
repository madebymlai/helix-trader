//! IC Markets' gold on its tick quotes (`common::ic`) at a latency of 30
//! s: a recorder on the hour MID bars buys on its first bar and closes that
//! position on its second, each command reaching the venue 30 s after it is
//! sent, Nautilus' static latency model (`backtest::Latency`). Each fills
//! on the first quote of the instrument at or after its arrival, not on the
//! one it was sent on, nor on the first after it: IC's quotes come at :07.250
//! and :37.750 each minute, so a command sent on the hour fills on the
//! quote at :37.750.

use crate::common::{
    ic::{self, at},
    recorder::Recorder,
};
use helix_trader::backtest::{Backtest, Latency};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    types::{Price, Quantity},
};

#[test]
fn at_a_latency_a_market_order_fills_on_the_first_quote_after_its_arrival() {
    // The quote at 10:00:07.250 set apart from the one at 10:00:37.750,
    // 4000.00 / 4000.20, as at 11:00:37.750.
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

    // Sent at 10:00 on the quote of 09:59:37.750 (ask 4006.10), the buy
    // reaches the venue at 10:00:30, after the quote of 10:00:07.250 (ask
    // 4003.20), and fills at the ask of the next, 4000.20 at 10:00:37.750;
    // the close, sent at 11:00 on the quote of 10:59:37.750 (bid 4011.90),
    // at the bid of 11:00:37.750, 4000.00.
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
