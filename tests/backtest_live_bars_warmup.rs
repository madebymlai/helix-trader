//! A strategy told to warm up on live bars (`warmup::WarmUp`), as a live
//! run does for an instrument with no history: no history is requested,
//! so no warmup bar comes, and the first bar, partial since it opens at
//! the start, is dropped; every later bar has its hour's quotes. One node
//! per process, so one run per file.

mod common;

use common::{hour_bars, noise, ohlc};
use helix_trader::strategy::warmup::WarmUp;
use jiff::SignedDuration;
use nautilus_model::identifiers::InstrumentId;

#[test]
fn an_instrument_warmed_up_on_live_bars_gets_no_warmup_and_only_full_bars() {
    let start = noise::at("12:30");
    let catalog = noise::catalog();
    let (mut recorder, record) = noise::recorder();
    recorder.warm_up_on_live_bars(&[InstrumentId::from(common::INSTRUMENT_ID)]);

    noise::backtest(catalog.path(), start)
        .run(recorder)
        .unwrap();

    let record = record.borrow();
    assert!(record.warmup.is_empty(), "{:?}", record.warmup);
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    // The engine starts 1 ns before the start: the bar closing 13:00
    // holds only its last half hour and is dropped; those closing 14:00
    // and 15:00 come whole.
    let expected = hour_bars(&noise::quotes(), start - SignedDuration::from_nanos(1));
    assert_eq!(replay, expected);
    assert_eq!(replay[0].0, noise::at("14:00"));
}
