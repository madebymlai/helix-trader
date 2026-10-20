//! The internal bars a strategy gets through `strategy::warmup`, run
//! starting off an hour: every bar, warmup and replay, has the open, high,
//! low and close of its hour's minute quotes; the bar spanning the warmup
//! end and the run start comes once, complete, as the first replayed bar.
//! One node per process, so one run per file.

mod common;

use common::{noise, ohlc};

#[test]
fn every_bar_has_the_prices_of_its_minute_quotes_and_the_boundary_bar_comes_once_complete() {
    let start = noise::at("12:30");
    let catalog = noise::catalog();
    let (recorder, record) = noise::recorder();

    noise::backtest(catalog.path(), start)
        .run(recorder)
        .unwrap();

    let record = record.borrow();
    let warmup: Vec<_> = record.warmup.iter().map(ohlc).collect();
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    // The warmup from 08:29:59.999999999 builds the bars closing 10:00 to
    // 12:00 (the one closing 09:00 lacks its first half hour); the bar
    // closing 13:00, half warmup, half replay, comes first in the replay.
    let expected = noise::expected_bars(start);
    assert_eq!(warmup, expected[..3]);
    assert_eq!(replay, expected[3..]);
    assert_eq!(replay[0].0, noise::at("13:00"));
}
