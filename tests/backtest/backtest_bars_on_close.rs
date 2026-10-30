//! The internal bars a strategy gets in a backtest, run starting exactly
//! on an hour's close: the bar closing at the start is not replayed, its
//! quotes all before it, and each bar of the run holds its whole hour. One
//! node per thread, so one run per test.

use crate::common::{noise, ohlc};

#[test]
fn a_run_starting_on_a_close_gets_whole_bars_from_the_next() {
    let start = noise::at("13:00");
    let catalog = noise::catalog();
    let (recorder, record) = noise::recorder();

    noise::backtest(catalog.path(), start)
        .run(recorder)
        .unwrap();

    let record = record.borrow();
    let bars: Vec<_> = record.bars.iter().map(ohlc).collect();
    let expected = noise::expected_bars(start);
    assert_eq!(bars, expected);
    let closes: Vec<_> = bars.iter().map(|bar| bar.0).collect();
    assert_eq!(closes, [noise::at("14:00"), noise::at("15:00")]);
}
