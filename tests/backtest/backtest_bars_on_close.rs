//! The internal bars a strategy gets through `strategy::warmup`, run
//! starting exactly on an hour's close: the warmup's oldest bar holds one
//! quote and is left out; the bar closing at the start, its last quote
//! stamped 1 ns before it, comes once, complete, as the first replayed bar,
//! not in the warmup. One node per thread, so one run per test.

use crate::common::{noise, ohlc};

#[test]
fn the_bar_closing_at_the_start_comes_once_complete_after_the_warmup_bars() {
    let start = noise::at("13:00");
    let catalog = noise::catalog();
    let (recorder, record) = noise::recorder();

    noise::backtest(catalog.path(), start)
        .run(recorder)
        .unwrap();

    let record = record.borrow();
    let warmup: Vec<_> = record.warmup.iter().map(ohlc).collect();
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    // The warmup from 08:59:59.999999999 builds the bars closing 10:00 to
    // 12:00 (the one closing 09:00 holds one quote); the bar closing 13:00
    // comes first in the replay.
    let expected = noise::expected_bars(start);
    assert_eq!(warmup, expected[..3]);
    assert_eq!(replay, expected[3..]);
    assert_eq!(replay[0].0, noise::at("13:00"));
}
