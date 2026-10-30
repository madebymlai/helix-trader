//! The internal bars a strategy gets in a backtest, run starting off an
//! hour: every bar has the open, high, low and close of its hour's minute
//! quotes replayed, the first only those from the run start on. One node
//! per thread, so one run per test.

use crate::common::{noise, ohlc};

#[test]
fn every_bar_has_the_prices_of_its_minute_quotes_from_the_run_start() {
    let start = noise::at("12:30");
    let catalog = noise::catalog();
    let (recorder, record) = noise::recorder();

    noise::backtest(catalog.path(), start)
        .run(recorder)
        .unwrap();

    let record = record.borrow();
    let bars: Vec<_> = record.bars.iter().map(ohlc).collect();
    // The bar closing 13:00 holds the quotes from 12:31 alone; those
    // closing 14:00 and 15:00 their whole hours.
    let expected = noise::expected_bars(start);
    assert_eq!(bars, expected);
    let closes: Vec<_> = bars.iter().map(|bar| bar.0).collect();
    assert_eq!(
        closes,
        [noise::at("13:00"), noise::at("14:00"), noise::at("15:00")]
    );
}
