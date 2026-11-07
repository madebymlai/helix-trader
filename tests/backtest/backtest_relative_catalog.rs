//! A backtest on a catalog given by a path relative to the working
//! directory, as `--catalog` takes one: the node replays the catalog's
//! quotes as it does from an absolute path. One node per thread, so one run
//! per test.

use crate::common::{noise, ohlc, relative};

#[test]
fn a_catalog_given_relative_to_the_working_directory_is_replayed() {
    let start = noise::at("12:30");
    let catalog = noise::catalog();
    let (recorder, record) = noise::recorder();

    noise::backtest(&relative(catalog.path()), start)
        .run(recorder)
        .unwrap();

    let bars: Vec<_> = record.borrow().bars.iter().map(ohlc).collect();
    assert_eq!(bars, noise::expected_bars(start));
}
