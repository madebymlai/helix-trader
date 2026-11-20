//! Tests the bars a strategy gets in a backtest. Each bar has the open, high, low and close of the
//! minute quotes in its hour. The first bar uses only the quotes from the run start on.

use std::path::{Path, PathBuf};

use jiff::Timestamp;
use rstest::rstest;

use crate::common::{noise, ohlc, relative};

/// Checks the bars of runs from 12:30 and 13:00.
///
/// - From 12:30, the bar closing 13:00 holds only the quotes from 12:31. The bars closing 14:00 and
///   15:00 hold their whole hours.
/// - From 13:00, the bar closing at the start is not replayed.
///
/// The catalog path is absolute or relative to the working directory, as `--catalog` takes.
#[rstest]
#[case::off_an_hour(noise::at("12:30"), Path::to_path_buf, &["13:00", "14:00", "15:00"])]
#[case::off_an_hour_on_a_relative_catalog(noise::at("12:30"), relative, &["13:00", "14:00", "15:00"])]
#[case::on_a_close(noise::at("13:00"), Path::to_path_buf, &["14:00", "15:00"])]
fn every_bar_has_the_prices_of_its_minute_quotes_from_the_run_start(
    #[case] start: Timestamp,
    #[case] catalog_path: fn(&Path) -> PathBuf,
    #[case] closes: &[&str],
) {
    let catalog = noise::catalog();
    let (recorder, record) = noise::recorder();

    noise::backtest(&catalog_path(catalog.path()), start)
        .run(recorder)
        .unwrap();

    let record = record.borrow();
    let bars: Vec<_> = record.bars.iter().map(ohlc).collect();
    assert_eq!(bars, noise::expected_bars(start));
    let bar_closes: Vec<_> = bars.iter().map(|bar| bar.0).collect();
    let expected: Vec<_> = closes.iter().map(|close| noise::at(close)).collect();
    assert_eq!(bar_closes, expected);
}
