//! A backtest, and a dataset build through it, replays what the catalog
//! stores over its window, with no check of ours before: the gaps between
//! files a live node promoted, a window reaching past the stored data, or
//! a series with no data at all, which Nautilus only warns of ("No data
//! found for config"), each run on what exists. One node per thread, so one
//! run per test.

use crate::common::{
    self, INSTRUMENT_ID, dataset, hour_bars, ic, noise, ohlc, recorder::Recorder, trades,
};
use helix_trader::{backtest::Backtest, model::dataset::Dataset};
use jiff::{SignedDuration, civil::Date};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, NautilusDataType, QuoteTick},
    identifiers::StrategyId,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

/// The time exit's quotes written as a live node promotes them, in two
/// files each named by its rows' first and last stamps, the one closing
/// 11:59:59.999999999 and the next opening a minute later: the catalog's
/// intervals miss the sliver between them, and the run from 10:00 trades
/// as on one file (`backtest_warmup.rs`).
#[test]
fn a_sliver_between_promoted_files_is_replayed_through() {
    let levels = trades::TIME_EXIT;
    let quotes = trades::quotes(&levels);
    let dir = common::catalog_without_quotes();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let noon = UnixNanos::from(trades::at("12:00"));
    let (morning, afternoon): (Vec<QuoteTick>, Vec<QuoteTick>) =
        quotes.into_iter().partition(|quote| quote.ts_event < noon);
    for file in [morning, afternoon] {
        catalog.write_to_parquet(&file, None, None, None).unwrap();
    }
    let missing = catalog
        .get_missing_intervals_for_request(
            trades::at("10:00").as_nanosecond() as u64,
            trades::end(&levels).as_nanosecond() as u64,
            &NautilusDataType::QuoteTick.into(),
            Some(INSTRUMENT_ID),
        )
        .unwrap();
    assert!(missing.len() > 1, "{missing:?}");

    let result = trades::backtest(dir.path(), trades::at("10:00"), &levels, "0 EUR")
        .run(trades::ema_cross(trades::time_exit()))
        .unwrap()
        .result;

    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), trades::TIME_EXIT_BALANCE);
}

/// The noise is stored from 06:00 to 15:00; a run from 05:00 to 16:00
/// gets the bars of what is stored: from the one closing 07:00, the first
/// whose hour is quoted, to the one closing 15:00.
#[test]
fn a_window_reaching_past_the_stored_quotes_runs_on_what_is_stored() {
    let catalog = noise::catalog();
    let (recorder, record) = Recorder::new(BarType::from(common::MID_BAR_TYPE), None);

    let backtest = Backtest {
        end: noise::at("16:00"),
        ..noise::backtest(catalog.path(), noise::at("05:00"))
    };

    backtest.run(recorder).unwrap();

    let record = record.borrow();
    let bars: Vec<_> = record.bars.iter().map(ohlc).collect();
    let expected = hour_bars(&noise::quotes(), noise::at("06:00"));
    assert_eq!(bars.first().map(|bar| bar.0), Some(noise::at("07:00")));
    assert_eq!(bars, expected);
}

/// A catalog storing the instrument and no quotes: Nautilus warns the
/// series has no data, and the run completes without a bar or an order.
#[test]
fn a_catalog_without_quotes_runs_with_no_bar_and_no_order() {
    let catalog = common::catalog_without_quotes();
    let (recorder, record) = Recorder::new(BarType::from(common::MID_BAR_TYPE), None);

    noise::backtest(catalog.path(), noise::at("07:00"))
        .run(recorder)
        .unwrap();

    assert!(record.borrow().bars.is_empty());
}

/// A dataset build over quotes written as a live node promotes them, in
/// two files with a sliver between them at noon of the 21st: it replays
/// the day through the sliver and stores a row for each hour's decision.
#[test]
fn a_dataset_build_replays_through_a_sliver_between_promoted_files() {
    let (from, to) = (
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(27),
    );
    let noon = UnixNanos::from(ic::at("12:00:00"));
    let (morning, afternoon): (Vec<QuoteTick>, Vec<QuoteTick>) = dataset::walk_quotes(from, to)
        .into_iter()
        .partition(|quote| quote.ts_event < noon);
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![ic::instrument()]).unwrap();
    for file in [morning, afternoon] {
        catalog.write_to_parquet(&file, None, None, None).unwrap();
    }
    let day = Date::constant(2026, 10, 21);
    let dataset = Dataset::new(
        dir.path(),
        StrategyId::from(dataset::STRATEGY),
        &dataset::features(),
        dataset::rule(),
        dataset::targets(),
        &dataset::requirements(),
        &dataset::commission(),
    )
    .unwrap();

    let built = dataset
        .build(day, day, |from, to| dataset::replay(&dir, from, to))
        .unwrap();

    assert_eq!(built, [day]);
    let rows = dataset
        .read(
            ic::at("00:00:00"),
            ic::at("00:00:00") + SignedDuration::from_hours(24),
        )
        .unwrap();
    assert_eq!(rows.len(), 24);
}
