//! The `model_cross` strategy (`strategy::model_cross`) decides only at the grid ticks after its
//! averages cross, so its dataset, its training and its trading see the same decisions. The
//! binary's `train` builds the dataset and publishes a model, its `backtest` trades on it with a
//! trades file, and the test reads the rows the two wrote to the catalog.
//!
//! The fixture catalog is IC's gold at two levels (`ic::level_quotes`): its hours 0 and 1 of the
//! day close at the low level, 2 and 3 ten dollars higher, and so on, so the level changes with
//! the bar closing at each odd hour. The averages are of 2 and 3 hourly closes, as the cross's.
//! The fast one moves two thirds of the way to each close and the slow one half, so a changed
//! level takes the fast past the slow, onto the new level's side, where the next bar at the level
//! keeps it. In dollars over the low level: a day's replay starts 3 hours before it, at 21:00,
//! low; its bars of 22:00, 23:00 and 00:00 close at 0, 10 and 10. At 00:00 the slow, warm, is
//! their mean 6.67, and the fast 5 + 2/3 of 5 = 8.33, above it. At 01:00, low, the fast is 2.78
//! and the slow 3.33, below; at 02:00 0.93 and 1.67; at 03:00, high, 6.98 and 5.83, above; and so
//! on: a cross at every odd hour's bar and none at the even hours'. A decision on the hourly grid
//! is made 10 ms past its hour (`decide::DECISION_MARGIN`), so the decisions at odd hours are
//! events and those at even hours none.
//!
//! The models are trained at midnight of the 22nd and the 23rd, each on a window of 46 hours. The
//! catalog runs from 21:00 of the 19th, the 3 hours of warmup before the first day of the first
//! window, which begins at 02:00 of the 20th, to 01:00 of the 24th, past the last trade of the
//! backtest. The long's margin, -1e9R, takes every decision, each closed 30 minutes later, before
//! the next.

use std::{
    path::Path,
    process::{Command, Output},
};

use arrow::{array::TimestampNanosecondArray, record_batch::RecordBatch};
use helix_trader::{
    model::ModelStrategy,
    strategy::model_cross::{ModelCross, ModelCrossConfig},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::live::block_on_nautilus_with;
use nautilus_persistence::backend::parquet::io::{
    create_object_store_from_path, read_parquet_from_object_store,
};
use object_store::path::Path as ObjectPath;
use rstest::rstest;

use crate::common::ic;

const CONFIG: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
    ]
    warmup = "3h"

    [indicators.fast]
    period = 2

    [indicators.slow]
    period = 3

    [indicators.cross]
    fast = { period = 2 }
    slow = { period = 3 }

    [trade]
    instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
    risk = "0.5%"

    [model]
    decision = { every = "1h" }
    target = { limit = "30m", atr = { period = 1 } }
    retrain = "24h"
    window = "46h"
    calibration = "12h"
    margins = { long = "-1e9R" }
    min_rows = 1
    learner = { kind = "fake" }
    dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
"#;

fn day(day: i64) -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(24 * (day - 21))
}

/// Returns the decisions at the odd hours of the days from `first` to `last`.
fn odd_hours(first: i64, last: i64) -> Vec<Timestamp> {
    (first..=last)
        .flat_map(|of| {
            (1..24)
                .step_by(2)
                .map(move |hour| day(of) + SignedDuration::from_hours(hour))
        })
        .map(|hour| hour + SignedDuration::from_millis(10))
        .collect()
}

/// Runs `subcommand` of the binary on the strategy and returns its stdout.
fn run(subcommand: &str, config: &Path, catalog: &Path, models: &Path, args: &[String]) -> String {
    let Output {
        status,
        stdout,
        stderr,
    } = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args([subcommand, "model_cross", "--config"])
        .arg(config)
        .arg("--catalog")
        .arg(catalog)
        .arg("--models")
        .arg(models)
        .args(args)
        .output()
        .unwrap();
    let stdout = String::from_utf8(stdout).unwrap();
    assert!(
        status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&stderr)
    );
    stdout
}

/// Returns the `ts_opened` of each position in the trades file at `path`.
fn opened(path: &Path) -> Vec<Timestamp> {
    let dir = path.parent().unwrap().to_str().unwrap().to_string();
    let name = path.file_name().unwrap().to_str().unwrap().to_string();
    let (batches, _) = block_on_nautilus_with(move || async move {
        let (store, _, _) = create_object_store_from_path(&dir, None)?;
        read_parquet_from_object_store(store, &ObjectPath::from(name)).await
    })
    .unwrap();
    batches
        .iter()
        .flat_map(|batch: &RecordBatch| {
            let column = batch.column_by_name("ts_opened").unwrap();
            let column = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            column
                .iter()
                .map(|ns| Timestamp::from_nanosecond(i128::from(ns.unwrap())).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The trains at the 22nd and 23rd write the 20th and 21st, each leaving the day before its time
/// unwritten, as it settles past it, and the backtest the 22nd and 23rd for its scores. Each train
/// fits on the odd hours of its window, from 03:00 two days before to 23:00 the day before: 17,
/// then 6 in its last 12 hours, the calibration. The backtest starts at 21:00 of the 22nd, the
/// warmup a backtest does not request, so its averages warm at 00:00 of the 23rd, as a day's
/// replay's do, and its trades are the longs of the 23rd's rows, one at each.
#[rstest]
fn the_dataset_has_rows_only_at_the_grid_ticks_after_a_cross_and_each_trade_enters_at_one() {
    let (from, to) = (
        day(19) + SignedDuration::from_hours(21),
        day(24) + SignedDuration::from_hours(1),
    );
    let catalog = ic::catalog_of(ic::level_quotes(from, to), from, to);
    let dir = tempfile::tempdir().unwrap();
    let (config, models, trades) = (
        dir.path().join("model_cross.toml"),
        dir.path().join("models"),
        dir.path().join("trades.parquet"),
    );
    std::fs::write(&config, CONFIG).unwrap();

    let trained = run(
        "train",
        &config,
        catalog.path(),
        &models,
        &["--from", &day(22).to_string(), "--to", &day(24).to_string()].map(String::from),
    );
    run(
        "backtest",
        &config,
        catalog.path(),
        &models,
        &[
            "--start",
            &(day(23) - SignedDuration::from_hours(3)).to_string(),
            "--end",
            &day(24).to_string(),
            "--commission-rate",
            "0.00003",
            "--balance",
            "10000000 USD",
            "--trades",
            trades.to_str().unwrap(),
        ]
        .map(String::from),
    );

    assert_eq!(
        trained
            .matches("  Rows of the sides: 17 fit, 6 calibration, min_rows 1\n")
            .count(),
        2,
        "{trained}"
    );
    let dataset =
        ModelCross::dataset(&ModelCrossConfig::load(CONFIG).unwrap(), catalog.path()).unwrap();
    let rows: Vec<Timestamp> = dataset
        .read(day(20), day(24))
        .unwrap()
        .iter()
        .map(|row| Timestamp::from(row.ts_event))
        .collect();
    assert_eq!(rows, odd_hours(20, 23));
    let entries = opened(&trades);
    assert!(
        entries.iter().all(|entry| rows.contains(entry)),
        "{entries:?} against {rows:?}"
    );
    assert_eq!(entries, odd_hours(23, 23));
}
