//! The golden rows: the dataset of the `model_ema` strategy built over
//! three days of the fixture's random walk of IC's gold, its rows (their
//! times, features and every target value: P&L, commission, exit and a
//! barrier's distances) printed exactly and compared with the committed
//! snapshot `golden_rows.csv`.
//!
//! The fingerprint a dataset and its models are stored under reads the
//! config by value and the code by version: the dataset version
//! (`model::dataset::VERSION`) and the strategy's feature version. A
//! change of the rows, by the features, the decisions, the targets or a
//! Nautilus pin changing indicators or fills, fails here until one of them
//! is bumped and the snapshot written again with:
//!
//! ```sh
//! GOLDEN_ROWS=write cargo test --test backtest backtest_golden_rows::
//! ```

use std::{fmt::Write as _, fs, path::Path, rc::Rc};

use crate::common::{
    dataset::{backtest, venue, walk},
    ic,
};
use helix_trader::{
    backtest::Backtest,
    model::{
        dataset::{self, DatasetRow},
        handle::{Mode, SettledRows},
    },
    strategy::model_ema::{self, ModelEma, ModelEmaConfig},
};
use jiff::{SignedDuration, Timestamp};

/// Hourly decisions on IC's gold's 1-minute bars, warmed up 4 hours; a
/// bracket 1.5 ATRs of 3 hourly bars in its favour, 7.5 bps against,
/// closed after 45 minutes, and a horizon of 30 minutes, of one ounce.
const CONFIG: &str = r#"
    instrument_ids = ["XAUUSD.ICMARKETS"]
    bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" }
    warmup = "4h"

    [model]
    decision = { every = "1h" }
    targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "30m", barrier = { take_profit = { atr = 1.5 }, stop = { bps = 7.5 }, limit = "45m", atr = { period = 3 } } }
    label = "barrier"
    retrain = "24h"
    window = "46h"
    holdout = "12h"
    margins = { long = 0.0, short = 0.0 }
    min_pnl = 0.0
    min_trades = 1
    learner = { kind = "fake" }
"#;

/// The snapshot, beside this file.
const SNAPSHOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/backtest/golden_rows.csv"
);

/// The variable that writes the snapshot instead of comparing with it.
const WRITE: &str = "GOLDEN_ROWS";

/// The rows of the 21st to the 23rd of October 2026 are the snapshot's.
#[test]
fn the_rows_of_the_example_model_strategy_are_the_snapshots() {
    let config: ModelEmaConfig = toml::from_str(CONFIG).unwrap();
    let (from, to) = (
        ic::at("00:00:00"),
        ic::at("00:00:00") + SignedDuration::from_hours(72),
    );
    let catalog = walk(
        from - SignedDuration::from_hours(5),
        to + SignedDuration::from_hours(1),
    );
    let dataset = model_ema::dataset(&config, catalog.path(), &venue()).unwrap();
    let (first, last) = dataset::days(from, to - SignedDuration::from_nanos(1));
    dataset
        .build(first, last, |start, end| {
            let requirements = config.bars.data_requirements();
            let rows = SettledRows::default();
            let mode = Mode::Dataset {
                rows: Rc::clone(&rows),
            };
            let backtest = Backtest {
                bar_types: requirements.bar_types,
                ticks: requirements.ticks,
                ..backtest(catalog.path(), start, end)
            };
            backtest.run_dataset(ModelEma::new(config.clone(), mode)?)?;
            Ok(rows.take())
        })
        .unwrap();
    let manifest = dataset.manifest().unwrap();
    let rows = dataset.read(from, to).unwrap();
    let names = manifest.feature_names.iter().chain(&manifest.target_names);

    let printed = print(names, &rows);

    if std::env::var_os(WRITE).is_some_and(|value| value == "write") {
        fs::write(SNAPSHOT, &printed).unwrap();
        return;
    }
    compare(&printed, Path::new(SNAPSHOT));
}

/// The rows as CSV, a header of the times and `names`, then each row's
/// decision and settling times and its values, printed exactly: Rust's
/// shortest representation reading back to the same `f64`.
fn print<'a>(names: impl Iterator<Item = &'a String>, rows: &[DatasetRow]) -> String {
    let mut out = String::from("ts_event,settled_at");
    names.for_each(|name| write!(out, ",{name}").unwrap());
    out.push('\n');
    for row in rows {
        write!(
            out,
            "{},{}",
            Timestamp::from(row.ts_event),
            Timestamp::from(row.settled_at)
        )
        .unwrap();
        for value in row.features.iter().chain(&row.targets) {
            write!(out, ",{value}").unwrap();
        }
        out.push('\n');
    }
    out
}

/// Fails, naming the first line that differs, if `printed` is not the
/// snapshot at `snapshot`.
fn compare(printed: &str, snapshot: &Path) {
    let committed = fs::read_to_string(snapshot).unwrap_or_default();
    if printed == committed {
        return;
    }
    let (want, got): (Vec<_>, Vec<_>) = (committed.lines().collect(), printed.lines().collect());
    let lines = want.len().max(got.len());
    let line = (0..lines)
        .find(|&i| want.get(i) != got.get(i))
        .unwrap_or(lines);
    let [want, got] = [&want, &got].map(|text| text.get(line).copied().unwrap_or("(none)"));
    panic!(
        "the dataset rows differ from the snapshot {} at line {}:\n  snapshot: {want}\n  rows:     {got}\n\
         A change of the rows needs a new fingerprint: bump the dataset version \
         (`model::dataset::VERSION`) for a change of the decisions, the targets, the shared \
         feature code or Nautilus' indicators or fills, or the strategy's feature version for \
         a change of its features; then write the snapshot again with \
         `{WRITE}=write cargo test --test backtest backtest_golden_rows::`.",
        snapshot.display(),
        line + 1
    );
}
