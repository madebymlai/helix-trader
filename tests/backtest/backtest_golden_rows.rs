//! The golden rows: the dataset of the `model_ema` strategy built over
//! three days of the fixture's random walk of IC's gold, its rows (their
//! times, features and each side's payoff features, P&L, commission, exit
//! and unit) printed exactly and compared with the committed snapshot
//! `golden_rows.csv`.
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

use std::{fmt::Write as _, fs, path::Path};

use crate::common::{
    dataset::{backtest, venue, walk},
    ic,
};
use helix_trader::{
    backtest::Backtest,
    model::{
        ModelStrategy,
        dataset::{self, DatasetRow},
        handle::{Mode, SettledRows},
    },
    strategy::model_ema::{ModelEma, ModelEmaConfig},
};
use jiff::{SignedDuration, Timestamp};

/// Hourly decisions on IC's gold's 1-minute bars, its averages of 10 and
/// 30 of them, warmed up 4 hours; a bracket 1.5 ATRs of 3 hourly bars in
/// its favour, 7.5 bps against, closed after 45 minutes, at the reference
/// stake of 4 USD of the dataset balance of 10M USD (`venue`), one ounce,
/// IC's whole ounces, on its stops of about 3 USD.
const CONFIG: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },
    ]
    warmup = "4h"

    [indicators.fast]
    period = 10

    [indicators.slow]
    period = 30

    [trade]
    instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
    risk = "0.00004%"

    [model]
    decision = { every = "1h" }
    target = { bracket = { take_profit = { atr = 1.5 }, stop = { bps = 7.5 }, atr = { period = 3 } }, limit = "45m" }
    retrain = "24h"
    window = "46h"
    holdout = "12h"
    margins = { long = "0R", short = "0R" }
    min_pnl = "0R"
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
    let config = ModelEmaConfig::load(CONFIG).unwrap();
    let (from, to) = (
        ic::at("00:00:00"),
        ic::at("00:00:00") + SignedDuration::from_hours(72),
    );
    let catalog = walk(
        from - SignedDuration::from_hours(5),
        to + SignedDuration::from_hours(1),
    );
    let dataset = ModelEma::dataset(&config, catalog.path(), &venue()).unwrap();
    let (first, last) = dataset::days(from, to - SignedDuration::from_nanos(1));
    dataset
        .build(first, last, |start, end| {
            let requirements = config.data_requirements();
            let rows = SettledRows::default();
            let mode = Mode::Dataset {
                rows: rows.clone(),
                venue: venue(),
            };
            let backtest = Backtest {
                bar_types: requirements.bar_types,
                ticks: requirements.ticks,
                ..backtest(catalog.path(), start, end)
            };
            backtest.run_dataset(ModelEma::new(config.clone(), mode)?)?;
            rows.take()
        })
        .unwrap();
    let manifest = dataset.manifest().unwrap();
    let rows = dataset.read(from, to).unwrap();

    let printed = print(&manifest.feature_names, &manifest.payoff_names, &rows);

    if std::env::var_os(WRITE).is_some_and(|value| value == "write") {
        fs::write(SNAPSHOT, &printed).unwrap();
        return;
    }
    compare(&printed, Path::new(SNAPSHOT));
}

/// The rows as CSV, a header of the times, `features`, each side's `payoff`
/// features and each side's values, then each row's decision and settling
/// times and its values, printed exactly: Rust's shortest representation
/// reading back to the same `f64`.
fn print(features: &[String], payoff: &[String], rows: &[DatasetRow]) -> String {
    let mut out = String::from("ts_event,settled_at");
    features
        .iter()
        .for_each(|name| write!(out, ",{name}").unwrap());
    for side in ["long", "short"] {
        payoff
            .iter()
            .for_each(|name| write!(out, ",{side}_{name}").unwrap());
    }
    out.push_str(
        ",long_pnl,long_commission,long_exit,long_unit,short_pnl,short_commission,short_exit,\
         short_unit\n",
    );
    for row in rows {
        write!(
            out,
            "{},{}",
            Timestamp::from(row.ts_event),
            Timestamp::from(row.settled_at)
        )
        .unwrap();
        let sides = [
            row.long_pnl,
            row.long_commission,
            row.long_exit,
            row.long_unit,
            row.short_pnl,
            row.short_commission,
            row.short_exit,
            row.short_unit,
        ];
        let values = row
            .features
            .iter()
            .chain(&row.long_payoff)
            .chain(&row.short_payoff)
            .chain(&sides);
        for value in values {
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
