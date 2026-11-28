//! Tests the `model_ema` dataset, built over three days of the fixture's random walk of IC's gold.
//! The test prints the rows exactly and compares them with the snapshot `golden_rows.csv`.
//!
//! A dataset and its models are stored under a fingerprint. The fingerprint reads the config by
//! value and the code by version: the dataset version (`model::dataset::VERSION`) and the
//! strategy's feature version. The rows can change through the features, decisions or targets, or
//! through a Nautilus pin that changes indicators or fills. Any such change fails here until one
//! version is bumped and the snapshot is written again with:
//!
//! ```sh
//! GOLDEN_ROWS=write cargo test --test backtest backtest_golden_rows::
//! ```
//!
//! The strategy decides hourly on 1-minute bars, with averages of 10 and 30. The bracket is 1.5
//! ATRs of 3 hourly bars in its favor and 7.5 bps against, and closes after 45 minutes. The
//! reference stake is one ounce, IC's whole ounces, on stops of about 3 USD.

use std::{fmt::Write as _, fs, path::Path};

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
use rstest::rstest;

use crate::common::{
    dataset::{backtest, walk},
    ic,
};

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
    dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
    decision = { every = "1h" }
    target = { bracket = { take_profit = { atr = 1.5 }, stop = { bps = 7.5 }, atr = { period = 3 } }, limit = "45m" }
    retrain = "24h"
    window = "46h"
    calibration = "12h"
    margins = { long = "0R", short = "0R" }
    min_rows = 1
    learner = { kind = "fake" }
"#;

const SNAPSHOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/backtest/golden_rows.csv"
);

const WRITE: &str = "GOLDEN_ROWS";

#[rstest]
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
    let dataset = ModelEma::dataset(&config, catalog.path()).unwrap();
    let (first, last) = dataset::days(from, to - SignedDuration::from_nanos(1));
    dataset
        .build(first, last, |start, end| {
            let requirements = config.data_requirements();
            let rows = SettledRows::default();
            let mode = Mode::Dataset { rows: rows.clone() };
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

/// Prints exactly, because Rust's shortest representation reads back to the same `f64`.
fn print(features: &[String], payoff: &[String], rows: &[DatasetRow]) -> String {
    let mut out = String::from("ts_event,settled_at");
    for name in features {
        write!(out, ",{name}").unwrap();
    }

    for side in ["long", "short"] {
        for name in payoff {
            write!(out, ",{side}_{name}").unwrap();
        }
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

fn compare(printed: &str, snapshot: &Path) {
    let committed = fs::read_to_string(snapshot).unwrap_or_default();
    if printed == committed {
        return;
    }
    let (committed, printed): (Vec<_>, Vec<_>) =
        (committed.lines().collect(), printed.lines().collect());
    let lines = committed.len().max(printed.len());
    let line = (0..lines)
        .find(|&i| committed.get(i) != printed.get(i))
        .unwrap_or(lines);
    let [committed, printed] =
        [&committed, &printed].map(|text| text.get(line).copied().unwrap_or("(none)"));
    panic!(
        "the dataset rows differ from the snapshot {} at line {}:\n  snapshot: {committed}\n  rows:     {printed}\n\
         A change of the rows needs a new fingerprint: bump the dataset version \
         (`model::dataset::VERSION`) for a change of the decisions, the targets, the shared \
         feature code or Nautilus' indicators or fills, or the strategy's feature version for \
         a change of its features; then write the snapshot again with \
         `{WRITE}=write cargo test --test backtest backtest_golden_rows::`.",
        snapshot.display(),
        line + 1
    );
}
