//! The `model_ema` strategy (`strategy::model_ema`) trained by the binary's
//! `train` and backtested by its `backtest` on a store `train` filled: a
//! backtest trains nothing. The fixture catalog is IC's gold with an edge
//! (`ic::edge_quotes`): it rises 4.00 over each even hour and falls back
//! over each odd one, so a short sent at the top of an odd hour wins and
//! one at the bottom of an even hour loses. The strategy trades the short
//! alone, on the fake learner, which predicts a row's first feature, the
//! fast average of the 1-minute bars: near the top at an odd hour's
//! decision, near the bottom at an even hour's. Its calibration's line
//! rises with it, so the short is expected to win at the odd hours alone,
//! clearing its margin of nought there, and each train with rows to fit
//! and calibrate on publishes.
//!
//! The catalog starts at 21:00 of the 18th of October 2026, a Sunday: the
//! train at midnight of the 19th has no row before its calibration slice
//! to fit on, and fails; the trains from the 20th on publish.

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use jiff::{SignedDuration, Timestamp};
use rstest::rstest;
use tempfile::TempDir;

use crate::common::ic;

/// Hourly decisions on IC's gold's 1-minute bars, its averages of 10 and
/// 30 of them, warmed up an hour, learning the net P&L of the short of a
/// 30-minute time-only target, its unit the worth of the ATR of one hourly
/// bar; retrained every day on a window of 46 hours, the last 12 calibrated
/// on, each slice of at least one row; trading the short alone, from an
/// expected nought.
const CONFIG: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },
    ]
    warmup = "1h"

    [indicators.fast]
    period = 10

    [indicators.slow]
    period = 30

    [trade]
    instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
    risk = "0.5%"

    [model]
    decision = { every = "1h" }
    target = { limit = "30m", atr = { period = 1 } }
    retrain = "24h"
    window = "46h"
    calibration = "12h"
    margins = { short = "0R" }
    min_rows = 1
    learner = { kind = "fake" }
    dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
"#;

/// Midnight of the `day`th of October 2026.
fn day(day: i64) -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(24 * (day - 21))
}

/// The edge's quotes from 21:00 of the 18th to 01:00 after midnight of
/// the `last`th: a backtest to that midnight settles its last decision's
/// short half an hour after it.
fn catalog(last: i64) -> TempDir {
    let (from, to) = (
        day(18) + SignedDuration::from_hours(21),
        day(last) + SignedDuration::from_hours(1),
    );
    ic::catalog_of(ic::edge_quotes(from, to), from, to)
}

/// The config written beside the store a test trains into.
struct Files {
    dir: TempDir,
    config: PathBuf,
    models: PathBuf,
}

fn files() -> Files {
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config, CONFIG).unwrap();
    Files {
        dir,
        config,
        models,
    }
}

/// The binary's `subcommand` of the strategy of `files` on `catalog`, with
/// `args`: its exit, stdout and stderr.
fn run(subcommand: &str, files: &Files, catalog: &Path, args: &[String]) -> (bool, String, String) {
    let Output {
        status,
        stdout,
        stderr,
    } = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args([subcommand, "model_ema", "--config"])
        .arg(&files.config)
        .arg("--catalog")
        .arg(catalog)
        .arg("--models")
        .arg(&files.models)
        .args(args)
        .output()
        .unwrap();
    (
        status.success(),
        String::from_utf8(stdout).unwrap(),
        String::from_utf8(stderr).unwrap(),
    )
}

/// `train` over `[from, to)`: its exit, stdout and stderr.
fn train_over(
    files: &Files,
    catalog: &Path,
    (from, to): (Timestamp, Timestamp),
) -> (bool, String, String) {
    let args = [
        "--from".to_string(),
        from.to_string(),
        "--to".to_string(),
        to.to_string(),
    ];
    run("train", files, catalog, &args)
}

/// `train` at `at`, which succeeds.
fn train_at(files: &Files, catalog: &Path, at: Timestamp) {
    let (succeeded, stdout, stderr) = run(
        "train",
        files,
        catalog,
        &["--at".to_string(), at.to_string()],
    );
    assert!(succeeded, "{stdout}\n{stderr}");
}

/// `backtest` over `[start, end)`, its account at the costs of the
/// dataset runs: its exit, stdout and stderr.
fn backtest(
    files: &Files,
    catalog: &Path,
    (start, end): (Timestamp, Timestamp),
) -> (bool, String, String) {
    let args = [
        "--start",
        &start.to_string(),
        "--end",
        &end.to_string(),
        "--commission-rate",
        "0.00003",
        "--balance",
        "10000000 USD",
    ]
    .map(String::from);
    run("backtest", files, catalog, &args)
}

/// The line of `text` reporting the train at `at`.
fn train_line(text: &str, at: Timestamp) -> &str {
    let named = format!("the train at {at}: ");
    text.lines()
        .find(|line| line.starts_with(&named))
        .unwrap_or_else(|| panic!("no {named:?}: {text}"))
}

/// The report of the train at `at` in `text`: the line naming it, then
/// the indented lines of its figures.
fn report(text: &str, at: Timestamp) -> Vec<&str> {
    let first = train_line(text, at);
    let rest = &text[train_line_at(text, at) + first.len()..];
    std::iter::once(first)
        .chain(
            rest.lines()
                .skip(1)
                .take_while(|line| line.starts_with("  ")),
        )
        .collect()
}

/// Whether `text` reports the train at `at` publishing its model set, the
/// store's directory of `at`.
fn published(text: &str, at: Timestamp) -> bool {
    let line = train_line(text, at);
    line.contains("published the model set ") && line.ends_with(&format!("/{at}"))
}

/// Where in `text` the line reporting the train at `at` is.
fn train_line_at(text: &str, at: Timestamp) -> usize {
    text.find(train_line(text, at)).unwrap()
}

/// A span from the 21st to the 23rd holds the retrain times of the 21st
/// and the 22nd, the 23rd its end: each is trained and published, in
/// order. A second train over it publishes nothing, each time already
/// published; one from the 20th trains the 20th alone, which the store
/// lacked.
#[rstest]
fn a_train_over_a_span_publishes_each_retrain_time_in_order_then_only_those_the_store_lacks() {
    let catalog = catalog(23);
    let files = files();
    let (twentieth, span) = (day(20), (day(21), day(23)));

    let (succeeded, first, stderr) = train_over(&files, catalog.path(), span);

    assert!(succeeded, "{first}\n{stderr}");

    for at in [day(21), day(22)] {
        assert!(published(&first, at), "{first}");
    }
    assert!(train_line_at(&first, day(21)) < train_line_at(&first, day(22)));
    assert!(
        !first.contains(&format!("the train at {}", day(23))),
        "{first}"
    );

    let (succeeded, again, stderr) = train_over(&files, catalog.path(), span);

    assert!(succeeded, "{again}\n{stderr}");
    assert!(!again.contains("published the model set"), "{again}");
    for at in [day(21), day(22)] {
        assert!(
            train_line(&again, at).contains("already published"),
            "{again}"
        );
    }

    let (succeeded, earlier, stderr) = train_over(&files, catalog.path(), (twentieth, span.1));

    assert!(succeeded, "{earlier}\n{stderr}");
    assert!(published(&earlier, twentieth), "{earlier}");
    assert_eq!(
        earlier.matches("published the model set").count(),
        1,
        "{earlier}"
    );

    for at in [day(21), day(22)] {
        assert!(
            train_line(&earlier, at).contains("already published"),
            "{earlier}"
        );
    }
}

/// The train of the 19th has no row to fit on, the catalog starting at
/// 21:00 of the 18th, within its calibration slice, from noon: it fails, and the
/// trains of the 20th and the 21st after it publish. Its report holds the
/// figures it reached: the days of its window from 02:00 of the 17th, the
/// 17th and the 18th, built; no row in its fit slice; 2 in its calibration
/// slice, the decisions of 22:00 and 23:00 of the 18th, the first once the
/// slow average of 30 bars is warm, from 21:30. The command fails, listing
/// the 19th alone, with its reason.
#[rstest]
fn a_train_over_a_span_goes_on_past_a_failed_retrain_time_and_fails_listing_it() {
    let catalog = catalog(22);
    let files = files();

    let (succeeded, stdout, stderr) = train_over(&files, catalog.path(), (day(19), day(22)));

    assert!(!succeeded, "{stdout}\n{stderr}");

    for at in [day(20), day(21)] {
        assert!(published(&stdout, at), "{stdout}");
    }
    assert!(train_line_at(&stdout, day(19)) < train_line_at(&stdout, day(20)));
    let report = report(&stdout, day(19));
    assert!(
        report[0].contains("failed: 0 rows in the fit slice"),
        "{report:#?}"
    );

    for line in [
        "  Dataset days: 2 built, 0 cached",
        "  Rows of the sides: 0 fit, 2 calibration, min_rows 1",
    ] {
        assert!(report.contains(&line), "{line}: {report:#?}");
    }

    for figure in ["Wall time", "CPU time", "Peak memory"] {
        let named = format!("  {figure}: ");
        assert!(
            report.iter().any(|line| line.starts_with(&named)),
            "{report:#?}"
        );
    }
    let failed = stderr
        .lines()
        .find(|line| line.contains(&day(19).to_string()))
        .unwrap_or_else(|| panic!("no 19th: {stderr}"));
    assert!(failed.contains("failed"), "{stderr}");
    assert!(!stderr.contains(&day(20).to_string()), "{stderr}");
    assert!(!stderr.contains(&day(21).to_string()), "{stderr}");
}

/// The config never retrained: no `[model] retrain`.
fn never_retrained() -> String {
    let daily = "    retrain = \"24h\"\n";
    assert!(CONFIG.contains(daily));
    CONFIG.replace(daily, "")
}

/// Never retrained, a strategy has no retrain times: a train over a span
/// is refused before any replay, naming why and the one time it trains at
/// instead.
#[rstest]
fn a_train_over_a_span_of_a_strategy_never_retrained_is_refused_before_any_replay() {
    let catalog = catalog(23);
    let files = files();
    std::fs::write(&files.config, never_retrained()).unwrap();

    let (succeeded, stdout, stderr) = train_over(&files, catalog.path(), (day(21), day(23)));

    assert!(!succeeded, "{stdout}\n{stderr}");
    assert!(
        stderr.contains("never retrained") && stderr.contains("--at"),
        "{stderr}"
    );
    assert!(!stdout.contains("BacktestEngine"), "{stdout}");
}

/// `train --at` of `files` at `at`: its stdout, the train succeeding.
fn trained_at(files: &Files, catalog: &Path, at: Timestamp) -> String {
    let (succeeded, stdout, stderr) = run(
        "train",
        files,
        catalog,
        &["--at".to_string(), at.to_string()],
    );
    assert!(succeeded, "{stdout}\n{stderr}");
    stdout
}

/// Never retrained, a train skips only a model set valid exactly from its
/// time: with the model set of the 21st stored, `train --at` the 22nd
/// publishes one valid from the 22nd, and again at the 22nd reports it
/// already published. A backtest of the 21st, before the 22nd, trades on
/// the model set of the 21st, its 12 shorts, as each decision uses the
/// latest model set valid by then.
#[rstest]
fn never_retrained_a_train_publishes_at_its_time_beside_an_older_model_set_which_decides_before_it()
{
    let catalog = catalog(23);
    let files = files();
    std::fs::write(&files.config, never_retrained()).unwrap();
    let older = trained_at(&files, catalog.path(), day(21));
    assert!(published(&older, day(21)), "{older}");

    let newer = trained_at(&files, catalog.path(), day(22));
    let again = trained_at(&files, catalog.path(), day(22));

    assert!(published(&newer, day(22)), "{newer}");
    assert!(
        train_line(&again, day(22)).contains("already published"),
        "{again}"
    );
    let (succeeded, stdout, stderr) = backtest(&files, catalog.path(), (day(21), day(22)));
    assert!(succeeded, "{stdout}\n{stderr}");
    assert!(stdout.contains("Orders: 24\nPositions: 12\n"), "{stdout}");
    assert_day_decided_by(section(&stdout, day(21), day(22)), day(21));
}

/// A backtest from 06:00 of the 22nd on an empty store refuses before
/// any replay, naming the retrain time at its start, midnight of the 22nd,
/// and the train commands filling it: there, or over its span.
#[rstest]
fn a_backtest_on_an_empty_store_is_refused_naming_its_retrain_time_and_the_train_to_run() {
    let catalog = catalog(23);
    let files = files();
    let start = day(22) + SignedDuration::from_hours(6);

    let (succeeded, stdout, stderr) = backtest(&files, catalog.path(), (start, day(23)));

    assert!(!succeeded, "{stdout}\n{stderr}");
    let command = format!(
        "train model_ema --config {} --catalog {} --models {}",
        files.config.display(),
        catalog.path().display(),
        files.models.display()
    );

    for named in [
        format!("{command} --at {}", day(22)),
        format!("{command} --from {start} --to {}", day(23)),
    ] {
        assert!(stderr.contains(&named), "{named}: {stderr}");
    }
    assert!(!stdout.contains("BacktestEngine"), "{stdout}");
    assert!(!stdout.contains("days of the dataset"), "{stdout}");
    assert!(!files.dir.path().join("models").exists());
}

/// The scores of `period` of `stdout`, from `from` to `to`: the section
/// of the short's.
fn section(stdout: &str, from: Timestamp, to: Timestamp) -> &str {
    let heading = format!("Short model scores, out of sample, from {from} to {to}\n");
    let (_, rest) = stdout
        .split_once(&heading)
        .unwrap_or_else(|| panic!("no {heading:?}: {stdout}"));
    rest.split("\n\n").next().unwrap()
}

/// Asserts that the section of a day's period decided by the model valid
/// from `valid_from` reads it, its 24 hourly rows, every short of them and
/// the 12 taken, at the odd hours.
fn assert_day_decided_by(section: &str, valid_from: Timestamp) {
    for line in [
        format!("  Model valid from: {valid_from}"),
        "  Rows: 24".to_string(),
        "  Trades taken: 12".to_string(),
        "  Every trade: 24".to_string(),
    ] {
        assert!(section.lines().any(|l| l == line), "{line}: {section}");
    }
}

/// On a store `train` filled over the span, the 21st and the 22nd, a
/// backtest of the span trains nothing: it builds the days of the span the
/// trains left unwritten, for its scores, the 21st, whose last decision
/// settles after the train of the 22nd, and the 22nd, and runs, the short
/// traded at each odd hour, 12 a day, each an order to open and one to
/// close. Each day's period is scored with its own model.
#[rstest]
fn a_backtest_on_a_store_train_filled_over_its_span_trains_nothing_and_scores_each_period() {
    let catalog = catalog(23);
    let files = files();
    let span = (day(21), day(23));
    let (succeeded, stdout, stderr) = train_over(&files, catalog.path(), span);
    assert!(succeeded, "{stdout}\n{stderr}");

    let (succeeded, stdout, stderr) = backtest(&files, catalog.path(), span);

    assert!(succeeded, "{stdout}\n{stderr}");
    assert!(
        stdout.contains("built 2 days of the dataset, 2026-10-21 to 2026-10-22"),
        "{stdout}"
    );
    assert!(!stdout.contains("published"), "{stdout}");
    assert!(stdout.contains("Orders: 48\nPositions: 24\n"), "{stdout}");
    assert_day_decided_by(section(&stdout, day(21), day(22)), day(21));
    assert_day_decided_by(section(&stdout, day(22), day(23)), day(22));
}

/// With models trained at the 21st and the 23rd alone, a backtest from the
/// 21st to the 24th runs over the hole: the model of the 21st decides the
/// 22nd too, its 12 shorts traded and scored, as live would trade it.
#[rstest]
fn a_backtest_over_a_missing_retrain_time_runs_the_previous_model_deciding_through_it() {
    let catalog = catalog(24);
    let files = files();
    for at in [day(21), day(23)] {
        train_at(&files, catalog.path(), at);
    }

    let (succeeded, stdout, stderr) = backtest(&files, catalog.path(), (day(21), day(24)));

    assert!(succeeded, "{stdout}\n{stderr}");
    assert!(stdout.contains("Orders: 72\nPositions: 36\n"), "{stdout}");
    assert_day_decided_by(section(&stdout, day(21), day(22)), day(21));
    assert_day_decided_by(section(&stdout, day(22), day(23)), day(21));
    assert_day_decided_by(section(&stdout, day(23), day(24)), day(23));
}

/// The config with a meta model on the fake learner, on a window of 24
/// hours, the last 12 calibrated on, each slice of at least one row,
/// taking every trade its finite expectation clears, from -1000R.
fn with_meta() -> String {
    format!(
        "{CONFIG}
    [model.meta]
    window = \"24h\"
    calibration = \"12h\"
    margin = \"-1000R\"
    min_rows = 1
    learner = {{ kind = \"fake\" }}
"
    )
}

/// With a meta, the first retrain time of a span, the 21st, publishes a
/// meta beside the short: the short of its out-of-fold pair, fitted as of
/// the 20th on its window back from there, as the train of the 20th fits it, picks the
/// shorts of the 20th's odd hours, the meta's rows. A backtest of the span
/// then trades from its first retrain time: the short at each odd hour of
/// the 21st and of the 22nd, 12 a day, each an order to open and one to
/// close, every one taken by the meta of its day's model set.
#[rstest]
fn a_periodic_strategy_with_a_meta_publishes_it_from_its_first_train_and_trades_from_that_retrain_time()
 {
    let catalog = catalog(23);
    let files = files();
    std::fs::write(&files.config, with_meta()).unwrap();
    let span = (day(21), day(23));

    let (succeeded, trained, stderr) = train_over(&files, catalog.path(), span);

    assert!(succeeded, "{trained}\n{stderr}");

    for at in [day(21), day(22)] {
        assert!(published(&trained, at), "{trained}");
        assert!(
            report(&trained, at)
                .iter()
                .any(|line| line.starts_with("  Rows of the meta: ")),
            "{trained}"
        );
    }

    let (succeeded, stdout, stderr) = backtest(&files, catalog.path(), span);

    assert!(succeeded, "{stdout}\n{stderr}");
    assert!(stdout.contains("Orders: 48\nPositions: 24\n"), "{stdout}");
    assert_day_decided_by(section(&stdout, day(21), day(22)), day(21));
    assert_day_decided_by(section(&stdout, day(22), day(23)), day(22));
    for (from, to) in [(day(21), day(22)), (day(22), day(23))] {
        let heading = format!("Meta model scores, out of sample, from {from} to {to}\n");
        let (_, rest) = stdout
            .split_once(&heading)
            .unwrap_or_else(|| panic!("no {heading:?}: {stdout}"));
        let meta = rest.split("\n\n").next().unwrap();
        for line in [
            format!("  Model valid from: {from}"),
            "  Rows: 12".to_string(),
            "  Trades taken: 12".to_string(),
        ] {
            assert!(meta.lines().any(|l| l == line), "{line}: {meta}");
        }
    }
}
