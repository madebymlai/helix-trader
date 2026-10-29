//! The learned strategy (`strategy::learned`) end to end on a fixture
//! catalog of IC's gold: the binary's `train` subcommand trains its model
//! at a time, replaying the strategy's dataset run, and a backtest of the
//! strategy then trades on the store. Its features are averages of 1-minute
//! bars, warm from the warmup's bars; the fake learner predicts the first,
//! the gold price clamped to 1, so every holdout row ties (AUC 0.5, which
//! the minimum of 0.5 publishes) and every decision from the model's
//! valid-from is a long, closed 30 minutes later. A live node's launcher
//! (`model::launcher`) starting that `train` as its child publishes the
//! same model. The binary's `backtest` retrains on the schedule first, in
//! child processes of its own, a retrain keeping the model no failure.

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::Command,
};

use crate::common::{dataset::walk, ic};
use helix_trader::{
    backtest::Backtest,
    model::{
        handle::Mode,
        launcher::{Launcher, Retrained},
    },
    strategy::learned::{Learned, LearnedConfig},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::types::Money;
use tempfile::TempDir;

/// Hourly decisions on IC's gold's 1-minute bars, warmed up an hour,
/// trades held 30 minutes; learning whether the horizon's long gains, on
/// a window of 46 hours, the last 12 held out.
const CONFIG: &str = r#"
    instrument_ids = ["XAUUSD.ICMARKETS"]
    bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" }
    warmup = "1h"
    hold = "30m"

    [model]
    decision = { every = "1h" }
    targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "1h" }
    label = "horizon"
    retrain = "24h"
    window = "46h"
    holdout = "12h"
    thresholds = { long = 0.6, short = 0.4 }
    min_score = 0.5
    learner = { kind = "fake" }
"#;

/// Midnight of the 23rd: the window starts at 02:00 of the 21st, its
/// first day's replay from 23:00 of the 20th, the strategy's warmup
/// before the day, its warmup request from 22:00. The catalogs start an
/// hour before.
fn t() -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(48)
}

/// The binary's `train` of the strategy of `config` at `at`; its stdout.
fn train(config: &Path, catalog: &Path, models: &Path, at: Timestamp) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .arg("train")
        .arg("learned")
        .arg("--config")
        .arg(config)
        .arg("--catalog")
        .arg(catalog)
        .arg("--models")
        .arg(models)
        .args(["--at", &at.to_string()])
        .args(["--commission-rate", "0.00003", "--balance", "10000000 USD"])
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

#[test]
fn a_model_trained_by_the_train_subcommand_trades_from_its_valid_from() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("learned.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();

    let stdout = train(&config_path, catalog.path(), &models, t());

    assert!(
        stdout.contains(&format!("published a model valid from {}", t())),
        "{stdout}"
    );
    // The decisions at 22:00 and 23:00 have no model; those of 00:00, 01:00
    // and 02:00 each a long and its close.
    assert_eq!(traded(&catalog, models), (6, 3));
}

/// The orders and positions of a backtest of the strategy on the models
/// under `models`, from 22:00 of the 22nd to 03:00 of the 23rd.
fn traded(catalog: &TempDir, models: PathBuf) -> (usize, usize) {
    let config: LearnedConfig = toml::from_str(CONFIG).unwrap();
    let requirements = config.bars.data_requirements();
    let backtest = Backtest {
        bar_types: requirements.bar_types,
        ticks: requirements.ticks,
        start: t() - SignedDuration::from_hours(2),
        end: t() + SignedDuration::from_hours(3),
        warmup: requirements.warmup,
        balance: Money::from("100000 USD"),
        ..ic::backtest(catalog)
    };
    let strategy = Learned::new(config, Mode::Backtest { models }).unwrap();
    let outcome = backtest.run(strategy).unwrap();
    (outcome.result.total_orders, outcome.result.total_positions)
}

#[test]
fn a_model_a_live_retrain_publishes_through_the_train_subcommand_is_traded() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("learned.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    // As a live node starts it (`retrain_args`), with the fixture's costs.
    let args = [
        "train".as_ref(),
        "learned".as_ref(),
        "--config".as_ref(),
        config_path.as_os_str(),
        "--catalog".as_ref(),
        catalog.path().as_os_str(),
        "--models".as_ref(),
        models.as_os_str(),
        "--commission-rate".as_ref(),
        "0.00003".as_ref(),
        "--balance".as_ref(),
        "10000000 USD".as_ref(),
    ]
    .map(OsStr::to_os_string);
    let launcher = Launcher::process(env!("CARGO_BIN_EXE_helix-trader").into(), args.to_vec());

    let retrained = launcher.launch(t()).wait();

    assert_eq!(retrained, Retrained::Published);
    assert_eq!(traded(&catalog, models), (6, 3));
}

/// The binary's `backtest` of the strategy of `config` over `[start,
/// end]`, retraining into the store under `models`; its stdout and its
/// stderr.
fn backtest(
    config: &Path,
    catalog: &Path,
    models: &Path,
    start: Timestamp,
    end: Timestamp,
) -> (String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .arg("backtest")
        .arg("learned")
        .arg("--config")
        .arg(config)
        .arg("--catalog")
        .arg(catalog)
        .arg("--models")
        .arg(models)
        .args(["--start", &start.to_string(), "--end", &end.to_string()])
        .args(["--commission-rate", "0.00003", "--balance", "100000 USD"])
        .args(["--dataset-balance", "10000000 USD"])
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "{stdout}\n{stderr}");
    (stdout, stderr)
}

/// A backtest from midnight of the 23rd to noon of the 24th retrains at
/// midnight of each, in child processes of the binary: the dataset built
/// from the first window, 02:00 of the 21st, to the end of the 24th plus
/// the horizon, then a train at each time, then the run, its report
/// followed by the scores of both periods. A second backtest of the span
/// starts no child.
#[test]
fn the_binarys_backtest_retrains_in_child_processes_once() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(51),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("learned.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));
    let next = t() + SignedDuration::from_hours(24);

    let (stdout, _) = backtest(&config_path, catalog.path(), &models, start, end);

    assert!(stdout.contains("built 4 days of the dataset"), "{stdout}");
    for at in [t(), next] {
        assert!(
            stdout.contains(&format!("published a model valid from {at}")),
            "{stdout}"
        );
    }
    let scores = stdout.find("Model scores").expect(&stdout);
    assert!(stdout.find("Final balances").unwrap() < scores, "{stdout}");
    for (from, to) in [(t(), next), (next, end)] {
        assert!(
            stdout.contains(&format!("Model scores, out of sample, from {from} to {to}")),
            "{stdout}"
        );
    }
    assert!(stdout.contains("over the span"), "{stdout}");

    let (again, _) = backtest(&config_path, catalog.path(), &models, start, end);

    assert!(!again.contains("days of the dataset"), "{again}");
    assert!(!again.contains("published a model"), "{again}");
    assert!(again.contains("over the span"), "{again}");
}

/// With a minimum score of 0.6, above the fake's 0.5, both retrains keep
/// the current model, there being none: `train` fails with the reason,
/// and the backtest goes on, its reason printed, to a run with no model,
/// no period scored but by the baseline.
#[test]
fn a_retrain_keeping_the_model_does_not_fail_the_binarys_backtest() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(51),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("learned.toml"), dir.path().join("models"));
    std::fs::write(
        &config_path,
        CONFIG.replace("min_score = 0.5", "min_score = 0.6"),
    )
    .unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));

    let (stdout, stderr) = backtest(&config_path, catalog.path(), &models, start, end);

    for at in [t(), t() + SignedDuration::from_hours(24)] {
        assert!(
            stderr.contains(&format!(
                "the retrain at {at}: kept the current model: holdout AUC 0.5 below the minimum 0.6"
            )),
            "{stderr}"
        );
    }
    assert!(stdout.contains("Orders: 0"), "{stdout}");
    assert!(stdout.contains("Model valid from: none"), "{stdout}");
    assert!(!stdout.contains("published a model"), "{stdout}");
}
