//! The `model_ema` strategy (`strategy::model_ema`) end to end on a fixture
//! catalog of IC's gold: the binary's `train` subcommand trains its model
//! at a time, replaying the strategy's dataset run, and a backtest of the
//! strategy then trades on the store. Its features are averages of 1-minute
//! bars, warm from the bars replayed; the fake learner predicts the first,
//! taking every trade, which earns no more than taking every trade: `train`
//! refuses it, failing with the reason
//! and the holdout's figures, as does a live node's launcher
//! (`model::launcher`) starting that `train` as its child. A model
//! published straight to the store, a baseline (`common::dataset`), trades
//! from its valid-from: every decision a long on its time-only target,
//! closed 30 minutes later. The binary's `backtest` builds its dataset and retrains
//! on the schedule first, in its own process, a retrain keeping the model
//! no failure.

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::Command,
};

use crate::common::{
    dataset::{baseline_stored, r, venue, walk},
    ic,
};
use helix_trader::{
    backtest::Backtest,
    model::{
        ModelStrategy,
        features::side_names,
        handle::Mode,
        launcher::{Launcher, Retrained},
        learner::TrainingSet,
        targets::Side,
    },
    strategy::model_ema::{ModelEma, ModelEmaConfig},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::types::Money;
use tempfile::TempDir;

/// Hourly decisions on IC's gold's 1-minute bars, its averages of 10 and
/// 30 of them, warmed up an hour, learning the net P&L of the long of a
/// 30-minute time-only target, its unit the worth of the ATR of one hourly
/// bar, warm over the hour, its trades, on
/// a window of 46 hours, the last 12 held out and the 12 before them
/// calibrated on; trading the long alone, at every decision, its margin
/// below any expected P&L.
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
    instruments = [{ instrument_id = "XAUUSD.ICMARKETS", trade_size = "1" }]

    [model]
    decision = { every = "1h" }
    target = { limit = "30m", atr = { period = 1 } }
    retrain = "24h"
    window = "46h"
    holdout = "12h"
    margins = { long = "-1e9R" }
    min_pnl = "0R"
    min_trades = 1
    learner = { kind = "fake" }
"#;

/// Midnight of the 23rd: the window starts at 02:00 of the 21st, its
/// first day's replay from 23:00 of the 20th, the features' warmup
/// before the day. The catalogs start two hours before.
fn t() -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(48)
}

/// The binary's `train` of the strategy of `config` at `at`, which fails;
/// its stderr.
fn train_refused(config: &Path, catalog: &Path, models: &Path, at: Timestamp) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .arg("train")
        .arg("model_ema")
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
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !output.status.success(),
        "{}\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    stderr
}

/// Whether `text` reports a long model kept with its holdout's figures:
/// its trades taken and every trade, at the commission and at 1.5 times
/// it, its skill, its mean R - E', its calibration slope and its top-decile
/// gap.
fn kept_with_its_figures(text: &str) -> bool {
    [
        "kept the current model: ",
        "taken ",
        "every trade ",
        "at 1.5x the commission",
        "skill ",
        "mean R - E' ",
        "calibration slope ",
        "top-decile gap ",
    ]
    .iter()
    .all(|figure| text.contains(figure))
}

/// Publishes straight to the store under `models` a long model valid from
/// `at`, a baseline of 0.1R on a side's features, the strategy's then the
/// target's payoff features, as the dataset the `train` subcommand built in
/// `catalog` names them.
fn publish(catalog: &TempDir, models: &Path, at: Timestamp) {
    let config = ModelEmaConfig::load(CONFIG).unwrap();
    let dataset = ModelEma::dataset(&config, catalog.path(), &venue())
        .unwrap()
        .manifest()
        .unwrap();
    let names = side_names(&dataset.feature_names, &dataset.payoff_names);
    let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![r(6)]).unwrap();
    let model = baseline_stored(
        dataset.strategy_id,
        dataset.fingerprint,
        names,
        &config.model(),
        Some(Side::Long),
        at,
        &set,
    );
    ModelEma::blueprint(&config)
        .unwrap()
        .model_set(models, &venue())
        .unwrap()
        .side(Side::Long)
        .unwrap()
        .store()
        .publish(&model)
        .unwrap();
}

/// `train` at t refuses the fake, and its store stays empty: no trade.
/// With a model published straight, valid from t, the decisions at 22:00
/// and 23:00 have no model; those of 00:00, 01:00 and 02:00 each a long
/// and its close.
#[test]
fn the_train_subcommand_refuses_a_model_without_an_edge_and_one_published_trades_from_its_valid_from()
 {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();

    let stderr = train_refused(&config_path, catalog.path(), &models, t());

    assert!(kept_with_its_figures(&stderr), "{stderr}");
    assert_eq!(traded(&catalog, models.clone()), (0, 0));
    publish(&catalog, &models, t());
    assert_eq!(traded(&catalog, models), (6, 3));
}

/// A backtest from midnight of the 23rd, the model's valid-from, warms its
/// features on its own 1-minute bars: at the decision of 00:00 it has none,
/// and decides first at 01:00, its slow average of 30 bars initialized at
/// 00:30. The decisions of 01:00 and 02:00 each a long and its close.
#[test]
fn a_backtest_from_the_valid_from_first_decides_once_its_features_are_initialized() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    train_refused(&config_path, catalog.path(), &models, t());
    publish(&catalog, &models, t());

    assert_eq!(traded_from(&catalog, models, t()), (4, 2));
}

/// The orders and positions of a backtest of the strategy on the models
/// under `models`, from 22:00 of the 22nd to 03:00 of the 23rd.
fn traded(catalog: &TempDir, models: PathBuf) -> (usize, usize) {
    traded_from(catalog, models, t() - SignedDuration::from_hours(2))
}

/// The orders and positions of a backtest of the strategy on the models
/// under `models`, from `start` to 03:00 of the 23rd.
fn traded_from(catalog: &TempDir, models: PathBuf, start: Timestamp) -> (usize, usize) {
    let config = ModelEmaConfig::load(CONFIG).unwrap();
    let requirements = config.data_requirements();
    let backtest = Backtest {
        bar_types: requirements.bar_types,
        ticks: requirements.ticks,
        start,
        end: t() + SignedDuration::from_hours(3),
        balance: Money::from("100000 USD"),
        ..ic::backtest(catalog)
    };
    let strategy = ModelEma::new(
        config,
        Mode::Backtest {
            models,
            venue: venue(),
        },
    )
    .unwrap();
    let outcome = backtest.run(strategy).unwrap();
    (outcome.result.total_orders, outcome.result.total_positions)
}

#[test]
fn a_live_retrain_through_the_train_subcommand_keeping_the_model_is_kept_with_its_figures() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    // As a live node starts it (`Children::train_args`), with the fixture's costs.
    let args = [
        "train".as_ref(),
        "model_ema".as_ref(),
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

    let Retrained::Kept(why) = retrained else {
        panic!("{retrained:?}");
    };
    assert!(kept_with_its_figures(&why), "{why}");
    assert_eq!(traded(&catalog, models), (0, 0));
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
        .arg("model_ema")
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

/// A backtest from midnight of the 23rd to noon of the 24th builds its
/// dataset, in its own process, from the first window, 02:00 of the 21st,
/// to the end of the 24th plus the target's limit, then retrains at
/// midnight of each, there too, a node after another: without an edge, each
/// keeps the current model, there being none, and the backtest goes on,
/// each reason printed with its figures, to a run with no model: no
/// period is scored, by a model or by a baseline, which is a model's.
#[test]
fn a_retrain_keeping_the_model_does_not_fail_the_binarys_backtest() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(51),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));

    let (stdout, stderr) = backtest(&config_path, catalog.path(), &models, start, end);

    assert!(stdout.contains("built 4 days of the dataset"), "{stdout}");
    for at in [t(), t() + SignedDuration::from_hours(24)] {
        let reason = format!("the retrain at {at}: kept the current model: ");
        let line = stderr
            .lines()
            .find(|line| line.contains(&reason))
            .unwrap_or_else(|| panic!("{stderr}"));
        assert!(kept_with_its_figures(line), "{line}");
    }
    assert!(stdout.contains("Orders: 0"), "{stdout}");
    assert!(stdout.contains("Model valid from: none"), "{stdout}");
    assert!(
        stdout
            .lines()
            .filter(|line| line.starts_with("  Baseline rows: "))
            .all(|line| line == "  Baseline rows: 0"),
        "{stdout}"
    );
    assert!(!stdout.contains("published a model"), "{stdout}");
}

/// With a model in the store at each retrain time of the span, published
/// straight once the dataset is built, a backtest builds and retrains
/// nothing, and its report is followed by the scores
/// of both periods, each of its model.
#[test]
fn the_binarys_backtest_on_a_store_holding_each_retrains_model_builds_and_trains_nothing() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(51),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));
    let next = t() + SignedDuration::from_hours(24);
    backtest(&config_path, catalog.path(), &models, start, end);
    for at in [t(), next] {
        publish(&catalog, &models, at);
    }

    let (stdout, stderr) = backtest(&config_path, catalog.path(), &models, start, end);

    assert!(!stdout.contains("days of the dataset"), "{stdout}");
    assert!(!stdout.contains("the dataset holds every day"), "{stdout}");
    assert!(!stderr.contains("the retrain at"), "{stderr}");
    let scores = stdout.find("Long model scores").expect(&stdout);
    assert!(stdout.find("Final balances").unwrap() < scores, "{stdout}");
    for (from, to) in [(t(), next), (next, end)] {
        assert!(
            stdout.contains(&format!(
                "Long model scores, out of sample, from {from} to {to}\n  Model valid from: {from}"
            )),
            "{stdout}"
        );
    }
    assert!(stdout.contains("over the span"), "{stdout}");
}
