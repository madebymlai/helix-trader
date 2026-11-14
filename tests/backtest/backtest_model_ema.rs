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
    dataset::{baseline_stored, r, walk},
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
    instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
    risk = "0.5%"

    [model]
    decision = { every = "1h" }
    target = { limit = "30m", atr = { period = 1 } }
    retrain = "24h"
    window = "46h"
    holdout = "12h"
    margins = { long = "-1e9R" }
    min_expectancy = "0R"
    min_trades = 1
    learner = { kind = "fake" }
    dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
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
    let dataset = ModelEma::dataset(&config, catalog.path())
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
        .model_set(models)
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
    let strategy = ModelEma::new(config, Mode::Backtest { models }).unwrap();
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
    // As a live node starts it (`LiveArgs::train_args`).
    let args = [
        "train".as_ref(),
        "model_ema".as_ref(),
        "--config".as_ref(),
        config_path.as_os_str(),
        "--catalog".as_ref(),
        catalog.path().as_os_str(),
        "--models".as_ref(),
        models.as_os_str(),
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
/// end]`, retraining into the store under `models`, its simulated account
/// at the commission and balance of the config's dataset runs; its stdout
/// and its stderr.
fn backtest(
    config: &Path,
    catalog: &Path,
    models: &Path,
    start: Timestamp,
    end: Timestamp,
) -> (String, String) {
    let costs = ["--commission-rate", "0.00003", "--balance", "10000000 USD"];
    backtest_at(config, catalog, models, (start, end), &costs)
}

/// `backtest` over `span`, its simulated account at `costs`, its commission
/// and balance flags.
fn backtest_at(
    config: &Path,
    catalog: &Path,
    models: &Path,
    (start, end): (Timestamp, Timestamp),
    costs: &[&str],
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
        .args(costs)
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
/// nothing, whatever its own commission, balance and latency, which are its
/// simulated account's alone: its models are the config's
/// (`[model] dataset`), whose costs it reports, followed by the scores of
/// both periods, each of its model.
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
    let other_costs = [
        "--commission-rate",
        "0.0001",
        "--balance",
        "100000 USD",
        "--latency",
        "150ms",
    ];

    let (stdout, stderr) = backtest_at(
        &config_path,
        catalog.path(),
        &models,
        (start, end),
        &other_costs,
    );

    assert!(!stdout.contains("days of the dataset"), "{stdout}");
    assert!(!stdout.contains("the dataset holds every day"), "{stdout}");
    assert!(!stderr.contains("the retrain at"), "{stderr}");
    assert!(
        stdout.contains(
            "Run at: commission rate 0.0001, balance 100000.00 USD, latency 150ms \
             (--commission, --balance, --latency)"
        ),
        "{stdout}"
    );
    assert!(
        stdout.contains("Models trained at: commission rate 0.00003, balance 10000000 USD"),
        "{stdout}"
    );
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

/// The binary's `subcommand` of the strategy of `config` on `catalog`, its
/// store under `models`, with `args`, run in the directory holding `config`
/// with no environment, so that neither the shell nor a `.env` above the
/// crate's root gives it a venue's credential; it fails: its stdout and
/// stderr.
fn refused(
    subcommand: &str,
    config: &Path,
    catalog: &Path,
    models: &Path,
    args: &[String],
) -> (String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .env_clear()
        .current_dir(config.parent().unwrap())
        .args([subcommand, "model_ema", "--config"])
        .arg(config)
        .arg("--catalog")
        .arg(catalog)
        .arg("--models")
        .arg(models)
        .args(args)
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!output.status.success(), "{stdout}\n{stderr}");
    (stdout, stderr)
}

/// The fixture's config, its dataset balance (`[model] dataset`) in EUR,
/// where IC's gold's P&L is in USD.
fn eur_config() -> String {
    let (usd, eur) = (r#"balance = "10000000 USD""#, r#"balance = "10000000 EUR""#);
    assert!(CONFIG.contains(usd));
    CONFIG.replace(usd, eur)
}

/// A dataset balance in EUR, where IC's gold's P&L is in USD: the model's
/// targets, sized on it, would be unconverted, so a backtest, whose own
/// account is in USD, refuses it before building any day of the dataset,
/// and `train` before replaying any, no node started, naming the dataset
/// balance and both currencies.
#[test]
fn a_dataset_balance_in_another_currency_than_the_pnl_is_refused_before_any_build() {
    let catalog = walk(ic::at("00:00:00"), ic::at("06:00:00"));
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config, eur_config()).unwrap();

    for subcommand in ["backtest", "train"] {
        let args = usd_args(subcommand, dir.path());

        let (stdout, stderr) = refused(subcommand, &config, catalog.path(), &models, &args);

        assert!(
            ["dataset balance", "EUR", "USD", ic::INSTRUMENT_ID]
                .iter()
                .all(|named| stderr.contains(named)),
            "{subcommand}: {stderr}"
        );
        assert!(!stdout.contains("BacktestEngine"), "{subcommand}: {stdout}");
    }
}

/// A live node whose config's dataset balance is in EUR, where IC's gold's
/// P&L is in USD: the decisions' payoff features would be sized on it
/// unconverted, so the start refuses it before any node is built, its
/// staging untouched, naming the dataset balance and both currencies.
#[test]
fn a_live_dataset_balance_in_another_currency_than_the_pnl_is_refused_before_any_node() {
    let catalog = walk(ic::at("00:00:00"), ic::at("06:00:00"));
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config, eur_config()).unwrap();

    let (_, stderr) = refused(
        "live",
        &config,
        catalog.path(),
        &models,
        &usd_args("live", dir.path()),
    );

    assert!(
        ["dataset balance", "EUR", "USD", ic::INSTRUMENT_ID]
            .iter()
            .all(|named| stderr.contains(named)),
        "{stderr}"
    );
    assert!(!dir.path().join("staging").exists());
}

/// The arguments of `subcommand` of `refused` beyond its config, catalog
/// and store: for `backtest` a window and its simulated account's costs, a
/// balance in USD, IC's gold's P&L currency; for `train` its time; for
/// `live` a staging directory under `dir`.
fn usd_args(subcommand: &str, dir: &Path) -> Vec<String> {
    let args: Vec<&str> = match subcommand {
        "backtest" => vec![
            "--start",
            "2026-07-21T03:00:00Z",
            "--end",
            "2026-07-21T05:00:00Z",
            "--commission-rate",
            "0.00003",
            "--balance",
            "10000000 USD",
        ],
        "train" => vec!["--at", "2026-07-21T05:00:00Z"],
        "live" => vec!["--staging"],
        _ => unreachable!("{subcommand}"),
    };
    let staging = (subcommand == "live").then(|| dir.join("staging").display().to_string());
    args.into_iter().map(String::from).chain(staging).collect()
}

/// A catalog directory that does not exist: `live`, `backtest` and
/// `train` each refuse it with an error naming it, none panicking.
#[test]
fn a_missing_catalog_is_refused_with_an_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    let catalog = dir.path().join("no-catalog");
    std::fs::write(&config, CONFIG).unwrap();

    for subcommand in ["live", "backtest", "train"] {
        let args = usd_args(subcommand, dir.path());

        let (_, stderr) = refused(subcommand, &config, &catalog, &models, &args);

        assert!(!stderr.contains("panicked"), "{subcommand}: {stderr}");
        assert!(
            stderr.contains(&catalog.display().to_string()),
            "{subcommand}: {stderr}"
        );
    }
}

/// A live node of a model strategy on a catalog without IC's gold, which
/// it trades: its models and retrains are built from the catalog, so the
/// start refuses it before any node is built, naming `--catalog` and the
/// instrument, not the dataset balance, which is in its P&L currency.
#[test]
fn a_live_model_strategy_on_a_catalog_without_its_traded_instrument_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    let catalog = dir.path().join("catalog");
    std::fs::create_dir(&catalog).unwrap();
    std::fs::write(&config, CONFIG).unwrap();

    let (_, stderr) = refused(
        "live",
        &config,
        &catalog,
        &models,
        &usd_args("live", dir.path()),
    );

    assert!(
        stderr.contains("--catalog") && stderr.contains(ic::INSTRUMENT_ID),
        "{stderr}"
    );
    assert!(!stderr.contains("dataset balance"), "{stderr}");
    assert!(!dir.path().join("staging").exists());
}
