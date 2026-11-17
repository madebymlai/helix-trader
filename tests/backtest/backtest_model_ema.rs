//! The `model_ema` strategy (`strategy::model_ema`) end to end on a fixture
//! catalog of IC's gold: the binary's `train` subcommand trains its model
//! at a time, replaying the strategy's dataset run, and a backtest of the
//! strategy then trades on the store. Its features are averages of 1-minute
//! bars, warm from the bars replayed; the fake learner predicts the first,
//! taking every trade, with no edge on the walk, which no gate judges:
//! `train` publishes it, and fails, publishing nothing, only on a slice of
//! fewer rows than `min_rows`, naming the slice, its count and the minimum,
//! as does a live node's launcher (`model::launcher`) starting that `train`
//! as its child; a live node's retrain actor publishes at a retrain time
//! the generation `train` publishes there. A model published straight to the store, a baseline
//! (`common::dataset`), trades from its valid-from: every decision a long on
//! its time-only target, closed 30 minutes later. The binary's `backtest`
//! trades on the store as it is (`backtest_model_ema_trained` backtests a
//! store `train` filled).

use std::{
    cell::RefCell,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::{Command, Output},
    rc::Rc,
    time::Duration,
};

use helix_trader::{
    backtest::Backtest,
    model::{
        ModelStrategy,
        features::side_names,
        handle::Mode,
        launcher::{Launcher, RetrainActor, Retrained},
        learner::TrainingSet,
        store::{ModelStore, StoredModel},
        targets::Side,
    },
    strategy::model_ema::{ModelEma, ModelEmaConfig},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, registry::deregister_actor},
    cache::Cache,
    clock::VirtualClock,
    component::{Component, deregister_component, register_component_actor, start_component},
    testing::wait_until_async,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::TraderId,
    types::{Currency, Money},
};
use rstest::rstest;
use tempfile::TempDir;
use ustr::Ustr;

use crate::common::{
    dataset::{baseline_stored, generation, r, walk},
    ic,
};

/// Hourly decisions on IC's gold's 1-minute bars, its averages of 10 and
/// 30 of them, warmed up an hour, learning the net P&L of the long of a
/// 30-minute time-only target, its unit the worth of the ATR of one hourly
/// bar, warm over the hour, its trades, on a window of 46 hours, the last
/// 12 calibrated on, each slice of at least one row; trading the long
/// alone, at every decision, its margin below any expected P&L.
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
    margins = { long = "-1e9R" }
    min_rows = 1
    learner = { kind = "fake" }
    dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
"#;

/// `CONFIG`'s `[model]` line `line` replaced by `with`.
fn replaced(line: &str, with: &str) -> String {
    assert!(CONFIG.contains(line), "{line}");
    CONFIG.replace(line, with)
}

/// `CONFIG` of a `[model] min_rows` of `min_rows`.
fn of_min_rows(min_rows: usize) -> String {
    replaced("min_rows = 1", &format!("min_rows = {min_rows}"))
}

/// `CONFIG` with a `[model.meta]` section of `lines`.
fn with_meta_section(lines: &str) -> String {
    format!("{CONFIG}\n    [model.meta]\n{lines}")
}

/// A meta section's lines on the fake learner, on a window of 24 hours, the
/// last 6 calibrated on, each slice of at least one row, taking every
/// trade.
const META: &str = r#"
    window = "24h"
    calibration = "6h"
    margin = "-1e9R"
    min_rows = 1
    learner = { kind = "fake" }
"#;

/// Midnight of the 23rd: the window starts at 02:00 of the 21st, its
/// first day's replay from 23:00 of the 20th, the features' warmup
/// before the day. The catalogs start two hours before.
fn t() -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(48)
}

/// The binary's `train` of the strategy of `config` at `at`.
fn train(config: &Path, catalog: &Path, models: &Path, at: Timestamp) -> Output {
    Command::new(env!("CARGO_BIN_EXE_helix-trader"))
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
        .unwrap()
}

/// The stdout of `train`, which succeeds.
fn trained(config: &Path, catalog: &Path, models: &Path, at: Timestamp) -> String {
    let output = train(config, catalog, models, at);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

/// The stderr of `train`, which fails.
fn train_failed(config: &Path, catalog: &Path, models: &Path, at: Timestamp) -> String {
    let output = train(config, catalog, models, at);
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        !output.status.success(),
        "{}\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    stderr
}

/// The models of `config` under `models`.
fn model_store(config: &str, models: &Path) -> ModelStore {
    ModelEma::blueprint(&ModelEmaConfig::load(config).unwrap())
        .unwrap()
        .model_store(models)
        .unwrap()
}

/// The times the generations of `config` under `models` are valid from.
fn generations(config: &str, models: &Path) -> Vec<Timestamp> {
    model_store(config, models).generations().unwrap()
}

/// Asserts that `text` names a slice `slice` of `rows` rows, fewer than
/// `min_rows`.
fn assert_too_few_rows(text: &str, slice: &str, rows: usize, min_rows: usize) {
    for fact in [
        slice.to_string(),
        format!("{rows} rows"),
        format!("min_rows {min_rows}"),
    ] {
        assert!(text.contains(&fact), "{fact}: {text}");
    }
}

/// Publishes straight to the store under `models` a generation valid from
/// `at`, its long a baseline of 0.1R on a side's features, the strategy's
/// then the target's payoff features, as the dataset the `train` subcommand
/// built in `catalog` names them.
fn publish(catalog: &TempDir, models: &Path, at: Timestamp) {
    publish_with(catalog, models, at, |_| {});
}

/// Publishes the generation of `publish`, its long changed by `change`.
fn publish_with(
    catalog: &TempDir,
    models: &Path,
    at: Timestamp,
    change: impl FnOnce(&mut StoredModel),
) {
    let config = ModelEmaConfig::load(CONFIG).unwrap();
    let dataset = ModelEma::dataset(&config, catalog.path())
        .unwrap()
        .manifest()
        .unwrap();
    let names = side_names(&dataset.feature_names, &dataset.payoff_names);
    let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![r(6)]).unwrap();
    let mut model = baseline_stored(
        dataset.fingerprint,
        names,
        &config.model(),
        Some(Side::Long),
        &set,
    );
    change(&mut model);
    ModelEma::blueprint(&config)
        .unwrap()
        .model_store(models)
        .unwrap()
        .publish(&generation(&config.model(), at, vec![model], None))
        .unwrap();
}

/// The catalog of the train at `t` and around it: from 21:00 of the 20th to
/// 04:00 of the 23rd.
fn catalog() -> TempDir {
    walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    )
}

/// `train` at t publishes the fake, which has no edge, no gate judging
/// it: its store holds a model valid from t, and the decisions at 22:00
/// and 23:00 have no model; those of 00:00, 01:00 and 02:00 each a long
/// and its close.
#[rstest]
fn the_train_subcommand_publishes_a_model_without_an_edge_which_trades_from_its_valid_from() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();

    let stdout = trained(&config_path, catalog.path(), &models, t());

    assert!(
        stdout.contains(&format!("published a model valid from {}", t())),
        "{stdout}"
    );
    assert_eq!(generations(CONFIG, &models), [t()]);
    assert_eq!(traded(&catalog, models), (6, 3));
}

/// Worked by hand: decisions every hour, each closed 30 minutes later, so
/// settled before the next. At t, of the window from 02:00 of the 21st,
/// the fit slice is the decisions before noon of the 22nd, the calibration
/// slice's start, settled by then: 22 of the 21st from 02:00 and 12 of the
/// 22nd, 34; the calibration slice those from noon to t, settled by t, 12.
/// At 00:15 of the 23rd, of the window from 02:15 of the 21st, the fit
/// slice is those from 03:00 of the 21st to 11:00 of the 22nd, 21 and 12,
/// 33, the decision of 12:00 settling after the calibration's start, 12:15;
/// the calibration slice those of 13:00 to 23:00, 11, the decision of 00:00
/// settling after the train. A `min_rows` above a slice's count fails the
/// train, which names the slice, its count and the minimum, the fit slice
/// checked first, and publishes nothing.
#[rstest]
#[case::fit_at_t(SignedDuration::ZERO, 35, "fit slice", 34)]
#[case::calibration_at_t(SignedDuration::ZERO, 13, "calibration slice", 12)]
#[case::fit_off_the_hour(SignedDuration::from_mins(15), 34, "fit slice", 33)]
#[case::calibration_off_the_hour(SignedDuration::from_mins(15), 12, "calibration slice", 11)]
fn a_train_with_a_slice_of_fewer_rows_than_the_minimum_fails_naming_it_and_publishes_nothing(
    #[case] after: SignedDuration,
    #[case] min_rows: usize,
    #[case] slice: &str,
    #[case] rows: usize,
) {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    let config = of_min_rows(min_rows);
    std::fs::write(&config_path, &config).unwrap();

    let stderr = train_failed(&config_path, catalog.path(), &models, t() + after);

    assert_too_few_rows(&stderr, slice, rows, min_rows);
    assert_eq!(generations(&config, &models), []);
}

/// Worked by hand: a train at midnight of the 22nd publishes the
/// generation of the long, valid from then, its meta waiting with no
/// generation before it. The train at t then gives the meta the long's
/// decisions of the 22nd, each a long of the generation of the 22nd, 24
/// rows: its fit slice those of 00:00 to 17:00, settled by 18:00, the
/// calibration slice's start, 18; its calibration slice those of 18:00 to
/// 23:00, 6. A meta `min_rows` above a slice's count fails the train,
/// naming the meta, the slice, its count and the minimum, and publishes no
/// generation at t, the long fitted beside the meta neither.
#[rstest]
#[case::fit(19, "fit slice", 18)]
#[case::calibration(7, "calibration slice", 6)]
fn a_meta_with_a_slice_of_fewer_rows_than_its_minimum_fails_naming_it_and_publishes_no_generation(
    #[case] min_rows: usize,
    #[case] slice: &str,
    #[case] rows: usize,
) {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(27),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    let config =
        with_meta_section(&META.replace("min_rows = 1", &format!("min_rows = {min_rows}")));
    std::fs::write(&config_path, &config).unwrap();
    let day_before = t() - SignedDuration::from_hours(24);
    train(&config_path, catalog.path(), &models, day_before);
    assert_eq!(generations(&config, &models), [day_before]);

    let stderr = train_failed(&config_path, catalog.path(), &models, t());

    let meta = stderr
        .split_once("the meta model: ")
        .unwrap_or_else(|| panic!("{stderr}"))
        .1;
    assert_too_few_rows(meta, slice, rows, min_rows);
    assert_eq!(generations(&config, &models), [day_before]);
}

/// A backtest from midnight of the 23rd, the model's valid-from, warms its
/// features on its own 1-minute bars: at the decision of 00:00 it has none,
/// and decides first at 01:00, its slow average of 30 bars initialized at
/// 00:30. The decisions of 01:00 and 02:00 each a long and its close.
#[rstest]
fn a_backtest_from_the_valid_from_first_decides_once_its_features_are_initialized() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    trained(&config_path, catalog.path(), &models, t());

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
        pairs: vec![],
        balance: Money::from("100000 USD"),
        ..ic::backtest(catalog)
    };
    let strategy = ModelEma::new(config, Mode::Backtest { models }).unwrap();
    let outcome = backtest.run(strategy).unwrap();
    (outcome.result.total_orders, outcome.result.total_positions)
}

/// The arguments of the binary's `train` but its `--at` as a live node
/// starts it (`LiveArgs::train_args`), on `config`, `catalog` and the store
/// under `models`.
fn live_train_args(config: &Path, catalog: &Path, models: &Path) -> Vec<OsString> {
    [
        "train".as_ref(),
        "model_ema".as_ref(),
        "--config".as_ref(),
        config.as_os_str(),
        "--catalog".as_ref(),
        catalog.as_os_str(),
        "--models".as_ref(),
        models.as_os_str(),
    ]
    .map(OsStr::to_os_string)
    .to_vec()
}

/// A live node's retrain, the binary's `train` started by its launcher,
/// fails as `train` does, the fit slice at t of 34 rows short of a
/// `min_rows` of 35 (`a_train_with_a_slice_of_fewer_rows_than_the_minimum_fails_naming_it_and_publishes_nothing`):
/// the launcher keeps the current model, its reason naming the slice, its
/// count and the minimum, and nothing trades.
#[rstest]
fn a_live_retrain_through_the_train_subcommand_failing_is_kept_with_its_reason() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, of_min_rows(35)).unwrap();
    let launcher = Launcher::process(
        env!("CARGO_BIN_EXE_helix-trader").into(),
        live_train_args(&config_path, catalog.path(), &models),
    );

    let retrained = launcher.launch(t()).wait();

    let Retrained::Kept(why) = retrained else {
        panic!("{retrained:?}");
    };
    assert_too_few_rows(&why, "fit slice", 34, 35);
    assert_eq!(traded(&catalog, models), (0, 0));
}

/// Adds the retrain actor `actor` at `now` as a node's trader adds an
/// actor: registered on a clock of its own and kept in Nautilus'
/// registries, then started. Its id.
fn added(mut actor: RetrainActor, now: Timestamp) -> Ustr {
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    clock.borrow_mut().advance_time(UnixNanos::from(now), true);
    Component::register(
        &mut actor,
        TraderId::from("HELIX-001"),
        clock,
        Rc::new(RefCell::new(Cache::default())),
    )
    .unwrap();
    let id = actor.actor_id().inner();
    register_component_actor(actor);
    start_component(&id).unwrap();
    id
}

/// A live node started at 00:30 on the 23rd, half an hour after the
/// retrain time t, its store holding no generation: its retrain actor,
/// added as the node's trader adds it, trains t as it starts, through the
/// binary's `train` its launcher starts, the catalog holding the quotes to
/// 04:00, past t. Its generation is the one `train --at t` publishes into
/// another store on the same catalog: valid from t, and of the same
/// models, calibration, baselines and probes, cut at t.
#[rstest]
fn a_live_node_started_after_a_retrain_time_trains_it_at_start_as_train_at_it_does() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("model_ema.toml");
    let (live, trained_at) = (dir.path().join("live"), dir.path().join("trained"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let config = ModelEmaConfig::load(CONFIG).unwrap();
    let actor = RetrainActor::new(
        config.strategy_id(),
        config.model().retrain,
        catalog.path().to_path_buf(),
        config.data_requirements(),
        model_store(CONFIG, &live),
        Launcher::process(
            env!("CARGO_BIN_EXE_helix-trader").into(),
            live_train_args(&config_path, catalog.path(), &live),
        ),
    )
    .unwrap();

    let id = added(actor, t() + SignedDuration::from_mins(30));
    // On a runtime of its own: the fixtures start theirs.
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(wait_until_async(
            || {
                let published = generations(CONFIG, &live) == [t()];
                async move { published }
            },
            Duration::from_secs(300),
        ));
    deregister_component(&id);
    deregister_actor(&id);
    trained(&config_path, catalog.path(), &trained_at, t());

    assert_eq!(
        model_store(CONFIG, &live).read(t()).unwrap(),
        model_store(CONFIG, &trained_at).read(t()).unwrap()
    );
}

/// The binary's `backtest` of the strategy of `config` over `span`, on the
/// store under `models`, its simulated account at `costs`, its commission
/// and balance flags; its stdout and its stderr.
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

/// With a model in the store at each retrain time of the span, published
/// straight once `train` built the dataset it reads, a backtest runs at its
/// own commission, balance and latency, which are its simulated account's
/// alone: its models are the config's (`[model] dataset`), whose costs it
/// reports, followed by the scores of both periods, each of its model.
#[rstest]
fn the_binarys_backtest_reports_its_own_costs_and_its_models_then_each_periods_scores() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(51),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));
    let next = t() + SignedDuration::from_hours(24);
    trained(&config_path, catalog.path(), &models, t());
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

    let (stdout, _) = backtest_at(
        &config_path,
        catalog.path(),
        &models,
        (start, end),
        &other_costs,
    );

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

/// The store holds a generation at t and one at the next retrain time
/// whose long is of another fingerprint, failing its load check: the
/// binary's backtest over both periods logs, at the next period's
/// decisions, the error naming that generation and the check, and decides
/// no trade there, on it nor on the generation of t, so the next period's
/// scores have no generation and no row.
#[rstest]
fn the_binarys_backtest_stops_new_entries_on_a_generation_failing_a_check_naming_it() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(51),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));
    let next = t() + SignedDuration::from_hours(24);
    trained(&config_path, catalog.path(), &models, t());
    publish(&catalog, &models, t());
    publish_with(&catalog, &models, next, |long| {
        long.manifest.fingerprint = "f".repeat(64);
    });

    let costs = ["--commission-rate", "0.00003", "--balance", "10000000 USD"];
    let (stdout, stderr) = backtest_at(&config_path, catalog.path(), &models, (start, end), &costs);

    let output = format!("{stdout}\n{stderr}");
    let refusal = format!("no new entry until the generation valid from {next} loads");
    assert!(output.contains(&refusal), "{output}");
    assert!(
        output.contains("the long model: its fingerprint ffff"),
        "{output}"
    );
    assert!(
        stdout.contains(&format!(
            "Long model scores, out of sample, from {next} to {end}\n  Model valid from: none"
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "Long model scores, out of sample, from {} to {next}\n  Model valid from: {}",
            t(),
            t()
        )),
        "{stdout}"
    );
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

/// The holdout and its minimums are gone, for the sides and the meta: a
/// config still declaring one is refused at load, naming the key, as is one
/// without `min_rows`, or of fewer than one row, or whose calibration and
/// the target's limit of 30 minutes leave no fit slice in the window, 46
/// hours for the sides and 24 for the meta. `train` refuses it before any
/// replay: no store is made.
#[rstest]
#[case::a_holdout(
    replaced("min_rows = 1", "min_rows = 1\n    holdout = \"12h\""),
    "unknown field `holdout`"
)]
#[case::a_minimum_of_trades(
    replaced("min_rows = 1", "min_rows = 1\n    min_trades = 30"),
    "unknown field `min_trades`"
)]
#[case::a_minimum_expectancy(
    replaced("min_rows = 1", "min_rows = 1\n    min_expectancy = \"0.1R\""),
    "unknown field `min_expectancy`"
)]
#[case::no_minimum_of_rows(replaced("min_rows = 1\n", ""), "missing field `min_rows`")]
#[case::a_minimum_of_no_row(of_min_rows(0), "model.min_rows")]
#[case::a_calibration_leaving_no_fit_slice(
    replaced("calibration = \"12h\"", "calibration = \"45h30m\""),
    "model.calibration"
)]
#[case::a_meta_holdout(with_meta_section(&format!("{META}    holdout = \"6h\"\n")), "unknown field `holdout`")]
#[case::a_meta_minimum_of_trades(
    with_meta_section(&format!("{META}    min_trades = 30\n")),
    "unknown field `min_trades`"
)]
#[case::a_meta_minimum_expectancy(
    with_meta_section(&format!("{META}    min_expectancy = \"0R\"\n")),
    "unknown field `min_expectancy`"
)]
#[case::no_meta_minimum_of_rows(with_meta_section(&META.replace("    min_rows = 1\n", "")), "missing field `min_rows`")]
#[case::a_meta_minimum_of_no_row(
    with_meta_section(&META.replace("min_rows = 1", "min_rows = 0")),
    "model.meta.min_rows"
)]
#[case::a_meta_calibration_leaving_no_fit_slice(
    with_meta_section(&META.replace("calibration = \"6h\"", "calibration = \"23h30m\"")),
    "model.meta.calibration"
)]
fn a_config_of_the_old_acceptance_or_without_room_to_fit_is_refused_at_load_naming_the_key(
    #[case] config: String,
    #[case] key: &str,
) {
    let catalog = walk(ic::at("00:00:00"), ic::at("06:00:00"));
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, config).unwrap();

    let (stdout, stderr) = refused(
        "train",
        &config_path,
        catalog.path(),
        &models,
        &usd_args("train", dir.path()),
    );

    assert!(stderr.contains(key), "{key}: {stderr}");
    assert!(!stdout.contains("BacktestEngine"), "{stdout}");
    assert!(!models.exists());
}

/// The fixture's config, its dataset balance (`[model] dataset`) in EUR,
/// where IC's gold's P&L is in USD.
fn eur_config() -> String {
    let (usd, eur) = (r#"balance = "10000000 USD""#, r#"balance = "10000000 EUR""#);
    assert!(CONFIG.contains(usd));
    CONFIG.replace(usd, eur)
}

/// A dataset balance in EUR, where IC's gold's P&L is in USD: the model's
/// targets, sized on it, would be unconverted, and so would a live node's
/// decisions' payoff features, so a backtest, whose own account is in USD,
/// refuses it before building any day of the dataset, `train` before
/// replaying any, and a live start before any node is built, its staging
/// untouched, each naming the dataset balance and both currencies.
#[rstest]
#[case::backtest("backtest")]
#[case::train("train")]
#[case::live("live")]
fn a_dataset_balance_in_another_currency_than_the_pnl_is_refused_before_any_node(
    #[case] subcommand: &str,
) {
    let catalog = walk(ic::at("00:00:00"), ic::at("06:00:00"));
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config, eur_config()).unwrap();
    let args = usd_args(subcommand, dir.path());

    let (stdout, stderr) = refused(subcommand, &config, catalog.path(), &models, &args);

    assert!(
        ["dataset balance", "EUR", "USD", ic::INSTRUMENT_ID]
            .iter()
            .all(|named| stderr.contains(named)),
        "{stderr}"
    );
    assert!(!stdout.contains("BacktestEngine"), "{stdout}");
    assert!(!dir.path().join("staging").exists());
}

/// The fixture's config converting through IC's EURUSD (`[trade]
/// conversions`), its dataset balance `balance`.
fn converting_config(balance: &str) -> String {
    let (risk, usd) = (r#"risk = "0.5%""#, r#"balance = "10000000 USD""#);
    assert!(CONFIG.contains(risk) && CONFIG.contains(usd));
    let conversions = format!("{risk}\n    conversions = [\"{}\"]", ic::EURUSD);
    CONFIG
        .replace(risk, &conversions)
        .replace(usd, &format!("balance = \"{balance}\""))
}

/// `walk`'s catalog of IC's gold over `[from, to)`, IC's EURUSD beside it,
/// quoted 1.24990 / 1.25010 every minute.
fn walk_with_eurusd(from: Timestamp, to: Timestamp) -> TempDir {
    let catalog = walk(from, to);
    ic::store_defined(
        catalog.path(),
        ic::pair(ic::EURUSD, Currency::EUR(), Currency::USD(), 5),
        ic::pair_quotes_over(ic::EURUSD, "1.24990", "1.25010", from, to),
        from,
        to,
    );
    catalog
}

/// A dataset balance in JPY, which EURUSD, the pair the config converts
/// through, does not reach USD, IC's gold's P&L currency, from: `live`,
/// `backtest` and `train` each refuse it before any node, by the check of
/// a backtest's balance, through the pair as the catalog defines it,
/// naming the dataset balance, both currencies and the pair.
#[rstest]
#[case::live("live")]
#[case::backtest("backtest")]
#[case::train("train")]
fn a_dataset_balance_its_conversions_do_not_reach_the_pnl_from_is_refused_before_any_node(
    #[case] subcommand: &str,
) {
    let catalog = walk_with_eurusd(ic::at("00:00:00"), ic::at("06:00:00"));
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config, converting_config("15000000 JPY")).unwrap();
    let args = usd_args(subcommand, dir.path());

    let (stdout, stderr) = refused(subcommand, &config, catalog.path(), &models, &args);

    assert!(
        [
            "dataset balance",
            "JPY",
            "USD",
            ic::INSTRUMENT_ID,
            ic::EURUSD
        ]
        .iter()
        .all(|named| stderr.contains(named)),
        "{stderr}"
    );
    assert!(!stdout.contains("BacktestEngine"), "{stdout}");
    assert!(!dir.path().join("staging").exists());
}

/// A dataset balance in EUR, which EURUSD, the pair the config converts
/// through, reaches USD, IC's gold's P&L currency, from: `train` accepts
/// it, builds the dataset at the balance converted at each row's rate and
/// publishes the fake learner, as it does of a dataset balance in USD.
#[rstest]
fn the_train_subcommand_accepts_a_dataset_balance_its_conversions_reach_the_pnl_from() {
    let catalog = walk_with_eurusd(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config, converting_config("10000000 EUR")).unwrap();

    let stdout = trained(&config, catalog.path(), &models, t());

    assert!(
        stdout.contains(&format!("published a model valid from {}", t())),
        "{stdout}"
    );
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
#[rstest]
#[case::live("live")]
#[case::backtest("backtest")]
#[case::train("train")]
fn a_missing_catalog_is_refused_with_an_error_not_a_panic(#[case] subcommand: &str) {
    let dir = tempfile::tempdir().unwrap();
    let (config, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    let catalog = dir.path().join("no-catalog");
    std::fs::write(&config, CONFIG).unwrap();
    let args = usd_args(subcommand, dir.path());

    let (_, stderr) = refused(subcommand, &config, &catalog, &models, &args);

    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(stderr.contains(&catalog.display().to_string()), "{stderr}");
}

/// A live node of a model strategy on a catalog without IC's gold, which
/// it trades: its models and retrains are built from the catalog, so the
/// start refuses it before any node is built, naming `--catalog` and the
/// instrument, not the dataset balance, which is in its P&L currency.
#[rstest]
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
