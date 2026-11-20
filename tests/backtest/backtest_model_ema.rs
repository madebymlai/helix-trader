//! The `model_ema` strategy (`strategy::model_ema`) end to end on a fixture catalog of IC's gold:
//! `train` replays the strategy's dataset run and publishes a model at a time, and a backtest
//! trades on the store. The fake learner takes every trade, with no edge on the walk and no gate,
//! so `train` publishes it; `train` fails, publishing nothing, only on a slice of fewer rows than
//! `min_rows`, naming the slice, its count and the minimum, as does a live node's launcher
//! (`model::launcher`) starting it. A baseline published straight to the store
//! (`common::dataset`) trades from its valid-from: a long at every decision on a time-only target
//! closed 30 minutes later.

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
        ModelPart, ModelStrategy,
        features::side_names,
        handle::Mode,
        launcher::{Launcher, RetrainActor, Retrained},
        learner::TrainingSet,
        store::{ModelStore, StoredModel},
        targets::Side,
    },
    strategy::model_ema::{ModelEma, ModelEmaConfig, ModelEmaParams},
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
    dataset::{baseline_stored, model_set, r, walk},
    ic,
};

/// Hourly decisions on 1-minute bars, a window of 46 hours with the last 12 calibrated on; every
/// decision a long on a 30-minute time-only target.
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

fn replaced(line: &str, with: &str) -> String {
    assert!(CONFIG.contains(line), "{line}");
    CONFIG.replace(line, with)
}

fn of_min_rows(min_rows: usize) -> String {
    replaced("min_rows = 1", &format!("min_rows = {min_rows}"))
}

fn with_meta_section(lines: &str) -> String {
    format!("{CONFIG}\n    [model.meta]\n{lines}")
}

const META: &str = r#"
    window = "24h"
    calibration = "6h"
    margin = "-1e9R"
    min_rows = 1
    learner = { kind = "fake" }
"#;

/// Midnight of the 23rd: its window starts at 02:00 of the 21st, whose replay starts at 23:00 of
/// the 20th, the warmup.
fn t() -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(48)
}

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

fn model_store(config: &str, models: &Path) -> ModelStore {
    ModelEma::blueprint(&ModelEmaConfig::load(config).unwrap())
        .unwrap()
        .model_store(models)
        .unwrap()
}

fn model_sets(config: &str, models: &Path) -> Vec<Timestamp> {
    model_store(config, models).model_sets().unwrap()
}

fn assert_too_few_rows(text: &str, slice: &str, rows: usize, min_rows: usize) {
    for fact in [
        slice.to_string(),
        format!("{rows} rows"),
        format!("min_rows {min_rows}"),
    ] {
        assert!(text.contains(&fact), "{fact}: {text}");
    }
}

/// Publishes a model set valid from `at`: a baseline long of 0.1R under its blueprint's
/// fingerprint, as its dataset names its features.
fn publish(models: &Path, at: Timestamp) {
    publish_with(models, at, |_| {});
}

fn publish_with(models: &Path, at: Timestamp, change: impl FnOnce(&mut StoredModel)) {
    let config = ModelEmaConfig::load(CONFIG).unwrap();
    let blueprint = ModelEma::blueprint(&config).unwrap();
    let features = ModelEmaParams::features(&config).unwrap();
    let names = side_names(features.names(), &config.model().target.payoff_names());
    let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![r(6)]).unwrap();
    let mut model = baseline_stored(
        blueprint.fingerprint().unwrap().to_string(),
        names,
        &config.model(),
        Some(Side::Long),
        &set,
    );
    change(&mut model);
    blueprint
        .model_store(models)
        .unwrap()
        .publish(&model_set(&config.model(), at, vec![model], None))
        .unwrap();
}

/// From 21:00 of the 20th to 04:00 of the 23rd.
fn catalog() -> TempDir {
    walk(
        ic::at("00:00:00") - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(4),
    )
}

/// The fake has no edge and no gate: its store holds a model from t; 22:00 and 23:00 have none,
/// 00:00 to 02:00 each a long and its close.
#[rstest]
fn the_train_subcommand_publishes_a_model_without_an_edge_which_trades_from_its_valid_from() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();

    let stdout = trained(&config_path, catalog.path(), &models, t());

    assert!(
        report(&stdout, t())[0].contains("published the model set"),
        "{stdout}"
    );
    assert_eq!(model_sets(CONFIG, &models), [t()]);
    assert_eq!(traded(&catalog, models), (6, 3));
}

/// Hourly decisions closed 30 minutes later settle before the next. At t, window from 02:00 of the
/// 21st: fit slice the decisions before noon of the 22nd (the calibration's start), 22 + 12 = 34;
/// calibration slice noon to t, 12. At 00:15, window from 02:15 of the 21st: fit slice 03:00 of the
/// 21st to 11:00 of the 22nd, 21 + 12 = 33 (12:00 settles at 12:15, after the calibration's
/// start); calibration slice 13:00 to 23:00, 11 (00:00 settles after the train). A `min_rows` above
/// a count fails the train naming the slice, count and minimum, the fit slice first, and publishes
/// nothing.
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
    assert_eq!(model_sets(&config, &models), []);
}

/// The train at t fits the long of its out-of-fold pair as of midnight of the 22nd, from 02:00 of
/// the 20th, and gives the meta its 24 decisions of the 22nd: fit slice 00:00 to 17:00 (settled by
/// 18:00, the calibration's start), 18; calibration slice 18:00 to 23:00, 6. A meta `min_rows`
/// above a count fails the train naming the meta, slice, count and minimum, and publishes no model
/// set.
#[rstest]
#[case::fit(19, "fit slice", 18)]
#[case::calibration(7, "calibration slice", 6)]
fn a_meta_with_a_slice_of_fewer_rows_than_its_minimum_fails_naming_it_and_publishes_no_model_set(
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

    let stderr = train_failed(&config_path, catalog.path(), &models, t());

    let meta = stderr
        .split_once("the meta model: ")
        .unwrap_or_else(|| panic!("{stderr}"))
        .1;
    assert_too_few_rows(meta, slice, rows, min_rows);
    assert_eq!(model_sets(&config, &models), []);
}

fn report(stdout: &str, at: Timestamp) -> Vec<&str> {
    let named = format!("the train at {at}: ");
    let mut lines = stdout.lines().skip_while(|line| !line.starts_with(&named));
    let first = lines
        .next()
        .unwrap_or_else(|| panic!("no {named:?}: {stdout}"));
    std::iter::once(first)
        .chain(lines.take_while(|line| line.starts_with("  ")))
        .collect()
}

/// The figures that vary per run are present, their values not asserted.
fn assert_run_figures(report: &[&str]) {
    let figure = |name: &str| {
        let line = report
            .iter()
            .find(|line| line.starts_with(&format!("  {name}: ")))
            .unwrap_or_else(|| panic!("no {name}: {report:#?}"));
        line.split_once(": ").unwrap().1
    };
    assert!(!figure("Wall time").is_empty(), "{report:#?}");
    let cpu = figure("CPU time");
    assert!(cpu.contains("user") && cpu.contains("system"), "{cpu}");
    assert!(!figure("Peak memory").is_empty(), "{report:#?}");
}

fn assert_figures(report: &[&str], lines: &[&str]) {
    for line in lines {
        assert!(report.contains(line), "{line}: {report:#?}");
    }
    let rows = report.iter().filter(|line| line.starts_with("  Rows of "));
    assert_eq!(
        rows.count(),
        lines
            .iter()
            .filter(|line| line.starts_with("  Rows of "))
            .count()
    );
}

fn assert_no_performance(report: &[&str]) {
    for figure in [
        "skill",
        "expectancy",
        "win rate",
        "earnings",
        "stress",
        "holdout",
        "slope",
    ] {
        assert!(
            report
                .iter()
                .all(|line| !line.to_lowercase().contains(figure)),
            "{figure}: {report:#?}"
        );
    }
}

/// The train at t reads the 21st and 22nd, both built, the dataset empty; sides' fit slice 34,
/// calibration 12. It writes the 21st alone (its last decision settles by t, the 22nd's not). The
/// train at 00:15 reads the 21st to the 23rd: the 21st cached, the 22nd and 23rd built; slices 33
/// and 11. Each report names the model set published under its time, no performance figure.
#[rstest]
fn a_train_reports_its_model_set_run_figures_dataset_days_and_rows_of_each_slice() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let later = t() + SignedDuration::from_mins(15);

    let first = trained(&config_path, catalog.path(), &models, t());
    let second = trained(&config_path, catalog.path(), &models, later);

    let store = model_store(CONFIG, &models);

    for (stdout, at, days, rows) in [
        (&first, t(), "2 built, 0 cached", "34 fit, 12 calibration"),
        (
            &second,
            later,
            "2 built, 1 cached",
            "33 fit, 11 calibration",
        ),
    ] {
        let report = report(stdout, at);
        let model_set = store.directory().join(at.to_string());
        assert_eq!(
            report[0],
            format!(
                "the train at {at}: published the model set {}",
                model_set.display()
            )
        );
        assert_run_figures(&report);
        assert_figures(
            &report,
            &[
                &format!("  Dataset days: {days}"),
                &format!("  Rows of the sides: {rows}, min_rows 1"),
            ],
        );
        assert_no_performance(&report);
    }
}

/// The train at t reads from its out-of-fold pair's window, from 02:00 of the 20th, so builds the
/// 20th to the 22nd. The pair, fitted as of midnight of the 22nd, has 34 fit rows (22 of the 20th
/// from 02:00 + 12 of the 21st before noon) and 12 calibration rows from noon; the sides at t 34
/// and 12 alike; the meta 18 and 6.
#[rstest]
fn a_train_with_a_meta_reports_the_rows_of_its_sides_out_of_fold_pair_and_meta() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(27),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, with_meta_section(META)).unwrap();

    let stdout = trained(&config_path, catalog.path(), &models, t());

    let report = report(&stdout, t());
    assert!(report[0].contains("published the model set"), "{report:#?}");
    assert_run_figures(&report);
    assert_figures(
        &report,
        &[
            "  Dataset days: 3 built, 0 cached",
            "  Rows of the sides: 34 fit, 12 calibration, min_rows 1",
            "  Rows of the out-of-fold pair: 34 fit, 12 calibration, min_rows 1",
            "  Rows of the meta: 18 fit, 6 calibration, min_rows 1",
        ],
    );
    assert_no_performance(&report);
}

/// A failed train reports the figures it reached and its reason, on the meta's catalog. With the
/// sides' fit slice short of a `min_rows` of 35, it built its days and counted the sides' slices,
/// 34 and 12. With the meta's fit slice short of a meta `min_rows` of 19, it counted the pair's
/// slices and the meta's, 18 and 6, and fitted no sides, the meta training first.
#[rstest]
#[case::sides(
    of_min_rows(35),
    "34 rows in the fit slice",
    &["  Dataset days: 2 built, 0 cached", "  Rows of the sides: 34 fit, 12 calibration, min_rows 35"],
)]
#[case::meta(
    with_meta_section(&META.replace("min_rows = 1", "min_rows = 19")),
    "18 rows in the fit slice",
    &[
        "  Dataset days: 3 built, 0 cached",
        "  Rows of the out-of-fold pair: 34 fit, 12 calibration, min_rows 1",
        "  Rows of the meta: 18 fit, 6 calibration, min_rows 19",
    ],
)]
fn a_failed_train_reports_the_figures_it_reached_and_its_reason(
    #[case] config: String,
    #[case] reason: &str,
    #[case] lines: &[&str],
) {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(27),
        t() + SignedDuration::from_hours(4),
    );
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, &config).unwrap();

    let output = train(&config_path, catalog.path(), &models, t());

    let (stdout, stderr) = (
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
    );
    assert!(!output.status.success(), "{stdout}\n{stderr}");
    let report = report(&stdout, t());
    assert!(
        report[0].contains("failed") && report[0].contains(reason),
        "{report:#?}"
    );
    assert!(!report[0].contains("published"), "{report:#?}");
    assert_run_figures(&report);
    assert_figures(&report, lines);
    assert!(stderr.contains(reason), "{stderr}");
    assert_eq!(model_sets(&config, &models), []);
}

/// A backtest from the valid-from, midnight of the 23rd, warms its features on its own 1-minute
/// bars: it has none at 00:00 and decides first at 01:00, its slow average of 30 bars initialized
/// at 00:30. 01:00 and 02:00 are each a long and its close.
#[rstest]
fn a_backtest_from_the_valid_from_first_decides_once_its_features_are_initialized() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    trained(&config_path, catalog.path(), &models, t());

    assert_eq!(traded_from(&catalog, models, t()), (4, 2));
}

fn traded(catalog: &TempDir, models: PathBuf) -> (usize, usize) {
    traded_from(catalog, models, t() - SignedDuration::from_hours(2))
}

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

/// The arguments of `train` but `--at`, as a live node starts it (`LiveArgs::train_args`).
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

/// A live node's retrain fails as `train` does: the fit slice at t, 34 rows, is short of a
/// `min_rows` of 35. The reason names the slice, its count and the minimum, and nothing trades.
#[rstest]
fn a_live_retrain_through_the_train_subcommand_failing_fails_with_its_reason() {
    let catalog = catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, of_min_rows(35)).unwrap();
    let launcher = Launcher::process(
        env!("CARGO_BIN_EXE_helix-trader").into(),
        live_train_args(&config_path, catalog.path(), &models),
    );

    let retrained = launcher.launch(t()).wait();

    let Retrained::Failed(why) = retrained else {
        panic!("{retrained:?}");
    };
    assert_too_few_rows(&why, "fit slice", 34, 35);
    assert_eq!(traded(&catalog, models), (0, 0));
}

/// Adds `actor` at `now` as a node's trader does: on a clock of its own, kept in Nautilus'
/// registries, then started. Returns its id.
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

/// A live node started at 00:30 on the 23rd, after the retrain time t, with an empty store: its
/// retrain actor trains t at start through the launcher's `train`, on a catalog with quotes to
/// 04:00. Its model set equals the one `train --at t` publishes into another store on the same
/// catalog: valid from t, the same models, calibration, baselines and probes, cut at t.
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

    // On a runtime of its own: the fixtures start theirs
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(wait_until_async(
            || {
                let published = model_sets(CONFIG, &live) == [t()];
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

/// From 21:00 of the 22nd, two hours before the first replay (warmup), to 03:00 of the 25th.
fn backtest_catalog() -> TempDir {
    walk(
        t() - SignedDuration::from_hours(3),
        t() + SignedDuration::from_hours(51),
    )
}

/// With a model published at each retrain time, a backtest runs at its own commission, balance and
/// latency, its simulated account's alone: its models are the config's (`[model] dataset`), whose
/// costs it reports, then each period's scores of its model.
#[rstest]
fn the_binarys_backtest_reports_its_own_costs_and_its_models_then_each_periods_scores() {
    let catalog = backtest_catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));
    let next = t() + SignedDuration::from_hours(24);
    for at in [t(), next] {
        publish(&models, at);
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

/// The store holds a model set at t and one at the next retrain time whose long has another
/// fingerprint, failing its load check: the backtest logs the error naming the set and the check
/// at that period's decisions and trades nothing there, so its scores have no model set and no row.
#[rstest]
fn the_binarys_backtest_stops_new_entries_on_a_model_set_failing_a_check_naming_it() {
    let catalog = backtest_catalog();
    let dir = tempfile::tempdir().unwrap();
    let (config_path, models) = (dir.path().join("model_ema.toml"), dir.path().join("models"));
    std::fs::write(&config_path, CONFIG).unwrap();
    let (start, end) = (t(), t() + SignedDuration::from_hours(36));
    let next = t() + SignedDuration::from_hours(24);
    publish(&models, t());
    publish_with(&models, next, |long| {
        long.manifest.fingerprint = "f".repeat(64);
    });

    let costs = ["--commission-rate", "0.00003", "--balance", "10000000 USD"];
    let (stdout, stderr) = backtest_at(&config_path, catalog.path(), &models, (start, end), &costs);

    let output = format!("{stdout}\n{stderr}");
    let refusal = format!("no new entry until the model set valid from {next} loads");
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

/// Runs `subcommand` of the strategy in `config`'s directory with no environment, so that neither
/// the shell nor a `.env` above the crate's root supplies a venue credential; it fails.
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

/// A config declaring the holdout or its minimums is refused at load naming the key, as is one
/// without `min_rows`, with fewer than one row, or whose calibration and the target's limit of 30
/// minutes leave no fit slice in the window (46 hours for the sides, 24 for the meta). `train`
/// refuses it before any replay: no store is made.
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

fn eur_config() -> String {
    let (usd, eur) = (r#"balance = "10000000 USD""#, r#"balance = "10000000 EUR""#);
    assert!(CONFIG.contains(usd));
    CONFIG.replace(usd, eur)
}

/// A dataset balance in EUR, where IC's gold's P&L is in USD, would leave the targets and a live
/// node's payoff features unconverted: a backtest refuses it before building any dataset day,
/// `train` before replaying any, a live start before any node, its staging untouched, each naming
/// the balance and both currencies.
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

fn converting_config(balance: &str) -> String {
    let (risk, usd) = (r#"risk = "0.5%""#, r#"balance = "10000000 USD""#);
    assert!(CONFIG.contains(risk) && CONFIG.contains(usd));
    let conversions = format!("{risk}\n    conversions = [\"{}\"]", ic::EURUSD);
    CONFIG
        .replace(risk, &conversions)
        .replace(usd, &format!("balance = \"{balance}\""))
}

/// `walk`'s catalog of IC's gold over `[from, to)` with IC's EURUSD at 1.24990 / 1.25010 every
/// minute.
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

/// A dataset balance in JPY, which EURUSD, the pair the config converts through, does not reach USD
/// from: `live`, `backtest` and `train` each refuse it before any node, by a backtest's balance
/// check, naming the balance, both currencies and the pair as the catalog defines it.
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

/// `train` builds the dataset at the balance converted at each row's rate.
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
        report(&stdout, t())[0].contains("published the model set"),
        "{stdout}"
    );
}

/// `subcommand`'s arguments beyond config, catalog and store: a window and USD balance for
/// `backtest`, its time for `train`, a staging directory under `dir` for `live`.
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

/// A live node on a catalog without the traded IC's gold is refused at start, before any node is
/// built. The error names `--catalog` and the instrument, not the dataset balance.
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
