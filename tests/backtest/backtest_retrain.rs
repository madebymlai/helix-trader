//! A backtest of a strategy retrained on its schedule (`model::walk`), on a
//! fixture catalog: the probe of `common::dataset` decides every hour on
//! one feature, missing (NaN) from the first quote. Before the run, the
//! dataset is built over the span and, at each daily retrain time of it, a
//! model set published as a train would, its long a baseline of its own R,
//! its calibration leaving it as it is (`common::dataset::baseline_of`),
//! which names it. The walk has no edge for a train to earn on, so its trains
//! would publish none (`backtest_train` trains on one). In the run, every
//! decision of a period trades on the model published at its start, and
//! the scores of each period are its model's, a constant, beside its own
//! baseline, the training mean its manifest has.
//! The build and the run are on the test's one thread, a node after
//! another, each disposed before the next, as the binary's backtest runs
//! them.

use std::{path::Path, rc::Rc};

use helix_trader::{
    model::{
        config::{Margins, ModelConfig},
        dataset,
        features::FeatureSet,
        schedule::Retrain,
        store::ModelStore,
        targets::Side,
        walk::WalkForward,
    },
    strategy::risk::R,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use rstest::rstest;
use tempfile::TempDir;

use crate::common::{
    dataset::{baseline_of, blueprint, config, missing, model_set, r, replay_on, trade_on, walk},
    ic,
};

fn features() -> Rc<FeatureSet> {
    missing().build().unwrap()
}

/// The probe's, of the long alone, retrained every day; every decision a
/// long, to record its level.
fn config_daily() -> ModelConfig {
    ModelConfig {
        retrain: Retrain::Every(SignedDuration::from_hours(24)),
        margins: Margins {
            long: Some(R::new(f64::MIN)),
            short: None,
        },
        ..config()
    }
}

/// Quotes from 23:00 of the 20th of October 2026 to 03:00 of the 26th:
/// the span's days, after their warmup, to the span's end plus the
/// target's limit.
fn catalog() -> TempDir {
    walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(123),
    )
}

/// The span: the 23rd to the 25th, retrained at midnight of each.
fn span() -> (Timestamp, Timestamp) {
    let start = ic::at("00:00:00") + SignedDuration::from_hours(48);
    (start, start + SignedDuration::from_hours(72))
}

/// The models of `config_daily` under `models`.
fn model_store(models: &Path) -> ModelStore {
    blueprint(features(), config_daily())
        .model_store(models)
        .unwrap()
}

fn walk_forward(catalog: &TempDir, models: &Path) -> WalkForward {
    blueprint(features(), config_daily())
        .walk_forward(catalog.path(), models)
        .unwrap()
}

/// Builds the dataset over the span, a node on this thread, as the
/// binary's backtest builds it, and publishes at each retrain time of the
/// span a baseline predicting an R of `r(tenths)`, `tenths` 3 at the
/// span's start and one more each day.
fn trained(catalog: &TempDir, models: &Path) {
    let (start, end) = span();
    let dataset = blueprint(features(), config_daily())
        .dataset(catalog.path())
        .unwrap();
    let (first, last) = dataset::days(start, end);
    dataset
        .build(first, last, |from, to| {
            replay_on(catalog, features(), from, to)
        })
        .unwrap();

    for day in 0..3 {
        let at = start + SignedDuration::from_hours(24 * day);
        let long = baseline_of(
            &features(),
            &config_daily(),
            Side::Long,
            3 + usize::try_from(day).unwrap(),
        );
        model_store(models)
            .publish(&model_set(&config_daily(), at, vec![long], None))
            .unwrap();
    }
}

#[rstest]
fn each_period_trades_on_the_model_trained_at_its_start() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    let day = SignedDuration::from_hours(24);
    let retrains = [start, start + day, start + day * 2];

    trained(&catalog, models.path());

    // The baselines published at each retrain (`trained`): -0.2, -0.1, 0.
    let means = [r(3), r(4), r(5)];

    let record = trade_on(
        &catalog,
        features(),
        start,
        end,
        &config_daily(),
        models.path(),
    );

    for (period, (&from, mean)) in retrains.iter().zip(&means).enumerate() {
        let (from, to) = (UnixNanos::from(from), UnixNanos::from(from + day));
        let levels: Vec<f64> = record
            .trades
            .iter()
            .filter(|(time, _)| from <= *time && *time < to)
            .map(|(_, trade)| trade.level)
            .collect();
        assert_eq!(levels.len(), 24, "period {period}");
        assert!(
            levels.iter().all(|level| level == mean),
            "period {period}: {levels:?}, not {mean}"
        );
    }
    assert_eq!(record.trades.len(), 72);
}

/// The model of each period expects one R on every decision of the period,
/// its constant, which its own baseline, the training mean its manifest has
/// (`common::dataset::baseline_of`), is too: -0.2, -0.1 and nought from the
/// first period on (`trained`). On the same rows, the model errs as its
/// baseline does, a skill of nought, its expectation never varies, no
/// slope, and its mean R - E' is the mean R of the period's longs, read from
/// the dataset, less its constant.
#[rstest]
fn each_period_scores_its_model_beside_that_models_own_baseline() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    trained(&catalog, models.path());

    let scores = walk_forward(&catalog, models.path())
        .scores(start, end)
        .unwrap();

    let rows = blueprint(features(), config_daily())
        .dataset(catalog.path())
        .unwrap()
        .read(start, end)
        .unwrap();
    assert_eq!(scores.short, None);
    let long = scores.long.unwrap();
    assert_eq!(long.periods.len(), 3);
    for (period, day) in long.periods.iter().zip(0..) {
        let from = start + SignedDuration::from_hours(24 * day);
        let (from_ns, to_ns) = (
            UnixNanos::from(from),
            UnixNanos::from(from + SignedDuration::from_hours(24)),
        );
        let realized: Vec<f64> = rows
            .iter()
            .filter(|row| from_ns <= row.ts_event && row.ts_event < to_ns)
            .map(|row| row.long_pnl / row.long_unit)
            .collect();
        let mean = realized.iter().sum::<f64>() / realized.len() as f64;
        assert_eq!(period.models, [from]);
        assert!(!realized.is_empty(), "{period:?}");
        let score = period.score;
        assert_eq!(score.rows, realized.len());
        assert_eq!((score.skill, score.slope), (Some(0.0), None), "{period:?}");
        let error = score.mean_error.unwrap() - (mean - r(3 + usize::try_from(day).unwrap()));
        assert!(error.abs() < 1e-12, "{period:?}: {mean}");
    }
    let rows: usize = long.periods.iter().map(|period| period.score.rows).sum();
    assert_eq!(long.score.rows, rows);
}
