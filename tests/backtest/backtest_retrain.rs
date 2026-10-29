//! A backtest retraining on its schedule (`model::walk`), on a fixture
//! catalog: the probe of `common::dataset` decides every hour on one
//! feature, missing (NaN) from the first quote, so the fake learner
//! predicts the mean of its training labels: each model's probability
//! names it. Before the run, the dataset is built and a model trained at
//! each daily retrain time of the span; in the run, every decision of a
//! period trades on the model trained at its start. A second backtest of
//! the span builds and trains nothing, and the scores of each period are
//! its model's, whose prediction, the training mean, is the baseline's.
//! One node per thread: each build, train and run on a thread of its own.

use std::{path::Path, rc::Rc};

use crate::common::{
    dataset::{Missing, STRATEGY, config, replay_on, rule, targets, trade_on, walk},
    ic,
};
use helix_trader::model::{
    config::{ModelConfig, Thresholds},
    dataset::{self, Dataset},
    features::FeatureSet,
    learner::LearnerConfig,
    store::ModelStore,
    train::Training,
    walk::WalkForward,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{InstrumentId, StrategyId};
use tempfile::TempDir;

fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(
            InstrumentId::from(ic::INSTRUMENT_ID),
            "missing",
            Missing::default(),
        )
        .build()
        .unwrap()
}

/// The probe's, retrained every day; every probability a trade, to record
/// it.
fn config_daily() -> ModelConfig {
    ModelConfig {
        retrain: SignedDuration::from_hours(24),
        thresholds: Thresholds {
            long: f64::MIN_POSITIVE,
            short: 0.0,
        },
        ..config()
    }
}

/// Quotes from 23:00 of the 20th of October 2026 to 03:00 of the 26th:
/// the dataset's days, from the window of the first retrain, to the
/// span's end plus the longest target.
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

fn walk_forward(catalog: &TempDir, models: &Path) -> WalkForward {
    WalkForward::new(
        catalog.path(),
        models,
        StrategyId::from(STRATEGY),
        &features(),
        config_daily(),
    )
    .unwrap()
}

/// Prepares the backtest of the span, each build and train on a thread of
/// its own, as a backtest's children each run a node of their own; the
/// spans built and the times trained at.
fn prepare(catalog: &TempDir, models: &Path) -> (Vec<(Timestamp, Timestamp)>, Vec<Timestamp>) {
    let (start, end) = span();
    let (mut built, mut trained) = (vec![], vec![]);
    walk_forward(catalog, models)
        .prepare(
            start,
            end,
            |from, to| {
                built.push((from, to));
                std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            let dataset = Dataset::new(
                                catalog.path(),
                                StrategyId::from(STRATEGY),
                                &features(),
                                rule(),
                                targets(),
                            )?;
                            let (first, last) = dataset::days(from, to);
                            dataset.build(first, last, |from, to| {
                                replay_on(catalog, features(), from, to)
                            })?;
                            Ok(())
                        })
                        .join()
                        .unwrap()
                })
            },
            |at| {
                trained.push(at);
                let outcome = std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            Training::new(
                                catalog.path(),
                                models,
                                StrategyId::from(STRATEGY),
                                &features(),
                                config_daily(),
                            )
                            .unwrap()
                            .train_at(at, |from, to| replay_on(catalog, features(), from, to))
                        })
                        .join()
                        .unwrap()
                });
                assert!(outcome.published(), "{outcome:?}");
                Ok(())
            },
        )
        .unwrap();
    (built, trained)
}

#[test]
fn each_period_trades_on_the_model_trained_at_its_start_and_a_second_backtest_trains_nothing() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    let day = SignedDuration::from_hours(24);
    let retrains = [start, start + day, start + day * 2];

    let (built, trained) = prepare(&catalog, models.path());

    // The window of the first retrain, 46 hours, starts at 02:00 of the
    // 21st.
    assert_eq!(built, [(start - SignedDuration::from_hours(46), end)]);
    assert_eq!(trained, retrains);
    let store = ModelStore::new(
        models.path(),
        StrategyId::from(STRATEGY),
        &features(),
        &config_daily(),
    )
    .unwrap();
    let means: Vec<f64> = retrains
        .iter()
        .map(|&at| {
            let stored = store.read(at).unwrap();
            let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
            model.predict(&[f64::NAN]).unwrap()
        })
        .collect();
    assert!(
        means[0] != means[1] && means[1] != means[2] && means[0] != means[2],
        "the models must differ to be told apart: {means:?}"
    );

    let record = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                trade_on(
                    &catalog,
                    features(),
                    start,
                    end,
                    &config_daily(),
                    models.path(),
                )
            })
            .join()
            .unwrap()
    });

    for (period, (&from, mean)) in retrains.iter().zip(&means).enumerate() {
        let (from, to) = (UnixNanos::from(from), UnixNanos::from(from + day));
        let probabilities: Vec<f64> = record
            .trades
            .iter()
            .filter(|(time, _)| from <= *time && *time < to)
            .map(|(_, trade)| trade.probability)
            .collect();
        assert_eq!(probabilities.len(), 24, "period {period}");
        assert!(
            probabilities.iter().all(|p| p == mean),
            "period {period}: {probabilities:?}, not {mean}"
        );
    }
    assert_eq!(record.trades.len(), 72);

    assert_eq!(prepare(&catalog, models.path()), (vec![], vec![]));
}

/// The fake's prediction on a missing feature is the mean of its fitted
/// labels, as the baseline's: the model of each period scores as the
/// baseline fitted at its start, on every decision of the span.
#[test]
fn each_period_scores_its_model_beside_the_baseline_fitted_at_its_start() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    prepare(&catalog, models.path());

    let scores = walk_forward(&catalog, models.path())
        .scores(start, end)
        .unwrap();

    assert_eq!(scores.periods.len(), 3);
    for (period, day) in scores.periods.iter().zip(0..) {
        let from = start + SignedDuration::from_hours(24 * day);
        assert_eq!(period.models, [from]);
        assert!(period.model.rows > 0, "{period:?}");
        assert_eq!(period.model, period.baseline, "{period:?}");
        assert_eq!(period.model.auc, Some(0.5), "{period:?}");
    }
    assert_eq!(scores.model, scores.baseline);
    let rows: usize = scores.periods.iter().map(|period| period.model.rows).sum();
    assert_eq!(scores.model.rows, rows);
}
