//! Training at a time (`model::train`) on a fixture catalog: the dataset
//! probe (`common::dataset`) replays the days the window lacks through
//! `Backtest::run_dataset`, and the fake learner fits on the rows. Its
//! prediction is the first feature, the mid of IC's gold, clamped to 1:
//! every row ties, so the holdout AUC is 0.5 exactly, which a minimum
//! score of 0.5 publishes and one of 0.6 does not. One node per thread: a
//! test training twice trains the first time on a thread of its own.

use std::path::Path;

use crate::common::{
    dataset::{STRATEGY, commission, features, replay, rule, targets, walk},
    ic,
};
use helix_trader::model::{
    config::{ModelConfig, Thresholds},
    learner::LearnerConfig,
    targets::Label,
    train::{Kept, Outcome, Training},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::identifiers::StrategyId;
use tempfile::TempDir;

/// The probe's decisions, learning whether the bracket's long gains, on a
/// window of 46 hours, the last 12 held out.
fn config(min_score: f64) -> ModelConfig {
    ModelConfig::builder()
        .decision(rule())
        .targets(targets())
        .label(Label::Barrier)
        .retrain(SignedDuration::from_hours(24))
        .window(SignedDuration::from_hours(46))
        .holdout(SignedDuration::from_hours(12))
        .thresholds(Thresholds {
            long: 0.6,
            short: 0.4,
        })
        .min_score(min_score)
        .learner(LearnerConfig::Fake {})
        .build()
}

fn training(catalog: &TempDir, models: &Path, min_score: f64) -> Training {
    Training::new(
        catalog.path(),
        models,
        StrategyId::from(STRATEGY),
        &features(),
        config(min_score),
        &commission(),
    )
    .unwrap()
}

/// Quotes from 23:00 of the 20th of October 2026 to 03:00 of the 23rd.
fn catalog() -> TempDir {
    walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(51),
    )
}

/// Midnight of the 23rd: the window starts at 02:00 of the 21st.
fn t() -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(48)
}

#[test]
fn train_at_t_publishes_a_model_whose_probes_reload_to_identical_predictions() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let training = training(&catalog, models.path(), 0.5);

    let outcome = training.train_at(t(), |from, to| replay(&catalog, from, to));

    assert_eq!(
        outcome,
        Outcome::Published {
            valid_from: t(),
            score: 0.5
        }
        .into()
    );
    let stored = training.store().read(t()).unwrap();
    let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
    assert!(!stored.manifest.probes.is_empty());
    for probe in &stored.manifest.probes {
        assert_eq!(
            model.predict(&probe.row).unwrap().to_bits(),
            probe.prediction.to_bits(),
            "{probe:?}"
        );
    }
}

#[test]
fn a_gate_above_the_achievable_score_keeps_the_current_model() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let first = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                training(&catalog, models.path(), 0.5)
                    .train_at(t(), |from, to| replay(&catalog, from, to))
            })
            .join()
            .unwrap()
    });
    assert!(first.published(), "{first:?}");
    // A minimum score keys the store: the gated one holds the model as if
    // it had published it.
    let current = training(&catalog, models.path(), 0.5)
        .store()
        .read(t())
        .unwrap();
    let gated = training(&catalog, models.path(), 0.6);
    gated.store().publish(&current).unwrap();

    let outcome = gated.train_at(t() + SignedDuration::from_hours(1), |from, to| {
        replay(&catalog, from, to)
    });

    assert_eq!(
        outcome,
        Outcome::Kept(Kept::BelowMinScore {
            score: 0.5,
            min_score: 0.6
        })
        .into()
    );
    assert_eq!(gated.store().valid_froms().unwrap(), [t()]);
    assert_eq!(gated.store().read(t()).unwrap(), current);
}
