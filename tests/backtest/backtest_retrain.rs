//! A backtest retraining on its schedule (`model::walk`), on a fixture
//! catalog: the probe of `common::dataset` decides every hour on one
//! feature, missing (NaN) from the first quote. Before the run, the
//! dataset is built and, at each daily retrain time of the span, a long
//! model published as a train would: a baseline of its own R, its
//! calibration leaving it as it is (`common::dataset::baseline_of`), which
//! names it. The walk has no edge for a train to earn on, so its trains
//! would publish none (`backtest_train` trains on one). In the run, every
//! decision of a period trades on the model published at its start. A
//! second backtest of the span builds and publishes nothing, and the scores
//! of each period are its model's, a constant, beside its own baseline, the
//! training mean its manifest has.
//! The build and the run are on the test's one thread, a node after
//! another, each disposed before the next, as the binary's backtest runs
//! them.

use std::{path::Path, rc::Rc};

use crate::common::{
    dataset::{baseline_of, blueprint, config, missing, replay_on, trade_on, venue, walk},
    ic,
};
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
use tempfile::TempDir;

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
/// the dataset's days, from the window of the first retrain, to the
/// span's end plus the target's limit.
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

/// The store of the long models under `models`.
fn long_store(models: &Path) -> ModelStore {
    blueprint(features(), config_daily())
        .model_set(models, &venue())
        .unwrap()
        .side(Side::Long)
        .unwrap()
        .store()
        .clone()
}

fn walk_forward(catalog: &TempDir, models: &Path) -> WalkForward {
    blueprint(features(), config_daily())
        .walk_forward(catalog.path(), models, &venue())
        .unwrap()
}

/// Prepares the backtest of the span, its build a node on this thread, as
/// the binary's backtest runs it; the spans built and the times trained
/// at, each publishing a baseline
/// predicting an R of `r(tenths)`, `tenths` 3 at the span's start and one
/// more each day.
fn prepare(catalog: &TempDir, models: &Path) -> (Vec<(Timestamp, Timestamp)>, Vec<Timestamp>) {
    let (start, end) = span();
    let (mut built, mut trained) = (vec![], vec![]);
    walk_forward(catalog, models)
        .prepare(
            start,
            end,
            |from, to| {
                built.push((from, to));
                let dataset =
                    blueprint(features(), config_daily()).dataset(catalog.path(), &venue())?;
                let (first, last) = dataset::days(from, to);
                dataset.build(first, last, |from, to| {
                    replay_on(catalog, features(), from, to)
                })?;
                Ok(())
            },
            |at| {
                trained.push(at);
                let tenths = 3 + (at.duration_since(start).as_hours() / 24) as usize;
                long_store(models).publish(&baseline_of(
                    &features(),
                    &config_daily(),
                    Side::Long,
                    at,
                    tenths,
                ))
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
    let store = long_store(models.path());
    let means: Vec<f64> = retrains.iter().map(|&at| predicts(&store, at)).collect();
    assert!(
        means[0] != means[1] && means[1] != means[2] && means[0] != means[2],
        "the models must differ to be told apart: {means:?}"
    );

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

    assert_eq!(prepare(&catalog, models.path()), (vec![], vec![]));
}

/// The R the long model of `store` valid from `at` predicts on the missing
/// feature, then the bracket's payoff features, which the baseline does not
/// read: the same on every row, its calibration, of the strategy's one side,
/// leaving it as it is.
fn predicts(store: &ModelStore, at: Timestamp) -> f64 {
    let stored = store.read(at).unwrap();
    stored
        .manifest
        .learner
        .load(&stored.model)
        .unwrap()
        .predict(&[f64::NAN, 0.0, 0.0, 0.0])
        .unwrap()
}

/// The model of each period expects one R on every decision of the period,
/// its constant, which its own baseline, the training mean its manifest has
/// (`common::dataset::baseline_of`), is too: -0.2, -0.1 and nought from the
/// first period on (`prepare`). On the same rows, the model errs as its
/// baseline does, a skill of nought, its expectation never varies, no
/// slope, and its mean R - E' is the mean R of the period's longs, read from
/// the dataset, less its constant.
#[test]
fn each_period_scores_its_model_beside_that_models_own_baseline() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    prepare(&catalog, models.path());

    let scores = walk_forward(&catalog, models.path())
        .scores(start, end)
        .unwrap();

    let store = long_store(models.path());
    let rows = blueprint(features(), config_daily())
        .dataset(catalog.path(), &venue())
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
        let realised: Vec<f64> = rows
            .iter()
            .filter(|row| from_ns <= row.ts_event && row.ts_event < to_ns)
            .map(|row| row.long_pnl / row.long_unit)
            .collect();
        let mean = realised.iter().sum::<f64>() / realised.len() as f64;
        assert_eq!(period.models, [from]);
        assert!(!realised.is_empty(), "{period:?}");
        let score = period.score;
        assert_eq!(score.rows, realised.len());
        assert_eq!((score.skill, score.slope), (Some(0.0), None), "{period:?}");
        let error = score.mean_error.unwrap() - (mean - predicts(&store, from));
        assert!(error.abs() < 1e-12, "{period:?}: {mean}");
    }
    let rows: usize = long.periods.iter().map(|period| period.score.rows).sum();
    assert_eq!(long.score.rows, rows);
}
