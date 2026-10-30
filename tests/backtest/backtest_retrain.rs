//! A backtest retraining on its schedule (`model::walk`), on a fixture
//! catalog: the probe of `common::dataset` decides every hour on one
//! feature, missing (NaN) from the first quote. Before the run, the
//! dataset is built and, at each daily retrain time of the span, a long
//! model published as a train would: a baseline of its own probability,
//! calibrated (`common::dataset::baseline_of`), which names it. The walk
//! has no edge for a train to earn on, so its trains would publish none
//! (`backtest_train` trains on one). In the run, every decision of a
//! period trades on the model published at its start. A second backtest
//! of the span builds and publishes nothing, and the scores of each period
//! are its model's, a constant ranking no row above another, as its
//! baseline, the training mean.
//! One node per thread: each build, train and run on a thread of its own.

use std::{path::Path, rc::Rc};

use crate::common::{
    dataset::{
        Missing, STRATEGY, baseline_of, commission, config, replay_on, requirements, rule, targets,
        trade_on, walk,
    },
    ic,
};
use helix_trader::model::{
    config::{Margins, ModelConfig},
    dataset::{self, Dataset},
    features::FeatureSet,
    store::ModelStore,
    targets::{Label, Side},
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

/// The probe's, of the long alone, retrained every day; every decision a
/// long, to record its probability.
fn config_daily() -> ModelConfig {
    ModelConfig {
        retrain: SignedDuration::from_hours(24),
        margins: Margins {
            long: Some(f64::MIN),
            short: None,
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

/// The store of the long models under `models`.
fn long_store(models: &Path) -> ModelStore {
    ModelStore::new(
        models,
        StrategyId::from(STRATEGY),
        &features(),
        &config_daily(),
        &requirements(),
        &commission(),
        Side::Long,
    )
    .unwrap()
}

fn walk_forward(catalog: &TempDir, models: &Path) -> WalkForward {
    WalkForward::new(
        catalog.path(),
        models,
        StrategyId::from(STRATEGY),
        &features(),
        config_daily(),
        &requirements(),
        &commission(),
    )
    .unwrap()
}

/// Prepares the backtest of the span, each build and train on a thread of
/// its own, as a backtest's children each run a node of their own; the
/// spans built and the times trained at, each publishing a baseline of
/// `trues` tenths, 3 at the span's start and one more each day.
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
                                &requirements(),
                                &commission(),
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
                let trues = 3 + (at.duration_since(start).as_hours() / 24) as usize;
                long_store(models).publish(&baseline_of(
                    &features(),
                    &config_daily(),
                    Side::Long,
                    at,
                    trues,
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

/// The calibrated probability of the long model of `store` valid from
/// `at` on the missing feature: the same on every row.
fn predicts(store: &ModelStore, at: Timestamp) -> f64 {
    let fingerprint = dataset::fingerprint(
        &features(),
        rule(),
        &targets(),
        &requirements(),
        &commission(),
    )
    .unwrap()
    .to_string();
    store
        .read(at)
        .unwrap()
        .load(features().names(), &fingerprint)
        .unwrap()
        .predict(&[f64::NAN])
        .unwrap()
}

/// The model of each period predicts one probability on every decision of
/// the period, its calibrated constant, as the baseline fitted at its
/// start predicts the training mean: neither ranks a row above another
/// (AUC 0.5), and the model's log loss is that of its constant against the
/// longs' outcomes of the period, read from the dataset.
#[test]
fn each_period_scores_its_model_beside_the_baseline_fitted_at_its_start() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    prepare(&catalog, models.path());

    let scores = walk_forward(&catalog, models.path())
        .scores(start, end)
        .unwrap();

    let store = long_store(models.path());
    let rows = Dataset::new(
        catalog.path(),
        StrategyId::from(STRATEGY),
        &features(),
        rule(),
        targets(),
        &requirements(),
        &commission(),
    )
    .unwrap()
    .read(start, end)
    .unwrap();
    let pnl = targets().pnl_index(Label::Barrier, Side::Long).unwrap();
    assert_eq!(scores.short, None);
    let long = scores.long.unwrap();
    assert_eq!(long.periods.len(), 3);
    for (period, day) in long.periods.iter().zip(0..) {
        let from = start + SignedDuration::from_hours(24 * day);
        let (from_ns, to_ns) = (
            UnixNanos::from(from),
            UnixNanos::from(from + SignedDuration::from_hours(24)),
        );
        let gained: Vec<bool> = rows
            .iter()
            .filter(|row| from_ns <= row.ts_event && row.ts_event < to_ns)
            .map(|row| row.targets[pnl] > 0.0)
            .collect();
        let p = predicts(&store, from);
        let loss = -gained
            .iter()
            .map(|&gained| if gained { p.ln() } else { (1.0 - p).ln() })
            .sum::<f64>()
            / gained.len() as f64;
        assert_eq!(period.models, [from]);
        assert!(!gained.is_empty(), "{period:?}");
        assert_eq!(
            (period.model.rows, period.baseline.rows),
            (gained.len(), gained.len())
        );
        assert_eq!(
            (period.model.auc, period.baseline.auc),
            (Some(0.5), Some(0.5)),
            "{period:?}"
        );
        let log_loss = period.model.log_loss.unwrap();
        assert!((log_loss - loss).abs() < 1e-12, "{log_loss} != {loss}");
    }
    let rows: usize = long.periods.iter().map(|period| period.model.rows).sum();
    assert_eq!(long.model.rows, rows);
}
