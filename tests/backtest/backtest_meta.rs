//! Meta-labeling on a fixture catalog (`model::meta`), the features a
//! missing one (`common::dataset::Missing`).
//! Over a span of three days retrained every midnight, the dataset is
//! built and the models trained; the fixture's walk has no edge, so a
//! train refuses each model, and the models traded on are published
//! straight, baselines whose predictions name them
//! (`common::dataset::baseline_of`). In the run, a decision trades only if
//! the meta model in use takes the trade of the side picked, on the
//! decision's level and side call.
//!
//! A strategy of the long alone trades long at every decision. Its
//! meta model's first retrain has no side's model before it, so it waits
//! for the sides' decisions; the next ones hold out the longs, every one
//! of which the meta's margin takes, and refuse it, on its skill or for
//! its trades, every trade, earning no more than every trade. With no meta
//! model the first day, no trade.
//!
//! One node per thread: each build, train and run on a thread of its own.

use std::{path::Path, rc::Rc};

use crate::common::{
    dataset::{
        baseline_of, blueprint, config, meta_baseline_of, missing, replay_on, trade_on, venue, walk,
    },
    ic,
};
use helix_trader::model::{
    config::{Margins, MetaConfig, ModelConfig},
    dataset,
    features::FeatureSet,
    handle::Decision,
    holdout::Earnings,
    learner::LearnerConfig,
    schedule::Retrain,
    store::{ModelSet, ModelStore},
    targets::Side,
    train::{Kept, Outcome, Trained},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::enums::OrderSide;
use tempfile::TempDir;

/// The missing feature.
fn features() -> Rc<FeatureSet> {
    missing().build().unwrap()
}

/// A meta model on 46 hours, the last 6 held out and the 6 before them
/// calibrated on, the fake learner, published from nought per trade over
/// a trade, taking every trade, to record its expected R.
fn meta() -> MetaConfig {
    MetaConfig::builder()
        .window(SignedDuration::from_hours(46))
        .holdout(SignedDuration::from_hours(6))
        .margin(f64::MIN)
        .min_pnl(0.0)
        .min_trades(1)
        .learner(LearnerConfig::Fake {})
        .build()
}

/// The probe's model of the long alone, retrained every day, a long at
/// every decision, its trades taken by `meta`.
fn long_only() -> ModelConfig {
    ModelConfig {
        retrain: Retrain::Every(SignedDuration::from_hours(24)),
        margins: Margins {
            long: Some(f64::MIN),
            short: None,
        },
        meta: Some(meta()),
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

fn day() -> SignedDuration {
    SignedDuration::from_hours(24)
}

/// Runs `run` on a thread of its own, a node's.
fn on_thread<T: Send>(run: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| scope.spawn(run).join().unwrap())
}

/// The R the long model of `store` valid from `at` predicts on `row`, its
/// calibration, of the strategy's one side, leaving it as it is: the
/// decision's level.
fn expects(store: &ModelStore, at: Timestamp, row: &[f64]) -> f64 {
    let stored = store.read(at).unwrap();
    stored
        .manifest
        .learner
        .load(&stored.model)
        .unwrap()
        .predict(row)
        .unwrap()
}

/// The calibrated expected R of the meta model of `store` valid from `at`
/// on `row`, as a strategy of `features` loads it.
fn predicts(store: &ModelStore, features: Rc<FeatureSet>, at: Timestamp, row: &[f64]) -> f64 {
    let stored = store.read(at).unwrap();
    let fingerprint = blueprint(features, long_only())
        .fingerprint(&venue())
        .unwrap()
        .to_string();
    stored
        .load(&stored.manifest.feature_names, &fingerprint)
        .unwrap()
        .predict(row)
        .unwrap()
}

/// Prepares the backtest of the span of `long_only`: the dataset built,
/// then at each retrain time the long and its meta model trained, and
/// the long model published straight after, a baseline predicting an R of
/// `r(tenths)` (`tenths` 3 at the span's start and one more each day),
/// which the train would keep in use had it been published before; what
/// each train did.
fn prepare_model(catalog: &TempDir, models: &Path) -> Vec<(Timestamp, Trained)> {
    let (start, end) = span();
    let mut trained = vec![];
    blueprint(features(), long_only())
        .walk_forward(catalog.path(), models, &venue())
        .unwrap()
        .prepare(
            start,
            end,
            |from, to| {
                on_thread(|| {
                    let dataset =
                        blueprint(features(), long_only()).dataset(catalog.path(), &venue())?;
                    let (first, last) = dataset::days(from, to);
                    dataset.build(first, last, |from, to| {
                        replay_on(catalog, features(), from, to)
                    })?;
                    Ok(())
                })
            },
            |at| {
                let outcome = on_thread(|| {
                    blueprint(features(), long_only())
                        .training(catalog.path(), models, &venue())
                        .unwrap()
                        .train_at(at, |from, to| replay_on(catalog, features(), from, to))
                });
                trained.push((at, outcome));
                let tenths = 3 + (at.duration_since(start).as_hours() / 24) as usize;
                long_store(models).publish(&baseline_of(
                    &features(),
                    &long_only(),
                    Side::Long,
                    at,
                    tenths,
                ))
            },
        )
        .unwrap();
    trained
}

/// The models of `long_only` under `models`.
fn model_set(models: &Path) -> ModelSet {
    blueprint(features(), long_only())
        .model_set(models, &venue())
        .unwrap()
}

/// The store of the long models of `long_only` under `models`.
fn long_store(models: &Path) -> ModelStore {
    model_set(models).side(Side::Long).unwrap().store().clone()
}

/// The store of the meta models of `long_only` under `models`.
fn meta_store(models: &Path) -> ModelStore {
    model_set(models).meta().unwrap().store().clone()
}

/// The fixture has no edge: a train refuses each model it fits, the
/// long's and, from the second retrain, when the long has a model
/// before it, the meta's, whose margin takes every trade of its
/// holdout, the longs, refused on its skill or for earning no more than
/// every trade.
/// With meta models published straight, a day's decisions trade only as
/// the meta model in use takes the trade of the side picked.
#[test]
fn a_meta_model_trains_on_the_sides_trades_and_takes_them_in_the_run() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    let retrains = [start, start + day(), start + day() * 2];

    let trained = prepare_model(&catalog, models.path());

    let times: Vec<Timestamp> = trained.iter().map(|(at, _)| *at).collect();
    assert_eq!(times, retrains);
    assert!(
        trained
            .iter()
            .all(|(_, trained)| matches!(trained.long, Some(Outcome::Kept(Kept::Refused { .. })))),
        "{trained:?}"
    );
    assert_eq!(
        trained[0].1.meta,
        Some(Outcome::Kept(Kept::Waiting)),
        "no side's model before the first retrain: {:?}",
        trained[0].1
    );
    for (at, trained) in &trained[1..] {
        let Some(Outcome::Kept(Kept::Refused { holdout, .. })) = &trained.meta else {
            panic!("{at}: {trained:?}");
        };
        let Earnings { taken, every, .. } = holdout.earnings;
        assert!(every.count > 0, "{at}: {holdout:?}");
        assert_eq!(taken, every, "{at}: every trade taken");
    }
    let long = long_store(models.path());
    let meta = meta_store(models.path());
    assert!(meta.valid_froms().unwrap().is_empty());
    let means: Vec<f64> = retrains
        .iter()
        // The long's row: the feature missing, then the bracket's three payoff
        // features, which the fake does not read.
        .map(|&at| expects(&long, at, &[f64::NAN, 0.0, 0.0, 0.0]))
        .collect();
    for (&at, tenths) in retrains[1..].iter().zip([6, 8]) {
        meta.publish(&meta_baseline_of(&features(), &long_only(), at, tenths))
            .unwrap();
    }

    let record = on_thread(|| {
        trade_on(
            &catalog,
            features(),
            start,
            end,
            &long_only(),
            models.path(),
        )
    });

    for (period, &from) in retrains.iter().enumerate() {
        let (from_ns, to_ns) = (UnixNanos::from(from), UnixNanos::from(from + day()));
        let trades: Vec<Decision> = record
            .trades
            .iter()
            .filter(|(time, _)| from_ns <= *time && *time < to_ns)
            .map(|(_, trade)| *trade)
            .collect();
        if period == 0 {
            assert!(
                trades.is_empty(),
                "no meta model on the first day: {trades:?}"
            );
            continue;
        }
        // The meta's row: the feature missing, the long picked, the
        // decision's level and no side call.
        let taken = predicts(
            &meta,
            features(),
            from,
            &[f64::NAN, 1.0, means[period], 0.0],
        );
        assert_eq!(trades.len(), 24, "period {period}");
        assert!(
            trades.iter().all(|trade| trade.side == OrderSide::Buy
                && trade.level == means[period]
                && trade.meta_expected_r == Some(taken)),
            "period {period}: {trades:?}, not longs at {taken}"
        );
    }
}
