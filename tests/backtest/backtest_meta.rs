//! Meta-labeling on a fixture catalog (`model::meta`), the features a
//! missing one (`common::dataset::Missing`).
//! Over a span of three days retrained every midnight, the models are
//! trained at each retrain time, each train publishing what it fits; the
//! models traded on are then published straight in their place, baselines
//! whose predictions name them (`common::dataset::baseline_of`). In the
//! run, a decision trades only if the meta model in use takes the trade of
//! the side picked, on the decision's level and side call.
//!
//! A strategy of the long alone trades long at every decision. Its
//! meta model's first retrain has no side's model before it, so it waits
//! for the sides' decisions; the next ones learn from the longs and
//! publish. With no meta model the first day, no trade.
//!
//! One node per thread: each build, train and run on a thread of its own.

use std::{path::Path, rc::Rc};

use helix_trader::{
    model::{
        config::{Margins, MetaConfig, ModelConfig},
        features::FeatureSet,
        handle::Decision,
        learner::LearnerConfig,
        schedule::Retrain,
        store::{ModelSet, ModelStore},
        targets::Side,
        train::{Kept, Outcome, Trained},
    },
    strategy::risk::R,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::enums::OrderSide;
use rstest::rstest;
use tempfile::TempDir;

use crate::common::{
    dataset::{
        baseline_of, blueprint, config, meta_baseline_of, missing, r, replay_on, trade_on, walk,
    },
    ic,
};

/// The missing feature.
fn features() -> Rc<FeatureSet> {
    missing().build().unwrap()
}

/// A meta model on 46 hours, the last 6 calibrated on, the fake learner,
/// each slice of at least one row, taking every trade, to record its
/// expected R.
fn meta() -> MetaConfig {
    MetaConfig::builder()
        .window(SignedDuration::from_hours(46))
        .calibration(SignedDuration::from_hours(6))
        .margin(R::new(f64::MIN))
        .min_rows(1)
        .learner(LearnerConfig::Fake {})
        .build()
}

/// The probe's model of the long alone, retrained every day, a long at
/// every decision, its trades taken by `meta`.
fn long_only() -> ModelConfig {
    ModelConfig {
        retrain: Retrain::Every(SignedDuration::from_hours(24)),
        margins: Margins {
            long: Some(R::new(f64::MIN)),
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

/// Trains at each retrain time of the span of `long_only` the long and its
/// meta model, as `train` over the span would, each train replaying the
/// days its window lacks, then publishes the long model straight in the
/// place of the one the train published, a baseline predicting an R of
/// `r(tenths)` (`tenths` 3 at the span's start and one more each day); what
/// each train did.
fn train_model(catalog: &TempDir, models: &Path) -> Vec<(Timestamp, Trained)> {
    let (start, end) = span();
    let training = || {
        blueprint(features(), long_only())
            .training(catalog.path(), models)
            .unwrap()
    };
    let mut trained = vec![];

    for at in training().retrain_times(start, end).unwrap() {
        let outcome = on_thread(|| {
            training().train_at(at, |from, to| replay_on(catalog, features(), from, to))
        });
        trained.push((at, outcome));
        let tenths = 3 + (at.duration_since(start).as_hours() / 24) as usize;
        long_store(models)
            .publish(&baseline_of(
                &features(),
                &long_only(),
                Side::Long,
                at,
                tenths,
            ))
            .unwrap();
    }
    trained
}

/// The models of `long_only` under `models`.
fn model_set(models: &Path) -> ModelSet {
    blueprint(features(), long_only())
        .model_set(models)
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

/// Each train publishes the long it fits and, from the second retrain,
/// when the long has a model before it, the meta's; the first meta waits.
/// With meta models published straight in their place, a day's decisions
/// trade only as the meta model in use takes the trade of the side picked:
/// each of the
/// second and third days a long at every decision, at the level of the
/// long's baseline published at its start, r(4) = -0.1 then r(5) = 0, and
/// at the expected R of the meta published then, a baseline of r(6) = 0.1
/// then r(8) = 0.3 through `CALIBRATION`, 0.1 + 0.5 r: 0.15 then 0.25.
#[rstest]
fn a_meta_model_trains_on_the_sides_trades_and_takes_them_in_the_run() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    let retrains = [start, start + day(), start + day() * 2];

    let trained = train_model(&catalog, models.path());

    let times: Vec<Timestamp> = trained.iter().map(|(at, _)| *at).collect();
    assert_eq!(times, retrains);
    for (at, trained) in &trained {
        assert_eq!(
            trained.long,
            Some(Outcome::Published { valid_from: *at }),
            "{trained:?}"
        );
    }
    assert_eq!(
        trained[0].1.meta,
        Some(Outcome::Kept(Kept::Waiting)),
        "no side's model before the first retrain: {:?}",
        trained[0].1
    );
    for (at, trained) in &trained[1..] {
        assert_eq!(
            trained.meta,
            Some(Outcome::Published { valid_from: *at }),
            "{trained:?}"
        );
    }
    let meta = meta_store(models.path());
    assert_eq!(meta.valid_froms().unwrap(), retrains[1..]);
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

    let expected = [(r(4), 0.1 + 0.5 * r(6)), (r(5), 0.1 + 0.5 * r(8))];

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
        let (level, taken) = expected[period - 1];
        assert_eq!(trades.len(), 24, "period {period}");
        assert!(
            trades.iter().all(|trade| trade.side == OrderSide::Buy
                && trade.level == level
                && trade.meta_expected_r == Some(R::new(taken))),
            "period {period}: {trades:?}, not longs at {level} taken at {taken}"
        );
    }
}
