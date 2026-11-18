//! Meta-labeling on a fixture catalog (`model::meta`), the features a
//! missing one (`common::dataset::Missing`).
//! Over a span of three days retrained every midnight, the models are
//! trained at each retrain time, each train publishing its model set; the
//! models traded on are then published straight in their place, baselines
//! whose predictions name them (`common::dataset::baseline_of`). In the
//! run, a decision trades only if the meta model of the model set in use
//! takes the trade of the side picked, on the decision's level and side
//! call.
//!
//! A strategy of the long alone trades long at every decision. Each
//! retrain, its first included, fits an out-of-fold pair, its long alone, a
//! meta window before it, whose longs over that window its meta learns
//! from, and publishes the long it fits at its time beside that meta, so the
//! run trades from its first retrain time. A model set's meta decides beside its own sides
//! alone, and a model set failing its checks takes no trade until it
//! loads.
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
        store::{ModelStore, StoredModelSet},
        targets::Side,
        train::{Outcome, Trained},
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
        baseline_of, blueprint, hourly_trades, meta_baseline_of, missing, model_set, r, replay_of,
        trade_on, unchanged, walk,
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
        ..hourly_trades()
    }
}

/// Quotes from 03:00 of the 19th of October 2026 to 03:00 of the 26th:
/// the dataset's days, from an hour before the window of the first
/// retrain's out-of-fold pair, fitted as of 02:00 of the 21st, the meta's 46 hours
/// before midnight of the 23rd, on its 46 hours back from there, to the
/// span's end plus the target's limit.
fn catalog() -> TempDir {
    walk(
        ic::at("00:00:00") - SignedDuration::from_hours(45),
        ic::at("00:00:00") + SignedDuration::from_hours(123),
    )
}

/// Quotes from 23:00 of the 22nd of October 2026 to 03:00 of the 26th, as
/// `catalog` ends: the span's days, from an hour before them, for a run
/// that trains nothing.
fn run_catalog() -> TempDir {
    walk(
        ic::at("00:00:00") + SignedDuration::from_hours(47),
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
/// days its window lacks, then publishes its model set again with the
/// long straight in the place of the one the train published, a baseline
/// predicting an R of `r(tenths)` (`tenths` 3 at the span's start and one
/// more each day), calibrated as it is, and the meta it trained; what each
/// train did.
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
            training().train_at(at, |from, to| {
                replay_of(catalog, features(), &long_only(), from, to)
            })
        });
        trained.push((at, outcome));
        let tenths = 3 + (at.duration_since(start).as_hours() / 24) as usize;
        let published = model_store(models).read(at).unwrap();
        model_store(models)
            .publish(&StoredModelSet {
                sides: vec![baseline_of(&features(), &long_only(), Side::Long, tenths)],
                calibration: unchanged(&long_only()),
                ..published
            })
            .unwrap();
    }
    trained
}

/// The models of `long_only` under `models`.
fn model_store(models: &Path) -> ModelStore {
    blueprint(features(), long_only())
        .model_store(models)
        .unwrap()
}

/// Each train publishes a model set of the long it fits and of the meta
/// it learns from the longs of its out-of-fold pair, the first train included. With
/// meta models published straight in their place, a day's decisions trade
/// only as the meta model of the model set in use takes the trade of the
/// side picked: each day a long at every decision, at the level of the
/// long's baseline published at its start, r(3) = -0.2, r(4) = -0.1 then
/// r(5) = 0, and at the expected R of the meta published then, a baseline
/// of r(4) = -0.1, r(6) = 0.1 then r(8) = 0.3 through `CALIBRATION`, 0.1 +
/// 0.5 r: 0.05, 0.15 then 0.25.
#[rstest]
fn a_meta_model_trains_on_its_out_of_fold_pairs_trades_and_takes_them_in_the_run_from_its_first_retrain()
 {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    let retrains = [start, start + day(), start + day() * 2];

    let trained = train_model(&catalog, models.path());

    let times: Vec<Timestamp> = trained.iter().map(|(at, _)| *at).collect();
    assert_eq!(times, retrains);

    let store = model_store(models.path());

    for (at, trained) in &trained {
        assert_eq!(
            trained.outcome,
            Outcome::Published {
                model_set: store.directory().join(at.to_string())
            },
            "{trained:?}"
        );
    }
    assert_eq!(store.model_sets().unwrap(), retrains);
    for (&at, tenths) in retrains.iter().zip([4, 6, 8]) {
        let published = store.read(at).unwrap();
        assert!(published.meta.is_some(), "{at}");
        store
            .publish(&StoredModelSet {
                meta: Some(meta_baseline_of(&features(), &long_only(), tenths)),
                ..published
            })
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

    let expected = [
        (r(3), 0.1 + 0.5 * r(4)),
        (r(4), 0.1 + 0.5 * r(6)),
        (r(5), 0.1 + 0.5 * r(8)),
    ];

    for (period, &from) in retrains.iter().enumerate() {
        let (from_ns, to_ns) = (UnixNanos::from(from), UnixNanos::from(from + day()));
        let trades: Vec<Decision> = record
            .trades
            .iter()
            .filter(|(time, _)| from_ns <= *time && *time < to_ns)
            .map(|(_, trade)| *trade)
            .collect();
        let (level, taken) = expected[period];
        assert_eq!(trades.len(), 24, "period {period}");
        assert!(
            trades.iter().all(|trade| trade.side == OrderSide::Buy
                && trade.level == level
                && trade.meta_expected_r == Some(R::new(taken))),
            "period {period}: {trades:?}, not longs at {level} taken at {taken}"
        );
    }
}

/// The trades of `record` in each day of the span from its start, as
/// `(level, meta's expected R)`.
fn by_day(record: &crate::common::dataset::Record) -> Vec<Vec<(f64, Option<R>)>> {
    let (start, _) = span();
    (0..3)
        .map(|n| {
            let from = start + day() * n;
            let (from, to) = (UnixNanos::from(from), UnixNanos::from(from + day()));
            record
                .trades
                .iter()
                .filter(|(time, _)| from <= *time && *time < to)
                .map(|(_, trade)| (trade.level, trade.meta_expected_r))
                .collect()
        })
        .collect()
}

/// Publishes under `models` the model sets of `long_only` at the span's
/// first two retrain times: the first's long of `r(4)` beside a meta of
/// `r(6)`, taking it at 0.1 + 0.5 r(6) = 0.15R; the second's long of `r(5)`
/// beside `second_meta`.
fn publish_two(models: &Path, second_meta: helix_trader::model::store::StoredModel) {
    let (start, _) = span();
    let long = |tenths| baseline_of(&features(), &long_only(), Side::Long, tenths);
    let first_meta = meta_baseline_of(&features(), &long_only(), 6);
    for (at, tenths, meta) in [(start, 4, first_meta), (start + day(), 5, second_meta)] {
        model_store(models)
            .publish(&model_set(&long_only(), at, vec![long(tenths)], Some(meta)))
            .unwrap();
    }
}

/// The original defect: a store holding a meta model from one train and
/// sides from another never combines them. Each model set holds a long
/// and its meta: each of the first day's decisions trades its long, at
/// r(4) = -0.1, taken at the first's meta's 0.15R; each of the second and
/// the third day's its long at r(5) = nought, taken at the second's meta's
/// 0.1 + 0.5 r(2) = -0.05R, never the first's meta beside the second's
/// long.
#[rstest]
fn a_meta_from_one_train_never_decides_beside_the_sides_of_another() {
    let catalog = run_catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    publish_two(
        models.path(),
        meta_baseline_of(&features(), &long_only(), 2),
    );

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

    let days = by_day(&record);
    assert_eq!(days[0].len(), 24, "{days:?}");
    assert!(
        days[0]
            .iter()
            .all(|&trade| trade == (r(4), Some(R::new(0.1 + 0.5 * r(6))))),
        "{days:?}"
    );
    assert_eq!(days[1].len(), 24, "{days:?}");
    assert!(
        days[1]
            .iter()
            .chain(&days[2])
            .all(|&trade| trade == (r(5), Some(R::new(0.1 + 0.5 * r(2))))),
        "{days:?}"
    );
}

/// The second day's model set holds a meta model of another fingerprint,
/// which fails its load check: no decision of the second or the third day
/// trades, not on it nor on the first day's model set, its error logged
/// at each. Once published again whole, a run trades on it: each of its
/// decisions its long at r(5) = nought, taken at 0.1 + 0.5 r(8) = 0.25R.
#[rstest]
fn a_model_set_failing_a_check_stops_new_entries_and_is_used_once_it_loads() {
    let catalog = run_catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    let mut refused = meta_baseline_of(&features(), &long_only(), 8);
    refused.manifest.fingerprint = "f".repeat(64);
    publish_two(models.path(), refused);
    let run = || {
        on_thread(|| {
            trade_on(
                &catalog,
                features(),
                start,
                end,
                &long_only(),
                models.path(),
            )
        })
    };

    let stopped = by_day(&run());
    publish_two(
        models.path(),
        meta_baseline_of(&features(), &long_only(), 8),
    );
    let loaded = by_day(&run());

    assert_eq!(stopped[0].len(), 24, "{stopped:?}");
    assert!(
        stopped[1].is_empty() && stopped[2].is_empty(),
        "{stopped:?}"
    );
    assert_eq!(loaded[1].len(), 24, "{loaded:?}");
    assert!(
        loaded[1]
            .iter()
            .chain(&loaded[2])
            .all(|&trade| trade == (r(5), Some(R::new(0.1 + 0.5 * r(8))))),
        "{loaded:?}"
    );
}
