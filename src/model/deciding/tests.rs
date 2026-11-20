//! Decisions on stores under a temp dir, with no strategy: the pick between sides, swapping model
//! sets, refused model sets, the meta's margin.

use std::path::Path;

use nautilus_model::{enums::OrderSide, types::Money};
use rstest::rstest;

use super::{
    testing::{
        at, baseline, blueprint_of, both_sides, config, features, meta, meta_baseline,
        model_set_of, model_store, r, requirements, store, venue, with_meta,
    },
    *,
};
use crate::{
    backtest::Commission,
    model::{
        calibration::{Ksplit, Line},
        config::{Margins, MetaConfig, ModelConfig},
        dataset::DatasetVenue,
        learner::{LearnerConfig, LightGbmConfig, TrainingSet},
        store::{SideModel, StoredModel, StoredModelSet},
    },
};

const MIDNIGHT: &str = "2026-10-22T00:00:00Z";
const NOON: &str = "2026-10-22T12:00:00Z";

/// The mid, which a baseline does not read.
const ROW: [f64; 1] = [0.7];

/// Each side's row: the mid, then an entry cost of 0.23R.
fn rows_of_one() -> PerSide<SideRow> {
    PerSide {
        long: SideRow::new(&ROW, &[0.23]),
        short: SideRow::new(&ROW, &[0.23]),
    }
}

fn listed(models: &Path, config: &ModelConfig) -> DecidingModels {
    DecidingModels::new(&model_store(models, &features(), config)).unwrap()
}

/// `listed` after publishing from midnight a model set of the long's and short's tenths, if any.
fn listed_with(
    models: &Path,
    config: &ModelConfig,
    (long, short): (Option<usize>, Option<usize>),
) -> DecidingModels {
    let features = features();
    let sides = [(Side::Long, long), (Side::Short, short)]
        .into_iter()
        .filter_map(|(side, tenths)| tenths.map(|tenths| (side, tenths)));
    model_store(models, &features, config)
        .publish(&model_set_of(&features, config, MIDNIGHT, sides, None))
        .unwrap();
    listed(models, config)
}

fn decided(models: &mut DecidingModels, time: &str) -> Option<Taken> {
    models.decide(at(time), &ROW, &rows_of_one()).unwrap()
}

fn expected(models: &mut DecidingModels, time: &str) -> Option<f64> {
    decided(models, time).map(|taken| taken.expected_r.as_f64())
}

/// Equal up to the rounding of a pair's calibration.
fn near(actual: Option<f64>, expected: Option<f64>) {
    let near = match (actual, expected) {
        (Some(actual), Some(expected)) => (actual - expected).abs() < 1e-12,
        (actual, expected) => actual == expected,
    };
    assert!(near, "{actual:?} != {expected:?}");
}

fn side_taken(models: &mut DecidingModels) -> Option<Side> {
    decided(models, NOON).map(|taken| taken.side)
}

#[rstest]
fn no_model_in_the_store_gives_no_trade() {
    let models = tempfile::tempdir().unwrap();

    assert_eq!(decided(&mut listed(models.path(), &config()), NOON), None);
}

/// A model learned targets net of its dataset runs' commission, so a config declaring another does
/// not trade on it.
#[rstest]
fn a_model_of_another_commission_gives_no_trade() {
    let models = tempfile::tempdir().unwrap();
    store(models.path())
        .publish(&baseline(MIDNIGHT, 8))
        .unwrap();
    let other_costs = ModelConfig {
        dataset: DatasetVenue {
            commission: Commission::PerOrder(Money::from("3 USD")),
            ..venue()
        },
        ..config()
    };
    let other_costs = blueprint_of(&features(), &other_costs, requirements())
        .model_store(models.path())
        .unwrap();

    assert_eq!(
        decided(&mut DecidingModels::new(&other_costs).unwrap(), NOON),
        None
    );
    assert_eq!(
        expected(&mut listed(models.path(), &config()), NOON),
        Some(r(8))
    );
}

#[rstest]
fn no_model_valid_yet_gives_no_trade() {
    let models = tempfile::tempdir().unwrap();
    store(models.path())
        .publish(&baseline("2026-10-22T13:00:00Z", 8))
        .unwrap();

    assert_eq!(decided(&mut listed(models.path(), &config()), NOON), None);
}

#[rstest]
fn a_decision_uses_the_latest_model_set_valid_at_or_before_it() {
    let models = tempfile::tempdir().unwrap();
    let store = store(models.path());
    for (valid_from, tenths) in [
        ("2026-10-22T10:00:00Z", 7),
        ("2026-10-22T12:00:00Z", 8),
        ("2026-10-22T13:00:00Z", 9),
    ] {
        store.publish(&baseline(valid_from, tenths)).unwrap();
    }
    let mut models = listed(models.path(), &config());

    let expectations = [
        "2026-10-22T11:00:00Z",
        "2026-10-22T12:00:00Z",
        "2026-10-22T12:59:59Z",
    ]
    .map(|time| expected(&mut models, time));

    assert_eq!(expectations, [Some(r(7)), Some(r(8)), Some(r(8))]);
}

#[rstest]
fn a_model_set_published_after_a_decision_is_used_from_the_next() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());
    let before = expected(&mut models, NOON);

    // Live, a retrain at 12:00 publishes once its child finishes
    store.publish(&baseline(NOON, 9)).unwrap();
    models.list().unwrap();

    assert_eq!(before, Some(r(7)));
    assert_eq!(expected(&mut models, "2026-10-22T13:00:00Z"), Some(r(9)));
}

#[rstest]
fn a_model_set_published_after_the_listing_is_not_used_until_listed_again() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());

    store.publish(&baseline(NOON, 9)).unwrap();

    assert_eq!(expected(&mut models, "2026-10-22T13:00:00Z"), Some(r(7)));
}

/// The error names the models root.
#[rstest]
fn an_unreadable_store_fails_the_models_when_built() {
    let root = tempfile::tempdir().unwrap();
    store(root.path()).break_listing();

    let built = DecidingModels::new(&model_store(root.path(), &features(), &config()));

    let err = format!("{:#}", built.unwrap_err());
    assert!(err.contains(&root.path().display().to_string()), "{err}");
}

/// The model set of 10:30, published before the failed listing, is not used.
#[rstest]
fn a_failed_listing_keeps_the_previous_list() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());

    store.publish(&baseline("2026-10-22T10:30:00Z", 9)).unwrap();
    store.break_listing();
    let listing = models.list();

    assert!(listing.is_err());
    assert_eq!(expected(&mut models, NOON), Some(r(7)));
}

#[rstest]
fn a_store_listing_again_is_listed_again() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());

    store.publish(&baseline("2026-10-22T10:30:00Z", 9)).unwrap();
    store.break_listing();
    models.list().unwrap_err();
    store.mend_listing();
    models.list().unwrap();

    assert_eq!(expected(&mut models, NOON), Some(r(9)));
}

fn with_both_and_meta() -> ModelConfig {
    ModelConfig {
        meta: Some(meta()),
        ..both_sides()
    }
}

/// A model set of `with_both_and_meta`: the long predicts `r(long)`, the short `r(3)` = -0.2, never
/// picked, and the meta `r(8)` = 0.3, expecting 0.1 + 0.5 * 0.3 = 0.25R, clearing its margin of
/// 0.15R.
fn model_set(valid_from: &str, long: usize) -> StoredModelSet {
    let (features, config) = (features(), with_both_and_meta());
    let meta = meta_baseline(&features, &config, 8);
    model_set_of(
        &features,
        &config,
        valid_from,
        [(Side::Long, long), (Side::Short, 3)],
        Some(meta),
    )
}

/// The model of `side`, or the meta's if none.
fn model_of(model_set: &mut StoredModelSet, side: Option<Side>) -> &mut StoredModel {
    match side {
        Some(side) => model_set
            .sides
            .iter_mut()
            .find(|model| model.manifest.side == Some(side))
            .unwrap(),
        None => model_set.meta.as_mut().unwrap(),
    }
}

/// Mutates a model set to fail a check.
type Refuse = fn(&mut StoredModelSet);

/// Other feature names.
fn long_of_other_feature_names(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.feature_names = vec!["spread".to_string()];
}

/// Another fingerprint.
fn long_of_another_fingerprint(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.fingerprint = "f".repeat(64);
}

/// Three wide, a side's rows being two: the mid and the entry cost.
fn long_of_another_width(model_set: &mut StoredModelSet) {
    let set = TrainingSet::new(3, vec![0.5; 6], vec![r(9); 2]).unwrap();
    let long = model_of(model_set, Some(Side::Long));
    long.model = LearnerConfig::Baseline {}
        .fit(&set)
        .unwrap()
        .save()
        .unwrap();
    long.manifest.probes.clear();
}

/// Not predicting its probes.
fn long_not_predicting_its_probes(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.probes[0].prediction = r(9) + f64::EPSILON;
}

/// A level line of its own, a meta's.
fn long_with_a_level_line(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.calibration = Some(super::testing::CALIBRATION);
}

/// Another fingerprint.
fn short_of_another_fingerprint(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Short)).manifest.fingerprint = "f".repeat(64);
}

/// Another fingerprint.
fn meta_of_another_fingerprint(model_set: &mut StoredModelSet) {
    model_of(model_set, None).manifest.fingerprint = "f".repeat(64);
}

/// No level line, as a side's.
fn meta_without_a_level_line(model_set: &mut StoredModelSet) {
    model_of(model_set, None).manifest.calibration = None;
}

/// A calibration of one side for two.
fn pair_calibrated_as_one_side(model_set: &mut StoredModelSet) {
    model_set.calibration.side_call = None;
}

/// The model set of 10:00, its long `r(7)`, decides at 11:00. That of noon, its long `r(9)`,
/// `refuse`d, fails each decision from noon with an error naming it and `check`, so no entry is
/// made, on it or on the 10:00 set. Published again whole, it is read again and used.
#[rstest]
#[case::of_the_longs_feature_names(long_of_other_feature_names as Refuse, "the long model: its features")]
#[case::of_the_longs_fingerprint(long_of_another_fingerprint, "the long model: its fingerprint")]
#[case::of_the_longs_width(long_of_another_width, "the long model: it takes 3 features")]
#[case::of_the_longs_probes(long_not_predicting_its_probes, "the long model: it predicts")]
#[case::of_a_long_with_a_level_line(
    long_with_a_level_line,
    "the long model: its manifest has a level line"
)]
#[case::of_the_shorts_fingerprint(short_of_another_fingerprint, "the short model: its fingerprint")]
#[case::of_the_metas_fingerprint(meta_of_another_fingerprint, "the meta model: its fingerprint")]
#[case::of_a_meta_without_its_level_line(
    meta_without_a_level_line,
    "the meta model: its manifest has no level line"
)]
#[case::of_a_pair_calibrated_as_one_side(
    pair_calibrated_as_one_side,
    "a calibration of 1 sides for a pair of 2"
)]
fn a_model_set_failing_a_check_stops_new_entries_naming_it_and_is_used_once_it_loads(
    #[case] refuse: Refuse,
    #[case] check: &str,
) {
    let root = tempfile::tempdir().unwrap();
    let config = with_both_and_meta();
    let set = model_store(root.path(), &features(), &config);
    set.publish(&model_set("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut refused = model_set(NOON, 9);
    refuse(&mut refused);
    set.publish(&refused).unwrap();
    let mut models = DecidingModels::new(&set).unwrap();
    let mut decide = |time: &str| models.decide(at(time), &ROW, &rows_of_one());

    let before = decide("2026-10-22T11:00:00Z").unwrap();
    let errors = ["2026-10-22T12:00:00Z", "2026-10-22T13:00:00Z"]
        .map(|time| format!("{:#}", decide(time).unwrap_err()));
    set.publish(&model_set(NOON, 9)).unwrap();
    let mended = decide("2026-10-22T14:00:00Z").unwrap();

    near(before.map(|taken| taken.expected_r.as_f64()), Some(r(7)));

    for error in errors {
        assert!(error.contains("valid from 2026-10-22T12:00:00Z"), "{error}");
        assert!(error.contains(check), "{error}");
    }
    near(mended.map(|taken| taken.expected_r.as_f64()), Some(r(9)));
}

/// A long expecting 0.1R clears its margin of 0; the short is not traded. One side has no side
/// call.
#[rstest]
fn a_side_expected_to_clear_its_margin_is_taken_with_its_expected_r_level_and_side_call() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config(), (Some(6), None));

    assert_eq!(
        decided(&mut models, NOON),
        Some(Taken {
            side: Side::Long,
            expected_r: R::new(0.1),
            expected: Expected {
                level: 0.1,
                side_call: 0.0
            },
            meta_expected_r: None,
        })
    );
}

/// Of the sides clearing their margins, the one expecting more R trades: long 0.2R and short 0.3R,
/// the short; long 0.2R and short 0.1R, the long; both 0.2R, neither; margins of 0.5R, long 0.2R
/// and short a loss, neither.
#[rstest]
#[case::the_short_expected_the_more(both_sides(), (7, 8), Some((Side::Short, 8)))]
#[case::the_long_expected_the_more(both_sides(), (7, 6), Some((Side::Long, 7)))]
#[case::two_expected_alike(both_sides(), (7, 7), None)]
#[case::neither_clearing_its_margin(
    ModelConfig {
        margins: Margins {
            long: Some(R::new(0.5)),
            short: Some(R::new(0.5)),
        },
        ..config()
    },
    (7, 3),
    None
)]
fn of_two_sides_clearing_their_margins_the_one_expected_the_more_trades(
    #[case] config: ModelConfig,
    #[case] (long, short): (usize, usize),
    #[case] traded: Option<(Side, usize)>,
) {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config, (Some(long), Some(short)));

    let taken = decided(&mut models, NOON);

    assert_eq!(
        taken.as_ref().map(|taken| taken.side),
        traded.map(|(side, _)| side)
    );
    near(
        taken.map(|taken| taken.expected_r.as_f64()),
        traded.map(|(_, tenths)| r(tenths)),
    );
}

/// A short model expecting 0.4R, which a model of both sides would sell on, is never read by a
/// long-only model.
#[rstest]
fn a_long_only_model_never_sells() {
    let root = tempfile::tempdir().unwrap();
    let mut both = listed_with(root.path(), &both_sides(), (Some(3), Some(9)));
    let mut long = listed(root.path(), &config());

    assert_eq!(side_taken(&mut both), Some(Side::Short));
    assert_eq!(side_taken(&mut long), None);
}

fn in_hand(tenths: usize) -> SideModel {
    let set = TrainingSet::new(2, vec![0.5; 2], vec![r(tenths)]).unwrap();
    SideModel::new(LearnerConfig::Baseline {}.fit(&set).unwrap(), r(tenths))
}

/// A pair in no store: long 0.2R, short 0.3R, level line `0.1 + 0.5 C`, side call slope 1.5. C =
/// 0.25 and D = -0.1, so C' = 0.225 and D' = -0.15, the long's E' 0.15 and the short's 0.3.
fn pair_in_hand() -> Pair {
    Pair::new(
        vec![(Side::Long, in_hand(7)), (Side::Short, in_hand(8))],
        Ksplit {
            level: Line {
                alpha: 0.1,
                beta: 0.5,
            },
            side_call: Some(1.5),
        },
    )
    .unwrap()
}

/// The short, expecting 0.3R, is picked, carrying C' and D' of `pair_in_hand`.
#[rstest]
fn a_pair_in_hand_is_picked_among_on_its_calibrated_expectation() {
    let picked = DecidingModels::pick_among(
        &pair_in_hand(),
        [(Side::Long, R::new(0.0)), (Side::Short, R::new(0.0))],
        &rows_of_one(),
    )
    .unwrap()
    .unwrap();

    assert_eq!(picked.side, Side::Short);
    for (actual, expected) in [
        (picked.expected_r.as_f64(), 0.3),
        (picked.expected.level, 0.225),
        (picked.expected.side_call, -0.15),
    ] {
        assert!((actual - expected).abs() < 1e-12, "{picked:?}");
    }
}

/// The short, 0.3R, misses its margin of 0.35R, so the long, 0.15R, clearing 0, is picked.
#[rstest]
fn a_side_of_a_pair_in_hand_is_held_to_its_own_margin() {
    let picked = DecidingModels::pick_among(
        &pair_in_hand(),
        [(Side::Long, R::new(0.0)), (Side::Short, R::new(0.35))],
        &rows_of_one(),
    )
    .unwrap();

    assert_eq!(picked.map(|picked| picked.side), Some(Side::Long));
}

/// A calibration of two sides for one model, or of one side for two, is no pair.
#[rstest]
fn a_calibration_of_other_sides_than_its_models_is_no_pair() {
    let line = Line {
        alpha: 0.0,
        beta: 1.0,
    };

    let two_for_one = Pair::new(
        vec![(Side::Long, in_hand(7))],
        Ksplit {
            level: line,
            side_call: Some(1.0),
        },
    );
    let one_for_two = Pair::new(
        vec![(Side::Long, in_hand(7)), (Side::Short, in_hand(8))],
        Ksplit {
            level: line,
            side_call: None,
        },
    );

    let (two_for_one, one_for_two) = (
        two_for_one.unwrap_err().to_string(),
        one_for_two.unwrap_err().to_string(),
    );
    assert!(two_for_one.contains("of 2 sides"), "{two_for_one}");
    assert!(two_for_one.contains("pair of 1"), "{two_for_one}");
    assert!(one_for_two.contains("of 1 sides"), "{one_for_two}");
    assert!(one_for_two.contains("pair of 2"), "{one_for_two}");
}

/// `listed` on a model set from midnight: a long of 6 tenths (0.1R, above its margin of 0) and a
/// meta predicting `r(tenths)`, if any.
fn with_meta_of(models: &Path, config: &ModelConfig, tenths: Option<usize>) -> DecidingModels {
    let features = features();
    let meta = tenths.map(|tenths| meta_baseline(&features, config, tenths));
    published_with(models, config, &[(Side::Long, 6)], meta)
}

/// `listed` on a model set from midnight: a model per side of `sides`, and `meta`, if any.
fn published_with(
    models: &Path,
    config: &ModelConfig,
    sides: &[(Side, usize)],
    meta: Option<StoredModel>,
) -> DecidingModels {
    let features = features();
    let set = model_store(models, &features, config);
    set.publish(&model_set_of(
        &features,
        config,
        MIDNIGHT,
        sides.iter().copied(),
        meta,
    ))
    .unwrap();
    DecidingModels::new(&set).unwrap()
}

/// The meta predicts 0.3, expecting 0.1 + 0.5 * 0.3 = 0.25R, at or above its margin of 0.15R, so
/// it takes the long.
#[rstest]
fn a_meta_model_from_its_margin_takes_the_side_picked_at_its_expected_r() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(8));

    assert_eq!(
        decided(&mut models, NOON),
        Some(Taken {
            side: Side::Long,

            // The side's, its long of 6 tenths
            expected_r: R::new(0.1),
            expected: Expected {
                level: 0.1,
                side_call: 0.0
            },
            meta_expected_r: Some(R::new(0.25)),
        })
    );
}

/// The meta predicting 0 expects 0.1R, below its margin of 0.15R.
#[rstest]
fn a_meta_model_below_its_margin_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(5));

    assert_eq!(decided(&mut models, NOON), None);
}

/// The set of 10:00 holds a meta expecting 0.25R, taking its long of 6. The set of 11:00 holds a
/// long of 9 and a meta predicting 0, expecting 0.1R, below its margin. At noon the latest set
/// takes no trade, never its long beside the 10:00 meta, which would take it.
#[rstest]
fn a_meta_never_decides_beside_the_sides_of_another_model_set() {
    let root = tempfile::tempdir().unwrap();
    let (features, config) = (features(), with_meta(meta()));
    let set = model_store(root.path(), &features, &config);
    for (valid_from, tenths, meta) in [
        ("2026-10-22T10:00:00Z", 6, 8),
        ("2026-10-22T11:00:00Z", 9, 5),
    ] {
        set.publish(&model_set_of(
            &features,
            &config,
            valid_from,
            [(Side::Long, tenths)],
            Some(meta_baseline(&features, &config, meta)),
        ))
        .unwrap();
    }
    let mut models = DecidingModels::new(&set).unwrap();

    let taken = decided(&mut models, "2026-10-22T10:30:00Z");
    let at_noon = decided(&mut models, NOON);

    assert_eq!(
        taken.map(|taken| (taken.expected_r, taken.meta_expected_r)),
        Some((R::new(0.1), Some(R::new(0.25))))
    );
    assert_eq!(at_noon, None);
}

/// A meta taking a trade from 0.15R, learned by LightGBM at its defaults.
fn lightgbm_meta() -> MetaConfig {
    MetaConfig {
        learner: LearnerConfig::LightGbm(LightGbmConfig::builder().build()),
        ..meta()
    }
}

/// A meta fitted by the learner of `config`'s meta on rows of the mid 0.7, the side (1 a buy, -1 a
/// sell), the level 0 and the side call. Ten buys and ten sells at each side call from 0.20 to
/// 0.79 by 0.01, at least 300 rows each side of a split where LightGBM's leaves need 200. Buys
/// realize 1R from a side call of 0.5 and -1R below it, sells -1R, so it predicts near 1R on those
/// buys and near -1R elsewhere: near 0.6R and -0.4R calibrated (`CALIBRATION`), either side of the
/// margin of 0.15R.
fn learned_meta(config: &ModelConfig) -> StoredModel {
    let (mut rows, mut labels) = (vec![], vec![]);
    for side in [1.0, -1.0] {
        for hundredths in 20..80 {
            for _ in 0..10 {
                rows.extend([0.7, side, 0.0, side * f64::from(hundredths) / 100.0]);
                labels.push(if side > 0.0 && hundredths >= 50 {
                    1.0
                } else {
                    -1.0
                });
            }
        }
    }
    let learner = &config.meta.as_ref().unwrap().learner;
    let fitted = learner
        .fit(&TrainingSet::new(4, rows, labels).unwrap())
        .unwrap();
    let mut meta = meta_baseline(&features(), config, 5);
    meta.manifest.learner = learner.clone();
    meta.model = fitted.save().unwrap();
    meta
}

/// The decision at noon on a pair whose `side` predicts `tenths` and the other `r(2)` = -0.3,
/// taken by `learned_meta`. The side call is the long's R less the short's.
fn learned_meta_decision(side: Side, tenths: usize) -> Option<Taken> {
    let root = tempfile::tempdir().unwrap();
    let config = ModelConfig {
        meta: Some(lightgbm_meta()),
        ..both_sides()
    };
    let meta = learned_meta(&config);
    let sides = Side::BOTH.map(|traded| (traded, if traded == side { tenths } else { 2 }));
    let mut models = published_with(root.path(), &config, &sides, Some(meta));
    decided(&mut models, NOON)
}

/// The meta reads the side call from its place in the row, after the side and level. It takes a
/// buy on a side call of 0.6 (long 0.3R, short -0.3R), refuses one on 0.4 (long 0.1R), and refuses
/// a sell, its side call -0.6.
#[rstest]
#[case::a_buy_above(Side::Long, 8, Some(Side::Long))]
#[case::a_buy_below(Side::Long, 6, None)]
#[case::a_sell(Side::Short, 8, None)]
fn a_meta_model_decides_on_the_side_call_of_the_decision(
    #[case] side: Side,
    #[case] tenths: usize,
    #[case] taken: Option<Side>,
) {
    let decided = learned_meta_decision(side, tenths);

    assert_eq!(decided.map(|taken| taken.side), taken);
}

/// `meta`, learned by LightGBM, with every leaf NaN, so it predicts NaN on any row. LightGBM finds
/// its trees by their sizes, if its text gives them, and those are no longer theirs.
fn predicting_nan(mut meta: StoredModel) -> StoredModel {
    let text = meta
        .model
        .as_str()
        .lines()
        .filter(|line| !line.starts_with("tree_sizes="))
        .map(|line| {
            if line.starts_with("leaf_value=") {
                let leaves = line.split_whitespace().count();
                format!("leaf_value={}", vec!["nan"; leaves].join(" "))
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    meta.model = text.into();
    let row = meta_row(
        &ROW,
        OrderSide::Buy,
        Expected {
            level: 0.0,
            side_call: 0.6,
        },
    );
    let loaded = meta
        .load(&meta.manifest.feature_names, &meta.manifest.fingerprint)
        .unwrap();
    assert!(loaded.predict(&row).unwrap().is_nan());
    meta
}

#[rstest]
fn a_nan_meta_expectation_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let config = with_meta(lightgbm_meta());
    let meta = predicting_nan(learned_meta(&config));
    let mut models = published_with(root.path(), &config, &[(Side::Long, 8)], Some(meta));

    assert_eq!(decided(&mut models, NOON), None);
}
