//! Tests decisions on stores under a temp dir, with no strategy.

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

fn rows_of_one() -> PerSide<SideRow> {
    PerSide {
        long: SideRow::new(&ROW, &[0.23]),
        short: SideRow::new(&ROW, &[0.23]),
    }
}

fn listed(models: &Path, config: &ModelConfig) -> DecidingModels {
    DecidingModels::new(&model_store(models, &features(), config)).unwrap()
}

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

/// Equal, up to rounding in a pair's calibration.
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

/// A model learns targets net of its dataset commission, so another commission gives no trade.
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

#[rstest]
fn an_unreadable_store_fails_the_models_when_built() {
    let root = tempfile::tempdir().unwrap();
    store(root.path()).break_listing();

    let built = DecidingModels::new(&model_store(root.path(), &features(), &config()));

    let err = format!("{:#}", built.unwrap_err());
    assert!(err.contains(&root.path().display().to_string()), "{err}");
}

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

/// The short predicts `r(3)` and is never picked. The meta expects 0.25R, which clears its margin
/// of 0.15R.
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

type Refuse = fn(&mut StoredModelSet);

fn long_of_other_feature_names(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.feature_names = vec!["spread".to_string()];
}

fn long_of_another_fingerprint(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.fingerprint = "f".repeat(64);
}

/// Makes the long model three wide, though a side's rows are two wide: the mid and the entry cost.
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

fn long_not_predicting_its_probes(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.probes[0].prediction = r(9) + f64::EPSILON;
}

/// Gives the long model a level line of its own, which is a meta's.
fn long_with_a_level_line(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Long)).manifest.calibration = Some(super::testing::CALIBRATION);
}

fn short_of_another_fingerprint(model_set: &mut StoredModelSet) {
    model_of(model_set, Some(Side::Short)).manifest.fingerprint = "f".repeat(64);
}

fn meta_of_another_fingerprint(model_set: &mut StoredModelSet) {
    model_of(model_set, None).manifest.fingerprint = "f".repeat(64);
}

fn meta_without_a_level_line(model_set: &mut StoredModelSet) {
    model_of(model_set, None).manifest.calibration = None;
}

fn pair_calibrated_as_one_side(model_set: &mut StoredModelSet) {
    model_set.calibration.side_call = None;
}

/// The 10:00 set has its long `r(7)`, the noon set `r(9)` mutated by `refuse`. Each decision from
/// noon fails with an error naming the model set and `check`, and enters no trade on either set.
/// Published again whole, the noon set is read again and used.
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

/// A long-only model never reads a short model expecting 0.4R, which a model of both sides would
/// sell on.
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

/// A pair in no store, with the long expecting 0.2R and the short 0.3R.
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

fn with_meta_of(models: &Path, config: &ModelConfig, tenths: Option<usize>) -> DecidingModels {
    let features = features();
    let meta = tenths.map(|tenths| meta_baseline(&features, config, tenths));
    published_with(models, config, &[(Side::Long, 6)], meta)
}

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

#[rstest]
fn a_meta_model_from_its_margin_takes_the_side_picked_at_its_expected_r() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(8));

    assert_eq!(
        decided(&mut models, NOON),
        Some(Taken {
            side: Side::Long,
            expected_r: R::new(0.1),
            expected: Expected {
                level: 0.1,
                side_call: 0.0
            },
            meta_expected_r: Some(R::new(0.25)),
        })
    );
}

#[rstest]
fn a_meta_model_below_its_margin_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(5));

    assert_eq!(decided(&mut models, NOON), None);
}

/// The set of 10:00 has a meta that takes its long. The set of 11:00 has a meta below its margin,
/// so at noon the latest set takes no trade, and does not pair its long with the 10:00 meta.
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

fn lightgbm_meta() -> MetaConfig {
    MetaConfig {
        learner: LearnerConfig::LightGbm(LightGbmConfig::builder().build()),
        ..meta()
    }
}

/// Fits a meta with at least 300 rows on each side of a split, where LightGBM's leaves need 200.
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

/// The meta reads the side call from its place in the row, after the side and level.
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

/// Makes `meta`, learned by LightGBM, predict NaN on any row, with every leaf NaN. LightGBM finds
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
