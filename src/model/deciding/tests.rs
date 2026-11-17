//! A decision on stores under a temp dir, with no strategy: the pick between
//! the sides on their pair's calibrated expected R, swapping generations,
//! refused generations, the meta's margin.

use std::path::Path;

use nautilus_model::{enums::OrderSide, types::Money};
use rstest::rstest;

use super::{
    testing::{
        at, baseline, blueprint_of, both_sides, config, features, generation_of, meta,
        meta_baseline, model_store, r, requirements, store, venue, with_meta,
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
        store::{SideModel, StoredGeneration, StoredModel},
    },
};

const MIDNIGHT: &str = "2026-10-22T00:00:00Z";
const NOON: &str = "2026-10-22T12:00:00Z";

/// The decision's features: the mid of 0.7, which a baseline does not read.
const ROW: [f64; 1] = [0.7];

/// Each side's row of `ROW` on the tests' time-only target: the mid, then
/// the side's entry cost, 0.23R.
fn rows_of_one() -> PerSide<SideRow> {
    PerSide {
        long: SideRow::new(&ROW, &[0.23]),
        short: SideRow::new(&ROW, &[0.23]),
    }
}

/// The models of `config` under `models`, listed now.
fn listed(models: &Path, config: &ModelConfig) -> DecidingModels {
    DecidingModels::new(&model_store(models, &features(), config)).unwrap()
}

/// Those of `listed`, the generation of the long's of `long` tenths and the
/// short's of `short` tenths, if any, published from midnight first.
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
        .publish(&generation_of(&features, config, MIDNIGHT, sides, None))
        .unwrap();
    listed(models, config)
}

/// What `models` decide at `time` on `ROW`.
fn decided(models: &mut DecidingModels, time: &str) -> Option<Taken> {
    models.decide(at(time), &ROW, &rows_of_one()).unwrap()
}

/// The net R the trade `decided` decides expects.
fn expected(models: &mut DecidingModels, time: &str) -> Option<f64> {
    decided(models, time).map(|taken| taken.expected_r.as_f64())
}

/// `actual` is `expected`, up to the rounding of a pair's calibration, which
/// leaves a side's prediction as it is only to the last bits.
fn near(actual: Option<f64>, expected: Option<f64>) {
    let near = match (actual, expected) {
        (Some(actual), Some(expected)) => (actual - expected).abs() < 1e-12,
        (actual, expected) => actual == expected,
    };
    assert!(near, "{actual:?} != {expected:?}");
}

/// The side `models` pick at noon on `ROW`.
fn side_picked(models: &mut DecidingModels) -> Option<Side> {
    models
        .pick(at(NOON), &rows_of_one())
        .unwrap()
        .map(|picked| picked.side)
}

#[rstest]
fn no_model_in_the_store_gives_no_trade() {
    let models = tempfile::tempdir().unwrap();

    assert_eq!(decided(&mut listed(models.path(), &config()), NOON), None);
}

/// A model learned targets net of its dataset runs' commission: a config
/// declaring another does not trade on it.
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
fn a_decision_uses_the_latest_generation_valid_at_or_before_it() {
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
fn a_generation_published_after_a_decision_is_used_from_the_next() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());
    let before = expected(&mut models, NOON);

    // Live, a retrain at 12:00 publishes once its child finishes.
    store.publish(&baseline(NOON, 9)).unwrap();
    models.list().unwrap();

    assert_eq!(before, Some(r(7)));
    assert_eq!(expected(&mut models, "2026-10-22T13:00:00Z"), Some(r(9)));
}

/// When to list is the caller's: the meta's examples decide on the stores as
/// they are when made.
#[rstest]
fn a_generation_published_after_the_listing_is_not_used_until_listed_again() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());

    store.publish(&baseline(NOON, 9)).unwrap();

    assert_eq!(expected(&mut models, "2026-10-22T13:00:00Z"), Some(r(7)));
}

/// The store, under the models root, fails to list: its reason names it.
#[rstest]
fn an_unreadable_store_fails_the_models_when_built() {
    let root = tempfile::tempdir().unwrap();
    store(root.path()).break_listing();

    let built = DecidingModels::new(&model_store(root.path(), &features(), &config()));

    let err = format!("{:#}", built.unwrap_err());
    assert!(err.contains(&root.path().display().to_string()), "{err}");
}

/// A failed listing keeps the previous list: the generation of 10:30,
/// published before it, is not used.
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

/// Both sides traded from nought, their trades taken by `meta`.
fn with_both_and_meta() -> ModelConfig {
    ModelConfig {
        meta: Some(meta()),
        ..both_sides()
    }
}

/// A generation of `with_both_and_meta` valid from `valid_from`: its long
/// predicting an R of `r(long)`, its short of 3, expecting a loss, never
/// picked, and its meta model of 8, expecting 0.1 + 0.5 r(8) = 0.25R, which
/// clears its margin of 0.15R.
fn generation(valid_from: &str, long: usize) -> StoredGeneration {
    let (features, config) = (features(), with_both_and_meta());
    let meta = meta_baseline(&features, &config, 8);
    generation_of(
        &features,
        &config,
        valid_from,
        [(Side::Long, long), (Side::Short, 3)],
        Some(meta),
    )
}

/// The generation's `side` model, a side's, or the meta's of none.
fn model_of(generation: &mut StoredGeneration, side: Option<Side>) -> &mut StoredModel {
    match side {
        Some(side) => generation
            .sides
            .iter_mut()
            .find(|model| model.manifest.side == Some(side))
            .unwrap(),
        None => generation.meta.as_mut().unwrap(),
    }
}

/// The model of a level of a generation, a side's or the meta's of none,
/// failing a check.
type Refuse = fn(&mut StoredGeneration);

/// Refuses the long's model: of other feature names.
fn long_of_other_feature_names(generation: &mut StoredGeneration) {
    model_of(generation, Some(Side::Long))
        .manifest
        .feature_names = vec!["spread".to_string()];
}

/// Refuses the long's model: of another fingerprint.
fn long_of_another_fingerprint(generation: &mut StoredGeneration) {
    model_of(generation, Some(Side::Long)).manifest.fingerprint = "f".repeat(64);
}

/// Refuses the long's model: of another width, a side's rows being two
/// wide, the mid and the entry cost, and the model three.
fn long_of_another_width(generation: &mut StoredGeneration) {
    let set = TrainingSet::new(3, vec![0.5; 6], vec![r(9); 2]).unwrap();
    let long = model_of(generation, Some(Side::Long));
    long.model = LearnerConfig::Baseline {}
        .fit(&set)
        .unwrap()
        .save()
        .unwrap();
    long.manifest.probes.clear();
}

/// Refuses the long's model: not predicting its probes.
fn long_not_predicting_its_probes(generation: &mut StoredGeneration) {
    model_of(generation, Some(Side::Long)).manifest.probes[0].prediction = r(9) + f64::EPSILON;
}

/// Refuses the long's model: with a level line of its own, a meta's.
fn long_with_a_level_line(generation: &mut StoredGeneration) {
    model_of(generation, Some(Side::Long)).manifest.calibration = Some(super::testing::CALIBRATION);
}

/// Refuses the short's model: of another fingerprint.
fn short_of_another_fingerprint(generation: &mut StoredGeneration) {
    model_of(generation, Some(Side::Short)).manifest.fingerprint = "f".repeat(64);
}

/// Refuses the meta's model: of another fingerprint.
fn meta_of_another_fingerprint(generation: &mut StoredGeneration) {
    model_of(generation, None).manifest.fingerprint = "f".repeat(64);
}

/// Refuses the meta's model: without its level line, as a side's.
fn meta_without_a_level_line(generation: &mut StoredGeneration) {
    model_of(generation, None).manifest.calibration = None;
}

/// Refuses the pair: its calibration of one side for two.
fn pair_calibrated_as_one_side(generation: &mut StoredGeneration) {
    generation.calibration.side_call = None;
}

/// The generation of 10:00, its long of 7, decides at 11:00; that of noon,
/// its long of 9, `refuse`d, fails each decision from noon with an error
/// naming it and the check, `check`, so no entry is made, neither on it nor
/// on the generation of 10:00; once published again whole, it is read
/// again at the next decision and used.
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
fn a_generation_failing_a_check_stops_new_entries_naming_it_and_is_used_once_it_loads(
    #[case] refuse: Refuse,
    #[case] check: &str,
) {
    let root = tempfile::tempdir().unwrap();
    let config = with_both_and_meta();
    let set = model_store(root.path(), &features(), &config);
    set.publish(&generation("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut refused = generation(NOON, 9);
    refuse(&mut refused);
    set.publish(&refused).unwrap();
    let mut models = DecidingModels::new(&set).unwrap();
    let mut decide = |time: &str| models.decide(at(time), &ROW, &rows_of_one());

    let before = decide("2026-10-22T11:00:00Z").unwrap();
    let errors = ["2026-10-22T12:00:00Z", "2026-10-22T13:00:00Z"]
        .map(|time| format!("{:#}", decide(time).unwrap_err()));
    set.publish(&generation(NOON, 9)).unwrap();
    let mended = decide("2026-10-22T14:00:00Z").unwrap();

    near(before.map(|taken| taken.expected_r.as_f64()), Some(r(7)));

    for error in errors {
        assert!(error.contains("valid from 2026-10-22T12:00:00Z"), "{error}");
        assert!(error.contains(check), "{error}");
    }
    near(mended.map(|taken| taken.expected_r.as_f64()), Some(r(9)));
}

/// A long expecting 0.1R, clearing its margin of nought; the short is not
/// traded. One side has no side call.
#[rstest]
fn a_side_expected_to_clear_its_margin_is_picked_with_its_expected_r_level_and_side_call() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config(), (Some(6), None));

    assert_eq!(
        models.pick(at(NOON), &rows_of_one()).unwrap(),
        Some(Picked {
            side: Side::Long,
            expected_r: R::new(0.1),
            expected: Expected {
                level: 0.1,
                side_call: 0.0
            },
        })
    );
}

/// Of the sides clearing their margins, the one expected the more R
/// trades, on R alone: the long expecting 0.2R and the short 0.3R, both
/// clearing nought, the short; the long expecting 0.2R and the short 0.1R,
/// the long; both expecting 0.2R, neither; and, margins of 0.5R, neither
/// of the long expecting 0.2R and the short to lose.
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

/// A short model in the store expecting 0.4R, which a model of both sides
/// would sell on: a long-only model never reads it.
#[rstest]
fn a_long_only_model_never_sells() {
    let root = tempfile::tempdir().unwrap();
    let mut both = listed_with(root.path(), &both_sides(), (Some(3), Some(9)));
    let mut long = listed(root.path(), &config());

    assert_eq!(side_picked(&mut both), Some(Side::Short));
    assert_eq!(side_picked(&mut long), None);
}

/// A side's model predicting an R of `r(tenths)`, in no store.
fn in_hand(tenths: usize) -> SideModel {
    let set = TrainingSet::new(2, vec![0.5; 2], vec![r(tenths)]).unwrap();
    SideModel::new(LearnerConfig::Baseline {}.fit(&set).unwrap(), r(tenths))
}

/// A pair in no store, its long predicting 0.2R and its short 0.3R, the
/// level line `0.1 + 0.5 C` and the side call's slope 1.5: C = 0.25 and D =
/// -0.1, so C' = 0.225 and D' = -0.15, the long's E' 0.15 and the short's
/// 0.3.
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

/// Worked by hand on `pair_in_hand`: of a pair in no store, the short,
/// expecting 0.3R, the more, its pick carrying the decision's level and side
/// call.
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

/// The short expects 0.3R, the more, short of its own margin of 0.35R: the
/// long, expecting 0.15R and clearing its margin of nought, is picked.
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

/// A pair's calibration is of as many sides as its models: one of two sides
/// for one model is no pair, nor one of one side for two.
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

/// `listed`, as `config` says, its generation from midnight: its long a
/// model of 6 tenths (expecting 0.1R, above its margin of nought) and its
/// meta model predicting an R of `r(tenths)`, if any.
fn with_meta_of(models: &Path, config: &ModelConfig, tenths: Option<usize>) -> DecidingModels {
    let features = features();
    let meta = tenths.map(|tenths| meta_baseline(&features, config, tenths));
    published_with(models, config, &[(Side::Long, 6)], meta)
}

/// `listed`, as `config` says, its generation from midnight: a model of
/// each side of `sides` predicting an R of `r(tenths)` of its tenths, and
/// its meta model `meta`, if any.
fn published_with(
    models: &Path,
    config: &ModelConfig,
    sides: &[(Side, usize)],
    meta: Option<StoredModel>,
) -> DecidingModels {
    let features = features();
    let set = model_store(models, &features, config);
    set.publish(&generation_of(
        &features,
        config,
        MIDNIGHT,
        sides.iter().copied(),
        meta,
    ))
    .unwrap();
    DecidingModels::new(&set).unwrap()
}

/// Worked by hand: the meta predicting an R of 0.3, its level line `0.1 +
/// 0.5 E` expecting 0.25R, at or above its margin of 0.15R, takes the long
/// picked, at its calibrated expected R.
#[rstest]
fn a_meta_model_from_its_margin_takes_the_side_picked_at_its_expected_r() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(8));

    assert_eq!(
        decided(&mut models, NOON),
        Some(Taken {
            side: Side::Long,
            // The side's, its long of 6 tenths.
            expected_r: R::new(0.1),
            expected: Expected {
                level: 0.1,
                side_call: 0.0
            },
            meta_expected_r: Some(R::new(0.25)),
        })
    );
}

/// The meta predicting nought expects 0.1R, below its margin of 0.15R:
/// the margin is in R, as the meta's expectation is. A generation holding
/// no meta model, its meta having waited, takes no trade either.
#[rstest]
#[case::below_its_margin(Some(5))]
#[case::of_a_generation_without_one(None)]
fn a_meta_model_below_its_margin_takes_no_trade(#[case] tenths: Option<usize>) {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), tenths);

    assert_eq!(decided(&mut models, NOON), None);
}

/// A generation's sides decide beside its own meta alone: the generation
/// of 10:00 holds a meta model taking the long, expecting 0.25R; the one of
/// 11:00, a long of 9, holds none, its meta having waited. At noon the
/// latest decides, and takes no trade, never its long beside the meta of
/// 10:00, which would take it; at 10:30 the generation of 10:00 takes its
/// long of 6.
#[rstest]
fn a_meta_never_decides_beside_the_sides_of_another_generation() {
    let root = tempfile::tempdir().unwrap();
    let (features, config) = (features(), with_meta(meta()));
    let set = model_store(root.path(), &features, &config);
    let meta = meta_baseline(&features, &config, 8);
    for (valid_from, tenths, meta) in [
        ("2026-10-22T10:00:00Z", 6, Some(meta)),
        ("2026-10-22T11:00:00Z", 9, None),
    ] {
        set.publish(&generation_of(
            &features,
            &config,
            valid_from,
            [(Side::Long, tenths)],
            meta,
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

/// A pick is the side before any meta: the meta's examples learn from the
/// sides picked whether or not a meta takes them.
#[rstest]
fn a_pick_is_the_side_before_any_meta() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(5));

    assert_eq!(side_picked(&mut models), Some(Side::Long));
}

/// The meta model taking a trade from 0.15R, learned by LightGBM at its
/// defaults.
fn lightgbm_meta() -> MetaConfig {
    MetaConfig {
        learner: LearnerConfig::LightGbm(LightGbmConfig::builder().build()),
        ..meta()
    }
}

/// A meta model of `config` valid from midnight of the 22nd, which learned on
/// rows of its features in their order, the mid, 0.7, then the side picked, 1
/// a buy and -1 a sell, then the decision's level, nought, and its side call,
/// that buys realize 1R from a side call of 0.5 and -1R below it, and sells
/// -1R: fitted by the learner of `config`'s meta on ten buys and ten sells at
/// each side call from 0.20 to 0.79 by 0.01, at least 300 rows on each side
/// of a split where LightGBM's leaves need 200, so that it predicts near 1R on
/// those buys and near -1R elsewhere, near 0.6R and -0.4R calibrated
/// (`CALIBRATION`), either side of the margin of 0.15R.
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

/// The decision at noon on a pair whose `side` predicts `tenths` and the
/// other 2, expecting a loss, its trade taken from 0.15R by the meta model
/// LightGBM learned (`learned_meta`): the side call is the long's R less the
/// short's, `r(tenths)` less -0.3 picking the long.
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

/// The meta model reads the decision's side call from its place in the row,
/// after its side and level, and is given the side picked after its
/// features: it takes a buy on a side call of 0.6, a long of 0.3R beside a
/// short of -0.3R, which it learned gains; refuses one on 0.4, a long of
/// 0.1R, which it learned loses; and refuses a sell, its side call -0.6,
/// which it learned never does.
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

/// `meta`, learned by LightGBM, with every leaf of its trees NaN: it
/// predicts NaN on any row, checked here, as a model refused at load
/// would take no trade either. LightGBM finds its trees by their sizes,
/// if its text gives them, and those are no longer theirs.
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
