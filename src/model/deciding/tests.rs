//! A decision on stores under a temp dir, with no strategy: the pick between
//! the sides on their pair's calibrated expected R, swapping, refused models,
//! the meta's margin.

use std::path::Path;

use nautilus_model::{enums::OrderSide, types::Money};

use super::{
    testing::{
        at, baseline, baseline_for, blueprint_of, both_sides, config, features, meta,
        meta_baseline, model_set, r, requirements, side_store, store, venue, with_meta,
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
        store::{ModelCalibration, SideModel, StoredModel},
    },
};

const MIDNIGHT: &str = "2026-10-22T00:00:00Z";
const NOON: &str = "2026-10-22T12:00:00Z";

/// The decision's features: the mid of 0.7, which a baseline does not read.
const ROW: [f64; 1] = [0.7];

/// Each side's row of `ROW` on the tests' time-only target, its unit worth
/// `units`' of its side: the mid, then the side's entry cost, 0.23R.
fn rows(units: PerSide<f64>) -> PerSide<SideRow> {
    units.map(|unit| SideRow::new(&ROW, &[0.23], unit))
}

/// Units worth 1 USD a side, at which a side's expected R is its expected
/// net P&L.
const UNITS_OF_ONE: PerSide<f64> = PerSide {
    long: 1.0,
    short: 1.0,
};

/// The rows of `ROW` at `UNITS_OF_ONE`.
fn rows_of_one() -> PerSide<SideRow> {
    rows(UNITS_OF_ONE)
}

/// The models of `config` under `models`, listed now.
fn listed(models: &Path, config: &ModelConfig) -> DecidingModels {
    DecidingModels::new(&model_set(models, &features(), config)).unwrap()
}

/// Those of `listed`, the long's of `long` tenths and the short's of `short`
/// tenths, if any, published from midnight first.
fn listed_with(
    models: &Path,
    config: &ModelConfig,
    (long, short): (Option<usize>, Option<usize>),
) -> DecidingModels {
    let features = features();
    for (side, tenths) in [(Side::Long, long), (Side::Short, short)] {
        if let Some(tenths) = tenths {
            side_store(models, &features, config, side)
                .publish(&baseline_for(&features, config, side, MIDNIGHT, tenths))
                .unwrap();
        }
    }
    listed(models, config)
}

/// What `models` decide at `time` on `ROW`, at `UNITS_OF_ONE`.
fn decided(models: &mut DecidingModels, time: &str) -> Option<Taken> {
    models.decide(at(time), &ROW, &rows_of_one()).unwrap()
}

/// The net R the trade `decided` decides expects.
fn expected(models: &mut DecidingModels, time: &str) -> Option<f64> {
    decided(models, time).map(|taken| taken.expected_r)
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

/// The side `models` pick at noon on `ROW`, the units being `units`.
fn side_picked(models: &mut DecidingModels, units: PerSide<f64>) -> Option<Side> {
    models
        .pick(at(NOON), &rows(units))
        .unwrap()
        .map(|picked| picked.side)
}

#[test]
fn no_model_in_the_store_gives_no_trade() {
    let models = tempfile::tempdir().unwrap();

    assert_eq!(decided(&mut listed(models.path(), &config()), NOON), None);
}

/// A model learned targets net of its dataset runs' commission: a
/// backtest of other costs does not trade on it.
#[test]
fn a_model_of_another_commission_gives_no_trade() {
    let models = tempfile::tempdir().unwrap();
    store(models.path())
        .publish(&baseline(MIDNIGHT, 8))
        .unwrap();
    let other_costs = blueprint_of(&features(), &config(), requirements())
        .model_set(
            models.path(),
            &DatasetVenue {
                commission: Commission::PerOrder(Money::from("3 USD")),
                ..venue()
            },
        )
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

#[test]
fn no_model_valid_yet_gives_no_trade() {
    let models = tempfile::tempdir().unwrap();
    store(models.path())
        .publish(&baseline("2026-10-22T13:00:00Z", 8))
        .unwrap();

    assert_eq!(decided(&mut listed(models.path(), &config()), NOON), None);
}

#[test]
fn a_decision_uses_the_latest_model_valid_at_or_before_it() {
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

#[test]
fn a_model_published_after_a_decision_is_used_from_the_next() {
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
#[test]
fn a_model_published_after_the_listing_is_not_used_until_listed_again() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());

    store.publish(&baseline(NOON, 9)).unwrap();

    assert_eq!(expected(&mut models, "2026-10-22T13:00:00Z"), Some(r(7)));
}

#[test]
fn an_unreadable_store_fails_the_models_when_built() {
    let root = tempfile::tempdir().unwrap();
    store(root.path()).break_listing();

    let built = DecidingModels::new(&model_set(root.path(), &features(), &config()));

    assert!(built.is_err());
}

/// The model of `side` of `both_sides` valid from `valid_from`, predicting
/// an R of `r(tenths)`, as `baseline_for` says.
fn side_model(side: Side, valid_from: &str, tenths: usize) -> StoredModel {
    baseline_for(&features(), &both_sides(), side, valid_from, tenths)
}

/// Publishes `model`, a side's of `both_sides`, under `models`.
fn publish_side(models: &Path, side: Side, model: &StoredModel) {
    side_store(models, &features(), &both_sides(), side)
        .publish(model)
        .unwrap();
}

/// Publishes under `models` a pair of `both_sides` valid from `valid_from`:
/// a long of `long` tenths, and a short of 3, expecting a loss, never
/// picked. Each side's expected R is its own prediction, the pair's
/// calibration leaving them as they are.
fn publish_pair(models: &Path, valid_from: &str, long: usize) {
    publish_side(
        models,
        Side::Long,
        &side_model(Side::Long, valid_from, long),
    );
    publish_side(models, Side::Short, &side_model(Side::Short, valid_from, 3));
}

/// The pair valid from 10:30 is in stores of which the long's lists, but
/// the short's fails: the failed listing keeps the long's list too, and a
/// decision uses no fresh list beside a stale one.
#[test]
fn a_failed_listing_keeps_every_sides_previous_list() {
    let root = tempfile::tempdir().unwrap();
    publish_pair(root.path(), "2026-10-22T10:00:00Z", 7);
    let mut models = listed(root.path(), &both_sides());
    let before = expected(&mut models, "2026-10-22T11:00:00Z");

    publish_pair(root.path(), "2026-10-22T10:30:00Z", 9);
    side_store(root.path(), &features(), &both_sides(), Side::Short).break_listing();
    let listing = models.list();

    assert!(listing.is_err());
    near(before, Some(r(7)));
    near(expected(&mut models, NOON), Some(r(7)));
}

/// A two-sided strategy decides on a pair, the sides one train published,
/// valid from one time. The long of 9 valid from 11:00 has no short beside
/// it: at noon the pair of 10:00 decides, its long of 7; once the short of
/// 11:00 is published too, the pair of 11:00 decides.
#[test]
fn a_side_published_without_the_other_is_never_used() {
    let root = tempfile::tempdir().unwrap();
    publish_pair(root.path(), "2026-10-22T10:00:00Z", 7);
    let eleven = "2026-10-22T11:00:00Z";
    publish_side(root.path(), Side::Long, &side_model(Side::Long, eleven, 9));
    let mut models = listed(root.path(), &both_sides());

    let alone = expected(&mut models, NOON);
    publish_side(
        root.path(),
        Side::Short,
        &side_model(Side::Short, eleven, 3),
    );
    models.list().unwrap();

    near(alone, Some(r(7)));
    near(expected(&mut models, "2026-10-22T13:00:00Z"), Some(r(9)));
}

/// A long published alone, with no pair before it, is no trade: the
/// strategy has no pair to decide on.
#[test]
fn a_two_sided_strategy_with_no_pair_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    publish_side(
        root.path(),
        Side::Long,
        &side_model(Side::Long, MIDNIGHT, 9),
    );

    assert_eq!(decided(&mut listed(root.path(), &both_sides()), NOON), None);
}

/// The pair valid from 10:00, of a long of 7, is in use at 11:00; the pair
/// of noon, its long of 9 beside the short `refused`, is refused as it
/// loads: the whole pair is, and the pair of 10:00 stays in use, at noon and
/// after, never the noon's long beside the short of 10:00.
fn pair_kept_over(refused: StoredModel) {
    let root = tempfile::tempdir().unwrap();
    publish_pair(root.path(), "2026-10-22T10:00:00Z", 7);
    publish_side(root.path(), Side::Long, &side_model(Side::Long, NOON, 9));
    publish_side(root.path(), Side::Short, &refused);
    let mut models = listed(root.path(), &both_sides());

    for time in [
        "2026-10-22T11:00:00Z",
        "2026-10-22T12:00:00Z",
        "2026-10-22T13:00:00Z",
    ] {
        near(expected(&mut models, time), Some(r(7)));
    }
}

#[test]
fn a_pair_with_a_side_failing_its_checks_is_refused_and_the_current_pair_kept() {
    let mut refused = side_model(Side::Short, NOON, 3);
    refused.manifest.fingerprint = "f".repeat(64);

    pair_kept_over(refused);
}

/// The pair's calibration is written once into each side's manifest: a pair
/// whose sides' copies differ is refused.
#[test]
fn a_pair_whose_sides_hold_different_calibrations_is_refused_and_the_current_pair_kept() {
    let mut refused = side_model(Side::Short, NOON, 3);
    let ModelCalibration::Pair(calibration) = &mut refused.manifest.calibration else {
        panic!("{:?}", refused.manifest.calibration);
    };
    calibration.lines.level.alpha = 0.1;

    pair_kept_over(refused);
}

/// The pair's calibration names the pair it was fitted on: a short whose copy
/// names the pair of 10:00, published beside the noon's long, is refused with
/// it, and one naming the long alone too.
#[test]
fn a_pair_whose_calibration_names_another_pair_is_refused_and_the_current_pair_kept() {
    let names = |valid_from: &str, sides: Vec<Side>| {
        let mut refused = side_model(Side::Short, NOON, 3);
        let mut long = side_model(Side::Long, NOON, 9);
        for model in [&mut refused, &mut long] {
            let ModelCalibration::Pair(calibration) = &mut model.manifest.calibration else {
                panic!("{:?}", model.manifest.calibration);
            };
            calibration.valid_from = at(valid_from);
            calibration.sides.clone_from(&sides);
        }
        (long, refused)
    };

    for (long, refused) in [
        names("2026-10-22T10:00:00Z", vec![Side::Long, Side::Short]),
        names(NOON, vec![Side::Long]),
    ] {
        let root = tempfile::tempdir().unwrap();
        publish_pair(root.path(), "2026-10-22T10:00:00Z", 7);
        publish_side(root.path(), Side::Long, &long);
        publish_side(root.path(), Side::Short, &refused);
        let mut models = listed(root.path(), &both_sides());

        near(expected(&mut models, "2026-10-22T11:00:00Z"), Some(r(7)));
        near(expected(&mut models, "2026-10-22T13:00:00Z"), Some(r(7)));
    }
}

/// The meta's store is the last listed: its failure keeps the sides' lists,
/// already read afresh, as they were.
#[test]
fn a_failed_listing_of_the_meta_keeps_the_sides_previous_lists() {
    let root = tempfile::tempdir().unwrap();
    let config = with_meta(meta());
    let set = model_set(root.path(), &features(), &config);
    let long = set.side(Side::Long).unwrap().store();
    long.publish(&baseline_for(
        &features(),
        &config,
        Side::Long,
        "2026-10-22T10:00:00Z",
        7,
    ))
    .unwrap();
    let mut models = DecidingModels::new(&set).unwrap();

    long.publish(&baseline_for(
        &features(),
        &config,
        Side::Long,
        "2026-10-22T10:30:00Z",
        9,
    ))
    .unwrap();
    set.meta().unwrap().store().break_listing();
    let listing = models.list();

    let picked = models.pick(at(NOON), &rows_of_one()).unwrap().unwrap();
    assert!(listing.is_err());
    assert_eq!(picked.expected_r, r(7));
}

#[test]
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

/// The model valid from 10:00 is used at 11:00; `refused`, valid from
/// 12:00, is not, at 12:00 nor after: the first is kept.
fn kept_over(refused: StoredModel) {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    store.publish(&refused).unwrap();
    let mut models = listed(root.path(), &config());

    let expectations = [
        "2026-10-22T11:00:00Z",
        "2026-10-22T12:00:00Z",
        "2026-10-22T13:00:00Z",
    ]
    .map(|time| expected(&mut models, time));

    assert_eq!(expectations, [Some(r(7)), Some(r(7)), Some(r(7))]);
}

#[test]
fn a_model_of_other_feature_names_is_refused_and_the_current_kept() {
    let mut refused = baseline(NOON, 9);
    refused.manifest.feature_names = vec!["spread".to_string()];

    kept_over(refused);
}

#[test]
fn a_model_of_another_fingerprint_is_refused_and_the_current_kept() {
    let mut refused = baseline(NOON, 9);
    refused.manifest.fingerprint = "f".repeat(64);

    kept_over(refused);
}

/// A side's rows are two wide, the mid and the entry cost: a model of three
/// is refused.
#[test]
fn a_model_of_another_width_is_refused_and_the_current_kept() {
    let mut refused = baseline(NOON, 9);
    let set = TrainingSet::new(3, vec![0.5; 6], vec![r(9); 2]).unwrap();
    refused.model = LearnerConfig::Baseline {}
        .fit(&set)
        .unwrap()
        .save()
        .unwrap();
    refused.manifest.probes.clear();

    kept_over(refused);
}

#[test]
fn a_model_not_predicting_its_probes_is_refused_and_the_current_kept() {
    let mut refused = baseline(NOON, 9);
    refused.manifest.probes[0].prediction = r(9) + f64::EPSILON;

    kept_over(refused);
}

/// A side model trades by its pair's calibration: one whose manifest has a
/// meta's instead is refused as it loads.
#[test]
fn a_side_model_without_its_pairs_calibration_is_refused_and_the_current_kept() {
    let mut refused = baseline(NOON, 9);
    refused.manifest.calibration = ModelCalibration::Meta(super::testing::CALIBRATION);

    kept_over(refused);
}

/// A long expecting 0.1R, clearing its margin of nought; the short is not
/// traded. One side has no side call.
#[test]
fn a_side_expected_to_clear_its_margin_is_picked_with_its_expected_r_level_and_side_call() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config(), (Some(6), None));

    assert_eq!(
        models.pick(at(NOON), &rows_of_one()).unwrap(),
        Some(Picked {
            side: Side::Long,
            expected_r: 0.1,
            expected: Expected {
                level: 0.1,
                side_call: 0.0
            },
        })
    );
}

/// The long expects 0.2R, the short 0.3R: both clear nought, the short
/// the more.
#[test]
fn of_two_sides_clearing_their_margins_the_one_expected_the_more_trades() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &both_sides(), (Some(7), Some(8)));

    let taken = decided(&mut models, NOON).unwrap();

    assert_eq!(taken.side, Side::Short);
    near(Some(taken.expected_r), Some(r(8)));
}

#[test]
fn two_sides_expected_alike_trade_neither() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &both_sides(), (Some(7), Some(7)));

    assert_eq!(decided(&mut models, NOON), None);
}

/// The long expects 0.2R, short of its margin of 0.5R; the short expects
/// to lose.
#[test]
fn no_side_clearing_its_margin_gives_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let config = ModelConfig {
        margins: Margins {
            long: Some(R::new(0.5)),
            short: Some(R::new(0.5)),
        },
        ..config()
    };
    let mut models = listed_with(root.path(), &config, (Some(7), Some(3)));

    assert_eq!(decided(&mut models, NOON), None);
}

/// A short model in the store expecting 0.4R, which a model of both sides
/// would sell on: a long-only model never reads it.
#[test]
fn a_long_only_model_never_sells() {
    let root = tempfile::tempdir().unwrap();
    let mut both = listed_with(root.path(), &both_sides(), (Some(3), Some(9)));
    let mut long = listed(root.path(), &config());

    assert_eq!(side_picked(&mut both, UNITS_OF_ONE), Some(Side::Short));
    assert_eq!(side_picked(&mut long, UNITS_OF_ONE), None);
}

/// Worked by hand. The long expects 0.4R, above its margin of 0.3R, on a
/// stop worth 0.90 USD as on one worth 0.70 USD, where it expects 0.28 USD:
/// the margin is in R, not in money.
#[test]
fn a_side_clears_its_margin_in_r_whatever_its_unit_is_worth() {
    let root = tempfile::tempdir().unwrap();
    let config = ModelConfig {
        margins: Margins {
            long: Some(R::new(0.3)),
            short: None,
        },
        ..config()
    };
    let mut models = listed_with(root.path(), &config, (Some(9), None));
    let worth = |long| PerSide { long, short: 1.0 };

    for long in [0.90, 0.70] {
        let picked = models.pick(at(NOON), &rows(worth(long))).unwrap();

        let expected = picked.map(|picked| (picked.side, picked.expected_r));
        assert!(
            expected.is_some_and(|(side, r)| side == Side::Long && (r - 0.4).abs() < 1e-12),
            "{long}: {expected:?}"
        );
    }
}

/// Worked by hand. The long expects 0.2R and the short 0.1R: the long is
/// picked at units of 1 USD, and at the short's unit of 3, where it expects
/// 0.3 USD, more than the long's 0.2.
#[test]
fn the_side_expected_the_more_r_is_picked_whatever_their_units() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &both_sides(), (Some(7), Some(6)));

    assert_eq!(side_picked(&mut models, UNITS_OF_ONE), Some(Side::Long));
    assert_eq!(
        side_picked(
            &mut models,
            PerSide {
                long: 1.0,
                short: 3.0
            }
        ),
        Some(Side::Long)
    );
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
#[test]
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
        (picked.expected_r, 0.3),
        (picked.expected.level, 0.225),
        (picked.expected.side_call, -0.15),
    ] {
        assert!((actual - expected).abs() < 1e-12, "{picked:?}");
    }
}

/// The short expects 0.3R, the more, short of its own margin of 0.35R: the
/// long, expecting 0.15R and clearing its margin of nought, is picked.
#[test]
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
#[test]
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

    assert!(two_for_one.is_err() && one_for_two.is_err());
}

/// `listed`, as `config` says, its long a model of 6 tenths from midnight
/// (expecting 0.1R, above its margin of nought) and its meta model
/// predicting an R of `r(tenths)`, if any, from midnight too.
fn with_meta_of(models: &Path, config: &ModelConfig, tenths: Option<usize>) -> DecidingModels {
    let features = features();
    let meta = tenths.map(|tenths| meta_baseline(&features, config, MIDNIGHT, tenths));
    published_with(models, config, (Side::Long, 6), meta.as_ref())
}

/// `listed`, as `config` says, its side a model of `side` predicting an R of
/// `r(tenths)` from midnight, and its meta model `meta`, if any.
fn published_with(
    models: &Path,
    config: &ModelConfig,
    (side, tenths): (Side, usize),
    meta: Option<&StoredModel>,
) -> DecidingModels {
    let features = features();
    let set = model_set(models, &features, config);
    set.side(side)
        .unwrap()
        .store()
        .publish(&baseline_for(&features, config, side, MIDNIGHT, tenths))
        .unwrap();
    if let Some(meta) = meta {
        set.meta().unwrap().store().publish(meta).unwrap();
    }
    DecidingModels::new(&set).unwrap()
}

/// Worked by hand: the meta predicting an R of 0.3, its level line `0.1 +
/// 0.5 E` expecting 0.25R, at or above its margin of 0.15R, takes the long
/// picked, at its calibrated expected R.
#[test]
fn a_meta_model_from_its_margin_takes_the_side_picked_at_its_expected_r() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(8));

    assert_eq!(
        decided(&mut models, NOON),
        Some(Taken {
            side: Side::Long,
            // The side's, its long of 6 tenths.
            expected_r: 0.1,
            expected: Expected {
                level: 0.1,
                side_call: 0.0
            },
            meta_expected_r: Some(0.25),
        })
    );
}

/// The meta predicting nought expects 0.1R, below its margin of 0.15R.
#[test]
fn a_meta_model_below_its_margin_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(5));

    assert_eq!(decided(&mut models, NOON), None);
}

/// Worked by hand: the meta expecting 0.1R, below its margin of 0.15R,
/// leaves the long on a unit worth 1 USD, and on one worth 2 USD too, where
/// it expects 0.2 USD: the margin is in R, not in money.
#[test]
fn a_meta_models_expected_r_is_held_to_its_margin_in_r_whatever_the_unit() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(5));
    let mut decided_at = |long| {
        models
            .decide(at(NOON), &ROW, &rows(PerSide { long, short: 1.0 }))
            .unwrap()
    };

    assert_eq!((decided_at(1.0), decided_at(2.0)), (None, None));
}

#[test]
fn no_meta_model_valid_yet_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), None);

    assert_eq!(decided(&mut models, NOON), None);
}

/// A pick is the side before any meta: the meta's examples learn from the
/// sides picked whether or not a meta takes them.
#[test]
fn a_pick_is_the_side_before_any_meta() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta()), Some(5));

    assert_eq!(side_picked(&mut models, UNITS_OF_ONE), Some(Side::Long));
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
/// that buys realise 1R from a side call of 0.5 and -1R below it, and sells
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
    let mut meta = meta_baseline(&features(), config, MIDNIGHT, 5);
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
    let features = features();
    let other = Side::BOTH.into_iter().find(|&other| other != side).unwrap();
    side_store(root.path(), &features, &config, other)
        .publish(&baseline_for(&features, &config, other, MIDNIGHT, 2))
        .unwrap();
    let mut models = published_with(root.path(), &config, (side, tenths), Some(&meta));
    decided(&mut models, NOON)
}

/// The meta model reads the decision's side call from its place in the row,
/// after its side and level: it takes a buy on a side call of 0.6, a long of
/// 0.3R beside a short of -0.3R, which it learned gains, and refuses one on
/// 0.4, a long of 0.1R, which it learned loses.
#[test]
fn a_meta_model_decides_on_the_side_call_of_the_decision() {
    let above = learned_meta_decision(Side::Long, 8);
    let below = learned_meta_decision(Side::Long, 6);

    assert_eq!(above.map(|taken| taken.side), Some(Side::Long));
    assert_eq!(below, None);
}

/// The meta model is given the side picked after its features: on a side
/// call of 0.6 it takes a buy, which it learned gains, and refuses a sell,
/// its side call -0.6, which it learned never does.
#[test]
fn a_meta_model_decides_on_the_side_picked() {
    let buy = learned_meta_decision(Side::Long, 8);
    let sell = learned_meta_decision(Side::Short, 8);

    assert_eq!(buy.map(|taken| taken.side), Some(Side::Long));
    assert_eq!(sell, None);
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

#[test]
fn a_nan_meta_expectation_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let config = with_meta(lightgbm_meta());
    let meta = predicting_nan(learned_meta(&config));
    let mut models = published_with(root.path(), &config, (Side::Long, 8), Some(&meta));

    assert_eq!(decided(&mut models, NOON), None);
}
