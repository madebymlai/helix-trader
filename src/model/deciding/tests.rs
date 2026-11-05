//! A decision on stores under a temp dir, with no strategy: the pick between
//! the sides, swapping, refused models, the meta's threshold and sizing.

use std::path::Path;

use nautilus_model::{enums::OrderSide, identifiers::StrategyId};
use rust_decimal::Decimal;

use super::{
    testing::{
        CALIBRATION, STRATEGY, at, baseline, baseline_for, c, config, features, meta,
        meta_baseline, model_set, requirements, side_store, store, venue, with_meta,
    },
    *,
};
use crate::model::{
    config::{Margins, ModelConfig, Sizing},
    dataset::{Commission, DatasetVenue},
    learner::{LearnerConfig, LightGbmConfig, TrainingSet},
    payoff::Payoff,
    store::StoredModel,
};

const MIDNIGHT: &str = "2026-10-22T00:00:00Z";
const NOON: &str = "2026-10-22T12:00:00Z";

/// The row decided on: the mid of 0.7, which a baseline does not read.
const ROW: [f64; 1] = [0.7];

/// The units of a time-only target, its payoffs being in money.
const IN_MONEY: PerSide<f64> = PerSide {
    long: 1.0,
    short: 1.0,
};

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
    for (side, trues) in [(Side::Long, long), (Side::Short, short)] {
        if let Some(trues) = trues {
            side_store(models, &features, config, side)
                .publish(&baseline_for(&features, config, side, MIDNIGHT, trues))
                .unwrap();
        }
    }
    listed(models, config)
}

/// What `models` decide at `time` on `ROW`, in money, of 1000.
fn decided(models: &mut DecidingModels, time: &str) -> Option<Taken> {
    models
        .decide(at(time), &ROW, IN_MONEY, Quantity::from(1000))
        .unwrap()
}

/// The probability `decided` decides at.
fn probability(models: &mut DecidingModels, time: &str) -> Option<f64> {
    decided(models, time).map(|taken| taken.probability)
}

/// The side `models` pick at noon on `ROW`, the units being `units`.
fn side_picked(models: &mut DecidingModels, units: PerSide<f64>) -> Option<Side> {
    models
        .pick(at(NOON), &ROW, units)
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
    let other_costs = ModelSet::new(
        models.path(),
        StrategyId::from(STRATEGY),
        &features(),
        &config(),
        &requirements(),
        &DatasetVenue {
            commission: Commission::PerOrder("3 USD".to_string()),
            ..venue()
        },
    )
    .unwrap();

    assert_eq!(
        decided(&mut DecidingModels::new(&other_costs).unwrap(), NOON),
        None
    );
    assert_eq!(
        probability(&mut listed(models.path(), &config()), NOON),
        Some(c(0.8))
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
    for (valid_from, trues) in [
        ("2026-10-22T10:00:00Z", 7),
        ("2026-10-22T12:00:00Z", 8),
        ("2026-10-22T13:00:00Z", 9),
    ] {
        store.publish(&baseline(valid_from, trues)).unwrap();
    }
    let mut models = listed(models.path(), &config());

    let probabilities = [
        "2026-10-22T11:00:00Z",
        "2026-10-22T12:00:00Z",
        "2026-10-22T12:59:59Z",
    ]
    .map(|time| probability(&mut models, time));

    assert_eq!(probabilities, [Some(c(0.7)), Some(c(0.8)), Some(c(0.8))]);
}

#[test]
fn a_model_published_after_a_decision_is_used_from_the_next() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());
    let before = probability(&mut models, NOON);

    // Live, a retrain at 12:00 publishes once its child finishes.
    store.publish(&baseline(NOON, 9)).unwrap();
    models.list().unwrap();

    assert_eq!(before, Some(c(0.7)));
    assert_eq!(
        probability(&mut models, "2026-10-22T13:00:00Z"),
        Some(c(0.9))
    );
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

    assert_eq!(
        probability(&mut models, "2026-10-22T13:00:00Z"),
        Some(c(0.7))
    );
}

#[test]
fn an_unreadable_store_fails_the_models_when_built() {
    let root = tempfile::tempdir().unwrap();
    store(root.path()).break_listing();

    let built = DecidingModels::new(&model_set(root.path(), &features(), &config()));

    assert!(built.is_err());
}

/// The long's new model, valid from 10:30, is in a store that lists, but the
/// short's fails: the failed listing keeps the long's list too, and a decision
/// uses no fresh list beside a stale one.
#[test]
fn a_failed_listing_keeps_every_sides_previous_list() {
    let root = tempfile::tempdir().unwrap();
    let long = store(root.path());
    let short = side_store(root.path(), &features(), &config(), Side::Short);
    long.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    let mut models = listed(root.path(), &config());
    let before = probability(&mut models, "2026-10-22T11:00:00Z");

    long.publish(&baseline("2026-10-22T10:30:00Z", 9)).unwrap();
    short.break_listing();
    let listing = models.list();

    assert!(listing.is_err());
    assert_eq!(before, Some(c(0.7)));
    assert_eq!(probability(&mut models, NOON), Some(c(0.7)));
}

/// The meta's store is the last listed: its failure keeps the sides' lists,
/// already read afresh, as they were.
#[test]
fn a_failed_listing_of_the_meta_keeps_the_sides_previous_lists() {
    let root = tempfile::tempdir().unwrap();
    let config = with_meta(meta(None));
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

    let picked = models.pick(at(NOON), &ROW, IN_MONEY).unwrap().unwrap();
    assert!(listing.is_err());
    assert_eq!(picked.probability, c(0.7));
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

    assert_eq!(probability(&mut models, NOON), Some(c(0.9)));
}

/// The model valid from 10:00 is used at 11:00; `refused`, valid from
/// 12:00, is not, at 12:00 nor after: the first is kept.
fn kept_over(refused: StoredModel) {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path());
    store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
    store.publish(&refused).unwrap();
    let mut models = listed(root.path(), &config());

    let probabilities = [
        "2026-10-22T11:00:00Z",
        "2026-10-22T12:00:00Z",
        "2026-10-22T13:00:00Z",
    ]
    .map(|time| probability(&mut models, time));

    assert_eq!(probabilities, [Some(c(0.7)), Some(c(0.7)), Some(c(0.7))]);
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

#[test]
fn a_model_of_another_width_is_refused_and_the_current_kept() {
    let mut refused = baseline(NOON, 9);
    let set = TrainingSet::new(2, vec![0.5; 4], vec![true, true]).unwrap();
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
    refused.manifest.probes[0].prediction = c(0.9) + f64::EPSILON;

    kept_over(refused);
}

/// A side model trades by its payoff: one whose manifest has none is
/// refused as it loads.
#[test]
fn a_side_model_without_a_payoff_is_refused_and_the_current_kept() {
    let mut refused = baseline(NOON, 9);
    refused.manifest.payoff = None;

    kept_over(refused);
}

/// A long expecting `2 c(0.6) - 1`, 0.180 USD, clearing its margin of
/// nought; the short has no model yet, and is not picked.
#[test]
fn a_side_expected_to_clear_its_margin_is_picked_with_its_probability_and_expected_pnl() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config(), (Some(6), None));

    assert_eq!(
        models.pick(at(NOON), &ROW, IN_MONEY).unwrap(),
        Some(Picked {
            side: Side::Long,
            probability: c(0.6),
            expected_pnl: 2.0 * c(0.6) - 1.0,
        })
    );
}

/// The long expects 0.364 USD, the short 0.554: both clear nought, the
/// short the more.
#[test]
fn of_two_sides_clearing_their_margins_the_one_expected_the_more_trades() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config(), (Some(7), Some(8)));

    let taken = decided(&mut models, NOON).unwrap();

    assert_eq!((taken.side, taken.probability), (Side::Short, c(0.8)));
}

#[test]
fn two_sides_expected_alike_trade_neither() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config(), (Some(7), Some(7)));

    assert_eq!(decided(&mut models, NOON), None);
}

/// The long expects 0.364 USD, short of its margin of 0.5 USD; the
/// short expects to lose.
#[test]
fn no_side_clearing_its_margin_gives_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let config = ModelConfig {
        margins: Margins {
            long: Some(0.5),
            short: Some(0.5),
        },
        ..config()
    };
    let mut models = listed_with(root.path(), &config, (Some(7), Some(3)));

    assert_eq!(decided(&mut models, NOON), None);
}

/// A short model in the store expecting 0.757 USD, which a model of
/// both sides would sell on: a long-only model never reads it.
#[test]
fn a_long_only_model_never_sells() {
    let root = tempfile::tempdir().unwrap();
    let long_only = ModelConfig {
        margins: Margins {
            long: Some(0.0),
            short: None,
        },
        ..config()
    };
    let mut both = listed_with(root.path(), &config(), (Some(3), Some(9)));
    let mut long = listed(root.path(), &long_only);

    assert_eq!(side_picked(&mut both, IN_MONEY), Some(Side::Short));
    assert_eq!(side_picked(&mut long, IN_MONEY), None);
}

/// Worked by hand. The long expects `2 c(0.8) - 1`, 0.554R, at either unit.
/// A stop worth 0.70 USD: 0.388 USD expected, above the margin of 0.30; a stop
/// worth 0.14 USD: 0.078 USD, below it.
#[test]
fn an_expected_r_is_weighed_by_the_unit_of_the_side() {
    let root = tempfile::tempdir().unwrap();
    let config = ModelConfig {
        margins: Margins {
            long: Some(0.3),
            short: None,
        },
        ..config()
    };
    let mut models = listed_with(root.path(), &config, (Some(8), None));
    let worth = |long| PerSide { long, short: 1.0 };

    let picked = models.pick(at(NOON), &ROW, worth(0.70)).unwrap().unwrap();

    assert!((picked.expected_pnl - 0.387_653).abs() < 1e-5, "{picked:?}");
    assert_eq!(side_picked(&mut models, worth(0.14)), None);
}

/// Worked by hand. The long expects 0.364R and the short 0.180R: at a unit of
/// 1 the long is picked, and at the short's unit of 3, worth 0.541 USD, the
/// short.
#[test]
fn each_side_is_weighed_by_its_own_unit() {
    let root = tempfile::tempdir().unwrap();
    let mut models = listed_with(root.path(), &config(), (Some(7), Some(6)));

    assert_eq!(side_picked(&mut models, IN_MONEY), Some(Side::Long));
    assert_eq!(
        side_picked(
            &mut models,
            PerSide {
                long: 1.0,
                short: 3.0
            }
        ),
        Some(Side::Short)
    );
}

/// A side's model predicting `trues` tenths, in no store.
fn in_hand(trues: usize) -> SideModel {
    let labels = (0..10).map(|i| i < trues).collect();
    let set = TrainingSet::new(1, vec![0.5; 10], labels).unwrap();
    let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
    SideModel::new(
        LoadedModel::new(model, CALIBRATION, trues as f64 / 10.0),
        Payoff {
            win: 1.0,
            loss: 1.0,
        },
    )
}

/// The long expects 0.364 USD, the short 0.554: of models in no store, the
/// short the more.
#[test]
fn models_in_hand_are_picked_among_with_no_store() {
    let (long, short) = (in_hand(7), in_hand(8));

    let picked = DecidingModels::pick_among(
        [
            Candidate {
                side: Side::Long,
                margin: 0.0,
                model: &long,
            },
            Candidate {
                side: Side::Short,
                margin: 0.0,
                model: &short,
            },
        ],
        &ROW,
        IN_MONEY,
    )
    .unwrap();

    assert_eq!(
        picked,
        Some(Picked {
            side: Side::Short,
            probability: c(0.8),
            expected_pnl: 2.0 * c(0.8) - 1.0,
        })
    );
}

/// The short expects 0.554 USD, the more, short of its own margin of 0.6: the
/// long, clearing its margin of nought, is picked.
#[test]
fn a_model_in_hand_is_held_to_its_own_margin() {
    let (long, short) = (in_hand(7), in_hand(8));

    let picked = DecidingModels::pick_among(
        [
            Candidate {
                side: Side::Long,
                margin: 0.0,
                model: &long,
            },
            Candidate {
                side: Side::Short,
                margin: 0.6,
                model: &short,
            },
        ],
        &ROW,
        IN_MONEY,
    )
    .unwrap();

    assert_eq!(picked.map(|picked| picked.side), Some(Side::Long));
}

/// `listed`, as `config` says, its long a model of 0.6 from midnight
/// (expecting 0.180 USD, above its margin of nought) and its meta model
/// predicting `trues` tenths, if any, from midnight too.
fn with_meta_of(models: &Path, config: &ModelConfig, trues: Option<usize>) -> DecidingModels {
    let features = features();
    let meta = trues.map(|trues| meta_baseline(&features, config, MIDNIGHT, trues));
    published_with(models, config, (Side::Long, 6), meta.as_ref())
}

/// `listed`, as `config` says, its side a model of `side` predicting `trues`
/// tenths from midnight, and its meta model `meta`, if any.
fn published_with(
    models: &Path,
    config: &ModelConfig,
    (side, trues): (Side, usize),
    meta: Option<&StoredModel>,
) -> DecidingModels {
    let features = features();
    let set = model_set(models, &features, config);
    set.side(side)
        .unwrap()
        .store()
        .publish(&baseline_for(&features, config, side, MIDNIGHT, trues))
        .unwrap();
    if let Some(meta) = meta {
        set.meta().unwrap().store().publish(meta).unwrap();
    }
    DecidingModels::new(&set).unwrap()
}

#[test]
fn a_meta_model_from_its_threshold_takes_the_side_picked_at_its_probability() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta(None)), Some(7));

    assert_eq!(
        decided(&mut models, NOON),
        Some(Taken {
            side: Side::Long,
            quantity: Quantity::from(1000),
            probability: c(0.7),
            // The side's, its long of 0.6.
            expected_pnl: 2.0 * c(0.6) - 1.0,
        })
    );
}

#[test]
fn a_meta_model_below_its_threshold_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta(None)), Some(5));

    assert_eq!(decided(&mut models, NOON), None);
}

#[test]
fn no_meta_model_valid_yet_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta(None)), None);

    assert_eq!(decided(&mut models, NOON), None);
}

/// A pick is the side before any meta: the meta's examples learn from the
/// sides picked whether or not a meta takes them.
#[test]
fn a_pick_is_the_side_before_any_meta() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta(None)), Some(5));

    assert_eq!(side_picked(&mut models, IN_MONEY), Some(Side::Long));
}

/// From the threshold 0.6 to 1, 0.5 to 2 times the size: the meta's
/// c(0.8), 0.776895, is 1.163358 times 1000.
#[test]
fn a_sizing_meta_model_sizes_the_trade_from_its_probability() {
    let root = tempfile::tempdir().unwrap();
    let sizing = Sizing {
        min: Decimal::new(5, 1),
        max: Decimal::from(2),
    };
    let mut models = with_meta_of(root.path(), &with_meta(meta(Some(sizing))), Some(8));

    let taken = decided(&mut models, NOON).unwrap();

    let quantity = taken.quantity.as_f64();
    assert!((quantity - 1_163.357_700).abs() < 1e-6, "{quantity}");
    assert_eq!(taken.probability, c(0.8));
}

/// A size of one decimal stays of one decimal, as sizing it would not.
#[test]
fn a_meta_model_not_sizing_keeps_the_sizes_precision() {
    let root = tempfile::tempdir().unwrap();
    let mut models = with_meta_of(root.path(), &with_meta(meta(None)), Some(7));
    let size = Quantity::from("1000.5");

    let taken = models
        .decide(at(NOON), &ROW, IN_MONEY, size)
        .unwrap()
        .unwrap();

    assert_eq!(
        (taken.quantity, taken.quantity.precision),
        (Quantity::from("1000.5"), 1)
    );
}

/// The meta model taking a trade from 0.6, learned by LightGBM at its
/// defaults.
fn lightgbm_meta() -> MetaConfig {
    MetaConfig {
        learner: LearnerConfig::LightGbm(LightGbmConfig::builder().build()),
        ..meta(None)
    }
}

/// A meta model of `config` valid from midnight of the 22nd, which learned on
/// rows of its features in their order, the mid, 0.7, then the side picked, 1
/// a buy and -1 a sell, then its probability, that buys win from a probability
/// of 0.7 and sells never: fitted by the learner of `config`'s meta on a buy
/// and a sell at each probability from 0.40 to 0.99 by 0.01, at least 30 rows
/// on each side of a split where LightGBM's leaves need 20, so that it
/// predicts near 1 on those wins and near 0 elsewhere, 0.88 and 0.12
/// calibrated, either side of the threshold.
fn learned_meta(config: &ModelConfig) -> StoredModel {
    let (mut rows, mut labels) = (vec![], vec![]);
    for side in [1.0, -1.0] {
        for hundredths in 40..100 {
            rows.extend([0.7, side, f64::from(hundredths) / 100.0]);
            labels.push(side > 0.0 && hundredths >= 70);
        }
    }
    let learner = &config.meta.as_ref().unwrap().learner;
    let fitted = learner
        .fit(&TrainingSet::new(3, rows, labels).unwrap())
        .unwrap();
    let mut meta = meta_baseline(&features(), config, MIDNIGHT, 5);
    meta.manifest.learner = learner.clone();
    meta.model = fitted.save().unwrap();
    meta
}

/// The decision at noon on a model of `side` predicting `trues` tenths, its
/// trade taken from 0.6 by the meta model LightGBM learned (`learned_meta`).
fn learned_meta_decision(side: Side, trues: usize) -> Option<Taken> {
    let root = tempfile::tempdir().unwrap();
    let config = with_meta(lightgbm_meta());
    let meta = learned_meta(&config);
    let mut models = published_with(root.path(), &config, (side, trues), Some(&meta));
    decided(&mut models, NOON)
}

/// The meta model reads the probability of the side picked from its place in
/// the row, after its side: it takes a buy at c(0.8) = 0.776895, which it
/// learned wins, and refuses one at c(0.6) = 0.590231, which it learned loses.
#[test]
fn a_meta_model_decides_on_the_probability_of_the_side_picked() {
    let above = learned_meta_decision(Side::Long, 8);
    let below = learned_meta_decision(Side::Long, 6);

    assert_eq!(above.map(|taken| taken.side), Some(Side::Long));
    assert_eq!(below, None);
}

/// The meta model is given the side picked after its features: at c(0.8) it
/// takes a buy, which it learned wins, and refuses a sell, which it learned
/// never does.
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
    let row = meta_row(&ROW, OrderSide::Buy, c(0.8));
    let loaded = meta
        .load(&meta.manifest.feature_names, &meta.manifest.fingerprint)
        .unwrap();
    assert!(loaded.predict(&row).unwrap().is_nan());
    meta
}

#[test]
fn a_nan_meta_probability_takes_no_trade() {
    let root = tempfile::tempdir().unwrap();
    let config = with_meta(lightgbm_meta());
    let meta = predicting_nan(learned_meta(&config));
    let mut models = published_with(root.path(), &config, (Side::Long, 8), Some(&meta));

    assert_eq!(decided(&mut models, NOON), None);
}
