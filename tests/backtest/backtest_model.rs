//! A strategy trading on its models (`model::handle`): the probe of
//! `common::dataset` decides every hour on the last mid of IC's gold,
//! trading in a backtest on the models of a store. On a fixed list of
//! baseline long models, each predicting its own probability whatever the
//! row, calibrated alike, each decision's probability names the model it
//! used: the latest valid at or before it. A model LightGBM fitted by a
//! train at t, reloaded in the node, predicts on the decisions of its
//! probe rows exactly as at training, calibrated.

use std::path::Path;

use crate::common::{
    dataset::{
        STRATEGY, commission, config, features, replay, requirements, rule, targets, trade, walk,
    },
    ic,
};
use helix_trader::model::{
    calibration::Calibration,
    config::{Margins, ModelConfig},
    dataset,
    learner::{LearnerConfig, LightGbmConfig, TrainingSet},
    payoff::Payoff,
    store::{ModelManifest, ModelStore, Probe, StoredModel},
    targets::Side,
    train::Training,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

fn store(models: &Path, config: &ModelConfig) -> ModelStore {
    ModelStore::new(
        models,
        StrategyId::from(STRATEGY),
        &features(),
        config,
        &requirements(),
        &commission(),
        Side::Long,
    )
    .unwrap()
}

/// The calibration of the baselines, `c(p) = 1 / (1 + e^(2 - 4p))`,
/// increasing: c(0.5) = 0.5.
const CALIBRATION: Calibration = Calibration { a: -4.0, b: 2.0 };

/// A baseline long model of the probe valid from `valid_from`, predicting
/// `trues` tenths whatever the row, calibrated by `CALIBRATION`, expecting
/// a win and a loss of 1R: above 0.5 a long expected to gain.
fn baseline(valid_from: Timestamp, trues: usize) -> StoredModel {
    let labels = (0..10).map(|i| i < trues).collect();
    let set = TrainingSet::new(1, vec![4000.0; 10], labels).unwrap();
    let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
    StoredModel {
        manifest: ModelManifest {
            strategy_id: StrategyId::from(STRATEGY),
            valid_from,
            fingerprint: dataset::fingerprint(
                &features(),
                rule(),
                &targets(),
                &requirements(),
                &commission(),
            )
            .unwrap()
            .to_string(),
            feature_names: vec!["mid".to_string()],
            targets: targets(),
            label: config().label,
            side: Some(Side::Long),
            learner: LearnerConfig::Baseline {},
            calibration: CALIBRATION,
            payoff: Some(Payoff {
                win: 1.0,
                loss: 1.0,
            }),
            score: 0.6,
            probes: vec![Probe {
                row: vec![4000.0],
                prediction: CALIBRATION.apply(model.predict(&[4000.0]).unwrap()),
            }],
        },
        model: model.save().unwrap(),
    }
}

/// Decisions from midnight of the 21st to 06:00, on the hour, 10 ms late;
/// models valid from 02:00 (0.7), 04:00 (0.8), 04:30
/// (0.9) and 09:00 (after the run), each probability calibrated. Before
/// 02:00 no model is valid: no trade.
#[test]
fn each_decision_uses_the_latest_model_valid_at_or_before_it() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("07:00:00"),
    );
    let models = tempfile::tempdir().unwrap();
    let config = ModelConfig {
        learner: LearnerConfig::Baseline {},
        ..config()
    };
    let store = store(models.path(), &config);
    for (valid_from, trues) in [
        ("02:00:00", 7),
        ("04:00:00", 8),
        ("04:30:00", 9),
        ("09:00:00", 9),
    ] {
        store.publish(&baseline(ic::at(valid_from), trues)).unwrap();
    }

    let record = trade(
        &catalog,
        ic::at("00:00:00"),
        ic::at("06:30:00"),
        &config,
        models.path(),
    );

    let decisions: Vec<UnixNanos> = record.rows.iter().map(|row| row.ts_event).collect();
    let hour = |h: &str| UnixNanos::from(ic::at(&format!("{h}:00:00.010")));
    assert_eq!(
        decisions,
        ["00", "01", "02", "03", "04", "05", "06"].map(hour)
    );
    let probabilities: Vec<(UnixNanos, f64)> = record
        .trades
        .iter()
        .map(|(time, trade)| (*time, trade.probability))
        .collect();
    assert_eq!(
        probabilities,
        [
            (hour("02"), CALIBRATION.apply(0.7)),
            (hour("03"), CALIBRATION.apply(0.7)),
            (hour("04"), CALIBRATION.apply(0.8)),
            (hour("05"), CALIBRATION.apply(0.9)),
            (hour("06"), CALIBRATION.apply(0.9)),
        ]
    );
}

/// A train at t (16:00 of the 25th) with LightGBM on a window of 72
/// hours, 48 before the calibration and the holdout, enough rows for its
/// leaves of 20 rows to split, published
/// whatever its score; the same model, valid from the holdout's start,
/// then trades over the holdout, whose decisions are the probe rows'. The
/// walk puts the holdout's mids on both sides of the fitted ones' split,
/// so the probes fall in different leaves: the check is that the trees
/// reloaded in the node give the bits they gave when fitted, on several
/// predictions.
#[test]
fn a_reloaded_model_predicts_in_the_node_as_its_probes() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(115),
    );
    let models = tempfile::tempdir().unwrap();
    let t = ic::at("00:00:00") + SignedDuration::from_hours(112);
    let config = ModelConfig {
        learner: LearnerConfig::LightGbm(LightGbmConfig::builder().num_iterations(20).build()),
        window: SignedDuration::from_hours(72),
        min_score: 0.0,
        // Every decision a long, to record its probability.
        margins: Margins {
            long: Some(f64::MIN),
            short: None,
        },
        ..config()
    };
    let mut model = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let training = Training::new(
                    catalog.path(),
                    models.path(),
                    StrategyId::from(STRATEGY),
                    &features(),
                    config.clone(),
                    &requirements(),
                    &commission(),
                )
                .unwrap();
                let outcome = training.train_at(t, |from, to| replay(&catalog, from, to));
                assert!(outcome.published(), "{outcome:?}");
                training.store(Side::Long).unwrap().read(t).unwrap()
            })
            .join()
            .unwrap()
    });
    let holdout = t - config.holdout;
    model.manifest.valid_from = holdout;
    store(models.path(), &config).publish(&model).unwrap();

    let record = trade(&catalog, holdout, t, &config, models.path());

    let probes = &model.manifest.probes;
    let mut predictions: Vec<u64> = probes.iter().map(|p| p.prediction.to_bits()).collect();
    predictions.sort_unstable();
    predictions.dedup();
    assert!(predictions.len() > 1, "{probes:?}");
    for probe in probes {
        let row = record
            .rows
            .iter()
            .find(|row| row.features == probe.row)
            .unwrap_or_else(|| panic!("no decision on the probe {probe:?}"));
        let (_, trade) = record
            .trades
            .iter()
            .find(|(time, _)| *time == row.ts_event)
            .unwrap_or_else(|| panic!("no prediction at {}", row.ts_event));
        assert_eq!(
            trade.probability.to_bits(),
            probe.prediction.to_bits(),
            "{probe:?}"
        );
    }
}
