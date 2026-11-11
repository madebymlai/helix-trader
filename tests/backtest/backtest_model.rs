//! A strategy trading on its models (`model::handle`): the probe of
//! `common::dataset` decides every hour on the last mid of IC's gold,
//! trading in a backtest on the models of a store. On a fixed list of
//! baseline long models of a strategy trading the long alone, each
//! predicting its own R whatever the row, its calibration leaving it as it
//! is, each decision's level names the model it used: the latest valid at
//! or before it. A LightGBM model fitted on the dataset's long rows'
//! realised R and published as a train would, reloaded in the node,
//! predicts on the decisions of its probe rows exactly as when fitted; and
//! one splitting on the payoff features alone predicts on every decision
//! as on the long's row the dataset holds for it, the row a decision reads
//! being the dataset's. The probe sends its
//! trades: on the hand-worked targets' quotes (`backtest_targets`), each
//! trade of a model of either side realises the P&L the dataset row of
//! its decision recorded of that side.

use std::path::Path;

use crate::{
    backtest_targets::hand_worked_quotes,
    common::{
        dataset::{
            STRATEGY, baseline_of, config, features, r, replay, replay_of, requirements, target,
            trade, venue, walk,
        },
        ic,
    },
};
use helix_trader::model::{
    config::{Margins, ModelConfig},
    dataset::DatasetRow,
    learner::{LearnerConfig, LightGbmConfig, TrainingSet},
    store::{ModelSet, ModelStore, Probe},
    targets::{BracketSpec, Distance, Side, Target, TargetConfig},
};
use jiff::SignedDuration;
use nautilus_core::UnixNanos;
use nautilus_model::{identifiers::StrategyId, types::Money};
use rust_decimal::Decimal;

fn store(models: &Path, config: &ModelConfig, side: Side) -> ModelStore {
    ModelSet::new(
        models,
        StrategyId::from(STRATEGY),
        &features(),
        config,
        &requirements(),
        &venue(),
    )
    .unwrap()
    .side(side)
    .unwrap()
    .store()
    .clone()
}

/// Decisions from midnight of the 21st to 06:00, on the hour, 10 ms late,
/// of a strategy trading the long alone; models valid from 02:00 (of 7
/// tenths, 0.2R), 04:00 (8), 04:30 (9) and 09:00 (after the run), each
/// decision's level its model's R, the strategy's one side calibrated to
/// the bit as it is. Before 02:00 no model is valid: no trade.
#[test]
fn each_decision_uses_the_latest_model_valid_at_or_before_it() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("07:00:00"),
    );
    let models = tempfile::tempdir().unwrap();
    let config = ModelConfig {
        learner: LearnerConfig::Baseline {},
        margins: Margins {
            long: Some(0.0),
            short: None,
        },
        ..config()
    };
    let store = store(models.path(), &config, Side::Long);
    for (valid_from, trues) in [
        ("02:00:00", 7),
        ("04:00:00", 8),
        ("04:30:00", 9),
        ("09:00:00", 9),
    ] {
        store
            .publish(&baseline_of(
                &features(),
                &config,
                Side::Long,
                ic::at(valid_from),
                trues,
            ))
            .unwrap();
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
    let levels: Vec<(UnixNanos, f64)> = record
        .trades
        .iter()
        .map(|(time, trade)| (*time, trade.level))
        .collect();
    assert_eq!(
        levels,
        [
            (hour("02"), r(7)),
            (hour("03"), r(7)),
            (hour("04"), r(8)),
            (hour("05"), r(9)),
            (hour("06"), r(9)),
        ]
    );
}

/// A LightGBM model fitted on the long's rows of the 72 hours before t
/// (16:00 of the 25th) but its last 12, enough for its leaves of 20 rows to
/// split, on each long's realised R, published as a train would, valid from
/// the last 12 hours, whose decisions' long rows are its probe rows and its
/// predictions on them; it then trades over them, each decision's level, the
/// strategy's one side calibrated as it is, its prediction to the bit. The
/// walk puts those mids on both sides of the fitted ones' split, so the
/// probes fall in different leaves: the check is that the trees reloaded in
/// the node give the bits they gave when fitted, on several predictions. The
/// walk has no edge to earn, so no train would publish it: it is published
/// straight.
#[test]
fn a_reloaded_model_predicts_in_the_node_as_its_probes() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(115),
    );
    let models = tempfile::tempdir().unwrap();
    let t = ic::at("00:00:00") + SignedDuration::from_hours(112);
    let learner = LearnerConfig::LightGbm(
        LightGbmConfig::builder()
            .num_iterations(20)
            .min_data_in_leaf(20)
            .build(),
    );
    let config = ModelConfig {
        learner: learner.clone(),
        window: SignedDuration::from_hours(72),
        // Every decision a long, to record its prediction.
        margins: Margins {
            long: Some(f64::MIN),
            short: None,
        },
        ..config()
    };
    let holdout = t - config.holdout;
    let rows = std::thread::scope(|scope| {
        scope
            .spawn(|| replay(&catalog, t - config.window, t).unwrap())
            .join()
            .unwrap()
    });
    let (fitted, held_out): (Vec<_>, Vec<_>) = rows
        .iter()
        .filter(|row| row.settled_at <= UnixNanos::from(t))
        .map(DatasetRow::of)
        .partition(|row| row.ts_event < UnixNanos::from(holdout));
    let long = |row: &DatasetRow| row.side(Side::Long).features;
    let set = TrainingSet::regression(
        4,
        fitted.iter().flat_map(long).collect(),
        fitted
            .iter()
            .map(|row| row.long_pnl / row.long_unit)
            .collect(),
    )
    .unwrap();
    let fitted = learner.fit(&set).unwrap();
    let mut model = baseline_of(&features(), &config, Side::Long, holdout, 5);
    model.manifest.learner = learner;
    model.manifest.probes = held_out
        .iter()
        .map(|row| Probe {
            row: long(row),
            prediction: fitted.predict(&long(row)).unwrap(),
        })
        .collect();
    model.model = fitted.save().unwrap();
    store(models.path(), &config, Side::Long)
        .publish(&model)
        .unwrap();

    let record = trade(&catalog, holdout, t, &config, models.path());

    let probes = &model.manifest.probes;
    let mut predictions: Vec<u64> = probes.iter().map(|p| p.prediction.to_bits()).collect();
    predictions.sort_unstable();
    predictions.dedup();
    assert!(predictions.len() > 1, "{probes:?}");
    for probe in probes {
        // The decision whose mid is the probe's first feature.
        let row = record
            .rows
            .iter()
            .find(|row| row.features[..] == probe.row[..1])
            .unwrap_or_else(|| panic!("no decision on the probe {probe:?}"));
        let (_, trade) = record
            .trades
            .iter()
            .find(|(time, _)| *time == row.ts_event)
            .unwrap_or_else(|| panic!("no prediction at {}", row.ts_event));
        assert_eq!(
            trade.level.to_bits(),
            probe.prediction.to_bits(),
            "{probe:?}"
        );
    }
}

/// A decision reads each side's row as the dataset holds it for the same
/// decision: the decision's features, then the side's payoff features. A
/// LightGBM model of the long's rows of the 72 hours before t (16:00 of the
/// 25th), their first feature, the mid, zeroed so that its trees split on
/// the payoff features alone, learning whether the take profit's R is above
/// its median, 1 or nought, is published straight; trading over the last 12
/// hours, every decision with a dataset row has for its level the model's
/// prediction on the long's row of that dataset row, to the bit, and the
/// predictions are not all one, so what they read of the payoff is what the
/// rows hold.
#[test]
fn a_decision_reads_each_sides_row_as_its_dataset_row_holds_it() {
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(115),
    );
    let models = tempfile::tempdir().unwrap();
    let t = ic::at("00:00:00") + SignedDuration::from_hours(112);
    let learner = LearnerConfig::LightGbm(
        LightGbmConfig::builder()
            .num_iterations(20)
            .min_data_in_leaf(20)
            .build(),
    );
    let config = ModelConfig {
        learner: learner.clone(),
        window: SignedDuration::from_hours(72),
        // Every decision a long, to record its prediction.
        margins: Margins {
            long: Some(f64::MIN),
            short: None,
        },
        ..config()
    };
    let holdout = t - config.holdout;
    let rows: Vec<DatasetRow> = std::thread::scope(|scope| {
        scope
            .spawn(|| replay(&catalog, t - config.window, t).unwrap())
            .join()
            .unwrap()
    })
    .iter()
    .map(DatasetRow::of)
    .collect();
    // A long's row: the mid, then its take profit's R, its stop's nominal
    // loss and its entry cost.
    let long_rows: Vec<Vec<f64>> = rows
        .iter()
        .map(|row| row.side(Side::Long).features)
        .collect();
    let mut take_profits: Vec<f64> = long_rows.iter().map(|row| row[1]).collect();
    take_profits.sort_by(f64::total_cmp);
    let median = take_profits[take_profits.len() / 2];
    let set = TrainingSet::regression(
        4,
        long_rows
            .iter()
            .flat_map(|row| [0.0, row[1], row[2], row[3]])
            .collect(),
        long_rows
            .iter()
            .map(|row| f64::from(u8::from(row[1] > median)))
            .collect(),
    )
    .unwrap();
    let fitted = learner.fit(&set).unwrap();
    let mut model = baseline_of(&features(), &config, Side::Long, holdout, 5);
    model.manifest.learner = learner;
    model.manifest.probes.clear();
    model.model = fitted.save().unwrap();
    store(models.path(), &config, Side::Long)
        .publish(&model)
        .unwrap();

    let record = trade(&catalog, holdout, t, &config, models.path());

    let mut read = vec![];
    for (time, decision) in &record.trades {
        let Some(row) = rows.iter().find(|row| row.ts_event == *time) else {
            continue;
        };
        let expected = fitted.predict(&row.side(Side::Long).features).unwrap();
        assert_eq!(
            decision.level.to_bits(),
            expected.to_bits(),
            "at {time}: {decision:?}, the dataset's {row:?}"
        );
        read.push(expected.to_bits());
    }
    assert!(read.len() >= 6, "{} decisions with a row", read.len());
    read.sort_unstable();
    read.dedup();
    assert!(read.len() > 1, "{read:?}");
}

/// The hand-worked targets' bracket (`backtest_targets`): 7.5 bps of the
/// decision's mid away on both sides, 3.00 USD on one ounce of gold,
/// closed after 45 minutes, inside the hourly cadence; trading `side` alone
/// at every decision.
fn hand_worked(side: Side) -> ModelConfig {
    let traded = Some(f64::MIN);
    let margins = match side {
        Side::Long => Margins {
            long: traded,
            short: None,
        },
        Side::Short => Margins {
            long: None,
            short: traded,
        },
    };
    let bps = Distance::BasisPoints(Decimal::new(75, 1));
    ModelConfig {
        target: TargetConfig {
            spec: Target::builder()
                .bracket(BracketSpec {
                    take_profit: bps,
                    stop: bps,
                    atr: None,
                })
                .limit(SignedDuration::from_mins(45))
                .build(),
            ..target()
        },
        learner: LearnerConfig::Baseline {},
        margins,
        ..config()
    }
}

/// Worked by hand on the quotes of the hand-worked targets: a dip at
/// 09:20, a spike at 10:45:00.010 and a rise from 11:20. Over 08:30 to
/// 12:30 a model of one side, trading every decision on its bracket,
/// realises at the decisions of 09:00, 10:00 and 11:00 the P&L the
/// dataset row of that decision recorded of that side: the long stopped
/// (-3.64 USD), taking profit (2.66) and closed by time (0.56), the short
/// taking profit (2.66), stopped (-3.64) and closed by time (-1.44). The
/// 12:00 decision's trade is still open at the end, as its row is
/// unsettled.
#[test]
fn a_model_strategy_realises_per_trade_the_pnl_its_dataset_row_recorded() {
    let catalog = ic::catalog_of(hand_worked_quotes(), ic::at("08:00:00"), ic::at("13:00:00"));
    let (from, to) = (ic::at("08:30:00"), ic::at("12:30:00"));
    let decision = |hour: &str| UnixNanos::from(ic::at(&format!("{hour}:00:00.010")));
    let rows = std::thread::scope(|scope| {
        scope
            .spawn(|| replay_of(&catalog, features(), &hand_worked(Side::Long), from, to))
            .join()
            .unwrap()
            .unwrap()
    });

    for (side, pnls) in [
        (Side::Long, ["-3.64 USD", "2.66 USD", "0.56 USD"]),
        (Side::Short, ["2.66 USD", "-3.64 USD", "-1.44 USD"]),
    ] {
        let config = hand_worked(side);
        let models = tempfile::tempdir().unwrap();
        store(models.path(), &config, side)
            .publish(&baseline_of(&features(), &config, side, from, 8))
            .unwrap();
        let record = std::thread::scope(|scope| {
            scope
                .spawn(|| trade(&catalog, from, to, &config, models.path()))
                .join()
                .unwrap()
        });

        let expected: Vec<(UnixNanos, Money)> = ["09", "10", "11"]
            .into_iter()
            .zip(pnls)
            .map(|(hour, pnl)| (decision(hour), Money::from(pnl)))
            .collect();
        let recorded: Vec<(UnixNanos, Money)> = rows
            .iter()
            .map(|row| {
                let outcome = match side {
                    Side::Long => row.outcomes.long,
                    Side::Short => row.outcomes.short,
                };
                (row.decision.ts_event, outcome.pnl)
            })
            .collect();
        assert_eq!(recorded, expected, "{side:?}");
        assert_eq!(record.closed, expected, "{side:?}");
    }
}
