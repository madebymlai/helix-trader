//! Meta-labeling on a fixture catalog (`model::meta`), its primary a model
//! or a rule, every model the fake learner on a missing feature, so that
//! each model's probability is the mean of its training labels and names
//! it (`common::dataset::Missing`). Over a span of three days retrained
//! every midnight, the dataset is built, then each retrain trains the
//! primary and its meta model; in the run, a decision trades only if the
//! meta model in use takes the primary's trade.
//!
//! A model primary trades long at every decision. Its meta model's rows
//! carry the probability of the primary model in use at each, never of
//! the one trained at the meta's own retrain: out of sample. Its first
//! retrain has no primary model before it, so no meta model, and the
//! first day no trade.
//!
//! A rule primary, the probe's own, signals a long at even hours and a
//! short at odd ones, but none every third hour: the dataset has a row of
//! each signal only, its side among its features, and the run a trade of
//! each signal only, on its side.
//!
//! One node per thread: each build, train and run on a thread of its own.

use std::{cell::RefCell, path::Path, rc::Rc};

use crate::common::{
    dataset::{
        Missing, STRATEGY, backtest, commission, config, replay_on, rule, targets, trade_on, walk,
    },
    ic,
};
use helix_trader::model::{
    config::{MetaConfig, ModelConfig, RuleConfig, Thresholds},
    dataset::{self, Dataset},
    decide,
    features::FeatureSet,
    handle::{Mode, ModelHandle, SettledRows, Trade},
    learner::LearnerConfig,
    store::ModelStore,
    targets::TargetRow,
    train::{Kept, Outcome, Trained, Training},
    walk::WalkForward,
};
use jiff::{SignedDuration, Timestamp, tz::TimeZone};
use nautilus_common::{actor::DataActor, timer::TimeEvent};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OrderSide,
    events::{OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use tempfile::TempDir;

/// The missing feature, and, for a rule primary, its side and
/// probability.
fn features(rule: bool) -> Rc<FeatureSet> {
    let builder = FeatureSet::builder().quotes(
        InstrumentId::from(ic::INSTRUMENT_ID),
        "missing",
        Missing::default(),
    );
    if rule { builder.primary() } else { builder }
        .build()
        .unwrap()
}

/// A meta model on 46 hours, the last 12 held out, the fake learner,
/// published from an AUC of 0.5 (its probability the same on every row,
/// its AUC 0.5), taking every trade, to record its probability.
fn meta() -> MetaConfig {
    MetaConfig::builder()
        .window(SignedDuration::from_hours(46))
        .holdout(SignedDuration::from_hours(12))
        .threshold(0.0)
        .min_score(0.5)
        .learner(LearnerConfig::Fake {})
        .build()
}

/// The probe's model, retrained every day, a long at every decision, its
/// trades taken by `meta`.
fn model_primary() -> ModelConfig {
    ModelConfig {
        retrain: SignedDuration::from_hours(24),
        thresholds: Thresholds {
            long: f64::MIN_POSITIVE,
            short: 0.0,
        },
        meta: Some(meta()),
        ..config()
    }
}

/// The rule probe's section, retrained every day, its meta model on 36
/// hours, the last 18 held out: with two signals in three, of either
/// side, a holdout of 12 hours of the fixture has one label, and the
/// models of a window of 46 hours do not all differ.
fn rule_primary() -> RuleConfig {
    RuleConfig {
        decision: rule(),
        targets: targets(),
        retrain: SignedDuration::from_hours(24),
        meta: MetaConfig {
            window: SignedDuration::from_hours(36),
            holdout: SignedDuration::from_hours(18),
            ..meta()
        },
    }
}

/// Quotes from 23:00 of the 20th of October 2026 to 03:00 of the 26th:
/// the dataset's days, from the window of the first retrain, to the
/// span's end plus the longest target.
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

/// The prediction of the model of `store` valid from `at` on `row`.
fn predicts(store: &ModelStore, at: Timestamp, row: &[f64]) -> f64 {
    let stored = store.read(at).unwrap();
    let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
    model.predict(row).unwrap()
}

/// Prepares the backtest of the span of the model primary: the dataset
/// built, then the primary and its meta model trained at each retrain
/// time; what each train did.
fn prepare_model(catalog: &TempDir, models: &Path) -> Vec<(Timestamp, Trained)> {
    let (start, end) = span();
    let mut trained = vec![];
    WalkForward::new(
        catalog.path(),
        models,
        StrategyId::from(STRATEGY),
        &features(false),
        model_primary(),
        &commission(),
    )
    .unwrap()
    .prepare(
        start,
        end,
        |from, to| {
            on_thread(|| {
                let dataset = Dataset::new(
                    catalog.path(),
                    StrategyId::from(STRATEGY),
                    &features(false),
                    rule(),
                    targets(),
                    &commission(),
                )?;
                let (first, last) = dataset::days(from, to);
                dataset.build(first, last, |from, to| {
                    replay_on(catalog, features(false), from, to)
                })?;
                Ok(())
            })
        },
        |at| {
            let outcome = on_thread(|| {
                Training::new(
                    catalog.path(),
                    models,
                    StrategyId::from(STRATEGY),
                    &features(false),
                    model_primary(),
                    &commission(),
                )
                .unwrap()
                .train_at(at, |from, to| replay_on(catalog, features(false), from, to))
            });
            trained.push((at, outcome));
            Ok(())
        },
    )
    .unwrap();
    trained
}

#[test]
fn a_model_primarys_meta_model_learns_from_its_out_of_sample_predictions_and_takes_its_trades() {
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
            .all(|(_, trained)| matches!(trained.primary, Some(Outcome::Published { .. }))),
        "{trained:?}"
    );
    assert!(
        matches!(trained[0].1.meta, Some(Outcome::Kept(Kept::Failed(_)))),
        "no primary model before the first retrain: {:?}",
        trained[0].1
    );
    assert!(
        trained[1..].iter().all(|(_, trained)| trained.published()),
        "{trained:?}"
    );
    let primary = ModelStore::new(
        models.path(),
        StrategyId::from(STRATEGY),
        &features(false),
        &model_primary(),
        &commission(),
    )
    .unwrap();
    let meta = primary.meta(&meta(), model_primary().thresholds).unwrap();
    let means: Vec<f64> = retrains
        .iter()
        .map(|&at| predicts(&primary, at, &[f64::NAN]))
        .collect();
    assert!(
        means[0] != means[1] && means[1] != means[2],
        "the primary's models must differ to be told apart: {means:?}"
    );
    // Each meta model's holdout, the last 12 hours before its retrain, is
    // of the decisions of the primary model trained a day before: its
    // probability, never the one of the primary trained with the meta.
    for (i, &at) in retrains.iter().enumerate().skip(1) {
        let probes = meta.read(at).unwrap().manifest.probes;
        assert!(!probes.is_empty(), "{at}");
        for probe in probes {
            assert!(probe.row[0].is_nan(), "{probe:?}");
            assert_eq!(probe.row[1..], [1.0, means[i - 1]], "{at}: {probe:?}");
        }
    }

    let record = on_thread(|| {
        trade_on(
            &catalog,
            features(false),
            start,
            end,
            &model_primary(),
            models.path(),
        )
    });

    for (period, &from) in retrains.iter().enumerate() {
        let (from_ns, to_ns) = (UnixNanos::from(from), UnixNanos::from(from + day()));
        let trades: Vec<Trade> = record
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
        let taken = predicts(&meta, from, &[f64::NAN, 1.0, means[period]]);
        assert_eq!(trades.len(), 24, "period {period}");
        assert!(
            trades
                .iter()
                .all(|trade| trade.side == OrderSide::Buy && trade.probability == taken),
            "period {period}: {trades:?}, not longs at {taken}"
        );
    }
}

/// The rule: a long at even hours, a short at odd ones, none every third
/// hour.
fn signal(at: UnixNanos) -> Option<OrderSide> {
    let hour = Timestamp::from(at).to_zoned(TimeZone::UTC).hour();
    match hour {
        h if h % 3 == 0 => None,
        h if h % 2 == 0 => Some(OrderSide::Buy),
        _ => Some(OrderSide::Sell),
    }
}

/// Decides every hour through its model handle on the side of its rule,
/// when the rule signals; records the trades, which it does not send.
#[derive(Debug)]
struct RuleProbe {
    core: StrategyCore,
    model: Rc<ModelHandle>,
    trades: Trades,
}

/// The trades a rule probe recorded, and when.
type Trades = Rc<RefCell<Vec<(UnixNanos, Trade)>>>;

nautilus_strategy!(RuleProbe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.model.on_order_filled(event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.model.on_order_denied(&event);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.model.on_order_rejected(&event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        Rc::clone(&self.model).on_position_closed(self, &event);
    }
});

impl DataActor for RuleProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_quotes(InstrumentId::from(ic::INSTRUMENT_ID), None, None);
        Rc::clone(&self.model).start(self, &[])
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if !decide::is_decision(event) {
            return Rc::clone(&self.model).on_time_event(self, event);
        }
        let now = self.clock().timestamp_ns();
        if let Some(side) = signal(now)
            && let Some(trade) = Rc::clone(&self.model).decide_on(self, side)?
        {
            self.trades.borrow_mut().push((now, trade));
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.model.on_stop();
        Ok(())
    }
}

/// The rule probe in `mode`, and the trades it records.
fn rule_probe(mode: Mode) -> (RuleProbe, Trades) {
    let trades = Rc::new(RefCell::new(vec![]));
    let probe = RuleProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        model: ModelHandle::of_rule(
            StrategyId::from(STRATEGY),
            features(true),
            &rule_primary(),
            mode,
        )
        .unwrap(),
        trades: Rc::clone(&trades),
    };
    (probe, trades)
}

/// The rule probe's dataset run over `[from, to]`: the rows it settled.
fn replay_rule(
    catalog: &TempDir,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    let rows = SettledRows::default();
    let (probe, _) = rule_probe(Mode::Dataset {
        rows: Rc::clone(&rows),
    });
    backtest(catalog.path(), from, to).run_dataset(probe)?;
    Ok(rows.take())
}

fn rule_dataset(catalog: &TempDir) -> Dataset {
    Dataset::new(
        catalog.path(),
        StrategyId::from(STRATEGY),
        &features(true),
        rule(),
        targets(),
        &commission(),
    )
    .unwrap()
}

#[test]
fn a_rule_primarys_signals_alone_are_rows_and_trades_on_its_side_taken_by_its_meta_model() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let (start, end) = span();
    let retrains = [start, start + day(), start + day() * 2];
    let from = start - rule_primary().meta.window;

    let (first, last) = dataset::days(from, end);
    on_thread(|| {
        rule_dataset(&catalog)
            .build(first, last, |from, to| replay_rule(&catalog, from, to))
            .unwrap()
    });
    let trained: Vec<Trained> = retrains
        .iter()
        .map(|&at| {
            on_thread(|| {
                Training::of_rule(
                    catalog.path(),
                    models.path(),
                    StrategyId::from(STRATEGY),
                    &features(true),
                    rule_primary(),
                    &commission(),
                )
                .unwrap()
                .train_at(at, |from, to| replay_rule(&catalog, from, to))
            })
        })
        .collect();

    let rows = rule_dataset(&catalog).read(start, end).unwrap();
    assert_eq!(rows.len(), 3 * 16, "16 signals a day");
    for row in &rows {
        let side = signal(row.ts_event).expect("a row of a signal");
        let value = if side == OrderSide::Buy { 1.0 } else { -1.0 };
        assert_eq!(row.features[1], value, "{row:?}");
        assert!(
            row.features[0].is_nan() && row.features[2].is_nan(),
            "{row:?}"
        );
    }
    assert!(
        trained
            .iter()
            .all(|trained| trained.primary.is_none() && trained.published()),
        "{trained:?}"
    );

    let trades = on_thread(|| {
        let (probe, trades) = rule_probe(Mode::Backtest {
            models: models.path().to_path_buf(),
            commission: commission(),
        });
        backtest(catalog.path(), start, end).run(probe).unwrap();
        trades.take()
    });

    let store = ModelStore::of_rule(
        models.path(),
        StrategyId::from(STRATEGY),
        &features(true),
        &rule_primary(),
        &commission(),
    )
    .unwrap();
    let longs: Vec<f64> = retrains
        .iter()
        .map(|&at| predicts(&store, at, &[f64::NAN, 1.0, f64::NAN]))
        .collect();
    assert!(
        longs[0] != longs[1] && longs[1] != longs[2],
        "the meta models must differ to be told apart: {longs:?}"
    );
    assert_eq!(trades.len(), 3 * 16);
    for (time, trade) in &trades {
        let side = signal(*time).expect("a trade of a signal");
        let value = if side == OrderSide::Buy { 1.0 } else { -1.0 };
        let retrain = retrains
            .iter()
            .rfind(|&&at| UnixNanos::from(at) <= *time)
            .unwrap();
        let taken = predicts(&store, *retrain, &[f64::NAN, value, f64::NAN]);
        assert_eq!((trade.side, trade.probability), (side, taken), "at {time}");
    }
}
