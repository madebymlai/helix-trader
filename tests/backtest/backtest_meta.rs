//! Meta-labeling on a fixture catalog (`model::meta`), its primary a model
//! or a rule, the features a missing one (`common::dataset::Missing`).
//! Over a span of three days retrained every midnight, the dataset is
//! built and the models trained; the fixture's walk has no edge, so a
//! train refuses each model, and the models traded on are published
//! straight, baselines whose probabilities name them
//! (`common::dataset::baseline_of`). In the run, a decision trades only if
//! the meta model in use takes the primary's trade.
//!
//! A model primary of the long alone trades long at every decision. Its
//! meta model's first retrain has no primary model before it, so it waits
//! for primary decisions; the next ones hold out the primary's longs,
//! every one of which the meta's threshold takes, refused for earning no
//! more than every trade. With no meta model the first day, no trade.
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
        STRATEGY, backtest, baseline_of, config, meta_baseline_of, missing, replay_on,
        requirements, rule, targets, trade_on, venue, walk,
    },
    ic,
};
use helix_trader::model::{
    config::{Margins, MetaConfig, ModelConfig, RuleConfig},
    dataset::{self, Dataset},
    decide,
    features::FeatureSet,
    handle::{Decision, Mode, ModelHandle, SettledRows},
    holdout::Earnings,
    learner::LearnerConfig,
    store::ModelStore,
    targets::{Side, TargetRow},
    train::{Kept, Outcome, Trained, Training},
    walk::WalkForward,
};
use helix_trader::strategy::orders;
use jiff::{SignedDuration, Timestamp, tz::TimeZone};
use nautilus_common::{actor::DataActor, timer::TimeEvent};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OrderSide,
    events::{OrderCanceled, OrderDenied, OrderRejected, PositionClosed},
    identifiers::StrategyId,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use tempfile::TempDir;

/// The missing feature, and, for a rule primary, its side and
/// probability.
fn features(rule: bool) -> Rc<FeatureSet> {
    let builder = missing();
    if rule { builder.primary() } else { builder }
        .build()
        .unwrap()
}

/// A meta model on 46 hours, the last 6 held out and the 6 before them
/// calibrated on, the fake learner, published from nought per trade over
/// a trade, taking every trade, to record its probability.
fn meta() -> MetaConfig {
    MetaConfig::builder()
        .window(SignedDuration::from_hours(46))
        .holdout(SignedDuration::from_hours(6))
        .threshold(0.0)
        .min_pnl(0.0)
        .min_trades(1)
        .learner(LearnerConfig::Fake {})
        .build()
}

/// The probe's model of the long alone, retrained every day, a long at
/// every decision, its trades taken by `meta`.
fn model_primary() -> ModelConfig {
    ModelConfig {
        retrain: SignedDuration::from_hours(24),
        margins: Margins {
            long: Some(f64::MIN),
            short: None,
        },
        meta: Some(meta()),
        ..config()
    }
}

/// The rule probe's section, retrained every day, its meta model on 46
/// hours, the last 18 held out and the 18 before them calibrated on: with
/// two signals in three, of either side, a holdout of 12 hours of the
/// fixture has one label.
fn rule_primary() -> RuleConfig {
    RuleConfig {
        decision: rule(),
        targets: targets(),
        retrain: SignedDuration::from_hours(24),
        meta: MetaConfig {
            window: SignedDuration::from_hours(46),
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

/// The calibrated prediction of the model of `store` valid from `at` on
/// `row`, as a strategy of `features` loads it.
fn predicts(store: &ModelStore, features: &FeatureSet, at: Timestamp, row: &[f64]) -> f64 {
    let stored = store.read(at).unwrap();
    let fingerprint = dataset::fingerprint(features, rule(), &targets(), &requirements(), &venue())
        .unwrap()
        .to_string();
    stored
        .load(&stored.manifest.feature_names, &fingerprint)
        .unwrap()
        .predict(row)
        .unwrap()
}

/// Prepares the backtest of the span of the model primary: the dataset
/// built, then at each retrain time the primary and its meta model
/// trained, and the primary's long model published straight after, a
/// baseline of `trues` tenths (3 at the span's start and one more each
/// day), which the train would keep in use had it been published before;
/// what each train did.
fn prepare_model(catalog: &TempDir, models: &Path) -> Vec<(Timestamp, Trained)> {
    let (start, end) = span();
    let mut trained = vec![];
    WalkForward::new(
        catalog.path(),
        models,
        StrategyId::from(STRATEGY),
        &features(false),
        model_primary(),
        &requirements(),
        &venue(),
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
                    &requirements(),
                    &venue(),
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
                    &requirements(),
                    &venue(),
                )
                .unwrap()
                .train_at(at, |from, to| replay_on(catalog, features(false), from, to))
            });
            trained.push((at, outcome));
            let trues = 3 + (at.duration_since(start).as_hours() / 24) as usize;
            primary_store(models).publish(&baseline_of(
                &features(false),
                &model_primary(),
                Side::Long,
                at,
                trues,
            ))
        },
    )
    .unwrap();
    trained
}

/// The store of the model primary's long models under `models`.
fn primary_store(models: &Path) -> ModelStore {
    ModelStore::new(
        models,
        StrategyId::from(STRATEGY),
        &features(false),
        &model_primary(),
        &requirements(),
        &venue(),
        Side::Long,
    )
    .unwrap()
}

/// The store of the model primary's meta models under `models`.
fn meta_store(models: &Path) -> ModelStore {
    ModelStore::meta(
        models,
        StrategyId::from(STRATEGY),
        &features(false),
        &model_primary(),
        &meta(),
        &requirements(),
        &venue(),
    )
    .unwrap()
}

/// The fixture has no edge: a train refuses each model it fits, the
/// primary's and, from the second retrain, when the primary has a model
/// before it, the meta's, whose threshold takes every trade of its
/// holdout, the primary's longs, and so earns no more than every trade.
/// With meta models published straight, a day's decisions trade only as
/// the meta model in use takes the primary's.
#[test]
fn a_model_primarys_meta_model_trains_on_its_trades_and_takes_them_in_the_run() {
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
            .all(|(_, trained)| matches!(trained.long, Some(Outcome::Kept(Kept::Refused { .. })))),
        "{trained:?}"
    );
    assert_eq!(
        trained[0].1.meta,
        Some(Outcome::Kept(Kept::Waiting)),
        "no primary model before the first retrain: {:?}",
        trained[0].1
    );
    for (at, trained) in &trained[1..] {
        let Some(Outcome::Kept(Kept::Refused { holdout, .. })) = trained.meta else {
            panic!("{at}: {trained:?}");
        };
        let Earnings { taken, every, .. } = holdout.earnings;
        assert!(every.count > 0, "{at}: {holdout:?}");
        assert_eq!(taken, every, "{at}: every trade taken");
    }
    let primary = primary_store(models.path());
    let meta = meta_store(models.path());
    assert!(meta.valid_froms().unwrap().is_empty());
    let means: Vec<f64> = retrains
        .iter()
        .map(|&at| predicts(&primary, &features(false), at, &[f64::NAN]))
        .collect();
    for (&at, trues) in retrains[1..].iter().zip([6, 8]) {
        meta.publish(&meta_baseline_of(
            &features(false),
            &model_primary(),
            at,
            trues,
        ))
        .unwrap();
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
        let taken = predicts(
            &meta,
            &features(false),
            from,
            &[f64::NAN, 1.0, means[period]],
        );
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
type Trades = Rc<RefCell<Vec<(UnixNanos, Decision)>>>;

nautilus_strategy!(RuleProbe, {
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.model.on_order_canceled(self, event);
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
        Rc::clone(&self.model).start(self)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if !decide::is_decision(event) {
            if !orders::on_time_event(self, event)? {
                Rc::clone(&self.model).on_time_event(self, event)?;
            }
            return Ok(());
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
            &requirements(),
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
        &requirements(),
        &venue(),
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
    let trained = on_thread(|| {
        Training::of_rule(
            catalog.path(),
            models.path(),
            StrategyId::from(STRATEGY),
            &features(true),
            rule_primary(),
            &requirements(),
            &venue(),
        )
        .unwrap()
        .train_at(start, |from, to| replay_rule(&catalog, from, to))
    });

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
    // The fixture has no edge, and the threshold takes every trade of the
    // holdout: a meta model earns no more than every trade, and is refused.
    assert_eq!((&trained.long, &trained.short), (&None, &None));
    let Some(Outcome::Kept(Kept::Refused { holdout, .. })) = trained.meta else {
        panic!("{trained:?}");
    };
    let Earnings { taken, every, .. } = holdout.earnings;
    assert!(every.count > 0, "{holdout:?}");
    assert_eq!(taken, every);
    let store = ModelStore::of_rule(
        models.path(),
        StrategyId::from(STRATEGY),
        &features(true),
        &rule_primary(),
        &requirements(),
        &venue(),
    )
    .unwrap();
    assert!(store.valid_froms().unwrap().is_empty());
    for (&at, trues) in retrains.iter().zip([6, 7, 8]) {
        store
            .publish(&meta_baseline_of(&features(true), &config(), at, trues))
            .unwrap();
    }

    let trades = on_thread(|| {
        let (probe, trades) = rule_probe(Mode::Backtest {
            models: models.path().to_path_buf(),
            venue: venue(),
        });
        backtest(catalog.path(), start, end).run(probe).unwrap();
        trades.take()
    });

    let longs: Vec<f64> = retrains
        .iter()
        .map(|&at| predicts(&store, &features(true), at, &[f64::NAN, 1.0, f64::NAN]))
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
        let taken = predicts(
            &store,
            &features(true),
            *retrain,
            &[f64::NAN, value, f64::NAN],
        );
        assert_eq!((trade.side, trade.probability), (side, taken), "at {time}");
    }
}
