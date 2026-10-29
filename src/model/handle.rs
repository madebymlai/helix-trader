//! The model handle: what a strategy trading on a learned model holds, the
//! one place it decides, in a backtest, live and a dataset run alike.
//!
//! The strategy shares a `ModelHandle` (an `Rc`), built from its features
//! and the model section of its config, in a `Mode`. `start` from
//! `on_start` registers the features (and the targets' ATR, in a dataset
//! run) and starts the decision cadence; `decide`, at each decision (the
//! cadence's timer, `decide::is_decision`, or an event of the strategy's
//! choosing), snapshots the features, if every one is initialized, and
//! publishes their row (`decide::decide`), then:
//!
//! - trading (`Mode::Backtest`, `Mode::Live`): takes the latest model of
//!   the store (`store::ModelStore`) valid at the decision, predicts on the
//!   row and returns the trade the config's thresholds make of the
//!   probability (`Thresholds::side`), of the targets' size. No model
//!   valid yet, cold features, or a probability between the thresholds
//!   are no trade, never an error.
//! - in a dataset run (`Mode::Dataset`): opens the decision's targets
//!   (`targets::Targets`) and returns no trade, so the strategy places
//!   none of its own; the strategy hands it its order, position and time
//!   events, and the rows the targets settle are collected for the caller
//!   of the run.
//!
//! A model is swapped in only inside `decide`, before it predicts: no
//! decision mixes two models. A backtest and a live node read the same
//! store, listed again at each decision, so a model published meanwhile
//! is used from the first decision at or after its valid-from. Before use
//! a model is checked against the strategy: its feature names and their
//! order, its fingerprint (`dataset::fingerprint`), its width, and its
//! probes, each of which the reloaded model must predict to the bit as it
//! did when fitted. A model failing a check is refused, logged as an
//! error, and the current one kept; it is not read again.
//!
//! A live node builds composite bars `warmup::live_bar_build_delay` after
//! their close, so a live handle starts the cadence with that delay, a
//! backtest's and a dataset run's with none (see `decide`).
//!
//! Live, the handle also retrains its model (`launcher::Retraining`): the
//! retrain timer, keyed by the strategy, launches a train at each retrain
//! time, one at a time, and `decide` first polls the running one, so the
//! model it publishes is used from the next decision.

use std::{cell::RefCell, path::PathBuf, rc::Rc};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_model::{
    data::{BarType, CustomData},
    enums::OrderSide,
    events::{OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::StrategyId,
    types::Quantity,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{
    config::{ModelConfig, Thresholds},
    dataset,
    decide::{self, DecisionRule},
    features::FeatureSet,
    launcher::{Launcher, Retraining},
    learner::Model,
    store::ModelStore,
    targets::{TargetRow, Targets},
};
use crate::strategy::warmup;

/// Where a handle runs, and so what it does at a decision.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Trades in a backtest on the models of the store under `models`.
    Backtest { models: PathBuf },
    /// Trades live on the models of the store under `models`, deciding
    /// the live node's bar build delay late, and retrains on the
    /// config's interval through `launcher`, each retrain launched
    /// `catalog_delay` after its time: once the data the node received up
    /// to then is in the catalog (`live::Recording::catalog_delay`).
    Live {
        models: PathBuf,
        launcher: Launcher,
        catalog_delay: SignedDuration,
    },
    /// Opens each decision's targets, for a dataset run, and collects the
    /// rows they settle in `rows`.
    Dataset { rows: SettledRows },
}

/// The rows a dataset run settled, shared with its caller.
pub type SettledRows = Rc<RefCell<Vec<TargetRow>>>;

/// A trade a decision makes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    pub side: OrderSide,
    /// The targets' size.
    pub quantity: Quantity,
    /// The model's probability that the label is true.
    pub probability: f64,
}

/// A strategy's model handle, shared with it as an `Rc`.
#[derive(Debug)]
pub struct ModelHandle {
    strategy_id: StrategyId,
    features: Rc<FeatureSet>,
    rule: DecisionRule,
    serving: Serving,
}

#[derive(Debug)]
enum Serving {
    Trading(Box<Trading>),
    Dataset {
        targets: Rc<Targets>,
        rows: SettledRows,
    },
}

/// The trading side of a handle.
#[derive(Debug)]
struct Trading {
    /// Live, the retrains; none in a backtest.
    retraining: Option<Retraining>,
    store: ModelStore,
    /// The feature names a model must carry, in order.
    feature_names: Vec<String>,
    /// The fingerprint a model must carry.
    fingerprint: String,
    thresholds: Thresholds,
    quantity: Quantity,
    current: RefCell<Current>,
}

/// The model in use, and the last model read.
#[derive(Debug, Default)]
struct Current {
    model: Option<Model>,
    /// The valid-from of the last model read, in use or refused.
    read: Option<Timestamp>,
}

impl ModelHandle {
    /// The handle of `strategy_id`, deciding on `features` as `config`
    /// says, in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule is invalid, the store cannot
    /// be keyed or, in a dataset run, the targets are invalid.
    pub fn new(
        strategy_id: StrategyId,
        features: Rc<FeatureSet>,
        config: &ModelConfig,
        mode: Mode,
    ) -> anyhow::Result<Rc<Self>> {
        config.decision.validate()?;
        let serving = match mode {
            Mode::Backtest { models } => Serving::Trading(Box::new(Trading::new(
                None,
                strategy_id,
                &features,
                config,
                models,
            )?)),
            Mode::Live {
                models,
                launcher,
                catalog_delay,
            } => {
                let retraining =
                    Retraining::new(strategy_id, config.retrain, catalog_delay, launcher);
                Serving::Trading(Box::new(Trading::new(
                    Some(retraining),
                    strategy_id,
                    &features,
                    config,
                    models,
                )?))
            }
            Mode::Dataset { rows } => Serving::Dataset {
                targets: Targets::new(config.targets.clone())?,
                rows,
            },
        };
        Ok(Rc::new(Self {
            strategy_id,
            features,
            rule: config.decision,
            serving,
        }))
    }

    /// Starts the handle on `strategy`, reading `bar_types`, from
    /// `on_start`: registers the features (and, in a dataset run, the
    /// targets' ATR, and collects the rows they settle), then starts the
    /// decision cadence, live `warmup::live_bar_build_delay(bar_types)`
    /// late, and live the retrain timer.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus refuses the cadence's timer or the
    /// retrain timer.
    pub fn start<S>(&self, strategy: &mut S, bar_types: &[BarType]) -> anyhow::Result<()>
    where
        S: DataActor + DataActorNative,
    {
        self.features.register(strategy);
        let delay = match &self.serving {
            Serving::Trading(trading) => match &trading.retraining {
                Some(retraining) => {
                    retraining.start(strategy)?;
                    warmup::live_bar_build_delay(bar_types)
                }
                None => SignedDuration::ZERO,
            },
            Serving::Dataset { targets, rows } => {
                targets.register(strategy);
                let rows = Rc::clone(rows);
                let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
                    if let Some(row) = data.data.as_any().downcast_ref::<TargetRow>() {
                        rows.borrow_mut().push(row.clone());
                    }
                });
                let topic = get_custom_topic(&TargetRow::data_type(self.strategy_id));
                msgbus::subscribe_any(topic.into(), handler, None);
                SignedDuration::ZERO
            }
        };
        decide::start(strategy, self.rule, delay)
    }

    /// Decides now, for `strategy`: the trade of the decision, if any.
    /// Live, first logs the running retrain if it finished, whose model
    /// the store then lists.
    ///
    /// # Errors
    ///
    /// Returns an error if the model fails to predict, or, in a dataset
    /// run, as `Targets::open` does.
    pub fn decide<S>(&self, strategy: &mut S) -> anyhow::Result<Option<Trade>>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        if let Serving::Trading(trading) = &self.serving
            && let Some(retraining) = &trading.retraining
        {
            retraining.poll();
        }
        let Some(row) = decide::decide(strategy, &self.features) else {
            return Ok(None);
        };
        match &self.serving {
            Serving::Trading(trading) => {
                trading.trade(Timestamp::from(row.ts_event), &row.features)
            }
            Serving::Dataset { targets, .. } => {
                targets.open(strategy, row)?;
                Ok(None)
            }
        }
    }

    /// Hands `event` to the targets of a dataset run, or live to the
    /// retrains, which launch one on their timer's; a no-op in a backtest.
    ///
    /// # Errors
    ///
    /// Returns an error as `Targets::on_time_event` does.
    pub fn on_time_event<S>(&self, strategy: &mut S, event: &TimeEvent) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        match &self.serving {
            Serving::Trading(trading) => {
                if let Some(retraining) = &trading.retraining {
                    retraining.on_time_event(event);
                }
                Ok(())
            }
            Serving::Dataset { targets, .. } => targets.on_time_event(strategy, event),
        }
    }

    /// Hands `event` to the targets of a dataset run; a no-op trading.
    pub fn on_order_filled(&self, event: &OrderFilled) {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_order_filled(event);
        }
    }

    /// Hands `event` to the targets of a dataset run; a no-op trading.
    pub fn on_order_denied(&self, event: &OrderDenied) {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_order_denied(event);
        }
    }

    /// Hands `event` to the targets of a dataset run; a no-op trading.
    pub fn on_order_rejected(&self, event: &OrderRejected) {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_order_rejected(event);
        }
    }

    /// Hands `event` to the targets of a dataset run; a no-op trading.
    pub fn on_position_closed<S>(&self, strategy: &S, event: &PositionClosed)
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_position_closed(strategy, event);
        }
    }

    /// Reports the targets of a dataset run unsettled at the stop; a no-op
    /// trading.
    pub fn on_stop(&self) {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_stop();
        }
    }
}

impl Trading {
    fn new(
        retraining: Option<Retraining>,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &ModelConfig,
        models: PathBuf,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            retraining,
            store: ModelStore::new(&models, strategy_id, features, config)?,
            feature_names: features.names().to_vec(),
            fingerprint: dataset::fingerprint(features, config.decision, &config.targets)
                .to_string(),
            thresholds: config.thresholds,
            quantity: config.targets.quantity,
            current: RefCell::default(),
        })
    }

    /// The trade of the decision at `now` on `row`: the latest model valid
    /// then swapped in, its prediction on `row` through the thresholds.
    fn trade(&self, now: Timestamp, row: &[f64]) -> anyhow::Result<Option<Trade>> {
        self.swap(now);
        let current = self.current.borrow();
        let Some(model) = &current.model else {
            return Ok(None);
        };
        let probability = model.predict(row)?;
        Ok(self.thresholds.side(probability).map(|side| Trade {
            side,
            quantity: self.quantity,
            probability,
        }))
    }

    /// Swaps in the latest model valid at `now`, if not read yet; one
    /// failing its checks is refused, logged, and the current one kept, as
    /// it is if the store cannot be listed.
    fn swap(&self, now: Timestamp) {
        let latest = match self.store.valid_froms() {
            Ok(times) => times.into_iter().rfind(|&time| time <= now),
            Err(e) => {
                log::error!("listing the models at {now}: {e:#}; the current model kept");
                return;
            }
        };
        let mut current = self.current.borrow_mut();
        let Some(latest) = latest.filter(|&latest| current.read != Some(latest)) else {
            return;
        };
        current.read = Some(latest);
        match self.load(latest) {
            Ok(model) => {
                log::info!("the model valid from {latest} in use from {now}");
                current.model = Some(model);
            }
            Err(e) => {
                log::error!("refused the model valid from {latest}: {e:#}; the current model kept");
            }
        }
    }

    /// The model valid from `valid_from`, checked against the strategy.
    fn load(&self, valid_from: Timestamp) -> anyhow::Result<Model> {
        self.store
            .read(valid_from)?
            .load(&self.feature_names, &self.fingerprint)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use nautilus_common::{
        actor::indicators::ActorIndicator,
        cache::Cache,
        clock::{Clock, VirtualClock},
        timer::TimeEventCallback,
    };
    use nautilus_core::UnixNanos;
    use nautilus_indicators::average::sma::SimpleMovingAverage;
    use nautilus_model::{
        data::QuoteTick,
        enums::PriceType,
        identifiers::{InstrumentId, TraderId},
        types::Price,
    };
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };

    use super::*;
    use crate::model::{
        learner::{LearnerConfig, TrainingSet},
        store::{ModelManifest, Probe, StoredModel},
        targets::{Label, TargetsConfig},
    };

    const STRATEGY: &str = "HANDLE-001";
    const AUD: &str = "AUD/USD.SIM";

    #[derive(Debug)]
    struct Decider {
        core: StrategyCore,
    }

    nautilus_strategy!(Decider);

    impl DataActor for Decider {}

    /// A strategy registered without a node at `now`, as the EMA's tests
    /// do, and its clock.
    fn registered(now: Timestamp) -> (Decider, Rc<RefCell<VirtualClock>>) {
        let mut probe = Decider {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(STRATEGY)),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
        };
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(UnixNanos::from(now), true);
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        probe
            .core
            .register(TraderId::from("HELIX-001"), clock.clone(), cache, portfolio)
            .unwrap();
        (probe, clock)
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn advance(clock: &Rc<RefCell<VirtualClock>>, to: &str) {
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(to)), true);
    }

    /// The last mid of AUD/USD, warm once a quote came.
    fn features() -> Rc<FeatureSet> {
        FeatureSet::builder()
            .quotes(
                InstrumentId::from(AUD),
                "mid",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .build()
            .unwrap()
    }

    fn warm(features: &FeatureSet) {
        let quote = QuoteTick::new(
            InstrumentId::from(AUD),
            Price::from("0.69990"),
            Price::from("0.70010"),
            Quantity::from(1),
            Quantity::from(1),
            UnixNanos::default(),
            UnixNanos::default(),
        );
        features.handle_quote(&quote).unwrap();
    }

    /// Hourly decisions on a horizon of 1 hour of 1000 AUD/USD, a long from
    /// 0.6 and a short to 0.4, the baseline learner.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .targets(
                TargetsConfig::builder()
                    .instrument_id(InstrumentId::from(AUD))
                    .quantity(Quantity::from(1000))
                    .horizon(SignedDuration::from_hours(1))
                    .build(),
            )
            .label(Label::Horizon)
            .retrain(SignedDuration::from_hours(24))
            .window(SignedDuration::from_hours(48))
            .holdout(SignedDuration::from_hours(12))
            .thresholds(Thresholds {
                long: 0.6,
                short: 0.4,
            })
            .min_score(0.5)
            .learner(LearnerConfig::Baseline {})
            .build()
    }

    fn store(models: &Path) -> ModelStore {
        ModelStore::new(models, StrategyId::from(STRATEGY), &features(), &config()).unwrap()
    }

    /// A baseline model valid from `valid_from` predicting `trues` tenths
    /// whatever the row, as the store keeps it for the probe's features,
    /// probed on one row.
    fn baseline(valid_from: &str, trues: usize) -> StoredModel {
        let labels = (0..10).map(|i| i < trues).collect();
        let set = TrainingSet::new(1, vec![0.5; 10], labels).unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        let config = config();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from: at(valid_from),
                fingerprint: dataset::fingerprint(&features(), config.decision, &config.targets)
                    .to_string(),
                feature_names: vec!["mid".to_string()],
                targets: config.targets,
                label: config.label,
                learner: config.learner,
                score: 0.6,
                probes: vec![Probe {
                    row: vec![0.7],
                    prediction: model.predict(&[0.7]).unwrap(),
                }],
            },
            model: model.save().unwrap(),
        }
    }

    /// The probe's handle trading in a backtest on the models under
    /// `models`, its features warm.
    fn handle(models: &Path) -> Rc<ModelHandle> {
        let features = features();
        warm(&features);
        ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            Mode::Backtest {
                models: models.to_path_buf(),
            },
        )
        .unwrap()
    }

    fn probability(trade: Option<Trade>) -> Option<f64> {
        trade.map(|trade| trade.probability)
    }

    #[test]
    fn no_model_in_the_store_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(handle(models.path()).decide(&mut probe).unwrap(), None);
    }

    #[test]
    fn no_model_valid_yet_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T13:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(handle(models.path()).decide(&mut probe).unwrap(), None);
    }

    #[test]
    fn cold_features_give_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let cold = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features(),
            &config(),
            Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();

        assert_eq!(cold.decide(&mut probe).unwrap(), None);
    }

    #[test]
    fn a_probability_from_the_long_threshold_buys_the_targets_size() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 6))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(
            handle(models.path()).decide(&mut probe).unwrap(),
            Some(Trade {
                side: OrderSide::Buy,
                quantity: Quantity::from(1000),
                probability: 0.6,
            })
        );
    }

    #[test]
    fn a_probability_to_the_short_threshold_sells() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 4))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        let trade = handle(models.path()).decide(&mut probe).unwrap().unwrap();

        assert_eq!((trade.side, trade.probability), (OrderSide::Sell, 0.4));
    }

    #[test]
    fn a_probability_inside_the_thresholds_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 5))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(handle(models.path()).decide(&mut probe).unwrap(), None);
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
        let (mut probe, clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = handle(models.path());

        let mut probabilities = vec![probability(handle.decide(&mut probe).unwrap())];
        for time in ["2026-10-22T12:00:00Z", "2026-10-22T12:59:59Z"] {
            advance(&clock, time);
            probabilities.push(probability(handle.decide(&mut probe).unwrap()));
        }

        assert_eq!(probabilities, [Some(0.7), Some(0.8), Some(0.8)]);
    }

    #[test]
    fn a_model_published_after_a_decision_is_used_from_the_next() {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = handle(models.path());
        let before = probability(handle.decide(&mut probe).unwrap());

        // Live, a retrain at 12:00 publishes once its child finishes.
        store.publish(&baseline("2026-10-22T12:00:00Z", 9)).unwrap();
        advance(&clock, "2026-10-22T13:00:00Z");

        assert_eq!(before, Some(0.7));
        assert_eq!(probability(handle.decide(&mut probe).unwrap()), Some(0.9));
    }

    /// The model valid from 10:00 is used at 11:00; `refused`, valid from
    /// 12:00, is not, at 12:00 nor after: the first is kept.
    fn kept_over(refused: StoredModel) {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
        store.publish(&refused).unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = handle(models.path());

        let mut probabilities = vec![probability(handle.decide(&mut probe).unwrap())];
        for time in ["2026-10-22T12:00:00Z", "2026-10-22T13:00:00Z"] {
            advance(&clock, time);
            probabilities.push(probability(handle.decide(&mut probe).unwrap()));
        }

        assert_eq!(probabilities, [Some(0.7), Some(0.7), Some(0.7)]);
    }

    #[test]
    fn a_model_of_other_feature_names_is_refused_and_the_current_kept() {
        let mut refused = baseline("2026-10-22T12:00:00Z", 9);
        refused.manifest.feature_names = vec!["spread".to_string()];

        kept_over(refused);
    }

    #[test]
    fn a_model_of_another_fingerprint_is_refused_and_the_current_kept() {
        let mut refused = baseline("2026-10-22T12:00:00Z", 9);
        refused.manifest.fingerprint = "f".repeat(64);

        kept_over(refused);
    }

    #[test]
    fn a_model_of_another_width_is_refused_and_the_current_kept() {
        let mut refused = baseline("2026-10-22T12:00:00Z", 9);
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
        let mut refused = baseline("2026-10-22T12:00:00Z", 9);
        refused.manifest.probes[0].prediction = 0.9 + f64::EPSILON;

        kept_over(refused);
    }

    #[test]
    fn a_live_cadence_waits_for_composite_bars_a_backtests_does_not() {
        let models = tempfile::tempdir().unwrap();
        let composite = [BarType::from(
            "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        )];
        let next = |mode: Mode| {
            let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));
            let handle =
                ModelHandle::new(StrategyId::from(STRATEGY), features(), &config(), mode).unwrap();
            handle.start(&mut probe, &composite).unwrap();
            let next = clock.borrow().next_time_ns(decide::DECISION_TIMER).unwrap();
            Timestamp::from(next)
        };

        let models = models.path().to_path_buf();
        assert_eq!(
            next(Mode::Backtest {
                models: models.clone()
            }),
            at("2026-10-22T12:00:00.010Z")
        );
        assert_eq!(
            next(live(models, Launcher::in_process(|_| unreachable!()))),
            at("2026-10-22T12:00:05.010Z")
        );
    }

    /// Live trading on the models under `models`, retraining through
    /// `launcher` 6 minutes after each retrain time.
    fn live(models: PathBuf, launcher: Launcher) -> Mode {
        Mode::Live {
            models,
            launcher,
            catalog_delay: SignedDuration::from_mins(6),
        }
    }

    /// The probe's handle trading live on the models under `models`,
    /// retraining through `launcher`, started, its features warm.
    fn live_handle(probe: &mut Decider, models: &Path, launcher: Launcher) -> Rc<ModelHandle> {
        let features = features();
        warm(&features);
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            live(models.to_path_buf(), launcher),
        )
        .unwrap();
        handle.start(probe, &[]).unwrap();
        handle
    }

    /// Advances `clock` to `to`, handing `handle` the events of the timers
    /// on the way.
    fn run_to(
        probe: &mut Decider,
        handle: &ModelHandle,
        clock: &Rc<RefCell<VirtualClock>>,
        to: &str,
    ) {
        let events = clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(to)), true);
        for event in events {
            handle.on_time_event(probe, &event).unwrap();
        }
    }

    #[test]
    fn a_live_node_trades_from_its_first_decision_on_the_newest_model() {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-21T00:00:00Z", 7)).unwrap();
        store.publish(&baseline("2026-10-22T00:00:00Z", 8)).unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = live_handle(
            &mut probe,
            models.path(),
            Launcher::in_process(|_| unreachable!()),
        );

        assert_eq!(probability(handle.decide(&mut probe).unwrap()), Some(0.8));
    }

    #[test]
    fn the_retrain_timer_is_the_strategys_on_the_grid_the_catalog_delay_late() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));
        live_handle(
            &mut probe,
            models.path(),
            Launcher::in_process(|_| unreachable!()),
        );

        let next = clock.borrow().next_time_ns("RETRAIN-HANDLE-001").unwrap();

        assert_eq!(Timestamp::from(next), at("2026-10-23T00:06:00Z"));
    }

    #[test]
    fn a_model_a_live_retrain_publishes_is_used_from_the_next_decision() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T10:00:00Z", 7))
            .unwrap();
        let (published, publishing) = std::sync::mpsc::channel();
        let retrained = store(models.path());
        let launcher = Launcher::in_process(move |at| {
            retrained.publish(&baseline(&at.to_string(), 9)).unwrap();
            published.send(at).unwrap();
            crate::model::train::Outcome::Published {
                valid_from: at,
                score: 0.6,
            }
        });
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let handle = live_handle(&mut probe, models.path(), launcher);
        let before = probability(handle.decide(&mut probe).unwrap());

        run_to(&mut probe, &handle, &clock, "2026-10-23T00:06:00Z");
        let trained_at = publishing.recv().unwrap();
        advance(&clock, "2026-10-23T01:00:00Z");

        assert_eq!(trained_at, at("2026-10-23T00:00:00Z"));
        assert_eq!(before, Some(0.7));
        assert_eq!(probability(handle.decide(&mut probe).unwrap()), Some(0.9));
    }

    #[test]
    fn a_live_retrain_keeping_the_model_leaves_the_current_one() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T10:00:00Z", 7))
            .unwrap();
        let (kept, keeping) = std::sync::mpsc::channel();
        let launcher = Launcher::in_process(move |at| {
            kept.send(at).unwrap();
            crate::model::train::Outcome::Kept(crate::model::train::Kept::BelowMinScore {
                score: 0.48,
                min_score: 0.5,
            })
        });
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let handle = live_handle(&mut probe, models.path(), launcher);

        run_to(&mut probe, &handle, &clock, "2026-10-23T00:06:00Z");
        keeping.recv().unwrap();
        advance(&clock, "2026-10-23T01:00:00Z");

        assert_eq!(probability(handle.decide(&mut probe).unwrap()), Some(0.7));
        assert_eq!(
            store(models.path()).valid_froms().unwrap(),
            [at("2026-10-22T10:00:00Z")]
        );
    }
}
