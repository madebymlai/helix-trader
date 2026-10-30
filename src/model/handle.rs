//! The model handle: what a strategy trading on a learned model holds, the
//! one place it decides, in a backtest, live and a dataset run alike.
//!
//! The strategy shares a `ModelHandle` (an `Rc`), built from its features
//! and the model section of its config, in a `Mode`. `start` from
//! `on_start` registers the features (and starts the targets' ATR, in a
//! dataset run) and starts the decision cadence; `decide`, at each
//! decision (the cadence's timer, `decide::is_decision`, or an event of
//! the strategy's choosing), snapshots the features, if the decision is
//! fresh (data of the targets' instrument since the previous) and every
//! one is initialized, and publishes their row (`decide::decide`), then:
//!
//! - trading (`Mode::Backtest`, `Mode::Live`): takes the latest model of
//!   the store (`store::ModelStore`) valid at the decision, predicts on the
//!   row and returns the trade the config's thresholds make of the
//!   probability (`Thresholds::side`), of the targets' size. No model
//!   valid yet, a stale decision, cold features, or a probability between
//!   the thresholds are no trade, never an error.
//! - in a dataset run (`Mode::Dataset`): opens the decision's targets
//!   (`targets::Targets`) and returns no trade, so the strategy places
//!   none of its own; the strategy hands it its order, position and time
//!   events, and the rows the targets settle are collected for the caller
//!   of the run.
//!
//! With a meta model (`model::meta`), a trade of the primary is taken only
//! if the meta model in use, on the row with the primary's side and
//! probability, predicts at least its threshold, sized from that
//! probability if the meta sizes (`MetaConfig::size`); no meta model valid
//! yet is no trade. A strategy whose primary is its rule (`of_rule`)
//! decides through `decide_on` with the rule's side, only when the rule
//! signals: the meta model alone decides whether to take it.
//!
//! A model is swapped in only inside `decide`, before it predicts: no
//! decision mixes two models. A backtest and a live node read the same
//! store, listed again at each decision, so a model published meanwhile
//! is used from the first decision at or after its valid-from. Before use
//! a model is checked against the strategy: its feature names and their
//! order, its fingerprint (`dataset::fingerprint`), its width, and its
//! probes, each of which the reloaded model must predict to the bit as it
//! did when fitted. A model failing a check is refused, logged as an
//! error, and the current one kept; it is not read again (`store::Swap`).
//!
//! A live node builds composite bars `warmup::live_bar_build_delay` after
//! their close, so a live handle starts the cadence with that delay, a
//! backtest's and a dataset run's with none (see `decide`).
//!
//! Live, the handle also retrains its model (`launcher::Retraining`): the
//! retrain timer, keyed by the strategy, launches a train at each retrain
//! time, one at a time, and `decide` first polls the running one, so the
//! model it publishes is used from the next decision.

use std::{
    cell::RefCell,
    fmt::Debug,
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::ensure;
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
    identifiers::{InstrumentId, StrategyId},
    types::{Quantity, fixed::FIXED_PRECISION},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{
    config::{MetaConfig, ModelConfig, RuleConfig, Thresholds},
    dataset::{self, Commission},
    decide::{self, DecisionRule},
    features::FeatureSet,
    launcher::{Launcher, Retraining},
    meta::{Meta, meta_row},
    store::{ModelStore, Swap},
    targets::{TargetRow, Targets, TargetsConfig},
};
use crate::strategy::{DataRequirements, warmup};

/// Where a handle runs, and so what it does at a decision.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Trades in a backtest on the models of the store under `models`
    /// whose targets are net of `commission`, that of its dataset runs.
    Backtest {
        models: PathBuf,
        commission: Commission,
    },
    /// Trades live on the models of the store under `models` whose targets
    /// are net of `commission`, that of its retrains' dataset runs, deciding
    /// the live node's bar build delay late, and retrains on the
    /// config's interval through `launcher`, each retrain launched
    /// `catalog_delay` after its time: once the data the node received up
    /// to then is in the catalog (`live::Recording::catalog_delay`).
    Live {
        models: PathBuf,
        commission: Commission,
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
    /// The targets' size, or, with a meta model sizing, its multiple of it.
    pub quantity: Quantity,
    /// The probability that the label is true of the model deciding: the
    /// meta model's, if any, else the primary's.
    pub probability: f64,
}

/// A strategy's model handle, shared with it as an `Rc`.
#[derive(Debug)]
pub struct ModelHandle {
    strategy_id: StrategyId,
    features: Rc<FeatureSet>,
    rule: DecisionRule,
    /// The instrument traded, whose data makes a decision fresh.
    traded: InstrumentId,
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
    primary: Primary,
    /// The targets' size.
    quantity: Quantity,
}

/// What picks the side of a trade.
#[derive(Debug)]
enum Primary {
    /// A model, and the meta model taking its trades, if any.
    Model {
        models: Models,
        thresholds: Thresholds,
        meta: Option<MetaModels>,
    },
    /// The strategy's rule, and the meta model taking its trades.
    Rule { meta: MetaModels },
}

/// The models of one level of the store, swapped in decision by decision.
#[derive(Debug)]
struct Models(RefCell<Swap>);

/// A meta model's models, and its config.
#[derive(Debug)]
struct MetaModels {
    models: Models,
    config: MetaConfig,
}

impl ModelHandle {
    /// The handle of `strategy_id`, deciding on `features` as `config`
    /// says, for a strategy of `requirements`, in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule is invalid, a cadence's
    /// features read no data of the targets' instrument (its decisions
    /// would never be fresh, see `decide::decide`), a store cannot be
    /// keyed, the meta model has no barrier to learn or, in a dataset run,
    /// the targets are invalid.
    pub fn new(
        strategy_id: StrategyId,
        features: Rc<FeatureSet>,
        config: &ModelConfig,
        requirements: &DataRequirements,
        mode: Mode,
    ) -> anyhow::Result<Rc<Self>> {
        let primary = |models: &Path, commission: &Commission| -> anyhow::Result<Primary> {
            let fingerprint = dataset::fingerprint(
                &features,
                config.decision,
                &config.targets,
                requirements,
                commission,
            )?
            .to_string();
            let meta = Meta::of_model(
                models,
                strategy_id,
                &features,
                config,
                requirements,
                commission,
            )?
            .map(|meta| MetaModels::of(&meta, &fingerprint));
            Ok(Primary::Model {
                models: Models::new(
                    ModelStore::new(
                        models,
                        strategy_id,
                        &features,
                        config,
                        requirements,
                        commission,
                    )?,
                    features.names().to_vec(),
                    fingerprint,
                ),
                thresholds: config.thresholds,
                meta,
            })
        };
        Self::built(
            strategy_id,
            &features,
            (config.decision, &config.targets, config.retrain),
            requirements,
            mode,
            primary,
        )
    }

    /// The handle of `strategy_id`, whose primary is its rule, deciding on
    /// `features` as `config` says, for a strategy of `requirements`, in
    /// `mode`: the strategy decides through `decide_on`.
    ///
    /// # Errors
    ///
    /// Returns an error if `features` do not declare the primary's side
    /// and probability (`FeatureSetBuilder::primary`), the decision rule is
    /// invalid, the store cannot be keyed, the targets have no barrier or,
    /// in a dataset run, are invalid.
    pub fn of_rule(
        strategy_id: StrategyId,
        features: Rc<FeatureSet>,
        config: &RuleConfig,
        requirements: &DataRequirements,
        mode: Mode,
    ) -> anyhow::Result<Rc<Self>> {
        ensure!(
            features.has_primary(),
            "the features of a rule primary declare its side and probability \
             (`FeatureSetBuilder::primary`)"
        );
        let primary = |models: &Path, commission: &Commission| -> anyhow::Result<Primary> {
            let meta = Meta::of_rule(
                models,
                strategy_id,
                &features,
                config,
                requirements,
                commission,
            )?;
            let fingerprint = dataset::fingerprint(
                &features,
                config.decision,
                &config.targets,
                requirements,
                commission,
            )?
            .to_string();
            Ok(Primary::Rule {
                meta: MetaModels::of(&meta, &fingerprint),
            })
        };
        Self::built(
            strategy_id,
            &features,
            (config.decision, &config.targets, config.retrain),
            requirements,
            mode,
            primary,
        )
    }

    /// The handle of `strategy_id` deciding with `features` on `rule`, with
    /// `targets`, retrained every `retrain`, for a strategy of
    /// `requirements`, in `mode`: trading on the models `primary` reads
    /// under the store's root, of the targets net of the mode's
    /// commission, or opening the targets.
    fn built(
        strategy_id: StrategyId,
        features: &Rc<FeatureSet>,
        (rule, targets, retrain): (DecisionRule, &TargetsConfig, SignedDuration),
        requirements: &DataRequirements,
        mode: Mode,
        primary: impl FnOnce(&Path, &Commission) -> anyhow::Result<Primary>,
    ) -> anyhow::Result<Rc<Self>> {
        rule.validate()?;
        let traded = targets.instrument_id;
        ensure!(
            rule == DecisionRule::Event || features.reads(traded),
            "the features read no data of {traded}, the targets' instrument: \
             no decision of the cadence would be fresh"
        );
        let serving = match mode {
            Mode::Backtest { models, commission } => Serving::Trading(Box::new(Trading {
                retraining: None,
                primary: primary(&models, &commission)?,
                quantity: targets.quantity,
            })),
            Mode::Live {
                models,
                commission,
                launcher,
                catalog_delay,
            } => Serving::Trading(Box::new(Trading {
                retraining: Some(Retraining::new(
                    strategy_id,
                    retrain,
                    catalog_delay,
                    launcher,
                )),
                primary: primary(&models, &commission)?,
                quantity: targets.quantity,
            })),
            Mode::Dataset { rows } => Serving::Dataset {
                targets: Targets::new(targets.clone(), rule, requirements)?,
                rows,
            },
        };
        Ok(Rc::new(Self {
            strategy_id,
            features: Rc::clone(features),
            rule,
            traded,
            serving,
        }))
    }

    /// Starts the handle on `strategy`, reading `bar_types`, from
    /// `on_start`: registers the features (and, in a dataset run, starts the
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
        S: DataActor + DataActorNative + Debug + 'static,
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
                targets.start(strategy);
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
    /// the store then lists. A handle whose primary is a rule makes no
    /// decision here: its strategy decides through `decide_on`.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict, or, in a dataset run,
    /// as `Targets::open` does.
    pub fn decide<S>(&self, strategy: &mut S) -> anyhow::Result<Option<Trade>>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        self.decide_as(strategy, None)
    }

    /// Decides now on `side`, the side the rule of `strategy`, its primary,
    /// signals (see `of_rule`): the trade of the decision, if the meta
    /// model takes it; in a dataset run, the decision's targets opened, its
    /// row carrying the side.
    ///
    /// # Errors
    ///
    /// As `decide`.
    ///
    /// # Panics
    ///
    /// Panics if the handle's primary is not a rule.
    pub fn decide_on<S>(&self, strategy: &mut S, side: OrderSide) -> anyhow::Result<Option<Trade>>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        self.features
            .on_side(side, f64::NAN, || self.decide_as(strategy, Some(side)))
    }

    /// Decides now, on the rule's `side` if the primary is a rule.
    fn decide_as<S>(
        &self,
        strategy: &mut S,
        side: Option<OrderSide>,
    ) -> anyhow::Result<Option<Trade>>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        if let Serving::Trading(trading) = &self.serving
            && let Some(retraining) = &trading.retraining
        {
            retraining.poll();
        }
        let Some(row) = decide::decide(strategy, &self.features, self.rule, self.traded) else {
            return Ok(None);
        };
        match &self.serving {
            Serving::Trading(trading) => {
                trading.trade(Timestamp::from(row.ts_event), &row.features, side)
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
    /// The trade of the decision at `now` on `row`, on the rule's `side` if
    /// the primary is a rule: a model primary's trade, of the latest model
    /// valid then through the thresholds, taken if its meta model takes
    /// it; a rule's taken if its meta model takes it.
    fn trade(
        &self,
        now: Timestamp,
        row: &[f64],
        side: Option<OrderSide>,
    ) -> anyhow::Result<Option<Trade>> {
        match (&self.primary, side) {
            (
                Primary::Model {
                    models,
                    thresholds,
                    meta,
                },
                None,
            ) => {
                let Some(probability) = models.predict(now, row)? else {
                    return Ok(None);
                };
                let Some(side) = thresholds.side(probability) else {
                    return Ok(None);
                };
                match meta {
                    None => Ok(Some(Trade {
                        side,
                        quantity: self.quantity,
                        probability,
                    })),
                    Some(meta) => {
                        meta.take(now, &meta_row(row, side, probability), side, self.quantity)
                    }
                }
            }
            (Primary::Rule { meta }, Some(side)) => meta.take(now, row, side, self.quantity),
            (Primary::Model { .. }, Some(_)) | (Primary::Rule { .. }, None) => {
                unreachable!("a rule primary decides on a side, a model primary on none")
            }
        }
    }
}

impl Models {
    /// The models of `store`, checked against `feature_names` and
    /// `fingerprint`.
    fn new(store: ModelStore, feature_names: Vec<String>, fingerprint: String) -> Self {
        Self(RefCell::new(Swap::new(store, feature_names, fingerprint)))
    }

    /// The prediction on `row` of the latest model valid at `now`, swapped
    /// in; none without a model. A store that cannot be listed keeps the
    /// current model.
    fn predict(&self, now: Timestamp, row: &[f64]) -> anyhow::Result<Option<f64>> {
        let mut swap = self.0.borrow_mut();
        let valid_froms = swap.store().valid_froms().unwrap_or_else(|e| {
            log::error!("listing the models at {now}: {e:#}; the current model kept");
            vec![]
        });
        swap.at(now, &valid_froms)
            .map(|(_, model)| model.predict(row))
            .transpose()
    }
}

impl MetaModels {
    /// The models of `meta`, of a strategy of `fingerprint`.
    fn of(meta: &Meta, fingerprint: &str) -> Self {
        Self {
            models: Models::new(
                meta.store().clone(),
                meta.feature_names().to_vec(),
                fingerprint.to_string(),
            ),
            config: meta.config().clone(),
        }
    }

    /// The trade on `side` of `quantity`, the targets' size, if the meta
    /// model in use at `now` predicts at least its threshold on `row`,
    /// sized from its probability; no trade without a meta model.
    ///
    /// # Errors
    ///
    /// Returns an error if the model fails to predict, or the size is no
    /// quantity.
    fn take(
        &self,
        now: Timestamp,
        row: &[f64],
        side: OrderSide,
        quantity: Quantity,
    ) -> anyhow::Result<Option<Trade>> {
        let Some(probability) = self.models.predict(now, row)? else {
            return Ok(None);
        };
        if probability < self.config.threshold {
            return Ok(None);
        }
        let quantity = match self.config.size {
            None => quantity,
            Some(_) => {
                let size = quantity.as_decimal() * self.config.size(probability);
                Quantity::from_decimal_dp(size, FIXED_PRECISION)
                    .map_err(|e| anyhow::anyhow!("sizing {size}: {e}"))?
            }
        };
        Ok(Some(Trade {
            side,
            quantity,
            probability,
        }))
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
        config::Sizing,
        learner::{LearnerConfig, TrainingSet},
        store::{ModelManifest, Probe, StoredModel},
        targets::{Barrier, Distance, Label, TargetsConfig},
    };
    use rust_decimal::Decimal;

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

    /// Advances `clock` to `to`, when a quote of AUD/USD comes to the
    /// features of `handle`: the market is open.
    fn advance(handle: &ModelHandle, clock: &Rc<RefCell<VirtualClock>>, to: &str) {
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(to)), true);
        warm(&handle.features, UnixNanos::from(at(to)));
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

    /// Hands `features` a quote of AUD/USD made at `now`.
    fn warm(features: &FeatureSet, now: UnixNanos) {
        let quote = QuoteTick::new(
            InstrumentId::from(AUD),
            Price::from("0.69990"),
            Price::from("0.70010"),
            Quantity::from(1),
            Quantity::from(1),
            now,
            now,
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
        ModelStore::new(
            models,
            StrategyId::from(STRATEGY),
            &features(),
            &config(),
            &requirements(),
            &commission(),
        )
        .unwrap()
    }

    /// What the strategy reads: the quotes of AUD/USD.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::TickSeries {
                data: nautilus_model::data::NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(AUD),
            }],
            warmup: SignedDuration::ZERO,
        }
    }

    /// A commission rate of 30 per million.
    fn commission() -> Commission {
        Commission::Rate("0.00003".to_string())
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
                fingerprint: dataset::fingerprint(
                    &features(),
                    config.decision,
                    &config.targets,
                    &requirements(),
                    &commission(),
                )
                .unwrap()
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
    /// `models`, its features warm: a quote came at the time of `probe`.
    fn handle(probe: &Decider, models: &Path) -> Rc<ModelHandle> {
        let features = features();
        warm(&features, probe.clock().timestamp_ns());
        ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements(),
            Mode::Backtest {
                models: models.to_path_buf(),
                commission: commission(),
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

        assert_eq!(
            handle(&probe, models.path()).decide(&mut probe).unwrap(),
            None
        );
    }

    /// A model learned targets net of its dataset runs' commission: a
    /// backtest of other costs does not trade on it.
    #[test]
    fn a_model_of_another_commission_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let features = features();
        warm(&features, probe.clock().timestamp_ns());
        let other = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements(),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                commission: Commission::PerOrder("3 USD".to_string()),
            },
        )
        .unwrap();

        assert_eq!(other.decide(&mut probe).unwrap(), None);
        assert_eq!(
            probability(handle(&probe, models.path()).decide(&mut probe).unwrap()),
            Some(0.8)
        );
    }

    #[test]
    fn no_model_valid_yet_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T13:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(
            handle(&probe, models.path()).decide(&mut probe).unwrap(),
            None
        );
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
            &requirements(),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                commission: commission(),
            },
        )
        .unwrap();

        assert_eq!(cold.decide(&mut probe).unwrap(), None);
    }

    /// No quote since the decision an hour before: the market is closed.
    #[test]
    fn a_stale_decision_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = handle(&probe, models.path());
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at("2026-10-22T12:00:00Z")), true);

        assert_eq!(handle.decide(&mut probe).unwrap(), None);
    }

    /// Its decisions would never be fresh.
    #[test]
    fn a_cadence_on_features_reading_no_data_of_the_traded_instrument_is_an_error() {
        let models = tempfile::tempdir().unwrap();
        let features = FeatureSet::builder()
            .quotes(
                InstrumentId::from("GBP/USD.SIM"),
                "gbp",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .build()
            .unwrap();

        let err = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements(),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                commission: commission(),
            },
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains(AUD), "{err}");
    }

    #[test]
    fn a_probability_from_the_long_threshold_buys_the_targets_size() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 6))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(
            handle(&probe, models.path()).decide(&mut probe).unwrap(),
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

        let trade = handle(&probe, models.path())
            .decide(&mut probe)
            .unwrap()
            .unwrap();

        assert_eq!((trade.side, trade.probability), (OrderSide::Sell, 0.4));
    }

    #[test]
    fn a_probability_inside_the_thresholds_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 5))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(
            handle(&probe, models.path()).decide(&mut probe).unwrap(),
            None
        );
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
        let handle = handle(&probe, models.path());

        let mut probabilities = vec![probability(handle.decide(&mut probe).unwrap())];
        for time in ["2026-10-22T12:00:00Z", "2026-10-22T12:59:59Z"] {
            advance(&handle, &clock, time);
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
        let handle = handle(&probe, models.path());
        let before = probability(handle.decide(&mut probe).unwrap());

        // Live, a retrain at 12:00 publishes once its child finishes.
        store.publish(&baseline("2026-10-22T12:00:00Z", 9)).unwrap();
        advance(&handle, &clock, "2026-10-22T13:00:00Z");

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
        let handle = handle(&probe, models.path());

        let mut probabilities = vec![probability(handle.decide(&mut probe).unwrap())];
        for time in ["2026-10-22T12:00:00Z", "2026-10-22T13:00:00Z"] {
            advance(&handle, &clock, time);
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
            let handle = ModelHandle::new(
                StrategyId::from(STRATEGY),
                features(),
                &config(),
                &requirements(),
                mode,
            )
            .unwrap();
            handle.start(&mut probe, &composite).unwrap();
            let next = clock.borrow().next_time_ns(decide::DECISION_TIMER).unwrap();
            Timestamp::from(next)
        };

        let models = models.path().to_path_buf();
        assert_eq!(
            next(Mode::Backtest {
                models: models.clone(),
                commission: commission(),
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
            commission: commission(),
            launcher,
            catalog_delay: SignedDuration::from_mins(6),
        }
    }

    /// The probe's handle trading live on the models under `models`,
    /// retraining through `launcher`, started, its features warm: a quote
    /// came at the time of `probe`.
    fn live_handle(probe: &mut Decider, models: &Path, launcher: Launcher) -> Rc<ModelHandle> {
        let features = features();
        warm(&features, probe.clock().timestamp_ns());
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements(),
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
            .into()
        });
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let handle = live_handle(&mut probe, models.path(), launcher);
        let before = probability(handle.decide(&mut probe).unwrap());

        run_to(&mut probe, &handle, &clock, "2026-10-23T00:06:00Z");
        let trained_at = publishing.recv().unwrap();
        advance(&handle, &clock, "2026-10-23T01:00:00Z");

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
            .into()
        });
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let handle = live_handle(&mut probe, models.path(), launcher);

        run_to(&mut probe, &handle, &clock, "2026-10-23T00:06:00Z");
        keeping.recv().unwrap();
        advance(&handle, &clock, "2026-10-23T01:00:00Z");

        assert_eq!(probability(handle.decide(&mut probe).unwrap()), Some(0.7));
        assert_eq!(
            store(models.path()).valid_froms().unwrap(),
            [at("2026-10-22T10:00:00Z")]
        );
    }

    /// A baseline valid from `valid_from` on rows of `feature_names`, of a
    /// strategy deciding with `features` as `config` says (its decisions
    /// and targets), predicting
    /// `trues` tenths whatever the row.
    fn baseline_of(
        features: &FeatureSet,
        (decision, targets): (DecisionRule, &TargetsConfig),
        feature_names: &[&str],
        valid_from: &str,
        trues: usize,
    ) -> StoredModel {
        let width = feature_names.len();
        let labels = (0..10).map(|i| i < trues).collect();
        let set = TrainingSet::new(width, vec![0.5; 10 * width], labels).unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from: at(valid_from),
                fingerprint: dataset::fingerprint(
                    features,
                    decision,
                    targets,
                    &requirements(),
                    &commission(),
                )
                .unwrap()
                .to_string(),
                feature_names: feature_names.iter().map(ToString::to_string).collect(),
                targets: targets.clone(),
                label: Label::Barrier,
                learner: LearnerConfig::Baseline {},
                score: 0.6,
                probes: vec![],
            },
            model: model.save().unwrap(),
        }
    }

    const META_NAMES: [&str; 3] = ["mid", "primary_side", "primary_probability"];

    /// The meta model taking a trade from 0.6, sizing it if `size`.
    fn meta(size: Option<Sizing>) -> MetaConfig {
        MetaConfig {
            window: SignedDuration::from_hours(48),
            holdout: SignedDuration::from_hours(12),
            threshold: 0.6,
            min_score: 0.5,
            learner: LearnerConfig::Baseline {},
            size,
        }
    }

    /// `config` on a barrier besides its horizon, its trades taken by
    /// `meta`.
    fn with_meta(meta: MetaConfig) -> ModelConfig {
        let mut config = config();
        config.targets.barrier = Some(Barrier {
            take_profit: Distance::BasisPoints(Decimal::from(10)),
            stop: Distance::BasisPoints(Decimal::from(10)),
            limit: SignedDuration::from_hours(2),
            atr: None,
        });
        config.meta = Some(meta);
        config
    }

    /// The probe's handle trading in a backtest on the models under
    /// `models` as `config` says, its primary a long of 0.6 from midnight,
    /// its meta model predicting `meta_trues` tenths, if any, its features
    /// warm by a quote at noon of the 22nd.
    fn meta_handle(
        models: &Path,
        config: &ModelConfig,
        meta_trues: Option<usize>,
    ) -> Rc<ModelHandle> {
        let features = features();
        ModelStore::new(
            models,
            StrategyId::from(STRATEGY),
            &features,
            config,
            &requirements(),
            &commission(),
        )
        .unwrap()
        .publish(&baseline_of(
            &features,
            (config.decision, &config.targets),
            &["mid"],
            "2026-10-22T00:00:00Z",
            6,
        ))
        .unwrap();
        if let Some(trues) = meta_trues {
            ModelStore::new(
                models,
                StrategyId::from(STRATEGY),
                &features,
                config,
                &requirements(),
                &commission(),
            )
            .unwrap()
            .meta(config.meta.as_ref().unwrap(), config.thresholds)
            .unwrap()
            .publish(&baseline_of(
                &features,
                (config.decision, &config.targets),
                &META_NAMES,
                "2026-10-22T00:00:00Z",
                trues,
            ))
            .unwrap();
        }
        warm(&features, UnixNanos::from(at("2026-10-22T12:00:00Z")));
        ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            config,
            &requirements(),
            Mode::Backtest {
                models: models.to_path_buf(),
                commission: commission(),
            },
        )
        .unwrap()
    }

    #[test]
    fn a_meta_model_from_its_threshold_takes_the_primarys_trade() {
        let models = tempfile::tempdir().unwrap();
        let handle = meta_handle(models.path(), &with_meta(meta(None)), Some(7));
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(
            handle.decide(&mut probe).unwrap(),
            Some(Trade {
                side: OrderSide::Buy,
                quantity: Quantity::from(1000),
                probability: 0.7,
            })
        );
    }

    #[test]
    fn a_meta_model_below_its_threshold_takes_no_trade() {
        let models = tempfile::tempdir().unwrap();
        let handle = meta_handle(models.path(), &with_meta(meta(None)), Some(5));
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(handle.decide(&mut probe).unwrap(), None);
    }

    #[test]
    fn no_meta_model_valid_yet_takes_no_trade() {
        let models = tempfile::tempdir().unwrap();
        let handle = meta_handle(models.path(), &with_meta(meta(None)), None);
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(handle.decide(&mut probe).unwrap(), None);
    }

    /// From the threshold 0.6 to 1, 0.5 to 2 times the size: 0.8 is 1.25
    /// times 1000.
    #[test]
    fn a_sizing_meta_model_sizes_the_trade_from_its_probability() {
        let models = tempfile::tempdir().unwrap();
        let sizing = Sizing {
            min: Decimal::new(5, 1),
            max: Decimal::from(2),
        };
        let handle = meta_handle(models.path(), &with_meta(meta(Some(sizing))), Some(8));
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        let trade = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(trade.quantity.as_decimal(), Decimal::from(1250));
        assert_eq!(trade.probability, 0.8);
    }

    /// The rule primary of `config`: its features declare the side.
    fn rule_features() -> Rc<FeatureSet> {
        FeatureSet::builder()
            .quotes(
                InstrumentId::from(AUD),
                "mid",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .primary()
            .build()
            .unwrap()
    }

    fn rule_config() -> RuleConfig {
        let config = with_meta(meta(None));
        RuleConfig {
            decision: config.decision,
            targets: config.targets,
            retrain: config.retrain,
            meta: meta(None),
        }
    }

    #[test]
    fn a_rule_primarys_side_is_traded_if_its_meta_model_takes_it() {
        let models = tempfile::tempdir().unwrap();
        let config = rule_config();
        let features = rule_features();
        ModelStore::of_rule(
            models.path(),
            StrategyId::from(STRATEGY),
            &features,
            &config,
            &requirements(),
            &commission(),
        )
        .unwrap()
        .publish(&baseline_of(
            &features,
            (config.decision, &config.targets),
            &META_NAMES,
            "2026-10-22T00:00:00Z",
            7,
        ))
        .unwrap();
        warm(&features, UnixNanos::from(at("2026-10-22T12:00:00Z")));
        let handle = ModelHandle::of_rule(
            StrategyId::from(STRATEGY),
            features,
            &config,
            &requirements(),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                commission: commission(),
            },
        )
        .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        let without_side = handle.decide(&mut probe).unwrap();
        let sold = handle.decide_on(&mut probe, OrderSide::Sell).unwrap();

        assert_eq!(without_side, None);
        assert_eq!(
            sold,
            Some(Trade {
                side: OrderSide::Sell,
                quantity: Quantity::from(1000),
                probability: 0.7,
            })
        );
    }

    #[test]
    fn a_rule_primarys_features_must_declare_its_side() {
        let models = tempfile::tempdir().unwrap();

        let err = ModelHandle::of_rule(
            StrategyId::from(STRATEGY),
            features(),
            &rule_config(),
            &requirements(),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                commission: commission(),
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("side"), "{err}");
    }
}
