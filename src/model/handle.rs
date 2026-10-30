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
//! - trading (`Mode::Backtest`, `Mode::Live`): takes, for each side of
//!   the config's margins, the latest model of its store
//!   (`store::ModelStore`) valid at the decision, its calibrated
//!   probability on the row, and its expected net P&L (`payoff`): on a
//!   barrier label, the expected R times the worth of the stop the
//!   decision sets (`targets::Stops`, from the decision's price and the
//!   barrier's ATR, which the handle runs), on a horizon in money. It
//!   returns the trade of the side clearing its margin, the larger of two
//!   (`payoff::pick`), of the targets' size, on the target of its label
//!   as the decision sets it on that side (`targets::TradeTarget`): the
//!   strategy sends it (`Trade::send`) as a dataset run sends its targets,
//!   and hands the handle its time events, which close it at its limit or
//!   horizon. A side with no model valid yet never trades; a stale
//!   decision, cold features, a barrier ATR not yet initialized, no side
//!   clearing its margin or two tied are no trade, never an error.
//! - in a dataset run (`Mode::Dataset`): opens the decision's targets
//!   (`targets::Targets`) and returns no trade, so the strategy places
//!   none of its own; the strategy hands it its order, position and time
//!   events, and the rows the targets settle are collected for the caller
//!   of the run.
//!
//! With a meta model (`model::meta`), a trade of the primary is taken only
//! if the meta model in use, on the row with the primary's side and that
//! side's probability, predicts at least its threshold, sized from that
//! probability if the meta sizes (`MetaConfig::size`); no meta model valid
//! yet is no trade. A strategy whose primary is its rule (`of_rule`)
//! decides through `decide_on` with the rule's side, only when the rule
//! signals: the meta model alone decides whether to take it.
//!
//! A model is swapped in only inside `decide`, before it predicts: no
//! decision mixes two models. A backtest and a live node read the same
//! store, listed again at each decision, so a model published meanwhile
//! is used from the first decision at or after its valid-from. Every
//! probability is calibrated (`store::LoadedModel`). Before use
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
//! Live, the strategy warms the barrier's ATR from its warmup request, as
//! it does the features: the request builds the ATR's bars too
//! (`warmup_bar_types`), and the strategy hands the bars of the response
//! to `handle_warmup_bar`, so the first decision after a start sets the
//! stop a dataset run sets there.
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

use anyhow::{Context, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative, indicators::ActorIndicator},
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_model::{
    data::{Bar, BarType, CustomData},
    enums::OrderSide,
    events::{OrderDenied, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId},
    types::{Quantity, fixed::FIXED_PRECISION},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{
    config::{MetaConfig, ModelConfig, RuleConfig},
    dataset::{self, DatasetVenue},
    decide::{self, DecisionRule},
    features::FeatureSet,
    launcher::{Launcher, Retraining},
    meta::{Meta, meta_row},
    payoff,
    store::{LoadedModel, ModelStore, Swap},
    targets::{self, Label, Side, Stops, TargetRow, Targets, TargetsConfig, TradeTarget},
};
use crate::strategy::{DataRequirements, warmup};

/// Where a handle runs, and so what it does at a decision.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Trades in a backtest on the models of the store under `models`
    /// whose targets are those of its dataset runs, on `venue`.
    Backtest {
        models: PathBuf,
        venue: DatasetVenue,
    },
    /// Trades live on the models of the store under `models` whose targets
    /// are those of its retrains' dataset runs, on `venue`, deciding
    /// the live node's bar build delay late, and retrains on the
    /// config's interval through `launcher`, each retrain launched at its
    /// time, trained at the last time the catalog at `catalog` holds for
    /// every series the strategy reads, if before it
    /// (`launcher::Retraining`).
    Live {
        models: PathBuf,
        venue: DatasetVenue,
        launcher: Launcher,
        catalog: PathBuf,
    },
    /// Opens each decision's targets, for a dataset run, and collects the
    /// rows they settle in `rows`.
    Dataset { rows: SettledRows },
}

/// The rows a dataset run settled, shared with its caller.
pub type SettledRows = Rc<RefCell<Vec<TargetRow>>>;

/// A trade a decision makes: the target its model was labelled on, as a
/// dataset run sets it at the decision on the side traded. A strategy
/// sends it (`send`) and hands the handle its time events, which close it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    /// The targets' instrument.
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    /// The targets' size, or, with a meta model sizing, its multiple of it.
    pub quantity: Quantity,
    /// The label's target: on a barrier its exits and its limit, else the
    /// horizon; a rule primary's, the barrier.
    pub target: TradeTarget,
    /// The calibrated probability that the label is true of the model
    /// deciding: the meta model's, if any, else that of the primary's
    /// model of the side.
    pub probability: f64,
}

impl Trade {
    /// Sends the trade from `strategy` (`TradeTarget::send`), as a dataset
    /// run sends its target: a bracket or a market entry, and the time
    /// alert closing it, which the strategy hands the handle
    /// (`ModelHandle::on_time_event`).
    ///
    /// # Errors
    ///
    /// As `TradeTarget::send`.
    pub fn send<S>(&self, strategy: &mut S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        self.target
            .send(strategy, self.instrument_id, self.side, self.quantity)
    }
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
    /// The targets' instrument.
    instrument_id: InstrumentId,
    /// The targets' size.
    quantity: Quantity,
}

/// What picks the side of a trade.
#[derive(Debug)]
enum Primary {
    /// A model of each side traded, the target of its label, and the meta
    /// model taking its trades, if any.
    Model {
        sides: Vec<SideModels>,
        labelled: Labelled,
        meta: Option<MetaModels>,
    },
    /// The strategy's rule, the barrier its meta model learns, which its
    /// trades are on, and the meta model taking them.
    Rule { stops: Stops, meta: MetaModels },
}

/// The target a model primary's trades are on, its label's.
#[derive(Debug)]
enum Labelled {
    /// The barrier's exits as the decision sets them; the worth of its
    /// stop weighs the expected R.
    Barrier(Stops),
    /// The horizon, expected in money.
    Horizon(SignedDuration),
}

/// The models of a side, and its margin.
#[derive(Debug)]
struct SideModels {
    side: Side,
    margin: f64,
    models: Models,
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
        let primary = |models: &Path, venue: &DatasetVenue| -> anyhow::Result<Primary> {
            let fingerprint = dataset::fingerprint(
                &features,
                config.decision,
                &config.targets,
                requirements,
                venue,
            )?
            .to_string();
            let meta = Meta::of_model(models, strategy_id, &features, config, requirements, venue)?
                .map(|meta| MetaModels::of(&meta, &fingerprint));
            let sides = config
                .margins
                .sides()
                .map(|(side, margin)| {
                    let store = ModelStore::new(
                        models,
                        strategy_id,
                        &features,
                        config,
                        requirements,
                        venue,
                        side,
                    )?;
                    Ok(SideModels {
                        side,
                        margin,
                        models: Models::new(store, features.names().to_vec(), fingerprint.clone()),
                    })
                })
                .collect::<anyhow::Result<_>>()?;
            let labelled = match config.label {
                Label::Barrier => Labelled::Barrier(
                    Stops::new(&config.targets, config.decision, requirements)?
                        .context("the label is the barrier, and the targets have none")?,
                ),
                Label::Horizon => Labelled::Horizon(
                    config
                        .targets
                        .horizon
                        .context("the label is the horizon, and the targets have none")?,
                ),
            };
            Ok(Primary::Model {
                sides,
                labelled,
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
        let primary = |models: &Path, venue: &DatasetVenue| -> anyhow::Result<Primary> {
            let meta = Meta::of_rule(models, strategy_id, &features, config, requirements, venue)?;
            let fingerprint = dataset::fingerprint(
                &features,
                config.decision,
                &config.targets,
                requirements,
                venue,
            )?
            .to_string();
            Ok(Primary::Rule {
                stops: Stops::new(&config.targets, config.decision, requirements)?.context(
                    "a rule primary's meta model learns the barrier, and the targets have none",
                )?,
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
    /// under the store's root, of the targets of the dataset runs on the
    /// mode's venue, or opening the targets.
    fn built(
        strategy_id: StrategyId,
        features: &Rc<FeatureSet>,
        (rule, targets, retrain): (DecisionRule, &TargetsConfig, SignedDuration),
        requirements: &DataRequirements,
        mode: Mode,
        primary: impl FnOnce(&Path, &DatasetVenue) -> anyhow::Result<Primary>,
    ) -> anyhow::Result<Rc<Self>> {
        rule.validate()?;
        let traded = targets.instrument_id;
        ensure!(
            rule == DecisionRule::Event || features.reads(traded),
            "the features read no data of {traded}, the targets' instrument: \
             no decision of the cadence would be fresh"
        );
        let serving = match mode {
            Mode::Backtest { models, venue } => Serving::Trading(Box::new(Trading {
                retraining: None,
                primary: primary(&models, &venue)?,
                instrument_id: traded,
                quantity: targets.quantity,
            })),
            Mode::Live {
                models,
                venue,
                launcher,
                catalog,
            } => Serving::Trading(Box::new(Trading {
                retraining: Some(Retraining::new(
                    strategy_id,
                    retrain,
                    catalog,
                    requirements.clone(),
                    launcher,
                )),
                primary: primary(&models, &venue)?,
                instrument_id: traded,
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
    /// `on_start`: registers the features (and starts the barrier's ATR:
    /// the stops', trading on a barrier, a rule's or a model's labelled on
    /// it, or, in a dataset run, the targets', and collects the rows they
    /// settle); trading, sets again the time close of each of its trades
    /// still open, as after a restart (`targets::arm_open_trades`); then
    /// starts the decision cadence, live
    /// `warmup::live_bar_build_delay(bar_types)` late, and live the
    /// retrain timer.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus refuses a trade's time alert, the
    /// cadence's timer or the retrain timer.
    pub fn start<S>(&self, strategy: &mut S, bar_types: &[BarType]) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative + Debug + 'static,
    {
        self.features.register(strategy);
        let delay = match &self.serving {
            Serving::Trading(trading) => {
                let after = match &trading.primary {
                    Primary::Model {
                        labelled: Labelled::Barrier(stops),
                        ..
                    }
                    | Primary::Rule { stops, .. } => {
                        stops.start(strategy);
                        stops.limit()
                    }
                    Primary::Model {
                        labelled: Labelled::Horizon(horizon),
                        ..
                    } => *horizon,
                };
                targets::arm_open_trades(strategy, trading.instrument_id, after)?;
                match &trading.retraining {
                    Some(retraining) => {
                        retraining.start(strategy)?;
                        warmup::live_bar_build_delay(bar_types)
                    }
                    None => SignedDuration::ZERO,
                }
            }
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

    /// The bar types a live warmup of a strategy reading `bar_types`
    /// requests (`warmup::start`): those, then the bar type of the
    /// barrier's ATR the handle trades on, if not among them, so that
    /// Nautilus builds its bars from the warmup's history and goes on with
    /// them live, the bar spanning the warmup's end once.
    #[must_use]
    pub fn warmup_bar_types(&self, bar_types: &[BarType]) -> Vec<BarType> {
        let mut warmed = bar_types.to_vec();
        if let Some(atr) = self.stops().and_then(Stops::atr_bar_type)
            && !warmed.contains(&atr)
        {
            warmed.push(atr);
        }
        warmed
    }

    /// Hands `bar`, a bar of a live warmup (`warmup::bars`), to the
    /// features and to the barrier's ATR the handle trades on, each of
    /// which reads the bars of its own bar types alone.
    ///
    /// # Errors
    ///
    /// Returns an error if a feature or the ATR fails to read the bar.
    pub fn handle_warmup_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        self.features.handle_bar(bar)?;
        match self.stops() {
            Some(stops) => stops.handle_warmup_bar(bar),
            None => Ok(()),
        }
    }

    /// The stops of the barrier the handle trades on, if any: none on a
    /// horizon, and in a dataset run, which warms on its replay.
    fn stops(&self) -> Option<&Stops> {
        let Serving::Trading(trading) = &self.serving else {
            return None;
        };
        match &trading.primary {
            Primary::Model {
                labelled: Labelled::Barrier(stops),
                ..
            }
            | Primary::Rule { stops, .. } => Some(stops),
            Primary::Model {
                labelled: Labelled::Horizon(_),
                ..
            } => None,
        }
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
                trading.trade(strategy, Timestamp::from(row.ts_event), &row.features, side)
            }
            Serving::Dataset { targets, .. } => {
                targets.open(strategy, row)?;
                Ok(None)
            }
        }
    }

    /// Hands `event` to the targets of a dataset run; trading, closes the
    /// trade whose time alert it is (`targets::on_trade_time`), and live
    /// hands it to the retrains, which launch one on their timer's.
    ///
    /// # Errors
    ///
    /// Returns an error as `Targets::on_time_event` or
    /// `targets::on_trade_time` does.
    pub fn on_time_event<S>(&self, strategy: &mut S, event: &TimeEvent) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        match &self.serving {
            Serving::Trading(trading) => {
                if let Some(retraining) = &trading.retraining {
                    retraining.on_time_event(event);
                }
                targets::on_trade_time(strategy, event)
            }
            Serving::Dataset { targets, .. } => targets.on_time_event(strategy, event),
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
    /// The trade of the decision of `strategy` at `now` on `row`, on the
    /// rule's `side` if the primary is a rule: a model primary's trade, of
    /// the side whose latest model valid then expects a net P&L clearing
    /// its margin, the larger of two, taken if its meta model takes it; a
    /// rule's taken if its meta model takes it. Either on the target of
    /// its label as the decision sets it on the side.
    fn trade<S>(
        &self,
        strategy: &S,
        now: Timestamp,
        row: &[f64],
        side: Option<OrderSide>,
    ) -> anyhow::Result<Option<Trade>>
    where
        S: DataActor + DataActorNative,
    {
        match (&self.primary, side) {
            (
                Primary::Model {
                    sides,
                    labelled,
                    meta,
                },
                None,
            ) => {
                let mut decided = vec![];
                for SideModels {
                    side,
                    margin,
                    models,
                } in sides
                {
                    let Some((probability, payoff)) =
                        models.at(now, |model| Ok((model.predict(row)?, model.payoff())))?
                    else {
                        continue;
                    };
                    let payoff = payoff.context("a primary's model has its payoff")?;
                    let Some((target, unit)) = labelled.at(strategy, *side)? else {
                        return Ok(None);
                    };
                    decided.push((
                        (*side, probability, target),
                        payoff.expected(probability) * unit,
                        *margin,
                    ));
                }
                let Some((side, probability, target)) = payoff::pick(decided) else {
                    return Ok(None);
                };
                let trade = self.trade_of(side.order_side(), target, probability);
                match meta {
                    None => Ok(Some(trade)),
                    Some(meta) => meta.take(now, &meta_row(row, trade.side, probability), trade),
                }
            }
            (Primary::Rule { stops, meta }, Some(side)) => {
                let of = match side {
                    OrderSide::Sell => Side::Short,
                    _ => Side::Long,
                };
                let Some((target, _)) = stops.at(strategy, of)? else {
                    return Ok(None);
                };
                meta.take(now, row, self.trade_of(side, target, f64::NAN))
            }
            (Primary::Model { .. }, Some(_)) | (Primary::Rule { .. }, None) => {
                unreachable!("a rule primary decides on a side, a model primary on none")
            }
        }
    }

    /// The trade on `side` of the targets' size and instrument, on
    /// `target`, at `probability`.
    const fn trade_of(&self, side: OrderSide, target: TradeTarget, probability: f64) -> Trade {
        Trade {
            instrument_id: self.instrument_id,
            side,
            quantity: self.quantity,
            target,
            probability,
        }
    }
}

impl Labelled {
    /// The target of a trade on `side` at a decision of `strategy` now,
    /// and what the unit of its side's payoff is worth: on a barrier its
    /// exits there and the worth of its stop, none as `Stops::at`; on a
    /// horizon the horizon and 1, the payoff being in money.
    ///
    /// # Errors
    ///
    /// As `Stops::at`.
    fn at<S>(&self, strategy: &S, side: Side) -> anyhow::Result<Option<(TradeTarget, f64)>>
    where
        S: DataActor + DataActorNative,
    {
        match self {
            Self::Barrier(stops) => stops.at(strategy, side),
            Self::Horizon(horizon) => Ok(Some((TradeTarget::Horizon(*horizon), 1.0))),
        }
    }
}

impl Models {
    /// The models of `store`, checked against `feature_names` and
    /// `fingerprint`.
    fn new(store: ModelStore, feature_names: Vec<String>, fingerprint: String) -> Self {
        Self(RefCell::new(Swap::new(store, feature_names, fingerprint)))
    }

    /// What `read` reads of the latest model valid at `now`, swapped in;
    /// none without a model. A store that cannot be listed keeps the
    /// current model.
    fn at<T>(
        &self,
        now: Timestamp,
        read: impl FnOnce(&LoadedModel) -> anyhow::Result<T>,
    ) -> anyhow::Result<Option<T>> {
        let mut swap = self.0.borrow_mut();
        let valid_froms = swap.store().valid_froms().unwrap_or_else(|e| {
            log::error!("listing the models at {now}: {e:#}; the current model kept");
            vec![]
        });
        swap.at(now, &valid_froms)
            .map(|(_, model)| read(model))
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

    /// The primary's `trade`, of the targets' size, if the meta model in
    /// use at `now` predicts at least its threshold on `row`, at its
    /// probability and sized from it; no trade without a meta model.
    ///
    /// # Errors
    ///
    /// Returns an error if the model fails to predict, or the size is no
    /// quantity.
    fn take(&self, now: Timestamp, row: &[f64], trade: Trade) -> anyhow::Result<Option<Trade>> {
        let Some(probability) = self.models.at(now, |model| model.predict(row))? else {
            return Ok(None);
        };
        if probability < self.config.threshold {
            return Ok(None);
        }
        let quantity = match self.config.size {
            None => trade.quantity,
            Some(_) => {
                let size = trade.quantity.as_decimal() * self.config.size(probability);
                Quantity::from_decimal_dp(size, FIXED_PRECISION)
                    .map_err(|e| anyhow::anyhow!("sizing {size}: {e}"))?
            }
        };
        Ok(Some(Trade {
            quantity,
            probability,
            ..trade
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
        data::{Bar, QuoteTick},
        enums::{ContingencyType, OmsType, OrderType, PriceType},
        events::OrderEventAny,
        identifiers::{
            AccountId, ClientOrderId, InstrumentId, PositionId, TradeId, TraderId, VenueOrderId,
        },
        instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
        orders::{Order, OrderTestBuilder, stubs::TestOrderEventStubs},
        position::Position,
        types::{Money, Price},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };

    use super::*;
    use crate::model::{
        calibration::Calibration,
        config::{Margins, Sizing},
        dataset::Commission,
        holdout::{Earnings, Holdout, Refusal, Trades},
        learner::{LearnerConfig, TrainingSet},
        payoff::Payoff,
        store::{ModelManifest, Probe, StoredModel},
        targets::{AtrConfig, Barrier, Distance, Label, TargetsConfig},
    };
    use rust_decimal::Decimal;

    /// The holdout of the models of the tests, as a train publishes it.
    const HOLDOUT: Holdout = Holdout {
        earnings: Earnings {
            taken: NO_TRADE,
            every: NO_TRADE,
            taken_stressed: NO_TRADE,
            every_stressed: NO_TRADE,
        },
        log_loss: 0.6,
        baseline_log_loss: 0.69,
        auc: Some(0.6),
    };

    const NO_TRADE: Trades = Trades {
        count: 0,
        expectancy: None,
        win_rate: None,
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
        // AUD/USD in the cache, quoted at 0.69990/0.70010: a barrier's
        // stops are set from its mid.
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(audusd_sim()))
            .unwrap();
        cache
            .borrow_mut()
            .add_quote(QuoteTick::new(
                InstrumentId::from(AUD),
                Price::from("0.69990"),
                Price::from("0.70010"),
                Quantity::from(1),
                Quantity::from(1),
                UnixNanos::from(now),
                UnixNanos::from(now),
            ))
            .unwrap();
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

    /// Hourly decisions on a horizon of 1 hour of 1000 AUD/USD, each side
    /// traded from an expected net P&L of nought, the baseline learner.
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
            .margins(Margins {
                long: Some(0.0),
                short: Some(0.0),
            })
            .min_pnl(0.0)
            .min_trades(1)
            .learner(LearnerConfig::Baseline {})
            .build()
    }

    /// The store of the long models under `models`.
    fn store(models: &Path) -> ModelStore {
        side_store(models, &config(), Side::Long)
    }

    /// The store of the `side` models of `config` under `models`.
    fn side_store(models: &Path, config: &ModelConfig, side: Side) -> ModelStore {
        ModelStore::new(
            models,
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            &venue(),
            side,
        )
        .unwrap()
    }

    /// The calibration of every model of the tests, `c(p) = 1 / (1 +
    /// e^(2 - 4p))`: c(0.5) = 0.5, c(0.6) = 0.598688, c(0.7) = 0.689974,
    /// c(0.8) = 0.768525, c(0.9) = 0.832018.
    const CALIBRATION: Calibration = Calibration { a: -4.0, b: 2.0 };

    /// The calibrated probability of a prediction of `p`.
    fn c(p: f64) -> f64 {
        CALIBRATION.apply(p)
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

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD, no leverage.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate("0.00003".to_string()),
            balance: Money::from("10000000 USD"),
            leverage: Decimal::ONE,
        }
    }

    /// A baseline long model of `config` valid from `valid_from` predicting
    /// `trues` tenths whatever the row, calibrated by `CALIBRATION`,
    /// expecting a win and a loss of 1, as the store keeps it for the
    /// probe's features, probed on one row.
    fn baseline(valid_from: &str, trues: usize) -> StoredModel {
        baseline_for(&config(), Side::Long, valid_from, trues)
    }

    /// The baseline of `baseline`, of `config`'s `side`.
    fn baseline_for(
        config: &ModelConfig,
        side: Side,
        valid_from: &str,
        trues: usize,
    ) -> StoredModel {
        let labels = (0..10).map(|i| i < trues).collect();
        let set = TrainingSet::new(1, vec![0.5; 10], labels).unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        let config = config.clone();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from: at(valid_from),
                fingerprint: dataset::fingerprint(
                    &features(),
                    config.decision,
                    &config.targets,
                    &requirements(),
                    &venue(),
                )
                .unwrap()
                .to_string(),
                feature_names: vec!["mid".to_string()],
                targets: config.targets,
                label: config.label,
                side: Some(side),
                learner: config.learner,
                calibration: CALIBRATION,
                payoff: Some(Payoff {
                    win: 1.0,
                    loss: 1.0,
                }),
                holdout: HOLDOUT,
                probes: vec![Probe {
                    row: vec![0.7],
                    prediction: CALIBRATION.apply(model.predict(&[0.7]).unwrap()),
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
                venue: venue(),
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
                venue: DatasetVenue {
                    commission: Commission::PerOrder("3 USD".to_string()),
                    ..venue()
                },
            },
        )
        .unwrap();

        assert_eq!(other.decide(&mut probe).unwrap(), None);
        assert_eq!(
            probability(handle(&probe, models.path()).decide(&mut probe).unwrap()),
            Some(c(0.8))
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
                venue: venue(),
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
                venue: venue(),
            },
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains(AUD), "{err}");
    }

    /// The probe's handle of `config`, trading in a backtest on the models
    /// under `models` with the long's of `long` tenths and the short's of
    /// `short` tenths, if any, valid from midnight, its features warm.
    fn sides_handle(
        probe: &Decider,
        models: &Path,
        config: &ModelConfig,
        (long, short): (Option<usize>, Option<usize>),
    ) -> Rc<ModelHandle> {
        for (side, trues) in [(Side::Long, long), (Side::Short, short)] {
            if let Some(trues) = trues {
                side_store(models, config, side)
                    .publish(&baseline_for(config, side, "2026-10-22T00:00:00Z", trues))
                    .unwrap();
            }
        }
        let features = features();
        warm(&features, probe.clock().timestamp_ns());
        ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            config,
            &requirements(),
            Mode::Backtest {
                models: models.to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap()
    }

    /// A long expecting `2 c(0.6) - 1`, 0.197 USD, clearing its margin of
    /// nought; the short has no model yet, and never trades.
    #[test]
    fn a_side_expected_to_clear_its_margin_trades_the_targets_size() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config(), (Some(6), None));

        assert_eq!(
            handle.decide(&mut probe).unwrap(),
            Some(Trade {
                instrument_id: InstrumentId::from(AUD),
                side: OrderSide::Buy,
                quantity: Quantity::from(1000),
                target: TradeTarget::Horizon(SignedDuration::from_hours(1)),
                probability: c(0.6),
            })
        );
    }

    /// The long expects 0.380 USD, the short 0.537: both clear nought, the
    /// short the more.
    #[test]
    fn of_two_sides_clearing_their_margins_the_one_expected_the_more_trades() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config(), (Some(7), Some(8)));

        let trade = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!((trade.side, trade.probability), (OrderSide::Sell, c(0.8)));
    }

    #[test]
    fn two_sides_expected_alike_trade_neither() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config(), (Some(7), Some(7)));

        assert_eq!(handle.decide(&mut probe).unwrap(), None);
    }

    /// The long expects 0.380 USD, short of its margin of 0.5 USD; the
    /// short expects to lose.
    #[test]
    fn no_side_clearing_its_margin_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        let config = ModelConfig {
            margins: Margins {
                long: Some(0.5),
                short: Some(0.5),
            },
            ..config()
        };
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config, (Some(7), Some(3)));

        assert_eq!(handle.decide(&mut probe).unwrap(), None);
    }

    /// A short model in the store expecting 0.664 USD, which a model of
    /// both sides would sell on: a long-only model never reads it.
    #[test]
    fn a_long_only_model_never_sells() {
        let models = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(0.0),
                short: None,
            },
            ..config()
        };
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let both = sides_handle(&probe, models.path(), &config(), (Some(3), Some(9)));
        let long = sides_handle(&probe, models.path(), &long_only, (None, None));

        assert_eq!(
            both.decide(&mut probe).unwrap().map(|trade| trade.side),
            Some(OrderSide::Sell)
        );
        assert_eq!(long.decide(&mut probe).unwrap(), None);
    }

    /// `config` learning a barrier a take profit 20 bps and a stop `stop`
    /// bps from the decision's price, trading the long from 0.30 USD.
    fn barrier_config(stop: i64) -> ModelConfig {
        let mut config = config();
        config.targets.barrier = Some(Barrier {
            take_profit: Distance::BasisPoints(Decimal::from(20)),
            stop: Distance::BasisPoints(Decimal::from(stop)),
            limit: SignedDuration::from_hours(2),
            atr: None,
        });
        config.label = Label::Barrier;
        config.margins = Margins {
            long: Some(0.3),
            short: None,
        };
        config
    }

    /// Worked by hand. The long expects `2 c(0.8) - 1`, 0.537R, at either
    /// stop. From the mid of 0.70000, a stop 10 bps away is 0.00070, worth
    /// 0.70 USD on 1000 AUD/USD: 0.376 USD expected, above the margin of
    /// 0.30; 2 bps away, 0.00014, worth 0.14 USD: 0.075 USD, below it.
    #[test]
    fn a_barriers_expected_r_is_weighed_by_the_worth_of_the_decisions_stop() {
        let decide = |stop| {
            let models = tempfile::tempdir().unwrap();
            let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
            let handle = sides_handle(
                &probe,
                models.path(),
                &barrier_config(stop),
                (Some(8), None),
            );
            handle.start(&mut probe, &[]).unwrap();
            handle.decide(&mut probe).unwrap()
        };

        assert_eq!(decide(10).map(|trade| trade.side), Some(OrderSide::Buy));
        assert_eq!(decide(2), None);
    }

    /// Worked by hand. From the mid of 0.70000, the long's take profit 20
    /// bps above, 0.00140, and its stop 10 bps below, 0.00070: the
    /// prices a dataset run's long sets there, closed 2 hours later.
    #[test]
    fn a_barrier_trade_is_on_the_exits_and_the_limit_its_decision_sets() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &barrier_config(10), (Some(8), None));
        handle.start(&mut probe, &[]).unwrap();

        let trade = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            (trade.side, trade.target),
            (
                OrderSide::Buy,
                TradeTarget::Barrier {
                    take_profit: Price::from("0.70140"),
                    stop: Price::from("0.69930"),
                    limit: SignedDuration::from_hours(2),
                }
            )
        );
    }

    /// `barrier_config` with a take profit 2 and a stop 1 average true
    /// ranges away, the ATR of 2 hourly bars, the cadence's.
    fn atr_config() -> ModelConfig {
        let mut config = barrier_config(10);
        config.targets.barrier = Some(Barrier {
            take_profit: Distance::Atr(Decimal::from(2)),
            stop: Distance::Atr(Decimal::ONE),
            limit: SignedDuration::from_hours(2),
            atr: Some(AtrConfig { period: 2 }),
        });
        config
    }

    /// The hourly mid bar of AUD/USD closing at `close_at`.
    fn hour_bar(close_at: &str, [open, high, low, close]: [&str; 4]) -> Bar {
        let ts = UnixNanos::from(at(close_at));
        Bar::new(
            BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL"),
            Price::from(open),
            Price::from(high),
            Price::from(low),
            Price::from(close),
            Quantity::from(1),
            ts,
            ts,
        )
    }

    /// The bars a live warmup requests: the strategy's, and the hourly
    /// bars of the barrier's ATR, once.
    #[test]
    fn a_live_warmup_requests_the_bars_of_the_barriers_atr() {
        let models = tempfile::tempdir().unwrap();
        let (probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &atr_config(), (Some(8), None));
        let minutes = BarType::from("AUD/USD.SIM-1-MINUTE-MID-INTERNAL");
        let hours = BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL");

        assert_eq!(handle.warmup_bar_types(&[minutes]), vec![minutes, hours]);
        assert_eq!(handle.warmup_bar_types(&[hours]), vec![hours]);
    }

    /// A barrier in basis points has no ATR: the warmup is the strategy's
    /// bars alone.
    #[test]
    fn a_live_warmup_without_an_atr_requests_the_strategys_bars_alone() {
        let models = tempfile::tempdir().unwrap();
        let (probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &barrier_config(10), (Some(8), None));
        let minutes = BarType::from("AUD/USD.SIM-1-MINUTE-MID-INTERNAL");

        assert_eq!(handle.warmup_bar_types(&[minutes]), vec![minutes]);
    }

    /// Worked by hand. The warmup's response arrives at 12:00, Nautilus
    /// having built from it the hourly bars closing 11:00 (high 0.70000,
    /// low 0.69880, close 0.69950: a true range of 0.00120) and 12:00
    /// (high 0.70020, low 0.69940, above and below the close before: a
    /// true range of 0.00080) into the cache. The ATR of 2 bars, Wilder's
    /// seeded by their mean, is 0.00100, as a dataset run replaying those
    /// bars has it at 12:00: from the mid of 0.70000, the long's take
    /// profit 0.00200 above, 0.70200, and its stop 0.00100 below, 0.69900,
    /// worth 1 USD on 1000 AUD/USD, 0.537 USD expected, above the margin.
    /// Without the warmup the ATR is cold and the decision no trade.
    #[test]
    fn a_live_warmup_sets_the_first_decisions_stop_from_the_barriers_atr() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &atr_config(), (Some(8), None));
        handle.start(&mut probe, &[]).unwrap();
        probe
            .cache_rc()
            .borrow_mut()
            .add_bars(&[
                hour_bar(
                    "2026-10-22T11:00:00Z",
                    ["0.69900", "0.70000", "0.69880", "0.69950"],
                ),
                hour_bar(
                    "2026-10-22T12:00:00Z",
                    ["0.69950", "0.70020", "0.69940", "0.70000"],
                ),
            ])
            .unwrap();
        let response = [QuoteTick::new(
            InstrumentId::from(AUD),
            Price::from("0.69990"),
            Price::from("0.70010"),
            Quantity::from(1),
            Quantity::from(1),
            UnixNanos::from(at("2026-10-22T12:00:00Z")),
            UnixNanos::from(at("2026-10-22T12:00:00Z")),
        )];

        for bar in warmup::bars(&probe, &response) {
            handle.handle_warmup_bar(&bar).unwrap();
        }
        let trade = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            trade.target,
            TradeTarget::Barrier {
                take_profit: Price::from("0.70200"),
                stop: Price::from("0.69900"),
                limit: SignedDuration::from_hours(2),
            }
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

        assert_eq!(probabilities, [Some(c(0.7)), Some(c(0.8)), Some(c(0.8))]);
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

        assert_eq!(before, Some(c(0.7)));
        assert_eq!(
            probability(handle.decide(&mut probe).unwrap()),
            Some(c(0.9))
        );
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

        assert_eq!(probabilities, [Some(c(0.7)), Some(c(0.7)), Some(c(0.7))]);
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
        refused.manifest.probes[0].prediction = c(0.9) + f64::EPSILON;

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
                venue: venue(),
            }),
            at("2026-10-22T12:00:00.010Z")
        );
        let catalog = tempfile::tempdir().unwrap();
        assert_eq!(
            next(live(
                models,
                catalog.path(),
                Launcher::in_process(|_| unreachable!())
            )),
            at("2026-10-22T12:00:05.010Z")
        );
    }

    /// Live trading on the models under `models`, retraining through
    /// `launcher` on the catalog at `catalog`.
    fn live(models: PathBuf, catalog: &Path, launcher: Launcher) -> Mode {
        Mode::Live {
            models,
            venue: venue(),
            launcher,
            catalog: catalog.to_path_buf(),
        }
    }

    /// A catalog whose AUD/USD quotes end at `time`, the series the
    /// strategy reads (`requirements`).
    fn catalog_ending(time: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let ts = UnixNanos::from(at(time));
        let quote = QuoteTick::new(
            InstrumentId::from(AUD),
            Price::from("0.69990"),
            Price::from("0.70010"),
            Quantity::from(1),
            Quantity::from(1),
            ts,
            ts,
        );
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .write_to_parquet(&[quote], None, None, None)
            .unwrap();
        dir
    }

    /// The probe's handle trading live on the models under `models`,
    /// retraining through `launcher` on the catalog at `catalog`, started,
    /// its features warm: a quote came at the time of `probe`.
    fn live_handle(
        probe: &mut Decider,
        models: &Path,
        catalog: &Path,
        launcher: Launcher,
    ) -> Rc<ModelHandle> {
        let features = features();
        warm(&features, probe.clock().timestamp_ns());
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements(),
            live(models.to_path_buf(), catalog, launcher),
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
        let catalog = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-21T00:00:00Z", 7)).unwrap();
        store.publish(&baseline("2026-10-22T00:00:00Z", 8)).unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = live_handle(
            &mut probe,
            models.path(),
            catalog.path(),
            Launcher::in_process(|_| unreachable!()),
        );

        assert_eq!(
            probability(handle.decide(&mut probe).unwrap()),
            Some(c(0.8))
        );
    }

    #[test]
    fn the_retrain_timer_is_the_strategys_on_the_grid() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));
        live_handle(
            &mut probe,
            models.path(),
            catalog.path(),
            Launcher::in_process(|_| unreachable!()),
        );

        let next = clock.borrow().next_time_ns("RETRAIN-HANDLE-001").unwrap();

        assert_eq!(Timestamp::from(next), at("2026-10-23T00:00:00Z"));
    }

    #[test]
    fn a_model_a_live_retrain_publishes_is_used_from_the_next_decision() {
        let models = tempfile::tempdir().unwrap();
        let catalog = catalog_ending("2026-10-22T23:00:00Z");
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
                holdout: HOLDOUT,
            }
            .into()
        });
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let handle = live_handle(&mut probe, models.path(), catalog.path(), launcher);
        let before = probability(handle.decide(&mut probe).unwrap());

        run_to(&mut probe, &handle, &clock, "2026-10-23T00:00:00Z");
        let trained_at = publishing.recv().unwrap();
        advance(&handle, &clock, "2026-10-23T01:00:00Z");

        // The catalog ends before the retrain's time: trained at its end.
        assert_eq!(trained_at, at("2026-10-22T23:00:00Z"));
        assert_eq!(before, Some(c(0.7)));
        assert_eq!(
            probability(handle.decide(&mut probe).unwrap()),
            Some(c(0.9))
        );
    }

    #[test]
    fn a_live_retrain_keeping_the_model_leaves_the_current_one() {
        let models = tempfile::tempdir().unwrap();
        let catalog = catalog_ending("2026-10-22T23:00:00Z");
        store(models.path())
            .publish(&baseline("2026-10-22T10:00:00Z", 7))
            .unwrap();
        let (kept, keeping) = std::sync::mpsc::channel();
        let launcher = Launcher::in_process(move |at| {
            kept.send(at).unwrap();
            crate::model::train::Outcome::Kept(crate::model::train::Kept::Refused {
                why: Refusal::MinPnl {
                    pnl: 0.25,
                    min_pnl: 0.5,
                },
                holdout: HOLDOUT,
            })
            .into()
        });
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let handle = live_handle(&mut probe, models.path(), catalog.path(), launcher);

        run_to(&mut probe, &handle, &clock, "2026-10-23T00:00:00Z");
        keeping.recv().unwrap();
        advance(&handle, &clock, "2026-10-23T01:00:00Z");

        assert_eq!(
            probability(handle.decide(&mut probe).unwrap()),
            Some(c(0.7))
        );
        assert_eq!(
            store(models.path()).valid_froms().unwrap(),
            [at("2026-10-22T10:00:00Z")]
        );
    }

    /// A baseline valid from `valid_from` on rows of `feature_names`, of a
    /// strategy deciding with `features` as `config` says (its decisions
    /// and targets), predicting `trues` tenths whatever the row, calibrated
    /// by `CALIBRATION`, expecting a win and a loss of 1.
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
                    &venue(),
                )
                .unwrap()
                .to_string(),
                feature_names: feature_names.iter().map(ToString::to_string).collect(),
                targets: targets.clone(),
                label: Label::Barrier,
                side: None,
                learner: LearnerConfig::Baseline {},
                calibration: CALIBRATION,
                payoff: Some(Payoff {
                    win: 1.0,
                    loss: 1.0,
                }),
                holdout: HOLDOUT,
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
            min_pnl: 0.0,
            min_trades: 1,
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
    /// `models` as `config` says, its primary a long of 0.6 from midnight
    /// (expecting 0.197 USD, above its margin of nought),
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
            &venue(),
            Side::Long,
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
            ModelStore::meta(
                models,
                StrategyId::from(STRATEGY),
                &features,
                config,
                config.meta.as_ref().unwrap(),
                &requirements(),
                &venue(),
            )
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
                venue: venue(),
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
                instrument_id: InstrumentId::from(AUD),
                side: OrderSide::Buy,
                quantity: Quantity::from(1000),
                target: TradeTarget::Horizon(SignedDuration::from_hours(1)),
                probability: c(0.7),
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

    /// From the threshold 0.6 to 1, 0.5 to 2 times the size: the meta's
    /// c(0.8), 0.768525, is 1.131968 times 1000.
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

        let quantity = trade.quantity.as_f64();
        assert!((quantity - 1_131.967_938).abs() < 1e-6, "{quantity}");
        assert_eq!(trade.probability, c(0.8));
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
            &venue(),
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
                venue: venue(),
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
                instrument_id: InstrumentId::from(AUD),
                side: OrderSide::Sell,
                quantity: Quantity::from(1000),
                // 10 bps of the mid of 0.70000 below and above it.
                target: TradeTarget::Barrier {
                    take_profit: Price::from("0.69930"),
                    stop: Price::from("0.70070"),
                    limit: SignedDuration::from_hours(2),
                },
                probability: c(0.7),
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
                venue: venue(),
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("side"), "{err}");
    }

    /// 2026-10-22T12:00:00Z, the time of the restart.
    const RESTART: &str = "2026-10-22T12:00:00Z";

    /// Adds to the cache of `probe` a trade of `strategy` buying 1000
    /// AUD/USD as a restart finds it, its entry `entry` filled at `filled`
    /// into an open position of its own: a market entry, a bracket's if
    /// `legs`, with its take profit and stop.
    fn open_trade(probe: &Decider, strategy: &str, entry: &str, legs: bool, filled: &str) {
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        let account = AccountId::from("SIM-001");
        let entry_id = ClientOrderId::from(entry);
        let (tp, sl) = (
            ClientOrderId::from(format!("{entry}-TP").as_str()),
            ClientOrderId::from(format!("{entry}-SL").as_str()),
        );
        let order = |id: ClientOrderId, side| {
            let mut builder = OrderTestBuilder::new(OrderType::Market);
            builder
                .instrument_id(aud.id())
                .strategy_id(StrategyId::from(strategy))
                .client_order_id(id)
                .side(side)
                .quantity(Quantity::from(1000));
            builder
        };
        let mut entry_builder = order(entry_id, OrderSide::Buy);
        if legs {
            entry_builder
                .contingency_type(ContingencyType::Oto)
                .linked_order_ids(vec![tp, sl]);
        }
        let entry_order = entry_builder.build();
        let cache = probe.cache_rc();
        let mut cache = cache.borrow_mut();
        cache
            .add_order(entry_order.clone(), None, None, false)
            .unwrap();
        if legs {
            for (leg, other) in [(tp, sl), (sl, tp)] {
                let leg = order(leg, OrderSide::Sell)
                    .parent_order_id(entry_id)
                    .contingency_type(ContingencyType::Oco)
                    .linked_order_ids(vec![other])
                    .reduce_only(true)
                    .build();
                cache.add_order(leg, None, None, false).unwrap();
            }
        }
        let position_id = PositionId::from(format!("P-{entry}").as_str());
        let filled = TestOrderEventStubs::filled(
            &entry_order,
            &aud,
            Some(TradeId::from(entry)),
            Some(position_id),
            Some(Price::from("0.70000")),
            None,
            None,
            None,
            Some(UnixNanos::from(at(filled))),
            Some(account),
        );
        for event in [
            TestOrderEventStubs::submitted(&entry_order, account),
            TestOrderEventStubs::accepted(&entry_order, account, VenueOrderId::from(entry)),
            filled.clone(),
        ] {
            cache.update_order(&event).unwrap();
        }
        let OrderEventAny::Filled(fill) = filled else {
            panic!("not a fill");
        };
        cache
            .add_position_id(&position_id, &aud.id().venue, &entry_id, &fill.strategy_id)
            .unwrap();
        cache
            .add_position(&Position::new(&aud, fill), OmsType::Hedging)
            .unwrap();
    }

    /// The time close of the trade of `entry`, as the clock holds it.
    fn time_close(clock: &Rc<RefCell<VirtualClock>>, entry: &str) -> Option<UnixNanos> {
        clock.borrow().next_time_ns(&format!("TARGET-TIME:{entry}"))
    }

    /// A barrier closed 2 hours after its entry: the bracket filled 30
    /// minutes before the restart is closed 90 minutes after it, the one
    /// filled 3 hours before at once.
    #[test]
    fn a_restart_sets_the_time_close_of_each_open_barrier_trade_again() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at(RESTART));
        open_trade(&probe, STRATEGY, "O-1", true, "2026-10-22T11:30:00Z");
        open_trade(&probe, STRATEGY, "O-2", true, "2026-10-22T09:00:00Z");
        let handle = sides_handle(&probe, models.path(), &barrier_config(10), (None, None));

        handle.start(&mut probe, &[]).unwrap();

        assert_eq!(
            time_close(&clock, "O-1"),
            Some(UnixNanos::from(at("2026-10-22T13:30:00Z")))
        );
        assert_eq!(
            time_close(&clock, "O-2"),
            Some(UnixNanos::from(at(RESTART)))
        );
    }

    /// A horizon of an hour: the market entry filled 20 minutes before the
    /// restart is closed 40 minutes after it. Another strategy's trade is
    /// not the handle's.
    #[test]
    fn a_restart_sets_the_time_close_of_its_own_open_horizon_trades_again() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at(RESTART));
        open_trade(&probe, STRATEGY, "O-1", false, "2026-10-22T11:40:00Z");
        open_trade(&probe, "OTHER-001", "O-2", false, "2026-10-22T11:40:00Z");
        let handle = handle(&probe, models.path());

        handle.start(&mut probe, &[]).unwrap();

        assert_eq!(
            time_close(&clock, "O-1"),
            Some(UnixNanos::from(at("2026-10-22T12:40:00Z")))
        );
        assert_eq!(time_close(&clock, "O-2"), None);
    }

    /// The market entry filled 90 minutes before the restart, its hour
    /// over: its alert fires at once, and the handle closes its position
    /// with a market sell of its 1000, tagged as a time close.
    #[test]
    fn a_trade_open_at_a_restart_is_closed_at_its_time() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at(RESTART));
        open_trade(&probe, STRATEGY, "O-1", false, "2026-10-22T10:30:00Z");
        let handle = handle(&probe, models.path());
        handle.start(&mut probe, &[]).unwrap();

        let events = clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(RESTART)), true);
        for event in events {
            handle.on_time_event(&mut probe, &event).unwrap();
        }

        let cache = probe.cache();
        let closes: Vec<_> = cache
            .orders(None, None, None, None, None)
            .into_iter()
            .filter(|order| order.client_order_id() != ClientOrderId::from("O-1"))
            .map(|order| {
                (
                    order.order_side(),
                    order.quantity(),
                    order.is_reduce_only(),
                    order.tags().unwrap_or_default().to_vec(),
                )
            })
            .collect();
        assert_eq!(
            closes,
            [(
                OrderSide::Sell,
                Quantity::from(1000),
                true,
                vec![ustr::Ustr::from("TARGET-TIME")]
            )]
        );
    }
}
