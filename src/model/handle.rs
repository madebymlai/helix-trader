//! The model handle: what a strategy trading on a learned model holds, the
//! one place it decides, in a backtest, live and a dataset run alike.
//!
//! The strategy shares a `ModelHandle` (an `Rc`), built from its features
//! and the model section of its config, in a `Mode`. `start` from
//! `on_start` registers the features (and starts the targets' ATR, in a
//! dataset run) and starts the decision cadence; `decide`, at a decision
//! of the cadence (its timer, `decide::is_decision`), snapshots the
//! features, if the decision is fresh (data of the targets' instrument
//! since the previous) and every one is initialized, and publishes their
//! row (`decide::decide`), then:
//!
//! - trading (`Mode::Backtest`, `Mode::Live`): takes, for each side of
//!   the config's margins, the latest model of its store
//!   (`store::ModelStore`) valid at the decision, its calibrated
//!   probability on the row, and its expected net P&L (`payoff`): on a
//!   barrier label, the expected R times the worth of the stop the
//!   decision sets (`targets::Stops`, from the decision's price and the
//!   barrier's ATR, which the handle runs), on a horizon in money. It
//!   returns the `Decision` of the side clearing its margin, the larger of
//!   two (`payoff::pick`), as plain data: the side, the targets' size, the
//!   probability and expected net P&L, and the target of its label as the
//!   decision sets it on that side, its reference price, take profit,
//!   stop and time limit. The handle sends no order and handles no
//!   trade's time: the strategy trades the decision, as given
//!   (`Decision::trade`, sent through `strategy::orders` as a dataset run
//!   sends its targets) or otherwise, and closes its trades through the
//!   same module. A side with no model valid yet never trades; a stale
//!   decision, cold features, a barrier ATR not yet initialized, no side
//!   clearing its margin or two tied are no decision, never an error.
//! - in a dataset run (`Mode::Dataset`): opens the decision's targets
//!   (`targets::Targets`, through `strategy::orders`) and returns no
//!   decision, so the strategy places no trade of its own; the strategy
//!   hands the order module its time events, which close the targets'
//!   trades, and the handle its order and position events, and the rows
//!   the targets settle are collected for the caller of the run.
//!
//! With a meta model (`model::meta`), a trade of the primary is taken only
//! if the meta model in use, on the row with the primary's side and that
//! side's probability, predicts at least its threshold, sized from that
//! probability if the meta sizes (`MetaConfig::size`); no meta model valid
//! yet is no trade.
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
//! The handle owns a `rolls::Roller` of the features' tick series
//! (`FeatureSet::ticks`, custom data among them) and of the tick series of
//! the traded instrument its data requirements hold, which the harness
//! reads for the decisions' freshness, its quotes among them when no
//! series of the strategy reads it (`StandardConfig::data_requirements`,
//! the decision's reference price, the venue's fills and the barrier's
//! ATR then coming from them): `start` subscribes each
//! on the contract standing for it, a plain instrument once, a root on its
//! front contract with its roll alert, and the strategy hands the handle
//! its time events (after `strategy::orders::on_time_event`), the roller
//! first. At a roll the root's features move to the post contract and
//! restart (`FeatureSet::roll`), so decisions wait on the strategy's
//! initialized gate until they refill. The same code runs in a backtest
//! and a dataset run, on the catalog's contract slices
//! (`DataRequirements::slices`), and live, where the roller loads each
//! post contract before its roll (`Roller::live`).
//!
//! A node builds composite bars `warmup::bar_build_delay` after their
//! close, a backtest and a dataset run as late as a live node, so the
//! handle starts the cadence with that delay whatever its mode (see
//! `decide`).
//!
//! The handle builds the strategy's warmup (`strategy::warmup::Warmup`)
//! from its requirements and mode, reading the features' tick series
//! (`FeatureSet::ticks`), and `start` starts it, the features registered
//! first; the strategy delegates its historical-data hooks to it
//! (`warmup`). Live, the warmup requests the history of the features'
//! ticks, a root's on the contract standing for it, their custom data
//! from the catalog alone, handed to the features, and the roller's
//! subscriptions continue from its end, so the first decision after a
//! start is warm. A series of the traded instrument no feature reads,
//! custom data among them, has no warmup. It warms the barrier's ATR from
//! the warmup request, as it does the features: the request builds the
//! ATR's bars too (`Stops::warmed`), and the warmup hands the bars of the
//! response to both, so the first decision after a start sets the stop a
//! dataset run sets there. A live node sizes its cache from the same
//! warmup, the ATR's bars included.
//!
//! Live, the handle also retrains its model (`launcher::Retraining`): the
//! retrain timer, keyed by the strategy, launches a train at each retrain
//! time, one at a time, and `decide` first polls the running one, so the
//! model it publishes is used from the next decision. Never retrained, no
//! timer is started and no train launched: the handle trades on the store
//! as it is, a model `train --at` publishes used from the next decision.

use std::{
    cell::RefCell,
    fmt::Debug,
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative, indicators::SharedActorIndicator},
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_model::{
    data::CustomData,
    enums::OrderSide,
    events::{OrderCanceled, OrderDenied, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId},
    types::{Price, Quantity, fixed::FIXED_PRECISION},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{
    config::{MetaConfig, ModelConfig},
    dataset::DatasetVenue,
    decide::{self, DecisionRule},
    features::{FeatureSet, meta_row},
    launcher::{Launcher, Retraining},
    payoff,
    schedule::Retrain,
    store::{LoadedModel, MetaLevel, ModelSet, SideModel, Swap},
    targets::{self, BarrierExits, Label, Side, Stops, TargetRow, Targets, TargetsConfig},
};
use crate::{
    rolls::Roller,
    strategy::{
        DataRequirements,
        orders::{Exits, Trade},
        warmup::{self, BookIndicator, CustomDataIndicator, Warmup},
    },
};

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
    /// the live node's bar build delay late, and retrains, if ever, on the
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

/// A decision to trade, as plain data: the side the model picks, of the
/// targets' size and instrument, its calibrated probability and expected
/// net P&L, and the target its model was labelled on as the decision sets
/// it on that side, the prices a dataset run sets there. The handle sends
/// nothing: a strategy trades it as given (`trade`), or otherwise, and then
/// the model's holdout figures describe the labelled trade, not its own.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    /// The targets' instrument.
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    /// The targets' size, or, with a meta model sizing, its multiple of it.
    pub quantity: Quantity,
    /// The calibrated probability that the label is true of the model
    /// deciding: the meta model's, if any, else that of the primary's
    /// model of the side.
    pub probability: f64,
    /// The net P&L the primary's model of the side expects, in the
    /// instrument's P&L currency, on the targets' size.
    pub expected_pnl: f64,
    /// The price the target is set from (`targets::Stops`): the mid of the
    /// instrument's last quote, else its last trade, else its last close.
    pub reference: Price,
    /// On a barrier, its take profit and stop on the instrument's ticks;
    /// none on a horizon.
    pub take_profit: Option<Price>,
    pub stop: Option<Price>,
    /// When the target ends after its send: the barrier's limit or the
    /// horizon.
    pub time_limit: SignedDuration,
}

impl Decision {
    /// The trade the model was labelled on, the one a dataset run sends at
    /// the decision on that side: a bracket with the barrier's exits, or a
    /// market entry, closed at the time limit (`strategy::orders`).
    #[must_use]
    pub const fn trade(&self) -> Trade {
        Trade {
            instrument_id: self.instrument_id,
            side: self.side,
            quantity: self.quantity,
            exits: Exits {
                take_profit: self.take_profit,
                stop_loss: self.stop,
                time_limit: Some(self.time_limit),
            },
        }
    }
}

/// What a decision on a side sets of its target.
#[derive(Debug, Clone, Copy)]
struct Target {
    reference: Price,
    take_profit: Option<Price>,
    stop: Option<Price>,
    time_limit: SignedDuration,
}

impl Target {
    /// The barrier `exits`, closed after `limit`.
    const fn barrier(exits: BarrierExits, limit: SignedDuration) -> Self {
        Self {
            reference: exits.reference,
            take_profit: Some(exits.take_profit),
            stop: Some(exits.stop),
            time_limit: limit,
        }
    }
}

/// A strategy's model handle, shared with it as an `Rc`.
#[derive(Debug)]
pub struct ModelHandle {
    strategy_id: StrategyId,
    features: Rc<FeatureSet>,
    rule: DecisionRule,
    /// How long after its grid times the cadence decides, before its
    /// margin: the node's bar build delay of the strategy's bar types
    /// (`warmup::bar_build_delay`), in every mode.
    delay: SignedDuration,
    /// The instrument traded, whose data makes a decision fresh.
    traded: InstrumentId,
    serving: Serving,
    /// The strategy's warmup, which feeds the features and the barrier's
    /// ATR the handle trades on.
    warmup: Rc<Warmup>,
    /// Subscribes the features' ticks, on the contract standing for each
    /// root, and the requirements' tick series of the traded instrument.
    roller: Roller,
}

#[derive(Debug)]
enum Serving {
    Trading(Box<Trading>),
    Dataset {
        targets: Rc<Targets>,
        rows: SettledRows,
    },
}

impl Serving {
    /// The stops of the barrier the handle trades on, if any: none on a
    /// horizon, and in a dataset run, which warms on its replay.
    fn stops(&self) -> Option<&Stops> {
        let Serving::Trading(trading) = self else {
            return None;
        };
        match &trading.labelled {
            Labelled::Barrier(stops) => Some(stops),
            Labelled::Horizon(_) => None,
        }
    }
}

/// The trading side of a handle.
#[derive(Debug)]
struct Trading {
    /// Live, the retrains; none in a backtest.
    retraining: Option<Retraining>,
    /// The models of each side traded, which pick the side of a trade.
    sides: Vec<SideModels>,
    /// The target of the side models' label, which their trades are on.
    labelled: Labelled,
    /// The meta model taking the side models' trades, if any.
    meta: Option<MetaModels>,
    /// The targets' instrument.
    instrument_id: InstrumentId,
    /// The targets' size.
    quantity: Quantity,
}

/// The target the side models' trades are on, their label's.
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
    models: Models<SideModel>,
}

/// The models of one level of the store, swapped in decision by decision.
#[derive(Debug)]
struct Models<M>(RefCell<Swap<M>>);

/// A meta model's models, and its config.
#[derive(Debug)]
struct MetaModels {
    models: Models<LoadedModel>,
    config: MetaConfig,
}

impl ModelHandle {
    /// The handle of `strategy_id`, deciding on `features` as `config`
    /// says, for a strategy of `requirements`, in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule is invalid, a store cannot be
    /// keyed or, in a dataset run, the targets are invalid.
    pub fn new(
        strategy_id: StrategyId,
        features: Rc<FeatureSet>,
        config: &ModelConfig,
        requirements: &DataRequirements,
        mode: Mode,
    ) -> anyhow::Result<Rc<Self>> {
        let trading = |models: &Path,
                       venue: &DatasetVenue,
                       retraining: Option<Retraining>|
         -> anyhow::Result<Trading> {
            let models =
                ModelSet::new(models, strategy_id, &features, config, requirements, venue)?;
            let meta = models.meta().map(MetaModels::of);
            let sides = models
                .sides()
                .map(|level| SideModels {
                    side: level.side(),
                    margin: level.margin(),
                    models: Models(RefCell::new(level.swap())),
                })
                .collect();
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
            Ok(Trading {
                retraining,
                sides,
                labelled,
                meta,
                instrument_id: config.targets.instrument_id,
                quantity: config.targets.quantity,
            })
        };
        Self::built(
            strategy_id,
            &features,
            (config.decision, &config.targets, config.retrain),
            requirements,
            mode,
            trading,
        )
    }

    /// The handle of `strategy_id` deciding with `features` on `rule`, with
    /// `targets`, retrained as `retrain` says, for a strategy of
    /// `requirements`, in `mode`: trading as `trading` builds it on the
    /// models under the store's root, of the targets of the dataset runs on
    /// the mode's venue, with the mode's retrains, or opening the targets.
    fn built(
        strategy_id: StrategyId,
        features: &Rc<FeatureSet>,
        (rule, targets, retrain): (DecisionRule, &TargetsConfig, Retrain),
        requirements: &DataRequirements,
        mode: Mode,
        trading: impl FnOnce(&Path, &DatasetVenue, Option<Retraining>) -> anyhow::Result<Trading>,
    ) -> anyhow::Result<Rc<Self>> {
        rule.validate()?;
        let traded = targets.instrument_id;
        let mut ticks = features.ticks();
        for tick in requirements
            .ticks
            .iter()
            .filter(|tick| tick.instrument_id == traded)
        {
            if !ticks.contains(tick) {
                ticks.push(tick.clone());
            }
        }
        let warmup = match mode {
            Mode::Live { .. } => Warmup::live(requirements),
            Mode::Backtest { .. } | Mode::Dataset { .. } => Warmup::replayed(requirements),
        }
        .reading(features.ticks())
        .feeding(Rc::clone(features) as SharedActorIndicator)
        .feeding_books(Rc::clone(features) as Rc<dyn BookIndicator>)
        .feeding_custom_data(Rc::clone(features) as Rc<dyn CustomDataIndicator>);
        let roller = match mode {
            Mode::Live { .. } => Roller::live(ticks),
            Mode::Backtest { .. } | Mode::Dataset { .. } => Roller::replayed(ticks),
        };
        let delay = warmup::bar_build_delay(&requirements.bar_types);
        let serving = match mode {
            Mode::Backtest { models, venue } => {
                Serving::Trading(Box::new(trading(&models, &venue, None)?))
            }
            Mode::Live {
                models,
                venue,
                launcher,
                catalog,
            } => {
                let retraining = Retraining::new(
                    strategy_id,
                    retrain,
                    catalog,
                    requirements.clone(),
                    launcher,
                );
                Serving::Trading(Box::new(trading(&models, &venue, Some(retraining))?))
            }
            Mode::Dataset { rows } => Serving::Dataset {
                targets: Targets::new(targets.clone(), rule, requirements)?,
                rows,
            },
        };
        let warmup = match serving.stops() {
            Some(stops) => stops.warmed(warmup),
            None => warmup,
        };
        Ok(Rc::new(Self {
            strategy_id,
            features: Rc::clone(features),
            rule,
            delay,
            traded,
            serving,
            warmup: Rc::new(warmup),
            roller,
        }))
    }

    /// Starts the handle on `strategy`, from `on_start`: registers the
    /// features, starts the strategy's warmup (`Warmup::start`), live
    /// requesting the history of the bars and of the features' ticks, then
    /// the roller, which subscribes the features' ticks and the traded
    /// instrument's from now, where the warmup ends, a root's on the
    /// contract standing for it; starts the barrier's ATR (the stops',
    /// trading on a model labelled on the barrier, or, in a dataset run,
    /// the targets', and collects the rows they settle); then
    /// starts the decision cadence, `warmup::bar_build_delay` late, and live
    /// the retrain timer, if retrained (`schedule::start`).
    ///
    /// # Errors
    ///
    /// Returns an error if the warmup fails to start, or Nautilus refuses
    /// a roll alert, the cadence's timer or the retrain timer.
    pub fn start<S>(&self, strategy: &mut S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative + Debug + 'static,
    {
        // Registered first: a warmup request the catalog answers alone is
        // answered within `start`, its quotes and trades handed to the
        // indicators registered by then.
        self.features.register(strategy);
        self.warmup.start(strategy)?;
        self.roller.start(strategy)?;
        match &self.serving {
            Serving::Trading(trading) => {
                if let Some(stops) = self.serving.stops() {
                    stops.start(strategy);
                }
                if let Some(retraining) = &trading.retraining {
                    retraining.start(strategy)?;
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
            }
        }
        decide::start(strategy, self.rule, self.delay)
    }

    /// The strategy's warmup, which the handle builds and starts: its
    /// strategy delegates `on_data_response`, `on_historical_quotes`,
    /// `on_historical_trades`, `on_historical_bars`,
    /// `on_historical_book_depth`, `on_historical_book_deltas` and
    /// `on_historical_data` to it, and a live node sizes its cache from it.
    #[must_use]
    pub fn warmup(&self) -> Rc<Warmup> {
        Rc::clone(&self.warmup)
    }

    /// Decides now, for `strategy`: the decision to trade, if any.
    /// Live, first logs the running retrain if it finished, whose model
    /// the store then lists.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict, or, in a dataset run,
    /// as `Targets::open` does.
    pub fn decide<S>(&self, strategy: &mut S) -> anyhow::Result<Option<Decision>>
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
                trading.decide(strategy, Timestamp::from(row.ts_event), &row.features)
            }
            Serving::Dataset { targets, .. } => {
                targets.open(strategy, row)?;
                Ok(None)
            }
        }
    }

    /// Hands `event` of `strategy` to the roller first: at a roll alert
    /// it switches the root's subscriptions to the post contract, and the
    /// features reading the root move there and restart (`FeatureSet::roll`),
    /// the decisions waiting until they are initialized again. Live, any
    /// other event goes to the retrains, which launch one on their timer's.
    /// A trade's time alert is the strategy's
    /// (`strategy::orders::on_time_event`).
    ///
    /// # Errors
    ///
    /// Returns an error if the clock refuses the next roll alert.
    pub fn on_time_event<S>(&self, strategy: &mut S, event: &TimeEvent) -> anyhow::Result<()>
    where
        S: DataActor + DataActorNative + Debug + 'static,
    {
        if let Some(roll) = self.roller.on_time_event(strategy, event)? {
            self.features.roll(strategy, &roll);
            return Ok(());
        }
        if let Serving::Trading(trading) = &self.serving
            && let Some(retraining) = &trading.retraining
        {
            retraining.on_time_event(event);
        }
        Ok(())
    }

    /// Hands `event` to the targets of a dataset run; a no-op trading.
    pub fn on_order_canceled<S>(&self, strategy: &S, event: &OrderCanceled)
    where
        S: DataActor + DataActorNative,
    {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_order_canceled(strategy, event);
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
    /// The decision of `strategy` at `now` on `row`: of the side whose
    /// latest model valid then expects a net P&L clearing its margin, the
    /// larger of two, on the target of its label as the decision sets it
    /// on the side, taken if its meta model takes it.
    fn decide<S>(
        &self,
        strategy: &S,
        now: Timestamp,
        row: &[f64],
    ) -> anyhow::Result<Option<Decision>>
    where
        S: DataActor + DataActorNative,
    {
        let mut decided = vec![];
        for SideModels {
            side,
            margin,
            models,
        } in &self.sides
        {
            // None without a model of the side; else its target,
            // probability and expected net P&L, none without the target's
            // unit yet.
            let Some(valued) = models.at(now, |model| {
                let probability = model.predict(row)?;
                let target = self.labelled.at(strategy, self.instrument_id, *side)?;
                Ok(target.map(|(target, unit)| {
                    (target, probability, model.expected_pnl(probability, unit))
                }))
            })?
            else {
                continue;
            };
            let Some((target, probability, expected)) = valued else {
                return Ok(None);
            };
            let decision = self.decision(side.order_side(), target, probability, expected);
            decided.push((decision, expected, *margin));
        }
        let Some(decision) = payoff::pick(decided) else {
            return Ok(None);
        };
        match &self.meta {
            None => Ok(Some(decision)),
            Some(meta) => meta.take(
                now,
                &meta_row(row, decision.side, decision.probability),
                decision,
            ),
        }
    }

    /// The decision on `side` of the targets' size and instrument, on
    /// `target`, at `probability`, expecting `expected_pnl`.
    const fn decision(
        &self,
        side: OrderSide,
        target: Target,
        probability: f64,
        expected_pnl: f64,
    ) -> Decision {
        Decision {
            instrument_id: self.instrument_id,
            side,
            quantity: self.quantity,
            probability,
            expected_pnl,
            reference: target.reference,
            take_profit: target.take_profit,
            stop: target.stop,
            time_limit: target.time_limit,
        }
    }
}

impl Labelled {
    /// The target of a trade on `side` of `instrument_id` at a decision of
    /// `strategy` now, and what the unit of its side's payoff is worth: on
    /// a barrier its exits there and the worth of its stop, none as
    /// `Stops::at`; on a horizon the horizon and 1, the payoff being in
    /// money, none with no price of the instrument yet.
    ///
    /// # Errors
    ///
    /// As `Stops::at`.
    fn at<S>(
        &self,
        strategy: &S,
        instrument_id: InstrumentId,
        side: Side,
    ) -> anyhow::Result<Option<(Target, f64)>>
    where
        S: DataActor + DataActorNative,
    {
        match self {
            Self::Barrier(stops) => Ok(stops
                .at(strategy, side)?
                .map(|exits| (Target::barrier(exits, stops.limit()), exits.stop_worth))),
            Self::Horizon(horizon) => {
                let reference = targets::reference(&strategy.cache(), &instrument_id);
                Ok(reference.map(|reference| {
                    let target = Target {
                        reference,
                        take_profit: None,
                        stop: None,
                        time_limit: *horizon,
                    };
                    (target, 1.0)
                }))
            }
        }
    }
}

impl<M> Models<M> {
    /// What `read` reads of the latest model valid at `now`, swapped in;
    /// none without a model. A store that cannot be listed keeps the
    /// current model.
    fn at<T>(
        &self,
        now: Timestamp,
        read: impl FnOnce(&M) -> anyhow::Result<T>,
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
    /// The models of the meta level `meta`, and its config.
    fn of(meta: &MetaLevel) -> Self {
        Self {
            models: Models(RefCell::new(meta.swap())),
            config: meta.config().clone(),
        }
    }

    /// The primary's `decision`, of the targets' size, if the meta model in
    /// use at `now` predicts at least its threshold on `row`, at its
    /// probability and sized from it; no trade without a meta model.
    ///
    /// # Errors
    ///
    /// Returns an error if the model fails to predict, or the size is no
    /// quantity.
    fn take(
        &self,
        now: Timestamp,
        row: &[f64],
        decision: Decision,
    ) -> anyhow::Result<Option<Decision>> {
        let Some(probability) = self.models.at(now, |model| model.predict(row))? else {
            return Ok(None);
        };
        if probability < self.config.threshold {
            return Ok(None);
        }
        let quantity = match self.config.size {
            None => decision.quantity,
            Some(_) => {
                let size = decision.quantity.as_decimal() * self.config.size(probability);
                Quantity::from_decimal_dp(size, FIXED_PRECISION)
                    .map_err(|e| anyhow::anyhow!("sizing {size}: {e}"))?
            }
        };
        Ok(Some(Decision {
            quantity,
            probability,
            ..decision
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
        messages::data::{
            CustomDataResponse, DataCommand, RequestCommand, SubscribeCommand, TradesResponse,
        },
        msgbus::{
            self, MessagingSwitchboard, TypedIntoHandler,
            switchboard::{get_custom_topic, get_quotes_topic, get_trades_topic},
        },
        timer::TimeEventCallback,
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        data::{Bar, BarType, NautilusDataType, QuoteTick, TradeTick},
        enums::AggressorSide,
        identifiers::{ClientId, InstrumentId, TradeId, TraderId},
        instruments::{InstrumentAny, stubs::audusd_sim},
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
        dataset::{self, Commission},
        features::testing::last_mid,
        holdout::{Earnings, Holdout, Refusal, Trades},
        learner::{LearnerConfig, LightGbmConfig, TrainingSet},
        payoff::Payoff,
        store::{ModelManifest, ModelStore, Probe, StoredModel},
        targets::{Barrier, DEFAULT_ATR, Distance, Label, TargetsConfig},
    };
    use crate::strategy::{AtrArgs, Declared, EmaArgs, IndicatorsConfig, TickSeries};
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

    /// The quotes of the AUD/USD traded, which the data requirements hold
    /// when no series reads it.
    fn aud_quotes() -> TickSeries {
        TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(AUD),
        }
    }

    #[derive(Debug)]
    struct Decider {
        core: StrategyCore,
        /// The warmup its historical custom data goes to, a model
        /// strategy's handle's, once given.
        warmup: Option<Rc<Warmup>>,
    }

    nautilus_strategy!(Decider);

    impl DataActor for Decider {
        /// Hands the warmup the custom data of a response, as a model
        /// strategy does.
        fn on_historical_data(&mut self, data: &dyn std::any::Any) -> anyhow::Result<()> {
            if let Some(warmup) = &self.warmup {
                warmup.on_historical_data(data);
            }
            Ok(())
        }
    }

    /// A strategy registered without a node at `now`, as the EMA's tests
    /// do, and its clock.
    fn registered(now: Timestamp) -> (Decider, Rc<RefCell<VirtualClock>>) {
        let mut probe = Decider {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(STRATEGY)),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            warmup: None,
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

    /// Advances `clock` to `to`, when a quote of AUD/USD comes to
    /// `features`, those a handle decides on: the market is open.
    fn advance(features: &FeatureSet, clock: &Rc<RefCell<VirtualClock>>, to: &str) {
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(to)), true);
        warm(features, UnixNanos::from(at(to)));
    }

    /// The last mid of AUD/USD, warm once a quote came.
    fn features() -> Rc<FeatureSet> {
        last_mid(InstrumentId::from(AUD)).build().unwrap()
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
        side_store(models, &features(), &config(), Side::Long)
    }

    /// The models of `features` and `config` under `models`.
    fn model_set(models: &Path, features: &FeatureSet, config: &ModelConfig) -> ModelSet {
        ModelSet::new(
            models,
            StrategyId::from(STRATEGY),
            features,
            config,
            &requirements(),
            &venue(),
        )
        .unwrap()
    }

    /// The store of the `side` models of `features` and `config` under
    /// `models`.
    fn side_store(
        models: &Path,
        features: &FeatureSet,
        config: &ModelConfig,
        side: Side,
    ) -> ModelStore {
        model_set(models, features, config)
            .side(side)
            .unwrap()
            .store()
            .clone()
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
    /// expecting a win and a loss of 1, its baseline the mean of its
    /// training labels, as the store keeps it for the probe's features,
    /// probed on one row.
    fn baseline(valid_from: &str, trues: usize) -> StoredModel {
        baseline_for(&features(), &config(), Side::Long, valid_from, trues)
    }

    /// The baseline of `baseline`, of `features` and `config`'s `side`.
    fn baseline_for(
        features: &FeatureSet,
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
                    features,
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
                baseline: trues as f64 / 10.0,
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
    /// `models`, deciding on `features`, warm: a quote came at the time of
    /// `probe`.
    fn handle(probe: &Decider, models: &Path, features: Rc<FeatureSet>) -> Rc<ModelHandle> {
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

    fn probability(decision: Option<Decision>) -> Option<f64> {
        decision.map(|decision| decision.probability)
    }

    #[test]
    fn no_model_in_the_store_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        assert_eq!(
            handle(&probe, models.path(), features())
                .decide(&mut probe)
                .unwrap(),
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
            Rc::clone(&features),
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
            probability(
                handle(&probe, models.path(), features)
                    .decide(&mut probe)
                    .unwrap()
            ),
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
            handle(&probe, models.path(), features())
                .decide(&mut probe)
                .unwrap(),
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
        let handle = handle(&probe, models.path(), features());
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at("2026-10-22T12:00:00Z")), true);

        assert_eq!(handle.decide(&mut probe).unwrap(), None);
    }

    /// No feature reads AUD/USD, which it trades: its quote in the cache,
    /// made at the decision, the harness's, makes the decision fresh, and
    /// the long model of these features trades at 0.8 calibrated.
    #[test]
    fn a_cadence_on_features_reading_no_data_of_the_traded_instrument_decides_on_its_quotes() {
        let models = tempfile::tempdir().unwrap();
        let features = last_mid(InstrumentId::from("GBP/USD.SIM")).build().unwrap();
        side_store(models.path(), &features, &config(), Side::Long)
            .publish(&baseline_for(
                &features,
                &config(),
                Side::Long,
                "2026-10-22T00:00:00Z",
                8,
            ))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        features
            .handle_quote(&QuoteTick::new(
                InstrumentId::from("GBP/USD.SIM"),
                Price::from("1.29990"),
                Price::from("1.30010"),
                Quantity::from(1),
                Quantity::from(1),
                UnixNanos::from(at("2026-10-22T10:00:00Z")),
                UnixNanos::from(at("2026-10-22T10:00:00Z")),
            ))
            .unwrap();
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements(),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap();

        assert_eq!(
            probability(handle.decide(&mut probe).unwrap()),
            Some(c(0.8))
        );
    }

    /// A strategy reading AUD/USD, which it trades, as trades alone, its
    /// features GBP/USD's quotes: the handle subscribes AUD/USD's trades,
    /// and no quotes of it, beside the features' GBP/USD quotes.
    #[test]
    fn the_handle_subscribes_the_series_the_strategy_reads_of_the_traded_instrument() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let gbp = InstrumentId::from("GBP/USD.SIM");
        let features = last_mid(gbp).build().unwrap();
        let tick = |data, instrument_id| TickSeries {
            data,
            instrument_id,
        };
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                tick(NautilusDataType::TradeTick, InstrumentId::from(AUD)),
                tick(NautilusDataType::QuoteTick, gbp),
            ],
            warmup: SignedDuration::ZERO,
        };
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements,
            Mode::Backtest {
                models: models.path().to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap();

        handle.start(&mut probe).unwrap();

        let aud = InstrumentId::from(AUD);
        assert_eq!(
            msgbus::exact_subscriber_count_trades(get_trades_topic(aud)),
            1
        );
        assert_eq!(
            msgbus::exact_subscriber_count_quotes(get_quotes_topic(aud)),
            0
        );
        assert_eq!(
            msgbus::exact_subscriber_count_quotes(get_quotes_topic(gbp)),
            1
        );
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
        let features = features();
        for (side, trues) in [(Side::Long, long), (Side::Short, short)] {
            if let Some(trues) = trues {
                side_store(models, &features, config, side)
                    .publish(&baseline_for(
                        &features,
                        config,
                        side,
                        "2026-10-22T00:00:00Z",
                        trues,
                    ))
                    .unwrap();
            }
        }
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
            Some(Decision {
                instrument_id: InstrumentId::from(AUD),
                side: OrderSide::Buy,
                quantity: Quantity::from(1000),
                probability: c(0.6),
                expected_pnl: 2.0 * c(0.6) - 1.0,
                reference: Price::from("0.70000"),
                take_profit: None,
                stop: None,
                time_limit: SignedDuration::from_hours(1),
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
            handle.start(&mut probe).unwrap();
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
        handle.start(&mut probe).unwrap();

        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            decision.trade(),
            Trade {
                instrument_id: InstrumentId::from(AUD),
                side: OrderSide::Buy,
                quantity: Quantity::from(1000),
                exits: Exits {
                    take_profit: Some(Price::from("0.70140")),
                    stop_loss: Some(Price::from("0.69930")),
                    time_limit: Some(SignedDuration::from_hours(2)),
                },
            }
        );
        assert_eq!(decision.reference, Price::from("0.70000"));
    }

    /// `barrier_config` with a take profit 2 and a stop 1 average true
    /// ranges away, the ATR of 2 hourly bars, the cadence's.
    fn atr_config() -> ModelConfig {
        let mut config = barrier_config(10);
        config.targets.barrier = Some(Barrier {
            take_profit: Distance::Atr(Decimal::from(2)),
            stop: Distance::Atr(Decimal::ONE),
            limit: SignedDuration::from_hours(2),
            atr: Some(AtrArgs {
                period: 2,
                ..DEFAULT_ATR
            }),
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

    /// The handle trading live on `config` for a strategy reading the
    /// quotes and the hourly bars of AUD/USD with a warmup of a day.
    fn hourly_live_handle(config: &ModelConfig, models: &Path, catalog: &Path) -> Rc<ModelHandle> {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL")],
            warmup: SignedDuration::from_hours(24),
            ..requirements()
        };
        ModelHandle::new(
            StrategyId::from(STRATEGY),
            features(),
            config,
            &requirements,
            live(
                models.to_path_buf(),
                catalog,
                Launcher::in_process(|_| unreachable!()),
            ),
        )
        .unwrap()
    }

    /// Deciding every minute, the barrier's ATR is on 1-minute bars, finer
    /// than the strategy's hourly bars: the live cache holds the day's
    /// 1,440 of them, and one spanning the start, on top of Nautilus'
    /// default 10,000, not the 25 hourly bars alone.
    #[test]
    fn a_live_cache_holds_the_warmup_of_the_barriers_atr_on_finer_bars() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let mut config = atr_config();
        config.decision = DecisionRule::Every(SignedDuration::from_mins(1));
        let handle = hourly_live_handle(&config, models.path(), catalog.path());

        let cache = handle.warmup().cache_config().unwrap();

        assert_eq!(cache.bar_capacity, 10_000 + 1_441);
    }

    /// A barrier in basis points has no ATR: the live cache holds the
    /// warmup of the strategy's bars alone, 24 hourly bars and one
    /// spanning the start.
    #[test]
    fn a_live_cache_without_an_atr_holds_the_warmup_of_the_strategys_bars() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let handle = hourly_live_handle(&barrier_config(10), models.path(), catalog.path());

        let cache = handle.warmup().cache_config().unwrap();

        assert_eq!(cache.bar_capacity, 10_000 + 25);
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
        handle.start(&mut probe).unwrap();
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

        handle.warmup().on_historical(&probe, &response).unwrap();
        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            (decision.take_profit, decision.stop, decision.time_limit),
            (
                Some(Price::from("0.70200")),
                Some(Price::from("0.69900")),
                SignedDuration::from_hours(2),
            )
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
        let features = features();
        let handle = handle(&probe, models.path(), Rc::clone(&features));

        let mut probabilities = vec![probability(handle.decide(&mut probe).unwrap())];
        for time in ["2026-10-22T12:00:00Z", "2026-10-22T12:59:59Z"] {
            advance(&features, &clock, time);
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
        let features = features();
        let handle = handle(&probe, models.path(), Rc::clone(&features));
        let before = probability(handle.decide(&mut probe).unwrap());

        // Live, a retrain at 12:00 publishes once its child finishes.
        store.publish(&baseline("2026-10-22T12:00:00Z", 9)).unwrap();
        advance(&features, &clock, "2026-10-22T13:00:00Z");

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
        let features = features();
        let handle = handle(&probe, models.path(), Rc::clone(&features));

        let mut probabilities = vec![probability(handle.decide(&mut probe).unwrap())];
        for time in ["2026-10-22T12:00:00Z", "2026-10-22T13:00:00Z"] {
            advance(&features, &clock, time);
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

    /// A side model trades by its payoff: one whose manifest has none is
    /// refused as it loads.
    #[test]
    fn a_side_model_without_a_payoff_is_refused_and_the_current_kept() {
        let mut refused = baseline("2026-10-22T12:00:00Z", 9);
        refused.manifest.payoff = None;

        kept_over(refused);
    }

    /// Hour bars built from minute bars, a composite bar type.
    const COMPOSITE: &str = "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

    /// When the cadence of the handle in `mode` of a strategy reading
    /// `bar_type`, started at 11:30, first decides.
    fn first_decision(mode: Mode, bar_type: &str) -> Timestamp {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(bar_type)],
            ..requirements()
        };
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features(),
            &config(),
            &requirements,
            mode,
        )
        .unwrap();
        handle.start(&mut probe).unwrap();
        let next = clock.borrow().next_time_ns(decide::DECISION_TIMER).unwrap();
        Timestamp::from(next)
    }

    #[test]
    fn a_backtests_cadence_waits_for_composite_bars_as_a_live_ones_does() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let models = models.path().to_path_buf();
        let backtest = Mode::Backtest {
            models: models.clone(),
            venue: venue(),
        };
        let live = live(
            models,
            catalog.path(),
            Launcher::in_process(|_| unreachable!()),
        );

        assert_eq!(
            first_decision(backtest, COMPOSITE),
            at("2026-10-22T12:00:05.010Z")
        );
        assert_eq!(
            first_decision(live, COMPOSITE),
            at("2026-10-22T12:00:05.010Z")
        );
    }

    #[test]
    fn a_dataset_runs_cadence_waits_for_composite_bars() {
        let dataset = Mode::Dataset {
            rows: SettledRows::default(),
        };

        assert_eq!(
            first_decision(dataset, COMPOSITE),
            at("2026-10-22T12:00:05.010Z")
        );
    }

    /// The minute bars the composite bars are built from, read as they
    /// are, are not composite.
    #[test]
    fn no_cadence_waits_for_bars_not_composite() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let models = models.path().to_path_buf();
        let modes = [
            (
                "backtest",
                Mode::Backtest {
                    models: models.clone(),
                    venue: venue(),
                },
            ),
            (
                "dataset",
                Mode::Dataset {
                    rows: SettledRows::default(),
                },
            ),
            (
                "live",
                live(
                    models,
                    catalog.path(),
                    Launcher::in_process(|_| unreachable!()),
                ),
            ),
        ];

        for (name, mode) in modes {
            assert_eq!(
                first_decision(mode, "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL"),
                at("2026-10-22T12:00:00.010Z"),
                "{name}"
            );
        }
    }

    /// `config`, never retrained.
    fn never_retrained() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..config()
        }
    }

    /// The probe's handle trading live, never retrained, on the models
    /// under `models` and the catalog at `catalog`, through `launcher`,
    /// for a strategy of `requirements`, started.
    fn never_retrained_live(
        probe: &mut Decider,
        models: &Path,
        catalog: &Path,
        launcher: Launcher,
        requirements: &DataRequirements,
    ) -> Rc<ModelHandle> {
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features(),
            &never_retrained(),
            requirements,
            live(models.to_path_buf(), catalog, launcher),
        )
        .unwrap();
        handle.start(probe).unwrap();
        handle
    }

    #[test]
    fn a_live_handle_never_retrained_starts_no_retrain_timer() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));

        never_retrained_live(
            &mut probe,
            models.path(),
            catalog.path(),
            Launcher::in_process(|_| unreachable!()),
            &requirements(),
        );

        let timer = crate::model::schedule::timer(StrategyId::from(STRATEGY));
        assert_eq!(clock.borrow().next_time_ns(&timer), None);
    }

    /// Run past three midnights, a daily strategy's retrain times, on a
    /// catalog holding quotes past them all, a live handle never retrained
    /// launches no train.
    #[test]
    fn a_live_handle_never_retrained_launches_no_train() {
        let models = tempfile::tempdir().unwrap();
        let catalog = catalog_ending("2026-10-26T00:00:00Z");
        let (launched, launches) = std::sync::mpsc::channel();
        let launcher = Launcher::in_process(move |at| {
            launched.send(at).unwrap();
            crate::model::train::Outcome::Published {
                valid_from: at,
                holdout: HOLDOUT,
            }
            .into()
        });
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));
        let handle = never_retrained_live(
            &mut probe,
            models.path(),
            catalog.path(),
            launcher,
            &requirements(),
        );

        run_to(&handle, &mut probe, &clock, "2026-10-25T12:00:00Z");
        drop(handle);

        assert_eq!(launches.recv(), Err(std::sync::mpsc::RecvError));
    }

    /// The bar-build delay is the bar types', not retraining's: a live
    /// handle never retrained decides on composite bars as late as a
    /// retrained one.
    #[test]
    fn a_live_handle_never_retrained_waits_for_composite_bars() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(COMPOSITE)],
            ..requirements()
        };
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));

        never_retrained_live(
            &mut probe,
            models.path(),
            catalog.path(),
            Launcher::in_process(|_| unreachable!()),
            &requirements,
        );

        let next = clock.borrow().next_time_ns(decide::DECISION_TIMER).unwrap();
        assert_eq!(Timestamp::from(next), at("2026-10-22T12:00:05.010Z"));
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
    /// deciding on `features`, warm: a quote came at the time of `probe`.
    fn live_handle(
        probe: &mut Decider,
        models: &Path,
        catalog: &Path,
        launcher: Launcher,
        features: Rc<FeatureSet>,
    ) -> Rc<ModelHandle> {
        warm(&features, probe.clock().timestamp_ns());
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements(),
            live(models.to_path_buf(), catalog, launcher),
        )
        .unwrap();
        handle.start(probe).unwrap();
        handle
    }

    /// Advances `clock` to `to`, handing `handle` the events of the timers
    /// on the way, of `probe`.
    fn run_to(
        handle: &ModelHandle,
        probe: &mut Decider,
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
            features(),
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
            features(),
        );

        let next = clock
            .borrow()
            .next_time_ns(&crate::model::schedule::timer(StrategyId::from(STRATEGY)))
            .unwrap();

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
        let features = features();
        let handle = live_handle(
            &mut probe,
            models.path(),
            catalog.path(),
            launcher,
            Rc::clone(&features),
        );
        let before = probability(handle.decide(&mut probe).unwrap());

        run_to(&handle, &mut probe, &clock, "2026-10-23T00:00:00Z");
        let trained_at = publishing.recv().unwrap();
        advance(&features, &clock, "2026-10-23T01:00:00Z");

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
        let features = features();
        let handle = live_handle(
            &mut probe,
            models.path(),
            catalog.path(),
            launcher,
            Rc::clone(&features),
        );

        run_to(&handle, &mut probe, &clock, "2026-10-23T00:00:00Z");
        keeping.recv().unwrap();
        advance(&features, &clock, "2026-10-23T01:00:00Z");

        assert_eq!(
            probability(handle.decide(&mut probe).unwrap()),
            Some(c(0.7))
        );
    }

    /// A baseline valid from `valid_from` on rows of `feature_names`, of a
    /// strategy deciding with `features` as `config` says (its decisions
    /// and targets), predicting `trues` tenths whatever the row, calibrated
    /// by `CALIBRATION`, expecting a win and a loss of 1, its baseline the
    /// mean of its training labels.
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
                baseline: trues as f64 / 10.0,
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
        let meta = meta_trues.map(|trues| {
            baseline_of(
                &features(),
                (config.decision, &config.targets),
                &META_NAMES,
                "2026-10-22T00:00:00Z",
                trues,
            )
        });
        primary_meta_handle(models, config, (Side::Long, 6), meta.as_ref())
    }

    /// The handle of `meta_handle`, its primary a model of `side`
    /// predicting `trues` tenths from midnight, its meta model `meta`, if
    /// any.
    fn primary_meta_handle(
        models: &Path,
        config: &ModelConfig,
        (side, trues): (Side, usize),
        meta: Option<&StoredModel>,
    ) -> Rc<ModelHandle> {
        let features = features();
        let set = model_set(models, &features, config);
        set.side(side)
            .unwrap()
            .store()
            .publish(&baseline_of(
                &features,
                (config.decision, &config.targets),
                &["mid"],
                "2026-10-22T00:00:00Z",
                trues,
            ))
            .unwrap();
        if let Some(meta) = meta {
            set.meta().unwrap().store().publish(meta).unwrap();
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
            Some(Decision {
                instrument_id: InstrumentId::from(AUD),
                side: OrderSide::Buy,
                quantity: Quantity::from(1000),
                probability: c(0.7),
                // The primary's, its long of 0.6.
                expected_pnl: 2.0 * c(0.6) - 1.0,
                reference: Price::from("0.70000"),
                take_profit: None,
                stop: None,
                time_limit: SignedDuration::from_hours(1),
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

    /// The meta model taking a trade from 0.6, learned by LightGBM at its
    /// defaults.
    fn lightgbm_meta() -> MetaConfig {
        MetaConfig {
            learner: LearnerConfig::LightGbm(LightGbmConfig::builder().build()),
            ..meta(None)
        }
    }

    /// A meta model valid from midnight of the 22nd, of a strategy
    /// deciding with `features` as `decision` says (its decisions and
    /// targets), fitted by `learner` on `rows` of `META_NAMES`, row after
    /// row, labelled `labels`: it predicts what `learner` learned,
    /// calibrated by `CALIBRATION`.
    fn learned_of(
        features: &FeatureSet,
        decision: (DecisionRule, &TargetsConfig),
        learner: &LearnerConfig,
        (rows, labels): (Vec<f64>, Vec<bool>),
    ) -> StoredModel {
        let fitted = learner
            .fit(&TrainingSet::new(META_NAMES.len(), rows, labels).unwrap())
            .unwrap();
        let mut meta = baseline_of(features, decision, &META_NAMES, "2026-10-22T00:00:00Z", 5);
        meta.manifest.learner = learner.clone();
        meta.model = fitted.save().unwrap();
        meta
    }

    /// A meta model of `config` valid from midnight of the 22nd, which
    /// learned on rows of its features in their order, the mid, 0.7, then
    /// the primary's side, 1 a buy and -1 a sell, then its probability,
    /// that the primary's buys win from a probability of 0.7 and its sells
    /// never: fitted by the learner of `config`'s meta on a buy and a sell
    /// at each probability from 0.40 to 0.99 by 0.01, at least 30 rows on
    /// each side of a split where LightGBM's leaves need 20, so that it
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
        learned_of(
            &features(),
            (config.decision, &config.targets),
            &config.meta.as_ref().unwrap().learner,
            (rows, labels),
        )
    }

    /// The decision at noon of the 22nd on a primary model of `side`
    /// predicting `trues` tenths, its trade taken from 0.6 by the meta
    /// model LightGBM learned (`learned_meta`).
    fn learned_meta_decision(side: Side, trues: usize) -> Option<Decision> {
        let models = tempfile::tempdir().unwrap();
        let config = with_meta(lightgbm_meta());
        let handle = primary_meta_handle(
            models.path(),
            &config,
            (side, trues),
            Some(&learned_meta(&config)),
        );
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        handle.decide(&mut probe).unwrap()
    }

    /// The meta model reads the primary's probability from its place in
    /// the row, after its side: it takes a buy at c(0.8) = 0.768525, which
    /// it learned wins, and refuses one at c(0.6) = 0.598688, which it
    /// learned loses.
    #[test]
    fn a_meta_model_decides_on_the_primarys_probability() {
        let above = learned_meta_decision(Side::Long, 8);
        let below = learned_meta_decision(Side::Long, 6);

        assert_eq!(above.map(|trade| trade.side), Some(OrderSide::Buy));
        assert_eq!(below, None);
    }

    /// The meta model is given the primary's side after its features: at
    /// c(0.8) it takes a buy, which it learned wins, and refuses a sell,
    /// which it learned never does.
    #[test]
    fn a_meta_model_decides_on_the_primarys_side() {
        let buy = learned_meta_decision(Side::Long, 8);
        let sell = learned_meta_decision(Side::Short, 8);

        assert_eq!(buy.map(|trade| trade.side), Some(OrderSide::Buy));
        assert_eq!(sell, None);
    }

    /// The data commands `probe`'s actor sends to the data engine from
    /// now, in order, each as what it asks for: requests with their
    /// window, subscriptions with the time their stream starts.
    fn sent_commands() -> Rc<RefCell<Vec<String>>> {
        let sent = Rc::new(RefCell::new(Vec::new()));
        let record = Rc::clone(&sent);
        msgbus::register_data_command_endpoint(
            MessagingSwitchboard::data_engine_queue_execute(),
            TypedIntoHandler::from(move |command: DataCommand| {
                let start_ns = |params: Option<&nautilus_core::Params>| {
                    params
                        .and_then(|params| params.get_u64("start_ns"))
                        .map(|ns| Timestamp::from(UnixNanos::from(ns)))
                };
                let ids = |ids: &[InstrumentId]| {
                    ids.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                };
                let line = match command {
                    DataCommand::Request(RequestCommand::Trades(r)) => format!(
                        "request trades [{}] {:?}..{:?}",
                        ids(&r.instrument_ids),
                        r.start,
                        r.end
                    ),
                    DataCommand::Request(RequestCommand::Instrument(r)) => {
                        format!("request instrument [{}]", ids(&r.instrument_ids))
                    }
                    DataCommand::Request(RequestCommand::Quotes(r)) => format!(
                        "request quotes [{}] {:?}..{:?}",
                        ids(&r.instrument_ids),
                        r.start,
                        r.end
                    ),
                    DataCommand::Request(RequestCommand::Bars(r)) => format!(
                        "request bars [{}] {:?}..{:?}",
                        r.bar_types
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                        r.start,
                        r.end
                    ),
                    DataCommand::Subscribe(SubscribeCommand::Trades(s)) => format!(
                        "subscribe trades [{}] from {:?}",
                        ids(&s.instrument_ids),
                        start_ns(s.params.as_ref())
                    ),
                    DataCommand::Subscribe(SubscribeCommand::Quotes(s)) => format!(
                        "subscribe quotes [{}] from {:?}",
                        ids(&s.instrument_ids),
                        start_ns(s.params.as_ref())
                    ),
                    DataCommand::Request(RequestCommand::Data(r)) => format!(
                        "request data {} of {:?} from {} {:?}..{:?}",
                        r.data_type.type_name(),
                        r.data_type.identifier(),
                        r.client_id,
                        r.start,
                        r.end
                    ),
                    other => format!("{other:?}"),
                };
                record.borrow_mut().push(line);
            }),
        );
        sent
    }

    /// Asserts `sent` holds each of `commands`, in whatever order.
    fn assert_sent(sent: &[String], commands: &[&str]) {
        for command in commands {
            assert!(
                sent.iter().any(|line| line == command),
                "{command} not in {sent:#?}"
            );
        }
    }

    /// A trade of GC's December contract at `time`, at `price`.
    fn gcz6_trade(time: &str, price: &str) -> TradeTick {
        let ts = UnixNanos::from(at(time));
        TradeTick::new(
            InstrumentId::from("GCZ6.GLBX"),
            Price::from(price),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::from(time),
            ts,
            ts,
        )
    }

    /// Nautilus' exponential average of two trades, warm from the second,
    /// `[indicators.gc_2_trades]`.
    const GC_2_TRADES: Declared<EmaArgs> = Declared::new("gc_2_trades");
    /// The last close of a bar, Nautilus' exponential average of one,
    /// `[indicators.hourly_close]`.
    const HOURLY_CLOSE: Declared<EmaArgs> = Declared::new("hourly_close");

    /// The tables of `GC_2_TRADES` and `HOURLY_CLOSE`, as `[indicators]`
    /// holds them.
    fn tables() -> IndicatorsConfig {
        toml::from_str(
            r#"
            gc_2_trades = { period = 2 }
            hourly_close = { period = 1 }
            "#,
        )
        .unwrap()
    }

    /// Live on 2026-08-10 at 14:30, a feature on the trades of the root
    /// GC, an average of two, reads GCZ6, the contract standing for
    /// it (GC rolls into GCZ6 on 2026-07-29, `rolls/README.md`): the start
    /// requests GCZ6's trades over the hour of warmup, ending now, then
    /// subscribes to them from now, so the live stream starts where the
    /// warmup ends, and loads GCG7, the post contract of the next roll;
    /// the traded AUD/USD's quotes, which no feature reads, are subscribed
    /// with no warmup. The response's two trades, which Nautilus hands to
    /// the indicators registered for GCZ6's trades, make the strategy's
    /// indicators initialized: the first decision after the warmup is warm.
    #[test]
    fn a_live_start_warms_a_roots_tick_features_on_its_front_contract() {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let features = FeatureSet::builder()
            .trades(InstrumentId::from("GC.GLBX"), GC_2_TRADES, &tables())
            .build()
            .unwrap();
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                TickSeries {
                    data: NautilusDataType::TradeTick,
                    instrument_id: InstrumentId::from("GC.GLBX"),
                },
                aud_quotes(),
            ],
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements,
            live(
                models.path().to_path_buf(),
                catalog.path(),
                Launcher::in_process(|_| unreachable!()),
            ),
        )
        .unwrap();
        let sent = sent_commands();

        handle.start(&mut probe).unwrap();
        let cold = probe.core().indicators_initialized().unwrap();
        probe.handle_trades_response(&TradesResponse::new(
            UUID4::new(),
            ClientId::from("GLBX"),
            InstrumentId::from("GCZ6.GLBX"),
            vec![
                gcz6_trade("2026-08-10T14:00:00Z", "4000.0"),
                gcz6_trade("2026-08-10T14:29:00Z", "4002.0"),
            ],
            None,
            None,
            UnixNanos::from(at("2026-08-10T14:30:00Z")),
            None,
        ));

        let sent = sent.take();
        assert_sent(
            &sent,
            &[
                "request trades [GCZ6.GLBX] Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "subscribe trades [GCZ6.GLBX] from Some(2026-08-10T14:30:00Z)",
                "request instrument [GCG7.GLBX]",
                "subscribe quotes [AUD/USD.SIM] from Some(2026-08-10T14:30:00Z)",
            ],
        );
        assert!(
            !sent
                .iter()
                .any(|line| line.contains("[GC.GLBX]") || line.starts_with("request quotes")),
            "{sent:#?}"
        );
        assert!(!cold);
        assert!(probe.core().indicators_initialized().unwrap());
    }

    /// The model strategy of `features`, on the hourly decisions of
    /// `config`, reading `bar_types` and `ticks`, started live on
    /// 2026-08-10 at 14:30 with an hour of warmup: the data commands its
    /// start sent.
    fn started_live(
        features: Rc<FeatureSet>,
        bar_types: Vec<BarType>,
        ticks: Vec<TickSeries>,
    ) -> Vec<String> {
        let models = tempfile::tempdir().unwrap();
        let catalog = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let requirements = DataRequirements {
            bar_types,
            ticks,
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements,
            live(
                models.path().to_path_buf(),
                catalog.path(),
                Launcher::in_process(|_| unreachable!()),
            ),
        )
        .unwrap();
        let sent = sent_commands();
        handle.start(&mut probe).unwrap();
        sent.take()
    }

    /// Custom data of the traded AUD/USD, which no feature reads as a tick
    /// series (`FeatureSet::ticks`), has no warmup: the start requests
    /// none of it.
    #[test]
    fn a_live_start_requests_no_custom_data_of_the_traded_instrument() {
        let imbalance = TickSeries {
            data: NautilusDataType::Custom {
                type_name: "Imbalance".to_string(),
            },
            instrument_id: InstrumentId::from(AUD),
        };

        let sent = started_live(features(), vec![], vec![aud_quotes(), imbalance]);

        assert!(
            !sent
                .iter()
                .any(|line| line.starts_with("request data") && line.contains("Imbalance")),
            "{sent:#?}"
        );
    }

    /// Features on bars built from external minute bars alone read no
    /// tick: the start requests the minute bars and no quotes or trades,
    /// the traded AUD/USD's quotes included.
    #[test]
    fn a_live_start_of_bar_features_requests_no_tick_history() {
        let hours = BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL@1-MINUTE-EXTERNAL");
        let features = FeatureSet::builder()
            .bars(hours, HOURLY_CLOSE, &tables())
            .build()
            .unwrap();

        let sent = started_live(features, vec![hours], vec![aud_quotes()]);

        assert_sent(
            &sent,
            &["request bars [AUD/USD.SIM-1-MINUTE-MID-EXTERNAL] \
               Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)"],
        );
        assert!(
            !sent.iter().any(
                |line| line.starts_with("request quotes") || line.starts_with("request trades")
            ),
            "{sent:#?}"
        );
    }

    /// Custom data of the tests: an imbalance of a book.
    #[nautilus_model::custom_data]
    pub struct Imbalance {
        pub value: f64,
        pub ts_event: UnixNanos,
        pub ts_init: UnixNanos,
    }

    /// The imbalances of `instrument_id`, custom data keyed by its id.
    fn imbalances_of(instrument_id: &str) -> TickSeries {
        TickSeries {
            data: NautilusDataType::Custom {
                type_name: "Imbalance".to_string(),
            },
            instrument_id: InstrumentId::from(instrument_id),
        }
    }

    /// The data type of the imbalances of `instrument_id`, keyed by its id.
    fn imbalance_type(instrument_id: &str) -> nautilus_model::data::DataType {
        nautilus_model::data::DataType::new("Imbalance", None, Some(instrument_id.to_string()))
    }

    /// An imbalance of `value` of `instrument_id` at `time`, keyed by its
    /// id.
    fn imbalance(instrument_id: &str, time: &str, value: f64) -> CustomData {
        let ts = UnixNanos::from(at(time));
        CustomData::new(
            std::sync::Arc::new(Imbalance::new(value, ts, ts)),
            imbalance_type(instrument_id),
        )
    }

    /// The handlers subscribed to the imbalances of `instrument_id` on the
    /// message bus: a subscription to custom data naming no data client
    /// is one to its topic alone, sending no command.
    fn imbalance_subscribers(instrument_id: &str) -> usize {
        msgbus::subscriptions_count_any(get_custom_topic(&imbalance_type(instrument_id))).unwrap()
    }

    /// A raw feature, the last imbalance of `instrument_id`.
    fn last_imbalance(instrument_id: &str) -> Rc<FeatureSet> {
        FeatureSet::builder()
            .data(
                "Imbalance",
                InstrumentId::from(instrument_id),
                "imbalance",
                |data| {
                    data.data
                        .as_any()
                        .downcast_ref::<Imbalance>()
                        .map_or(f64::NAN, |imbalance| imbalance.value)
                },
            )
            .build()
            .unwrap()
    }

    /// Live on 2026-08-10 at 14:30, with an hour of warmup, the model
    /// strategy of `features` reading `ticks`, its warmup handed its
    /// historical custom data, started: the probe, the handle and the
    /// data commands the start sent.
    fn started_live_reading(
        features: Rc<FeatureSet>,
        ticks: Vec<TickSeries>,
    ) -> (Decider, Rc<ModelHandle>, Vec<String>, tempfile::TempDir) {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks,
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            &config(),
            &requirements,
            live(
                models.path().to_path_buf(),
                models.path(),
                Launcher::in_process(|_| unreachable!()),
            ),
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        let sent = sent_commands();
        handle.start(&mut probe).unwrap();
        let sent = sent.take();
        (probe, handle, sent, models)
    }

    /// Nautilus' answer to a request of custom data of `instrument_id`
    /// from the catalog: `data`.
    fn catalog_answer(instrument_id: &str, data: Vec<CustomData>) -> CustomDataResponse {
        CustomDataResponse::new(
            UUID4::new(),
            ClientId::from("CATALOG"),
            None,
            imbalance_type(instrument_id),
            data,
            None,
            None,
            UnixNanos::from(at("2026-08-10T14:30:00Z")),
            None,
        )
    }

    /// Live on 2026-08-10 at 14:30, a feature on AUD/USD's imbalances,
    /// custom data no data client serves: the start requests the hour of
    /// warmup of them, ending now, naming the client `CATALOG`, which
    /// Nautilus answers from the catalog alone, and subscribes to them.
    /// Nautilus hands the answer's imbalances to the strategy's
    /// `on_historical_data`, which hands them to the warmup: the feature
    /// holds the last one, warm before the first decision.
    #[test]
    fn a_live_start_warms_a_custom_feature_from_the_catalog() {
        let features = last_imbalance(AUD);
        let (mut probe, _handle, sent, _models) =
            started_live_reading(Rc::clone(&features), vec![imbalances_of(AUD)]);
        let cold = features.values();

        probe.handle_data_response(&catalog_answer(
            AUD,
            vec![
                imbalance(AUD, "2026-08-10T14:00:00Z", 0.25),
                imbalance(AUD, "2026-08-10T14:29:00Z", -0.5),
            ],
        ));

        assert_eq!(
            sent,
            [
                "request data Imbalance of Some(\"AUD/USD.SIM\") from CATALOG \
              Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)"
            ]
        );
        assert_eq!(imbalance_subscribers(AUD), 1);
        assert_eq!(cold, None);
        assert_eq!(features.values(), Some(vec![-0.5]));
    }

    /// Live on 2026-08-10 at 14:30, a feature on the imbalances of the
    /// root GC reads GCZ6's, the contract standing for it (GC rolls into
    /// GCZ6 on 2026-07-29, `rolls/README.md`): the start requests GCZ6's
    /// over the hour of warmup from the catalog, subscribes to them, not
    /// to the root's, and loads GCG7, the post contract of the next roll.
    /// The answer's imbalance of GCZ6 makes the feature hold it.
    #[test]
    fn a_live_start_warms_a_custom_feature_on_a_root_on_its_front_contract() {
        let features = last_imbalance("GC.GLBX");
        let (mut probe, _handle, sent, _models) = started_live_reading(
            Rc::clone(&features),
            vec![imbalances_of("GC.GLBX"), aud_quotes()],
        );

        probe.handle_data_response(&catalog_answer(
            "GCZ6.GLBX",
            vec![imbalance("GCZ6.GLBX", "2026-08-10T14:10:00Z", 0.75)],
        ));

        assert_sent(
            &sent,
            &[
                "request data Imbalance of Some(\"GCZ6.GLBX\") from CATALOG \
                 Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "request instrument [GCG7.GLBX]",
                "subscribe quotes [AUD/USD.SIM] from Some(2026-08-10T14:30:00Z)",
            ],
        );
        assert!(
            !sent.iter().any(|line| line.contains("\"GC.GLBX\"")),
            "{sent:#?}"
        );
        assert_eq!(
            (
                imbalance_subscribers("GCZ6.GLBX"),
                imbalance_subscribers("GC.GLBX")
            ),
            (1, 0)
        );
        assert_eq!(features.values(), Some(vec![0.75]));
    }

    /// A feature on the imbalances of the root GC, in a backtest from
    /// 2026-11-25 at 23:00, reads GCZ6's, the contract standing for it,
    /// not GCG7's. GC rolls into GCG7 at 00:00 UTC on 2026-11-26: the
    /// roll's alert, handed to the handle, moves the subscription to
    /// GCG7's and resets the feature, cold through GCZ6's imbalances after
    /// it, and the feature reads GCG7's from then.
    #[test]
    fn a_custom_feature_on_a_root_reads_each_contract_in_turn_reset_at_the_roll() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at("2026-11-25T23:00:00Z"));
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![imbalances_of("GC.GLBX"), aud_quotes()],
            warmup: SignedDuration::ZERO,
        };
        let features = last_imbalance("GC.GLBX");
        let handle = ModelHandle::new(
            StrategyId::from(STRATEGY),
            Rc::clone(&features),
            &config(),
            &requirements,
            Mode::Backtest {
                models: models.path().to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap();
        handle.start(&mut probe).unwrap();
        let read = |data: CustomData| {
            features.handle_data(&data);
            features.values()
        };

        let pre = read(imbalance("GCZ6.GLBX", "2026-11-25T23:10:00Z", 0.25));
        let post_before_the_roll = read(imbalance("GCG7.GLBX", "2026-11-25T23:20:00Z", 0.75));
        run_to(&handle, &mut probe, &clock, "2026-11-26T00:30:00Z");
        let rolled = features.values();
        let pre_after_the_roll = read(imbalance("GCZ6.GLBX", "2026-11-26T00:40:00Z", -0.25));
        let post = read(imbalance("GCG7.GLBX", "2026-11-26T00:50:00Z", -0.75));

        assert_eq!(pre, Some(vec![0.25]));
        assert_eq!(post_before_the_roll, Some(vec![0.25]));
        assert_eq!(rolled, None);
        assert_eq!(pre_after_the_roll, None);
        assert_eq!(post, Some(vec![-0.75]));
        assert_eq!(
            (
                imbalance_subscribers("GCZ6.GLBX"),
                imbalance_subscribers("GCG7.GLBX")
            ),
            (0, 1)
        );
    }
}
