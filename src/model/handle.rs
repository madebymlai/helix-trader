//! The model handle: what a strategy trading on a learned model holds, the
//! one place it decides, in a backtest, live and a dataset run alike.
//!
//! The strategy shares a `ModelHandle` (an `Rc`), built from its model
//! blueprint (`blueprint::ModelBlueprint`), in a `Mode`. `start` from
//! `on_start` registers the features (and starts the target's ATR, if it
//! has one) and starts the decision cadence; `decide`, at a decision
//! of the cadence (its timer, `decide::is_decision`), snapshots the
//! features, if no live warmup is still answering (`Warmup::is_warming`),
//! the target's ATR, if any, is initialized, as the handle reads it itself,
//! not Nautilus' registered indicators, the decision is fresh (data of the
//! target's instrument since the previous) and every feature is
//! initialized, and publishes their row (`decide::decide`), then:
//!
//! - deciding (`Mode::Backtest`, `Mode::Live`): takes the latest pair of
//!   the sides of the config's margins valid at the decision, the sides
//!   one train published (`store::Pair`), each side's model predicting its
//!   R on the side's row, the decision's features then the side's payoff
//!   features as the target sets them, which a dataset row of the decision
//!   holds alike (`deciding::SideRow`), the pair calibrating both
//!   predictions together into the decision's level and side call and each
//!   side's expected R (`calibration::Ksplit`), on the target the decision
//!   sets (`targets::TargetSetter`): with exits from the decision's price
//!   and the target's ATR, which the handle runs, and on a time-only target
//!   that ATR alone. It returns the `Decision` of the side whose expected R
//!   clears its margin, in R, the larger of two (`payoff::pick`), as plain
//!   data: the side, the expected R and the level and side call behind it,
//!   and the trade the decision sets on that side, without a quantity: its
//!   reference price, take profit, stop, time limit and one R. The handle
//!   sends no order, reads no equity and handles no trade's time: the
//!   strategy sends the decision's trade at a stake of its own, the config's
//!   or a multiple (`Decision::trade`, sent through `strategy::orders` as a
//!   dataset run sends its targets), drops it or trades otherwise, and
//!   closes its trades through the same module. The models see the target
//!   at the reference stake alone, the config's risk of the dataset balance
//!   (`targets::TargetSetter`), whatever the strategy stakes. With no pair
//!   valid yet no side trades; a stale decision, cold features, a target's
//!   ATR not yet initialized, a target of no size at the reference stake,
//!   no side clearing its margin or two tied are no decision, never an
//!   error. The target is set before the models are asked, so with the
//!   instrument missing from the cache `decide` errs, whatever the models.
//! - in a dataset run (`Mode::Dataset`): opens the decision's target
//!   (`targets::Targets`, through `strategy::orders`) and returns no
//!   decision, so the strategy places no trade of its own; the strategy
//!   hands the order module its time events, which close the target's
//!   trades, and the handle its order and position events, and the rows
//!   the targets settle are collected for the caller of the run.
//!
//! With a meta model (`model::meta`), the trade of the side picked is
//! taken only if the meta model in use, on the row with that side and the
//! decision's level and side call, expects of it a net R, its calibrated
//! expected R, of at least its margin (`MetaConfig::margin`), the trade as
//! the target sets it; no meta model valid yet is no trade.
//!
//! The decision itself is `deciding::DecidingModels`'s; the handle is what
//! talks to the strategy. A model is swapped in only inside `decide`, before
//! it predicts, the sides' as a pair: no decision mixes two models, nor the
//! sides of two trains. A backtest and a live node read
//! the same store, which must be readable when the handle is built, and is
//! listed again at each decision, a failure logged and the previous lists
//! kept, so a model published meanwhile is used from the first decision at or
//! after its valid-from. Every prediction is calibrated, the sides' by their
//! pair (`store::Pair`), the meta's on its own (`store::LoadedModel`). Before
//! use a model is checked against the strategy:
//! its feature names and their
//! order, its fingerprint (`dataset::fingerprint`), its width, and its
//! probes, each of which the reloaded model must predict to the bit as it
//! did when fitted. A model failing a check is refused, logged as an
//! error, a side's with its pair, and the current one kept; it is not read
//! again (`store::Swap`).
//!
//! The handle owns a `rolls::Roller` of the features' tick series
//! (`FeatureSet::ticks`, custom data among them) and of the tick series of
//! the traded instrument its data requirements hold, which the harness
//! reads for the decisions' freshness, its quotes among them when no
//! series of the strategy reads it (`StandardConfig::data_requirements`,
//! the decision's reference price, the venue's fills and the target's
//! ATR then coming from them): `start` subscribes each
//! on the contract standing for it, a plain instrument once, a root on its
//! front contract with its roll alert, and the strategy hands the handle
//! its time events (after `strategy::orders::on_time_event`), for the
//! roller. At a roll the root's features move to the post contract and
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
//! (`FeatureSet::ticks`) and feeding the features, and `start` starts it,
//! the features registered first. The strategy's data hooks hand it the
//! responses and the live quotes and trades: `model_data_hooks!` writes
//! them, the one place they are written, so that no strategy misses one.
//! The features are registered with Nautilus for their bars alone: the
//! warmup hands them the quotes and trades of its responses and the live
//! ones, held until it ends, so that no live tick reaches them before an
//! older window of the history (`strategy::warmup`'s `hold`); in a replay
//! the ticks go straight through. Live, the warmup requests the history of
//! the features' ticks, a root's on the contract standing for it, their
//! custom data from the catalog alone, handed to the features, and the
//! roller's subscriptions continue from its end, so the first decision
//! after a start is warm. A series of the traded instrument no feature
//! reads, custom data among them, has no warmup, and its live ticks reach
//! no feature. It warms the target's ATR from
//! the warmup request, as it does the features: the request builds the
//! ATR's bars too (`TargetSetter::warmed`), and the warmup hands the bars of the
//! response to both, so the first decision after a start sets the stop and
//! the unit a dataset run sets there. A live node sizes its cache from the same
//! warmup, the ATR's bars included.
//!
//! Features over the bracket's width (`features::OverBracket`) read the
//! width the handle builds the setter on (`FeatureSet::bracket_width`,
//! `TargetSetter::sharing`): its ATR is the target's, started and warmed
//! with it, and `decide` reads the features and sets the target in the one
//! decision event, with no data between, so a row reads the width its
//! target is set at.
//!
//! The handle does not retrain: live, the retrains of a strategy are a
//! Nautilus actor of their own beside it (`launcher::RetrainActor`), and
//! the model a retrain publishes, or `train --at`, is the handle's from its
//! next decision, which lists the store again. Never retrained, a strategy
//! has no retrain actor: the handle trades on the store as it is.

use std::{cell::RefCell, fmt::Debug, path::PathBuf, rc::Rc};

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
};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{
    blueprint::ModelBlueprint,
    dataset::DatasetVenue,
    decide::{self, DecisionRule},
    deciding::{DecidingModels, SideRow},
    features::FeatureSet,
    targets::{TargetRow, TargetSetter, Targets},
};
use crate::{
    rolls::Roller,
    strategy::{
        orders::{self, Trade},
        risk::R,
        warmup::{self, BookIndicator, CustomDataIndicator, Warmup},
    },
};

/// Where a handle runs, and so what it does at a decision.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Trades in a backtest on the models of the store under `models`
    /// whose target is that of its dataset runs, on `venue`, the target at
    /// its commission.
    Backtest {
        models: PathBuf,
        venue: DatasetVenue,
    },
    /// Trades live on the models of the store under `models` whose target
    /// is that of the dataset runs of its trains, on `venue`, the target at
    /// its commission, deciding
    /// the live node's bar build delay late, as a backtest does; apart from
    /// `Backtest` for its warmup, which requests the history up to now
    /// (`Warmup::live`), and its roller, which loads each post contract
    /// before its roll (`Roller::live`). Its retrains, if any, are the
    /// node's retrain actor's (`launcher::RetrainActor`).
    Live {
        models: PathBuf,
        venue: DatasetVenue,
    },
    /// Opens each decision's target, for a dataset run on `venue`, at its
    /// commission and at the reference stake of its balance, and collects
    /// the rows it settles in `rows`.
    Dataset {
        rows: SettledRows,
        venue: DatasetVenue,
    },
}

/// What a dataset run settles, shared with its caller: the rows its
/// targets settle, unless an order of theirs was denied, which fails the
/// run (`Targets::on_order_denied`).
#[derive(Debug, Clone, Default)]
pub struct SettledRows(Rc<RefCell<Settled>>);

#[derive(Debug, Default)]
struct Settled {
    rows: Vec<TargetRow>,
    /// The first denial, which failed the run.
    denial: Option<String>,
}

impl SettledRows {
    /// The rows settled so far, taken.
    ///
    /// # Errors
    ///
    /// Returns the denial that failed the run, if one did.
    pub fn take(&self) -> anyhow::Result<Vec<TargetRow>> {
        let Settled { rows, denial } = self.0.take();
        match denial {
            Some(denial) => Err(anyhow::anyhow!(denial)),
            None => Ok(rows),
        }
    }

    fn push(&self, row: TargetRow) {
        self.0.borrow_mut().rows.push(row);
    }

    /// Fails the run with `denial`, unless an earlier one failed it.
    pub(super) fn fail(&self, denial: String) {
        self.0.borrow_mut().denial.get_or_insert(denial);
    }
}

/// A decision to trade, as plain data: the side the models pick, of the
/// target's instrument, its expected net R and the calibrated level and
/// side call it is of, the meta's expected R if a meta took it, and the
/// trade its models learned as the decision sets it on that side, the one a
/// dataset run sends there, without a quantity (`trade`). The handle sends
/// nothing and reads no equity: a strategy sends the trade at a stake of its
/// choosing, the config's or a multiple of it (`orders::Trade::send`), or
/// drops it, or trades otherwise, and then the models' holdout figures
/// describe the target's trade, not its own. Its expected R, in R, is the
/// same at any stake.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    /// The target's instrument.
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    /// The net R the pair expects of the side's trade: its calibrated
    /// expected R, `E'`, which the side's margin is held to.
    pub expected_r: R,
    /// The decision's calibrated level, `C'`, in R.
    pub level: f64,
    /// The decision's calibrated side call, `D'`, in R: nought of a
    /// one-sided strategy.
    pub side_call: f64,
    /// The meta model's calibrated expected R of the trade, if a meta took
    /// it, which the meta's margin is held to.
    pub meta_expected_r: Option<R>,
    /// The trade, as the target sets it on the side (`SideTarget::trade`).
    trade: Trade,
}

impl Decision {
    /// The trade the model learned, the one a dataset run sends at the
    /// decision on that side: from the price the target is set from (the
    /// mid of the instrument's last quote, else its last trade, else its
    /// last close), a bracket with the target's exits on the instrument's
    /// ticks, or a market entry, closed at the time limit
    /// (`strategy::orders`), its one R its stop's distance or, time only,
    /// the target's ATR. It has no quantity: sent at a stake, it is sized
    /// as it is sent. Its exits and its one R cannot be changed.
    #[must_use]
    pub const fn trade(&self) -> Trade {
        self.trade
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
    /// The strategy's warmup, which feeds the features and the target's
    /// ATR the handle trades on.
    warmup: Rc<Warmup>,
    /// Subscribes the features' ticks, on the contract standing for each
    /// root, and the requirements' tick series of the traded instrument.
    roller: Roller,
}

#[derive(Debug)]
enum Serving {
    Deciding(Box<Deciding>),
    Dataset {
        targets: Rc<Targets>,
        rows: SettledRows,
    },
}

/// The deciding side of a handle, which returns decisions and sends nothing.
#[derive(Debug)]
struct Deciding {
    /// The models deciding on the target's trades.
    models: RefCell<DecidingModels>,
    /// The target the side models' trades are on, as each decision sets
    /// it.
    target: TargetSetter,
    /// The target's instrument.
    instrument_id: InstrumentId,
}

impl ModelHandle {
    /// The handle of the model `blueprint` describes, in `mode`: deciding
    /// on its models under the store's root (`ModelBlueprint::model_set`),
    /// of the target of its dataset runs on the mode's venue, or opening
    /// its target. It decides on the blueprint's features, the very set its
    /// dataset and its stores are fingerprinted from.
    ///
    /// # Errors
    ///
    /// Returns an error if, deciding, a store cannot be keyed or read.
    pub fn new(blueprint: &ModelBlueprint, mode: Mode) -> anyhow::Result<Rc<Self>> {
        let (features, requirements) = (blueprint.features(), blueprint.requirements());
        let (rule, target) = (blueprint.config().decision, &blueprint.config().target);
        let traded = target.instrument_id;
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
        .feeding_ticks(Rc::clone(features) as SharedActorIndicator)
        .feeding_books(Rc::clone(features) as Rc<dyn BookIndicator>)
        .feeding_custom_data(Rc::clone(features) as Rc<dyn CustomDataIndicator>);
        let roller = match mode {
            Mode::Live { .. } => Roller::live(ticks),
            Mode::Backtest { .. } | Mode::Dataset { .. } => Roller::replayed(ticks),
        };
        let delay = warmup::bar_build_delay(&requirements.bar_types);
        let serving = match mode {
            Mode::Backtest { models, venue } | Mode::Live { models, venue } => {
                let models = blueprint.model_set(&models, &venue)?;
                Serving::Deciding(Box::new(Deciding {
                    models: RefCell::new(DecidingModels::new(&models)?),
                    target: TargetSetter::sharing(
                        target,
                        rule,
                        requirements,
                        &venue,
                        features.bracket_width(),
                    )?,
                    instrument_id: target.instrument_id,
                }))
            }
            Mode::Dataset { rows, venue } => Serving::Dataset {
                targets: Targets::sharing(
                    target.clone(),
                    rule,
                    requirements,
                    &venue,
                    features.bracket_width(),
                )?,
                rows,
            },
        };
        // A dataset run warms on its replay.
        let warmup = match &serving {
            Serving::Deciding(deciding) => deciding.target.warmed(warmup),
            Serving::Dataset { .. } => warmup,
        };
        Ok(Rc::new(Self {
            strategy_id: blueprint.strategy_id(),
            features: Rc::clone(features),
            rule,
            delay,
            traded,
            serving,
            warmup: Rc::new(warmup),
            roller,
        }))
    }

    /// Starts the handle on `strategy`, from `on_start`: refuses the
    /// traded instrument if a stake cannot size a trade on it
    /// (`orders::refuse_inverse`), registers the features for their bars,
    /// starts the strategy's warmup
    /// (`Warmup::start`), holding the live ticks while it is pending, live
    /// requesting the history of the bars and of the features' ticks, then
    /// the roller, which subscribes the features' ticks and the traded
    /// instrument's from now, where the warmup ends, a root's on the
    /// contract standing for it; starts the target's ATR, if it has one (in
    /// a dataset run the targets', and collects the rows they settle); then
    /// starts the decision cadence, `warmup::bar_build_delay` late.
    ///
    /// # Errors
    ///
    /// Returns an error if the traded instrument is inverse, the warmup
    /// fails to start, Nautilus refuses a roll alert or the cadence's
    /// timer, or, in a dataset run, the targets refuse to start
    /// (`Targets::start`).
    pub fn start<S>(&self, strategy: &mut S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative + Debug + 'static,
    {
        orders::refuse_inverse(strategy, self.traded)?;
        // Registered first: it sets the contract each feature reads, which
        // the warmup's responses are read against.
        self.features.register(strategy);
        self.warmup.start(strategy)?;
        self.roller.start(strategy)?;
        match &self.serving {
            Serving::Deciding(deciding) => deciding.target.start(strategy),
            Serving::Dataset { targets, rows } => {
                targets.start(strategy)?;
                let rows = rows.clone();
                let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
                    if let Some(row) = data.data.as_any().downcast_ref::<TargetRow>() {
                        rows.push(row.clone());
                    }
                });
                let topic = get_custom_topic(&TargetRow::data_type(self.strategy_id));
                msgbus::subscribe_any(topic.into(), handler, None);
            }
        }
        decide::start(strategy, self.rule, self.delay)
    }

    /// The strategy's warmup, which the handle builds and starts: the
    /// strategy's data hooks hand it the responses and the live quotes and
    /// trades (`model_data_hooks!`), and a live node sizes its cache from
    /// it.
    #[must_use]
    pub fn warmup(&self) -> Rc<Warmup> {
        Rc::clone(&self.warmup)
    }

    /// Decides now, for `strategy`: the decision to trade, if any; none
    /// while the warmup is not answered yet (`Warmup::is_warming`), the
    /// target's ATR is not initialized, or `decide::decide` publishes no
    /// row (a stale decision, a feature not initialized).
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict, deciding as
    /// `TargetSetter::at` does, or, in a dataset run, as `Targets::open` does.
    pub fn decide<S>(&self, strategy: &mut S) -> anyhow::Result<Option<Decision>>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        if self.warmup.is_warming() {
            log::debug!("No decision: the warmup is not answered yet");
            return Ok(None);
        }
        if !self.target_initialized() {
            log::debug!("No decision: the target's ATR is not initialized");
            return Ok(None);
        }
        let Some(row) = decide::decide(strategy, &self.features, self.rule, self.traded) else {
            return Ok(None);
        };
        match &self.serving {
            Serving::Deciding(deciding) => {
                deciding.decide(strategy, Timestamp::from(row.ts_event), &row.features)
            }
            Serving::Dataset { targets, .. } => {
                targets.open(strategy, row)?;
                Ok(None)
            }
        }
    }

    /// Whether the target's ATR, if any, is initialized, read of it, not of
    /// Nautilus' registered indicators, for which the features are
    /// registered for their bars alone (`FeatureSet::register`). The
    /// features are not read here: `decide::decide` publishes no row while
    /// one is cold.
    fn target_initialized(&self) -> bool {
        match &self.serving {
            Serving::Deciding(deciding) => deciding.target.initialized(),
            Serving::Dataset { targets, .. } => targets.initialized(),
        }
    }

    /// Hands `event` of `strategy` to the roller: at a roll alert
    /// it switches the root's subscriptions to the post contract, and the
    /// features reading the root move there and restart (`FeatureSet::roll`),
    /// the decisions waiting until they are initialized again; any other
    /// event is not the handle's. A trade's time alert is the strategy's
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
        }
        Ok(())
    }

    /// Hands `event` to the targets of a dataset run; a no-op deciding.
    pub fn on_order_canceled<S>(&self, strategy: &S, event: &OrderCanceled)
    where
        S: DataActor + DataActorNative,
    {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_order_canceled(strategy, event);
        }
    }

    /// Hands `event` to the targets of a dataset run, which it fails
    /// (`Targets::on_order_denied`); a no-op deciding.
    pub fn on_order_denied<S>(&self, strategy: &S, event: &OrderDenied)
    where
        S: DataActor + DataActorNative,
    {
        if let Serving::Dataset { targets, rows } = &self.serving {
            targets.on_order_denied(strategy, event, rows);
        }
    }

    /// Hands `event` to the targets of a dataset run; a no-op deciding.
    pub fn on_order_rejected(&self, event: &OrderRejected) {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_order_rejected(event);
        }
    }

    /// Hands `event` to the targets of a dataset run; a no-op deciding.
    pub fn on_position_closed<S>(&self, strategy: &S, event: &PositionClosed)
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_position_closed(strategy, event);
        }
    }

    /// Reports the targets of a dataset run unsettled at the stop; a no-op
    /// deciding.
    pub fn on_stop(&self) {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_stop();
        }
    }
}

/// The data hooks of a model strategy, written once for every one: expanded
/// in its `DataActor` impl as `model_data_hooks!(model);`, `model` the field
/// holding its `Rc<ModelHandle>`, they hand its warmup (`ModelHandle::warmup`)
/// the end of each request (`on_data_response`), the quotes, trades, book
/// depth, deltas and custom data of each response (`on_historical_*`), and
/// the live quotes and trades (`on_quote`, `on_trade`), which it holds while
/// it is pending and then hands to the features (`strategy::warmup`).
#[macro_export]
macro_rules! model_data_hooks {
    ($model:ident) => {
        fn on_data_response(
            &mut self,
            response: &::nautilus_common::messages::data::DataResponseEnvelope,
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_data_response(self, response)
        }

        fn on_historical_quotes(
            &mut self,
            quotes: &[::nautilus_model::data::QuoteTick],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_quotes(quotes)
        }

        fn on_historical_trades(
            &mut self,
            trades: &[::nautilus_model::data::TradeTick],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_trades(trades)
        }

        fn on_historical_book_depth(
            &mut self,
            depths: &[::nautilus_model::data::OrderBookDepth],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_book_depth(depths);
            Ok(())
        }

        fn on_historical_book_deltas(
            &mut self,
            deltas: &[::nautilus_model::data::OrderBookDelta],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_book_deltas(deltas);
            Ok(())
        }

        fn on_historical_data(&mut self, data: &dyn ::std::any::Any) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_data(data);
            Ok(())
        }

        fn on_quote(&mut self, quote: &::nautilus_model::data::QuoteTick) -> ::anyhow::Result<()> {
            self.$model.warmup().on_quote(quote)
        }

        fn on_trade(&mut self, trade: &::nautilus_model::data::TradeTick) -> ::anyhow::Result<()> {
            self.$model.warmup().on_trade(trade)
        }
    };
}

impl Deciding {
    /// The decision of `strategy` at `now` on the decision's `features`: the
    /// stores listed again, a failure logged and the previous lists kept; the
    /// target set once for both sides, no target no decision; and the trade
    /// its sides' rows decide (`DecidingModels::decide`), each the features
    /// then its target's payoff features, as a dataset row gives them
    /// (`SideRow::new`), on the target as set on the side.
    fn decide<S>(
        &self,
        strategy: &S,
        now: Timestamp,
        features: &[f64],
    ) -> anyhow::Result<Option<Decision>>
    where
        S: DataActor + DataActorNative,
    {
        let mut models = self.models.borrow_mut();
        if let Err(e) = models.list() {
            log::error!("listing the models at {now}: {e:#}; the previous lists kept");
        }
        let Ok(targets) = self.target.at(strategy)? else {
            return Ok(None);
        };
        let sides = targets
            .as_ref()
            .map(|target| SideRow::new(features, &target.payoff));
        let Some(taken) = models.decide(now, features, &sides)? else {
            return Ok(None);
        };
        Ok(Some(Decision {
            instrument_id: self.instrument_id,
            side: taken.side.order_side(),
            expected_r: taken.expected_r,
            level: taken.expected.level,
            side_call: taken.expected.side_call,
            meta_expected_r: taken.meta_expected_r,
            trade: targets.of(taken.side).trade(self.instrument_id, taken.side),
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
            CustomDataResponse, DataCommand, QuotesResponse, RequestCommand, SubscribeCommand,
            TradesResponse,
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
        instruments::{
            Instrument, InstrumentAny,
            stubs::{audusd_sim, btcusd_bybit},
        },
        types::{Price, Quantity},
    };
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };

    use super::*;
    use crate::model::{
        config::{Margins, ModelConfig},
        decide::FeatureRow,
        deciding::testing::{
            AUD, GOLD, STRATEGY, at, baseline, baseline_for, blueprint_of, both_sides, config,
            features, gold, meta, meta_baseline, model_set, r, requirements, side_store, store,
            venue, with_meta,
        },
        features::testing::{last_mid, mid_over},
        schedule::Retrain,
        targets::{BracketSpec, BracketWidth, DEFAULT_ATR, Distance, Side, Target, TargetConfig},
    };
    use crate::strategy::{
        AtrArgs, DataRequirements, Declared, EmaArgs, IndicatorsConfig, TickSeries,
        orders::{Bracket, Exits},
        risk::R,
    };
    use rust_decimal::Decimal;

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

    /// Hands the warmup, once given, the custom data, quotes and trades of a
    /// response and the live quotes and trades, as a model strategy does
    /// (`model_data_hooks!`).
    impl DataActor for Decider {
        fn on_historical_data(&mut self, data: &dyn std::any::Any) -> anyhow::Result<()> {
            if let Some(warmup) = &self.warmup {
                warmup.on_historical_data(data);
            }
            Ok(())
        }

        fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
            self.warmup
                .as_ref()
                .map_or(Ok(()), |warmup| warmup.on_historical_quotes(quotes))
        }

        fn on_historical_trades(&mut self, trades: &[TradeTick]) -> anyhow::Result<()> {
            self.warmup
                .as_ref()
                .map_or(Ok(()), |warmup| warmup.on_historical_trades(trades))
        }

        fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
            self.warmup
                .as_ref()
                .map_or(Ok(()), |warmup| warmup.on_quote(quote))
        }

        fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
            self.warmup
                .as_ref()
                .map_or(Ok(()), |warmup| warmup.on_trade(trade))
        }
    }

    /// The core of a strategy of id `STRATEGY`.
    fn strategy_core() -> StrategyCore {
        StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        })
    }

    /// A strategy registered without a node at `now`, as the EMA's tests
    /// do, and its clock.
    fn registered(now: Timestamp) -> (Decider, Rc<RefCell<VirtualClock>>) {
        let mut probe = Decider {
            core: strategy_core(),
            warmup: None,
        };
        let clock = register(&mut probe.core, now);
        (probe, clock)
    }

    /// Registers `core` without a node at `now`, AUD/USD in its cache: its
    /// clock.
    fn register(core: &mut StrategyCore, now: Timestamp) -> Rc<RefCell<VirtualClock>> {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(UnixNanos::from(now), true);
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        let cache = Rc::new(RefCell::new(Cache::default()));
        // AUD/USD in the cache, quoted at 0.69990/0.70010: a target's
        // exits are set from its mid.
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
        core.register(TraderId::from("HELIX-001"), clock.clone(), cache, portfolio)
            .unwrap();
        clock
    }

    /// Advances `clock` to `to`, when a quote of AUD/USD comes to
    /// `features`, those a handle decides on: the market is open.
    fn advance(features: &FeatureSet, clock: &Rc<RefCell<VirtualClock>>, to: &str) {
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(to)), true);
        warm(features, UnixNanos::from(at(to)));
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

    /// The probe's handle trading in a backtest on the models under
    /// `models`, deciding on `features`, warm: a quote came at the time of
    /// `probe`.
    fn handle(probe: &Decider, models: &Path, features: Rc<FeatureSet>) -> Rc<ModelHandle> {
        warm(&features, probe.clock().timestamp_ns());
        ModelHandle::new(
            &blueprint_of(&features, &config(), requirements()),
            Mode::Backtest {
                models: models.to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap()
    }

    /// The level of `decision`: of the tests' one side, its model's R, which
    /// names the model, whatever the decision's unit.
    fn level(decision: Option<Decision>) -> Option<f64> {
        decision.map(|decision| decision.level)
    }

    #[test]
    fn cold_features_give_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let cold = ModelHandle::new(
            &blueprint_of(&features(), &config(), requirements()),
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
    /// the long model of these features trades, expecting 0.3R.
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
            &blueprint_of(&features, &config(), requirements()),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap();
        started(&handle, &mut probe);

        assert_eq!(level(handle.decide(&mut probe).unwrap()), Some(r(8)));
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
            &blueprint_of(&features, &config(), requirements),
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
        sides: (Option<usize>, Option<usize>),
    ) -> Rc<ModelHandle> {
        let mode = Mode::Backtest {
            models: models.to_path_buf(),
            venue: venue(),
        };
        sides_handle_in(mode, probe, models, config, sides)
    }

    /// `sides_handle`, in `mode`.
    fn sides_handle_in(
        mode: Mode,
        probe: &Decider,
        models: &Path,
        config: &ModelConfig,
        (long, short): (Option<usize>, Option<usize>),
    ) -> Rc<ModelHandle> {
        let features = features();
        for (side, tenths) in [(Side::Long, long), (Side::Short, short)] {
            if let Some(tenths) = tenths {
                side_store(models, &features, config, side)
                    .publish(&baseline_for(
                        &features,
                        config,
                        side,
                        "2026-10-22T00:00:00Z",
                        tenths,
                    ))
                    .unwrap();
            }
        }
        warm(&features, probe.clock().timestamp_ns());
        ModelHandle::new(&blueprint_of(&features, config, requirements()), mode).unwrap()
    }

    /// A long expecting 0.1R, clearing its margin of nought; the short is
    /// not traded. The decision carries its expected R, its level and, of
    /// one side, no side call.
    #[test]
    fn a_side_expected_to_clear_its_margin_is_a_decision_of_its_expected_r() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config(), (Some(6), None));
        started(&handle, &mut probe);

        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            (
                decision.instrument_id,
                decision.side,
                decision.expected_r,
                decision.level,
                decision.side_call,
                decision.meta_expected_r
            ),
            (
                InstrumentId::from(AUD),
                OrderSide::Buy,
                R::new(0.1),
                0.1,
                0.0,
                None
            )
        );
    }

    /// `config` learning a bracket a take profit 20 bps and a stop `stop`
    /// bps from the decision's price, trading the long from 0.3R.
    fn bracket_config(stop: i64) -> ModelConfig {
        let mut config = config();
        config.target.spec = Target {
            bracket: Some(BracketSpec {
                take_profit: Distance::BasisPoints(Decimal::from(20)),
                stop: Distance::BasisPoints(Decimal::from(stop)),
                atr: None,
            }),
            limit: SignedDuration::from_hours(2),
            atr: None,
        };
        config.margins = Margins {
            long: Some(R::new(0.3)),
            short: None,
        };
        config
    }

    /// Worked by hand. From the mid of 0.70000, the long's take profit 20
    /// bps above, 0.00140, and its stop 10 bps below, 0.00070, its one R:
    /// the prices a dataset run's long sets there, closed 2 hours later,
    /// with no quantity. The long expects 0.5R.
    #[test]
    fn a_bracket_trade_is_on_the_exits_and_the_limit_its_decision_sets() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &bracket_config(10), (Some(10), None));
        handle.start(&mut probe).unwrap();

        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            decision.trade(),
            Trade::new(
                InstrumentId::from(AUD),
                OrderSide::Buy,
                Price::from("0.70000"),
                Decimal::new(70, 5),
                Exits {
                    bracket: Some(Bracket {
                        take_profit: Price::from("0.70140"),
                        stop_loss: Price::from("0.69930"),
                    }),
                    time_limit: Some(SignedDuration::from_hours(2)),
                },
            )
        );
        assert!(
            (decision.expected_r.as_f64() - 0.5).abs() < 1e-9,
            "{}",
            decision.expected_r
        );
    }

    /// Worked by hand: the long of 0.5R clears a margin of 0.4R, whatever
    /// its stop is worth in money: the margin is in R.
    #[test]
    fn a_side_clears_its_margin_in_r_whatever_its_stop_is_worth() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let config = ModelConfig {
            margins: Margins {
                long: Some(R::new(0.4)),
                short: None,
            },
            ..bracket_config(10)
        };
        let handle = sides_handle(&probe, models.path(), &config, (Some(10), None));
        handle.start(&mut probe).unwrap();

        let decision = handle.decide(&mut probe).unwrap();

        assert_eq!(decision.map(|decision| decision.side), Some(OrderSide::Buy));
    }

    /// Worked by hand. Both sides traded from 0.3R: the long of 0.1R falls
    /// short of it, and the short of 0.5R clears it. From the mid of
    /// 0.70000, the short's take profit is 0.00140 below and its stop
    /// 0.00070 above.
    #[test]
    fn a_short_trade_is_on_the_exits_its_decision_sets_on_the_short() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let config = ModelConfig {
            margins: Margins {
                long: Some(R::new(0.3)),
                short: Some(R::new(0.3)),
            },
            ..bracket_config(10)
        };
        let handle = sides_handle(&probe, models.path(), &config, (Some(6), Some(10)));
        handle.start(&mut probe).unwrap();

        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(decision.side, OrderSide::Sell);
        assert_eq!(
            decision.trade(),
            Trade::new(
                InstrumentId::from(AUD),
                OrderSide::Sell,
                Price::from("0.70000"),
                Decimal::new(70, 5),
                Exits {
                    bracket: Some(Bracket {
                        take_profit: Price::from("0.69860"),
                        stop_loss: Price::from("0.70070"),
                    }),
                    time_limit: Some(SignedDuration::from_hours(2)),
                },
            )
        );
    }

    /// `config` trading IC's gold, each exit a hundredth of a basis point
    /// from the decision's price.
    fn half_tick_config() -> ModelConfig {
        let bps = Distance::BasisPoints(Decimal::new(1, 2));
        let mut config = config();
        config.target = TargetConfig::builder()
            .instrument_id(InstrumentId::from(GOLD))
            .risk(config.target.risk)
            .spec(Target {
                bracket: Some(BracketSpec {
                    take_profit: bps,
                    stop: bps,
                    atr: None,
                }),
                limit: SignedDuration::from_hours(1),
                atr: None,
            })
            .build();
        config
    }

    /// `registered`, IC's gold in the cache quoted at 4000.00/4000.01 at
    /// `now`: the mid, 4000.005, on a half tick, and each exit of
    /// `half_tick_config` on the tick half a cent from it, a stop of 0.005.
    fn on_a_half_tick(now: Timestamp) -> (Decider, Rc<RefCell<VirtualClock>>) {
        let (probe, clock) = registered(now);
        let cache = probe.cache_rc();
        cache.borrow_mut().add_instrument(gold()).unwrap();
        cache
            .borrow_mut()
            .add_quote(QuoteTick::new(
                InstrumentId::from(GOLD),
                Price::from("4000.00"),
                Price::from("4000.01"),
                Quantity::from(1),
                Quantity::from(1),
                UnixNanos::from(now),
                UnixNanos::from(now),
            ))
            .unwrap();
        (probe, clock)
    }

    /// The long of -0.3R expects a loss whatever its stop is worth, half a
    /// cent here: it does not clear a margin of nought.
    #[test]
    fn a_side_expected_to_lose_on_a_half_tick_stop_does_not_clear_a_margin_of_nought() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = on_a_half_tick(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &half_tick_config(), (Some(2), None));

        assert_eq!(handle.decide(&mut probe).unwrap(), None);
    }

    /// Worked by hand. The short expects 0.3R and the long -0.3R, a loss, so
    /// the short trades from a margin of nought, its stop on the ask.
    #[test]
    fn a_side_expected_to_gain_on_a_half_tick_stop_trades_its_stop_in_the_market() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = on_a_half_tick(at("2026-10-22T12:00:00Z"));
        let config = ModelConfig {
            margins: both_sides().margins,
            ..half_tick_config()
        };
        let handle = sides_handle(&probe, models.path(), &config, (Some(2), Some(8)));

        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(decision.side, OrderSide::Sell);
        assert_eq!(
            decision.trade(),
            Trade::new(
                InstrumentId::from(GOLD),
                OrderSide::Sell,
                Price::from("4000.005"),
                Decimal::new(5, 3),
                Exits {
                    bracket: Some(Bracket {
                        take_profit: Price::from("4000.00"),
                        stop_loss: Price::from("4000.01"),
                    }),
                    time_limit: Some(SignedDuration::from_hours(1)),
                },
            )
        );
        assert!(
            (decision.expected_r.as_f64() - 0.3).abs() < 1e-12,
            "{}",
            decision.expected_r
        );
    }

    /// Worked by hand. `config`'s target is a limit alone, its ATR of one
    /// hourly bar warm on a bar from 0.50000 to 0.75000, a true range of
    /// 0.25000, whole in binary, and the long expects 0.1R. Its trade is a
    /// market entry closed an hour later, no exit set from the mid of
    /// 0.70000, its one R the ATR, what `Trade::send_at` sends without them.
    #[test]
    fn a_time_only_trade_has_no_exits_its_one_r_its_atr_and_closes_at_its_limit() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config(), (Some(6), None));
        handle.start(&mut probe).unwrap();
        probe.handle_bar(&hour_bar(
            "2026-10-22T12:00:00Z",
            ["0.70000", "0.75000", "0.50000", "0.70000"],
        ));

        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            decision.trade(),
            Trade::new(
                InstrumentId::from(AUD),
                OrderSide::Buy,
                Price::from("0.70000"),
                Decimal::new(25, 2),
                Exits {
                    bracket: None,
                    time_limit: Some(SignedDuration::from_hours(1)),
                },
            )
        );
        assert!(
            (decision.expected_r.as_f64() - 0.1).abs() < 1e-12,
            "{}",
            decision.expected_r
        );
    }

    /// `bracket_config` with a take profit 2 and a stop 1 average true
    /// ranges away, the ATR of 2 hourly bars, the cadence's.
    fn atr_config() -> ModelConfig {
        let mut config = bracket_config(10);
        config.target.spec = Target {
            bracket: Some(BracketSpec {
                take_profit: Distance::Atr(Decimal::from(2)),
                stop: Distance::Atr(Decimal::ONE),
                atr: Some(AtrArgs {
                    period: 2,
                    ..DEFAULT_ATR
                }),
            }),
            limit: SignedDuration::from_hours(2),
            atr: None,
        };
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

    /// The hourly mid bar of AUD/USD closing at noon of the 22nd, from
    /// 0.69950 to 0.70050: `config`'s target's ATR of one bar of it is
    /// 0.00100, its unit worth 1 USD on 1000 AUD/USD.
    fn unit_bar() -> Bar {
        hour_bar(
            "2026-10-22T12:00:00Z",
            ["0.70000", "0.70050", "0.69950", "0.70000"],
        )
    }

    /// Starts `handle` on `probe` in a backtest and hands it `unit_bar`,
    /// which the ATR of `config`'s target reads: its unit is 1 USD.
    fn started(handle: &ModelHandle, probe: &mut Decider) {
        handle.start(probe).unwrap();
        probe.handle_bar(&unit_bar());
    }

    /// The handle trading live on `config` for a strategy reading the
    /// quotes and the hourly bars of AUD/USD with a warmup of a day.
    fn hourly_live_handle(config: &ModelConfig, models: &Path) -> Rc<ModelHandle> {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL")],
            warmup: SignedDuration::from_hours(24),
            ..requirements()
        };
        ModelHandle::new(
            &blueprint_of(&features(), config, requirements),
            live(models.to_path_buf()),
        )
        .unwrap()
    }

    /// Deciding every minute, the target's ATR is on 1-minute bars, finer
    /// than the strategy's hourly bars: the live cache holds the day's
    /// 1,440 of them, and one spanning the start, on top of Nautilus'
    /// default 10,000, not the 25 hourly bars alone.
    #[test]
    fn a_live_cache_holds_the_warmup_of_the_targets_atr_on_finer_bars() {
        let models = tempfile::tempdir().unwrap();
        let mut config = atr_config();
        config.decision = DecisionRule::Every(SignedDuration::from_mins(1));
        let handle = hourly_live_handle(&config, models.path());

        let cache = handle.warmup().cache_config().unwrap();

        assert_eq!(cache.bar_capacity, 10_000 + 1_441);
    }

    /// A bracket in basis points has no ATR: the live cache holds the
    /// warmup of the strategy's bars alone, 24 hourly bars and one
    /// spanning the start.
    #[test]
    fn a_live_cache_without_an_atr_holds_the_warmup_of_the_strategys_bars() {
        let models = tempfile::tempdir().unwrap();
        let handle = hourly_live_handle(&bracket_config(10), models.path());

        let cache = handle.warmup().cache_config().unwrap();

        assert_eq!(cache.bar_capacity, 10_000 + 25);
    }

    /// A time-only target's ATR runs on the cadence's bars as a bracket's
    /// does: deciding every minute, the live cache holds the day's 1,440
    /// minute bars of its ATR, and one spanning the start, on top of
    /// Nautilus' default 10,000, not the 25 hourly bars alone.
    #[test]
    fn a_live_cache_holds_the_warmup_of_a_time_only_targets_atr() {
        let models = tempfile::tempdir().unwrap();
        let mut config = config();
        config.decision = DecisionRule::Every(SignedDuration::from_mins(1));
        let handle = hourly_live_handle(&config, models.path());

        let cache = handle.warmup().cache_config().unwrap();

        assert_eq!(cache.bar_capacity, 10_000 + 1_441);
    }

    /// Worked by hand. The warmup's response arrives at 12:00, Nautilus
    /// having built from it the hourly bars closing 11:00 (high 0.70000,
    /// low 0.69880, close 0.69950: a true range of 0.00120) and 12:00
    /// (high 0.70020, low 0.69940, above and below the close before: a
    /// true range of 0.00080) into the cache. The ATR of 2 bars, Wilder's
    /// seeded by their mean, is 0.00100, as a dataset run replaying those
    /// bars has it at 12:00: from the mid of 0.70000, the long's take
    /// profit 0.00200 above, 0.70200, and its stop 0.00100 below, 0.69900,
    /// its one R, the long's 0.4R expected above the margin. Without the
    /// warmup the ATR is cold and the decision no trade.
    #[test]
    fn a_live_warmup_sets_the_first_decisions_stop_from_the_targets_atr() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle_in(
            live(models.path().to_path_buf()),
            &probe,
            models.path(),
            &atr_config(),
            (Some(9), None),
        );
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
        handle.warmup().complete(&probe).unwrap();
        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            decision.trade(),
            Trade::new(
                InstrumentId::from(AUD),
                OrderSide::Buy,
                Price::from("0.70000"),
                Decimal::new(100, 5),
                Exits {
                    bracket: Some(Bracket {
                        take_profit: Price::from("0.70200"),
                        stop_loss: Price::from("0.69900"),
                    }),
                    time_limit: Some(SignedDuration::from_hours(2)),
                },
            )
        );
    }

    /// The handle decides only once the target's ATR is initialized, which
    /// it reads itself, not from Nautilus' registered indicators: at 12:00,
    /// the features warm from a quote and the ATR of 2 hourly bars without
    /// a bar, the decision publishes no row and makes no trade.
    #[test]
    fn no_row_is_published_while_the_targets_atr_is_cold() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let backtest = Mode::Backtest {
            models: models.path().to_path_buf(),
            venue: venue(),
        };
        let handle = sides_handle_in(
            backtest,
            &probe,
            models.path(),
            &atr_config(),
            (Some(8), None),
        );
        handle.start(&mut probe).unwrap();
        let rows = published();

        let decision = handle.decide(&mut probe).unwrap();

        assert_eq!(decision, None);
        assert_eq!(rows.borrow().len(), 0);
    }

    /// Features reading bars alone, as model_ema's: the set is registered
    /// with Nautilus for its bars as before, and the quotes the strategy
    /// hands its warmup reach none of its features. Worked by hand: at
    /// 12:00 the last hourly close is cold, a quote of AUD/USD handed over
    /// leaves it cold, and the bar closing at 12:00, which Nautilus hands
    /// it, warms it: the long model expecting 0.3R trades.
    #[test]
    fn features_reading_no_ticks_warm_on_their_bars_through_nautilus_as_before() {
        let models = tempfile::tempdir().unwrap();
        let hours = BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL");
        let features = FeatureSet::builder()
            .bars(hours, HOURLY_CLOSE, &tables())
            .build()
            .unwrap();
        let long = baseline_for(&features, &config(), Side::Long, "2026-10-22T00:00:00Z", 8);
        side_store(models.path(), &features, &config(), Side::Long)
            .publish(&long)
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements()),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        handle.start(&mut probe).unwrap();
        let quote = QuoteTick::new(
            InstrumentId::from(AUD),
            Price::from("0.69990"),
            Price::from("0.70010"),
            Quantity::from(1),
            Quantity::from(1),
            UnixNanos::from(at("2026-10-22T12:00:00Z")),
            UnixNanos::from(at("2026-10-22T12:00:00Z")),
        );

        let cold = handle.decide(&mut probe).unwrap();
        DataActor::on_quote(&mut probe, &quote).unwrap();
        let quoted = handle.decide(&mut probe).unwrap();
        probe.handle_bar(&hour_bar(
            "2026-10-22T12:00:00Z",
            ["0.69950", "0.70020", "0.69940", "0.70000"],
        ));
        let barred = handle.decide(&mut probe).unwrap();

        assert_eq!(
            (level(cold), level(quoted), level(barred)),
            (None, None, Some(r(8)))
        );
    }

    /// A model strategy whose data hooks are `model_data_hooks!`'s.
    #[derive(Debug)]
    struct Hooked {
        core: StrategyCore,
        model: Rc<ModelHandle>,
    }

    nautilus_strategy!(Hooked);

    impl DataActor for Hooked {
        crate::model_data_hooks!(model);
    }

    /// The hooks `model_data_hooks!` writes hand the warmup a strategy's
    /// ticks, of the history and live. Live on 2026-08-10 at 14:30, features
    /// on AUD/USD's quotes and GCZ26's trades, an average of two: a
    /// response of a quote and one of two trades warm them; once the group
    /// ends, a live quote at 14:30:01 and a live trade at 14:30:02 reach them.
    #[test]
    fn the_hooks_of_model_data_hooks_hand_the_warmup_a_strategys_ticks() {
        let models = tempfile::tempdir().unwrap();
        let gcz26 = InstrumentId::from("GCZ26.GLBX");
        let features = last_mid(InstrumentId::from(AUD))
            .trades(gcz26, GC_2_TRADES, &tables())
            .build()
            .unwrap();
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                aud_quotes(),
                TickSeries {
                    data: NautilusDataType::TradeTick,
                    instrument_id: gcz26,
                },
            ],
            warmup: SignedDuration::from_hours(1),
        };
        let model = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements),
            live(models.path().to_path_buf()),
        )
        .unwrap();
        let mut hooked = Hooked {
            core: strategy_core(),
            model: Rc::clone(&model),
        };
        register(&mut hooked.core, at("2026-08-10T14:30:00Z"));
        model.start(&mut hooked).unwrap();
        let aud_quote = |time: &str| {
            let ts = UnixNanos::from(at(time));
            QuoteTick::new(
                InstrumentId::from(AUD),
                Price::from("0.69990"),
                Price::from("0.70010"),
                Quantity::from(1),
                Quantity::from(1),
                ts,
                ts,
            )
        };

        hooked.handle_quotes_response(&QuotesResponse::new(
            UUID4::new(),
            ClientId::from("SIM"),
            InstrumentId::from(AUD),
            vec![aud_quote("2026-08-10T14:29:00Z")],
            None,
            None,
            UnixNanos::from(at("2026-08-10T14:30:00Z")),
            None,
        ));
        hooked.handle_trades_response(&TradesResponse::new(
            UUID4::new(),
            ClientId::from("GLBX"),
            gcz26,
            vec![
                gcz6_trade("2026-08-10T14:00:00Z", "4000.0"),
                gcz6_trade("2026-08-10T14:29:00Z", "4002.0"),
            ],
            None,
            None,
            UnixNanos::from(at("2026-08-10T14:30:00Z")),
            None,
        ));
        let warm = features.initialized();
        model.warmup().complete(&hooked).unwrap();
        DataActor::on_quote(&mut hooked, &aud_quote("2026-08-10T14:30:01Z")).unwrap();
        DataActor::on_trade(&mut hooked, &gcz6_trade("2026-08-10T14:30:02Z", "4003.0")).unwrap();

        assert!(warm);
        assert_eq!(
            (
                features.last_update(InstrumentId::from(AUD)),
                features.last_update(gcz26)
            ),
            (
                Some(UnixNanos::from(at("2026-08-10T14:30:01Z"))),
                Some(UnixNanos::from(at("2026-08-10T14:30:02Z")))
            )
        );
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
        let handle =
            ModelHandle::new(&blueprint_of(&features(), &config(), requirements), mode).unwrap();
        handle.start(&mut probe).unwrap();
        let next = clock.borrow().next_time_ns(decide::DECISION_TIMER).unwrap();
        Timestamp::from(next)
    }

    #[test]
    fn a_backtests_cadence_waits_for_composite_bars_as_a_live_ones_does() {
        let models = tempfile::tempdir().unwrap();
        let models = models.path().to_path_buf();
        let backtest = Mode::Backtest {
            models: models.clone(),
            venue: venue(),
        };
        let live = live(models);

        assert_eq!(
            first_decision(backtest, COMPOSITE),
            at("2026-10-22T12:00:05.010Z")
        );
        assert_eq!(
            first_decision(live, COMPOSITE),
            at("2026-10-22T12:00:05.010Z")
        );
    }

    /// `config` with a take profit and a stop 1 ATR of one hourly bar away,
    /// from 0.30 USD on the long, and the width of that bracket.
    fn width_config() -> (ModelConfig, Rc<BracketWidth>) {
        let mut config = bracket_config(10);
        config.target.spec.bracket = Some(BracketSpec {
            take_profit: Distance::Atr(Decimal::ONE),
            stop: Distance::Atr(Decimal::ONE),
            atr: Some(AtrArgs {
                period: 1,
                ..DEFAULT_ATR
            }),
        });
        let width = config
            .target
            .width(config.decision, &requirements())
            .unwrap();
        (config, Rc::new(width))
    }

    /// The hourly bar of AUD/USD's mids closing at `close_at`, from `low`
    /// to `high`, opening and closing at 0.70000.
    fn mid_bar(close_at: &str, low: &str, high: &str) -> Bar {
        hour_bar(close_at, ["0.70000", high, low, "0.70000"])
    }

    /// The rows the decisions of `STRATEGY` publish from now on.
    fn published() -> Rc<RefCell<Vec<FeatureRow>>> {
        let rows = Rc::new(RefCell::new(vec![]));
        let sink = Rc::clone(&rows);
        let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
            if let Some(row) = data.data.as_any().downcast_ref::<FeatureRow>() {
                sink.borrow_mut().push(row.clone());
            }
        });
        let topic = get_custom_topic(&FeatureRow::data_type(StrategyId::from(STRATEGY)));
        msgbus::subscribe_any(topic.into(), handler, None);
        rows
    }

    /// Worked by hand. The features read the mid over the width of the
    /// bracket, 1 ATR of one hourly bar a side, and the long's model of 0.8
    /// trades. At 12:00, after a bar from 0.69900 to 0.70100, the ATR is
    /// 0.00200: the row reads the mid of 0.70000 over it, 350, and the
    /// long's stop is 0.00200 below the mid, 0.69800. At 13:00, after a bar
    /// from 0.69800 to 0.70200, it is 0.00400: the row reads 175, the stop
    /// 0.69600. Each decision's row reads the width its trade is set at.
    #[test]
    fn a_decisions_row_reads_the_width_its_trade_is_set_at() {
        let models = tempfile::tempdir().unwrap();
        let (config, width) = width_config();
        let features = mid_over(InstrumentId::from(AUD), &width).build().unwrap();
        let long = baseline_for(&features, &config, Side::Long, "2026-10-22T00:00:00Z", 8);
        side_store(models.path(), &features, &config, Side::Long)
            .publish(&long)
            .unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T12:00:00Z"));
        warm(&features, probe.clock().timestamp_ns());
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config, requirements()),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap();
        handle.start(&mut probe).unwrap();
        let rows = published();
        let trade = |decision: Option<Decision>| decision.map(|decision| decision.trade());
        // A long one ATR from the mid either way, closed 2 hours later.
        let long = |atr: i64, take_profit: &str, stop_loss: &str| {
            Trade::new(
                InstrumentId::from(AUD),
                OrderSide::Buy,
                Price::from("0.70000"),
                Decimal::new(atr, 5),
                Exits {
                    bracket: Some(Bracket {
                        take_profit: Price::from(take_profit),
                        stop_loss: Price::from(stop_loss),
                    }),
                    time_limit: Some(SignedDuration::from_hours(2)),
                },
            )
        };

        probe.handle_bar(&mid_bar("2026-10-22T12:00:00Z", "0.69900", "0.70100"));
        let noon = trade(handle.decide(&mut probe).unwrap());
        advance(&features, &clock, "2026-10-22T13:00:00Z");
        probe.handle_bar(&mid_bar("2026-10-22T13:00:00Z", "0.69800", "0.70200"));
        let one = trade(handle.decide(&mut probe).unwrap());

        let read: Vec<f64> = rows.borrow().iter().map(|row| row.features[0]).collect();
        assert_eq!(read.len(), 2, "{read:?}");
        assert!((read[0] - 350.0).abs() < 1e-9, "{read:?}");
        assert!((read[1] - 175.0).abs() < 1e-9, "{read:?}");
        assert_eq!(
            (noon, one),
            (
                Some(long(200, "0.70200", "0.69800")),
                Some(long(400, "0.70400", "0.69600"))
            )
        );
    }

    /// A dataset run's targets run on the ATR of the width the features
    /// read: started, the handle has the target's bars, AUD/USD's hourly
    /// mids, reach it, and nothing else does. One bar from 0.699 to 0.701
    /// warms an ATR of one bar.
    #[test]
    fn a_dataset_runs_targets_run_on_the_atr_of_the_features_width() {
        let (config, width) = width_config();
        let features = mid_over(InstrumentId::from(AUD), &width).build().unwrap();
        let (mut probe, _) = registered(at("2026-10-22T11:30:00Z"));
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config, requirements()),
            Mode::Dataset {
                rows: SettledRows::default(),
                venue: venue(),
            },
        )
        .unwrap();
        handle.start(&mut probe).unwrap();
        assert!(!width.initialized());

        probe.handle_bar(&mid_bar("2026-10-22T11:00:00Z", "0.69900", "0.70100"));

        assert!(width.initialized());
    }

    #[test]
    fn a_dataset_runs_cadence_waits_for_composite_bars() {
        let dataset = Mode::Dataset {
            rows: SettledRows::default(),
            venue: venue(),
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
                    venue: venue(),
                },
            ),
            ("live", live(models)),
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
    /// under `models`, for a strategy of `requirements`, started.
    fn never_retrained_live(
        probe: &mut Decider,
        models: &Path,
        requirements: &DataRequirements,
    ) -> Rc<ModelHandle> {
        let handle = ModelHandle::new(
            &blueprint_of(&features(), &never_retrained(), requirements.clone()),
            live(models.to_path_buf()),
        )
        .unwrap();
        handle.start(probe).unwrap();
        handle
    }

    /// The bar-build delay is the bar types', not retraining's: a live
    /// handle never retrained decides on composite bars as late as a
    /// retrained one.
    #[test]
    fn a_live_handle_never_retrained_waits_for_composite_bars() {
        let models = tempfile::tempdir().unwrap();
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(COMPOSITE)],
            ..requirements()
        };
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));

        never_retrained_live(&mut probe, models.path(), &requirements);

        let next = clock.borrow().next_time_ns(decide::DECISION_TIMER).unwrap();
        assert_eq!(Timestamp::from(next), at("2026-10-22T12:00:05.010Z"));
    }

    /// Live trading on the models under `models`.
    fn live(models: PathBuf) -> Mode {
        Mode::Live {
            models,
            venue: venue(),
        }
    }

    /// The probe's handle trading live on the models under `models`,
    /// started, its warmup ended, deciding on `features`, warm: a quote came
    /// at the time of `probe`.
    fn live_handle(
        probe: &mut Decider,
        models: &Path,
        features: Rc<FeatureSet>,
    ) -> Rc<ModelHandle> {
        let handle = warming_handle(probe, models, features);
        handle.warmup().complete(probe).unwrap();
        handle
    }

    /// `live_handle`, its warmup not answered yet, `unit_bar` built into
    /// the cache as Nautilus builds a response's bars: the warmup hands it
    /// to the target's ATR as it ends.
    fn warming_handle(
        probe: &mut Decider,
        models: &Path,
        features: Rc<FeatureSet>,
    ) -> Rc<ModelHandle> {
        warm(&features, probe.clock().timestamp_ns());
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements()),
            live(models.to_path_buf()),
        )
        .unwrap();
        handle.start(probe).unwrap();
        probe.cache_rc().borrow_mut().add_bar(unit_bar()).unwrap();
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
        let store = store(models.path());
        store.publish(&baseline("2026-10-21T00:00:00Z", 7)).unwrap();
        store.publish(&baseline("2026-10-22T00:00:00Z", 8)).unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = live_handle(&mut probe, models.path(), features());

        assert_eq!(level(handle.decide(&mut probe).unwrap()), Some(r(8)));
    }

    /// A warmup reaches the features window by window with the node running
    /// between them: features warm and fresh on what they got so far give no
    /// decision until it ends, then the decision they give.
    #[test]
    fn a_live_node_does_not_decide_until_its_warmup_ends() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = warming_handle(&mut probe, models.path(), features());

        let warming = level(handle.decide(&mut probe).unwrap());
        handle.warmup().complete(&probe).unwrap();

        assert_eq!(warming, None);
        assert_eq!(level(handle.decide(&mut probe).unwrap()), Some(r(8)));
    }

    /// Live, a model is published while the node runs, by a retrain or by
    /// `train --at`, valid from 23:00, a retrain's time on a catalog
    /// ending there: the handle decides at 23:30 on the model valid from
    /// 10:00, and from its next decision, at 01:00, on the new one, the
    /// store listed again at each.
    #[test]
    fn a_model_published_while_live_is_used_from_the_next_decision() {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let features = features();
        let handle = live_handle(&mut probe, models.path(), Rc::clone(&features));
        let before = level(handle.decide(&mut probe).unwrap());

        store.publish(&baseline("2026-10-22T23:00:00Z", 9)).unwrap();
        advance(&features, &clock, "2026-10-23T01:00:00Z");

        assert_eq!(before, Some(r(7)));
        assert_eq!(level(handle.decide(&mut probe).unwrap()), Some(r(9)));
    }

    /// The handle trading in a backtest on the models under `models` as
    /// `config` says, its long a model of 0.1R from midnight (expecting 0.1
    /// USD, above its margin of nought), its meta model predicting an R of
    /// `r(meta_tenths)` from midnight, its features warm by a quote at noon of
    /// the 22nd.
    fn meta_handle(models: &Path, config: &ModelConfig, meta_tenths: usize) -> Rc<ModelHandle> {
        let features = features();
        let set = model_set(models, &features, config);
        let midnight = "2026-10-22T00:00:00Z";
        set.side(Side::Long)
            .unwrap()
            .store()
            .publish(&baseline_for(&features, config, Side::Long, midnight, 6))
            .unwrap();
        set.meta()
            .unwrap()
            .store()
            .publish(&meta_baseline(&features, config, midnight, meta_tenths))
            .unwrap();
        warm(&features, UnixNanos::from(at("2026-10-22T12:00:00Z")));
        ModelHandle::new(
            &blueprint_of(&features, config, requirements()),
            Mode::Backtest {
                models: models.to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap()
    }

    /// The meta predicting an R of 0.3 expects 0.25R, `0.1 + 0.5 E`, at or
    /// above its margin of 0.15R: the decision carries the meta's expected R
    /// beside the side's.
    #[test]
    fn a_meta_model_from_its_margin_takes_the_trade_of_the_side_picked() {
        let models = tempfile::tempdir().unwrap();
        let handle = meta_handle(models.path(), &with_meta(meta()), 8);
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        started(&handle, &mut probe);

        let decision = handle.decide(&mut probe).unwrap().unwrap();

        assert_eq!(
            (
                decision.side,
                decision.expected_r,
                decision.level,
                decision.side_call,
                decision.meta_expected_r
            ),
            (
                OrderSide::Buy,
                // The side's model's, its long of 0.1R.
                R::new(0.1),
                0.1,
                0.0,
                Some(R::new(0.25))
            )
        );
    }

    /// A file where the store's root should be has no store to list.
    #[test]
    fn an_unreadable_store_fails_when_the_handle_is_built() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a file");
        std::fs::write(&file, "").unwrap();

        let built = ModelHandle::new(
            &blueprint_of(&features(), &config(), requirements()),
            Mode::Backtest {
                models: file,
                venue: venue(),
            },
        );

        assert!(built.is_err());
    }

    /// A dataset run's handle hands on the targets' refusal, which fails the
    /// strategy's `on_start`, and so the run.
    #[test]
    fn a_dataset_runs_start_fails_on_a_bracket_on_an_inverse_instrument() {
        let inverse = InstrumentAny::CryptoPerpetual(btcusd_bybit());
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        probe
            .cache_rc()
            .borrow_mut()
            .add_instrument(inverse.clone())
            .unwrap();
        let mut config = bracket_config(10);
        config.target.instrument_id = inverse.id();
        let handle = ModelHandle::new(
            &blueprint_of(&features(), &config, requirements()),
            Mode::Dataset {
                rows: SettledRows::default(),
                venue: venue(),
            },
        )
        .unwrap();

        let err = handle.start(&mut probe).unwrap_err();

        assert!(format!("{err:#}").contains("inverse"), "{err:#}");
    }

    /// Nautilus' sizer reads a trade's P&L as a multiple of a distance in
    /// price, which an inverse instrument's is not: a deciding handle
    /// trading one refuses to start, which fails the strategy's
    /// `on_start`.
    #[test]
    fn a_deciding_handles_start_fails_on_an_inverse_instrument() {
        let inverse = InstrumentAny::CryptoPerpetual(btcusd_bybit());
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        probe
            .cache_rc()
            .borrow_mut()
            .add_instrument(inverse.clone())
            .unwrap();
        let models = tempfile::tempdir().unwrap();
        let mut config = bracket_config(10);
        config.target.instrument_id = inverse.id();
        let handle = ModelHandle::new(
            &blueprint_of(&features(), &config, requirements()),
            Mode::Backtest {
                models: models.path().to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap();

        let err = handle.start(&mut probe).unwrap_err();

        assert!(format!("{err:#}").contains("inverse"), "{err:#}");
    }

    /// A quote of `eur` at noon, its mid 1.00000.
    fn eur_quote(eur: InstrumentId) -> QuoteTick {
        let noon = UnixNanos::from(at("2026-10-22T12:00:00Z"));
        QuoteTick::new(
            eur,
            Price::from("0.99990"),
            Price::from("1.00010"),
            Quantity::from(1),
            Quantity::from(1),
            noon,
            noon,
        )
    }

    /// What a strategy reading the quotes of `eur` alone reads.
    fn reading_eur(eur: InstrumentId) -> DataRequirements {
        DataRequirements {
            ticks: vec![TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: eur,
            }],
            ..requirements()
        }
    }

    /// `eur`, which `registered`'s cache does not hold as an instrument, its
    /// quotes read by the features of the returned handle in a backtest on
    /// `config` under `models`, warm at noon.
    fn handle_on_an_instrument_not_in_the_cache(
        models: &Path,
        config: &ModelConfig,
        eur: InstrumentId,
    ) -> Rc<ModelHandle> {
        let features = last_mid(eur).build().unwrap();
        features.handle_quote(&eur_quote(eur)).unwrap();
        ModelHandle::new(
            &blueprint_of(&features, config, reading_eur(eur)),
            Mode::Backtest {
                models: models.to_path_buf(),
                venue: venue(),
            },
        )
        .unwrap()
    }

    /// The target is set before the models are asked: its exits need the
    /// instrument, whether or not a model is valid.
    #[test]
    fn no_model_valid_with_the_instrument_missing_from_the_cache_is_an_error() {
        let models = tempfile::tempdir().unwrap();
        let eur = InstrumentId::from("EUR/USD.SIM");
        let mut config = bracket_config(10);
        config.target.instrument_id = eur;
        let handle = handle_on_an_instrument_not_in_the_cache(models.path(), &config, eur);
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        let err = handle.decide(&mut probe).unwrap_err();

        assert!(format!("{err:#}").contains("EUR/USD.SIM"), "{err:#}");
    }

    /// A time-only target reads the instrument as a bracket does, its unit
    /// the worth of its ATR on it and its entry cost costing its fills:
    /// missing from the cache, it is an error, though the long of 0.8 is
    /// valid, the instrument quoted and the ATR warm.
    #[test]
    fn a_time_only_target_with_the_instrument_missing_from_the_cache_is_an_error() {
        let models = tempfile::tempdir().unwrap();
        let eur = InstrumentId::from("EUR/USD.SIM");
        let mut config = config();
        config.target.instrument_id = eur;
        let features = last_mid(eur).build().unwrap();
        // The target's ATR, on the quotes of EUR/USD, keys its store.
        blueprint_of(&features, &config, reading_eur(eur))
            .model_set(models.path(), &venue())
            .unwrap()
            .side(Side::Long)
            .unwrap()
            .store()
            .publish(&baseline_for(
                &features,
                &config,
                Side::Long,
                "2026-10-22T00:00:00Z",
                8,
            ))
            .unwrap();
        let handle = handle_on_an_instrument_not_in_the_cache(models.path(), &config, eur);
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        probe
            .cache_rc()
            .borrow_mut()
            .add_quote(eur_quote(eur))
            .unwrap();
        handle.start(&mut probe).unwrap();
        probe.handle_bar(&Bar {
            bar_type: BarType::from("EUR/USD.SIM-1-HOUR-MID-INTERNAL"),
            ..unit_bar()
        });

        let err = handle.decide(&mut probe).unwrap_err();

        assert!(format!("{err:#}").contains("EUR/USD.SIM"), "{err:#}");
    }

    /// The model valid from 12:00, listed by the decision at 11:00, swaps
    /// in at 12:00 though the store then fails to list.
    #[test]
    fn a_failed_listing_keeps_the_previous_list() {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
        store.publish(&baseline("2026-10-22T12:00:00Z", 8)).unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:00:00Z"));
        let features = features();
        let handle = handle(&probe, models.path(), Rc::clone(&features));
        started(&handle, &mut probe);
        let before = level(handle.decide(&mut probe).unwrap());

        store.break_listing();
        advance(&features, &clock, "2026-10-22T12:00:00Z");

        assert_eq!(before, Some(r(7)));
        assert_eq!(level(handle.decide(&mut probe).unwrap()), Some(r(8)));
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
            InstrumentId::from("GCZ26.GLBX"),
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
    /// GC, an average of two, reads GCZ26, the contract standing for
    /// it (GC rolls into GCZ26 on 2026-07-29, `rolls/README.md`): the start
    /// requests GCZ26's trades over the hour of warmup, ending now, then
    /// subscribes to them from now, so the live stream starts where the
    /// warmup ends, and loads GCG27, the post contract of the next roll;
    /// the traded AUD/USD's quotes, which no feature reads, are subscribed
    /// with no warmup, the target, `bracket_config`'s, having no ATR to warm.
    /// The response's two trades, which Nautilus hands to the strategy's
    /// `on_historical_trades` and that to the warmup, make the features
    /// initialized: the first decision after the warmup is warm.
    #[test]
    fn a_live_start_warms_a_roots_tick_features_on_its_front_contract() {
        let models = tempfile::tempdir().unwrap();
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
            &blueprint_of(&features, &bracket_config(10), requirements),
            live(models.path().to_path_buf()),
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        let sent = sent_commands();

        handle.start(&mut probe).unwrap();
        let cold = features.initialized();
        probe.handle_trades_response(&TradesResponse::new(
            UUID4::new(),
            ClientId::from("GLBX"),
            InstrumentId::from("GCZ26.GLBX"),
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
                "request trades [GCZ26.GLBX] Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "subscribe trades [GCZ26.GLBX] from Some(2026-08-10T14:30:00Z)",
                "request instrument [GCG27.GLBX]",
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
        assert!(features.initialized());
    }

    /// The model strategy of `features`, on the hourly decisions of
    /// `bracket_config`, whose target has no ATR to warm, reading `bar_types`
    /// and `ticks`, started live on 2026-08-10 at 14:30 with an hour of
    /// warmup: the data commands its start sent.
    fn started_live(
        features: Rc<FeatureSet>,
        bar_types: Vec<BarType>,
        ticks: Vec<TickSeries>,
    ) -> Vec<String> {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let requirements = DataRequirements {
            bar_types,
            ticks,
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            &blueprint_of(&features, &bracket_config(10), requirements),
            live(models.path().to_path_buf()),
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
    /// strategy of `features` reading `ticks`, on `bracket_config`, whose
    /// target has no ATR to warm, its warmup handed its historical custom
    /// data, started: the probe, the handle and the data commands the start
    /// sent.
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
            &blueprint_of(&features, &bracket_config(10), requirements),
            live(models.path().to_path_buf()),
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
    /// root GC reads GCZ26's, the contract standing for it (GC rolls into
    /// GCZ26 on 2026-07-29, `rolls/README.md`): the start requests GCZ26's
    /// over the hour of warmup from the catalog, subscribes to them, not
    /// to the root's, and loads GCG27, the post contract of the next roll.
    /// The answer's imbalance of GCZ26 makes the feature hold it.
    #[test]
    fn a_live_start_warms_a_custom_feature_on_a_root_on_its_front_contract() {
        let features = last_imbalance("GC.GLBX");
        let (mut probe, _handle, sent, _models) = started_live_reading(
            Rc::clone(&features),
            vec![imbalances_of("GC.GLBX"), aud_quotes()],
        );

        probe.handle_data_response(&catalog_answer(
            "GCZ26.GLBX",
            vec![imbalance("GCZ26.GLBX", "2026-08-10T14:10:00Z", 0.75)],
        ));

        assert_sent(
            &sent,
            &[
                "request data Imbalance of Some(\"GCZ26.GLBX\") from CATALOG \
                 Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "request instrument [GCG27.GLBX]",
                "subscribe quotes [AUD/USD.SIM] from Some(2026-08-10T14:30:00Z)",
            ],
        );
        assert!(
            !sent.iter().any(|line| line.contains("\"GC.GLBX\"")),
            "{sent:#?}"
        );
        assert_eq!(
            (
                imbalance_subscribers("GCZ26.GLBX"),
                imbalance_subscribers("GC.GLBX")
            ),
            (1, 0)
        );
        assert_eq!(features.values(), Some(vec![0.75]));
    }

    /// A feature on the imbalances of the root GC, in a backtest from
    /// 2026-11-25 at 23:00, reads GCZ26's, the contract standing for it,
    /// not GCG27's. GC rolls into GCG27 at 00:00 UTC on 2026-11-26: the
    /// roll's alert, handed to the handle, moves the subscription to
    /// GCG27's and resets the feature, cold through GCZ26's imbalances after
    /// it, and the feature reads GCG27's from then.
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
            &blueprint_of(&features, &config(), requirements),
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

        let pre = read(imbalance("GCZ26.GLBX", "2026-11-25T23:10:00Z", 0.25));
        let post_before_the_roll = read(imbalance("GCG27.GLBX", "2026-11-25T23:20:00Z", 0.75));
        run_to(&handle, &mut probe, &clock, "2026-11-26T00:30:00Z");
        let rolled = features.values();
        let pre_after_the_roll = read(imbalance("GCZ26.GLBX", "2026-11-26T00:40:00Z", -0.25));
        let post = read(imbalance("GCG27.GLBX", "2026-11-26T00:50:00Z", -0.75));

        assert_eq!(pre, Some(vec![0.25]));
        assert_eq!(post_before_the_roll, Some(vec![0.25]));
        assert_eq!(rolled, None);
        assert_eq!(pre_after_the_roll, None);
        assert_eq!(post, Some(vec![-0.75]));
        assert_eq!(
            (
                imbalance_subscribers("GCZ26.GLBX"),
                imbalance_subscribers("GCG27.GLBX")
            ),
            (0, 1)
        );
    }
}
