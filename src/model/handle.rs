//! The model handle holds what a strategy trading on a learned model needs. It is the one place
//! where the strategy decides, in a backtest, live and a dataset run alike.
//!
//! The strategy shares a `ModelHandle` (an `Rc`), built from its `blueprint::ModelBlueprint` in a
//! `Mode`. `start`, called from `on_start`, registers the features, starts the warmup that feeds
//! them and the target's ATR, and starts the decision cadence.
//!
//! At a decision (`decide::is_decision`), `decide` publishes the feature row (`decide::decide`)
//! only if all of these hold:
//! - No live warmup is still answering (`Warmup::is_warming`).
//! - The target's ATR, if any, is initialized. The handle reads this itself, not through
//!   Nautilus' indicators.
//! - The decision is fresh: data of the target's instrument has come since the previous decision.
//! - The decision is an event: the event filter, if any, is initialized and marks it
//!   (`FeatureSet::event`).
//! - Every feature is initialized.
//!
//! Then the mode decides what happens:
//!
//! - `Mode::Backtest`, `Mode::Live`: takes the latest model set valid at the decision
//!   (`store::ModelSet`). Each side's model predicts its R on the side's row
//!   (`deciding::SideRow`). The pair calibrates the prediction (`calibration::Ksplit`) on the
//!   target the decision sets (`targets::TargetSetter`). The handle returns the `Decision` of the
//!   side whose expected R clears its margin, the larger if both do (`payoff::pick`). It returns
//!   one only while the strategy has no working order or open position on the traded instrument.
//!   The handle sends no order and reads no equity. The strategy sends `Decision::trade` through
//!   `strategy::orders`, at a stake of its own. The models see the target at the reference stake
//!   alone. With no model set valid yet, no side trades. These are no decision, never an error: a
//!   stale decision, a cold event filter or no event, cold features, an uninitialized ATR, a target of no size, no side clearing
//!   its margin, or a tie. The target is set before the models are asked, so `decide` errs when
//!   the instrument is missing from the cache.
//! - `Mode::Dataset`: opens the decision's target (`targets::Targets`) and returns no decision.
//!   The strategy hands the order module its time events and the handle its order and position
//!   events. The handle collects the rows the targets settle for the caller of the run.
//!
//! With a meta model (`model::meta`), the handle takes the trade of the picked side only if the
//! meta model's calibrated expected net R on that row is at least its margin
//! (`MetaConfig::margin`). Every model set of a strategy with a meta holds one.
//!
//! A model set is swapped in only inside `decide`, before it predicts, and always whole. No
//! decision mixes two model sets, or a meta with the sides of another train. The store must be
//! readable when the handle is built. The handle lists it again at each decision. A failed listing
//! is logged and the previous list is kept. So a model set published meanwhile is used from the
//! first decision at or after its valid-from.
//!
//! Before use, each model of a model set is checked against the strategy:
//! - Its feature names and their order.
//! - Its fingerprint (`dataset::fingerprint`).
//! - Its width.
//! - Its probes: the reloaded model must predict them to the bit, as when fitted.
//!
//! A model set that fails a check stops new entries. `decide` errs, naming the model set and the
//! check. No decision is made on it or on the model set before it. The handle reads it again at
//! the next decision (`store::Swap`). Open trades finish on their exits, which the handle does not
//! hold.
//!
//! The handle owns the strategy's subscriptions (`strategy::subscriptions::Subscriptions`):
//! - The bars of its requirements and of the target's ATR (`TargetSetter::atr_bar_type`,
//!   `Targets::atr_bar_type`).
//! - Every tick series of its data requirements. The harness reads them for the decisions'
//!   freshness.
//! - The custom series (`DataRequirements::custom`). They reach `on_data`.
//!
//! A `rolls::Roller` covers the instruments of the ticks and the roots of the custom series
//! (`Roller::custom_roots`). `start` subscribes each series once, on the contract standing for its
//! instrument or root, and sets each root's roll alert. The strategy hands the handle its time
//! events, after `strategy::orders::on_time_event`, for the roller. At a roll, the root's series
//! move to the post contract (`Subscriptions::roll`) and its features and event filter restart
//! there (`FeatureSet::roll`), so decisions wait until they refill. A backtest and a dataset run roll on
//! the catalog's contract slices (`DataRequirements::slices`). Live, the roller loads each post
//! contract before its roll (`Roller::live`).
//!
//! A node builds composite bars `warmup::bar_build_delay` after their close. A backtest and a
//! dataset run build them as late as a live node does, so the cadence starts with that delay in
//! every mode.
//!
//! The handle builds the strategy's warmup (`strategy::warmup::Warmup`) from its requirements and
//! mode. `start` starts it, with the features registered first. `model_data_hooks!` writes the
//! data hooks that hand it the responses and the live data, so that no strategy misses one. The
//! features and the target's ATR are registered with none of Nautilus' indicators. The warmup
//! hands them its data in time order across series and holds the live data until it ends. So none
//! reaches them before an older window of the history (`strategy::warmup`'s `merge` and `hold`).
//!
//! Live, the warmup requests the history of every tick series of the requirements that its venue
//! serves, and of every custom series, so the first decision after a start is warm. It also warms
//! a series no feature reads, such as the traded instrument's or a conversion pair's, so that the
//! cache holds its latest price at the start. Its ticks reach no feature, because each feature
//! reads those of its own series alone (`FeatureSet::warmed`). The target's ATR is warmed from the
//! same request (`TargetSetter::warmed`, `Targets::warmed`), and a live node sizes its cache from
//! it.
//!
//! Features over the bracket's width (`features::OverBracket`) read the width the setter is built
//! on (`FeatureSet::bracket_width`, `TargetSetter::sharing`). `decide` reads the features and sets
//! the target in one decision event, so a row reads the width its target is set at.
//!
//! The handle does not retrain. Live, retrains are a Nautilus actor of their own
//! (`launcher::RetrainActor`). The handle uses the model set that a retrain or `train --at`
//! publishes from its next decision.

use std::{cell::RefCell, fmt::Debug, path::PathBuf, rc::Rc};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
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
    decide::{self, DecisionRule},
    deciding::{DecidingModels, SideRow},
    features::FeatureSet,
    targets::{NoTarget, TargetRow, TargetSetter, Targets},
};
use crate::{
    rolls::Roller,
    strategy::{
        orders::{self, Trade},
        requirements::CustomSeries,
        risk::R,
        subscriptions::Subscriptions,
        warmup::{self, Warmup},
    },
};

/// Where a handle runs, and so what it does at a decision.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Trades on the models of the store under `models`. The target is the one its dataset runs
    /// use (`ModelConfig::dataset`).
    Backtest { models: PathBuf },
    /// As `Backtest`, with a warmup that requests the history up to now (`Warmup::live`) and a
    /// roller that loads each post contract before its roll (`Roller::live`). The node's retrain
    /// actor runs its retrains (`launcher::RetrainActor`).
    Live { models: PathBuf },
    /// Opens the target of each decision at the reference stake of the dataset balance. Collects
    /// the rows it settles in `rows`.
    Dataset { rows: SettledRows },
}

/// The rows a dataset run settles, shared with its caller. A denied order fails the run instead
/// (`Targets::on_order_denied`).
#[derive(Debug, Clone, Default)]
pub struct SettledRows(Rc<RefCell<Settled>>);

#[derive(Debug, Default)]
struct Settled {
    rows: Vec<TargetRow>,
    /// The first denial, which failed the run.
    denial: Option<String>,
}

impl SettledRows {
    /// Takes the rows settled so far.
    ///
    /// # Errors
    ///
    /// Returns an error if a denial failed the run. The error is the denial.
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

    /// Fails the run with `denial`, unless an earlier denial already failed it.
    pub(super) fn fail(&self, denial: String) {
        self.0.borrow_mut().denial.get_or_insert(denial);
    }
}

/// A decision to trade, as plain data.
///
/// It holds:
/// - The side the models pick, and its expected net R.
/// - The calibrated level and side call.
/// - The meta's expected R, if a meta took it.
/// - The trade the models learned on that side, without a quantity (`trade`).
///
/// The handle sends nothing. A strategy sends the trade at a stake of its choosing
/// (`orders::Trade::send`), drops it, or trades otherwise. Then the models' scores describe the
/// target's trade, not the strategy's own. The expected R is the same at any stake.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    /// The target's instrument.
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    /// The calibrated expected net R of the side's trade, `E'`. The side's margin applies to it.
    pub expected_r: R,
    /// The calibrated level of the decision, `C'`, in R.
    pub level: f64,
    /// The calibrated side call of the decision, `D'`, in R. It is zero for a one-sided strategy.
    pub side_call: f64,
    /// The meta model's calibrated expected R, if a meta took it. The meta's margin applies to it.
    pub meta_expected_r: Option<R>,
    trade: Trade,
}

impl Decision {
    /// Returns the trade the model learned, the one a dataset run sends on that side.
    ///
    /// The trade starts from the price the target is set from: the mid of the last quote, else the
    /// last trade, else the last close. It is either a bracket with the target's exits on the
    /// instrument's ticks, or a market entry closed at the time limit (`strategy::orders`). One R
    /// is the stop's distance or, for a time-only target, the target's ATR. The trade has no
    /// quantity: it is sized as it is sent. Its send tags the entry with `expected_r`
    /// (`orders::entry_expected_r`), which a dataset run's trade lacks.
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
    /// The delay after each grid time at which the cadence decides (`warmup::bar_build_delay`).
    delay: SignedDuration,
    traded: InstrumentId,
    serving: Serving,
    warmup: Rc<Warmup>,
    subscriptions: Subscriptions,
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

#[derive(Debug)]
struct Deciding {
    models: RefCell<DecidingModels>,
    target: TargetSetter,
    instrument_id: InstrumentId,
}

impl ModelHandle {
    /// Builds the handle for the model `blueprint` describes, in `mode`.
    ///
    /// It decides on the blueprint's features, the set that fingerprints its dataset and stores.
    ///
    /// # Errors
    ///
    /// Returns an error if the mode decides and a store cannot be keyed or read.
    pub fn new(blueprint: &ModelBlueprint, mode: &Mode) -> anyhow::Result<Rc<Self>> {
        let (features, requirements) = (blueprint.features(), blueprint.requirements());
        let (rule, target) = (blueprint.config().decision, &blueprint.config().target);
        let traded = target.instrument_id;
        let mut ticks = features.ticks();
        for tick in &requirements.ticks {
            if !ticks.contains(tick) {
                ticks.push(tick.clone());
            }
        }
        let warmup = match mode {
            Mode::Live { .. } => Warmup::live(requirements),
            Mode::Backtest { .. } | Mode::Dataset { .. } => Warmup::replayed(requirements),
        };
        let warmup = features.warmed(warmup);
        let instrument_ids = ticks.iter().map(|tick| tick.instrument_id).collect();
        let roller = match mode {
            Mode::Live { .. } => Roller::live(instrument_ids),
            Mode::Backtest { .. } | Mode::Dataset { .. } => Roller::replayed(instrument_ids),
        }
        .custom_roots(requirements.custom.iter().filter_map(CustomSeries::root));
        let delay = warmup::bar_build_delay(&requirements.bar_types);
        let serving = match mode {
            Mode::Backtest { models } | Mode::Live { models } => {
                let models = blueprint.model_store(models)?;
                Serving::Deciding(Box::new(Deciding {
                    models: RefCell::new(DecidingModels::new(&models)?),
                    target: TargetSetter::sharing(
                        target,
                        rule,
                        requirements,
                        &blueprint.config().dataset,
                        features.bracket_width(),
                    )?,
                    instrument_id: target.instrument_id,
                }))
            }
            Mode::Dataset { rows } => Serving::Dataset {
                targets: Targets::sharing(
                    target.clone(),
                    rule,
                    requirements,
                    &blueprint.config().dataset,
                    features.bracket_width(),
                )?,
                rows: rows.clone(),
            },
        };

        // Live, the ATR warms on the warmup's history, else on the replay
        let warmup = match &serving {
            Serving::Deciding(deciding) => deciding.target.warmed(warmup),
            Serving::Dataset { targets, .. } => targets.warmed(warmup),
        };

        // Nautilus subscribes the ATR's bars once if the strategy reads them too
        // (`DataActorCore::subscribe_bars`).
        let atr_bars = match &serving {
            Serving::Deciding(deciding) => deciding.target.atr_bar_type(),
            Serving::Dataset { targets, .. } => targets.atr_bar_type(),
        };
        let bar_types = requirements.bar_types.iter().copied().chain(atr_bars);
        let subscriptions =
            Subscriptions::new(bar_types.collect(), ticks, requirements.custom.clone());
        let subscriptions = match mode {
            Mode::Live { .. } => subscriptions.live(),
            Mode::Backtest { .. } | Mode::Dataset { .. } => subscriptions,
        };
        Ok(Rc::new(Self {
            strategy_id: blueprint.strategy_id(),
            features: Rc::clone(features),
            rule,
            delay,
            traded,
            serving,
            subscriptions,
            warmup: Rc::new(warmup),
            roller,
        }))
    }

    /// Starts the handle on `strategy`, from `on_start`.
    ///
    /// It does these steps in order:
    /// - Refuses an inverse traded instrument (`orders::refuse_inverse`).
    /// - Registers the features.
    /// - Starts the warmup (`Warmup::start`), then the subscriptions (`Subscriptions::start`) from
    ///   where the warmup ends, and the roller.
    /// - Starts the target's ATR, or in a dataset run the targets.
    /// - Starts the decision cadence.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The traded instrument is inverse.
    /// - The warmup fails to start.
    /// - Nautilus refuses a roll alert or the cadence's timer.
    /// - In a dataset run, the targets refuse to start (`Targets::start`).
    pub fn start<S>(&self, strategy: &mut S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative + Debug + 'static,
    {
        orders::refuse_inverse(strategy, self.traded)?;

        // Registered first, because it sets the contract each feature reads, and the warmup's
        // responses are read against that contract.
        self.features.register(strategy);
        let now = self.warmup.start(strategy)?;
        self.subscriptions.start(strategy, now);
        self.roller.start(strategy, now)?;
        if let Serving::Dataset { targets, rows } = &self.serving {
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
        decide::start(strategy, self.rule, self.delay)
    }

    /// Returns the strategy's warmup.
    ///
    /// Its data hooks feed it (`model_data_hooks!`), and a live node sizes its cache from it.
    #[must_use]
    pub fn warmup(&self) -> Rc<Warmup> {
        Rc::clone(&self.warmup)
    }

    // panics-doc-ok (transitive via decide::decide)
    /// Returns the decision to trade at `event`, the decision timer's, for `strategy`, if any
    /// (`decide::decide`).
    ///
    /// Returns `None` while the warmup is answering, the target's ATR is not initialized, or
    /// `decide::decide` publishes no row: a stale decision, no event, or cold features. In backtest and live, a working order or open position
    /// of this strategy on the traded instrument suppresses the entry, not the feature row.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A model fails to predict.
    /// - `TargetSetter::at` fails.
    /// - In a dataset run, `Targets::open` fails.
    ///
    /// # Panics
    ///
    /// Panics if `event` is not the decision timer's, or as `decide::decide` does otherwise.
    pub fn decide<S>(&self, strategy: &mut S, event: &TimeEvent) -> anyhow::Result<Option<Decision>>
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
        let Some(row) = decide::decide(strategy, event, &self.features, self.rule, self.traded)
        else {
            return Ok(None);
        };
        match &self.serving {
            Serving::Deciding(deciding) => {
                let decision =
                    deciding.decide(strategy, Timestamp::from(row.ts_event), &row.features)?;
                Ok(decision.filter(|_| {
                    let cache = strategy.cache();
                    let instrument = Some(&self.traded);
                    let owner = Some(&row.strategy_id);

                    // An order blocks from local initialization until terminal, exits included
                    cache.positions_open_count(None, instrument, owner, None, None) == 0
                        && cache.orders_active_local_count(None, instrument, owner, None, None) == 0
                        && cache.orders_inflight_count(None, instrument, owner, None, None) == 0
                        && cache.orders_open_count(None, instrument, owner, None, None) == 0
                }))
            }
            Serving::Dataset { targets, .. } => {
                targets.open(strategy, row)?;
                Ok(None)
            }
        }
    }

    /// Returns whether the target's ATR, if any, is initialized. `decide::decide` checks the
    /// features.
    fn target_initialized(&self) -> bool {
        match &self.serving {
            Serving::Deciding(deciding) => deciding.target.initialized(),
            Serving::Dataset { targets, .. } => targets.initialized(),
        }
    }

    /// Hands `event` to the roller.
    ///
    /// At a roll alert, the root's subscriptions and features move to the post contract
    /// (`Subscriptions::roll`, `FeatureSet::roll`). A trade's time alert belongs to the strategy
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
            self.subscriptions.roll(strategy, &roll);
            self.features.roll(&roll);
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

    /// Hands `event` to the targets of a dataset run, where it fails the run
    /// (`Targets::on_order_denied`). A no-op when deciding.
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

    /// Reports the targets of a dataset run still unsettled at the stop. A no-op when deciding.
    pub fn on_stop(&self) {
        if let Serving::Dataset { targets, .. } = &self.serving {
            targets.on_stop();
        }
    }
}

/// Writes the data hooks of a model strategy.
///
/// Expand it in the strategy's `DataActor` impl as `model_data_hooks!(model);`, where `model` is
/// the field holding its `Rc<ModelHandle>`. The hooks hand the warmup (`ModelHandle::warmup`) the
/// responses and historical data of each request, and the live data. The warmup holds the live
/// data while pending (`strategy::warmup`).
///
/// Temporary: the live hooks, each marked, stand in until Nautilus holds live data for an actor
/// while its requests are pending and hands history in time order across data types:
/// helix-trader#16, helix-trader#46.
#[macro_export]
macro_rules! model_data_hooks {
    // The hooks expand in the strategy's module, which need not import the types they name, so
    // each names them by their full path.
    ($model:ident) => {
        // nautilus-import-ok
        fn on_data_response(
            &mut self,
            response: &::nautilus_common::messages::data::DataResponseEnvelope,
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_data_response(self, response)
        }

        // nautilus-import-ok
        fn on_historical_quotes(
            &mut self,
            quotes: &[::nautilus_model::data::QuoteTick],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_quotes(quotes)
        }

        // nautilus-import-ok
        fn on_historical_trades(
            &mut self,
            trades: &[::nautilus_model::data::TradeTick],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_trades(trades)
        }

        // nautilus-import-ok
        fn on_historical_book_depth(
            &mut self,
            depths: &[::nautilus_model::data::OrderBookDepth],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_book_depth(depths)
        }

        // nautilus-import-ok
        fn on_historical_book_deltas(
            &mut self,
            deltas: &[::nautilus_model::data::OrderBookDelta],
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_book_deltas(deltas)
        }

        // Temporary: live data through the warmup, helix-trader#16 and helix-trader#46.
        // nautilus-import-ok
        fn on_book_depth(
            &mut self,
            depth: &::nautilus_model::data::OrderBookDepth,
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_book_depth(depth)
        }

        // Temporary: live data through the warmup, helix-trader#16 and helix-trader#46.
        // nautilus-import-ok
        fn on_book_deltas(
            &mut self,
            deltas: &::nautilus_model::data::OrderBookDeltas,
        ) -> ::anyhow::Result<()> {
            self.$model.warmup().on_book_deltas(deltas)
        }

        fn on_historical_data(&mut self, data: &dyn ::std::any::Any) -> ::anyhow::Result<()> {
            self.$model.warmup().on_historical_data(self, data)
        }

        // Temporary: live data through the warmup, helix-trader#16 and helix-trader#46.
        // nautilus-import-ok
        fn on_data(&mut self, data: &::nautilus_model::data::CustomData) -> ::anyhow::Result<()> {
            self.$model.warmup().on_data(data)
        }

        // Temporary: live data through the warmup, helix-trader#16 and helix-trader#46.
        // nautilus-import-ok
        fn on_quote(&mut self, quote: &::nautilus_model::data::QuoteTick) -> ::anyhow::Result<()> {
            self.$model.warmup().on_quote(quote)
        }

        // Temporary: live data through the warmup, helix-trader#16 and helix-trader#46.
        // nautilus-import-ok
        fn on_trade(&mut self, trade: &::nautilus_model::data::TradeTick) -> ::anyhow::Result<()> {
            self.$model.warmup().on_trade(trade)
        }

        // Temporary: live data through the warmup, helix-trader#16 and helix-trader#46.
        // nautilus-import-ok
        fn on_bar(&mut self, bar: &::nautilus_model::data::Bar) -> ::anyhow::Result<()> {
            self.$model.warmup().on_bar(bar)
        }
    };
}

impl Deciding {
    /// Decides at `now` on the decision's `features`.
    ///
    /// It lists the stores again, then sets the target once for both sides (`TargetSetter::at`).
    /// With no target there is no decision. Otherwise it decides on the sides' rows
    /// (`DecidingModels::decide`, `SideRow::new`).
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
        let targets = match self.target.at(strategy)? {
            Ok(targets) => targets,
            Err(NoTarget::NoRate {
                balance,
                pnl,
                rates,
            }) => {
                log::warn!(
                    "No decision at {now}: the dataset balance {balance}, which the payoff \
                     features are priced at, has no rate into {pnl}, the currency of the P&L of \
                     {}, at {rates}",
                    self.instrument_id
                );
                return Ok(None);
            }
            Err(_) => return Ok(None),
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
            trade: targets
                .of(taken.side)
                .trade(self.instrument_id, taken.side)
                .expecting(taken.expected_r),
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
        data::{
            Bar, BarType, BookOrder, DataType, NautilusDataType, OrderBookDelta, OrderBookDeltas,
            OrderBookDepth, QuoteTick, TradeTick, stubs::stub_depth10,
        },
        enums::{AggressorSide, BookAction, RecordFlag},
        identifiers::{ClientId, InstrumentId, Symbol, TradeId, TraderId, Venue},
        instruments::{
            Instrument, InstrumentAny,
            stubs::{audusd_sim, btcusd_bybit, default_fx_ccy},
        },
        types::{Money, Price, Quantity},
    };
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        model::{
            config::{Margins, ModelConfig},
            decide::FeatureRow,
            deciding::testing::{
                AUD, GOLD, STRATEGY, at, baseline, blueprint_of, both_sides, config, features,
                gold, meta, meta_baseline, model_set_of, model_store, r, requirements, store,
                with_meta,
            },
            features::{
                NoEvent,
                testing::{RISE, last_mid, mid_over},
            },
            schedule::Retrain,
            targets::{
                BracketSpec, BracketWidth, DEFAULT_ATR, Distance, Side, Target, TargetConfig,
            },
        },
        strategy::{
            AtrArgs, Declared, EmaArgs, IndicatorsConfig, Series,
            conversions::Conversions,
            orders::{Bracket, Exits},
            requirements::{CustomSeries, DataRequirements, TickSeries},
            risk::R,
        },
    };

    fn aud_quotes() -> TickSeries {
        TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(AUD),
        }
    }

    #[derive(Debug)]
    struct Decider {
        core: StrategyCore,
        warmup: Option<Rc<Warmup>>,
    }

    nautilus_strategy!(Decider);

    impl DataActor for Decider {
        fn on_historical_data(&mut self, data: &dyn std::any::Any) -> anyhow::Result<()> {
            self.warmup
                .clone()
                .map_or(Ok(()), |warmup| warmup.on_historical_data(self, data))
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

        fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
            self.warmup
                .as_ref()
                .map_or(Ok(()), |warmup| warmup.on_bar(bar))
        }
    }

    fn strategy_core() -> StrategyCore {
        StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        })
    }

    fn registered(now: Timestamp) -> (Decider, Rc<RefCell<VirtualClock>>) {
        let mut probe = Decider {
            core: strategy_core(),
            warmup: None,
        };
        let clock = register(&mut probe.core, now);
        (probe, clock)
    }

    fn register(core: &mut StrategyCore, now: Timestamp) -> Rc<RefCell<VirtualClock>> {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(UnixNanos::from(now), true);
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        let cache = Rc::new(RefCell::new(Cache::default()));

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

    fn advance(features: &FeatureSet, clock: &Rc<RefCell<VirtualClock>>, to: &str) {
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(to)), true);
        warm(features, UnixNanos::from(at(to)));
    }

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

    fn handle(probe: &Decider, models: &Path, features: &Rc<FeatureSet>) -> Rc<ModelHandle> {
        warm(features, probe.clock().timestamp_ns());
        ModelHandle::new(
            &blueprint_of(features, &config(), requirements()),
            &Mode::Backtest {
                models: models.to_path_buf(),
            },
        )
        .unwrap()
    }

    /// Decides on `handle` as the decision timer firing at the probe's clock, as in a backtest.
    fn decide_now<S>(handle: &ModelHandle, probe: &mut S) -> anyhow::Result<Option<Decision>>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let now = probe.clock().timestamp_ns();
        let event = TimeEvent::new(decide::DECISION_TIMER.into(), UUID4::new(), now, now);
        handle.decide(probe, &event)
    }

    /// Returns the level of `decision`, which is the model's R.
    fn level(decision: Option<Decision>) -> Option<f64> {
        decision.map(|decision| decision.level)
    }

    #[rstest]
    fn cold_features_give_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let cold = ModelHandle::new(
            &blueprint_of(&features(), &config(), requirements()),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();
        started(&cold, &mut probe);

        assert_eq!(decide_now(&cold, &mut probe).unwrap(), None);
    }

    #[rstest]
    fn a_stale_decision_gives_no_trade() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = handle(&probe, models.path(), &features());
        started(&handle, &mut probe);
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at("2026-10-22T12:00:00Z")), true);

        assert_eq!(decide_now(&handle, &mut probe).unwrap(), None);
    }

    #[rstest]
    fn a_cadence_on_features_reading_no_data_of_the_traded_instrument_decides_on_its_quotes() {
        let models = tempfile::tempdir().unwrap();
        let features = last_mid(InstrumentId::from("GBP/USD.SIM")).build().unwrap();
        model_store(models.path(), &features, &config())
            .publish(&model_set_of(
                &features,
                &config(),
                "2026-10-22T00:00:00Z",
                [(Side::Long, 8)],
                None,
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
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();
        started(&handle, &mut probe);

        assert_eq!(level(decide_now(&handle, &mut probe).unwrap()), Some(r(8)));
    }

    #[rstest]
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
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
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

    fn sides_handle(
        probe: &Decider,
        models: &Path,
        config: &ModelConfig,
        sides: (Option<usize>, Option<usize>),
    ) -> Rc<ModelHandle> {
        let mode = Mode::Backtest {
            models: models.to_path_buf(),
        };
        sides_handle_in(&mode, probe, models, config, sides)
    }

    fn sides_handle_in(
        mode: &Mode,
        probe: &Decider,
        models: &Path,
        config: &ModelConfig,
        (long, short): (Option<usize>, Option<usize>),
    ) -> Rc<ModelHandle> {
        let features = features();
        let sides = [(Side::Long, long), (Side::Short, short)]
            .into_iter()
            .filter_map(|(side, tenths)| tenths.map(|tenths| (side, tenths)));
        model_store(models, &features, config)
            .publish(&model_set_of(
                &features,
                config,
                "2026-10-22T00:00:00Z",
                sides,
                None,
            ))
            .unwrap();
        warm(&features, probe.clock().timestamp_ns());
        ModelHandle::new(&blueprint_of(&features, config, requirements()), mode).unwrap()
    }

    #[rstest]
    fn a_side_expected_to_clear_its_margin_is_a_decision_of_its_expected_r() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config(), (Some(6), None));
        started(&handle, &mut probe);

        let decision = decide_now(&handle, &mut probe).unwrap().unwrap();

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

    mod entry;

    const EURUSD: &str = "EUR/USD.SIM";

    fn in_euros() -> ModelConfig {
        let mut config = config();
        config.target.conversions = Conversions::new(vec![InstrumentId::from(EURUSD)]);
        config.dataset.balance = Money::from("10000000 EUR");
        config
    }

    fn holding_eurusd(probe: &Decider, quote: Option<[&str; 2]>) {
        let cache = probe.cache_rc();
        let mut cache = cache.borrow_mut();
        cache
            .add_instrument(InstrumentAny::CurrencyPair(default_fx_ccy(
                Symbol::from("EUR/USD"),
                Some(Venue::from("SIM")),
            )))
            .unwrap();
        if let Some([bid, ask]) = quote {
            let now = probe.clock().timestamp_ns();
            cache
                .add_quote(QuoteTick::new(
                    InstrumentId::from(EURUSD),
                    Price::from(bid),
                    Price::from(ask),
                    Quantity::from(1),
                    Quantity::from(1),
                    now,
                    now,
                ))
                .unwrap();
        }
    }

    #[rstest]
    fn a_dataset_balance_in_another_currency_with_a_rate_cached_decides() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        holding_eurusd(&probe, Some(["1.24990", "1.25010"]));
        let handle = sides_handle(&probe, models.path(), &in_euros(), (Some(6), None));
        started(&handle, &mut probe);

        let decision = decide_now(&handle, &mut probe).unwrap();

        assert_eq!(level(decision), Some(r(6)));
    }

    #[rstest]
    fn a_decision_with_no_rate_of_the_dataset_balance_is_not_made() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        holding_eurusd(&probe, None);
        let handle = sides_handle(&probe, models.path(), &in_euros(), (Some(6), None));
        started(&handle, &mut probe);

        let decision = decide_now(&handle, &mut probe).unwrap();

        assert_eq!(decision, None);
    }

    /// A bracket with a take profit 20 bps and a stop `stop` bps from the price.
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

    #[rstest]
    fn a_bracket_trade_is_on_the_exits_and_the_limit_its_decision_sets() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &bracket_config(10), (Some(10), None));
        handle.start(&mut probe).unwrap();

        let decision = decide_now(&handle, &mut probe).unwrap().unwrap();

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
            .expecting(decision.expected_r)
        );
        assert!(
            (decision.expected_r.as_f64() - 0.5).abs() < 1e-9,
            "{}",
            decision.expected_r
        );
    }

    #[rstest]
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

        let decision = decide_now(&handle, &mut probe).unwrap().unwrap();

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
            .expecting(decision.expected_r)
        );
    }

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

    /// IC's gold is quoted at 4000.00/4000.01, so its mid is on a half tick.
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

    #[rstest]
    fn a_side_expected_to_lose_on_a_half_tick_stop_does_not_clear_a_margin_of_nought() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = on_a_half_tick(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &half_tick_config(), (Some(2), None));

        assert_eq!(decide_now(&handle, &mut probe).unwrap(), None);
    }

    #[rstest]
    fn a_side_expected_to_gain_on_a_half_tick_stop_trades_its_stop_in_the_market() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = on_a_half_tick(at("2026-10-22T12:00:00Z"));
        let config = ModelConfig {
            margins: both_sides().margins,
            ..half_tick_config()
        };
        let handle = sides_handle(&probe, models.path(), &config, (Some(2), Some(8)));

        let decision = decide_now(&handle, &mut probe).unwrap().unwrap();

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
            .expecting(decision.expected_r)
        );
        assert!(
            (decision.expected_r.as_f64() - 0.3).abs() < 1e-12,
            "{}",
            decision.expected_r
        );
    }

    #[rstest]
    fn a_time_only_trade_has_no_exits_its_one_r_its_atr_and_closes_at_its_limit() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = sides_handle(&probe, models.path(), &config(), (Some(6), None));
        probe.warmup = Some(handle.warmup());
        handle.start(&mut probe).unwrap();
        hand_bar(
            &mut probe,
            &hour_bar(
                "2026-10-22T12:00:00Z",
                ["0.70000", "0.75000", "0.50000", "0.70000"],
            ),
        );

        let decision = decide_now(&handle, &mut probe).unwrap().unwrap();

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
            .expecting(decision.expected_r)
        );
        assert!(
            (decision.expected_r.as_f64() - 0.1).abs() < 1e-12,
            "{}",
            decision.expected_r
        );
    }

    /// A take profit 2 ATRs away and a stop 1 ATR away, where the ATR spans 2 hourly bars.
    fn atr_config() -> ModelConfig {
        let mut config = bracket_config(10);
        config.target.spec = Target {
            bracket: Some(BracketSpec {
                take_profit: Distance::Atr(Decimal::from(2)),
                stop: Distance::Atr(Decimal::ONE),
                atr: Some(
                    AtrArgs {
                        period: 2,
                        ..DEFAULT_ATR
                    }
                    .into(),
                ),
            }),
            limit: SignedDuration::from_hours(2),
            atr: None,
        };
        config
    }

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

    /// Returns a bar of ATR 0.00100, which sizes the unit past AUD/USD's maximum quantity.
    fn unit_bar() -> Bar {
        hour_bar(
            "2026-10-22T12:00:00Z",
            ["0.70000", "0.70050", "0.69950", "0.70000"],
        )
    }

    fn hand_bar(probe: &mut Decider, bar: &Bar) {
        DataActor::on_bar(probe, bar).unwrap();
    }

    fn started(handle: &ModelHandle, probe: &mut Decider) {
        probe.warmup = Some(handle.warmup());
        handle.start(probe).unwrap();
        hand_bar(probe, &unit_bar());
    }

    fn hourly_live_handle(config: &ModelConfig, models: &Path) -> Rc<ModelHandle> {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL")],
            warmup: SignedDuration::from_hours(24),
            ..requirements()
        };
        ModelHandle::new(
            &blueprint_of(&features(), config, requirements),
            &live(models.to_path_buf()),
        )
        .unwrap()
    }

    /// The handle decides every minute, so a target's ATR warms on the day's 1,440 minute bars, not
    /// 25 hourly bars, and fits Nautilus' default cache of 10,000 bars. A bracket in basis points
    /// has no ATR: 24 hourly bars.
    #[rstest]
    #[case::a_brackets_atr_on_finer_bars(atr_config(), SignedDuration::from_mins(1), 1_441)]
    #[case::no_atr(bracket_config(10), SignedDuration::from_hours(1), 25)]
    #[case::a_time_only_targets_atr(config(), SignedDuration::from_mins(1), 1_441)]
    fn a_live_cache_holds_the_warmup_of_the_targets_atr_or_the_strategys_bars(
        #[case] config: ModelConfig,
        #[case] decision: SignedDuration,
        #[case] warmup_bars: usize,
    ) {
        let models = tempfile::tempdir().unwrap();
        let config = ModelConfig {
            decision: DecisionRule::Every(decision),
            ..config
        };
        let handle = hourly_live_handle(&config, models.path());

        let cache = handle.warmup().cache_config().unwrap();

        assert_eq!(cache.bar_capacity, 10_000 + warmup_bars);
    }

    #[rstest]
    fn no_row_is_published_while_the_targets_atr_is_cold() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let backtest = Mode::Backtest {
            models: models.path().to_path_buf(),
        };
        let handle = sides_handle_in(
            &backtest,
            &probe,
            models.path(),
            &atr_config(),
            (Some(8), None),
        );
        handle.start(&mut probe).unwrap();
        let rows = published();

        let decision = decide_now(&handle, &mut probe).unwrap();

        assert_eq!(decision, None);
        assert_eq!(rows.borrow().len(), 0);
    }

    #[rstest]
    fn features_reading_no_ticks_warm_on_the_bars_the_strategy_hands_its_warmup() {
        let models = tempfile::tempdir().unwrap();
        let hours = BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL");
        let features = FeatureSet::builder()
            .feature(&[Series::Bars(hours)], HOURLY_CLOSE, &tables())
            .build()
            .unwrap();
        model_store(models.path(), &features, &config())
            .publish(&model_set_of(
                &features,
                &config(),
                "2026-10-22T00:00:00Z",
                [(Side::Long, 8)],
                None,
            ))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements()),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
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

        let cold = decide_now(&handle, &mut probe).unwrap();
        DataActor::on_quote(&mut probe, &quote).unwrap();
        let quoted = decide_now(&handle, &mut probe).unwrap();
        hand_bar(
            &mut probe,
            &hour_bar(
                "2026-10-22T12:00:00Z",
                ["0.69950", "0.70020", "0.69940", "0.70000"],
            ),
        );
        let barred = decide_now(&handle, &mut probe).unwrap();

        assert_eq!(
            (level(cold), level(quoted), level(barred)),
            (None, None, Some(r(8)))
        );
    }

    #[derive(Debug)]
    struct Hooked {
        core: StrategyCore,
        model: Rc<ModelHandle>,
    }

    nautilus_strategy!(Hooked);

    impl DataActor for Hooked {
        crate::model_data_hooks!(model);
    }

    #[rstest]
    fn the_hooks_of_model_data_hooks_hand_the_warmup_a_strategys_ticks() {
        let models = tempfile::tempdir().unwrap();
        let gcz26 = InstrumentId::from("GCZ26.GLBX");
        let features = last_mid(InstrumentId::from(AUD))
            .feature(&[Series::trades(gcz26)], GC_2_TRADES, &tables())
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
            custom: vec![],
            warmup: SignedDuration::from_hours(1),
        };
        let model = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements),
            &live(models.path().to_path_buf()),
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
        model.warmup().complete(&hooked).unwrap();
        let warm = features.initialized();
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

    #[rstest]
    fn the_features_read_of_a_warmup_the_quotes_of_the_series_they_read_alone() {
        const GC_2_MIDS: Declared<EmaArgs> = Declared::new("gc_2_mids");
        let models = tempfile::tempdir().unwrap();
        let gcz26 = InstrumentId::from("GCZ26.GLBX");
        let mids: IndicatorsConfig =
            toml::from_str(r#"gc_2_mids = { period = 2, price_type = "MID" }"#).unwrap();
        let features = FeatureSet::builder()
            .feature(&[Series::quotes(gcz26)], GC_2_MIDS, &mids)
            .build()
            .unwrap();
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                TickSeries {
                    data: NautilusDataType::QuoteTick,
                    instrument_id: gcz26,
                },
                aud_quotes(),
                eurusd_quotes(),
            ],
            custom: vec![],
            warmup: SignedDuration::from_hours(1),
        };
        let model = ModelHandle::new(
            &blueprint_of(&features, &bracket_config(10), requirements),
            &live(models.path().to_path_buf()),
        )
        .unwrap();
        let mut hooked = Hooked {
            core: strategy_core(),
            model: Rc::clone(&model),
        };
        register(&mut hooked.core, at("2026-08-10T14:30:00Z"));
        model.start(&mut hooked).unwrap();
        let quote = |instrument_id: InstrumentId, time: &str| {
            let ts = UnixNanos::from(at(time));
            QuoteTick::new(
                instrument_id,
                Price::from("1.00000"),
                Price::from("1.00010"),
                Quantity::from(1),
                Quantity::from(1),
                ts,
                ts,
            )
        };
        let answer = |hooked: &mut Hooked, instrument_id: InstrumentId| {
            hooked.handle_quotes_response(&QuotesResponse::new(
                UUID4::new(),
                ClientId::from("SIM"),
                instrument_id,
                ["2026-08-10T14:00:00Z", "2026-08-10T14:29:00Z"]
                    .map(|time| quote(instrument_id, time))
                    .to_vec(),
                None,
                None,
                UnixNanos::from(at("2026-08-10T14:30:00Z")),
                None,
            ));
        };
        let others = [
            InstrumentId::from(AUD),
            InstrumentId::from("EURUSD.ICMARKETS"),
        ];

        for instrument_id in others {
            answer(&mut hooked, instrument_id);
            DataActor::on_quote(&mut hooked, &quote(instrument_id, "2026-08-10T14:30:01Z"))
                .unwrap();
        }
        model.warmup().complete(&hooked).unwrap();
        let cold = (
            features.initialized(),
            others.map(|instrument_id| features.last_update(instrument_id)),
        );
        answer(&mut hooked, gcz26);

        assert_eq!(cold, (false, [None, None]));
        assert!(features.initialized());
    }

    /// The last imbalance of the history, sent again live, is dropped once the group ends.
    #[rstest]
    fn the_hooks_of_model_data_hooks_hand_the_features_a_strategys_custom_data() {
        let models = tempfile::tempdir().unwrap();
        let features = crate::model::features::testing::custom_count(&imbalance_series(AUD))
            .build()
            .unwrap();
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![],
            custom: vec![imbalance_series(AUD)],
            warmup: SignedDuration::from_hours(1),
        };
        let model = ModelHandle::new(
            &blueprint_of(&features, &bracket_config(10), requirements),
            &live(models.path().to_path_buf()),
        )
        .unwrap();
        let mut hooked = Hooked {
            core: strategy_core(),
            model: Rc::clone(&model),
        };
        register(&mut hooked.core, at("2026-08-10T14:30:00Z"));
        model.start(&mut hooked).unwrap();

        hooked.handle_data_response(&catalog_answer(
            AUD,
            vec![
                imbalance(AUD, "2026-08-10T14:00:00Z", 0.25),
                imbalance(AUD, "2026-08-10T14:29:00Z", -0.5),
            ],
        ));
        DataActor::on_data(&mut hooked, &imbalance(AUD, "2026-08-10T14:29:00Z", -0.5)).unwrap();
        DataActor::on_data(&mut hooked, &imbalance(AUD, "2026-08-10T14:30:01Z", 0.75)).unwrap();
        let pending = features.values();
        model.warmup().complete(&hooked).unwrap();

        assert_eq!(pending, Some(vec![2.0]));
        assert_eq!(features.values(), Some(vec![3.0]));
    }

    #[rstest]
    fn the_hooks_of_model_data_hooks_hand_the_features_a_strategys_bars() {
        let models = tempfile::tempdir().unwrap();
        let hours = BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL");
        let features = FeatureSet::builder()
            .feature(&[Series::Bars(hours)], HOURLY_CLOSE, &tables())
            .build()
            .unwrap();
        let model = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements()),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();
        let mut hooked = Hooked {
            core: strategy_core(),
            model: Rc::clone(&model),
        };
        register(&mut hooked.core, at("2026-10-22T12:00:00Z"));
        model.start(&mut hooked).unwrap();
        let bar = unit_bar();

        hooked.handle_bar(&bar);
        let through_nautilus = features.values();
        DataActor::on_bar(&mut hooked, &bar).unwrap();

        assert_eq!(through_nautilus, None);
        assert_eq!(features.values(), Some(vec![0.7]));
    }

    #[rstest]
    fn the_hooks_of_model_data_hooks_hand_the_features_a_strategys_live_book() {
        let models = tempfile::tempdir().unwrap();
        let gcz26 = InstrumentId::from("GCZ26.GLBX");
        let features = crate::model::features::testing::book_counts(gcz26)
            .build()
            .unwrap();
        let series = |data| TickSeries {
            data,
            instrument_id: gcz26,
        };
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                aud_quotes(),
                series(NautilusDataType::OrderBookDepth),
                series(NautilusDataType::OrderBookDelta),
            ],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let model = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();
        let mut hooked = Hooked {
            core: strategy_core(),
            model: Rc::clone(&model),
        };
        register(&mut hooked.core, at("2026-08-10T14:30:00Z"));
        model.start(&mut hooked).unwrap();
        let ts = UnixNanos::from(at("2026-08-10T14:30:01Z"));
        let depth = OrderBookDepth {
            instrument_id: gcz26,
            ts_event: ts,
            ts_init: ts,
            ..stub_depth10()
        };
        let delta = OrderBookDelta::new(
            gcz26,
            BookAction::Add,
            BookOrder::new(OrderSide::Buy, Price::from("4000.0"), Quantity::from(1), 1),
            RecordFlag::F_LAST as u8,
            1,
            ts,
            ts,
        );
        let cold = features.values();

        DataActor::on_book_depth(&mut hooked, &depth).unwrap();
        DataActor::on_book_deltas(&mut hooked, &OrderBookDeltas::new(gcz26, vec![delta])).unwrap();

        assert_eq!(cold, None);
        assert_eq!(features.values(), Some(vec![1.0, 1.0]));
        assert_eq!(features.last_update(gcz26), Some(ts));
    }

    const COMPOSITE: &str = "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

    const MINUTES: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";

    fn first_decision(
        mode: fn(PathBuf) -> Mode,
        config: &ModelConfig,
        bar_type: &str,
    ) -> Timestamp {
        let models = tempfile::tempdir().unwrap();
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(bar_type)],
            ..requirements()
        };
        let (mut probe, clock) = registered(at("2026-10-22T11:30:00Z"));
        let handle = ModelHandle::new(
            &blueprint_of(&features(), config, requirements),
            &mode(models.path().to_path_buf()),
        )
        .unwrap();
        handle.start(&mut probe).unwrap();
        let next = clock.borrow().next_time_ns(decide::DECISION_TIMER).unwrap();
        Timestamp::from(next)
    }

    fn backtest(models: PathBuf) -> Mode {
        Mode::Backtest { models }
    }

    fn dataset(_models: PathBuf) -> Mode {
        Mode::Dataset {
            rows: SettledRows::default(),
        }
    }

    /// A cadence on composite bars waits for them, 5 seconds after the hour, in every mode. The
    /// minute bars they are built from are not composite.
    #[rstest]
    #[case::a_backtest_on_composite_bars(backtest, config(), COMPOSITE, "2026-10-22T12:00:05.010Z")]
    #[case::live_on_composite_bars(live, config(), COMPOSITE, "2026-10-22T12:00:05.010Z")]
    #[case::a_dataset_run_on_composite_bars(
        dataset,
        config(),
        COMPOSITE,
        "2026-10-22T12:00:05.010Z"
    )]
    #[case::live_never_retrained_on_composite_bars(
        live,
        ModelConfig {
            retrain: Retrain::Never,
            ..config()
        },
        COMPOSITE,
        "2026-10-22T12:00:05.010Z"
    )]
    #[case::a_backtest_on_minute_bars(backtest, config(), MINUTES, "2026-10-22T12:00:00.010Z")]
    #[case::a_dataset_run_on_minute_bars(dataset, config(), MINUTES, "2026-10-22T12:00:00.010Z")]
    #[case::live_on_minute_bars(live, config(), MINUTES, "2026-10-22T12:00:00.010Z")]
    fn a_cadence_waits_for_composite_bars_alone_in_every_mode(
        #[case] mode: fn(PathBuf) -> Mode,
        #[case] config: ModelConfig,
        #[case] bar_type: &str,
        #[case] first: &str,
    ) {
        assert_eq!(first_decision(mode, &config, bar_type), at(first));
    }

    /// Returns a config with a take profit and a stop 1 ATR of one hourly bar away, and the width
    /// of that bracket.
    fn width_config() -> (ModelConfig, Rc<BracketWidth>) {
        let mut config = bracket_config(10);
        config.target.spec.bracket = Some(BracketSpec {
            take_profit: Distance::Atr(Decimal::ONE),
            stop: Distance::Atr(Decimal::ONE),
            atr: Some(
                AtrArgs {
                    period: 1,
                    ..DEFAULT_ATR
                }
                .into(),
            ),
        });
        let width = config
            .target
            .width(config.decision, &requirements())
            .unwrap();
        (config, Rc::new(width))
    }

    /// Returns the hourly mid bar closing at `close_at`. It opens and closes at 0.70000.
    fn mid_bar(close_at: &str, low: &str, high: &str) -> Bar {
        hour_bar(close_at, ["0.70000", high, low, "0.70000"])
    }

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

    #[rstest]
    fn a_decisions_row_reads_the_width_its_trade_is_set_at() {
        let models = tempfile::tempdir().unwrap();
        let (config, width) = width_config();
        let features = mid_over(InstrumentId::from(AUD), &width).build().unwrap();
        model_store(models.path(), &features, &config)
            .publish(&model_set_of(
                &features,
                &config,
                "2026-10-22T00:00:00Z",
                [(Side::Long, 8)],
                None,
            ))
            .unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T12:00:00Z"));
        warm(&features, probe.clock().timestamp_ns());
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config, requirements()),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        handle.start(&mut probe).unwrap();
        let rows = published();
        let trade = |decision: Option<Decision>| {
            decision.map(|decision| (decision.trade(), decision.expected_r))
        };

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

        hand_bar(
            &mut probe,
            &mid_bar("2026-10-22T12:00:00Z", "0.69900", "0.70100"),
        );
        let (noon, noon_r) = trade(decide_now(&handle, &mut probe).unwrap()).unwrap();
        advance(&features, &clock, "2026-10-22T13:00:00Z");
        hand_bar(
            &mut probe,
            &mid_bar("2026-10-22T13:00:00Z", "0.69800", "0.70200"),
        );
        let (one, one_r) = trade(decide_now(&handle, &mut probe).unwrap()).unwrap();

        let read: Vec<f64> = rows.borrow().iter().map(|row| row.features[0]).collect();
        assert_eq!(read.len(), 2, "{read:?}");
        assert!((read[0] - 350.0).abs() < 1e-9, "{read:?}");
        assert!((read[1] - 175.0).abs() < 1e-9, "{read:?}");
        assert_eq!(
            (noon, one),
            (
                long(200, "0.70200", "0.69800").expecting(noon_r),
                long(400, "0.70400", "0.69600").expecting(one_r)
            )
        );
    }

    #[rstest]
    fn a_dataset_runs_targets_run_on_the_atr_of_the_features_width() {
        let (config, width) = width_config();
        let features = mid_over(InstrumentId::from(AUD), &width).build().unwrap();
        let (mut probe, _) = registered(at("2026-10-22T11:30:00Z"));
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config, requirements()),
            &Mode::Dataset {
                rows: SettledRows::default(),
            },
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        handle.start(&mut probe).unwrap();
        assert!(!width.initialized());

        hand_bar(
            &mut probe,
            &mid_bar("2026-10-22T11:00:00Z", "0.69900", "0.70100"),
        );

        assert!(width.initialized());
    }

    fn live(models: PathBuf) -> Mode {
        Mode::Live { models }
    }

    fn live_handle(
        probe: &mut Decider,
        models: &Path,
        features: &Rc<FeatureSet>,
    ) -> Rc<ModelHandle> {
        let handle = warming_handle(probe, models, features);
        handle.warmup().complete(probe).unwrap();
        handle
    }

    fn warming_handle(
        probe: &mut Decider,
        models: &Path,
        features: &Rc<FeatureSet>,
    ) -> Rc<ModelHandle> {
        warm(features, probe.clock().timestamp_ns());
        let handle = ModelHandle::new(
            &blueprint_of(features, &config(), requirements()),
            &live(models.to_path_buf()),
        )
        .unwrap();
        handle.start(probe).unwrap();
        probe.cache_rc().borrow_mut().add_bar(unit_bar()).unwrap();
        handle
    }

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

    #[rstest]
    fn a_live_node_trades_from_its_first_decision_on_the_newest_model() {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-21T00:00:00Z", 7)).unwrap();
        store.publish(&baseline("2026-10-22T00:00:00Z", 8)).unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = live_handle(&mut probe, models.path(), &features());

        assert_eq!(level(decide_now(&handle, &mut probe).unwrap()), Some(r(8)));
    }

    #[rstest]
    fn a_live_node_does_not_decide_until_its_warmup_ends() {
        let models = tempfile::tempdir().unwrap();
        store(models.path())
            .publish(&baseline("2026-10-22T00:00:00Z", 8))
            .unwrap();
        let (mut probe, _clock) = registered(at("2026-10-22T11:00:00Z"));
        let handle = warming_handle(&mut probe, models.path(), &features());

        let warming = level(decide_now(&handle, &mut probe).unwrap());
        handle.warmup().complete(&probe).unwrap();

        assert_eq!(warming, None);
        assert_eq!(level(decide_now(&handle, &mut probe).unwrap()), Some(r(8)));
    }

    #[rstest]
    fn a_model_published_while_live_is_used_from_the_next_decision() {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T23:30:00Z"));
        let features = features();
        let handle = live_handle(&mut probe, models.path(), &features);
        let before = level(decide_now(&handle, &mut probe).unwrap());

        store.publish(&baseline("2026-10-22T23:00:00Z", 9)).unwrap();
        advance(&features, &clock, "2026-10-23T01:00:00Z");

        assert_eq!(before, Some(r(7)));
        assert_eq!(level(decide_now(&handle, &mut probe).unwrap()), Some(r(9)));
    }

    /// Returns a long model of 0.1R from midnight, with a meta model predicting `r(meta_tenths)`.
    fn meta_handle(models: &Path, config: &ModelConfig, meta_tenths: usize) -> Rc<ModelHandle> {
        let features = features();
        model_store(models, &features, config)
            .publish(&model_set_of(
                &features,
                config,
                "2026-10-22T00:00:00Z",
                [(Side::Long, 6)],
                Some(meta_baseline(&features, config, meta_tenths)),
            ))
            .unwrap();
        warm(&features, UnixNanos::from(at("2026-10-22T12:00:00Z")));
        ModelHandle::new(
            &blueprint_of(&features, config, requirements()),
            &Mode::Backtest {
                models: models.to_path_buf(),
            },
        )
        .unwrap()
    }

    #[rstest]
    fn a_meta_model_from_its_margin_takes_the_trade_of_the_side_picked() {
        let models = tempfile::tempdir().unwrap();
        let handle = meta_handle(models.path(), &with_meta(meta()), 8);
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        started(&handle, &mut probe);

        let decision = decide_now(&handle, &mut probe).unwrap().unwrap();

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
                // The side's model's, its long of 0.1R
                R::new(0.1),
                0.1,
                0.0,
                Some(R::new(0.25))
            )
        );
    }

    #[rstest]
    fn an_unreadable_store_fails_when_the_handle_is_built() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a file");
        std::fs::write(&file, "").unwrap();

        let built = ModelHandle::new(
            &blueprint_of(&features(), &config(), requirements()),
            &Mode::Backtest {
                models: file.clone(),
            },
        );

        let err = format!("{:#}", built.err().unwrap());
        assert!(err.contains(&file.display().to_string()), "{err}");
    }

    /// Nautilus' sizer reads a trade's P&L as a multiple of a distance in price, which an inverse
    /// instrument's is not.
    #[rstest]
    #[case::deciding(backtest)]
    #[case::a_dataset_run(dataset)]
    fn a_handles_start_fails_on_an_inverse_instrument(#[case] mode: fn(PathBuf) -> Mode) {
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
            &mode(models.path().to_path_buf()),
        )
        .unwrap();

        let err = handle.start(&mut probe).unwrap_err();

        assert!(format!("{err:#}").contains("inverse"), "{err:#}");
    }

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

    fn reading_eur(eur: InstrumentId) -> DataRequirements {
        DataRequirements {
            ticks: vec![TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: eur,
            }],
            ..requirements()
        }
    }

    fn handle_on_an_instrument_not_in_the_cache(
        models: &Path,
        config: &ModelConfig,
        eur: InstrumentId,
    ) -> Rc<ModelHandle> {
        let features = last_mid(eur).build().unwrap();
        features.handle_quote(&eur_quote(eur)).unwrap();
        ModelHandle::new(
            &blueprint_of(&features, config, reading_eur(eur)),
            &Mode::Backtest {
                models: models.to_path_buf(),
            },
        )
        .unwrap()
    }

    #[rstest]
    fn no_model_valid_with_the_instrument_missing_from_the_cache_is_an_error() {
        let models = tempfile::tempdir().unwrap();
        let eur = InstrumentId::from("EUR/USD.SIM");
        let mut config = bracket_config(10);
        config.target.instrument_id = eur;
        let handle = handle_on_an_instrument_not_in_the_cache(models.path(), &config, eur);
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));

        let err = decide_now(&handle, &mut probe).unwrap_err();

        assert!(format!("{err:#}").contains("EUR/USD.SIM"), "{err:#}");
    }

    #[rstest]
    fn a_time_only_target_with_the_instrument_missing_from_the_cache_is_an_error() {
        let models = tempfile::tempdir().unwrap();
        let eur = InstrumentId::from("EUR/USD.SIM");
        let mut config = config();
        config.target.instrument_id = eur;
        let features = last_mid(eur).build().unwrap();

        blueprint_of(&features, &config, reading_eur(eur))
            .model_store(models.path())
            .unwrap()
            .publish(&model_set_of(
                &features,
                &config,
                "2026-10-22T00:00:00Z",
                [(Side::Long, 8)],
                None,
            ))
            .unwrap();
        let handle = handle_on_an_instrument_not_in_the_cache(models.path(), &config, eur);
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        probe
            .cache_rc()
            .borrow_mut()
            .add_quote(eur_quote(eur))
            .unwrap();
        probe.warmup = Some(handle.warmup());
        handle.start(&mut probe).unwrap();
        hand_bar(
            &mut probe,
            &Bar {
                bar_type: BarType::from("EUR/USD.SIM-1-HOUR-MID-INTERNAL"),
                ..unit_bar()
            },
        );

        let err = decide_now(&handle, &mut probe).unwrap_err();

        assert!(format!("{err:#}").contains("EUR/USD.SIM"), "{err:#}");
    }

    #[rstest]
    fn a_failed_listing_keeps_the_previous_list() {
        let models = tempfile::tempdir().unwrap();
        let store = store(models.path());
        store.publish(&baseline("2026-10-22T10:00:00Z", 7)).unwrap();
        store.publish(&baseline("2026-10-22T12:00:00Z", 8)).unwrap();
        let (mut probe, clock) = registered(at("2026-10-22T11:00:00Z"));
        let features = features();
        let handle = handle(&probe, models.path(), &features);
        started(&handle, &mut probe);
        let before = level(decide_now(&handle, &mut probe).unwrap());

        store.break_listing();
        advance(&features, &clock, "2026-10-22T12:00:00Z");

        assert_eq!(before, Some(r(7)));
        assert_eq!(level(decide_now(&handle, &mut probe).unwrap()), Some(r(8)));
    }

    /// Records the data commands the probe's actor sends from now, in order.
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
                    DataCommand::Subscribe(SubscribeCommand::Bars(s)) => format!(
                        "subscribe bars [{}] from {:?}",
                        s.bar_types
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
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
                    DataCommand::Subscribe(SubscribeCommand::Data(s)) => format!(
                        "subscribe data {} of {:?} naming {:?} from {:?}",
                        s.data_type.type_name(),
                        s.data_type.identifier(),
                        s.client_id,
                        start_ns(s.params.as_ref())
                    ),
                    other => format!("{other:?}"),
                };
                record.borrow_mut().push(line);
            }),
        );
        sent
    }

    fn assert_sent(sent: &[String], commands: &[&str]) {
        for command in commands {
            assert!(
                sent.iter().any(|line| line == command),
                "{command} not in {sent:#?}"
            );
        }
    }

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

    const GC_2_TRADES: Declared<EmaArgs> = Declared::new("gc_2_trades");
    const HOURLY_CLOSE: Declared<EmaArgs> = Declared::new("hourly_close");

    fn tables() -> IndicatorsConfig {
        toml::from_str(
            "
            gc_2_trades = { period = 2 }
            hourly_close = { period = 1 }
            ",
        )
        .unwrap()
    }

    /// Live on 2026-08-10 at 14:30, a feature on the trades of the root GC reads GCZ26, the
    /// contract standing for it (GC rolls into GCZ26 on 2026-07-29, `rolls/README.md`).
    #[rstest]
    fn a_live_start_warms_a_roots_tick_features_on_its_front_contract() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let features = FeatureSet::builder()
            .feature(
                &[Series::trades(InstrumentId::from("GC.GLBX"))],
                GC_2_TRADES,
                &tables(),
            )
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
            custom: vec![],
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            &blueprint_of(&features, &bracket_config(10), requirements),
            &live(models.path().to_path_buf()),
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
        handle.warmup().complete(&probe).unwrap();

        let sent = sent.take();
        assert_sent(
            &sent,
            &[
                "request trades [GCZ26.GLBX] Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "subscribe trades [GCZ26.GLBX] from Some(2026-08-10T14:30:00Z)",
                "request instrument [GCG27.GLBX]",
                "request quotes [AUD/USD.SIM] Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "subscribe quotes [AUD/USD.SIM] from Some(2026-08-10T14:30:00Z)",
            ],
        );
        assert!(
            !sent.iter().any(|line| line.contains("[GC.GLBX]")),
            "{sent:#?}"
        );
        assert!(!cold);
        assert!(features.initialized());
    }

    fn gc_trade(contract: &str, time: &str, price: &str) -> TradeTick {
        TradeTick {
            instrument_id: InstrumentId::from(contract),
            ..gcz6_trade(time, price)
        }
    }

    /// Decides at a rise of the price of GC's trades.
    fn filtered_on_gc() -> Rc<FeatureSet> {
        let tables: IndicatorsConfig = toml::from_str(
            r#"
            mid = { period = 1, price_type = "MID" }
            rise = { step = 0.0 }
            "#,
        )
        .unwrap();
        last_mid(InstrumentId::from(AUD))
            .event_filter(
                &[Series::trades(InstrumentId::from("GC.GLBX"))],
                RISE,
                &tables,
            )
            .build()
            .unwrap()
    }

    fn gc_trades() -> TickSeries {
        TickSeries {
            data: NautilusDataType::TradeTick,
            instrument_id: InstrumentId::from("GC.GLBX"),
        }
    }

    fn event_at(features: &FeatureSet, previous: &str, now: &str) -> Result<(), NoEvent> {
        features.event(UnixNanos::from(at(previous)), UnixNanos::from(at(now)))
    }

    /// Live on 2026-08-10 at 14:30, the event filter on GC's trades reads GCZ26's history, whose
    /// rise at 14:29 makes the first decision, at 14:30, an event.
    #[rstest]
    fn a_live_start_warms_the_event_filter_from_its_series_history() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let features = filtered_on_gc();
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![aud_quotes(), gc_trades()],
            custom: vec![],
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            &blueprint_of(&features, &bracket_config(10), requirements),
            &live(models.path().to_path_buf()),
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        let sent = sent_commands();

        handle.start(&mut probe).unwrap();
        let cold = event_at(&features, "2026-08-10T14:15:00Z", "2026-08-10T14:30:00Z");
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
        handle.warmup().complete(&probe).unwrap();

        assert!(sent.take().iter().any(|line| line
            == "request trades [GCZ26.GLBX] \
                    Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)"),);
        assert_eq!(cold, Err(NoEvent::Cold));
        assert_eq!(
            event_at(&features, "2026-08-10T14:15:00Z", "2026-08-10T14:30:00Z"),
            Ok(())
        );
    }

    /// GC rolls into GCG27 at 00:00 UTC on 2026-11-26. The roll resets the event filter on GC's
    /// trades, which then warms on GCG27's.
    #[rstest]
    fn a_roll_of_its_root_resets_the_event_filter() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at("2026-11-25T23:00:00Z"));
        let features = filtered_on_gc();
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![aud_quotes(), gc_trades()],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        handle.start(&mut probe).unwrap();
        let trade = |probe: &mut Decider, contract: &str, time: &str, price: &str| {
            DataActor::on_trade(probe, &gc_trade(contract, time, price)).unwrap();
        };

        trade(&mut probe, "GCZ26.GLBX", "2026-11-25T23:10:00Z", "4000.0");
        trade(&mut probe, "GCZ26.GLBX", "2026-11-25T23:20:00Z", "4002.0");
        let before_the_roll = event_at(&features, "2026-11-25T23:00:00Z", "2026-11-26T00:00:00Z");
        run_to(&handle, &mut probe, &clock, "2026-11-26T00:30:00Z");
        let rolled = event_at(&features, "2026-11-25T23:00:00Z", "2026-11-26T00:30:00Z");
        trade(&mut probe, "GCG27.GLBX", "2026-11-26T00:40:00Z", "4100.0");
        trade(&mut probe, "GCG27.GLBX", "2026-11-26T00:50:00Z", "4105.0");

        assert_eq!(before_the_roll, Ok(()));
        assert_eq!(rolled, Err(NoEvent::Cold));
        assert_eq!(
            event_at(&features, "2026-11-26T00:30:00Z", "2026-11-26T01:00:00Z"),
            Ok(())
        );
    }

    fn started_live(
        features: &Rc<FeatureSet>,
        bar_types: Vec<BarType>,
        ticks: Vec<TickSeries>,
    ) -> Vec<String> {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let requirements = DataRequirements {
            bar_types,
            ticks,
            custom: vec![],
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            &blueprint_of(features, &bracket_config(10), requirements),
            &live(models.path().to_path_buf()),
        )
        .unwrap();
        let sent = sent_commands();
        handle.start(&mut probe).unwrap();
        sent.take()
    }

    fn bars_subscribed_at_start(mode: &Mode, bar_types: &[&str]) -> Vec<String> {
        let requirements = DataRequirements {
            bar_types: bar_types
                .iter()
                .map(|&bar_type| BarType::from(bar_type))
                .collect(),
            ..requirements()
        };
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle =
            ModelHandle::new(&blueprint_of(&features(), &config(), requirements), mode).unwrap();
        let sent = sent_commands();

        handle.start(&mut probe).unwrap();

        sent.take()
            .into_iter()
            .filter(|line| line.starts_with("subscribe bars"))
            .collect()
    }

    /// The bars of the target's ATR are subscribed from the start in a backtest and a dataset run,
    /// and from where the warmup ends live.
    #[rstest]
    #[case::a_backtest(backtest)]
    #[case::a_dataset_run(dataset)]
    #[case::live(live)]
    fn a_start_subscribes_the_targets_atr_bars_from_its_start(#[case] mode: fn(PathBuf) -> Mode) {
        let models = tempfile::tempdir().unwrap();

        let subscribed = bars_subscribed_at_start(&mode(models.path().to_path_buf()), &[]);

        assert_eq!(
            subscribed,
            ["subscribe bars [AUD/USD.SIM-1-HOUR-MID-INTERNAL] from Some(2026-10-22T12:00:00Z)"]
        );
    }

    fn custom_subscribed_at_start(mode: &Mode) -> Vec<String> {
        let metadata = nautilus_core::Params::new(
            [
                ("source".to_string(), serde_json::json!("bond_yields")),
                ("symbol".to_string(), serde_json::json!("US10Y")),
            ]
            .into(),
        );
        let us10y = CustomSeries::new("LseRow", "US10Y".to_string(), Some(metadata)).unwrap();
        let requirements = DataRequirements {
            custom: vec![us10y],
            ..requirements()
        };
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle =
            ModelHandle::new(&blueprint_of(&features(), &config(), requirements), mode).unwrap();
        let sent = sent_commands();

        handle.start(&mut probe).unwrap();

        sent.take()
            .into_iter()
            .filter(|line| line.starts_with("subscribe data"))
            .collect()
    }

    /// Live, a custom series' subscription names its data client, so that Nautilus sends it. A
    /// backtest and a dataset run name none, because their engine has no such client.
    #[rstest]
    #[case::live(
        live,
        &["subscribe data LseRow of Some(\"US10Y\") naming Some(\"LSE\") \
           from Some(2026-10-22T12:00:00Z)"]
    )]
    #[case::a_backtest(backtest, &[])]
    #[case::a_dataset_run(dataset, &[])]
    fn a_start_subscribes_a_custom_series_naming_its_data_client_live_alone(
        #[case] mode: fn(PathBuf) -> Mode,
        #[case] commands: &[&str],
    ) {
        let models = tempfile::tempdir().unwrap();

        let subscribed = custom_subscribed_at_start(&mode(models.path().to_path_buf()));

        assert_eq!(subscribed, commands);
    }

    #[rstest]
    fn the_targets_atr_bars_the_strategy_reads_too_are_subscribed_once() {
        let models = tempfile::tempdir().unwrap();

        let subscribed = bars_subscribed_at_start(
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
            &[
                "AUD/USD.SIM-1-HOUR-MID-INTERNAL",
                "AUD/USD.SIM-4-HOUR-MID-INTERNAL",
            ],
        );

        assert_eq!(
            subscribed,
            [
                "subscribe bars [AUD/USD.SIM-1-HOUR-MID-INTERNAL,AUD/USD.SIM-4-HOUR-MID-INTERNAL] \
                 from Some(2026-10-22T12:00:00Z)"
            ]
        );
    }

    fn eurusd_quotes() -> TickSeries {
        TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from("EURUSD.ICMARKETS"),
        }
    }

    /// The start requests a conversion pair's history over the warmup, so that the cache holds a
    /// rate when a trade is sized at the start.
    #[rstest]
    fn a_live_start_warms_up_a_conversion_pairs_quotes_then_subscribes_to_them_from_now() {
        let sent = started_live(&features(), vec![], vec![aud_quotes(), eurusd_quotes()]);

        assert_sent(
            &sent,
            &[
                "request quotes [EURUSD.ICMARKETS] \
                 Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "subscribe quotes [EURUSD.ICMARKETS] from Some(2026-08-10T14:30:00Z)",
            ],
        );
    }

    #[rstest]
    fn a_live_start_requests_custom_data_of_the_traded_instrument_from_the_catalog() {
        let (_probe, _handle, sent, _models) =
            started_live_reading(&features(), vec![aud_quotes()], vec![imbalance_series(AUD)]);

        assert_sent(
            &sent,
            &[
                "request data Imbalance of Some(\"AUD/USD.SIM\") from CATALOG \
               Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
            ],
        );
    }

    #[rstest]
    fn a_live_start_of_bar_features_requests_the_minute_bars_and_the_traded_quotes() {
        let hours = BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL@1-MINUTE-EXTERNAL");
        let features = FeatureSet::builder()
            .feature(&[Series::Bars(hours)], HOURLY_CLOSE, &tables())
            .build()
            .unwrap();

        let sent = started_live(&features, vec![hours], vec![aud_quotes()]);

        assert_sent(
            &sent,
            &[
                "request bars [AUD/USD.SIM-1-MINUTE-MID-EXTERNAL] \
                 Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
                "request quotes [AUD/USD.SIM] \
                 Some(2026-08-10T13:30:00Z)..Some(2026-08-10T14:30:00Z)",
            ],
        );
        assert!(
            !sent.iter().any(|line| line.starts_with("request trades")),
            "{sent:#?}"
        );
    }

    #[nautilus_model::custom_data]
    pub struct Imbalance {
        pub value: f64,
        pub ts_event: UnixNanos,
        pub ts_init: UnixNanos,
    }

    fn imbalance_type(instrument_id: &str) -> DataType {
        DataType::new("Imbalance", None, Some(instrument_id.to_string()))
    }

    fn imbalance(instrument_id: &str, time: &str, value: f64) -> CustomData {
        let ts = UnixNanos::from(at(time));
        CustomData::new(
            std::sync::Arc::new(Imbalance::new(value, ts, ts)),
            imbalance_type(instrument_id),
        )
    }

    /// A subscription to custom data that names no data client sends no command.
    fn imbalance_subscribers(instrument_id: &str) -> usize {
        msgbus::subscriptions_count_any(get_custom_topic(&imbalance_type(instrument_id))).unwrap()
    }

    fn read_imbalance(data: &CustomData) -> f64 {
        data.data
            .as_any()
            .downcast_ref::<Imbalance>()
            .map_or(f64::NAN, |imbalance| imbalance.value)
    }

    fn imbalance_series(identifier: &str) -> CustomSeries {
        CustomSeries::new("Imbalance", identifier.to_string(), None).unwrap()
    }

    fn last_imbalance(identifier: &str) -> Rc<FeatureSet> {
        FeatureSet::builder()
            .custom(&imbalance_series(identifier), "imbalance", read_imbalance)
            .build()
            .unwrap()
    }

    fn started_live_reading(
        features: &Rc<FeatureSet>,
        ticks: Vec<TickSeries>,
        custom: Vec<CustomSeries>,
    ) -> (Decider, Rc<ModelHandle>, Vec<String>, tempfile::TempDir) {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, _clock) = registered(at("2026-08-10T14:30:00Z"));
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks,
            custom,
            warmup: SignedDuration::from_hours(1),
        };
        let handle = ModelHandle::new(
            &blueprint_of(features, &bracket_config(10), requirements),
            &live(models.path().to_path_buf()),
        )
        .unwrap();
        probe.warmup = Some(handle.warmup());
        let sent = sent_commands();
        handle.start(&mut probe).unwrap();
        let sent = sent.take();
        (probe, handle, sent, models)
    }

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

    /// Live, a custom series that no data client serves is requested from the client `CATALOG`,
    /// and Nautilus answers from the catalog alone.
    #[rstest]
    fn a_live_start_warms_a_custom_feature_from_the_catalog() {
        let features = last_imbalance(AUD);
        let (mut probe, _handle, sent, _models) =
            started_live_reading(&features, vec![], vec![imbalance_series(AUD)]);
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

    #[rstest]
    fn two_features_share_custom_history_without_losing_or_repeating_a_point() {
        let series = imbalance_series(AUD);
        let features = crate::model::features::testing::custom_count(&series)
            .custom(&series, "imbalance", read_imbalance)
            .build()
            .unwrap();
        let (mut probe, _handle, _sent, _models) =
            started_live_reading(&features, vec![], vec![series]);

        probe.handle_data_response(&catalog_answer(
            AUD,
            vec![
                imbalance(AUD, "2026-08-10T14:00:00Z", 0.25),
                imbalance(AUD, "2026-08-10T14:29:00Z", -0.5),
            ],
        ));

        assert_eq!(features.values(), Some(vec![2.0, -0.5]));
    }

    /// A feature on the imbalances under the root GC's id reads GCZ26's, the contract standing for
    /// it (GC rolls into GCZ26 on 2026-07-29, `rolls/README.md`).
    #[rstest]
    fn a_live_start_warms_a_custom_feature_on_a_root_on_its_front_contract() {
        let features = last_imbalance("GC.GLBX");
        let (mut probe, _handle, sent, _models) = started_live_reading(
            &features,
            vec![aud_quotes()],
            vec![imbalance_series("GC.GLBX")],
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
                "subscribe quotes [AUD/USD.SIM] from Some(2026-08-10T14:30:00Z)",
            ],
        );
        assert!(
            !sent
                .iter()
                .any(|line| line.contains("\"GC.GLBX\"") || line.starts_with("request instrument")),
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

    /// GC rolls into GCG27 at 00:00 UTC on 2026-11-26, and the roll's alert moves the feature to
    /// GCG27's latest imbalance in the cache.
    #[rstest]
    fn a_feature_on_a_roots_custom_series_reads_each_contract_in_turn_from_the_cache() {
        let models = tempfile::tempdir().unwrap();
        let (mut probe, clock) = registered(at("2026-11-25T23:00:00Z"));
        let series = imbalance_series("GC.GLBX");
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![aud_quotes()],
            custom: vec![series.clone()],
            warmup: SignedDuration::ZERO,
        };
        let features = FeatureSet::builder()
            .custom(&series, "imbalance", read_imbalance)
            .build()
            .unwrap();
        let handle = ModelHandle::new(
            &blueprint_of(&features, &config(), requirements),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();
        handle.start(&mut probe).unwrap();
        let read = |probe: &Decider, data: CustomData| {
            probe.cache_rc().borrow_mut().add_custom_data(data);
            features.values()
        };

        let pre = read(
            &probe,
            imbalance("GCZ26.GLBX", "2026-11-25T23:10:00Z", 0.25),
        );
        let post_before_the_roll = read(
            &probe,
            imbalance("GCG27.GLBX", "2026-11-25T23:20:00Z", 0.75),
        );
        let subscribed = (
            imbalance_subscribers("GCZ26.GLBX"),
            imbalance_subscribers("GCG27.GLBX"),
        );
        run_to(&handle, &mut probe, &clock, "2026-11-26T00:30:00Z");
        let rolled = features.values();
        let pre_after_the_roll = read(
            &probe,
            imbalance("GCZ26.GLBX", "2026-11-26T00:40:00Z", -0.25),
        );

        assert_eq!(subscribed, (1, 0));
        assert_eq!(pre, Some(vec![0.25]));
        assert_eq!(post_before_the_roll, Some(vec![0.25]));
        assert_eq!(rolled, Some(vec![0.75]));
        assert_eq!(pre_after_the_roll, Some(vec![0.75]));
        assert_eq!(
            (
                imbalance_subscribers("GCZ26.GLBX"),
                imbalance_subscribers("GCG27.GLBX")
            ),
            (0, 1)
        );
    }

    fn yields(symbol: &str) -> DataType {
        DataType::new("Yield", None, Some(symbol.to_string()))
    }

    #[rstest]
    fn a_backtests_start_subscribes_the_custom_series_of_its_requirements() {
        let models = tempfile::tempdir().unwrap();
        let series = |symbol: &str| CustomSeries::new("Yield", symbol.to_string(), None).unwrap();
        let requirements = DataRequirements {
            custom: vec![series("US10Y"), series("US02Y")],
            ..requirements()
        };
        let (mut probe, _clock) = registered(at("2026-10-22T12:00:00Z"));
        let handle = ModelHandle::new(
            &blueprint_of(&features(), &config(), requirements),
            &Mode::Backtest {
                models: models.path().to_path_buf(),
            },
        )
        .unwrap();

        handle.start(&mut probe).unwrap();

        let subscribers =
            |symbol| msgbus::subscriptions_count_any(get_custom_topic(&yields(symbol))).unwrap();
        assert_eq!([subscribers("US10Y"), subscribers("US02Y")], [1, 1]);
    }
}
