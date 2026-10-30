//! Model EMA, an example strategy trading on a learned model: at each
//! decision of its model section, its model handle (`model::handle`)
//! predicts each side on its features, the averages of its bars
//! (`features.rs`), and a side expected to clear its margin is a trade,
//! which it sends as given (`Trade::send`): the target its model was
//! labelled on, as a dataset run sets it at the decision on that side, a
//! bracket closed at the barrier's limit or a market entry closed at the
//! horizon, the time alert handed to the handle. Every target closes
//! before the next decision (`ModelConfig::validate`), so trades do not
//! overlap, on a hedging venue as on a netting one. It decides only once
//! its indicators are initialized: warm from the replay in a backtest or
//! a dataset run, from its warmup request live. A restart does not set
//! again the alerts of the trades still open. Nautilus' IB execution
//! client denies the reduce-only orders a bracket's legs and a time close
//! are: it cannot trade live on IB (`live/ib/README.md`).
//!
//! A `[model.meta]` table in its config adds a meta model, which takes
//! or leaves each trade of the model (`model::meta`), and may size it.
//!
//! In a dataset run (`Mode::Dataset`, through `training`) it places no
//! trade of its own: the handle opens each decision's targets and is
//! handed the order, position and time events.

mod config;
mod features;

pub use config::ModelEmaConfig;

use nautilus_common::{
    actor::{DataActor, DataActorNative},
    messages::data::DataResponseEnvelope,
    timer::TimeEvent,
};
use nautilus_model::{
    data::{Bar, QuoteTick, TradeTick},
    events::{OrderDenied, OrderRejected, PositionClosed},
};
use nautilus_trading::{nautilus_strategy, strategy::StrategyCore};
use std::{path::Path, rc::Rc};

use crate::{
    model::{
        dataset::{Dataset, DatasetVenue},
        decide,
        features::FeatureSet,
        handle::{Mode, ModelHandle},
        train::Training,
        walk::WalkForward,
    },
    strategy::warmup::Warmup,
};

#[derive(Debug)]
pub struct ModelEma {
    core: StrategyCore,
    /// Its model handle, which owns its warmup.
    model: Rc<ModelHandle>,
}

impl ModelEma {
    /// The strategy, its model handle in `mode`: live, its warmup
    /// (`DataConfig::warmup`) is requested at start, which hydrates its
    /// indicators; otherwise they warm on the data replayed.
    ///
    /// # Errors
    ///
    /// Returns an error as `ModelHandle::new` does, or if Nautilus refuses
    /// the base (`StrategyCore::new_checked`).
    pub fn new(config: ModelEmaConfig, mode: Mode) -> anyhow::Result<Self> {
        let features = feature_set(&config)?;
        let model = ModelHandle::new(
            config.strategy_id(),
            features,
            &config.model(),
            &config.data_requirements(),
            mode,
        )?;
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            model,
        })
    }

    /// Its warmup, its model handle's: a live node sizes its cache from
    /// it.
    #[must_use]
    pub fn warmup(&self) -> Rc<Warmup> {
        self.model.warmup()
    }
}

/// The training of the model of the strategy `config` describes, on its
/// dataset, run on `venue`, in the catalog at `catalog` and its models
/// in the store under `models`; its replay runs the strategy in
/// `Mode::Dataset`.
///
/// # Errors
///
/// Returns an error as `Training::new` does.
pub fn training(
    config: &ModelEmaConfig,
    catalog: &Path,
    models: &Path,
    venue: &DatasetVenue,
) -> anyhow::Result<Training> {
    let features = feature_set(config)?;
    Training::new(
        catalog,
        models,
        config.strategy_id(),
        &features,
        config.model(),
        &config.data_requirements(),
        venue,
    )
}

/// The backtests of the model of the strategy `config` describes, on its
/// dataset, run on `venue`, in the catalog at `catalog` and its models
/// in the store under `models`: their retrains and their scores.
///
/// # Errors
///
/// Returns an error as `WalkForward::new` does.
pub fn walk_forward(
    config: &ModelEmaConfig,
    catalog: &Path,
    models: &Path,
    venue: &DatasetVenue,
) -> anyhow::Result<WalkForward> {
    let features = feature_set(config)?;
    WalkForward::new(
        catalog,
        models,
        config.strategy_id(),
        &features,
        config.model(),
        &config.data_requirements(),
        venue,
    )
}

/// The dataset of the strategy `config` describes, run on `venue`, in
/// the catalog at `catalog`; its replay runs the strategy in
/// `Mode::Dataset`.
///
/// # Errors
///
/// Returns an error as `Dataset::new` does.
pub fn dataset(
    config: &ModelEmaConfig,
    catalog: &Path,
    venue: &DatasetVenue,
) -> anyhow::Result<Dataset> {
    let features = feature_set(config)?;
    let model = config.model();
    Dataset::new(
        catalog,
        config.strategy_id(),
        &features,
        model.decision,
        model.targets,
        &config.data_requirements(),
        venue,
    )
}

/// The features of the strategy `config` describes: the averages of each
/// of its bars series, warmed over its warmup.
fn feature_set(config: &ModelEmaConfig) -> anyhow::Result<Rc<FeatureSet>> {
    let bar_types: Vec<_> = config.data.bar_types().collect();
    features::features(&bar_types, config.data.warmup)
}

nautilus_strategy!(ModelEma, {
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

impl DataActor for ModelEma {
    /// Starts the model handle, which starts the warmup: live, it requests
    /// the warmup of the bars and of the barrier's ATR, then subscribes to
    /// them; then the features, registered as its Nautilus indicator, and
    /// the decisions.
    fn on_start(&mut self) -> anyhow::Result<()> {
        Rc::clone(&self.model).start(self)
    }

    /// Logs the end of the warmup; Nautilus logs a failed one as an error.
    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        self.model.warmup().on_data_response(response)
    }

    /// The warmup's bars reach only the cache: the warmup hands them to
    /// the features and the barrier's ATR.
    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        self.model.warmup().on_historical(self, quotes)
    }

    fn on_historical_trades(&mut self, trades: &[TradeTick]) -> anyhow::Result<()> {
        self.model.warmup().on_historical(self, trades)
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        self.model.warmup().on_historical(self, bars)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            // It decides only once its indicators are initialized: the
            // features, and in a dataset run the barrier's ATR.
            if !self.core().indicators_initialized()? {
                return Ok(());
            }
            return match Rc::clone(&self.model).decide(self)? {
                Some(trade) => trade.send(self),
                None => Ok(()),
            };
        }
        Rc::clone(&self.model).on_time_event(self, event)
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.model.on_stop();
        Ok(())
    }
}
