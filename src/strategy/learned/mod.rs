//! Learned, an example strategy trading on a learned model: at each
//! decision of its model section, its model handle (`model::handle`)
//! predicts on its features, the averages of its bars (`features.rs`),
//! and a probability past a threshold is a trade, a market order of the
//! targets' instrument and size on its side, closed `hold` after it is
//! sent (`close_position`, on a time alert). Trades may overlap: on a
//! hedging venue each is a position of its own, on a netting venue they
//! add into one, which the first close then closes. A restart does not set
//! again the alerts of the trades still open.
//!
//! In a dataset run (`Mode::Dataset`, through `training`) it places no
//! trade of its own: the handle opens each decision's targets and is
//! handed the order, position and time events.

mod config;
mod features;

pub use config::LearnedConfig;

use std::{path::Path, rc::Rc};

use anyhow::Context;
use jiff::Timestamp;
use nautilus_common::{
    actor::{DataActor, indicators::ActorIndicator},
    messages::data::DataResponseEnvelope,
    timer::TimeEvent,
};
use nautilus_core::UUID4;
use nautilus_model::{
    data::{Bar, QuoteTick},
    events::{OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::ClientOrderId,
    instruments::Instrument,
    orders::Order,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyCore},
};

use crate::{
    model::{
        dataset::Dataset,
        decide,
        features::FeatureSet,
        handle::{Mode, ModelHandle, Trade},
        train::Training,
        walk::WalkForward,
    },
    rolls::ContinuousBars,
    strategy::warmup,
};

/// The prefix of a trade's time alert, followed by its order's client
/// order id.
const HOLD_ALERT: &str = "LEARNED-HOLD:";

#[derive(Debug)]
pub struct Learned {
    core: StrategyCore,
    config: LearnedConfig,
    features: Rc<FeatureSet>,
    model: Rc<ModelHandle>,
    /// The request group of the warmup of the last start.
    warmup: UUID4,
}

impl Learned {
    /// The strategy, its model handle in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn new(config: LearnedConfig, mode: Mode) -> anyhow::Result<Self> {
        config.validate()?;
        let features = features::features(&config.bars.bar_types(), config.bars.warmup)?;
        let model = ModelHandle::new(
            strategy_id(&config)?,
            Rc::clone(&features),
            &config.model,
            mode,
        )?;
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            config,
            features,
            model,
            warmup: UUID4::new(),
        })
    }

    /// Sends `trade`: a market order of its size on its side, and the time
    /// alert closing it `hold` later. The size is rounded to the
    /// instrument's size precision.
    fn enter(&mut self, trade: Trade) -> anyhow::Result<()> {
        let instrument_id = self.config.model.targets.instrument_id;
        let instrument = self
            .cache()
            .instrument(&instrument_id)
            .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} not in cache"))?;
        let size = trade.quantity.as_decimal();
        let quantity = instrument
            .try_make_qty_from_decimal(size, None)
            // In the message, not as context: Nautilus logs only the top
            // error of the chain.
            .map_err(|e| anyhow::anyhow!("ordering {size} of {instrument_id}: {e}"))?;
        let order = self.order().market(
            instrument_id,
            trade.side,
            quantity,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let id = order.client_order_id();
        self.submit_order(order, None, None, None)?;
        let at = Timestamp::from(self.clock().timestamp_ns()) + self.config.hold;
        self.clock().set_time_alert(&hold_alert(id), at, None, None)
    }

    /// Closes the position the order `entry` went into, if it is open.
    fn exit(&mut self, entry: ClientOrderId) -> anyhow::Result<()> {
        let position = self.cache().position_for_order(&entry);
        match position {
            Some(position) if position.is_open() => {
                self.close_position(&position, None, None, None, None, None, None)
            }
            _ => Ok(()),
        }
    }
}

/// The training of the model of the strategy `config` describes, on its
/// dataset in the catalog at `catalog` and its models in the store under
/// `models`; its replay runs the strategy in `Mode::Dataset`.
///
/// # Errors
///
/// Returns an error if the config is invalid, or as `Training::new` does.
pub fn training(config: &LearnedConfig, catalog: &Path, models: &Path) -> anyhow::Result<Training> {
    config.validate()?;
    let features = features::features(&config.bars.bar_types(), config.bars.warmup)?;
    Training::new(
        catalog,
        models,
        strategy_id(config)?,
        &features,
        config.model.clone(),
    )
}

/// The backtests of the model of the strategy `config` describes, on its
/// dataset in the catalog at `catalog` and its models in the store under
/// `models`: their retrains and their scores.
///
/// # Errors
///
/// Returns an error if the config is invalid, or as `WalkForward::new`
/// does.
pub fn walk_forward(
    config: &LearnedConfig,
    catalog: &Path,
    models: &Path,
) -> anyhow::Result<WalkForward> {
    config.validate()?;
    let features = features::features(&config.bars.bar_types(), config.bars.warmup)?;
    WalkForward::new(
        catalog,
        models,
        strategy_id(config)?,
        &features,
        config.model.clone(),
    )
}

/// The dataset of the strategy `config` describes, in the catalog at
/// `catalog`; its replay runs the strategy in `Mode::Dataset`.
///
/// # Errors
///
/// Returns an error if the config is invalid, or as `Dataset::new` does.
pub fn dataset(config: &LearnedConfig, catalog: &Path) -> anyhow::Result<Dataset> {
    config.validate()?;
    let features = features::features(&config.bars.bar_types(), config.bars.warmup)?;
    Dataset::new(
        catalog,
        strategy_id(config)?,
        &features,
        config.model.decision,
        config.model.targets.clone(),
    )
}

/// The strategy's id, from its base.
fn strategy_id(config: &LearnedConfig) -> anyhow::Result<nautilus_model::identifiers::StrategyId> {
    config
        .base
        .strategy_id
        .context("the strategy has no id to key its models")
}

/// The name of the time alert closing the trade of `entry`.
fn hold_alert(entry: ClientOrderId) -> String {
    format!("{HOLD_ALERT}{entry}")
}

nautilus_strategy!(Learned, {
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

impl DataActor for Learned {
    /// Starts the warmup, then the model handle: the features and the
    /// decisions.
    fn on_start(&mut self) -> anyhow::Result<()> {
        let (bar_types, warmup) = (self.config.bars.bar_types(), self.config.bars.warmup);
        self.warmup = UUID4::new();
        warmup::start(
            self,
            &bar_types,
            warmup,
            &ContinuousBars::default(),
            self.warmup,
        )?;
        Rc::clone(&self.model).start(self, &bar_types)
    }

    /// Logs the end of the warmup; Nautilus logs a failed one as an error.
    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if warmup::finished(self.warmup, response)? {
            log::info!("Warmup complete");
        }
        Ok(())
    }

    /// The warmup's bars reach only the cache: they are handed to the
    /// features here.
    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, quotes) {
            self.features.handle_bar(&bar)?;
        }
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, bars) {
            self.features.handle_bar(&bar)?;
        }
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            return match Rc::clone(&self.model).decide(self)? {
                Some(trade) => self.enter(trade),
                None => Ok(()),
            };
        }
        match event.name.strip_prefix(HOLD_ALERT) {
            Some(entry) => self.exit(ClientOrderId::new_checked(entry)?),
            None => Rc::clone(&self.model).on_time_event(self, event),
        }
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.model.on_stop();
        Ok(())
    }
}
