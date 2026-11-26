//! Model cross, an example strategy that trades on a learned model at the crosses of two averages.
//!
//! It is `model_ema` with an event filter (`model::features::EventFilter`): its features are the
//! same averages of `[indicators.fast]` and `[indicators.slow]` (`features.rs`), and it decides
//! only at the grid ticks after a cross of the averages of `[indicators.cross]` on the traded
//! instrument's bars (`cross.rs`). At such a decision of its `[model]` section, the model handle
//! (`model::handle`) predicts each side from the features. A side expected to clear its margin is
//! a decision (`handle::Decision`). At any other grid tick there is no decision: no dataset row,
//! no score and no trade.
//!
//! The strategy sends the trade of each decision at the config's stake through
//! `strategy::orders`:
//! - The trade uses the target the model learned.
//! - It closes at the target's limit, counted from the send.
//! - It is sized at `[trade] risk` of equity.
//!
//! The handle skips executable entries while a trade is outstanding. It decides only once its
//! features, its event filter and the target's ATR are initialized. They initialize from the
//! replay in a backtest or dataset run, and from the warmup request live. The data hooks are the
//! handle's (`model_data_hooks!`).
//!
//! A `[model.meta]` table adds a meta model, which takes or leaves each trade (`model::meta`).
//!
//! In a dataset run (`Mode::Dataset`, see `model::ModelStrategy`) the strategy places no trade of
//! its own. The handle opens the target of each decision through the same module and is handed
//! the order and position events.

mod cross;
mod features;

use nautilus_common::{
    actor::DataActor,
    config::{ConfigError, ConfigResult},
    timer::TimeEvent,
};
use nautilus_model::{
    events::{OrderCanceled, OrderDenied, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use serde::Deserialize;
use std::rc::Rc;

use crate::{
    model::{
        self, ModelPart, ModelStrategy,
        blueprint::ModelBlueprint,
        decide,
        features::FeatureSet,
        handle::{Mode, ModelHandle},
    },
    strategy::{
        Declaration, Part, Series, StandardConfig, Visitor, conversions::Rates, orders,
        risk::Stake, warmup::Warmup,
    },
};

/// The config of the `model_cross` strategy.
///
/// Its part, `ModelCrossParams`, has no fields of its own.
pub type ModelCrossConfig = StandardConfig<ModelCrossParams>;

/// The part of the `model_cross` config.
///
/// It trades the one instrument of `[trade]` on a model. Its features are a fast and a slow
/// `ExponentialMovingAverage` of every bars series of `[data]`, and its event filter a cross of the
/// `SimpleMovingAverage`s of `[indicators.cross]` on the one bars series of the traded instrument.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCrossParams {}

impl Part for ModelCrossParams {
    const INSTRUMENTS: usize = 1;
    const INDICATORS: &'static [&'static dyn Declaration] = &[&features::FAST, &features::SLOW, &features::CROSS];
    const TAKES_MODEL: bool = true;

    /// Sets only the strategy id, which Nautilus tags orders with.
    ///
    /// Leaves `oms_type` unset. Nautilus keeps positions as the venue's execution client does, in
    /// a backtest as in live.
    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("MODEL_CROSS-001")),
            ..Default::default()
        }
    }

    /// Returns what the features, the event filter and the target's ATR read (`model::reads`).
    fn reads(config: &ModelCrossConfig) -> ConfigResult<Vec<Series>> {
        model::reads(config)
    }
}

impl ModelPart for ModelCrossParams {
    /// Builds the averages of each bars series and the cross, warmed over the warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if `data.series` lists no bars series, or not exactly one of the traded
    /// instrument. The error is reported under that field.
    fn features(config: &ModelCrossConfig) -> ConfigResult<Rc<FeatureSet>> {
        features::features(config)
            .map_err(|e| ConfigError::invalid_value("data.series", format!("{e:#}")))
    }
}

#[derive(Debug)]
pub struct ModelCross {
    core: StrategyCore,
    model: Rc<ModelHandle>,
    stake: Stake,
    rates: Rates,
}

impl ModelStrategy for ModelCross {
    type Params = ModelCrossParams;

    /// Creates the strategy, which decides on the model of `blueprint`.
    ///
    /// Live, it requests its warmup at start.
    ///
    /// # Errors
    ///
    /// Returns an error if `ModelHandle::new` fails, or if Nautilus refuses the base
    /// (`StrategyCore::new_checked`).
    fn with_blueprint(
        config: ModelCrossConfig,
        blueprint: ModelBlueprint,
        mode: Mode,
    ) -> anyhow::Result<Self> {
        let model = ModelHandle::new(&blueprint, &mode)?;
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            model,
            stake: config.stake(),
            rates: config.rates(),
        })
    }

    /// Returns the warmup of the model handle.
    fn warmup(&self) -> Rc<Warmup> {
        self.model.warmup()
    }
}

/// Hands the strategy to `visitor` as one trading on a model (`strategy::Visitor`).
pub fn visit<V: Visitor>(visitor: V) -> V::Output {
    visitor.model::<ModelCross>()
}

nautilus_strategy!(ModelCross, {
    /// The instruments the config claims, which the live node registers.
    fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
        self.config().external_order_instrument_ids.clone()
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.model.on_order_canceled(self, event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        Rc::clone(&self.model).on_order_denied(self, &event);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.model.on_order_rejected(&event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        Rc::clone(&self.model).on_position_closed(self, &event);
    }
});

impl DataActor for ModelCross {
    /// Starts the model handle. The handle starts the warmup and, live, subscribes.
    fn on_start(&mut self) -> anyhow::Result<()> {
        orders::arm_open_trades(self)?;
        Rc::clone(&self.model).start(self)
    }

    crate::model_data_hooks!(model);

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            return match Rc::clone(&self.model).decide(self, event)? {
                Some(decision) => decision.trade().send(self, self.stake, self.rates),
                None => Ok(()),
            };
        }

        if !orders::on_time_event(self, event)? {
            Rc::clone(&self.model).on_time_event(self, event)?;
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.model.on_stop();
        Ok(())
    }
}


#[cfg(all(test, feature = "strategy-tests"))]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn the_example_config_loads() {
        ModelCrossConfig::load(include_str!("../../../config/model_cross.toml")).unwrap();
    }

    /// Without `[base]`, only the strategy id is set.
    #[rstest]
    fn defaults_its_base_to_its_strategy_id_alone() {
        let base = ModelCrossConfig::load(include_str!("../../../config/model_cross.toml"))
            .unwrap()
            .base;
        assert_eq!(base.strategy_id, Some(StrategyId::from("MODEL_CROSS-001")));
        assert_eq!(base.order_id_tag, None);
        assert_eq!(base.oms_type, None);
    }
}
