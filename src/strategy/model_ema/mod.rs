//! Model EMA, an example strategy that trades on a learned model.
//!
//! At each decision of its `[model]` section, the model handle (`model::handle`) predicts each
//! side from the averages of `[indicators.fast]` and `[indicators.slow]` (`features.rs`). A side
//! expected to clear its margin is a decision (`handle::Decision`).
//!
//! The strategy sends the trade of each decision at the config's stake through
//! `strategy::orders`:
//! - The trade uses the target the model learned.
//! - It closes at the target's limit, counted from the send.
//! - It is sized at `[trade] risk` of equity.
//!
//! The handle skips executable entries while a trade is outstanding. It decides only once its
//! features and the target's ATR are initialized. They initialize from the replay in a backtest
//! or dataset run, and from the warmup request live. The data hooks are the handle's
//! (`model_data_hooks!`).
//!
//! A `[model.meta]` table adds a meta model, which takes or leaves each trade (`model::meta`).
//!
//! In a dataset run (`Mode::Dataset`, see `model::ModelStrategy`) the strategy places no trade of
//! its own. The handle opens the target of each decision through the same module and is handed
//! the order and position events.

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

/// The config of the `model_ema` strategy.
///
/// Its part, `ModelEmaParams`, has no fields of its own.
pub type ModelEmaConfig = StandardConfig<ModelEmaParams>;

/// The part of the `model_ema` config.
///
/// It trades the one instrument of `[trade]` on a model. Its features are a fast and a slow
/// `ExponentialMovingAverage` of every bars series of `[data]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEmaParams {}

impl Part for ModelEmaParams {
    const INSTRUMENTS: usize = 1;
    const INDICATORS: &'static [&'static dyn Declaration] = &[&features::FAST, &features::SLOW];
    const TAKES_MODEL: bool = true;

    /// Sets only the strategy id, which Nautilus tags orders with.
    ///
    /// Leaves `oms_type` unset. Nautilus keeps positions as the venue's execution client does, in
    /// a backtest as in live.
    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("MODEL_EMA-001")),
            ..Default::default()
        }
    }

    /// Returns what the features and the target's ATR read (`model::reads`).
    fn reads(config: &ModelEmaConfig) -> ConfigResult<Vec<Series>> {
        model::reads(config)
    }
}

impl ModelPart for ModelEmaParams {
    /// Builds the averages of each bars series, warmed over the warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if `data.series` lists no bars series. The error is reported under that
    /// field.
    fn features(config: &ModelEmaConfig) -> ConfigResult<Rc<FeatureSet>> {
        features::features(config)
            .map_err(|e| ConfigError::invalid_value("data.series", format!("{e:#}")))
    }
}

#[derive(Debug)]
pub struct ModelEma {
    core: StrategyCore,
    model: Rc<ModelHandle>,
    stake: Stake,
    rates: Rates,
}

impl ModelStrategy for ModelEma {
    type Params = ModelEmaParams;

    /// Creates the strategy, which decides on the model of `blueprint`.
    ///
    /// Live, it requests its warmup at start.
    ///
    /// # Errors
    ///
    /// Returns an error if `ModelHandle::new` fails, or if Nautilus refuses the base
    /// (`StrategyCore::new_checked`).
    fn with_blueprint(
        config: ModelEmaConfig,
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
    visitor.model::<ModelEma>()
}

nautilus_strategy!(ModelEma, {
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

impl DataActor for ModelEma {
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
    use std::{cell::RefCell, path::Path};
    use rstest::rstest;

    use jiff::SignedDuration;
    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        timer::TimeEventCallback,
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        enums::{OmsType, OrderSide},
        events::OrderEventAny,
        identifiers::{
            AccountId, ClientOrderId, InstrumentId, PositionId, TradeId, TraderId, VenueOrderId,
        },
        instruments::{InstrumentAny, stubs::audusd_sim},
        orders::{Order, stubs::TestOrderEventStubs},
        position::Position,
        types::{Money, Price},
    };
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::strategy::Strategy;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        model::{decide::DecisionRule, learner::LearnerConfig},
        strategy::orders::{Exits, OrderIds, Trade},
    };

    /// AUD/USD on a time-only target of 30 minutes, with a unit of the ATR of 14
    /// hourly bars, deciding hourly.
    const CONFIG: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "AUD/USD.SIM", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
        ]
        warmup = "30h"

        [indicators.fast]
        period = 10

        [indicators.slow]
        period = 30

        [trade]
        instruments = [{ instrument_id = "AUD/USD.SIM" }]
        risk = "0.5%"

        [model]
        dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
        decision = { every = "1h" }
        target = { limit = "30m", atr = { period = 14 } }
        retrain = "24h"
        window = "48h"
        calibration = "12h"
        margins = { long = "0.5R", short = "0.5R" }
        min_rows = 30
        learner = { kind = "baseline" }
    "#;

    #[rstest]
    fn the_example_config_loads() {
        ModelEmaConfig::load(include_str!("../../../config/model_ema.toml")).unwrap();
    }

    /// Window less calibration is 480 hours, which is 20 days and at least 14 weekdays of 23
    /// trading hours. That makes 322 hourly decisions. A split leaves `min_data_in_leaf` rows on
    /// each side.
    #[rstest]
    fn the_example_configs_leaves_split_on_the_rows_its_window_fits_on() {
        let model = ModelEmaConfig::load(include_str!("../../../config/model_ema.toml"))
            .unwrap()
            .model();
        let LearnerConfig::LightGbm(learner) = &model.learner else {
            panic!("not LightGBM: {:?}", model.learner);
        };
        let DecisionRule::Every(every) = model.decision;
        let days = (model.window - model.calibration).as_hours() / 24;
        let weekdays = days / 7 * 5 + (days % 7 - 2).max(0);
        let rows = weekdays * 23 * 60 / every.as_mins();

        assert!(
            2 * i64::from(learner.min_data_in_leaf) <= rows,
            "leaves of {} rows on {rows} rows",
            learner.min_data_in_leaf
        );
    }

    /// A bars series listed again is refused under its second listing.
    #[rstest]
    fn a_bars_series_listed_twice_is_refused_under_its_second_listing() {
        let bars = r#"{ data = "bars", instrument_id = "AUD/USD.SIM", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },"#;
        let toml = CONFIG.replace(bars, &[bars; 2].join("\n"));

        let err = ModelEmaConfig::load(&toml).unwrap_err();

        let Some(ConfigError::Duplicate {
            field,
            value: Some(value),
        }) = err.downcast_ref::<ConfigError>()
        else {
            panic!("not one duplicate naming the series: {err}");
        };
        assert_eq!(field, "data.series[1]");
        assert!(value.contains("AUD/USD.SIM-1-HOUR-MID-INTERNAL"), "{value}");
    }

    #[rstest]
    fn a_config_without_bars_series_is_refused_under_the_series() {
        let toml = CONFIG.replace(
            r#"{ data = "bars", instrument_id = "AUD/USD.SIM", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },"#,
            r#"{ data = "quotes", instrument_id = "AUD/USD.SIM" },"#,
        );

        let err = ModelEmaConfig::load(&toml).unwrap_err();

        let Some(ConfigError::InvalidValue { field, .. }) = err.downcast_ref::<ConfigError>()
        else {
            panic!("not one invalid value: {err}");
        };
        assert_eq!(field, "data.series");
    }

    /// Without `[base]`, only the strategy id is set.
    #[rstest]
    fn defaults_its_base_to_its_strategy_id_alone() {
        let base = ModelEmaConfig::load(CONFIG).unwrap().base;
        assert_eq!(base.strategy_id, Some(StrategyId::from("MODEL_EMA-001")));
        assert_eq!(base.order_id_tag, None);
        assert_eq!(base.oms_type, None);
    }

    #[rstest]
    fn claims_the_external_orders_of_the_instrument_it_trades() {
        let models = tempfile::tempdir().unwrap();
        let mode = Mode::Backtest {
            models: models.path().to_path_buf(),
        };

        let strategy = ModelEma::new(ModelEmaConfig::load(CONFIG).unwrap(), mode).unwrap();

        assert_eq!(
            strategy.external_order_instrument_ids(),
            Some(vec![InstrumentId::from("AUD/USD.SIM")])
        );
    }

    /// The restart, 2026-10-26T10:00:00Z.
    const NOW: u64 = 1_792_922_400_000_000_000;
    const MINUTE: u64 = 60_000_000_000;

    fn started(
        cache: &Rc<RefCell<Cache>>,
        models: &Path,
        now: u64,
    ) -> (ModelEma, Rc<RefCell<VirtualClock>>) {
        let mode = Mode::Backtest {
            models: models.to_path_buf(),
        };
        let mut strategy = ModelEma::new(ModelEmaConfig::load(CONFIG).unwrap(), mode).unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(UnixNanos::from(now), true);
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        strategy
            .core
            .register(
                TraderId::from("HELIX-001"),
                clock.clone(),
                cache.clone(),
                portfolio,
            )
            .unwrap();
        DataActor::on_start(&mut strategy).unwrap();
        (strategy, clock)
    }

    /// A market entry is held 30 minutes and filled 20 minutes before a restart. The restart sets
    /// its time again, 10 minutes after.
    #[rstest]
    fn a_restart_sets_the_time_of_its_open_trade_again() {
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache.borrow_mut().add_instrument(aud.clone()).unwrap();
        let models = tempfile::tempdir().unwrap();
        let sent = NOW - 20 * MINUTE;
        let (mut before, _) = started(&cache, models.path(), sent);
        let trade = Trade::new(
            InstrumentId::from("AUD/USD.SIM"),
            OrderSide::Buy,
            Price::from("0.70010"),
            Decimal::new(5, 3),
            Exits {
                bracket: None,
                time_limit: Some(SignedDuration::from_mins(30)),
            },
        );
        let ids = OrderIds {
            entry: ClientOrderId::from("T-1"),
            take_profit: ClientOrderId::from("T-1-TP"),
            stop_loss: ClientOrderId::from("T-1-SL"),
        };
        let stake = before.stake;
        trade
            .send_at(&mut before, stake, Money::from("100000 USD"), ids)
            .unwrap();
        let entry = cache
            .borrow()
            .orders(None, None, None, None, None)
            .into_iter()
            .next()
            .unwrap()
            .clone();
        let account = AccountId::from("SIM-001");
        let filled = TestOrderEventStubs::filled(
            &entry,
            &aud,
            Some(TradeId::from("T-1")),
            Some(PositionId::from("P-1")),
            Some(Price::from("0.70010")),
            None,
            None,
            None,
            Some(UnixNanos::from(sent)),
            Some(account),
        );
        for event in [
            TestOrderEventStubs::submitted(&entry, account),
            TestOrderEventStubs::accepted(&entry, account, VenueOrderId::from("V-1")),
            filled.clone(),
        ] {
            cache.borrow_mut().update_order(&event).unwrap();
        }
        let OrderEventAny::Filled(fill) = filled else {
            panic!("not a fill");
        };
        cache
            .borrow_mut()
            .add_position(&Position::new(&aud, fill), OmsType::Hedging)
            .unwrap();

        let (_strategy, clock) = started(&cache, models.path(), NOW);

        let clock = clock.borrow();
        let entry = entry.client_order_id();
        let names = clock.timer_names();
        let alert = names
            .iter()
            .find(|name| name.ends_with(entry.as_str()))
            .unwrap_or_else(|| panic!("{names:?}"));
        assert_eq!(
            clock.next_time_ns(alert),
            Some(UnixNanos::from(NOW + 10 * MINUTE))
        );
    }
}
