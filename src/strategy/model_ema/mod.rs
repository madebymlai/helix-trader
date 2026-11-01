//! Model EMA, an example strategy trading on a learned model: at each
//! decision of its model section, its model handle (`model::handle`)
//! predicts each side on its features, the averages of its bars
//! (`features.rs`), and a side expected to clear its margin is a decision
//! (`handle::Decision`), whose trade it sends as given through the shared
//! order module (`decision.trade().send`, `strategy::orders`): the target
//! its model was labelled on, as a dataset run sets it at the decision on
//! that side, a bracket closed at the barrier's limit or a market entry
//! closed at the horizon, both from the send. It hands the module its time
//! events, which close the trades at their time, and the module sets the
//! time of the trades still open again at start, as after a restart.
//! Every target closes before the next decision (`ModelConfig::validate`),
//! so trades do not overlap, on a hedging venue as on a netting one. It
//! decides only once its indicators are initialized: warm from the replay
//! in a backtest or a dataset run, from its warmup request live. Nautilus'
//! IB execution client denies the reduce-only orders a bracket's legs and
//! a time close are: it cannot trade live on IB (`live/ib/README.md`).
//!
//! A `[model.meta]` table in its config adds a meta model, which takes
//! or leaves each trade of the model (`model::meta`), and may size it.
//!
//! In a dataset run (`Mode::Dataset`, the replay of its dataset and its
//! training, `model::ModelStrategy`) it places no trade of its own: the
//! handle opens each decision's targets, through the same module, and is
//! handed the order and position events; the module closes the targets
//! at their time as it does the trades.

mod features;

use nautilus_common::{
    actor::{DataActor, DataActorNative},
    config::{ConfigError, ConfigResult},
    messages::data::DataResponseEnvelope,
    timer::TimeEvent,
};
use nautilus_model::{
    data::{Bar, OrderBookDelta, OrderBookDepth, QuoteTick, TradeTick},
    events::{OrderCanceled, OrderDenied, OrderRejected, PositionClosed},
    identifiers::StrategyId,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use serde::Deserialize;
use std::{any::Any, rc::Rc};

use crate::{
    model::{
        self, ModelPart, ModelStrategy, decide,
        features::FeatureSet,
        handle::{Mode, ModelHandle},
    },
    strategy::{Part, Series, StandardConfig, orders, warmup::Warmup},
};

/// Config of the `model_ema` strategy, loaded from TOML: the standard
/// tables, its model in `[model]`, and no fields of its own
/// (`ModelEmaParams`).
pub type ModelEmaConfig = StandardConfig<ModelEmaParams>;

/// The strategy's part: no fields of its own. It trades on a model, whose
/// targets trade the one instrument of `[trade]`, of its size; its
/// features are read from every bars series of `[data]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEmaParams {}

impl Part for ModelEmaParams {
    const INSTRUMENTS: usize = 1;
    const TAKES_MODEL: bool = true;

    /// The strategy's id alone, `MODEL_EMA-001`: Nautilus tags its orders
    /// with the id's tag. `oms_type` is left unset: Nautilus keeps the
    /// strategy's positions as the execution client of the venue does,
    /// netting or hedging, in a backtest as live.
    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("MODEL_EMA-001")),
            ..Default::default()
        }
    }

    /// What its features and the barrier's ATR read (`model::reads`).
    fn reads(config: &ModelEmaConfig) -> ConfigResult<Vec<Series>> {
        model::reads(config)
    }
}

impl ModelPart for ModelEmaParams {
    /// The averages of each of its bars series, warmed over its warmup.
    ///
    /// # Errors
    ///
    /// Returns an error under `data.series` if it lists no bars series.
    fn features(config: &ModelEmaConfig) -> ConfigResult<Rc<FeatureSet>> {
        let bar_types: Vec<_> = config.data.bar_types().collect();
        features::features(&bar_types, config.data.warmup)
            .map_err(|e| ConfigError::invalid_value("data.series", format!("{e:#}")))
    }
}

#[derive(Debug)]
pub struct ModelEma {
    core: StrategyCore,
    /// Its model handle, which owns its warmup.
    model: Rc<ModelHandle>,
}

impl ModelStrategy for ModelEma {
    type Params = ModelEmaParams;

    /// The strategy, deciding on `features`, its model handle in `mode`:
    /// live, its warmup (`DataConfig::warmup`) is requested at start, which
    /// hydrates its indicators; otherwise they warm on the data replayed.
    ///
    /// # Errors
    ///
    /// Returns an error as `ModelHandle::new` does, or if Nautilus refuses
    /// the base (`StrategyCore::new_checked`).
    fn with_features(
        config: ModelEmaConfig,
        features: Rc<FeatureSet>,
        mode: Mode,
    ) -> anyhow::Result<Self> {
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
    fn warmup(&self) -> Rc<Warmup> {
        self.model.warmup()
    }
}

nautilus_strategy!(ModelEma, {
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.model.on_order_canceled(self, event);
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

impl DataActor for ModelEma {
    /// Starts the model handle, which starts the warmup: live, it requests
    /// the warmup of the bars and of the barrier's ATR, then subscribes to
    /// them; then the features, registered as its Nautilus indicator, and
    /// the decisions.
    fn on_start(&mut self) -> anyhow::Result<()> {
        orders::arm_open_trades(self)?;
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

    /// Nautilus routes no book or custom data to indicators: the warmup
    /// hands it to the features.
    fn on_historical_book_depth(&mut self, depths: &[OrderBookDepth]) -> anyhow::Result<()> {
        self.model.warmup().on_historical_book_depth(depths);
        Ok(())
    }

    fn on_historical_book_deltas(&mut self, deltas: &[OrderBookDelta]) -> anyhow::Result<()> {
        self.model.warmup().on_historical_book_deltas(deltas);
        Ok(())
    }

    fn on_historical_data(&mut self, data: &dyn Any) -> anyhow::Result<()> {
        self.model.warmup().on_historical_data(data);
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            // It decides only once its indicators are initialized: the
            // features, and in a dataset run the barrier's ATR.
            if !self.core().indicators_initialized()? {
                return Ok(());
            }
            return match Rc::clone(&self.model).decide(self)? {
                Some(decision) => decision.trade().send(self),
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

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::Path};

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
        identifiers::{AccountId, InstrumentId, PositionId, TradeId, TraderId, VenueOrderId},
        instruments::{InstrumentAny, stubs::audusd_sim},
        orders::{Order, stubs::TestOrderEventStubs},
        position::Position,
        types::{Money, Price, Quantity},
    };
    use nautilus_portfolio::portfolio::Portfolio;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        model::dataset::{Commission, DatasetVenue},
        strategy::orders::{Exits, Trade},
    };

    /// Trades AUD/USD on a horizon of 30 minutes, deciding hourly.
    const CONFIG: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "AUD/USD.SIM", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
        ]
        warmup = "30h"

        [trade]
        instruments = [{ instrument_id = "AUD/USD.SIM", trade_size = "100000" }]

        [model]
        decision = { every = "1h" }
        targets = { horizon = "30m" }
        label = "horizon"
        retrain = "24h"
        window = "48h"
        holdout = "12h"
        margins = { long = 0.5, short = 0.5 }
        min_pnl = 0.5
        min_trades = 30
        learner = { kind = "baseline" }
    "#;

    #[test]
    fn the_example_config_loads() {
        ModelEmaConfig::load(include_str!("../../../config/model_ema.toml")).unwrap();
    }

    /// Its features read the series its example lists, and no other: the
    /// features build of it.
    #[test]
    fn the_features_of_the_example_config_read_its_series() {
        let config = ModelEmaConfig::load(include_str!("../../../config/model_ema.toml")).unwrap();

        ModelEmaParams::features(&config).unwrap();
    }

    /// `CONFIG` listing the quotes of the AUD/USD traded before its bars.
    fn with_quotes() -> String {
        CONFIG.replace(
            "series = [",
            r#"series = [
            { data = "quotes", instrument_id = "AUD/USD.SIM" },"#,
        )
    }

    /// Its features read bars alone: the quotes of the AUD/USD traded,
    /// which the barrier's ATR of a horizon does not run on, are read by
    /// none.
    #[test]
    fn a_tick_series_its_features_do_not_read_is_refused_at_load() {
        let err = ModelEmaConfig::load(&with_quotes()).unwrap_err();

        let Some(ConfigError::InvalidValue { field, reason }) = err.downcast_ref::<ConfigError>()
        else {
            panic!("not one invalid value: {err}");
        };
        assert_eq!(field, "data.series[0]");
        assert!(reason.contains("QuoteTick of AUD/USD.SIM"), "{reason}");
    }

    /// Labelled on a barrier 2 and 1 ATRs out, the barrier's ATR is built
    /// of the quotes of the AUD/USD traded: it reads them.
    #[test]
    fn quotes_the_barrier_atr_is_built_of_load() {
        let toml = with_quotes()
            .replace(
                r#"targets = { horizon = "30m" }"#,
                r#"targets = { barrier = { take_profit = { atr = 2 }, stop = { atr = 1 }, limit = "45m" } }"#,
            )
            .replace(r#"label = "horizon""#, r#"label = "barrier""#);

        ModelEmaConfig::load(&toml).unwrap();
    }

    /// Its features average each bars series once: listed again, it is
    /// refused under its second listing, naming it, and not as features
    /// of one name.
    #[test]
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

    /// Its features are averages of bars: ticks alone give it none.
    #[test]
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

    /// Without `[base]`, its default: its strategy id alone, Nautilus
    /// tagging its orders with the id's tag.
    #[test]
    fn defaults_its_base_to_its_strategy_id_alone() {
        let base = ModelEmaConfig::load(CONFIG).unwrap().base;
        assert_eq!(base.strategy_id, Some(StrategyId::from("MODEL_EMA-001")));
        assert_eq!(base.order_id_tag, None);
        assert_eq!(base.oms_type, None);
    }

    /// 2026-10-26T10:00:00Z, the time of the restart.
    const NOW: u64 = 1_792_922_400_000_000_000;
    const MINUTE: u64 = 60_000_000_000;

    /// The strategy, backtesting on the empty store under `models`,
    /// registered without a node on `cache` at `now`, started, and its
    /// clock.
    fn started(
        cache: &Rc<RefCell<Cache>>,
        models: &Path,
        now: u64,
    ) -> (ModelEma, Rc<RefCell<VirtualClock>>) {
        let mode = Mode::Backtest {
            models: models.to_path_buf(),
            venue: DatasetVenue {
                commission: Commission::Rate("0.00003".to_string()),
                balance: Money::from("10000000 USD"),
                leverage: Decimal::ONE,
            },
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

    /// A market entry held 30 minutes, sent and filled 20 minutes before a
    /// restart: started again, the strategy sets its time again, 10
    /// minutes after the restart.
    #[test]
    fn a_restart_sets_the_time_of_its_open_trade_again() {
        let aud = InstrumentAny::CurrencyPair(audusd_sim());
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache.borrow_mut().add_instrument(aud.clone()).unwrap();
        let models = tempfile::tempdir().unwrap();
        let sent = NOW - 20 * MINUTE;
        let (mut before, _) = started(&cache, models.path(), sent);
        let trade = Trade {
            instrument_id: InstrumentId::from("AUD/USD.SIM"),
            side: OrderSide::Buy,
            quantity: Quantity::from(100_000),
            exits: Exits {
                take_profit: None,
                stop_loss: None,
                time_limit: Some(SignedDuration::from_mins(30)),
            },
        };
        trade.send(&mut before).unwrap();
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
