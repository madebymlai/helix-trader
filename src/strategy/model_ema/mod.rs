//! Model EMA, an example strategy trading on a learned model: at each
//! decision of its model section, its model handle (`model::handle`)
//! predicts each side on its features, the averages of its bars of
//! `[indicators.fast]` and `[indicators.slow]` (`features.rs`), and a
//! side expected to clear its margin is a decision
//! (`handle::Decision`), whose trade it sends as given through the shared
//! order module (`decision.trade().send`, `strategy::orders`): the target
//! its model learned, as a dataset run sets it at the decision on that
//! side, a bracket or a market entry, closed at the target's limit from the
//! send. It hands the module its time
//! events, which close the trades at their time, and the module sets the
//! time of the trades still open again at start, as after a restart.
//! The target closes before the next decision (`ModelConfig::validate`),
//! so trades do not overlap, on a hedging venue as on a netting one. Its
//! handle decides only once its features and the target's ATR are
//! initialized: warm from the replay in a backtest or a dataset run, from
//! its warmup request live. Its data hooks are the handle's
//! (`model_data_hooks!`).
//!
//! A `[model.meta]` table in its config adds a meta model, which takes
//! or leaves each trade of the model (`model::meta`).
//!
//! In a dataset run (`Mode::Dataset`, the replay of its dataset and its
//! training, `model::ModelStrategy`) it places no trade of its own: the
//! handle opens each decision's target, through the same module, and is
//! handed the order and position events; the module closes its trades at
//! their time as it does its own.

mod features;

use nautilus_common::{
    actor::DataActor,
    config::{ConfigError, ConfigResult},
    timer::TimeEvent,
};
use nautilus_model::{
    events::{OrderCanceled, OrderDenied, OrderRejected, PositionClosed},
    identifiers::StrategyId,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use serde::Deserialize;
use std::rc::Rc;

use crate::{
    model::{
        self, ModelPart, ModelStrategy, decide,
        features::FeatureSet,
        handle::{Mode, ModelHandle},
    },
    strategy::{Declaration, Part, Series, StandardConfig, orders, warmup::Warmup},
};

/// Config of the `model_ema` strategy, loaded from TOML: the standard
/// tables, its averages in `[indicators]`, its model in `[model]`, and no
/// fields of its own (`ModelEmaParams`).
pub type ModelEmaConfig = StandardConfig<ModelEmaParams>;

/// The strategy's part: no fields of its own. It trades on a model, whose
/// target trades the one instrument of `[trade]`, of its size; its
/// features are a fast and a slow Nautilus `ExponentialMovingAverage`,
/// `[indicators.fast]` and `[indicators.slow]`, of every bars series of
/// `[data]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEmaParams {}

impl Part for ModelEmaParams {
    const INSTRUMENTS: usize = 1;
    const INDICATORS: &'static [&'static dyn Declaration] = &[&features::FAST, &features::SLOW];
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

    /// What its features and the target's ATR read (`model::reads`).
    fn reads(config: &ModelEmaConfig) -> ConfigResult<Vec<Series>> {
        model::reads(config)
    }
}

impl ModelPart for ModelEmaParams {
    /// The averages of each of its bars series, of its tables, warmed
    /// over its warmup.
    ///
    /// # Errors
    ///
    /// Returns an error under `data.series` if it lists no bars series.
    fn features(config: &ModelEmaConfig) -> ConfigResult<Rc<FeatureSet>> {
        features::features(config)
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
    /// the warmup of the bars and of the target's ATR, then subscribes to
    /// them; then the features, registered as its Nautilus indicator for
    /// their bars, and the decisions.
    fn on_start(&mut self) -> anyhow::Result<()> {
        orders::arm_open_trades(self)?;
        Rc::clone(&self.model).start(self)
    }

    crate::model_data_hooks!(model);

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
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

#[cfg(all(test, feature = "strategy-tests"))]
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
        backtest::Commission,
        model::{dataset::DatasetVenue, decide::DecisionRule, learner::LearnerConfig},
        strategy::orders::{Exits, Trade},
    };

    /// Trades AUD/USD on a time-only target of 30 minutes, its unit measured
    /// in the ATR of 14 hourly bars, deciding hourly on its averages of 10 and
    /// 30 hourly bars.
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
        instruments = [{ instrument_id = "AUD/USD.SIM", trade_size = "100000" }]

        [model]
        decision = { every = "1h" }
        target = { limit = "30m", atr = { period = 14 } }
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

    /// Its side models are fitted on its window less its holdout and the
    /// calibration before it, as long: 480 hours, 20 days, of which 14 at
    /// the fewest are weekdays, gold trading 23 hours of each, so 322
    /// hourly decisions. A split leaves `min_data_in_leaf` rows each side
    /// at least: its leaves split on those rows.
    #[test]
    fn the_example_configs_leaves_split_on_the_rows_its_window_fits_on() {
        let model = ModelEmaConfig::load(include_str!("../../../config/model_ema.toml"))
            .unwrap()
            .model();
        let LearnerConfig::LightGbm(learner) = &model.learner else {
            panic!("not LightGBM: {:?}", model.learner);
        };
        let DecisionRule::Every(every) = model.decision;
        let days = (model.window - model.holdout * 2).as_hours() / 24;
        let weekdays = days / 7 * 5 + (days % 7 - 2).max(0);
        let rows = weekdays * 23 * 60 / every.as_mins();

        assert!(
            2 * i64::from(learner.min_data_in_leaf) <= rows,
            "leaves of {} rows on {rows} rows",
            learner.min_data_in_leaf
        );
    }

    /// `CONFIG` listing the quotes of the AUD/USD traded before its bars.
    fn with_quotes() -> String {
        CONFIG.replace(
            "series = [",
            r#"series = [
            { data = "quotes", instrument_id = "AUD/USD.SIM" },"#,
        )
    }

    /// Its features read bars alone: the quotes of the AUD/USD traded, which
    /// a bracket in basis points has no ATR to run on, are read by none.
    #[test]
    fn a_tick_series_its_features_do_not_read_is_refused_at_load() {
        let toml = with_quotes().replace(
            r#"target = { limit = "30m", atr = { period = 14 } }"#,
            r#"target = { bracket = { take_profit = { bps = 20 }, stop = { bps = 10 } }, limit = "45m" }"#,
        );

        let err = ModelEmaConfig::load(&toml).unwrap_err();

        let Some(ConfigError::InvalidValue { field, reason }) = err.downcast_ref::<ConfigError>()
        else {
            panic!("not one invalid value: {err}");
        };
        assert_eq!(field, "data.series[0]");
        assert!(reason.contains("QuoteTick of AUD/USD.SIM"), "{reason}");
    }

    /// The target's ATR, its time-only target's or that of a bracket 2 and
    /// 1 ATRs out, is built of the quotes of the AUD/USD traded: it reads
    /// them.
    #[test]
    fn quotes_the_target_atr_is_built_of_load() {
        let bracket = with_quotes().replace(
            r#"target = { limit = "30m", atr = { period = 14 } }"#,
            r#"target = { bracket = { take_profit = { atr = 2 }, stop = { atr = 1 } }, limit = "45m" }"#,
        );

        ModelEmaConfig::load(&with_quotes()).unwrap();
        ModelEmaConfig::load(&bracket).unwrap();
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

    /// The fields of the config errors refusing `toml`.
    fn refused_fields(toml: &str) -> Vec<String> {
        let err = ModelEmaConfig::load(toml).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err}"),
        };
        errors
            .into_iter()
            .map(|err| match err {
                ConfigError::MissingField { field }
                | ConfigError::UnsupportedField { field, .. }
                | ConfigError::InvalidValue { field, .. }
                | ConfigError::Range { field, .. } => field,
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    /// Its slow average has no table: refused under it, not built.
    #[test]
    fn a_missing_average_table_is_refused_under_its_name() {
        let toml = CONFIG.replace("[indicators.slow]\n        period = 30\n", "");

        assert_eq!(refused_fields(&toml), ["indicators.slow"]);
    }

    /// Its features are its code's: a table it does not declare adds
    /// none.
    #[test]
    fn a_table_it_does_not_declare_is_refused_under_its_name() {
        let toml = CONFIG.replace("[trade]", "[indicators.medium]\nperiod = 20\n\n[trade]");

        assert_eq!(refused_fields(&toml), ["indicators.medium"]);
    }

    /// Nautilus' `ExponentialMovingAverage::new_checked` refuses a period
    /// of 0.
    #[test]
    fn a_period_nautilus_refuses_is_refused_under_its_table() {
        let toml = CONFIG.replace("period = 10", "period = 0");

        assert_eq!(refused_fields(&toml), ["indicators.fast"]);
    }

    /// A refused period, a table it does not declare and a limit of an
    /// hour decided every hour are reported at once, each under its
    /// field.
    #[test]
    fn its_table_refusals_are_reported_with_the_other_violations() {
        let toml = CONFIG
            .replace("period = 10", "period = 0")
            .replace("[trade]", "[indicators.medium]\nperiod = 20\n\n[trade]")
            .replace(r#"limit = "30m""#, r#"limit = "1h""#);

        let mut fields = refused_fields(&toml);
        fields.sort();

        assert_eq!(
            fields,
            ["indicators.fast", "indicators.medium", "model.target.limit"]
        );
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
                commission: Commission::Rate(Decimal::new(3, 5)),
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
                bracket: None,
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
