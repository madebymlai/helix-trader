use nautilus_common::config::{ConfigError, ConfigErrorCollector};
use nautilus_model::identifiers::StrategyId;
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use crate::{
    model::{
        config::{ModelConfig, ModelSection},
        handle::traded_quotes,
    },
    strategy::{Part, StandardConfig, TickSeries},
};

/// Config of the `model_ema` strategy, loaded from TOML: the standard
/// tables and, at the top level, its own `[model]` table
/// (`ModelEmaParams`).
pub type ModelEmaConfig = StandardConfig<ModelEmaParams>;

/// The strategy's own part: its model, a `[model]` table, whose targets
/// trade the one instrument of `[trade]`, of its size. The features are
/// read from every bars series of `[data]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEmaParams {
    pub model: ModelSection,
}

impl Part for ModelEmaParams {
    const INSTRUMENTS: usize = 1;

    /// The strategy's id and order tag. `oms_type` is left unset: Nautilus
    /// keeps the strategy's positions as the execution client of the venue
    /// does, netting or hedging, in a backtest as live.
    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("MODEL_EMA-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }
    }

    /// The base without a strategy id, which keys the model's store, or
    /// the model invalid for the data read and the instrument traded (see
    /// `ModelConfig::validate`), its first error under `model`.
    fn validate(config: &ModelEmaConfig, errors: &mut ConfigErrorCollector) {
        errors.check(
            config.base.strategy_id.is_some(),
            ConfigError::missing_field("base.strategy_id"),
        );
        if let [traded] = config.trade.instruments.as_slice() {
            let model = config.own.model.clone().trading(traded);
            errors.collect(
                model
                    .validate(&config.data_requirements())
                    .map_err(|e| ConfigError::invalid_value("model", format!("{e:#}"))),
            );
        }
    }

    /// The quotes of the instrument traded, which the model's harness
    /// reads (`handle::traded_quotes`).
    fn harness_ticks(config: &ModelEmaConfig) -> Vec<TickSeries> {
        config
            .trade
            .instruments
            .iter()
            .map(|traded| traded_quotes(traded.instrument_id))
            .collect()
    }
}

impl ModelEmaConfig {
    /// The model, its targets trading the instrument of `[trade]`, of its
    /// size.
    ///
    /// # Panics
    ///
    /// Panics if `[trade]` does not list exactly one instrument, which
    /// `validate` refuses.
    #[must_use]
    pub fn model(&self) -> ModelConfig {
        let [traded] = self.trade.instruments.as_slice() else {
            panic!("a validated model_ema trades one instrument");
        };
        self.own.model.clone().trading(traded)
    }

    /// The strategy's id, from its base, which `validate` requires.
    ///
    /// # Panics
    ///
    /// Panics if the base has none.
    #[must_use]
    pub fn strategy_id(&self) -> StrategyId {
        self.base
            .strategy_id
            .expect("a validated model_ema has a strategy id")
    }
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_indicators::average::MovingAverageType;
    use nautilus_model::{
        data::{BarType, NautilusDataType},
        identifiers::InstrumentId,
        types::Quantity,
    };

    use super::*;
    use crate::model::{
        decide::DecisionRule,
        learner::LearnerConfig,
        targets::{Barrier, Distance, Label},
    };
    use crate::strategy::AtrArgs;

    const TOML: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },
        ]
        warmup = "1h"

        [trade]
        instruments = [{ instrument_id = "XAUUSD.ICMARKETS", trade_size = "2" }]

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

    /// `TOML` labelled on a barrier 2 ATRs out and 1.5 ATRs against,
    /// closed after 45 minutes, its ATR `atr`, as the `targets` line has
    /// it, warmed up over the 3 hourly bars an ATR of 3 bars needs.
    fn with_atr(atr: &str) -> String {
        TOML.replace(r#"warmup = "1h""#, r#"warmup = "3h""#).replace(
            r#"targets = { horizon = "30m" }"#,
            &format!(
                r#"targets = {{ barrier = {{ take_profit = {{ atr = 2 }}, stop = {{ atr = 1.5 }}, limit = "45m"{atr} }} }}"#
            ),
        )
        .replace(r#"label = "horizon""#, r#"label = "barrier""#)
    }

    /// The fields of the config errors refusing `toml`.
    fn error_fields(toml: &str) -> Vec<String> {
        let err = ModelEmaConfig::load(toml).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err}"),
        };
        errors
            .into_iter()
            .map(|err| match err {
                ConfigError::Range { field, .. }
                | ConfigError::InvalidValue { field, .. }
                | ConfigError::InvalidReference { field, .. }
                | ConfigError::MissingField { field } => field,
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    /// The reason of the one config error refusing `toml`, under `model`.
    fn model_refusal(toml: &str) -> String {
        let err = ModelEmaConfig::load(toml).unwrap_err();
        match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::InvalidValue { field, reason }) if field == "model" => reason.clone(),
            _ => panic!("not refused under model: {err}"),
        }
    }

    /// The message of the error refusing `toml`, serde's on its shape.
    fn shape_error(toml: &str) -> String {
        let err = ModelEmaConfig::load(toml).unwrap_err();
        assert!(err.downcast_ref::<ConfigError>().is_none(), "{err}");
        err.to_string()
    }

    #[test]
    fn loads_the_standard_tables_and_the_model() {
        let config = ModelEmaConfig::load(TOML).unwrap();
        assert_eq!(
            config.data_requirements().bar_types,
            [BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL")]
        );
        let model = config.model();
        assert_eq!(model.targets.horizon, Some(SignedDuration::from_mins(30)));
        assert_eq!(
            model.decision,
            DecisionRule::Every(SignedDuration::from_hours(1))
        );
        assert_eq!(model.learner, LearnerConfig::Baseline {});
        assert_eq!(config.strategy_id(), StrategyId::from("MODEL_EMA-001"));
    }

    #[test]
    fn the_example_config_loads() {
        let config = ModelEmaConfig::load(include_str!("../../../config/model_ema.toml")).unwrap();
        let model = config.model();
        assert_eq!(
            model.targets.instrument_id,
            InstrumentId::from("XAUUSD.ICMARKETS")
        );
        assert_eq!(model.label, Label::Barrier);
        assert_eq!(
            model.targets.barrier.map(|barrier| barrier.limit),
            Some(SignedDuration::from_mins(45))
        );
        assert_eq!(config.strategy_id(), StrategyId::from("MODEL_EMA-001"));
    }

    /// The targets trade what `[trade]` names: its one instrument, of its
    /// size.
    #[test]
    fn the_targets_trade_the_instrument_and_size_of_the_trade_table() {
        let targets = ModelEmaConfig::load(TOML).unwrap().model().targets;
        assert_eq!(
            targets.instrument_id,
            InstrumentId::from("XAUUSD.ICMARKETS")
        );
        assert_eq!(targets.quantity, Quantity::from(2));
    }

    #[test]
    fn targets_naming_an_instrument_or_a_quantity_are_refused_at_load() {
        for key in [r#"instrument_id = "XAUUSD.ICMARKETS""#, r#"quantity = "1""#] {
            let toml = TOML.replace(
                r#"targets = { horizon"#,
                &format!(r#"targets = {{ {key}, horizon"#),
            );
            let name = key.split(' ').next().unwrap();
            let err = shape_error(&toml);
            assert!(err.contains(&format!("`{name}`")), "{name}: {err}");
        }
    }

    #[test]
    fn a_second_traded_instrument_is_refused() {
        let toml = TOML.replace(
            r#"trade_size = "2" }]"#,
            r#"trade_size = "2" }, { instrument_id = "EURUSD.ICMARKETS", trade_size = "1" }]"#,
        );
        assert_eq!(error_fields(&toml), ["trade.instruments"]);
    }

    /// The harness reads the quotes of the instrument traded, whatever the
    /// features read: the requirements hold them after the series' ticks.
    #[test]
    fn a_traded_instrument_not_read_loads_its_quotes_into_the_requirements() {
        let toml = TOML.replace(
            r#"instruments = [{ instrument_id = "XAUUSD.ICMARKETS""#,
            r#"instruments = [{ instrument_id = "EURUSD.ICMARKETS""#,
        );

        let requirements = ModelEmaConfig::load(&toml).unwrap().data_requirements();

        assert_eq!(
            requirements.ticks,
            [TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from("EURUSD.ICMARKETS"),
            }]
        );
    }

    /// Quotes of the instrument traded the series list already are listed
    /// once.
    #[test]
    fn traded_quotes_the_series_read_are_required_once() {
        let toml = TOML.replace(
            "series = [",
            r#"series = [
            { data = "quotes", instrument_id = "XAUUSD.ICMARKETS" },"#,
        );

        let requirements = ModelEmaConfig::load(&toml).unwrap().data_requirements();

        assert_eq!(requirements.ticks.len(), 1, "{:?}", requirements.ticks);
    }

    #[test]
    fn a_futures_root_traded_is_refused_at_load() {
        let toml = TOML.replace(
            r#"instruments = [{ instrument_id = "XAUUSD.ICMARKETS""#,
            r#"instruments = [{ instrument_id = "GC.GLBX""#,
        );
        let reason = model_refusal(&toml);
        assert!(reason.contains("GC.GLBX"), "{reason}");
    }

    /// A trade on a horizon of an hour, decided every hour, would still be
    /// open at the next decision.
    #[test]
    fn a_target_not_shorter_than_the_decision_cadence_is_refused() {
        let toml = TOML.replace(r#"horizon = "30m""#, r#"horizon = "1h""#);
        let reason = model_refusal(&toml);
        assert!(
            reason.contains("horizon 1h") && reason.contains("cadence 1h"),
            "{reason}"
        );
    }

    #[test]
    fn a_base_without_a_strategy_id_is_refused() {
        let toml = format!("{TOML}\n[base]\norder_id_tag = \"001\"\n");
        assert_eq!(error_fields(&toml), ["base.strategy_id"]);
    }

    #[test]
    fn a_misspelt_field_of_the_model_is_refused_at_load() {
        let err = shape_error(&TOML.replace("min_pnl", "min_pn"));
        assert!(err.contains("min_pn"), "{err}");
    }

    /// The barrier's ATR takes Nautilus' `AverageTrueRange` arguments by
    /// their names, the moving average by Nautilus' name of it.
    #[test]
    fn the_barrier_atr_takes_nautilus_average_true_range_arguments() {
        let toml = with_atr(
            r#", atr = { period = 3, ma_type = "simple", use_previous = false, value_floor = 0.5 }"#,
        );
        let barrier = ModelEmaConfig::load(&toml)
            .unwrap()
            .model()
            .targets
            .barrier
            .unwrap();
        assert_eq!(
            barrier,
            Barrier {
                take_profit: Distance::Atr(2.into()),
                stop: Distance::Atr(rust_decimal::Decimal::new(15, 1)),
                limit: SignedDuration::from_mins(45),
                atr: Some(AtrArgs {
                    period: 3,
                    ma_type: Some(MovingAverageType::Simple),
                    use_previous: Some(false),
                    value_floor: Some(0.5),
                }),
            }
        );
    }

    #[test]
    fn a_moving_average_nautilus_does_not_name_is_refused_at_load() {
        let err = shape_error(&with_atr(
            r#", atr = { period = 3, ma_type = "TRIANGULAR" }"#,
        ));
        assert!(err.contains("TRIANGULAR"), "{err}");
    }

    /// Nautilus' `AverageTrueRange::new` panics on an argument it refuses,
    /// and the load builds the ATR.
    #[test]
    #[should_panic(expected = "value_floor")]
    fn a_barrier_atr_argument_nautilus_refuses_panics_at_load() {
        let _ = ModelEmaConfig::load(&with_atr(r#", atr = { period = 3, value_floor = -1.0 }"#));
    }

    #[test]
    #[should_panic(expected = "period")]
    fn a_barrier_atr_of_no_bars_panics_at_load() {
        let _ = ModelEmaConfig::load(&with_atr(r#", atr = { period = 0 }"#));
    }

    #[test]
    fn reports_its_own_violations_with_the_standard_ones() {
        let toml = format!("{TOML}\n[base]\norder_id_tag = \"001\"\n")
            .replace("\"1h\"\n", "\"0s\"\n")
            .replace(r#"horizon = "30m""#, r#"horizon = "1h""#);
        assert_eq!(
            error_fields(&toml),
            ["data.warmup", "base.strategy_id", "model"]
        );
    }

    /// A meta model is a `[model.meta]` table, and no more: a meta of the
    /// meta is an unknown field. It learns the barrier trades of a primary
    /// labelled on the barrier.
    #[test]
    fn loads_a_meta_model_from_its_table_alone_and_no_deeper() {
        let meta = r#"
            [model.meta]
            window = "48h"
            holdout = "12h"
            threshold = 0.55
            min_pnl = 0.5
            min_trades = 30
            learner = { kind = "baseline" }
        "#;
        let toml = format!(
            "{}\n{meta}",
            TOML.replace(
                r#"targets = { horizon = "30m" }"#,
                r#"targets = { horizon = "30m", barrier = { take_profit = { bps = 10 }, stop = { bps = 10 }, limit = "45m" } }"#,
            )
            .replace(r#"label = "horizon""#, r#"label = "barrier""#)
        );

        let config = ModelEmaConfig::load(&toml).unwrap();
        assert_eq!(config.model().meta.unwrap().threshold, 0.55);

        let nested = format!("{toml}\n[model.meta.meta]\nthreshold = 0.5\n");
        let err = shape_error(&nested);
        assert!(err.contains("unknown field `meta`"), "{err}");
    }
}
