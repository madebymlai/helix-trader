use nautilus_common::config::{ConfigError, ConfigErrorCollector};
use nautilus_model::{
    data::BarType,
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use crate::strategy::{Part, StandardConfig};

/// Config of the EMA crossover strategy, loaded from TOML: the standard
/// tables and, at the top level, its own fields (`EmaParams`).
pub type EmaCrossConfig = StandardConfig<EmaParams>;

/// The crossover's own fields: the periods of its fast and slow EMAs, in
/// bars. It trades one instrument, the one of `[trade]`, on its one bars
/// series of `[data]`; the other series are read only. Its exits are its
/// code's (`bracket::STOP_LOSS`, `TAKE_PROFIT`, `HOLD`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaParams {
    #[serde(default = "default_fast_period")]
    pub fast_period: usize,
    #[serde(default = "default_slow_period")]
    pub slow_period: usize,
}

impl Part for EmaParams {
    const INSTRUMENTS: usize = 1;

    /// The strategy's id and order tag. `oms_type` is left unset: Nautilus
    /// keeps the strategy's positions as the execution client of the venue
    /// does, netting or hedging, in a backtest as live.
    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("EMA_CROSS-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }
    }

    /// The periods out of order or the fast one zero, or the traded
    /// instrument without exactly one bars series.
    fn validate(config: &EmaCrossConfig, errors: &mut ConfigErrorCollector) {
        let EmaParams {
            fast_period,
            slow_period,
        } = config.own;
        errors.check(
            fast_period > 0 && fast_period < slow_period,
            ConfigError::range(
                "fast_period",
                format!("must be positive and below the slow period {slow_period}"),
            ),
        );
        if let Some(traded) = config.trade.instruments.first() {
            let id = traded.instrument_id;
            let bars = config.data.bar_types().filter(|b| b.instrument_id() == id);
            let count = bars.count();
            errors.check(
                count == 1,
                ConfigError::invalid_reference(
                    "trade.instruments[0].instrument_id",
                    id.to_string(),
                    format!("has {count} bars series in data.series, the crossover reads one"),
                ),
            );
        }
    }
}

impl EmaCrossConfig {
    /// The one instrument traded.
    #[must_use]
    pub fn trade_instrument_id(&self) -> InstrumentId {
        self.trade.instruments[0].instrument_id
    }

    /// The size of each trade.
    #[must_use]
    pub fn trade_size(&self) -> Quantity {
        self.trade.instruments[0].trade_size
    }

    /// The bars of the traded instrument, the ones the crossover reads.
    ///
    /// # Panics
    ///
    /// Panics if the traded instrument has no bars series, which
    /// `validate` refuses.
    #[must_use]
    pub fn trade_bar_type(&self) -> BarType {
        let id = self.trade_instrument_id();
        self.data
            .bar_types()
            .find(|bar_type| bar_type.instrument_id() == id)
            .expect("the traded instrument's bars validated")
    }
}

const fn default_fast_period() -> usize {
    10
}

const fn default_slow_period() -> usize {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOML: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "ESH4.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
            { data = "bars", instrument_id = "ESM4.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
        ]
        warmup = "720h"
        session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }

        [trade]
        instruments = [{ instrument_id = "ESM4.CME", trade_size = "1" }]
    "#;

    /// The fields of the config errors refusing `toml`.
    fn error_fields(toml: &str) -> Vec<String> {
        let err = EmaCrossConfig::load(toml).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err}"),
        };
        errors
            .into_iter()
            .map(|err| match err {
                ConfigError::Range { field, .. }
                | ConfigError::InvalidReference { field, .. }
                | ConfigError::InvalidValue { field, .. } => field,
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    #[test]
    fn loads_from_toml_with_defaults() {
        let config = EmaCrossConfig::load(TOML).unwrap();
        assert_eq!(config.own.fast_period, 10);
        assert_eq!(config.own.slow_period, 30);
        assert_eq!(config.trade_size(), Quantity::from(1));
        assert_eq!(config.trade_instrument_id(), InstrumentId::from("ESM4.CME"));
        assert_eq!(
            config.trade_bar_type(),
            BarType::from("ESM4.CME-1-HOUR-MID-INTERNAL")
        );
    }

    #[test]
    fn the_example_config_loads() {
        let config = EmaCrossConfig::load(include_str!("../../../config/ema.toml")).unwrap();
        assert_eq!(
            config.trade_bar_type(),
            BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")
        );
    }

    #[test]
    fn reads_its_periods_from_the_top_level() {
        let toml = format!("fast_period = 5\nslow_period = 15\n{TOML}");
        let config = EmaCrossConfig::load(&toml).unwrap();
        assert_eq!((config.own.fast_period, config.own.slow_period), (5, 15));
    }

    #[test]
    fn defaults_its_base_and_leaves_the_oms_to_the_venue() {
        let config = EmaCrossConfig::load(TOML).unwrap();
        assert_eq!(
            config.base.strategy_id,
            Some(StrategyId::from("EMA_CROSS-001"))
        );
        assert_eq!(config.base.oms_type, None);
    }

    #[test]
    fn rejects_a_misspelt_period_at_load() {
        let toml = format!("fast_perod = 5\n{TOML}");
        let err = EmaCrossConfig::load(&toml).unwrap_err();
        assert!(err.to_string().contains("fast_perod"), "{err}");
    }

    #[test]
    fn rejects_exits_in_the_toml() {
        let toml = format!("hold = \"1h\"\n{TOML}");
        let err = EmaCrossConfig::load(&toml).unwrap_err();
        assert!(err.to_string().contains("hold"), "{err}");
    }

    #[test]
    fn rejects_fast_period_not_below_slow() {
        let toml = format!("fast_period = 30\n{TOML}");
        assert_eq!(error_fields(&toml), ["fast_period"]);
    }

    #[test]
    fn rejects_two_traded_instruments() {
        let toml = TOML.replace(
            "instruments = [{ instrument_id = \"ESM4.CME\", trade_size = \"1\" }]",
            "instruments = [{ instrument_id = \"ESM4.CME\", trade_size = \"1\" }, \
             { instrument_id = \"ESH4.CME\", trade_size = \"1\" }]",
        );
        assert_eq!(error_fields(&toml), ["trade.instruments"]);
    }

    #[test]
    fn rejects_a_trade_instrument_without_bars() {
        let toml = TOML.replace(
            "{ instrument_id = \"ESM4.CME\", trade_size",
            "{ instrument_id = \"ESU4.CME\", trade_size",
        );
        assert_eq!(error_fields(&toml), ["trade.instruments[0].instrument_id"]);
    }

    #[test]
    fn rejects_a_trade_instrument_with_two_bars_series() {
        let toml = TOML
            .replace(
                "{ data = \"bars\", instrument_id = \"ESH4.CME\"",
                "{ data = \"bars\", instrument_id = \"ESM4.CME\"",
            )
            .replacen(
                "step = 1, aggregation = \"HOUR\"",
                "step = 2, aggregation = \"HOUR\"",
                1,
            );
        assert_eq!(error_fields(&toml), ["trade.instruments[0].instrument_id"]);
    }

    #[test]
    fn reports_its_own_violations_with_the_standard_ones() {
        let toml = format!("fast_period = 0\n{TOML}").replace("\"720h\"", "\"0s\"");
        assert_eq!(error_fields(&toml), ["data.warmup", "fast_period"]);
    }
}
