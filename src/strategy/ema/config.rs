use nautilus_common::config::{ConfigError, ConfigErrorCollector};
use nautilus_indicators::average::ema::ExponentialMovingAverage;
use nautilus_model::{
    data::BarType,
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use crate::strategy::{Declaration, Declared, EmaArgs, Part, StandardConfig};

/// Config of the EMA crossover strategy, loaded from TOML: the standard
/// tables, its EMAs in `[indicators]`, and no fields of its own
/// (`EmaParams`).
pub type EmaCrossConfig = StandardConfig<EmaParams>;

/// The crossover's part: no fields of its own. It reads two Nautilus
/// `ExponentialMovingAverage`s, `[indicators.fast]` and
/// `[indicators.slow]`, the fast one of the shorter period; it trades one
/// instrument, the one of `[trade]`, on its one bars series of `[data]`;
/// the other series are read only. Its exits are its code's
/// (`bracket::STOP_LOSS`, `TAKE_PROFIT`, `HOLD`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaParams {}

/// The fast EMA, `[indicators.fast]`.
const FAST: Declared<EmaArgs> = Declared::new("fast");
/// The slow EMA, `[indicators.slow]`.
const SLOW: Declared<EmaArgs> = Declared::new("slow");

impl Part for EmaParams {
    const INSTRUMENTS: usize = 1;
    const INDICATORS: &'static [&'static dyn Declaration] = &[&FAST, &SLOW];

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

    /// The fast period not below the slow one, or the traded instrument
    /// without exactly one bars series.
    fn validate(config: &EmaCrossConfig, errors: &mut ConfigErrorCollector) {
        let args = |declared| config.indicators.args(declared);
        if let (Some(fast), Some(slow)) = (args(FAST), args(SLOW)) {
            errors.check(
                fast.period < slow.period,
                ConfigError::range(
                    "indicators.fast.period",
                    format!("must be below the slow period {}", slow.period),
                ),
            );
        }
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

    /// The fast EMA, `[indicators.fast]`, built.
    ///
    /// # Panics
    ///
    /// Panics if it is missing or of arguments it or Nautilus refuses,
    /// which `validate` refuses.
    #[must_use]
    pub fn fast_ema(&self) -> ExponentialMovingAverage {
        self.indicators.build(FAST)
    }

    /// The slow EMA, `[indicators.slow]`, built.
    ///
    /// # Panics
    ///
    /// As `fast_ema`.
    #[must_use]
    pub fn slow_ema(&self) -> ExponentialMovingAverage {
        self.indicators.build(SLOW)
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

        [indicators.fast]
        period = 10

        [indicators.slow]
        period = 30

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
                | ConfigError::MissingField { field }
                | ConfigError::UnsupportedField { field, .. }
                | ConfigError::InvalidReference { field, .. }
                | ConfigError::InvalidValue { field, .. } => field,
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    #[test]
    fn loads_from_toml() {
        let config = EmaCrossConfig::load(TOML).unwrap();
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
        assert_eq!(
            (config.fast_ema().period, config.slow_ema().period),
            (10, 30)
        );
    }

    #[test]
    fn reads_its_emas_from_the_indicators() {
        let toml = TOML
            .replace("period = 10", "period = 5")
            .replace("period = 30", "period = 15");
        let config = EmaCrossConfig::load(&toml).unwrap();
        assert_eq!(
            (config.fast_ema().period, config.slow_ema().period),
            (5, 15)
        );
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
    fn rejects_a_period_at_the_top_level() {
        let toml = format!("fast_period = 5\n{TOML}");
        let err = EmaCrossConfig::load(&toml).unwrap_err();
        assert!(err.to_string().contains("fast_period"), "{err}");
    }

    #[test]
    fn rejects_exits_in_the_toml() {
        let toml = format!("hold = \"1h\"\n{TOML}");
        let err = EmaCrossConfig::load(&toml).unwrap_err();
        assert!(err.to_string().contains("hold"), "{err}");
    }

    #[test]
    fn rejects_a_missing_ema() {
        let start = TOML.find("[indicators.slow]").unwrap();
        let end = TOML.find("[trade]").unwrap();
        let toml = format!("{}{}", &TOML[..start], &TOML[end..]);
        assert_eq!(error_fields(&toml), ["indicators.slow"]);
    }

    #[test]
    fn rejects_an_ema_of_average_true_range_arguments() {
        let toml = TOML.replacen(
            "period = 10",
            "period = 10\n        ma_type = \"SIMPLE\"",
            1,
        );
        assert_eq!(error_fields(&toml), ["indicators.fast"]);
    }

    #[test]
    fn rejects_a_zero_period_by_nautilus_check() {
        let toml = TOML.replace("period = 10", "period = 0");
        assert_eq!(error_fields(&toml), ["indicators.fast"]);
    }

    #[test]
    fn rejects_fast_period_not_below_slow() {
        let toml = TOML.replace("period = 10", "period = 30");
        assert_eq!(error_fields(&toml), ["indicators.fast.period"]);
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
        let toml = TOML
            .replace("period = 10", "period = 30")
            .replace("\"720h\"", "\"0s\"");
        assert_eq!(
            error_fields(&toml),
            ["data.warmup", "indicators.fast.period"]
        );
    }
}
