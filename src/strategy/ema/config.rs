use anyhow::ensure;
use nautilus_model::{identifiers::StrategyId, types::Quantity};
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use crate::strategy::BarsConfig;

/// Config of the EMA crossover strategy, loaded from TOML.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct EmaCrossConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    /// The bars traded, their warmup and their session.
    #[serde(flatten)]
    pub bars: BarsConfig,
    #[serde(default = "default_fast_period")]
    #[builder(default = default_fast_period())]
    pub fast_period: usize,
    #[serde(default = "default_slow_period")]
    #[builder(default = default_slow_period())]
    pub slow_period: usize,
    /// Position held per instrument, long or short.
    pub trade_size: Quantity,
}

impl EmaCrossConfig {
    /// # Errors
    ///
    /// Returns an error if the bars are inconsistent (see
    /// `BarsConfig::validate`), the periods out of order or the size not
    /// positive.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.bars.validate()?;
        ensure!(
            self.fast_period > 0 && self.fast_period < self.slow_period,
            "fast period must be positive and below the slow period"
        );
        ensure!(self.trade_size.is_positive(), "trade size must be positive");
        Ok(())
    }
}

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

const fn default_fast_period() -> usize {
    10
}

const fn default_slow_period() -> usize {
    30
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::BarType;

    use super::*;

    const TOML: &str = r#"
        instrument_ids = ["ESH4.CME", "ESM4.CME"]
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
        trade_size = "1"
        warmup = "720h"
        session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
    "#;

    #[test]
    fn loads_from_toml_with_defaults() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        config.validate().unwrap();
        assert_eq!(config.fast_period, 10);
        assert_eq!(config.slow_period, 30);
        assert_eq!(config.trade_size, Quantity::from(1));
    }

    #[test]
    fn loads_the_bars_from_the_top_of_the_toml() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        assert_eq!(
            config.bars.data_requirements().bar_types,
            [
                BarType::from("ESH4.CME-1-HOUR-MID-INTERNAL"),
                BarType::from("ESM4.CME-1-HOUR-MID-INTERNAL")
            ]
        );
        assert_eq!(config.bars.session.unwrap().open.to_string(), "08:30:00");
    }

    #[test]
    fn leaves_the_oms_to_the_venue() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        assert_eq!(config.base.oms_type, None);
    }

    #[test]
    fn rejects_a_misspelt_session_at_load() {
        let toml = TOML.replace("session =", "sesion =");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("sesion"), "{err}");
    }

    #[test]
    fn rejects_a_misspelt_period_at_load() {
        let toml = format!("{TOML}\nfast_perod = 5");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("fast_perod"), "{err}");
    }

    #[test]
    fn rejects_an_unknown_time_zone_at_load() {
        let toml = TOML.replace("America/Chicago", "America/Gotham");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("America/Gotham"), "{err}");
    }

    #[test]
    fn rejects_inconsistent_bars() {
        let toml = TOML.replace("ESM4.CME", "ESM4.EUREX");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("venue"), "{err}");
    }

    #[test]
    fn rejects_fast_period_not_below_slow() {
        let toml = format!("{TOML}\nfast_period = 30");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_a_non_positive_trade_size() {
        let toml = TOML.replace("trade_size = \"1\"", "trade_size = \"0\"");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("trade size"), "{err}");
    }
}
