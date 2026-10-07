use anyhow::ensure;
use jiff::SignedDuration;
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::AggregationSource,
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

/// Config of the EMA crossover strategy, loaded from TOML.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct EmaCrossConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    /// The contracts traded and stored in the catalog.
    pub instrument_ids: Vec<InstrumentId>,
    /// The cadence, the same for every instrument.
    pub bar_spec: BarSpecification,
    #[serde(default = "default_fast_period")]
    #[builder(default = default_fast_period())]
    pub fast_period: usize,
    #[serde(default = "default_slow_period")]
    #[builder(default = default_slow_period())]
    pub slow_period: usize,
    /// Position held per instrument, long or short.
    pub trade_size: Quantity,
    /// How far back the warmup history starts.
    pub warmup: SignedDuration,
}

impl EmaCrossConfig {
    /// One external bar type per instrument.
    #[must_use]
    pub fn bar_types(&self) -> Vec<BarType> {
        self.instrument_ids
            .iter()
            .map(|id| BarType::new(*id, self.bar_spec, AggregationSource::External))
            .collect()
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.instrument_ids.is_empty(), "no instruments");
        let venue = self.instrument_ids[0].venue;
        ensure!(
            self.instrument_ids.iter().all(|id| id.venue == venue),
            "instruments span more than one venue"
        );
        ensure!(
            self.fast_period > 0 && self.fast_period < self.slow_period,
            "fast period must be positive and below the slow period"
        );
        ensure!(self.trade_size.is_positive(), "trade size must be positive");
        ensure!(self.warmup.is_positive(), "warmup must be positive");
        Ok(())
    }
}

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
    use super::*;

    const TOML: &str = r#"
        instrument_ids = ["ESH4.CME", "ESM4.CME"]
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
        trade_size = "1"
        warmup = "720h"
    "#;

    #[test]
    fn loads_from_toml_with_defaults() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        config.validate().unwrap();
        assert_eq!(config.fast_period, 10);
        assert_eq!(config.slow_period, 30);
        assert_eq!(
            config.bar_types()[1].to_string(),
            "ESM4.CME-1-HOUR-LAST-EXTERNAL"
        );
    }

    #[test]
    fn rejects_an_invalid_cadence_at_load() {
        let toml = TOML.replace("step = 1, aggregation = \"HOUR\"", "step = 60, aggregation = \"MINUTE\"");
        assert!(toml::from_str::<EmaCrossConfig>(&toml).is_err());
    }

    #[test]
    fn rejects_instruments_on_two_venues() {
        let toml = TOML.replace("ESM4.CME", "ESM4.EUREX");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_fast_period_not_below_slow() {
        let toml = format!("{TOML}\nfast_period = 30");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        assert!(config.validate().is_err());
    }
}
