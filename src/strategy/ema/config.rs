use anyhow::ensure;
use jiff::SignedDuration;
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::{AggregationSource, PriceType},
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use super::session::Session;
use crate::strategy::DataRequirements;

/// Config of the EMA crossover strategy, loaded from TOML.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct EmaCrossConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    /// The contracts traded and stored in the catalog.
    pub instrument_ids: Vec<InstrumentId>,
    /// The bars, the same for every instrument: time bars Nautilus builds
    /// from the quotes, so priced BID, ASK or MID.
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
    /// The hours traded: only bars wholly inside count. Without one, as on
    /// a market open around the clock, every bar counts.
    #[serde(default)]
    pub session: Option<Session>,
}

impl EmaCrossConfig {
    /// The bars, hence the instruments, and the warmup the strategy needs
    /// from data.
    #[must_use]
    pub fn data_requirements(&self) -> DataRequirements {
        DataRequirements {
            bar_types: self.bar_types(),
            warmup: self.warmup,
        }
    }

    /// One internal bar type per instrument.
    #[must_use]
    pub fn bar_types(&self) -> Vec<BarType> {
        self.instrument_ids
            .iter()
            .map(|id| BarType::new(*id, self.bar_spec, AggregationSource::Internal))
            .collect()
    }

    /// # Errors
    ///
    /// Returns an error if the config is inconsistent: no instruments,
    /// several venues, a bar spec not aggregated on time or not priced on
    /// the quotes (BID, ASK or MID), periods out of
    /// order, a non-positive size or warmup, a session, if any, not opening
    /// before it closes.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.instrument_ids.is_empty(), "no instruments");
        let venue = self.instrument_ids[0].venue;
        ensure!(
            self.instrument_ids.iter().all(|id| id.venue == venue),
            "instruments span more than one venue"
        );
        // The session needs the bar's span, so its length.
        ensure!(
            self.bar_spec.is_time_aggregated(),
            "bar spec {} is not aggregated on time",
            self.bar_spec
        );
        ensure!(
            matches!(
                self.bar_spec.price_type,
                PriceType::Bid | PriceType::Ask | PriceType::Mid
            ),
            "bar spec {} is not priced BID, ASK or MID: its bars are built from quotes",
            self.bar_spec
        );
        ensure!(
            self.fast_period > 0 && self.fast_period < self.slow_period,
            "fast period must be positive and below the slow period"
        );
        ensure!(self.trade_size.is_positive(), "trade size must be positive");
        ensure!(self.warmup.is_positive(), "warmup must be positive");
        self.session.as_ref().map_or(Ok(()), Session::validate)
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
        assert_eq!(
            config.bar_types()[1].to_string(),
            "ESM4.CME-1-HOUR-MID-INTERNAL"
        );
    }

    #[test]
    fn states_its_data_requirements() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        assert_eq!(
            config.data_requirements(),
            DataRequirements {
                bar_types: vec![
                    BarType::from("ESH4.CME-1-HOUR-MID-INTERNAL"),
                    BarType::from("ESM4.CME-1-HOUR-MID-INTERNAL")
                ],
                warmup: SignedDuration::from_hours(720),
            }
        );
    }

    #[test]
    fn loads_without_a_session() {
        let toml = TOML.replace(
            "session = { time_zone = \"America/Chicago\", open = \"08:30\", close = \"15:00\" }",
            "",
        );
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        config.validate().unwrap();
        assert!(config.session.is_none());
    }

    #[test]
    fn rejects_an_unknown_time_zone_at_load() {
        let toml = TOML.replace("America/Chicago", "America/Gotham");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("America/Gotham"), "{err}");
    }

    #[test]
    fn rejects_a_session_not_opening_before_its_close() {
        let toml = TOML.replace("open = \"08:30\"", "open = \"15:00\"");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("session open"), "{err}");
    }

    #[test]
    fn rejects_a_bar_spec_not_aggregated_on_time() {
        let toml = TOML.replace(
            "step = 1, aggregation = \"HOUR\"",
            "step = 100, aggregation = \"TICK\"",
        );
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("time"), "{err}");
    }

    #[test]
    fn accepts_bars_of_bid_and_ask_quotes() {
        for price_type in ["BID", "ASK"] {
            let toml = TOML.replace("\"MID\"", &format!("\"{price_type}\""));
            let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
            config.validate().unwrap();
        }
    }

    #[test]
    fn rejects_bars_of_last_prices_which_quotes_do_not_build() {
        let toml = TOML.replace("\"MID\"", "\"LAST\"");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("LAST"), "{err}");
    }

    #[test]
    fn rejects_an_invalid_cadence_at_load() {
        let toml = TOML.replace(
            "step = 1, aggregation = \"HOUR\"",
            "step = 60, aggregation = \"MINUTE\"",
        );
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
