use anyhow::{Context, ensure};
use jiff::SignedDuration;
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
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
    /// from the quotes, so priced BID, ASK or MID, or, with `composite`,
    /// from the instrument's source bars, priced as those.
    pub bar_spec: BarSpecification,
    /// The source bars of a composite bar type, if the bars are built from
    /// bars rather than quotes: `{ step = 1, aggregation = "MINUTE",
    /// aggregation_source = "EXTERNAL" }` makes
    /// `<instrument>-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` of a `1-HOUR`
    /// `LAST` bar spec, as OKX's 1-minute candles are.
    #[serde(default)]
    pub composite: Option<CompositeSource>,
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

    /// One internal bar type per instrument, composite if built from
    /// source bars.
    ///
    /// # Panics
    ///
    /// Panics if the config has invalid composite source bars, which
    /// `validate` rejects.
    #[must_use]
    pub fn bar_types(&self) -> Vec<BarType> {
        self.try_bar_types()
            .expect("composite source bars validated")
    }

    /// The bar types, or an error if the composite source bars are
    /// invalid: Nautilus checks them in `BarType::new_composite_checked`.
    fn try_bar_types(&self) -> anyhow::Result<Vec<BarType>> {
        self.instrument_ids
            .iter()
            .map(|&id| match self.composite {
                None => Ok(BarType::new(id, self.bar_spec, AggregationSource::Internal)),
                Some(source) => BarType::new_composite_checked(
                    id,
                    self.bar_spec,
                    AggregationSource::Internal,
                    source.step,
                    source.aggregation,
                    source.aggregation_source,
                )
                .with_context(|| format!("invalid composite source bars {source:?}")),
            })
            .collect()
    }

    /// # Errors
    ///
    /// Returns an error if the config is inconsistent: no instruments,
    /// several venues, a bar spec not aggregated on time or, built from
    /// quotes, not priced on them (BID, ASK or MID), invalid composite
    /// source bars, a session together with composite bars, periods out of
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
            self.composite.is_some()
                || matches!(
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
        self.try_bar_types()?;
        // Live, composite bars arrive after their close, so a session would
        // drop its last bar there but not in a backtest.
        ensure!(
            self.session.is_none() || self.composite.is_none(),
            "a session cannot be combined with composite bars"
        );
        ensure!(self.trade_size.is_positive(), "trade size must be positive");
        ensure!(self.warmup.is_positive(), "warmup must be positive");
        self.session.as_ref().map_or(Ok(()), Session::validate)
    }
}

/// The source bars of a composite bar type: the `composite_*` arguments of
/// Nautilus' `BarType::new_composite`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositeSource {
    pub step: usize,
    pub aggregation: BarAggregation,
    pub aggregation_source: AggregationSource,
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
    fn leaves_the_oms_to_the_venue() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        assert_eq!(config.base.oms_type, None);
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

    const COMPOSITE_TOML: &str = r#"
        instrument_ids = ["BTC-USDT-SWAP.OKX"]
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
        composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
        trade_size = "0.01"
        warmup = "2h"
    "#;

    #[test]
    fn bars_built_from_source_bars_are_a_composite_bar_type_priced_as_their_source() {
        let config: EmaCrossConfig = toml::from_str(COMPOSITE_TOML).unwrap();
        config.validate().unwrap();
        assert_eq!(
            config.bar_types(),
            [BarType::from(
                "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"
            )]
        );
    }

    #[test]
    fn rejects_invalid_source_bars() {
        let toml = COMPOSITE_TOML.replace(
            "step = 1, aggregation = \"MINUTE\"",
            "step = 60, aggregation = \"MINUTE\"",
        );
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("composite"), "{err}");
    }

    #[test]
    fn rejects_a_session_with_composite_bars() {
        let toml = format!(
            "{COMPOSITE_TOML}\nsession = {{ time_zone = \"UTC\", open = \"08:00\", close = \"16:00\" }}"
        );
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("session") && err.contains("composite"),
            "{err}"
        );
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
