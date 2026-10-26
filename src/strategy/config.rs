use anyhow::{Context, ensure};
use jiff::SignedDuration;
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
};
use serde::Deserialize;

use super::{DataRequirements, session::Session};

/// The config every strategy shares: the bars it trades on, their warmup
/// and their session. A strategy config embeds it with
/// `#[serde(flatten)]`, so its fields sit at the top of the strategy's TOML,
/// and the strategy config's `#[serde(deny_unknown_fields)]` rejects any key
/// that neither takes.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
pub struct BarsConfig {
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
    /// How far back the warmup history starts.
    pub warmup: SignedDuration,
    /// The hours traded: only bars wholly inside count. Without one, as on
    /// a market open around the clock, every bar counts.
    #[serde(default)]
    pub session: Option<Session>,
}

impl BarsConfig {
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
    /// source bars, a session together with composite bars, a non-positive
    /// warmup, a session, if any, not opening before it closes.
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
        self.try_bar_types()?;
        // Live, composite bars arrive after their close, so a session would
        // drop its last bar there but not in a backtest.
        ensure!(
            self.session.is_none() || self.composite.is_none(),
            "a session cannot be combined with composite bars"
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    const TOML: &str = r#"
        instrument_ids = ["ESH4.CME", "ESM4.CME"]
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
        warmup = "720h"
        session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
    "#;

    #[test]
    fn loads_from_toml() {
        let config: BarsConfig = toml::from_str(TOML).unwrap();
        config.validate().unwrap();
        assert_eq!(
            config.bar_types()[1].to_string(),
            "ESM4.CME-1-HOUR-MID-INTERNAL"
        );
    }

    #[test]
    fn states_its_data_requirements() {
        let config: BarsConfig = toml::from_str(TOML).unwrap();
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
        let config: BarsConfig = toml::from_str(&toml).unwrap();
        config.validate().unwrap();
        assert!(config.session.is_none());
    }

    #[test]
    fn rejects_an_unknown_time_zone_at_load() {
        let toml = TOML.replace("America/Chicago", "America/Gotham");
        let err = toml::from_str::<BarsConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("America/Gotham"), "{err}");
    }

    #[test]
    fn rejects_a_session_not_opening_before_its_close() {
        let toml = TOML.replace("open = \"08:30\"", "open = \"15:00\"");
        let config: BarsConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("session open"), "{err}");
    }

    #[test]
    fn rejects_a_bar_spec_not_aggregated_on_time() {
        let toml = TOML.replace(
            "step = 1, aggregation = \"HOUR\"",
            "step = 100, aggregation = \"TICK\"",
        );
        let config: BarsConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("time"), "{err}");
    }

    #[test]
    fn accepts_bars_of_bid_and_ask_quotes() {
        for price_type in ["BID", "ASK"] {
            let toml = TOML.replace("\"MID\"", &format!("\"{price_type}\""));
            let config: BarsConfig = toml::from_str(&toml).unwrap();
            config.validate().unwrap();
        }
    }

    #[test]
    fn rejects_bars_of_last_prices_which_quotes_do_not_build() {
        let toml = TOML.replace("\"MID\"", "\"LAST\"");
        let config: BarsConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("LAST"), "{err}");
    }

    const COMPOSITE_TOML: &str = r#"
        instrument_ids = ["BTC-USDT-SWAP.OKX"]
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
        composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
        warmup = "2h"
    "#;

    #[test]
    fn bars_built_from_source_bars_are_a_composite_bar_type_priced_as_their_source() {
        let config: BarsConfig = toml::from_str(COMPOSITE_TOML).unwrap();
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
        let config: BarsConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("composite"), "{err}");
    }

    #[test]
    fn rejects_a_session_with_composite_bars() {
        let toml = format!(
            "{COMPOSITE_TOML}\nsession = {{ time_zone = \"UTC\", open = \"08:00\", close = \"16:00\" }}"
        );
        let config: BarsConfig = toml::from_str(&toml).unwrap();
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
        assert!(toml::from_str::<BarsConfig>(&toml).is_err());
    }

    #[test]
    fn rejects_instruments_on_two_venues() {
        let toml = TOML.replace("ESM4.CME", "ESM4.EUREX");
        let config: BarsConfig = toml::from_str(&toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_a_non_positive_warmup() {
        let toml = TOML.replace("\"720h\"", "\"0s\"");
        let config: BarsConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("warmup"), "{err}");
    }
}
