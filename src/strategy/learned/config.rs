use anyhow::ensure;
use jiff::SignedDuration;
use nautilus_model::identifiers::StrategyId;
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use crate::{model::config::ModelConfig, strategy::BarsConfig};

/// Config of the learned strategy, loaded from TOML: the bars, at the top,
/// how long a trade is held, and the model, in a `[model]` table.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct LearnedConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    /// The bars read, their warmup and their session: the features are
    /// read from the bars of every instrument in `instrument_ids`.
    #[serde(flatten)]
    pub bars: BarsConfig,
    /// How long a trade is held after it is sent, such as `"1h"`.
    pub hold: SignedDuration,
    /// The model: its decisions, targets, training, thresholds and
    /// learner. The trades are of the targets' instrument and size.
    pub model: ModelConfig,
}

impl LearnedConfig {
    /// # Errors
    ///
    /// Returns an error if the bars are inconsistent (see
    /// `BarsConfig::validate`), the model is invalid for them (see
    /// `ModelConfig::validate`), the hold is not positive, or the base has
    /// no strategy id, which keys the model's store.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.bars.validate()?;
        self.model.validate(&self.bars.data_requirements())?;
        ensure!(self.hold.is_positive(), "hold must be positive");
        ensure!(
            self.base.strategy_id.is_some(),
            "the strategy has no id to key its models"
        );
        Ok(())
    }
}

/// The strategy's id and order tag. `oms_type` is left unset: Nautilus
/// keeps the strategy's positions as the execution client of the venue
/// does, netting or hedging, in a backtest as live.
fn default_base() -> StrategyConfig {
    StrategyConfig {
        strategy_id: Some(StrategyId::from("LEARNED-001")),
        order_id_tag: Some("001".to_string()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::BarType;

    use super::*;
    use crate::model::{decide::DecisionRule, learner::LearnerConfig};

    const TOML: &str = r#"
        instrument_ids = ["XAUUSD.ICMARKETS"]
        bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" }
        warmup = "1h"
        hold = "30m"

        [model]
        decision = { every = "1h" }
        targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "1h" }
        label = "horizon"
        window = "48h"
        holdout = "12h"
        thresholds = { long = 0.6, short = 0.4 }
        min_score = 0.5
        learner = { kind = "baseline" }
    "#;

    #[test]
    fn loads_the_bars_the_hold_and_the_model_from_toml() {
        let config: LearnedConfig = toml::from_str(TOML).unwrap();
        config.validate().unwrap();
        assert_eq!(
            config.bars.data_requirements().bar_types,
            [BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL")]
        );
        assert_eq!(config.hold, SignedDuration::from_mins(30));
        assert_eq!(
            config.model.decision,
            DecisionRule::Every(SignedDuration::from_hours(1))
        );
        assert_eq!(config.model.learner, LearnerConfig::Baseline {});
        assert_eq!(
            config.base.strategy_id,
            Some(StrategyId::from("LEARNED-001"))
        );
    }

    #[test]
    fn rejects_a_misspelt_field_of_the_model_at_load() {
        let toml = TOML.replace("min_score", "min_scor");
        let err = toml::from_str::<LearnedConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("min_scor"), "{err}");
    }

    #[test]
    fn rejects_targets_on_an_instrument_not_read() {
        let toml = TOML.replace(
            r#"targets = { instrument_id = "XAUUSD.ICMARKETS""#,
            r#"targets = { instrument_id = "EURUSD.ICMARKETS""#,
        );
        let config: LearnedConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("EURUSD.ICMARKETS"), "{err}");
    }

    #[test]
    fn rejects_a_non_positive_hold() {
        let toml = TOML.replace(r#"hold = "30m""#, r#"hold = "0s""#);
        let config: LearnedConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("hold"), "{err}");
    }

    #[test]
    fn rejects_inconsistent_bars() {
        let toml = TOML.replace(
            r#"instrument_ids = ["XAUUSD.ICMARKETS"]"#,
            r#"instrument_ids = ["XAUUSD.ICMARKETS", "ESM4.CME"]"#,
        );
        let config: LearnedConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("venue"), "{err}");
    }
}
