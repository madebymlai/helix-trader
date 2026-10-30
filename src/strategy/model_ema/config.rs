use anyhow::ensure;
use nautilus_model::identifiers::StrategyId;
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use crate::{model::config::ModelConfig, strategy::BarsConfig};

/// Config of the `model_ema` strategy, loaded from TOML: the bars, at the
/// top, and the model, in a `[model]` table.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct ModelEmaConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    /// The bars read, their warmup and their session: the features are
    /// read from the bars of every instrument in `instrument_ids`.
    #[serde(flatten)]
    pub bars: BarsConfig,
    /// The model: its decisions, targets, training, margins and
    /// learner. The trades are on the target of its label, of the
    /// targets' instrument and size.
    pub model: ModelConfig,
}

impl ModelEmaConfig {
    /// # Errors
    ///
    /// Returns an error if the bars are inconsistent (see
    /// `BarsConfig::validate`), the model is invalid for them (see
    /// `ModelConfig::validate`), or the base has no strategy id, which
    /// keys the model's store.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.bars.validate()?;
        self.model.validate(&self.bars.data_requirements())?;
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
        strategy_id: Some(StrategyId::from("MODEL_EMA-001")),
        order_id_tag: Some("001".to_string()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_model::data::BarType;

    use super::*;
    use crate::model::{decide::DecisionRule, learner::LearnerConfig};

    const TOML: &str = r#"
        instrument_ids = ["XAUUSD.ICMARKETS"]
        bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" }
        warmup = "1h"

        [model]
        decision = { every = "1h" }
        targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "30m" }
        label = "horizon"
        retrain = "24h"
        window = "48h"
        holdout = "12h"
        margins = { long = 0.5, short = 0.5 }
        min_pnl = 0.5
        min_trades = 30
        learner = { kind = "baseline" }
    "#;

    #[test]
    fn loads_the_bars_and_the_model_from_toml() {
        let config: ModelEmaConfig = toml::from_str(TOML).unwrap();
        config.validate().unwrap();
        assert_eq!(
            config.bars.data_requirements().bar_types,
            [BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL")]
        );
        assert_eq!(
            config.model.targets.horizon,
            Some(SignedDuration::from_mins(30))
        );
        assert_eq!(
            config.model.decision,
            DecisionRule::Every(SignedDuration::from_hours(1))
        );
        assert_eq!(config.model.learner, LearnerConfig::Baseline {});
        assert_eq!(
            config.base.strategy_id,
            Some(StrategyId::from("MODEL_EMA-001"))
        );
    }

    #[test]
    fn rejects_a_misspelt_field_of_the_model_at_load() {
        let toml = TOML.replace("min_pnl", "min_pn");
        let err = toml::from_str::<ModelEmaConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("min_pn"), "{err}");
    }

    #[test]
    fn rejects_targets_on_an_instrument_not_read() {
        let toml = TOML.replace(
            r#"targets = { instrument_id = "XAUUSD.ICMARKETS""#,
            r#"targets = { instrument_id = "EURUSD.ICMARKETS""#,
        );
        let config: ModelEmaConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("EURUSD.ICMARKETS"), "{err}");
    }

    /// A trade on a horizon of an hour, decided every hour, would still be
    /// open at the next decision.
    #[test]
    fn rejects_a_target_not_shorter_than_the_decision_cadence() {
        let toml = TOML.replace(r#"horizon = "30m""#, r#"horizon = "1h""#);
        let config: ModelEmaConfig = toml::from_str(&toml).unwrap();
        let err = format!("{:#}", config.validate().unwrap_err());
        assert!(
            err.contains("horizon 1h") && err.contains("cadence 1h"),
            "{err}"
        );
    }

    #[test]
    fn rejects_inconsistent_bars() {
        let toml = TOML.replace(
            r#"instrument_ids = ["XAUUSD.ICMARKETS"]"#,
            r#"instrument_ids = ["XAUUSD.ICMARKETS", "ESM4.CME"]"#,
        );
        let config: ModelEmaConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("venue"), "{err}");
    }

    /// A meta model is a `[model.meta]` table, and no more: a meta of the
    /// meta is an unknown field. It learns the barrier trades of a primary
    /// labelled on the barrier.
    #[test]
    fn loads_a_meta_model_from_its_table_alone_and_no_deeper() {
        let barrier =
            r#"barrier = { take_profit = { bps = 10 }, stop = { bps = 10 }, limit = "45m" } }"#;
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
                r#"horizon = "30m" }"#,
                &format!(r#"horizon = "30m", {barrier}"#)
            )
            .replace(r#"label = "horizon""#, r#"label = "barrier""#)
        );

        let config: ModelEmaConfig = toml::from_str(&toml).unwrap();
        config.validate().unwrap();
        assert_eq!(config.model.meta.unwrap().threshold, 0.55);

        let nested = format!("{toml}\n[model.meta.meta]\nthreshold = 0.5\n");
        let err = toml::from_str::<ModelEmaConfig>(&nested).unwrap_err();
        assert!(err.to_string().contains("unknown field `meta`"), "{err}");
    }
}
