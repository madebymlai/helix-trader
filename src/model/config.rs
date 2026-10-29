//! The model section of a strategy's TOML: when the strategy decides, the
//! targets of its decisions and the one its model learns, the window a
//! model is trained on, the gate it must pass, the thresholds that turn
//! its probability into a trade, and its learner.

use anyhow::ensure;
use jiff::SignedDuration;
use serde::Deserialize;

use super::{
    decide::DecisionRule,
    learner::LearnerConfig,
    targets::{Label, TargetsConfig},
};
use crate::strategy::DataRequirements;

/// A strategy's model, loaded from TOML, such as
///
/// ```toml
/// [model]
/// decision = { every = "15m" }
/// targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "1h" }
/// label = "horizon"
/// window = "720h"
/// holdout = "120h"
/// thresholds = { long = 0.6, short = 0.4 }
/// min_score = 0.55
/// learner = { kind = "lightgbm" }
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    /// When the strategy decides.
    pub decision: DecisionRule,
    /// The targets of each decision, read from orders in a dataset run.
    pub targets: TargetsConfig,
    /// The target the model learns: whether its long gains.
    pub label: Label,
    /// How far back from a retrain its rows go, such as `"720h"`.
    pub window: SignedDuration,
    /// The last part of the window the model is scored on and not fitted
    /// on, such as `"120h"`.
    pub holdout: SignedDuration,
    /// The probabilities from which a prediction is a trade.
    pub thresholds: Thresholds,
    /// The lowest holdout AUC a model is published with.
    pub min_score: f64,
    /// The learner and its settings.
    pub learner: LearnerConfig,
}

/// A prediction at or above `long` is a long, at or below `short` a short,
/// between them no trade.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Thresholds {
    pub long: f64,
    pub short: f64,
}

impl ModelConfig {
    /// Checks the section against itself and against the strategy's data
    /// `requirements`.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule or the targets are invalid
    /// (see `DecisionRule::validate`, `TargetsConfig::validate`), the
    /// targets trade an instrument the strategy does not read, the label
    /// names a target the config lacks, a target (the barrier's limit or
    /// the horizon) is not shorter than the window, the holdout is not
    /// positive or leaves no fitted row (it and the longest target not
    /// shorter than the window), the thresholds are not probabilities with
    /// the short below the long, or the minimum score is not an AUC.
    pub fn validate(&self, requirements: &DataRequirements) -> anyhow::Result<()> {
        self.decision.validate()?;
        self.targets.validate()?;
        let traded = self.targets.instrument_id;
        ensure!(
            requirements.instrument_ids().contains(&traded),
            "the targets trade {traded}, which the strategy does not read"
        );
        ensure!(
            self.targets.label_index(self.label).is_some(),
            "the label {:?} names a target the targets do not have",
            self.label
        );
        let longest = self.targets.settles_within();
        ensure!(
            longest < self.window,
            "the longest target {longest:#} is not shorter than the window {:#}",
            self.window
        );
        ensure!(
            self.holdout.is_positive() && self.holdout + longest < self.window,
            "the holdout {:#} leaves no fitted row in the window {:#} \
             after the longest target {longest:#}",
            self.holdout,
            self.window
        );
        let Thresholds { long, short } = self.thresholds;
        ensure!(
            (0.0..=1.0).contains(&short) && (0.0..=1.0).contains(&long) && short < long,
            "the thresholds short {short} and long {long} are not probabilities \
             with the short below the long"
        );
        ensure!(
            (0.0..=1.0).contains(&self.min_score),
            "the minimum score {} is not an AUC",
            self.min_score
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;

    use super::*;
    use crate::strategy::TickSeries;

    const GOLD: &str = "XAUUSD.ICMARKETS";

    /// A model section with a barrier closed after 2 hours and a horizon
    /// of 1 hour, on a window of 48 hours, 12 of them held out.
    const SECTION: &str = r#"
        decision = { every = "1h" }
        targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "1h", barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "2h" } }
        label = "barrier"
        window = "48h"
        holdout = "12h"
        thresholds = { long = 0.6, short = 0.4 }
        min_score = 0.55
        learner = { kind = "fake" }
    "#;

    /// The section with `line` added or, if its key is there, replacing it.
    fn section(line: &str) -> String {
        let key = line.split('=').next().unwrap().trim();
        let kept = SECTION
            .lines()
            .filter(|kept| kept.split('=').next().unwrap().trim() != key);
        kept.chain([line]).collect::<Vec<_>>().join("\n")
    }

    /// The requirements of a strategy reading the quotes of `instrument`.
    fn reading(instrument: &str) -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![
                toml::from_str::<TickSeries>(&format!(
                    r#"data = "quotes"
                       instrument_id = "{instrument}""#
                ))
                .unwrap(),
            ],
            warmup: SignedDuration::ZERO,
        }
    }

    /// The error of loading `toml` for a strategy reading gold, if any.
    fn load(toml: &str) -> anyhow::Result<ModelConfig> {
        let config: ModelConfig = toml::from_str(toml)?;
        config.validate(&reading(GOLD))?;
        Ok(config)
    }

    fn refused(toml: &str) -> String {
        format!("{:#}", load(toml).unwrap_err())
    }

    #[test]
    fn a_section_reads_from_toml() {
        let config = load(SECTION).unwrap();

        assert_eq!(
            config.decision,
            DecisionRule::Every(SignedDuration::from_hours(1))
        );
        assert_eq!(config.targets.instrument_id, InstrumentId::from(GOLD));
        assert_eq!(config.label, Label::Barrier);
        assert_eq!(config.window, SignedDuration::from_hours(48));
        assert_eq!(config.holdout, SignedDuration::from_hours(12));
        assert_eq!(
            config.thresholds,
            Thresholds {
                long: 0.6,
                short: 0.4
            }
        );
        assert_eq!(config.min_score, 0.55);
        assert_eq!(config.learner, LearnerConfig::Fake {});
    }

    #[test]
    fn a_section_refuses_an_unknown_field() {
        assert!(refused(&section("retrain_every = \"24h\"")).contains("retrain_every"));
    }

    #[test]
    fn a_horizon_not_shorter_than_the_window_is_refused() {
        let toml = section(
            r#"targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "48h" }"#,
        )
        .replace(r#"label = "barrier""#, r#"label = "horizon""#);

        assert!(
            refused(&toml).contains("not shorter than the window"),
            "{}",
            refused(&toml)
        );
    }

    #[test]
    fn a_limit_not_shorter_than_the_window_is_refused() {
        let toml = section(
            r#"targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "49h" } }"#,
        );

        assert!(
            refused(&toml).contains("not shorter than the window"),
            "{}",
            refused(&toml)
        );
    }

    #[test]
    fn a_holdout_leaving_no_fitted_row_is_refused() {
        for holdout in ["46h", "0h"] {
            let toml = section(&format!(r#"holdout = "{holdout}""#));
            assert!(refused(&toml).contains("holdout"), "{holdout}");
        }
        assert!(load(&section(r#"holdout = "45h""#)).is_ok());
    }

    #[test]
    fn thresholds_out_of_order_are_refused() {
        for thresholds in [
            "{ long = 0.4, short = 0.6 }",
            "{ long = 0.5, short = 0.5 }",
            "{ long = 1.2, short = 0.4 }",
            "{ long = 0.6, short = -0.1 }",
        ] {
            let toml = section(&format!("thresholds = {thresholds}"));
            assert!(refused(&toml).contains("thresholds"), "{thresholds}");
        }
    }

    #[test]
    fn a_minimum_score_outside_an_auc_is_refused() {
        for score in ["1.5", "-0.1"] {
            let toml = section(&format!("min_score = {score}"));
            assert!(refused(&toml).contains("minimum score"), "{score}");
        }
    }

    #[test]
    fn a_traded_instrument_the_strategy_does_not_read_is_refused() {
        let config: ModelConfig = toml::from_str(SECTION).unwrap();

        let err = format!(
            "{:#}",
            config.validate(&reading("EURUSD.ICMARKETS")).unwrap_err()
        );

        assert!(err.contains(GOLD), "{err}");
    }

    #[test]
    fn a_label_of_a_target_the_config_lacks_is_refused() {
        let toml = section(
            r#"targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "1h" }"#,
        );

        assert!(refused(&toml).contains("label"), "{}", refused(&toml));
    }
}
