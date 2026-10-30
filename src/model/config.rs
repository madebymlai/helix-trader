//! The model section of a strategy's TOML: when the strategy decides, the
//! targets of its decisions and the one its model learns, how often a
//! model is retrained and the window it is trained on, the gate it must
//! pass, the thresholds that turn its probability into a trade, and its
//! learner; and, optionally, the meta model filtering its trades.
//!
//! A strategy has at most two levels of models: a primary, its rule
//! (`RuleConfig`) or a model (`ModelConfig`), and one meta model
//! (`MetaConfig`) deciding whether to take the primary's trade. The types
//! hold no third: a meta section has no meta section of its own, and
//! writing one is refused at load as an unknown field.

use anyhow::ensure;
use jiff::SignedDuration;
use nautilus_model::{data::NautilusDataType, enums::OrderSide};
use rust_decimal::{Decimal, prelude::FromPrimitive};
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
/// retrain = "168h"
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
    /// How often the model is retrained, such as `"168h"`: at every multiple
    /// of it since the Unix epoch, in a backtest as live
    /// (`model::schedule`).
    pub retrain: SignedDuration,
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
    /// The meta model deciding whether to take each trade of this one, a
    /// `[model.meta]` table; none takes every trade.
    pub meta: Option<MetaConfig>,
}

/// The model section of a strategy whose primary is a rule of its own: the
/// strategy decides on a side when its rule signals
/// (`ModelHandle::decide_on`), and the meta model decides whether to take
/// that trade. Such as
///
/// ```toml
/// [model]
/// decision = "event"
/// targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", barrier = { .. } }
/// retrain = "168h"
///
/// [model.meta]
/// window = "720h"
/// holdout = "120h"
/// threshold = 0.55
/// min_score = 0.52
/// learner = { kind = "lightgbm" }
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct RuleConfig {
    /// When the strategy decides: its rule is asked then, and a decision is
    /// made only on the side it signals.
    pub decision: DecisionRule,
    /// The targets of each decision; a barrier, which the meta model learns.
    pub targets: TargetsConfig,
    /// How often the meta model is retrained, as `ModelConfig::retrain`.
    pub retrain: SignedDuration,
    /// The meta model.
    pub meta: MetaConfig,
}

/// A meta model: on each decision the primary makes a trade of, whether
/// the triple-barrier trade on the primary's side wins, learned from the
/// primary's features, its side and its probability. It is retrained on
/// the primary's schedule, right after the primary.
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct MetaConfig {
    /// How far back from a retrain its rows go; never before the
    /// primary's first model, whose predictions the rows need.
    pub window: SignedDuration,
    /// The last part of the window the model is scored on and not fitted
    /// on.
    pub holdout: SignedDuration,
    /// The probability from which the primary's trade is taken.
    pub threshold: f64,
    /// The lowest holdout AUC a model is published with.
    pub min_score: f64,
    /// The learner and its settings.
    pub learner: LearnerConfig,
    /// How a trade taken is sized from the probability; none trades the
    /// targets' size.
    pub size: Option<Sizing>,
}

/// A trade's size from the meta model's probability: a multiple of the
/// targets' size, `min` at the threshold, `max` at a probability of 1,
/// linear between, such as `size = { min = 0.5, max = 2 }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sizing {
    pub min: Decimal,
    pub max: Decimal,
}

/// A prediction at or above `long` is a long, at or below `short` a short,
/// between them no trade.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Thresholds {
    pub long: f64,
    pub short: f64,
}

impl Thresholds {
    /// The side of the trade a prediction of `probability` makes: a buy at
    /// or above `long`, a sell at or below `short`, none between them.
    #[must_use]
    pub fn side(&self, probability: f64) -> Option<OrderSide> {
        if probability >= self.long {
            Some(OrderSide::Buy)
        } else if probability <= self.short {
            Some(OrderSide::Sell)
        } else {
            None
        }
    }
}

impl ModelConfig {
    /// Checks the section against itself and against the strategy's data
    /// `requirements`.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule, the targets or the learner
    /// are invalid (see `DecisionRule::validate`, `TargetsConfig::validate`,
    /// `LearnerConfig::validate`), the retrain interval is not positive,
    /// the targets trade an instrument the strategy does not read, the
    /// barrier's ATR is on an instrument whose quotes it does not read
    /// (they build the ATR's bars) or warms over longer than its warmup,
    /// the label names a target the config lacks, a target (the barrier's
    /// limit or the horizon) is not shorter than the window, the holdout
    /// is not positive or leaves no fitted row (it and the longest target
    /// not shorter than the window), the thresholds are not probabilities
    /// with the short below the long, the minimum score is not an AUC, or
    /// the meta model is invalid (see `MetaConfig::validate`).
    pub fn validate(&self, requirements: &DataRequirements) -> anyhow::Result<()> {
        validate_decisions(self.decision, &self.targets, self.retrain, requirements)?;
        self.learner.validate()?;
        ensure!(
            self.targets.label_index(self.label).is_some(),
            "the label {:?} names a target the targets do not have",
            self.label
        );
        validate_window(self.window, self.holdout, &self.targets)?;
        let Thresholds { long, short } = self.thresholds;
        ensure!(
            (0.0..=1.0).contains(&short) && (0.0..=1.0).contains(&long) && short < long,
            "the thresholds short {short} and long {long} are not probabilities \
             with the short below the long"
        );
        validate_min_score(self.min_score)?;
        if let Some(meta) = &self.meta {
            meta.validate(&self.targets)?;
        }
        Ok(())
    }
}

impl RuleConfig {
    /// Checks the section against itself and against the strategy's data
    /// `requirements`.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule or the targets are invalid,
    /// the retrain interval is not positive, the targets trade an
    /// instrument the strategy does not read, the barrier's ATR is on
    /// quotes it does not read or warms over longer than its warmup (as
    /// `ModelConfig::validate`), or the meta model is invalid (see
    /// `MetaConfig::validate`).
    pub fn validate(&self, requirements: &DataRequirements) -> anyhow::Result<()> {
        validate_decisions(self.decision, &self.targets, self.retrain, requirements)?;
        self.meta.validate(&self.targets)
    }
}

impl MetaConfig {
    /// Checks the meta model against the primary's `targets`.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets have no barrier, the trade the meta
    /// model learns, the learner is invalid (see `LearnerConfig::validate`),
    /// a target is not shorter than the window, the holdout is not
    /// positive or leaves no fitted row, the threshold is not a
    /// probability, the minimum score is not an AUC, or the sizing's
    /// multiples are not positive with the minimum not above the maximum.
    pub fn validate(&self, targets: &TargetsConfig) -> anyhow::Result<()> {
        ensure!(
            targets.barrier.is_some(),
            "the meta model learns whether the primary's barrier trade wins, \
             and the targets have no barrier"
        );
        self.learner.validate()?;
        validate_window(self.window, self.holdout, targets)?;
        ensure!(
            (0.0..=1.0).contains(&self.threshold),
            "the meta threshold {} is not a probability",
            self.threshold
        );
        validate_min_score(self.min_score)?;
        if let Some(Sizing { min, max }) = self.size {
            ensure!(
                min.is_sign_positive() && !min.is_zero() && min <= max,
                "the meta sizing's multiples min {min} and max {max} are not positive \
                 with the min not above the max"
            );
        }
        Ok(())
    }

    /// The multiple of the targets' size a trade taken at `probability`
    /// is: one without sizing; with it, `min` at the threshold, `max` at a
    /// probability of 1, linear between.
    #[must_use]
    pub fn size(&self, probability: f64) -> Decimal {
        let Some(Sizing { min, max }) = self.size else {
            return Decimal::ONE;
        };
        let span = 1.0 - self.threshold;
        let along = if span > 0.0 {
            ((probability - self.threshold) / span).clamp(0.0, 1.0)
        } else {
            1.0
        };
        min + (max - min) * Decimal::from_f64(along).unwrap_or(Decimal::ONE)
    }
}

/// Checks what every model section has: the decision `rule`, the
/// `targets`, the `retrain` interval, against the strategy's
/// `requirements`.
fn validate_decisions(
    rule: DecisionRule,
    targets: &TargetsConfig,
    retrain: SignedDuration,
    requirements: &DataRequirements,
) -> anyhow::Result<()> {
    rule.validate()?;
    targets.validate(rule)?;
    ensure!(
        retrain.is_positive(),
        "the retrain interval {retrain:#} is not positive"
    );
    let traded = targets.instrument_id;
    ensure!(
        requirements.instrument_ids().contains(&traded),
        "the targets trade {traded}, which the strategy does not read"
    );
    if let Some(atr) = targets.atr_bars(rule)? {
        ensure!(
            requirements
                .catalog_series()
                .contains(&(NautilusDataType::QuoteTick, traded.to_string())),
            "the barrier's ATR is on {}, built from the quotes of {traded}, which the \
             strategy does not read",
            atr.bar_type
        );
        // A dataset run replays the warmup before its first day, and no
        // more: an ATR still cold then opens no target at the day's first
        // decisions, which a longer build would.
        let warms = i32::try_from(atr.period)
            .ok()
            .and_then(|period| atr.bar_type.spec().timedelta().checked_mul(period));
        ensure!(
            warms.is_some_and(|warms| warms <= requirements.warmup),
            "the barrier's ATR of {} bars of {} warms over longer than the warmup {:#}",
            atr.period,
            atr.bar_type,
            requirements.warmup
        );
    }
    Ok(())
}

/// Checks that the longest of `targets` is shorter than `window`, and
/// `holdout` positive and short enough to leave a fitted row after it.
fn validate_window(
    window: SignedDuration,
    holdout: SignedDuration,
    targets: &TargetsConfig,
) -> anyhow::Result<()> {
    let longest = targets.settles_within();
    ensure!(
        longest < window,
        "the longest target {longest:#} is not shorter than the window {window:#}"
    );
    ensure!(
        holdout.is_positive() && holdout + longest < window,
        "the holdout {holdout:#} leaves no fitted row in the window {window:#} \
         after the longest target {longest:#}"
    );
    Ok(())
}

fn validate_min_score(min_score: f64) -> anyhow::Result<()> {
    ensure!(
        (0.0..=1.0).contains(&min_score),
        "the minimum score {min_score} is not an AUC"
    );
    Ok(())
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
        retrain = "24h"
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
        assert_eq!(config.retrain, SignedDuration::from_hours(24));
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
    fn a_retrain_interval_not_positive_is_refused() {
        for retrain in ["0h", "-24h"] {
            let toml = section(&format!(r#"retrain = "{retrain}""#));
            assert!(refused(&toml).contains("retrain interval"), "{retrain}");
        }
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
    fn lightgbm_leaves_of_fewer_than_20_rows_are_refused() {
        let lightgbm = |rows: u32| {
            section(&format!(
                r#"learner = {{ kind = "lightgbm", min_data_in_leaf = {rows} }}"#
            ))
        };

        for rows in [1, 19] {
            assert!(
                refused(&lightgbm(rows)).contains("min_data_in_leaf"),
                "{}",
                refused(&lightgbm(rows))
            );
        }
        for rows in [20, 50] {
            assert!(load(&lightgbm(rows)).is_ok(), "{rows}");
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

    /// A barrier 2 ATRs away in the side's favour, 1.5 against it, its ATR
    /// `atr`, a table or none.
    fn atr_section(atr: Option<&str>) -> String {
        let atr = atr.map(|atr| format!(", atr = {atr}")).unwrap_or_default();
        section(&format!(
            r#"targets = {{ instrument_id = "XAUUSD.ICMARKETS", quantity = "1", barrier = {{ take_profit = {{ atr = 2.0 }}, stop = {{ atr = 1.5 }}, limit = "2h"{atr} }} }}"#
        ))
    }

    /// The requirements of a strategy reading the quotes of gold, warming
    /// for `warmup`.
    fn reading_gold_for(warmup: SignedDuration) -> DataRequirements {
        DataRequirements {
            warmup,
            ..reading(GOLD)
        }
    }

    /// Deciding every hour, the ATR's bars are hours: its default 14 warm
    /// in 14 hours, 3 in 3. A dataset run, which replays the warmup before
    /// its first day, would open no target at that day's first decisions
    /// after a shorter one.
    #[test]
    fn a_barrier_atr_warming_longer_than_the_warmup_is_refused() {
        for (atr, hours) in [(None, 14), (Some("{ period = 3 }"), 3)] {
            let config: ModelConfig = toml::from_str(&atr_section(atr)).unwrap();

            let shorter = SignedDuration::from_hours(hours - 1);
            let err = format!(
                "{:#}",
                config.validate(&reading_gold_for(shorter)).unwrap_err()
            );
            assert!(err.contains("warmup"), "{atr:?}: {err}");

            let warmup = SignedDuration::from_hours(hours);
            assert!(
                config.validate(&reading_gold_for(warmup)).is_ok(),
                "{atr:?}"
            );
        }
    }

    /// The ATR's bars are built from the quotes of the targets' instrument:
    /// a strategy replaying only its trades would leave it cold.
    #[test]
    fn a_barrier_atr_without_the_quotes_of_its_instrument_is_refused() {
        let config: ModelConfig = toml::from_str(&atr_section(None)).unwrap();
        let requirements = DataRequirements {
            ticks: vec![
                toml::from_str::<TickSeries>(&format!(
                    r#"data = "trades"
                       instrument_id = "{GOLD}""#
                ))
                .unwrap(),
            ],
            ..reading_gold_for(SignedDuration::from_hours(14))
        };

        let err = format!("{:#}", config.validate(&requirements).unwrap_err());

        assert!(err.contains("quotes"), "{err}");
    }

    #[test]
    fn a_barrier_atr_deciding_on_events_is_refused() {
        let toml =
            atr_section(None).replace(r#"decision = { every = "1h" }"#, r#"decision = "event""#);
        let config: ModelConfig = toml::from_str(&toml).unwrap();

        let err = format!(
            "{:#}",
            config
                .validate(&reading_gold_for(SignedDuration::from_hours(14)))
                .unwrap_err()
        );

        assert!(err.contains("ATR"), "{err}");
    }

    #[test]
    fn the_thresholds_make_a_probability_a_long_a_short_or_no_trade() {
        let thresholds = Thresholds {
            long: 0.6,
            short: 0.4,
        };

        assert_eq!(thresholds.side(0.6), Some(OrderSide::Buy));
        assert_eq!(thresholds.side(0.9), Some(OrderSide::Buy));
        assert_eq!(thresholds.side(0.4), Some(OrderSide::Sell));
        assert_eq!(thresholds.side(0.1), Some(OrderSide::Sell));
        assert_eq!(thresholds.side(0.5), None);
        assert_eq!(thresholds.side(0.59), None);
        assert_eq!(thresholds.side(f64::NAN), None);
    }

    /// A meta model on the section's barrier, held out its last 12 hours.
    const META: &str = r#"
        [meta]
        window = "36h"
        holdout = "12h"
        threshold = 0.55
        min_score = 0.52
        learner = { kind = "baseline" }
    "#;

    #[test]
    fn a_meta_section_reads_from_toml() {
        let config = load(&format!("{SECTION}\n{META}")).unwrap();

        let meta = config.meta.unwrap();
        assert_eq!(meta.window, SignedDuration::from_hours(36));
        assert_eq!(meta.holdout, SignedDuration::from_hours(12));
        assert_eq!(meta.threshold, 0.55);
        assert_eq!(meta.min_score, 0.52);
        assert_eq!(meta.learner, LearnerConfig::Baseline {});
        assert_eq!(meta.size, None);
    }

    #[test]
    fn a_section_without_meta_has_none() {
        assert_eq!(load(SECTION).unwrap().meta, None);
    }

    /// Two levels at most: a meta section has no meta section of its own.
    #[test]
    fn a_nested_meta_section_is_refused() {
        let nested = format!(
            "{SECTION}\n{META}\n{}",
            META.replace("[meta]", "[meta.meta]")
        );

        let err = refused(&nested);

        assert!(err.contains("unknown field `meta`"), "{err}");
    }

    #[test]
    fn a_meta_model_without_a_barrier_is_refused() {
        let toml = section(
            r#"targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", horizon = "1h" }"#,
        )
        .replace(r#"label = "barrier""#, r#"label = "horizon""#);

        let err = refused(&format!("{toml}\n{META}"));

        assert!(err.contains("barrier"), "{err}");
    }

    #[test]
    fn a_meta_window_holdout_threshold_or_score_out_of_range_is_refused() {
        for (line, why) in [
            (r#"window = "2h""#, "not shorter than the window"),
            (r#"holdout = "35h""#, "holdout"),
            ("threshold = 1.5", "threshold"),
            ("min_score = -0.1", "minimum score"),
            ("size = { min = 0, max = 2 }", "sizing"),
            ("size = { min = 2, max = 1 }", "sizing"),
        ] {
            let key = line.split('=').next().unwrap().trim();
            let meta: Vec<&str> = META
                .lines()
                .filter(|kept| kept.split('=').next().unwrap().trim() != key)
                .chain([line])
                .collect();
            let err = refused(&format!("{SECTION}\n{}", meta.join("\n")));
            assert!(err.contains(why), "{line}: {err}");
        }
    }

    #[test]
    fn a_rule_primarys_section_reads_its_meta_from_toml() {
        let toml = r#"
            decision = "event"
            targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "2h" } }
            retrain = "24h"

            [meta]
            window = "36h"
            holdout = "12h"
            threshold = 0.55
            min_score = 0.52
            learner = { kind = "fake" }
            size = { min = 0.5, max = 2 }
        "#;

        let config: RuleConfig = toml::from_str(toml).unwrap();
        config.validate(&reading(GOLD)).unwrap();

        assert_eq!(config.decision, DecisionRule::Event);
        assert_eq!(config.meta.learner, LearnerConfig::Fake {});
        assert_eq!(
            config.meta.size,
            Some(Sizing {
                min: Decimal::new(5, 1),
                max: Decimal::from(2)
            })
        );
    }

    #[test]
    fn a_rule_primarys_section_needs_its_meta() {
        let toml = r#"
            decision = "event"
            targets = { instrument_id = "XAUUSD.ICMARKETS", quantity = "1", barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "2h" } }
            retrain = "24h"
        "#;

        let err = toml::from_str::<RuleConfig>(toml).unwrap_err().to_string();

        assert!(err.contains("meta"), "{err}");
    }

    /// From the threshold 0.6 to a probability of 1, 0.5 to 2 times the
    /// targets' size: 0.8, half way, is 1.25 times.
    #[test]
    fn a_meta_probability_sizes_the_trade_from_the_threshold_to_one() {
        let meta = MetaConfig::builder()
            .window(SignedDuration::from_hours(36))
            .holdout(SignedDuration::from_hours(12))
            .threshold(0.6)
            .min_score(0.5)
            .learner(LearnerConfig::Fake {})
            .size(Sizing {
                min: Decimal::new(5, 1),
                max: Decimal::from(2),
            })
            .build();

        assert_eq!(meta.size(0.6), Decimal::new(5, 1));
        assert_eq!(meta.size(0.8).round_dp(9), Decimal::new(125, 2));
        assert_eq!(meta.size(1.0), Decimal::from(2));
        let unsized_meta = MetaConfig { size: None, ..meta };
        assert_eq!(unsized_meta.size(0.8), Decimal::ONE);
    }
}
