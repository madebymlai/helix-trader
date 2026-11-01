//! The model section of a strategy's TOML: when the strategy decides, the
//! targets of its decisions and the one its model learns, how often a
//! model is retrained, if ever, and the window it is trained on, the
//! least its holdout earns to be published, the margins its expected P&L
//! must clear to trade a side, and its learner; and, optionally, the meta
//! model filtering its trades.
//!
//! A strategy has at most two levels of models: a primary, its rule
//! (`RuleConfig`) or a model (`ModelConfig`), and one meta model
//! (`MetaConfig`) deciding whether to take the primary's trade. The types
//! hold no third: a meta section has no meta section of its own, and
//! writing one is refused at load as an unknown field.

use jiff::SignedDuration;
use nautilus_common::config::{ConfigError, ConfigErrorCollector, ConfigResult};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use serde::Deserialize;

use super::{
    decide::DecisionRule,
    holdout::Minimums,
    learner::LearnerConfig,
    schedule::Retrain,
    targets::{Label, Side, TargetsConfig, TargetsTable},
};
use crate::strategy::{DataRequirements, TradedInstrument};

/// A strategy's model, loaded from TOML, such as
///
/// ```toml
/// [model]
/// decision = { every = "1h" }
/// targets = { horizon = "45m" }
/// label = "horizon"
/// retrain = "168h"
/// window = "720h"
/// holdout = "120h"
/// margins = { long = 0.5, short = 0.5 }
/// min_pnl = 1.0
/// min_trades = 30
/// learner = { kind = "lightgbm" }
/// ```
///
/// Its targets `T` are as the TOML has them, a `TargetsTable` (a
/// `ModelSection`), until the section trades the instrument of the
/// strategy's `[trade]` (`ModelSection::trading`).
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig<T = TargetsConfig> {
    /// When the strategy decides.
    pub decision: DecisionRule,
    /// The targets of each decision, read from orders in a dataset run.
    pub targets: T,
    /// The target the models learn: whether each side of it gains.
    pub label: Label,
    /// How often the model is retrained, such as `"168h"`: at every multiple
    /// of it since the Unix epoch, in a backtest as live
    /// (`model::schedule`). Left out, never: each model is trained once and
    /// kept (`Retrain::Never`).
    #[serde(default)]
    #[builder(default, into)]
    pub retrain: Retrain,
    /// How far back from a retrain its rows go, such as `"720h"`.
    pub window: SignedDuration,
    /// The last part of the window the model is scored on and not fitted
    /// on, such as `"120h"`.
    pub holdout: SignedDuration,
    /// The sides traded, each with the expected net P&L it trades from.
    pub margins: Margins,
    /// The least net P&L per trade a side's decisions earn on the holdout
    /// for its model to be published, in the targets' P&L currency
    /// (`holdout`).
    pub min_pnl: f64,
    /// The fewest trades a side's decisions take on the holdout for its
    /// model to be published.
    pub min_trades: usize,
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
/// targets = { barrier = { .. } }
/// retrain = "168h"
///
/// [model.meta]
/// window = "720h"
/// holdout = "120h"
/// threshold = 0.55
/// min_pnl = 1.0
/// min_trades = 30
/// learner = { kind = "lightgbm" }
/// ```
///
/// Its targets `T` are as `ModelConfig`'s.
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct RuleConfig<T = TargetsConfig> {
    /// When the strategy decides: its rule is asked then, and a decision is
    /// made only on the side it signals.
    pub decision: DecisionRule,
    /// The targets of each decision; a barrier, which the meta model learns.
    pub targets: T,
    /// How often the meta model is retrained, as `ModelConfig::retrain`;
    /// left out, never.
    #[serde(default)]
    #[builder(default, into)]
    pub retrain: Retrain,
    /// The meta model.
    pub meta: MetaConfig,
}

/// A meta model: on each decision the primary makes a trade of, whether
/// the triple-barrier trade on the primary's side wins, learned from the
/// primary's features, its side and its probability. It is retrained on
/// the primary's schedule, right after the primary, or, the primary never
/// retrained, trained once and kept.
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
    /// The least net P&L per trade the meta's decisions earn on the
    /// holdout for its model to be published, in the targets' P&L
    /// currency (`holdout`).
    pub min_pnl: f64,
    /// The fewest trades the meta's decisions take on the holdout for its
    /// model to be published.
    pub min_trades: usize,
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

/// The sides a model primary trades, each with its margin: the expected
/// net P&L of a trade, in the targets' P&L currency per trade of their
/// quantity, from which the side is traded (`payoff`), such as
/// `margins = { long = 0.5, short = 0.5 }`. A side without one has no
/// model, trained, stored, scored nor traded: `{ long = 0.5 }` is a
/// long-only model.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Margins {
    pub long: Option<f64>,
    pub short: Option<f64>,
}

impl Margins {
    /// The margin of `side`, if it is traded.
    #[must_use]
    pub const fn of(&self, side: Side) -> Option<f64> {
        match side {
            Side::Long => self.long,
            Side::Short => self.short,
        }
    }

    /// The sides traded, the long first, each with its margin.
    pub fn sides(&self) -> impl Iterator<Item = (Side, f64)> {
        Side::BOTH
            .into_iter()
            .filter_map(|side| self.of(side).map(|margin| (side, margin)))
    }
}

/// A model section as a strategy's TOML has it: its targets trade nothing
/// yet.
pub type ModelSection = ModelConfig<TargetsTable>;

impl ModelSection {
    /// The section, its targets trading `traded` (`TargetsTable::trading`).
    #[must_use]
    pub fn trading(self, traded: &TradedInstrument) -> ModelConfig {
        let Self {
            decision,
            targets,
            label,
            retrain,
            window,
            holdout,
            margins,
            min_pnl,
            min_trades,
            learner,
            meta,
        } = self;
        ModelConfig {
            decision,
            targets: targets.trading(traded),
            label,
            retrain,
            window,
            holdout,
            margins,
            min_pnl,
            min_trades,
            learner,
            meta,
        }
    }
}

impl RuleConfig<TargetsTable> {
    /// The section, its targets trading `traded` (`TargetsTable::trading`).
    #[must_use]
    pub fn trading(self, traded: &TradedInstrument) -> RuleConfig {
        let Self {
            decision,
            targets,
            retrain,
            meta,
        } = self;
        RuleConfig {
            decision,
            targets: targets.trading(traded),
            retrain,
            meta,
        }
    }
}

impl ModelConfig {
    /// Checks the section against itself and against the strategy's data
    /// `requirements`.
    ///
    /// # Errors
    ///
    /// Returns every violation, each Nautilus' `ConfigError` under its
    /// field of `[model]` (`model.…`), as one `ConfigError::Multiple` if
    /// there are several: the decision rule, the targets or the learner
    /// invalid (see `DecisionRule::validate`, `TargetsConfig::validate`,
    /// `LearnerConfig::validate`), the retrain interval not positive, the
    /// barrier's ATR without bars built from what the strategy reads of
    /// the instrument traded (see
    /// `TargetsConfig::atr_bars`) or warming over longer than its warmup,
    /// the label naming a target the config lacks, a target (the barrier's
    /// limit or the horizon) not shorter than the window, the holdout not
    /// positive or leaving no fitted row (see `validate_window`), the
    /// margins naming no side or one not finite, the minimums invalid (see
    /// `validate_minimums`), a meta model gating a primary not labelled on
    /// the barrier, whose trades it learns, the meta model invalid (see
    /// `MetaConfig::validate`), or the decisions on events or a target not
    /// shorter than their cadence (see `validate_cadence`).
    pub fn validate(&self, requirements: &DataRequirements) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();
        validate_decisions(
            self.decision,
            &self.targets,
            self.retrain,
            requirements,
            &mut errors,
        );
        errors.collect(learner(&self.learner, "model.learner"));
        errors.check(
            self.targets.pnl_index(self.label, Side::Long).is_some(),
            ConfigError::invalid_value(
                "model.label",
                format!(
                    "the label {:?} names a target the targets do not have",
                    self.label
                ),
            ),
        );
        validate_window(
            self.window,
            self.holdout,
            &self.targets,
            "model",
            &mut errors,
        );
        errors.check(
            self.margins.sides().next().is_some(),
            ConfigError::invalid_value("model.margins", "the margins name no side to trade"),
        );
        for (side, margin) in self.margins.sides() {
            errors.check(
                margin.is_finite(),
                ConfigError::invalid_value(
                    format!("model.margins.{}", side.name()),
                    format!("the {} margin {margin} is not a number", side.name()),
                ),
            );
        }
        validate_minimums(self.minimums(), "model", &mut errors);
        if let Some(meta) = &self.meta {
            errors.check(
                self.label == Label::Barrier,
                ConfigError::invalid_value(
                    "model.meta",
                    "the meta model learns whether the primary's barrier trade wins, and the \
                     primary is labelled \"horizon\": it places horizon trades, not barrier ones",
                ),
            );
            meta.validate(&self.targets, &mut errors);
        }
        validate_cadence(self.decision, &self.targets, &mut errors);
        errors.into_result()
    }

    /// The least a side's model is published with, on its holdout.
    #[must_use]
    pub const fn minimums(&self) -> Minimums {
        Minimums {
            min_pnl: self.min_pnl,
            min_trades: self.min_trades,
        }
    }
}

/// Pushes onto `errors` the violation of a target of `targets` not closing
/// before the next decision of `rule`, or of no cadence: a model primary
/// trades the target of its label, and a live account nets a new trade
/// into one still open, where a dataset run's hedging venue keeps them
/// apart.
fn validate_cadence(
    rule: DecisionRule,
    targets: &TargetsConfig,
    errors: &mut ConfigErrorCollector,
) {
    let DecisionRule::Every(cadence) = rule else {
        errors.push(ConfigError::invalid_value(
            "model.decision",
            "a model primary trades its target, and decisions on events have no cadence for \
             it to close within",
        ));
        return;
    };
    if let Some(barrier) = targets.barrier {
        errors.check(
            barrier.limit < cadence,
            ConfigError::invalid_value(
                "model.targets.barrier.limit",
                format!(
                    "the barrier's limit {:#} is not shorter than the decision cadence \
                     {cadence:#}: a trade would still be open at the next decision",
                    barrier.limit
                ),
            ),
        );
    }
    if let Some(horizon) = targets.horizon {
        errors.check(
            horizon < cadence,
            ConfigError::invalid_value(
                "model.targets.horizon",
                format!(
                    "the horizon {horizon:#} is not shorter than the decision cadence \
                     {cadence:#}: a trade would still be open at the next decision"
                ),
            ),
        );
    }
}

impl RuleConfig {
    /// Checks the section against itself and against the strategy's data
    /// `requirements`.
    ///
    /// # Errors
    ///
    /// Returns every violation, as `ModelConfig::validate` does: the
    /// decision rule or the targets invalid, the retrain interval not
    /// positive, the barrier's ATR without bars built from what it reads
    /// or warming over longer than its warmup, or the meta model invalid
    /// (see `MetaConfig::validate`).
    pub fn validate(&self, requirements: &DataRequirements) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();
        validate_decisions(
            self.decision,
            &self.targets,
            self.retrain,
            requirements,
            &mut errors,
        );
        self.meta.validate(&self.targets, &mut errors);
        errors.into_result()
    }
}

impl MetaConfig {
    /// Pushes onto `errors` every violation of the meta model against the
    /// primary's `targets`, each under its field of `[model.meta]`
    /// (`model.meta.…`): the targets without a barrier, the trade the meta
    /// model learns, the learner invalid (see `LearnerConfig::validate`), a
    /// target not shorter than the window, the holdout not positive or
    /// leaving no fitted row, the threshold not a probability, the minimums
    /// invalid (see `validate_minimums`), or the sizing's multiples not
    /// positive with the minimum not above the maximum.
    pub fn validate(&self, targets: &TargetsConfig, errors: &mut ConfigErrorCollector) {
        errors.check(
            targets.barrier.is_some(),
            ConfigError::invalid_value(
                "model.meta",
                "the meta model learns whether the primary's barrier trade wins, and the \
                 targets have no barrier",
            ),
        );
        errors.collect(learner(&self.learner, "model.meta.learner"));
        validate_window(self.window, self.holdout, targets, "model.meta", errors);
        errors.check(
            (0.0..=1.0).contains(&self.threshold),
            ConfigError::invalid_value(
                "model.meta.threshold",
                format!("the meta threshold {} is not a probability", self.threshold),
            ),
        );
        validate_minimums(self.minimums(), "model.meta", errors);
        if let Some(Sizing { min, max }) = self.size {
            errors.check(
                min.is_sign_positive() && !min.is_zero() && min <= max,
                ConfigError::invalid_value(
                    "model.meta.size",
                    format!(
                        "the meta sizing's multiples min {min} and max {max} are not positive \
                         with the min not above the max"
                    ),
                ),
            );
        }
    }

    /// The least a meta model is published with, on its holdout.
    #[must_use]
    pub const fn minimums(&self) -> Minimums {
        Minimums {
            min_pnl: self.min_pnl,
            min_trades: self.min_trades,
        }
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

/// Pushes onto `errors` the violations of what every model section has:
/// the decision `rule`, the `targets`, the `retrain` (`Retrain::validate`),
/// against the strategy's `requirements`.
fn validate_decisions(
    rule: DecisionRule,
    targets: &TargetsConfig,
    retrain: Retrain,
    requirements: &DataRequirements,
    errors: &mut ConfigErrorCollector,
) {
    errors.collect(
        rule.validate()
            .map_err(|e| ConfigError::invalid_value("model.decision", format!("{e:#}"))),
    );
    errors.collect(targets.validate(rule, requirements));
    retrain.validate(errors);
    // An ATR without bars is the targets' violation, pushed above.
    if let Ok(Some(atr)) = targets.atr_bars(rule, requirements) {
        // A dataset run replays the warmup before its first day, and no
        // more: an ATR still cold then opens no target at the day's first
        // decisions, which a longer build would.
        let warms = i32::try_from(atr.args.period)
            .ok()
            .and_then(|period| atr.bar_type.spec().timedelta().checked_mul(period));
        errors.check(
            warms.is_some_and(|warms| warms <= requirements.warmup),
            ConfigError::invalid_value(
                "model.targets.barrier.atr",
                format!(
                    "the barrier's ATR of {} bars of {} warms over longer than the warmup {:#}",
                    atr.args.period, atr.bar_type, requirements.warmup
                ),
            ),
        );
    }
}

/// The violation of `learner`, under `field`.
fn learner(learner: &LearnerConfig, field: &str) -> ConfigResult<()> {
    learner
        .validate()
        .map_err(|e| ConfigError::invalid_value(field, format!("{e:#}")))
}

/// Pushes onto `errors`, under `section` (`model` or `model.meta`), the
/// violations of the longest of `targets` not shorter than `window`, or
/// `holdout` not positive or too long to leave a fitted row before it and
/// the calibration, as long as it, before it (`train`): the window longer
/// than twice the holdout and the longest target.
fn validate_window(
    window: SignedDuration,
    holdout: SignedDuration,
    targets: &TargetsConfig,
    section: &str,
    errors: &mut ConfigErrorCollector,
) {
    let longest = targets.settles_within();
    errors.check(
        longest < window,
        ConfigError::invalid_value(
            format!("{section}.window"),
            format!("the longest target {longest:#} is not shorter than the window {window:#}"),
        ),
    );
    errors.check(
        holdout.is_positive() && holdout * 2 + longest < window,
        ConfigError::invalid_value(
            format!("{section}.holdout"),
            format!(
                "the holdout {holdout:#} and the calibration as long before it leave no fitted \
                 row in the window {window:#} after the longest target {longest:#}"
            ),
        ),
    );
}

/// Pushes onto `errors`, under `section` (`model` or `model.meta`), the
/// violations of the minimum net P&L of `minimums` not a number and their
/// minimum count of trades below one.
fn validate_minimums(minimums: Minimums, section: &str, errors: &mut ConfigErrorCollector) {
    errors.check(
        minimums.min_pnl.is_finite(),
        ConfigError::invalid_value(
            format!("{section}.min_pnl"),
            format!(
                "the minimum net P&L per trade {} is not a number",
                minimums.min_pnl
            ),
        ),
    );
    errors.check(
        minimums.min_trades >= 1,
        ConfigError::invalid_value(
            format!("{section}.min_trades"),
            format!(
                "the minimum count of trades is {}, not at least one",
                minimums.min_trades
            ),
        ),
    );
}

#[cfg(test)]
mod tests {
    use nautilus_model::{data::BarType, identifiers::InstrumentId, types::Quantity};

    use super::*;
    use crate::strategy::TickSeries;

    const GOLD: &str = "XAUUSD.ICMARKETS";

    /// A model section deciding every hour, with a barrier closed after 45
    /// minutes and a horizon of 30, on a window of 48 hours, 12 of them
    /// held out.
    const SECTION: &str = r#"
        decision = { every = "1h" }
        targets = { horizon = "30m", barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "45m" } }
        label = "barrier"
        retrain = "24h"
        window = "48h"
        holdout = "12h"
        margins = { long = 0.5, short = 0.5 }
        min_pnl = 1.0
        min_trades = 30
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

    /// What every test's strategy trades: an ounce of gold.
    fn gold() -> TradedInstrument {
        TradedInstrument {
            instrument_id: InstrumentId::from(GOLD),
            trade_size: Quantity::from(1),
        }
    }

    /// The section of `toml`, trading gold.
    fn parse(toml: &str) -> anyhow::Result<ModelConfig> {
        Ok(toml::from_str::<ModelSection>(toml)?.trading(&gold()))
    }

    /// The error of loading `toml` for a strategy reading gold, if any.
    fn load(toml: &str) -> anyhow::Result<ModelConfig> {
        let config = parse(toml)?;
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
        assert_eq!(
            config.retrain,
            Retrain::Every(SignedDuration::from_hours(24))
        );
        assert_eq!(config.window, SignedDuration::from_hours(48));
        assert_eq!(config.holdout, SignedDuration::from_hours(12));
        assert_eq!(
            config.margins,
            Margins {
                long: Some(0.5),
                short: Some(0.5)
            }
        );
        assert_eq!(
            config.minimums(),
            Minimums {
                min_pnl: 1.0,
                min_trades: 30
            }
        );
        assert_eq!(config.learner, LearnerConfig::Fake {});
    }

    #[test]
    fn a_section_refuses_an_unknown_field() {
        assert!(refused(&section("retrain_every = \"24h\"")).contains("retrain_every"));
    }

    /// One load shows every mistake: a retrain interval not positive is
    /// reported under `model.retrain`, beside a minimum of no trade.
    #[test]
    fn a_retrain_interval_not_positive_is_refused_with_the_sections_other_violations() {
        for retrain in ["0h", "-24h"] {
            let toml = section(&format!(r#"retrain = "{retrain}""#))
                .replace("min_trades = 30", "min_trades = 0");

            let err = refused(&toml);

            assert!(
                err.contains("model.retrain") && err.contains("retrain interval"),
                "{retrain}: {err}"
            );
            assert!(err.contains("model.min_trades"), "{retrain}: {err}");
        }
    }

    #[test]
    fn a_horizon_not_shorter_than_the_window_is_refused() {
        let toml = section(r#"targets = { horizon = "48h" }"#)
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
            r#"targets = { barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "49h" } }"#,
        );

        assert!(
            refused(&toml).contains("not shorter than the window"),
            "{}",
            refused(&toml)
        );
    }

    /// Deciding every hour, a barrier closed after an hour, or a horizon of
    /// one, would still be open at the next decision.
    #[test]
    fn a_target_not_shorter_than_the_decision_cadence_is_refused_naming_both() {
        for (targets, named) in [
            (
                r#"horizon = "30m", barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "1h" }"#,
                "limit 1h",
            ),
            (
                r#"horizon = "90m", barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "45m" }"#,
                "horizon 1h 30m",
            ),
        ] {
            let toml = section(&format!(r#"targets = {{ {targets} }}"#));

            let err = refused(&toml);

            assert!(
                err.contains(named) && err.contains("cadence 1h"),
                "{named}: {err}"
            );
        }
    }

    /// Decisions on events have no cadence their trades would close
    /// within.
    #[test]
    fn a_model_primary_deciding_on_events_is_refused() {
        let toml = section(r#"decision = "event""#);

        assert!(refused(&toml).contains("event"), "{}", refused(&toml));
    }

    /// The window of 48 hours holds the holdout, the calibration as long
    /// before it, and before them a fitted row settled 45 minutes later: 23
    /// hours of holdout leave one, 24 none.
    #[test]
    fn a_holdout_leaving_no_fitted_row_before_it_and_its_calibration_is_refused() {
        for holdout in ["24h", "0h"] {
            let toml = section(&format!(r#"holdout = "{holdout}""#));
            assert!(refused(&toml).contains("holdout"), "{holdout}");
        }
        assert!(load(&section(r#"holdout = "23h""#)).is_ok());
    }

    #[test]
    fn margins_naming_no_side_or_not_a_number_are_refused() {
        for margins in ["{}", "{ long = nan }", "{ long = 0.5, short = inf }"] {
            let toml = section(&format!("margins = {margins}"));
            assert!(refused(&toml).contains("margin"), "{margins}");
        }
        assert!(refused(&section("margins = { both = 0.5 }")).contains("both"));
    }

    /// `{ long = 0.5 }` is a long-only model: the short is not traded.
    #[test]
    fn margins_of_one_side_trade_that_side_alone() {
        let config = load(&section("margins = { long = 0.5 }")).unwrap();

        assert_eq!(
            config.margins.sides().collect::<Vec<_>>(),
            [(Side::Long, 0.5)]
        );
        assert_eq!(config.margins.of(Side::Short), None);
    }

    #[test]
    fn a_minimum_net_pnl_not_a_number_or_no_minimum_trade_is_refused() {
        for line in ["min_pnl = nan", "min_pnl = -inf", "min_trades = 0"] {
            assert!(refused(&section(line)).contains("minimum"), "{line}");
        }
        assert!(load(&section("min_pnl = -0.5")).is_ok());
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

    /// Its harness reads the quotes of what it trades when no series reads
    /// it.
    #[test]
    fn a_traded_instrument_the_strategy_does_not_read_is_no_error() {
        let config = parse(SECTION).unwrap();

        config.validate(&reading("EURUSD.ICMARKETS")).unwrap();
    }

    #[test]
    fn a_label_of_a_target_the_config_lacks_is_refused() {
        let toml = section(r#"targets = { horizon = "1h" }"#);

        assert!(refused(&toml).contains("label"), "{}", refused(&toml));
    }

    /// A barrier 2 ATRs away in the side's favour, 1.5 against it, its ATR
    /// `atr`, a table or none.
    fn atr_section(atr: Option<&str>) -> String {
        let atr = atr.map(|atr| format!(", atr = {atr}")).unwrap_or_default();
        section(&format!(
            r#"targets = {{ barrier = {{ take_profit = {{ atr = 2.0 }}, stop = {{ atr = 1.5 }}, limit = "45m"{atr} }} }}"#
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
            let config = parse(&atr_section(atr)).unwrap();

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

    /// The requirements of a strategy reading `ticks` of gold, each by the
    /// name Nautilus parses, and `bar_types`, warming for 14 hours, as the
    /// default ATR of hourly bars does.
    fn reading_gold(ticks: &[&str], bar_types: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: bar_types
                .iter()
                .map(|&bar_type| BarType::from(bar_type))
                .collect(),
            ticks: ticks
                .iter()
                .map(|data| {
                    toml::from_str::<TickSeries>(&format!(
                        r#"data = "{data}"
                           instrument_id = "{GOLD}""#
                    ))
                    .unwrap()
                })
                .collect(),
            warmup: SignedDuration::from_hours(14),
        }
    }

    /// Without quotes, the ATR's bars are built from the trades, or from
    /// the source bars the strategy reads.
    #[test]
    fn a_barrier_atr_is_built_from_the_trades_or_the_bars_of_a_strategy_without_quotes() {
        let config = parse(&atr_section(None)).unwrap();

        for requirements in [
            reading_gold(&["trades"], &[]),
            reading_gold(
                &[],
                &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"],
            ),
        ] {
            assert!(config.validate(&requirements).is_ok(), "{requirements:?}");
        }
    }

    /// Neither quotes, trades nor bars of the targets' instrument: the
    /// ATR's bars have nothing to be built from.
    #[test]
    fn a_barrier_atr_on_an_instrument_whose_prices_the_strategy_does_not_read_is_refused() {
        let config = parse(&atr_section(None)).unwrap();

        let err = format!(
            "{:#}",
            config
                .validate(&reading_gold(&["order_book_depths"], &[]))
                .unwrap_err()
        );

        assert!(err.contains("ATR") && err.contains(GOLD), "{err}");
    }

    /// A meta model on the section's barrier, held out its last 12 hours.
    const META: &str = r#"
        [meta]
        window = "36h"
        holdout = "12h"
        threshold = 0.55
        min_pnl = 0.5
        min_trades = 20
        learner = { kind = "baseline" }
    "#;

    #[test]
    fn a_meta_section_reads_from_toml() {
        let config = load(&format!("{SECTION}\n{META}")).unwrap();

        let meta = config.meta.unwrap();
        assert_eq!(meta.window, SignedDuration::from_hours(36));
        assert_eq!(meta.holdout, SignedDuration::from_hours(12));
        assert_eq!(meta.threshold, 0.55);
        assert_eq!(
            meta.minimums(),
            Minimums {
                min_pnl: 0.5,
                min_trades: 20
            }
        );
        assert_eq!(meta.learner, LearnerConfig::Baseline {});
        assert_eq!(meta.size, None);
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

    /// A rule primary on a horizon alone: its meta model has no barrier
    /// trade to learn, and that is the one violation.
    #[test]
    fn a_meta_model_without_a_barrier_is_refused() {
        let toml = format!(
            r#"
            decision = "event"
            targets = {{ horizon = "1h" }}
            retrain = "24h"
            {META}"#
        );
        let config = toml::from_str::<RuleConfig<TargetsTable>>(&toml)
            .unwrap()
            .trading(&gold());

        let err = config.validate(&reading(GOLD)).unwrap_err();

        assert!(
            matches!(
                &err,
                ConfigError::InvalidValue { field, reason }
                    if field == "model.meta" && reason.contains("no barrier")
            ),
            "{err:#}"
        );
    }

    /// A horizon-labelled primary trades its horizon, and the meta model
    /// would gate it on barrier trades never placed.
    #[test]
    fn a_meta_model_on_a_horizon_labelled_primary_is_refused_naming_both() {
        let toml = SECTION.replace(r#"label = "barrier""#, r#"label = "horizon""#);

        let err = refused(&format!("{toml}\n{META}"));

        assert!(err.contains("meta"), "{err}");
        assert!(err.contains("horizon"), "{err}");
        assert!(err.contains("barrier"), "{err}");
    }

    /// The refusal of the section with `META`, `line` added to it or, if
    /// its key is there, replacing it.
    fn meta_refused(line: &str) -> String {
        let key = line.split('=').next().unwrap().trim();
        let meta: Vec<&str> = META
            .lines()
            .filter(|kept| kept.split('=').next().unwrap().trim() != key)
            .chain([line])
            .collect();
        refused(&format!("{SECTION}\n{}", meta.join("\n")))
    }

    /// The barrier's limit, 45 minutes, is the longest target.
    #[test]
    fn a_meta_window_not_longer_than_the_longest_target_is_refused() {
        let err = meta_refused(r#"window = "45m""#);

        assert!(err.contains("model.meta.window"), "{err}");
    }

    /// The meta's window of 36 hours cannot hold 35 of holdout and as many
    /// of calibration.
    #[test]
    fn a_meta_holdout_leaving_no_fitted_row_is_refused() {
        let err = meta_refused(r#"holdout = "35h""#);

        assert!(err.contains("model.meta.holdout"), "{err}");
    }

    #[test]
    fn a_meta_threshold_not_a_probability_is_refused() {
        let err = meta_refused("threshold = 1.5");

        assert!(err.contains("model.meta.threshold"), "{err}");
    }

    #[test]
    fn a_meta_minimum_of_no_trade_is_refused() {
        let err = meta_refused("min_trades = 0");

        assert!(err.contains("model.meta.min_trades"), "{err}");
    }

    #[test]
    fn a_meta_sizing_not_positive_or_its_min_above_its_max_is_refused() {
        for line in ["size = { min = 0, max = 2 }", "size = { min = 2, max = 1 }"] {
            let err = meta_refused(line);

            assert!(err.contains("model.meta.size"), "{line}: {err}");
        }
    }

    #[test]
    fn a_rule_primarys_section_reads_its_meta_from_toml() {
        let toml = r#"
            decision = "event"
            targets = { barrier = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "2h" } }
            retrain = "24h"

            [meta]
            window = "36h"
            holdout = "12h"
            threshold = 0.55
            min_pnl = 0.5
            min_trades = 20
            learner = { kind = "fake" }
            size = { min = 0.5, max = 2 }
        "#;

        let config = toml::from_str::<RuleConfig<TargetsTable>>(toml)
            .unwrap()
            .trading(&gold());
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

    /// From the threshold 0.6 to a probability of 1, 0.5 to 2 times the
    /// targets' size: 0.8, half way, is 1.25 times.
    #[test]
    fn a_meta_probability_sizes_the_trade_from_the_threshold_to_one() {
        let meta = MetaConfig::builder()
            .window(SignedDuration::from_hours(36))
            .holdout(SignedDuration::from_hours(12))
            .threshold(0.6)
            .min_pnl(0.0)
            .min_trades(1)
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
