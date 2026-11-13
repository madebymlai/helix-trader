//! The model section of a strategy's TOML: when the strategy decides, the
//! target of its decisions, which its model learns, how often a model is
//! retrained, if ever, and the window it is trained on, the least its
//! holdout earns to be published, the margins its expected P&L must clear
//! to trade a side, and its learner; and, optionally, the meta model
//! filtering its trades.
//!
//! A strategy has at most two levels of models: the side models of a
//! `ModelConfig`, and one meta model (`MetaConfig`) deciding whether to
//! take the trade of the side picked. The types hold no third: a meta
//! section has no meta section of its own, and writing one is refused at
//! load as an unknown field.

use jiff::SignedDuration;
use nautilus_common::config::{ConfigError, ConfigErrorCollector, ConfigResult};
use serde::{Deserialize, de::IgnoredAny};

use super::{
    decide::DecisionRule,
    holdout::Minimums,
    learner::LearnerConfig,
    schedule::Retrain,
    targets::{Side, Target, TargetConfig},
};
use crate::strategy::{DataRequirements, TradedInstrument};

/// A strategy's model, loaded from TOML, such as
///
/// ```toml
/// [model]
/// decision = { every = "1h" }
/// target = { bracket = { take_profit = { atr = 2.0 }, stop = { atr = 1.0 } }, limit = "45m" }
/// retrain = "168h"
/// window = "720h"
/// holdout = "120h"
/// margins = { long = 0.5, short = 0.5 }
/// min_pnl = 1.0
/// min_trades = 30
/// learner = { kind = "lightgbm" }
/// ```
///
/// Its target `T` is as the TOML has it, a `Target` (a `ModelSection`),
/// until the section trades the instrument of the strategy's `[trade]`
/// (`ModelSection::trading`).
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig<T = TargetConfig> {
    /// When the strategy decides.
    pub decision: DecisionRule,
    /// The target of each decision, read from orders in a dataset run: the
    /// side models learn the net R of each side of it.
    pub target: T,
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
    /// The least net P&L per trade the trades picked between the sides earn
    /// on the holdout for the sides' models to be published, in the target's
    /// P&L currency (`holdout`, `train`).
    pub min_pnl: f64,
    /// The fewest trades picked between the sides on the holdout for the
    /// sides' models to be published.
    pub min_trades: usize,
    /// The learner and its settings.
    pub learner: LearnerConfig,
    /// The meta model deciding whether to take each trade of this one, a
    /// `[model.meta]` table; none takes every trade.
    pub meta: Option<MetaConfig>,
}

/// A meta model: on each decision that picks a side, the realised net R of
/// that side's trade, the target on it, learned from the sides' features,
/// the side picked and the decision's calibrated level and side call. It is
/// retrained on the sides' schedule, right after them, or, the sides never
/// retrained, trained once and kept.
///
/// In the TOML, a probability `threshold` or a `size` is refused as the
/// section is parsed, saying what takes its place (`MetaFields`).
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(try_from = "MetaFields")]
pub struct MetaConfig {
    /// How far back from a retrain its rows go; never before the
    /// sides' first model, whose predictions the rows need. Never
    /// retrained, sides trained with their meta, neither in use, are
    /// trained as of this much before the meta, so their out-of-sample
    /// decisions over the window are the meta's rows.
    pub window: SignedDuration,
    /// The last part of the window the model is scored on and not fitted
    /// on.
    pub holdout: SignedDuration,
    /// The net P&L the meta expects of the trade of the side picked, its
    /// calibrated expected R times the trade's unit, in the target's P&L
    /// currency, from which the trade is taken (`holdout::MetaMargin`).
    pub margin: f64,
    /// The least net P&L per trade the meta's decisions earn on the
    /// holdout for its model to be published, in the target's P&L
    /// currency (`holdout`).
    pub min_pnl: f64,
    /// The fewest trades the meta's decisions take on the holdout for its
    /// model to be published.
    pub min_trades: usize,
    /// The learner and its settings.
    pub learner: LearnerConfig,
}

/// The keys of a meta section as the TOML has them: a `MetaConfig`'s, and
/// those of a meta deciding on a probability, `threshold` and `size`, each
/// refused naming what takes its place, before a margin is looked for.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetaFields {
    window: SignedDuration,
    holdout: SignedDuration,
    margin: Option<f64>,
    min_pnl: f64,
    min_trades: usize,
    learner: LearnerConfig,
    threshold: Option<IgnoredAny>,
    size: Option<IgnoredAny>,
}

impl TryFrom<MetaFields> for MetaConfig {
    type Error = String;

    fn try_from(fields: MetaFields) -> Result<Self, String> {
        let MetaFields {
            window,
            holdout,
            margin,
            min_pnl,
            min_trades,
            learner,
            threshold,
            size,
        } = fields;
        if threshold.is_some() {
            return Err(
                "a meta has no probability `threshold`: it takes the trade of the side \
                 picked from `margin`, the net P&L it expects of the trade, its expected R \
                 times the trade's unit, in the target's P&L currency"
                    .to_string(),
            );
        }
        if size.is_some() {
            return Err(
                "a meta has no `size`: the trade it takes is of the target's size, the \
                 instrument's `trade_size` in `[trade]`"
                    .to_string(),
            );
        }
        Ok(Self {
            window,
            holdout,
            margin: margin.ok_or("missing field `margin`")?,
            min_pnl,
            min_trades,
            learner,
        })
    }
}

/// The sides a strategy trades, each with its margin: the expected
/// net P&L of a trade, in the target's P&L currency per trade of its
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

/// A model section as a strategy's TOML has it: its target trades nothing
/// yet.
pub type ModelSection = ModelConfig<Target>;

impl ModelSection {
    /// The section, its target trading `traded` (`Target::trading`).
    #[must_use]
    pub fn trading(self, traded: &TradedInstrument) -> ModelConfig {
        let Self {
            decision,
            target,
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
            target: target.trading(traded),
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

impl ModelConfig {
    /// Checks the section against itself and against the strategy's data
    /// `requirements`.
    ///
    /// # Errors
    ///
    /// Returns every violation, each Nautilus' `ConfigError` under its
    /// field of `[model]` (`model.…`), as one `ConfigError::Multiple` if
    /// there are several: the decision rule, the target or the learner
    /// invalid (see `DecisionRule::validate`, `TargetConfig::validate`,
    /// `LearnerConfig::validate`), the retrain interval not positive, the
    /// target's ATR without bars built from what the strategy reads of
    /// the instrument traded (see
    /// `TargetConfig::atr_bars`) or warming over longer than its warmup,
    /// the target's limit not shorter than the window, the holdout not
    /// positive or leaving no fitted row (see `validate_window`), the
    /// margins naming no side or one not finite, the minimums invalid (see
    /// `validate_minimums`), the meta model invalid (see
    /// `MetaConfig::validate`), or the target's limit not shorter than the
    /// decision cadence (see `validate_cadence`).
    pub fn validate(&self, requirements: &DataRequirements) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();
        validate_decisions(
            self.decision,
            &self.target,
            self.retrain,
            requirements,
            &mut errors,
        );
        errors.collect(learner(&self.learner, "model.learner"));
        validate_window(
            self.window,
            self.holdout,
            &self.target,
            "model",
            "model.target.limit",
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
            meta.validate(&self.target, &mut errors);
        }
        validate_cadence(self.decision, &self.target, &mut errors);
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

/// Pushes onto `errors` the violation of `target` not closing before the
/// next decision of `rule`: the sides trade the target, and a live
/// account nets a new trade into one still open, where a dataset run's
/// hedging venue keeps them apart.
fn validate_cadence(rule: DecisionRule, target: &TargetConfig, errors: &mut ConfigErrorCollector) {
    let DecisionRule::Every(cadence) = rule;
    let limit = target.settles_within();
    errors.check(
        limit < cadence,
        ConfigError::invalid_value(
            "model.target.limit",
            format!(
                "the target's limit {limit:#} is not shorter than the decision cadence \
                 {cadence:#}: a trade would still be open at the next decision"
            ),
        ),
    );
}

impl MetaConfig {
    /// Pushes onto `errors` every violation of the meta model against the
    /// sides' `target`, each under its field of `[model.meta]`
    /// (`model.meta.…`): the learner invalid (see
    /// `LearnerConfig::validate`), the target's limit not shorter than the
    /// window, the holdout not positive or leaving no fitted row, the margin
    /// not a number, or the minimums invalid (see `validate_minimums`).
    pub fn validate(&self, target: &TargetConfig, errors: &mut ConfigErrorCollector) {
        errors.collect(learner(&self.learner, "model.meta.learner"));
        validate_window(
            self.window,
            self.holdout,
            target,
            "model.meta",
            "model.meta.window",
            errors,
        );
        errors.check(
            self.margin.is_finite(),
            ConfigError::invalid_value(
                "model.meta.margin",
                format!("the meta margin {} is not a number", self.margin),
            ),
        );
        validate_minimums(self.minimums(), "model.meta", errors);
    }

    /// The least a meta model is published with, on its holdout.
    #[must_use]
    pub const fn minimums(&self) -> Minimums {
        Minimums {
            min_pnl: self.min_pnl,
            min_trades: self.min_trades,
        }
    }
}

/// Pushes onto `errors` the violations of what every model section has:
/// the decision `rule`, the `target`, the `retrain` (`Retrain::validate`),
/// against the strategy's `requirements`.
fn validate_decisions(
    rule: DecisionRule,
    target: &TargetConfig,
    retrain: Retrain,
    requirements: &DataRequirements,
    errors: &mut ConfigErrorCollector,
) {
    errors.collect(
        rule.validate()
            .map_err(|e| ConfigError::invalid_value("model.decision", format!("{e:#}"))),
    );
    errors.collect(target.validate(rule, requirements));
    retrain.validate(errors);
    // An ATR without bars is the target's violation, pushed above.
    if let Ok(Some(atr)) = target.atr_bars(rule, requirements) {
        // A dataset run replays the warmup before its first day, and no
        // more: an ATR still cold then opens no target at the day's first
        // decisions, which a longer build would.
        let warms = i32::try_from(atr.args.period)
            .ok()
            .and_then(|period| atr.bar_type.spec().timedelta().checked_mul(period));
        errors.check(
            warms.is_some_and(|warms| warms <= requirements.warmup),
            ConfigError::invalid_value(
                target.spec.atr_field(),
                format!(
                    "the target's ATR of {} bars of {} warms over longer than the warmup {:#}",
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

/// Pushes onto `errors`, of `section` (`model` or `model.meta`), the
/// violations of the limit of `target` not shorter than `window`, under
/// `limit_field`, or `holdout` not positive or too long to leave a fitted
/// row before it and the calibration, as long as it, before it (`train`):
/// the window longer than twice the holdout and the limit. The sides'
/// window is their target's to fit, and a meta's the meta's own, set
/// beside the target it shares.
fn validate_window(
    window: SignedDuration,
    holdout: SignedDuration,
    target: &TargetConfig,
    section: &str,
    limit_field: &str,
    errors: &mut ConfigErrorCollector,
) {
    let limit = target.settles_within();
    errors.check(
        limit < window,
        ConfigError::invalid_value(
            limit_field,
            format!("the target's limit {limit:#} is not shorter than the window {window:#}"),
        ),
    );
    errors.check(
        holdout.is_positive() && holdout * 2 + limit < window,
        ConfigError::invalid_value(
            format!("{section}.holdout"),
            format!(
                "the holdout {holdout:#} and the calibration as long before it leave no fitted \
                 row in the window {window:#} after the target's limit {limit:#}"
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
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        model::{
            learner::LightGbmConfig,
            targets::{BracketSpec, DEFAULT_ATR, Distance},
        },
        strategy::{AtrArgs, TickSeries},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";

    /// A model section deciding every hour, with a bracket closed after 45
    /// minutes, on a window of 48 hours, 12 of them held out.
    const SECTION: &str = r#"
        decision = { every = "1h" }
        target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 } }, limit = "45m" }
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
            custom: vec![],
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
        load_warming(toml, SignedDuration::ZERO)
    }

    /// `load`, the strategy warming for `warmup`.
    fn load_warming(toml: &str, warmup: SignedDuration) -> anyhow::Result<ModelConfig> {
        let config = parse(toml)?;
        config.validate(&reading_gold_for(warmup))?;
        Ok(config)
    }

    fn refused(toml: &str) -> String {
        format!("{:#}", load(toml).unwrap_err())
    }

    /// The message of the parse error refusing `toml`, no config error, so
    /// that nothing else of the section was looked at.
    fn refused_parsing(toml: &str) -> String {
        let err = load(toml).unwrap_err();
        assert!(err.downcast_ref::<ConfigError>().is_none(), "{err:#}");
        format!("{err:#}")
    }

    /// The fields of the config errors refusing `toml`.
    fn refused_fields(toml: &str) -> Vec<String> {
        refused_fields_warming(toml, SignedDuration::ZERO)
    }

    /// `refused_fields`, the strategy warming for `warmup`.
    fn refused_fields_warming(toml: &str, warmup: SignedDuration) -> Vec<String> {
        let err = load_warming(toml, warmup).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err:#}"),
        };
        errors
            .into_iter()
            .map(|err| match err {
                ConfigError::InvalidValue { field, .. } => field,
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    #[test]
    fn a_section_reads_from_toml() {
        let config = load(SECTION).unwrap();

        assert_eq!(
            config.decision,
            DecisionRule::Every(SignedDuration::from_hours(1))
        );
        assert_eq!(config.target.instrument_id, InstrumentId::from(GOLD));
        assert_eq!(
            config.target.settles_within(),
            SignedDuration::from_mins(45)
        );
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

    /// Time only, or a bracket: the target closes after its limit at the
    /// latest, which the window must hold. Deciding every 72 hours, so that
    /// the cadence holds these limits and the window alone refuses them, the
    /// time-only target's ATR of one bar of 72 hours warm over a warmup as
    /// long.
    #[test]
    fn a_limit_not_shorter_than_the_window_is_refused_under_the_limit() {
        let warmup = SignedDuration::from_hours(72);
        for target in [
            r#"target = { limit = "48h", atr = { period = 1 } }"#,
            r#"target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 } }, limit = "49h" }"#,
        ] {
            let toml = section(target).replace(r#"every = "1h""#, r#"every = "72h""#);

            let err = format!("{:#}", load_warming(&toml, warmup).unwrap_err());
            assert!(err.contains("not shorter than the window"), "{err}");
            assert_eq!(
                refused_fields_warming(&toml, warmup),
                ["model.target.limit", "model.holdout"],
                "{target}"
            );
        }
    }

    /// Deciding every hour, a bracket closed after an hour, or a time-only
    /// trade of an hour and a half, its ATR of one hourly bar warm over a
    /// warmup of an hour, would still be open at the next decision.
    #[test]
    fn a_limit_not_shorter_than_the_decision_cadence_is_refused_naming_both() {
        let warmup = SignedDuration::from_hours(1);
        for (target, named) in [
            (
                r#"bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 } }, limit = "1h""#,
                "limit 1h",
            ),
            (r#"limit = "90m", atr = { period = 1 }"#, "limit 1h 30m"),
        ] {
            let toml = section(&format!(r#"target = {{ {target} }}"#));

            let err = format!("{:#}", load_warming(&toml, warmup).unwrap_err());

            assert!(
                err.contains(named) && err.contains("cadence 1h"),
                "{named}: {err}"
            );
            assert_eq!(
                refused_fields_warming(&toml, warmup),
                ["model.target.limit"],
                "{named}"
            );
        }
    }

    /// A trade closed at its time alone: no exit, and the ATR its unit is
    /// measured in, of its `atr` table on the decision cadence's bars, as a
    /// bracket's: 3 hourly bars of gold's mids.
    #[test]
    fn a_time_only_target_loads_with_its_atr_and_no_exits() {
        let toml = section(r#"target = { limit = "30m", atr = { period = 3 } }"#);

        let config = load_warming(&toml, SignedDuration::from_hours(3)).unwrap();

        assert_eq!(config.target.spec.bracket, None);
        assert_eq!(
            config.target.settles_within(),
            SignedDuration::from_mins(30)
        );
        let atr = config
            .target
            .atr_bars(config.decision, &reading(GOLD))
            .unwrap()
            .unwrap();
        assert_eq!(
            (atr.bar_type, atr.args.period),
            (BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL"), 3)
        );
    }

    /// A bracket has both exits: one alone is refused as the section is
    /// parsed, on its own, a minimum of no trade beside it not reported.
    #[test]
    fn a_take_profit_or_a_stop_alone_is_refused_at_parsing_on_its_own() {
        for (bracket, missing) in [
            ("take_profit = { bps = 7.5 }", "stop"),
            ("stop = { bps = 5 }", "take_profit"),
        ] {
            let toml = section(&format!(
                r#"target = {{ bracket = {{ {bracket} }}, limit = "45m" }}"#
            ))
            .replace("min_trades = 30", "min_trades = 0");

            let err = refused_parsing(&toml);

            assert!(err.contains(&format!("missing field `{missing}`")), "{err}");
            assert!(!err.contains("min_trades"), "{err}");
        }
    }

    /// A target's exits are in its bracket or nowhere: beside the limit,
    /// together or one alone, they are unknown fields of the target.
    #[test]
    fn exits_beside_the_limit_are_refused_as_unknown() {
        for target in [
            r#"target = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "45m" }"#,
            r#"target = { take_profit = { bps = 7.5 }, limit = "45m" }"#,
            r#"target = { stop = { bps = 5 }, limit = "45m" }"#,
        ] {
            let err = refused_parsing(&section(target));

            assert!(err.contains("unknown field"), "{target}: {err}");
        }
    }

    /// The two targets and the label chosen among them are gone: each is
    /// an unknown field, of the section and of the target.
    #[test]
    fn the_fields_of_two_targets_and_a_label_are_refused_as_unknown() {
        for (toml, name) in [
            (section(r#"barrier = { limit = "45m" }"#), "barrier"),
            (section(r#"horizon = "45m""#), "horizon"),
            (section(r#"targets = { horizon = "45m" }"#), "targets"),
            (section(r#"label = "barrier""#), "label"),
            (
                section(r#"target = { limit = "45m", barrier = { limit = "45m" } }"#),
                "barrier",
            ),
            (
                section(r#"target = { limit = "45m", horizon = "45m" }"#),
                "horizon",
            ),
        ] {
            let err = refused(&toml);

            assert!(err.contains(&format!("unknown field `{name}`")), "{err}");
        }
    }

    #[test]
    fn a_target_without_a_limit_is_refused() {
        let toml = section(
            r#"target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 } } }"#,
        );

        let err = refused(&toml);

        assert!(err.contains("missing field `limit`"), "{err}");
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
    fn a_strategy_sets_each_of_lightgbms_settings() {
        let config = load(&section(
            r#"learner = { kind = "lightgbm", num_iterations = 300, learning_rate = 0.02, num_leaves = 15, min_data_in_leaf = 50 }"#,
        ))
        .unwrap();

        assert_eq!(
            config.learner,
            LearnerConfig::LightGbm(
                LightGbmConfig::builder()
                    .num_iterations(300)
                    .learning_rate(0.02)
                    .num_leaves(15)
                    .min_data_in_leaf(50)
                    .build()
            )
        );
    }

    /// The floor is the adapter's dataset's, 20 rows, not the default of
    /// 200 rows a leaf: 20 and 50 load.
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

    /// A bracket 2 ATRs away in the side's favour, 1.5 against it, its ATR
    /// `atr`, a table or none.
    fn atr_section(atr: Option<&str>) -> String {
        let atr = atr.map(|atr| format!(", atr = {atr}")).unwrap_or_default();
        section(&format!(
            r#"target = {{ bracket = {{ take_profit = {{ atr = 2.0 }}, stop = {{ atr = 1.5 }}{atr} }}, limit = "45m" }}"#
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
    fn a_target_atr_warming_longer_than_the_warmup_is_refused() {
        for (atr, hours) in [(None, 14), (Some("{ period = 3 }"), 3)] {
            let config = parse(&atr_section(atr)).unwrap();

            let shorter = SignedDuration::from_hours(hours - 1);
            let err = format!(
                "{:#}",
                config.validate(&reading_gold_for(shorter)).unwrap_err()
            );
            assert!(err.contains("warmup"), "{atr:?}: {err}");
            assert!(err.contains("model.target.bracket.atr"), "{atr:?}: {err}");

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
            custom: vec![],
            warmup: SignedDuration::from_hours(14),
        }
    }

    /// A time-only target's unit is the worth of its ATR: one without an
    /// `atr` table is refused under it, beside the section's others, saying
    /// so.
    #[test]
    fn a_time_only_target_without_an_atr_is_refused_under_its_atr() {
        let toml =
            section(r#"target = { limit = "30m" }"#).replace("min_trades = 30", "min_trades = 0");

        assert_eq!(
            refused_fields(&toml),
            ["model.target.atr", "model.min_trades"]
        );
        let err = refused(&toml);
        assert!(err.contains("ATR") && err.contains("unit"), "{err}");
    }

    /// A time-only target's ATR warms as a bracket's does, its 3 hourly bars
    /// in 3 hours: over a shorter warmup it is refused under its own table.
    #[test]
    fn a_time_only_targets_atr_warming_longer_than_the_warmup_is_refused_under_it() {
        let toml = section(r#"target = { limit = "30m", atr = { period = 3 } }"#);

        assert_eq!(
            refused_fields_warming(&toml, SignedDuration::from_hours(2)),
            ["model.target.atr"]
        );
        assert!(load_warming(&toml, SignedDuration::from_hours(3)).is_ok());
    }

    /// A bracket in basis points may be written with an `atr` table, so the
    /// load refuses it, beside the section's others.
    #[test]
    fn an_atr_table_no_distance_uses_is_refused_under_the_bracket() {
        let toml = section(
            r#"target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, atr = { period = 3 } }, limit = "45m" }"#,
        )
        .replace("min_trades = 30", "min_trades = 0");

        assert_eq!(
            refused_fields(&toml),
            ["model.target.bracket.atr", "model.min_trades"]
        );
    }

    /// One distance in ATRs, of its table, the other in basis points.
    #[test]
    fn a_bracket_mixing_atrs_and_basis_points_loads() {
        let config = parse(&section(
            r#"target = { bracket = { take_profit = { atr = 1.5 }, stop = { bps = 7.5 }, atr = { period = 3 } }, limit = "45m" }"#,
        ))
        .unwrap();

        config
            .validate(&reading_gold_for(SignedDuration::from_hours(3)))
            .unwrap();
        assert_eq!(
            config.target.spec.bracket,
            Some(BracketSpec {
                take_profit: Distance::Atr(Decimal::new(15, 1)),
                stop: Distance::BasisPoints(Decimal::new(75, 1)),
                atr: Some(AtrArgs {
                    period: 3,
                    ..DEFAULT_ATR
                }),
            })
        );
    }

    #[test]
    fn a_distance_not_positive_is_refused_under_the_bracket() {
        for bracket in [
            "take_profit = { bps = 0 }, stop = { bps = 5 }",
            "take_profit = { bps = 7.5 }, stop = { bps = -1 }",
        ] {
            let toml = section(&format!(
                r#"target = {{ bracket = {{ {bracket} }}, limit = "45m" }}"#
            ));

            assert_eq!(refused_fields(&toml), ["model.target.bracket"], "{bracket}");
        }
    }

    /// Without quotes, the ATR's bars are built from the trades, or from
    /// the source bars the strategy reads.
    #[test]
    fn a_target_atr_is_built_from_the_trades_or_the_bars_of_a_strategy_without_quotes() {
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

    /// Neither quotes, trades nor bars of the target's instrument: the
    /// ATR's bars have nothing to be built from.
    #[test]
    fn a_target_atr_on_an_instrument_whose_prices_the_strategy_does_not_read_is_refused() {
        let config = parse(&atr_section(None)).unwrap();

        let err = format!(
            "{:#}",
            config
                .validate(&reading_gold(&["order_book_depths"], &[]))
                .unwrap_err()
        );

        assert!(err.contains("ATR") && err.contains(GOLD), "{err}");
        assert!(err.contains("model.target.bracket.atr"), "{err}");
    }

    /// A meta model on the section's target, held out its last 12 hours.
    const META: &str = r#"
        [meta]
        window = "36h"
        holdout = "12h"
        margin = 0.25
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
        assert_eq!(meta.margin, 0.25);
        assert_eq!(
            meta.minimums(),
            Minimums {
                min_pnl: 0.5,
                min_trades: 20
            }
        );
        assert_eq!(meta.learner, LearnerConfig::Baseline {});
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

    /// Sides on a time-only target: their meta model learns those trades,
    /// no exit being needed.
    #[test]
    fn a_meta_model_on_a_time_only_target_is_loaded() {
        let toml = section(r#"target = { limit = "30m", atr = { period = 1 } }"#);

        let config =
            load_warming(&format!("{toml}\n{META}"), SignedDuration::from_hours(1)).unwrap();

        assert_eq!(config.target.spec.bracket, None);
        assert!(config.meta.is_some());
    }

    /// A meta takes a trade from a margin in money, not from a probability:
    /// a threshold, alone as a section of probabilities has it or beside a
    /// margin, is refused as the section is parsed, saying that the margin
    /// takes its place, not as any unknown field.
    #[test]
    fn a_meta_threshold_is_refused_at_parsing_naming_the_margin() {
        for meta in [
            META.replace("margin = 0.25", "threshold = 0.55"),
            format!("{META}\nthreshold = 0.55"),
        ] {
            let err = refused_parsing(&format!("{SECTION}\n{meta}"));

            assert!(
                err.contains("threshold") && err.contains("`margin`"),
                "{err}"
            );
            assert!(!err.contains("unknown field"), "{err}");
        }
    }

    #[test]
    fn a_meta_without_a_margin_is_refused_at_parsing() {
        let meta = META.replace("margin = 0.25", "");

        let err = refused_parsing(&format!("{SECTION}\n{meta}"));

        assert!(err.contains("missing field `margin`"), "{err}");
    }

    /// A meta takes or leaves a trade of the target's size: a size is
    /// refused as the section is parsed, naming the size the trade is of.
    #[test]
    fn a_meta_size_is_refused_at_parsing_naming_the_size_of_the_trade() {
        let toml = format!("{SECTION}\n{META}\nsize = {{ min = 0.5, max = 2 }}");

        let err = refused_parsing(&toml);

        assert!(
            err.contains("`size`") && err.contains("trade_size"),
            "{err}"
        );
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

    /// The target's limit, 45 minutes, does not fit the meta's window: that
    /// window is the meta's own to widen, the limit the sides' too.
    #[test]
    fn a_meta_window_not_longer_than_the_targets_limit_is_refused() {
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
    fn a_meta_margin_not_a_number_is_refused() {
        let err = meta_refused("margin = nan");

        assert!(err.contains("model.meta.margin"), "{err}");
    }

    #[test]
    fn a_meta_minimum_of_no_trade_is_refused() {
        let err = meta_refused("min_trades = 0");

        assert!(err.contains("model.meta.min_trades"), "{err}");
    }
}
