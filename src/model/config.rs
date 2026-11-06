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
use rust_decimal::{Decimal, prelude::FromPrimitive};
use serde::Deserialize;

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
/// target = { take_profit = { atr = 2.0 }, stop = { atr = 1.0 }, limit = "45m" }
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
    /// models learn whether each side of it gains.
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

/// A meta model: on each decision that picks a side, whether that side's
/// trade, the target on it, wins, learned from the sides' features, the
/// side picked and its probability. It is retrained on the sides'
/// schedule, right after them, or, the sides never retrained, trained
/// once and kept.
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
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
    /// The probability from which the trade of the side picked is taken.
    pub threshold: f64,
    /// The least net P&L per trade the meta's decisions earn on the
    /// holdout for its model to be published, in the target's P&L
    /// currency (`holdout`).
    pub min_pnl: f64,
    /// The fewest trades the meta's decisions take on the holdout for its
    /// model to be published.
    pub min_trades: usize,
    /// The learner and its settings.
    pub learner: LearnerConfig,
    /// How a trade taken is sized from the probability; none trades the
    /// target's size.
    pub size: Option<Sizing>,
}

/// A trade's size from the meta model's probability: a multiple of the
/// target's size, `min` at the threshold, `max` at a probability of 1,
/// linear between, such as `size = { min = 0.5, max = 2 }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sizing {
    pub min: Decimal,
    pub max: Decimal,
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
    /// window, the holdout not positive or leaving no fitted row, the
    /// threshold not a probability, the minimums invalid (see `validate_minimums`), or the
    /// sizing's multiples not positive with the minimum not above the
    /// maximum.
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

    /// The multiple of the target's size a trade taken at `probability`
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
                "model.target.atr",
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

    use super::*;
    use crate::strategy::TickSeries;

    const GOLD: &str = "XAUUSD.ICMARKETS";

    /// A model section deciding every hour, with a bracket closed after 45
    /// minutes, on a window of 48 hours, 12 of them held out.
    const SECTION: &str = r#"
        decision = { every = "1h" }
        target = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "45m" }
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

    /// The fields of the config errors refusing `toml`.
    fn refused_fields(toml: &str) -> Vec<String> {
        let err = load(toml).unwrap_err();
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
    /// the cadence holds these limits and the window alone refuses them.
    #[test]
    fn a_limit_not_shorter_than_the_window_is_refused_under_the_limit() {
        for target in [
            r#"target = { limit = "48h" }"#,
            r#"target = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "49h" }"#,
        ] {
            let toml = section(target).replace(r#"every = "1h""#, r#"every = "72h""#);

            assert!(
                refused(&toml).contains("not shorter than the window"),
                "{}",
                refused(&toml)
            );
            assert_eq!(
                refused_fields(&toml),
                ["model.target.limit", "model.holdout"],
                "{target}"
            );
        }
    }

    /// Deciding every hour, a bracket closed after an hour, or a time-only
    /// trade of an hour and a half, would still be open at the next
    /// decision.
    #[test]
    fn a_limit_not_shorter_than_the_decision_cadence_is_refused_naming_both() {
        for (target, named) in [
            (
                r#"take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "1h""#,
                "limit 1h",
            ),
            (r#"limit = "90m""#, "limit 1h 30m"),
        ] {
            let toml = section(&format!(r#"target = {{ {target} }}"#));

            let err = refused(&toml);

            assert!(
                err.contains(named) && err.contains("cadence 1h"),
                "{named}: {err}"
            );
            assert_eq!(refused_fields(&toml), ["model.target.limit"], "{named}");
        }
    }

    /// A trade closed at its time alone: no exit, and no ATR to read a
    /// price of, warm or subscribe to.
    #[test]
    fn a_target_of_a_limit_alone_loads_with_no_exits() {
        let config = parse(&section(r#"target = { limit = "30m" }"#)).unwrap();

        config.validate(&reading("EURUSD.ICMARKETS")).unwrap();
        assert_eq!(config.target.spec.bracket(), None);
        assert_eq!(
            config.target.settles_within(),
            SignedDuration::from_mins(30)
        );
        assert!(
            config
                .target
                .atr_bars(config.decision, &reading_gold(&["order_book_depths"], &[]))
                .unwrap()
                .is_none()
        );
    }

    /// `Trade::send_as` has both exits or neither: one alone would refuse
    /// every order, so the load refuses it, beside the section's others.
    #[test]
    fn a_take_profit_or_a_stop_alone_is_refused_under_the_target() {
        for target in [
            r#"target = { take_profit = { bps = 7.5 }, limit = "45m" }"#,
            r#"target = { stop = { bps = 5 }, limit = "45m" }"#,
        ] {
            let toml = section(target);

            assert_eq!(refused_fields(&toml), ["model.target"], "{target}");

            let with_another = toml.replace("min_trades = 30", "min_trades = 0");
            let fields = refused_fields(&with_another);
            assert!(
                fields.iter().any(|field| field == "model.target")
                    && fields.iter().any(|field| field == "model.min_trades"),
                "{target}: {fields:?}"
            );
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
        let toml = section(r#"target = { take_profit = { bps = 7.5 }, stop = { bps = 5 } }"#);

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
            r#"target = {{ take_profit = {{ atr = 2.0 }}, stop = {{ atr = 1.5 }}, limit = "45m"{atr} }}"#
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

    /// A time-only target has no distance to warm an ATR for, whatever the
    /// warmup, and an `atr` table is refused on it as on any target with
    /// no distance in ATRs.
    #[test]
    fn a_time_only_target_has_no_atr_to_warm_and_refuses_an_atr_table() {
        let config = parse(&section(r#"target = { limit = "30m" }"#)).unwrap();
        config
            .validate(&reading_gold_for(SignedDuration::ZERO))
            .unwrap();

        let toml = section(r#"target = { limit = "30m", atr = { period = 3 } }"#);

        assert_eq!(refused_fields(&toml), ["model.target.atr"]);
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
    }

    /// A meta model on the section's target, held out its last 12 hours.
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

    /// Sides on a time-only target: their meta model learns those trades,
    /// no exit being needed.
    #[test]
    fn a_meta_model_on_a_time_only_target_is_loaded() {
        let toml = section(r#"target = { limit = "30m" }"#);

        let config = load(&format!("{toml}\n{META}")).unwrap();

        assert_eq!(config.target.spec.bracket(), None);
        assert!(config.meta.is_some());
    }

    #[test]
    fn a_meta_sizing_reads_from_toml() {
        let config = load(&format!(
            "{SECTION}\n{META}\nsize = {{ min = 0.5, max = 2 }}"
        ))
        .unwrap();

        assert_eq!(
            config.meta.unwrap().size,
            Some(Sizing {
                min: Decimal::new(5, 1),
                max: Decimal::from(2)
            })
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

    /// From the threshold 0.6 to a probability of 1, 0.5 to 2 times the
    /// target's size: 0.8, half way, is 1.25 times.
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
