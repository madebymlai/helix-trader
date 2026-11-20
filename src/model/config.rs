//! Holds the model section of a strategy's TOML.
//!
//! A strategy has at most two levels of models: the side models of a `ModelConfig`, and one meta
//! model (`MetaConfig`). The meta model decides whether to take the trade of the picked side. A
//! meta section has no meta section of its own. Writing one is refused at load as an unknown
//! field.

use jiff::SignedDuration;
use nautilus_common::config::{ConfigError, ConfigErrorCollector, ConfigResult};
use serde::{Deserialize, de::IgnoredAny};

use super::{
    dataset::DatasetVenue,
    decide::DecisionRule,
    learner::LearnerConfig,
    schedule::Retrain,
    targets::{Side, Target, TargetConfig},
};
use crate::strategy::{
    TradedInstrument,
    conversions::Conversions,
    requirements::DataRequirements,
    risk::{R, Risk},
};

/// A strategy's model.
///
/// For example:
///
/// ```toml
/// [model]
/// decision = { every = "1h" }
/// target = { bracket = { take_profit = { atr = 2.0 }, stop = { atr = 1.0 } }, limit = "45m" }
/// retrain = "168h"
/// window = "720h"
/// calibration = "120h"
/// margins = { long = "0.5R", short = "0.5R" }
/// min_rows = 300
/// learner = { kind = "lightgbm" }
/// dataset = { commission = { rate = "0.00003" }, balance = "10000 USD" }
/// ```
///
/// The target `T` is a `Target` until `ModelSection::trading`.
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig<T = TargetConfig> {
    /// When the strategy decides.
    pub decision: DecisionRule,
    /// The target. The side models learn its net R for each side.
    pub target: T,
    /// How often the model is retrained, at every multiple of the interval since the Unix epoch
    /// (`model::schedule`).
    ///
    /// Left out, the model never retrains (`Retrain::Never`), and each `train --at` trains a model
    /// set at its time.
    #[serde(default)]
    #[builder(default, into)]
    pub retrain: Retrain,
    /// How far back from a retrain the rows go.
    pub window: SignedDuration,
    /// The last part of the window. The models are calibrated on it and not fitted on it (`train`).
    pub calibration: SignedDuration,
    /// The sides traded, each with its margin.
    pub margins: Margins,
    /// The fewest rows of each slice, at least one. A train with fewer fails.
    pub min_rows: usize,
    /// The learner and its settings.
    pub learner: LearnerConfig,
    /// The `[model.meta]` table. It decides whether to take each trade. None takes every trade.
    pub meta: Option<MetaConfig>,
    /// The venue the dataset runs are made at. It keys the dataset and the stores
    /// (`dataset::fingerprint`). So `train`, a backtest and a live node read the one store,
    /// whatever the commission, balance and latency of the backtest.
    pub dataset: DatasetVenue,
}

/// A meta model.
///
/// On each decision that picks a side, it learns the realized net R of that side's trade. It
/// learns from the sides' features, the side picked, and the calibrated level and side call of the
/// decision. It is retrained right after the sides, or at each `train --at` if they never are.
///
/// A probability `threshold` or a `size` is refused at parsing (`MetaFields`).
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(try_from = "MetaFields")]
pub struct MetaConfig {
    /// How far back from a retrain its rows go. Its train fits an out-of-fold pair of the sides as
    /// of this much before. The out-of-sample picks of that pair over the window are the rows.
    pub window: SignedDuration,
    /// The last part of the window. The model is calibrated on it and not fitted on it.
    pub calibration: SignedDuration,
    /// The calibrated net R the meta model must expect from the trade (`earnings::MetaMargin`).
    pub margin: R,
    /// The fewest rows of each slice, at least one.
    pub min_rows: usize,
    /// The learner and its settings.
    pub learner: LearnerConfig,
}

/// The keys of a `MetaConfig`, plus `threshold` and `size`.
///
/// Parsing refuses `threshold` and `size`, naming what takes their place, before it looks for a
/// margin.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetaFields {
    window: SignedDuration,
    calibration: SignedDuration,
    margin: Option<R>,
    min_rows: usize,
    learner: LearnerConfig,
    threshold: Option<IgnoredAny>,
    size: Option<IgnoredAny>,
}

impl TryFrom<MetaFields> for MetaConfig {
    type Error = String;

    fn try_from(fields: MetaFields) -> Result<Self, String> {
        let MetaFields {
            window,
            calibration,
            margin,
            min_rows,
            learner,
            threshold,
            size,
        } = fields;
        if threshold.is_some() {
            return Err(
                "a meta has no probability `threshold`: it takes the trade of the side \
                 picked from `margin`, the net R it expects of the trade, such as \"0.2R\""
                    .to_string(),
            );
        }
        if size.is_some() {
            return Err(
                "a meta has no `size`: it takes or leaves the decision's trade, which the \
                 strategy sends at its stake, `[trade] risk` times the multiple it picks"
                    .to_string(),
            );
        }
        Ok(Self {
            window,
            calibration,
            margin: margin.ok_or("missing field `margin`")?,
            min_rows,
            learner,
        })
    }
}

/// The sides a strategy trades, each with the calibrated expected net R a trade must clear
/// (`payoff`). A side without one has no model, so it is not trained, stored, scored or traded.
/// For example, `{ long = "0.5R" }` is a long-only model.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Margins {
    pub long: Option<R>,
    pub short: Option<R>,
}

impl Margins {
    /// Returns the margin of `side`, if it is traded.
    #[must_use]
    pub const fn of(&self, side: Side) -> Option<R> {
        match side {
            Side::Long => self.long,
            Side::Short => self.short,
        }
    }

    /// Returns the sides traded, the long first, each with its margin.
    pub fn sides(&self) -> impl Iterator<Item = (Side, R)> {
        Side::BOTH
            .into_iter()
            .filter_map(|side| self.of(side).map(|margin| (side, margin)))
    }
}

/// A model section whose target trades nothing yet.
pub type ModelSection = ModelConfig<Target>;

impl ModelSection {
    /// Returns the section with its target trading `traded` at `risk` through `conversions`
    /// (`Target::trading`).
    #[must_use]
    pub fn trading(
        self,
        traded: &TradedInstrument,
        risk: Risk,
        conversions: Conversions,
    ) -> ModelConfig {
        let Self {
            decision,
            target,
            retrain,
            window,
            calibration,
            margins,
            min_rows,
            learner,
            meta,
            dataset,
        } = self;
        ModelConfig {
            decision,
            target: target.trading(traded, risk, conversions),
            retrain,
            window,
            calibration,
            margins,
            min_rows,
            learner,
            meta,
            dataset,
        }
    }
}

impl ModelConfig {
    /// Checks the section against the strategy's data `requirements`.
    ///
    /// Reports every violation as a `ConfigError` under its field, or as `ConfigError::Multiple`
    /// if there are several.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The decision rule, the target or the learner is invalid (see `DecisionRule::validate`,
    ///   `TargetConfig::validate`, `LearnerConfig::validate`).
    /// - The retrain interval is not positive.
    /// - The target's ATR has no bars built from what the strategy reads for the traded instrument
    ///   (see `TargetConfig::atr_bars`).
    /// - The target's ATR warms over longer than its warmup.
    /// - The target's limit is not shorter than the window.
    /// - The calibration is not positive or leaves no fit slice (see `validate_window`).
    /// - The margins name no side.
    /// - The minimum number of rows is below one (see `validate_min_rows`).
    /// - The meta model is invalid (see `MetaConfig::validate`).
    /// - The target's limit is shorter than the longest time its dataset runs take to process a
    ///   command (see `validate_latency`).
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
            self.calibration,
            &self.target,
            "model",
            "model.target.limit",
            &mut errors,
        );
        errors.check(
            self.margins.sides().next().is_some(),
            ConfigError::invalid_value("model.margins", "the margins name no side to trade"),
        );
        validate_min_rows(self.min_rows, "model", &mut errors);
        if let Some(meta) = &self.meta {
            meta.validate(&self.target, &mut errors);
        }
        validate_latency(&self.target, &self.dataset, &mut errors);
        errors.into_result()
    }
}

/// Refuses a `target` limit shorter than the longest time a dataset run takes to process a command
/// (`DatasetVenue::delay`).
///
/// The time alert would find the entry neither open nor filled (`Order::is_open`), and so not
/// cancel it. A limit of exactly that delay is taken, because the purge timer fires once a minute
/// and the entry is processed before the alert.
fn validate_latency(
    target: &TargetConfig,
    dataset: &DatasetVenue,
    errors: &mut ConfigErrorCollector,
) {
    let Some(latency) = dataset.latency else {
        return;
    };
    let (limit, delay) = (target.settles_within(), dataset.delay());
    errors.check(
        limit >= delay,
        ConfigError::invalid_value(
            "model.target.limit",
            format!(
                "the target's limit {limit:#} is shorter than {delay:#}, the dataset latency \
                 {latency} and the minute a dataset run may wait to process a command: its time \
                 alert could fire before its entry reaches the venue, which would then fill \
                 with no time limit"
            ),
        ),
    );
}

impl MetaConfig {
    /// Pushes onto `errors` every violation of the meta model against the sides' `target`.
    ///
    /// The violations are:
    /// - An invalid learner.
    /// - A limit not shorter than the window.
    /// - A calibration that is not positive or leaves no fit slice.
    /// - A minimum number of rows below one.
    pub fn validate(&self, target: &TargetConfig, errors: &mut ConfigErrorCollector) {
        errors.collect(learner(&self.learner, "model.meta.learner"));
        validate_window(
            self.window,
            self.calibration,
            target,
            "model.meta",
            "model.meta.window",
            errors,
        );
        validate_min_rows(self.min_rows, "model.meta", errors);
    }
}

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

    // An ATR without bars is the target's violation, pushed above
    if let Ok(Some(atr)) = target.atr_bars(rule, requirements) {
        // A dataset run replays only the warmup before its first day, and a cold ATR opens no
        // target.
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

fn learner(learner: &LearnerConfig, field: &str) -> ConfigResult<()> {
    learner
        .validate()
        .map_err(|e| ConfigError::invalid_value(field, format!("{e:#}")))
}

/// Refuses a `target` limit not shorter than `window`, under `limit_field`.
///
/// Also refuses a `calibration` that is not positive or leaves no fit slice (`train`), because a
/// fitted row settles by the start of the calibration.
fn validate_window(
    window: SignedDuration,
    calibration: SignedDuration,
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
        calibration.is_positive() && calibration + limit < window,
        ConfigError::invalid_value(
            format!("{section}.calibration"),
            format!(
                "the calibration {calibration:#} leaves no fit slice in the window {window:#} \
                 after the target's limit {limit:#}"
            ),
        ),
    );
}

fn validate_min_rows(min_rows: usize, section: &str, errors: &mut ConfigErrorCollector) {
    errors.check(
        min_rows >= 1,
        ConfigError::invalid_value(
            format!("{section}.min_rows"),
            format!("the minimum of rows is {min_rows}, not at least one"),
        ),
    );
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::{BarType, NautilusDataType},
        identifiers::InstrumentId,
        types::Money,
    };
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::Commission,
        model::{
            dataset::DatasetVenue,
            targets::{BracketSpec, DEFAULT_ATR, Distance},
        },
        strategy::{AtrArgs, requirements::TickSeries},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";

    /// Hourly decisions, a 45 minute bracket, a 48 hour window with 12 hours calibrated, 30 rows.
    const SECTION: &str = r#"
        decision = { every = "1h" }
        target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 } }, limit = "45m" }
        retrain = "24h"
        window = "48h"
        calibration = "12h"
        margins = { long = "0.5R", short = "0.5R" }
        min_rows = 30
        learner = { kind = "fake" }
        dataset = { commission = { per_quantity = "0.035 USD" }, balance = "10000 USD" }
    "#;

    fn section(line: &str) -> String {
        let key = line.split('=').next().unwrap().trim();
        let kept = SECTION
            .lines()
            .filter(|kept| kept.split('=').next().unwrap().trim() != key);
        kept.chain([line]).collect::<Vec<_>>().join("\n")
    }

    fn reading(instrument: &str) -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(instrument),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    fn gold() -> TradedInstrument {
        TradedInstrument {
            instrument_id: InstrumentId::from(GOLD),
        }
    }

    fn parse(toml: &str) -> anyhow::Result<ModelConfig> {
        Ok(toml::from_str::<ModelSection>(toml)?.trading(
            &gold(),
            "0.5%".parse().unwrap(),
            Conversions::default(),
        ))
    }

    fn load(toml: &str) -> anyhow::Result<ModelConfig> {
        load_warming(toml, SignedDuration::ZERO)
    }

    fn load_warming(toml: &str, warmup: SignedDuration) -> anyhow::Result<ModelConfig> {
        let config = parse(toml)?;
        config.validate(&reading_gold_for(warmup))?;
        Ok(config)
    }

    fn refused(toml: &str) -> String {
        format!("{:#}", load(toml).unwrap_err())
    }

    /// The parse error's message. It is no config error, because nothing else was looked at.
    fn refused_parsing(toml: &str) -> String {
        let err = load(toml).unwrap_err();
        assert!(err.downcast_ref::<ConfigError>().is_none(), "{err:#}");
        format!("{err:#}")
    }

    fn refused_fields(toml: &str) -> Vec<String> {
        refused_fields_warming(toml, SignedDuration::ZERO)
    }

    fn refused_fields_warming(toml: &str, warmup: SignedDuration) -> Vec<String> {
        let err = load_warming(toml, warmup).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err:#}"),
        };
        errors
            .into_iter()
            .map(|e| match e {
                ConfigError::InvalidValue { field, .. } => field,
                e => panic!("unexpected config error {e:?}"),
            })
            .collect()
    }

    #[rstest]
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
        assert_eq!(config.calibration, SignedDuration::from_hours(12));
        assert_eq!(
            config.margins,
            Margins {
                long: Some(R::new(0.5)),
                short: Some(R::new(0.5))
            }
        );
        assert_eq!(config.min_rows, 30);
        assert_eq!(config.learner, LearnerConfig::Fake {});
    }

    #[rstest]
    fn a_section_refuses_an_unknown_field() {
        assert!(refused(&section("retrain_every = \"24h\"")).contains("retrain_every"));
    }

    #[rstest]
    #[case::nought("0h")]
    #[case::negative("-24h")]
    fn a_retrain_interval_not_positive_is_refused_with_the_sections_other_violations(
        #[case] retrain: &str,
    ) {
        let toml =
            section(&format!(r#"retrain = "{retrain}""#)).replace("min_rows = 30", "min_rows = 0");

        assert_eq!(refused_fields(&toml), ["model.retrain", "model.min_rows"]);
    }

    #[rstest]
    #[case::time_only(r#"target = { limit = "48h", atr = { period = 1 } }"#)]
    #[case::bracket(
        r#"target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 } }, limit = "49h" }"#
    )]
    fn a_limit_not_shorter_than_the_window_is_refused_under_the_limit(#[case] target: &str) {
        let warmup = SignedDuration::from_hours(1);
        let toml = section(target);

        assert_eq!(
            refused_fields_warming(&toml, warmup),
            ["model.target.limit", "model.calibration"]
        );
    }

    #[rstest]
    #[case::bracket(
        r#"bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 } }, limit = "1h""#
    )]
    #[case::time_only(r#"limit = "90m", atr = { period = 1 }"#)]
    fn a_target_may_reach_or_outlast_the_next_decision(#[case] target: &str) {
        let warmup = SignedDuration::from_hours(1);
        let toml = section(&format!("target = {{ {target} }}"));

        load_warming(&toml, warmup).unwrap();
    }

    /// No exit. The ATR that measures its unit is as for a bracket: 3 hourly bars of gold's mids.
    #[rstest]
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

    /// One exit alone is refused at parsing, on its own. No other violation is reported.
    #[rstest]
    #[case::take_profit_alone("take_profit = { bps = 7.5 }", "stop")]
    #[case::stop_alone("stop = { bps = 5 }", "take_profit")]
    fn a_take_profit_or_a_stop_alone_is_refused_at_parsing_on_its_own(
        #[case] bracket: &str,
        #[case] missing: &str,
    ) {
        let toml = section(&format!(
            r#"target = {{ bracket = {{ {bracket} }}, limit = "45m" }}"#
        ))
        .replace("min_rows = 30", "min_rows = 0");

        let err = refused_parsing(&toml);

        assert!(err.contains(&format!("missing field `{missing}`")), "{err}");
        assert!(!err.contains("min_rows"), "{err}");
    }

    /// Exits beside the limit are unknown fields of the target. The refusal names one, together the
    /// stop.
    #[rstest]
    #[case::both(
        r#"target = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, limit = "45m" }"#,
        "stop"
    )]
    #[case::take_profit(
        r#"target = { take_profit = { bps = 7.5 }, limit = "45m" }"#,
        "take_profit"
    )]
    #[case::stop(r#"target = { stop = { bps = 5 }, limit = "45m" }"#, "stop")]
    fn exits_beside_the_limit_are_refused_as_unknown(#[case] target: &str, #[case] key: &str) {
        let err = refused_parsing(&section(target));

        assert!(err.contains(&format!("`{key}`")), "{err}");
    }

    /// A 48 hour window less a 45 minute limit: 47 hours of calibration leave a fit slice, and 47
    /// hours and 15 minutes leave none.
    #[rstest]
    #[case::to_the_limit("47h15m")]
    #[case::of_nought("0h")]
    fn a_calibration_leaving_no_fitted_row_before_it_is_refused(#[case] calibration: &str) {
        let toml = section(&format!(r#"calibration = "{calibration}""#));

        assert_eq!(refused_fields(&toml), ["model.calibration"]);
        assert!(load(&section(r#"calibration = "47h""#)).is_ok());
    }

    #[rstest]
    fn margins_naming_no_side_are_refused() {
        assert_eq!(refused_fields(&section("margins = {}")), ["model.margins"]);
        assert!(refused(&section(r#"margins = { both = "0.5R" }"#)).contains("both"));
    }

    #[rstest]
    fn margins_of_one_side_trade_that_side_alone() {
        let config = load(&section(r#"margins = { long = "0.5R" }"#)).unwrap();

        assert_eq!(
            config.margins.sides().collect::<Vec<_>>(),
            [(Side::Long, R::new(0.5))]
        );
        assert_eq!(config.margins.of(Side::Short), None);
    }

    fn dataset_of(line: &str) -> anyhow::Result<DatasetVenue> {
        Ok(load(&section(line))?.dataset)
    }

    #[rstest]
    fn a_section_declares_the_commission_and_balance_of_its_dataset_runs() {
        let exact = Money::from_str_exact("0.035 USD").unwrap();
        assert_ne!(exact, Money::from("0.04 USD"));
        assert_eq!(
            load(SECTION).unwrap().dataset,
            DatasetVenue {
                commission: Commission::PerQuantity(exact),
                balance: Money::from("10000 USD"),
                latency: None,
            }
        );
    }

    #[rstest]
    #[case::per_order(r#"per_order = "3 EUR""#, Commission::PerOrder(Money::from("3 EUR")))]
    #[case::rate(r#"rate = "0.0005""#, Commission::Rate(Decimal::new(5, 4)))]
    #[case::per_quantity(
        r#"per_quantity = "0.035 EUR""#,
        Commission::PerQuantity(Money::from_str_exact("0.035 EUR").unwrap())
    )]
    fn a_dataset_commission_is_per_order_a_rate_or_per_quantity(
        #[case] commission: &str,
        #[case] expected: Commission,
    ) {
        let dataset = dataset_of(&format!(
            r#"dataset = {{ commission = {{ {commission} }}, balance = "1000 EUR" }}"#
        ))
        .unwrap();

        assert_eq!(dataset.commission, expected);
    }

    /// Nautilus refuses a negative amount per order or quantity as it builds the fee model. Helix
    /// refuses a negative rate.
    #[rstest]
    #[case::per_order(r#"per_order = "-3 USD""#, "greater than or equal to zero")]
    #[case::rate(r#"rate = "-0.0005""#, "commission rate -0.0005 must not be negative")]
    #[case::per_quantity(r#"per_quantity = "-0.035 USD""#, "greater than or equal to zero")]
    fn a_negative_dataset_commission_is_refused_at_parsing(
        #[case] commission: &str,
        #[case] reason: &str,
    ) {
        let line =
            format!(r#"dataset = {{ commission = {{ {commission} }}, balance = "1000 USD" }}"#);

        let err = refused_parsing(&section(&line));

        assert!(err.contains(reason), "{err}");
    }

    #[rstest]
    fn a_dataset_commission_or_balance_that_is_no_amount_is_refused_at_parsing() {
        for dataset in [
            r#"{ commission = { rate = 0.0005 }, balance = "1000 USD" }"#,
            r#"{ commission = { per_quantity = "0.035" }, balance = "1000 USD" }"#,
            r#"{ commission = { per_order = "3 XYZ" }, balance = "1000 USD" }"#,
            r#"{ commission = { rate = "0.0005" }, balance = "1000" }"#,
            r#"{ commission = { rate = "0.0005" }, balance = 1000 }"#,
        ] {
            assert!(
                dataset_of(&format!("dataset = {dataset}")).is_err(),
                "{dataset}"
            );
        }
    }

    /// The refusal names the balance as Nautilus' money type reads it, to the cent.
    #[rstest]
    #[case::nought("0 USD", "0.00 USD")]
    #[case::negative("-1000 USD", "-1000.00 USD")]
    fn a_dataset_balance_not_positive_is_refused_at_parsing(
        #[case] balance: &str,
        #[case] refused: &str,
    ) {
        let line =
            format!(r#"dataset = {{ commission = {{ rate = "0.0005" }}, balance = "{balance}" }}"#);

        let err = format!("{:#}", dataset_of(&line).unwrap_err());

        assert!(err.contains(refused), "{err}");
    }

    /// With 150 ms, each command reaches the venue 150 ms after it is sent.
    #[rstest]
    fn a_dataset_venue_may_declare_the_latency_of_its_runs() {
        let line = r#"dataset = { commission = { rate = "0.0005" }, balance = "1000 USD", latency = "150ms" }"#;

        let latency = dataset_of(line).unwrap().latency;

        assert_eq!(
            latency.map(|latency| latency.to_string()),
            Some("150ms".into())
        );
    }

    fn at_latency(limit: &str, latency: &str) -> String {
        let target = format!(
            r#"target = {{ bracket = {{ take_profit = {{ bps = 7.5 }}, stop = {{ bps = 5 }} }}, limit = "{limit}" }}"#
        );
        let dataset = format!(
            r#"dataset = {{ commission = {{ rate = "0.00003" }}, balance = "10000 USD", latency = "{latency}" }}"#
        );
        let kept = SECTION.lines().filter(|line| {
            let key = line.split('=').next().unwrap().trim();
            key != "target" && key != "dataset"
        });
        kept.chain([target.as_str(), dataset.as_str()])
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A command is processed at most its latency and a minute after it is sent. The cases are 30 s
    /// against 1 m and a minute, and 1 m against 150 ms and a minute.
    #[rstest]
    #[case::of_half_a_minute("30s", "1m")]
    #[case::of_a_minute("1m", "150ms")]
    fn a_limit_shorter_than_its_dataset_runs_latency_and_a_minute_is_refused_naming_both(
        #[case] limit: &str,
        #[case] latency: &str,
    ) {
        let toml = at_latency(limit, latency);

        assert_eq!(refused_fields(&toml), ["model.target.limit"]);
        let err = refused(&toml);
        assert!(
            err.contains(&format!("limit {limit}")) && err.contains(&format!("latency {latency}")),
            "{err}"
        );
    }

    /// 61 s against 1 s and a minute is the shortest limit taken. Also 45 m against 150 ms.
    #[rstest]
    #[case::the_shortest("61s", "1s")]
    #[case::of_45_minutes("45m", "150ms")]
    fn a_limit_as_long_as_its_dataset_runs_latency_and_a_minute_loads(
        #[case] limit: &str,
        #[case] latency: &str,
    ) {
        assert!(load(&at_latency(limit, latency)).is_ok());
    }

    /// The refusal names the latency in jiff's friendly format, and a negative one as ago.
    #[rstest]
    #[case::negative("-1s", "1s ago")]
    #[case::nought("0s", "0s")]
    fn a_dataset_latency_that_is_no_positive_duration_is_refused_at_parsing(
        #[case] latency: &str,
        #[case] named: &str,
    ) {
        let line = format!(
            r#"dataset = {{ commission = {{ rate = "0.0005" }}, balance = "1000 USD", latency = "{latency}" }}"#
        );

        let err = format!("{:#}", dataset_of(&line).unwrap_err());

        assert!(
            err.contains(&format!("must be positive, not {named}")),
            "{err}"
        );
    }

    #[rstest]
    fn a_dataset_venue_refuses_an_unknown_field() {
        let line =
            r#"dataset = { commission = { rate = "0.0005" }, balance = "1000 USD", leverage = 1 }"#;
        let err = format!("{:#}", dataset_of(line).unwrap_err());
        assert!(err.contains("leverage"), "{err}");
    }

    /// The floor is the 20 rows of the adapter's dataset, not the default 200 of a leaf. 20 and 50
    /// load.
    #[rstest]
    #[case::of_one_row(1, &["model.learner"])]
    #[case::of_19_rows(19, &["model.learner"])]
    #[case::of_20_rows(20, &[])]
    #[case::of_50_rows(50, &[])]
    fn lightgbm_leaves_of_fewer_than_20_rows_are_refused(
        #[case] rows: u32,
        #[case] refused: &[&str],
    ) {
        let toml = section(&format!(
            r#"learner = {{ kind = "lightgbm", min_data_in_leaf = {rows} }}"#
        ));

        let fields = match load(&toml) {
            Ok(_) => vec![],
            Err(_) => refused_fields(&toml),
        };

        assert_eq!(fields, refused);
    }

    /// The harness reads the quotes of what it trades when no series reads it.
    #[rstest]
    fn a_traded_instrument_the_strategy_does_not_read_is_no_error() {
        let config = parse(SECTION).unwrap();

        config.validate(&reading("EURUSD.ICMARKETS")).unwrap();
    }

    fn atr_section(atr: Option<&str>) -> String {
        let atr = atr.map_or_default(|atr| format!(", atr = {atr}"));
        section(&format!(
            r#"target = {{ bracket = {{ take_profit = {{ atr = 2.0 }}, stop = {{ atr = 1.5 }}{atr} }}, limit = "45m" }}"#
        ))
    }

    fn reading_gold_for(warmup: SignedDuration) -> DataRequirements {
        DataRequirements {
            warmup,
            ..reading(GOLD)
        }
    }

    /// Deciding hourly, the ATR's bars are hours. Its default 14 bars warm in 14 hours, and 3 in 3.
    #[rstest]
    #[case::of_14_bars(None, 14)]
    #[case::of_3_bars(Some("{ period = 3 }"), 3)]
    fn a_target_atr_warming_longer_than_the_warmup_is_refused(
        #[case] atr: Option<&str>,
        #[case] hours: i64,
    ) {
        let config = parse(&atr_section(atr)).unwrap();

        let shorter = SignedDuration::from_hours(hours - 1);
        let err = format!(
            "{:#}",
            config.validate(&reading_gold_for(shorter)).unwrap_err()
        );
        assert!(err.contains("warmup"), "{err}");
        assert!(err.contains("model.target.bracket.atr"), "{err}");
        let warmup = SignedDuration::from_hours(hours);
        assert!(config.validate(&reading_gold_for(warmup)).is_ok());
    }

    /// Reads the `ticks` of gold by the name Nautilus parses, and `bar_types`, warming 14 hours.
    fn reading_gold(ticks: &[&str], bar_types: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: bar_types
                .iter()
                .map(|&bar_type| BarType::from(bar_type))
                .collect(),
            ticks: ticks
                .iter()
                .map(|data| TickSeries {
                    data: data.parse().unwrap(),
                    instrument_id: InstrumentId::from(GOLD),
                })
                .collect(),
            custom: vec![],
            warmup: SignedDuration::from_hours(14),
        }
    }

    #[rstest]
    fn a_time_only_target_without_an_atr_is_refused_under_its_atr() {
        let toml =
            section(r#"target = { limit = "30m" }"#).replace("min_rows = 30", "min_rows = 0");

        assert_eq!(
            refused_fields(&toml),
            ["model.target.atr", "model.min_rows"]
        );
        let err = refused(&toml);
        assert!(err.contains("ATR") && err.contains("unit"), "{err}");
    }

    #[rstest]
    #[case::bracket(
        atr_section(Some("{ period = 14, bar_interval = '15m' }")),
        "model.target.bracket.atr"
    )]
    #[case::time_only(
        section("target = { limit = '45m', atr = { period = 14, bar_interval = '15m' } }"),
        "model.target.atr"
    )]
    fn target_atr_interval_requires_fourteen_complete_quarter_hours_of_warmup(
        #[case] toml: String,
        #[case] owner: &str,
    ) {
        let toml = toml.replace("every = \"1h\"", "every = \"1m\"");
        let threshold = SignedDuration::from_mins(210);
        assert_eq!(
            refused_fields_warming(&toml, threshold - SignedDuration::from_nanos(1)),
            [owner]
        );
        let config = load_warming(&toml, threshold).unwrap();
        assert_eq!(
            config.decision,
            DecisionRule::Every(SignedDuration::from_mins(1))
        );
        let bars = config
            .target
            .atr_bars(config.decision, &reading(GOLD))
            .unwrap()
            .unwrap();
        assert_eq!(bars.args.period, 14);
        assert_eq!(
            bars.bar_type,
            BarType::from("XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL")
        );
    }

    #[rstest]
    #[case::zero("0s")]
    #[case::negative("-15m")]
    #[case::submillisecond("1us")]
    #[case::unsupported_step("7m")]
    fn target_atr_interval_must_make_a_positive_supported_time_bar(#[case] interval: &str) {
        let toml = atr_section(Some(&format!(
            "{{ period = 1, bar_interval = '{interval}' }}"
        )));
        let fields = refused_fields_warming(&toml, SignedDuration::from_hours(14));
        assert!(
            fields
                .iter()
                .any(|f| f == "model.target.bracket.atr"
                    || f == "model.target.bracket.atr.bar_interval"),
            "{fields:?}"
        );
    }

    #[rstest]
    fn target_atr_interval_cannot_fall_back_to_buildable_decision_bars() {
        let config = parse(&section(
            "target = { limit = '45m', atr = { period = 1, bar_interval = '15m' } }",
        ))
        .unwrap();
        let requirements = reading_gold(
            &[],
            &["XAUUSD.ICMARKETS-1-DAY-LAST-INTERNAL@1-HOUR-EXTERNAL"],
        );
        let err = config.validate(&requirements).unwrap_err();
        assert!(err.to_string().contains("model.target.atr"), "{err}");
    }

    #[rstest]
    fn target_atr_interval_is_not_a_setting_for_basis_point_exits() {
        let toml = section(
            "target = { limit = '45m', bracket = { take_profit = { bps = 20 }, stop = { bps = 10 }, atr = { period = 1, bar_interval = '15m' } } }",
        );
        assert_eq!(
            refused_fields_warming(&toml, SignedDuration::from_hours(1)),
            ["model.target.bracket.atr"]
        );
    }

    /// Its 3 hourly bars warm in 3 hours. Over a shorter warmup it is refused under its table.
    #[rstest]
    fn a_time_only_targets_atr_warming_longer_than_the_warmup_is_refused_under_it() {
        let toml = section(r#"target = { limit = "30m", atr = { period = 3 } }"#);

        assert_eq!(
            refused_fields_warming(&toml, SignedDuration::from_hours(2)),
            ["model.target.atr"]
        );
        assert!(load_warming(&toml, SignedDuration::from_hours(3)).is_ok());
    }

    #[rstest]
    fn an_atr_table_no_distance_uses_is_refused_under_the_bracket() {
        let toml = section(
            r#"target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 5 }, atr = { period = 3 } }, limit = "45m" }"#,
        )
        .replace("min_rows = 30", "min_rows = 0");

        assert_eq!(
            refused_fields(&toml),
            ["model.target.bracket.atr", "model.min_rows"]
        );
    }

    #[rstest]
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
                atr: Some(
                    AtrArgs {
                        period: 3,
                        ..DEFAULT_ATR
                    }
                    .into()
                ),
            })
        );
    }

    #[rstest]
    #[case::a_take_profit_of_nought("take_profit = { bps = 0 }, stop = { bps = 5 }")]
    #[case::a_negative_stop("take_profit = { bps = 7.5 }, stop = { bps = -1 }")]
    fn a_distance_not_positive_is_refused_under_the_bracket(#[case] bracket: &str) {
        let toml = section(&format!(
            r#"target = {{ bracket = {{ {bracket} }}, limit = "45m" }}"#
        ));

        assert_eq!(refused_fields(&toml), ["model.target.bracket"]);
    }

    #[rstest]
    #[case::trades(reading_gold(&["trades"], &[]))]
    #[case::source_bars(reading_gold(
        &[],
        &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"],
    ))]
    fn a_target_atr_is_built_from_the_trades_or_the_bars_of_a_strategy_without_quotes(
        #[case] requirements: DataRequirements,
    ) {
        let config = parse(&atr_section(None)).unwrap();

        assert!(config.validate(&requirements).is_ok());
    }

    const META: &str = r#"
        [meta]
        window = "36h"
        calibration = "12h"
        margin = "0.25R"
        min_rows = 20
        learner = { kind = "baseline" }
    "#;

    #[rstest]
    fn a_meta_section_reads_from_toml() {
        let config = load(&format!("{SECTION}\n{META}")).unwrap();

        let meta = config.meta.unwrap();
        assert_eq!(meta.window, SignedDuration::from_hours(36));
        assert_eq!(meta.calibration, SignedDuration::from_hours(12));
        assert_eq!(meta.margin, R::new(0.25));
        assert_eq!(meta.min_rows, 20);
        assert_eq!(meta.learner, LearnerConfig::Baseline {});
    }

    #[rstest]
    fn a_nested_meta_section_is_refused() {
        let nested = format!(
            "{SECTION}\n{META}\n{}",
            META.replace("[meta]", "[meta.meta]")
        );

        let err = refused(&nested);

        assert!(err.contains("unknown field `meta`"), "{err}");
    }

    #[rstest]
    fn a_meta_model_on_a_time_only_target_is_loaded() {
        let toml = section(r#"target = { limit = "30m", atr = { period = 1 } }"#);

        let config =
            load_warming(&format!("{toml}\n{META}"), SignedDuration::from_hours(1)).unwrap();

        assert_eq!(config.target.spec.bracket, None);
        assert!(config.meta.is_some());
    }

    /// A threshold, alone or beside a margin, is refused at parsing as replaced by the margin. It
    /// is not refused as an unknown field.
    #[rstest]
    #[case::alone(META.replace(r#"margin = "0.25R""#, "threshold = 0.55"))]
    #[case::beside_a_margin(format!("{META}\nthreshold = 0.55"))]
    fn a_meta_threshold_is_refused_at_parsing_naming_the_margin(#[case] meta: String) {
        let err = refused_parsing(&format!("{SECTION}\n{meta}"));

        assert!(
            err.contains("threshold") && err.contains("`margin`"),
            "{err}"
        );
        assert!(!err.contains("unknown field"), "{err}");
    }

    #[rstest]
    fn a_meta_without_a_margin_is_refused_at_parsing() {
        let meta = META.replace(r#"margin = "0.25R""#, "");

        let err = refused_parsing(&format!("{SECTION}\n{meta}"));

        assert!(err.contains("missing field `margin`"), "{err}");
    }

    /// A size is refused at parsing. The refusal points to the stake: `[trade] risk` and the
    /// strategy's multiple of it.
    #[rstest]
    fn a_meta_size_is_refused_at_parsing_pointing_to_the_stake() {
        let toml = format!("{SECTION}\n{META}\nsize = {{ min = 0.5, max = 2 }}");

        let err = refused_parsing(&toml);

        assert!(
            err.contains("`size`") && err.contains("stake") && err.contains("risk"),
            "{err}"
        );
        assert!(!err.contains("trade_size"), "{err}");
    }

    fn meta_refused(line: &str) -> String {
        let key = line.split('=').next().unwrap().trim();
        let meta: Vec<&str> = META
            .lines()
            .filter(|kept| kept.split('=').next().unwrap().trim() != key)
            .chain([line])
            .collect();
        refused(&format!("{SECTION}\n{}", meta.join("\n")))
    }

    /// The 45 minute limit does not fit the meta window. The meta model must widen its own window.
    #[rstest]
    fn a_meta_window_not_longer_than_the_targets_limit_is_refused() {
        let err = meta_refused(r#"window = "45m""#);

        assert!(err.contains("model.meta.window"), "{err}");
    }
}
