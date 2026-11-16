//! The risk vocabulary: the unit a trade is measured in, and what it risks.
//!
//! - `R`: the unit a trade is measured in. One R is the money the trade
//!   risks, what its stop loses, or, a trade without a stop, what a move of
//!   one ATR against it loses. A trade's P&L in R is its P&L over that, so
//!   that trades of any size, on any instrument, are measured alike. A
//!   model's margins are in R (`model::config::Margins`,
//!   `model::config::MetaConfig::margin`), the least net R its decision
//!   expects of a trade it takes.
//! - `Risk`: the fraction of equity a trade risks at the config's stake,
//!   `[trade] risk`, written as a percentage, `"0.5%"`.
//! - `Stake`: what a trade risks, the config's `Risk` times the multiple a
//!   strategy picks. A trade sent at a stake (`orders::Trade::send`) is
//!   sized to risk that fraction of equity over its one R.
//!
//! Each is read as a config writes it, a bare number refused: a value in R
//! with an R suffix, `"0.5R"`, so that a number, of money say, is never
//! taken for R, and a risk with a percent sign, since a bare number could
//! be a fraction or a percentage.

use std::{fmt::Display, str::FromStr};

use anyhow::Context;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// A finite number of R, as a config writes it: `"0.5R"`, `"-1R"`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct R(f64);

impl R {
    /// `value` R.
    ///
    /// # Panics
    ///
    /// Panics if `value` is not finite.
    #[must_use]
    pub const fn new(value: f64) -> Self {
        assert!(value.is_finite(), "a value in R is a finite number");
        Self(value)
    }

    /// `value` R, if it is finite: none of NaN or an infinity, which a
    /// model's prediction may be.
    #[must_use]
    pub fn finite(value: f64) -> Option<Self> {
        value.is_finite().then_some(Self(value))
    }

    /// Its number of R.
    #[must_use]
    pub const fn as_f64(self) -> f64 {
        self.0
    }

    /// Whether `r`, a value in R, is at least this: NaN is not.
    #[must_use]
    pub fn reached_by(self, r: f64) -> bool {
        r >= self.0
    }
}

impl FromStr for R {
    type Err = anyhow::Error;

    /// The R of `text`, a finite number with an R suffix.
    fn from_str(text: &str) -> anyhow::Result<Self> {
        let number = text.strip_suffix('R').ok_or_else(|| unsuffixed(text))?;
        let value: f64 = number
            .parse()
            .with_context(|| format!("{text} is not a number of R"))?;
        anyhow::ensure!(value.is_finite(), "{text} is not a finite number of R");
        Ok(Self(value))
    }
}

/// The refusal of `written`, a value without its R suffix.
fn unsuffixed(written: impl Display) -> anyhow::Error {
    anyhow::anyhow!("{written} has no R suffix: the value is in R, written such as \"0.5R\"")
}

impl Display for R {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}R", self.0)
    }
}

impl<'de> Deserialize<'de> for R {
    /// An R from its text (`from_str`); a bare number refused, saying that
    /// the value is in R.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(Written)
    }
}

/// How an R is read: its text, or a bare number, refused.
struct Written;

impl de::Visitor<'_> for Written {
    type Value = R;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a value in R, written such as \"0.5R\"")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<R, E> {
        text.parse().map_err(|e| E::custom(format!("{e:#}")))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<R, E> {
        Err(E::custom(unsuffixed(value)))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<R, E> {
        Err(E::custom(unsuffixed(value)))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<R, E> {
        Err(E::custom(unsuffixed(value)))
    }
}

/// The fraction of equity a trade risks at the config's stake, `[trade]
/// risk`, written as a percentage, `"0.5%"`. The config's load holds it
/// to a proper fraction (`is_proper`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Risk(Decimal);

impl Risk {
    /// Whether it is a proper fraction of equity: above none of it and
    /// below all of it.
    #[must_use]
    pub fn is_proper(self) -> bool {
        Decimal::ZERO < self.0 && self.0 < Decimal::ONE
    }
}

/// `"0.5%"`: a number and a percent sign, nothing else.
impl FromStr for Risk {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let percent = s
            .strip_suffix('%')
            .and_then(|number| Decimal::from_str(number).ok())
            .ok_or_else(|| format!("{s:?} is not a percentage of equity, such as \"0.5%\""))?;
        Ok(Self(percent / Decimal::ONE_HUNDRED))
    }
}

/// As it is written, `0.5%`.
impl Display for Risk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}%", (self.0 * Decimal::ONE_HUNDRED).normalize())
    }
}

impl<'de> Deserialize<'de> for Risk {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Percentage)
    }
}

/// As it is written, `"0.5%"`, which reads back to it.
impl Serialize for Risk {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Reads a `Risk` from its string, saying what it takes of anything else:
/// a bare number could be a fraction or a percentage.
struct Percentage;

impl de::Visitor<'_> for Percentage {
    type Value = Risk;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a percentage of equity, such as \"0.5%\"")
    }

    fn visit_str<E: de::Error>(self, s: &str) -> Result<Risk, E> {
        s.parse().map_err(E::custom)
    }
}

/// How much a trade risks: the config's `Risk` times a positive multiple
/// the strategy picks, 1 at the config's stake (`StandardConfig::stake`).
/// A trade sent at a stake (`orders::Trade::send`) is sized to risk that
/// fraction of equity over its one R.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stake {
    risk: Risk,
    multiple: Decimal,
}

impl Stake {
    /// The stake of `risk` alone, its multiple 1.
    #[must_use]
    pub const fn new(risk: Risk) -> Self {
        Self {
            risk,
            multiple: Decimal::ONE,
        }
    }

    /// The stake `multiple` times this one.
    ///
    /// # Errors
    ///
    /// Returns an error if `multiple` is not positive.
    pub fn times(self, multiple: Decimal) -> anyhow::Result<Self> {
        anyhow::ensure!(
            multiple.is_sign_positive() && !multiple.is_zero(),
            "a stake's multiple must be positive, not {multiple}"
        );
        Ok(Self {
            multiple: self.multiple * multiple,
            ..self
        })
    }

    /// Its multiple of the config's risk.
    #[must_use]
    pub const fn multiple(self) -> Decimal {
        self.multiple
    }

    /// The fraction of equity it risks: the risk times the multiple.
    pub(super) fn fraction(self) -> Decimal {
        self.risk.0 * self.multiple
    }
}

/// The risk and its multiple, `0.5% × 2`.
impl Display for Stake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} × {}", self.risk, self.multiple.normalize())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::de::{
        IntoDeserializer,
        value::{Error, F64Deserializer, I64Deserializer},
    };

    use super::*;

    #[rstest]
    #[case::half("0.5R", 0.5)]
    #[case::nought("0R", 0.0)]
    #[case::exponent("-1e9R", -1e9)]
    #[case::whole("2R", 2.0)]
    fn a_value_in_r_reads_from_its_text_with_an_r_suffix(#[case] text: &str, #[case] value: f64) {
        assert_eq!(text.parse::<R>().unwrap(), R::new(value));
    }

    /// A number without its suffix is not taken for R, nor a suffix
    /// without its number, nor an R that is not finite; the refusal opens
    /// with the text written, which the example it offers, `"0.5R"`, would
    /// otherwise hold for `0.5`.
    #[rstest]
    #[case::bare("0.5")]
    #[case::lowercase("0.5r")]
    #[case::spaced("0.5 R")]
    #[case::suffix_alone("R")]
    #[case::word("halfR")]
    #[case::nan("nanR")]
    #[case::infinity("infR")]
    #[case::negative_infinity("-infR")]
    fn a_value_without_an_r_suffix_or_not_a_finite_number_is_refused(#[case] text: &str) {
        let err = text.parse::<R>().unwrap_err();

        assert!(err.to_string().starts_with(&format!("{text} ")), "{err}");
    }

    /// Its text reads back to it, so a store keyed by its text is keyed
    /// by its value.
    #[rstest]
    #[case::half(R::new(0.5), "0.5R")]
    #[case::negative(R::new(-0.25), "-0.25R")]
    #[case::large(R::new(-1e9), "-1000000000R")]
    #[case::small(R::new(1e-7), "0.0000001R")]
    #[case::whole(R::new(3.0), "3R")]
    fn a_value_in_r_is_written_with_its_suffix(#[case] r: R, #[case] text: &str) {
        assert_eq!(r.to_string(), text);
        assert_eq!(text.parse::<R>().unwrap(), r);
    }

    #[rstest]
    #[should_panic(expected = "finite")]
    fn a_value_in_r_not_finite_cannot_be_made() {
        let _ = R::new(f64::NAN);
    }

    /// A prediction not finite is no value in R, rather than a panic.
    #[rstest]
    #[case::finite(0.25, Some(0.25))]
    #[case::nan(f64::NAN, None)]
    #[case::infinity(f64::INFINITY, None)]
    #[case::negative_infinity(f64::NEG_INFINITY, None)]
    fn a_finite_value_is_an_r_and_nan_or_an_infinity_none(
        #[case] value: f64,
        #[case] r: Option<f64>,
    ) {
        assert_eq!(R::finite(value).map(R::as_f64), r);
    }

    #[rstest]
    #[case::at(0.25, 0.25, true)]
    #[case::above(0.25, 0.3, true)]
    #[case::below(0.25, 0.2, false)]
    #[case::nan(0.25, f64::NAN, false)]
    #[case::at_a_negative(-0.5, -0.5, true)]
    fn a_value_reaches_an_r_at_or_above_it_and_nan_does_not(
        #[case] r: f64,
        #[case] value: f64,
        #[case] reached: bool,
    ) {
        assert_eq!(R::new(r).reached_by(value), reached);
    }

    /// The message of deserializing `deserializer` as an R.
    fn refusal<'de, D: Deserializer<'de, Error = Error>>(deserializer: D) -> String {
        R::deserialize(deserializer).unwrap_err().to_string()
    }

    #[rstest]
    fn a_value_in_r_deserializes_from_its_text() {
        let text: serde::de::value::StrDeserializer<'_, Error> = "0.5R".into_deserializer();

        assert_eq!(R::deserialize(text).unwrap(), R::new(0.5));
    }

    /// A bare number, an old margin in money say, is refused naming it and
    /// showing the value written in R, an integer as a float.
    #[rstest]
    #[case::float(refusal::<F64Deserializer<Error>>(0.5.into_deserializer()), "0.5")]
    #[case::integer(refusal::<I64Deserializer<Error>>(1_i64.into_deserializer()), "1")]
    fn a_bare_number_is_refused_saying_the_value_is_in_r(
        #[case] err: String,
        #[case] number: &str,
    ) {
        assert!(err.starts_with(&format!("{number} ")), "{err}");
        assert!(err.contains("\"0.5R\""), "{err}");
    }

    fn risk(s: &str) -> Risk {
        s.parse().unwrap()
    }

    fn decimal(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[rstest]
    #[case::half("0.5%", "0.005")]
    #[case::whole("2%", "0.02")]
    fn a_risk_is_its_percentage_of_equity(#[case] written: &str, #[case] fraction: &str) {
        assert_eq!(Stake::new(risk(written)).fraction(), decimal(fraction));
    }

    /// The refusal quotes the text written.
    #[rstest]
    #[case::fraction("0.005")]
    #[case::bare("0.5")]
    #[case::sign_alone("%")]
    #[case::spaced("0.5 %")]
    #[case::word("half%")]
    #[case::empty("")]
    fn a_risk_not_written_as_a_percentage_is_refused(#[case] written: &str) {
        let err = written.parse::<Risk>().unwrap_err();

        assert!(err.contains(&format!("{written:?}")), "{err}");
    }

    /// A bare number could be a fraction or a percentage: it is refused
    /// saying a risk is a percentage of equity.
    #[rstest]
    fn a_bare_number_is_refused_saying_a_risk_is_a_percentage() {
        let float: F64Deserializer<Error> = 0.005.into_deserializer();

        let err = Risk::deserialize(float).unwrap_err().to_string();

        assert!(err.contains("percentage of equity"), "{err}");
    }

    #[rstest]
    #[case::least("0.01%", true)]
    #[case::most("99.9%", true)]
    #[case::none("0%", false)]
    #[case::negative("-0.5%", false)]
    #[case::all("100%", false)]
    #[case::more_than_all("150%", false)]
    fn a_risk_is_proper_above_none_and_below_all_of_equity(
        #[case] written: &str,
        #[case] proper: bool,
    ) {
        assert_eq!(risk(written).is_proper(), proper);
    }

    #[rstest]
    fn a_stake_times_a_multiple_risks_that_many_times_its_risk() {
        let stake = Stake::new(risk("0.5%")).times(decimal("2")).unwrap();
        assert_eq!(stake.fraction(), decimal("0.01"));
        assert_eq!(stake.multiple(), decimal("2"));
        let again = stake.times(decimal("1.5")).unwrap();
        assert_eq!(again.fraction(), decimal("0.015"));
        assert_eq!(again.multiple(), decimal("3"));
    }

    /// The refusal names the multiple.
    #[rstest]
    #[case::nought("0")]
    #[case::negative("-1")]
    fn a_stake_times_a_multiple_not_positive_is_refused(#[case] multiple: &str) {
        let err = Stake::new(risk("0.5%"))
            .times(decimal(multiple))
            .unwrap_err();

        assert!(err.to_string().contains(multiple), "{err}");
    }

    /// A target's manifest holds its risk as the config writes it.
    #[rstest]
    fn a_risk_serializes_as_written_and_reads_back() {
        let json = serde_json::to_string(&risk("0.25%")).unwrap();
        assert_eq!(json, "\"0.25%\"");
        assert_eq!(serde_json::from_str::<Risk>(&json).unwrap(), risk("0.25%"));
    }

    #[rstest]
    fn a_risk_reads_and_prints_as_written() {
        assert_eq!(risk("0.5%").to_string(), "0.5%");
        let stake = Stake::new(risk("0.25%")).times(decimal("2")).unwrap();
        assert_eq!(stake.to_string(), "0.25% × 2");
    }
}
