//! R, the unit a trade is measured in: one R is the money the trade
//! risks, what its stop loses, or, a trade without a stop, what a move of
//! one ATR against it loses. A trade's P&L in R is its P&L over that, so
//! that trades of any size, on any instrument, are measured alike. A
//! model's margins are in R (`model::config::Margins`,
//! `model::config::MetaConfig::margin`), the least net R its decision
//! expects of a trade it takes.
//!
//! A config writes a value in R with an R suffix, `"0.5R"`: a bare number,
//! of money say, is refused as it is read, so it is never taken for R.

use std::{fmt, str::FromStr};

use anyhow::{Context, anyhow, ensure};
use serde::{Deserialize, Deserializer, de};

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
        ensure!(value.is_finite(), "{text} is not a finite number of R");
        Ok(Self(value))
    }
}

/// The refusal of `written`, a value without its R suffix.
fn unsuffixed(written: impl fmt::Display) -> anyhow::Error {
    anyhow!("{written} has no R suffix: the value is in R, written such as \"0.5R\"")
}

impl fmt::Display for R {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[cfg(test)]
mod tests {
    use serde::de::{
        IntoDeserializer,
        value::{Error, F64Deserializer, I64Deserializer},
    };

    use super::*;

    #[test]
    fn a_value_in_r_reads_from_its_text_with_an_r_suffix() {
        for (text, value) in [("0.5R", 0.5), ("0R", 0.0), ("-1e9R", -1e9), ("2R", 2.0)] {
            assert_eq!(text.parse::<R>().unwrap(), R::new(value), "{text}");
        }
    }

    /// A number without its suffix is not taken for R, nor a suffix
    /// without its number, nor an R that is not finite.
    #[test]
    fn a_value_without_an_r_suffix_or_not_a_finite_number_is_refused() {
        for text in [
            "0.5", "0.5r", "0.5 R", "R", "halfR", "nanR", "infR", "-infR",
        ] {
            assert!(text.parse::<R>().is_err(), "{text}");
        }
    }

    /// Its text reads back to it, so a store keyed by its text is keyed
    /// by its value.
    #[test]
    fn a_value_in_r_is_written_with_its_suffix() {
        assert_eq!(R::new(0.5).to_string(), "0.5R");
        assert_eq!(R::new(-1e9).to_string(), "-1000000000R");
        for r in [R::new(0.5), R::new(-0.25), R::new(1e-7), R::new(3.0)] {
            assert_eq!(r.to_string().parse::<R>().unwrap(), r);
        }
    }

    #[test]
    #[should_panic(expected = "finite")]
    fn a_value_in_r_not_finite_cannot_be_made() {
        let _ = R::new(f64::NAN);
    }

    /// A prediction not finite is no value in R, rather than a panic.
    #[test]
    fn a_finite_value_is_an_r_and_nan_or_an_infinity_none() {
        assert_eq!(R::finite(0.25), Some(R::new(0.25)));
        assert_eq!(R::finite(0.25).map(R::as_f64), Some(0.25));
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(R::finite(value), None, "{value}");
        }
    }

    #[test]
    fn a_value_reaches_an_r_at_or_above_it_and_nan_does_not() {
        let r = R::new(0.25);

        assert!(r.reached_by(0.25) && r.reached_by(0.3));
        assert!(!r.reached_by(0.2) && !r.reached_by(f64::NAN));
        assert!(R::new(-0.5).reached_by(-0.5));
    }

    /// The message of deserializing `deserializer` as an R.
    fn refusal<'de, D: Deserializer<'de, Error = Error>>(deserializer: D) -> String {
        R::deserialize(deserializer).unwrap_err().to_string()
    }

    #[test]
    fn a_value_in_r_deserializes_from_its_text() {
        let text: serde::de::value::StrDeserializer<'_, Error> = "0.5R".into_deserializer();

        assert_eq!(R::deserialize(text).unwrap(), R::new(0.5));
    }

    /// A bare number, an old margin in money say, is refused saying that
    /// the value is in R, an integer as a float.
    #[test]
    fn a_bare_number_is_refused_saying_the_value_is_in_r() {
        let float: F64Deserializer<Error> = 0.5.into_deserializer();
        let integer: I64Deserializer<Error> = 1_i64.into_deserializer();

        for err in [refusal(float), refusal(integer)] {
            assert!(err.contains("in R") && err.contains("\"0.5R\""), "{err}");
        }
    }
}
