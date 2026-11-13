//! What a trade risks: the config's `Risk`, a fraction of equity, and a
//! `Stake`, that risk times the multiple a strategy picks.

use std::{fmt, str::FromStr};

use anyhow::ensure;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de};

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
impl fmt::Display for Risk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}%", (self.0 * Decimal::ONE_HUNDRED).normalize())
    }
}

impl<'de> Deserialize<'de> for Risk {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Percentage)
    }
}

/// Reads a `Risk` from its string, saying what it takes of anything else:
/// a bare number could be a fraction or a percentage.
struct Percentage;

impl de::Visitor<'_> for Percentage {
    type Value = Risk;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        ensure!(
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
impl fmt::Display for Stake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} × {}", self.risk, self.multiple.normalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn risk(s: &str) -> Risk {
        s.parse().unwrap()
    }

    fn decimal(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn a_risk_is_its_percentage_of_equity() {
        assert_eq!(Stake::new(risk("0.5%")).fraction(), decimal("0.005"));
        assert_eq!(Stake::new(risk("2%")).fraction(), decimal("0.02"));
    }

    #[test]
    fn a_risk_not_written_as_a_percentage_is_refused() {
        for written in ["0.005", "0.5", "%", "0.5 %", "half%", ""] {
            let err = written.parse::<Risk>().unwrap_err();
            assert!(err.contains("percentage of equity"), "{written}: {err}");
        }
    }

    #[test]
    fn a_risk_is_proper_above_none_and_below_all_of_equity() {
        assert!(risk("0.01%").is_proper());
        assert!(risk("99.9%").is_proper());
        for improper in ["0%", "-0.5%", "100%", "150%"] {
            assert!(!risk(improper).is_proper(), "{improper}");
        }
    }

    #[test]
    fn a_stake_times_a_multiple_risks_that_many_times_its_risk() {
        let stake = Stake::new(risk("0.5%")).times(decimal("2")).unwrap();
        assert_eq!(stake.fraction(), decimal("0.01"));
        assert_eq!(stake.multiple(), decimal("2"));
        let again = stake.times(decimal("1.5")).unwrap();
        assert_eq!(again.fraction(), decimal("0.015"));
        assert_eq!(again.multiple(), decimal("3"));
    }

    #[test]
    fn a_stake_times_a_multiple_not_positive_is_refused() {
        for multiple in ["0", "-1"] {
            let err = Stake::new(risk("0.5%"))
                .times(decimal(multiple))
                .unwrap_err();
            assert!(err.to_string().contains("positive"), "{multiple}: {err}");
        }
    }

    #[test]
    fn a_risk_reads_and_prints_as_written() {
        assert_eq!(risk("0.5%").to_string(), "0.5%");
        let stake = Stake::new(risk("0.25%")).times(decimal("2")).unwrap();
        assert_eq!(stake.to_string(), "0.25% × 2");
    }
}
