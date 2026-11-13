//! LSE's answers in JSON, as LSE sends them: its refusals and its
//! catalogue's listings.

use std::fmt;

use jiff::civil::Date;
use serde::Deserialize;

/// The codes of an export LSE holds no row of: no stored row in the
/// selection, or no session in its dates. Its answer is no rows.
const NO_ROWS: &[&str] = &["data-unavailable", "date-not-trading"];

/// The code of an export past the rows one export may read (2,500,000 on
/// every plan), or one LSE could not count in time: shorter dates may be
/// served.
const TOO_WIDE: &str = "export-too-wide";

/// The codes of a 429 that waiting a minute does not lift: the free
/// plan's weekly allowance and its rows a day, which reset on Monday and
/// at 00:00 UTC.
const QUOTAS: &[&str] = &["allowance-exhausted", "rows-exhausted"];

/// A refusal, LSE's answer to a call it does not serve, its HTTP status
/// saying the kind: a code, a sentence, and the fields named for that
/// code, of which the plan that would serve the call is kept (LSE's docs,
/// "Answers and refusals" and "Refusal codes"). LSE's codes are an open
/// set, kept as they come.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LseRefusal {
    pub(crate) code: String,
    pub(crate) message: String,
    /// The cheapest tier on sale that serves the call, for a plan, a
    /// market or a level the key lacks.
    #[serde(default)]
    pub(crate) required_plan: Option<RequiredPlan>,
}

/// A page of LSE's catalogue of a market's instruments (`/v3/pairs`), of
/// which the listings are kept.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LsePairs {
    pub(crate) pairs: Vec<LsePair>,
}

/// A listing in LSE's catalogue: its number, which always names one
/// instrument, its symbol, its name and, where LSE knows them, the days it
/// was listed and delisted. A CME contract's name says its month, as in
/// `GC futures 2026-12`, or is its symbol alone, as in `GCZ4`, for some of
/// the contracts delisted (LSE's docs, "The market: cme", and `test_data/`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LsePair {
    pub(crate) pair_id: u64,
    pub(crate) symbol: String,
    pub(crate) name: String,
    pub(crate) listed_date: Option<Date>,
    pub(crate) delisted_date: Option<Date>,
}

impl fmt::Display for LsePair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}, pair {}", self.symbol, self.name, self.pair_id)?;
        if let Some(listed) = self.listed_date {
            write!(f, ", listed {listed}")?;
        }
        if let Some(delisted) = self.delisted_date {
            write!(f, ", delisted {delisted}")?;
        }
        f.write_str(")")
    }
}

/// A plan LSE names in a refusal.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct RequiredPlan {
    pub(crate) name: String,
    pub(crate) market: Option<String>,
    pub(crate) tier: Option<String>,
    pub(crate) subscribe_url: Option<String>,
}

impl LseRefusal {
    /// Whether the refusal says LSE holds no row of the selection.
    pub(crate) fn is_no_rows(&self) -> bool {
        NO_ROWS.contains(&self.code.as_str())
    }

    /// Whether the refusal says the export selects too many rows.
    pub(crate) fn is_too_wide(&self) -> bool {
        self.code == TOO_WIDE
    }

    /// Whether the refusal is a quota of the free plan, past which every
    /// export is refused until it resets.
    pub(crate) fn is_quota(&self) -> bool {
        QUOTAS.contains(&self.code.as_str())
    }
}

impl fmt::Display for LseRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)?;
        if let Some(plan) = &self.required_plan {
            write!(f, "; required plan: {}", plan.name)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::testing::{
        ALLOWANCE_EXHAUSTED, DATA_UNAVAILABLE, DATE_NOT_TRADING, EXPORT_TOO_WIDE, PLAN_REQUIRED,
        ROWS_EXHAUSTED,
    };

    fn refusal(body: &str) -> LseRefusal {
        serde_json::from_str(body).unwrap()
    }

    #[test]
    fn a_refusal_reads_as_its_code_message_and_required_plan() {
        assert_eq!(
            refusal(PLAN_REQUIRED).to_string(),
            "plan-required: This feed requires a subscription; required plan: CME Group Lite"
        );
    }

    #[test]
    fn no_stored_row_and_no_session_are_no_rows() {
        assert!(refusal(DATA_UNAVAILABLE).is_no_rows());
        assert!(refusal(DATE_NOT_TRADING).is_no_rows());
        assert!(!refusal(PLAN_REQUIRED).is_no_rows());
    }

    #[rstest]
    fn an_export_past_the_rows_one_export_may_read_is_too_wide() {
        assert!(refusal(EXPORT_TOO_WIDE).is_too_wide());
        assert!(!refusal(ROWS_EXHAUSTED).is_too_wide());
    }

    #[test]
    fn the_rows_a_day_are_a_quota_and_a_plan_is_not() {
        assert!(refusal(ROWS_EXHAUSTED).is_quota());
        assert!(refusal(ALLOWANCE_EXHAUSTED).is_quota());
        assert!(!refusal(PLAN_REQUIRED).is_quota());
    }
}
