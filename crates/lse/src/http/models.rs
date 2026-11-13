//! LSE's answers in JSON, as LSE sends them.

use std::fmt;

use serde::Deserialize;

/// The codes of an export LSE holds no row of: no stored row in the
/// selection, or no session in its dates. Its answer is no rows.
const NO_ROWS: &[&str] = &["data-unavailable", "date-not-trading"];

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
    use super::*;
    use crate::testing::{DATA_UNAVAILABLE, DATE_NOT_TRADING, PLAN_REQUIRED, ROWS_EXHAUSTED};

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

    #[test]
    fn the_rows_a_day_are_a_quota_and_a_plan_is_not() {
        assert!(refusal(ROWS_EXHAUSTED).is_quota());
        assert!(!refusal(PLAN_REQUIRED).is_quota());
    }
}
