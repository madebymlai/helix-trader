//! LSE's answers in JSON, as LSE sends them: its refusals and its
//! catalogue's listings.

use std::fmt;

use jiff::SignedDuration;
use jiff::civil::Date;
use serde::Deserialize;

/// The code of an export of no session in its dates. Its answer is no rows.
const DATE_NOT_TRADING: &str = "date-not-trading";

/// The code of data LSE does not serve, among which a selection with no
/// stored row, whose message alone begins with `EMPTY_SELECTION` (LSE's
/// docs, "Refusal codes"). Only that one's answer is no rows.
const DATA_UNAVAILABLE: &str = "data-unavailable";
const EMPTY_SELECTION: &str = "No stored data for this selection";

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

/// LSE's catalogue of its reference collections (`/v3/reference`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ReferenceCatalogue {
    pub(crate) datasets: Vec<ReferenceCollection>,
}

/// A reference collection, as LSE's catalogue lists it: its name and the
/// column holding its rows' time, a date or a time.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ReferenceCollection {
    pub(crate) dataset: String,
    pub(crate) time_column: String,
}

/// LSE's reference collections of a row an interval, each keyed by the
/// open of its interval (`ts`), and the interval's width: the bond yields
/// at 1, 5, 15 and 30 minutes, an hour (`bond_yields_hourly`, and
/// `bond_yields_intraday`, LSE's "Hourly bond yields") and 4 hours (LSE's
/// catalogue, `/v3/reference`; `test_data/README.md`: a 4-hour row of
/// 20:00 closes at the day's close).
const INTERVALS: &[(&str, SignedDuration)] = &[
    ("bond_yields_minute", SignedDuration::from_mins(1)),
    ("bond_yields_five_minute", SignedDuration::from_mins(5)),
    ("bond_yields_fifteen_minute", SignedDuration::from_mins(15)),
    ("bond_yields_thirty_minute", SignedDuration::from_mins(30)),
    ("bond_yields_hourly", SignedDuration::from_hours(1)),
    ("bond_yields_intraday", SignedDuration::from_hours(1)),
    ("bond_yields_four_hour", SignedDuration::from_hours(4)),
];

/// What a reference collection keys its rows by, its time column: a date,
/// a row of each, or the open of an interval of a width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowKey {
    Date,
    Open(SignedDuration),
}

impl RowKey {
    /// How long after its key a row's values are all known: a date's day,
    /// an interval's width. A row is stamped then, as a candle is at its
    /// close, so that no reader sees a value before it is known.
    pub(crate) fn span(self) -> SignedDuration {
        match self {
            Self::Date => SignedDuration::from_hours(24),
            Self::Open(width) => width,
        }
    }
}

impl ReferenceCollection {
    /// What the collection keys its rows by: the open of its interval for
    /// LSE's interval collections (`INTERVALS`), a date for every other.
    pub(crate) fn key(&self) -> RowKey {
        INTERVALS
            .iter()
            .find(|(dataset, _)| *dataset == self.dataset)
            .map_or(RowKey::Date, |&(_, width)| RowKey::Open(width))
    }
}

impl LseRefusal {
    /// Whether the refusal says LSE holds no row of the selection.
    pub(crate) fn is_no_rows(&self) -> bool {
        self.code == DATE_NOT_TRADING
            || (self.code == DATA_UNAVAILABLE && self.message.starts_with(EMPTY_SELECTION))
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
        ALLOWANCE_EXHAUSTED, DATA_UNAVAILABLE, DATA_UNAVAILABLE_TODAY, DATE_NOT_TRADING,
        EXPORT_TOO_WIDE, PLAN_REQUIRED, ROWS_EXHAUSTED, TICKS_NOT_STORED,
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
        assert!(refusal(DATA_UNAVAILABLE_TODAY).is_no_rows());
        assert!(refusal(DATE_NOT_TRADING).is_no_rows());
        assert!(!refusal(PLAN_REQUIRED).is_no_rows());
    }

    /// LSE's `data-unavailable` names an empty selection only by its
    /// message.
    #[rstest]
    fn data_unavailable_for_a_market_storing_no_trades_is_not_no_rows() {
        assert!(!refusal(TICKS_NOT_STORED).is_no_rows());
    }

    #[rstest]
    fn an_export_past_the_rows_one_export_may_read_is_too_wide() {
        assert!(refusal(EXPORT_TOO_WIDE).is_too_wide());
        assert!(!refusal(ROWS_EXHAUSTED).is_too_wide());
    }

    fn collection(dataset: &str) -> ReferenceCollection {
        ReferenceCollection {
            dataset: dataset.to_string(),
            time_column: "date".to_string(),
        }
    }

    /// LSE's catalogue lists the bond yields at every interval beside the
    /// daily ones; its other collections key their rows by date.
    #[rstest]
    #[case::daily("bond_yields", RowKey::Date)]
    #[case::four_hours("bond_yields_four_hour", RowKey::Open(SignedDuration::from_hours(4)))]
    #[case::hourly("bond_yields_intraday", RowKey::Open(SignedDuration::from_hours(1)))]
    #[case::a_minute("bond_yields_minute", RowKey::Open(SignedDuration::from_mins(1)))]
    #[case::cot("cot", RowKey::Date)]
    fn a_collection_keys_its_rows_by_date_or_by_the_open_of_its_interval(
        #[case] dataset: &str,
        #[case] key: RowKey,
    ) {
        assert_eq!(collection(dataset).key(), key);
    }

    /// A row of a date is known at the date's end, one of an interval at
    /// its close.
    #[rstest]
    fn a_row_is_known_a_day_or_an_interval_after_its_key() {
        assert_eq!(RowKey::Date.span(), SignedDuration::from_hours(24));
        assert_eq!(
            RowKey::Open(SignedDuration::from_hours(4)).span(),
            SignedDuration::from_hours(4)
        );
    }

    #[test]
    fn the_rows_a_day_are_a_quota_and_a_plan_is_not() {
        assert!(refusal(ROWS_EXHAUSTED).is_quota());
        assert!(refusal(ALLOWANCE_EXHAUSTED).is_quota());
        assert!(!refusal(PLAN_REQUIRED).is_quota());
    }
}
