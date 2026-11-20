//! LSE's JSON answers: refusals, catalogue listings, the reference catalogue and an instrument's
//! factors, and when the rows of each reference collection are known (`Stamp`).

use std::fmt::Display;

use anyhow::Context;
use jiff::{
    SignedDuration, Timestamp,
    civil::{self, Date, Time},
    tz::TimeZone,
};
use serde::Deserialize;
use serde_json::{Map, Value};

const DATE_NOT_TRADING: &str = "date-not-trading";

/// Only a message beginning with `EMPTY_SELECTION` means no rows (LSE's docs, "Refusal codes").
const DATA_UNAVAILABLE: &str = "data-unavailable";
const EMPTY_SELECTION: &str = "No stored data for this selection";

/// Past the 2,500,000 rows one export may read, or uncountable in time: shorter dates may serve.
const TOO_WIDE: &str = "export-too-wide";

/// The 429s that waiting a minute does not lift: the free plan's weekly allowance and rows a day,
/// resetting on Monday and at 00:00 UTC.
const QUOTAS: &[&str] = &["allowance-exhausted", "rows-exhausted"];

/// LSE's answer to a call it does not serve (LSE's docs, "Answers and refusals"). Its codes are an
/// open set.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseRefusal {
    pub code: String,
    pub message: String,
    /// The cheapest tier on sale that serves the call.
    #[serde(default)]
    pub required_plan: Option<RequiredPlan>,
}

/// A page of LSE's catalogue (`/v3/pairs`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LsePairs {
    pub pairs: Vec<LsePair>,
}

/// A listing in LSE's catalogue, its number naming one instrument.
///
/// A CME contract's name carries its month (`GC futures 2026-12`), or is its symbol alone (`GCZ4`)
/// for some delisted contracts (LSE's docs, "The market: cme", and `test_data/`). Some delisted
/// listings have none (`BRKD` of `nasdaq`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LsePair {
    pub pair_id: u64,
    pub symbol: String,
    pub name: Option<String>,
    pub listed_date: Option<Date>,
    pub delisted_date: Option<Date>,
}

impl Display for LsePair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (", self.symbol)?;
        if let Some(name) = &self.name {
            write!(f, "{name}, ")?;
        }
        write!(f, "pair {}", self.pair_id)?;
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
pub struct RequiredPlan {
    pub name: String,
}

/// LSE's catalogue of its reference collections (`/v3/reference`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReferenceCatalogue {
    pub datasets: Vec<ReferenceCollection>,
}

/// A reference collection (`last`: `2026-11-11` or `2026-11-12 23:29:00`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReferenceCollection {
    pub dataset: String,
    pub time_column: String,
    pub last: Option<String>,
}

impl ReferenceCollection {
    /// Returns the catalogue's `last` in UTC, a date at its start, or `None` if it gives none.
    ///
    /// # Errors
    ///
    /// Returns an error if `last` is neither a date nor a time.
    pub fn last_key(&self) -> anyhow::Result<Option<Timestamp>> {
        self.last
            .as_deref()
            .map(|last| {
                let time: civil::DateTime = last.parse().with_context(|| {
                    format!(
                        "LSE's reference catalogue gives {} a last of {last}, neither a date \
                         nor a time",
                        self.dataset
                    )
                })?;
                Ok(time.to_zoned(TimeZone::UTC)?.timestamp())
            })
            .transpose()
    }
}

/// An instrument's splits and dividends (`/v3/factors`), the fields as LSE sends them: a split's
/// day is its `date`, a dividend's its `ex_date`. A market of no dividends (`futures`) has none.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseFactors {
    #[serde(default)]
    pub splits: Vec<Map<String, Value>>,
    #[serde(default)]
    pub dividends: Option<LseDividends>,
}

/// Dividends, newest first, in `unit` (`USD`, or `GBX` pence). LSE gives the 500 newest, and
/// `truncated` says if there are more.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LseDividends {
    pub rows: Vec<Map<String, Value>>,
    pub unit: Option<String>,
    #[serde(default)]
    pub truncated: bool,
    pub count: Option<u64>,
}

const DAY: SignedDuration = SignedDuration::from_hours(24);

/// When a row is known, which is its `ts_event` and `ts_init`.
///
/// The key is the column LSE's export selects rows by (`time_column`). A row is never stamped
/// before its values are all known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stamp {
    /// At 00:00 UTC of the day after its key, a date.
    DateEnd,
    /// At a time of the day of its key, a date, in a time zone.
    ///
    /// An insider trade is at 22:00 New York time of its filing's day, the latest a Form 3, 4 or 5
    /// is filed that day (Regulation S-T, Rule 13(a)(4)).
    DateAt(Time, &'static str),
    /// At the close of its interval of the width, its key being the open.
    Close(SignedDuration),
    /// At its key, a time.
    Key,
    /// At the time in the column, its key a date.
    At(&'static str),
    /// COT's: at the end of its `release_date`, or at its `created_at`, about an hour after the
    /// release, where LSE gives no release date (rows since 28 April 2026). Its key is the
    /// report's Tuesday.
    Release,
    /// Financial reports': a statement (`income`, `balance`, `cashflow`) at its `accepted_date`, a
    /// derived row (`metrics`, `ratios`, `growth`) at the latest of its period's (`FY`, `Q3`). Its
    /// key is the end of its fiscal period.
    Filing,
}

// Closed, so that a collection LSE adds is refused until classified, rather than stamped at its
// key's end, which would reach a reader early for a row published after its key. The 21 of
// `/v3/reference` (`test_data/README.md`).
const COLLECTIONS: &[(&str, Stamp)] = &[
    // LSE stamps an interval at its open: a 4-hour row of 20:00 closes at the day's close
    ("bond_yields", Stamp::DateEnd),
    (
        "bond_yields_minute",
        Stamp::Close(SignedDuration::from_mins(1)),
    ),
    (
        "bond_yields_five_minute",
        Stamp::Close(SignedDuration::from_mins(5)),
    ),
    (
        "bond_yields_fifteen_minute",
        Stamp::Close(SignedDuration::from_mins(15)),
    ),
    (
        "bond_yields_thirty_minute",
        Stamp::Close(SignedDuration::from_mins(30)),
    ),
    (
        "bond_yields_hourly",
        Stamp::Close(SignedDuration::from_hours(1)),
    ),
    (
        "bond_yields_intraday",
        Stamp::Close(SignedDuration::from_hours(1)),
    ),
    (
        "bond_yields_four_hour",
        Stamp::Close(SignedDuration::from_hours(4)),
    ),
    ("sovereign_yields", Stamp::DateEnd),
    ("corporate_bonds", Stamp::DateEnd),
    ("credit_indices", Stamp::DateEnd),
    ("fx_derivatives", Stamp::DateEnd),
    // Keyed by the filing's day, a Form 3, 4 or 5 taking it up to 22:00 Eastern time
    (
        "insider_trades",
        Stamp::DateAt(civil::time(22, 0, 0, 0), "America/New_York"),
    ),
    // Keyed by the report date, LSE giving no time, its figures restated (split-adjusted)
    ("earnings_calendar", Stamp::DateEnd),
    ("dividends", Stamp::DateEnd),
    ("stock_splits", Stamp::DateEnd),
    ("company_profiles", Stamp::Key),
    ("stock_fundamentals", Stamp::Key),
    // Keyed by the day of a release, which `datetime` times, in UTC
    ("economic_calendar", Stamp::At("datetime")),
    ("cot", Stamp::Release),
    ("financial_reports", Stamp::Filing),
];

impl Stamp {
    /// Returns when the rows of reference collection `dataset` are known, or `None` if it is not
    /// classified.
    pub fn of_collection(dataset: &str) -> Option<Self> {
        COLLECTIONS
            .iter()
            .find(|(name, _)| *name == dataset)
            .map(|&(_, stamp)| stamp)
    }

    /// Returns whether its key is a date, not a time.
    pub fn keyed_by_date(self) -> bool {
        !matches!(self, Self::Close(_) | Self::Key)
    }

    /// Returns when a row keyed `key` is known, or `None` if its key alone does not say.
    pub fn known(self, key: Timestamp) -> Option<Timestamp> {
        match self {
            Self::DateEnd => Some(key + DAY),
            Self::DateAt(time, zone) => Some(
                key.to_zoned(TimeZone::UTC)
                    .date()
                    .to_datetime(time)
                    .to_zoned(time_zone(zone))
                    .expect("a key of LSE's, from 1970 to 2262, is a time jiff holds")
                    .timestamp(),
            ),
            Self::Close(width) => Some(key + width),
            Self::Key => Some(key),
            Self::At(_) | Self::Release | Self::Filing => None,
        }
    }

    /// Returns the latest key of a row known at `at`, or `None` if its key alone does not say (see
    /// `known`).
    pub fn key(self, at: Timestamp) -> Option<Timestamp> {
        match self {
            Self::DateEnd => Some(at - DAY),
            Self::DateAt(time, zone) => {
                let local = at.to_zoned(time_zone(zone));
                let date = if local.time() >= time {
                    local.date()
                } else {
                    local
                        .date()
                        .yesterday()
                        .expect("a request's time is past jiff's first day")
                };
                Some(
                    date.to_zoned(TimeZone::UTC)
                        .expect("a request's date is one jiff holds")
                        .timestamp(),
                )
            }
            Self::Close(width) => Some(at - width),
            Self::Key => Some(at),
            Self::At(_) | Self::Release | Self::Filing => None,
        }
    }
}

// From jiff's bundled tz database (`tzdb-bundle-always`), not the host's
fn time_zone(name: &str) -> TimeZone {
    TimeZone::get(name).unwrap_or_else(|e| panic!("jiff's bundled tz database holds {name}: {e}"))
}

impl LseRefusal {
    /// Returns whether the refusal says LSE holds no row of the selection.
    pub fn is_no_rows(&self) -> bool {
        self.code == DATE_NOT_TRADING
            || (self.code == DATA_UNAVAILABLE && self.message.starts_with(EMPTY_SELECTION))
    }

    /// Returns whether the refusal says the export selects too many rows.
    pub fn is_too_wide(&self) -> bool {
        self.code == TOO_WIDE
    }

    /// Returns whether the refusal is a quota of the free plan, refusing every export until reset.
    pub fn is_quota(&self) -> bool {
        QUOTAS.contains(&self.code.as_str())
    }
}

impl Display for LseRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        ALLOWANCE_EXHAUSTED, EXPORT_TOO_WIDE, PAIRS_NASDAQ_BRK, PLAN_REQUIRED, REFERENCE_CATALOGUE,
        ROWS_EXHAUSTED,
    };

    fn refusal(body: &str) -> LseRefusal {
        serde_json::from_str(body).unwrap()
    }

    /// `BRKD` of `nasdaq`, delisted on 16 April 2026, has no name (captured).
    #[rstest]
    fn a_listing_of_no_name_is_read() {
        let listed: LsePairs = serde_json::from_str(PAIRS_NASDAQ_BRK).unwrap();

        let brkd = listed
            .pairs
            .iter()
            .find(|pair| pair.symbol == "BRKD")
            .unwrap();
        assert_eq!(
            (brkd.name.as_deref(), brkd.delisted_date),
            (None, Some(jiff::civil::date(2026, 4, 16)))
        );
    }

    #[rstest]
    #[case::too_wide(EXPORT_TOO_WIDE, true)]
    #[case::rows_exhausted(ROWS_EXHAUSTED, false)]
    fn an_export_past_the_rows_one_export_may_read_is_too_wide(
        #[case] body: &str,
        #[case] too_wide: bool,
    ) {
        assert_eq!(refusal(body).is_too_wide(), too_wide);
    }

    /// Captured on 13 November 2026.
    #[rstest]
    fn every_collection_of_lse_s_catalogue_is_classified() {
        let catalogue: ReferenceCatalogue = serde_json::from_str(REFERENCE_CATALOGUE).unwrap();

        let unclassified: Vec<_> = catalogue
            .datasets
            .iter()
            .filter(|collection| Stamp::of_collection(&collection.dataset).is_none())
            .map(|collection| collection.dataset.as_str())
            .collect();

        assert_eq!(catalogue.datasets.len(), 21);
        assert_eq!(unclassified, Vec::<&str>::new());
    }

    /// Captured on 13 November 2026.
    #[rstest]
    fn each_collection_s_last_key_is_read_from_lse_s_catalogue() {
        let catalogue: ReferenceCatalogue = serde_json::from_str(REFERENCE_CATALOGUE).unwrap();
        let last = |name: &str| {
            catalogue
                .datasets
                .iter()
                .find(|collection| collection.dataset == name)
                .unwrap()
                .last_key()
                .unwrap()
        };

        assert_eq!(last("insider_trades"), Some(at("2026-10-13T00:00:00Z")));
        assert_eq!(last("bond_yields_minute"), Some(at("2026-11-12T23:29:00Z")));
        assert!(
            catalogue
                .datasets
                .iter()
                .all(|collection| collection.last_key().unwrap().is_some())
        );
    }

    #[rstest]
    fn a_last_key_that_is_no_date_or_time_fails_naming_its_collection() {
        let collection = ReferenceCollection {
            dataset: "insider_trades".to_string(),
            time_column: "filing_date".to_string(),
            last: Some("yesterday".to_string()),
        };

        let err = collection.last_key().unwrap_err().to_string();

        assert!(err.contains("insider_trades"), "{err}");
    }

    #[rstest]
    fn a_collection_keyed_by_a_time_in_lse_s_catalogue_is_classified_as_keyed_by_a_time() {
        let catalogue: ReferenceCatalogue = serde_json::from_str(REFERENCE_CATALOGUE).unwrap();
        let keyed_by_time = ["ts", "updated_at"];

        for collection in catalogue.datasets {
            let stamp = Stamp::of_collection(&collection.dataset).unwrap();
            assert_eq!(
                stamp.keyed_by_date(),
                !keyed_by_time.contains(&collection.time_column.as_str()),
                "{collection:?}"
            );
        }
    }

    #[rstest]
    #[case::daily("bond_yields", Some(Stamp::DateEnd))]
    #[case::four_hours(
        "bond_yields_four_hour",
        Some(Stamp::Close(SignedDuration::from_hours(4)))
    )]
    #[case::hourly(
        "bond_yields_intraday",
        Some(Stamp::Close(SignedDuration::from_hours(1)))
    )]
    #[case::a_minute("bond_yields_minute", Some(Stamp::Close(SignedDuration::from_mins(1))))]
    #[case::market_values("sovereign_yields", Some(Stamp::DateEnd))]
    #[case::a_filing_s_day(
        "insider_trades",
        Some(Stamp::DateAt(civil::time(22, 0, 0, 0), "America/New_York"))
    )]
    #[case::a_snapshot("company_profiles", Some(Stamp::Key))]
    #[case::a_release_s_time("economic_calendar", Some(Stamp::At("datetime")))]
    #[case::cot("cot", Some(Stamp::Release))]
    #[case::financial_reports("financial_reports", Some(Stamp::Filing))]
    #[case::unclassified("no_such_collection", None)]
    fn a_collection_s_rows_are_stamped_when_they_are_known(
        #[case] dataset: &str,
        #[case] stamp: Option<Stamp>,
    ) {
        assert_eq!(Stamp::of_collection(dataset), stamp);
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn insider_trades() -> Stamp {
        Stamp::of_collection("insider_trades").unwrap()
    }

    /// An insider trade is known at 22:00 New York time: 02:00 UTC the next day in summer, 03:00 in
    /// winter.
    #[rstest]
    #[case::a_date(Stamp::DateEnd, "2026-10-13T00:00:00Z", Some("2026-10-14T00:00:00Z"))]
    #[case::an_interval(
        Stamp::Close(SignedDuration::from_hours(4)),
        "2026-10-13T20:00:00Z",
        Some("2026-10-14T00:00:00Z")
    )]
    #[case::a_snapshot(Stamp::Key, "2026-10-13T05:08:01Z", Some("2026-10-13T05:08:01Z"))]
    #[case::a_filing_s_day_in_summer(
        insider_trades(),
        "2026-10-13T00:00:00Z",
        Some("2026-10-14T02:00:00Z")
    )]
    #[case::a_filing_s_day_in_winter(
        insider_trades(),
        "2026-12-01T00:00:00Z",
        Some("2026-12-02T03:00:00Z")
    )]
    #[case::a_release(Stamp::At("datetime"), "2026-10-13T00:00:00Z", None)]
    #[case::cot(Stamp::Release, "2026-10-13T00:00:00Z", None)]
    #[case::a_filing(Stamp::Filing, "2026-10-13T00:00:00Z", None)]
    fn a_row_is_known_when_its_key_says(
        #[case] stamp: Stamp,
        #[case] key: &str,
        #[case] known: Option<&str>,
    ) {
        assert_eq!(stamp.known(at(key)), known.map(at));
        assert_eq!(
            stamp.key(known.map_or_else(|| at(key), at)),
            known.map(|_| at(key))
        );
    }

    /// Filed on 13 October 2026, known at 02:00 UTC on the 14th: a second before, the 12th.
    #[rstest]
    fn an_insider_trade_is_not_known_before_22_00_new_york_time_of_its_filing_s_day() {
        assert_eq!(
            insider_trades().key(at("2026-10-14T01:59:59Z")),
            Some(at("2026-10-12T00:00:00Z"))
        );
    }

    #[rstest]
    #[case::rows_a_day(ROWS_EXHAUSTED, true)]
    #[case::allowance(ALLOWANCE_EXHAUSTED, true)]
    #[case::plan(PLAN_REQUIRED, false)]
    fn the_rows_a_day_are_a_quota_and_a_plan_is_not(#[case] body: &str, #[case] quota: bool) {
        assert_eq!(refusal(body).is_quota(), quota);
    }
}
