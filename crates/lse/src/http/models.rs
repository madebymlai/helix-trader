//! LSE's answers in JSON, as LSE sends them: its refusals, its catalogue's
//! listings, its reference catalogue and an instrument's factors; and when
//! the rows of each of LSE's reference collections are known (`Stamp`).

use std::fmt;

use anyhow::Context;
use jiff::{
    SignedDuration, Timestamp,
    civil::{self, Date, Time},
    tz::TimeZone,
};
use serde::Deserialize;
use serde_json::{Map, Value};

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
/// instrument, its symbol and, where LSE knows them, its name and the days
/// it was listed and delisted. A CME contract's name says its month, as in
/// `GC futures 2026-12`, or is its symbol alone, as in `GCZ4`, for some of
/// the contracts delisted (LSE's docs, "The market: cme", and `test_data/`);
/// some listings delisted have none (`BRKD` of `nasdaq`, captured).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LsePair {
    pub(crate) pair_id: u64,
    pub(crate) symbol: String,
    pub(crate) name: Option<String>,
    pub(crate) listed_date: Option<Date>,
    pub(crate) delisted_date: Option<Date>,
}

impl fmt::Display for LsePair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// A reference collection, as LSE's catalogue lists it: its name, the
/// column holding its rows' time, a date or a time, and the last of them
/// LSE holds, if any (`last`: `2026-11-11`, `2026-11-12 23:29:00`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ReferenceCollection {
    pub(crate) dataset: String,
    pub(crate) time_column: String,
    pub(crate) last: Option<String>,
}

impl ReferenceCollection {
    /// The key of the last rows LSE holds of the collection, its
    /// catalogue's `last` in UTC, LSE's time: a date at its start, or a
    /// time; `None` if the catalogue gives none.
    ///
    /// # Errors
    ///
    /// Returns an error naming the collection if `last` is neither a date
    /// nor a time.
    pub(crate) fn last_key(&self) -> anyhow::Result<Option<Timestamp>> {
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

/// An instrument's splits and dividends, as LSE's `/v3/factors` answers
/// them, whole: each split and each dividend a row of fields as LSE sends
/// them, a split's day its `date`, a dividend's its `ex_date`. An
/// instrument of a market of no dividends (`futures`) has none.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LseFactors {
    #[serde(default)]
    pub(crate) splits: Vec<Map<String, Value>>,
    #[serde(default)]
    pub(crate) dividends: Option<LseDividends>,
}

/// An instrument's dividends, newest first, their amounts in `unit` (`USD`,
/// or `GBX`, pence), LSE giving the 500 newest alone of more, `truncated`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LseDividends {
    pub(crate) rows: Vec<Map<String, Value>>,
    pub(crate) unit: Option<String>,
    #[serde(default)]
    pub(crate) truncated: bool,
    pub(crate) count: Option<u64>,
}

/// A UTC day.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// When a row of a source is known, so what it is stamped at, both
/// `ts_event` and `ts_init`, read from its columns: its key, the column
/// LSE's export selects its rows by (a reference collection's
/// `time_column`), or a column of its release. A row is never stamped
/// before its values are all known, so that no reader sees one early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stamp {
    /// At the end of its key, a date, 00:00 UTC of the next: a day's close,
    /// an ex-date or a report date.
    DateEnd,
    /// At a time of the day of its key, a date, in a time zone: an insider
    /// trade at 22:00 New York time of its SEC filing's day, the latest a
    /// Form 3, 4 or 5 is filed on that day (Regulation S-T, Rule 13(a)(4):
    /// one "submitted by direct transmission commencing on or before 10
    /// p.m." Eastern time "shall be deemed filed on the same business
    /// day"), which EDGAR disseminates that evening.
    DateAt(Time, &'static str),
    /// At the close of its interval of the width, its key the interval's
    /// open, as a candle is.
    Close(SignedDuration),
    /// At its key, a time: when a snapshot was updated or a release came
    /// out.
    Key,
    /// At the time in the column, its release, its key a date.
    At(&'static str),
    /// COT's: at the end of its `release_date`, the Friday a report of a
    /// Tuesday comes out, or at its `created_at`, LSE's ingest of the
    /// report about an hour after it came out, where LSE gives no release
    /// date (its rows since 28 April 2026); its key the report's Tuesday.
    Release,
    /// Financial reports': a statement's (`income`, `balance`, `cashflow`)
    /// at its `accepted_date`, when the SEC accepted its filing; a row
    /// derived from the statements (`metrics`, `ratios`, `growth`) at the
    /// latest `accepted_date` of the statements of its period, its key and
    /// `period` (`FY`, `Q3`); its key the end of its fiscal period.
    Filing,
}

/// LSE's reference collections, all 21 its catalogue lists (`/v3/reference`,
/// `test_data/README.md`), each with when its rows are known: a closed
/// table, so that a collection LSE adds is refused until it is classified,
/// rather than stamped at its key's end, which a row published after its
/// key would reach a reader early at.
const COLLECTIONS: &[(&str, Stamp)] = &[
    // A day's close; an interval's close, LSE stamping an interval at its
    // open (a 4-hour row of 20:00 closes at the day's close).
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
    // A day's market values, a row a field (`symbol, bucket, date, field,
    // value`): bid, ask and their day's range, yields, spreads.
    ("sovereign_yields", Stamp::DateEnd),
    ("corporate_bonds", Stamp::DateEnd),
    ("credit_indices", Stamp::DateEnd),
    ("fx_derivatives", Stamp::DateEnd),
    // Keyed by the day of its publication, an SEC filing's, a Form 3, 4 or
    // 5 taking it up to 22:00 Eastern time.
    (
        "insider_trades",
        Stamp::DateAt(civil::time(22, 0, 0, 0), "America/New_York"),
    ),
    // Keyed by the report date, LSE giving no report time; its figures as
    // LSE holds them today, restated (split-adjusted).
    ("earnings_calendar", Stamp::DateEnd),
    // Keyed by the ex-date or the effective date, the day an instrument's
    // price takes them.
    ("dividends", Stamp::DateEnd),
    ("stock_splits", Stamp::DateEnd),
    // One snapshot of each company, keyed by when LSE updated it.
    ("company_profiles", Stamp::Key),
    ("stock_fundamentals", Stamp::Key),
    // Keyed by the day of a release, which `datetime` times, in UTC.
    ("economic_calendar", Stamp::At("datetime")),
    ("cot", Stamp::Release),
    ("financial_reports", Stamp::Filing),
];

impl Stamp {
    /// When the rows of LSE's reference collection `dataset` are known, or
    /// `None` if it is none the client has classified.
    pub(crate) fn of_collection(dataset: &str) -> Option<Self> {
        COLLECTIONS
            .iter()
            .find(|(name, _)| *name == dataset)
            .map(|&(_, stamp)| stamp)
    }

    /// Whether its key is a date, not a time.
    pub(crate) fn keyed_by_date(self) -> bool {
        !matches!(self, Self::Close(_) | Self::Key)
    }

    /// When a row keyed `key` is known, if its key alone says: at its
    /// date's end, at its date's time in its zone, at its interval's close
    /// or at its key. `None` for a row known at a time of another column,
    /// which may be any time after its key.
    pub(crate) fn known(self, key: Timestamp) -> Option<Timestamp> {
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

    /// The latest key of a row known at `at`, if its key alone says when it
    /// is known (see `known`): the rows known over a window are those keyed
    /// from the key of its start to the key of its end, a date's at its
    /// start. `None` for a row known at a time of another column, of which
    /// the rows known over a window may be keyed any time before it.
    pub(crate) fn key(self, at: Timestamp) -> Option<Timestamp> {
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

/// The time zone `name` of the tz database jiff bundles in the build
/// (its `tzdb-bundle-always` feature), whatever the host holds.
fn time_zone(name: &str) -> TimeZone {
    TimeZone::get(name).unwrap_or_else(|e| panic!("jiff's bundled tz database holds {name}: {e}"))
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
        EXPORT_TOO_WIDE, PAIRS_NASDAQ_BRK, PLAN_REQUIRED, REFERENCE_CATALOGUE, ROWS_EXHAUSTED,
        TICKS_NOT_STORED,
    };

    fn refusal(body: &str) -> LseRefusal {
        serde_json::from_str(body).unwrap()
    }

    /// LSE's catalogue gives some delisted listings no name: `BRKD` of
    /// `nasdaq`, delisted on 16 April 2026, captured.
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

    /// Each collection LSE's catalogue lists, captured on 13 November 2026,
    /// is classified: none is refused as unknown.
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

    /// LSE's catalogue gives each collection's last key, a date or a time,
    /// in UTC, captured on 13 November 2026.
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

    /// A collection's key is its catalogue's time column: a date for those
    /// LSE keys by date, a time for its intervals and snapshots.
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
    #[case::daily("bond_yields", Stamp::DateEnd)]
    #[case::four_hours("bond_yields_four_hour", Stamp::Close(SignedDuration::from_hours(4)))]
    #[case::hourly("bond_yields_intraday", Stamp::Close(SignedDuration::from_hours(1)))]
    #[case::a_minute("bond_yields_minute", Stamp::Close(SignedDuration::from_mins(1)))]
    #[case::market_values("sovereign_yields", Stamp::DateEnd)]
    #[case::a_filing_s_day(
        "insider_trades",
        Stamp::DateAt(civil::time(22, 0, 0, 0), "America/New_York")
    )]
    #[case::a_snapshot("company_profiles", Stamp::Key)]
    #[case::a_release_s_time("economic_calendar", Stamp::At("datetime"))]
    #[case::cot("cot", Stamp::Release)]
    #[case::financial_reports("financial_reports", Stamp::Filing)]
    fn a_collection_s_rows_are_stamped_when_they_are_known(
        #[case] dataset: &str,
        #[case] stamp: Stamp,
    ) {
        assert_eq!(Stamp::of_collection(dataset), Some(stamp));
    }

    #[rstest]
    fn a_collection_not_classified_is_none() {
        assert_eq!(Stamp::of_collection("no_such_collection"), None);
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn insider_trades() -> Stamp {
        Stamp::of_collection("insider_trades").unwrap()
    }

    /// A row of a date is known a day after its key, one of an interval an
    /// interval after, a snapshot at its key, and an insider trade at 22:00
    /// New York time of its filing's day, 02:00 UTC the next day in summer
    /// and 03:00 in winter; and the latest key of a row known then is that
    /// key again. One released at another column's time is known any time
    /// after its key, which says nothing of it.
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

    /// Filed on 13 October 2026, known at 02:00 UTC on the 14th: a second
    /// before, the latest filing day known is the 12th.
    #[rstest]
    fn an_insider_trade_is_not_known_before_22_00_new_york_time_of_its_filing_s_day() {
        assert_eq!(
            insider_trades().key(at("2026-10-14T01:59:59Z")),
            Some(at("2026-10-12T00:00:00Z"))
        );
    }

    #[test]
    fn the_rows_a_day_are_a_quota_and_a_plan_is_not() {
        assert!(refusal(ROWS_EXHAUSTED).is_quota());
        assert!(refusal(ALLOWANCE_EXHAUSTED).is_quota());
        assert!(!refusal(PLAN_REQUIRED).is_quota());
    }
}
