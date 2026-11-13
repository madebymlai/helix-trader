//! The rows of LSE's sources that are not prices, as `LseRow`s, each
//! stamped when its values are known (`Stamp`): a reference collection's
//! (`/v3/reference/export`), a macro series' of releases or of yields
//! (`/v3/macro/export`), and an instrument's splits or dividends
//! (`/v3/factors`). Which call a source names, what is asked of LSE for the
//! rows known over a window, and whether LSE may not have loaded them all
//! yet.
//!
//! A row known a fixed time after its key, the column LSE's export selects
//! rows by (`Stamp::lag`), is asked by the dates of the keys of the rows
//! known in the window; one known at a time of another column, which may
//! be any time after its key, is asked of the whole series up to the
//! window's last date, so that none known in the window is missed. The
//! rows outside the window are left out. LSE stores a date the morning
//! after it (see `export`): the window's last key date may not be loaded,
//! the current UTC day, or the day before while LSE holds no row of it
//! (`export::loaded`). A release LSE has not filled yet is a row of no
//! actual: a window of releases holding one, past, is not loaded either.

use std::fmt;

use anyhow::{Context, bail, ensure};
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp, civil::Date, tz::TimeZone};
use nautilus_core::UnixNanos;
use serde_json::Value;

use super::{
    client::LseHttpClient,
    error::LseHttpError,
    export::{loaded, midnight},
    models::{LseFactors, Stamp},
    parse::{Keyed, Parsed, parse_rows},
    query::{Dates, FactorsQuery, MacroQuery, ReferenceQuery},
};
use crate::types::LseRow;

/// The sources of LSE's macro series, by the kind of its rows: its
/// releases, its yields, and its observations, which are refused.
const MACRO_RELEASE: &str = "macro_release";
const MACRO_YIELD: &str = "macro_yield";
const MACRO_OBSERVATION: &str = "macro_observation";
/// The sources of an instrument's factors: its splits and its dividends.
const FACTORS_SPLITS: &str = "factors_splits";
const FACTORS_DIVIDENDS: &str = "factors_dividends";
/// The reference collections whose rows are releases, each filled with its
/// `actual` once out.
const RELEASES: &[&str] = &["economic_calendar"];
/// The field of a release's value, empty until LSE fills it.
const ACTUAL: &str = "actual";
/// A UTC day.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// A source of LSE's rows that are not prices, and the series of it, as a
/// data type names them (`LseRow::data_type`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowSource {
    source: String,
    symbol: String,
    call: Call,
}

/// The call that answers a source's rows, and how they are stamped.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    /// A reference collection, its rows stamped as its `Stamp` says.
    Reference(Stamp),
    /// A macro series of a kind.
    Macro(MacroKind),
    /// An instrument's splits or dividends, by its market and symbol,
    /// each known at its day's end.
    Factors {
        factor: Factor,
        market: String,
        symbol: String,
    },
}

/// A kind of LSE's macro series that the client serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MacroKind {
    /// Releases, each keyed by the time it came out, `released_at`, and
    /// known then, with its `actual` once LSE has filled it.
    Release,
    /// Yields, a day's close each, keyed by its `day`, and known at its
    /// end.
    Yield,
}

impl MacroKind {
    /// LSE's name of the kind, as its export asks it (`kind`).
    fn name(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Yield => "yield",
        }
    }

    /// The column the export selects its rows by.
    fn time_column(self) -> &'static str {
        match self {
            Self::Release => "released_at",
            Self::Yield => "day",
        }
    }

    fn stamp(self) -> Stamp {
        match self {
            Self::Release => Stamp::Key,
            Self::Yield => Stamp::DateEnd,
        }
    }
}

/// An instrument's factor LSE serves: its splits, each of its `date`, or
/// its dividends, each of its `ex_date`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Factor {
    Splits,
    Dividends,
}

impl Factor {
    /// The field of a factor's day.
    fn day_field(self) -> &'static str {
        match self {
            Self::Splits => "date",
            Self::Dividends => "ex_date",
        }
    }
}

/// What LSE holds of a source's rows known over a window.
#[derive(Debug)]
pub(crate) struct Fetched {
    /// The rows known over the window, in the order they are known.
    pub(crate) rows: Vec<LseRow>,
    /// Where the span LSE may not have loaded yet starts, if the window
    /// reaches into it: from there, LSE may hold rows it does not yet.
    pub(crate) unloaded_from: Option<Timestamp>,
}

impl RowSource {
    /// The series `symbol` of LSE's `source`: a reference collection by
    /// its name (`bond_yields`), a macro series of releases or of yields by
    /// its code (`macro_release`, `macro_yield`), or an instrument's splits
    /// or dividends by its symbol and market (`factors_splits`,
    /// `factors_dividends` of `AAPL-US_STOCKS`, LSE's symbol, a hyphen and
    /// its market in capitals, as an `LSEDGE` series' symbol names its
    /// market).
    ///
    /// # Errors
    ///
    /// Returns an error, before LSE is asked: for a macro series of
    /// observations, each keyed by the day of its period, not when it came
    /// out; for a reference collection the client has not classified,
    /// whose rows may be known at any time; for factors of a symbol naming
    /// no market.
    pub(crate) fn of(source: &str, symbol: &str) -> anyhow::Result<Self> {
        let call = match source {
            MACRO_OBSERVATION => bail!(
                "LSE's {source} {symbol} is refused: an observation is keyed by the day of its \
                 period, not when it came out (US core inflation's September at 1 September, out \
                 on 14 October), so it would be read weeks early; ask its releases, \
                 {MACRO_RELEASE}, the same values at the times they came out, from about 2015"
            ),
            MACRO_RELEASE => Call::Macro(MacroKind::Release),
            MACRO_YIELD => Call::Macro(MacroKind::Yield),
            FACTORS_SPLITS | FACTORS_DIVIDENDS => {
                let (instrument, market) = symbol
                    .rsplit_once('-')
                    .filter(|(instrument, market)| !instrument.is_empty() && !market.is_empty())
                    .with_context(|| {
                        format!(
                            "{source} of {symbol}: LSE's factors are of a symbol in a market, \
                             written <symbol>-<MARKET>, as AAPL-US_STOCKS"
                        )
                    })?;
                Call::Factors {
                    factor: if source == FACTORS_DIVIDENDS {
                        Factor::Dividends
                    } else {
                        Factor::Splits
                    },
                    market: market.to_lowercase(),
                    symbol: instrument.to_owned(),
                }
            }
            dataset => Call::Reference(Stamp::of_collection(dataset).with_context(|| {
                format!(
                    "{dataset} is no source of LSE's rows the client stamps: neither a reference \
                     collection it knows when the rows of are known, nor {MACRO_RELEASE}, \
                     {MACRO_YIELD}, {FACTORS_SPLITS} or {FACTORS_DIVIDENDS}"
                )
            })?),
        };
        Ok(Self {
            source: source.to_owned(),
            symbol: symbol.to_owned(),
            call,
        })
    }

    /// Whether its rows are releases, each with its `actual` once LSE has
    /// filled it: the economic calendar's and a macro series of releases'.
    fn awaits_actual(&self) -> bool {
        match &self.call {
            Call::Reference(_) => RELEASES.contains(&self.source.as_str()),
            Call::Macro(kind) => *kind == MacroKind::Release,
            Call::Factors { .. } => false,
        }
    }

    /// When its rows are known.
    fn stamp(&self) -> Stamp {
        match &self.call {
            Call::Reference(stamp) => *stamp,
            Call::Macro(kind) => kind.stamp(),
            Call::Factors { .. } => Stamp::DateEnd,
        }
    }
}

impl fmt::Display for RowSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the rows of {} in LSE's {}", self.symbol, self.source)
    }
}

/// The rows of `source` known over `[start, end]`, asked through `http`,
/// in the order they are known, and where the span LSE has not loaded as
/// of `now` starts, if the window reaches into it (see the module's doc). A
/// row known at no time LSE gives is left out, with a warning naming it.
///
/// # Errors
///
/// Returns an error naming the call that failed: LSE refused it, answered
/// as its docs do not say, or could not be reached; a reference collection
/// LSE's catalogue does not list; a file not read (see `parse_rows`); or a
/// window from before the oldest of an instrument's dividends LSE cut to
/// their newest.
pub(crate) async fn fetch(
    http: &LseHttpClient,
    source: &RowSource,
    start: Timestamp,
    end: Timestamp,
    now: Timestamp,
) -> anyhow::Result<Fetched> {
    let stamp = source.stamp();
    let dates = dates(stamp, start, end);
    let RowSource {
        source: name,
        symbol,
        call,
    } = source;
    let parsed = match call {
        Call::Reference(_) => {
            let collection = http
                .reference_collections()
                .await
                .context("reading LSE's reference catalogue")?
                .into_iter()
                .find(|collection| collection.dataset == *name)
                .with_context(|| format!("LSE lists no reference collection {name}"))?;
            let query = ReferenceQuery::new(name, symbol, dates);
            let file = http.reference_export(&query).await;
            read(file, &query, source, &collection.time_column, stamp)?
        }
        Call::Macro(kind) => {
            let query = MacroQuery::new(kind.name(), symbol, dates);
            let file = http.macro_export(&query).await;
            read(file, &query, source, kind.time_column(), stamp)?
        }
        Call::Factors {
            factor,
            market,
            symbol: instrument,
        } => {
            let query = FactorsQuery::new(market, instrument);
            let factors = http
                .factors(&query)
                .await
                .with_context(|| format!("asking for {query}"))?;
            factor_rows(factors, *factor, name, symbol, start)
                .with_context(|| format!("reading {query}"))?
        }
    };
    if !parsed.refused.is_empty() {
        log::warn!(
            "Left out {} of {source}, known at no time LSE gives: {}",
            parsed.refused.len(),
            parsed.refused.join("; ")
        );
    }
    // The window's last key date, the one LSE may not have loaded yet.
    let lag = stamp.lag().unwrap_or(SignedDuration::ZERO);
    let last = end - lag;
    let rows_of_last = parsed
        .rows
        .iter()
        .any(|keyed| midnight(keyed.key) == midnight(last));
    let mut unloaded_from = (!loaded(last, rows_of_last, now)).then(|| midnight(last) + lag);
    let window = UnixNanos::from(start)..=UnixNanos::from(end);
    let mut rows: Vec<LseRow> = parsed
        .rows
        .into_iter()
        .map(|keyed| keyed.row)
        .filter(|row| window.contains(&row.ts_init))
        .collect();
    rows.sort_by_key(|row| row.ts_init);
    if source.awaits_actual()
        && let Some(unfilled) = rows
            .iter()
            .find(|row| row.fields.get(ACTUAL).is_none_or(Value::is_null))
    {
        let unfilled = Timestamp::from(unfilled.ts_init);
        unloaded_from = Some(unloaded_from.map_or(unfilled, |from| from.min(unfilled)));
    }
    Ok(Fetched {
        rows,
        unloaded_from,
    })
}

/// The rows of `file`, LSE's answer to the export `query` of `source`,
/// each keyed by `time_column` and stamped as `stamp` (see `parse_rows`),
/// none if LSE holds no row of the selection.
///
/// # Errors
///
/// Returns an error naming `query` if LSE refused the export or its file is
/// not read.
fn read(
    file: Result<Option<Bytes>, LseHttpError>,
    query: &impl fmt::Display,
    source: &RowSource,
    time_column: &str,
    stamp: Stamp,
) -> anyhow::Result<Parsed> {
    let Some(file) = file.with_context(|| format!("exporting {query}"))? else {
        return Ok(Parsed::default());
    };
    parse_rows(file, &source.source, &source.symbol, time_column, stamp)
        .with_context(|| format!("reading {query}"))
}

/// The dates of the keys of the rows stamped as `stamp` known over
/// `[start, end]`: those of the window, `stamp`'s lag before, or, for rows
/// known at another column's time, every date up to the window's last.
fn dates(stamp: Stamp, start: Timestamp, end: Timestamp) -> Dates {
    let date = |at: Timestamp| at.to_zoned(TimeZone::UTC).date();
    match stamp.lag() {
        Some(lag) => Dates {
            from: Some(date(start - lag)),
            to: date(end - lag),
        },
        None => Dates {
            from: None,
            to: date(end),
        },
    }
}

/// The rows of `factors`' `factor`, of `symbol` in LSE's `source`, each
/// keyed by its day (`Factor::day_field`) and known at its end, a dividend
/// with the unit of its amount (`unit`).
///
/// # Errors
///
/// Returns an error if a row has no day, or if LSE cut the dividends to
/// their newest and `start` is not after the oldest of them: the window
/// may lack some LSE left out, which the reference collection `dividends`
/// holds.
fn factor_rows(
    factors: LseFactors,
    factor: Factor,
    source: &str,
    symbol: &str,
    start: Timestamp,
) -> anyhow::Result<Parsed> {
    let day_field = factor.day_field();
    let (rows, unit) = if factor == Factor::Dividends {
        let Some(dividends) = factors.dividends else {
            return Ok(Parsed::default());
        };
        if dividends.truncated {
            // ISO dates, ordered as text.
            let oldest = dividends
                .rows
                .iter()
                .filter_map(|row| row.get(day_field).and_then(Value::as_str))
                .min()
                .context("LSE cut the dividends to none")?;
            ensure!(
                start > midnight_of(oldest)? + DAY,
                "LSE gives the {} newest of the {} dividends of {symbol}, from {oldest}, and the \
                 window from {start} reaches before them: the reference collection dividends \
                 holds them all",
                dividends.rows.len(),
                dividends
                    .count
                    .map_or_else(|| "more".to_string(), |count| count.to_string()),
            );
        }
        (dividends.rows, dividends.unit)
    } else {
        (factors.splits, None)
    };
    let mut parsed = Parsed::default();
    for mut fields in rows {
        let day = fields
            .remove(day_field)
            .and_then(|day| day.as_str().map(str::to_owned))
            .with_context(|| format!("a row of {symbol}'s {source} with no {day_field}"))?;
        let key = midnight_of(&day)?;
        if let Some(unit) = &unit {
            fields.insert("unit".to_owned(), Value::String(unit.clone()));
        }
        let at = UnixNanos::from(key + DAY);
        parsed.rows.push(Keyed {
            key,
            row: LseRow::new(source.to_owned(), symbol.to_owned(), fields, at, at),
        });
    }
    Ok(parsed)
}

/// The start of `day`, a date LSE writes `YYYY-MM-DD`, in UTC.
fn midnight_of(day: &str) -> anyhow::Result<Timestamp> {
    let date: Date = day
        .parse()
        .with_context(|| format!("a day of {day}, not a date"))?;
    Ok(date.to_zoned(TimeZone::UTC)?.timestamp())
}
