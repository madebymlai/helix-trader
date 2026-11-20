//! Typed queries for LSE's calls, with parameters named as LSE's docs name them.
//!
//! The calls are:
//! - Exports of an instrument (`/v3/export`), of a reference collection (`/v3/reference/export`)
//!   and of a macro series (`/v3/macro/export`).
//! - Splits and dividends (`/v3/factors`).
//! - Catalogue lookups (`/v3/pairs`).

use std::fmt::Display;

use jiff::{SignedDuration, Timestamp, civil::Date};
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
};
use serde::{Serialize, Serializer, ser::SerializeMap};

use crate::common::{
    consts::{NASDAQ_MARKET, US_OPTIONS_MARKET},
    symbol::{LseSymbol, NasdaqStock, OpraContract},
};

// The candle widths LSE serves below a day, as the unit, its letter and the steps served (LSE's
// docs, "The selection", `timeframe`).
const CANDLE_WIDTHS: [(BarAggregation, char, &[usize]); 3] = [
    (BarAggregation::Second, 's', &[1, 5, 15, 30]),
    (BarAggregation::Minute, 'm', &[1, 3, 5, 15, 30]),
    (BarAggregation::Hour, 'h', &[1, 4]),
];

/// What an export's rows are, and the parameter that selects them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rows {
    /// Trades, or index values that carry no size (`timeframe=tick`).
    TradeSlice,
    /// Candles, each stamped at its open (`timeframe=5m`).
    Candles(CandleWidth),
    /// The trades of a paid exchange market (`data=trades`). US options serve only their tick
    /// slice.
    Trades,
    /// The quotes of an exchange market (`data=quotes`).
    Quotes,
    /// The Level 2 book, ten bid and ten ask levels (`data=book`).
    Book,
    /// The Level 3 orders, `time, action, side, order_id, price, size` (`data=orders`).
    Orders,
}

impl Serialize for Rows {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::TradeSlice => map.serialize_entry("timeframe", "tick")?,
            Self::Candles(width) => map.serialize_entry("timeframe", &width.to_string())?,
            Self::Trades => map.serialize_entry("data", "trades")?,
            Self::Quotes => map.serialize_entry("data", "quotes")?,
            Self::Book => map.serialize_entry("data", "book")?,
            Self::Orders => map.serialize_entry("data", "orders")?,
        }
        map.end()
    }
}

impl Display for Rows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TradeSlice => f.write_str("trade slice"),
            Self::Candles(width) => write!(f, "{width} candles"),
            Self::Trades => f.write_str("trades"),
            Self::Quotes => f.write_str("quotes"),
            Self::Book => f.write_str("book"),
            Self::Orders => f.write_str("orders"),
        }
    }
}

// A symbol as `/v3/pairs` lists it, or a listing number, which always names one instrument (LSE's
// docs, "Symbols").
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Selection {
    Symbol(String),
    PairId(u64),
}

impl Display for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Symbol(symbol) => f.write_str(symbol),
            Self::PairId(pair_id) => write!(f, "pair {pair_id}"),
        }
    }
}

/// A width of LSE's candles, as a Nautilus bar specification.
///
/// LSE's `1s` is `1-SECOND-LAST` and its `4h` is `4-HOUR-LAST`. Widths below a day are stamped on
/// a grid from 1970. LSE stamps daily, weekly and monthly candles at the session's first traded
/// minute and gives no close. So a bar of those widths is declared from a shorter LSE candle,
/// which Nautilus aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandleWidth(BarSpecification);

impl CandleWidth {
    /// Returns the LSE candles that `bar_type` names.
    ///
    /// They are external bars of the last price, at a width LSE serves below a day.
    ///
    /// # Errors
    ///
    /// Returns an error if `bar_type` is not one of LSE's candles. For daily, weekly and monthly
    /// bars, the error says to declare the bar from a shorter LSE candle.
    pub fn of(bar_type: BarType) -> anyhow::Result<Self> {
        let spec = bar_type.spec();
        anyhow::ensure!(
            bar_type.aggregation_source() == AggregationSource::External,
            "{bar_type}: LSE's candles are EXTERNAL bars"
        );
        anyhow::ensure!(
            spec.price_type == PriceType::Last,
            "{bar_type}: LSE's candles are of the LAST price, their one price"
        );

        if matches!(
            spec.aggregation,
            BarAggregation::Day | BarAggregation::Week | BarAggregation::Month
        ) {
            anyhow::bail!(
                "{bar_type}: LSE stamps its daily, weekly and monthly candles at their \
                 session's first traded minute and gives no close, and builds them from its \
                 minute bars; declare the bar from a shorter LSE candle (1-MINUTE or 4-HOUR \
                 EXTERNAL) instead, which Nautilus aggregates"
            );
        }
        anyhow::ensure!(
            CANDLE_WIDTHS.iter().any(|(aggregation, _, steps)| {
                *aggregation == spec.aggregation && steps.contains(&spec.step.get())
            }),
            "{bar_type}: LSE serves no candles of that width, only 1, 5, 15 and 30 seconds, \
             1, 3, 5, 15 and 30 minutes, and 1 and 4 hours wide"
        );
        Ok(Self(spec))
    }

    /// Returns the `[from, to)` span of the whole candles that close within `[start, end]`.
    ///
    /// Returns none if no candle closes. A candle opens on a multiple of its width from 1970. The
    /// width divides a day.
    pub fn candles_closing(
        self,
        start: Timestamp,
        end: Timestamp,
    ) -> Option<(Timestamp, Timestamp)> {
        let width = self.duration();
        let first = self.floor(start - SignedDuration::from_nanos(1));
        let last = self.floor(end - width);
        (first <= last).then(|| (first, last + width))
    }

    fn floor(self, at: Timestamp) -> Timestamp {
        let (nanos, width) = (at.as_nanosecond(), self.duration().as_nanos());
        Timestamp::from_nanosecond(nanos - nanos.rem_euclid(width))
            .expect("the open of a candle at or before a timestamp is one")
    }

    fn duration(self) -> SignedDuration {
        self.0.timedelta()
    }
}

/// Writes the width as LSE names it, such as `5m`.
impl Display for CandleWidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (_, unit, _) = CANDLE_WIDTHS
            .iter()
            .find(|(aggregation, _, _)| *aggregation == self.0.aggregation)
            .expect("a width LSE serves");
        write!(f, "{}{unit}", self.0.step)
    }
}

/// The file format of an export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    Parquet,
}

/// An export of an instrument's rows over `[from, to)`, in UTC, at prices as published.
///
/// LSE adjusts prices for splits unless told not to (`adjusted=0`). The adapter adjusts nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExportQuery {
    market: String,
    #[serde(flatten)]
    instrument: Selection,
    #[serde(flatten)]
    rows: Rows,
    from: Timestamp,
    to: Timestamp,
    adjusted: u8,
    format: ExportFormat,
}

impl ExportQuery {
    /// Returns the `rows` of the derived-market `series` over `[from, to)`.
    pub fn series(series: &LseSymbol, rows: Rows, from: Timestamp, to: Timestamp) -> Self {
        Self::new(
            series.market(),
            Selection::Symbol(series.symbol().to_owned()),
            rows,
            from,
            to,
        )
    }

    /// Returns the `rows` of the US option `contract` over `[from, to)`, by its OSI symbol.
    pub fn option(contract: &OpraContract, rows: Rows, from: Timestamp, to: Timestamp) -> Self {
        Self::new(
            US_OPTIONS_MARKET,
            Selection::Symbol(contract.osi().to_owned()),
            rows,
            from,
            to,
        )
    }

    /// Returns the `rows` of the Nasdaq TotalView `stock` over `[from, to)`, by its ticker.
    pub fn stock(stock: &NasdaqStock, rows: Rows, from: Timestamp, to: Timestamp) -> Self {
        Self::new(
            NASDAQ_MARKET,
            Selection::Symbol(stock.ticker().to_owned()),
            rows,
            from,
            to,
        )
    }

    /// Returns the `rows` of LSE's listing `pair_id` in `market` over `[from, to)`.
    pub fn listing(market: &str, pair_id: u64, rows: Rows, from: Timestamp, to: Timestamp) -> Self {
        Self::new(market, Selection::PairId(pair_id), rows, from, to)
    }

    fn new(
        market: &str,
        instrument: Selection,
        rows: Rows,
        from: Timestamp,
        to: Timestamp,
    ) -> Self {
        Self {
            market: market.to_owned(),
            instrument,
            rows,
            from,
            to,
            adjusted: 0,
            format: ExportFormat::Parquet,
        }
    }

    /// Returns the span the export selects, `[from, to)`.
    pub fn window(&self) -> (Timestamp, Timestamp) {
        (self.from, self.to)
    }

    /// Returns the longest span that one export is asked over.
    ///
    /// The span is:
    /// - One UTC hour for a book or for orders.
    /// - One UTC day for every other kind of rows.
    ///
    /// A span starts on a multiple of its length counted from 1970. LSE's docs ask for an hour on a
    /// busy contract ("The market: cme"). ESZ6's book for the minute from 14:30 on 5 November 2026
    /// held 99,104 rows, 33 MB as CSV. Its book for that whole day was 10.8 GB.
    pub fn longest_span(&self) -> SignedDuration {
        match self.rows {
            Rows::Book | Rows::Orders => SignedDuration::from_hours(1),
            _ => SignedDuration::from_hours(24),
        }
    }

    #[must_use]
    pub fn over(&self, from: Timestamp, to: Timestamp) -> Self {
        Self {
            from,
            to,
            ..self.clone()
        }
    }

    /// Returns the export split at the middle of its span.
    ///
    /// Returns none if the span is a second or less. No second of any series holds the 2,500,000
    /// rows that one export may read. ESZ6's busiest minute of book held 99,104 (LSE's docs, "The
    /// market: cme"). So halving below a second would not get past a refusal.
    ///
    /// Candles split between the two candles nearest the middle, because LSE folds a candle from
    /// the rows inside the export alone. One candle is not split.
    pub fn halves(&self) -> Option<(Self, Self)> {
        let span = self.to.duration_since(self.from);
        let middle = match self.rows {
            Rows::Candles(width) => width.floor(self.from + span / 2),
            _ if span <= SignedDuration::from_secs(1) => return None,
            _ => self.from + span / 2,
        };
        if middle <= self.from {
            return None;
        }
        Some((self.over(self.from, middle), self.over(middle, self.to)))
    }
}

impl Display for ExportQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            market,
            instrument,
            rows,
            from,
            to,
            ..
        } = self;
        write!(
            f,
            "the {rows} of {instrument} in LSE's {market} over [{from}, {to})"
        )
    }
}

/// The dates of a series' export. Both dates are included, and they bound its time column.
///
/// LSE refuses a time (`filter-bad`) on a collection keyed by dates. Either way, LSE selects a
/// day's rows by its date. With no first date, the series is asked from its first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Dates {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<Date>,
    pub to: Date,
}

impl Display for Dates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.from {
            Some(from) => write!(f, "over [{from}, {}]", self.to),
            None => write!(f, "up to {}", self.to),
        }
    }
}

/// An export of `symbol`'s rows in LSE's reference collection `dataset` over `dates`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReferenceQuery {
    dataset: String,
    symbol: String,
    #[serde(flatten)]
    dates: Dates,
    format: ExportFormat,
}

impl ReferenceQuery {
    /// Returns the rows of `symbol` in `dataset` over `dates`.
    pub fn new(dataset: &str, symbol: &str, dates: Dates) -> Self {
        Self {
            dataset: dataset.to_owned(),
            symbol: symbol.to_owned(),
            dates,
            format: ExportFormat::Parquet,
        }
    }
}

impl Display for ReferenceQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            dataset,
            symbol,
            dates,
            ..
        } = self;
        write!(
            f,
            "the rows of {symbol} in LSE's reference collection {dataset} {dates}"
        )
    }
}

/// An export of LSE's macro series `code` over `dates`. The `kind` is `release` or `yield`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MacroQuery {
    kind: &'static str,
    code: String,
    #[serde(flatten)]
    dates: Dates,
    format: ExportFormat,
}

impl MacroQuery {
    /// Returns the rows of the series `code` of `kind` over `dates`.
    pub fn new(kind: &'static str, code: &str, dates: Dates) -> Self {
        Self {
            kind,
            code: code.to_owned(),
            dates,
            format: ExportFormat::Parquet,
        }
    }
}

impl Display for MacroQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            kind, code, dates, ..
        } = self;
        write!(f, "LSE's macro series of {kind}s {code} {dates}")
    }
}

/// A call for an instrument's splits and dividends (`/v3/factors`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FactorsQuery {
    market: String,
    symbol: String,
}

impl FactorsQuery {
    /// Returns the factors of `symbol` in LSE's `market`.
    pub fn new(market: &str, symbol: &str) -> Self {
        Self {
            market: market.to_owned(),
            symbol: symbol.to_owned(),
        }
    }
}

impl Display for FactorsQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the splits and dividends of {} in LSE's {}",
            self.symbol, self.market
        )
    }
}

/// A lookup of the listings of one symbol in one market (`/v3/pairs`).
///
/// The symbol matches exactly. A symbol names a few listings, so they fit on one page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PairsQuery {
    market: String,
    symbol: String,
}

impl PairsQuery {
    /// Returns the listings of `symbol` in LSE's `market`.
    pub fn new(market: &str, symbol: &str) -> Self {
        Self {
            market: market.to_owned(),
            symbol: symbol.to_owned(),
        }
    }
}

impl Display for PairsQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the listings of {} in LSE's {}",
            self.symbol, self.market
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn gold(from: &str, to: &str) -> ExportQuery {
        let gold = LseSymbol::new("futures", "GOLD").unwrap();
        ExportQuery::series(
            &gold,
            Rows::TradeSlice,
            from.parse().unwrap(),
            to.parse().unwrap(),
        )
    }

    fn gold_candles(spec: &str, from: &str, to: &str) -> ExportQuery {
        let gold = LseSymbol::new("futures", "GOLD").unwrap();
        let bar_type = BarType::from(format!("GOLD-FUTURES.LSEDGE-{spec}-LAST-EXTERNAL").as_str());
        let width = CandleWidth::of(bar_type).unwrap();
        ExportQuery::series(
            &gold,
            Rows::Candles(width),
            from.parse().unwrap(),
            to.parse().unwrap(),
        )
    }

    /// Three 4-hour candles are cut after the first, not at 06:00, because LSE folds a candle from
    /// the rows inside the export alone.
    #[rstest]
    fn a_candle_export_halves_between_two_candles() {
        let (first, second) =
            gold_candles("4-HOUR", "2026-10-21T00:00:00Z", "2026-10-21T12:00:00Z")
                .halves()
                .unwrap();

        assert_eq!(
            first,
            gold_candles("4-HOUR", "2026-10-21T00:00:00Z", "2026-10-21T04:00:00Z")
        );
        assert_eq!(
            second,
            gold_candles("4-HOUR", "2026-10-21T04:00:00Z", "2026-10-21T12:00:00Z")
        );
    }

    #[rstest]
    #[case::four_hours("4-HOUR", "2026-10-21T04:00:00Z")]
    #[case::a_second("1-SECOND", "2026-10-21T00:00:01Z")]
    fn a_candle_export_of_one_candle_does_not_halve(#[case] spec: &str, #[case] to: &str) {
        assert_eq!(
            gold_candles(spec, "2026-10-21T00:00:00Z", to).halves(),
            None
        );
    }

    /// A day halves at noon. Three seconds halve at a second and a half.
    #[rstest]
    #[case::a_day("2026-10-21T00:00:00Z", "2026-10-21T12:00:00Z", "2026-10-22T00:00:00Z")]
    #[case::three_seconds(
        "2026-10-21T10:00:00Z",
        "2026-10-21T10:00:01.5Z",
        "2026-10-21T10:00:03Z"
    )]
    fn an_export_halves_at_its_middle(#[case] from: &str, #[case] mid: &str, #[case] to: &str) {
        let (first, second) = gold(from, to).halves().unwrap();

        assert_eq!(first, gold(from, mid));
        assert_eq!(second, gold(mid, to));
    }

    #[rstest]
    #[case::a_second("2026-10-21T10:00:01Z")]
    #[case::half_a_second("2026-10-21T10:00:00.5Z")]
    fn an_export_of_a_second_or_less_does_not_halve(#[case] to: &str) {
        assert_eq!(gold("2026-10-21T10:00:00Z", to).halves(), None);
    }
}
