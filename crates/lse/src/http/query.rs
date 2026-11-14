//! The queries of LSE's calls, typed: the exports of a market's
//! instrument (`/v3/export`), LSE's selection of its rows, of a reference
//! collection's (`/v3/reference/export`) and of a macro series'
//! (`/v3/macro/export`), an instrument's splits and dividends
//! (`/v3/factors`), and a lookup of LSE's catalogue (`/v3/pairs`), as LSE's
//! docs name their parameters.

use std::fmt;

use anyhow::{bail, ensure};
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

/// The candle widths LSE serves below a day, by unit: its letter, and the
/// steps of it LSE serves (LSE's docs, "The selection", `timeframe`).
const CANDLE_WIDTHS: [(BarAggregation, char, &[usize]); 3] = [
    (BarAggregation::Second, 's', &[1, 5, 15, 30]),
    (BarAggregation::Minute, 'm', &[1, 3, 5, 15, 30]),
    (BarAggregation::Hour, 'h', &[1, 4]),
];

/// What an export's rows are, and the parameter that selects them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rows {
    /// The tick slice of a derived market or of US options, `time, price,
    /// size, kind` (`timeframe=tick`): its trades, or an index's values, of
    /// no size.
    TradeSlice,
    /// Candles, `time, open, high, low, close, volume, trades`, each
    /// stamped at its open (`timeframe=5m`, LSE's name of the width).
    Candles(CandleWidth),
    /// The trades of a paid exchange market, CME Globex or Nasdaq
    /// TotalView, `time, price, size, kind` (`data=trades`), which US
    /// options, an included market, serve as their tick slice alone.
    Trades,
    /// The quotes of an exchange market, `time, bid_price, ask_price,
    /// bid_size, ask_size` (`data=quotes`).
    Quotes,
    /// The Level 2 book of an exchange market, `time, sequence, action,
    /// side, price, size, depth` and ten bid and ten ask levels of price,
    /// size and orders (`data=book`).
    Book,
    /// The Level 3 orders of an exchange market, `time, action, side,
    /// order_id, price, size` (`data=orders`).
    Orders,
}

/// The one parameter selecting the rows, flattened into the query.
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

impl fmt::Display for Rows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// The instrument an export selects: a symbol, as `/v3/pairs` lists it, or
/// the number of a listing, which always names one instrument (LSE's docs,
/// "Symbols").
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Selection {
    Symbol(String),
    PairId(u64),
}

impl fmt::Display for Selection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Symbol(symbol) => f.write_str(symbol),
            Self::PairId(pair_id) => write!(f, "pair {pair_id}"),
        }
    }
}

/// A width of LSE's candles, as the bar specification of Nautilus' it is:
/// LSE's `1s` is `1-SECOND-LAST`, its `4h` `4-HOUR-LAST`, below a day, the
/// widths whose candles LSE stamps on a grid from 1970. LSE's daily, weekly
/// and monthly candles are none: LSE stamps each at its session's first
/// traded minute and gives no close, and builds them from its minute bars,
/// so a bar of those widths is declared from a shorter LSE candle, which
/// Nautilus aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CandleWidth(BarSpecification);

impl CandleWidth {
    /// The candles of `bar_type`: external bars, of the last price, LSE's
    /// one price a candle, a width LSE serves below a day.
    ///
    /// # Errors
    ///
    /// Returns an error naming `bar_type` if its bars are not those of
    /// LSE's candles; for daily, weekly and monthly bars, saying why and
    /// that the bar is declared from a shorter LSE candle instead.
    pub(crate) fn of(bar_type: BarType) -> anyhow::Result<Self> {
        let spec = bar_type.spec();
        ensure!(
            bar_type.aggregation_source() == AggregationSource::External,
            "{bar_type}: LSE's candles are EXTERNAL bars"
        );
        ensure!(
            spec.price_type == PriceType::Last,
            "{bar_type}: LSE's candles are of the LAST price, their one price"
        );
        if matches!(
            spec.aggregation,
            BarAggregation::Day | BarAggregation::Week | BarAggregation::Month
        ) {
            bail!(
                "{bar_type}: LSE stamps its daily, weekly and monthly candles at their \
                 session's first traded minute and gives no close, and builds them from its \
                 minute bars; declare the bar from a shorter LSE candle (1-MINUTE or 4-HOUR \
                 EXTERNAL) instead, which Nautilus aggregates"
            );
        }
        ensure!(
            CANDLE_WIDTHS.iter().any(|(aggregation, _, steps)| {
                *aggregation == spec.aggregation && steps.contains(&spec.step.get())
            }),
            "{bar_type}: LSE serves no candles of that width, only 1, 5, 15 and 30 seconds, \
             1, 3, 5, 15 and 30 minutes, and 1 and 4 hours wide"
        );
        Ok(Self(spec))
    }

    /// The span of the whole candles that close over `[start, end]`,
    /// closed, as LSE takes a span, `[from, to)`: from the first one's open
    /// to the last one's close; none if no candle closes in it. A candle
    /// opens on a multiple of its width from 1970, which divides a day, as
    /// LSE stamps them.
    pub(crate) fn candles_closing(
        self,
        start: Timestamp,
        end: Timestamp,
    ) -> Option<(Timestamp, Timestamp)> {
        let width = self.duration();
        let first = self.floor(start - SignedDuration::from_nanos(1));
        let last = self.floor(end - width);
        (first <= last).then(|| (first, last + width))
    }

    /// The open of the candle `at` falls in.
    fn floor(self, at: Timestamp) -> Timestamp {
        let (nanos, width) = (at.as_nanosecond(), self.duration().as_nanos());
        Timestamp::from_nanosecond(nanos - nanos.rem_euclid(width))
            .expect("the open of a candle at or before a timestamp is one")
    }

    /// How long a candle lasts, from its open to its close.
    fn duration(self) -> SignedDuration {
        self.0.timedelta()
    }
}

/// LSE's name of the width, such as `5m`.
impl fmt::Display for CandleWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
pub(crate) enum ExportFormat {
    Parquet,
}

/// An export of an instrument's rows over `[from, to)`, two UTC timestamps,
/// the second excluded, at prices as published: LSE adjusts them for splits
/// unless told not to (`adjusted=0`), and the adapter adjusts nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExportQuery {
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
    /// The `rows` of `series`, a derived market's, over `[from, to)`, as
    /// Parquet.
    pub(crate) fn series(series: &LseSymbol, rows: Rows, from: Timestamp, to: Timestamp) -> Self {
        Self::new(
            series.market(),
            Selection::Symbol(series.symbol().to_owned()),
            rows,
            from,
            to,
        )
    }

    /// The `rows` of the US option `contract` over `[from, to)`, as
    /// Parquet: of LSE's `us_options`, by its OSI symbol, which names one
    /// contract.
    pub(crate) fn option(
        contract: &OpraContract,
        rows: Rows,
        from: Timestamp,
        to: Timestamp,
    ) -> Self {
        Self::new(
            US_OPTIONS_MARKET,
            Selection::Symbol(contract.osi().to_owned()),
            rows,
            from,
            to,
        )
    }

    /// The `rows` of the Nasdaq TotalView `stock` over `[from, to)`, as
    /// Parquet: of LSE's `nasdaq`, by its ticker.
    pub(crate) fn stock(stock: &NasdaqStock, rows: Rows, from: Timestamp, to: Timestamp) -> Self {
        Self::new(
            NASDAQ_MARKET,
            Selection::Symbol(stock.ticker().to_owned()),
            rows,
            from,
            to,
        )
    }

    /// The `rows` of LSE's listing `pair_id` in `market` over `[from,
    /// to)`, as Parquet.
    pub(crate) fn listing(
        market: &str,
        pair_id: u64,
        rows: Rows,
        from: Timestamp,
        to: Timestamp,
    ) -> Self {
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

    /// The span the export selects, `[from, to)`.
    pub(crate) fn window(&self) -> (Timestamp, Timestamp) {
        (self.from, self.to)
    }

    /// The longest span one export of the query's rows is asked over, from
    /// a multiple of it from 1970: a UTC hour of a book or of orders, as
    /// LSE's docs ask of a busy contract ("The market: cme": ESZ6's book of
    /// the minute from 14:30 on 5 November 2026 held 99,104 rows, 33 MB as
    /// CSV, and of that whole day 10.8 GB), a UTC day of every other.
    pub(crate) fn longest_span(&self) -> SignedDuration {
        match self.rows {
            Rows::Book | Rows::Orders => SignedDuration::from_hours(1),
            _ => SignedDuration::from_hours(24),
        }
    }

    /// The export's selection over `[from, to)`.
    pub(crate) fn over(&self, from: Timestamp, to: Timestamp) -> Self {
        Self {
            from,
            to,
            ..self.clone()
        }
    }

    /// The export split at the middle of its span, or none if it spans a
    /// second or less: no second of any series LSE serves holds the
    /// 2,500,000 rows one export may read (ESZ6's busiest minute of book
    /// held 99,104, LSE's docs, "The market: cme"), so halving one further
    /// would not get past a refusal. An export of candles is split between
    /// the two candles nearest its middle, as LSE folds a candle from the
    /// rows inside the export alone, and one of a candle is not split.
    pub(crate) fn halves(&self) -> Option<(Self, Self)> {
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

impl fmt::Display for ExportQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// The dates of a series' export, both included, which bound its time
/// column, a date or a time: LSE refuses a time (`filter-bad`) of a
/// collection keyed by dates, and selects a day's rows by its date either
/// way. With no first date, the series is asked from its first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct Dates {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<Date>,
    pub(crate) to: Date,
}

impl fmt::Display for Dates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.from {
            Some(from) => write!(f, "over [{from}, {}]", self.to),
            None => write!(f, "up to {}", self.to),
        }
    }
}

/// An export of the rows of `symbol` in LSE's reference collection
/// `dataset` over its `dates`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReferenceQuery {
    dataset: String,
    symbol: String,
    #[serde(flatten)]
    dates: Dates,
    format: ExportFormat,
}

impl ReferenceQuery {
    /// The rows of `symbol` in `dataset` over `dates`, as Parquet.
    pub(crate) fn new(dataset: &str, symbol: &str, dates: Dates) -> Self {
        Self {
            dataset: dataset.to_owned(),
            symbol: symbol.to_owned(),
            dates,
            format: ExportFormat::Parquet,
        }
    }
}

impl fmt::Display for ReferenceQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// An export of LSE's macro series `code` of the kind `kind` (`release`,
/// `yield`) over its `dates` (`/v3/macro/export`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MacroQuery {
    kind: &'static str,
    code: String,
    #[serde(flatten)]
    dates: Dates,
    format: ExportFormat,
}

impl MacroQuery {
    /// The rows of the series `code` of `kind` over `dates`, as Parquet.
    pub(crate) fn new(kind: &'static str, code: &str, dates: Dates) -> Self {
        Self {
            kind,
            code: code.to_owned(),
            dates,
            format: ExportFormat::Parquet,
        }
    }
}

impl fmt::Display for MacroQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            kind, code, dates, ..
        } = self;
        write!(f, "LSE's macro series of {kind}s {code} {dates}")
    }
}

/// A call of an instrument's splits and dividends (`/v3/factors`), by its
/// market and its symbol in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FactorsQuery {
    market: String,
    symbol: String,
}

impl FactorsQuery {
    /// The factors of `symbol` in LSE's `market`.
    pub(crate) fn new(market: &str, symbol: &str) -> Self {
        Self {
            market: market.to_owned(),
            symbol: symbol.to_owned(),
        }
    }
}

impl fmt::Display for FactorsQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the splits and dividends of {} in LSE's {}",
            self.symbol, self.market
        )
    }
}

/// A lookup of LSE's catalogue (`/v3/pairs`): the listings of one symbol,
/// exactly, in one market. A symbol names a few listings, one page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PairsQuery {
    market: String,
    symbol: String,
}

impl PairsQuery {
    /// The listings of `symbol` in LSE's `market`.
    pub(crate) fn new(market: &str, symbol: &str) -> Self {
        Self {
            market: market.to_owned(),
            symbol: symbol.to_owned(),
        }
    }
}

impl fmt::Display for PairsQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

    /// GOLD's candles of the bar specification `spec` over `[from, to)`.
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

    /// Three 4-hour candles, cut after the first rather than at 06:00, the
    /// middle: LSE folds a candle from the rows inside the export alone.
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

    #[rstest]
    fn an_export_halves_at_its_middle() {
        let (first, second) = gold("2026-10-21T00:00:00Z", "2026-10-22T00:00:00Z")
            .halves()
            .unwrap();

        assert_eq!(first, gold("2026-10-21T00:00:00Z", "2026-10-21T12:00:00Z"));
        assert_eq!(second, gold("2026-10-21T12:00:00Z", "2026-10-22T00:00:00Z"));
    }

    #[rstest]
    fn an_export_of_three_seconds_halves_at_a_second_and_a_half() {
        let (first, second) = gold("2026-10-21T10:00:00Z", "2026-10-21T10:00:03Z")
            .halves()
            .unwrap();

        assert_eq!(
            first,
            gold("2026-10-21T10:00:00Z", "2026-10-21T10:00:01.5Z")
        );
        assert_eq!(
            second,
            gold("2026-10-21T10:00:01.5Z", "2026-10-21T10:00:03Z")
        );
    }

    #[rstest]
    #[case::a_second("2026-10-21T10:00:01Z")]
    #[case::half_a_second("2026-10-21T10:00:00.5Z")]
    fn an_export_of_a_second_or_less_does_not_halve(#[case] to: &str) {
        assert_eq!(gold("2026-10-21T10:00:00Z", to).halves(), None);
    }
}
