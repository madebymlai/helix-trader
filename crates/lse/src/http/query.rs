//! The queries of LSE's calls, typed: an export's (`/v3/export`), LSE's
//! selection of an instrument's rows, and a catalogue lookup's
//! (`/v3/pairs`), as LSE's docs name their parameters.

use std::fmt;

use jiff::{SignedDuration, Timestamp};
use serde::{Serialize, Serializer, ser::SerializeMap};

use crate::common::symbol::LseSymbol;

/// What an export's rows are, and the parameter that selects them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rows {
    /// The tick slice of a derived market, its trades, `time, price, size,
    /// kind` (`timeframe=tick`).
    TradeSlice,
    /// The trades of an exchange market, `time, price, size, kind`
    /// (`data=trades`).
    Trades,
    /// The quotes of an exchange market, `time, bid_price, ask_price,
    /// bid_size, ask_size` (`data=quotes`).
    Quotes,
}

impl Rows {
    /// The parameter selecting the rows, and its value.
    const fn parameter(self) -> (&'static str, &'static str) {
        match self {
            Self::TradeSlice => ("timeframe", "tick"),
            Self::Trades => ("data", "trades"),
            Self::Quotes => ("data", "quotes"),
        }
    }
}

/// The one parameter selecting the rows, flattened into the query.
impl Serialize for Rows {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (name, value) = self.parameter();
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(name, value)?;
        map.end()
    }
}

impl fmt::Display for Rows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TradeSlice => "trade slice",
            Self::Trades => "trades",
            Self::Quotes => "quotes",
        })
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
    /// would not get past a refusal.
    pub(crate) fn halves(&self) -> Option<(Self, Self)> {
        let span = self.to.duration_since(self.from);
        if span <= SignedDuration::from_secs(1) {
            return None;
        }
        let middle = self.from + span / 2;
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
