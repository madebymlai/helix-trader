//! The query of an export (`/v3/export`), typed: LSE's selection of a
//! series, as LSE's docs name its parameters.

use std::fmt;

use jiff::{SignedDuration, Timestamp};
use serde::Serialize;

use crate::common::symbol::LseSymbol;

/// The width of an export's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Timeframe {
    /// The tick slice of a derived market: its trades, `time, price, size,
    /// kind`.
    Tick,
}

/// The file format of an export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ExportFormat {
    Parquet,
}

/// An export of a series over `[from, to)`, two UTC timestamps, the second
/// excluded, at prices as published: LSE adjusts them for splits unless
/// told not to (`adjusted=0`), and the adapter adjusts nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ExportQuery {
    market: String,
    symbol: String,
    timeframe: Timeframe,
    from: Timestamp,
    to: Timestamp,
    adjusted: u8,
    format: ExportFormat,
}

impl ExportQuery {
    /// The trade slice of `series` over `[from, to)`, as Parquet.
    pub(crate) fn trades(series: &LseSymbol, from: Timestamp, to: Timestamp) -> Self {
        Self {
            market: series.market().to_owned(),
            symbol: series.symbol().to_owned(),
            timeframe: Timeframe::Tick,
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
            symbol,
            timeframe,
            from,
            to,
            ..
        } = self;
        let rows = match timeframe {
            Timeframe::Tick => "trade slice",
        };
        write!(
            f,
            "the {rows} of {symbol} in LSE's {market} over [{from}, {to})"
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn gold(from: &str, to: &str) -> ExportQuery {
        let gold = LseSymbol::new("futures", "GOLD").unwrap();
        ExportQuery::trades(&gold, from.parse().unwrap(), to.parse().unwrap())
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
