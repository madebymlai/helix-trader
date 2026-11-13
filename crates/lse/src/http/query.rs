//! The queries of LSE's exports, typed: of a market's series
//! (`/v3/export`) and of a reference collection's (`/v3/reference/export`),
//! as LSE's docs name their parameters.

use std::fmt;

use jiff::{Timestamp, civil::Date};
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

/// An export of the rows of `symbol` in LSE's reference collection
/// `dataset` over the dates `[from, to]`, both included, which bound the
/// collection's time column, a date or a time: LSE refuses a time
/// (`filter-bad`) of a collection keyed by dates, and selects a day's rows
/// by its date either way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReferenceQuery {
    dataset: String,
    symbol: String,
    from: Date,
    to: Date,
    format: ExportFormat,
}

impl ReferenceQuery {
    /// The rows of `symbol` in `dataset` over `[from, to]`, as Parquet.
    pub(crate) fn new(dataset: &str, symbol: &str, from: Date, to: Date) -> Self {
        Self {
            dataset: dataset.to_owned(),
            symbol: symbol.to_owned(),
            from,
            to,
            format: ExportFormat::Parquet,
        }
    }
}

impl fmt::Display for ReferenceQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            dataset,
            symbol,
            from,
            to,
            ..
        } = self;
        write!(
            f,
            "the rows of {symbol} in LSE's reference collection {dataset} over [{from}, {to}]"
        )
    }
}
