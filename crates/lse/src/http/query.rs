//! The query of an export (`/v3/export`), typed: LSE's selection of a
//! series, as LSE's docs name its parameters.

use std::fmt;

use jiff::Timestamp;
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
