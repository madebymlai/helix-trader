//! The query of an export (`/v3/export`), typed: LSE's selection of a
//! series, as LSE's docs name its parameters.

use std::fmt;

use anyhow::{bail, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::{
    data::{BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
};
use serde::{Serialize, Serializer};

use crate::common::symbol::LseSymbol;

/// The candle widths LSE serves below a day, by unit: its letter, and the
/// steps of it LSE serves (LSE's docs, "The selection", `timeframe`).
const CANDLE_WIDTHS: [(BarAggregation, char, &[usize]); 3] = [
    (BarAggregation::Second, 's', &[1, 5, 15, 30]),
    (BarAggregation::Minute, 'm', &[1, 3, 5, 15, 30]),
    (BarAggregation::Hour, 'h', &[1, 4]),
];

/// The width of an export's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Timeframe {
    /// The tick slice of a derived market: its trades, `time, price, size,
    /// kind`.
    Tick,
    /// Candles, `time, open, high, low, close, volume, trades`, each
    /// stamped at its open.
    Candles(CandleWidth),
}

impl Serialize for Timeframe {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Tick => serializer.serialize_str("tick"),
            Self::Candles(width) => serializer.collect_str(width),
        }
    }
}

/// A width of LSE's candles, as the bar specification of Nautilus' it is:
/// LSE's `1s` is `1-SECOND-LAST`, its `4h` `4-HOUR-LAST`, below a day, the
/// widths whose candles LSE stamps on a grid from 1970. LSE's daily, weekly
/// and monthly candles follow exchange sessions instead, each stamped at
/// its first traded minute and folded from the sessions inside the dates
/// asked, so none is one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CandleWidth(BarSpecification);

impl CandleWidth {
    /// The candles of `bar_type`: external bars, of the last price, LSE's
    /// one price a candle, a width LSE serves below a day.
    ///
    /// # Errors
    ///
    /// Returns an error naming `bar_type` if its bars are not those of
    /// LSE's candles.
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
                "{bar_type}: LSE's daily, weekly and monthly candles follow exchange sessions, \
                 each stamped at its first traded minute, so their close is not known; \
                 LSE's candles are served from 1-SECOND to 4-HOUR"
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

    /// How long a candle lasts, from its open to its close.
    pub(crate) fn duration(self) -> SignedDuration {
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
        Self::of(series, Timeframe::Tick, from, to)
    }

    /// The candles `width` wide of `series` opening over `[from, to)`, as
    /// Parquet. LSE folds a candle from the rows inside the export alone.
    pub(crate) fn candles(
        series: &LseSymbol,
        width: CandleWidth,
        from: Timestamp,
        to: Timestamp,
    ) -> Self {
        Self::of(series, Timeframe::Candles(width), from, to)
    }

    fn of(series: &LseSymbol, timeframe: Timeframe, from: Timestamp, to: Timestamp) -> Self {
        Self {
            market: series.market().to_owned(),
            symbol: series.symbol().to_owned(),
            timeframe,
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
            Timeframe::Tick => "trade slice".to_string(),
            Timeframe::Candles(width) => format!("{width} candles"),
        };
        write!(
            f,
            "the {rows} of {symbol} in LSE's {market} over [{from}, {to})"
        )
    }
}
