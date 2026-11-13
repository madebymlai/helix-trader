//! The queries of LSE's exports, typed: of a market's series
//! (`/v3/export`) and of a reference collection's (`/v3/reference/export`),
//! as LSE's docs name their parameters.

use std::fmt;

use anyhow::{bail, ensure};
use jiff::{SignedDuration, Timestamp, civil::Date};
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
    /// would not get past a refusal. An export of candles is split between
    /// the two candles nearest its middle, as LSE folds a candle from the
    /// rows inside the export alone, and one of a candle is not split.
    pub(crate) fn halves(&self) -> Option<(Self, Self)> {
        let span = self.to.duration_since(self.from);
        let middle = match self.timeframe {
            Timeframe::Tick if span <= SignedDuration::from_secs(1) => return None,
            Timeframe::Tick => self.from + span / 2,
            Timeframe::Candles(width) => width.floor(self.from + span / 2),
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn gold(from: &str, to: &str) -> ExportQuery {
        let gold = LseSymbol::new("futures", "GOLD").unwrap();
        ExportQuery::trades(&gold, from.parse().unwrap(), to.parse().unwrap())
    }

    /// GOLD's candles of the bar specification `spec` over `[from, to)`.
    fn gold_candles(spec: &str, from: &str, to: &str) -> ExportQuery {
        let gold = LseSymbol::new("futures", "GOLD").unwrap();
        let bar_type = BarType::from(format!("GOLD-FUTURES.LSEDGE-{spec}-LAST-EXTERNAL").as_str());
        let width = CandleWidth::of(bar_type).unwrap();
        ExportQuery::candles(&gold, width, from.parse().unwrap(), to.parse().unwrap())
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
