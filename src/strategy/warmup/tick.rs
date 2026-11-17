//! What a warmup hands its indicators, ticks and bars alike, and the series
//! each belongs to: what the merge of its history (`merge`) and the hold of
//! its live ticks and bars (`hold`) both carry.

use std::fmt::Display;

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, DataType, NautilusDataType, OrderBookDeltas, OrderBookDepth,
        QuoteTick, TradeTick,
    },
    identifiers::InstrumentId,
};

use crate::strategy::requirements::TickSeries;

/// A quote, a trade, a snapshot of a book's depth, a batch of its deltas, a
/// point of custom data or a bar: all a warmup hands its indicators, named
/// for the most of them.
#[derive(Debug, Clone)]
pub(super) enum Tick {
    Quote(QuoteTick),
    Trade(TradeTick),
    /// Boxed: ten levels a side, against a quote's few fields.
    Depth(Box<OrderBookDepth>),
    Deltas(OrderBookDeltas),
    Custom(CustomData),
    Bar(Bar),
}

/// The series of a tick: the quotes, the trades, the depth or the deltas
/// of one instrument, the custom data of one data type, or the bars of one
/// standard bar type, as a node caches them (`BarType::standard`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum Series {
    Quotes(InstrumentId),
    Trades(InstrumentId),
    Depth(InstrumentId),
    Deltas(InstrumentId),
    Custom(DataType),
    Bars(BarType),
}

impl Series {
    /// The series of `tick`'s ticks.
    pub(super) fn of(tick: &TickSeries) -> Self {
        let id = tick.instrument_id;
        match tick.data {
            NautilusDataType::QuoteTick => Self::Quotes(id),
            NautilusDataType::TradeTick => Self::Trades(id),
            NautilusDataType::OrderBookDepth => Self::Depth(id),
            NautilusDataType::OrderBookDelta => Self::Deltas(id),
            ref data => unreachable!("{data} is not a tick series (see `TickSeries`)"),
        }
    }
}

impl Tick {
    pub(super) fn series(&self) -> Series {
        match self {
            Self::Quote(quote) => Series::Quotes(quote.instrument_id),
            Self::Trade(trade) => Series::Trades(trade.instrument_id),
            Self::Depth(depth) => Series::Depth(depth.instrument_id),
            Self::Deltas(deltas) => Series::Deltas(deltas.instrument_id),
            Self::Custom(data) => Series::Custom(data.data_type.clone()),
            Self::Bar(bar) => Series::Bars(bar.bar_type.standard()),
        }
    }

    /// When the tick was made, a batch of deltas when its last delta was, a
    /// bar when it closed.
    pub(super) fn ts_event(&self) -> UnixNanos {
        match self {
            Self::Quote(quote) => quote.ts_event,
            Self::Trade(trade) => trade.ts_event,
            Self::Depth(depth) => depth.ts_event,
            Self::Deltas(deltas) => deltas.ts_event,
            Self::Custom(data) => data.data.ts_event(),
            Self::Bar(bar) => bar.ts_event,
        }
    }
}

impl Display for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Quotes(id) => write!(f, "the quotes of {id}"),
            Self::Trades(id) => write!(f, "the trades of {id}"),
            Self::Depth(id) => write!(f, "the depth of {id}"),
            Self::Deltas(id) => write!(f, "the deltas of {id}"),
            Self::Custom(data_type) => write!(f, "the custom data {data_type}"),
            Self::Bars(bar_type) => write!(f, "the bars of {bar_type}"),
        }
    }
}
