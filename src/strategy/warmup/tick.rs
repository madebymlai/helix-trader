//! What a warmup hands its features besides bars, and the series each
//! belongs to: what the merge of its history (`merge`) and the hold of its
//! live ticks (`hold`) both carry.

use std::fmt::Display;

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        CustomData, DataType, NautilusDataType, OrderBookDeltas, OrderBookDepth, QuoteTick,
        TradeTick,
    },
    identifiers::InstrumentId,
};

use crate::strategy::requirements::TickSeries;

/// A quote, a trade, a snapshot of a book's depth, a batch of its deltas or
/// a point of custom data.
#[derive(Debug, Clone)]
pub(super) enum Tick {
    Quote(QuoteTick),
    Trade(TradeTick),
    /// Boxed: ten levels a side, against a quote's few fields.
    Depth(Box<OrderBookDepth>),
    Deltas(OrderBookDeltas),
    Custom(CustomData),
}

/// The series of a tick: the quotes, the trades, the depth or the deltas
/// of one instrument, or the custom data of one data type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum Series {
    Quotes(InstrumentId),
    Trades(InstrumentId),
    Depth(InstrumentId),
    Deltas(InstrumentId),
    Custom(DataType),
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
        }
    }

    /// When the tick was made, a batch of deltas when its last delta was.
    pub(super) fn ts_event(&self) -> UnixNanos {
        match self {
            Self::Quote(quote) => quote.ts_event,
            Self::Trade(trade) => trade.ts_event,
            Self::Depth(depth) => depth.ts_event,
            Self::Deltas(deltas) => deltas.ts_event,
            Self::Custom(data) => data.data.ts_event(),
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
        }
    }
}
