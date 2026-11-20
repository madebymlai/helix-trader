//! What a warmup hands its indicators, and the series each belongs to, as `merge` and `hold` carry
//! them.

use std::fmt::Display;

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, DataType, NautilusDataType, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth, QuoteTick, TradeTick,
    },
    identifiers::InstrumentId,
};

use crate::strategy::requirements::TickSeries;

/// A quote, trade, depth snapshot, batch of deltas, custom data point or bar.
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

/// The series of a tick as a node caches it, bars by their standard bar type (`BarType::standard`).
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

    /// The bytes the tick takes held, a custom data point's heap counted as the length of its JSON
    /// (`CustomDataTrait::to_json`).
    ///
    /// # Errors
    ///
    /// Returns an error if a point of custom data cannot be written as JSON, naming its data type.
    pub(super) fn size(&self) -> anyhow::Result<usize> {
        let heap = match self {
            Self::Quote(_) | Self::Trade(_) | Self::Bar(_) => 0,
            Self::Depth(depth) => size_of_val(&**depth),
            Self::Deltas(deltas) => deltas.deltas.capacity() * size_of::<OrderBookDelta>(),
            Self::Custom(data) => {
                let json = data.data.to_json().with_context(|| {
                    format!("the warmup cannot size a point of {}", data.data_type)
                })?;
                let data_type = &data.data_type;
                size_of_val(&*data.data)
                    + json.len()
                    + data_type.type_name().len()
                    + data_type.topic().len()
                    + data_type.identifier().map_or(0, str::len)
                    + data_type.metadata_str().len()
            }
        };
        Ok(size_of::<Self>() + heap)
    }

    pub(super) fn ts_init(&self) -> UnixNanos {
        match self {
            Self::Quote(quote) => quote.ts_init,
            Self::Trade(trade) => trade.ts_init,
            Self::Depth(depth) => depth.ts_init,
            Self::Deltas(deltas) => deltas.ts_init,
            Self::Custom(data) => data.data.ts_init(),
            Self::Bar(bar) => bar.ts_init,
        }
    }

    /// When the tick was made: a batch's last delta's, a bar's close.
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
