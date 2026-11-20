//! EUR/USD.IDEALPRO, Nautilus' FX pair, and its 1-minute BID and ASK bars as fill-catalog stores
//! them from IB's history. The date is 21 October 2026, the day of `common::ic`.
//!
//! Each bar is stamped at its close and has no volume, as IB's BID and ASK bars carry none. A
//! strategy that converts through the pair reads its rates from these bars. They are the first
//! form of a pair that IB's history serves (`venues::rate_series`).

use std::path::Path;

use jiff::{SignedDuration, Timestamp};
use nautilus_model::{
    data::{Bar, BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::{InstrumentAny, stubs::default_fx_ccy},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use super::ic::at;

pub(crate) const EURUSD: &str = "EUR/USD.IDEALPRO";

pub(crate) fn pair() -> InstrumentAny {
    let id = InstrumentId::from(EURUSD);
    InstrumentAny::CurrencyPair(default_fx_ccy(id.symbol, Some(id.venue)))
}

pub(crate) fn bar_type(price_type: PriceType) -> BarType {
    BarType::new(
        InstrumentId::from(EURUSD),
        BarSpecification::new(1, BarAggregation::Minute, price_type),
        AggregationSource::External,
    )
}

/// Returns flat bars at `price`, one a minute, closing from `from` + 1 minute to 12:00.
pub(crate) fn bars(price_type: PriceType, price: &str, from: Timestamp) -> Vec<Bar> {
    let price = Price::from(price);
    let mut bars = vec![];
    let mut close = from + SignedDuration::from_mins(1);
    while close <= at("12:00:00") {
        bars.push(Bar::new(
            bar_type(price_type),
            price,
            price,
            price,
            price,
            Quantity::from(0),
            close.into(),
            close.into(),
        ));
        close += SignedDuration::from_mins(1);
    }
    bars
}

/// Stores the pair, its BID bars at `bid` and its ASK bars at `ask`, from `from` to 12:00.
pub(crate) fn store(dir: &Path, bid: &str, ask: &str, from: Timestamp) {
    store_definition(dir);
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    for (price_type, price) in [(PriceType::Bid, bid), (PriceType::Ask, ask)] {
        catalog
            .write_to_parquet(
                &bars(price_type, price, from),
                Some(from.into()),
                Some(at("12:00:00").into()),
                None,
            )
            .unwrap();
    }
}

pub(crate) fn store_definition(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![pair()]).unwrap();
}
