//! IB's euro in US dollars on IDEALPRO as fill-catalog stores it from IB's
//! history: EUR/USD.IDEALPRO, Nautilus' FX pair, and its 1-minute BID and
//! ASK bars, each stamped at its close and of no volume, as IB's BID and
//! ASK bars carry none. On Wednesday 21 October 2026, the day of
//! `common::ic`. A strategy converting through the pair reads its rates
//! from these bars, the first form of a pair IB's history serves
//! (`venues::rate_series`).

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

/// IB's euro in US dollars.
pub const EURUSD: &str = "EUR/USD.IDEALPRO";

/// EUR/USD on IDEALPRO, IB's cash FX pair, on 5 decimals.
pub fn pair() -> InstrumentAny {
    let id = InstrumentId::from(EURUSD);
    InstrumentAny::CurrencyPair(default_fx_ccy(id.symbol, Some(id.venue)))
}

/// The pair's 1-minute bars priced at `price_type`, IB's own.
pub fn bar_type(price_type: PriceType) -> BarType {
    BarType::new(
        InstrumentId::from(EURUSD),
        BarSpecification::new(1, BarAggregation::Minute, price_type),
        AggregationSource::External,
    )
}

/// The pair's bars priced at `price_type`, flat at `price`, one a minute
/// closing from `from` + 1 minute to 12:00.
pub fn bars(price_type: PriceType, price: &str, from: Timestamp) -> Vec<Bar> {
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

/// Stores the pair in the catalog in `dir`, and its BID bars at `bid` and
/// ASK bars at `ask` from `from` to 12:00, stored over that window.
pub fn store(dir: &Path, bid: &str, ask: &str, from: Timestamp) {
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

/// Stores the pair's definition alone in the catalog in `dir`, none of its
/// bars.
pub fn store_definition(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![pair()]).unwrap();
}
