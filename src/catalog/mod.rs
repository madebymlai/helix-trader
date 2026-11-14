//! Catalog filling, naming no venue: the traded instruments, the series
//! their strategy bars are built from (the source bars of a composite bar
//! type, or quotes) and funding, written into the Nautilus catalog through
//! a node (`NodeFill`) or directly (`fill_catalog`), from the history
//! source of each instrument's venue (see `venues`), and custom data of no
//! instrument, from the source its type names, through the node.
//!
//! Build step 1. Backtest reads what this module writes. The instruments
//! come from the strategy's data requirements.

mod fill_catalog;
pub use fill_catalog::fill_catalog;
pub(crate) use fill_catalog::fill_slices;
mod node_fill;
pub use node_fill::{
    CustomFillRequest, FillData, FillRequest, NodeClient, NodeFill, missing, missing_custom,
};

use std::path::Path;

use anyhow::Context;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};

/// The config a node takes of the catalog in directory `dir`, by its path
/// as given: Nautilus resolves a relative one against the working
/// directory when it opens the catalog.
///
/// # Errors
///
/// Returns an error if the path is not UTF-8.
pub(crate) fn catalog_config(dir: &Path) -> anyhow::Result<DataCatalogConfig> {
    let path = dir
        .to_str()
        .with_context(|| format!("catalog path {} is not UTF-8", dir.display()))?;
    Ok(DataCatalogConfig::new(path.to_owned(), None, None))
}

/// The instruments of `traded`, those a strategy trades, as the catalog at
/// `catalog` defines them. The catalog is opened through Nautilus'
/// `ParquetDataCatalog::from_uri`, whose error a missing directory is,
/// where `ParquetDataCatalog::new` panics.
///
/// # Errors
///
/// Returns an error if the catalog cannot be opened or read, or an
/// instrument of `traded` is not in it.
pub fn traded_instruments(
    catalog: &Path,
    traded: &[InstrumentId],
) -> anyhow::Result<Vec<InstrumentAny>> {
    let ids: Vec<String> = traded.iter().map(ToString::to_string).collect();
    let defined = ParquetDataCatalog::from_uri(&catalog.to_string_lossy(), None, None, None, None)
        .with_context(|| format!("opening the catalog {}", catalog.display()))?
        .instruments(Some(&ids), None, None)
        .with_context(|| format!("reading {} from the catalog", ids.join(", ")))?;
    traded
        .iter()
        .map(|&id| {
            defined
                .iter()
                .find(|instrument| instrument.id() == id)
                .cloned()
                .with_context(|| {
                    format!(
                        "the traded instrument {id} is not in the catalog {}",
                        catalog.display()
                    )
                })
        })
        .collect()
}
