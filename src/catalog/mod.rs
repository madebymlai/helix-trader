//! Fills the Nautilus catalog and names no venue (build step 1).
//!
//! It writes the traded instruments, the series of their strategy bars and their funding. Each
//! instrument's venue history source supplies the data (see `venues`). The fill runs through a
//! node (`NodeFill`) or directly (`fill_catalog`). Custom data that belongs to no instrument goes
//! through the node, from the source that its type names.

mod fill_catalog;
pub use fill_catalog::fill_catalog;
pub(crate) use fill_catalog::{fill_slices, resumed};
mod node_fill;
use std::path::Path;

use anyhow::Context;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
pub use node_fill::{
    CustomFillRequest, FillData, FillRequest, NodeClient, NodeFill, missing, missing_custom,
};

/// Returns the config a node takes for the catalog in `dir`, using the path as given.
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

/// Returns the instruments of `instrument_ids`, in order, as the catalog at `catalog` defines them.
///
/// Opens the catalog with `ParquetDataCatalog::builder`. It errors on a missing directory or a path
/// not in UTF-8, where `new` panics.
///
/// # Errors
///
/// Returns an error if:
/// - The catalog cannot be opened or read.
/// - An instrument in `instrument_ids` is not in the catalog.
pub fn instruments(
    catalog: &Path,
    instrument_ids: &[InstrumentId],
) -> anyhow::Result<Vec<InstrumentAny>> {
    let ids: Vec<String> = instrument_ids.iter().map(ToString::to_string).collect();
    let defined = ParquetDataCatalog::builder()
        .uri(catalog)
        .build()
        .with_context(|| format!("opening the catalog {}", catalog.display()))?
        .instruments(Some(&ids), None, None)
        .with_context(|| format!("reading {} from the catalog", ids.join(", ")))?;
    instrument_ids
        .iter()
        .map(|&id| {
            defined
                .iter()
                .find(|instrument| instrument.id() == id)
                .cloned()
                .with_context(|| format!("{id} is not in the catalog {}", catalog.display()))
        })
        .collect()
}
