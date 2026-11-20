//! Catalog filling, naming no venue (build step 1): writes the traded instruments, their
//! strategy bars' series and funding into the Nautilus catalog, through a node (`NodeFill`) or
//! directly (`fill_catalog`), from each instrument's venue history source (see `venues`). Custom
//! data of no instrument goes through the node, from the source its type names.

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

/// The config a node takes of the catalog in `dir`, by its path as given.
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

/// The instruments of `instrument_ids`, in their order, as the catalog at `catalog` defines them.
///
/// Opens it with `ParquetDataCatalog::from_uri`, which errors on a missing directory where `new`
/// panics.
///
/// # Errors
///
/// Returns an error if the catalog cannot be opened or read, or an instrument of
/// `instrument_ids` is not in it.
pub fn instruments(
    catalog: &Path,
    instrument_ids: &[InstrumentId],
) -> anyhow::Result<Vec<InstrumentAny>> {
    let ids: Vec<String> = instrument_ids.iter().map(ToString::to_string).collect();
    let defined = ParquetDataCatalog::from_uri(&catalog.to_string_lossy(), None, None, None, None)
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
