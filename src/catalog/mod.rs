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
use nautilus_persistence::config::DataCatalogConfig;

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
