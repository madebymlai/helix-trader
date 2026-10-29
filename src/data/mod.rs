//! Market data: the traded instruments, the series their strategy bars are
//! built from (the source bars of a composite bar type, or quotes) and the
//! funding of OKX swaps and X-Perps, in the Nautilus catalog.
//!
//! Build step 1. Backtest reads what this module writes. The instruments
//! come from the strategy's data requirements.

mod databento;
pub use databento::DatabentoHistory;
mod fill_catalog;
pub use fill_catalog::fill_catalog;
pub(crate) mod live_runs;
pub use live_runs::record_run_start;
mod ib;
pub use ib::{IbAccount, IbHistory};
pub(crate) use ib::{IbGateway, data_client_config};
pub(crate) mod ic;
mod node_fill;
pub use node_fill::{FillData, FillRequest, NodeClient, NodeFill, REQUEST_TIMEOUT, missing};
mod okx;
pub use ic::{IcConnection, IcHistory};
pub(crate) use okx::Contract as OkxContract;
pub use okx::OkxHistory;

use std::path::Path;

use anyhow::Context;

/// The path of the catalog directory `dir` as a node takes it: absolute,
/// since the node's DataFusion reader finds no object store for a relative
/// path ("file://catalog/...").
///
/// # Errors
///
/// Returns an error if the working directory is unknown or the path is not
/// UTF-8.
pub(crate) fn catalog_path(dir: &Path) -> anyhow::Result<String> {
    let absolute = std::path::absolute(dir)?;
    absolute
        .to_str()
        .map(str::to_owned)
        .with_context(|| format!("catalog path {} is not UTF-8", absolute.display()))
}
