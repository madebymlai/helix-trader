//! Market data: the traded instruments and their quotes in the Nautilus catalog.
//!
//! Build step 1. Backtest reads what this module writes. The instruments
//! come from the strategy's data requirements.

mod fill_catalog;
pub use fill_catalog::fill_catalog;
mod ib;
pub use ib::{IbAccount, IbHistory};
pub(crate) use ib::{IbGateway, data_client_config};
mod rolls;

use anyhow::Context;
use jiff::Timestamp;
use nautilus_core::UnixNanos;

/// `ts` as Nautilus' nanoseconds since the Unix epoch, the catalog's time.
///
/// # Errors
///
/// Returns an error if `ts` is before the epoch, where Nautilus'
/// `UnixNanos::from` would panic.
pub(crate) fn to_unix_nanos(ts: Timestamp) -> anyhow::Result<UnixNanos> {
    let ns = u64::try_from(ts.as_nanosecond())
        .with_context(|| format!("{ts} is outside the range of the catalog"))?;
    Ok(UnixNanos::from(ns))
}
