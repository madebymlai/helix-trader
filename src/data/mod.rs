//! Market data: the traded instruments, the series their strategy bars are
//! built from (quotes, or the source bars of a composite bar type) and the
//! funding of OKX swaps and X-Perps, in the Nautilus catalog.
//!
//! Build step 1. Backtest reads what this module writes. The instruments
//! come from the strategy's data requirements.

mod fill_catalog;
pub use fill_catalog::fill_catalog;
mod ib;
pub use ib::{IbAccount, IbHistory};
pub(crate) use ib::{IbGateway, data_client_config};
mod okx;
pub(crate) use okx::Contract as OkxContract;
pub use okx::OkxHistory;
mod rolls;
