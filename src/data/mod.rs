//! Market data: the traded instruments and their quotes in the Nautilus catalog.
//!
//! Build step 1. Backtest reads what this module writes. The instruments
//! come from the strategy's data requirements.

mod fill_catalog;
pub use fill_catalog::fill_catalog;
pub(crate) use fill_catalog::to_unix_nanos;
mod ib;
pub use ib::{IbGateway, IbHistory};
mod rolls;
