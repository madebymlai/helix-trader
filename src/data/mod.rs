//! Market data: the history of the traded instruments in the Nautilus catalog.
//!
//! Build step 1. Backtest reads what this module writes. The instruments
//! and bar types come from the strategy config.

mod fill_catalog;
pub use fill_catalog::fill_catalog;
pub(crate) use fill_catalog::to_unix_nanos;
mod ib;
pub use ib::{IbGateway, IbHistory};
mod rolls;
