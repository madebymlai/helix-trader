//! Market data: the history of the traded instruments in the Nautilus catalog.
//!
//! Build step 1. Backtest reads what this module writes. The instruments
//! and bar types come from the strategy config.

mod ingest;
mod rolls;
