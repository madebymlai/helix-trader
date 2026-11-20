//! LSE's REST API.
//!
//! - `client`: the client.
//! - `query`: its typed queries.
//! - `export`: an export over any span, asked a UTC day and, past LSE's rows an export, a half at a
//!   time, with the span LSE has not loaded yet.
//! - `listing`: the listings of CME contracts in LSE's catalogue.
//! - `models` and `error`: LSE's answers and refusals.
//! - `parse`: the parsing of an export's Parquet file into Nautilus data.
//! - `rows`: the rows of LSE's sources that are not prices over any window.

pub mod client;
pub mod error;
pub mod export;
pub mod listing;
pub mod models;
pub mod parse;
pub mod query;
pub mod rows;
