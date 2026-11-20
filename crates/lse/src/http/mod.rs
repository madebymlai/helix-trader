//! The client for LSE's REST API.
//!
//! The submodules are:
//! - `client`: the HTTP client.
//! - `query`: the typed queries.
//! - `export`: exports over any span.
//! - `listing`: the listings of CME contracts.
//! - `models` and `error`: LSE's answers and refusals.
//! - `parse`: turns an export's Parquet file into Nautilus data.
//! - `rows`: the rows of LSE's sources that are not prices.

pub mod client;
pub mod error;
pub mod export;
pub mod listing;
pub mod models;
pub mod parse;
pub mod query;
pub mod rows;
