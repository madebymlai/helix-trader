//! LSE's REST API: the client (`client`), its typed queries (`query`), an
//! export over any span, asked a UTC day and, past LSE's rows an export, a
//! half at a time, with the span LSE has not loaded yet (`export`), the
//! listings of CME contracts in LSE's catalogue (`listing`), LSE's answers
//! and refusals (`models`, `error`), the parsing of an export's Parquet
//! file into Nautilus data (`parse`), and the rows of LSE's sources that are
//! not prices over any window (`rows`).

pub mod client;
pub mod error;
pub mod export;
pub mod listing;
pub mod models;
pub mod parse;
pub mod query;
pub mod rows;
