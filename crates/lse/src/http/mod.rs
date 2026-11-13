//! LSE's REST API: the client (`client`), its typed export query
//! (`query`), LSE's refusals (`models`, `error`) and the parsing of an
//! export's Parquet file into Nautilus data (`parse`).

pub(crate) mod client;
pub(crate) mod error;
pub(crate) mod models;
pub(crate) mod parse;
pub(crate) mod query;
