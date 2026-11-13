//! A NautilusTrader adapter for London Strategic Edge's market data (LSE
//! Data 3.0): a data client, and no execution client, LSE trading nothing.
//! It names no symbol and decides nothing a key may have: it asks LSE, and
//! LSE's refusal is the answer, passed on with its code, message and
//! required plan. See `README.md`.
//!
//! Nautilus has no LSE adapter; this one is laid out as Nautilus' adapters
//! are (`docs/developer_guide/adapters.md`): `common` (the venue and the
//! variable of the key, the key, and the symbol boundary between LSE's
//! market and symbol and an instrument id), `http` (the HTTP client, its
//! typed export query, LSE's refusals and the parsing of an export's
//! Parquet file), `config` (the data client's config), `data` (the data
//! client), `factories`, and `types`, the custom data type of LSE's rows
//! that are not prices (`LseRow`).

pub use self::{
    common::{
        consts::{LSE_API_KEY, LSEDGE_VENUE},
        symbol::LseSymbol,
    },
    config::LseDataClientConfig,
    factories::LseDataClientFactory,
    types::LseRow,
};

mod common;
mod config;
mod data;
mod factories;
mod http;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
mod types;
