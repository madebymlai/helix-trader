//! A NautilusTrader adapter for London Strategic Edge's market data (LSE
//! Data 3.0): a data client, and no execution client, LSE trading nothing.
//! It names no symbol and decides nothing a key may have: it asks LSE, and
//! LSE's refusal is the answer, passed on with its code, message and
//! required plan. See `README.md`.
//!
//! Nautilus has no LSE adapter; this one is laid out as Nautilus' adapters
//! are (`docs/developer_guide/adapters.md`): `common` (the venue and the
//! variables of the keys, the keys by the product each opens, and the
//! symbol boundary between an instrument id and what LSE is asked for, a
//! derived market's symbol or a CME contract's listing), `http` (the HTTP
//! client, its typed queries, an export over any span, the listings of CME
//! contracts, LSE's answers and refusals and the parsing of an export's
//! Parquet file), `config` (the data client's config), `data` (the data
//! client) and `factories`.

pub use self::{
    common::{
        consts::{LSE_API_KEY, LSE_CME_API_KEY, LSEDGE_VENUE},
        symbol::LseSymbol,
    },
    config::LseDataClientConfig,
    factories::LseDataClientFactory,
};

mod common;
mod config;
mod data;
mod factories;
mod http;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
