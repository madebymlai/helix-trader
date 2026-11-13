//! A NautilusTrader adapter for London Strategic Edge's market data (LSE
//! Data 3.0): a data client, and no execution client, LSE trading nothing.
//! It names no symbol and decides nothing a key may have: it asks LSE, and
//! LSE's refusal is the answer, passed on with its code, message and
//! required plan. See `README.md`.
//!
//! Nautilus has no LSE adapter; this one is laid out as Nautilus' adapters
//! are (`docs/developer_guide/adapters.md`): `common` (the venue, the
//! sockets and the variable of the key, the key, and the symbol boundary
//! between LSE's market and symbol and an instrument id, and its name on
//! a socket), `http` (the HTTP client, its typed export query, an export
//! over any span, LSE's refusals and the parsing of an export's Parquet
//! file), `websocket` (the frames of LSE's sockets, the client of one, and
//! the parsing of a tick), `config` (the data client's config), `data`
//! (the data client), `factories`, and `types`, the custom data type of
//! LSE's rows that are not prices (`LseRow`).

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
mod websocket;
