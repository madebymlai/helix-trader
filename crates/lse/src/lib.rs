//! A NautilusTrader adapter for London Strategic Edge's market data (LSE
//! Data 3.0): a data client, and no execution client, LSE trading nothing.
//! It names no symbol and decides nothing a key may have: it asks LSE, and
//! LSE's refusal is the answer, passed on with its code, message and
//! required plan. See `README.md`.
//!
//! Nautilus has no LSE adapter; this one is laid out as Nautilus' adapters
//! are (`docs/developer_guide/adapters.md`): `common` (the venue, the
//! sockets and the variables of the keys, the keys by the product each
//! opens, and the symbol boundary between an instrument id and what LSE is
//! asked for, a derived market's symbol, its name on a socket, a CME
//! contract's listing, a Nasdaq stock's ticker, or a US option's OSI symbol
//! and chain), `http` (the HTTP client, its typed queries, an
//! export over any span, the listings of CME contracts, LSE's answers and
//! refusals, the parsing of an export's Parquet file, and the rows of its
//! sources that are not prices over any window), `websocket` (the frames of
//! LSE's sockets, the client of one, and the parsing of a tick; and LSE's
//! exchange streams, CME Globex's and Nasdaq TotalView's, of one protocol,
//! its messages and its client),
//! `config` (the data client's config), `data` (the data client),
//! `factories`, and `types`, the custom data type of LSE's rows that are
//! not prices (`LseRow`).

pub use self::{
    common::consts::{LSE_API_KEY, LSE_CME_API_KEY, LSE_NASDAQ_API_KEY, LSEDGE_VENUE},
    config::LseDataClientConfig,
    data::check_streamed,
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
