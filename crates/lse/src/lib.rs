//! A NautilusTrader adapter for London Strategic Edge's market data (LSE Data 3.0).
//!
//! It has a data client and no execution client, because LSE trades nothing.
//!
//! The adapter names no symbol and decides nothing about what a key may have. It asks LSE. If LSE
//! refuses, the refusal is the answer, passed on with its code, message and required plan. See
//! `README.md`.
//!
//! `common`, `http` and `websocket` are public, as in Nautilus' adapters
//! (`docs/developer_guide/adapters.md`). The tests in `tests/` can then drive the clients'
//! boundaries as a caller does. `testing` holds the mock LSE peers and their captured payloads. It
//! comes with `test-support`.
//!
//! # Feature Flags
//!
//! - `test-support`: Exposes `testing` for a caller's tests (see `README.md`).

pub use self::{
    common::consts::{LSE_API_KEY, LSE_CME_API_KEY, LSE_NASDAQ_API_KEY, LSEDGE_VENUE},
    config::LseDataClientConfig,
    data::{serves, serves_custom, streams, streams_custom},
    factories::LseDataClientFactory,
    types::LseRow,
};

pub mod common;
mod config;
mod data;
mod factories;
pub mod http;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
mod types;
pub mod websocket;
