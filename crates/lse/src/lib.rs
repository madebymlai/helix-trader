//! A NautilusTrader adapter for London Strategic Edge's market data (LSE Data 3.0): a data client
//! and no execution client, LSE trading nothing.
//!
//! It names no symbol and decides nothing a key may have: it asks LSE, and LSE's refusal is the
//! answer, passed on with its code, message and required plan. See `README.md`.
//!
//! `common`, `http` and `websocket` are public, as in Nautilus' adapters
//! (`docs/developer_guide/adapters.md`), so that the tests of the clients' boundaries (`tests/`)
//! drive them as a caller does. `testing`, the mock LSE peers and their captured payloads, comes
//! with `test-support`.
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
