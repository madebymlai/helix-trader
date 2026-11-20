//! A NautilusTrader adapter for Spotware's cTrader Open API.
//!
//! The adapter names no broker. The caller names the broker's venue and account variables
//! (`config`). It serves forex and metals quotes from the tick history and live spot events, and
//! trades a hedging account. See `README.md`.
//!
//! Nautilus has no cTrader adapter. This one is laid out as Nautilus' adapters are
//! (`docs/developer_guide/adapters.md`):
//! - `common`: the protobuf messages from the vendored `proto/`, symbol parsing, the account check,
//!   and the token pair with its file.
//! - `config`: the environment, credential and client configs.
//! - `websocket`: the connection, an outer client and its handler task.
//! - `session`: `CTraderSession`, which both clients share. It merges tick history into quotes and
//!   handles spot subscriptions and trade requests.
//! - `data`: the data client.
//! - `execution`: the execution client.
//! - `factories`.
//!
//! # Feature Flags
//!
//! - `test-support`: Exposes the hooks for an end-to-end test of token renewal on a real server
//!   (see `README.md`).

use jiff::SignedDuration;
use nautilus_model::data::NautilusDataType;

pub use self::{
    common::parse::quoted,
    config::{
        CTraderAccountId, CTraderAccountVars, CTraderConnection, CTraderCredential,
        CTraderDataClientConfig, CTraderEnvironment, CTraderExecutionClientConfig,
    },
    factories::{CTraderDataClientFactory, CTraderExecutionClientFactory},
    session::CTraderSession,
};
// Returned by the `test-support` hooks
pub use self::{common::token::TokenPair, websocket::SpotEvents};

mod common;
mod config;
mod data;
mod execution;
mod factories;
mod session;
#[cfg(test)]
mod testing;
mod websocket;

/// The only data served.
///
/// Quotes come from cTrader's BID and ASK ticks. Nautilus builds bars from them.
pub const SERVED_DATA: NautilusDataType = NautilusDataType::QuoteTick;

/// The longest window one tick request spans.
///
/// That is a week, per Spotware's docs: about 0.7 M BID and 0.7 M ASK ticks of XAUUSD.
pub const TICK_REQUEST_SPAN: SignedDuration = SignedDuration::from_hours(7 * 24);
