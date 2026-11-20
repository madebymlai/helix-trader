//! A NautilusTrader adapter for Spotware's cTrader Open API, naming no broker: the caller names the
//! broker's venue and account variables (`config`). Serves forex and metals quotes from the tick
//! history and live spot events, and trades a hedging account. See `README.md`.
//!
//! Nautilus has no cTrader adapter; this one is laid out as Nautilus' adapters are
//! (`docs/developer_guide/adapters.md`):
//! - `common`: the protobuf messages from the vendored `proto/`, symbol parsing, the account check,
//!   the token pair and its file.
//! - `config`: the environment, credential and client configs.
//! - `websocket`: the connection, an outer client and its handler task.
//! - `session`: `CTraderSession`, shared by both clients: tick history merged into quotes, spot
//!   subscriptions and trade requests.
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

/// The only data served: quotes from cTrader's BID and ASK ticks, from which Nautilus builds bars.
pub const SERVED_DATA: NautilusDataType = NautilusDataType::QuoteTick;

/// The longest window one tick request spans (a week, per Spotware's docs), about 0.7 M BID and
/// 0.7 M ASK ticks of XAUUSD.
pub const TICK_REQUEST_SPAN: SignedDuration = SignedDuration::from_hours(7 * 24);
