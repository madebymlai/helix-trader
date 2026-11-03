//! A NautilusTrader adapter for Spotware's cTrader Open API, naming no
//! broker: the caller configures the broker's venue, the name of its
//! clients and the connection (`config`), and the adapter serves the
//! broker's forex and metals, their quotes from cTrader's tick history and
//! live from its spot events, and trades a hedging account. See
//! `README.md`.
//!
//! Nautilus has no cTrader adapter; this one is laid out as Nautilus'
//! adapters are (`docs/developer_guide/adapters.md`): `common` (Spotware's
//! protobuf messages from the vendored `proto/`, the parsing of symbols
//! into instruments, the account check, the token pair and its token
//! file), `config` (the environment, the credential and the client
//! configs), `websocket` (the connection: an outer client and its handler
//! task), `session` (`CTraderSession`, the cTrader session history and both
//! clients share: the tick history merged into quotes, the spot
//! subscriptions, the trade requests), `data` (the data client, its live
//! quotes from the spot events), `execution` (the execution client: the
//! trade requests of orders, the dispatch of trade events, the
//! reconciliation reports) and `factories`.

use jiff::SignedDuration;
use nautilus_model::data::NautilusDataType;

pub use self::{
    config::{
        CTraderAccountId, CTraderConnection, CTraderCredential, CTraderDataClientConfig,
        CTraderEnvironment, CTraderExecutionClientConfig, CTraderSettingNames,
    },
    factories::{CTraderDataClientFactory, CTraderExecutionClientFactory},
    session::CTraderSession,
};

mod common;
mod config;
mod data;
#[cfg(feature = "demo-test")]
pub mod demo;
mod execution;
mod factories;
mod session;
#[cfg(test)]
mod testing;
mod websocket;

/// The only data the adapter serves: quotes, from cTrader's BID and ASK
/// ticks; bars are Nautilus' to build from them.
pub const SERVED_DATA: NautilusDataType = NautilusDataType::QuoteTick;

/// The longest window one tick request spans (Spotware's docs: a week),
/// about 0.7 M BID and 0.7 M ASK ticks of XAUUSD.
pub const TICK_REQUEST_SPAN: SignedDuration = SignedDuration::from_hours(7 * 24);
