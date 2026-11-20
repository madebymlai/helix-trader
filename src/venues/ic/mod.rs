//! Connects to IC Markets over Spotware's cTrader Open API, through the cTrader adapter
//! (`ctrader`).
//!
//! It serves forex and metals quotes on a hedging account, for the catalog and the live node's
//! clients, both through `BROKER` (see `super`). Instruments are on venue `ICMARKETS`
//! (`XAUUSD.ICMARKETS`), and the clients are named `ICMARKETS` too. See `README.md`.
//!
//! The adapter names no broker and the credential belongs to the cTrader login, so this module
//! names only the venue and the account variables.

use ctrader::{CTraderAccountVars, CTraderConnection};

use super::CTraderBroker;

/// The venue of IC's instruments and the name of its clients.
pub const VENUE: &str = "ICMARKETS";

/// IC as the routing table takes it: its venue and account variables.
pub(super) const BROKER: CTraderBroker = CTraderBroker {
    venue: VENUE,
    account: CTraderAccountVars {
        environment: "IC_ENVIRONMENT",
        account_id: "IC_ACCOUNT_ID",
    },
};

/// Returns the IC connection, built from the process' environment.
///
/// See `CTraderConnection::from_vars`.
///
/// # Errors
///
/// Returns an error if `CTraderConnection::from_vars` fails.
pub fn connection() -> anyhow::Result<CTraderConnection> {
    BROKER.connection()
}
