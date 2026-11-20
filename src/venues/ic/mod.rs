//! IC Markets, over Spotware's cTrader Open API through the cTrader adapter (`ctrader`): forex and
//! metals quotes for the catalog and the live node's clients on a hedging account, both through
//! `BROKER` (see `super`). Instruments are on venue `ICMARKETS` (`XAUUSD.ICMARKETS`), as are the
//! clients' names. See `README.md`.
//!
//! The adapter names no broker and the credential is the cTrader login's, so this module names only
//! the venue and the account variables.

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

/// IC's connection, from the process' environment: see `CTraderConnection::from_vars`.
///
/// # Errors
///
/// Returns an error if `CTraderConnection::from_vars` fails.
pub fn connection() -> anyhow::Result<CTraderConnection> {
    BROKER.connection()
}
