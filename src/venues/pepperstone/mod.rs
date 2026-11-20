//! Provides Pepperstone forex and metals for the catalog, and the live node's clients on a hedging
//! account. Both use Spotware's cTrader Open API through the cTrader adapter (`ctrader`), and both
//! go through `BROKER`, one of the cTrader brokers in the routing table (`super`). The instruments
//! are on venue `PEPPERSTONE` (`XAUUSD.PEPPERSTONE`), and the clients are named `PEPPERSTONE`. See
//! `README.md`.
//!
//! The adapter names no broker, and the credential is the cTrader login. So this module names only
//! the venue and the account variables.

use ctrader::{CTraderAccountVars, CTraderConnection};

use super::CTraderBroker;

/// The venue for the Pepperstone instruments, and the name of its clients.
pub const VENUE: &str = "PEPPERSTONE";

pub(super) const BROKER: CTraderBroker = CTraderBroker {
    venue: VENUE,
    account: CTraderAccountVars {
        environment: "PEPPERSTONE_ENVIRONMENT",
        account_id: "PEPPERSTONE_ACCOUNT_ID",
    },
};

/// Returns the Pepperstone connection from the process environment.
///
/// # Errors
///
/// Returns an error if `CTraderConnection::from_vars` fails.
pub fn connection() -> anyhow::Result<CTraderConnection> {
    BROKER.connection()
}
