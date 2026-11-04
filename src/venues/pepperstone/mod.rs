//! Pepperstone, over Spotware's cTrader Open API through the cTrader
//! adapter (`ctrader`): Pepperstone's forex and metals and their quotes for
//! the catalog, which fill-catalog fills through the node, and the live
//! node's clients on a hedging account, wired in the routing table
//! (`super`). Pepperstone's instruments are on venue `PEPPERSTONE`
//! (`XAUUSD.PEPPERSTONE`), its clients named `PEPPERSTONE`. See `README.md`.
//!
//! The adapter names no broker and the credential is the cTrader login's:
//! this module names Pepperstone's venue and the variables of its account.

use ctrader::{CTraderAccountVars, CTraderConnection};
use nautilus_model::identifiers::Venue;

/// The venue of Pepperstone's instruments, and the name of its clients.
pub const VENUE: &str = "PEPPERSTONE";

/// The variables of Pepperstone's account.
pub(super) const ACCOUNT: CTraderAccountVars = CTraderAccountVars {
    environment: "PEPPERSTONE_ENVIRONMENT",
    account_id: "PEPPERSTONE_ACCOUNT_ID",
};

/// Pepperstone's connection, from the process' environment (or `.env`, once
/// read): see `CTraderConnection::from_vars`.
///
/// # Errors
///
/// As `CTraderConnection::from_vars`.
pub fn connection() -> anyhow::Result<CTraderConnection> {
    CTraderConnection::from_vars(Venue::new(VENUE), ACCOUNT, |name| std::env::var(name).ok())
}
