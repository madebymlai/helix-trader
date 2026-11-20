//! IC Markets, over Spotware's cTrader Open API through the cTrader adapter (`ctrader`): its forex
//! and metals and their quotes for the catalog, which fill-catalog fills through the node, and the
//! live node's clients on a hedging account, both through `BROKER`, one of the routing table's
//! cTrader brokers (`super`). Its instruments are on venue `ICMARKETS` (`XAUUSD.ICMARKETS`), its
//! clients named `ICMARKETS`. See `README.md`.
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

/// IC's connection, from the process' environment (or `.env`, once read): see
/// `CTraderConnection::from_vars`.
///
/// # Errors
///
/// Returns an error if `CTraderConnection::from_vars` fails.
pub fn connection() -> anyhow::Result<CTraderConnection> {
    BROKER.connection()
}
