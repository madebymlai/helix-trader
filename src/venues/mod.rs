//! The venues: each one's history and live clients in a module of its
//! own, and the routing table, which says which history source fills an
//! instrument's series (`HistorySource`) and which broker trades it live
//! (`LiveBroker`), both by its venue. Fill-catalog and the live runner
//! partition the instruments through it, the live runner by asking for the
//! node's clients (`live_clients`), and venue code checks its own
//! instruments against it.

pub mod databento;
pub mod ib;
pub mod ic;
pub mod okx;

use anyhow::Context;
use nautilus_model::identifiers::{InstrumentId, Venue};
use nautilus_okx::common::consts::OKX_VENUE;
use rust_decimal::Decimal;

use self::{ib::IbAccount, ic::IC_VENUE};
use crate::{live::node::Clients, strategy::DataRequirements};

/// The source of an instrument's history in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySource {
    /// OKX's public API, through Nautilus' OKX data client.
    Okx,
    /// IC Markets' cTrader tick history.
    Ic,
    /// Databento's `GLBX.MDP3` dataset.
    Databento,
    /// Interactive Brokers' historical bars.
    Ib,
}

/// The broker of an instrument on the live node: its data and execution
/// clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveBroker {
    /// OKX's clients.
    Okx,
    /// IC Markets' cTrader clients, on a hedging account.
    Ic,
    /// Interactive Brokers' clients, on the gateway.
    Ib,
}

impl HistorySource {
    /// The history source of `id`: its venue's row (see `row`).
    #[must_use]
    pub fn of(id: InstrumentId) -> Self {
        row(id.venue).0
    }
}

impl LiveBroker {
    /// The live broker of `id`: its venue's row (see `row`).
    #[must_use]
    pub fn of(id: InstrumentId) -> Self {
        row(id.venue).1
    }
}

/// The routing table's row of `venue`: its history source and its live
/// broker. GLBX, CME Globex as Databento names it, has its history from
/// Databento and its live broker in IB, Databento having no live clients
/// here; every venue without a row of its own is IB's, history and live.
fn row(venue: Venue) -> (HistorySource, LiveBroker) {
    if venue == *OKX_VENUE {
        (HistorySource::Okx, LiveBroker::Okx)
    } else if venue == Venue::new(IC_VENUE) {
        (HistorySource::Ic, LiveBroker::Ic)
    } else if venue == Venue::GLBX() {
        (HistorySource::Databento, LiveBroker::Ib)
    } else {
        (HistorySource::Ib, LiveBroker::Ib)
    }
}

/// The live node's clients of `instrument_ids`, those of each one's live
/// broker (`LiveBroker`), for the instruments `requirements` reads: OKX's,
/// IC's then IB's. IC's bar types are checked and IC's environment and
/// credential read before anything connects; OKX's clients are set up at
/// `leverage`, IB's trading `account` with `client_id`.
///
/// # Errors
///
/// Returns an error if an IC bar type is not built from quotes or IC's
/// environment or credential is missing (see `ic::clients`, checked before
/// anything connects), if OKX's clients cannot be set up or the OKX
/// account does not suit the run (see `okx::clients`), if IC is
/// unreachable or its account is not hedging or not `IC_ACCOUNT_ID`'s (see
/// `ic::IcClients::check_account`), if the strategy trades IB instruments
/// and no account is given, or if IB's clients cannot be set up (see
/// `ib::clients`).
pub(crate) async fn live_clients(
    requirements: &DataRequirements,
    instrument_ids: &[InstrumentId],
    account: Option<&IbAccount>,
    client_id: i32,
    leverage: Decimal,
) -> anyhow::Result<Vec<Clients>> {
    let (mut okx_ids, mut ic_ids, mut ib_ids) = (Vec::new(), Vec::new(), Vec::new());
    for &id in instrument_ids {
        match LiveBroker::of(id) {
            LiveBroker::Okx => okx_ids.push(id),
            LiveBroker::Ic => ic_ids.push(id),
            LiveBroker::Ib => ib_ids.push(id),
        }
    }
    // Read before anything connects.
    let ic = if ic_ids.is_empty() {
        None
    } else {
        Some(ic::clients(
            &requirements.of_instruments(&ic_ids).bar_types,
            |name| std::env::var(name).ok(),
        )?)
    };
    let mut clients = Vec::new();
    if !okx_ids.is_empty() {
        let okx = okx::clients(&okx_ids, leverage).await?;
        clients.push(okx.into_node_clients());
    }
    if let Some(ic) = ic {
        ic.check_account().await?;
        clients.push(ic.into_node_clients());
    }
    if !ib_ids.is_empty() {
        let account = account.context(
            "the strategy trades IB instruments: give the IB account \
             (--account or NAUTILUS_IB_ACCOUNT_ID)",
        )?;
        let ib = ib::clients(account, client_id, &ib_ids).await?;
        clients.push(ib.into_node_clients());
    }
    Ok(clients)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_instrument_routes_to_the_row_of_its_venue() {
        use HistorySource as H;
        use LiveBroker as L;
        for (id, history, live) in [
            ("BTC-USDT-SWAP.OKX", H::Okx, L::Okx),
            ("XAUUSD.ICMARKETS", H::Ic, L::Ic),
            ("GCZ26.GLBX", H::Databento, L::Ib),
            ("GC.GLBX", H::Databento, L::Ib),
            ("GC.v.0.GLBX", H::Databento, L::Ib),
            ("SXR8.IBIS", H::Ib, L::Ib),
            ("ESZ26.CME", H::Ib, L::Ib),
            ("GCZ26.XCEC", H::Ib, L::Ib),
            ("EUR/USD.IDEALPRO", H::Ib, L::Ib),
            ("BTC-USDT-SWAP.BINANCE", H::Ib, L::Ib),
        ] {
            let id = InstrumentId::from(id);
            assert_eq!(
                (HistorySource::of(id), LiveBroker::of(id)),
                (history, live),
                "{id}"
            );
        }
    }
}
