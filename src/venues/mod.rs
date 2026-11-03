//! The venues: each one's history and live clients in a module of its
//! own, and the routing table, which says which history source fills an
//! instrument's series (`HistorySource`) and which broker trades it live
//! (`LiveBroker`), both by its venue. Fill-catalog and the live runner
//! partition the instruments through it, and venue code checks its own
//! instruments against it.

pub mod databento;
pub mod ib;
pub mod okx;

use nautilus_model::identifiers::{InstrumentId, Venue};
use nautilus_okx::common::consts::OKX_VENUE;

use crate::data::ic::parse::IC_VENUE;

/// The source of an instrument's history in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySource {
    /// OKX's archive and public API.
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
            ("GCZ6.GLBX", H::Databento, L::Ib),
            ("GC.GLBX", H::Databento, L::Ib),
            ("GC.v.0.GLBX", H::Databento, L::Ib),
            ("SXR8.IBIS", H::Ib, L::Ib),
            ("ESZ6.CME", H::Ib, L::Ib),
            ("GCZ6.XCEC", H::Ib, L::Ib),
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
