//! LSE's names and addresses, and the adapter's.

use std::{num::NonZeroU32, sync::LazyLock};

use nautilus_model::identifiers::Venue;

/// The venue of LSE's own derived series, one instrument each: `LSEDGE`,
/// not `LSE`, which is IB's London Stock Exchange.
pub static LSEDGE_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("LSEDGE"));

/// The variable holding the LSE key, for LSE's own markets.
pub const LSE_API_KEY: &str = "LSE_API_KEY";

/// The name of the data client, and of its factory.
pub(crate) const LSE_CLIENT: &str = "LSE";

/// LSE's REST API, every call a GET under `/v3`.
pub(crate) const LSE_HTTP_BASE_URL: &str = "https://download.londonstrategicedge.com";

/// The calls a minute of LSE's free plan, the fewest of any plan (LSE's
/// docs, "Calls a minute").
pub(crate) const FREE_CALLS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(60).expect("not zero");

/// An LSE socket of the protocol four of them share (LSE's WebSocket
/// documentation, "The sockets"), each carrying its markets: those a
/// derived market's series streams on. The fourth, chicago-ws, carries
/// futures and forex, which data-ws carries whole, sending the same ticks
/// (captured on 13 November 2026), so no series streams there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LseSocket {
    /// data-ws: US stocks and ETFs, US option contracts, forex,
    /// commodities, futures, indices and US bond yields.
    Data,
    /// london-ws: UK stocks, and crypto since 8 November 2026.
    London,
    /// tsx-ws: Canadian stocks.
    Tsx,
}

impl LseSocket {
    /// LSE's name of the socket.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Data => "data-ws",
            Self::London => "london-ws",
            Self::Tsx => "tsx-ws",
        }
    }

    /// The socket's address.
    pub(crate) fn url(self) -> &'static str {
        match self {
            Self::Data => "wss://data-ws.londonstrategicedge.com",
            Self::London => "wss://london-ws.londonstrategicedge.com",
            Self::Tsx => "wss://tsx-ws.londonstrategicedge.com",
        }
    }
}

/// LSE's exchange markets: real listings, which take the ids Databento
/// gives them, not `LSEDGE` ones. `opra` is a second name for
/// `us_options`. Every other market is derived.
pub(crate) const EXCHANGE_MARKETS: &[&str] = &["cme", "nasdaq", "us_options", "opra"];
