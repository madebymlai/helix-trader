//! LSE's names and addresses, and the adapter's.

use std::{fmt, num::NonZeroU32, sync::LazyLock};

use nautilus_model::identifiers::Venue;

use super::credential::LseProduct;

/// The venue of LSE's own derived series, one instrument each: `LSEDGE`,
/// not `LSE`, which is IB's London Stock Exchange.
pub static LSEDGE_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("LSEDGE"));

/// The variable holding the LSE key, for LSE's own markets.
pub const LSE_API_KEY: &str = "LSE_API_KEY";

/// The variable holding a CME Group plan's key, for CME Globex.
pub const LSE_CME_API_KEY: &str = "LSE_CME_API_KEY";

/// The variable holding a Nasdaq plan's key, for Nasdaq TotalView.
pub const LSE_NASDAQ_API_KEY: &str = "LSE_NASDAQ_API_KEY";

/// The name of the data client, and of its factory.
pub const LSE_CLIENT: &str = "LSE";

/// LSE's REST API, every call a GET under `/v3`.
pub const LSE_HTTP_BASE_URL: &str = "https://download.londonstrategicedge.com";

/// The calls a minute of LSE's free plan, the fewest of any plan (LSE's
/// docs, "Calls a minute").
pub const FREE_CALLS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(60).expect("not zero");

/// The calls a minute of an exchange plan's Lite, CME Group's or Nasdaq's,
/// the fewest of its plans (LSE's docs, "Calls a minute": CME Group and
/// Nasdaq, Lite, Lite+, Standard, Standard+ and Ultimate allow 200, 300,
/// 500, 1,000 and 5,000).
pub const EXCHANGE_LITE_CALLS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(200).expect("not zero");

/// LSE's name of CME Globex, its market's folder.
pub const CME_MARKET: &str = "cme";

/// LSE's CME Globex stream, a socket of its own protocol for a CME Group
/// plan's key (LSE's WebSocket documentation, "The CME Group stream").
pub const CME_STREAM_URL: &str = "wss://cme.londonstrategicedge.com/v1/stream";

/// LSE's name of Nasdaq TotalView, its market's folder.
pub const NASDAQ_MARKET: &str = "nasdaq";

/// LSE's Nasdaq TotalView stream, for a Nasdaq plan's key, which "sends
/// the same messages for stocks" as the CME stream (LSE's WebSocket
/// documentation, "The CME Group stream").
pub const NASDAQ_STREAM_URL: &str = "wss://nasdaq.londonstrategicedge.com/v1/stream";

/// The venue of Nasdaq TotalView's stocks, XNAS, as Databento names the
/// listings of Nasdaq's TotalView-ITCH feed (`XNAS.ITCH`, whose
/// definitions its publisher `XNAS` sends) and Nautilus' IB adapter maps
/// them, to IB's NASDAQ.
pub static XNAS_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("XNAS"));

/// An exchange plan's market: CME Globex or Nasdaq TotalView, its rows
/// asked with the plan's key and its listings streamed live on a socket of
/// its own, both of one protocol (LSE's WebSocket documentation, "The CME
/// Group stream": the Nasdaq stream "sends the same messages for stocks"),
/// under the venue Databento gives them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseExchange {
    /// CME Globex (`cme`), its contracts under `GLBX`.
    Cme,
    /// Nasdaq TotalView (`nasdaq`), its stocks under `XNAS`.
    Nasdaq,
}

impl LseExchange {
    /// The exchange whose listings are on `venue`, if one's are.
    pub fn of_venue(venue: Venue) -> Option<Self> {
        if venue == Venue::GLBX() {
            Some(Self::Cme)
        } else if venue == *XNAS_VENUE {
            Some(Self::Nasdaq)
        } else {
            None
        }
    }

    /// The product whose key opens the market.
    pub const fn product(self) -> LseProduct {
        match self {
            Self::Cme => LseProduct::Cme,
            Self::Nasdaq => LseProduct::Nasdaq,
        }
    }

    /// The market's stream.
    pub const fn stream_url(self) -> &'static str {
        match self {
            Self::Cme => CME_STREAM_URL,
            Self::Nasdaq => NASDAQ_STREAM_URL,
        }
    }
}

/// The exchange's name, as LSE names its stream.
impl fmt::Display for LseExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cme => "CME",
            Self::Nasdaq => "Nasdaq",
        })
    }
}

/// LSE's name of US options, its market's folder: every listed US option
/// contract, as OPRA reports it.
pub const US_OPTIONS_MARKET: &str = "us_options";

/// The source an `LseRow` data type names LSE's options chain by
/// (`/v3/options/chain`): the listed contracts of an underlying as they
/// stand, the call taking no dates, so it has no history.
pub const OPTIONS_CHAIN: &str = "options_chain";

/// The venue of US option contracts, OPRA, as Databento names the
/// consolidated feed's listings (`OPRA.PILLAR`, publisher `OPRA`) and
/// Nautilus' IB adapter maps them.
pub static OPRA_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("OPRA"));

/// An LSE socket of the protocol four of them share (LSE's WebSocket
/// documentation, "The sockets"), each carrying its markets: those a
/// derived market's series streams on. The fourth, chicago-ws, carries
/// futures and forex, which data-ws carries whole, sending the same ticks
/// (captured on 13 November 2026), so no series streams there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseSocket {
    /// data-ws: US stocks and ETFs, US option contracts, forex,
    /// commodities, futures and indices. The four "US bond yields" it lists
    /// (`USB02Y/USD` to `USB30Y/USD`) tick the notes' prices, not yields
    /// (`USB10Y/USD` at 105.36 when the 10-year yield closed at 5.133), and
    /// belong to no market of LSE's HTTP API, so none is streamed.
    Data,
    /// london-ws: UK stocks, and crypto since 8 November 2026.
    London,
    /// tsx-ws: Canadian stocks.
    Tsx,
}

impl LseSocket {
    /// LSE's name of the socket.
    pub fn name(self) -> &'static str {
        match self {
            Self::Data => "data-ws",
            Self::London => "london-ws",
            Self::Tsx => "tsx-ws",
        }
    }

    /// The socket's address.
    pub fn url(self) -> &'static str {
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
pub const EXCHANGE_MARKETS: &[&str] = &["cme", "nasdaq", "us_options", "opra"];
