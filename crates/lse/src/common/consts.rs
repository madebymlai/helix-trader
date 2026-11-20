//! LSE's names and addresses, and the adapter's.

use std::{fmt::Display, num::NonZeroU32, sync::LazyLock};

use nautilus_model::identifiers::Venue;

use super::credential::LseProduct;

/// The venue of LSE's derived series.
///
/// It is `LSEDGE`, not `LSE`, which is IB's London Stock Exchange.
pub static LSEDGE_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("LSEDGE"));

/// The variable holding the LSE key.
pub const LSE_API_KEY: &str = "LSE_API_KEY";

/// The variable holding a CME Group plan's key.
pub const LSE_CME_API_KEY: &str = "LSE_CME_API_KEY";

/// The variable holding a Nasdaq plan's key.
pub const LSE_NASDAQ_API_KEY: &str = "LSE_NASDAQ_API_KEY";

/// The name of the data client and its factory.
pub const LSE_CLIENT: &str = "LSE";

/// LSE's REST API. Every call is a GET under `/v3`.
pub const LSE_HTTP_BASE_URL: &str = "https://download.londonstrategicedge.com";

/// The calls a minute on LSE's free plan, which has the fewest of any plan (LSE's docs, "Calls a
/// minute").
pub const FREE_CALLS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(60).expect("not zero");

/// The calls a minute on an exchange plan's Lite, which has the fewest of its plans.
///
/// CME Group and Nasdaq plans have 200, 300, 500, 1,000 and 5,000, from Lite to Ultimate (LSE's
/// docs, "Calls a minute").
pub const EXCHANGE_LITE_CALLS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(200).expect("not zero");

/// LSE's name of CME Globex.
pub const CME_MARKET: &str = "cme";

/// LSE's CME Globex stream (LSE's WebSocket documentation, "The CME Group stream").
pub const CME_STREAM_URL: &str = "wss://cme.londonstrategicedge.com/v1/stream";

/// LSE's name of Nasdaq TotalView.
pub const NASDAQ_MARKET: &str = "nasdaq";

/// LSE's Nasdaq TotalView stream, which "sends the same messages for stocks" as the CME stream.
pub const NASDAQ_STREAM_URL: &str = "wss://nasdaq.londonstrategicedge.com/v1/stream";

/// The venue of Nasdaq TotalView's stocks.
///
/// Databento names them `XNAS.ITCH`, and Nautilus' IB adapter maps them to IB's NASDAQ.
pub static XNAS_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("XNAS"));

/// An exchange plan's market: CME Globex or Nasdaq TotalView.
///
/// Its listings are under the venue Databento gives them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseExchange {
    /// CME Globex (`cme`). Its contracts are under `GLBX`.
    Cme,
    /// Nasdaq TotalView (`nasdaq`). Its stocks are under `XNAS`.
    Nasdaq,
}

impl LseExchange {
    /// Returns the exchange whose listings are on `venue`, if one's are.
    pub fn of_venue(venue: Venue) -> Option<Self> {
        if venue == Venue::GLBX() {
            Some(Self::Cme)
        } else if venue == *XNAS_VENUE {
            Some(Self::Nasdaq)
        } else {
            None
        }
    }

    /// Returns the product whose key opens the market.
    pub const fn product(self) -> LseProduct {
        match self {
            Self::Cme => LseProduct::Cme,
            Self::Nasdaq => LseProduct::Nasdaq,
        }
    }

    /// Returns the market's stream.
    pub const fn stream_url(self) -> &'static str {
        match self {
            Self::Cme => CME_STREAM_URL,
            Self::Nasdaq => NASDAQ_STREAM_URL,
        }
    }
}

impl Display for LseExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Cme => "CME",
            Self::Nasdaq => "Nasdaq",
        })
    }
}

/// LSE's name of US options, which covers every listed contract, as OPRA reports it.
pub const US_OPTIONS_MARKET: &str = "us_options";

/// The source an `LseRow` data type uses to name the options chain (`/v3/options/chain`).
///
/// The endpoint takes no dates and so has no history.
pub const OPTIONS_CHAIN: &str = "options_chain";

/// The venue of US option contracts, as Databento names them (`OPRA.PILLAR`).
pub static OPRA_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("OPRA"));

/// An LSE socket. Four sockets share the protocol (LSE's WebSocket documentation, "The sockets").
///
/// The fourth, chicago-ws, carries futures and forex. data-ws carries them whole with the same
/// ticks (captured on 13 November 2026), so nothing streams on chicago-ws.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseSocket {
    /// data-ws: US stocks and ETFs, US option contracts, forex, commodities, futures and indices.
    ///
    /// It lists four "US bond yields" (`USB02Y/USD` to `USB30Y/USD`). They tick the notes' prices,
    /// not yields (`USB10Y/USD` at 105.36 when the 10-year yield closed at 5.133). They belong to
    /// no HTTP market, so none is streamed.
    Data,
    /// london-ws: UK stocks, and crypto since 8 November 2026.
    London,
    /// tsx-ws: Canadian stocks.
    Tsx,
}

impl LseSocket {
    /// Returns LSE's name of the socket.
    pub fn name(self) -> &'static str {
        match self {
            Self::Data => "data-ws",
            Self::London => "london-ws",
            Self::Tsx => "tsx-ws",
        }
    }

    /// Returns the socket's address.
    pub fn url(self) -> &'static str {
        match self {
            Self::Data => "wss://data-ws.londonstrategicedge.com",
            Self::London => "wss://london-ws.londonstrategicedge.com",
            Self::Tsx => "wss://tsx-ws.londonstrategicedge.com",
        }
    }
}

/// LSE's exchange markets. They have real listings with Databento's ids, not `LSEDGE` ones.
///
/// `opra` is a second name for `us_options`. Every other market is derived.
pub const EXCHANGE_MARKETS: &[&str] = &["cme", "nasdaq", "us_options", "opra"];
