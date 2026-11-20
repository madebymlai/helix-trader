//! The JSON frames on LSE's sockets (LSE's WebSocket documentation, "Protocol").
//!
//! The client sends `LseWsAction` frames, and LSE sends `LseWsFrame` frames. Each frame is an
//! object tagged by its `type`. The types leave out the fields the client does not read.

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A frame the client sends, tagged by its `action`.
#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum LseWsAction<'a> {
    /// Sends the key in answer to the socket's welcome.
    Auth { api_key: &'a str },
    /// Streams ticks for a symbol from now on.
    Subscribe { symbol: &'a str },
    /// Stops ticks for a symbol.
    Unsubscribe { symbol: &'a str },
    /// Subscribes to every listed contract in an underlying's option chain as one subscription.
    SubscribeOptions { underlying: &'a str },
    /// Stops ticks for an underlying's option chain.
    UnsubscribeOptions { underlying: &'a str },
    /// Keeps the application alive. LSE answers with a pong.
    Ping,
}

impl LseWsAction<'_> {
    /// Returns the frame's text.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("an action serializes")
    }
}

/// A frame LSE sends, tagged by its `type`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LseWsFrame {
    /// The socket's first frame, sent before the key.
    Welcome,
    /// LSE accepted the key. The frame lists what the socket streams to it.
    Authenticated(LseWsAuthenticated),
    /// LSE accepted a subscription. The frame gives the symbol's current spelling.
    Subscribed {
        symbol: String,
    },
    /// A subscription ended.
    Unsubscribed {
        symbol: String,
    },
    /// LSE accepted the subscription to an underlying's option chain.
    OptionsSubscribed {
        underlying: String,
    },
    /// The subscription to an underlying's option chain ended.
    OptionsUnsubscribed {
        underlying: String,
    },
    Tick(LseTick),
    /// The answer to a ping.
    Pong,
    /// The catalogue, sent in answer to a listing.
    Symbols,
    /// LSE refused the key or a subscription.
    Error(LseWsRefusal),
}

/// What the socket streams to the key it accepted.
#[derive(Debug, Clone, Deserialize)]
pub struct LseWsAuthenticated {
    /// The symbols the socket streams. LSE accepts a subscription to an unlisted symbol and sends
    /// nothing.
    pub symbols: Vec<LseWsListed>,
    /// The number of symbols one connection of the key may stream at once.
    pub max_subscriptions: Option<usize>,
}

/// A symbol the socket lists.
#[derive(Debug, Clone, Deserialize)]
pub struct LseWsListed {
    pub symbol: String,
}

/// A tick with a symbol, LSE's time, price, best bid and ask, and volume (LSE's WebSocket
/// documentation, "The tick").
///
/// The volume is the trade's size, and it is 0 on a quote.
///
/// The client reads a series' tick as a quote and ignores its price and volume, because a futures
/// tick's price is its bid (LSE's captures). It reads an option contract's tick as a trade,
/// because the US options feed carries trades alone.
#[derive(Debug, Clone, Deserialize)]
pub struct LseTick {
    pub symbol: String,
    /// The source time in UTC, in a form that differs by feed.
    pub ts: String,
    pub price: Option<f64>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub volume: Option<f64>,
}

/// LSE's refusal, with a code, a message and any other details, such as `max_connections`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LseWsRefusal {
    pub code: String,
    pub message: String,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

impl Display for LseWsRefusal {
    /// Writes `LIMIT_REACHED: Max 16 symbols on this key.`, then `(max_connections 1,
    /// open_connections 1)`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)?;
        if !self.details.is_empty() {
            let details: Vec<String> = self
                .details
                .iter()
                .map(|(name, value)| format!("{name} {value}"))
                .collect();
            write!(f, " ({})", details.join(", "))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::testing::{
        WS_AUTHENTICATED_FUTURES, WS_INVALID_KEY, WS_OPTIONS_SUBSCRIBED_AAPL,
        WS_OPTIONS_UNSUBSCRIBED_AAPL, WS_PONG, WS_SUBSCRIBED_GOLD, WS_TICKS_AAPL_OPTIONS,
        WS_TICKS_GOLD, WS_TOO_MANY_CONNECTIONS, WS_UNSUBSCRIBED_GOLD, WS_WELCOME,
    };

    fn frame(text: &str) -> LseWsFrame {
        serde_json::from_str(text).unwrap()
    }

    #[rstest]
    fn every_captured_frame_reads_as_its_type() {
        assert!(matches!(frame(WS_WELCOME), LseWsFrame::Welcome));
        assert!(matches!(frame(WS_PONG), LseWsFrame::Pong));
        let LseWsFrame::Subscribed { symbol } = frame(WS_SUBSCRIBED_GOLD) else {
            panic!("not a subscription")
        };
        assert_eq!(symbol, "GOLD.F");
        let LseWsFrame::Unsubscribed { symbol } = frame(WS_UNSUBSCRIBED_GOLD) else {
            panic!("not an unsubscription")
        };
        assert_eq!(symbol, "GOLD.F");
        let LseWsFrame::Tick(tick) = frame(WS_TICKS_GOLD.lines().next().unwrap()) else {
            panic!("not a tick")
        };
        assert_eq!(
            (tick.symbol.as_str(), tick.bid, tick.ask),
            ("GOLD.F", Some(3888.0), Some(3888.2))
        );
    }

    /// A chain's acknowledgements name its underlying. An option's tick is a trade, so the price
    /// is the trade price and the volume is its size (LSE's WebSocket documentation, "The tick").
    #[rstest]
    fn the_documented_option_frames_read_as_their_types() {
        let LseWsFrame::OptionsSubscribed { underlying } = frame(WS_OPTIONS_SUBSCRIBED_AAPL) else {
            panic!("not a chain's subscription")
        };
        assert_eq!(underlying, "AAPL");
        let LseWsFrame::OptionsUnsubscribed { underlying } = frame(WS_OPTIONS_UNSUBSCRIBED_AAPL)
        else {
            panic!("not a chain's unsubscription")
        };
        assert_eq!(underlying, "AAPL");
        let LseWsFrame::Tick(tick) = frame(WS_TICKS_AAPL_OPTIONS.lines().next().unwrap()) else {
            panic!("not a tick")
        };
        assert_eq!(
            (tick.symbol.as_str(), tick.price, tick.volume, tick.bid),
            ("AAPL261120C00355000", Some(6.7), Some(1.0), None)
        );
    }

    /// The frame is trimmed to the futures it lists (see `test_data/README.md`).
    #[rstest]
    fn the_authenticated_frame_gives_the_symbols_listed_and_the_subscriptions_allowed() {
        let LseWsFrame::Authenticated(authenticated) = frame(WS_AUTHENTICATED_FUTURES) else {
            panic!("not authenticated")
        };

        assert_eq!(authenticated.max_subscriptions, Some(16));
        assert!(authenticated.symbols.iter().any(|s| s.symbol == "GOLD.F"));
        assert_eq!(authenticated.symbols.len(), 33);
    }

    /// A second connection on a key limited to one gives the limit and the open count. An unknown
    /// key gives no details.
    #[rstest]
    #[case::too_many_connections(
        WS_TOO_MANY_CONNECTIONS,
        &[
            "TOO_MANY_CONNECTIONS",
            "Max 1 concurrent websocket connection(s) for this API key; 1 already open from \
             this same address. Close it and retry.",
            "max_connections 1",
            "open_connections 1",
        ]
    )]
    #[case::invalid_key(WS_INVALID_KEY, &["INVALID_KEY", "Invalid or inactive API key"])]
    fn a_refusal_reads_as_its_code_its_message_and_its_details(
        #[case] text: &str,
        #[case] facts: &[&str],
    ) {
        let LseWsFrame::Error(refusal) = frame(text) else {
            panic!("not a refusal")
        };

        let read = refusal.to_string();
        for fact in facts {
            assert!(read.contains(fact), "{fact}: {read}");
        }
    }
}
