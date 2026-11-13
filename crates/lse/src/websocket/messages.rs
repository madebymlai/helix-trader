//! The JSON frames of LSE's sockets (LSE's WebSocket documentation,
//! "Protocol"): those the client sends (`LseWsAction`), and those LSE sends
//! (`LseWsFrame`), each an object tagged by its `type`, the fields the
//! client does not read left out.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A frame the client sends, tagged by its `action`.
#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(crate) enum LseWsAction<'a> {
    /// The key, answering the socket's welcome.
    Auth { api_key: &'a str },
    /// The ticks of a symbol, from now on.
    Subscribe { symbol: &'a str },
    /// No more ticks of a symbol.
    Unsubscribe { symbol: &'a str },
    /// The application keepalive, which LSE answers with a pong.
    Ping,
}

impl LseWsAction<'_> {
    /// The frame's text.
    pub(crate) fn to_json(&self) -> String {
        serde_json::to_string(self).expect("an action serializes")
    }
}

/// A frame LSE sends, tagged by its `type`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum LseWsFrame {
    /// The socket's first frame, before the key.
    Welcome,
    /// The key accepted: what the socket streams to it.
    Authenticated(LseWsAuthenticated),
    /// A subscription accepted, under the symbol's current spelling.
    Subscribed {
        symbol: String,
    },
    /// A subscription ended.
    Unsubscribed {
        symbol: String,
    },
    Tick(LseTick),
    /// The answer to a ping.
    Pong,
    /// The catalogue, answering a listing.
    Symbols,
    /// A refusal, of the key or of a subscription.
    Error(LseWsRefusal),
}

/// What the socket streams to the key it accepted.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LseWsAuthenticated {
    /// The symbols the socket streams, as it spells them. LSE accepts a
    /// subscription to a symbol it does not list, and sends nothing.
    pub(crate) symbols: Vec<LseWsListed>,
    /// The symbols one connection of the key may stream at once.
    pub(crate) max_subscriptions: Option<usize>,
}

/// A symbol the socket lists.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LseWsListed {
    pub(crate) symbol: String,
}

/// A tick: its symbol as the socket spells it, LSE's time of it, and the
/// best bid and ask. Its price and volume are not read: a futures tick's
/// price is its bid (LSE's captures), and its volume a size of no quote.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LseTick {
    pub(crate) symbol: String,
    /// The source time in UTC, in a form that differs by feed.
    pub(crate) ts: String,
    pub(crate) bid: Option<f64>,
    pub(crate) ask: Option<f64>,
}

/// LSE's refusal: its code, its message, and what else it gives, such as
/// `max_connections`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct LseWsRefusal {
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(flatten)]
    pub(crate) details: Map<String, Value>,
}

impl fmt::Display for LseWsRefusal {
    /// `LIMIT_REACHED: Max 16 symbols on this key.`, then its details, as
    /// `(max_connections 1, open_connections 1)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        WS_AUTHENTICATED_FUTURES, WS_INVALID_KEY, WS_PONG, WS_SUBSCRIBED_GOLD, WS_TICKS_GOLD,
        WS_TOO_MANY_CONNECTIONS, WS_UNSUBSCRIBED_GOLD, WS_WELCOME,
    };

    fn frame(text: &str) -> LseWsFrame {
        serde_json::from_str(text).unwrap()
    }

    /// The captured frames, each as its type, their fields beyond those
    /// read ignored.
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

    /// Trimmed to the futures it lists (see `test_data/README.md`).
    #[rstest]
    fn the_authenticated_frame_gives_the_symbols_listed_and_the_subscriptions_allowed() {
        let LseWsFrame::Authenticated(authenticated) = frame(WS_AUTHENTICATED_FUTURES) else {
            panic!("not authenticated")
        };

        assert_eq!(authenticated.max_subscriptions, Some(16));
        assert!(authenticated.symbols.iter().any(|s| s.symbol == "GOLD.F"));
        assert_eq!(authenticated.symbols.len(), 33);
    }

    #[rstest]
    fn a_refusal_reads_as_its_code_its_message_and_its_details() {
        let LseWsFrame::Error(refusal) = frame(WS_TOO_MANY_CONNECTIONS) else {
            panic!("not a refusal")
        };
        let LseWsFrame::Error(key) = frame(WS_INVALID_KEY) else {
            panic!("not a refusal")
        };

        assert_eq!(
            refusal.to_string(),
            "TOO_MANY_CONNECTIONS: Max 1 concurrent websocket connection(s) for this API key; \
             1 already open from this same address. Close it and retry. \
             (max_connections 1, open_connections 1)"
        );
        assert_eq!(key.to_string(), "INVALID_KEY: Invalid or inactive API key");
    }

    /// As LSE's documentation writes them.
    #[rstest]
    fn the_client_s_frames_are_lse_s_actions() {
        assert_eq!(
            LseWsAction::Auth { api_key: "k" }.to_json(),
            r#"{"action":"auth","api_key":"k"}"#
        );
        assert_eq!(
            LseWsAction::Subscribe { symbol: "GOLD.F" }.to_json(),
            r#"{"action":"subscribe","symbol":"GOLD.F"}"#
        );
        assert_eq!(
            LseWsAction::Unsubscribe { symbol: "GOLD.F" }.to_json(),
            r#"{"action":"unsubscribe","symbol":"GOLD.F"}"#
        );
        assert_eq!(LseWsAction::Ping.to_json(), r#"{"action":"ping"}"#);
    }
}
