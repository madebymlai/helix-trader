//! The config of the data client.

use std::{any::Any, num::NonZeroU32};

use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::SecretString;
use nautilus_model::identifiers::InstrumentId;

use crate::common::consts::{FREE_CALLS_PER_MINUTE, LSE_HTTP_BASE_URL, LseSocket};

/// The config of the data client: the key, where LSE is, the calls a
/// minute of the key's plan, and the instruments it streams. Its `Debug`
/// shows no key.
#[derive(Debug, Clone, bon::Builder)]
pub struct LseDataClientConfig {
    /// The LSE key; the one in `LSE_API_KEY` if none, read when the client
    /// is created.
    pub api_key: Option<SecretString>,
    /// LSE's REST API; LSE's own if none.
    pub base_url_http: Option<String>,
    /// LSE's data-ws; LSE's own if none.
    pub base_url_ws_data: Option<String>,
    /// LSE's london-ws; LSE's own if none.
    pub base_url_ws_london: Option<String>,
    /// LSE's tsx-ws; LSE's own if none.
    pub base_url_ws_tsx: Option<String>,
    /// The calls a minute the client keeps within: the free plan's, the
    /// fewest of any plan, unless the key's plan allows more (LSE's
    /// `/v3/usage`, `calls_per_minute`).
    #[builder(default = FREE_CALLS_PER_MINUTE)]
    pub calls_per_minute: NonZeroU32,
    /// The instruments the client streams, on a live node: `connect` opens
    /// the socket each one's market streams on, authenticates the key, and
    /// fails unless each socket lists its instruments and allows them all
    /// on its one connection. None on a fill's node, which opens no socket.
    #[builder(default)]
    pub stream_ids: Vec<InstrumentId>,
}

impl Default for LseDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl LseDataClientConfig {
    /// The REST API the client calls: the override, or LSE's own.
    #[must_use]
    pub fn http_base_url(&self) -> &str {
        self.base_url_http.as_deref().unwrap_or(LSE_HTTP_BASE_URL)
    }

    /// The address the client connects to `socket` at: the override, or
    /// LSE's own.
    pub(crate) fn ws_url(&self, socket: LseSocket) -> &str {
        let url = match socket {
            LseSocket::Data => &self.base_url_ws_data,
            LseSocket::London => &self.base_url_ws_london,
            LseSocket::Tsx => &self.base_url_ws_tsx,
        };
        url.as_deref().unwrap_or(socket.url())
    }
}

impl ClientConfig for LseDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lse_s_own_api_unless_told_otherwise() {
        let mock = LseDataClientConfig::builder()
            .base_url_http("http://127.0.0.1:9".to_string())
            .build();

        assert_eq!(
            LseDataClientConfig::default().http_base_url(),
            "https://download.londonstrategicedge.com"
        );
        assert_eq!(mock.http_base_url(), "http://127.0.0.1:9");
    }

    /// LSE's addresses, as its socket documentation gives them ("The
    /// sockets").
    #[test]
    fn each_socket_is_lse_s_own_unless_told_otherwise() {
        let mock = LseDataClientConfig::builder()
            .base_url_ws_london("ws://127.0.0.1:9".to_string())
            .build();
        let own = LseDataClientConfig::default();

        assert_eq!(
            [LseSocket::Data, LseSocket::London, LseSocket::Tsx].map(|socket| own.ws_url(socket)),
            [
                "wss://data-ws.londonstrategicedge.com",
                "wss://london-ws.londonstrategicedge.com",
                "wss://tsx-ws.londonstrategicedge.com",
            ]
        );
        assert_eq!(
            [LseSocket::Data, LseSocket::London, LseSocket::Tsx].map(|socket| mock.ws_url(socket)),
            [
                "wss://data-ws.londonstrategicedge.com",
                "ws://127.0.0.1:9",
                "wss://tsx-ws.londonstrategicedge.com",
            ]
        );
    }

    #[test]
    fn the_free_plan_s_60_calls_a_minute_unless_told_otherwise() {
        assert_eq!(LseDataClientConfig::default().calls_per_minute.get(), 60);
    }

    #[test]
    fn the_key_is_not_in_the_config_s_debug() {
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from("lse-test-key-0123456789"))
            .build();

        assert!(!format!("{config:?}").contains("lse-test-key-0123456789"));
    }
}
