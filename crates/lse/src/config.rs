//! The config of the data client.

use std::{any::Any, num::NonZeroU32};

use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::SecretString;
use nautilus_model::identifiers::InstrumentId;

use crate::common::{
    consts::{
        EXCHANGE_LITE_CALLS_PER_MINUTE, FREE_CALLS_PER_MINUTE, LSE_HTTP_BASE_URL, LseExchange,
        LseSocket,
    },
    credential::LseProduct,
};

/// The config of the data client: the keys, where LSE is, the calls a
/// minute of each key's plan, and the instruments it streams. Its `Debug`
/// shows no key.
#[derive(Debug, Clone, bon::Builder)]
pub struct LseDataClientConfig {
    /// The LSE key, for LSE's own markets; the one in `LSE_API_KEY` if
    /// none, read when the client is created.
    pub api_key: Option<SecretString>,
    /// A CME Group plan's key, for CME Globex; the one in
    /// `LSE_CME_API_KEY` if none, read when the client is created.
    pub cme_api_key: Option<SecretString>,
    /// A Nasdaq plan's key, for Nasdaq TotalView; the one in
    /// `LSE_NASDAQ_API_KEY` if none, read when the client is created.
    pub nasdaq_api_key: Option<SecretString>,
    /// LSE's REST API; LSE's own if none.
    pub base_url_http: Option<String>,
    /// LSE's data-ws; LSE's own if none.
    pub base_url_ws_data: Option<String>,
    /// LSE's london-ws; LSE's own if none.
    pub base_url_ws_london: Option<String>,
    /// LSE's tsx-ws; LSE's own if none.
    pub base_url_ws_tsx: Option<String>,
    /// LSE's CME Globex stream; LSE's own if none.
    pub base_url_ws_cme: Option<String>,
    /// LSE's Nasdaq TotalView stream; LSE's own if none.
    pub base_url_ws_nasdaq: Option<String>,
    /// The calls a minute the client keeps within on the LSE key: the free
    /// plan's, the fewest of any plan, unless the key's plan allows more
    /// (LSE's `/v3/usage`, `calls_per_minute`).
    #[builder(default = FREE_CALLS_PER_MINUTE)]
    pub calls_per_minute: NonZeroU32,
    /// The calls a minute the client keeps within on the CME key: CME
    /// Group Lite's, the fewest of its plans, unless the key's plan allows
    /// more. Each key counts its own calls.
    #[builder(default = EXCHANGE_LITE_CALLS_PER_MINUTE)]
    pub cme_calls_per_minute: NonZeroU32,
    /// The calls a minute the client keeps within on the Nasdaq key:
    /// Nasdaq Lite's, the fewest of its plans, unless the key's plan allows
    /// more.
    #[builder(default = EXCHANGE_LITE_CALLS_PER_MINUTE)]
    pub nasdaq_calls_per_minute: NonZeroU32,
    /// The instruments the client streams, on a live node: `connect` opens
    /// the socket each one's market streams on, authenticates the key, and
    /// fails unless each socket lists its instruments and allows them all
    /// on its one connection; for `GLBX` contracts, LSE's CME stream, with
    /// the CME key, subscribing them all, and fails with LSE's refusal of
    /// the subscription; and for `XNAS` stocks, LSE's Nasdaq TotalView
    /// stream, with the Nasdaq key, alike. None on a fill's node, which
    /// opens no socket.
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

    /// The address of the stream of `exchange`'s market: the override, or
    /// LSE's own.
    pub(crate) fn exchange_stream_url(&self, exchange: LseExchange) -> &str {
        let url = match exchange {
            LseExchange::Cme => &self.base_url_ws_cme,
            LseExchange::Nasdaq => &self.base_url_ws_nasdaq,
        };
        url.as_deref().unwrap_or(exchange.stream_url())
    }

    /// The key of `product` the config gives, if it gives one.
    pub(crate) fn api_key_of(&self, product: LseProduct) -> Option<SecretString> {
        match product {
            LseProduct::Global => self.api_key.clone(),
            LseProduct::Cme => self.cme_api_key.clone(),
            LseProduct::Nasdaq => self.nasdaq_api_key.clone(),
        }
    }

    /// The calls a minute of `product`'s key.
    pub(crate) fn calls_per_minute_of(&self, product: LseProduct) -> NonZeroU32 {
        match product {
            LseProduct::Global => self.calls_per_minute,
            LseProduct::Cme => self.cme_calls_per_minute,
            LseProduct::Nasdaq => self.nasdaq_calls_per_minute,
        }
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

    /// LSE's addresses, as its socket documentation gives them ("The CME
    /// Group stream").
    #[test]
    fn each_exchange_s_stream_is_lse_s_own_unless_told_otherwise() {
        let mock = LseDataClientConfig::builder()
            .base_url_ws_nasdaq("ws://127.0.0.1:9/v1/stream".to_string())
            .build();
        let own = LseDataClientConfig::default();

        assert_eq!(
            [LseExchange::Cme, LseExchange::Nasdaq]
                .map(|exchange| own.exchange_stream_url(exchange)),
            [
                "wss://cme.londonstrategicedge.com/v1/stream",
                "wss://nasdaq.londonstrategicedge.com/v1/stream",
            ]
        );
        assert_eq!(
            [LseExchange::Cme, LseExchange::Nasdaq]
                .map(|exchange| mock.exchange_stream_url(exchange)),
            [
                "wss://cme.londonstrategicedge.com/v1/stream",
                "ws://127.0.0.1:9/v1/stream",
            ]
        );
    }

    #[test]
    fn the_free_plan_s_60_calls_a_minute_unless_told_otherwise() {
        assert_eq!(LseDataClientConfig::default().calls_per_minute.get(), 60);
    }

    #[test]
    fn each_exchange_plan_s_lite_200_calls_a_minute_on_its_key_unless_told_otherwise() {
        let config = LseDataClientConfig::default();

        assert_eq!(
            (
                config.cme_calls_per_minute.get(),
                config.nasdaq_calls_per_minute.get()
            ),
            (200, 200)
        );
    }

    #[test]
    fn the_keys_are_not_in_the_config_s_debug() {
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from("lse-test-key-0123456789"))
            .cme_api_key(SecretString::from("lse-cme-test-key-0123456789"))
            .nasdaq_api_key(SecretString::from("lse-nasdaq-test-key-0123456789"))
            .build();

        let debug = format!("{config:?}");
        assert!(!debug.contains("lse-test-key-0123456789"), "{debug}");
        assert!(!debug.contains("lse-cme-test-key-0123456789"), "{debug}");
        assert!(!debug.contains("lse-nasdaq-test-key-0123456789"), "{debug}");
    }
}
