//! Defines the config of the data client.

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

/// Configures the data client.
///
/// Its `Debug` shows no key.
#[derive(Debug, Clone, bon::Builder)]
pub struct LseDataClientConfig {
    /// The LSE key. If `None`, the client reads `LSE_API_KEY` when it is created.
    pub api_key: Option<SecretString>,
    /// The CME Globex key. If `None`, the client reads `LSE_CME_API_KEY` when it is created.
    pub cme_api_key: Option<SecretString>,
    /// The Nasdaq TotalView key. If `None`, the client reads `LSE_NASDAQ_API_KEY` when it is
    /// created.
    pub nasdaq_api_key: Option<SecretString>,
    /// The REST API address. `None` uses LSE's own.
    pub base_url_http: Option<String>,
    /// The data-ws address. `None` uses LSE's own.
    pub base_url_ws_data: Option<String>,
    /// The london-ws address. `None` uses LSE's own.
    pub base_url_ws_london: Option<String>,
    /// The tsx-ws address. `None` uses LSE's own.
    pub base_url_ws_tsx: Option<String>,
    /// The CME Globex stream address. `None` uses LSE's own.
    pub base_url_ws_cme: Option<String>,
    /// The Nasdaq TotalView stream address. `None` uses LSE's own.
    pub base_url_ws_nasdaq: Option<String>,
    /// The calls per minute allowed on the LSE key.
    ///
    /// The default is the free plan's limit, the fewest of any plan (LSE's `/v3/usage`,
    /// `calls_per_minute`).
    #[builder(default = FREE_CALLS_PER_MINUTE)]
    pub calls_per_minute: NonZeroU32,
    /// The calls per minute allowed on the CME key.
    ///
    /// The default is CME Group Lite's limit, the fewest of its plans. Each key counts its own
    /// calls.
    #[builder(default = EXCHANGE_LITE_CALLS_PER_MINUTE)]
    pub cme_calls_per_minute: NonZeroU32,
    /// The calls per minute allowed on the Nasdaq key.
    ///
    /// The default is Nasdaq Lite's limit, the fewest of its plans.
    #[builder(default = EXCHANGE_LITE_CALLS_PER_MINUTE)]
    pub nasdaq_calls_per_minute: NonZeroU32,
    /// The instruments streamed on a live node. Empty on a fill's node, which opens no socket.
    ///
    /// `connect` opens the socket for each instrument's market and authenticates the key. It fails
    /// unless each socket lists its instruments and allows them all on its one connection.
    ///
    /// An `OPRA` contract subscribes to its underlying's whole chain with one subscription. Every
    /// contract in the chain is streamed, whether named here or not.
    ///
    /// `GLBX` contracts are subscribed on LSE's CME stream with the CME key. `XNAS` stocks are
    /// subscribed on LSE's Nasdaq TotalView stream with the Nasdaq key. If LSE refuses a
    /// subscription, `connect` fails with that refusal.
    #[builder(default)]
    pub stream_ids: Vec<InstrumentId>,
}

impl Default for LseDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl LseDataClientConfig {
    /// Returns the REST API address: the override if set, else LSE's own.
    #[must_use]
    pub fn http_base_url(&self) -> &str {
        self.base_url_http.as_deref().unwrap_or(LSE_HTTP_BASE_URL)
    }

    pub(crate) fn ws_url(&self, socket: LseSocket) -> &str {
        let url = match socket {
            LseSocket::Data => &self.base_url_ws_data,
            LseSocket::London => &self.base_url_ws_london,
            LseSocket::Tsx => &self.base_url_ws_tsx,
        };
        url.as_deref().unwrap_or(socket.url())
    }

    pub(crate) fn exchange_stream_url(&self, exchange: LseExchange) -> &str {
        let url = match exchange {
            LseExchange::Cme => &self.base_url_ws_cme,
            LseExchange::Nasdaq => &self.base_url_ws_nasdaq,
        };
        url.as_deref().unwrap_or(exchange.stream_url())
    }

    pub(crate) fn api_key_of(&self, product: LseProduct) -> Option<SecretString> {
        match product {
            LseProduct::Global => self.api_key.clone(),
            LseProduct::Cme => self.cme_api_key.clone(),
            LseProduct::Nasdaq => self.nasdaq_api_key.clone(),
        }
    }

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
    use rstest::rstest;

    use super::*;

    #[rstest]
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

    /// LSE's socket documentation, "The sockets".
    #[rstest]
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

    /// LSE's socket documentation, "The CME Group stream".
    #[rstest]
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

    #[rstest]
    fn the_free_plan_s_60_calls_a_minute_unless_told_otherwise() {
        assert_eq!(LseDataClientConfig::default().calls_per_minute.get(), 60);
    }

    #[rstest]
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

    #[rstest]
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
