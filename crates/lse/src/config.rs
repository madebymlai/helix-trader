//! The config of the data client.

use std::{any::Any, num::NonZeroU32};

use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::SecretString;
use nautilus_model::identifiers::InstrumentId;

use crate::common::{
    consts::{CME_LITE_CALLS_PER_MINUTE, FREE_CALLS_PER_MINUTE, LSE_HTTP_BASE_URL},
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
    /// LSE's REST API; LSE's own if none.
    pub base_url_http: Option<String>,
    /// The address of every LSE socket; LSE's own of each if none.
    pub base_url_ws: Option<String>,
    /// The calls a minute the client keeps within on the LSE key: the free
    /// plan's, the fewest of any plan, unless the key's plan allows more
    /// (LSE's `/v3/usage`, `calls_per_minute`).
    #[builder(default = FREE_CALLS_PER_MINUTE)]
    pub calls_per_minute: NonZeroU32,
    /// The calls a minute the client keeps within on the CME key: CME
    /// Group Lite's, the fewest of its plans, unless the key's plan allows
    /// more. Each key counts its own calls.
    #[builder(default = CME_LITE_CALLS_PER_MINUTE)]
    pub cme_calls_per_minute: NonZeroU32,
    /// The instruments the client streams, on a live node: `connect` opens
    /// the socket each one's market streams on, authenticates the key, and
    /// fails unless the socket lists each and allows them all on its one
    /// connection. None on a fill's node, which opens no socket.
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

    /// The key of `product` the config gives, if it gives one.
    pub(crate) fn api_key_of(&self, product: LseProduct) -> Option<SecretString> {
        match product {
            LseProduct::Global => self.api_key.clone(),
            LseProduct::Cme => self.cme_api_key.clone(),
        }
    }

    /// The calls a minute of `product`'s key.
    pub(crate) fn calls_per_minute_of(&self, product: LseProduct) -> NonZeroU32 {
        match product {
            LseProduct::Global => self.calls_per_minute,
            LseProduct::Cme => self.cme_calls_per_minute,
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

    #[test]
    fn the_free_plan_s_60_calls_a_minute_unless_told_otherwise() {
        assert_eq!(LseDataClientConfig::default().calls_per_minute.get(), 60);
    }

    #[test]
    fn cme_group_lite_s_200_calls_a_minute_on_the_cme_key_unless_told_otherwise() {
        assert_eq!(
            LseDataClientConfig::default().cme_calls_per_minute.get(),
            200
        );
    }

    #[test]
    fn the_keys_are_not_in_the_config_s_debug() {
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from("lse-test-key-0123456789"))
            .cme_api_key(SecretString::from("lse-cme-test-key-0123456789"))
            .build();

        let debug = format!("{config:?}");
        assert!(!debug.contains("lse-test-key-0123456789"), "{debug}");
        assert!(!debug.contains("lse-cme-test-key-0123456789"), "{debug}");
    }
}
