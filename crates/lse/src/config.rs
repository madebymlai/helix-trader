//! The config of the data client.

use std::{any::Any, num::NonZeroU32};

use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::SecretString;

use crate::common::consts::{FREE_CALLS_PER_MINUTE, LSE_HTTP_BASE_URL};

/// The config of the data client: the key, where LSE is, and the calls a
/// minute of the key's plan. Its `Debug` shows no key.
#[derive(Debug, Clone, bon::Builder)]
pub struct LseDataClientConfig {
    /// The LSE key; the one in `LSE_API_KEY` if none, read when the client
    /// is created.
    pub api_key: Option<SecretString>,
    /// LSE's REST API; LSE's own if none.
    pub base_url_http: Option<String>,
    /// The calls a minute the client keeps within: the free plan's, the
    /// fewest of any plan, unless the key's plan allows more (LSE's
    /// `/v3/usage`, `calls_per_minute`).
    #[builder(default = FREE_CALLS_PER_MINUTE)]
    pub calls_per_minute: NonZeroU32,
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
    fn the_key_is_not_in_the_config_s_debug() {
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from("lse-test-key-0123456789"))
            .build();

        assert!(!format!("{config:?}").contains("lse-test-key-0123456789"));
    }
}
