//! The factory the node creates the data client by, named `LSE`.

use std::{cell::RefCell, rc::Rc};

use anyhow::Context;
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{common::consts::LSE_CLIENT, config::LseDataClientConfig, data::LseDataClient};

/// Creates `LseDataClient`s from an `LseDataClientConfig`.
#[derive(Debug, Default)]
pub struct LseDataClientFactory;

impl LseDataClientFactory {
    /// Returns the factory.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl DataClientFactory for LseDataClientFactory {
    /// Creates the client named `name`, reading each key not in `config` from its variable.
    ///
    /// # Errors
    ///
    /// Returns an error if `config` is not an `LseDataClientConfig`, if no key is given or in
    /// `LSE_API_KEY`, `LSE_CME_API_KEY` or `LSE_NASDAQ_API_KEY`, or if an HTTP client cannot be
    /// built.
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<LseDataClientConfig>()
            .with_context(|| {
                format!("LseDataClientFactory takes an LseDataClientConfig, not {config:?}")
            })?;
        Ok(Box::new(LseDataClient::new(
            ClientId::from(name),
            config,
            cache,
            clock,
            |name| std::env::var(name).ok(),
        )?))
    }

    fn name(&self) -> &str {
        LSE_CLIENT
    }

    fn config_type(&self) -> &'static str {
        "LseDataClientConfig"
    }
}
