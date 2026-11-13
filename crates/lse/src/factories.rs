//! The factory the node creates the data client by, from its config (see
//! `config`), named `LSE`.

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
    /// The factory, named `LSE`: the node creates the client under that
    /// name, its client id.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl DataClientFactory for LseDataClientFactory {
    /// The client named `name`, on its config's keys and API, each key not
    /// given read from its variable, reading the instruments it converts
    /// data of from `cache`.
    ///
    /// # Errors
    ///
    /// Returns an error if `config` is not an `LseDataClientConfig`, if no
    /// key is given or in `LSE_API_KEY` or `LSE_CME_API_KEY`, or if an HTTP
    /// client cannot be built.
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
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
            |name| std::env::var(name).ok(),
        )?))
    }

    fn name(&self) -> &str {
        LSE_CLIENT
    }

    fn config_type(&self) -> &str {
        "LseDataClientConfig"
    }
}
