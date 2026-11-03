//! The factories the node creates IC's clients by, each from its config
//! (see `config`): the data client (`data`) and the execution client
//! (`execution`), both named `ICMARKETS`.

use std::{cell::RefCell, rc::Rc};

use anyhow::Context;
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId, Venue},
};

use super::{
    config::{IcDataClientConfig, IcExecutionClientConfig},
    data::IcDataClient,
    execution::IcExecutionClient,
};

/// The name of IC's clients, and so their `ClientId`.
pub(crate) const NAME: &str = "ICMARKETS";

/// Creates `IcDataClient`s from an `IcDataClientConfig`.
#[derive(Debug, Default)]
pub struct IcDataClientFactory;

impl DataClientFactory for IcDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<IcDataClientConfig>()
            .with_context(|| {
                format!("IcDataClientFactory takes an IcDataClientConfig, not {config:?}")
            })?;
        Ok(Box::new(IcDataClient::new(
            ClientId::from(name),
            &config.connection,
            config.instrument_ids.clone(),
        )))
    }

    fn name(&self) -> &str {
        NAME
    }

    fn config_type(&self) -> &str {
        "IcDataClientConfig"
    }
}

/// Creates `IcExecutionClient`s from an `IcExecutionClientConfig`: a
/// hedging venue (each cTrader position one Nautilus position), a margin
/// account named after the environment (`IcConnection::account_id`).
#[derive(Debug, Default)]
pub struct IcExecutionClientFactory;

impl ExecutionClientFactory for IcExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<IcExecutionClientConfig>()
            .with_context(|| {
                format!("IcExecutionClientFactory takes an IcExecutionClientConfig, not {config:?}")
            })?;
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            Venue::new(NAME),
            OmsType::Hedging,
            config.connection.account_id(),
            AccountType::Margin,
            None,
            cache,
        );
        Ok(Box::new(IcExecutionClient::new(
            core,
            &config.connection,
            config.instrument_ids.clone(),
        )))
    }

    fn name(&self) -> &str {
        NAME
    }

    fn config_type(&self) -> &str {
        "IcExecutionClientConfig"
    }
}
