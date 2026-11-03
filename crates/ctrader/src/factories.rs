//! The factories the node creates the clients by, each from its config
//! (see `config`): the data client (`data`) and the execution client
//! (`execution`), each named after its config's client id.

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
    identifiers::{ClientId, TraderId},
};

use super::{
    config::{CTraderDataClientConfig, CTraderExecutionClientConfig},
    data::CTraderDataClient,
    execution::CTraderExecutionClient,
};

/// Creates `CTraderDataClient`s from a `CTraderDataClientConfig`.
#[derive(Debug)]
pub struct CTraderDataClientFactory {
    name: ClientId,
}

impl CTraderDataClientFactory {
    /// The factory of the client `config` configures, named after its
    /// client id: the node creates the client under that name.
    #[must_use]
    pub fn new(config: &CTraderDataClientConfig) -> Self {
        Self {
            name: config.client_id,
        }
    }
}

impl DataClientFactory for CTraderDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<CTraderDataClientConfig>()
            .with_context(|| {
                format!("CTraderDataClientFactory takes a CTraderDataClientConfig, not {config:?}")
            })?;
        Ok(Box::new(CTraderDataClient::new(
            ClientId::from(name),
            &config.connection,
            config.instrument_ids.clone(),
        )))
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn config_type(&self) -> &str {
        "CTraderDataClientConfig"
    }
}

/// Creates `CTraderExecutionClient`s from a `CTraderExecutionClientConfig`:
/// a hedging venue, the connection's (each cTrader position one Nautilus
/// position), a margin account named after the venue and the environment
/// (`CTraderConnection::account_id`).
#[derive(Debug)]
pub struct CTraderExecutionClientFactory {
    name: ClientId,
}

impl CTraderExecutionClientFactory {
    /// The factory of the client `config` configures, named after its
    /// client id: the node creates the client under that name.
    #[must_use]
    pub fn new(config: &CTraderExecutionClientConfig) -> Self {
        Self {
            name: config.client_id,
        }
    }
}

impl ExecutionClientFactory for CTraderExecutionClientFactory {
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
            .downcast_ref::<CTraderExecutionClientConfig>()
            .with_context(|| {
                format!(
                    "CTraderExecutionClientFactory takes a CTraderExecutionClientConfig, not \
                     {config:?}"
                )
            })?;
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            config.connection.venue,
            OmsType::Hedging,
            config.connection.account_id(),
            AccountType::Margin,
            None,
            cache,
        );
        Ok(Box::new(CTraderExecutionClient::new(
            core,
            &config.connection,
            config.instrument_ids.clone(),
        )))
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn config_type(&self) -> &str {
        "CTraderExecutionClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        live::runner::{replace_data_event_sender, replace_exec_event_sender},
    };
    use nautilus_model::identifiers::{AccountId, InstrumentId, Venue};

    use super::*;
    use crate::testing::{self, CLIENT, VENUE};

    fn gold() -> Vec<InstrumentId> {
        vec![InstrumentId::from("XAUUSD.ACMEFX")]
    }

    /// As the node does: the client created by its factory, under the
    /// factory's name, from its config.
    #[test]
    fn the_data_client_is_named_after_its_config_on_the_venue_of_its_connection() {
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let (connection, _tokens) = testing::connection();
        let config = CTraderDataClientConfig {
            client_id: ClientId::from(CLIENT),
            connection,
            instrument_ids: gold(),
        };
        let cache = Rc::new(RefCell::new(Cache::default()));
        let factory = CTraderDataClientFactory::new(&config);

        let client = factory
            .create(
                factory.name(),
                &config,
                cache.into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap();

        assert_eq!(factory.name(), CLIENT);
        assert_eq!(client.client_id(), ClientId::from(CLIENT));
        assert_eq!(client.venue(), Some(Venue::new(VENUE)));
        assert!(client.is_disconnected());
    }

    /// As the node does, for the execution client: a hedging margin
    /// account named after the venue and the environment.
    #[test]
    fn the_execution_client_is_named_after_its_config_on_a_hedging_account_of_the_venue() {
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let (connection, _tokens) = testing::connection();
        let config = CTraderExecutionClientConfig {
            client_id: ClientId::from(CLIENT),
            connection,
            instrument_ids: gold(),
        };
        let cache = Rc::new(RefCell::new(Cache::default()));
        let factory = CTraderExecutionClientFactory::new(&config);

        let client = factory
            .create(
                TraderId::from("HELIX-001"),
                factory.name(),
                &config,
                cache.into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap();

        assert_eq!(factory.name(), CLIENT);
        assert_eq!(client.client_id(), ClientId::from(CLIENT));
        assert_eq!(client.venue(), Venue::new(VENUE));
        assert_eq!(client.oms_type(), OmsType::Hedging);
        assert_eq!(client.account_id(), AccountId::from("ACMEFX-DEMO"));
        assert!(!client.is_connected());
    }
}
