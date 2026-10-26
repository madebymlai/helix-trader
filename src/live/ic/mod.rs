//! IC Markets clients for the live node.
//!
//! Provides the data and execution client factories and configs of the
//! adapter in `data/ic` (`IcDataClient`, `IcExecutionClient`), on the
//! environment `IC_ENVIRONMENT` names, `demo` or `live`, required, with the
//! credential from `IC_*`, both read before anything connects. The data
//! client loads the run's instruments at connect; the execution client
//! checks the account (hedging, the configured one), publishes its state
//! and reconciles it. It places no order yet: IC orders are denied.

use std::{any::Any, cell::RefCell, rc::Rc};

use anyhow::Context;
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::{ExecutionClientCore, config::RoutingConfig};
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, InstrumentId, TraderId, Venue},
};

use super::node::Clients;
use crate::data::{IcConnection, IcDataClient, IcExecutionClient, IcHistory};

/// The name of IC's clients, and so their `ClientId`.
const NAME: &str = "ICMARKETS";

/// The config of IC's data client: where it connects, as whom, and the
/// instruments it loads.
#[derive(Debug, Clone)]
pub struct IcDataClientConfig {
    pub connection: IcConnection,
    pub instrument_ids: Vec<InstrumentId>,
}

impl ClientConfig for IcDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

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

/// The config of IC's execution client: where it connects, as whom, and
/// the instruments whose reports it reads.
#[derive(Debug, Clone)]
pub struct IcExecutionClientConfig {
    pub connection: IcConnection,
    pub instrument_ids: Vec<InstrumentId>,
}

impl ClientConfig for IcExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
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

/// The IC client configs of a node trading IC instruments.
#[derive(Debug, Clone)]
pub(super) struct IcClients {
    pub(super) data: IcDataClientConfig,
    pub(super) exec: IcExecutionClientConfig,
}

/// The client configs of `instrument_ids` on the connection `var` reads
/// (`IcConnection::from_vars`), before anything connects.
///
/// # Errors
///
/// Returns an error if an id is not IC's, if `IC_ENVIRONMENT` is not
/// `demo` or `live`, or if a credential variable is missing or invalid.
pub(super) fn clients(
    instrument_ids: &[InstrumentId],
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<IcClients> {
    for &id in instrument_ids {
        anyhow::ensure!(IcHistory::serves(id), "{id} is not an IC instrument");
    }
    let connection = IcConnection::from_vars(var)?;
    Ok(IcClients {
        data: IcDataClientConfig {
            connection: connection.clone(),
            instrument_ids: instrument_ids.to_vec(),
        },
        exec: IcExecutionClientConfig {
            connection,
            instrument_ids: instrument_ids.to_vec(),
        },
    })
}

impl IcClients {
    /// Where and as whom the clients connect, for the catalog fill.
    pub(super) fn connection(&self) -> &IcConnection {
        &self.data.connection
    }

    /// The factories and configs, for the node. Both clients are on venue
    /// `ICMARKETS`, so Nautilus routes IC's data and orders to them without
    /// a route of ours.
    pub(super) fn into_node_clients(self) -> Clients {
        Clients {
            data_factory: Box::new(IcDataClientFactory),
            data_config: Box::new(self.data),
            exec: Some((Box::new(IcExecutionClientFactory), Box::new(self.exec))),
            routing: RoutingConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        live::runner::{replace_data_event_sender, replace_exec_event_sender},
    };
    use nautilus_model::identifiers::AccountId;

    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    const CREDENTIAL: [(&str, &str); 6] = [
        ("IC_CLIENT_ID", "12345_clientIdValue"),
        ("IC_CLIENT_SECRET", "clientSecretValue"),
        ("IC_ACCESS_TOKEN", "accessTokenValue"),
        ("IC_REFRESH_TOKEN", "refreshTokenValue"),
        ("IC_ACCOUNT_ID", "1000001"),
        ("HOME", "/home/u"),
    ];

    fn demo() -> Vec<(&'static str, &'static str)> {
        let mut pairs = CREDENTIAL.to_vec();
        pairs.push(("IC_ENVIRONMENT", "demo"));
        pairs
    }

    fn xauusd() -> InstrumentId {
        InstrumentId::from("XAUUSD.ICMARKETS")
    }

    #[test]
    fn a_missing_or_unknown_environment_is_refused_before_anything_connects() {
        for environment in [None, Some("paper")] {
            let mut pairs = CREDENTIAL.to_vec();
            pairs.extend(environment.map(|e| ("IC_ENVIRONMENT", e)));

            let err = clients(&[xauusd()], vars(&pairs)).unwrap_err();

            assert!(err.to_string().contains("IC_ENVIRONMENT"), "{err}");
        }
    }

    #[test]
    fn a_missing_credential_variable_is_refused_before_anything_connects() {
        let err = clients(&[xauusd()], vars(&[("IC_ENVIRONMENT", "demo")])).unwrap_err();

        assert!(err.to_string().contains("IC_CLIENT_ID"), "{err}");
    }

    #[test]
    fn an_instrument_not_on_icmarkets_is_refused() {
        let err = clients(&[InstrumentId::from("EUR/USD.IDEALPRO")], vars(&demo())).unwrap_err();

        assert!(err.to_string().contains("EUR/USD.IDEALPRO"), "{err}");
    }

    #[test]
    fn the_factory_creates_an_unconnected_data_client_on_icmarkets() {
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let config = clients(&[xauusd()], vars(&demo())).unwrap().data;
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));

        let client = IcDataClientFactory
            .create(NAME, &config, cache.into(), clock)
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("ICMARKETS"));
        assert_eq!(client.venue(), Some(Venue::new("ICMARKETS")));
        assert!(client.is_disconnected());
    }

    #[test]
    fn the_factory_creates_an_unconnected_hedging_execution_client_on_icmarkets() {
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let config = clients(&[xauusd()], vars(&demo())).unwrap().exec;
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));

        let client = IcExecutionClientFactory
            .create(
                TraderId::from("HELIX-001"),
                NAME,
                &config,
                cache.into(),
                clock,
            )
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("ICMARKETS"));
        assert_eq!(client.venue(), Venue::new("ICMARKETS"));
        assert_eq!(client.oms_type(), OmsType::Hedging);
        assert_eq!(client.account_id(), AccountId::from("ICMARKETS-DEMO"));
        assert!(!client.is_connected());
    }

    #[test]
    fn the_node_gets_ic_s_execution_client() {
        let clients = clients(&[xauusd()], vars(&demo())).unwrap();

        let (factory, _) = clients.into_node_clients().exec.unwrap();

        assert_eq!(factory.name(), "ICMARKETS");
    }
}
