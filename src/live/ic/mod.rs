//! IC Markets clients for the live node.
//!
//! Provides the data client (`data`, its live quotes in `spots`), the
//! execution client (`execution`, its reconciliation reports in `reports`)
//! and their factories and configs, on the cTrader session of `data/ic`
//! (`IcHistory`), on the environment `IC_ENVIRONMENT` names, `demo` or
//! `live`, required, with the credential from `IC_*`, both read before
//! anything connects. The data
//! client loads the run's instruments at connect; the execution client
//! checks the account (hedging, the configured one), publishes its state,
//! reconciles it, and trades (`orders` the requests, `dispatch` the trade
//! events).

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
    data::BarType,
    enums::{AccountType, OmsType},
    identifiers::{ClientId, InstrumentId, TraderId, Venue},
};

use self::{data::IcDataClient, execution::IcExecutionClient};
use super::node::Clients;
use crate::{
    data::{IcConnection, IcHistory, ic::quotes_of},
    strategy::catalog_series,
};

mod data;
mod dispatch;
mod execution;
mod orders;
mod reports;
mod spots;

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

/// The client configs of the IC instruments `bar_types` are built from,
/// their quotes (IC serves nothing else), on the connection `var` reads
/// (`IcConnection::from_vars`), before anything connects.
///
/// # Errors
///
/// Returns an error if a bar type is not built from the quotes of an IC
/// instrument (see `strategy::catalog_series`), if `IC_ENVIRONMENT` is not
/// `demo` or `live`, or if a credential variable is missing or invalid.
pub(super) fn clients(
    bar_types: &[BarType],
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<IcClients> {
    let instrument_ids = catalog_series(bar_types)
        .iter()
        .map(|(data_type, identifier)| quotes_of(data_type, identifier))
        .collect::<anyhow::Result<Vec<InstrumentId>>>()?;
    let connection = IcConnection::from_vars(var)?;
    Ok(IcClients {
        data: IcDataClientConfig {
            connection: connection.clone(),
            instrument_ids: instrument_ids.clone(),
        },
        exec: IcExecutionClientConfig {
            connection,
            instrument_ids,
        },
    })
}

impl IcClients {
    /// Checks the account the clients trade, on a session of its own
    /// closed before the node connects: hedging and `IC_ACCOUNT_ID`'s (see
    /// `IcHistory::check_account`). Run before the node is built, so a
    /// wrong account fails the start with its reason.
    ///
    /// # Errors
    ///
    /// Returns an error if cTrader cannot be reached or refuses the
    /// credential, or if the account does not suit (see
    /// `IcHistory::check_account`).
    pub(super) async fn check_account(&self) -> anyhow::Result<()> {
        let ic = IcHistory::connect_as(&self.data.connection).await?;
        let checked = ic.check_account().await;
        ic.disconnect().await;
        checked
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

    /// Gold's hourly bars, built from its quotes.
    fn gold_bars() -> BarType {
        BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")
    }

    #[test]
    fn a_missing_or_unknown_environment_is_refused_before_anything_connects() {
        for environment in [None, Some("paper")] {
            let mut pairs = CREDENTIAL.to_vec();
            pairs.extend(environment.map(|e| ("IC_ENVIRONMENT", e)));

            let err = clients(&[gold_bars()], vars(&pairs)).unwrap_err();

            assert!(err.to_string().contains("IC_ENVIRONMENT"), "{err}");
        }
    }

    #[test]
    fn a_missing_credential_variable_is_refused_before_anything_connects() {
        let err = clients(&[gold_bars()], vars(&[("IC_ENVIRONMENT", "demo")])).unwrap_err();

        assert!(err.to_string().contains("IC_CLIENT_ID"), "{err}");
    }

    #[test]
    fn an_instrument_not_on_icmarkets_is_refused() {
        let bars = BarType::from("EUR/USD.IDEALPRO-1-HOUR-MID-INTERNAL");

        let err = clients(&[bars], vars(&demo())).unwrap_err();

        assert!(err.to_string().contains("EUR/USD.IDEALPRO"), "{err}");
    }

    #[test]
    fn bars_not_built_from_quotes_are_refused_before_anything_connects() {
        for bars in [
            "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL",
            "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL",
        ] {
            let err = clients(&[BarType::from(bars)], vars(&demo())).unwrap_err();

            assert!(err.to_string().contains("IC serves quotes only"), "{err}");
        }
    }

    #[test]
    fn the_clients_load_the_instruments_of_the_bars() {
        let bars = [
            gold_bars(),
            BarType::from("XAUUSD.ICMARKETS-1-DAY-MID-INTERNAL"),
            BarType::from("EURUSD.ICMARKETS-1-HOUR-BID-INTERNAL"),
        ];

        let clients = clients(&bars, vars(&demo())).unwrap();

        let ids = [
            InstrumentId::from("EURUSD.ICMARKETS"),
            InstrumentId::from("XAUUSD.ICMARKETS"),
        ];
        assert_eq!(clients.data.instrument_ids, ids);
        assert_eq!(clients.exec.instrument_ids, ids);
    }

    #[test]
    fn the_factory_creates_an_unconnected_data_client_on_icmarkets() {
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let config = clients(&[gold_bars()], vars(&demo())).unwrap().data;
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
        let config = clients(&[gold_bars()], vars(&demo())).unwrap().exec;
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
        let clients = clients(&[gold_bars()], vars(&demo())).unwrap();

        let (factory, _) = clients.into_node_clients().exec.unwrap();

        assert_eq!(factory.name(), "ICMARKETS");
    }
}
