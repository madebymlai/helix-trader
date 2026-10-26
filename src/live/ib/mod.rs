//! Interactive Brokers clients for the live node.
//!
//! Provides the Nautilus client factories and configs from
//! `nautilus_interactive_brokers`: `InteractiveBrokersDataClientFactory`,
//! `InteractiveBrokersExecutionClientFactory` and their configs, on the
//! gateway of the account's trading mode (`data::IbGateway`); the account
//! id goes in the execution config. Another broker is another file next
//! to this one (`okx/`).

use anyhow::ensure;
use nautilus_interactive_brokers::{
    config::{
        InteractiveBrokersDataClientConfig, InteractiveBrokersExecutionClientConfig,
        InteractiveBrokersInstrumentProviderConfig,
    },
    factories::{InteractiveBrokersDataClientFactory, InteractiveBrokersExecutionClientFactory},
};
use nautilus_live::config::RoutingConfig;
use nautilus_model::identifiers::InstrumentId;

use super::node::Clients;
use crate::data::{IbAccount, IbGateway, data_client_config};

/// The IB data and execution client configs of a node trading `account`
/// on `instrument_ids` through `gateway`, both on one connection.
#[derive(Debug, Clone)]
pub(super) struct IbClients {
    pub(super) data: InteractiveBrokersDataClientConfig,
    pub(super) exec: InteractiveBrokersExecutionClientConfig,
}

/// Checks `client_id`, then starts or reuses the gateway of `account`'s
/// trading mode and waits for its login (see `IbGateway::of_account`), and
/// returns the client configs on it (see `configs`).
///
/// # Errors
///
/// Returns an error if the client id is a multiple of 1000 (IB reserves
/// those for the execution client), checked before any gateway starts, or
/// if the gateway does not start and log in.
pub(super) async fn clients(
    account: &IbAccount,
    client_id: i32,
    instrument_ids: &[InstrumentId],
) -> anyhow::Result<IbClients> {
    ensure!(
        client_id % 1000 != 0,
        "client id {client_id} is a multiple of 1000, which the IB execution client rejects"
    );
    let gateway = IbGateway::of_account(account, client_id).await?;
    Ok(configs(&gateway, account, instrument_ids))
}

/// The client configs for `account` on `gateway`, loading `instrument_ids`
/// at startup.
///
/// The data client is the history's (`data::data_client_config`: the full
/// session, the symbology and venues of the catalog's instruments), taking
/// quotes tick by tick (`batch_quotes: false`; the adapter falls back to
/// `reqMktData` where IB refuses them), so internal bars are built from
/// every quote change. Data and exec share `gateway`'s client id, hence
/// one connection.
fn configs(gateway: &IbGateway, account: &IbAccount, instrument_ids: &[InstrumentId]) -> IbClients {
    let instrument_provider = InteractiveBrokersInstrumentProviderConfig {
        load_ids: instrument_ids.iter().copied().collect(),
        ..Default::default()
    };
    IbClients {
        data: InteractiveBrokersDataClientConfig {
            batch_quotes: false,
            instrument_provider: instrument_provider.clone(),
            ..data_client_config(gateway.clone())
        },
        exec: InteractiveBrokersExecutionClientConfig {
            host: gateway.host.clone(),
            port: gateway.port,
            client_id: gateway.client_id,
            account_id: Some(account.to_string()),
            instrument_provider,
            ..Default::default()
        },
    }
}

impl IbClients {
    /// The factories and configs, for the node: the default route for
    /// data and orders, since IB spans many venues.
    pub(super) fn into_node_clients(self) -> Clients {
        Clients {
            data_factory: Box::new(InteractiveBrokersDataClientFactory::new()),
            data_config: Box::new(self.data),
            exec: Some((
                Box::new(InteractiveBrokersExecutionClientFactory::new()),
                Box::new(self.exec),
            )),
            routing: RoutingConfig::builder().default(true).build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gateway() -> IbGateway {
        IbGateway {
            host: "127.0.0.1".to_string(),
            port: 4002,
            client_id: 11,
        }
    }

    fn paper() -> IbAccount {
        "DU1234567".parse().unwrap()
    }

    fn sxr8() -> InstrumentId {
        InstrumentId::from("SXR8.IBIS")
    }

    #[test]
    fn the_data_client_streams_tick_by_tick_quotes_over_the_full_session() {
        let data = configs(&gateway(), &paper(), &[sxr8()]).data;
        assert!(!data.batch_quotes);
        assert!(!data.use_regular_trading_hours);
        assert_eq!(
            (data.host.as_str(), data.port, data.client_id),
            ("127.0.0.1", 4002, 11)
        );
        assert_eq!(data.instrument_provider.load_ids, [sxr8()].into());
    }

    #[test]
    fn the_exec_client_trades_the_account_on_the_same_connection() {
        let exec = configs(&gateway(), &paper(), &[sxr8()]).exec;
        assert_eq!(exec.account_id.as_deref(), Some("DU1234567"));
        assert_eq!(
            (exec.host.as_str(), exec.port, exec.client_id),
            ("127.0.0.1", 4002, 11)
        );
        assert_eq!(exec.instrument_provider.load_ids, [sxr8()].into());
    }

    /// Starting the gateway would fail without credentials or Docker, or
    /// start a container: the client id is checked first.
    #[tokio::test]
    async fn rejects_a_client_id_ib_reserves_before_starting_the_gateway() {
        let Err(err) = clients(&paper(), 2000, &[sxr8()]).await else {
            panic!("started");
        };
        assert!(err.to_string().contains("1000"), "{err}");
    }
}
