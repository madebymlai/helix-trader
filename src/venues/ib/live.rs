//! Interactive Brokers clients for the live node, and the live command's
//! IB flags (`IbLiveArgs`).
//!
//! Provides the Nautilus client factories and configs from
//! `nautilus_interactive_brokers`: `InteractiveBrokersDataClientFactory`,
//! `InteractiveBrokersExecutionClientFactory` and their configs, on the
//! gateway of the account's trading mode (`IbGateway`); the account id
//! goes in the execution config.

use anyhow::ensure;
use clap::Args;
use nautilus_interactive_brokers::{
    config::{
        InteractiveBrokersDataClientConfig, InteractiveBrokersExecutionClientConfig,
        InteractiveBrokersInstrumentProviderConfig,
    },
    factories::{InteractiveBrokersDataClientFactory, InteractiveBrokersExecutionClientFactory},
};
use nautilus_live::config::RoutingConfig;
use nautilus_model::identifiers::InstrumentId;

use super::{IbAccount, IbGateway, data_client_config};
use crate::live::node::Clients;

/// IB's settings of a live node, the live command's flags: the account
/// traded and the client id of the node's IB clients (see `clients`).
#[derive(Debug, Clone, Args)]
pub(crate) struct IbLiveArgs {
    /// The IB account traded, for example DU1234567 (paper); needed only
    /// for IB instruments.
    #[arg(long, env = "NAUTILUS_IB_ACCOUNT_ID")]
    pub(in crate::venues) account: Option<IbAccount>,
    /// IB client id of the node, the adapter default.
    #[arg(long, default_value_t = 1)]
    pub(in crate::venues) client_id: i32,
}

/// The IB data and execution client configs of a node trading `account`
/// on `instrument_ids` through `gateway`, both on one connection.
#[derive(Debug, Clone)]
pub(crate) struct IbClients {
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
pub(crate) async fn clients(
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
/// The data client is the history's (`data_client_config`: the full
/// session, the symbology and venues of the catalog's instruments), so its
/// 1-minute bars, requested for the warmup and subscribed to live, are
/// those `fill-catalog` stores. Quotes a strategy subscribes to come tick
/// by tick (`batch_quotes: false`; the adapter falls back to `reqMktData`
/// where IB refuses them). Data and exec share `gateway`'s client id, hence
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
    pub(crate) fn into_node_clients(self) -> Clients {
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
    use crate::venues::{Broker, tests::created_oms_type};

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

    /// Against the adapter's client: the broker's answer is our copy of it.
    /// The client needs no credential and connects to nothing.
    #[test]
    fn the_execution_client_keeps_positions_as_the_broker_answers() {
        let clients = configs(&gateway(), &paper(), &[sxr8()]);

        let created = created_oms_type(clients.into_node_clients());

        assert_eq!(created, Broker::Ib.oms_type());
    }

    #[test]
    fn the_data_client_takes_the_full_session_and_tick_by_tick_quotes() {
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
