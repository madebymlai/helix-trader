//! Interactive Brokers clients for the live node.
//!
//! Provides the Nautilus client factories and configs from
//! `nautilus_interactive_brokers`: `InteractiveBrokersDataClientFactory`,
//! `InteractiveBrokersExecutionClientFactory` and their configs. Paper or
//! live is chosen by gateway port; the account id goes in the execution
//! config. Another broker is another file next to this one.

use std::{fmt, str::FromStr};

use anyhow::{bail, ensure};
use nautilus_interactive_brokers::{
    common::consts::{DEFAULT_PORT, DEFAULT_TWS_PORT},
    config::{
        InteractiveBrokersDataClientConfig, InteractiveBrokersExecutionClientConfig,
        InteractiveBrokersInstrumentProviderConfig,
    },
    factories::{InteractiveBrokersDataClientFactory, InteractiveBrokersExecutionClientFactory},
};
use nautilus_model::identifiers::InstrumentId;

use super::node::Clients;
use crate::data::IbGateway;

/// An IB account id as IB shows it, without the client name: `DU1234567`
/// on a paper account, `U1234567` on a live one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IbAccount(String);

impl IbAccount {
    /// Whether the account is a paper one: IB starts their ids with `D`.
    fn is_paper(&self) -> bool {
        self.0.starts_with('D')
    }
}

impl FromStr for IbAccount {
    type Err = anyhow::Error;

    /// Takes the bare id: the execution factory prefixes it with the client
    /// name, and orders carry what follows the first `-`.
    fn from_str(s: &str) -> anyhow::Result<Self> {
        ensure!(
            !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric()),
            "{s:?} is not an IB account id like DU1234567"
        );
        Ok(Self(s.to_string()))
    }
}

impl fmt::Display for IbAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The IB data and execution client configs of a node trading `account`
/// on `instrument_ids` through `gateway`, both on one connection.
#[derive(Debug, Clone)]
pub(super) struct IbClients {
    pub(super) data: InteractiveBrokersDataClientConfig,
    pub(super) exec: InteractiveBrokersExecutionClientConfig,
}

/// The client configs for `account` on `gateway`, loading `instrument_ids`
/// at startup.
///
/// The data client takes quotes tick by tick (`batch_quotes: false`; the
/// adapter falls back to `reqMktData` where IB refuses them), so internal
/// bars are built from every quote change, and the full session, not
/// regular hours, as the history in the catalog (see `data_client_config`
/// in the data module). Data and exec share `gateway`'s client id, hence
/// one connection.
///
/// # Errors
///
/// Returns an error if the gateway's port is a paper one and `account` a
/// live one, or the reverse, or if the client id is a multiple of 1000
/// (IB reserves those for the execution client).
pub(super) fn clients(
    gateway: &IbGateway,
    account: &IbAccount,
    instrument_ids: &[InstrumentId],
) -> anyhow::Result<IbClients> {
    match (gateway.port, account.is_paper()) {
        (DEFAULT_PORT | DEFAULT_TWS_PORT, false) => {
            bail!(
                "port {} is a paper one and {account} not a paper account",
                gateway.port
            )
        }
        (LIVE_GATEWAY | LIVE_TWS, true) => {
            bail!(
                "port {} is a live one and {account} a paper account",
                gateway.port
            )
        }
        _ => {}
    }
    ensure!(
        gateway.client_id % 1000 != 0,
        "client id {} is a multiple of 1000, which the IB execution client rejects",
        gateway.client_id
    );
    let instrument_provider = InteractiveBrokersInstrumentProviderConfig {
        load_ids: instrument_ids.iter().copied().collect(),
        ..Default::default()
    };
    Ok(IbClients {
        data: InteractiveBrokersDataClientConfig {
            host: gateway.host.clone(),
            port: gateway.port,
            client_id: gateway.client_id,
            use_regular_trading_hours: false,
            batch_quotes: false,
            instrument_provider: instrument_provider.clone(),
            ..Default::default()
        },
        exec: InteractiveBrokersExecutionClientConfig {
            host: gateway.host.clone(),
            port: gateway.port,
            client_id: gateway.client_id,
            account_id: Some(account.to_string()),
            instrument_provider,
            ..Default::default()
        },
    })
}

impl IbClients {
    /// The factories and configs, for the node.
    pub(super) fn into_node_clients(self) -> Clients {
        Clients {
            data_factory: Box::new(InteractiveBrokersDataClientFactory::new()),
            data_config: Box::new(self.data),
            exec_factory: Box::new(InteractiveBrokersExecutionClientFactory::new()),
            exec_config: Box::new(self.exec),
        }
    }
}

/// The IB Gateway and TWS ports of live accounts; the adapter names only
/// the paper ones.
const LIVE_GATEWAY: u16 = 4001;
const LIVE_TWS: u16 = 7496;

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;

    use super::*;
    use crate::data::IbGateway;

    fn gateway(port: u16) -> IbGateway {
        IbGateway {
            host: "127.0.0.1".to_string(),
            port,
            client_id: 11,
        }
    }

    fn sxr8() -> InstrumentId {
        InstrumentId::from("SXR8.IBIS")
    }

    #[test]
    fn the_data_client_streams_tick_by_tick_quotes_over_the_full_session() {
        let account: IbAccount = "DU1234567".parse().unwrap();
        let clients = clients(&gateway(4002), &account, &[sxr8()]).unwrap();
        let data = clients.data;
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
        let account: IbAccount = "DU1234567".parse().unwrap();
        let exec = clients(&gateway(4002), &account, &[sxr8()]).unwrap().exec;
        assert_eq!(exec.account_id.as_deref(), Some("DU1234567"));
        assert_eq!(
            (exec.host.as_str(), exec.port, exec.client_id),
            ("127.0.0.1", 4002, 11)
        );
        assert_eq!(exec.instrument_provider.load_ids, [sxr8()].into());
    }

    #[test]
    fn rejects_a_paper_port_with_a_live_account_and_the_reverse() {
        let live: IbAccount = "U1234567".parse().unwrap();
        let paper: IbAccount = "DU1234567".parse().unwrap();
        for port in [4002, 7497] {
            let err = clients(&gateway(port), &live, &[sxr8()]).unwrap_err();
            assert!(err.to_string().contains("paper"), "{err}");
        }
        for port in [4001, 7496] {
            let err = clients(&gateway(port), &paper, &[sxr8()]).unwrap_err();
            assert!(err.to_string().contains("live"), "{err}");
        }
        assert!(clients(&gateway(4001), &live, &[sxr8()]).is_ok());
    }

    #[test]
    fn rejects_a_malformed_account() {
        for account in ["", "IB-DU1234567", "DU 123"] {
            assert!(account.parse::<IbAccount>().is_err(), "{account:?}");
        }
    }

    #[test]
    fn rejects_a_client_id_ib_reserves() {
        let account: IbAccount = "DU1234567".parse().unwrap();
        let mut gateway = gateway(4002);
        gateway.client_id = 2000;
        let err = clients(&gateway, &account, &[sxr8()]).unwrap_err();
        assert!(err.to_string().contains("1000"), "{err}");
    }
}
