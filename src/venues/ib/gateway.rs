//! The IB Gateway this process connects to: Nautilus' Docker one, started
//! or reused, and logged in.
//!
//! Uses `DockerizedIBGateway` (`nautilus_interactive_brokers::gateway`), as
//! Nautilus documents it: started before the clients, which then take its
//! host and port. One container per trading mode, shared by every process
//! of that mode. The login comes from `TWS_USERNAME` and `TWS_PASSWORD`,
//! which the adapter reads from the environment; the Docker socket from
//! `DOCKER_HOST`, else the local default.

use std::{fmt, str::FromStr};

use anyhow::ensure;
use nautilus_interactive_brokers::{
    config::{DockerizedIBGatewayConfig, TradingMode},
    gateway::DockerizedIBGateway,
};

/// An IB account id as IB shows it, without the client name: `DU1234567`
/// on a paper account, `U1234567` on a live one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IbAccount(String);

impl IbAccount {
    /// The trading mode of the account: paper if its id starts with `D`,
    /// as IB's paper ids do.
    fn trading_mode(&self) -> TradingMode {
        if self.0.starts_with('D') {
            TradingMode::Paper
        } else {
            TradingMode::Live
        }
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

/// A logged-in IB Gateway and the client id this process connects with.
#[derive(Debug, Clone)]
pub(super) struct IbGateway {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) client_id: i32,
}

impl IbGateway {
    /// The gateway of `account`'s trading mode, for `client_id`.
    ///
    /// # Errors
    ///
    /// See `start`.
    pub(super) async fn of_account(account: &IbAccount, client_id: i32) -> anyhow::Result<Self> {
        Self::start(account.trading_mode(), client_id).await
    }

    /// The paper gateway, for `client_id`: history needs no live login.
    ///
    /// # Errors
    ///
    /// See `start`.
    pub(super) async fn paper(client_id: i32) -> anyhow::Result<Self> {
        Self::start(TradingMode::Paper, client_id).await
    }

    /// Starts the Docker gateway of `trading_mode`, or reuses its container,
    /// and waits for its login: the adapter's `safe_start`, within its start
    /// timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the login credentials are not in the environment,
    /// if Docker is unreachable, if the container cannot be created or
    /// started, or if it is not logged in within the adapter's start timeout
    /// (on a live account, the 2FA must be approved in IBKR Mobile by then).
    async fn start(trading_mode: TradingMode, client_id: i32) -> anyhow::Result<Self> {
        let mut gateway = DockerizedIBGateway::new(gateway_config(trading_mode))?;
        log::info!(
            "Starting or reusing the IB Gateway container {}",
            gateway.container_name()
        );
        gateway.safe_start(None).await?;
        log::info!(
            "IB Gateway {} logged in on {}:{}",
            gateway.container_name(),
            gateway.host(),
            gateway.port()
        );
        Ok(Self {
            host: gateway.host().to_string(),
            port: gateway.port(),
            client_id,
        })
    }
}

/// The container config of `trading_mode`: the adapter defaults (the
/// credentials from the environment, its image and start timeout), with the
/// API open to orders, since a paper container started for history also
/// serves a paper node.
fn gateway_config(trading_mode: TradingMode) -> DockerizedIBGatewayConfig {
    DockerizedIBGatewayConfig {
        trading_mode,
        read_only_api: false,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_d_account_is_paper_and_any_other_live() {
        let mode = |account: &str| account.parse::<IbAccount>().unwrap().trading_mode();
        assert_eq!(mode("DU1234567"), TradingMode::Paper);
        assert_eq!(mode("DF1234567"), TradingMode::Paper);
        assert_eq!(mode("U1234567"), TradingMode::Live);
    }

    #[test]
    fn rejects_a_malformed_account() {
        for account in ["", "IB-DU1234567", "DU 123"] {
            assert!(account.parse::<IbAccount>().is_err(), "{account:?}");
        }
    }

    /// The port is the adapter's, by trading mode (its own tests cover
    /// it): building its gateway needs a Docker socket, so this checks the
    /// mode the config of each account asks for.
    #[test]
    fn the_container_of_an_account_has_its_trading_mode() {
        let mode = |account: &str| {
            let account: IbAccount = account.parse().unwrap();
            gateway_config(account.trading_mode()).trading_mode
        };
        assert_eq!(mode("DU1234567"), TradingMode::Paper);
        assert_eq!(mode("U1234567"), TradingMode::Live);
    }

    #[test]
    fn every_container_accepts_orders() {
        assert!(!gateway_config(TradingMode::Paper).read_only_api);
        assert!(!gateway_config(TradingMode::Live).read_only_api);
    }
}
