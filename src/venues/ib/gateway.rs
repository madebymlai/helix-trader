//! The IB Gateway this process connects to: Nautilus' Docker one, started or reused, and logged in.
//!
//! Uses `DockerizedIBGateway` (`nautilus_interactive_brokers::gateway`), started before the
//! clients, which take its host and port. One container per trading mode, shared by every process
//! of that mode. The adapter reads the login from `TWS_USERNAME` and `TWS_PASSWORD`; the Docker
//! socket comes from `DOCKER_HOST`, else the local default.

use std::{fmt::Display, str::FromStr};

use nautilus_interactive_brokers::{
    config::{DockerizedIBGatewayConfig, TradingMode},
    gateway::DockerizedIBGateway,
};

/// An IB account id without the client name: `DU1234567` (paper) or `U1234567` (live).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IbAccount(String);

impl IbAccount {
    /// Paper if the id starts with `D`, as IB's paper ids do.
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

    // The bare id: the execution factory prefixes the client name, and orders carry what follows
    // the first `-`.
    fn from_str(s: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric()),
            "{s:?} is not an IB account id like DU1234567"
        );
        Ok(Self(s.to_string()))
    }
}

impl Display for IbAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone)]
pub(super) struct IbGateway {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) client_id: i32,
}

impl IbGateway {
    /// # Errors
    ///
    /// Returns an error if `start` fails.
    pub(super) async fn of_account(account: &IbAccount, client_id: i32) -> anyhow::Result<Self> {
        Self::start(account.trading_mode(), client_id).await
    }

    /// The paper gateway: history needs no live login.
    ///
    /// # Errors
    ///
    /// Returns an error if `start` fails.
    pub(super) async fn paper(client_id: i32) -> anyhow::Result<Self> {
        Self::start(TradingMode::Paper, client_id).await
    }

    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The login credentials are not in the environment.
    /// - Docker is unreachable.
    /// - The container cannot be created or started.
    /// - It is not logged in within the adapter's start timeout (on a live account, the 2FA must be
    ///   approved in IBKR Mobile by then).
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

/// The adapter defaults, with the API open to orders, since a paper container started for history
/// also serves a paper node.
fn gateway_config(trading_mode: TradingMode) -> DockerizedIBGatewayConfig {
    DockerizedIBGatewayConfig {
        trading_mode,
        read_only_api: false,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::an_individual_paper_account("DU1234567", TradingMode::Paper)]
    #[case::an_advisor_s_paper_account("DF1234567", TradingMode::Paper)]
    #[case::a_live_account("U1234567", TradingMode::Live)]
    fn a_d_account_is_paper_and_any_other_live(#[case] account: &str, #[case] mode: TradingMode) {
        assert_eq!(account.parse::<IbAccount>().unwrap().trading_mode(), mode);
    }

    #[rstest]
    #[case::empty("")]
    #[case::prefixed_with_the_client_name("IB-DU1234567")]
    #[case::with_a_space("DU 123")]
    fn rejects_a_malformed_account_naming_it(#[case] account: &str) {
        let err = account.parse::<IbAccount>().unwrap_err().to_string();
        assert!(err.contains(&format!("{account:?}")), "{err}");
    }

    /// Building the gateway needs a Docker socket, so this checks the config's mode.
    #[rstest]
    fn the_container_of_an_account_has_its_trading_mode() {
        let mode = |account: &str| {
            let account: IbAccount = account.parse().unwrap();
            gateway_config(account.trading_mode()).trading_mode
        };
        assert_eq!(mode("DU1234567"), TradingMode::Paper);
        assert_eq!(mode("U1234567"), TradingMode::Live);
    }

    #[rstest]
    fn every_container_accepts_orders() {
        assert!(!gateway_config(TradingMode::Paper).read_only_api);
        assert!(!gateway_config(TradingMode::Live).read_only_api);
    }
}
