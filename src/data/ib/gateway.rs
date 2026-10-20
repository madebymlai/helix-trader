//! The IB Gateway this process connects to: Nautilus' Docker one, started
//! or reused, and logged in.
//!
//! Uses `DockerizedIBGateway` (`nautilus_interactive_brokers::gateway`), as
//! Nautilus documents it: started before the clients, which then take its
//! host and port. One container per trading mode, shared by every process
//! of that mode. The login comes from `TWS_USERNAME` and `TWS_PASSWORD`,
//! which the adapter reads from the environment; the Docker socket from
//! `DOCKER_HOST`, else the local default.

use std::{fmt, str::FromStr, time::Duration};

use anyhow::{bail, ensure};
use nautilus_interactive_brokers::{
    config::{DockerizedIBGatewayConfig, TradingMode},
    gateway::{ContainerStatus, DockerizedIBGateway},
};
use tokio::time::{Instant, sleep};

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
pub(crate) struct IbGateway {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) client_id: i32,
}

impl IbGateway {
    /// The gateway of `account`'s trading mode, for `client_id`.
    ///
    /// # Errors
    ///
    /// See `start`.
    pub(crate) async fn of_account(account: &IbAccount, client_id: i32) -> anyhow::Result<Self> {
        Self::start(account.trading_mode(), client_id).await
    }

    /// The paper gateway, for `client_id`: history needs no live login.
    ///
    /// # Errors
    ///
    /// See `start`.
    pub(crate) async fn paper(client_id: i32) -> anyhow::Result<Self> {
        Self::start(TradingMode::Paper, client_id).await
    }

    /// Starts the Docker gateway of `trading_mode`, or reuses its running
    /// container, and waits for its login.
    ///
    /// `start` waits for the login of a container it creates, but returns at
    /// once for one already starting: the wait here covers both. The adapter
    /// reports `Ready` on a login logged since the container's last start, so
    /// a container restarted by its restart policy is not taken for logged in
    /// on an earlier session's login.
    ///
    /// # Errors
    ///
    /// Returns an error if the login credentials are not in the environment,
    /// if Docker is unreachable, if the container cannot be created or
    /// started, or if it is not logged in within the adapter's start timeout
    /// (on a live account, the 2FA must be approved in IBKR Mobile by then).
    async fn start(trading_mode: TradingMode, client_id: i32) -> anyhow::Result<Self> {
        let config = gateway_config(trading_mode);
        let timeout = Duration::from_secs(config.timeout);
        let mut gateway = DockerizedIBGateway::new(config)?;
        log::info!(
            "Starting or reusing the IB Gateway container {}",
            gateway.container_name()
        );
        gateway.safe_start(None).await?;
        await_login(
            async || gateway.container_status().await,
            timeout,
            LOGIN_POLL,
        )
        .await?;
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

/// How often the login wait asks for the container's status, as the
/// adapter's own wait does.
const LOGIN_POLL: Duration = Duration::from_secs(1);

/// Asks `status` every `poll` until it reports `Ready`, for at most
/// `timeout`. Any status but `Ready` and `NoContainer` may still turn
/// `Ready`: a container restarting by its policy passes through stopped and
/// created.
///
/// # Errors
///
/// Returns an error if `status` fails or reports no container, or if it is
/// not `Ready` within `timeout`, naming the last status.
async fn await_login(
    mut status: impl AsyncFnMut() -> anyhow::Result<ContainerStatus>,
    timeout: Duration,
    poll: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match status().await? {
            ContainerStatus::Ready => return Ok(()),
            ContainerStatus::NoContainer => bail!("the IB Gateway container is gone"),
            last if Instant::now() >= deadline => {
                bail!("the IB Gateway did not log in within {timeout:?}; last status {last:?}")
            }
            _ => sleep(poll).await,
        }
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

    /// Waits for the login on `statuses`, answered in turn, the last one
    /// forever; returns the result and how many statuses were asked for.
    async fn wait_on(statuses: &[ContainerStatus]) -> (anyhow::Result<()>, usize) {
        let mut asked = 0;
        let result = await_login(
            async || {
                asked += 1;
                Ok(statuses[(asked - 1).min(statuses.len() - 1)])
            },
            Duration::from_secs(300),
            LOGIN_POLL,
        )
        .await;
        (result, asked)
    }

    #[tokio::test(start_paused = true)]
    async fn a_ready_container_needs_no_wait() {
        let (result, asked) = wait_on(&[ContainerStatus::Ready]).await;
        result.unwrap();
        assert_eq!(asked, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_a_starting_container_to_log_in() {
        use ContainerStatus::{ContainerStarting, ContainerStopped, Ready};
        let statuses = [
            ContainerStarting,
            ContainerStopped,
            ContainerStarting,
            Ready,
        ];
        let start = Instant::now();
        let (result, asked) = wait_on(&statuses).await;
        result.unwrap();
        assert_eq!(asked, 4);
        assert_eq!(start.elapsed(), 3 * LOGIN_POLL);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_at_the_deadline_naming_the_last_status() {
        let start = Instant::now();
        let (result, asked) = wait_on(&[ContainerStatus::ContainerStarting]).await;
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("300s") && err.contains("ContainerStarting"),
            "{err}"
        );
        assert_eq!(start.elapsed(), Duration::from_secs(300));
        assert_eq!(asked, 301);
    }

    #[tokio::test(start_paused = true)]
    async fn a_vanished_container_fails_at_once() {
        let (result, asked) = wait_on(&[ContainerStatus::NoContainer]).await;
        assert!(result.unwrap_err().to_string().contains("gone"));
        assert_eq!(asked, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_status_fails_the_wait() {
        let result = await_login(
            async || anyhow::bail!("Docker unreachable"),
            Duration::from_secs(300),
            LOGIN_POLL,
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("Docker"));
    }
}
