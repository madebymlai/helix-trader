//! What connects to cTrader: the broker's venue, the environment and the
//! credential; and the configs of the data and execution clients, which
//! connect with them. Typed: the caller reads them from wherever it keeps
//! them, and names those settings (`CTraderSettingNames`) for the errors
//! that ask to check or replace one.
//!
//! The app's client id and secret come from Spotware's Open API portal;
//! the first token pair (access and refresh token), and the account it
//! grants, from the portal's playground; the pair seeds the token file,
//! which keeps the latest one (see `token`). The secret and the tokens are
//! secret material, the client id and the account id credential identity:
//! none of them appears in `Debug`, errors or logs.

use std::{any::Any, fmt, path::PathBuf};

use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::{REDACTED, SecretString};
use nautilus_model::identifiers::{AccountId, ClientId, InstrumentId, Venue};

use super::common::token::{TokenPair, TokenStore};

/// cTrader's demo or live servers, fully separate: an account
/// authenticates only on its own environment's server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CTraderEnvironment {
    Demo,
    Live,
}

impl CTraderEnvironment {
    /// The protobuf endpoint (port 5035) over WebSocket with TLS
    /// (help.ctrader.com/open-api, "Proxies and endpoints").
    pub(crate) fn url(self) -> &'static str {
        match self {
            Self::Demo => "wss://demo.ctraderapi.com:5035",
            Self::Live => "wss://live.ctraderapi.com:5035",
        }
    }
}

/// The trader's account on cTrader (`ctidTraderAccountId`), which
/// requests name; credential identity, so redacted in `Debug`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CTraderAccountId(i64);

impl CTraderAccountId {
    #[must_use]
    pub const fn new(id: i64) -> Self {
        Self(id)
    }

    /// The id, for the requests that carry it.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Debug for CTraderAccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// What the errors asking to check or replace a setting call it: the
/// names the caller read the environment and the credential under (its
/// environment variables, say), where it read them from, and where setting
/// them up is explained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CTraderSettingNames {
    pub environment: &'static str,
    pub client_id: &'static str,
    pub client_secret: &'static str,
    pub access_token: &'static str,
    pub refresh_token: &'static str,
    pub account_id: &'static str,
    /// Where the settings are set, such as a file of them.
    pub source: &'static str,
    /// Where setting up the credential is explained.
    pub guide: &'static str,
}

/// The app's client id and secret, the account, and the token file with
/// the pair that seeds it.
#[derive(Debug, Clone)]
pub struct CTraderCredential {
    pub client_id: SecretString,
    pub client_secret: SecretString,
    pub account_id: CTraderAccountId,
    /// The pair that seeds the token file: the playground's, from which
    /// the file takes over, until another pair seeds it again (see
    /// `token`).
    pub access_token: SecretString,
    pub refresh_token: SecretString,
    /// The token file, which keeps the latest pair for every session of
    /// the credential; nothing is read from it until a session connects.
    pub token_file: PathBuf,
    pub names: CTraderSettingNames,
}

impl CTraderCredential {
    /// The token file, seeded with the credential's pair.
    pub(crate) fn tokens(&self) -> TokenStore {
        let seed = TokenPair {
            access_token: self.access_token.clone(),
            refresh_token: self.refresh_token.clone(),
            expires_at: None,
        };
        TokenStore::new(self.token_file.clone(), seed, self.names)
    }
}

/// Where and as whom to connect: the broker's venue, the environment and
/// the credential.
#[derive(Debug, Clone)]
pub struct CTraderConnection {
    /// The venue of the broker's instruments, each the symbol cTrader
    /// lists on it (`XAUUSD.<venue>`), and of its account.
    pub venue: Venue,
    pub environment: CTraderEnvironment,
    pub credential: CTraderCredential,
}

impl CTraderConnection {
    /// The Nautilus account of the connection: `<venue>-DEMO` or
    /// `<venue>-LIVE`. Named after the venue and the environment, not the
    /// account, whose id and login are credential identity, never logged;
    /// stable across runs, so a persisted cache finds its account again.
    #[must_use]
    pub fn account_id(&self) -> AccountId {
        let environment = match self.environment {
            CTraderEnvironment::Demo => "DEMO",
            CTraderEnvironment::Live => "LIVE",
        };
        AccountId::new(format!("{}-{environment}", self.venue))
    }
}

/// The config of the data client: its id, where it connects, as whom, and
/// the instruments it loads.
#[derive(Debug, Clone)]
pub struct CTraderDataClientConfig {
    /// The client's id, its factory's name.
    pub client_id: ClientId,
    pub connection: CTraderConnection,
    pub instrument_ids: Vec<InstrumentId>,
}

impl ClientConfig for CTraderDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The config of the execution client: its id, where it connects, as
/// whom, and the instruments whose reports it reads.
#[derive(Debug, Clone)]
pub struct CTraderExecutionClientConfig {
    /// The client's id, its factory's name.
    pub client_id: ClientId,
    pub connection: CTraderConnection,
    pub instrument_ids: Vec<InstrumentId>,
}

impl ClientConfig for CTraderExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{self, VENUE};

    #[test]
    fn each_environment_has_spotware_s_endpoint() {
        assert_eq!(
            CTraderEnvironment::Demo.url(),
            "wss://demo.ctraderapi.com:5035"
        );
        assert_eq!(
            CTraderEnvironment::Live.url(),
            "wss://live.ctraderapi.com:5035"
        );
    }

    #[test]
    fn the_account_is_named_after_the_venue_and_the_environment() {
        let (demo, _tokens) = testing::connection();
        let live = CTraderConnection {
            environment: CTraderEnvironment::Live,
            ..demo.clone()
        };

        assert_eq!(demo.venue, Venue::new(VENUE));
        assert_eq!(demo.account_id(), AccountId::from("ACMEFX-DEMO"));
        assert_eq!(live.account_id(), AccountId::from("ACMEFX-LIVE"));
    }

    #[test]
    fn the_credential_s_pair_seeds_its_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let (mut credential, _tokens) = testing::credential();
        credential.token_file = dir.path().join("state").join("token.json");

        let pair = credential.tokens().try_load().unwrap().unwrap();

        assert_eq!(pair.access_token.expose_secret(), testing::ACCESS_TOKEN);
        assert_eq!(pair.refresh_token.expose_secret(), testing::REFRESH_TOKEN);
        assert_eq!(credential.tokens().path(), credential.token_file);
    }

    #[test]
    fn debug_shows_no_credential_value() {
        let (credential, _tokens) = testing::credential();

        let debug = format!("{credential:?}");

        for value in [
            credential.client_id.expose_secret(),
            credential.client_secret.expose_secret(),
            credential.access_token.expose_secret(),
            credential.refresh_token.expose_secret(),
            &credential.account_id.get().to_string(),
        ] {
            assert!(!debug.contains(value), "{debug}");
        }
        assert!(debug.contains(REDACTED), "{debug}");
    }
}
