//! What connects to cTrader: the broker's venue, the environment and the
//! credential, read from the process' environment
//! (`CTraderConnection::from_vars`); and the configs of the data and
//! execution clients, which connect with them.
//!
//! The credential is the cTrader login's, not a broker's: one Open API app
//! and one token pair serve the accounts the login holds at every broker,
//! so `CTRADER_*` and the token file name no broker, and each broker's
//! caller names only the settings of its account (`CTraderAccountVars`).
//!
//! The app's client id and secret come from Spotware's Open API portal;
//! the first token pair (access and refresh token), and the accounts it
//! grants, from the portal's playground; the pair seeds the token file,
//! which keeps the latest one (see `token`). The secret and the tokens are
//! secret material, the client id and the account id credential identity:
//! none of them appears in `Debug`, errors or logs.

use std::{any::Any, fmt, path::PathBuf};

use anyhow::{Context, bail};
use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::{REDACTED, SecretString};
use nautilus_model::identifiers::{AccountId, ClientId, InstrumentId, Venue};

use super::common::token::{TokenPair, TokenStore};

const CLIENT_ID_VAR: &str = "CTRADER_CLIENT_ID";
const CLIENT_SECRET_VAR: &str = "CTRADER_CLIENT_SECRET";
const ACCESS_TOKEN_VAR: &str = "CTRADER_ACCESS_TOKEN";
const REFRESH_TOKEN_VAR: &str = "CTRADER_REFRESH_TOKEN";

/// Where setting up the credential is explained.
const GUIDE: &str = "crates/ctrader/README.md, \"Credentials\"";

/// The token file under the user's state directory.
const TOKEN_FILE: &str = "ctrader/token.json";

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

/// The variables of a broker's account: its environment, `demo` or `live`,
/// and its `ctidTraderAccountId`. All the caller names of a connection's
/// settings; the rest are the login's (`CTRADER_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CTraderAccountVars {
    pub environment: &'static str,
    pub account_id: &'static str,
}

/// What the errors asking to check or replace a setting call it: the
/// variables the connection was read from, where they are set, and where
/// setting them up is explained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CTraderSettingNames {
    pub(crate) environment: &'static str,
    pub(crate) client_id: &'static str,
    pub(crate) client_secret: &'static str,
    pub(crate) access_token: &'static str,
    pub(crate) refresh_token: &'static str,
    pub(crate) account_id: &'static str,
    /// Where the settings are set, such as a file of them.
    pub(crate) source: &'static str,
    /// Where setting up the credential is explained.
    pub(crate) guide: &'static str,
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
    pub(crate) names: CTraderSettingNames,
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

    /// The pair the token file holds now: the credential's, until a
    /// session renews it.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no token file, or it cannot be read
    /// or parsed; never a token.
    #[cfg(any(test, feature = "test-support"))]
    pub fn stored_tokens(&self) -> anyhow::Result<TokenPair> {
        self.tokens().read()
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
    /// The connection to the account of the broker of `venue`, through
    /// `var`, the process' environment in production: the environment and
    /// the account id from the variables `account` names, the credential
    /// from `CTRADER_CLIENT_ID`, `CTRADER_CLIENT_SECRET`,
    /// `CTRADER_ACCESS_TOKEN` and `CTRADER_REFRESH_TOKEN`, its token file
    /// under the user's state directory (`XDG_STATE_HOME` or `HOME`).
    /// Nothing is read from the token file yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment is not `demo` or `live`, if a
    /// credential variable is missing or empty (naming every one), if the
    /// account id is not an integer, or if there is no state directory;
    /// never a value.
    pub fn from_vars(
        venue: Venue,
        account: CTraderAccountVars,
        var: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            venue,
            environment: environment(venue, account, &var)?,
            credential: credential(account, &var)?,
        })
    }

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

/// The environment the variable of `account` names, `demo` or `live`,
/// through `var`. No default: a missing or other value is an error.
fn environment(
    venue: Venue,
    account: CTraderAccountVars,
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<CTraderEnvironment> {
    match var(account.environment).as_deref() {
        Some("demo") => Ok(CTraderEnvironment::Demo),
        Some("live") => Ok(CTraderEnvironment::Live),
        value => bail!(
            "{} must be `demo` or `live` (got {value:?}): set it to the environment of the \
             {venue} account, `demo` for a demo account",
            account.environment
        ),
    }
}

/// The credential, as `CTraderConnection::from_vars`.
fn credential(
    account: CTraderAccountVars,
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<CTraderCredential> {
    // Each value is read once, straight into a zeroizing owner.
    let read = |name| SecretString::from(var(name).unwrap_or_default());
    let values = [
        (CLIENT_ID_VAR, read(CLIENT_ID_VAR)),
        (CLIENT_SECRET_VAR, read(CLIENT_SECRET_VAR)),
        (ACCESS_TOKEN_VAR, read(ACCESS_TOKEN_VAR)),
        (REFRESH_TOKEN_VAR, read(REFRESH_TOKEN_VAR)),
        (account.account_id, read(account.account_id)),
    ];
    let missing: Vec<&str> = values
        .iter()
        .filter(|(_, value)| value.expose_secret().is_empty())
        .map(|(name, _)| *name)
        .collect();
    if !missing.is_empty() {
        bail!(
            "the cTrader credential is incomplete: set {} (see {GUIDE})",
            missing.join(", ")
        );
    }
    let [
        (_, client_id),
        (_, client_secret),
        (_, access_token),
        (_, refresh_token),
        (_, account_id),
    ] = values;
    let account_id = account_id.expose_secret().parse().ok().with_context(|| {
        format!(
            "{} must be an integer, the account's ctidTraderAccountId",
            account.account_id
        )
    })?;
    Ok(CTraderCredential {
        client_id,
        client_secret,
        account_id: CTraderAccountId::new(account_id),
        access_token,
        refresh_token,
        token_file: token_file(&var)?,
        names: CTraderSettingNames {
            environment: account.environment,
            client_id: CLIENT_ID_VAR,
            client_secret: CLIENT_SECRET_VAR,
            access_token: ACCESS_TOKEN_VAR,
            refresh_token: REFRESH_TOKEN_VAR,
            account_id: account.account_id,
            source: "the environment",
            guide: GUIDE,
        },
    })
}

/// The token file's path under the user's state directory: under
/// `XDG_STATE_HOME` if set to an absolute path (the XDG base directory
/// specification ignores a relative one), else under `$HOME/.local/state`,
/// through `var`.
///
/// # Errors
///
/// Returns an error if neither is set to an absolute path.
fn token_file(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<PathBuf> {
    let absolute = |name| var(name).map(PathBuf::from).filter(|p| p.is_absolute());
    if let Some(state) = absolute("XDG_STATE_HOME") {
        return Ok(state.join(TOKEN_FILE));
    }
    match absolute("HOME") {
        Some(home) => Ok(home.join(".local/state").join(TOKEN_FILE)),
        None => {
            bail!(
                "no place for cTrader's token file: set HOME, or XDG_STATE_HOME, to an absolute \
                 path"
            )
        }
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

impl CTraderDataClientConfig {
    /// The config of the data client on `connection`, named after its
    /// venue, loading `instrument_ids`.
    #[must_use]
    pub fn new(connection: CTraderConnection, instrument_ids: Vec<InstrumentId>) -> Self {
        Self {
            client_id: ClientId::new(connection.venue.as_str()),
            connection,
            instrument_ids,
        }
    }
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

impl CTraderExecutionClientConfig {
    /// The config of the execution client on `connection`, named after its
    /// venue, reading the reports of `instrument_ids`.
    #[must_use]
    pub fn new(connection: CTraderConnection, instrument_ids: Vec<InstrumentId>) -> Self {
        Self {
            client_id: ClientId::new(connection.venue.as_str()),
            connection,
            instrument_ids,
        }
    }
}

impl ClientConfig for CTraderExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::Path};

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

    /// The settings of a made-up broker's account, beside the shared
    /// credential.
    const ACCOUNT: CTraderAccountVars = CTraderAccountVars {
        environment: "ACMEFX_ENVIRONMENT",
        account_id: "ACMEFX_ACCOUNT_ID",
    };

    const FULL: [(&str, &str); 7] = [
        ("CTRADER_CLIENT_ID", "12345_clientIdValue"),
        ("CTRADER_CLIENT_SECRET", "clientSecretValue"),
        ("CTRADER_ACCESS_TOKEN", "accessTokenValue"),
        ("CTRADER_REFRESH_TOKEN", "refreshTokenValue"),
        ("ACMEFX_ACCOUNT_ID", "1000001"),
        ("ACMEFX_ENVIRONMENT", "demo"),
        ("HOME", "/home/u"),
    ];

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn read(pairs: &[(&str, &str)]) -> anyhow::Result<CTraderConnection> {
        CTraderConnection::from_vars(Venue::new(VENUE), ACCOUNT, vars(pairs))
    }

    /// `FULL` without the setting `name`, or with it set to `value`.
    fn full_with(name: &str, value: Option<&'static str>) -> Vec<(&'static str, &'static str)> {
        FULL.iter()
            .filter_map(|&(k, v)| {
                if k == name {
                    value.map(|value| (k, value))
                } else {
                    Some((k, v))
                }
            })
            .collect()
    }

    #[test]
    fn a_connection_is_the_venue_s_on_its_account_s_environment_and_the_shared_credential() {
        let connection = read(&FULL).unwrap();

        assert_eq!(connection.venue, Venue::new(VENUE));
        assert_eq!(connection.environment, CTraderEnvironment::Demo);
        assert_eq!(connection.account_id(), AccountId::from("ACMEFX-DEMO"));
        let credential = connection.credential;
        assert_eq!(credential.client_id.expose_secret(), "12345_clientIdValue");
        assert_eq!(
            credential.client_secret.expose_secret(),
            "clientSecretValue"
        );
        assert_eq!(credential.access_token.expose_secret(), "accessTokenValue");
        assert_eq!(
            credential.refresh_token.expose_secret(),
            "refreshTokenValue"
        );
        assert_eq!(credential.account_id.get(), 1_000_001);
    }

    #[test]
    fn the_environment_is_demo_or_live() {
        let live = read(&full_with("ACMEFX_ENVIRONMENT", Some("live"))).unwrap();

        assert_eq!(live.environment, CTraderEnvironment::Live);
        assert_eq!(live.account_id(), AccountId::from("ACMEFX-LIVE"));
    }

    #[test]
    fn the_environment_has_no_default() {
        for value in [None, Some(""), Some("Demo"), Some("paper")] {
            let err = read(&full_with("ACMEFX_ENVIRONMENT", value))
                .unwrap_err()
                .to_string();

            assert!(
                err.contains("ACMEFX_ENVIRONMENT must be `demo` or `live`"),
                "{err}"
            );
        }
    }

    #[test]
    fn a_missing_or_empty_variable_is_named_without_any_value() {
        let pairs = [
            ("CTRADER_CLIENT_ID", "12345_clientIdValue"),
            ("CTRADER_CLIENT_SECRET", ""),
            ("CTRADER_ACCESS_TOKEN", "accessTokenValue"),
            ("ACMEFX_ACCOUNT_ID", "1000001"),
            ("ACMEFX_ENVIRONMENT", "demo"),
            ("HOME", "/home/u"),
        ];

        let err = read(&pairs).unwrap_err().to_string();

        assert!(err.contains("CTRADER_CLIENT_SECRET"), "{err}");
        assert!(err.contains("CTRADER_REFRESH_TOKEN"), "{err}");
        assert!(!err.contains("CTRADER_CLIENT_ID"), "{err}");
        assert!(
            !err.contains("12345_clientIdValue") && !err.contains("1000001"),
            "{err}"
        );
    }

    #[test]
    fn a_missing_account_id_is_named_by_the_broker_s_variable() {
        let err = read(&full_with("ACMEFX_ACCOUNT_ID", None))
            .unwrap_err()
            .to_string();

        assert!(err.contains("ACMEFX_ACCOUNT_ID"), "{err}");
    }

    #[test]
    fn an_account_id_not_an_integer_is_refused_without_its_value() {
        let err = read(&full_with("ACMEFX_ACCOUNT_ID", Some("acct-77")))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("ACMEFX_ACCOUNT_ID must be an integer"),
            "{err}"
        );
        assert!(!err.contains("acct-77"), "{err}");
    }

    #[test]
    fn a_connection_s_debug_shows_no_credential_value() {
        let debug = format!("{:?}", read(&FULL).unwrap());

        for (name, value) in FULL {
            if name.starts_with("CTRADER") || name == "ACMEFX_ACCOUNT_ID" {
                assert!(!debug.contains(value), "{debug}");
            }
        }
    }

    #[test]
    fn the_errors_name_the_settings_read() {
        let names = read(&FULL).unwrap().credential.names;

        assert_eq!(names.environment, "ACMEFX_ENVIRONMENT");
        assert_eq!(names.account_id, "ACMEFX_ACCOUNT_ID");
        assert_eq!(names.access_token, "CTRADER_ACCESS_TOKEN");
        assert_eq!(names.refresh_token, "CTRADER_REFRESH_TOKEN");
    }

    /// Whichever broker's account it connects to, one token file: the pair
    /// is the login's, and a renewal invalidates the old one.
    #[test]
    fn two_brokers_share_the_token_file() {
        let other = CTraderAccountVars {
            environment: "OTHERFX_ENVIRONMENT",
            account_id: "OTHERFX_ACCOUNT_ID",
        };
        let mut pairs = FULL.to_vec();
        pairs.extend([("OTHERFX_ENVIRONMENT", "demo"), ("OTHERFX_ACCOUNT_ID", "2")]);

        let acme = read(&pairs).unwrap();
        let second =
            CTraderConnection::from_vars(Venue::new("OTHERFX"), other, vars(&pairs)).unwrap();

        assert_eq!(acme.credential.token_file, second.credential.token_file);
        assert_eq!(second.credential.account_id.get(), 2);
    }

    #[test]
    fn the_token_file_is_under_the_xdg_state_home() {
        let mut pairs = FULL.to_vec();
        pairs.push(("XDG_STATE_HOME", "/home/u/state"));

        let connection = read(&pairs).unwrap();

        assert_eq!(
            connection.credential.token_file,
            Path::new("/home/u/state/ctrader/token.json")
        );
    }

    /// The XDG base directory specification ignores a relative one.
    #[test]
    fn without_an_absolute_xdg_state_home_the_token_file_is_under_local_state() {
        let mut relative = FULL.to_vec();
        relative.push(("XDG_STATE_HOME", "state"));

        for pairs in [FULL.to_vec(), relative] {
            let connection = read(&pairs).unwrap();

            assert_eq!(
                connection.credential.token_file,
                Path::new("/home/u/.local/state/ctrader/token.json")
            );
        }
    }

    #[test]
    fn without_a_home_there_is_no_token_file() {
        let err = read(&full_with("HOME", None)).unwrap_err().to_string();

        assert!(err.contains("set HOME"), "{err}");
    }

    #[test]
    fn each_client_is_named_after_the_venue_on_its_connection() {
        let ids = vec![InstrumentId::from("XAUUSD.ACMEFX")];

        let data = CTraderDataClientConfig::new(read(&FULL).unwrap(), ids.clone());
        let exec = CTraderExecutionClientConfig::new(read(&FULL).unwrap(), ids.clone());

        assert_eq!(data.client_id, ClientId::new(VENUE));
        assert_eq!(exec.client_id, ClientId::new(VENUE));
        assert_eq!(data.instrument_ids, ids);
        assert_eq!(exec.instrument_ids, ids);
    }
}
