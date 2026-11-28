//! Configures what connects to cTrader. `CTraderConnection::from_vars` reads the broker's venue,
//! the environment and the credential from the process' environment. This module also holds the
//! configs of the data and execution clients.
//!
//! The credential belongs to the cTrader login, not to a broker. One Open API app and one token
//! pair serve the accounts the login holds at every broker. So `CTRADER_*` and the token file name
//! no broker, and each broker's caller names only the settings of its account
//! (`CTraderAccountVars`).
//!
//! The app's client id and secret come from Spotware's Open API portal. The first token pair and
//! its accounts come from the portal's playground. The pair seeds the token file (see `token`).
//! The secret and the tokens are secret material. The client id and the account id are credential
//! identity. None of them appears in `Debug`, errors or logs.

use std::{any::Any, fmt::Debug, path::PathBuf};

use anyhow::Context;
use nautilus_common::factories::ClientConfig;
use nautilus_core::string::secret::{REDACTED, SecretString};
use nautilus_model::identifiers::{AccountId, ClientId, InstrumentId, Venue};

use super::common::token::{TokenPair, TokenStore};

const CLIENT_ID_VAR: &str = "CTRADER_CLIENT_ID";
const CLIENT_SECRET_VAR: &str = "CTRADER_CLIENT_SECRET";
const ACCESS_TOKEN_VAR: &str = "CTRADER_ACCESS_TOKEN";
const REFRESH_TOKEN_VAR: &str = "CTRADER_REFRESH_TOKEN";

const GUIDE: &str = "crates/ctrader/README.md, \"Credentials\"";

const TOKEN_FILE: &str = "ctrader/token.json";

/// The cTrader servers, demo or live.
///
/// The two are fully separate. An account authenticates only on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CTraderEnvironment {
    Demo,
    Live,
}

impl CTraderEnvironment {
    /// Returns the protobuf endpoint over WebSocket with TLS (help.ctrader.com/open-api, "Proxies
    /// and endpoints").
    pub(crate) fn url(self) -> &'static str {
        match self {
            Self::Demo => "wss://demo.ctraderapi.com:5035",
            Self::Live => "wss://live.ctraderapi.com:5035",
        }
    }
}

/// The trader's account on cTrader (`ctidTraderAccountId`), redacted in `Debug`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CTraderAccountId(i64);

impl CTraderAccountId {
    #[must_use]
    pub const fn new(id: i64) -> Self {
        Self(id)
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl Debug for CTraderAccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(REDACTED)
    }
}

/// The variables that name a broker's environment (`demo` or `live`) and its
/// `ctidTraderAccountId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CTraderAccountVars {
    pub environment: &'static str,
    pub account_id: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CTraderSettingNames {
    pub(crate) environment: &'static str,
    pub(crate) client_id: &'static str,
    pub(crate) client_secret: &'static str,
    pub(crate) access_token: &'static str,
    pub(crate) refresh_token: &'static str,
    pub(crate) account_id: &'static str,
    pub(crate) source: &'static str,
    pub(crate) guide: &'static str,
}

/// The app's client id and secret, the account, and the token file with the pair that seeds it.
#[derive(Debug, Clone)]
pub struct CTraderCredential {
    pub client_id: SecretString,
    pub client_secret: SecretString,
    pub account_id: CTraderAccountId,
    pub access_token: SecretString,
    pub refresh_token: SecretString,
    /// The token file, which keeps the latest pair. It stays unread until a session connects.
    pub token_file: PathBuf,
    pub(crate) names: CTraderSettingNames,
}

impl CTraderCredential {
    pub(crate) fn tokens(&self) -> TokenStore {
        let seed = TokenPair {
            access_token: self.access_token.clone(),
            refresh_token: self.refresh_token.clone(),
            expires_at: None,
        };
        TokenStore::new(self.token_file.clone(), seed, self.names)
    }

    /// The pair the token file holds now: the credential's, until a session renews it.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no token file, or it cannot be read or parsed; the error never
    /// holds a token.
    #[cfg(any(test, feature = "test-support"))]
    pub fn stored_tokens(&self) -> anyhow::Result<TokenPair> {
        self.tokens().read()
    }
}

/// Where and as whom to connect: the broker's venue, the environment and the credential.
#[derive(Debug, Clone)]
pub struct CTraderConnection {
    /// The venue of the broker's instruments (`XAUUSD.<venue>`) and of its account.
    pub venue: Venue,
    pub environment: CTraderEnvironment,
    pub credential: CTraderCredential,
}

impl CTraderConnection {
    /// Returns the connection to the account of the broker on `venue`, reading settings through
    /// `var`.
    ///
    /// The environment and account id come from the variables `account` names. The credential
    /// comes from `CTRADER_CLIENT_ID`, `CTRADER_CLIENT_SECRET`, `CTRADER_ACCESS_TOKEN` and
    /// `CTRADER_REFRESH_TOKEN`. The token file is under the user's state directory
    /// (`XDG_STATE_HOME` or `HOME`), and is not read yet. An error never holds a value.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The environment is not `demo` or `live`.
    /// - A credential variable is missing or empty. The error names every such variable.
    /// - The account id is not an integer.
    /// - There is no state directory.
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

    /// Returns the Nautilus account of the connection: `<venue>-DEMO` or `<venue>-LIVE`.
    ///
    /// The name comes from the environment, not the account id, which is credential identity. It
    /// is stable across runs.
    #[must_use]
    pub fn account_id(&self) -> AccountId {
        let environment = match self.environment {
            CTraderEnvironment::Demo => "DEMO",
            CTraderEnvironment::Live => "LIVE",
        };
        AccountId::new(format!("{}-{environment}", self.venue))
    }
}

/// Returns the environment that the variable of `account` names.
///
/// There is no default. A missing or other value is an error.
fn environment(
    venue: Venue,
    account: CTraderAccountVars,
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<CTraderEnvironment> {
    match var(account.environment).as_deref() {
        Some("demo") => Ok(CTraderEnvironment::Demo),
        Some("live") => Ok(CTraderEnvironment::Live),
        value => anyhow::bail!(
            "{} must be `demo` or `live` (got {value:?}): set it to the environment of the \
             {venue} account, `demo` for a demo account",
            account.environment
        ),
    }
}

fn credential(
    account: CTraderAccountVars,
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<CTraderCredential> {
    // Each value is read once, straight into a zeroizing owner
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
        anyhow::bail!(
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

/// Returns the token file's path.
///
/// The path is under `XDG_STATE_HOME` if that is set to an absolute path (the XDG base directory
/// specification ignores a relative one). Otherwise it is under `$HOME/.local/state`.
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
            anyhow::bail!(
                "no place for cTrader's token file: set HOME, or XDG_STATE_HOME, to an absolute \
                 path"
            )
        }
    }
}

/// The config of the data client: the connection and the instruments it loads.
#[derive(Debug, Clone)]
pub struct CTraderDataClientConfig {
    /// The client's id, which is its factory's name.
    pub client_id: ClientId,
    pub connection: CTraderConnection,
    pub instrument_ids: Vec<InstrumentId>,
}

impl CTraderDataClientConfig {
    /// Returns the config on `connection`, named after its venue, that loads `instrument_ids`.
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

/// The config of the execution client: the connection and the instruments whose reports it reads.
#[derive(Debug, Clone)]
pub struct CTraderExecutionClientConfig {
    /// The client's id, which is its factory's name.
    pub client_id: ClientId,
    pub connection: CTraderConnection,
    pub instrument_ids: Vec<InstrumentId>,
}

impl CTraderExecutionClientConfig {
    /// Returns the config on `connection`, named after its venue, that reads the reports of
    /// `instrument_ids`.
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

    use rstest::rstest;

    use super::*;
    use crate::testing::{self, VENUE};

    #[rstest]
    #[case::demo(CTraderEnvironment::Demo, "wss://demo.ctraderapi.com:5035")]
    #[case::live(CTraderEnvironment::Live, "wss://live.ctraderapi.com:5035")]
    fn each_environment_has_spotware_s_endpoint(
        #[case] environment: CTraderEnvironment,
        #[case] url: &str,
    ) {
        assert_eq!(environment.url(), url);
    }

    #[rstest]
    fn the_credential_s_pair_seeds_its_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let (mut credential, _tokens) = testing::credential();
        credential.token_file = dir.path().join("state").join("token.json");

        let pair = credential.tokens().try_load().unwrap().unwrap();

        assert_eq!(pair.access_token.expose_secret(), testing::ACCESS_TOKEN);
        assert_eq!(pair.refresh_token.expose_secret(), testing::REFRESH_TOKEN);
        assert_eq!(credential.tokens().path(), credential.token_file);
    }

    /// The settings of a made-up broker's account, used with the shared credential.
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

    #[rstest]
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

    #[rstest]
    fn the_environment_is_demo_or_live() {
        let live = read(&full_with("ACMEFX_ENVIRONMENT", Some("live"))).unwrap();

        assert_eq!(live.environment, CTraderEnvironment::Live);
        assert_eq!(live.account_id(), AccountId::from("ACMEFX-LIVE"));
    }

    #[rstest]
    #[case::unset(None)]
    #[case::empty(Some(""))]
    #[case::capitalized(Some("Demo"))]
    #[case::paper(Some("paper"))]
    fn the_environment_has_no_default(#[case] value: Option<&'static str>) {
        let err = read(&full_with("ACMEFX_ENVIRONMENT", value))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("ACMEFX_ENVIRONMENT must be `demo` or `live`"),
            "{err}"
        );
    }

    #[rstest]
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

    #[rstest]
    fn a_missing_account_id_is_named_by_the_broker_s_variable() {
        let err = read(&full_with("ACMEFX_ACCOUNT_ID", None))
            .unwrap_err()
            .to_string();

        assert!(err.contains("ACMEFX_ACCOUNT_ID"), "{err}");
    }

    #[rstest]
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

    #[rstest]
    fn a_connection_s_debug_shows_no_credential_value() {
        let debug = format!("{:?}", read(&FULL).unwrap());

        for (name, value) in FULL {
            if name.starts_with("CTRADER") || name == "ACMEFX_ACCOUNT_ID" {
                assert!(!debug.contains(value), "{debug}");
            }
        }
    }

    #[rstest]
    fn a_connection_from_the_environment_names_the_variables_it_read() {
        let names = read(&FULL).unwrap().credential.names;

        assert_eq!(names.environment, "ACMEFX_ENVIRONMENT");
        assert_eq!(names.account_id, "ACMEFX_ACCOUNT_ID");
        assert_eq!(names.client_id, "CTRADER_CLIENT_ID");
        assert_eq!(names.client_secret, "CTRADER_CLIENT_SECRET");
        assert_eq!(names.access_token, "CTRADER_ACCESS_TOKEN");
        assert_eq!(names.refresh_token, "CTRADER_REFRESH_TOKEN");
        assert_eq!(names.source, "the environment");
        assert_eq!(names.guide, "crates/ctrader/README.md, \"Credentials\"");
    }

    /// Whichever broker's account it connects to, the credential uses one token file. The pair
    /// belongs to the login, and a renewal invalidates the old one.
    #[rstest]
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

    #[rstest]
    fn the_token_file_is_under_the_xdg_state_home() {
        let mut pairs = FULL.to_vec();
        pairs.push(("XDG_STATE_HOME", "/home/u/state"));

        let connection = read(&pairs).unwrap();

        assert_eq!(
            connection.credential.token_file,
            Path::new("/home/u/state/ctrader/token.json")
        );
    }

    #[rstest]
    #[case::unset(None)]
    #[case::relative(Some("state"))]
    fn without_an_absolute_xdg_state_home_the_token_file_is_under_local_state(
        #[case] xdg_state_home: Option<&'static str>,
    ) {
        let mut pairs = FULL.to_vec();
        pairs.extend(xdg_state_home.map(|dir| ("XDG_STATE_HOME", dir)));

        let connection = read(&pairs).unwrap();

        assert_eq!(
            connection.credential.token_file,
            Path::new("/home/u/.local/state/ctrader/token.json")
        );
    }

    #[rstest]
    fn without_a_home_there_is_no_token_file() {
        let err = read(&full_with("HOME", None)).unwrap_err().to_string();

        assert!(err.contains("set HOME"), "{err}");
    }

    #[rstest]
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
