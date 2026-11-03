//! IC's connection, read from the environment (or `.env`) once, into the
//! cTrader adapter's typed config: the environment `IC_ENVIRONMENT` names,
//! required, and the credential from `IC_*`, with its token file under the
//! user's state directory.
//!
//! The app's client id and secret come from Spotware's Open API portal;
//! the first token pair (access and refresh token), and the account it
//! grants, from the portal's playground (see `README.md`, "Credentials");
//! the pair seeds the token file, which keeps the latest one. The secret
//! and the tokens are secret material, the client id and the account id
//! credential identity: none of them appears in `Debug`, errors or logs.

use std::path::PathBuf;

use anyhow::{Context, bail};
use ctrader::{
    CTraderAccountId, CTraderConnection, CTraderCredential, CTraderEnvironment, CTraderSettingNames,
};
use nautilus_core::string::secret::SecretString;
use nautilus_model::identifiers::Venue;

use super::IC_VENUE;

/// The variable naming the environment, `demo` or `live`.
const ENVIRONMENT_VAR: &str = "IC_ENVIRONMENT";
const CLIENT_ID_VAR: &str = "IC_CLIENT_ID";
const CLIENT_SECRET_VAR: &str = "IC_CLIENT_SECRET";
const ACCESS_TOKEN_VAR: &str = "IC_ACCESS_TOKEN";
const REFRESH_TOKEN_VAR: &str = "IC_REFRESH_TOKEN";
const ACCOUNT_ID_VAR: &str = "IC_ACCOUNT_ID";

/// What the adapter's errors call the settings above.
const NAMES: CTraderSettingNames = CTraderSettingNames {
    environment: ENVIRONMENT_VAR,
    client_id: CLIENT_ID_VAR,
    client_secret: CLIENT_SECRET_VAR,
    access_token: ACCESS_TOKEN_VAR,
    refresh_token: REFRESH_TOKEN_VAR,
    account_id: ACCOUNT_ID_VAR,
    source: ".env",
    guide: "src/venues/ic/README.md, \"Credentials\"",
};

/// The token file under the user's state directory.
const TOKEN_FILE: &str = "helix-trader/ic-token.json";

/// IC's connection: the environment `IC_ENVIRONMENT` names and the
/// credential from `IC_CLIENT_ID`, `IC_CLIENT_SECRET`, `IC_ACCESS_TOKEN`,
/// `IC_REFRESH_TOKEN` and `IC_ACCOUNT_ID`, its token file under the user's
/// state directory (`XDG_STATE_HOME` or `HOME`), through `var`, on venue
/// `ICMARKETS`. Nothing is read from the token file yet.
///
/// # Errors
///
/// Returns an error if the environment is not `demo` or `live`, if a
/// credential variable is missing or empty (naming every one), if the
/// account id is not an integer, or if there is no state directory; never
/// a value.
pub(super) fn connection(
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<CTraderConnection> {
    Ok(CTraderConnection {
        venue: Venue::new(IC_VENUE),
        environment: environment(&var)?,
        credential: credential(&var)?,
    })
}

/// The environment `IC_ENVIRONMENT` names, `demo` or `live`, through
/// `var`. No default: a missing or other value is an error.
fn environment(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<CTraderEnvironment> {
    match var(ENVIRONMENT_VAR).as_deref() {
        Some("demo") => Ok(CTraderEnvironment::Demo),
        Some("live") => Ok(CTraderEnvironment::Live),
        value => bail!(
            "{ENVIRONMENT_VAR} must be `demo` or `live` (got {value:?}): set it in \
             .env to the environment of the IC account, `demo` for a demo account"
        ),
    }
}

/// The credential from `IC_*`, as `connection`.
fn credential(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<CTraderCredential> {
    // Each value is read once, straight into a zeroizing owner.
    let read = |name| SecretString::from(var(name).unwrap_or_default());
    let values = [
        (CLIENT_ID_VAR, read(CLIENT_ID_VAR)),
        (CLIENT_SECRET_VAR, read(CLIENT_SECRET_VAR)),
        (ACCESS_TOKEN_VAR, read(ACCESS_TOKEN_VAR)),
        (REFRESH_TOKEN_VAR, read(REFRESH_TOKEN_VAR)),
        (ACCOUNT_ID_VAR, read(ACCOUNT_ID_VAR)),
    ];
    let missing: Vec<&str> = values
        .iter()
        .filter(|(_, value)| value.expose_secret().is_empty())
        .map(|(name, _)| *name)
        .collect();
    if !missing.is_empty() {
        bail!(
            "the IC credential is incomplete: set {} in .env (see src/venues/ic/README.md, \
             \"Credentials\")",
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
        format!("{ACCOUNT_ID_VAR} must be an integer, the account's ctidTraderAccountId")
    })?;
    Ok(CTraderCredential {
        client_id,
        client_secret,
        account_id: CTraderAccountId::new(account_id),
        access_token,
        refresh_token,
        token_file: token_file(&var)?,
        names: NAMES,
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
            bail!("no place for IC's token file: set HOME, or XDG_STATE_HOME, to an absolute path")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::Path};

    use nautilus_core::string::secret::REDACTED;
    use nautilus_model::identifiers::AccountId;

    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    const FULL: [(&str, &str); 6] = [
        ("IC_CLIENT_ID", "12345_clientIdValue"),
        ("IC_CLIENT_SECRET", "clientSecretValue"),
        ("IC_ACCESS_TOKEN", "accessTokenValue"),
        ("IC_REFRESH_TOKEN", "refreshTokenValue"),
        ("IC_ACCOUNT_ID", "1000001"),
        ("HOME", "/home/u"),
    ];

    #[test]
    fn the_environment_is_demo_or_live() {
        let demo = environment(vars(&[("IC_ENVIRONMENT", "demo")])).unwrap();
        let live = environment(vars(&[("IC_ENVIRONMENT", "live")])).unwrap();

        assert_eq!(demo, CTraderEnvironment::Demo);
        assert_eq!(live, CTraderEnvironment::Live);
    }

    #[test]
    fn the_environment_has_no_default() {
        for value in [None, Some(""), Some("Demo"), Some("paper")] {
            let pairs: Vec<(&str, &str)> =
                value.map(|v| ("IC_ENVIRONMENT", v)).into_iter().collect();

            let err = environment(vars(&pairs)).unwrap_err().to_string();

            assert!(
                err.contains("IC_ENVIRONMENT must be `demo` or `live`"),
                "{err}"
            );
        }
    }

    #[test]
    fn the_credential_is_read_whole() {
        let credential = credential(vars(&FULL)).unwrap();

        assert_eq!(credential.client_id.expose_secret(), "12345_clientIdValue");
        assert_eq!(
            credential.client_secret.expose_secret(),
            "clientSecretValue"
        );
        assert_eq!(credential.account_id.get(), 1_000_001);
        assert_eq!(
            credential.token_file,
            Path::new("/home/u/.local/state/helix-trader/ic-token.json")
        );
        assert_eq!(credential.names, NAMES);
    }

    /// The pair that seeds the token file is the environment's, the file
    /// under `XDG_STATE_HOME`.
    #[test]
    fn the_env_pair_seeds_the_token_file() {
        let mut pairs = FULL.to_vec();
        pairs.push(("XDG_STATE_HOME", "/home/u/state"));

        let credential = credential(vars(&pairs)).unwrap();

        assert_eq!(credential.access_token.expose_secret(), "accessTokenValue");
        assert_eq!(
            credential.refresh_token.expose_secret(),
            "refreshTokenValue"
        );
        assert_eq!(
            credential.token_file,
            Path::new("/home/u/state/helix-trader/ic-token.json")
        );
    }

    #[test]
    fn debug_shows_no_credential_value() {
        let debug = format!("{:?}", credential(vars(&FULL)).unwrap());

        for (_, value) in FULL.iter().filter(|(name, _)| name.starts_with("IC_")) {
            assert!(!debug.contains(value), "{debug}");
        }
        assert!(debug.contains(REDACTED), "{debug}");
    }

    #[test]
    fn a_missing_or_empty_variable_is_named_without_any_value() {
        let pairs = [
            ("IC_CLIENT_ID", "12345_clientIdValue"),
            ("IC_CLIENT_SECRET", ""),
            ("IC_ACCESS_TOKEN", "accessTokenValue"),
            ("IC_ACCOUNT_ID", "1000001"),
            ("HOME", "/home/u"),
        ];

        let err = credential(vars(&pairs)).unwrap_err().to_string();

        assert!(
            err.contains("set IC_CLIENT_SECRET, IC_REFRESH_TOKEN in .env"),
            "{err}"
        );
        assert!(
            !err.contains("12345_clientIdValue") && !err.contains("1000001"),
            "{err}"
        );
    }

    #[test]
    fn a_connection_needs_the_environment_and_the_whole_credential() {
        let mut full = FULL.to_vec();
        full.push(("IC_ENVIRONMENT", "demo"));

        let connection = connection(vars(&full)).unwrap();
        let no_environment = super::connection(vars(&FULL)).unwrap_err();
        let no_token = super::connection(vars(&[("IC_ENVIRONMENT", "demo")])).unwrap_err();

        assert_eq!(connection.venue, Venue::new("ICMARKETS"));
        assert_eq!(connection.environment, CTraderEnvironment::Demo);
        assert_eq!(connection.account_id(), AccountId::from("ICMARKETS-DEMO"));
        assert!(
            no_environment.to_string().contains("IC_ENVIRONMENT"),
            "{no_environment}"
        );
        assert!(
            no_token.to_string().contains("IC_ACCESS_TOKEN"),
            "{no_token}"
        );
    }

    #[test]
    fn an_account_id_not_an_integer_is_refused_without_its_value() {
        let mut pairs = FULL;
        pairs[4] = ("IC_ACCOUNT_ID", "acct-77");

        let err = credential(vars(&pairs)).unwrap_err().to_string();

        assert!(err.contains("IC_ACCOUNT_ID must be an integer"), "{err}");
        assert!(!err.contains("acct-77"), "{err}");
    }

    #[test]
    fn the_file_is_under_the_xdg_state_home() {
        let path = token_file(vars(&[
            ("XDG_STATE_HOME", "/home/u/state"),
            ("HOME", "/home/u"),
        ]))
        .unwrap();

        assert_eq!(path, Path::new("/home/u/state/helix-trader/ic-token.json"));
    }

    #[test]
    fn without_an_absolute_xdg_state_home_the_file_is_under_local_state() {
        for pairs in [
            vec![("HOME", "/home/u")],
            vec![("XDG_STATE_HOME", "state"), ("HOME", "/home/u")],
        ] {
            let path = token_file(vars(&pairs)).unwrap();

            assert_eq!(
                path,
                Path::new("/home/u/.local/state/helix-trader/ic-token.json")
            );
        }
    }

    #[test]
    fn without_a_home_there_is_no_token_file() {
        let err = token_file(vars(&[])).unwrap_err();

        assert!(err.to_string().contains("set HOME"), "{err}");
    }
}
