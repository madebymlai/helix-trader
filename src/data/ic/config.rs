//! What connects to IC's cTrader: the environment, required, and the
//! credential, both read from the environment (or `.env`) once.
//!
//! The app's client id and secret come from Spotware's Open API portal;
//! the access token, and the account it grants, from the portal's
//! playground (see the data README, "IC Markets specifics"). The secret and
//! the token are secret material, the client id and the account id
//! credential identity: none of them appears in `Debug`, errors or logs.

use std::fmt;

use anyhow::{Context, bail};
use nautilus_core::string::secret::{REDACTED, SecretString};

/// The variable naming the environment, `demo` or `live`.
const ENVIRONMENT_VAR: &str = "IC_ENVIRONMENT";
const CLIENT_ID_VAR: &str = "IC_CLIENT_ID";
const CLIENT_SECRET_VAR: &str = "IC_CLIENT_SECRET";
const ACCESS_TOKEN_VAR: &str = "IC_ACCESS_TOKEN";
const ACCOUNT_ID_VAR: &str = "IC_ACCOUNT_ID";

/// cTrader's demo or live servers, fully separate: an account
/// authenticates only on its own environment's server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IcEnvironment {
    Demo,
    Live,
}

impl IcEnvironment {
    /// The environment `IC_ENVIRONMENT` names, `demo` or `live`, through
    /// `var`. No default: a missing or other value is an error.
    pub(super) fn from_vars(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        match var(ENVIRONMENT_VAR).as_deref() {
            Some("demo") => Ok(Self::Demo),
            Some("live") => Ok(Self::Live),
            value => bail!(
                "{ENVIRONMENT_VAR} must be `demo` or `live` (got {value:?}): set it in \
                 .env to the environment of the IC account, `demo` for a demo account"
            ),
        }
    }

    /// The protobuf endpoint (port 5035) over WebSocket with TLS
    /// (help.ctrader.com/open-api, "Proxies and endpoints").
    pub(super) fn url(self) -> &'static str {
        match self {
            Self::Demo => "wss://demo.ctraderapi.com:5035",
            Self::Live => "wss://live.ctraderapi.com:5035",
        }
    }
}

/// The trader's account on cTrader (`ctidTraderAccountId`), which
/// requests name; credential identity, so redacted in `Debug`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct IcAccountId(i64);

impl IcAccountId {
    pub(super) const fn new(id: i64) -> Self {
        Self(id)
    }

    /// The id, for the requests that carry it.
    pub(super) const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Debug for IcAccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// The app's client id and secret, and the access token with the account
/// it authenticates.
#[derive(Debug, Clone)]
pub(super) struct IcCredential {
    pub(super) client_id: SecretString,
    pub(super) client_secret: SecretString,
    pub(super) access_token: SecretString,
    pub(super) account_id: IcAccountId,
}

impl IcCredential {
    /// The credential from `IC_CLIENT_ID`, `IC_CLIENT_SECRET`,
    /// `IC_ACCESS_TOKEN` and `IC_ACCOUNT_ID`, through `var`.
    ///
    /// # Errors
    ///
    /// Returns an error naming every variable missing or empty, or if the
    /// account id is not an integer; never a value.
    pub(super) fn from_vars(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        // Each value is read once, straight into a zeroizing owner.
        let read = |name| SecretString::from(var(name).unwrap_or_default());
        let values = [
            (CLIENT_ID_VAR, read(CLIENT_ID_VAR)),
            (CLIENT_SECRET_VAR, read(CLIENT_SECRET_VAR)),
            (ACCESS_TOKEN_VAR, read(ACCESS_TOKEN_VAR)),
            (ACCOUNT_ID_VAR, read(ACCOUNT_ID_VAR)),
        ];
        let missing: Vec<&str> = values
            .iter()
            .filter(|(_, value)| value.expose_secret().is_empty())
            .map(|(name, _)| *name)
            .collect();
        if !missing.is_empty() {
            bail!(
                "the IC credential is incomplete: set {} in .env (see the data README, \
                 \"IC Markets specifics\")",
                missing.join(", ")
            );
        }
        let [
            (_, client_id),
            (_, client_secret),
            (_, access_token),
            (_, account_id),
        ] = values;
        let account_id = account_id.expose_secret().parse().ok().with_context(|| {
            format!("{ACCOUNT_ID_VAR} must be an integer, the account's ctidTraderAccountId")
        })?;
        Ok(Self {
            client_id,
            client_secret,
            access_token,
            account_id: IcAccountId::new(account_id),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    const FULL: [(&str, &str); 4] = [
        ("IC_CLIENT_ID", "12345_clientIdValue"),
        ("IC_CLIENT_SECRET", "clientSecretValue"),
        ("IC_ACCESS_TOKEN", "accessTokenValue"),
        ("IC_ACCOUNT_ID", "1000001"),
    ];

    #[test]
    fn the_environment_is_demo_or_live_with_its_endpoint() {
        let demo = IcEnvironment::from_vars(vars(&[("IC_ENVIRONMENT", "demo")])).unwrap();
        let live = IcEnvironment::from_vars(vars(&[("IC_ENVIRONMENT", "live")])).unwrap();

        assert_eq!(demo, IcEnvironment::Demo);
        assert_eq!(demo.url(), "wss://demo.ctraderapi.com:5035");
        assert_eq!(live, IcEnvironment::Live);
        assert_eq!(live.url(), "wss://live.ctraderapi.com:5035");
    }

    #[test]
    fn the_environment_has_no_default() {
        for value in [None, Some(""), Some("Demo"), Some("paper")] {
            let pairs: Vec<(&str, &str)> =
                value.map(|v| ("IC_ENVIRONMENT", v)).into_iter().collect();

            let err = IcEnvironment::from_vars(vars(&pairs))
                .unwrap_err()
                .to_string();

            assert!(
                err.contains("IC_ENVIRONMENT must be `demo` or `live`"),
                "{err}"
            );
        }
    }

    #[test]
    fn the_credential_is_read_whole() {
        let credential = IcCredential::from_vars(vars(&FULL)).unwrap();

        assert_eq!(credential.client_id.expose_secret(), "12345_clientIdValue");
        assert_eq!(
            credential.client_secret.expose_secret(),
            "clientSecretValue"
        );
        assert_eq!(credential.access_token.expose_secret(), "accessTokenValue");
        assert_eq!(credential.account_id.get(), 1_000_001);
    }

    #[test]
    fn debug_shows_no_credential_value() {
        let debug = format!("{:?}", IcCredential::from_vars(vars(&FULL)).unwrap());

        for (_, value) in FULL {
            assert!(!debug.contains(value), "{debug}");
        }
        assert!(debug.contains(REDACTED), "{debug}");
    }

    #[test]
    fn a_missing_or_empty_variable_is_named_without_any_value() {
        let pairs = [
            ("IC_CLIENT_ID", "12345_clientIdValue"),
            ("IC_CLIENT_SECRET", ""),
            ("IC_ACCOUNT_ID", "1000001"),
        ];

        let err = IcCredential::from_vars(vars(&pairs))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("set IC_CLIENT_SECRET, IC_ACCESS_TOKEN in .env"),
            "{err}"
        );
        assert!(
            !err.contains("12345_clientIdValue") && !err.contains("1000001"),
            "{err}"
        );
    }

    #[test]
    fn an_account_id_not_an_integer_is_refused_without_its_value() {
        let mut pairs = FULL;
        pairs[3] = ("IC_ACCOUNT_ID", "acct-77");

        let err = IcCredential::from_vars(vars(&pairs))
            .unwrap_err()
            .to_string();

        assert!(err.contains("IC_ACCOUNT_ID must be an integer"), "{err}");
        assert!(!err.contains("acct-77"), "{err}");
    }
}
