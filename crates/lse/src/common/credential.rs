//! The LSE key, sent in the `X-API-Key` header of every call: given, or
//! read from `LSE_API_KEY`, and never printed.

use anyhow::Context;
use nautilus_core::string::secret::SecretString;

use super::consts::LSE_API_KEY;

/// An LSE key. Its `Debug` shows `<redacted>`, the key's own.
#[derive(Debug, Clone)]
pub(crate) struct LseCredential {
    api_key: SecretString,
}

impl LseCredential {
    /// The key `api_key`, or, if none is given, the one `var` reads from
    /// `LSE_API_KEY`; an empty key is none.
    ///
    /// # Errors
    ///
    /// Returns an error naming `LSE_API_KEY` if neither holds a key.
    pub(crate) fn resolve(
        api_key: Option<SecretString>,
        var: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        let api_key = api_key
            .or_else(|| var(LSE_API_KEY).map(SecretString::from))
            .filter(|key| !key.expose_secret().is_empty())
            .with_context(|| format!("no LSE key: set {LSE_API_KEY}, or give the config's"))?;
        Ok(Self { api_key })
    }

    /// The key, for the header that carries it.
    pub(crate) fn api_key(&self) -> &str {
        self.api_key.expose_secret()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "lse-test-key-0123456789";

    fn environment(key: Option<&str>) -> impl Fn(&str) -> Option<String> {
        move |name| (name == LSE_API_KEY).then(|| key.map(ToString::to_string))?
    }

    #[test]
    fn a_key_given_is_used_whatever_lse_api_key_holds() {
        let credential =
            LseCredential::resolve(Some(SecretString::from(KEY)), environment(Some("other")))
                .unwrap();

        assert_eq!(credential.api_key(), KEY);
    }

    #[test]
    fn with_no_key_given_lse_api_key_is_read() {
        let credential = LseCredential::resolve(None, environment(Some(KEY))).unwrap();

        assert_eq!(credential.api_key(), KEY);
    }

    #[test]
    fn no_key_anywhere_is_an_error_naming_lse_api_key() {
        for set in [None, Some("")] {
            let err = LseCredential::resolve(None, environment(set)).unwrap_err();

            assert!(err.to_string().contains(LSE_API_KEY), "{set:?}: {err}");
        }
    }

    #[test]
    fn the_key_is_not_in_its_debug() {
        let credential = LseCredential::resolve(Some(SecretString::from(KEY)), |_| None).unwrap();

        assert!(!format!("{credential:?}").contains(KEY));
    }
}
