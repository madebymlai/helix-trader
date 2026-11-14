//! The LSE keys, sent in the `X-API-Key` header of every call: one for
//! LSE's own markets and one for each exchange plan's market, each given
//! or read from its variable, and never printed.

use std::fmt;

use nautilus_core::string::secret::SecretString;

use super::consts::{LSE_API_KEY, LSE_CME_API_KEY, LSE_NASDAQ_API_KEY};

/// What an LSE key opens. An exchange plan issues a key of its own, which
/// authenticates only where its market is carried (LSE's `/v3/usage`:
/// `product`, `global` for an LSE key, else the exchange), so each market
/// is asked with its product's key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LseProduct {
    /// LSE's own markets, the derived ones and US options, an included
    /// market: the LSE key, `LSE_API_KEY`.
    Global,
    /// CME Globex (`cme`): a CME Group plan's key, `LSE_CME_API_KEY`.
    Cme,
    /// Nasdaq TotalView (`nasdaq`): a Nasdaq plan's key,
    /// `LSE_NASDAQ_API_KEY`.
    Nasdaq,
}

impl LseProduct {
    /// Every product, in the order a missing key's error names them.
    pub(crate) const ALL: [Self; 3] = [Self::Global, Self::Cme, Self::Nasdaq];

    /// The variable holding the product's key.
    pub(crate) const fn api_key_var(self) -> &'static str {
        match self {
            Self::Global => LSE_API_KEY,
            Self::Cme => LSE_CME_API_KEY,
            Self::Nasdaq => LSE_NASDAQ_API_KEY,
        }
    }
}

impl fmt::Display for LseProduct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Global => "LSE",
            Self::Cme => "CME Group",
            Self::Nasdaq => "Nasdaq",
        })
    }
}

/// An LSE key. Its `Debug` shows `<redacted>`, the key's own.
#[derive(Debug, Clone)]
pub(crate) struct LseCredential {
    api_key: SecretString,
}

impl LseCredential {
    /// The key `api_key` of `product`, or, if none is given, the one `var`
    /// reads from the product's variable; none if neither holds one, an
    /// empty key being none.
    pub(crate) fn resolve(
        product: LseProduct,
        api_key: Option<SecretString>,
        var: impl Fn(&str) -> Option<String>,
    ) -> Option<Self> {
        api_key
            .or_else(|| var(product.api_key_var()).map(SecretString::from))
            .filter(|key| !key.expose_secret().is_empty())
            .map(|api_key| Self { api_key })
    }

    /// The key, for the header that carries it.
    pub(crate) fn api_key(&self) -> &str {
        self.api_key.expose_secret()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const KEY: &str = "lse-test-key-0123456789";

    /// An environment where `name` holds `key`.
    fn environment(name: &'static str, key: Option<&str>) -> impl Fn(&str) -> Option<String> {
        move |asked| (asked == name).then(|| key.map(ToString::to_string))?
    }

    #[rstest]
    #[case::lse(LseProduct::Global)]
    #[case::cme(LseProduct::Cme)]
    #[case::nasdaq(LseProduct::Nasdaq)]
    fn a_key_given_is_used_whatever_the_product_s_variable_holds(#[case] product: LseProduct) {
        let credential = LseCredential::resolve(
            product,
            Some(SecretString::from(KEY)),
            environment(product.api_key_var(), Some("other")),
        )
        .unwrap();

        assert_eq!(credential.api_key(), KEY);
    }

    #[rstest]
    #[case::lse(LseProduct::Global, "LSE_API_KEY")]
    #[case::cme(LseProduct::Cme, "LSE_CME_API_KEY")]
    #[case::nasdaq(LseProduct::Nasdaq, "LSE_NASDAQ_API_KEY")]
    fn with_no_key_given_the_product_s_variable_is_read(
        #[case] product: LseProduct,
        #[case] variable: &'static str,
    ) {
        let credential = LseCredential::resolve(product, None, environment(variable, Some(KEY)));

        assert_eq!(
            credential.map(|c| c.api_key().to_owned()),
            Some(KEY.to_owned())
        );
    }

    /// An exchange plan's key opens its market alone: the CME key is no
    /// LSE key, nor a Nasdaq key.
    #[rstest]
    #[case::lse(LseProduct::Global)]
    #[case::nasdaq(LseProduct::Nasdaq)]
    fn another_product_s_key_is_none(#[case] product: LseProduct) {
        let credential =
            LseCredential::resolve(product, None, environment(LSE_CME_API_KEY, Some(KEY)));

        assert!(credential.is_none());
    }

    #[rstest]
    #[case::unset(None)]
    #[case::empty(Some(""))]
    fn no_key_anywhere_is_none(#[case] set: Option<&str>) {
        assert!(
            LseCredential::resolve(LseProduct::Global, None, environment(LSE_API_KEY, set))
                .is_none()
        );
    }

    #[rstest]
    fn the_key_is_not_in_its_debug() {
        let credential =
            LseCredential::resolve(LseProduct::Cme, Some(SecretString::from(KEY)), |_| None)
                .unwrap();

        assert!(!format!("{credential:?}").contains(KEY));
    }
}
