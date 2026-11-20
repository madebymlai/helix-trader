//! Holds the LSE keys, which go in the `X-API-Key` header.
//!
//! One key covers LSE's own markets, and each exchange plan has its own. A key is given or read
//! from its variable. It is never printed.

use std::fmt::Display;

use nautilus_core::string::secret::SecretString;

use super::consts::{LSE_API_KEY, LSE_CME_API_KEY, LSE_NASDAQ_API_KEY};

/// What an LSE key opens.
///
/// An exchange plan's key authenticates only its own market. LSE's `/v3/usage` reports `product`
/// as `global` for an LSE key, else as the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LseProduct {
    /// LSE's own markets, including derived ones and US options: `LSE_API_KEY`.
    Global,
    /// CME Globex (`cme`): `LSE_CME_API_KEY`.
    Cme,
    /// Nasdaq TotalView (`nasdaq`): `LSE_NASDAQ_API_KEY`.
    Nasdaq,
}

impl LseProduct {
    /// Lists every product in the order a missing-key error names them.
    pub const ALL: [Self; 3] = [Self::Global, Self::Cme, Self::Nasdaq];

    /// Returns the variable holding the product's key.
    pub const fn api_key_var(self) -> &'static str {
        match self {
            Self::Global => LSE_API_KEY,
            Self::Cme => LSE_CME_API_KEY,
            Self::Nasdaq => LSE_NASDAQ_API_KEY,
        }
    }
}

impl Display for LseProduct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Global => "LSE",
            Self::Cme => "CME Group",
            Self::Nasdaq => "Nasdaq",
        })
    }
}

/// An LSE key. Its `Debug` shows `<redacted>`, not the key.
#[derive(Debug, Clone)]
pub struct LseCredential {
    api_key: SecretString,
}

impl LseCredential {
    /// Returns `api_key` if given, else the key that `var` reads from the product's variable.
    ///
    /// Returns `None` if neither holds a key. An empty key counts as none.
    pub fn resolve(
        product: LseProduct,
        api_key: Option<SecretString>,
        var: impl Fn(&str) -> Option<String>,
    ) -> Option<Self> {
        api_key
            .or_else(|| var(product.api_key_var()).map(SecretString::from))
            .filter(|key| !key.expose_secret().is_empty())
            .map(|api_key| Self { api_key })
    }

    /// Returns the key for the header that carries it.
    pub fn api_key(&self) -> &str {
        self.api_key.expose_secret()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const KEY: &str = "lse-test-key-0123456789";

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

    /// A product reads only its own variable, and an empty key counts as none.
    #[rstest]
    #[case::cme_key_for_lse(LseProduct::Global, LSE_CME_API_KEY, Some(KEY))]
    #[case::cme_key_for_nasdaq(LseProduct::Nasdaq, LSE_CME_API_KEY, Some(KEY))]
    #[case::unset(LseProduct::Global, LSE_API_KEY, None)]
    #[case::empty(LseProduct::Global, LSE_API_KEY, Some(""))]
    fn no_key_of_the_product_is_none(
        #[case] product: LseProduct,
        #[case] variable: &'static str,
        #[case] value: Option<&str>,
    ) {
        let credential = LseCredential::resolve(product, None, environment(variable, value));

        assert!(credential.is_none());
    }

    #[rstest]
    fn the_key_is_not_in_its_debug() {
        let credential =
            LseCredential::resolve(LseProduct::Cme, Some(SecretString::from(KEY)), |_| None)
                .unwrap();

        assert!(!format!("{credential:?}").contains(KEY));
    }
}
