//! The listings of CME contracts in LSE's catalogue: a contract is asked of
//! LSE by the number of its listing, which names it alone, its exchange
//! name naming a contract every ten years (see `CmeContract`).

use anyhow::{Context, bail};
use nautilus_core::collections::AtomicMap;
use nautilus_model::identifiers::InstrumentId;

use super::{client::LseHttpClient, query::PairsQuery};
use crate::common::{consts::CME_MARKET, symbol::CmeContract};

/// The numbers of the listings of the CME contracts looked up so far: a
/// listing's number never changes, so each contract is looked up once.
#[derive(Debug, Default)]
pub(crate) struct CmeListings(AtomicMap<InstrumentId, u64>);

impl CmeListings {
    /// The number of `contract`'s listing (`pair_id`): the one found
    /// before, or the one LSE's catalogue lists under the contract's
    /// exchange names, looked up through `http` in the order
    /// `CmeContract::exchange_names` gives them, until one lists it.
    ///
    /// # Errors
    ///
    /// Returns an error naming the contract if LSE lists it under neither
    /// name, with the listings LSE has of them, if LSE lists it twice, or
    /// if a lookup fails (LSE's refusal in the error).
    pub(crate) async fn pair_id(
        &self,
        http: &LseHttpClient,
        contract: &CmeContract,
    ) -> anyhow::Result<u64> {
        let id = contract.instrument_id();
        if let Some(pair_id) = self.0.get_cloned(&id) {
            return Ok(pair_id);
        }
        let names = contract.exchange_names();
        let mut listed = Vec::new();
        for name in &names {
            let query = PairsQuery::new(CME_MARKET, name);
            let pairs = http
                .pairs(&query)
                .await
                .with_context(|| format!("looking up {query}"))?;
            if let Some(listing) = contract.listing(&pairs)? {
                log::info!("{contract} is LSE's listing {listing}");
                self.0.insert(id, listing.pair_id);
                return Ok(listing.pair_id);
            }
            listed.extend(pairs);
        }
        bail!(
            "LSE lists no listing of {contract} in its {CME_MARKET} market: under {} it lists {}",
            names.join(" and "),
            if listed.is_empty() {
                "none".to_string()
            } else {
                listed
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use nautilus_core::string::secret::SecretString;

    use super::*;
    use crate::{
        common::credential::{LseCredential, LseProduct},
        testing::{
            CME_API_KEY, MockLse, PAIRS_ESZ6, PAIRS_ESZ26, PAIRS_GCZ6, PAIRS_NGZ6, PAIRS_NGZ26,
            Reply,
        },
    };

    fn client(lse: &MockLse) -> LseHttpClient {
        let credential = LseCredential::resolve(
            LseProduct::Cme,
            Some(SecretString::from(CME_API_KEY)),
            |_| None,
        )
        .unwrap();
        LseHttpClient::new(
            lse.url().to_string(),
            &credential,
            NonZeroU32::new(6000).unwrap(),
        )
        .unwrap()
    }

    fn contract(id: &str) -> CmeContract {
        CmeContract::from_instrument_id(InstrumentId::from(id), 2026).unwrap()
    }

    /// The symbols looked up, in order.
    fn looked_up(lse: &MockLse) -> Vec<String> {
        lse.calls()
            .iter()
            .map(|call| {
                assert_eq!(
                    (call.path.as_str(), call.param("market")),
                    ("/v3/pairs", Some("cme"))
                );
                call.param("symbol").unwrap().to_string()
            })
            .collect()
    }

    /// LSE's GCZ6 of 2026 is its listing 4000000002.
    #[tokio::test]
    async fn a_contract_is_its_listing_under_its_exchange_name() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);

        let pair_id = CmeListings::default()
            .pair_id(&client(&lse), &contract("GCZ26.GLBX"))
            .await
            .unwrap();

        assert_eq!(pair_id, 4_000_000_002);
        assert_eq!(looked_up(&lse), ["GCZ6"]);
    }

    #[tokio::test]
    async fn a_contract_is_looked_up_once() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::json(PAIRS_GCZ6)]);
        let (listings, http) = (CmeListings::default(), client(&lse));

        let first = listings
            .pair_id(&http, &contract("GCZ26.GLBX"))
            .await
            .unwrap();
        let again = listings
            .pair_id(&http, &contract("GCZ26.GLBX"))
            .await
            .unwrap();

        assert_eq!((first, again), (4_000_000_002, 4_000_000_002));
        assert_eq!(looked_up(&lse), ["GCZ6"]);
    }

    /// LSE's NGZ6 is the contract of 2016; natural gas names 2026's NGZ26.
    #[tokio::test]
    async fn a_contract_not_listed_under_its_year_s_last_digit_is_looked_up_under_its_two() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_NGZ6), Reply::json(PAIRS_NGZ26)]);

        let pair_id = CmeListings::default()
            .pair_id(&client(&lse), &contract("NGZ26.GLBX"))
            .await
            .unwrap();

        assert_eq!(pair_id, 4_000_000_128);
        assert_eq!(looked_up(&lse), ["NGZ6", "NGZ26"]);
    }

    /// ESZ36: LSE's ESZ6 names the contracts of 2016 and 2026, and its
    /// ESZ36 nothing (LSE's answer for a name it lists nothing under,
    /// captured for ESZ26).
    #[tokio::test]
    async fn a_contract_lse_lists_under_neither_name_fails_naming_it_and_what_lse_lists() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6), Reply::json(PAIRS_ESZ26)]);

        let err = CmeListings::default()
            .pair_id(&client(&lse), &contract("ESZ36.GLBX"))
            .await
            .unwrap_err();

        let err = format!("{err:#}");
        for part in ["ESZ36.GLBX", "ESZ6 and ESZ36", "4000000001", "4000658762"] {
            assert!(err.contains(part), "{part}: {err}");
        }
    }
}
