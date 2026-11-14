//! The listings of CME contracts in LSE's catalogue: a contract is asked of
//! LSE by the number of its listing, which names it alone, its exchange
//! name naming a contract every ten years (see `CmeContract`), and streamed
//! under the listing's symbol, the name LSE's CME stream takes it by.

use anyhow::{Context, bail};
use nautilus_core::collections::AtomicMap;
use nautilus_model::identifiers::InstrumentId;

use super::{client::LseHttpClient, models::LsePair, query::PairsQuery};
use crate::common::{consts::CME_MARKET, symbol::CmeContract};

/// The listings of the CME contracts looked up so far: a listing never
/// changes, so each contract is looked up once.
#[derive(Debug, Default)]
pub struct CmeListings(AtomicMap<InstrumentId, LsePair>);

impl CmeListings {
    /// `contract`'s listing: the one found before, or the one LSE's
    /// catalogue lists under the contract's exchange names, looked up
    /// through `http` in the order `CmeContract::exchange_names` gives them,
    /// until one lists it. Its symbol is the name LSE's CME stream takes the
    /// contract by, the HTTP API's (LSE's WebSocket documentation,
    /// "Contracts").
    ///
    /// # Errors
    ///
    /// Returns an error naming the contract if LSE lists it under neither
    /// name, with the listings LSE has of them, if LSE lists it twice, or
    /// if a lookup fails (LSE's refusal in the error).
    pub async fn listing(
        &self,
        http: &LseHttpClient,
        contract: &CmeContract,
    ) -> anyhow::Result<LsePair> {
        let id = contract.instrument_id();
        if let Some(listing) = self.0.get_cloned(&id) {
            return Ok(listing);
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
                self.0.insert(id, listing.clone());
                return Ok(listing.clone());
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
