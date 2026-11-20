//! The listings of CME contracts in LSE's catalogue.
//!
//! Queries ask LSE for a contract by the listing's number, because the contract's exchange name
//! repeats every ten years (see `CmeContract`). The contract streams under the listing's symbol.

use anyhow::Context;
use nautilus_core::collections::AtomicMap;
use nautilus_model::identifiers::InstrumentId;

use super::{client::LseHttpClient, models::LsePair, query::PairsQuery};
use crate::common::{consts::CME_MARKET, symbol::CmeContract};

/// The listings looked up so far.
///
/// A listing never changes, so each contract is looked up once.
#[derive(Debug, Default)]
pub struct CmeListings(AtomicMap<InstrumentId, LsePair>);

impl CmeListings {
    /// Returns the listing of `contract`.
    ///
    /// Looks the contract up under each `CmeContract::exchange_names` name in order. The symbol is
    /// the name LSE's CME stream takes (LSE's WebSocket docs, "Contracts").
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - LSE lists the contract under neither name.
    /// - LSE lists the contract twice.
    /// - A lookup fails.
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
        anyhow::bail!(
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
