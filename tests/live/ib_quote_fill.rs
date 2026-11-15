//! fill-catalog of an hour of quotes from IB, end to end on the Docker
//! paper gateway: IB's history serves an instrument's quotes as its
//! historical BID_ASK ticks (`venues::serves`), which IB's writer asks of
//! Nautilus' historical client (`request_ticks`, a page of 1,000 ticks at
//! a time back from the end), and the catalog holds every quote of the
//! hour, those IB stamps in one second among them.
//!
//! Gated as `ib_gateway.rs`: it returns at once, saying it was skipped,
//! unless `NAUTILUS_IB_RUN=1` is set in the shell. It needs what
//! `ib_gateway.rs` needs and places no order:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test live ib_quote_fill:: -- --nocapture
//! ```
//!
//! Client id 51.

use std::collections::BTreeMap;

use clap::{Args, FromArgMatches};
use helix_trader::{
    strategy::requirements::{DataRequirements, TickSeries},
    venues::{HistoryVenueArgs, fill_history},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{data::NautilusDataType, identifiers::InstrumentId};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

/// EUR/USD on IDEALPRO, IB's cash FX pair, quoted through the hour.
const EUR_USD: &str = "EUR/USD.IDEALPRO";

/// The venues' settings of fill-catalog, on client id 51.
fn history_venues() -> HistoryVenueArgs {
    let matches = HistoryVenueArgs::augment_args(clap::Command::new("fill-catalog"))
        .try_get_matches_from(["fill-catalog", "--client-id", "51"])
        .unwrap();
    HistoryVenueArgs::from_arg_matches(&matches).unwrap()
}

/// fill-catalog fills the pair's definition and its quotes over an hour FX
/// trades through, Thursday 12 November 2026 from 10:00 UTC: every quote
/// within the hour, of the pair, its bid at or below its ask, the hour
/// covered.
#[tokio::test(flavor = "multi_thread")]
async fn fill_catalog_fills_an_hour_of_an_idealpro_pairs_quotes() {
    if !std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1") {
        println!("skipped; set NAUTILUS_IB_RUN=1 to run it against IB");
        return;
    }
    crate::claim_process(module_path!());
    dotenvy::dotenv().ok();
    let pair = InstrumentId::from(EUR_USD);
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: pair,
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let start: Timestamp = "2026-11-12T10:00:00Z".parse().unwrap();
    let end = start + SignedDuration::from_hours(1);
    let dir = tempfile::tempdir().unwrap();

    fill_history(&requirements, start, end, &history_venues(), dir.path())
        .await
        .unwrap();

    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let defined = catalog
        .instruments(Some(&[EUR_USD.to_string()]), None, None)
        .unwrap();
    assert_eq!(defined.len(), 1, "{EUR_USD} is not defined once");
    let quotes = catalog
        .quote_ticks(Some(vec![EUR_USD.to_string()]), None, None)
        .unwrap();
    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let mut per_second: BTreeMap<u64, usize> = BTreeMap::new();
    for quote in &quotes {
        *per_second
            .entry(quote.ts_event.as_u64() / 1_000_000_000)
            .or_default() += 1;
    }
    println!(
        "{EUR_USD}: {} quotes over {} seconds, at most {} in one",
        quotes.len(),
        per_second.len(),
        per_second.values().max().copied().unwrap_or_default()
    );
    assert!(!quotes.is_empty(), "no quote of {EUR_USD}");
    assert!(
        quotes.iter().all(|quote| quote.instrument_id == pair
            && (from..=to).contains(&quote.ts_event)
            && quote.bid_price <= quote.ask_price),
        "a quote of another instrument, outside the hour or crossed"
    );
    let lacking = catalog
        .get_missing_intervals_for_request(
            from.as_u64(),
            to.as_u64(),
            &NautilusDataType::QuoteTick.into(),
            Some(EUR_USD),
        )
        .unwrap();
    assert_eq!(lacking, [], "{EUR_USD}");
}
