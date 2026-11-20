//! fill-catalog of IB's history, end to end on the Docker paper gateway. The tests cover:
//!
//! - An hour of quotes. IB's history serves them as an instrument's historical BID_ASK ticks
//!   (`venues::serves`), and IB's writer pages back from the end, 1,000 ticks a request
//!   (`ib::page_back`). The catalog holds every quote of the hour, including those IB stamps in
//!   one second.
//! - Two probes of what IB answers across a gap in trading, which no offline test can tell: quotes
//!   across IDEALPRO's daily break, and 5-second bars across Xetra's night. The catalog holds each
//!   on both sides of the gap.
//!
//! Gated as `ib_gateway.rs`. They need what `ib_gateway.rs` needs and place no order. IB holds
//! 5-second bars six months back, so the bars' probe tells until May 2027:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test live ib_quote_fill:: -- --nocapture
//! ```
//!
//! Client id 51, one test at a time (`GATEWAY`).

use std::collections::BTreeMap;

use clap::{Args, FromArgMatches};
use helix_trader::{
    strategy::requirements::{DataRequirements, TickSeries},
    venues::{HistoryVenueArgs, fill_history},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::InstrumentId,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const EUR_USD: &str = "EUR/USD.IDEALPRO";

static GATEWAY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Returns whether the run is opted in, and claims the process (`claim_process`).
///
/// It reads the shell before `.env`, so that the file cannot opt in.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
        dotenvy::dotenv().ok();
    } else {
        println!("{test}: skipped; set NAUTILUS_IB_RUN=1 to run it against IB");
    }
    run
}

fn history_venues() -> HistoryVenueArgs {
    let matches = HistoryVenueArgs::augment_args(clap::Command::new("fill-catalog"))
        .try_get_matches_from(["fill-catalog", "--client-id", "51"])
        .unwrap();
    HistoryVenueArgs::from_arg_matches(&matches).unwrap()
}

/// Parses `time`, written `DDTHH:MM:SS`, as a time on a day of November 2026.
fn november(time: &str) -> Timestamp {
    format!("2026-11-{time}Z").parse().unwrap()
}

/// Returns the catalog of a fill of `requirements`, with its directory kept while it is read.
async fn filled(
    requirements: &DataRequirements,
    start: Timestamp,
    end: Timestamp,
) -> (ParquetDataCatalog, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    fill_history(requirements, start, end, &history_venues(), dir.path())
        .await
        .unwrap();
    (
        ParquetDataCatalog::new(dir.path(), None, None, None, None),
        dir,
    )
}

fn eur_usd_quotes() -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(EUR_USD),
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    }
}

/// fill-catalog fills the pair's definition and its quotes over an hour that FX trades through,
/// Thursday 12 November 2026 from 10:00 UTC.
///
/// The test checks that every quote is within the hour and of the pair, that each bid is at or
/// below its ask, and that the hour is covered.
#[tokio::test(flavor = "multi_thread")]
async fn fill_catalog_fills_an_hour_of_an_idealpro_pairs_quotes() {
    if !opted_in("fill_catalog_fills_an_hour_of_an_idealpro_pairs_quotes") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    let pair = InstrumentId::from(EUR_USD);
    let start = november("12T10:00:00");
    let end = start + SignedDuration::from_hours(1);

    let (mut catalog, _dir) = filled(&eur_usd_quotes(), start, end).await;

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

/// IDEALPRO trades Sunday to Friday from 17:15 to 17:00 New York time (IB's product listings). Its
/// daily break is 17:00 to 17:15, on Thursday 12 November 2026 22:00 to 22:15 UTC.
///
/// A fill of EUR/USD's quotes over 21:30 to 22:30 UTC pages back across the break. IB answers no
/// request across two sessions. A page ending in the break, answered empty, would end the paging
/// with the quotes after it alone. The catalog holds quotes on both sides.
#[tokio::test(flavor = "multi_thread")]
async fn a_quote_fill_across_idealpros_daily_break_holds_quotes_on_both_sides() {
    if !opted_in("a_quote_fill_across_idealpros_daily_break_holds_quotes_on_both_sides") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    let (start, end) = (november("12T21:30:00"), november("12T22:30:00"));
    let (closes, opens) = (november("12T22:00:00"), november("12T22:15:00"));

    let (mut catalog, _dir) = filled(&eur_usd_quotes(), start, end).await;

    let stamps: Vec<Timestamp> = catalog
        .quote_ticks(Some(vec![EUR_USD.to_string()]), None, None)
        .unwrap()
        .iter()
        .map(|quote| Timestamp::from(quote.ts_event))
        .collect();
    let before = stamps.iter().filter(|&&at| at < closes).count();
    let during = stamps
        .iter()
        .filter(|&&at| (closes..opens).contains(&at))
        .count();
    let after = stamps.iter().filter(|&&at| at >= opens).count();
    println!(
        "{EUR_USD}: {before} quotes before the break, {during} in it, {after} after it, \
         earliest {:?}, latest {:?}",
        stamps.iter().min(),
        stamps.iter().max()
    );
    assert!(before > 0, "no quote before the break");
    assert!(after > 0, "no quote after the break");
}

/// Xetra trades SXR8 09:00 to 17:30 Frankfurt time, 08:00 to 16:30 UTC in November.
///
/// A fill of its 5-second LAST bars from Thursday 12 November 2026 16:00 UTC to Friday 13 November
/// 08:30 UTC walks back across the night, an hour a request (`ib::request_duration`). A request
/// answered empty overnight would end the walk with the 13th's bars alone. The catalog holds the
/// 12th's last half hour and the 13th's first.
#[tokio::test(flavor = "multi_thread")]
async fn a_small_bar_fill_across_a_night_holds_bars_of_both_sessions() {
    if !opted_in("a_small_bar_fill_across_a_night_holds_bars_of_both_sessions") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    let bar_type = BarType::from("SXR8.IBIS-5-SECOND-LAST-EXTERNAL");
    let requirements = DataRequirements {
        bar_types: vec![bar_type],
        ticks: vec![],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let (start, end) = (november("12T16:00:00"), november("13T08:30:00"));
    let night = november("12T20:00:00");

    let (mut catalog, _dir) = filled(&requirements, start, end).await;

    let stamps: Vec<Timestamp> = catalog
        .bars(Some(vec![bar_type.to_string()]), None, None)
        .unwrap()
        .iter()
        .map(|bar| Timestamp::from(bar.ts_event))
        .collect();
    let (twelfth, thirteenth) = stamps
        .iter()
        .partition::<Vec<&Timestamp>, _>(|&&at| at < night);
    println!(
        "{bar_type}: {} bars of the 12th, {} of the 13th, earliest {:?}, latest {:?}",
        twelfth.len(),
        thirteenth.len(),
        stamps.iter().min(),
        stamps.iter().max()
    );
    assert!(!twelfth.is_empty(), "no bar of the 12th");
    assert!(!thirteenth.is_empty(), "no bar of the 13th");
}
