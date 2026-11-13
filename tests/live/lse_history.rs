//! End to end on LSE: fill-catalog fills one UTC day, Thursday 12 November
//! 2026, into a temporary catalog, through the node from LSE's exports: of
//! LSE's gold and 10-year Treasury futures trades, `GOLD-FUTURES.LSEDGE`
//! and `US10Y-FUTURES.LSEDGE`, the catalog holding their definitions,
//! written by hand as LSE publishes none; and of the trades and quotes of
//! GCZ6 of 2026, `GCZ26.GLBX`, from LSE's CME Globex, its definition from
//! Databento. It needs the LSE key, `LSE_API_KEY`, a CME Group key,
//! `LSE_CME_API_KEY`, and Databento's, `DATABENTO_API_KEY`, from the
//! environment or `.env`; the free LSE key serves the derived series, CME
//! Group Lite+ the CME contract's Level 1. The derived series' exports
//! count against the free key's rows a day; Databento charges about
//! $0.000001 for the definition.
//!
//! Gated as `okx_demo.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_LSE_RUN=1` is set in the shell. Run it with:
//!
//! ```sh
//! NAUTILUS_LSE_RUN=1 cargo test --test live lse_history:: -- --nocapture
//! ```

use clap::{Args, Command, FromArgMatches};
use helix_trader::{
    strategy::{DataRequirements, TickSeries},
    venues::{HistoryVenueArgs, fill_history},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::NautilusDataType,
    enums::AggressorSide,
    identifiers::{InstrumentId, Symbol},
    instruments::{IndexInstrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const GOLD: &str = "GOLD-FUTURES.LSEDGE";
const US10Y: &str = "US10Y-FUTURES.LSEDGE";
const GCZ26: &str = "GCZ26.GLBX";

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
/// An opted-in run claims the process for this module (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_LSE_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_LSE_RUN=1 to run it on LSE");
    }
    run
}

/// A definition of the LSE series `id`, `raw` in LSE, as one is written
/// by hand: an index in USD, priced to `tick`, its sizes whole lots.
fn index(id: &str, raw: &str, tick: &str) -> InstrumentAny {
    let tick = Price::from(tick);
    InstrumentAny::IndexInstrument(
        IndexInstrument::builder()
            .instrument_id(InstrumentId::from(id))
            .raw_symbol(Symbol::from(raw))
            .currency(Currency::USD())
            .price_precision(tick.precision)
            .size_precision(0)
            .price_increment(tick)
            .size_increment(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The UTC day filled, Thursday 12 November 2026: `[start, end]`.
fn the_12th() -> (Timestamp, Timestamp) {
    let start: Timestamp = "2026-11-12T00:00:00Z".parse().unwrap();
    (
        start,
        start + SignedDuration::from_hours(24) - SignedDuration::from_nanos(1),
    )
}

/// fill-catalog's venue flags, none given.
fn venues() -> HistoryVenueArgs {
    HistoryVenueArgs::from_arg_matches(
        &HistoryVenueArgs::augment_args(Command::new("fill-catalog"))
            .get_matches_from(["fill-catalog"]),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_day_of_gold_and_treasury_futures_trades_fills_from_lse() {
    if !opted_in("a_day_of_gold_and_treasury_futures_trades_fills_from_lse") {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    // Gold to a tenth of a dollar, the 10-year note to a 64th of a point.
    catalog
        .write_instruments(vec![
            index(GOLD, "GOLD", "0.1"),
            index(US10Y, "US10Y", "0.015625"),
        ])
        .unwrap();
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: [GOLD, US10Y]
            .map(|id| TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from(id),
            })
            .to_vec(),
        warmup: SignedDuration::ZERO,
    };
    let (start, end) = the_12th();

    fill_history(&requirements, start, end, &venues(), dir.path())
        .await
        .unwrap();

    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    for id in [GOLD, US10Y] {
        let trades = catalog
            .trade_ticks(Some(vec![id.to_string()]), None, None)
            .unwrap();
        println!("{id}: {} trades", trades.len());
        assert!(!trades.is_empty(), "{id}: no trades");
        assert!(
            trades.iter().all(|t| (from..=to).contains(&t.ts_event)
                && t.aggressor_side == AggressorSide::NoAggressor),
            "{id}: a trade outside the day or with a side"
        );
        let lacking = catalog
            .get_missing_intervals_for_request(
                from.as_u64(),
                to.as_u64(),
                &NautilusDataType::TradeTick.into(),
                Some(id),
            )
            .unwrap();
        assert_eq!(lacking, [], "{id}");
    }
}

/// A day of GCZ6 of 2026's quotes, about 2.7 million, is past the 2,500,000
/// rows one export may read, so LSE is asked for it in halves.
#[tokio::test(flavor = "multi_thread")]
async fn a_day_of_a_gc_contract_s_trades_and_quotes_fills_from_lse_its_definition_from_databento() {
    if !opted_in(
        "a_day_of_a_gc_contract_s_trades_and_quotes_fills_from_lse_its_definition_from_databento",
    ) {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: [NautilusDataType::TradeTick, NautilusDataType::QuoteTick]
            .map(|data| TickSeries {
                data,
                instrument_id: InstrumentId::from(GCZ26),
            })
            .to_vec(),
        warmup: SignedDuration::ZERO,
    };
    let (start, end) = the_12th();

    fill_history(&requirements, start, end, &venues(), dir.path())
        .await
        .unwrap();

    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let definitions = catalog
        .instruments(Some(&[GCZ26.to_string()]), None, None)
        .unwrap();
    let [InstrumentAny::FuturesContract(definition)] = &definitions[..] else {
        panic!("{GCZ26}: {definitions:?}");
    };
    assert_eq!(definition.raw_symbol.as_str(), "GCZ6");
    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let trades = catalog
        .trade_ticks(Some(vec![GCZ26.to_string()]), None, None)
        .unwrap();
    let quotes = catalog
        .quote_ticks(Some(vec![GCZ26.to_string()]), None, None)
        .unwrap();
    println!("{GCZ26}: {} trades, {} quotes", trades.len(), quotes.len());
    assert!(!trades.is_empty() && !quotes.is_empty());
    assert!(
        trades.iter().all(|t| (from..=to).contains(&t.ts_event)
            && t.aggressor_side == AggressorSide::NoAggressor
            && t.price.precision == definition.price_precision),
        "a trade outside the day, with a side or off the definition's precision"
    );
    assert!(
        quotes
            .iter()
            .all(|q| (from..=to).contains(&q.ts_event) && q.bid_price < q.ask_price),
        "a quote outside the day or crossed"
    );
    for data in [NautilusDataType::TradeTick, NautilusDataType::QuoteTick] {
        let lacking = catalog
            .get_missing_intervals_for_request(
                from.as_u64(),
                to.as_u64(),
                &data.clone().into(),
                Some(GCZ26),
            )
            .unwrap();
        assert_eq!(lacking, [], "{data}");
    }
}
