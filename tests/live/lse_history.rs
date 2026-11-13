//! End to end on LSE: fill-catalog fills one UTC day, Thursday 12 November
//! 2026, of LSE's gold and 10-year Treasury futures trades,
//! `GOLD-FUTURES.LSEDGE` and `US10Y-FUTURES.LSEDGE`, into a temporary
//! catalog holding their definitions, written by hand as LSE publishes
//! none, and of LSE's daily 10-year US bond yield, custom data of no
//! instrument (`lse::LseRow`), through the node from LSE's exports. It
//! needs the LSE key, `LSE_API_KEY`, from the environment or `.env`; the
//! free key serves every series. The day's exports count against the key's
//! rows a day.
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
use lse::LseRow;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Data, NautilusDataType},
    enums::AggressorSide,
    identifiers::{InstrumentId, Symbol},
    instruments::{IndexInstrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const GOLD: &str = "GOLD-FUTURES.LSEDGE";
const US10Y: &str = "US10Y-FUTURES.LSEDGE";

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
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let start: Timestamp = "2026-11-12T00:00:00Z".parse().unwrap();
    let end = start + SignedDuration::from_hours(24) - SignedDuration::from_nanos(1);
    let venues = HistoryVenueArgs::from_arg_matches(
        &HistoryVenueArgs::augment_args(Command::new("fill-catalog"))
            .get_matches_from(["fill-catalog"]),
    )
    .unwrap();

    fill_history(&requirements, start, end, &venues, dir.path())
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

#[tokio::test(flavor = "multi_thread")]
async fn a_day_of_bond_yields_fills_from_lse_as_custom_data() {
    if !opted_in("a_day_of_bond_yields_fills_from_lse_as_custom_data") {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    let us10y = LseRow::data_type("bond_yields", "US10Y");
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![],
        custom: vec![us10y.clone()],
        warmup: SignedDuration::ZERO,
    };
    let start: Timestamp = "2026-11-12T00:00:00Z".parse().unwrap();
    let end = start + SignedDuration::from_hours(24) - SignedDuration::from_nanos(1);
    let venues = HistoryVenueArgs::from_arg_matches(
        &HistoryVenueArgs::augment_args(Command::new("fill-catalog"))
            .get_matches_from(["fill-catalog"]),
    )
    .unwrap();

    fill_history(&requirements, start, end, &venues, dir.path())
        .await
        .unwrap();

    LseRow::register();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let identifier = us10y.identifier().unwrap().to_string();
    let stored = catalog
        .query_custom_data_dynamic(
            us10y.type_name(),
            Some(std::slice::from_ref(&identifier)),
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    println!("{us10y}: {} rows", stored.len());
    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let rows: Vec<&LseRow> = stored
        .iter()
        .map(|data| {
            let Data::Custom(custom) = data else {
                panic!("not custom data: {data:?}");
            };
            assert_eq!(custom.data_type, us10y);
            custom.data.as_any().downcast_ref::<LseRow>().unwrap()
        })
        .collect();
    assert_eq!(rows.len(), 1, "one daily row");
    assert!(
        rows.iter()
            .all(|row| (from..=to).contains(&row.ts_init) && row.fields["close"].is_number()),
        "a row outside the day or with no close: {rows:?}"
    );
    let lacking = catalog
        .get_missing_intervals_for_request(
            from.as_u64(),
            to.as_u64(),
            &NautilusDataType::Custom {
                type_name: us10y.type_name().to_string(),
            }
            .into(),
            Some(&identifier),
        )
        .unwrap();
    assert_eq!(lacking, []);
}
