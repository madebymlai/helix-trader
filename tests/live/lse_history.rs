//! End to end on LSE: fill-catalog fills one UTC day, Thursday 12 November
//! 2026, of LSE's gold and 10-year Treasury futures trades,
//! `GOLD-FUTURES.LSEDGE` and `US10Y-FUTURES.LSEDGE`, and of its gold spot
//! candles as 1-minute bars, `XAU/USD-COMMODITIES.LSEDGE`, a series LSE
//! serves only as candles, into a temporary catalog holding their
//! definitions, written by hand as LSE publishes none, and that day's row
//! of LSE's daily 10-year US bond yield, custom data of no instrument
//! (`lse::LseRow`) known at the day's end, through the node from LSE's
//! exports; five minutes of that day of the S&P 500's values as quotes,
//! `SPX-INDICES.LSEDGE`, from its tick slice; of the trades and quotes of
//! GCZ6 of 2026, `GCZ26.GLBX`, from LSE's CME Globex, its definition from
//! Databento; and of the trades and 1-minute bars of AAPL's 355 call of 20
//! November 2026, `AAPL  261120C00355000.OPRA`, from LSE's US options, its
//! definition from Databento. It needs the LSE key, `LSE_API_KEY`, a CME
//! Group key, `LSE_CME_API_KEY`, and Databento's, `DATABENTO_API_KEY`, from
//! the environment or `.env`; the free LSE key serves the derived series,
//! the bond yield and the option, CME Group Lite+ the CME contract's
//! Level 1. The free key's exports count against its rows a day;
//! Databento charges about $0.000001 for the CME definition and $0.000002
//! for the option's.
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
    data::{BarType, Data, NautilusDataType},
    enums::AggressorSide,
    identifiers::{InstrumentId, Symbol},
    instruments::{IndexInstrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const GOLD: &str = "GOLD-FUTURES.LSEDGE";
const US10Y: &str = "US10Y-FUTURES.LSEDGE";
const GOLD_SPOT: &str = "XAU/USD-COMMODITIES.LSEDGE";
const SPX: &str = "SPX-INDICES.LSEDGE";
const GCZ26: &str = "GCZ26.GLBX";
const AAPL_CALL: &str = "AAPL  261120C00355000.OPRA";

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

/// The venues' settings of fill-catalog, by default.
fn history_venues() -> HistoryVenueArgs {
    HistoryVenueArgs::from_arg_matches(
        &HistoryVenueArgs::augment_args(Command::new("fill-catalog"))
            .get_matches_from(["fill-catalog"]),
    )
    .unwrap()
}

/// Thursday 12 November 2026, a UTC day, closed.
fn the_day() -> (Timestamp, Timestamp) {
    let start: Timestamp = "2026-11-12T00:00:00Z".parse().unwrap();
    (
        start,
        start + SignedDuration::from_hours(24) - SignedDuration::from_nanos(1),
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
    let (start, end) = the_day();

    fill_history(&requirements, start, end, &history_venues(), dir.path())
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

/// Each bar closes on a minute of the day, LSE stamping its candles at
/// their open.
#[tokio::test(flavor = "multi_thread")]
async fn a_day_of_gold_spot_candles_fills_from_lse_as_bars_stamped_at_their_close() {
    if !opted_in("a_day_of_gold_spot_candles_fills_from_lse_as_bars_stamped_at_their_close") {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    // Gold spot to a cent; LSE gives it no volume.
    catalog
        .write_instruments(vec![index(GOLD_SPOT, "XAU/USD", "0.01")])
        .unwrap();
    let bar_type = format!("{GOLD_SPOT}-1-MINUTE-LAST-EXTERNAL");
    let requirements = DataRequirements {
        bar_types: vec![BarType::from(bar_type.as_str())],
        ticks: vec![],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let (start, end) = the_day();

    fill_history(&requirements, start, end, &history_venues(), dir.path())
        .await
        .unwrap();

    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let bars = catalog
        .bars(Some(vec![bar_type.clone()]), None, None)
        .unwrap();
    println!("{bar_type}: {} bars", bars.len());
    assert!(!bars.is_empty(), "no bars");
    assert!(
        bars.iter().all(|bar| (from..=to).contains(&bar.ts_event)
            && bar.ts_event.as_u64() % 60_000_000_000 == 0),
        "a bar outside the day or off the minute"
    );
    let lacking = catalog
        .get_missing_intervals_for_request(
            from.as_u64(),
            to.as_u64(),
            &NautilusDataType::Bar.into(),
            Some(&bar_type),
        )
        .unwrap();
    assert_eq!(lacking, []);
}

/// An index's ticks are its values, each a quote whose bid and ask are the
/// value, with no sizes, as data-ws streams them.
#[tokio::test(flavor = "multi_thread")]
async fn five_minutes_of_the_s_and_p_500_fill_from_lse_as_quotes() {
    if !opted_in("five_minutes_of_the_s_and_p_500_fill_from_lse_as_quotes") {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    // The index to a cent.
    catalog
        .write_instruments(vec![index(SPX, "SPX", "0.01")])
        .unwrap();
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(SPX),
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let start: Timestamp = "2026-11-12T15:00:00Z".parse().unwrap();
    let end = start + SignedDuration::from_mins(5) - SignedDuration::from_nanos(1);

    fill_history(&requirements, start, end, &history_venues(), dir.path())
        .await
        .unwrap();

    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let quotes = catalog
        .quote_ticks(Some(vec![SPX.to_string()]), None, None)
        .unwrap();
    println!("{SPX}: {} quotes", quotes.len());
    assert!(!quotes.is_empty(), "no quotes");
    assert!(
        quotes.iter().all(|q| (from..=to).contains(&q.ts_event)
            && q.bid_price == q.ask_price
            && q.bid_price.precision == 2
            && q.bid_size.is_zero()
            && q.ask_size.is_zero()),
        "a quote outside the window, of a spread, sized or off the definition's precision"
    );
    let lacking = catalog
        .get_missing_intervals_for_request(
            from.as_u64(),
            to.as_u64(),
            &NautilusDataType::QuoteTick.into(),
            Some(SPX),
        )
        .unwrap();
    assert_eq!(lacking, []);
}

/// The day's row, its close known at its end: the window from the day's
/// start to the next day's holds it, stamped at the next day's 00:00.
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
    let (start, _) = the_day();
    let end = start + SignedDuration::from_hours(24);

    fill_history(&requirements, start, end, &history_venues(), dir.path())
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
    let [row] = &rows[..] else {
        panic!("not one daily row: {rows:?}");
    };
    assert_eq!((row.ts_event, row.ts_init), (to, to), "{row:?}");
    assert!(row.fields["close"].is_number(), "no close: {row:?}");
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

/// A day of GCZ6 of 2026's quotes, about 2.6 million, is past the 2,500,000
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
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let (start, end) = the_day();

    fill_history(&requirements, start, end, &history_venues(), dir.path())
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

/// A listed option near the money that day, AAPL at about 355: its
/// trades, which OPRA reports with no side, and its 1-minute bars, each
/// closing on a minute of the day, at the precision of Databento's
/// definition.
#[tokio::test(flavor = "multi_thread")]
async fn a_day_of_an_option_s_trades_and_bars_fills_from_lse_its_definition_from_databento() {
    if !opted_in(
        "a_day_of_an_option_s_trades_and_bars_fills_from_lse_its_definition_from_databento",
    ) {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    let bar_type = format!("{AAPL_CALL}-1-MINUTE-LAST-EXTERNAL");
    let requirements = DataRequirements {
        bar_types: vec![BarType::from(bar_type.as_str())],
        ticks: vec![TickSeries {
            data: NautilusDataType::TradeTick,
            instrument_id: InstrumentId::from(AAPL_CALL),
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let (start, end) = the_day();

    fill_history(&requirements, start, end, &history_venues(), dir.path())
        .await
        .unwrap();

    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let definitions = catalog
        .instruments(Some(&[AAPL_CALL.to_string()]), None, None)
        .unwrap();
    let [InstrumentAny::OptionContract(definition)] = &definitions[..] else {
        panic!("{AAPL_CALL}: {definitions:?}");
    };
    assert_eq!(definition.raw_symbol.as_str(), "AAPL  261120C00355000");
    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let trades = catalog
        .trade_ticks(Some(vec![AAPL_CALL.to_string()]), None, None)
        .unwrap();
    let bars = catalog
        .bars(Some(vec![bar_type.clone()]), None, None)
        .unwrap();
    println!("{AAPL_CALL}: {} trades, {} bars", trades.len(), bars.len());
    assert!(!trades.is_empty() && !bars.is_empty());
    assert!(
        trades.iter().all(|t| (from..=to).contains(&t.ts_event)
            && t.aggressor_side == AggressorSide::NoAggressor
            && t.price.precision == definition.price_precision),
        "a trade outside the day, with a side or off the definition's precision"
    );
    assert!(
        bars.iter().all(|bar| (from..=to).contains(&bar.ts_event)
            && bar.ts_event.as_u64() % 60_000_000_000 == 0),
        "a bar outside the day or off the minute"
    );
    for (data, identifier) in [
        (NautilusDataType::TradeTick, AAPL_CALL),
        (NautilusDataType::Bar, bar_type.as_str()),
    ] {
        let lacking = catalog
            .get_missing_intervals_for_request(
                from.as_u64(),
                to.as_u64(),
                &data.clone().into(),
                Some(identifier),
            )
            .unwrap();
        assert_eq!(lacking, [], "{data}");
    }
}
