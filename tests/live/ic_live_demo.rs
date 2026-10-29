//! End to end on IC Markets' cTrader demo: `Live` on XAU/USD checks the
//! account, builds a node with IC's clients and a catalog already holding
//! gold's quotes up to `CATALOG_BEHIND` before now (filled as fill-catalog
//! fills it), and a recording strategy gets its minute bars warmed up from
//! the catalog, then from IC's data client up to now, then live quotes and
//! the bars Nautilus builds from them, until SIGINT; IC's answer is written
//! back to the catalog, and the live quotes the node receives are streamed
//! into it. It needs the app's `IC_CLIENT_ID` and
//! `IC_CLIENT_SECRET`, the demo account's `IC_ACCESS_TOKEN`,
//! `IC_REFRESH_TOKEN` (they seed the token file, which then holds the
//! latest pair) and `IC_ACCOUNT_ID`, and `IC_ENVIRONMENT=demo`, from the environment or
//! `.env` (any other environment fails the test before connecting), and
//! gold's market open.
//!
//! Gated as `ic_demo.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_IC_RUN=1` is set in the shell. The strategy
//! sends no order; IC's execution client reconciles the account. One node per
//! process, hence its own module, run alone (`main.rs`). The node keeps its cache in Redis
//! (`live::redis`) under the test's trader, `HELIXTEST-101`, whose records
//! the test deletes once the node has stopped cleanly. Run it with:
//!
//! ```sh
//! NAUTILUS_IC_RUN=1 cargo test --test live ic_live_demo:: -- --nocapture
//! ```

use crate::common;

use std::{cell::RefCell, process::Command, rc::Rc, time::Duration};

use helix_trader::{
    live::Live,
    rolls::ContinuousBars,
    strategy::{DataRequirements, warmup},
};
use jiff::SignedDuration;
use nautilus_common::{
    actor::DataActor, logging::logger::LoggerConfig, messages::data::DataResponseEnvelope,
};
use nautilus_core::UUID4;
use nautilus_model::{
    data::{Bar, BarType, NautilusDataType, QuoteTick},
    identifiers::{InstrumentId, StrategyId, TraderId},
    types::Quantity,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
/// An opted-in run claims the process for this module (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IC_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_IC_RUN=1 to run it on IC's cTrader demo");
    }
    run
}

/// Reads `.env` and refuses any environment but the demo.
fn demo_only() {
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("IC_ENVIRONMENT").as_deref(),
        Ok("demo"),
        "the IC end to end test runs on the demo only: set IC_ENVIRONMENT=demo"
    );
}

const XAUUSD: &str = "XAUUSD.ICMARKETS";

/// How far before now the catalog's quotes end.
const CATALOG_BEHIND: SignedDuration = SignedDuration::from_mins(10);

/// What the recorder got, in order.
#[derive(Debug, Default)]
struct Record {
    warmup_quotes: Vec<QuoteTick>,
    warmup: Vec<Bar>,
    /// How the warmup ended: `Ok` complete, or the error.
    warmup_end: Option<Result<(), String>>,
    bars: Vec<Bar>,
    quotes: Vec<QuoteTick>,
}

/// Warms up its bars through `strategy::warmup`, as a real strategy does,
/// subscribes to the quotes, and records both. It sends no order.
#[derive(Debug)]
struct Recorder {
    core: StrategyCore,
    bar_type: BarType,
    warmup: SignedDuration,
    warmup_group: UUID4,
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(Recorder);

impl DataActor for Recorder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        warmup::start(
            self,
            &[self.bar_type],
            self.warmup,
            &ContinuousBars::default(),
            self.warmup_group,
        )?;
        self.subscribe_quotes(self.bar_type.instrument_id(), None, None);
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        let end = match warmup::finished(self.warmup_group, response) {
            Ok(false) => return Ok(()),
            Ok(true) => Ok(()),
            Err(e) => Err(e.to_string()),
        };
        self.record.borrow_mut().warmup_end = Some(end);
        Ok(())
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        let bars = warmup::bars(self, quotes);
        let mut record = self.record.borrow_mut();
        record.warmup_quotes.extend_from_slice(quotes);
        record.warmup.extend(bars);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.record.borrow_mut().quotes.push(*quote);
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.record.borrow_mut().bars.push(*bar);
        Ok(())
    }
}

/// Long enough for the node's start, the warmup and two minute bars.
const LIVE_RUN: Duration = Duration::from_secs(150);

#[tokio::test(flavor = "multi_thread")]
async fn live_gold_quotes_and_bars_built_from_them_follow_a_warmup_up_to_now() {
    if !opted_in("live_gold_quotes_and_bars_built_from_them_follow_a_warmup_up_to_now") {
        return;
    }
    demo_only();
    let bar_type = BarType::from(format!("{XAUUSD}-1-MINUTE-MID-INTERNAL").as_str());
    let warmup = SignedDuration::from_mins(30);
    let requirements = DataRequirements {
        bar_types: vec![bar_type],
        ticks: vec![],
        warmup,
    };
    let record = Rc::new(RefCell::new(Record::default()));
    let recorder = Recorder {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("RECORDER-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        bar_type,
        warmup,
        warmup_group: UUID4::new(),
        record: Rc::clone(&record),
    };
    // The catalog: gold and its quotes over [now - warmup, now -
    // CATALOG_BEHIND], as fill-catalog stores them.
    let catalog = tempfile::tempdir().unwrap();
    let now = jiff::Timestamp::now();
    let catalog_end = now - CATALOG_BEHIND;
    crate::ic_demo::fill_quotes_through_node(
        catalog.path(),
        bar_type.instrument_id(),
        now - warmup,
        catalog_end,
    )
    .await
    .unwrap();
    let started = jiff::Timestamp::now();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-101"),
        account: None,
        client_id: 1,
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };
    // As Ctrl-C would, once the node runs: its signal handler stops it.
    let interrupt = tokio::spawn(async {
        tokio::time::sleep(LIVE_RUN).await;
        let status = Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    });

    let ran = tokio::time::timeout(
        LIVE_RUN + Duration::from_secs(60),
        live.run(&requirements, recorder),
    )
    .await;

    let interrupted = interrupt.is_finished();
    interrupt.abort();
    ran.expect("the node stops within a minute of SIGINT")
        .unwrap();
    assert!(interrupted, "the node stopped before SIGINT");
    // The node's records in Redis, under the test's trader.
    common::cache::delete_trader("HELIXTEST-101").await;
    let record = record.borrow();
    let (warmup_quotes, warmup_bars, bars, quotes) = (
        &record.warmup_quotes,
        &record.warmup,
        &record.bars,
        &record.quotes,
    );
    println!(
        "{} warmup quotes, first {:?}, last {:?}\n{} warmup bars, last {:?}\n\
         {} live bars: {bars:?}\n{} live quotes, first {:?}, last {:?}",
        warmup_quotes.len(),
        warmup_quotes.first(),
        warmup_quotes.last(),
        warmup_bars.len(),
        warmup_bars.last(),
        bars.len(),
        quotes.len(),
        quotes.first(),
        quotes.last()
    );
    // One response over `[now - warmup, now]`, the catalog's quotes, then
    // IC's for the rest, written back to the catalog.
    assert_eq!(
        record.warmup_end,
        Some(Ok(())),
        "the warmup did not complete"
    );
    assert!(!warmup_quotes.is_empty(), "no warmup quote");
    let catalog_end_ns = catalog_end.as_nanosecond();
    let from_catalog = warmup_quotes
        .iter()
        .filter(|q| i128::from(q.ts_event.as_u64()) <= catalog_end_ns)
        .count();
    let parquet = ParquetDataCatalog::new(catalog.path(), None, None, None, None);
    let stored_to = parquet
        .query_last_timestamp(&NautilusDataType::QuoteTick.into(), Some(XAUUSD))
        .unwrap()
        .unwrap();
    println!(
        "{from_catalog} warmup quotes up to the catalog's end {catalog_end}; \
         the catalog's quotes now end at {stored_to}"
    );
    assert!(from_catalog > 0, "no warmup quote from the catalog");
    assert!(
        from_catalog < warmup_quotes.len(),
        "no warmup quote from IC"
    );
    // IC's answer, written back: the catalog's quotes after its own end up
    // to the warmup's last are the warmup's from IC. A check of the
    // catalog's end alone would pass on the quotes the node streams.
    let stored: Vec<QuoteTick> = ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .query(Some(vec![XAUUSD.to_owned()]), None, None, None, None, true)
        .unwrap();
    let seen = |q: &QuoteTick| (q.ts_event, q.bid_price, q.ask_price);
    let warmup_end = warmup_quotes.last().unwrap().ts_event;
    let written_back: Vec<_> = stored
        .iter()
        .filter(|q| i128::from(q.ts_event.as_u64()) > catalog_end_ns && q.ts_event <= warmup_end)
        .map(seen)
        .collect();
    let from_ic: Vec<_> = warmup_quotes
        .iter()
        .filter(|q| i128::from(q.ts_event.as_u64()) > catalog_end_ns)
        .map(seen)
        .collect();
    assert_eq!(written_back, from_ic, "IC's answer was not written back");
    let from_window_start = i128::from(warmup_quotes[0].ts_event.as_u64())
        - started.checked_sub(warmup).unwrap().as_nanosecond();
    assert!(
        (0..120_000_000_000).contains(&from_window_start),
        "the warmup starts {from_window_start} ns after the run's start - warmup"
    );
    assert!(
        warmup_quotes
            .windows(2)
            .all(|w| w[0].ts_event <= w[1].ts_event)
    );
    // Warm, up to now: the first live bar is the minute after the last
    // warmup bar.
    assert!(warmup_bars.len() >= 20, "{} warmup bars", warmup_bars.len());
    assert!(bars.len() >= 2, "{} live bars", bars.len());
    let minute = 60_000_000_000;
    assert_eq!(
        bars[0].ts_event.as_u64() - warmup_bars.last().unwrap().ts_event.as_u64(),
        minute
    );
    assert!(
        bars.windows(2)
            .all(|w| w[1].ts_event.as_u64() - w[0].ts_event.as_u64() == minute)
    );
    // Live quotes: at the spot's time, sized as the history's, ordered.
    assert!(!quotes.is_empty());
    assert!(
        quotes
            .iter()
            .all(|q| q.instrument_id == InstrumentId::from(XAUUSD))
    );
    assert!(quotes.iter().all(|q| q.bid_price <= q.ask_price));
    assert!(quotes.iter().all(|q| q.ts_event.as_u64() % 1_000_000 == 0));
    assert!(quotes.windows(2).all(|w| w[0].ts_event <= w[1].ts_event));
    assert!(quotes.iter().all(|q| q.bid_size == Quantity::from(10_000)));
    // The hand-off: no live quote repeats the warmup's last.
    assert!(quotes[0].ts_event > warmup_quotes.last().unwrap().ts_event);
    // Streamed into the catalog: the live quotes the strategy got, in order,
    // then those the node received until its clients disconnected (see
    // `live/README.md`, "Recording into the catalog").
    let streamed: Vec<_> = stored
        .iter()
        .filter(|q| q.ts_event >= quotes[0].ts_event)
        .map(seen)
        .collect();
    let got: Vec<_> = quotes.iter().map(seen).collect();
    println!("{} quotes streamed", streamed.len());
    assert!(
        streamed.starts_with(&got),
        "the live quotes were not streamed: {} streamed, {} got",
        streamed.len(),
        got.len()
    );
}
