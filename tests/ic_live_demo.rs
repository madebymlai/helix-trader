//! End to end on IC Markets' cTrader demo: `Live` on XAU/USD fills the
//! catalog up to now, builds a node with IC's clients, and a recording
//! strategy gets its minute bars warmed up from the catalog and the data
//! client up to now, then live quotes and the bars Nautilus builds from
//! them, until SIGINT. It needs the app's `IC_CLIENT_ID` and
//! `IC_CLIENT_SECRET`, the demo account's `IC_ACCESS_TOKEN`,
//! `IC_REFRESH_TOKEN` (they seed the token file, which then holds the
//! latest pair) and `IC_ACCOUNT_ID`, and `IC_ENVIRONMENT=demo`, from the environment or
//! `.env` (any other environment fails the test before connecting), and
//! gold's market open.
//!
//! Gated as `ic_demo.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_IC_RUN=1` is set in the shell. The strategy
//! sends no order; IC's execution client reconciles the account. One node per
//! process, hence its own file. The node keeps its cache in Redis
//! (`live::redis`) under the test's trader, `HELIXTEST-101`, whose records
//! the test deletes once the node has stopped cleanly. Run it with:
//!
//! ```sh
//! NAUTILUS_IC_RUN=1 cargo test --test ic_live_demo -- --nocapture
//! ```

mod common;

use std::{cell::RefCell, process::Command, rc::Rc, time::Duration};

use helix_trader::{
    live::Live,
    strategy::{DataRequirements, warmup},
};
use jiff::SignedDuration;
use nautilus_common::{actor::DataActor, logging::logger::LoggerConfig};
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
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IC_RUN").is_ok_and(|value| value == "1");
    if !run {
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

/// What the recorder got, in order.
#[derive(Debug, Default)]
struct Record {
    warmup: Vec<Bar>,
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
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(Recorder);

impl DataActor for Recorder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        warmup::start(self, &[self.bar_type], self.warmup)?;
        self.subscribe_quotes(self.bar_type.instrument_id(), None, None);
        Ok(())
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        let bars = warmup::bars(self, quotes);
        self.record.borrow_mut().warmup.extend(bars);
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

/// Long enough for the fill, the node's start and two minute bars.
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
        record: Rc::clone(&record),
    };
    let catalog = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-101"),
        catalog: catalog.path().to_path_buf(),
        account: None,
        client_id: 1,
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
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
    let (warmup_bars, bars, quotes) = (&record.warmup, &record.bars, &record.quotes);
    println!(
        "{} warmup bars, last {:?}\n{} live bars: {bars:?}\n{} live quotes, first {:?}, last {:?}",
        warmup_bars.len(),
        warmup_bars.last(),
        bars.len(),
        quotes.len(),
        quotes.first(),
        quotes.last()
    );
    // The fill stored the warmup's quotes.
    let mut catalog = ParquetDataCatalog::new(catalog.path(), None, None, None, None);
    let stored = catalog
        .quote_ticks(Some(vec![XAUUSD.to_owned()]), None, None)
        .unwrap();
    assert!(!stored.is_empty(), "the start stored no quote");
    assert!(
        catalog
            .get_intervals(&NautilusDataType::QuoteTick.into(), Some(XAUUSD))
            .unwrap()
            .len()
            == 1
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
    assert!(quotes[0].ts_event > stored.last().unwrap().ts_event);
}
