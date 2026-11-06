//! End to end on a cTrader broker's demo: `Live` on XAU/USD checks the
//! account, builds a node with the broker's clients and a catalog already
//! holding gold's quotes up to `CATALOG_BEHIND` before now (filled as
//! fill-catalog fills it), and a recording strategy gets its minute bars
//! warmed up over two windows from the catalog, then from the broker's data
//! client up to now, each bar reaching its indicator once, then live quotes and the bars Nautilus builds from them, until SIGINT;
//! the broker's answer is written back to the catalog, and the live quotes
//! the node receives are streamed into it. It needs what `ctrader_demo.rs`
//! needs, and gold's market open.
//!
//! Gated as `ctrader_demo.rs` is. The strategy sends no order; the broker's
//! execution client reconciles the account. One node per process, hence its
//! own module, run alone (`main.rs`). The node keeps its cache in Redis
//! (`live::redis`) under the test's trader, `HELIXTEST-101`, whose records
//! the test deletes once the node has stopped cleanly. Run it with:
//!
//! ```sh
//! NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_live_demo:: -- --nocapture
//! ```

use crate::common;

use std::{cell::RefCell, process::Command, rc::Rc, time::Duration};

use helix_trader::{
    live::Live,
    strategy::{DataRequirements, warmup::Warmup},
};
use jiff::SignedDuration;
use nautilus_common::{
    actor::{DataActor, indicators::SharedActorIndicator},
    logging::logger::LoggerConfig,
    messages::data::DataResponseEnvelope,
};
use nautilus_model::{
    data::{Bar, BarType, NautilusDataType, QuoteTick},
    identifiers::{StrategyId, TraderId},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

use crate::demo_broker::opted_in;

/// How far before now the catalog's quotes end.
const CATALOG_BEHIND: SignedDuration = SignedDuration::from_mins(10);

/// What the recorder got, in order.
#[derive(Debug, Default)]
struct Record {
    warmup_quotes: Vec<QuoteTick>,
    /// Why the warmup failed, if it did.
    warmup_failure: Option<String>,
    bars: Vec<Bar>,
    quotes: Vec<QuoteTick>,
}

/// Warms up its bars through a `Warmup`, as a real strategy does,
/// subscribes to the quotes, and records both. It sends no order.
#[derive(Debug)]
struct Recorder {
    core: StrategyCore,
    bar_type: BarType,
    warmup: Rc<Warmup>,
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(Recorder);

impl DataActor for Recorder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        Rc::clone(&self.warmup).start(self)?;
        self.subscribe_quotes(self.bar_type.instrument_id(), None, None);
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if let Err(e) = Rc::clone(&self.warmup).on_data_response(self, response) {
            self.record.borrow_mut().warmup_failure = Some(e.to_string());
        }
        Ok(())
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        self.record
            .borrow_mut()
            .warmup_quotes
            .extend_from_slice(quotes);
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
    let Some(broker) = opted_in(
        "live_gold_quotes_and_bars_built_from_them_follow_a_warmup_up_to_now",
        module_path!(),
    ) else {
        return;
    };
    let gold = broker.gold();
    let bar_type = BarType::from(format!("{gold}-1-MINUTE-MID-INTERNAL").as_str());
    // Over 23 hours: two windows.
    let warmup = SignedDuration::from_hours(26);
    let requirements = DataRequirements {
        bar_types: vec![bar_type],
        ticks: vec![],
        warmup,
    };
    let record = Rc::new(RefCell::new(Record::default()));
    let fed = Rc::new(crate::FedBars::default());
    let warming =
        Rc::new(Warmup::live(&requirements).feeding(Rc::clone(&fed) as SharedActorIndicator));
    let recorder = Recorder {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("RECORDER-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        bar_type,
        warmup: Rc::clone(&warming),
        record: Rc::clone(&record),
    };
    // The catalog: gold and its quotes over [now - warmup, now -
    // CATALOG_BEHIND], as fill-catalog stores them.
    let catalog = tempfile::tempdir().unwrap();
    let now = jiff::Timestamp::now();
    let catalog_end = now - CATALOG_BEHIND;
    crate::ctrader_demo::fill_quotes_through_node(
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
        venues: crate::venues(&[]),
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
        live.run(&requirements, &warming, recorder, None),
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
    let fed_bars = fed.bars.borrow();
    let (warmup_quotes, warmup_bars, bars, quotes) = (
        &record.warmup_quotes,
        &*fed_bars,
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
    // Responses over `[now - warmup, now]`, a window at a time, the
    // catalog's quotes, then the broker's for the rest, written back to the
    // catalog.
    assert_eq!(record.warmup_failure, None);
    assert!(!warming.is_warming(), "the warmup did not complete");
    assert!(!warmup_quotes.is_empty(), "no warmup quote");
    let catalog_end_ns = catalog_end.as_nanosecond();
    let from_catalog = warmup_quotes
        .iter()
        .filter(|q| i128::from(q.ts_event.as_u64()) <= catalog_end_ns)
        .count();
    let parquet = ParquetDataCatalog::new(catalog.path(), None, None, None, None);
    let stored_to = parquet
        .query_last_timestamp(
            &NautilusDataType::QuoteTick.into(),
            Some(gold.to_string().as_str()),
        )
        .unwrap()
        .unwrap();
    println!(
        "{from_catalog} warmup quotes up to the catalog's end {catalog_end}; \
         the catalog's quotes now end at {stored_to}"
    );
    assert!(from_catalog > 0, "no warmup quote from the catalog");
    assert!(
        from_catalog < warmup_quotes.len(),
        "no warmup quote from the broker"
    );
    // The broker's answer, written back: the catalog's quotes after its own end up
    // to the warmup's last are the warmup's from it. A check of the
    // catalog's end alone would pass on the quotes the node streams.
    let stored: Vec<QuoteTick> = ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .query(Some(vec![gold.to_string()]), None, None, None, None, true)
        .unwrap();
    let seen = |q: &QuoteTick| (q.ts_event, q.bid_price, q.ask_price);
    let warmup_last = warmup_quotes.last().unwrap();
    let warmup_end = warmup_last.ts_event;
    // cTrader's first spot after a subscription resends the latest tick, so
    // the first live quote can be the warmup's last again.
    let repeated = quotes.first().is_some_and(|q| seen(q) == seen(warmup_last));
    println!(
        "the first live quote repeats the warmup's last: {repeated}; \
         the catalog holds that tick {} time(s)",
        stored
            .iter()
            .filter(|q| seen(q) == seen(warmup_last))
            .count()
    );
    let mut written_back: Vec<_> = stored
        .iter()
        .filter(|q| i128::from(q.ts_event.as_u64()) > catalog_end_ns && q.ts_event <= warmup_end)
        .map(seen)
        .collect();
    let from_broker: Vec<_> = warmup_quotes
        .iter()
        .filter(|q| i128::from(q.ts_event.as_u64()) > catalog_end_ns)
        .map(seen)
        .collect();
    // The repeat, stamped as the warmup's last, is streamed beside it.
    if repeated && written_back.len() > from_broker.len() {
        written_back.pop();
    }
    assert_eq!(
        written_back, from_broker,
        "the broker's answer was not written back"
    );
    let from_window_start = i128::from(warmup_quotes[0].ts_event.as_u64())
        - started.checked_sub(warmup).unwrap().as_nanosecond();
    // Within gold's daily close, should the window start in it.
    assert!(
        (0..5_400_000_000_000).contains(&from_window_start),
        "the warmup starts {from_window_start} ns after the run's start - warmup"
    );
    assert!(
        warmup_quotes
            .windows(2)
            .all(|w| w[0].ts_event <= w[1].ts_event)
    );
    // Warm, up to now, each bar once: the first live bar is the minute after
    // the last warmup bar.
    assert!(warmup_bars.len() >= 20, "{} warmup bars", warmup_bars.len());
    assert!(
        warmup_bars
            .windows(2)
            .all(|w| w[0].ts_event < w[1].ts_event),
        "a warmup bar handed over twice or out of order"
    );
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
    assert!(quotes.iter().all(|q| q.instrument_id == gold));
    assert!(quotes.iter().all(|q| q.bid_price <= q.ask_price));
    assert!(quotes.iter().all(|q| q.ts_event.as_u64() % 1_000_000 == 0));
    assert!(quotes.windows(2).all(|w| w[0].ts_event <= w[1].ts_event));
    assert!(
        quotes
            .iter()
            .all(|q| q.bid_size == broker.gold_quote_size())
    );
    // The hand-off: past the one repeat allowed, every live quote is newer
    // than the warmup's last.
    let new = &quotes[usize::from(repeated)..];
    assert!(new.iter().all(|q| q.ts_event > warmup_end));
    // Streamed into the catalog: the new live quotes the strategy got, in
    // order, then those the node received until its clients disconnected
    // (see `live/README.md`, "Recording into the catalog").
    let streamed: Vec<_> = stored
        .iter()
        .filter(|q| q.ts_event > warmup_end)
        .map(seen)
        .collect();
    let got: Vec<_> = new.iter().map(seen).collect();
    println!("{} quotes streamed", streamed.len());
    assert!(
        streamed.starts_with(&got),
        "the live quotes were not streamed: {} streamed, {} got",
        streamed.len(),
        got.len()
    );
}
