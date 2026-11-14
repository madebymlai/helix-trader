//! End to end on LSE: a node streams, for a minute each, through the LSE
//! adapter's data client routed as the live node routes it, LSE's gold
//! futures quotes, `GOLD-FUTURES.LSEDGE`, on data-ws, the series'
//! definition written by hand into a temporary catalog, as LSE publishes
//! none; and the Level 1 of GCZ6 of 2026, `GCZ26.GLBX`, its quotes and
//! trades, on LSE's CME stream, its definition Databento's (the crate's
//! `testing::gcz26`), in the catalog too. Each definition is asked of the
//! client as a live warmup asks one: the catalog answers. They need the
//! LSE key, `LSE_API_KEY`, and a CME Group key, `LSE_CME_API_KEY`, from the
//! environment or `.env`; the free key streams the gold series, on one
//! connection a key, so no other live run may hold data-ws meanwhile. LSE
//! opens a subscription with the symbol's last tick, and a CME contract
//! with its last best bid and offer, so a quote comes while the market is
//! closed too; trades come only while CME trades.
//!
//! Gated as `lse_history.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_LSE_RUN=1` is set in the shell. Run it with:
//!
//! ```sh
//! NAUTILUS_LSE_RUN=1 cargo test --test live lse_quotes:: -- --nocapture
//! ```

use std::{cell::RefCell, rc::Rc, time::Duration};

use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    nautilus_actor,
};
use nautilus_core::UnixNanos;
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveNodeConfig, RoutingConfig},
};
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::{ClientId, InstrumentId, Symbol, TraderId},
    instruments::{IndexInstrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};

const GOLD: &str = "GOLD-FUTURES.LSEDGE";

/// How long the quotes stream.
const STREAMED: Duration = Duration::from_secs(60);

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

/// The gold series' definition, as one is written by hand: an index in
/// USD, priced to a tenth, its sizes whole lots.
fn gold() -> InstrumentAny {
    InstrumentAny::IndexInstrument(
        IndexInstrument::builder()
            .instrument_id(InstrumentId::from(GOLD))
            .raw_symbol(Symbol::from("GOLD"))
            .currency(Currency::USD())
            .price_precision(1)
            .size_precision(0)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// Asks the gold series' definition of LSE's client, as a live warmup
/// does, then subscribes to its quotes, recording each.
#[derive(Debug)]
struct Quotes {
    core: DataActorCore,
    quotes: Rc<RefCell<Vec<QuoteTick>>>,
}

nautilus_actor!(Quotes);

impl DataActor for Quotes {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let gold = InstrumentId::from(GOLD);
        self.request_instrument(gold, None, None, Some(ClientId::from("LSE")), None)?;
        self.subscribe_quotes(gold, None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.quotes.borrow_mut().push(*quote);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn gold_futures_quotes_stream_from_lse_for_a_minute() {
    if !opted_in("gold_futures_quotes_stream_from_lse_for_a_minute") {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(vec![gold()])
        .unwrap();
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("LSE-QUOTES-001"),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
        )],
        ..Default::default()
    };
    let lse = lse::LseDataClientConfig::builder()
        .stream_ids(vec![InstrumentId::from(GOLD)])
        .build();
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-QUOTES")
        .with_delay_post_stop_secs(1)
        .add_data_client_with_routing(
            None,
            Box::new(lse::LseDataClientFactory::new()),
            Box::new(lse),
            RoutingConfig {
                default: false,
                venues: Some(vec!["LSEDGE".to_string()]),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let quotes: Rc<RefCell<Vec<QuoteTick>>> = Rc::default();
    node.add_actor(Quotes {
        core: DataActorCore::new(DataActorConfig::default()),
        quotes: Rc::clone(&quotes),
    })
    .unwrap();
    let handle = node.handle();

    let (ran, ()) = tokio::join!(node.run(), async {
        tokio::time::sleep(STREAMED).await;
        handle.stop();
    });
    ran.unwrap();
    node.dispose();

    let quotes = quotes.borrow();
    println!("{GOLD}: {} quotes in {STREAMED:?}", quotes.len());
    let first = quotes.first().expect("no quote of the gold series");
    println!("{GOLD}: first {first:?}");
    assert!(
        quotes.iter().all(|quote| quote.bid_price <= quote.ask_price
            && quote.bid_size.is_zero()
            && quote.ask_size.is_zero()
            && quote.bid_price.precision == 1),
        "a quote crossed, sized or at another precision"
    );
}

/// Asks GCZ26's definition of LSE's client, as a live warmup does, then
/// subscribes to its quotes and trades, recording each.
#[derive(Debug)]
struct Level1 {
    core: DataActorCore,
    quotes: Rc<RefCell<Vec<QuoteTick>>>,
    trades: Rc<RefCell<Vec<TradeTick>>>,
}

nautilus_actor!(Level1);

impl DataActor for Level1 {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let gcz26 = InstrumentId::from(lse::testing::GCZ26);
        self.request_instrument(gcz26, None, None, Some(ClientId::from("LSE")), None)?;
        self.subscribe_quotes(gcz26, None, None);
        self.subscribe_trades(gcz26, None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.quotes.borrow_mut().push(*quote);
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.trades.borrow_mut().push(*trade);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_gc_contract_s_level_1_streams_from_lse_s_cme_stream_for_a_minute() {
    if !opted_in("a_gc_contract_s_level_1_streams_from_lse_s_cme_stream_for_a_minute") {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(vec![lse::testing::gcz26()])
        .unwrap();
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("LSE-CME-001"),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
        )],
        ..Default::default()
    };
    let gcz26 = InstrumentId::from(lse::testing::GCZ26);
    let lse = lse::LseDataClientConfig::builder()
        .stream_ids(vec![gcz26])
        .build();
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-CME")
        .with_delay_post_stop_secs(1)
        .add_data_client_with_routing(
            None,
            Box::new(lse::LseDataClientFactory::new()),
            Box::new(lse),
            RoutingConfig {
                default: false,
                venues: Some(vec!["GLBX".to_string()]),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let quotes: Rc<RefCell<Vec<QuoteTick>>> = Rc::default();
    let trades: Rc<RefCell<Vec<TradeTick>>> = Rc::default();
    node.add_actor(Level1 {
        core: DataActorCore::new(DataActorConfig::default()),
        quotes: Rc::clone(&quotes),
        trades: Rc::clone(&trades),
    })
    .unwrap();
    let handle = node.handle();

    let (ran, ()) = tokio::join!(node.run(), async {
        tokio::time::sleep(STREAMED).await;
        handle.stop();
    });
    ran.unwrap();
    node.dispose();

    let (quotes, trades) = (quotes.borrow(), trades.borrow());
    println!(
        "{gcz26}: {} quotes and {} trades in {STREAMED:?}",
        quotes.len(),
        trades.len()
    );
    let first = quotes
        .first()
        .expect("no quote of GCZ26, not even its opening");
    println!("{gcz26}: first {first:?}");
    if let Some(trade) = trades.first() {
        println!("{gcz26}: first trade {trade:?}");
    }
    assert!(
        quotes.iter().all(|quote| quote.bid_price < quote.ask_price
            && quote.bid_price.precision == 2
            && quote.ts_event.as_u64() > 1_000_000_000_000_000_000),
        "a quote crossed, at another precision or not in nanoseconds"
    );
    assert!(
        trades
            .iter()
            .all(|trade| trade.aggressor_side == AggressorSide::NoAggressor
                && trade.price.precision == 2),
        "a trade with an aggressor side or at another precision"
    );
}
