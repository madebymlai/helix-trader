//! End to end on LSE: a node streams LSE's gold futures quotes,
//! `GOLD-FUTURES.LSEDGE`, for a minute through the LSE adapter's data
//! client on data-ws, routed LSEDGE as the live node routes it, the
//! series' definition written by hand into a temporary catalog, as LSE
//! publishes none, and asked of the client as a live warmup asks it: the
//! catalog answers. It needs the LSE key, `LSE_API_KEY`, from the
//! environment or `.env`; the free key streams the series, on one
//! connection a key, so no other live run may hold data-ws meanwhile.
//! LSE opens a subscription with the symbol's last tick, so a quote comes
//! while the market is closed too.
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
    data::QuoteTick,
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
