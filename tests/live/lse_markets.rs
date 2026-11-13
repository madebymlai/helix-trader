//! End to end on LSE: a node streams the quotes of a series of each LSE
//! derived market through the LSE adapter's data client, each on the
//! socket that carries its market, routed LSEDGE as the live node routes
//! it, the definitions written by hand into a temporary catalog, as LSE
//! publishes none, and asked of the client as a live warmup asks them: the
//! catalog answers. LSE opens a subscription with the symbol's last tick,
//! so each series quotes at once while its market is closed too; the node
//! stops once each has, or after a minute. Bitcoin trades around the clock,
//! so its quote is of the last hour: data-ws, which lists it too, sends
//! only a tick of 8 November, when crypto moved to london-ws. It needs the
//! LSE key, `LSE_API_KEY`, from the environment or `.env`; the free key
//! allows one connection a socket, so no other live run may hold data-ws,
//! london-ws or tsx-ws meanwhile.
//!
//! Gated as `lse_history.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_LSE_RUN=1` is set in the shell. Run it with:
//!
//! ```sh
//! NAUTILUS_LSE_RUN=1 cargo test --test live lse_markets:: -- --nocapture
//! ```

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

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

/// A series of each derived market and its LSE symbol: on data-ws, futures,
/// a US stock, forex, a commodity and an index; on london-ws, crypto and a
/// UK stock; on tsx-ws, the one Canadian stock it lists.
const SERIES: [(&str, &str); 8] = [
    ("GOLD-FUTURES.LSEDGE", "GOLD"),
    ("AAPL-US_STOCKS.LSEDGE", "AAPL"),
    ("EUR_USD-FOREX.LSEDGE", "EUR_USD"),
    ("XAU/USD-COMMODITIES.LSEDGE", "XAU/USD"),
    ("SPX-INDICES.LSEDGE", "SPX"),
    ("BTCUSDT-CRYPTO.LSEDGE", "BTCUSDT"),
    ("VOD-UK_STOCKS.LSEDGE", "VOD"),
    ("NVDA-CA_STOCKS.LSEDGE", "NVDA"),
];

const BITCOIN: &str = "BTCUSDT-CRYPTO.LSEDGE";

/// How long the node waits for the quotes at most.
const WAITED: Duration = Duration::from_secs(60);

/// The precision of every series' prices: finer than LSE's of any of them.
const PRECISION: u8 = 5;

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

/// `id`'s definition, as one is written by hand: an index of LSE's symbol
/// `raw`, priced at `PRECISION`, its sizes whole.
fn index(id: &str, raw: &str) -> InstrumentAny {
    InstrumentAny::IndexInstrument(
        IndexInstrument::builder()
            .instrument_id(InstrumentId::from(id))
            .raw_symbol(Symbol::from(raw))
            .currency(Currency::USD())
            .price_precision(PRECISION)
            .size_precision(0)
            .price_increment(Price::new(10f64.powi(-i32::from(PRECISION)), PRECISION))
            .size_increment(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// Asks each series' definition of LSE's client, as a live warmup does,
/// then subscribes to its quotes, keeping the first of each.
#[derive(Debug)]
struct Quotes {
    core: DataActorCore,
    first: Rc<RefCell<BTreeMap<InstrumentId, QuoteTick>>>,
}

nautilus_actor!(Quotes);

impl DataActor for Quotes {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for (id, _) in SERIES {
            let id = InstrumentId::from(id);
            self.request_instrument(id, None, None, Some(ClientId::from("LSE")), None)?;
            self.subscribe_quotes(id, None, None);
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.first
            .borrow_mut()
            .entry(quote.instrument_id)
            .or_insert(*quote);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_series_of_each_derived_market_quotes_from_its_socket() {
    if !opted_in("a_series_of_each_derived_market_quotes_from_its_socket") {
        return;
    }
    dotenvy::dotenv().ok();
    let dir = tempfile::tempdir().unwrap();
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(SERIES.iter().map(|&(id, raw)| index(id, raw)).collect())
        .unwrap();
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("LSE-MARKETS-001"),
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
        .stream_ids(
            SERIES
                .iter()
                .map(|&(id, _)| InstrumentId::from(id))
                .collect(),
        )
        .build();
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-MARKETS")
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
    let first: Rc<RefCell<BTreeMap<InstrumentId, QuoteTick>>> = Rc::default();
    node.add_actor(Quotes {
        core: DataActorCore::new(DataActorConfig::default()),
        first: Rc::clone(&first),
    })
    .unwrap();
    let handle = node.handle();

    let watched = Rc::clone(&first);
    let (ran, ()) = tokio::join!(node.run(), async move {
        let started = tokio::time::Instant::now();
        while watched.borrow().len() < SERIES.len() && started.elapsed() < WAITED {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        handle.stop();
    });
    ran.unwrap();
    node.dispose();

    let first = first.borrow();
    for (id, quote) in first.iter() {
        println!("{id}: first {quote:?}");
    }
    let unquoted: Vec<&str> = SERIES
        .iter()
        .map(|&(id, _)| id)
        .filter(|id| !first.contains_key(&InstrumentId::from(*id)))
        .collect();
    assert!(unquoted.is_empty(), "no quote of {unquoted:?}");
    assert!(
        first
            .values()
            .all(|quote| quote.bid_price <= quote.ask_price
                && quote.bid_size.is_zero()
                && quote.ask_size.is_zero()),
        "a quote crossed or sized"
    );
    // LSE's times run ahead of the local clock, by about 290 ms.
    let bitcoin = first[&InstrumentId::from(BITCOIN)];
    let hour = 3_600_000_000_000;
    assert!(
        bitcoin.ts_init.as_u64().abs_diff(bitcoin.ts_event.as_u64()) < hour,
        "bitcoin's quote is not of the last hour: {bitcoin:?}"
    );
}
