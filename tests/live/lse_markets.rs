//! Tests end to end on LSE that a node streams the quotes of a series of each LSE derived market,
//! through the LSE adapter's data client, each series on the socket that carries its market, routed
//! LSEDGE as the live node routes it. LSE publishes no definitions, so the test writes them by hand
//! into a temporary catalog, which answers the client's request for them as in a live warmup. LSE
//! opens a subscription with the symbol's last tick, so each series quotes at once even with its
//! market closed. The node stops once each series has quoted, or after a minute. Bitcoin's quote is
//! from the last hour: data-ws also lists it, but sends only a tick of 8 November, when crypto
//! moved to london-ws.
//!
//! The test needs `LSE_API_KEY` from the environment or `.env`. The free key allows one connection
//! a socket, so no other live run may hold data-ws, london-ws or tsx-ws meanwhile. It is gated as
//! `lse_history.rs` is. Run it with:
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

/// A series of each derived market and its LSE symbol.
///
/// - On data-ws: futures, a US stock, forex, a commodity and an index.
/// - On london-ws: crypto and a UK stock.
/// - On tsx-ws: the one Canadian stock it lists.
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

const WAITED: Duration = Duration::from_secs(60);

/// The precision of every series' prices, which is finer than LSE's for any of them.
const PRECISION: u8 = 5;

/// Returns whether the run is opted in, reading the shell before `.env`, and claims the process.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_LSE_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_LSE_RUN=1 to run it on LSE");
    }
    run
}

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
        let quoted = {
            let mut first = self.first.borrow_mut();
            first.entry(quote.instrument_id).or_insert(*quote);
            first.len()
        };

        if quoted == SERIES.len() {
            self.shutdown_system(Some("every series quoted".to_string()));
        }
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

    let ran = {
        let run = node.run();
        tokio::pin!(run);

        tokio::select! {
            ran = &mut run => ran,
            () = tokio::time::sleep(WAITED) => {
                handle.stop();
                run.await
            }
        }
    };
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

    // LSE's times run ahead of the local clock, by about 290 ms
    let bitcoin = first[&InstrumentId::from(BITCOIN)];
    let hour = 3_600_000_000_000;
    assert!(
        bitcoin.ts_init.as_u64().abs_diff(bitcoin.ts_event.as_u64()) < hour,
        "bitcoin's quote is not of the last hour: {bitcoin:?}"
    );
}
