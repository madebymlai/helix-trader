//! The data client on a Nautilus node, driven by Nautilus' `DataTester` as
//! Nautilus' data testing specification runs an adapter
//! (`docs/developer_guide/spec_data_testing.md`), for the data it streams,
//! quotes, against the mock LSE socket replaying LSE's captured frames: the
//! definition loaded from the node's catalog (TC-D01, LSE publishing none,
//! from the catalog alone), quotes flowing after start (TC-D20) and their
//! subscription ended on stop (TC-D70). TC-D21, historical quotes, is
//! skipped here: LSE has no quotes of a derived market's past, and a CME
//! contract's run against LSE in `examples/node_data_tester.rs`. A CME
//! contract's history of book deltas and depth, which `DataTester` does not
//! request, reaches a custom actor on a node against the mock LSE peer, as
//! the specification asks historical book deltas to be tested.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc, time::Duration};

use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    msgbus::{self, TypedHandler, switchboard::get_quotes_topic},
    nautilus_actor,
    testing::wait_until_async,
};
use nautilus_core::{UnixNanos, string::secret::SecretString};
use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_model::{
    data::{OrderBookDelta, OrderBookDepth, QuoteTick},
    enums::RecordFlag,
    identifiers::{ClientId, InstrumentId, TraderId},
    types::{Price, Quantity},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};
use serde_json::json;

use crate::{
    LseDataClientConfig, LseDataClientFactory,
    testing::{
        API_KEY, CME_API_KEY, GCZ26, GCZ26_BOOK, GCZ26_ORDERS, GOLD_FUTURES, MockLse,
        MockLseSocket, PAIRS_GCZ6, Reply, data_ws_answer, gcz26, gold_futures,
    },
};

/// LSE's gold futures quotes, its definition in the node's catalog, until
/// one reaches the message bus; then the node stops.
#[tokio::test]
async fn the_data_tester_receives_lse_s_quotes_and_ends_their_subscription_on_stop() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let dir = tempfile::tempdir().unwrap();
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(vec![gold_futures()])
        .unwrap();
    let gold = InstrumentId::from(GOLD_FUTURES);
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("TESTER-001"),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
        )],
        ..Default::default()
    };
    let lse = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_ws_data(socket.url().to_string())
        .stream_ids(vec![gold])
        .build();
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-TESTER")
        .with_delay_post_stop_secs(1)
        .add_data_client(None, Box::new(LseDataClientFactory::new()), Box::new(lse))
        .unwrap()
        .build()
        .unwrap();
    let tester = DataTesterConfig::builder()
        .client_id(ClientId::from("LSE"))
        .instrument_ids(vec![gold])
        .request_instruments(true)
        .subscribe_quotes(true)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    node.add_actor(DataTester::new(tester)).unwrap();
    let quotes: Rc<RefCell<Vec<QuoteTick>>> = Rc::default();
    let seen = Rc::clone(&quotes);
    msgbus::subscribe_quotes(
        get_quotes_topic(gold).into(),
        TypedHandler::from(move |quote: &QuoteTick| seen.borrow_mut().push(*quote)),
        None,
    );
    let handle = node.handle();
    let stopped = async {
        wait_until_async(
            || {
                let any = !quotes.borrow().is_empty();
                async move { any }
            },
            Duration::from_secs(30),
        )
        .await;
        handle.stop();
    };

    let (ran, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(node.run(), stopped)
    })
    .await
    .expect("the node stops within a minute");
    ran.unwrap();
    node.dispose();

    let at: UnixNanos = "2026-11-13T16:44:55.499567Z"
        .parse::<jiff::Timestamp>()
        .unwrap()
        .into();
    let first = quotes.borrow()[0];
    assert_eq!(
        (
            first.bid_price,
            first.ask_price,
            first.bid_size,
            first.ask_size,
            first.ts_event
        ),
        (
            Price::from("3888.0"),
            Price::from("3888.2"),
            Quantity::from(0),
            Quantity::from(0),
            at
        )
    );
    assert!(first.bid_price < first.ask_price);
    let sent: Vec<_> = socket
        .wait_until(|received| received.len() == 3)
        .await
        .into_iter()
        .map(|(_, frame)| frame)
        .filter(|frame| frame["action"] != "auth")
        .collect();
    assert_eq!(
        sent,
        [
            json!({"action": "subscribe", "symbol": "GOLD.F"}),
            json!({"action": "unsubscribe", "symbol": "GOLD.F"}),
        ]
    );
}

/// An actor asking LSE's client for GCZ26's history of book deltas, then,
/// once they came, of its book depth: `DataTester` requests neither, and
/// Nautilus' data testing specification asks historical book deltas to be
/// tested through a custom actor until it does.
#[derive(Debug)]
struct BookHistory {
    core: DataActorCore,
    deltas: Rc<RefCell<Vec<OrderBookDelta>>>,
    depths: Rc<RefCell<Vec<OrderBookDepth>>>,
}

impl BookHistory {
    /// The captured second of GCZ6's trades and quotes, `[start, end]`.
    fn second() -> (Option<jiff::Timestamp>, Option<jiff::Timestamp>) {
        (
            Some("2026-11-12T14:30:00Z".parse().unwrap()),
            Some("2026-11-12T14:30:00.999999999Z".parse().unwrap()),
        )
    }
}

impl DataActor for BookHistory {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let (start, end) = Self::second();
        self.request_book_deltas(
            InstrumentId::from(GCZ26),
            start,
            end,
            None,
            Some(ClientId::from("LSE")),
            None,
        )?;
        Ok(())
    }

    fn on_historical_book_deltas(&mut self, deltas: &[OrderBookDelta]) -> anyhow::Result<()> {
        self.deltas.borrow_mut().extend_from_slice(deltas);
        let (start, end) = Self::second();
        self.request_book_depth(
            InstrumentId::from(GCZ26),
            start,
            end,
            None,
            NonZeroUsize::new(10),
            Some(ClientId::from("LSE")),
            None,
        )?;
        Ok(())
    }

    fn on_historical_book_depth(&mut self, depths: &[OrderBookDepth]) -> anyhow::Result<()> {
        self.depths.borrow_mut().extend_from_slice(depths);
        Ok(())
    }
}

nautilus_actor!(BookHistory);

/// GCZ6's orders and book over the captured second, as LSE's docs record
/// them, reach the actor that asked for them on a node, through the data
/// engine, the contract's definition in the node's cache.
#[tokio::test]
async fn an_actor_receives_lse_s_book_deltas_and_depth_of_a_cme_contract() {
    let lse = MockLse::start(vec![
        Reply::json(PAIRS_GCZ6),
        Reply::file(GCZ26_ORDERS),
        Reply::file(GCZ26_BOOK),
    ]);
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("TESTER-001"),
        load_state: false,
        save_state: false,
        ..Default::default()
    };
    let lse_config = LseDataClientConfig::builder()
        .cme_api_key(SecretString::from(CME_API_KEY))
        .base_url_http(lse.url().to_string())
        .build();
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-BOOK-HISTORY")
        .with_delay_post_stop_secs(0)
        .add_data_client(
            None,
            Box::new(LseDataClientFactory::new()),
            Box::new(lse_config),
        )
        .unwrap()
        .build()
        .unwrap();
    node.kernel()
        .cache()
        .borrow_mut()
        .add_instrument(gcz26())
        .unwrap();
    let deltas: Rc<RefCell<Vec<OrderBookDelta>>> = Rc::default();
    let depths: Rc<RefCell<Vec<OrderBookDepth>>> = Rc::default();
    node.add_actor(BookHistory {
        core: DataActorCore::new(DataActorConfig::default()),
        deltas: Rc::clone(&deltas),
        depths: Rc::clone(&depths),
    })
    .unwrap();
    let handle = node.handle();
    let stopped = async {
        wait_until_async(
            || {
                let any = !depths.borrow().is_empty();
                async move { any }
            },
            Duration::from_secs(30),
        )
        .await;
        handle.stop();
    };

    let (ran, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(node.run(), stopped)
    })
    .await
    .expect("the node stops within a minute");
    ran.unwrap();
    node.dispose();

    let gcz26 = InstrumentId::from(GCZ26);
    let deltas = deltas.borrow();
    assert_eq!(deltas.len(), 4);
    assert!(deltas.iter().all(|delta| delta.instrument_id == gcz26));
    assert!(RecordFlag::F_LAST.matches(deltas[3].flags));
    let depths = depths.borrow();
    assert_eq!(depths.len(), 4);
    assert_eq!(
        (depths[0].bids[0].price, depths[0].asks[0].price),
        (Price::from("3908.10"), Price::from("3908.30"))
    );
}
