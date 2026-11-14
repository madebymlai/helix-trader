//! The data client on a Nautilus node, driven by Nautilus' `DataTester` as
//! Nautilus' data testing specification runs an adapter
//! (`docs/developer_guide/spec_data_testing.md`), for the data it streams:
//! a series' quotes and an option contract's trades, against the mock LSE
//! socket replaying LSE's captured frames and, for the option, the frames
//! LSE's documentation gives, and a `GLBX` contract's quotes and trades,
//! against the mock CME stream replaying LSE's documented records; the
//! definition loaded from the node's catalog (TC-D01, LSE publishing none,
//! from the catalog alone), quotes and trades flowing after start (TC-D20,
//! TC-D30) and their subscriptions ended on stop (TC-D70), and its book
//! depth (TC-D12) and book deltas (TC-D10), against the mock CME stream
//! replaying the Level 2 and Level 3 records LSE's documentation describes,
//! ended on stop too (TC-D70). TC-D21,
//! historical quotes, is skipped here: LSE has no quotes of a derived
//! market's past, and a CME contract's run against LSE in
//! `examples/node_data_tester.rs`. A CME
//! contract's history of book deltas and depth, which `DataTester` does not
//! request, reaches a custom actor on a node against the mock LSE peer, as
//! the specification asks historical book deltas to be tested.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc, time::Duration};

use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    msgbus::{
        self, TypedHandler,
        switchboard::{
            get_book_deltas_pattern, get_book_depth_pattern, get_quotes_topic, get_trades_topic,
        },
    },
    nautilus_actor,
    testing::wait_until_async,
};
use nautilus_core::{UnixNanos, string::secret::SecretString};
use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_model::{
    data::{OrderBookDelta, OrderBookDeltas, OrderBookDepth, QuoteTick, TradeTick},
    enums::{AggressorSide, BookAction, BookType, RecordFlag},
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
        AAPL_CALL, API_KEY, CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7,
        CME_GCZ6_L2_DOCUMENTED, CME_GCZ6_L3_DOCUMENTED, ESZ26, GCZ26, GCZ26_BOOK, GCZ26_ORDERS,
        GOLD_FUTURES, MockCmeStream, MockLse, MockLseSocket, PAIRS_ESZ6, PAIRS_GCZ6, Reply,
        aapl_call, data_ws_answer, dbn_frames, esz26, frames, gcz26, gold_futures, held, sent,
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

/// The trades of AAPL's 355 call of 20 November 2026 from its underlying's
/// chain, as LSE's documentation gives the chain's frames, its definition in
/// the node's catalog, until one reaches the message bus; then the node
/// stops.
#[tokio::test]
async fn the_data_tester_receives_an_option_s_trades_from_its_chain_and_ends_it_on_stop() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let dir = tempfile::tempdir().unwrap();
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(vec![aapl_call()])
        .unwrap();
    let call = InstrumentId::from(AAPL_CALL);
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
        .stream_ids(vec![call])
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
        .instrument_ids(vec![call])
        .request_instruments(true)
        .subscribe_trades(true)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    node.add_actor(DataTester::new(tester)).unwrap();
    let trades: Rc<RefCell<Vec<TradeTick>>> = Rc::default();
    let seen = Rc::clone(&trades);
    msgbus::subscribe_trades(
        get_trades_topic(call).into(),
        TypedHandler::from(move |trade: &TradeTick| seen.borrow_mut().push(*trade)),
        None,
    );
    let handle = node.handle();
    let stopped = async {
        wait_until_async(
            || {
                let any = !trades.borrow().is_empty();
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

    let at: UnixNanos = "2026-11-12T14:30:00.816861166Z"
        .parse::<jiff::Timestamp>()
        .unwrap()
        .into();
    let first = trades.borrow()[0];
    assert_eq!(
        (
            first.price,
            first.size,
            first.aggressor_side,
            first.ts_event
        ),
        (
            Price::from("6.70"),
            Quantity::from(1),
            AggressorSide::NoAggressor,
            at
        )
    );
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
            json!({"action": "subscribe_options", "underlying": "AAPL"}),
            json!({"action": "unsubscribe_options", "underlying": "AAPL"}),
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

/// LSE's documented ESZ6 at Level 1, its definition in the node's catalog:
/// the tester's quotes open with its best bid and offer; then the stream
/// sends its live records, the last a trade, and the node stops once the
/// trade reached the message bus.
#[tokio::test]
async fn the_data_tester_receives_a_cme_contract_s_quotes_and_trades_from_lse() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6)]);
    let stream = MockCmeStream::start(|_, _| held(dbn_frames(CME_ESZ6_DOCUMENTED), 4));
    let dir = tempfile::tempdir().unwrap();
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(vec![esz26()])
        .unwrap();
    let es = InstrumentId::from(ESZ26);
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
        .cme_api_key(SecretString::from(CME_API_KEY))
        .base_url_http(lse.url().to_string())
        .base_url_ws_cme(stream.url().to_string())
        .stream_ids(vec![es])
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
        .instrument_ids(vec![es])
        .request_instruments(true)
        .subscribe_quotes(true)
        .subscribe_trades(true)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    node.add_actor(DataTester::new(tester)).unwrap();
    let quotes: Rc<RefCell<Vec<QuoteTick>>> = Rc::default();
    let trades: Rc<RefCell<Vec<TradeTick>>> = Rc::default();
    let (seen_quotes, seen_trades) = (Rc::clone(&quotes), Rc::clone(&trades));
    msgbus::subscribe_quotes(
        get_quotes_topic(es).into(),
        TypedHandler::from(move |quote: &QuoteTick| seen_quotes.borrow_mut().push(*quote)),
        None,
    );
    msgbus::subscribe_trades(
        get_trades_topic(es).into(),
        TypedHandler::from(move |trade: &TradeTick| seen_trades.borrow_mut().push(*trade)),
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
        stream.release();
        wait_until_async(
            || {
                let any = !trades.borrow().is_empty();
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

    let quotes = quotes.borrow();
    let first = quotes[0];
    assert_eq!(
        (
            first.bid_price,
            first.bid_size,
            first.ask_price,
            first.ask_size
        ),
        (
            Price::from("7569.75"),
            Quantity::from(2),
            Price::from("7570.00"),
            Quantity::from(11)
        )
    );
    assert!(quotes.iter().all(|quote| quote.bid_price < quote.ask_price));
    let trade = trades.borrow()[0];
    assert_eq!(
        (trade.price, trade.size, trade.aggressor_side),
        (
            Price::from("7570.00"),
            Quantity::from(1),
            AggressorSide::NoAggressor
        )
    );
}

/// LSE's CME stream answering each connection by its level: GCZ6's
/// captured opening at Level 1, and its Level 2 and Level 3 sessions as
/// LSE's documentation records them.
fn gcz6_by_level() -> MockCmeStream {
    MockCmeStream::start(|_, subscription| match subscription["level"].as_u64() {
        Some(2) => sent(dbn_frames(CME_GCZ6_L2_DOCUMENTED)),
        Some(3) => sent(dbn_frames(CME_GCZ6_L3_DOCUMENTED)),
        _ => sent(frames(CME_GCZ6_GCG7)[..4].to_vec()),
    })
}

/// A node streaming GCZ26 from LSE against `lse` and `stream`, its
/// definition in the catalog at `dir`, with `tester`.
fn gcz26_node(
    lse: &MockLse,
    stream: &MockCmeStream,
    dir: &std::path::Path,
    tester: DataTesterConfig,
) -> nautilus_live::node::LiveNode {
    ParquetDataCatalog::new(dir, None, None, None, None)
        .write_instruments(vec![gcz26()])
        .unwrap();
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("TESTER-001"),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(
            dir.to_str().unwrap().to_string(),
            None,
            None,
        )],
        ..Default::default()
    };
    let lse = LseDataClientConfig::builder()
        .cme_api_key(SecretString::from(CME_API_KEY))
        .base_url_http(lse.url().to_string())
        .base_url_ws_cme(stream.url().to_string())
        .stream_ids(vec![InstrumentId::from(GCZ26)])
        .build();
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-TESTER")
        .with_delay_post_stop_secs(1)
        .add_data_client(None, Box::new(LseDataClientFactory::new()), Box::new(lse))
        .unwrap()
        .build()
        .unwrap();
    node.add_actor(DataTester::new(tester)).unwrap();
    node
}

/// Runs `node` until `done` holds, then stops it.
async fn run_until(node: &mut nautilus_live::node::LiveNode, done: impl Fn() -> bool) {
    let handle = node.handle();
    let stopped = async {
        wait_until_async(
            || {
                let met = done();
                async move { met }
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
}

/// GCZ6's book depth at Level 2 as LSE's documentation records it, its
/// definition in the node's catalog: the tester's depth opens with the
/// contract's last ten levels, then a depth a change of its book, each
/// ten levels a side or fewer, bids falling and asks rising from the best;
/// the node stops once the four reached the message bus.
#[tokio::test]
async fn the_data_tester_receives_a_cme_contract_s_book_depth_from_lse() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = gcz6_by_level();
    let dir = tempfile::tempdir().unwrap();
    let gc = InstrumentId::from(GCZ26);
    let tester = DataTesterConfig::builder()
        .client_id(ClientId::from("LSE"))
        .instrument_ids(vec![gc])
        .request_instruments(true)
        .subscribe_book_depth(true)
        .book_type(BookType::L2_MBP)
        .book_depth(10)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    let mut node = gcz26_node(&lse, &stream, dir.path(), tester);
    let depths: Rc<RefCell<Vec<OrderBookDepth>>> = Rc::default();
    let seen = Rc::clone(&depths);
    msgbus::subscribe_book_depth(
        get_book_depth_pattern(gc),
        TypedHandler::from(move |depth: &OrderBookDepth| seen.borrow_mut().push(depth.clone())),
        None,
    );

    run_until(&mut node, || depths.borrow().len() == 4).await;

    let depths = depths.borrow();
    assert_eq!(
        (depths[0].bids[0].price, depths[0].asks[0].price),
        (Price::from("3908.10"), Price::from("3908.30"))
    );
    for depth in depths.iter() {
        let bids: Vec<Price> = depth
            .bids
            .iter()
            .filter(|bid| bid.side.is_some())
            .map(|bid| bid.price)
            .collect();
        let asks: Vec<Price> = depth
            .asks
            .iter()
            .filter(|ask| ask.side.is_some())
            .map(|ask| ask.price)
            .collect();
        assert!(bids.len() <= 10 && asks.len() <= 10);
        assert!(bids.windows(2).all(|pair| pair[0] > pair[1]), "{bids:?}");
        assert!(asks.windows(2).all(|pair| pair[0] < pair[1]), "{asks:?}");
        assert!(bids[0] < asks[0]);
    }
}

/// GCZ6's book deltas at Level 3 as LSE's documentation records them, its
/// definition in the node's catalog: the tester's deltas start at the
/// first order event, no book opening them, each batch ending `F_LAST`,
/// the first adding a bid and an ask; the node stops once the three
/// batches reached the message bus.
#[tokio::test]
async fn the_data_tester_receives_a_cme_contract_s_book_deltas_from_lse() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = gcz6_by_level();
    let dir = tempfile::tempdir().unwrap();
    let gc = InstrumentId::from(GCZ26);
    let tester = DataTesterConfig::builder()
        .client_id(ClientId::from("LSE"))
        .instrument_ids(vec![gc])
        .request_instruments(true)
        .subscribe_book_deltas(true)
        .book_type(BookType::L3_MBO)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    let mut node = gcz26_node(&lse, &stream, dir.path(), tester);
    let batches: Rc<RefCell<Vec<OrderBookDeltas>>> = Rc::default();
    let seen = Rc::clone(&batches);
    msgbus::subscribe_book_deltas(
        get_book_deltas_pattern(gc),
        TypedHandler::from(move |deltas: &OrderBookDeltas| seen.borrow_mut().push(deltas.clone())),
        None,
    );

    run_until(&mut node, || batches.borrow().len() == 3).await;

    let batches = batches.borrow();
    let first: Vec<(BookAction, Price)> = batches[0]
        .deltas
        .iter()
        .map(|delta| (delta.action, delta.order.price))
        .collect();
    assert_eq!(
        first,
        [
            (BookAction::Add, Price::from("3908.10")),
            (BookAction::Add, Price::from("3908.30")),
        ]
    );
    for batch in batches.iter() {
        assert_eq!(batch.instrument_id, gc);
        assert!(RecordFlag::F_LAST.matches(batch.deltas.last().unwrap().flags));
        assert!(
            batch
                .deltas
                .iter()
                .all(|delta| delta.action != BookAction::Clear
                    && !RecordFlag::F_SNAPSHOT.matches(delta.flags))
        );
    }
}
