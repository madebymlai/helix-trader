//! Tests the data client on a Nautilus node, driven by Nautilus' `DataTester` as its data testing
//! specification runs an adapter (`docs/developer_guide/spec_data_testing.md`).
//!
//! A series' quotes and an option contract's trades run against the mock LSE socket. The socket
//! replays captured frames and, for the option, the frames LSE's documentation gives. A `GLBX`
//! contract and an `XNAS` stock run against the mock CME and Nasdaq streams, which replay the
//! records LSE's documentation gives.
//!
//! The cases are:
//! - The node loads the definition from its catalog (TC-D01), because LSE publishes none.
//! - Quotes and trades flow after start (TC-D20, TC-D30) and end on stop (TC-D70).
//! - Book depth (TC-D12) and deltas (TC-D10) come from the Level 2 and 3 records and end on stop
//!   (TC-D70).
//!
//! TC-D21 (historical quotes) is skipped. LSE has no quotes for a derived market's past, and a CME
//! contract's run against LSE is in `examples/node_data_tester.rs`. A custom actor on a node
//! against the mock LSE peer requests the history of book deltas and depth. `DataTester` does not
//! request it, but the specification asks historical book deltas to be tested.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc, time::Duration};

use lse::{
    LseDataClientConfig, LseDataClientFactory,
    testing::{
        AAPL_CALL, API_KEY, CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7,
        CME_GCZ6_L2_DOCUMENTED, CME_GCZ6_L3_DOCUMENTED, ESZ26, GCZ26, GCZ26_BOOK, GCZ26_ORDERS,
        GOLD_FUTURES, MockExchangeStream, MockLse, MockLseSocket, NASDAQ_API_KEY,
        NASDAQ_NVDA_L1_DOCUMENTED, NASDAQ_NVDA_L2_DOCUMENTED, NASDAQ_NVDA_L3_DOCUMENTED, NVDA,
        NVDA_BOOK, NVDA_ORDERS, PAIRS_ESZ6, PAIRS_GCZ6, Reply, aapl_call, data_ws_answer,
        dbn_frames, esz26, frames, gcz26, gold_futures, held, nvda, sent,
    },
};
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
use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig, node::LiveNode};
use nautilus_model::{
    data::{OrderBookDelta, OrderBookDeltas, OrderBookDepth, QuoteTick, TradeTick},
    enums::{AggressorSide, BookAction, BookType, RecordFlag},
    identifiers::{ClientId, InstrumentId, TraderId},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};
use rstest::rstest;
use serde_json::json;

/// Runs LSE's gold futures quotes until one reaches the message bus, then the node stops.
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

/// Runs the trades of AAPL's 355 call of 20 November 2026 from its underlying's chain, using the
/// frames LSE's documentation gives, until one reaches the message bus.
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

/// A contract on an LSE exchange stream.
///
/// GCZ6 of 2026 is on CME, and the client looks it up in LSE's catalogue with the CME key. NVDA is
/// on Nasdaq, and the client looks it up by ticker with the Nasdaq key.
#[derive(Debug, Clone, Copy)]
enum Streamed {
    Gcz26,
    Nvda,
}

impl Streamed {
    fn id(self) -> InstrumentId {
        InstrumentId::from(match self {
            Self::Gcz26 => GCZ26,
            Self::Nvda => NVDA,
        })
    }

    fn definition(self) -> InstrumentAny {
        match self {
            Self::Gcz26 => gcz26(),
            Self::Nvda => nvda(),
        }
    }

    /// Starts LSE's API answering with `replies`, after GCZ6's listing, which the client looks up
    /// first.
    fn lse(self, replies: Vec<Reply>) -> MockLse {
        let lookup = match self {
            Self::Gcz26 => vec![Reply::json(PAIRS_GCZ6)],
            Self::Nvda => vec![],
        };
        MockLse::start(lookup.into_iter().chain(replies).collect())
    }

    /// Starts the stream, which answers each connection by its level. The Level 1 opening is
    /// GCZ6's captured one and NVDA's documented one. The Level 2 and 3 sessions are as LSE's
    /// documentation records them.
    fn stream(self) -> MockExchangeStream {
        let (opening, level_2, level_3) = match self {
            Self::Gcz26 => (
                frames(CME_GCZ6_GCG7),
                CME_GCZ6_L2_DOCUMENTED,
                CME_GCZ6_L3_DOCUMENTED,
            ),
            Self::Nvda => (
                dbn_frames(NASDAQ_NVDA_L1_DOCUMENTED),
                NASDAQ_NVDA_L2_DOCUMENTED,
                NASDAQ_NVDA_L3_DOCUMENTED,
            ),
        };
        let script = move |_, subscription: &serde_json::Value| match subscription["level"].as_u64()
        {
            Some(2) => sent(dbn_frames(level_2)),
            Some(3) => sent(dbn_frames(level_3)),
            _ => sent(opening[..4].to_vec()),
        };
        match self {
            Self::Gcz26 => MockExchangeStream::cme(script),
            Self::Nvda => MockExchangeStream::nasdaq(script),
        }
    }

    /// Returns the client's config with the plan's key, LSE's API on `lse` and, if given, the
    /// stream on `stream`.
    fn config(self, lse: &MockLse, stream: Option<&MockExchangeStream>) -> LseDataClientConfig {
        let mut config = LseDataClientConfig::builder()
            .base_url_http(lse.url().to_string())
            .build();
        let url = stream.map(|stream| stream.url().to_string());
        match self {
            Self::Gcz26 => {
                config.cme_api_key = Some(SecretString::from(CME_API_KEY));
                config.base_url_ws_cme = url;
            }
            Self::Nvda => {
                config.nasdaq_api_key = Some(SecretString::from(NASDAQ_API_KEY));
                config.base_url_ws_nasdaq = url;
            }
        }
        if stream.is_some() {
            config.stream_ids = vec![self.id()];
        }
        config
    }

    fn best(self) -> (Price, Price) {
        let (bid, ask) = match self {
            Self::Gcz26 => ("3908.10", "3908.30"),
            Self::Nvda => ("211.15", "211.17"),
        };
        (Price::from(bid), Price::from(ask))
    }
}

/// An actor that asks for a contract's history of book deltas, then its history of book depth.
///
/// `DataTester` requests neither. Nautilus' data testing specification asks for historical book
/// deltas to be tested through a custom actor until it does.
#[derive(Debug)]
struct BookHistory {
    core: DataActorCore,
    instrument_id: InstrumentId,
    deltas: Rc<RefCell<Vec<OrderBookDelta>>>,
    depths: Rc<RefCell<Vec<OrderBookDepth>>>,
}

impl BookHistory {
    /// Returns the second of the documented books and orders as `[start, end]`. It is the captured
    /// second of GCZ6's trades and quotes.
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
            self.instrument_id,
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
            self.instrument_id,
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

/// A contract's orders and book over the second, as LSE's docs record them, reach the actor on a
/// node through the data engine. The definition is in the node's cache.
#[rstest]
#[case::cme_contract(Streamed::Gcz26, GCZ26_ORDERS, GCZ26_BOOK)]
#[case::nasdaq_stock(Streamed::Nvda, NVDA_ORDERS, NVDA_BOOK)]
#[tokio::test]
async fn an_actor_receives_lse_s_book_deltas_and_depth_of_a_contract(
    #[case] streamed: Streamed,
    #[case] orders: &[u8],
    #[case] book: &[u8],
) {
    let lse = streamed.lse(vec![Reply::file(orders), Reply::file(book)]);
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("TESTER-001"),
        load_state: false,
        save_state: false,
        ..Default::default()
    };
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-BOOK-HISTORY")
        .with_delay_post_stop_secs(0)
        .add_data_client(
            None,
            Box::new(LseDataClientFactory::new()),
            Box::new(streamed.config(&lse, None)),
        )
        .unwrap()
        .build()
        .unwrap();
    node.kernel()
        .cache()
        .borrow_mut()
        .add_instrument(streamed.definition())
        .unwrap();
    let deltas: Rc<RefCell<Vec<OrderBookDelta>>> = Rc::default();
    let depths: Rc<RefCell<Vec<OrderBookDepth>>> = Rc::default();
    node.add_actor(BookHistory {
        core: DataActorCore::new(DataActorConfig::default()),
        instrument_id: streamed.id(),
        deltas: Rc::clone(&deltas),
        depths: Rc::clone(&depths),
    })
    .unwrap();

    run_until(&mut node, || !depths.borrow().is_empty()).await;

    let deltas = deltas.borrow();
    assert_eq!(deltas.len(), 4);
    assert!(
        deltas
            .iter()
            .all(|delta| delta.instrument_id == streamed.id())
    );
    assert!(RecordFlag::F_LAST.matches(deltas[3].flags));
    let depths = depths.borrow();
    assert_eq!(depths.len(), 4);
    assert_eq!(
        (depths[0].bids[0].price, depths[0].asks[0].price),
        streamed.best()
    );
}

fn esz26_on_cme(lse: &MockLse, stream: &MockExchangeStream) -> LseDataClientConfig {
    LseDataClientConfig::builder()
        .cme_api_key(SecretString::from(CME_API_KEY))
        .base_url_http(lse.url().to_string())
        .base_url_ws_cme(stream.url().to_string())
        .stream_ids(vec![InstrumentId::from(ESZ26)])
        .build()
}

fn nvda_on_nasdaq(lse: &MockLse, stream: &MockExchangeStream) -> LseDataClientConfig {
    Streamed::Nvda.config(lse, Some(stream))
}

/// A Level 1 session as LSE's documentation records it. ESZ6 is on the CME stream, and the client
/// looks it up in LSE's catalogue. NVDA is on the Nasdaq stream, and the client looks it up by
/// ticker.
///
/// The tester's quotes open with the best bid and offer. The live records follow, and the last is a
/// trade with no aggressor side. The node stops once the trade reaches the message bus.
#[rstest]
#[case::a_cme_contract(
    MockLse::start(vec![Reply::json(PAIRS_ESZ6)]),
    MockExchangeStream::cme(|_, _| held(dbn_frames(CME_ESZ6_DOCUMENTED), 4)),
    esz26_on_cme,
    esz26(),
    (("7569.75", 2), ("7570.00", 11)),
    ("7570.00", 1),
    &["ESZ6"],
)]
#[case::an_xnas_stock(
    MockLse::start(vec![]),
    MockExchangeStream::nasdaq(|_, _| held(dbn_frames(NASDAQ_NVDA_L1_DOCUMENTED), 4)),
    nvda_on_nasdaq,
    nvda(),
    (("211.15", 200), ("211.17", 900)),
    ("211.17", 100),
    &[],
)]
#[tokio::test]
async fn the_data_tester_receives_an_instrument_s_quotes_and_trades_from_lse(
    #[case] lse: MockLse,
    #[case] stream: MockExchangeStream,
    #[case] config: fn(&MockLse, &MockExchangeStream) -> LseDataClientConfig,
    #[case] definition: InstrumentAny,
    #[case] ((bid, bid_size), (ask, ask_size)): ((&str, u64), (&str, u64)),
    #[case] (traded, traded_size): (&str, u64),
    #[case] looked_up: &[&str],
) {
    let dir = tempfile::tempdir().unwrap();
    let id = definition.id();
    let tester = DataTesterConfig::builder()
        .client_id(ClientId::from("LSE"))
        .instrument_ids(vec![id])
        .request_instruments(true)
        .subscribe_quotes(true)
        .subscribe_trades(true)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    let mut node = tester_node(definition, config(&lse, &stream), dir.path(), tester);
    let quotes: Rc<RefCell<Vec<QuoteTick>>> = Rc::default();
    let trades: Rc<RefCell<Vec<TradeTick>>> = Rc::default();
    let (seen_quotes, seen_trades) = (Rc::clone(&quotes), Rc::clone(&trades));
    msgbus::subscribe_quotes(
        get_quotes_topic(id).into(),
        TypedHandler::from(move |quote: &QuoteTick| seen_quotes.borrow_mut().push(*quote)),
        None,
    );
    msgbus::subscribe_trades(
        get_trades_topic(id).into(),
        TypedHandler::from(move |trade: &TradeTick| seen_trades.borrow_mut().push(*trade)),
        None,
    );

    run_until(&mut node, || {
        if !quotes.borrow().is_empty() {
            stream.release();
        }
        !trades.borrow().is_empty()
    })
    .await;

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
            Price::from(bid),
            Quantity::from(bid_size),
            Price::from(ask),
            Quantity::from(ask_size)
        )
    );
    assert!(quotes.iter().all(|quote| quote.bid_price < quote.ask_price));
    let trade = trades.borrow()[0];
    assert_eq!(
        (trade.price, trade.size, trade.aggressor_side),
        (
            Price::from(traded),
            Quantity::from(traded_size),
            AggressorSide::NoAggressor
        )
    );
    let symbols: Vec<_> = lse
        .calls()
        .iter()
        .map(|call| call.param("symbol").unwrap().to_string())
        .collect();
    assert_eq!(symbols, looked_up);
}

fn tester_node(
    definition: InstrumentAny,
    lse: LseDataClientConfig,
    dir: &std::path::Path,
    tester: DataTesterConfig,
) -> LiveNode {
    ParquetDataCatalog::new(dir, None, None, None, None)
        .write_instruments(vec![definition])
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

async fn run_until(node: &mut LiveNode, done: impl Fn() -> bool) {
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

type Best = (u64, (&'static str, u64), (&'static str, u64));

/// Book depth at Level 2 as LSE's documentation records it, for GCZ6 on the CME stream and NVDA on
/// the Nasdaq stream.
///
/// The depth opens with the last ten levels. A depth follows each change to the book, numbered by
/// its sequence:
/// - The best bid's size is cut.
/// - An order joins the best ask.
/// - The ninth ask level is removed.
///
/// The node stops once the four depths reach the message bus. Each case gives each depth's
/// sequence and best bid and ask.
#[rstest]
#[case::cme_contract(
    Streamed::Gcz26,
    [
        (1001, ("3908.10", 2), ("3908.30", 9)),
        (1002, ("3908.10", 1), ("3908.30", 9)),
        (1003, ("3908.10", 1), ("3908.30", 11)),
        (1004, ("3908.10", 1), ("3908.30", 11)),
    ],
)]
#[case::nasdaq_stock(
    Streamed::Nvda,
    [
        (1001, ("211.15", 200), ("211.17", 900)),
        (1002, ("211.15", 100), ("211.17", 900)),
        (1003, ("211.15", 100), ("211.17", 1100)),
        (1004, ("211.15", 100), ("211.17", 1100)),
    ],
)]
#[tokio::test]
async fn the_data_tester_receives_a_contract_s_book_depth_from_lse(
    #[case] streamed: Streamed,
    #[case] best: [Best; 4],
) {
    let lse = streamed.lse(vec![]);
    let stream = streamed.stream();
    let dir = tempfile::tempdir().unwrap();
    let id = streamed.id();
    let tester = DataTesterConfig::builder()
        .client_id(ClientId::from("LSE"))
        .instrument_ids(vec![id])
        .request_instruments(true)
        .subscribe_book_depth(true)
        .book_type(BookType::L2_MBP)
        .book_depth(10)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    let mut node = tester_node(
        streamed.definition(),
        streamed.config(&lse, Some(&stream)),
        dir.path(),
        tester,
    );
    let depths: Rc<RefCell<Vec<OrderBookDepth>>> = Rc::default();
    let seen = Rc::clone(&depths);
    msgbus::subscribe_book_depth(
        get_book_depth_pattern(id),
        TypedHandler::from(move |depth: &OrderBookDepth| seen.borrow_mut().push(depth.clone())),
        None,
    );

    run_until(&mut node, || depths.borrow().len() == 4).await;

    let received: Vec<_> = depths
        .borrow()
        .iter()
        .map(|depth| {
            (
                depth.instrument_id,
                depth.sequence,
                (depth.bids[0].price, depth.bids[0].size),
                (depth.asks[0].price, depth.asks[0].size),
            )
        })
        .collect();
    let expected: Vec<_> = best
        .iter()
        .map(|&(sequence, (bid, bid_size), (ask, ask_size))| {
            (
                id,
                sequence,
                (Price::from(bid), Quantity::from(bid_size)),
                (Price::from(ask), Quantity::from(ask_size)),
            )
        })
        .collect();
    assert_eq!(received, expected);
}

/// Book deltas at Level 3 as LSE's documentation records it, for GCZ6 on the CME stream and NVDA
/// on the Nasdaq stream.
///
/// The deltas start at the first order event, with no opening book. Each batch ends with `F_LAST`,
/// and the batches are numbered 7312 to 7315:
/// - The first adds a bid and an ask at the best.
/// - The second changes the bid.
/// - The third removes the ask.
///
/// The node stops once the three batches reach the message bus.
#[rstest]
#[case::cme_contract(Streamed::Gcz26)]
#[case::nasdaq_stock(Streamed::Nvda)]
#[tokio::test]
async fn the_data_tester_receives_a_contract_s_book_deltas_from_lse(#[case] streamed: Streamed) {
    let lse = streamed.lse(vec![]);
    let stream = streamed.stream();
    let dir = tempfile::tempdir().unwrap();
    let id = streamed.id();
    let tester = DataTesterConfig::builder()
        .client_id(ClientId::from("LSE"))
        .instrument_ids(vec![id])
        .request_instruments(true)
        .subscribe_book_deltas(true)
        .book_type(BookType::L3_MBO)
        .can_unsubscribe(true)
        .build()
        .unwrap();
    let mut node = tester_node(
        streamed.definition(),
        streamed.config(&lse, Some(&stream)),
        dir.path(),
        tester,
    );
    let batches: Rc<RefCell<Vec<OrderBookDeltas>>> = Rc::default();
    let seen = Rc::clone(&batches);
    msgbus::subscribe_book_deltas(
        get_book_deltas_pattern(id),
        TypedHandler::from(move |deltas: &OrderBookDeltas| seen.borrow_mut().push(deltas.clone())),
        None,
    );

    run_until(&mut node, || batches.borrow().len() == 3).await;

    let received: Vec<Vec<_>> = batches
        .borrow()
        .iter()
        .map(|batch| {
            assert_eq!(batch.instrument_id, id);
            batch
                .deltas
                .iter()
                .map(|delta| (delta.sequence, delta.action, delta.order.price, delta.flags))
                .collect()
        })
        .collect();
    let (bid, ask) = streamed.best();
    let last = RecordFlag::F_LAST as u8;
    assert_eq!(
        received,
        [
            vec![
                (7312, BookAction::Add, bid, 0),
                (7313, BookAction::Add, ask, last)
            ],
            vec![(7314, BookAction::Update, bid, last)],
            vec![(7315, BookAction::Delete, ask, last)],
        ]
    );
}
