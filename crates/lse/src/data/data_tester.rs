//! The data client on a Nautilus node, driven by Nautilus' `DataTester` as
//! Nautilus' data testing specification runs an adapter
//! (`docs/developer_guide/spec_data_testing.md`), for the data it streams:
//! an `LSEDGE` series' quotes, against the mock LSE socket replaying LSE's
//! captured frames, and a `GLBX` contract's quotes and trades, against the
//! mock CME stream replaying LSE's documented records; the definition
//! loaded from the node's catalog (TC-D01, LSE publishing none, from the
//! catalog alone), quotes (TC-D20) and trades (TC-D30) flowing after start,
//! and their subscriptions ended on stop (TC-D70). TC-D21, historical
//! quotes, is skipped here: LSE has no quotes of a derived market's past,
//! and a CME contract's run against LSE in `examples/node_data_tester.rs`.

use std::{cell::RefCell, rc::Rc, time::Duration};

use nautilus_common::{
    enums::Environment,
    msgbus::{
        self, TypedHandler,
        switchboard::{get_quotes_topic, get_trades_topic},
    },
    testing::wait_until_async,
};
use nautilus_core::{UnixNanos, string::secret::SecretString};
use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    enums::AggressorSide,
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
        API_KEY, CME_API_KEY, CME_ESZ6_DOCUMENTED, ESZ26, GOLD_FUTURES, MockCmeStream, MockLse,
        MockLseSocket, PAIRS_ESZ6, Reply, data_ws_answer, dbn_frames, esz26, gold_futures, held,
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
        .base_url_ws(socket.url().to_string())
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
