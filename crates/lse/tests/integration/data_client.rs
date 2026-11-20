//! Tests the data client through its factory. Its requests run against the mock LSE peer, which
//! replays LSE's captured exports, refusals and catalogues. Its subscriptions run against the mock
//! sockets.
//!
//! A test's doc says only what its capture or case adds. A request for something the cache or the
//! client does not know fails, naming it, before LSE is asked. LSE loads a day the morning after,
//! so a request reaching into a day it has not loaded (`http::export`) is answered plain, which the
//! data engine writes to no catalog (`DataEngine::response`), and a fill asks again.

use std::{
    cell::RefCell,
    num::{NonZeroU32, NonZeroUsize},
    rc::Rc,
    time::Duration,
};

use jiff::{SignedDuration, Timestamp};
use lse::{
    LseDataClientConfig, LseDataClientFactory, LseRow,
    testing::{
        AAPL_CALL, AAPL_CALL_MINUTES, AAPL_CALL_TRADES, AAPL_DIVIDENDS, AAPL_FACTORS,
        AAPL_FINANCIAL_REPORTS, AAPL_PUT_2027, AHMA_COMPANY_PROFILES, API_KEY,
        AUGB10Y_SOVEREIGN_YIELDS, CME_API_KEY, DATA_UNAVAILABLE, DATA_UNAVAILABLE_QUOTES,
        DATA_UNAVAILABLE_TODAY, DATE_NOT_TRADING, EXPORT_TOO_WIDE, GCZ26, GCZ26_BOOK, GCZ26_ORDERS,
        GCZ26_QUOTES, GCZ26_TRADES, GOLD_FACTORS, GOLD_FUTURES, GOLD_FUTURES_4H, GOLD_SPOT_MINUTES,
        GOLD_TRADES, HISTORY_LIMIT, LEVEL_REQUIRED_BOOK, LEVEL_REQUIRED_ORDERS, MockLse,
        MockLseSocket, NASDAQ_API_KEY, NVDA, NVDA_BOOK, NVDA_ORDERS, NVDA_QUOTES, NVDA_TRADES,
        PAIRS_GCZ6, PLAN_REQUIRED, PLAN_REQUIRED_NASDAQ, REFERENCE_CATALOGUE,
        REFERENCE_DATA_UNAVAILABLE, Reply, SPX_TICKS, SYMBOL_UNKNOWN, SocketAction,
        TEAM_INSIDER_TRADES, US_CORE_CPI_CALENDAR, US_CORE_CPI_RELEASES, US10Y_BOND_YIELDS,
        US10Y_BOND_YIELDS_MINUTE, US10Y_YIELDS, WS_AUTHENTICATED_DATA, WS_AUTHENTICATED_LONDON,
        WS_AUTHENTICATED_TSX, WS_TICKS_DATA, WS_TICKS_LONDON, WS_TICKS_TSX, ZN_COT, aapl_call,
        aapl_put_2027, data_ws_answer, gcz26, gold_futures, gold_spot, nvda, socket_answer,
    },
};
use nautilus_common::{
    cache::{Cache, CacheView},
    clients::DataClient,
    clock::{Clock, VirtualClock},
    factories::DataClientFactory,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent, DataResponse,
        data::{
            BarsResponse, CustomDataResponse, DataResponseEnvelope, RequestBars, RequestBookDeltas,
            RequestBookDepth, RequestCustomData, RequestFundingRates, RequestInstrument,
            RequestQuotes, RequestTrades, SubscribeQuotes, SubscribeTrades, TradesResponse,
            UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
    testing::wait_until_async,
};
use nautilus_core::{Params, UUID4, UnixNanos, string::secret::SecretString};
use nautilus_model::{
    data::{BarType, CustomData, CustomDataTrait, Data, DataType, HasTsInit, QuoteTick, TradeTick},
    enums::{AggressorSide, BookAction, RecordFlag},
    identifiers::{ClientId, InstrumentId, Symbol},
    instruments::{IndexInstrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rstest::rstest;
use serde_json::{Value, json};

type Events = tokio::sync::mpsc::UnboundedReceiver<DataEvent>;

async fn connected(lse: &MockLse, defined: bool) -> (Box<dyn DataClient>, Events) {
    let (tx, events) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut cache = Cache::default();
    if defined {
        cache.add_instrument(gold_futures()).unwrap();
        cache.add_instrument(gold_spot()).unwrap();
    }
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_http(lse.url().to_string())
        .calls_per_minute(std::num::NonZeroU32::new(6000).unwrap())
        .build();
    let factory = LseDataClientFactory::new();
    let clock = node_clock();
    let mut client = factory
        .create(
            factory.name(),
            &config,
            Rc::new(RefCell::new(cache)).into(),
            clock,
        )
        .unwrap();
    client.connect().await.unwrap();
    (client, events)
}

async fn streaming(
    socket: &MockLseSocket,
    streamed: &[&str],
    defined: bool,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_ws_data(socket.url().to_string())
        .stream_ids(streamed.iter().map(|&id| InstrumentId::from(id)).collect())
        .build();
    let definitions = if defined {
        vec![gold_futures()]
    } else {
        vec![]
    };
    connecting(&config, definitions).await
}

async fn connecting(
    config: &LseDataClientConfig,
    definitions: Vec<InstrumentAny>,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    let events = events();
    let factory = LseDataClientFactory::new();
    let clock = node_clock();
    let mut client = factory
        .create(factory.name(), config, cache_of(definitions), clock)
        .unwrap();
    let connected = client.connect().await;
    (client, connected, events)
}

fn quotes_of(ids: &[&str]) -> SubscribeQuotes {
    SubscribeQuotes::new(
        ids.iter()
            .map(|&id| InstrumentId::from(id))
            .collect::<Vec<_>>(),
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn sent(socket: &MockLseSocket) -> Vec<Value> {
    socket
        .received()
        .into_iter()
        .map(|(_, frame)| frame)
        .filter(|frame| frame["action"] != "auth")
        .collect()
}

const LISTED_FUTURES: [&str; 17] = [
    "AUDUSD",
    "BRENT",
    "CADUSD",
    "CHFUSD",
    "COPPER",
    "CORN",
    "CRUDEOIL",
    "DOW",
    "EURUSD",
    "GASOLINE",
    "GBPUSD",
    "GOLD",
    "HEATINGOIL",
    "JPYUSD",
    "LEANHOGS",
    "LIVECATTLE",
    "SILVER",
];

/// A time days after every window the tests ask for at fixed times, so LSE has loaded each one.
fn now() -> Timestamp {
    "2026-11-16T12:00:00Z".parse().unwrap()
}

fn node_clock() -> Rc<RefCell<dyn Clock>> {
    let clock = VirtualClock::new();
    clock.set_time(UnixNanos::from(now()));
    Rc::new(RefCell::new(clock))
}

#[tokio::test]
async fn a_subscribed_lsedge_instrument_receives_lse_s_quotes_of_bid_and_ask_with_no_sizes() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (mut client, connected, mut events) = streaming(&socket, &[GOLD_FUTURES], true).await;
    connected.unwrap();

    client.subscribe_quotes(quotes_of(&[GOLD_FUTURES])).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("no data event within 10 s")
        .unwrap();
    let DataEvent::Data(Data::Quote(quote)) = event else {
        panic!("not a quote: {event:?}")
    };
    let at: UnixNanos = "2026-11-13T16:44:55.499567Z"
        .parse::<Timestamp>()
        .unwrap()
        .into();
    assert_eq!(
        quote,
        QuoteTick::new(
            InstrumentId::from(GOLD_FUTURES),
            Price::from("3888.0"),
            Price::from("3888.2"),
            Quantity::from(0),
            Quantity::from(0),
            at,
            quote.ts_init,
        )
    );
    assert_eq!(
        sent(&socket),
        [json!({"action": "subscribe", "symbol": "GOLD.F"})]
    );
}

#[tokio::test]
async fn an_unsubscription_of_quotes_goes_to_lse() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (mut client, connected, _) = streaming(&socket, &[GOLD_FUTURES], true).await;
    connected.unwrap();
    client.subscribe_quotes(quotes_of(&[GOLD_FUTURES])).unwrap();
    client.unsubscribe_quotes(&gold_quotes_ended()).unwrap();

    socket.wait_until(|received| received.len() == 3).await;
    assert_eq!(
        sent(&socket)[1],
        json!({"action": "unsubscribe", "symbol": "GOLD.F"})
    );
}

fn gold_quotes_ended() -> UnsubscribeQuotes {
    UnsubscribeQuotes::new(
        InstrumentId::from(GOLD_FUTURES),
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

/// LSE would accept a subscription to the symbol and send nothing.
#[tokio::test]
async fn a_symbol_its_socket_does_not_list_fails_the_connect_naming_it_before_subscribing() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));

    let (client, connected, _) =
        streaming(&socket, &[GOLD_FUTURES, "NOSUCH-FUTURES.LSEDGE"], true).await;

    let err = format!("{:#}", connected.unwrap_err());
    assert!(err.contains("NOSUCH-FUTURES.LSEDGE (NOSUCH.F)"), "{err}");
    assert!(!err.contains(GOLD_FUTURES), "{err}");
    assert!(client.is_disconnected());
    assert_eq!(sent(&socket), Vec::<Value>::new());
}

/// data-ws allows the free key 16 symbols on its one connection.
#[tokio::test]
async fn more_instruments_than_a_connection_allows_fail_the_connect_naming_lse_s_limit() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let ids: Vec<String> = LISTED_FUTURES
        .iter()
        .map(|symbol| format!("{symbol}-FUTURES.LSEDGE"))
        .collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();

    let (_, connected, _) = streaming(&socket, &ids, true).await;

    let err = format!("{:#}", connected.unwrap_err());
    assert!(err.contains("allows 16 symbols"), "{err}");
    assert!(err.contains("17 are streamed"), "{err}");
    assert_eq!(sent(&socket), Vec::<Value>::new());
}

fn trades_subscribed(ids: &[&str]) -> SubscribeTrades {
    SubscribeTrades::new(
        ids.iter()
            .map(|&id| InstrumentId::from(id))
            .collect::<Vec<_>>(),
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn trades_ended(id: &str) -> UnsubscribeTrades {
    UnsubscribeTrades::new(
        InstrumentId::from(id),
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

async fn streaming_options(
    socket: &MockLseSocket,
    streamed: &[&str],
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_ws_data(socket.url().to_string())
        .stream_ids(streamed.iter().map(|&id| InstrumentId::from(id)).collect())
        .build();
    connecting(&config, vec![aapl_call(), aapl_put_2027()]).await
}

#[tokio::test]
async fn subscribed_option_contracts_receive_their_trades_from_one_chain_of_their_underlying() {
    let socket = MockLseSocket::start(|_, frame| {
        let mut answer = data_ws_answer(frame);
        if frame["action"] == "subscribe_options" {
            answer.rotate_left(1); // Send the acknowledgement after all five ticks.
        }
        answer
    });
    let (mut client, connected, mut events) =
        streaming_options(&socket, &[AAPL_CALL, AAPL_PUT_2027]).await;
    connected.unwrap();

    client
        .subscribe_trades(trades_subscribed(&[AAPL_CALL, AAPL_CALL, AAPL_PUT_2027]))
        .unwrap();

    let mut trades = Vec::new();
    for _ in 0..3 {
        let event = next_event(&mut events).await;
        let DataEvent::Data(Data::Trade(trade)) = event else {
            panic!("not a trade: {event:?}")
        };
        trades.push(trade);
    }
    let at = |time: &str| UnixNanos::from(at(&format!("2026-11-12T14:30:00.{time}Z")));
    assert_eq!(
        trades
            .iter()
            .map(|t| (
                t.instrument_id,
                t.price,
                t.size,
                t.aggressor_side,
                t.ts_event
            ))
            .collect::<Vec<_>>(),
        [
            (
                InstrumentId::from(AAPL_CALL),
                Price::from("6.70"),
                Quantity::from(1),
                AggressorSide::NoAggressor,
                at("816861166")
            ),
            (
                InstrumentId::from(AAPL_CALL),
                Price::from("6.75"),
                Quantity::from(2),
                AggressorSide::NoAggressor,
                at("820596224")
            ),
            (
                InstrumentId::from(AAPL_PUT_2027),
                Price::from("19.10"),
                Quantity::from(2),
                AggressorSide::NoAggressor,
                at("998442462")
            ),
        ]
    );
    assert_eq!(
        sent(&socket),
        [json!({"action": "subscribe_options", "underlying": "AAPL"})]
    );
}

/// The retried option command must still reach the queue, not succeed as a no-op.
#[rstest]
#[case::add(false)]
#[case::remove(true)]
#[tokio::test]
async fn a_closed_socket_queue_leaves_an_option_trade_command_retryable(#[case] removing: bool) {
    let socket = MockLseSocket::start(|_, frame| {
        if frame["action"] == "unsubscribe" {
            vec![
                SocketAction::Send(
                    json!({"type":"error", "code":"KEY_REVOKED", "message":"key revoked"})
                        .to_string(),
                ),
                SocketAction::Close(1008, "key revoked".into()),
            ]
        } else {
            data_ws_answer(frame)
        }
    });
    let (mut client, connected, _) = streaming_options(&socket, &[AAPL_CALL, GOLD_FUTURES]).await;
    connected.unwrap();

    if removing {
        client
            .subscribe_trades(trades_subscribed(&[AAPL_CALL]))
            .unwrap();
    }
    let probe = gold_quotes_ended();
    wait_until_async(
        || std::future::ready(client.unsubscribe_quotes(&probe).is_err()),
        Duration::from_secs(10),
    )
    .await;

    for _ in 0..2 {
        let result = if removing {
            client.unsubscribe_trades(&trades_ended(AAPL_CALL))
        } else {
            client.subscribe_trades(trades_subscribed(&[AAPL_CALL]))
        };
        let reason = format!("{:#}", result.unwrap_err());
        assert!(reason.contains("closed"), "{reason}");
    }
    client.disconnect().await.unwrap();
}

/// GOLD.F's first quote fences the replay.
#[tokio::test]
async fn a_shared_chain_replays_only_while_a_distinct_contract_remains() {
    let socket = MockLseSocket::start(|connection, frame| {
        let mut answer = data_ws_answer(frame);
        match (connection, frame["action"].as_str()) {
            (0, Some("subscribe_options")) | (1, Some("subscribe")) => answer.truncate(1),
            (0, Some("subscribe")) | (1, Some("unsubscribe_options")) => {
                return vec![SocketAction::Close(1001, "going away".into())];
            }
            _ => (),
        }
        answer
    });
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_ws_data(socket.url().to_string())
        .stream_ids(vec![
            InstrumentId::from(AAPL_CALL),
            InstrumentId::from(GOLD_FUTURES),
        ])
        .build();
    let (mut client, connected, mut events) =
        connecting(&config, vec![aapl_call(), aapl_put_2027(), gold_futures()]).await;
    connected.unwrap();
    client
        .subscribe_trades(trades_subscribed(&[AAPL_CALL]))
        .unwrap();
    client
        .subscribe_trades(trades_subscribed(&[AAPL_CALL, AAPL_PUT_2027]))
        .unwrap();
    client.unsubscribe_trades(&trades_ended(AAPL_CALL)).unwrap();
    client.subscribe_quotes(quotes_of(&[GOLD_FUTURES])).unwrap();

    let DataEvent::Data(Data::Trade(trade)) = next_event(&mut events).await else {
        panic!("expected the remaining put's trade")
    };
    assert_eq!(trade.instrument_id, InstrumentId::from(AAPL_PUT_2027));
    client
        .unsubscribe_trades(&trades_ended(AAPL_PUT_2027))
        .unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        DataEvent::Data(Data::Quote(_))
    ));
    client.unsubscribe_quotes(&gold_quotes_ended()).unwrap();
    let received = socket
        .wait_until(|frames| {
            frames
                .iter()
                .any(|(n, f)| *n == 2 && f["action"] == "unsubscribe")
        })
        .await;
    client.disconnect().await.unwrap();
    let subscribe = json!({"action": "subscribe_options", "underlying": "AAPL"});
    let unsubscribe = json!({"action": "unsubscribe_options", "underlying": "AAPL"});

    for (connection, expected) in [
        (0, vec![&subscribe]),
        (1, vec![&subscribe, &unsubscribe]),
        (2, vec![]),
    ] {
        let mut frames = received
            .iter()
            .filter(|(n, _)| *n == connection)
            .map(|(_, f)| f);
        assert_eq!(frames.next().unwrap()["action"], "auth");
        assert_eq!(
            frames
                .filter(|f| f.get("underlying").is_some())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

/// AAPL's chain carries every listed contract, so a strategy that follows strikes may subscribe one
/// the config does not name.
#[tokio::test]
async fn an_option_contract_of_an_underlying_the_client_streams_receives_its_trades_unnamed() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (mut client, connected, mut events) = streaming_options(&socket, &[AAPL_CALL]).await;
    connected.unwrap();

    client
        .subscribe_trades(trades_subscribed(&[AAPL_PUT_2027]))
        .unwrap();

    let event = next_event(&mut events).await;
    let DataEvent::Data(Data::Trade(trade)) = event else {
        panic!("not a trade: {event:?}")
    };
    assert_eq!(
        (trade.instrument_id, trade.price, trade.size),
        (
            InstrumentId::from(AAPL_PUT_2027),
            Price::from("19.10"),
            Quantity::from(2)
        )
    );
    assert_eq!(
        sent(&socket),
        [json!({"action": "subscribe_options", "underlying": "AAPL"})]
    );
}

#[tokio::test]
async fn an_option_contract_the_config_does_not_name_ends_its_chain_with_its_trades() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (mut client, connected, _) = streaming_options(&socket, &[AAPL_CALL]).await;
    connected.unwrap();
    client
        .subscribe_trades(trades_subscribed(&[AAPL_PUT_2027]))
        .unwrap();

    client
        .unsubscribe_trades(&trades_ended(AAPL_PUT_2027))
        .unwrap();

    socket.wait_until(|received| received.len() == 3).await;
    assert_eq!(
        sent(&socket),
        [
            json!({"action": "subscribe_options", "underlying": "AAPL"}),
            json!({"action": "unsubscribe_options", "underlying": "AAPL"}),
        ]
    );
}

/// The socket's list holds no contract to check against, so 15 futures and two AAPL contracts
/// connect, and 16 futures and one contract do not.
#[rstest]
#[case::fifteen_futures_and_two_contracts_connect(15, &[AAPL_CALL, AAPL_PUT_2027], true)]
#[case::sixteen_futures_and_one_contract_do_not(16, &[AAPL_CALL], false)]
#[tokio::test]
async fn an_underlying_s_chain_takes_one_subscription_of_its_connection(
    #[case] n: usize,
    #[case] contracts: &[&str],
    #[case] connects: bool,
) {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let futures: Vec<String> = LISTED_FUTURES[..n]
        .iter()
        .map(|symbol| format!("{symbol}-FUTURES.LSEDGE"))
        .collect();
    let ids: Vec<&str> = futures
        .iter()
        .map(String::as_str)
        .chain(contracts.iter().copied())
        .collect();

    let (_, connected, _) = streaming_options(&socket, &ids).await;

    if connects {
        connected.unwrap();
    } else {
        let err = format!("{:#}", connected.unwrap_err());
        assert!(err.contains("allows 16 symbols"), "{err}");
        assert!(err.contains("17 are streamed"), "{err}");
    }
}

#[tokio::test]
async fn trades_of_an_option_contract_the_cache_does_not_define_fail_naming_it_unasked() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_ws_data(socket.url().to_string())
        .stream_ids(vec![InstrumentId::from(AAPL_CALL)])
        .build();
    let (mut client, connected, _) = connecting(&config, vec![]).await;
    connected.unwrap();

    let err = client
        .subscribe_trades(trades_subscribed(&[AAPL_CALL]))
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(
        err.contains(&format!("{AAPL_CALL} is not in the cache")),
        "{err}"
    );
    assert_eq!(sent(&socket), Vec::<Value>::new());
}

/// The client refuses another underlying though the cache defines it, as it would need its own
/// chain beyond the limit `connect` checked.
#[tokio::test]
async fn trades_of_an_option_contract_of_an_underlying_the_client_does_not_stream_fail_naming_it() {
    const MSFT_CALL: &str = "MSFT  261120C00500000.OPRA";
    let InstrumentAny::OptionContract(mut msft_call) = aapl_call() else {
        unreachable!("an option contract")
    };
    msft_call.id = InstrumentId::from(MSFT_CALL);
    msft_call.raw_symbol = Symbol::from("MSFT  261120C00500000");
    msft_call.underlying = "MSFT".into();
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_ws_data(socket.url().to_string())
        .stream_ids(vec![InstrumentId::from(AAPL_CALL)])
        .build();
    let (mut client, connected, _) = connecting(
        &config,
        vec![aapl_call(), InstrumentAny::OptionContract(msft_call)],
    )
    .await;
    connected.unwrap();

    let err = client
        .subscribe_trades(trades_subscribed(&[MSFT_CALL]))
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(err.contains(MSFT_CALL), "{err}");
    assert_eq!(sent(&socket), Vec::<Value>::new());
}

/// LSE's sockets stream a series' ticks as quotes, because a tick's price is not a trade price (a
/// futures tick's price is its bid).
#[tokio::test]
async fn trades_of_an_lsedge_series_are_refused_naming_it() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (mut client, connected, _) = streaming(&socket, &[GOLD_FUTURES], true).await;
    connected.unwrap();

    let err = client
        .subscribe_trades(trades_subscribed(&[GOLD_FUTURES]))
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(err.contains(GOLD_FUTURES), "{err}");
    assert_eq!(sent(&socket), Vec::<Value>::new());
}

#[tokio::test]
async fn quotes_of_an_instrument_the_cache_does_not_define_fail_naming_it_unasked() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (mut client, connected, _) = streaming(&socket, &[GOLD_FUTURES], false).await;
    connected.unwrap();

    let err = client
        .subscribe_quotes(quotes_of(&[GOLD_FUTURES]))
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(
        err.contains(&format!("{GOLD_FUTURES} is not in the cache")),
        "{err}"
    );
    assert_eq!(sent(&socket), Vec::<Value>::new());
}

#[tokio::test]
async fn quotes_of_an_instrument_the_client_does_not_stream_fail_naming_it() {
    let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (mut client, connected, _) = streaming(&socket, &[GOLD_FUTURES], true).await;
    connected.unwrap();

    let err = client
        .subscribe_quotes(quotes_of(&["SILVER-FUTURES.LSEDGE"]))
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(err.contains("SILVER-FUTURES.LSEDGE is not among"), "{err}");
    assert_eq!(sent(&socket), Vec::<Value>::new());
}

fn index(id: &str, raw: &str, precision: u8) -> InstrumentAny {
    InstrumentAny::IndexInstrument(
        IndexInstrument::builder()
            .instrument_id(InstrumentId::from(id))
            .raw_symbol(Symbol::from(raw))
            .currency(Currency::USD())
            .price_precision(precision)
            .size_precision(0)
            .price_increment(Price::new(10f64.powi(-i32::from(precision)), precision))
            .size_increment(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

fn subscribed(socket: &MockLseSocket) -> Vec<String> {
    sent(socket)
        .iter()
        .filter(|frame| frame["action"] == "subscribe")
        .map(|frame| frame["symbol"].as_str().unwrap().to_string())
        .collect()
}

/// Each socket answers as LSE's did on 13 November 2026. data-ws lists `BTC/USD` too, with a tick
/// of 8 November. Crypto has streamed on london-ws since.
#[tokio::test]
async fn a_series_of_each_derived_market_subscribes_on_its_socket_under_that_socket_s_name() {
    let data =
        MockLseSocket::start(|_, frame| socket_answer(WS_AUTHENTICATED_DATA, WS_TICKS_DATA, frame));
    let london = MockLseSocket::start(|_, frame| {
        socket_answer(WS_AUTHENTICATED_LONDON, WS_TICKS_LONDON, frame)
    });
    let tsx =
        MockLseSocket::start(|_, frame| socket_answer(WS_AUTHENTICATED_TSX, WS_TICKS_TSX, frame));

    // Each row has the series, its LSE symbol, its price precision, and the captured tick's bid/ask
    let series = [
        ("GOLD-FUTURES.LSEDGE", "GOLD", 1, "3888.1", "3888.5"),
        ("AAPL-US_STOCKS.LSEDGE", "AAPL", 2, "355.55", "355.64"),
        ("EUR_USD-FOREX.LSEDGE", "EUR_USD", 5, "1.10209", "1.10221"),
        (
            "XAU/USD-COMMODITIES.LSEDGE",
            "XAU/USD",
            2,
            "3881.06",
            "3882.03",
        ),
        ("SPX-INDICES.LSEDGE", "SPX", 2, "7561.67", "7561.67"),
        (
            "BTCUSDT-CRYPTO.LSEDGE",
            "BTCUSDT",
            2,
            "68267.34",
            "68267.35",
        ),
        ("VOD-UK_STOCKS.LSEDGE", "VOD", 1, "114.2", "120.0"),
        ("NVDA-CA_STOCKS.LSEDGE", "NVDA", 2, "36.15", "36.15"),
    ];
    let ids: Vec<&str> = series.iter().map(|(id, ..)| *id).collect();
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_ws_data(data.url().to_string())
        .base_url_ws_london(london.url().to_string())
        .base_url_ws_tsx(tsx.url().to_string())
        .stream_ids(ids.iter().map(|&id| InstrumentId::from(id)).collect())
        .build();
    let definitions = series
        .iter()
        .map(|&(id, raw, precision, ..)| index(id, raw, precision))
        .collect();
    let (mut client, connected, mut events) = connecting(&config, definitions).await;
    connected.unwrap();

    client.subscribe_quotes(quotes_of(&ids)).unwrap();

    let mut quoted = Vec::new();
    while quoted.len() < series.len() {
        let event = next_event(&mut events).await;
        let DataEvent::Data(Data::Quote(quote)) = event else {
            panic!("not a quote: {event:?}")
        };
        quoted.push((quote.instrument_id, quote.bid_price, quote.ask_price));
    }
    quoted.sort_by_key(|(id, ..)| id.to_string());
    let mut captured: Vec<_> = series
        .iter()
        .map(|&(id, .., bid, ask)| (InstrumentId::from(id), Price::from(bid), Price::from(ask)))
        .collect();
    captured.sort_by_key(|(id, ..)| id.to_string());
    assert_eq!(quoted, captured);
    assert_eq!(
        subscribed(&data),
        ["GOLD.F", "AAPL", "EUR/USD", "XAU/USD", "SPX.IDX"]
    );
    assert_eq!(subscribed(&london), ["BTC/USD", "VOD.L"]);
    assert_eq!(subscribed(&tsx), ["NVDA.TO"]);
}

fn config(lse: &MockLse) -> LseDataClientConfig {
    LseDataClientConfig {
        api_key: Some(SecretString::from(API_KEY)),
        cme_api_key: Some(SecretString::from(CME_API_KEY)),
        nasdaq_api_key: Some(SecretString::from(NASDAQ_API_KEY)),
        base_url_http: Some(lse.url().to_string()),
        base_url_ws_data: None,
        base_url_ws_london: None,
        base_url_ws_tsx: None,
        base_url_ws_cme: None,
        base_url_ws_nasdaq: None,
        calls_per_minute: NonZeroU32::new(6000).unwrap(),
        cme_calls_per_minute: NonZeroU32::new(6000).unwrap(),
        nasdaq_calls_per_minute: NonZeroU32::new(6000).unwrap(),
        stream_ids: vec![],
    }
}

fn cache_of(instruments: Vec<InstrumentAny>) -> CacheView {
    let mut cache = Cache::default();
    for instrument in instruments {
        cache.add_instrument(instrument).unwrap();
    }
    Rc::new(RefCell::new(cache)).into()
}

fn events() -> Events {
    let (tx, events) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    events
}

async fn connected_with(
    config: LseDataClientConfig,
    instruments: Vec<InstrumentAny>,
) -> (Box<dyn DataClient>, Events) {
    let events = events();
    let factory = LseDataClientFactory::new();
    let clock = node_clock();
    let mut client = factory
        .create(factory.name(), &config, cache_of(instruments), clock)
        .unwrap();
    client.connect().await.unwrap();
    (client, events)
}

async fn next_event(events: &mut Events) -> DataEvent {
    tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("no data event within 10 s")
        .expect("the data events ended")
}

async fn next_envelope(events: &mut Events) -> DataResponseEnvelope {
    let event = next_event(events).await;
    let DataEvent::ResponseEnvelope(envelope) = event else {
        panic!("not a response envelope: {event:?}");
    };
    assert!(envelope.completed());
    envelope
}

fn at(s: &str) -> Timestamp {
    s.parse().unwrap()
}

fn trades(from: &str, to: &str) -> RequestTrades {
    RequestTrades::new(
        InstrumentId::from(GOLD_FUTURES),
        Some(at(from)),
        Some(at(to)),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn captured_second() -> RequestTrades {
    trades("2026-10-21T10:00:00Z", "2026-10-21T10:00:00.999999999Z")
}

fn trades_of(envelope: &DataResponseEnvelope) -> TradesResponse {
    match envelope.response() {
        Some(DataResponse::Trades(response)) => response.clone(),
        other => panic!("not a trades response: {other:?}"),
    }
}

fn bars(bar_type: &str, from: &str, to: &str) -> RequestBars {
    RequestBars::new(
        BarType::from(bar_type),
        Some(at(from)),
        Some(at(to)),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn bars_of(envelope: &DataResponseEnvelope) -> BarsResponse {
    match envelope.response() {
        Some(DataResponse::Bars(response)) => response.clone(),
        other => panic!("not a bars response: {other:?}"),
    }
}

fn asked_windows(lse: &MockLse) -> Vec<(String, String)> {
    lse.calls()
        .iter()
        .map(|call| {
            (
                call.param("from").unwrap().to_string(),
                call.param("to").unwrap().to_string(),
            )
        })
        .collect()
}

const GOLD_SPOT_MINUTE_BARS: &str = "XAU/USD-COMMODITIES.LSEDGE-1-MINUTE-LAST-EXTERNAL";

#[tokio::test]
async fn the_factory_creates_the_client_named_lse_on_no_venue() {
    let lse = MockLse::start(vec![]);

    let (client, _) = connected(&lse, true).await;

    assert_eq!(client.client_id(), ClientId::from("LSE"));
    assert_eq!(client.venue(), None);
    assert!(client.is_connected());
}

/// LSE publishes no definitions. The data engine asks the client for one only when its catalogs
/// lack it.
#[tokio::test]
async fn an_instrument_request_fails_naming_the_instrument_unasked() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected(&lse, false).await;
    let request = RequestInstrument::new(
        InstrumentId::from(GOLD_FUTURES),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let id = request.request_id;

    client.request_instrument(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(*envelope.correlation_id(), id);
    assert!(envelope.response().is_none());
    let reason = envelope.failure_reason().expect("a failure");
    for part in [GOLD_FUTURES, "LSE publishes no instrument definitions"] {
        assert!(reason.contains(part), "{part}: {reason}");
    }
    assert!(lse.calls().is_empty());
}

#[tokio::test]
async fn a_trades_request_is_answered_with_lse_s_trades_in_one_complete_envelope() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let (client, mut events) = connected(&lse, true).await;
    let request = captured_second();
    let id = request.request_id;

    client.request_trades(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let response = trades_of(&envelope);
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.client_id, ClientId::from("LSE"));
    assert_eq!(response.data.len(), 6);
    let [call] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls())
    };
    assert_eq!(
        (call.param("from"), call.param("to")),
        (Some("2026-10-21T10:00:00Z"), Some("2026-10-21T10:00:01Z"))
    );
}

/// A stopped client connects again (Nautilus' adapter guide, "Connection lifecycle").
#[tokio::test]
async fn a_stopped_client_connects_again_and_answers_requests() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let (mut client, mut events) = connected(&lse, true).await;

    client.stop().unwrap();
    assert!(!client.is_connected());
    client.connect().await.unwrap();

    assert!(client.is_connected());
    client.request_trades(captured_second()).unwrap();
    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(trades_of(&envelope).data.len(), 6);
}

/// The 20th's hour is one LSE holds no session of.
#[tokio::test]
async fn a_window_over_midnight_is_asked_a_utc_day_at_a_time() {
    let lse = MockLse::start(vec![
        Reply::refusal(404, DATE_NOT_TRADING),
        Reply::file(GOLD_TRADES),
    ]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_trades(trades(
            "2026-10-20T23:00:00Z",
            "2026-10-21T00:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(trades_of(&envelope).data.len(), 6);
    let asked: Vec<_> = lse
        .calls()
        .iter()
        .map(|call| {
            (
                call.param("from").unwrap().to_string(),
                call.param("to").unwrap().to_string(),
            )
        })
        .collect();
    let day = |from: &str, to: &str| (from.to_string(), to.to_string());
    assert_eq!(
        asked,
        [
            day("2026-10-20T23:00:00Z", "2026-10-21T00:00:00Z"),
            day("2026-10-21T00:00:00Z", "2026-10-21T01:00:00Z"),
        ]
    );
}

#[tokio::test]
async fn a_day_window_ending_at_a_midnight_is_one_export() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_trades(trades("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z"))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let [call] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls())
    };
    assert_eq!(
        (call.param("from"), call.param("to")),
        (
            Some("2026-10-20T00:00:00Z"),
            Some("2026-10-21T00:00:00.000000001Z")
        )
    );
}

#[tokio::test]
async fn a_trades_request_reaching_into_the_day_lse_has_not_loaded_is_answered_unwritten() {
    let lse = MockLse::start(vec![
        Reply::file(GOLD_TRADES),
        Reply::refusal(404, DATA_UNAVAILABLE_TODAY),
    ]);
    let (client, mut events) = connected(&lse, true).await;
    let today = Timestamp::from_second(now().as_second() / 86_400 * 86_400).unwrap();
    let day_before = today - SignedDuration::from_hours(24);
    let request = RequestTrades::new(
        InstrumentId::from(GOLD_FUTURES),
        Some(day_before),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let id = request.request_id;

    client.request_trades(request).unwrap();

    let event = next_event(&mut events).await;
    let DataEvent::Response(DataResponse::Trades(response)) = event else {
        panic!("not a plain trades response: {event:?}");
    };
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.data.len(), 6);
    let day_before = day_before.to_string();
    assert_eq!(
        lse.calls().first().and_then(|call| call.param("from")),
        Some(day_before.as_str())
    );
}

#[tokio::test]
async fn a_trades_request_of_an_instrument_not_in_the_cache_fails_naming_it_unasked() {
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let (client, mut events) = connected(&lse, false).await;

    client.request_trades(captured_second()).unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(
        reason.contains(&format!("{GOLD_FUTURES} is not in the cache")),
        "{reason}"
    );
    assert!(lse.calls().is_empty());
}

#[rstest]
#[case::a_plan_the_key_lacks(
    403,
    PLAN_REQUIRED,
    &["plan-required", "This feed requires a subscription", "CME Group Lite"],
)]
#[case::a_symbol_lse_does_not_know(
    404,
    SYMBOL_UNKNOWN,
    &["symbol-unknown", "symbol is not available"],
)]
#[tokio::test]
async fn lse_s_refusal_is_the_request_s_failure_with_its_code_message_and_required_plan(
    #[case] status: u16,
    #[case] refusal: &str,
    #[case] parts: &[&str],
) {
    let lse = MockLse::start(vec![Reply::refusal(status, refusal)]);
    let (client, mut events) = connected(&lse, true).await;

    client.request_trades(captured_second()).unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    for part in parts {
        assert!(reason.contains(part), "{part}: {reason}");
    }
}

fn lse_rows(source: &str, symbol: &str) -> DataType {
    let metadata = Params::new(
        [
            ("source".to_string(), json!(source)),
            ("symbol".to_string(), json!(symbol)),
        ]
        .into(),
    );
    DataType::new(
        LseRow::type_name_static(),
        Some(metadata),
        Some(symbol.to_string()),
    )
}

fn bond_yields(from: &str, to: &str) -> RequestCustomData {
    rows_of(lse_rows("bond_yields", "US10Y"), from, to)
}

fn rows_of(data_type: DataType, from: &str, to: &str) -> RequestCustomData {
    RequestCustomData::new(
        ClientId::from("LSE"),
        data_type,
        Some(at(from)),
        Some(at(to)),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn rows_in(envelope: &DataResponseEnvelope) -> CustomDataResponse {
    match envelope.response() {
        Some(DataResponse::Data(response)) => response.clone(),
        other => panic!("not a custom data response: {other:?}"),
    }
}

fn custom_data(response: &CustomDataResponse) -> Vec<CustomData> {
    response
        .data
        .downcast_ref::<Vec<CustomData>>()
        .expect("custom data")
        .clone()
}

fn day(date: &str) -> UnixNanos {
    UnixNanos::from(at(&format!("{date}T00:00:00Z")))
}

#[tokio::test]
async fn a_custom_data_request_is_answered_with_lse_s_rows_in_one_complete_envelope() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::file(US10Y_BOND_YIELDS),
    ]);
    let (client, mut events) = connected(&lse, false).await;
    let mut request = bond_yields("2026-11-10T00:00:00Z", "2026-11-13T00:00:00Z");
    let params = Params::new([("update_catalog".to_string(), serde_json::json!(true))].into());
    request.params = Some(params.clone());
    let id = request.request_id;

    client.request_data(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let response = rows_in(&envelope);
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.params, Some(params));
    let data_type = lse_rows("bond_yields", "US10Y");
    assert_eq!(response.data_type, data_type);
    let rows = custom_data(&response);
    assert!(rows.iter().all(|row| row.data_type == data_type));
    let times: Vec<_> = rows.iter().map(CustomData::ts_init).collect();
    assert_eq!(
        times,
        [day("2026-11-10"), day("2026-11-11"), day("2026-11-13")]
    );
    let [catalogue, export] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls())
    };
    assert_eq!(catalogue.path, "/v3/reference");
    assert_eq!(export.path, "/v3/reference/export");
    assert_eq!(
        (
            export.param("dataset"),
            export.param("symbol"),
            export.param("from"),
            export.param("to")
        ),
        (
            Some("bond_yields"),
            Some("US10Y"),
            Some("2026-11-09"),
            Some("2026-11-12")
        )
    );
}

/// The client answers with the data type as asked. The data engine then writes the rows back under
/// the caller's identifier.
#[tokio::test]
async fn a_custom_data_request_is_answered_under_the_identifier_it_names() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::file(US10Y_BOND_YIELDS),
    ]);
    let (client, mut events) = connected(&lse, false).await;
    let metadata = lse_rows("bond_yields", "US10Y").metadata().cloned();
    let treasury = DataType::new(
        LseRow::type_name_static(),
        metadata,
        Some("TREASURY-10Y".to_string()),
    );

    client
        .request_data(rows_of(
            treasury,
            "2026-11-10T00:00:00Z",
            "2026-11-13T00:00:00Z",
        ))
        .unwrap();

    let response = rows_in(&next_envelope(&mut events).await);
    let identifiers: Vec<_> = std::iter::once(&response.data_type)
        .chain(custom_data(&response).iter().map(|row| &row.data_type))
        .map(|data_type| data_type.identifier().map(String::from))
        .collect();
    assert_eq!(identifiers, vec![Some("TREASURY-10Y".to_string()); 4]);
}

#[tokio::test]
async fn rows_outside_the_request_s_window_are_left_out() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::file(US10Y_BOND_YIELDS),
    ]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(bond_yields(
            "2026-11-11T00:00:00Z",
            "2026-11-11T23:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let times: Vec<_> = custom_data(&rows_in(&envelope))
        .iter()
        .map(CustomData::ts_init)
        .collect();
    assert_eq!(times, [day("2026-11-11")]);
    let export = &lse.calls()[1];
    assert_eq!(
        (export.param("from"), export.param("to")),
        (Some("2026-11-10"), Some("2026-11-10"))
    );
}

#[tokio::test]
async fn a_request_reaching_into_a_day_lse_has_not_loaded_is_answered_unwritten() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::refusal(404, REFERENCE_DATA_UNAVAILABLE),
    ]);
    let (client, mut events) = connected(&lse, false).await;
    let today = Timestamp::from_second(now().as_second() / 86_400 * 86_400).unwrap();
    let mut request = bond_yields("2026-11-10T00:00:00Z", "2026-11-13T00:00:00Z");
    request.start = Some(today - SignedDuration::from_hours(24));
    request.end = None;
    let id = request.request_id;

    client.request_data(request).unwrap();

    let event = next_event(&mut events).await;
    let DataEvent::Response(DataResponse::Data(response)) = event else {
        panic!("not a plain custom data response: {event:?}");
    };
    assert_eq!(response.correlation_id, id);
    assert_eq!(custom_data(&response), []);
}

/// A window ending on the last date is complete, because LSE loads a day whole.
#[rstest]
#[case::filed_after_the_last_date(
    "insider_trades", "TEAM", TEAM_INSIDER_TRADES,
    ("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z"), false
)]
#[case::updated_after_the_last_date(
    "company_profiles", "AHMA", AHMA_COMPANY_PROFILES,
    ("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z"), false
)]
#[case::updated_on_the_last_date(
    "company_profiles", "AHMA", AHMA_COMPANY_PROFILES,
    ("2026-10-13T00:00:00Z", "2026-10-13T12:00:00Z"), true
)]
#[tokio::test]
async fn a_window_keyed_after_the_last_lse_s_catalogue_holds_is_answered_unwritten(
    #[case] dataset: &str,
    #[case] symbol: &str,
    #[case] file: &'static [u8],
    #[case] (from, to): (&str, &str),
    #[case] written: bool,
) {
    let lse = MockLse::start(vec![Reply::json(REFERENCE_CATALOGUE), Reply::file(file)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(rows_of(lse_rows(dataset, symbol), from, to))
        .unwrap();

    let event = next_event(&mut events).await;
    let answered = match event {
        DataEvent::ResponseEnvelope(envelope) if envelope.failure_reason().is_none() => true,
        DataEvent::Response(DataResponse::Data(_)) => false,
        other => panic!("neither a complete envelope nor a plain response: {other:?}"),
    };
    assert_eq!(answered, written);
}

#[tokio::test]
async fn a_selection_lse_holds_no_row_of_is_answered_complete_with_no_rows() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::refusal(404, REFERENCE_DATA_UNAVAILABLE),
    ]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(bond_yields(
            "2026-11-08T00:00:00Z",
            "2026-11-09T23:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(custom_data(&rows_in(&envelope)), []);
    let export = &lse.calls()[1];
    assert_eq!(
        (export.param("from"), export.param("to")),
        (Some("2026-11-07"), Some("2026-11-08"))
    );
}

#[tokio::test]
async fn lse_s_refusal_of_the_export_is_the_request_s_failure() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::refusal(404, SYMBOL_UNKNOWN),
    ]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(bond_yields(
            "2026-11-09T00:00:00Z",
            "2026-11-12T23:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("symbol-unknown"), "{reason}");
}

#[tokio::test]
async fn a_source_the_client_knows_no_stamp_of_fails_naming_it_unasked() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(rows_of(
            lse_rows("no_such_collection", "US10Y"),
            "2026-11-09T00:00:00Z",
            "2026-11-12T23:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("no_such_collection"), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

#[tokio::test]
async fn a_collection_lse_s_catalogue_does_not_list_fails_naming_it() {
    let lse = MockLse::start(vec![Reply::json(r#"{"datasets": []}"#)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(bond_yields(
            "2026-11-09T00:00:00Z",
            "2026-11-12T23:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("bond_yields"), "{reason}");
    assert_eq!(lse.calls().len(), 1);
}

fn stamps(envelope: &DataResponseEnvelope) -> Vec<UnixNanos> {
    custom_data(&rows_in(envelope))
        .iter()
        .map(CustomData::ts_init)
        .collect()
}

fn utc(time: &str) -> UnixNanos {
    UnixNanos::from(at(time))
}

/// Each collection is asked for the rows known over the window, by the dates of their keys: a day
/// before the window for a row known at the end of its date, an interval before for one known at
/// its interval's close, and the window's own dates for a snapshot known when updated.
///
/// Atlassian's insider trade is stamped 22:00 New York time, 02:00 UTC on the 14th, as a Form 3, 4
/// or 5 takes the filing date up to then.
#[rstest]
#[case::market_values(
    "sovereign_yields", "AUGB10Y", AUGB10Y_SOVEREIGN_YIELDS,
    ("2026-11-11T00:00:00Z", "2026-11-12T00:00:00Z"), ("2026-11-10", "2026-11-11"),
    [&["2026-11-11T00:00:00Z"; 36][..], &["2026-11-12T00:00:00Z"; 36]].concat()
)]
#[case::a_filing_s_day(
    "insider_trades", "TEAM", TEAM_INSIDER_TRADES,
    ("2026-10-14T00:00:00Z", "2026-10-14T23:59:59Z"), ("2026-10-12", "2026-10-13"),
    vec!["2026-10-14T02:00:00Z"]
)]
#[case::an_ex_date(
    "dividends", "AAPL", AAPL_DIVIDENDS,
    ("2026-05-01T00:00:00Z", "2026-11-01T00:00:00Z"), ("2026-04-30", "2026-10-31"),
    vec!["2026-05-12T00:00:00Z", "2026-08-11T00:00:00Z"]
)]
#[case::a_minute(
    "bond_yields_minute", "US10Y", US10Y_BOND_YIELDS_MINUTE,
    ("2026-11-10T00:00:00Z", "2026-11-10T00:30:00Z"), ("2026-11-09", "2026-11-10"),
    vec![
        "2026-11-10T00:09:00Z", "2026-11-10T00:10:00Z", "2026-11-10T00:12:00Z",
        "2026-11-10T00:13:00Z", "2026-11-10T00:14:00Z", "2026-11-10T00:17:00Z",
        "2026-11-10T00:19:00Z", "2026-11-10T00:20:00Z", "2026-11-10T00:21:00Z",
        "2026-11-10T00:22:00Z", "2026-11-10T00:23:00Z", "2026-11-10T00:25:00Z",
        "2026-11-10T00:26:00Z", "2026-11-10T00:27:00Z", "2026-11-10T00:29:00Z",
        "2026-11-10T00:30:00Z",
    ]
)]
#[case::a_snapshot(
    "company_profiles", "AHMA", AHMA_COMPANY_PROFILES,
    ("2026-10-13T00:00:00Z", "2026-10-13T12:00:00Z"), ("2026-10-13", "2026-10-13"),
    vec!["2026-10-13T05:08:01Z"]
)]
#[tokio::test]
async fn a_collection_s_rows_known_over_the_window_are_asked_by_the_dates_of_their_keys(
    #[case] dataset: &str,
    #[case] symbol: &str,
    #[case] file: &'static [u8],
    #[case] (from, to): (&str, &str),
    #[case] dates: (&str, &str),
    #[case] expected: Vec<&str>,
) {
    let lse = MockLse::start(vec![Reply::json(REFERENCE_CATALOGUE), Reply::file(file)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(rows_of(lse_rows(dataset, symbol), from, to))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(
        stamps(&envelope),
        expected.iter().map(|time| utc(time)).collect::<Vec<_>>()
    );
    let export = &lse.calls()[1];
    assert_eq!(
        (
            export.path.as_str(),
            export.param("dataset"),
            export.param("symbol"),
            export.param("from"),
            export.param("to")
        ),
        (
            "/v3/reference/export",
            Some(dataset),
            Some(symbol),
            Some(dates.0),
            Some(dates.1)
        )
    );
}

/// A row stamped by a column other than its key may be keyed any time before the window, so the
/// client asks for the whole series, up to the window's last date.
#[rstest]
#[case::cot(
    "cot", "ZN", ZN_COT, ("2026-09-01T00:00:00Z", "2026-10-01T00:00:00Z"), "2026-10-01",
    vec!["2026-09-04T20:30:16Z", "2026-09-11T20:30:11Z", "2026-09-18T20:30:09Z", "2026-09-25T20:30:08Z"]
)]
#[case::financial_reports(
    "financial_reports", "AAPL", AAPL_FINANCIAL_REPORTS,
    ("2025-07-01T00:00:00Z", "2026-06-30T00:00:00Z"), "2026-06-30",
    [
        &["2025-08-01T10:00:42Z"; 6][..],
        &["2025-10-31T10:01:26Z"; 12],
        &["2026-01-30T11:01:32Z"; 6],
        &["2026-05-01T14:01:00Z"; 6],
    ].concat()
)]
#[tokio::test]
async fn a_series_of_rows_known_at_another_column_s_time_is_asked_whole_up_to_the_window_s_end(
    #[case] dataset: &str,
    #[case] symbol: &str,
    #[case] file: &'static [u8],
    #[case] (from, to): (&str, &str),
    #[case] last_date: &str,
    #[case] expected: Vec<&str>,
) {
    let lse = MockLse::start(vec![Reply::json(REFERENCE_CATALOGUE), Reply::file(file)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(rows_of(lse_rows(dataset, symbol), from, to))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(
        stamps(&envelope),
        expected.iter().map(|time| utc(time)).collect::<Vec<_>>()
    );
    let export = &lse.calls()[1];
    assert_eq!(
        (export.param("from"), export.param("to")),
        (None, Some(last_date))
    );
}

fn core_cpi_calendar(from: &str, to: &str) -> RequestCustomData {
    rows_of(
        lse_rows("economic_calendar", "US.Core Inflation Rate YoY"),
        from,
        to,
    )
}

#[tokio::test]
async fn economic_releases_are_stamped_when_they_came_out_and_answered_complete() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::file(US_CORE_CPI_CALENDAR),
    ]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(core_cpi_calendar(
            "2026-03-01T00:00:00Z",
            "2026-10-31T00:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(stamps(&envelope).len(), 8);
    assert_eq!(stamps(&envelope)[0], utc("2026-03-11T12:30:00Z"));
    let export = &lse.calls()[1];
    assert_eq!(
        (
            export.param("symbol"),
            export.param("from"),
            export.param("to")
        ),
        (Some("US.Core Inflation Rate YoY"), None, Some("2026-10-31"))
    );
}

#[tokio::test]
async fn a_window_of_releases_one_of_which_lse_has_not_filled_is_answered_unwritten() {
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::file(US_CORE_CPI_CALENDAR),
    ]);
    let (client, mut events) = connected(&lse, false).await;
    let request = core_cpi_calendar("2026-02-01T00:00:00Z", "2026-02-28T00:00:00Z");
    let id = request.request_id;

    client.request_data(request).unwrap();

    let event = next_event(&mut events).await;
    let DataEvent::Response(DataResponse::Data(response)) = event else {
        panic!("not a plain custom data response: {event:?}");
    };
    assert_eq!(response.correlation_id, id);
    let stamped: Vec<_> = custom_data(&response)
        .iter()
        .map(CustomData::ts_init)
        .collect();
    assert_eq!(
        stamped,
        [utc("2026-02-11T13:30:00Z"), utc("2026-02-13T13:30:00Z")]
    );
}

fn macro_series(source: &str, code: &str, from: &str, to: &str) -> RequestCustomData {
    rows_of(lse_rows(source, code), from, to)
}

#[rstest]
#[case::releases(
    "macro_release", "release", "US.Core Inflation Rate YoY", US_CORE_CPI_RELEASES,
    ("2026-03-01T00:00:00Z", "2026-10-31T00:00:00Z"), ("2026-03-01", "2026-10-31"), 8
)]
#[case::yields(
    "macro_yield", "yield", "US10Y", US10Y_YIELDS,
    ("2026-11-10T00:00:00Z", "2026-11-13T00:00:00Z"), ("2026-11-09", "2026-11-12"), 3
)]
#[tokio::test]
async fn a_macro_series_is_asked_by_its_kind_and_code(
    #[case] source: &str,
    #[case] kind: &str,
    #[case] code: &str,
    #[case] file: &'static [u8],
    #[case] (from, to): (&str, &str),
    #[case] dates: (&str, &str),
    #[case] count: usize,
) {
    let lse = MockLse::start(vec![Reply::file(file)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(macro_series(source, code, from, to))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(stamps(&envelope).len(), count);
    let [export] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls());
    };
    assert_eq!(
        (
            export.path.as_str(),
            export.param("kind"),
            export.param("code"),
            export.param("from"),
            export.param("to"),
            export.param("format"),
            export.api_key.as_deref()
        ),
        (
            "/v3/macro/export",
            Some(kind),
            Some(code),
            Some(dates.0),
            Some(dates.1),
            Some("parquet"),
            Some(API_KEY)
        )
    );
}

/// An observation is keyed by the day of its period, not its release (US core inflation's September
/// is at 1 September, out on 14 October).
#[tokio::test]
async fn a_macro_observation_is_refused_unasked_naming_the_releases() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(macro_series(
            "macro_observation",
            "US.core_inflation_rate",
            "2026-06-01T00:00:00Z",
            "2026-10-31T00:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("macro_release"), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

fn factors(source: &str, symbol: &str, from: &str, to: &str) -> RequestCustomData {
    rows_of(lse_rows(source, symbol), from, to)
}

#[tokio::test]
async fn an_instrument_s_splits_are_asked_of_lse_s_factors_by_its_market_and_symbol() {
    let lse = MockLse::start(vec![Reply::json(AAPL_FACTORS)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(factors(
            "factors_splits",
            "AAPL-US_STOCKS",
            "2000-01-01T00:00:00Z",
            "2026-11-01T00:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let rows: Vec<_> = custom_data(&rows_in(&envelope))
        .iter()
        .map(|data| {
            let row = data.data.as_any().downcast_ref::<LseRow>().unwrap().clone();
            (row.ts_init, row.fields["volume_factor"].clone())
        })
        .collect();
    let split = |day: &str, factor: f64| (utc(day), serde_json::json!(factor));
    assert_eq!(
        rows,
        [
            split("2000-06-22T00:00:00Z", 2.0),
            split("2005-03-01T00:00:00Z", 2.0),
            split("2014-06-10T00:00:00Z", 7.0),
            split("2020-09-01T00:00:00Z", 4.0),
        ]
    );
    let [call] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls());
    };
    assert_eq!(
        (
            call.path.as_str(),
            call.param("market"),
            call.param("symbol")
        ),
        ("/v3/factors", Some("us_stocks"), Some("AAPL"))
    );
}

#[tokio::test]
async fn an_instrument_s_dividends_are_its_factors_with_the_unit_of_their_amount() {
    let lse = MockLse::start(vec![Reply::json(AAPL_FACTORS)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(factors(
            "factors_dividends",
            "AAPL-US_STOCKS",
            "2026-01-01T00:00:00Z",
            "2026-10-31T00:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let rows: Vec<_> = custom_data(&rows_in(&envelope))
        .iter()
        .map(|data| {
            let row = data.data.as_any().downcast_ref::<LseRow>().unwrap().clone();
            (
                row.ts_init,
                row.fields["amount"].clone(),
                row.fields["unit"].clone(),
            )
        })
        .collect();
    let dividend = |day: &str, amount: f64| {
        (
            utc(day),
            serde_json::json!(amount),
            serde_json::json!("USD"),
        )
    };
    assert_eq!(
        rows,
        [
            dividend("2026-02-10T00:00:00Z", 0.26),
            dividend("2026-05-12T00:00:00Z", 0.27),
            dividend("2026-08-11T00:00:00Z", 0.27),
        ]
    );
}

#[tokio::test]
async fn an_instrument_of_no_dividends_has_none() {
    let lse = MockLse::start(vec![Reply::json(GOLD_FACTORS)]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(factors(
            "factors_dividends",
            "GOLD-FUTURES",
            "2026-01-01T00:00:00Z",
            "2026-10-31T00:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(stamps(&envelope), Vec::<UnixNanos>::new());
}

fn truncated_aapl_factors() -> String {
    let mut factors: Value = serde_json::from_str(AAPL_FACTORS).unwrap();
    factors["dividends"]["truncated"] = json!(true);
    factors["dividends"]["count"] = json!(600);
    factors.to_string()
}

/// A window from before the oldest dividend LSE gave may lack some dividends. It fails, naming the
/// reference collection that holds them all.
#[rstest]
#[case::from_before_the_oldest("1980-01-01T00:00:00Z", false)]
#[case::from_after_the_oldest("2026-01-01T00:00:00Z", true)]
#[tokio::test]
async fn dividends_lse_cut_to_its_newest_fail_a_window_from_before_them(
    #[case] from: &str,
    #[case] served: bool,
) {
    let lse = MockLse::start(vec![Reply::json(&truncated_aapl_factors())]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(factors(
            "factors_dividends",
            "AAPL-US_STOCKS",
            from,
            "2026-10-31T00:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    match envelope.failure_reason() {
        None => assert!(served, "served a window from before the oldest"),
        Some(reason) => {
            assert!(!served, "{reason}");
            assert!(reason.contains("600"), "{reason}");
            assert!(reason.contains("dividends"), "{reason}");
        }
    }
}

#[tokio::test]
async fn factors_of_a_symbol_naming_no_market_fail_naming_it_unasked() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(factors(
            "factors_splits",
            "AAPL",
            "2026-01-01T00:00:00Z",
            "2026-10-31T00:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("AAPL"), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

/// LSE's `/v3/options/chain` takes no dates and answers the chain as it stands, so the client
/// cannot ask for a window.
#[tokio::test]
async fn an_options_chain_s_history_is_refused_by_name_unasked_lse_serving_it_now_alone() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(rows_of(
            lse_rows("options_chain", "AAPL"),
            "2026-11-09T00:00:00Z",
            "2026-11-12T23:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("options_chain"), "{reason}");
    assert!(reason.contains("snapshot"), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

#[rstest]
#[case::not_of_lse_s_rows(DataType::new("Imbalance", None, Some("GC.v.0.GLBX".into())))]
#[case::naming_no_source(DataType::new(LseRow::type_name_static(), None, Some("US10Y".into())))]
#[tokio::test]
async fn custom_data_not_of_lse_s_rows_or_naming_no_source_fails_naming_its_type_unasked(
    #[case] data_type: DataType,
) {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected(&lse, false).await;

    client
        .request_data(rows_of(
            data_type.clone(),
            "2026-11-09T00:00:00Z",
            "2026-11-12T23:59:59.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains(data_type.type_name()), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

/// LSE has no call for funding rates. A request for one instrument is refused as it is made, and a
/// later one fails among its answers.
#[tokio::test]
async fn a_kind_lse_has_no_call_for_is_refused() {
    let lse = MockLse::start(vec![]);
    let (client, _) = connected(&lse, true).await;
    let request = RequestFundingRates::new(
        InstrumentId::from(GOLD_FUTURES),
        None,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    let answer = client.request_funding_rates(request);

    assert!(answer.is_err(), "{answer:?}");
}

#[tokio::test]
async fn a_bars_request_is_answered_with_lse_s_candles_stamped_at_their_close() {
    let lse = MockLse::start(vec![Reply::file(GOLD_SPOT_MINUTES)]);
    let (client, mut events) = connected(&lse, true).await;
    let request = bars(
        GOLD_SPOT_MINUTE_BARS,
        "2026-10-21T10:01:00Z",
        "2026-10-21T10:05:00Z",
    );
    let id = request.request_id;

    client.request_bars(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let response = bars_of(&envelope);
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.client_id, ClientId::from("LSE"));
    assert_eq!(response.bar_type, BarType::from(GOLD_SPOT_MINUTE_BARS));
    let closes: Vec<_> = response
        .data
        .iter()
        .map(|bar| (bar.ts_event, bar.ts_init, bar.close))
        .collect();
    let close = |time: &str, price| {
        let time = UnixNanos::from(at(&format!("2026-10-21T{time}Z")));
        (time, time, Price::from(price))
    };
    assert_eq!(
        closes,
        [
            close("10:01:00", "4118.01"),
            close("10:02:00", "4116.94"),
            close("10:03:00", "4116.17"),
            close("10:04:00", "4116.12"),
            close("10:05:00", "4118.16"),
        ]
    );
    assert_eq!(
        asked_windows(&lse),
        [(
            "2026-10-21T10:00:00Z".to_string(),
            "2026-10-21T10:05:00Z".to_string()
        )]
    );
}

/// LSE adjusts prices and volumes for splits unless told not to.
#[tokio::test]
async fn candles_are_asked_of_the_series_as_published() {
    let lse = MockLse::start(vec![Reply::file(GOLD_SPOT_MINUTES)]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_bars(bars(
            GOLD_SPOT_MINUTE_BARS,
            "2026-10-21T10:01:00Z",
            "2026-10-21T10:05:00Z",
        ))
        .unwrap();

    next_envelope(&mut events).await;
    let [call] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls())
    };
    assert_eq!(call.path, "/v3/export");
    let mut query = call.query.clone();
    query.sort();
    let expected = [
        ("adjusted", "0"),
        ("format", "parquet"),
        ("from", "2026-10-21T10:00:00Z"),
        ("market", "commodities"),
        ("symbol", "XAU/USD"),
        ("timeframe", "1m"),
        ("to", "2026-10-21T10:05:00Z"),
    ]
    .map(|(name, value)| (name.to_string(), value.to_string()));
    assert_eq!(query, expected);
}

/// The bars close from 02:00. The first closes at 04:00 and opens at 00:00. LSE folds a candle from
/// the rows inside the export alone, so an export from 22:00 or 02:00 would cut it.
#[tokio::test]
async fn only_whole_candles_are_asked() {
    let lse = MockLse::start(vec![Reply::file(GOLD_FUTURES_4H)]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_bars(bars(
            &format!("{GOLD_FUTURES}-4-HOUR-LAST-EXTERNAL"),
            "2026-10-21T02:00:00Z",
            "2026-10-21T12:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(bars_of(&envelope).data.len(), 3);
    assert_eq!(
        asked_windows(&lse),
        [(
            "2026-10-21T00:00:00Z".to_string(),
            "2026-10-21T12:00:00Z".to_string()
        )]
    );
}

#[tokio::test]
async fn candles_over_midnight_are_asked_a_utc_day_at_a_time() {
    let lse = MockLse::start(vec![
        Reply::refusal(404, DATA_UNAVAILABLE),
        Reply::refusal(404, DATA_UNAVAILABLE),
    ]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_bars(bars(
            GOLD_SPOT_MINUTE_BARS,
            "2026-10-20T23:59:00Z",
            "2026-10-21T00:01:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let day = |from: &str, to: &str| (from.to_string(), to.to_string());
    assert_eq!(
        asked_windows(&lse),
        [
            day("2026-10-20T23:58:00Z", "2026-10-21T00:00:00Z"),
            day("2026-10-21T00:00:00Z", "2026-10-21T00:01:00Z"),
        ]
    );
}

#[tokio::test]
async fn a_window_no_bar_closes_in_is_answered_with_none_unasked() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_bars(bars(
            GOLD_SPOT_MINUTE_BARS,
            "2026-10-21T10:00:30Z",
            "2026-10-21T10:00:50Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(bars_of(&envelope).data.len(), 0);
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

/// LSE's candle widths below a day (LSE's API docs, "The selection", `timeframe`), each a Nautilus
/// bar specification.
#[rstest]
#[case::a_second("1-SECOND", "1s")]
#[case::five_seconds("5-SECOND", "5s")]
#[case::fifteen_seconds("15-SECOND", "15s")]
#[case::thirty_seconds("30-SECOND", "30s")]
#[case::a_minute("1-MINUTE", "1m")]
#[case::three_minutes("3-MINUTE", "3m")]
#[case::five_minutes("5-MINUTE", "5m")]
#[case::fifteen_minutes("15-MINUTE", "15m")]
#[case::thirty_minutes("30-MINUTE", "30m")]
#[case::an_hour("1-HOUR", "1h")]
#[case::four_hours("4-HOUR", "4h")]
#[tokio::test]
async fn each_lse_candle_width_is_asked_for_its_bar_specification(
    #[case] spec: &str,
    #[case] timeframe: &str,
) {
    let lse = MockLse::start(vec![Reply::refusal(404, DATA_UNAVAILABLE)]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_bars(bars(
            &format!("{GOLD_FUTURES}-{spec}-LAST-EXTERNAL"),
            "2026-10-21T10:00:00Z",
            "2026-10-21T14:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let asked: Vec<_> = lse
        .calls()
        .iter()
        .map(|call| call.param("timeframe").unwrap().to_string())
        .collect();
    assert_eq!(asked, [timeframe]);
}

#[rstest]
#[case::a_width_lse_has_no_candles_of("2-MINUTE-LAST-EXTERNAL")]
#[case::ticks("100-TICK-LAST-EXTERNAL")]
#[case::a_price_lse_s_candles_do_not_carry("1-MINUTE-MID-EXTERNAL")]
#[case::bars_nautilus_builds("1-MINUTE-LAST-INTERNAL")]
#[tokio::test]
async fn a_bar_type_lse_does_not_serve_is_refused_naming_it_unasked(#[case] spec: &str) {
    let bar_type = format!("{GOLD_FUTURES}-{spec}");
    let lse = MockLse::start(vec![Reply::file(GOLD_FUTURES_4H)]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_bars(bars(
            &bar_type,
            "2026-10-21T10:00:00Z",
            "2026-10-21T14:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains(&bar_type), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

/// LSE stamps them at their session's first traded minute, gives no close, and builds them from its
/// minute bars, so the refusal names the candles to declare instead.
#[rstest]
#[case::daily("1-DAY")]
#[case::weekly("1-WEEK")]
#[case::monthly("1-MONTH")]
#[tokio::test]
async fn daily_weekly_and_monthly_candles_are_refused_naming_the_candles_to_declare_instead(
    #[case] spec: &str,
) {
    let bar_type = format!("{GOLD_FUTURES}-{spec}-LAST-EXTERNAL");
    let lse = MockLse::start(vec![Reply::file(GOLD_FUTURES_4H)]);
    let (client, mut events) = connected(&lse, true).await;

    client
        .request_bars(bars(
            &bar_type,
            "2026-10-21T10:00:00Z",
            "2026-10-21T14:00:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    for fact in [bar_type.as_str(), "1-MINUTE", "4-HOUR"] {
        assert!(reason.contains(fact), "{fact}: {reason}");
    }
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

#[tokio::test]
async fn a_bars_request_reaching_into_the_day_lse_has_not_loaded_is_answered_unwritten() {
    let lse = MockLse::start(vec![
        Reply::file(GOLD_SPOT_MINUTES),
        Reply::refusal(404, DATA_UNAVAILABLE_TODAY),
    ]);
    let (client, mut events) = connected(&lse, true).await;
    let today = Timestamp::from_second(now().as_second() / 86_400 * 86_400).unwrap();
    let day_before = today - SignedDuration::from_hours(24);
    let request = RequestBars::new(
        BarType::from(GOLD_SPOT_MINUTE_BARS),
        Some(day_before + SignedDuration::from_mins(1)),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let id = request.request_id;

    client.request_bars(request).unwrap();

    let event = next_event(&mut events).await;
    let DataEvent::Response(DataResponse::Bars(response)) = event else {
        panic!("not a plain bars response: {event:?}");
    };
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.data.len(), 5);
    let day_before = day_before.to_string();
    assert_eq!(
        lse.calls().first().and_then(|call| call.param("from")),
        Some(day_before.as_str())
    );
}

type WindowRequest<R> = fn(
    InstrumentId,
    Option<Timestamp>,
    Option<Timestamp>,
    Option<std::num::NonZeroUsize>,
    Option<ClientId>,
    UUID4,
    UnixNanos,
    Option<nautilus_core::Params>,
) -> R;

fn gcz26_second<R>(request: WindowRequest<R>) -> R {
    second_of(GCZ26, request)
}

/// Returns `id`'s rows over the captured second of GCZ6 of 2026, `[14:30:00, 14:30:01)` on 12
/// November 2026.
fn second_of<R>(id: &str, request: WindowRequest<R>) -> R {
    request(
        InstrumentId::from(id),
        Some(at("2026-11-12T14:30:00Z")),
        Some(at("2026-11-12T14:30:00.999999999Z")),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

async fn defining_gcz26(lse: &MockLse) -> (Box<dyn DataClient>, Events) {
    connected_with(config(lse), vec![gcz26()]).await
}

type Asked = (String, Option<String>, Option<String>, Option<String>);

fn asked(lse: &MockLse) -> Vec<Asked> {
    lse.calls()
        .iter()
        .map(|call| {
            let param = |name| call.param(name).map(ToString::to_string);
            (
                call.path.clone(),
                param("pair_id").or_else(|| param("symbol")),
                param("data").or_else(|| param("timeframe")),
                call.api_key.clone(),
            )
        })
        .collect()
}

fn call(path: &str, instrument: &str, rows: Option<&str>, key: &str) -> Asked {
    (
        path.to_string(),
        Some(instrument.to_string()),
        rows.map(ToString::to_string),
        Some(key.to_string()),
    )
}

#[tokio::test]
async fn a_cme_contract_s_trades_are_asked_by_its_listing_with_the_cme_key() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::file(GCZ26_TRADES)]);
    let (client, mut events) = defining_gcz26(&lse).await;
    let request = gcz26_second(RequestTrades::new);
    let id = request.request_id;

    client.request_trades(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let response = trades_of(&envelope);
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.data.len(), 38);
    assert_eq!(response.data[0].price, Price::from("3908.10"));
    assert_eq!(
        asked(&lse),
        [
            call("/v3/pairs", "GCZ6", None, CME_API_KEY),
            call("/v3/export", "4000000002", Some("trades"), CME_API_KEY),
        ]
    );
}

#[tokio::test]
async fn a_cme_contract_s_quotes_are_its_best_bid_and_offer_with_lse_s_sizes() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::file(GCZ26_QUOTES)]);
    let (client, mut events) = defining_gcz26(&lse).await;
    let request = gcz26_second(RequestQuotes::new);
    let id = request.request_id;

    client.request_quotes(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let Some(DataResponse::Quotes(response)) = envelope.response() else {
        panic!("not a quotes response: {:?}", envelope.response());
    };
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.client_id, ClientId::from("LSE"));
    assert_eq!(response.data.len(), 636);
    let first = &response.data[0];
    assert_eq!(
        (
            first.bid_price,
            first.ask_price,
            first.bid_size,
            first.ask_size
        ),
        (
            Price::from("3908.10"),
            Price::from("3908.30"),
            Quantity::from(2),
            Quantity::from(9)
        )
    );
    assert_eq!(
        asked(&lse)[1],
        call("/v3/export", "4000000002", Some("quotes"), CME_API_KEY)
    );
}

#[tokio::test]
async fn a_contract_s_listing_is_looked_up_once_for_all_its_requests() {
    let lse = MockLse::start(vec![
        Reply::json(PAIRS_GCZ6),
        Reply::file(GCZ26_TRADES),
        Reply::file(GCZ26_QUOTES),
    ]);
    let (client, mut events) = defining_gcz26(&lse).await;

    client
        .request_trades(gcz26_second(RequestTrades::new))
        .unwrap();
    next_envelope(&mut events).await;
    client
        .request_quotes(gcz26_second(RequestQuotes::new))
        .unwrap();
    next_envelope(&mut events).await;

    let paths: Vec<_> = asked(&lse).into_iter().map(|(path, ..)| path).collect();
    assert_eq!(paths, ["/v3/pairs", "/v3/export", "/v3/export"]);
}

#[tokio::test]
async fn lse_s_refusal_of_a_cme_contract_s_rows_is_the_request_s_failure() {
    let lse = MockLse::start(vec![
        Reply::json(PAIRS_GCZ6),
        Reply::refusal(403, HISTORY_LIMIT),
    ]);
    let (client, mut events) = defining_gcz26(&lse).await;

    client
        .request_trades(gcz26_second(RequestTrades::new))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    for part in ["history-limit", "12 years of L1 history", "pair 4000000002"] {
        assert!(reason.contains(part), "{part}: {reason}");
    }
}

fn book_depth(depth: Option<usize>, from: &str, to: &str) -> RequestBookDepth {
    RequestBookDepth::new(
        InstrumentId::from(GCZ26),
        Some(at(from)),
        Some(at(to)),
        None,
        depth.map(|depth| std::num::NonZeroUsize::new(depth).unwrap()),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn book_deltas(from: &str, to: &str) -> RequestBookDeltas {
    RequestBookDeltas::new(
        InstrumentId::from(GCZ26),
        Some(at(from)),
        Some(at(to)),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

#[derive(Debug, Clone, Copy)]
enum Level {
    Book,
    Orders,
}

impl Level {
    fn request(self, client: &dyn DataClient, from: &str, to: &str) {
        match self {
            Self::Book => client
                .request_book_depth(book_depth(Some(10), from, to))
                .unwrap(),
            Self::Orders => client.request_book_deltas(book_deltas(from, to)).unwrap(),
        }
    }

    fn rows_of(self, envelope: &DataResponseEnvelope) -> usize {
        match (self, envelope.response()) {
            (Self::Book, Some(DataResponse::BookDepth(response))) => response.data.len(),
            (Self::Orders, Some(DataResponse::BookDeltas(response))) => response.data.len(),
            (_, other) => panic!("not a response of the {self:?}: {other:?}"),
        }
    }
}

/// CME Globex's Level 2, as LSE's docs record it (`test_data/README.md`).
#[tokio::test]
async fn a_cme_contract_s_book_is_its_depth_snapshots_asked_by_its_listing_with_the_cme_key() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::file(GCZ26_BOOK)]);
    let (client, mut events) = defining_gcz26(&lse).await;
    let request = book_depth(
        Some(10),
        "2026-11-12T14:30:00Z",
        "2026-11-12T14:30:00.999999999Z",
    );
    let id = request.request_id;

    client.request_book_depth(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let Some(DataResponse::BookDepth(response)) = envelope.response() else {
        panic!("not a book depth response: {:?}", envelope.response());
    };
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.client_id, ClientId::from("LSE"));
    assert_eq!(response.instrument_id, InstrumentId::from(GCZ26));
    assert_eq!(response.data.len(), 4);
    let first = &response.data[0];
    assert_eq!((first.bids.len(), first.asks.len()), (10, 10));
    assert_eq!(
        (first.bids[0].price, first.asks[0].price),
        (Price::from("3908.10"), Price::from("3908.30"))
    );
    assert_eq!(
        asked(&lse),
        [
            call("/v3/pairs", "GCZ6", None, CME_API_KEY),
            call("/v3/export", "4000000002", Some("book"), CME_API_KEY),
        ]
    );
}

#[tokio::test]
async fn a_cme_contract_s_orders_are_its_book_deltas_asked_by_its_listing_with_the_cme_key() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::file(GCZ26_ORDERS)]);
    let (client, mut events) = defining_gcz26(&lse).await;
    let request = book_deltas("2026-11-12T14:30:00Z", "2026-11-12T14:30:00.999999999Z");
    let id = request.request_id;

    client.request_book_deltas(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let Some(DataResponse::BookDeltas(response)) = envelope.response() else {
        panic!("not a book deltas response: {:?}", envelope.response());
    };
    assert_eq!(response.correlation_id, id);
    assert_eq!(response.client_id, ClientId::from("LSE"));
    let actions: Vec<_> = response.data.iter().map(|delta| delta.action).collect();
    assert_eq!(
        actions,
        [
            BookAction::Add,
            BookAction::Add,
            BookAction::Update,
            BookAction::Delete
        ]
    );
    assert_eq!(
        asked(&lse)[1],
        call("/v3/export", "4000000002", Some("orders"), CME_API_KEY)
    );
}

/// The CME Group Lite+ key holds Level 1 alone, and LSE's refusals were captured with it.
#[rstest]
#[case::book(
    Level::Book,
    LEVEL_REQUIRED_BOOK,
    "Level 2 book needs CME Group Standard or above"
)]
#[case::orders(
    Level::Orders,
    LEVEL_REQUIRED_ORDERS,
    "Level 3 orders needs CME Group Standard or above"
)]
#[tokio::test]
async fn lse_s_level_refusal_of_the_cme_key_is_the_request_s_failure_with_its_required_plan(
    #[case] level: Level,
    #[case] refusal: &str,
    #[case] message: &str,
) {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::refusal(403, refusal)]);
    let (client, mut events) = defining_gcz26(&lse).await;

    level.request(
        &*client,
        "2026-11-12T14:30:00Z",
        "2026-11-12T14:30:00.999999999Z",
    );

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    for part in [
        "403",
        "level-required",
        message,
        "required plan: CME Group Standard",
    ] {
        assert!(reason.contains(part), "{part}: {reason}");
    }
    assert!(envelope.response().is_none());
}

#[rstest]
#[case::book(Level::Book, GCZ26_BOOK, 4)]
#[case::orders(Level::Orders, GCZ26_ORDERS, 4)]
#[tokio::test]
async fn a_book_or_orders_window_lse_refuses_as_too_wide_is_asked_again_in_halves(
    #[case] level: Level,
    #[case] file: &[u8],
    #[case] rows_a_file: usize,
) {
    let lse = MockLse::start(vec![
        Reply::json(PAIRS_GCZ6),
        Reply::refusal(413, EXPORT_TOO_WIDE),
        Reply::file(file),
        Reply::file(file),
    ]);
    let (client, mut events) = defining_gcz26(&lse).await;

    level.request(
        &*client,
        "2026-11-12T14:30:00Z",
        "2026-11-12T14:30:01.999999999Z",
    );

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    assert_eq!(level.rows_of(&envelope), 2 * rows_a_file);
    let windows: Vec<_> = lse
        .calls()
        .iter()
        .filter(|call| call.path == "/v3/export")
        .map(|call| {
            (
                call.param("from").unwrap().to_string(),
                call.param("to").unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        windows,
        [
            ("2026-11-12T14:30:00Z", "2026-11-12T14:30:02Z"),
            ("2026-11-12T14:30:00Z", "2026-11-12T14:30:01Z"),
            ("2026-11-12T14:30:01Z", "2026-11-12T14:30:02Z"),
        ]
        .map(|(from, to)| (from.to_string(), to.to_string()))
    );
}

/// LSE's book holds ten levels on each side, as Nautilus' Databento client serves MBP-10's alone.
#[tokio::test]
async fn a_book_depth_other_than_lse_s_ten_levels_fails_naming_it_unasked() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = defining_gcz26(&lse).await;

    client
        .request_book_depth(book_depth(
            Some(5),
            "2026-11-12T14:30:00Z",
            "2026-11-12T14:30:00.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("depth of 5"), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

#[tokio::test]
async fn a_derived_series_quotes_are_asked_and_lse_s_refusal_fails_them() {
    let lse = MockLse::start(vec![Reply::refusal(400, DATA_UNAVAILABLE_QUOTES)]);
    let (client, mut events) = connected(&lse, true).await;
    let request = RequestQuotes::new(
        InstrumentId::from(GOLD_FUTURES),
        Some(at("2026-10-21T10:00:00Z")),
        Some(at("2026-10-21T10:00:00.999999999Z")),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.request_quotes(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains("data-unavailable"), "{reason}");
    assert_eq!(
        asked(&lse),
        [call("/v3/export", "GOLD", Some("quotes"), API_KEY)]
    );
}

const SPX: &str = "SPX-INDICES.LSEDGE";

fn of_spx<R>(
    request: impl FnOnce(
        InstrumentId,
        Option<Timestamp>,
        Option<Timestamp>,
        Option<NonZeroUsize>,
        Option<ClientId>,
        UUID4,
        UnixNanos,
        Option<Params>,
    ) -> R,
    from: &str,
    to: &str,
) -> R {
    request(
        InstrumentId::from(SPX),
        Some(at(from)),
        Some(at(to)),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

/// Each value is a quote whose bid and ask are the value, with no sizes, as data-ws sends an
/// index's ticks.
#[tokio::test]
async fn an_index_s_quotes_are_the_values_of_its_tick_slice_as_bid_and_ask_with_no_sizes() {
    let lse = MockLse::start(vec![Reply::file(SPX_TICKS)]);
    let (client, mut events) = connected_with(config(&lse), vec![index(SPX, "SPX", 2)]).await;

    client
        .request_quotes(of_spx(
            RequestQuotes::new,
            "2026-11-12T15:00:00Z",
            "2026-11-12T15:00:04.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let Some(DataResponse::Quotes(response)) = envelope.response() else {
        panic!("not a quotes response: {:?}", envelope.response())
    };
    let quote = |time: &str, value: &str| {
        let at = UnixNanos::from(at(&format!("2026-11-12T{time}Z")));
        QuoteTick::new(
            InstrumentId::from(SPX),
            Price::from(value),
            Price::from(value),
            Quantity::from(0),
            Quantity::from(0),
            at,
            at,
        )
    };
    assert_eq!(
        response.data[..],
        [
            quote("15:00:00.034", "7600.07"),
            quote("15:00:02.035", "7599.15"),
            quote("15:00:03.067", "7599.18"),
        ]
    );
    assert_eq!(
        asked(&lse),
        [call("/v3/export", "SPX", Some("tick"), API_KEY)]
    );
}

#[tokio::test]
async fn an_index_s_trades_are_refused_naming_it_unasked() {
    let lse = MockLse::start(vec![]);
    let (client, mut events) = connected_with(config(&lse), vec![index(SPX, "SPX", 2)]).await;

    client
        .request_trades(of_spx(
            RequestTrades::new,
            "2026-11-12T15:00:00Z",
            "2026-11-12T15:00:04.999999999Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert!(envelope.response().is_none());
    let reason = envelope.failure_reason().expect("a failure");
    for part in [SPX, "no trades"] {
        assert!(reason.contains(part), "{part}: {reason}");
    }
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

/// The client serves bars for LSEDGE series and OPRA contracts alone, not for a CME contract or an
/// XNAS stock.
#[rstest]
#[case::a_cme_contract(gcz26(), vec![Reply::json(PAIRS_GCZ6)], GCZ26)]
#[case::an_xnas_stock(nvda(), vec![], NVDA)]
#[tokio::test]
async fn an_exchange_s_instrument_s_bars_fail_naming_them_unasked(
    #[case] definition: InstrumentAny,
    #[case] replies: Vec<Reply>,
    #[case] id: &str,
) {
    let lse = MockLse::start(replies);
    let (client, mut events) = connected_with(config(&lse), vec![definition]).await;
    let bar_type = format!("{id}-1-MINUTE-LAST-EXTERNAL");

    client
        .request_bars(bars(
            &bar_type,
            "2026-11-12T14:31:00Z",
            "2026-11-12T14:35:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    let reason = envelope.failure_reason().expect("a failure");
    assert!(reason.contains(&bar_type), "{reason}");
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

async fn defining_aapl_call(lse: &MockLse) -> (Box<dyn DataClient>, Events) {
    connected_with(config(lse), vec![aapl_call()]).await
}

fn markets(lse: &MockLse) -> Vec<Option<String>> {
    lse.calls()
        .iter()
        .map(|call| call.param("market").map(ToString::to_string))
        .collect()
}

/// The client asks `us_options` by its padded OSI symbol, with the LSE key, which opens LSE's US
/// options.
#[tokio::test]
async fn an_option_s_trades_are_its_tick_slice_asked_by_its_osi_symbol_with_the_lse_key() {
    let lse = MockLse::start(vec![Reply::file(AAPL_CALL_TRADES)]);
    let (client, mut events) = defining_aapl_call(&lse).await;
    let request = RequestTrades::new(
        InstrumentId::from(AAPL_CALL),
        Some(at("2026-11-12T14:30:00Z")),
        Some(at("2026-11-12T14:30:59.999999999Z")),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let id = request.request_id;

    client.request_trades(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let response = trades_of(&envelope);
    assert_eq!(response.correlation_id, id);
    let trade = |trade: &TradeTick| {
        (
            trade.instrument_id,
            trade.ts_event,
            trade.price,
            trade.size,
            trade.aggressor_side,
        )
    };
    let expected = |time: &str, price, size| {
        (
            InstrumentId::from(AAPL_CALL),
            UnixNanos::from(at(time)),
            Price::from(price),
            Quantity::from(size),
            AggressorSide::NoAggressor,
        )
    };
    assert_eq!(response.data.len(), 23);
    assert_eq!(
        [&response.data[0], &response.data[3], &response.data[22]].map(trade),
        [
            expected("2026-11-12T14:30:00.816861166Z", "6.70", 1),
            expected("2026-11-12T14:30:00.820636337Z", "6.75", 11),
            expected("2026-11-12T14:30:49.185406364Z", "6.22", 1),
        ]
    );
    assert_eq!(
        asked(&lse),
        [call(
            "/v3/export",
            "AAPL  261120C00355000",
            Some("tick"),
            API_KEY
        )]
    );
    assert_eq!(markets(&lse), [Some("us_options".to_string())]);
}

#[tokio::test]
async fn an_option_s_candles_are_bars_stamped_at_their_close_asked_by_its_osi_symbol() {
    let lse = MockLse::start(vec![Reply::file(AAPL_CALL_MINUTES)]);
    let (client, mut events) = defining_aapl_call(&lse).await;
    let bar_type = format!("{AAPL_CALL}-1-MINUTE-LAST-EXTERNAL");

    client
        .request_bars(bars(
            &bar_type,
            "2026-11-12T14:31:00Z",
            "2026-11-12T14:35:00Z",
        ))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let rows: Vec<_> = bars_of(&envelope)
        .data
        .iter()
        .map(|b| (b.ts_event, b.open, b.high, b.low, b.close, b.volume))
        .collect();
    let row = |close: &str, [open, high, low, last]: [&str; 4], volume| {
        (
            UnixNanos::from(at(close)),
            Price::from(open),
            Price::from(high),
            Price::from(low),
            Price::from(last),
            Quantity::from(volume),
        )
    };
    assert_eq!(
        rows,
        [
            row("2026-11-12T14:31:00Z", ["6.70", "7.75", "6.12", "6.22"], 60),
            row("2026-11-12T14:32:00Z", ["6.22", "6.65", "6.22", "6.57"], 21),
            row("2026-11-12T14:33:00Z", ["6.35", "6.40", "6.15", "6.15"], 5),
            row(
                "2026-11-12T14:34:00Z",
                ["6.57", "7.00", "6.40", "6.40"],
                121
            ),
        ]
    );
    assert_eq!(
        asked(&lse),
        [call(
            "/v3/export",
            "AAPL  261120C00355000",
            Some("1m"),
            API_KEY
        )]
    );
    assert_eq!(
        asked_windows(&lse),
        [(
            "2026-11-12T14:30:00Z".to_string(),
            "2026-11-12T14:35:00Z".to_string()
        )]
    );
    assert_eq!(markets(&lse), [Some("us_options".to_string())]);
}

async fn defining_nvda(lse: &MockLse) -> (Box<dyn DataClient>, Events) {
    connected_with(config(lse), vec![nvda()]).await
}

fn on_the_12th(time: &str) -> UnixNanos {
    UnixNanos::from(at(&format!("2026-11-12T{time}Z")))
}

/// As LSE's docs record a paid market's trades (`test_data/README.md`). The four trades have no
/// aggressor side, and their ids tell the two alike apart.
#[tokio::test]
async fn an_xnas_stock_s_trades_are_asked_of_lse_s_nasdaq_by_its_ticker_with_the_nasdaq_key() {
    let lse = MockLse::start(vec![Reply::file(NVDA_TRADES)]);
    let (client, mut events) = defining_nvda(&lse).await;

    client
        .request_trades(second_of(NVDA, RequestTrades::new))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let response = trades_of(&envelope);
    let trades: Vec<_> = response
        .data
        .iter()
        .map(|trade| {
            (
                trade.instrument_id,
                trade.ts_event,
                trade.price,
                trade.size,
                trade.aggressor_side,
            )
        })
        .collect();
    let trade = |time: &str, price: &str, size: u64| {
        (
            InstrumentId::from(NVDA),
            on_the_12th(time),
            Price::from(price),
            Quantity::from(size),
            AggressorSide::NoAggressor,
        )
    };
    assert_eq!(
        trades,
        [
            trade("14:30:00.001418191", "211.15", 100),
            trade("14:30:00.001418191", "211.15", 100),
            trade("14:30:00.027098067", "211.14", 200),
            trade("14:30:00.998844133", "211.17", 50),
        ]
    );
    assert_ne!(response.data[0].trade_id, response.data[1].trade_id);
    assert_eq!(
        asked(&lse),
        [call("/v3/export", "NVDA", Some("trades"), NASDAQ_API_KEY)]
    );
    assert_eq!(markets(&lse), [Some("nasdaq".to_string())]);
}

#[tokio::test]
async fn an_xnas_stock_s_quotes_are_its_best_bid_and_offer_with_lse_s_sizes() {
    let lse = MockLse::start(vec![Reply::file(NVDA_QUOTES)]);
    let (client, mut events) = defining_nvda(&lse).await;

    client
        .request_quotes(second_of(NVDA, RequestQuotes::new))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let Some(DataResponse::Quotes(response)) = envelope.response() else {
        panic!("not a quotes response: {:?}", envelope.response());
    };
    let quotes: Vec<_> = response
        .data
        .iter()
        .map(|quote| {
            (
                quote.ts_event,
                quote.bid_price,
                quote.ask_price,
                quote.bid_size,
                quote.ask_size,
            )
        })
        .collect();
    let quote = |time: &str, bid: &str, ask: &str, bid_size: u64, ask_size: u64| {
        (
            on_the_12th(time),
            Price::from(bid),
            Price::from(ask),
            Quantity::from(bid_size),
            Quantity::from(ask_size),
        )
    };
    assert_eq!(
        quotes,
        [
            quote("14:30:00.000184205", "211.15", "211.17", 200, 900),
            quote("14:30:00.001418191", "211.15", "211.17", 100, 900),
            quote("14:30:00.001444877", "211.13", "211.17", 500, 900),
            quote("14:30:00.001576125", "211.13", "211.17", 500, 800),
        ]
    );
    assert_eq!(
        asked(&lse),
        [call("/v3/export", "NVDA", Some("quotes"), NASDAQ_API_KEY)]
    );
}

/// The last snapshot has eight asks, with its last two levels empty. It is Nasdaq TotalView's Level
/// 2.
#[tokio::test]
async fn an_xnas_stock_s_book_is_its_depth_snapshots_asked_by_its_ticker_with_the_nasdaq_key() {
    let lse = MockLse::start(vec![Reply::file(NVDA_BOOK)]);
    let (client, mut events) = defining_nvda(&lse).await;
    let request = RequestBookDepth::new(
        InstrumentId::from(NVDA),
        Some(at("2026-11-12T14:30:00Z")),
        Some(at("2026-11-12T14:30:00.999999999Z")),
        None,
        std::num::NonZeroUsize::new(10),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.request_book_depth(request).unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let Some(DataResponse::BookDepth(response)) = envelope.response() else {
        panic!("not a book depth response: {:?}", envelope.response());
    };
    let depths: Vec<_> = response
        .data
        .iter()
        .map(|depth| {
            (
                depth.ts_event,
                (depth.bids[0].price, depth.bids[0].size, depth.bid_counts[0]),
                (depth.asks[0].price, depth.asks[0].size, depth.ask_counts[0]),
                depth.asks.iter().filter(|ask| ask.side.is_some()).count(),
            )
        })
        .collect();
    let level =
        |price: &str, size: u64, orders: u32| (Price::from(price), Quantity::from(size), orders);
    assert_eq!(
        depths,
        [
            (
                on_the_12th("14:30:00.000184205"),
                level("211.15", 200, 1),
                level("211.17", 900, 4),
                10
            ),
            (
                on_the_12th("14:30:00.001418191"),
                level("211.15", 100, 1),
                level("211.17", 900, 4),
                10
            ),
            (
                on_the_12th("14:30:00.001418191"),
                level("211.15", 100, 1),
                level("211.17", 1100, 5),
                10
            ),
            (
                on_the_12th("14:30:00.998844133"),
                level("211.15", 100, 1),
                level("211.17", 1100, 5),
                8
            ),
        ]
    );
    assert_eq!(
        asked(&lse),
        [call("/v3/export", "NVDA", Some("book"), NASDAQ_API_KEY)]
    );
}

/// An event of no action gives no delta, and the last event of each time ends it. The export is
/// Nasdaq TotalView's Level 3.
#[tokio::test]
async fn an_xnas_stock_s_orders_are_its_book_deltas_asked_by_its_ticker_with_the_nasdaq_key() {
    let lse = MockLse::start(vec![Reply::file(NVDA_ORDERS)]);
    let (client, mut events) = defining_nvda(&lse).await;

    client
        .request_book_deltas(second_of(NVDA, RequestBookDeltas::new))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert_eq!(envelope.failure_reason(), None);
    let Some(DataResponse::BookDeltas(response)) = envelope.response() else {
        panic!("not a book deltas response: {:?}", envelope.response());
    };
    let deltas: Vec<_> = response
        .data
        .iter()
        .map(|delta| {
            (
                delta.action,
                delta.order.order_id,
                delta.order.price,
                delta.order.size,
                RecordFlag::F_LAST.matches(delta.flags),
            )
        })
        .collect();
    assert_eq!(
        deltas,
        [
            (
                BookAction::Add,
                382_910_001,
                Price::from("211.15"),
                Quantity::from(100),
                false
            ),
            (
                BookAction::Add,
                382_910_002,
                Price::from("211.17"),
                Quantity::from(200),
                true
            ),
            (
                BookAction::Update,
                382_910_001,
                Price::from("211.15"),
                Quantity::from(300),
                true
            ),
            (
                BookAction::Delete,
                382_910_002,
                Price::from("211.17"),
                Quantity::from(200),
                true
            ),
        ]
    );
    assert_eq!(
        asked(&lse),
        [call("/v3/export", "NVDA", Some("orders"), NASDAQ_API_KEY)]
    );
}

/// LSE's refusal of Nasdaq TotalView to the keys helix holds (the free key and the CME Group Lite+
/// key), captured alike.
#[tokio::test]
async fn lse_s_refusal_of_a_key_of_no_nasdaq_plan_is_the_request_s_failure_naming_nasdaq_lite() {
    let lse = MockLse::start(vec![Reply::refusal(403, PLAN_REQUIRED_NASDAQ)]);
    let (client, mut events) = defining_nvda(&lse).await;

    client
        .request_trades(second_of(NVDA, RequestTrades::new))
        .unwrap();

    let envelope = next_envelope(&mut events).await;
    assert!(envelope.response().is_none());
    let reason = envelope.failure_reason().expect("a failure");
    for part in ["403", "plan-required", "required plan: Nasdaq Lite"] {
        assert!(reason.contains(part), "{part}: {reason}");
    }
}
