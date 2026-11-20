//! Tests the data client's exchange data through its factory.
//!
//! The tests cover `GLBX` contracts and `XNAS` stocks on the mock CME and Nasdaq streams, at
//! Levels 1 to 3.
//!
//! LSE's documented sessions:
//!
//! - Level 1 of ESZ6 on the CME stream (looked up in LSE's catalogue with the CME key) opens with
//!   the best bid and offer, then a change of them and a trade record's quote and trade, with no
//!   aggressor side and the record's sequence as its id.
//! - Level 1 of NVDA on the Nasdaq stream (taken by its ticker, with LSE's catalogue asked nothing)
//!   follows the same course.
//! - Level 2 of GCZ6 and NVDA opens with the last ten levels a side, then the book after each of
//!   three changes: the best bid's size is cut, an order joins the best ask, the ninth ask level
//!   is removed.
//! - Level 3 has no opening book, so the deltas start at the first order event. The events of each
//!   exchange message come as one batch ending `F_LAST`: a buy and a sell order are added at one
//!   time, the buy's size changes, and the sell is removed. LSE ends its message with an event of
//!   no action, which gives no delta.
//!
//! Each record is stamped when LSE received it (`ts_recv`), as Nautilus' Databento decoder does,
//! and numbered by its sequence on the stream.
//!
//! GCZ6 is the front contract and GCG7 its next, which a roll subscribes. Once a contract has
//! expired, LSE refuses a subscription naming it (`symbol unavailable`, a close with 1008, which is
//! not reconnected), so a reconnection names the live contracts alone.

use std::{
    cell::RefCell,
    num::{NonZeroU32, NonZeroUsize},
    rc::Rc,
    time::Duration,
};

use jiff::Timestamp;
use lse::{
    LseDataClientConfig, LseDataClientFactory,
    testing::{
        CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7, CME_GCZ6_L2_DOCUMENTED,
        CME_GCZ6_L3_DOCUMENTED, CME_LEVEL_REQUIRED, CME_SYMBOL_UNAVAILABLE, ESZ26, GCZ26,
        MockExchangeStream, MockLse, NASDAQ_API_KEY, NASDAQ_NVDA_L1_DOCUMENTED,
        NASDAQ_NVDA_L2_DOCUMENTED, NASDAQ_NVDA_L3_DOCUMENTED, NVDA, PAIRS_ESZ6, PAIRS_GCG7,
        PAIRS_GCZ6, Reply, StreamAction, dbn_frames, esz26, frames, gcz26, held, nvda, sent,
    },
};
use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    clock::{Clock, VirtualClock},
    factories::DataClientFactory,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeBookDepth, SubscribeQuotes, SubscribeTrades,
            UnsubscribeBookDepth, UnsubscribeQuotes,
        },
    },
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDelta, QuoteTick, TradeTick},
    enums::{AggressorSide, BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, Symbol, TradeId},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rstest::rstest;
use serde_json::{Value, json};

type Events = tokio::sync::mpsc::UnboundedReceiver<DataEvent>;

const GCG27: &str = "GCG27.GLBX";

fn gcg27() -> InstrumentAny {
    let InstrumentAny::FuturesContract(mut contract) = gcz26() else {
        unreachable!("a futures contract")
    };
    contract.id = InstrumentId::from(GCG27);
    contract.raw_symbol = Symbol::from("GCG7");
    InstrumentAny::FuturesContract(contract)
}

async fn streaming(
    lse: &MockLse,
    stream: &MockExchangeStream,
    streamed: &[&str],
    instruments: Vec<InstrumentAny>,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    streaming_with(&cme_config(lse, stream, streamed), instruments).await
}

fn cme_config(
    lse: &MockLse,
    stream: &MockExchangeStream,
    streamed: &[&str],
) -> LseDataClientConfig {
    LseDataClientConfig::builder()
        .cme_api_key(SecretString::from(CME_API_KEY))
        .base_url_http(lse.url().to_string())
        .base_url_ws_cme(stream.url().to_string())
        .cme_calls_per_minute(NonZeroU32::new(6000).unwrap())
        .stream_ids(ids(streamed))
        .build()
}

fn nasdaq_config(lse: &MockLse, stream: &MockExchangeStream, key: &str) -> LseDataClientConfig {
    LseDataClientConfig::builder()
        .nasdaq_api_key(SecretString::from(key))
        .base_url_http(lse.url().to_string())
        .base_url_ws_nasdaq(stream.url().to_string())
        .stream_ids(ids(&[NVDA]))
        .build()
}

type Configured = fn(&MockLse, &MockExchangeStream) -> LseDataClientConfig;

fn esz26_on_cme(lse: &MockLse, stream: &MockExchangeStream) -> LseDataClientConfig {
    cme_config(lse, stream, &[ESZ26])
}

fn gcz26_on_cme(lse: &MockLse, stream: &MockExchangeStream) -> LseDataClientConfig {
    cme_config(lse, stream, &[GCZ26])
}

fn nvda_on_nasdaq(lse: &MockLse, stream: &MockExchangeStream) -> LseDataClientConfig {
    nasdaq_config(lse, stream, NASDAQ_API_KEY)
}

async fn streaming_with(
    config: &LseDataClientConfig,
    instruments: Vec<InstrumentAny>,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    let (tx, events) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut cache = Cache::default();
    for instrument in instruments {
        cache.add_instrument(instrument).unwrap();
    }
    let factory = LseDataClientFactory::new();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let mut client = factory
        .create(
            factory.name(),
            config,
            Rc::new(RefCell::new(cache)).into(),
            clock,
        )
        .unwrap();
    let connected = client.connect().await;
    (client, connected, events)
}

fn ids(ids: &[&str]) -> Vec<InstrumentId> {
    ids.iter().map(|&id| InstrumentId::from(id)).collect()
}

fn quotes_of(of: &[&str]) -> SubscribeQuotes {
    SubscribeQuotes::new(
        ids(of),
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn trades_of(of: &[&str]) -> SubscribeTrades {
    SubscribeTrades::new(
        ids(of),
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

async fn next(events: &mut Events) -> Data {
    let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("no data event within 10 s")
        .unwrap();
    let DataEvent::Data(data) = event else {
        panic!("not data: {event:?}")
    };
    data
}

fn es_quote(
    (bid, bid_size): (&str, u64),
    (ask, ask_size): (&str, u64),
    ts_recv: u64,
    ts_init: UnixNanos,
) -> Data {
    Data::Quote(QuoteTick::new(
        InstrumentId::from(ESZ26),
        Price::from(bid),
        Price::from(ask),
        Quantity::from(bid_size),
        Quantity::from(ask_size),
        UnixNanos::from(ts_recv),
        ts_init,
    ))
}

/// Returns the quote or trade `data` stamped as made at nought. The client's clock is no fact of
/// LSE's.
fn made_at_nought(data: Data) -> Data {
    match data {
        Data::Quote(quote) => Data::Quote(QuoteTick {
            ts_init: UnixNanos::default(),
            ..quote
        }),
        Data::Trade(trade) => Data::Trade(TradeTick {
            ts_init: UnixNanos::default(),
            ..trade
        }),
        other => panic!("neither a quote nor a trade: {other:?}"),
    }
}

#[rstest]
#[case::a_glbx_contract(
    MockLse::start(vec![Reply::json(PAIRS_ESZ6)]),
    MockExchangeStream::cme(|_, _| held(dbn_frames(CME_ESZ6_DOCUMENTED), 4)),
    esz26_on_cme,
    esz26(),
    [
        es_quote(("7569.75", 2), ("7570.00", 11), 1_794_272_217_707_781_000, UnixNanos::default()),
        es_quote(("7569.50", 18), ("7570.00", 12), 1_794_272_218_208_093_000, UnixNanos::default()),
        es_quote(("7569.50", 18), ("7570.00", 12), 1_794_272_218_213_015_000, UnixNanos::default()),
        Data::Trade(TradeTick::new(
            InstrumentId::from(ESZ26),
            Price::from("7570.00"),
            Quantity::from(1),
            AggressorSide::NoAggressor,
            TradeId::new("556"),
            UnixNanos::from(1_794_272_218_213_015_000),
            UnixNanos::default(),
        )),
    ],
    &[("/v3/pairs", Some("ESZ6"), Some(CME_API_KEY))],
    ("ESZ6", CME_API_KEY),
)]
#[case::an_xnas_stock(
    MockLse::start(vec![]),
    MockExchangeStream::nasdaq(|_, _| held(dbn_frames(NASDAQ_NVDA_L1_DOCUMENTED), 4)),
    nvda_on_nasdaq,
    nvda(),
    [
        nvda_quote(("211.15", 200), ("211.17", 900), "14:30:00.000471", UnixNanos::default()),
        nvda_quote(("211.15", 100), ("211.17", 900), "14:30:00.001706", UnixNanos::default()),
        nvda_quote(("211.15", 100), ("211.17", 800), "14:30:00.001862", UnixNanos::default()),
        Data::Trade(TradeTick::new(
            InstrumentId::from(NVDA),
            Price::from("211.17"),
            Quantity::from(100),
            AggressorSide::NoAggressor,
            TradeId::new("503"),
            on_the_12th("14:30:00.001862"),
            UnixNanos::default(),
        )),
    ],
    &[],
    ("NVDA", NASDAQ_API_KEY),
)]
#[tokio::test]
async fn an_instrument_receives_its_opening_best_bid_and_offer_then_its_quotes_and_trades(
    #[case] lse: MockLse,
    #[case] stream: MockExchangeStream,
    #[case] config: Configured,
    #[case] instrument: InstrumentAny,
    #[case] expected: [Data; 4],
    #[case] looked_up: &[(&str, Option<&str>, Option<&str>)],
    #[case] (symbol, key): (&str, &str),
) {
    let id = instrument.id().to_string();
    let (mut client, connected, mut events) =
        streaming_with(&config(&lse, &stream), vec![instrument]).await;
    connected.unwrap();

    client.subscribe_quotes(quotes_of(&[&id])).unwrap();
    client.subscribe_trades(trades_of(&[&id])).unwrap();
    let opening = next(&mut events).await;
    stream.release();
    let received = [
        opening,
        next(&mut events).await,
        next(&mut events).await,
        next(&mut events).await,
    ];

    assert_eq!(received.map(made_at_nought), expected);
    let calls = lse.calls();
    let lookups: Vec<_> = calls
        .iter()
        .map(|call| {
            (
                call.path.as_str(),
                call.param("symbol"),
                call.api_key.as_deref(),
            )
        })
        .collect();
    assert_eq!(lookups, looked_up);
    assert_eq!(
        stream.received(),
        [(
            0,
            json!({"op": "subscribe", "symbols": [symbol], "level": 1, "encoding": "dbn"})
        )]
    );
    assert_eq!(stream.keys(), [Some(key.to_string())]);
}

/// Synthetic: LSE's documented session without its record 555. The gap is logged and left, and the
/// trade after it comes.
#[tokio::test]
async fn the_records_after_a_gap_come_and_nothing_stands_in_for_those_missed() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6)]);
    let stream = MockExchangeStream::cme(|_, _| {
        let mut frames = dbn_frames(CME_ESZ6_DOCUMENTED);
        frames.remove(4);
        held(frames, 4)
    });
    let (mut client, connected, mut events) =
        streaming(&lse, &stream, &[ESZ26], vec![esz26()]).await;
    connected.unwrap();
    client.subscribe_trades(trades_of(&[ESZ26])).unwrap();

    stream.release();

    let Data::Trade(trade) = next(&mut events).await else {
        panic!("not the trade")
    };
    assert_eq!(trade.trade_id, TradeId::new("556"));
    client.disconnect().await.unwrap();
    assert!(events.try_recv().is_err(), "data beside the trade");
}

#[tokio::test]
async fn a_contract_subscribed_after_the_start_is_looked_up_and_carried_from_a_new_connection() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::json(PAIRS_GCG7)]);
    let stream = MockExchangeStream::cme(|_, subscription| {
        let mut frames = frames(CME_GCZ6_GCG7);
        if subscription["symbols"] == json!(["GCZ6"]) {
            frames.truncate(4);
        }
        sent(frames)
    });
    let (mut client, connected, mut events) =
        streaming(&lse, &stream, &[GCZ26], vec![gcz26(), gcg27()]).await;
    connected.unwrap();
    client.subscribe_quotes(quotes_of(&[GCZ26])).unwrap();
    let Data::Quote(opening) = next(&mut events).await else {
        panic!("not GCZ6's opening quote")
    };
    assert_eq!(opening.instrument_id, InstrumentId::from(GCZ26));

    client.subscribe_quotes(quotes_of(&[GCG27])).unwrap();

    let Data::Quote(quote) = next(&mut events).await else {
        panic!("not a quote")
    };
    assert_eq!(
        (
            quote.instrument_id,
            quote.bid_price,
            quote.bid_size,
            quote.ask_price,
            quote.ask_size,
            quote.ts_event
        ),
        (
            InstrumentId::from(GCG27),
            Price::from("3890.00"),
            Quantity::from(1),
            Price::from("3932.00"),
            Quantity::from(1),
            UnixNanos::from(1_794_607_200_675_397_000)
        )
    );
    let subscriptions: Vec<Value> = stream
        .received()
        .into_iter()
        .map(|(_, frame)| frame["symbols"].clone())
        .collect();
    assert_eq!(subscriptions, [json!(["GCZ6"]), json!(["GCG7", "GCZ6"])]);
    let looked_up: Vec<_> = lse
        .calls()
        .iter()
        .map(|call| call.param("symbol").unwrap().to_string())
        .collect();
    assert_eq!(looked_up, ["GCZ6", "GCG7"]);
}

#[tokio::test]
async fn a_contract_whose_data_at_a_level_all_ended_is_left_out_of_its_next_connection() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::json(PAIRS_GCG7)]);
    let stream = MockExchangeStream::cme(|connection, _| {
        let mut actions = sent(frames(CME_GCZ6_GCG7));
        if connection == 0 {
            actions.push(StreamAction::Hold);
            actions.push(StreamAction::Close(1001, "going away".to_string()));
        }
        actions
    });
    let (mut client, connected, _events) =
        streaming(&lse, &stream, &[GCZ26, GCG27], vec![gcz26(), gcg27()]).await;
    connected.unwrap();
    client.subscribe_quotes(quotes_of(&[GCZ26, GCG27])).unwrap();

    client
        .unsubscribe_quotes(&UnsubscribeQuotes::new(
            ids(&[GCG27]),
            Some(ClientId::from("LSE")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    stream.release();

    let subscriptions: Vec<Value> = stream
        .wait_until(|received| received.len() == 2)
        .await
        .into_iter()
        .map(|(_, frame)| frame["symbols"].clone())
        .collect();
    assert_eq!(subscriptions, [json!(["GCG7", "GCZ6"]), json!(["GCZ6"])]);
}

/// Level 1's first connection is held until the test releases it, then dropped.
fn level_1_dropped_once() -> MockExchangeStream {
    let mut level_1 = 0;
    MockExchangeStream::cme(move |_, subscription| {
        if subscription["level"] != 1 {
            return sent(dbn_frames(CME_GCZ6_L2_DOCUMENTED));
        }
        level_1 += 1;
        let mut actions = sent(frames(CME_GCZ6_GCG7));
        if level_1 == 1 {
            actions.push(StreamAction::Hold);
            actions.push(StreamAction::Close(1001, "going away".to_string()));
        }
        actions
    })
}

async fn level_1_subscriptions(stream: &MockExchangeStream, count: usize) -> Vec<Value> {
    let level_1 = |received: &[(usize, Value)]| -> Vec<Value> {
        received
            .iter()
            .filter(|(_, frame)| frame["level"] == 1)
            .map(|(_, frame)| frame["symbols"].clone())
            .collect()
    };
    level_1(
        &stream
            .wait_until(|received| level_1(received).len() == count)
            .await,
    )
}

/// A config that reads a rolled root's depth and the next contract's quotes streams GCZ6 and GCG7.
/// GCZ6's depth, taken at Level 2, ends at the roll, so Level 1's next connection leaves it out, or
/// LSE would refuse it and stop GCG7's quotes.
#[tokio::test]
async fn a_contract_whose_data_ended_at_one_level_is_left_out_of_every_level_it_takes_none_at() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::json(PAIRS_GCG7)]);
    let stream = level_1_dropped_once();
    let (mut client, connected, _events) =
        streaming(&lse, &stream, &[GCZ26, GCG27], vec![gcz26(), gcg27()]).await;
    connected.unwrap();
    client.subscribe_book_depth(depth_of(&[GCZ26], 10)).unwrap();
    client.subscribe_quotes(quotes_of(&[GCG27])).unwrap();

    client
        .unsubscribe_book_depth(&UnsubscribeBookDepth::new(
            ids(&[GCZ26]),
            Some(ClientId::from("LSE")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    stream.release();

    assert_eq!(
        level_1_subscriptions(&stream, 2).await,
        [json!(["GCG7", "GCZ6"]), json!(["GCG7"])]
    );
}

#[tokio::test]
async fn a_reconnect_names_only_the_contracts_taken_at_its_level_not_those_streamed_untaken() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::json(PAIRS_GCG7)]);
    let stream = level_1_dropped_once();
    let (mut client, connected, _events) =
        streaming(&lse, &stream, &[GCZ26, GCG27], vec![gcz26(), gcg27()]).await;
    connected.unwrap();

    client.subscribe_quotes(quotes_of(&[GCG27])).unwrap();
    stream.release();

    assert_eq!(
        level_1_subscriptions(&stream, 2).await,
        [json!(["GCG7", "GCZ6"]), json!(["GCG7"])]
    );
}

#[tokio::test]
async fn lse_s_refusal_of_the_subscription_fails_the_connect_with_its_reason() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let close: Value = serde_json::from_str(CME_SYMBOL_UNAVAILABLE).unwrap();
    let reason = close["reason"].as_str().unwrap().to_string();
    let stream =
        MockExchangeStream::cme(move |_, _| vec![StreamAction::Close(1008, reason.clone())]);

    let (_client, connected, _) = streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;

    let err = format!("{:#}", connected.unwrap_err());
    assert!(err.contains("1008 symbol unavailable: NGZ6"), "{err}");
}

/// ESZ6 of 2016 was delisted on 16 December 2016 (`PAIRS_ESZ6`). LSE's CME stream takes a contract
/// by its listing's symbol, ESZ6, which now carries 2026's contract, so streaming it would pass
/// 2026's records on as 2016's.
#[tokio::test]
async fn a_delisted_contract_is_refused_for_streaming_naming_it() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6)]);
    let stream = MockExchangeStream::cme(|_, _| sent(dbn_frames(CME_ESZ6_DOCUMENTED)));

    let (_client, connected, _) = streaming(&lse, &stream, &["ESZ16.GLBX"], vec![]).await;

    let err = format!("{:#}", connected.unwrap_err());
    assert!(
        err.contains("ESZ16.GLBX") && err.contains("delisted 2016-12-16"),
        "{err}"
    );
    assert_eq!(stream.received(), []);
}

fn depth_of(of: &[&str], depth: usize) -> SubscribeBookDepth {
    SubscribeBookDepth::new(
        InstrumentId::from(of[0]),
        BookType::L2_MBP,
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        NonZeroUsize::new(depth),
        false,
        None,
        None,
    )
}

fn deltas_of(of: &str) -> SubscribeBookDeltas {
    SubscribeBookDeltas::new(
        InstrumentId::from(of),
        BookType::L3_MBO,
        Some(ClientId::from("LSE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        false,
        None,
        None,
    )
}

fn by_level(
    level_2: impl Fn(usize) -> Vec<StreamAction> + Send + 'static,
    level_3: impl Fn(usize) -> Vec<StreamAction> + Send + 'static,
) -> MockExchangeStream {
    MockExchangeStream::cme(
        move |connection, subscription| match subscription["level"].as_u64() {
            Some(2) => level_2(connection),
            Some(3) => level_3(connection),
            _ => sent(frames(CME_GCZ6_GCG7)[..4].to_vec()),
        },
    )
}

fn level_required() -> Vec<StreamAction> {
    let close: Value = serde_json::from_str(CME_LEVEL_REQUIRED).unwrap();
    vec![StreamAction::Close(
        u16::try_from(close["code"].as_u64().unwrap()).unwrap(),
        close["reason"].as_str().unwrap().to_string(),
    )]
}

fn level(order: &BookOrder, count: u32) -> (Price, Quantity, u32) {
    (order.price, order.size, count)
}

type Level = (Price, Quantity, u32);

/// Each case gives the opening's tenth bid and ask, then for each depth its sequence, stamp, best
/// bid, best ask and count of asks.
#[rstest]
#[case::a_glbx_contract(
    MockLse::start(vec![Reply::json(PAIRS_GCZ6)]),
    by_level(|_| sent(dbn_frames(CME_GCZ6_L2_DOCUMENTED)), |_| Vec::new()),
    gcz26_on_cme,
    gcz26(),
    (
        (Price::from("3907.20"), Quantity::from(8), 6),
        (Price::from("3909.20"), Quantity::from(7), 4),
    ),
    [
        (
            1001,
            UnixNanos::from(1_794_493_800_000_471_000),
            (Price::from("3908.10"), Quantity::from(2), 1),
            (Price::from("3908.30"), Quantity::from(9), 4),
            10,
        ),
        (
            1002,
            UnixNanos::from(1_794_493_800_001_706_000),
            (Price::from("3908.10"), Quantity::from(1), 1),
            (Price::from("3908.30"), Quantity::from(9), 4),
            10,
        ),
        (
            1003,
            UnixNanos::from(1_794_493_800_001_707_000),
            (Price::from("3908.10"), Quantity::from(1), 1),
            (Price::from("3908.30"), Quantity::from(11), 5),
            10,
        ),
        (
            1004,
            UnixNanos::from(1_794_493_800_999_131_000),
            (Price::from("3908.10"), Quantity::from(1), 1),
            (Price::from("3908.30"), Quantity::from(11), 5),
            8,
        ),
    ],
    "GCZ6",
)]
#[case::an_xnas_stock(
    MockLse::start(vec![]),
    nvda_by_level(),
    nvda_on_nasdaq,
    nvda(),
    (
        (Price::from("211.06"), Quantity::from(800), 6),
        (Price::from("211.26"), Quantity::from(700), 4),
    ),
    [
        (
            1001,
            on_the_12th("14:30:00.000471"),
            (Price::from("211.15"), Quantity::from(200), 1),
            (Price::from("211.17"), Quantity::from(900), 4),
            10,
        ),
        (
            1002,
            on_the_12th("14:30:00.001706"),
            (Price::from("211.15"), Quantity::from(100), 1),
            (Price::from("211.17"), Quantity::from(900), 4),
            10,
        ),
        (
            1003,
            on_the_12th("14:30:00.001707"),
            (Price::from("211.15"), Quantity::from(100), 1),
            (Price::from("211.17"), Quantity::from(1100), 5),
            10,
        ),
        (
            1004,
            on_the_12th("14:30:00.999131"),
            (Price::from("211.15"), Quantity::from(100), 1),
            (Price::from("211.17"), Quantity::from(1100), 5),
            8,
        ),
    ],
    "NVDA",
)]
#[tokio::test]
async fn a_level_2_subscription_delivers_the_instrument_s_ten_levels_then_its_depths(
    #[case] lse: MockLse,
    #[case] stream: MockExchangeStream,
    #[case] config: Configured,
    #[case] instrument: InstrumentAny,
    #[case] tenth: (Level, Level),
    #[case] expected: [(u64, UnixNanos, Level, Level, usize); 4],
    #[case] symbol: &str,
) {
    let id = instrument.id();
    let (mut client, connected, mut events) =
        streaming_with(&config(&lse, &stream), vec![instrument]).await;
    connected.unwrap();

    client
        .subscribe_book_depth(depth_of(&[&id.to_string()], 10))
        .unwrap();

    let mut depths = Vec::new();
    for _ in 0..4 {
        let Data::BookDepth(depth) = next(&mut events).await else {
            panic!("not a depth")
        };
        depths.push(depth);
    }
    let opening = &depths[0];
    assert_eq!(
        (
            level(&opening.bids[9], opening.bid_counts[9]),
            level(&opening.asks[9], opening.ask_counts[9]),
        ),
        tenth
    );
    assert_eq!(
        depths
            .iter()
            .map(|depth| depth.instrument_id)
            .collect::<Vec<_>>(),
        [id; 4]
    );
    assert_eq!(
        depths
            .iter()
            .map(|depth| (
                depth.sequence,
                depth.ts_event,
                level(&depth.bids[0], depth.bid_counts[0]),
                level(&depth.asks[0], depth.ask_counts[0]),
                depth.asks.iter().filter(|ask| ask.side.is_some()).count(),
            ))
            .collect::<Vec<_>>(),
        expected
    );
    let subscriptions: Vec<Value> = stream
        .received()
        .into_iter()
        .map(|(_, frame)| frame)
        .collect();
    assert_eq!(
        subscriptions,
        [
            json!({"op": "subscribe", "symbols": [symbol], "level": 1, "encoding": "dbn"}),
            json!({"op": "subscribe", "symbols": [symbol], "level": 2, "encoding": "dbn"}),
        ]
    );
}

#[rstest]
#[case::a_glbx_contract(
    MockLse::start(vec![Reply::json(PAIRS_GCZ6)]),
    by_level(|_| Vec::new(), |_| sent(dbn_frames(CME_GCZ6_L3_DOCUMENTED))),
    gcz26_on_cme,
    gcz26(),
    (6_411_893_001, "3908.10", 1, 3),
    (6_411_893_002, "3908.30", 2),
    "GCZ6",
)]
#[case::an_xnas_stock(
    MockLse::start(vec![]),
    nvda_by_level(),
    nvda_on_nasdaq,
    nvda(),
    (382_910_001, "211.15", 100, 300),
    (382_910_002, "211.17", 200),
    "NVDA",
)]
#[tokio::test]
async fn a_level_3_subscription_delivers_order_book_deltas_from_the_first_live_record(
    #[case] lse: MockLse,
    #[case] stream: MockExchangeStream,
    #[case] config: Configured,
    #[case] instrument: InstrumentAny,
    #[case] (buy, buy_price, buy_size, changed_size): (u64, &str, u64, u64),
    #[case] (sell, sell_price, sell_size): (u64, &str, u64),
    #[case] symbol: &str,
) {
    let id = instrument.id();
    let (mut client, connected, mut events) =
        streaming_with(&config(&lse, &stream), vec![instrument]).await;
    connected.unwrap();

    client
        .subscribe_book_deltas(deltas_of(&id.to_string()))
        .unwrap();

    let mut batches = Vec::new();
    for _ in 0..3 {
        let Data::BookDeltas(deltas) = next(&mut events).await else {
            panic!("not book deltas")
        };
        batches.push(deltas);
    }
    let last = RecordFlag::F_LAST as u8;
    let delta = |action, side, order_id, price, size, flags, sequence, ts: u64| {
        OrderBookDelta::new(
            id,
            action,
            BookOrder::new(side, Price::from(price), Quantity::from(size), order_id),
            flags,
            sequence,
            UnixNanos::from(ts),
            UnixNanos::default(),
        )
    };
    let received: Vec<Vec<OrderBookDelta>> = batches
        .iter()
        .map(|batch| {
            assert_eq!(batch.instrument_id, id);
            batch
                .deltas
                .iter()
                .map(|delta| OrderBookDelta {
                    ts_init: UnixNanos::default(),
                    ..*delta
                })
                .collect()
        })
        .collect();
    assert_eq!(
        received,
        [
            vec![
                delta(
                    BookAction::Add,
                    OrderSide::Buy,
                    buy,
                    buy_price,
                    buy_size,
                    0,
                    7312,
                    1_794_493_800_000_471_000
                ),
                delta(
                    BookAction::Add,
                    OrderSide::Sell,
                    sell,
                    sell_price,
                    sell_size,
                    last,
                    7313,
                    1_794_493_800_000_472_000
                ),
            ],
            vec![delta(
                BookAction::Update,
                OrderSide::Buy,
                buy,
                buy_price,
                changed_size,
                last,
                7314,
                1_794_493_800_001_706_000
            )],
            vec![delta(
                BookAction::Delete,
                OrderSide::Sell,
                sell,
                sell_price,
                sell_size,
                last,
                7315,
                1_794_493_800_027_391_000
            )],
        ]
    );
    let subscriptions: Vec<Value> = stream
        .received()
        .into_iter()
        .map(|(_, frame)| frame)
        .collect();
    assert_eq!(
        subscriptions,
        [
            json!({"op": "subscribe", "symbols": [symbol], "level": 1, "encoding": "dbn"}),
            json!({"op": "subscribe", "symbols": [symbol], "level": 3, "encoding": "dbn"}),
        ]
    );
}

#[tokio::test]
async fn a_level_lse_refused_is_asked_again_by_a_later_subscription() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = by_level(
        |connection| {
            if connection == 1 {
                level_required()
            } else {
                sent(dbn_frames(CME_GCZ6_L2_DOCUMENTED))
            }
        },
        |_| Vec::new(),
    );
    let (mut client, connected, mut events) =
        streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;
    connected.unwrap();
    client.subscribe_book_depth(depth_of(&[GCZ26], 10)).unwrap();
    stream.wait_until(|received| received.len() == 2).await;

    wait_until_async(
        || {
            let asked_again = stream
                .received()
                .iter()
                .any(|(connection, _)| *connection == 2);

            if !asked_again {
                client.subscribe_book_depth(depth_of(&[GCZ26], 10)).unwrap();
            }
            async move { asked_again }
        },
        Duration::from_secs(10),
    )
    .await;

    let Data::BookDepth(depth) = next(&mut events).await else {
        panic!("not a depth")
    };
    assert_eq!(depth.sequence, 1001);
    let levels: Vec<(usize, Value)> = stream
        .received()
        .into_iter()
        .map(|(connection, frame)| (connection, frame["level"].clone()))
        .collect();
    assert_eq!(levels, [(0, json!(1)), (1, json!(2)), (2, json!(2))]);
}

/// Nautilus' Databento client serves MBP-10's ten levels alone, and LSE streams ten. The client
/// refuses another depth before LSE is asked.
#[tokio::test]
async fn a_depth_of_other_than_ten_levels_is_refused_unasked() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = by_level(|_| sent(dbn_frames(CME_GCZ6_L2_DOCUMENTED)), |_| Vec::new());
    let (mut client, connected, _events) = streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;
    connected.unwrap();

    let err = client
        .subscribe_book_depth(depth_of(&[GCZ26], 5))
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(err.contains(GCZ26) && err.contains("depth of 5"), "{err}");
    assert_eq!(stream.received().len(), 1);
}

/// LSE's sockets stream a series' ticks alone, with no book, so no Level 2 or 3 connection opens.
/// GCG7's quotes, asked after the refusals, open a Level 1 connection naming GCG7 alone, because
/// GCZ6 is taken at no level.
#[tokio::test]
async fn the_book_of_an_lsedge_series_is_refused_naming_it() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::json(PAIRS_GCG7)]);
    let stream = by_level(|_| Vec::new(), |_| Vec::new());
    let (mut client, connected, _events) =
        streaming(&lse, &stream, &[GCZ26], vec![gcz26(), gcg27()]).await;
    connected.unwrap();

    let depth = client
        .subscribe_book_depth(depth_of(&["GOLD-FUTURES.LSEDGE"], 10))
        .unwrap_err();
    let deltas = client
        .subscribe_book_deltas(deltas_of("GOLD-FUTURES.LSEDGE"))
        .unwrap_err();
    client.subscribe_quotes(quotes_of(&[GCG27])).unwrap();

    for err in [depth, deltas] {
        assert!(
            format!("{err:#}").contains("GOLD-FUTURES.LSEDGE"),
            "{err:#}"
        );
    }
    let received = stream.wait_until(|received| received.len() == 2).await;
    assert_eq!(
        received,
        [
            (
                0,
                json!({"op": "subscribe", "symbols": ["GCZ6"], "level": 1, "encoding": "dbn"})
            ),
            (
                1,
                json!({"op": "subscribe", "symbols": ["GCG7"], "level": 1, "encoding": "dbn"})
            ),
        ]
    );
}

async fn streaming_nvda(
    lse: &MockLse,
    stream: &MockExchangeStream,
    key: &str,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    streaming_with(&nasdaq_config(lse, stream, key), vec![nvda()]).await
}

fn nvda_by_level() -> MockExchangeStream {
    MockExchangeStream::nasdaq(|_, subscription| match subscription["level"].as_u64() {
        Some(2) => sent(dbn_frames(NASDAQ_NVDA_L2_DOCUMENTED)),
        Some(3) => sent(dbn_frames(NASDAQ_NVDA_L3_DOCUMENTED)),
        _ => sent(dbn_frames(NASDAQ_NVDA_L1_DOCUMENTED)[..4].to_vec()),
    })
}

fn on_the_12th(time: &str) -> UnixNanos {
    UnixNanos::from(format!("2026-11-12T{time}Z").parse::<Timestamp>().unwrap())
}

fn nvda_quote(
    (bid, bid_size): (&str, u64),
    (ask, ask_size): (&str, u64),
    ts_recv: &str,
    ts_init: UnixNanos,
) -> Data {
    Data::Quote(QuoteTick::new(
        InstrumentId::from(NVDA),
        Price::from(bid),
        Price::from(ask),
        Quantity::from(bid_size),
        Quantity::from(ask_size),
        on_the_12th(ts_recv),
        ts_init,
    ))
}

/// LSE's Nasdaq stream answers a key with no Nasdaq plan with HTTP 401 (captured), for the free key
/// and the CME Group Lite+ key alike.
#[tokio::test]
async fn lse_s_refusal_of_a_key_of_no_nasdaq_plan_fails_the_connect() {
    let lse = MockLse::start(vec![]);
    let stream = nvda_by_level();

    let (_client, connected, _) = streaming_nvda(&lse, &stream, CME_API_KEY).await;

    let err = format!("{:#}", connected.unwrap_err());
    assert!(err.contains("401"), "{err}");
    assert!(stream.received().is_empty(), "{:?}", stream.received());
}

#[tokio::test]
async fn a_glbx_contract_and_an_xnas_stock_stream_each_on_its_exchange_s_stream() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let cme = MockExchangeStream::cme(|_, _| sent(frames(CME_GCZ6_GCG7)[..4].to_vec()));
    let nasdaq = nvda_by_level();
    let config = LseDataClientConfig::builder()
        .cme_api_key(SecretString::from(CME_API_KEY))
        .nasdaq_api_key(SecretString::from(NASDAQ_API_KEY))
        .base_url_http(lse.url().to_string())
        .base_url_ws_cme(cme.url().to_string())
        .base_url_ws_nasdaq(nasdaq.url().to_string())
        .stream_ids(ids(&[GCZ26, NVDA]))
        .build();
    let (mut client, connected, mut events) = streaming_with(&config, vec![gcz26(), nvda()]).await;
    connected.unwrap();

    client.subscribe_quotes(quotes_of(&[GCZ26, NVDA])).unwrap();

    let mut opened: Vec<(InstrumentId, Price)> = Vec::new();
    for _ in 0..2 {
        let Data::Quote(quote) = next(&mut events).await else {
            panic!("not a quote")
        };
        opened.push((quote.instrument_id, quote.bid_price));
    }
    opened.sort_by_key(|(id, _)| id.to_string());
    assert_eq!(
        opened,
        [
            (InstrumentId::from(GCZ26), Price::from("3870.00")),
            (InstrumentId::from(NVDA), Price::from("211.15")),
        ]
    );
    let subscribed = |stream: &MockExchangeStream| -> Vec<Value> {
        stream
            .received()
            .into_iter()
            .map(|(_, frame)| frame["symbols"].clone())
            .collect()
    };
    assert_eq!(subscribed(&cme), [json!(["GCZ6"])]);
    assert_eq!(subscribed(&nasdaq), [json!(["NVDA"])]);
    assert_eq!(cme.keys(), [Some(CME_API_KEY.to_string())]);
    assert_eq!(nasdaq.keys(), [Some(NASDAQ_API_KEY.to_string())]);
    let looked_up: Vec<_> = lse
        .calls()
        .iter()
        .map(|call| {
            (
                call.param("market").map(ToString::to_string),
                call.api_key.clone(),
            )
        })
        .collect();
    assert_eq!(
        looked_up,
        [(Some("cme".to_string()), Some(CME_API_KEY.to_string()))]
    );
}
