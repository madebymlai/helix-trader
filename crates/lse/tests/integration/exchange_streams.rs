//! The data client's exchange data through its factory: the `GLBX`
//! contracts and `XNAS` stocks it streams on the mock CME and Nasdaq
//! streams, at Levels 1 to 3, their openings, gaps, refusals, later
//! contracts and unsubscriptions.

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
        API_KEY, CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7, CME_GCZ6_L2_DOCUMENTED,
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
            UnsubscribeQuotes,
        },
    },
};
use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDelta, QuoteTick, TradeTick},
    enums::{AggressorSide, BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, Symbol, TradeId},
    instruments::InstrumentAny,
    types::{Price, Quantity},
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

type Events = mpsc::UnboundedReceiver<DataEvent>;

/// GCG7 of 2027, `GCG27.GLBX`, as GCZ26's definition with its id.
const GCG27: &str = "GCG27.GLBX";

fn gcg27() -> InstrumentAny {
    let InstrumentAny::FuturesContract(mut contract) = gcz26() else {
        unreachable!("a futures contract")
    };
    contract.id = InstrumentId::from(GCG27);
    contract.raw_symbol = Symbol::from("GCG7");
    InstrumentAny::FuturesContract(contract)
}

/// The client the factory creates with the CME key alone, LSE's API on
/// `lse` and its CME stream on `stream`, streaming `streamed`, its cache
/// holding `instruments`, and the node's data events, once it connected
/// or failed to.
async fn streaming(
    lse: &MockLse,
    stream: &MockExchangeStream,
    streamed: &[&str],
    instruments: Vec<InstrumentAny>,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    let config = LseDataClientConfig::builder()
        .cme_api_key(SecretString::from(CME_API_KEY))
        .base_url_http(lse.url().to_string())
        .base_url_ws_cme(stream.url().to_string())
        .cme_calls_per_minute(NonZeroU32::new(6000).unwrap())
        .stream_ids(ids(streamed))
        .build();
    streaming_with(&config, instruments).await
}

/// The client the factory creates on `config`, its cache holding
/// `instruments`, and the node's data events, once it connected or
/// failed to.
async fn streaming_with(
    config: &LseDataClientConfig,
    instruments: Vec<InstrumentAny>,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    let (tx, events) = mpsc::unbounded_channel();
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

/// The quotes of `of`, as the data engine asks the client for them.
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

/// The trades of `of`, as the data engine asks the client for them.
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

/// A quote of ESZ26, received by LSE at `ts_recv`, made at `ts_init`.
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

fn ts_init(data: &Data) -> UnixNanos {
    match data {
        Data::Quote(quote) => quote.ts_init,
        Data::Trade(trade) => trade.ts_init,
        other => panic!("neither a quote nor a trade: {other:?}"),
    }
}

/// LSE's documented ESZ6 at Level 1: its opening, bid 7569.75 x 2, ask
/// 7570.00 x 11, comes with the quotes subscription; its next records,
/// a change of the best bid and offer and a trade, come after: a quote,
/// then the trade's record's quote and its trade, 1 lot at 7570.00, of
/// no aggressor side, its id the record's sequence. Each is stamped when
/// LSE received the record (`ts_recv`), as Nautilus' Databento decoder
/// stamps it.
#[tokio::test]
async fn a_glbx_contract_receives_its_opening_best_bid_and_offer_then_its_quotes_and_trades() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6)]);
    let stream = MockExchangeStream::cme(|_, _| held(dbn_frames(CME_ESZ6_DOCUMENTED), 4));
    let (mut client, connected, mut events) =
        streaming(&lse, &stream, &[ESZ26], vec![esz26()]).await;
    connected.unwrap();

    client.subscribe_quotes(quotes_of(&[ESZ26])).unwrap();
    client.subscribe_trades(trades_of(&[ESZ26])).unwrap();
    let opening = next(&mut events).await;
    stream.release();
    let live = [
        next(&mut events).await,
        next(&mut events).await,
        next(&mut events).await,
    ];

    assert_eq!(
        opening,
        es_quote(
            ("7569.75", 2),
            ("7570.00", 11),
            1_794_272_217_707_781_000,
            ts_init(&opening)
        )
    );
    assert_eq!(
        live,
        [
            es_quote(
                ("7569.50", 18),
                ("7570.00", 12),
                1_794_272_218_208_093_000,
                ts_init(&live[0])
            ),
            es_quote(
                ("7569.50", 18),
                ("7570.00", 12),
                1_794_272_218_213_015_000,
                ts_init(&live[1])
            ),
            Data::Trade(TradeTick::new(
                InstrumentId::from(ESZ26),
                Price::from("7570.00"),
                Quantity::from(1),
                AggressorSide::NoAggressor,
                TradeId::new("556"),
                UnixNanos::from(1_794_272_218_213_015_000),
                ts_init(&live[2]),
            )),
        ]
    );
    let [lookup] = &lse.calls()[..] else {
        panic!("{:?}", lse.calls())
    };
    assert_eq!(
        (
            lookup.path.as_str(),
            lookup.param("symbol"),
            lookup.api_key.as_deref()
        ),
        ("/v3/pairs", Some("ESZ6"), Some(CME_API_KEY))
    );
    assert_eq!(
        stream.received(),
        [(
            0,
            json!({"op": "subscribe", "symbols": ["ESZ6"], "level": 1, "encoding": "dbn"})
        )]
    );
}

/// Synthetic: LSE's documented session without its record 555. The gap
/// is logged and left: the trade after it comes, and nothing stands in
/// for the record missed.
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

/// GCZ6 of 2026 streamed from the start; GCG7 of 2027, its next
/// contract, subscribed later, as a roll subscribes it: looked up, then
/// carried from a new connection, which opens it with its captured best
/// bid and offer, bid 3890.0 x 1 and ask 3932.0 x 1.
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

/// GCZ6 and GCG7 streamed from the start and quoted; then GCG7's quotes
/// ended, as a roll ends its old contract's: when LSE drops the
/// connection, the next one subscribes GCZ6 alone, so that GCG7, once
/// expired, does not have the level's whole subscription refused
/// (`symbol unavailable`, a close with 1008, which is not reconnected).
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

/// LSE's captured close of a subscription naming a contract the stream
/// does not carry.
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

/// ESZ6 of 2016, delisted on 16 December 2016 (`PAIRS_ESZ6`): LSE's
/// CME stream takes a contract by its listing's symbol, ESZ6, which
/// carries 2026's contract now, so streaming it would pass 2026's
/// records on as 2016's.
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

/// LSE's sockets stream quotes alone.
#[tokio::test]
async fn the_trades_of_an_lsedge_series_are_refused_naming_it() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = MockExchangeStream::cme(|_, _| sent(frames(CME_GCZ6_GCG7)));
    let (mut client, connected, _) = streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;
    connected.unwrap();

    let err = client
        .subscribe_trades(trades_of(&["GOLD-FUTURES.LSEDGE"]))
        .unwrap_err();

    assert!(
        format!("{err:#}").contains("GOLD-FUTURES.LSEDGE"),
        "{err:#}"
    );
}

#[tokio::test]
async fn a_glbx_contract_streamed_with_no_cme_key_is_refused_naming_its_variable() {
    let (tx, _events) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .stream_ids(ids(&[GCZ26]))
        .build();
    let factory = LseDataClientFactory::new();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));

    let Err(err) = factory.create(
        factory.name(),
        &config,
        Rc::new(RefCell::new(Cache::default())).into(),
        clock,
    ) else {
        panic!("created a client streaming GCZ26 with no CME key")
    };

    let err = format!("{err:#}");
    assert!(
        err.contains(GCZ26) && err.contains("LSE_CME_API_KEY"),
        "{err}"
    );
}

/// The book depth of `of`, `depth` levels, as the data engine asks the
/// client for it.
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

/// The book deltas of `of`, as the data engine asks the client for them.
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

/// The mock stream answering each connection by its level: GCZ6's
/// captured opening at Level 1, and `level_2` and `level_3` at theirs.
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

/// LSE's captured close of a level the key's plan lacks.
fn level_required() -> Vec<StreamAction> {
    let close: Value = serde_json::from_str(CME_LEVEL_REQUIRED).unwrap();
    vec![StreamAction::Close(
        u16::try_from(close["code"].as_u64().unwrap()).unwrap(),
        close["reason"].as_str().unwrap().to_string(),
    )]
}

/// A level of a depth: its price, size and count of orders.
fn level(order: &BookOrder, count: u32) -> (Price, Quantity, u32) {
    (order.price, order.size, count)
}

/// GCZ6 at Level 2 as LSE's documentation records it: its opening, its
/// last ten levels a side, then the book after each of three changes,
/// the best bid's size cut to 1, an order of 2 joining the best ask,
/// and the ninth ask level removed, each a depth of ten levels, its
/// counts of orders, stamped when LSE received the record and numbered
/// by its sequence on the stream.
#[tokio::test]
async fn a_level_2_subscription_delivers_the_contract_s_ten_levels_then_its_depths() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = by_level(|_| sent(dbn_frames(CME_GCZ6_L2_DOCUMENTED)), |_| Vec::new());
    let (mut client, connected, mut events) =
        streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;
    connected.unwrap();

    client.subscribe_book_depth(depth_of(&[GCZ26], 10)).unwrap();

    let mut depths = Vec::new();
    for _ in 0..4 {
        let Data::BookDepth(depth) = next(&mut events).await else {
            panic!("not a depth")
        };
        depths.push(depth);
    }
    let opening = &depths[0];
    assert_eq!(opening.instrument_id, InstrumentId::from(GCZ26));
    assert_eq!(
        (
            opening.ts_event,
            opening.sequence,
            level(&opening.bids[0], opening.bid_counts[0]),
            level(&opening.asks[0], opening.ask_counts[0]),
            level(&opening.bids[9], opening.bid_counts[9]),
            level(&opening.asks[9], opening.ask_counts[9]),
        ),
        (
            UnixNanos::from(1_794_493_800_000_471_000),
            1001,
            (Price::from("3908.10"), Quantity::from(2), 1),
            (Price::from("3908.30"), Quantity::from(9), 4),
            (Price::from("3907.20"), Quantity::from(8), 6),
            (Price::from("3909.20"), Quantity::from(7), 4),
        )
    );
    assert_eq!(
        depths[1..]
            .iter()
            .map(|depth| (
                depth.sequence,
                depth.ts_event,
                level(&depth.bids[0], depth.bid_counts[0]),
                level(&depth.asks[0], depth.ask_counts[0]),
                depth.asks.iter().filter(|ask| ask.side.is_some()).count(),
            ))
            .collect::<Vec<_>>(),
        [
            (
                1002,
                UnixNanos::from(1_794_493_800_001_706_000),
                (Price::from("3908.10"), Quantity::from(1), 1),
                (Price::from("3908.30"), Quantity::from(9), 4),
                10
            ),
            (
                1003,
                UnixNanos::from(1_794_493_800_001_707_000),
                (Price::from("3908.10"), Quantity::from(1), 1),
                (Price::from("3908.30"), Quantity::from(11), 5),
                10
            ),
            (
                1004,
                UnixNanos::from(1_794_493_800_999_131_000),
                (Price::from("3908.10"), Quantity::from(1), 1),
                (Price::from("3908.30"), Quantity::from(11), 5),
                8
            ),
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
            json!({"op": "subscribe", "symbols": ["GCZ6"], "level": 1, "encoding": "dbn"}),
            json!({"op": "subscribe", "symbols": ["GCZ6"], "level": 2, "encoding": "dbn"}),
        ]
    );
}

/// GCZ6 at Level 3 as LSE's documentation records it: no opening book,
/// so the deltas start at the first order event; each exchange message's
/// events, up to the one LSE flags 128, its last, come as one batch
/// ending `F_LAST`: the two orders added at one time, the change of one,
/// then its removal, whose message LSE ends with an event of no action,
/// which changes no order and gives no delta. Each is stamped when LSE
/// received the record and numbered by its sequence on the stream.
#[tokio::test]
async fn a_level_3_subscription_delivers_order_book_deltas_from_the_first_live_record() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = by_level(|_| Vec::new(), |_| sent(dbn_frames(CME_GCZ6_L3_DOCUMENTED)));
    let (mut client, connected, mut events) =
        streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;
    connected.unwrap();

    client.subscribe_book_deltas(deltas_of(GCZ26)).unwrap();

    let mut batches = Vec::new();
    for _ in 0..3 {
        let Data::BookDeltas(deltas) = next(&mut events).await else {
            panic!("not book deltas")
        };
        batches.push(deltas);
    }
    let last = RecordFlag::F_LAST as u8;
    let delta = |action, side, id: u64, price: &str, size: u64, flags, sequence, ts: u64| {
        OrderBookDelta::new(
            InstrumentId::from(GCZ26),
            action,
            BookOrder::new(side, Price::from(price), Quantity::from(size), id),
            flags,
            sequence,
            UnixNanos::from(ts),
            UnixNanos::default(),
        )
    };
    let received: Vec<Vec<OrderBookDelta>> = batches
        .iter()
        .map(|batch| {
            assert_eq!(batch.instrument_id, InstrumentId::from(GCZ26));
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
                    6_411_893_001,
                    "3908.10",
                    1,
                    0,
                    7312,
                    1_794_493_800_000_471_000
                ),
                delta(
                    BookAction::Add,
                    OrderSide::Sell,
                    6_411_893_002,
                    "3908.30",
                    2,
                    last,
                    7313,
                    1_794_493_800_000_472_000
                ),
            ],
            vec![delta(
                BookAction::Update,
                OrderSide::Buy,
                6_411_893_001,
                "3908.10",
                3,
                last,
                7314,
                1_794_493_800_001_706_000
            )],
            vec![delta(
                BookAction::Delete,
                OrderSide::Sell,
                6_411_893_002,
                "3908.30",
                2,
                last,
                7315,
                1_794_493_800_027_391_000
            )],
        ]
    );
    assert_eq!(
        stream.received().last().unwrap().1,
        json!({"op": "subscribe", "symbols": ["GCZ6"], "level": 3, "encoding": "dbn"})
    );
}

/// LSE refuses Level 2 to the CME Group Lite+ key, closing the
/// connection with its reason, which the client logs; a later
/// subscription of the level asks LSE again, on a new connection, here
/// answered.
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !stream
        .received()
        .iter()
        .any(|(connection, _)| *connection == 2)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "LSE not asked again"
        );
        client.subscribe_book_depth(depth_of(&[GCZ26], 10)).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

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

/// Nautilus' Databento client serves MBP-10's ten levels alone, and LSE
/// streams ten: another depth is refused naming it, before LSE is
/// asked.
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

/// LSE's sockets stream a series' ticks alone, no book.
#[tokio::test]
async fn the_book_of_an_lsedge_series_is_refused_naming_it() {
    let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
    let stream = by_level(|_| Vec::new(), |_| Vec::new());
    let (mut client, connected, _) = streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;
    connected.unwrap();

    let depth = client
        .subscribe_book_depth(depth_of(&["GOLD-FUTURES.LSEDGE"], 10))
        .unwrap_err();
    let deltas = client
        .subscribe_book_deltas(deltas_of("GOLD-FUTURES.LSEDGE"))
        .unwrap_err();

    for err in [depth, deltas] {
        assert!(
            format!("{err:#}").contains("GOLD-FUTURES.LSEDGE"),
            "{err:#}"
        );
    }
}

/// The client with the Nasdaq key alone, its Nasdaq stream on `stream`
/// and LSE's API on `lse`, streaming NVDA, its definition in the cache,
/// once it connected or failed to.
async fn streaming_nvda(
    lse: &MockLse,
    stream: &MockExchangeStream,
    key: &str,
) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
    let config = LseDataClientConfig::builder()
        .nasdaq_api_key(SecretString::from(key))
        .base_url_http(lse.url().to_string())
        .base_url_ws_nasdaq(stream.url().to_string())
        .stream_ids(ids(&[NVDA]))
        .build();
    streaming_with(&config, vec![nvda()]).await
}

/// LSE's Nasdaq stream answering each connection by its level: NVDA's
/// sessions at Levels 1, 2 and 3, as LSE's documentation records the
/// stream's messages.
fn nvda_by_level() -> MockExchangeStream {
    MockExchangeStream::nasdaq(|_, subscription| match subscription["level"].as_u64() {
        Some(2) => sent(dbn_frames(NASDAQ_NVDA_L2_DOCUMENTED)),
        Some(3) => sent(dbn_frames(NASDAQ_NVDA_L3_DOCUMENTED)),
        _ => sent(dbn_frames(NASDAQ_NVDA_L1_DOCUMENTED)[..4].to_vec()),
    })
}

/// 12 November 2026 at `time`, UTC.
fn on_the_12th(time: &str) -> UnixNanos {
    UnixNanos::from(format!("2026-11-12T{time}Z").parse::<Timestamp>().unwrap())
}

/// A quote of NVDA, received by LSE at `ts_recv`, made at `ts_init`.
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

/// NVDA at Level 1 on LSE's Nasdaq stream, as LSE's documentation
/// records the stream's messages: its opening, bid 211.15 x 200, ask
/// 211.17 x 900, comes with the quotes subscription; then a change of its
/// best bid, and a trade of 100 at 211.17, of no aggressor side, its id
/// the record's sequence, each stamped when LSE received the record. The
/// stream takes NVDA by its ticker, with the Nasdaq key, LSE's
/// catalogue asked nothing.
#[tokio::test]
async fn an_xnas_stock_receives_its_opening_best_bid_and_offer_then_its_quotes_and_trades() {
    let lse = MockLse::start(vec![]);
    let stream = MockExchangeStream::nasdaq(|_, _| held(dbn_frames(NASDAQ_NVDA_L1_DOCUMENTED), 4));
    let (mut client, connected, mut events) = streaming_nvda(&lse, &stream, NASDAQ_API_KEY).await;
    connected.unwrap();

    client.subscribe_quotes(quotes_of(&[NVDA])).unwrap();
    client.subscribe_trades(trades_of(&[NVDA])).unwrap();
    let opening = next(&mut events).await;
    stream.release();
    let live = [
        next(&mut events).await,
        next(&mut events).await,
        next(&mut events).await,
    ];

    assert_eq!(
        opening,
        nvda_quote(
            ("211.15", 200),
            ("211.17", 900),
            "14:30:00.000471",
            ts_init(&opening)
        )
    );
    assert_eq!(
        live,
        [
            nvda_quote(
                ("211.15", 100),
                ("211.17", 900),
                "14:30:00.001706",
                ts_init(&live[0])
            ),
            nvda_quote(
                ("211.15", 100),
                ("211.17", 800),
                "14:30:00.001862",
                ts_init(&live[1])
            ),
            Data::Trade(TradeTick::new(
                InstrumentId::from(NVDA),
                Price::from("211.17"),
                Quantity::from(100),
                AggressorSide::NoAggressor,
                TradeId::new("503"),
                on_the_12th("14:30:00.001862"),
                ts_init(&live[2]),
            )),
        ]
    );
    assert_eq!(
        stream.received(),
        [(
            0,
            json!({"op": "subscribe", "symbols": ["NVDA"], "level": 1, "encoding": "dbn"})
        )]
    );
    assert_eq!(stream.keys(), [Some(NASDAQ_API_KEY.to_string())]);
    assert!(lse.calls().is_empty(), "{:?}", lse.calls());
}

/// NVDA at Level 2 on LSE's Nasdaq stream: its opening, its last ten
/// levels a side, then the book after each of three changes, each a
/// depth of ten levels with its counts of orders, numbered by its
/// sequence on the stream.
#[tokio::test]
async fn an_xnas_stock_s_level_2_subscription_delivers_its_ten_levels_then_its_depths() {
    let lse = MockLse::start(vec![]);
    let stream = nvda_by_level();
    let (mut client, connected, mut events) = streaming_nvda(&lse, &stream, NASDAQ_API_KEY).await;
    connected.unwrap();

    client.subscribe_book_depth(depth_of(&[NVDA], 10)).unwrap();

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
            opening.instrument_id,
            level(&opening.bids[9], opening.bid_counts[9]),
            level(&opening.asks[9], opening.ask_counts[9]),
        ),
        (
            InstrumentId::from(NVDA),
            (Price::from("211.06"), Quantity::from(800), 6),
            (Price::from("211.26"), Quantity::from(700), 4),
        )
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
        [
            (
                1001,
                on_the_12th("14:30:00.000471"),
                (Price::from("211.15"), Quantity::from(200), 1),
                (Price::from("211.17"), Quantity::from(900), 4),
                10
            ),
            (
                1002,
                on_the_12th("14:30:00.001706"),
                (Price::from("211.15"), Quantity::from(100), 1),
                (Price::from("211.17"), Quantity::from(900), 4),
                10
            ),
            (
                1003,
                on_the_12th("14:30:00.001707"),
                (Price::from("211.15"), Quantity::from(100), 1),
                (Price::from("211.17"), Quantity::from(1100), 5),
                10
            ),
            (
                1004,
                on_the_12th("14:30:00.999131"),
                (Price::from("211.15"), Quantity::from(100), 1),
                (Price::from("211.17"), Quantity::from(1100), 5),
                8
            ),
        ]
    );
    assert_eq!(
        stream.received().last().unwrap().1,
        json!({"op": "subscribe", "symbols": ["NVDA"], "level": 2, "encoding": "dbn"})
    );
}

/// NVDA at Level 3 on LSE's Nasdaq stream: no opening book, so the
/// deltas start at the first order event, each exchange message's
/// events, up to the one LSE flags 128, one batch ending `F_LAST`: the
/// two orders added at one time, the change of one, then the removal of
/// the other, whose message ends with an event of no action.
#[tokio::test]
async fn an_xnas_stock_s_level_3_subscription_delivers_its_order_events_as_deltas() {
    let lse = MockLse::start(vec![]);
    let stream = nvda_by_level();
    let (mut client, connected, mut events) = streaming_nvda(&lse, &stream, NASDAQ_API_KEY).await;
    connected.unwrap();

    client.subscribe_book_deltas(deltas_of(NVDA)).unwrap();

    let mut batches = Vec::new();
    for _ in 0..3 {
        let Data::BookDeltas(deltas) = next(&mut events).await else {
            panic!("not book deltas")
        };
        batches.push(deltas);
    }
    let received: Vec<Vec<_>> = batches
        .iter()
        .map(|batch| {
            batch
                .deltas
                .iter()
                .map(|delta| {
                    (
                        delta.instrument_id,
                        delta.action,
                        delta.order.side,
                        delta.order.order_id,
                        delta.order.price,
                        delta.order.size,
                        RecordFlag::F_LAST.matches(delta.flags),
                        delta.sequence,
                    )
                })
                .collect()
        })
        .collect();
    let delta = |action, side, id: u64, price: &str, size: u64, last, sequence| {
        (
            InstrumentId::from(NVDA),
            action,
            Some(side),
            id,
            Price::from(price),
            Quantity::from(size),
            last,
            sequence,
        )
    };
    assert_eq!(
        received,
        [
            vec![
                delta(
                    BookAction::Add,
                    OrderSide::Buy,
                    382_910_001,
                    "211.15",
                    100,
                    false,
                    7312
                ),
                delta(
                    BookAction::Add,
                    OrderSide::Sell,
                    382_910_002,
                    "211.17",
                    200,
                    true,
                    7313
                ),
            ],
            vec![delta(
                BookAction::Update,
                OrderSide::Buy,
                382_910_001,
                "211.15",
                300,
                true,
                7314
            )],
            vec![delta(
                BookAction::Delete,
                OrderSide::Sell,
                382_910_002,
                "211.17",
                200,
                true,
                7315
            )],
        ]
    );
    assert_eq!(
        stream.received().last().unwrap().1,
        json!({"op": "subscribe", "symbols": ["NVDA"], "level": 3, "encoding": "dbn"})
    );
}

/// LSE's Nasdaq stream answers a key of no Nasdaq plan, the free key
/// and the CME Group Lite+ key alike, with HTTP 401 (captured), which
/// fails the connect.
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
async fn an_xnas_stock_streamed_with_no_nasdaq_key_is_refused_naming_its_variable() {
    let (tx, _events) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .cme_api_key(SecretString::from(CME_API_KEY))
        .stream_ids(ids(&[NVDA]))
        .build();
    let factory = LseDataClientFactory::new();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));

    let Err(err) = factory.create(
        factory.name(),
        &config,
        Rc::new(RefCell::new(Cache::default())).into(),
        clock,
    ) else {
        panic!("created a client streaming NVDA with no Nasdaq key")
    };

    let err = format!("{err:#}");
    assert!(
        err.contains(NVDA) && err.contains("LSE_NASDAQ_API_KEY"),
        "{err}"
    );
}

/// One client streaming a CME contract and a Nasdaq stock: each on its
/// exchange's stream, with its plan's key, the contract looked up in
/// LSE's catalogue with the CME key and the stock by its ticker, each
/// opening with its own best bid and offer.
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
