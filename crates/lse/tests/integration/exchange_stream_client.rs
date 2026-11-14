//! The client of an LSE exchange stream against the mock stream
//! (`lse::testing::MockExchangeStream`): its subscription at each level,
//! records and gaps, refusals, closes and reconnects.

use lse::{
    common::{
        consts::LseExchange,
        credential::{LseCredential, LseProduct},
    },
    testing::{
        CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7, CME_GCZ6_L2_DOCUMENTED,
        CME_GCZ6_L3_DOCUMENTED, CME_LEVEL_REQUIRED, CME_SYMBOL_UNAVAILABLE, MockExchangeStream,
        StreamAction, dbn_frames, frames, sent,
    },
    websocket::exchange::{
        client::{ExchangeStreamClient, ExchangeWsMessage},
        messages::{ExchangeLevel, ExchangeRecord},
    },
};
use nautilus_common::testing::wait_until_async;
use nautilus_core::string::secret::SecretString;
use serde_json::{Value, json};

type Out = tokio::sync::mpsc::UnboundedReceiver<ExchangeWsMessage>;

fn credential(key: &str) -> LseCredential {
    LseCredential::resolve(LseProduct::Cme, Some(SecretString::from(key)), |_| None).unwrap()
}

async fn connect(
    peer: &MockExchangeStream,
    symbols: &[&str],
) -> anyhow::Result<(ExchangeStreamClient, Out)> {
    connect_at(peer, ExchangeLevel::Mbp1, symbols).await
}

async fn connect_at(
    peer: &MockExchangeStream,
    level: ExchangeLevel,
    symbols: &[&str],
) -> anyhow::Result<(ExchangeStreamClient, Out)> {
    let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
    let symbols: Vec<String> = symbols.iter().map(ToString::to_string).collect();
    let client = ExchangeStreamClient::connect(
        LseExchange::Cme,
        peer.url(),
        &credential(CME_API_KEY),
        level,
        &symbols,
        out_tx,
    )
    .await?;
    Ok((client, out))
}

/// LSE's captured close of a subscription at a level the key's plan
/// lacks, as a script's answer.
fn level_required() -> Vec<StreamAction> {
    let close: Value = serde_json::from_str(CME_LEVEL_REQUIRED).unwrap();
    vec![StreamAction::Close(
        u16::try_from(close["code"].as_u64().unwrap()).unwrap(),
        close["reason"].as_str().unwrap().to_string(),
    )]
}

async fn next(out: &mut Out) -> ExchangeWsMessage {
    tokio::time::timeout(std::time::Duration::from_secs(10), out.recv())
        .await
        .expect("nothing passed on within 10 s")
        .expect("the client stopped")
}

/// A record passed on: its contract, sequence, whether it opens the
/// contract, and its action.
fn record(message: &ExchangeWsMessage) -> (&str, u64, bool, u8) {
    let ExchangeWsMessage::Record {
        symbol,
        sequence,
        record,
        opening,
    } = message
    else {
        panic!("not a record: {message:?}")
    };
    let action = match record {
        ExchangeRecord::Mbp1(record) => record.action,
        ExchangeRecord::Mbp10(record) => record.action,
        ExchangeRecord::Mbo(record) => record.action,
    };
    (symbol, *sequence, *opening, action as u8)
}

fn subscription(symbols: &[&str]) -> Value {
    subscription_at(1, symbols)
}

fn subscription_at(level: u8, symbols: &[&str]) -> Value {
    json!({"op": "subscribe", "symbols": symbols, "level": level, "encoding": "dbn"})
}

/// The captured opening of GCZ6 and GCG7 at CME's Friday close.
#[tokio::test]
async fn the_key_goes_in_the_header_and_each_contract_opens_with_its_best_bid_and_offer() {
    let peer = MockExchangeStream::cme(|_, _| sent(frames(CME_GCZ6_GCG7)));

    let (_client, mut out) = connect(&peer, &["GCZ6", "GCG7"]).await.unwrap();

    assert_eq!(record(&next(&mut out).await), ("GCZ6", 1794, true, b'M'));
    assert_eq!(record(&next(&mut out).await), ("GCG7", 58, true, b'M'));
    assert_eq!(peer.keys(), [Some(CME_API_KEY.to_string())]);
    assert_eq!(peer.received(), [(0, subscription(&["GCG7", "GCZ6"]))]);
}

/// LSE's documented opening of ESZ6, then a change of its best bid and
/// offer and a trade.
#[tokio::test]
async fn a_contract_s_live_records_follow_its_opening_in_sequence() {
    let peer = MockExchangeStream::cme(|_, _| sent(dbn_frames(CME_ESZ6_DOCUMENTED)));

    let (_client, mut out) = connect(&peer, &["ESZ6"]).await.unwrap();

    let passed = [
        next(&mut out).await,
        next(&mut out).await,
        next(&mut out).await,
    ];
    assert_eq!(
        passed.iter().map(record).collect::<Vec<_>>(),
        [
            ("ESZ6", 554, true, b'M'),
            ("ESZ6", 555, false, b'M'),
            ("ESZ6", 556, false, b'T'),
        ]
    );
}

/// Synthetic: LSE's documented session without its record 555, as a
/// reader that fell behind misses it.
#[tokio::test]
async fn a_jump_of_a_contract_s_sequence_is_passed_on_as_a_gap() {
    let peer = MockExchangeStream::cme(|_, _| {
        let mut frames = dbn_frames(CME_ESZ6_DOCUMENTED);
        frames.remove(4);
        sent(frames)
    });

    let (_client, mut out) = connect(&peer, &["ESZ6"]).await.unwrap();

    assert_eq!(record(&next(&mut out).await), ("ESZ6", 554, true, b'M'));
    assert_eq!(
        next(&mut out).await,
        ExchangeWsMessage::Gap {
            symbol: "ESZ6".into(),
            from: 555,
            to: 555
        }
    );
    assert_eq!(record(&next(&mut out).await), ("ESZ6", 556, false, b'T'));
}

/// LSE's close of a subscription naming NGZ6, which the stream does not
/// carry, captured.
#[tokio::test]
async fn lse_s_refusal_of_the_subscription_fails_the_connect_with_its_code_and_reason() {
    let close: Value = serde_json::from_str(CME_SYMBOL_UNAVAILABLE).unwrap();
    let (code, reason) = (
        u16::try_from(close["code"].as_u64().unwrap()).unwrap(),
        close["reason"].as_str().unwrap().to_string(),
    );
    let peer = MockExchangeStream::cme(move |_, _| vec![StreamAction::Close(code, reason.clone())]);

    let err = connect(&peer, &["GCZ6", "NGZ6"])
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("1008 symbol unavailable: NGZ6"), "{err}");
}

/// LSE answers a key that is no CME Group key with HTTP 401.
#[tokio::test]
async fn a_key_lse_does_not_take_fails_the_connect_without_showing_the_key() {
    let peer = MockExchangeStream::cme(|_, _| sent(frames(CME_GCZ6_GCG7)));
    let (out_tx, _out) = tokio::sync::mpsc::unbounded_channel();
    let other = "lse-other-test-key-0123456789";

    let err = ExchangeStreamClient::connect(
        LseExchange::Cme,
        peer.url(),
        &credential(other),
        ExchangeLevel::Mbp1,
        &["GCZ6".to_string()],
        out_tx,
    )
    .await
    .unwrap_err();

    let err = format!("{err:#}");
    assert!(err.contains("401"), "{err}");
    assert!(!err.contains(other), "{err}");
}

/// GCZ6 named and opened, then subscribed, which this connection carries,
/// and GCG7 asked: a second connection carries both, and GCZ6's opening
/// there brings nothing new.
#[tokio::test]
async fn a_contract_not_carried_yet_is_carried_from_a_new_connection() {
    let peer = MockExchangeStream::cme(|_, subscription| {
        let carried: Vec<&str> = subscription["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .map(|symbol| symbol.as_str().unwrap())
            .collect();
        // The captured frames of the contracts carried.
        let mut frames = frames(CME_GCZ6_GCG7);
        if !carried.contains(&"GCG7") {
            frames.truncate(4);
        }
        sent(frames)
    });
    let (client, mut out) = connect(&peer, &["GCZ6"]).await.unwrap();
    assert_eq!(record(&next(&mut out).await), ("GCZ6", 1794, true, b'M'));
    client.subscriber().subscribe("GCZ6", false).unwrap();

    client.subscriber().subscribe("GCG7", true).unwrap();

    assert_eq!(record(&next(&mut out).await), ("GCG7", 58, true, b'M'));
    let received = peer.wait_until(|received| received.len() == 2).await;
    assert_eq!(
        received,
        [
            (0, subscription(&["GCZ6"])),
            (1, subscription(&["GCG7", "GCZ6"]))
        ]
    );
    client.disconnect().await;
    assert!(out.recv().await.is_none(), "GCZ6 passed on again");
}

/// GCZ6 and GCG7 subscribed, then GCG7 no longer, as a roll leaves its old
/// contract: the peer drops the connection, and the next one's
/// subscription names GCZ6 alone, LSE refusing a whole subscription
/// that names a contract it does not carry, such as one expired.
#[tokio::test]
async fn a_contract_carried_no_longer_is_left_out_of_the_next_connection_s_subscription() {
    let peer = MockExchangeStream::cme(|connection, _| {
        let mut actions = sent(frames(CME_GCZ6_GCG7));
        if connection == 0 {
            actions.push(StreamAction::Hold);
            actions.push(StreamAction::Close(1001, "going away".to_string()));
        }
        actions
    });
    let (client, mut out) = connect(&peer, &["GCZ6", "GCG7"]).await.unwrap();
    assert_eq!(record(&next(&mut out).await), ("GCZ6", 1794, true, b'M'));
    assert_eq!(record(&next(&mut out).await), ("GCG7", 58, true, b'M'));
    client.subscriber().subscribe("GCZ6", false).unwrap();
    client.subscriber().subscribe("GCG7", false).unwrap();

    client.subscriber().unsubscribe("GCG7");
    peer.release();

    let received = peer.wait_until(|received| received.len() == 2).await;
    assert_eq!(
        received,
        [
            (0, subscription(&["GCG7", "GCZ6"])),
            (1, subscription(&["GCZ6"]))
        ]
    );
}

/// On the same connection, after the records passed on before it.
#[tokio::test]
async fn a_new_subscriber_of_a_contract_carried_is_passed_on_after_its_records() {
    let peer = MockExchangeStream::cme(|_, _| sent(dbn_frames(CME_ESZ6_DOCUMENTED)));
    let (client, mut out) = connect(&peer, &["ESZ6"]).await.unwrap();
    for _ in 0..3 {
        next(&mut out).await;
    }

    client.subscriber().subscribe("ESZ6", true).unwrap();

    assert_eq!(
        next(&mut out).await,
        ExchangeWsMessage::Reopen {
            symbol: "ESZ6".into()
        }
    );
    assert_eq!(peer.received().len(), 1);
}

/// ESZ6 subscribed; the peer drops the first connection after its record
/// 555; the second replays LSE's documented session from its opening, of
/// which only the trade 556 is new.
#[tokio::test]
async fn a_dropped_connection_subscribes_again_and_passes_on_only_what_is_new() {
    let peer = MockExchangeStream::cme(|connection, _| {
        let mut frames = sent(dbn_frames(CME_ESZ6_DOCUMENTED));
        if connection == 0 {
            frames.truncate(5);
            frames.push(StreamAction::Hold);
            frames.push(StreamAction::Close(1001, "going away".to_string()));
        }
        frames
    });
    let (client, mut out) = connect(&peer, &["ESZ6"]).await.unwrap();
    client.subscriber().subscribe("ESZ6", false).unwrap();
    peer.release();

    let passed = [
        next(&mut out).await,
        next(&mut out).await,
        next(&mut out).await,
    ];

    assert_eq!(
        passed.iter().map(record).collect::<Vec<_>>(),
        [
            ("ESZ6", 554, true, b'M'),
            ("ESZ6", 555, false, b'M'),
            ("ESZ6", 556, false, b'T'),
        ]
    );
    assert_eq!(
        peer.received(),
        [(0, subscription(&["ESZ6"])), (1, subscription(&["ESZ6"]))]
    );
}

/// GCZ6 at Level 2, as LSE's documentation records the level: its
/// opening, its last ten levels, then three changes of its book.
#[tokio::test]
async fn a_level_2_contract_opens_with_its_ten_levels_then_streams_its_book_s_changes() {
    let peer = MockExchangeStream::cme(|_, _| sent(dbn_frames(CME_GCZ6_L2_DOCUMENTED)));

    let (_client, mut out) = connect_at(&peer, ExchangeLevel::Mbp10, &["GCZ6"])
        .await
        .unwrap();

    let mut passed = Vec::new();
    for _ in 0..4 {
        passed.push(next(&mut out).await);
    }
    assert_eq!(
        passed.iter().map(record).collect::<Vec<_>>(),
        [
            ("GCZ6", 1001, true, b'M'),
            ("GCZ6", 1002, false, b'M'),
            ("GCZ6", 1003, false, b'A'),
            ("GCZ6", 1004, false, b'C'),
        ]
    );
    assert_eq!(peer.received(), [(0, subscription_at(2, &["GCZ6"]))]);
}

/// GCZ6 at Level 3: its opening holds no record ("the stream carries
/// order events and no opening book"), so its order events come first.
#[tokio::test]
async fn a_level_3_contract_opens_with_no_book_and_streams_its_order_events() {
    let peer = MockExchangeStream::cme(|_, _| sent(dbn_frames(CME_GCZ6_L3_DOCUMENTED)));

    let (_client, mut out) = connect_at(&peer, ExchangeLevel::Mbo, &["GCZ6"])
        .await
        .unwrap();

    let mut passed = Vec::new();
    for _ in 0..5 {
        passed.push(next(&mut out).await);
    }
    assert_eq!(
        passed.iter().map(record).collect::<Vec<_>>(),
        [
            ("GCZ6", 7312, false, b'A'),
            ("GCZ6", 7313, false, b'A'),
            ("GCZ6", 7314, false, b'M'),
            ("GCZ6", 7315, false, b'C'),
            ("GCZ6", 7316, false, b'N'),
        ]
    );
    assert_eq!(peer.received(), [(0, subscription_at(3, &["GCZ6"]))]);
}

/// LSE's captured close of a Level 2 and of a Level 3 subscription on
/// the CME Group Lite+ key, which holds Level 1 alone.
#[rstest::rstest]
#[case::level_2(ExchangeLevel::Mbp10)]
#[case::level_3(ExchangeLevel::Mbo)]
#[tokio::test]
async fn lse_s_refusal_of_a_level_the_key_s_plan_lacks_fails_the_connect_with_its_reason(
    #[case] level: ExchangeLevel,
) {
    let peer = MockExchangeStream::cme(|_, _| level_required());

    let err = connect_at(&peer, level, &["GCZ6"])
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains(&format!("{level} subscription"))
            && err.contains("1008 CME Levels 2 and 3 need CME Standard or above"),
        "{err}"
    );
}

/// Started with no contract, the client connects with its first
/// subscription; another made before the connection opened joins it.
#[tokio::test]
async fn a_client_started_with_no_contract_connects_with_those_subscribed_by_then() {
    let peer = MockExchangeStream::cme(|_, _| sent(frames(CME_GCZ6_GCG7)));
    let (out_tx, mut out) = tokio::sync::mpsc::unbounded_channel();
    let (client, opening) = ExchangeStreamClient::start(
        LseExchange::Cme,
        peer.url(),
        &credential(CME_API_KEY),
        ExchangeLevel::Mbp1,
        &[],
        out_tx,
    )
    .unwrap();

    let subscriber = client.subscriber();
    subscriber.subscribe("GCZ6", true).unwrap();
    subscriber.subscribe("GCG7", true).unwrap();

    opening.await.unwrap().unwrap();
    assert_eq!(record(&next(&mut out).await), ("GCZ6", 1794, true, b'M'));
    assert_eq!(record(&next(&mut out).await), ("GCG7", 58, true, b'M'));
    assert_eq!(peer.received(), [(0, subscription(&["GCG7", "GCZ6"]))]);
}

/// A client started on its own learns LSE's refusal from its opening,
/// and stops: a later subscription fails, the connection being closed.
#[tokio::test]
async fn a_client_whose_first_subscription_lse_refuses_stops() {
    let peer = MockExchangeStream::cme(|_, _| level_required());
    let (out_tx, _out) = tokio::sync::mpsc::unbounded_channel();
    let (client, opening) = ExchangeStreamClient::start(
        LseExchange::Cme,
        peer.url(),
        &credential(CME_API_KEY),
        ExchangeLevel::Mbp10,
        &["GCZ6".to_string()],
        out_tx,
    )
    .unwrap();

    let refused = opening.await.unwrap().unwrap_err();

    assert!(
        refused.contains("1008 CME Levels 2 and 3 need CME Standard or above"),
        "{refused}"
    );
    wait_until_async(
        || {
            let closed = client.is_closed();
            async move { closed }
        },
        std::time::Duration::from_secs(10),
    )
    .await;
    assert!(client.subscriber().subscribe("GCZ6", false).is_err());
}

/// Synthetic: LSE closes the first connection with 1013, a contract not
/// ready, which a new connection may lift: the opening learns of it,
/// and the transport reconnects, the new connection's opening coming. The
/// contract is subscribed, as a level's connection after the first
/// carries the contracts taken at it.
#[tokio::test]
async fn a_client_whose_first_subscription_is_closed_for_now_carries_it_from_its_next_connection() {
    let peer = MockExchangeStream::cme(|connection, _| {
        if connection == 0 {
            vec![StreamAction::Close(1013, "GCZ6 not ready".to_string())]
        } else {
            sent(frames(CME_GCZ6_GCG7)[..4].to_vec())
        }
    });
    let (out_tx, mut out) = tokio::sync::mpsc::unbounded_channel();
    let (client, opening) = ExchangeStreamClient::start(
        LseExchange::Cme,
        peer.url(),
        &credential(CME_API_KEY),
        ExchangeLevel::Mbp1,
        &[],
        out_tx,
    )
    .unwrap();
    client.subscriber().subscribe("GCZ6", false).unwrap();

    let refused = opening.await.unwrap().unwrap_err();

    assert!(refused.contains("1013 GCZ6 not ready"), "{refused}");
    assert_eq!(record(&next(&mut out).await), ("GCZ6", 1794, true, b'M'));
}
