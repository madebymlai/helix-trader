//! The client of one of LSE's sockets against the mock socket
//! (`lse::testing::MockLseSocket`): authentication, subscriptions, chains,
//! refusals, keepalive and reconnects.

use lse::{
    common::{
        consts::LseSocket,
        credential::{LseCredential, LseProduct},
    },
    testing::{
        API_KEY, MockLseSocket, SocketAction, WS_INVALID_KEY, WS_INVALID_UNDERLYING,
        WS_LIMIT_REACHED, WS_TICKS_GOLD, WS_TOO_MANY_CONNECTIONS, data_ws_answer,
    },
    websocket::client::{KEEPALIVE_SECS, LseWebSocketClient, LseWsMessage},
};
use nautilus_core::string::secret::SecretString;
use serde_json::{Value, json};

type Out = tokio::sync::mpsc::UnboundedReceiver<LseWsMessage>;

fn credential() -> LseCredential {
    LseCredential::resolve(
        LseProduct::Global,
        Some(SecretString::from(API_KEY)),
        |_| None,
    )
    .unwrap()
}

async fn connect(peer: &MockLseSocket) -> anyhow::Result<(LseWebSocketClient, Out)> {
    connect_keeping_alive(peer, KEEPALIVE_SECS).await
}

async fn connect_keeping_alive(
    peer: &MockLseSocket,
    keepalive_secs: u64,
) -> anyhow::Result<(LseWebSocketClient, Out)> {
    let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
    let client = LseWebSocketClient::connect(
        LseSocket::Data,
        peer.url(),
        &credential(),
        keepalive_secs,
        out_tx,
    )
    .await?;
    Ok((client, out))
}

async fn next(out: &mut Out) -> LseWsMessage {
    tokio::time::timeout(std::time::Duration::from_secs(10), out.recv())
        .await
        .expect("nothing passed on within 10 s")
        .expect("the client stopped")
}

fn frames_of(received: &[(usize, Value)], connection: usize) -> Vec<Value> {
    received
        .iter()
        .filter(|(of, _)| *of == connection)
        .map(|(_, frame)| frame.clone())
        .collect()
}

fn auth() -> Value {
    json!({"action": "auth", "api_key": API_KEY})
}

fn subscribe(symbol: &str) -> Value {
    json!({"action": "subscribe", "symbol": symbol})
}

fn subscribe_options(underlying: &str) -> Value {
    json!({"action": "subscribe_options", "underlying": underlying})
}

fn send(text: &str) -> SocketAction {
    SocketAction::Send(text.trim().to_string())
}

#[tokio::test]
async fn the_key_answers_the_welcome_and_the_socket_s_list_and_limit_are_kept() {
    let peer = MockLseSocket::start(|_, frame| data_ws_answer(frame));

    let (client, _) = connect(&peer).await.unwrap();

    assert_eq!(peer.received(), [(0, auth())]);
    assert!(client.lists("GOLD.F") && client.lists("gold.f"));
    assert!(!client.lists("NOSUCH.F"));
    assert_eq!(client.max_subscriptions(), Some(16));
}

#[tokio::test]
async fn a_key_lse_does_not_know_fails_the_connect_with_lse_s_code_and_message() {
    let peer = MockLseSocket::start(|_, _| vec![send(WS_INVALID_KEY)]);

    let err = connect(&peer).await.unwrap_err().to_string();

    assert!(
        err.contains("INVALID_KEY: Invalid or inactive API key"),
        "{err}"
    );
    assert!(!err.contains(API_KEY), "{err}");
}

/// LSE closes the connection with 1008 after its refusal.
#[tokio::test]
async fn a_connection_beyond_the_key_s_fails_the_connect_naming_lse_s_limit() {
    let peer = MockLseSocket::start(|_, _| {
        vec![
            send(WS_TOO_MANY_CONNECTIONS),
            SocketAction::Close(1008, "too many connections".to_string()),
        ]
    });

    let err = connect(&peer).await.unwrap_err().to_string();

    assert!(err.contains("TOO_MANY_CONNECTIONS"), "{err}");
    assert!(err.contains("max_connections 1"), "{err}");
}

/// The three ticks captured after `GOLD.F`'s acknowledgement.
#[tokio::test]
async fn a_subscription_brings_the_ticks_of_its_symbol() {
    let peer = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (client, mut out) = connect(&peer).await.unwrap();

    client.subscribe("GOLD.F").unwrap();

    let mut bids = Vec::new();
    for _ in 0..3 {
        let LseWsMessage::Tick(tick) = next(&mut out).await else {
            panic!("not a tick")
        };
        assert_eq!(tick.symbol, "GOLD.F");
        bids.push(tick.bid.unwrap());
    }
    assert_eq!(bids, [3888.0, 3887.6, 3888.0]);
    assert_eq!(peer.received(), [(0, auth()), (0, subscribe("GOLD.F"))]);
}

/// LSE's documented keepalive, which it answers with a pong.
#[tokio::test]
async fn the_keepalive_is_lse_s_ping() {
    let peer = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let _connected = connect_keeping_alive(&peer, 1).await.unwrap();

    peer.wait_until_any(|frame| *frame == json!({"action": "ping"}))
        .await;
}

/// A frame not read is skipped: the ticks after it come through.
#[tokio::test]
async fn a_malformed_frame_loses_none_of_the_frames_after_it() {
    let peer = MockLseSocket::start(|_, frame| match frame["action"].as_str() {
        Some("subscribe") => vec![
            send(r#"{"type":"tick","symbol":"GOLD.F"}"#),
            send("not json"),
            send(WS_TICKS_GOLD.lines().next().unwrap()),
        ],
        _ => data_ws_answer(frame),
    });
    let (client, mut out) = connect(&peer).await.unwrap();

    client.subscribe("GOLD.F").unwrap();

    let LseWsMessage::Tick(tick) = next(&mut out).await else {
        panic!("not a tick")
    };
    assert_eq!(tick.ts, "2026-11-13T16:44:55.499567+00:00");
}

#[tokio::test]
async fn an_unsubscription_goes_to_lse() {
    let peer = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (client, _) = connect(&peer).await.unwrap();
    client.subscribe("SILVER.F").unwrap();

    client.unsubscribe("SILVER.F").unwrap();

    let received = peer.wait_until(|received| received.len() == 3).await;
    assert_eq!(
        received[2],
        (0, json!({"action": "unsubscribe", "symbol": "SILVER.F"}))
    );
}

/// LSE's error names no symbol: it answers the oldest subscription not
/// answered yet.
#[tokio::test]
async fn lse_s_refusal_of_a_subscription_is_passed_on_with_its_symbol_code_and_message() {
    let peer = MockLseSocket::start(|_, frame| match frame["symbol"].as_str() {
        Some("SILVER.F") => vec![send(WS_LIMIT_REACHED)],
        _ => data_ws_answer(frame),
    });
    let (client, mut out) = connect(&peer).await.unwrap();

    client.subscribe("SILVER.F").unwrap();

    let LseWsMessage::Refused { symbol, refusal } = next(&mut out).await else {
        panic!("not a refusal")
    };
    assert_eq!(symbol, "SILVER.F");
    assert_eq!(
        refusal.to_string(),
        "LIMIT_REACHED: Max 16 symbols on this key."
    );
}

/// LSE's documented chain of AAPL: one subscription, then the ticks of
/// its contracts, each under its OSI symbol without spaces.
#[tokio::test]
async fn a_chain_s_subscription_brings_the_ticks_of_its_contracts() {
    let peer = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (client, mut out) = connect(&peer).await.unwrap();

    client.subscribe_options("AAPL").unwrap();

    let mut symbols = Vec::new();
    for _ in 0..5 {
        let LseWsMessage::Tick(tick) = next(&mut out).await else {
            panic!("not a tick")
        };
        symbols.push(tick.symbol);
    }
    assert_eq!(
        symbols,
        [
            "AAPL261120C00355000",
            "AAPL261120C00355000",
            "AAPL261113C00360000",
            "AAPL261118C00360000",
            "AAPL270917P00325000",
        ]
    );
    assert_eq!(
        peer.received(),
        [(0, auth()), (0, subscribe_options("AAPL"))]
    );
}

#[tokio::test]
async fn a_chain_s_unsubscription_goes_to_lse() {
    let peer = MockLseSocket::start(|_, frame| data_ws_answer(frame));
    let (client, _) = connect(&peer).await.unwrap();
    client.subscribe_options("AAPL").unwrap();

    client.unsubscribe_options("AAPL").unwrap();

    let received = peer.wait_until(|received| received.len() == 3).await;
    assert_eq!(
        received[2],
        (
            0,
            json!({"action": "unsubscribe_options", "underlying": "AAPL"})
        )
    );
}

/// LSE's error names no underlying: it answers the oldest subscription
/// not answered yet, here the chain's after a symbol's.
#[tokio::test]
async fn lse_s_refusal_of_a_chain_is_passed_on_with_its_underlying_code_and_message() {
    let peer = MockLseSocket::start(|_, frame| match frame["underlying"].as_str() {
        Some("NOSUCHX") => vec![send(WS_INVALID_UNDERLYING)],
        _ => data_ws_answer(frame),
    });
    let (client, mut out) = connect(&peer).await.unwrap();
    client.subscribe("SILVER.F").unwrap();

    client.subscribe_options("NOSUCHX").unwrap();

    let LseWsMessage::OptionsRefused {
        underlying,
        refusal,
    } = next(&mut out).await
    else {
        panic!("not a chain's refusal")
    };
    assert_eq!(underlying, "NOSUCHX");
    assert_eq!(
        refusal.to_string(),
        "INVALID_UNDERLYING: No options available for NOSUCHX"
    );
}

/// The peer drops the first connection after AAPL's chain's first
/// tick: the new connection subscribes the chain again.
#[tokio::test]
async fn a_dropped_connection_subscribes_its_chains_again() {
    let peer = MockLseSocket::start(|connection, frame| {
        let mut answer = data_ws_answer(frame);
        if connection == 0 && frame["action"] == "subscribe_options" {
            answer.truncate(2);
            answer.push(SocketAction::Close(1001, "going away".to_string()));
        }
        answer
    });
    let (client, mut out) = connect(&peer).await.unwrap();

    client.subscribe_options("AAPL").unwrap();

    assert!(matches!(next(&mut out).await, LseWsMessage::Tick(_)));
    assert!(matches!(
        next(&mut out).await,
        LseWsMessage::Reconnected { .. }
    ));
    assert!(matches!(next(&mut out).await, LseWsMessage::Tick(_)));
    let received = peer.received();
    assert_eq!(frames_of(&received, 1), [auth(), subscribe_options("AAPL")]);
    client.disconnect().await;
}

/// The peer drops the first connection after `GOLD.F`'s first tick.
#[tokio::test]
async fn a_dropped_connection_reconnects_subscribes_again_and_tells_the_gap() {
    let peer = MockLseSocket::start(|connection, frame| {
        let mut answer = data_ws_answer(frame);
        if connection == 0 && frame["action"] == "subscribe" {
            answer.truncate(2);
            answer.push(SocketAction::Close(1001, "going away".to_string()));
        }
        answer
    });
    let (client, mut out) = connect(&peer).await.unwrap();

    client.subscribe("GOLD.F").unwrap();

    assert!(matches!(next(&mut out).await, LseWsMessage::Tick(_)));
    let LseWsMessage::Reconnected { last, at } = next(&mut out).await else {
        panic!("not a reconnect")
    };
    assert!(last.is_some_and(|last| last <= at), "{last:?} {at}");
    assert!(matches!(next(&mut out).await, LseWsMessage::Tick(_)));
    let received = peer.received();
    assert_eq!(frames_of(&received, 1), [auth(), subscribe("GOLD.F")]);
    client.disconnect().await;
}
