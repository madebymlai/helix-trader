//! A mock cTrader peer for the tests: a WebSocket server on 127.0.0.1
//! that records every frame it receives and answers through a script.

use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nautilus_core::string::secret::SecretString;
use prost::Message as _;
use tungstenite::Message;

use super::config::{IcAccountId, IcCredential};
use super::messages::{
    Payload, ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
    ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes, ProtoOaGetTickDataReq,
    ProtoOaGetTickDataRes, ProtoOaTickData, frame,
};
use super::ticks::Tick;

/// The fake account the tests authenticate: the `ctidTraderAccountId` the
/// captured responses were scrubbed to.
pub(super) const ACCOUNT_ID: i64 = 1_000_001;

// Responses captured from IC's cTrader demo, scrubbed: the payloads, as
// served (see the `.txtpb` beside each in `testdata/`).
pub(super) const APPLICATION_AUTH_RES: &[u8] = include_bytes!("testdata/application_auth_res.bin");
pub(super) const APPLICATION_AUTH_REFUSED: &[u8] =
    include_bytes!("testdata/application_auth_refused.bin");
pub(super) const ACCOUNT_AUTH_RES: &[u8] = include_bytes!("testdata/account_auth_res.bin");
pub(super) const ACCOUNT_AUTH_TOKEN_INVALID: &[u8] =
    include_bytes!("testdata/account_auth_token_invalid.bin");
pub(super) const ASSET_LIST_RES: &[u8] = include_bytes!("testdata/asset_list_res.bin");
pub(super) const SYMBOLS_LIST_RES: &[u8] = include_bytes!("testdata/symbols_list_res.bin");
pub(super) const SYMBOL_BY_ID_RES: &[u8] = include_bytes!("testdata/symbol_by_id_res.bin");
pub(super) const SYMBOL_BY_ID_UNKNOWN: &[u8] = include_bytes!("testdata/symbol_by_id_unknown.bin");
pub(super) const TICK_DATA_BID_RES: &[u8] = include_bytes!("testdata/tick_data_bid_res.bin");
pub(super) const TICK_DATA_ASK_RES: &[u8] = include_bytes!("testdata/tick_data_ask_res.bin");

/// What the peer does after a frame.
pub(super) enum Action {
    /// Sends these bytes as one binary frame.
    Send(Vec<u8>),
    /// Closes this connection.
    Close,
}

/// The answer to `request`: `response` under the request's `clientMsgId`.
pub(super) fn reply<M: Payload>(request: &ProtoMessage, response: &M) -> Action {
    Action::Send(frame(response, request.client_msg_id.clone()))
}

/// The answer to `request`: the captured `payload`, an `M`, under the
/// request's `clientMsgId`. The bytes go as captured, fields the vendored
/// `.proto` lacks included.
pub(super) fn replay<M: Payload>(request: &ProtoMessage, payload: &[u8]) -> Action {
    Action::Send(
        ProtoMessage {
            payload_type: M::TYPE,
            payload: Some(payload.to_vec()),
            client_msg_id: request.client_msg_id.clone(),
        }
        .encode_to_vec(),
    )
}

/// The captured answers of a cTrader that accepts the application and the
/// account `ACCOUNT_ID`, or `None` for a frame that is no auth request.
pub(super) fn accept_auth(request: &ProtoMessage) -> Option<Vec<Action>> {
    match request.payload_type {
        t if t == ProtoOaApplicationAuthReq::TYPE => {
            Some(vec![replay::<ProtoOaApplicationAuthRes>(
                request,
                APPLICATION_AUTH_RES,
            )])
        }
        t if t == ProtoOaAccountAuthReq::TYPE => Some(vec![replay::<ProtoOaAccountAuthRes>(
            request,
            ACCOUNT_AUTH_RES,
        )]),
        _ => None,
    }
}

/// The frames the peer received: the connection (0, 1, ...) and the frame.
pub(super) type Received = Arc<Mutex<Vec<(usize, ProtoMessage)>>>;

/// Starts the peer; `script` answers each frame received on a connection,
/// heartbeats aside, which cTrader does not answer. Serves connections one
/// after another, until the test ends.
pub(super) fn start(
    mut script: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
) -> (String, Received) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let received: Received = Arc::default();
    let log = Arc::clone(&received);
    std::thread::spawn(move || {
        for (connection, stream) in listener.incoming().enumerate() {
            let Ok(mut socket) = tungstenite::accept(stream.unwrap()) else {
                continue;
            };
            'frames: while let Ok(message) = socket.read() {
                let Message::Binary(bytes) = message else {
                    continue;
                };
                let request = ProtoMessage::decode(&bytes[..]).unwrap();
                log.lock().unwrap().push((connection, request.clone()));
                if request.payload_type == ProtoHeartbeatEvent::TYPE {
                    continue;
                }
                for action in script(connection, &request) {
                    match action {
                        Action::Send(bytes) => {
                            if socket.send(Message::Binary(bytes.into())).is_err() {
                                break 'frames;
                            }
                        }
                        Action::Close => {
                            let _ = socket.close(None);
                            let _ = socket.flush();
                            break 'frames;
                        }
                    }
                }
            }
        }
    });
    (url, received)
}

/// Waits, up to 10 s, until `condition` holds on the frames received.
pub(super) async fn wait_until(
    received: &Received,
    condition: impl Fn(&[(usize, ProtoMessage)]) -> bool,
) -> Vec<(usize, ProtoMessage)> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let frames = received.lock().unwrap().clone();
        if condition(&frames) {
            return frames;
        }
        assert!(
            Instant::now() < deadline,
            "condition not met; received: {frames:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A credential of made-up values, for account `ACCOUNT_ID`.
pub(super) fn credential() -> IcCredential {
    IcCredential {
        client_id: SecretString::from("12345_clientIdValue"),
        client_secret: SecretString::from("clientSecretValue"),
        access_token: SecretString::from("accessTokenValue"),
        account_id: IcAccountId::new(ACCOUNT_ID),
    }
}

/// cTrader's answer to a tick request over `[from, to)` on a side whose
/// ticks are `ticks`, oldest first: the newest `page_size` ticks of the
/// window, newest first, and whether the window holds more (as measured
/// on the demo, see `ic/README.md`, "Tick history").
pub(super) fn venue_page(
    ticks: &[Tick],
    from: i64,
    to: i64,
    page_size: usize,
) -> (Vec<Tick>, bool) {
    let window: Vec<Tick> = ticks
        .iter()
        .rev()
        .filter(|t| from <= t.ms && t.ms < to)
        .copied()
        .collect();
    let more = window.len() > page_size;
    (window.into_iter().take(page_size).collect(), more)
}

/// cTrader's answer to `request`, a tick request, on a symbol whose BID
/// and ASK ticks are `bids` and `asks`, oldest first, in pages of
/// `page_size` (see `venue_page`): the first tick absolute, the others
/// differences from the one before.
pub(super) fn tick_answer(
    request: &ProtoOaGetTickDataReq,
    bids: &[Tick],
    asks: &[Tick],
    page_size: usize,
) -> ProtoOaGetTickDataRes {
    let ticks = if request.r#type == 1 { bids } else { asks };
    let (page, has_more) = venue_page(
        ticks,
        request.from_timestamp.unwrap(),
        request.to_timestamp.unwrap(),
        page_size,
    );
    let mut before: Option<Tick> = None;
    let tick_data = page
        .into_iter()
        .map(|tick| {
            let data = match before {
                None => ProtoOaTickData {
                    timestamp: tick.ms,
                    tick: tick.price,
                },
                Some(newer) => ProtoOaTickData {
                    timestamp: tick.ms - newer.ms,
                    tick: tick.price - newer.price,
                },
            };
            before = Some(tick);
            data
        })
        .collect();
    ProtoOaGetTickDataRes {
        ctid_trader_account_id: request.ctid_trader_account_id,
        tick_data,
        has_more,
        ..Default::default()
    }
}
