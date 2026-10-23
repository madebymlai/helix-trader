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
    ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes, frame,
};

/// The fake account the tests authenticate.
pub(super) const ACCOUNT_ID: i64 = 1_000_001;

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

/// The answers of a cTrader that accepts the application and the
/// account `ACCOUNT_ID`, or `None` for a frame that is no auth request.
pub(super) fn accept_auth(request: &ProtoMessage) -> Option<Vec<Action>> {
    match request.payload_type {
        t if t == ProtoOaApplicationAuthReq::TYPE => {
            Some(vec![reply(request, &ProtoOaApplicationAuthRes::default())])
        }
        t if t == ProtoOaAccountAuthReq::TYPE => Some(vec![reply(
            request,
            &ProtoOaAccountAuthRes {
                ctid_trader_account_id: ACCOUNT_ID,
                ..Default::default()
            },
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
