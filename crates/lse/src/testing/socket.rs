//! A mock LSE socket: a WebSocket server on 127.0.0.1 that opens each
//! connection with LSE's captured welcome, answers each frame it receives
//! with what a script says, replaying LSE's captured frames, and records
//! each frame it receives.

use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
    time::Duration,
};

use nautilus_common::testing::wait_until_async;
use serde_json::Value;
use tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

use super::{
    WS_AUTHENTICATED_FUTURES, WS_PONG, WS_SUBSCRIBED_GOLD, WS_TICKS_GOLD, WS_UNSUBSCRIBED_GOLD,
    WS_WELCOME,
};

/// What the peer does after a frame.
#[derive(Debug, Clone)]
pub enum SocketAction {
    /// Sends this text frame.
    Send(String),
    /// Closes the connection with this code and reason.
    Close(u16, String),
}

/// The frames the peer received: the connection (0, 1, ... in the order
/// they came) and the frame.
pub type Received = Arc<Mutex<Vec<(usize, Value)>>>;

/// The peer: its address and the frames it received.
#[derive(Debug)]
pub struct MockLseSocket {
    url: String,
    received: Received,
}

impl MockLseSocket {
    /// Starts a peer answering each frame received on a connection with
    /// `script` of the connection's number and the frame, each connection
    /// on its own thread, opened with LSE's welcome.
    ///
    /// # Panics
    ///
    /// Panics if no port of 127.0.0.1 is free.
    #[must_use]
    pub fn start(script: impl FnMut(usize, &Value) -> Vec<SocketAction> + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
        let url = format!("ws://{}", listener.local_addr().expect("an address"));
        let received: Received = Arc::default();
        let log = Arc::clone(&received);
        let script = Arc::new(Mutex::new(script));
        std::thread::spawn(move || {
            for (connection, stream) in listener.incoming().enumerate() {
                let Ok(stream) = stream else { return };
                let log = Arc::clone(&log);
                let script = Arc::clone(&script);
                std::thread::spawn(move || {
                    let Ok(mut socket) = tungstenite::accept(stream) else {
                        return;
                    };
                    if socket.send(Message::text(WS_WELCOME.trim())).is_err() {
                        return;
                    }
                    while let Ok(message) = socket.read() {
                        let Message::Text(text) = message else {
                            continue;
                        };
                        let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        log.lock()
                            .expect("not poisoned")
                            .push((connection, frame.clone()));
                        let actions = (script.lock().expect("not poisoned"))(connection, &frame);
                        for action in actions {
                            match action {
                                SocketAction::Send(text) => {
                                    if socket.send(Message::text(text)).is_err() {
                                        return;
                                    }
                                }
                                SocketAction::Close(code, reason) => {
                                    let _ = socket.close(Some(CloseFrame {
                                        code: CloseCode::from(code),
                                        reason: reason.into(),
                                    }));
                                    let _ = socket.flush();
                                    // Drains until the client answers the close.
                                    while socket.read().is_ok() {}
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        Self { url, received }
    }

    /// The peer's address, the adapter's socket address.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The frames received so far, in the order they came, pings aside.
    #[must_use]
    pub fn received(&self) -> Vec<(usize, Value)> {
        self.received
            .lock()
            .expect("not poisoned")
            .iter()
            .filter(|(_, frame)| frame["action"] != "ping")
            .cloned()
            .collect()
    }

    /// Waits, up to 10 s, until a frame `matches`, pings included.
    ///
    /// # Panics
    ///
    /// Panics if none does within 10 s.
    pub async fn wait_until_any(&self, matches: impl Fn(&Value) -> bool) {
        wait_until_async(
            || {
                let met = self
                    .received
                    .lock()
                    .expect("not poisoned")
                    .iter()
                    .any(|(_, frame)| matches(frame));
                async move { met }
            },
            Duration::from_secs(10),
        )
        .await;
    }

    /// Waits, up to 10 s, until `condition` holds on the frames received
    /// (`received`), and returns them.
    ///
    /// # Panics
    ///
    /// Panics if it does not hold within 10 s.
    pub async fn wait_until(
        &self,
        condition: impl Fn(&[(usize, Value)]) -> bool,
    ) -> Vec<(usize, Value)> {
        wait_until_async(
            || {
                let met = condition(&self.received());
                async move { met }
            },
            Duration::from_secs(10),
        )
        .await;
        self.received()
    }
}

/// What LSE's data-ws answers the free key, from its first captures: the
/// key with its futures (`WS_AUTHENTICATED_FUTURES`, 16 subscriptions a
/// connection), and `GOLD.F`'s subscription with its three ticks captured
/// (see `socket_answer`).
///
/// # Panics
///
/// Panics on a frame data-ws has no answer for here.
#[must_use]
pub fn data_ws_answer(frame: &Value) -> Vec<SocketAction> {
    socket_answer(WS_AUTHENTICATED_FUTURES, WS_TICKS_GOLD, frame)
}

/// What an LSE socket answers the free key, from its captures: the key
/// with `authenticated`, its authenticated frame; a subscription with its
/// acknowledgement, then the ticks of its symbol among `ticks`, one a
/// line; an unsubscription with its acknowledgement; a ping with a pong.
///
/// # Panics
///
/// Panics on a frame the socket has no answer for here, or on a line of
/// `ticks` that is no tick.
#[must_use]
pub fn socket_answer(authenticated: &str, ticks: &str, frame: &Value) -> Vec<SocketAction> {
    let send = |text: &str| SocketAction::Send(text.trim().to_string());
    let symbol = frame["symbol"].as_str().unwrap_or_default();
    match frame["action"].as_str() {
        Some("auth") => vec![send(authenticated)],
        Some("subscribe") => {
            let mut answer = vec![send(&WS_SUBSCRIBED_GOLD.replace("GOLD.F", symbol))];
            answer.extend(
                ticks
                    .lines()
                    .filter(|line| {
                        let tick: Value = serde_json::from_str(line).expect("a tick a line");
                        tick["symbol"] == symbol
                    })
                    .map(send),
            );
            answer
        }
        Some("unsubscribe") => vec![send(&WS_UNSUBSCRIBED_GOLD.replace("GOLD.F", symbol))],
        Some("ping") => vec![send(WS_PONG)],
        _ => panic!("the socket has no answer here for {frame}"),
    }
}
