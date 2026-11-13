//! The client of one LSE socket: an outer client and the handler task
//! that owns Nautilus' `WebSocketClient`, as Nautilus' adapter guide lays
//! one out ("WebSocket client patterns").
//!
//! `connect` opens the socket, and the handler answers LSE's welcome with
//! the key; the client is connected once LSE authenticates it, keeping the
//! symbols the socket lists to the key and the subscriptions it allows a
//! connection, or fails with LSE's refusal (`INVALID_KEY`,
//! `TOO_MANY_CONNECTIONS`, ...). The handler tracks the subscriptions in
//! Nautilus' `SubscriptionState` (topics: the symbols as the socket spells
//! them), sends each once the connection is authenticated, has LSE's
//! acknowledgement confirm it, and passes LSE's refusal of one on with its
//! symbol: LSE answers subscriptions and unsubscriptions in turn, and its
//! error names no symbol. Ticks are passed on as they come.
//!
//! Nautilus' transport reconnects a dropped connection with backoff; on
//! the new one the handler authenticates again, sends every subscription
//! again (`reset_after_reconnect`), and passes on the gap: when the last
//! frame of the dropped connection came, and when the new one was
//! authenticated. Nothing is fetched to fill it. A connection LSE closes
//! for a refusal that a new connection cannot lift (`TERMINAL`) is not
//! reconnected. Every frame is bound to the connection it is meant for
//! (`send_text_on_connection`), so none reaches a connection before its
//! authentication; one not sent waits in the subscription state for the
//! next. The application keepalive LSE asks for (`{"action": "ping"}`)
//! is the transport's heartbeat, as often as LSE's Python client sends it.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use ahash::AHashSet;
use anyhow::Context;
use nautilus_common::live::dst::time::{self, Duration};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::task::{TaskJoinOutcome, TaskSlot, finish_task};
use nautilus_network::{
    Message, RECONNECTED,
    websocket::{
        CloseHandler, EpochMessageHandler, SubscriptionState, WebSocketClient, WebSocketConfig,
    },
};

use super::messages::{LseTick, LseWsAction, LseWsAuthenticated, LseWsFrame, LseWsRefusal};
use crate::common::{consts::LseSocket, credential::LseCredential};

/// How long LSE may take to authenticate the key.
const AUTH_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the keepalive goes: as LSE's Python client sends it, which
/// keeps a connection open on every socket (LSE's WebSocket
/// documentation, "The Python client").
pub(crate) const KEEPALIVE_SECS: u64 = 25;

/// The transport's backoff between reconnect attempts.
const RECONNECT_DELAY_INITIAL_MS: u64 = 1_000;
const RECONNECT_DELAY_MAX_MS: u64 = 30_000;
const RECONNECT_BACKOFF_FACTOR: f64 = 2.0;
const RECONNECT_JITTER_MS: u64 = 500;

/// How long `disconnect` waits for the handler to close the transport,
/// then for its abort.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const ABORT_TIMEOUT: Duration = Duration::from_secs(2);

/// The close code LSE refuses a connection with (LSE's WebSocket
/// documentation, "Error codes").
const POLICY_VIOLATION: u16 = 1008;

/// LSE's refusals closing a connection that a new one would not lift
/// ("Error codes"): a newer connection took its slot, the key is in use
/// elsewhere or revoked, or the month's streaming is used up. The others
/// closing one (`TOO_MANY_CONNECTIONS` and those of the address) clear
/// once a connection LSE still counts is gone, so the transport goes on
/// reconnecting with backoff.
const TERMINAL: [&str; 4] = [
    "CONNECTION_REPLACED",
    "KEY_IN_USE",
    "KEY_REVOKED",
    "QUOTA_EXCEEDED",
];

/// The delimiter of the subscription state's topics, which no LSE symbol
/// holds: a topic is one symbol.
const TOPIC_DELIMITER: char = ':';

/// What the client passes on.
#[derive(Debug, Clone)]
pub(crate) enum LseWsMessage {
    Tick(LseTick),
    /// LSE refused the subscription of `symbol`.
    Refused {
        symbol: String,
        refusal: LseWsRefusal,
    },
    /// The connection dropped and its replacement is authenticated, its
    /// subscriptions sent again: nothing came from `last`, the dropped
    /// connection's last frame, if one came, to `at`.
    Reconnected {
        last: Option<UnixNanos>,
        at: UnixNanos,
    },
}

/// A connection to one socket, authenticated with one key.
#[derive(Debug)]
pub(crate) struct LseWebSocketClient {
    socket: LseSocket,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
    /// The handler task, explicitly singular: `disconnect` joins it.
    handler: TaskSlot<()>,
    /// The symbols the socket lists to the key, in capitals: LSE matches
    /// them in any case.
    listed: AHashSet<String>,
    max_subscriptions: Option<usize>,
}

impl LseWebSocketClient {
    /// Connects to `socket` at `url` and authenticates with the key of
    /// `credential`, sending the keepalive every `keepalive_secs`, and
    /// passing ticks, refusals and reconnects to `out`.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails, or if LSE refuses the key
    /// (with its code, message and details, such as the connections it
    /// allows) or does not authenticate it within `AUTH_TIMEOUT`.
    pub(crate) async fn connect(
        socket: LseSocket,
        url: &str,
        credential: &LseCredential,
        keepalive_secs: u64,
        out: tokio::sync::mpsc::UnboundedSender<LseWsMessage>,
    ) -> anyhow::Result<Self> {
        let name = socket.name();
        let (raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Message)>();
        // The last refusal's code, read on the transport's reader task as
        // it comes, so that its close, which follows it there, is judged by
        // it (`TERMINAL`).
        let refused: Arc<Mutex<Option<String>>> = Arc::default();
        let seen = Arc::clone(&refused);
        let message_handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            if let Message::Text(text) = &message
                && let Ok(LseWsFrame::Error(refusal)) = serde_json::from_slice(text)
            {
                *seen.lock().expect("not poisoned") = Some(refusal.code);
            }
            // The receiver is gone only once the handler has stopped.
            let _ = raw_tx.send((epoch, message));
        });
        let close_handler: CloseHandler = Arc::new(move |frame| {
            let code = refused.lock().expect("not poisoned").take();
            match (frame, code) {
                (Some(frame), Some(code))
                    if frame.code == POLICY_VIOLATION && TERMINAL.contains(&code.as_str()) =>
                {
                    log::error!("LSE's {name} closed the connection for {code}: not reconnecting");
                    false
                }
                _ => true,
            }
        });
        let config = WebSocketConfig::builder()
            .url(url.to_owned())
            .heartbeat_interval_secs(keepalive_secs)
            .heartbeat_payload(LseWsAction::Ping.to_json())
            .reconnect_delay_initial_ms(RECONNECT_DELAY_INITIAL_MS)
            .reconnect_delay_max_ms(RECONNECT_DELAY_MAX_MS)
            .reconnect_backoff_factor(RECONNECT_BACKOFF_FACTOR)
            .reconnect_jitter_ms(RECONNECT_JITTER_MS)
            .build()
            .with_context(|| format!("the config of LSE's {name}"))?;
        let client = WebSocketClient::epoch_builder()
            .config(config)
            .epoch_handler(message_handler)
            .close_handler(close_handler)
            .connect()
            .await
            .with_context(|| format!("connecting to LSE's {name}"))?;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        cmd_tx
            .send(HandlerCommand::SetClient(client))
            .ok()
            .with_context(|| format!("the handler of LSE's {name} is gone"))?;
        let (authenticated_tx, authenticated_rx) = tokio::sync::oneshot::channel();
        let handler = LseWsHandler {
            socket,
            credential: credential.clone(),
            client: None,
            cmd_rx,
            raw_rx,
            out,
            authenticating: Some(authenticated_tx),
            subscriptions: SubscriptionState::new(TOPIC_DELIMITER),
            epoch: 0,
            authenticated: false,
            unanswered: VecDeque::new(),
            last_frame: None,
            dropped: None,
        };
        let mut task = TaskSlot::new();
        task.spawn(handler.run())
            .map_err(|e| anyhow::anyhow!("starting the handler of LSE's {name}: {e}"))?;
        let mut connected = Self {
            socket,
            cmd_tx,
            handler: task,
            listed: AHashSet::new(),
            max_subscriptions: None,
        };
        let refused = match time::timeout(AUTH_TIMEOUT, authenticated_rx).await {
            Ok(Ok(Ok(authenticated))) => {
                connected.listed = authenticated
                    .symbols
                    .into_iter()
                    .map(|listed| listed.symbol.to_ascii_uppercase())
                    .collect();
                connected.max_subscriptions = authenticated.max_subscriptions;
                return Ok(connected);
            }
            Ok(Ok(Err(refusal))) => format!("LSE's {name} refused the key: {refusal}"),
            Ok(Err(_)) => format!("LSE's {name} stopped before authenticating the key"),
            Err(_) => format!("LSE's {name} did not authenticate the key within {AUTH_TIMEOUT:?}"),
        };
        connected.disconnect().await;
        anyhow::bail!(refused)
    }

    /// Whether the socket lists `symbol` to the key, in any case: LSE
    /// accepts a subscription to a symbol it does not list, and sends
    /// nothing.
    pub(crate) fn lists(&self, symbol: &str) -> bool {
        self.listed.contains(&symbol.to_ascii_uppercase())
    }

    /// The symbols one connection of the key may stream at once, as LSE
    /// stated it when it authenticated the key.
    pub(crate) fn max_subscriptions(&self) -> Option<usize> {
        self.max_subscriptions
    }

    /// Subscribes to the ticks of `symbol`, as the socket spells it, for
    /// the rest of the connection, reconnects included. LSE's answer comes
    /// after this returns: its refusal is passed on (`Refused`).
    ///
    /// # Errors
    ///
    /// Returns an error if the client is disconnected.
    pub(crate) fn subscribe(&self, symbol: &str) -> anyhow::Result<()> {
        self.command(HandlerCommand::Subscribe(symbol.to_owned()))
    }

    /// Ends the subscription of `symbol`. LSE may still send a tick or
    /// two of it.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is disconnected.
    pub(crate) fn unsubscribe(&self, symbol: &str) -> anyhow::Result<()> {
        self.command(HandlerCommand::Unsubscribe(symbol.to_owned()))
    }

    fn command(&self, command: HandlerCommand) -> anyhow::Result<()> {
        self.cmd_tx
            .send(command)
            .ok()
            .with_context(|| format!("the connection to LSE's {} is closed", self.socket.name()))
    }

    /// Closes the connection, joining the handler within
    /// `DISCONNECT_TIMEOUT`, then aborting it.
    pub(crate) async fn disconnect(mut self) {
        // The handler is gone once it has stopped.
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        let name = self.socket.name();
        match finish_task(&mut self.handler, DISCONNECT_TIMEOUT, ABORT_TIMEOUT).await {
            None | Some(TaskJoinOutcome::Completed(())) => {}
            Some(TaskJoinOutcome::Aborted) => {
                log::warn!("the handler of LSE's {name} did not stop in time: aborted");
            }
            Some(TaskJoinOutcome::Failed(e)) => {
                log::error!("the handler of LSE's {name} failed: {e}")
            }
            Some(TaskJoinOutcome::Incomplete) => {
                log::error!("the handler of LSE's {name} did not stop after its abort");
            }
        }
    }
}

/// Commands from the outer client to the handler.
enum HandlerCommand {
    /// The connected client; the first command, queued before the command
    /// sender is shared, so no subscription can overtake it.
    SetClient(WebSocketClient),
    Subscribe(String),
    Unsubscribe(String),
    Disconnect,
}

/// A subscription or unsubscription sent and not answered yet.
enum Unanswered {
    Subscribe(String),
    Unsubscribe(String),
}

/// The handler task: the one owner of the `WebSocketClient`.
struct LseWsHandler {
    socket: LseSocket,
    credential: LseCredential,
    client: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<(u64, Message)>,
    out: tokio::sync::mpsc::UnboundedSender<LseWsMessage>,
    /// Where the first authentication's outcome goes, until it comes.
    authenticating: Option<tokio::sync::oneshot::Sender<Result<LseWsAuthenticated, LseWsRefusal>>>,
    subscriptions: SubscriptionState,
    /// The connection the handler speaks on: 0, then each replacement's.
    epoch: u64,
    /// Whether LSE authenticated the key on that connection.
    authenticated: bool,
    /// The subscriptions and unsubscriptions sent on that connection and
    /// not answered yet, in the order sent, which LSE answers them in.
    unanswered: VecDeque<Unanswered>,
    /// When the last frame came.
    last_frame: Option<UnixNanos>,
    /// Between a reconnect and the new connection's authentication: the
    /// dropped connection's last frame, if one came.
    dropped: Option<Option<UnixNanos>>,
}

impl LseWsHandler {
    /// Runs until disconnected or the outer client is dropped.
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                command = self.cmd_rx.recv() => match command {
                    Some(HandlerCommand::SetClient(client)) => self.client = Some(client),
                    Some(HandlerCommand::Subscribe(symbol)) => {
                        self.subscriptions.mark_subscribe(&symbol);
                        if self.authenticated {
                            self.send_subscribe(symbol).await;
                        }
                    }
                    Some(HandlerCommand::Unsubscribe(symbol)) => {
                        self.subscriptions.mark_unsubscribe(&symbol);
                        if self.authenticated {
                            let frame = LseWsAction::Unsubscribe { symbol: &symbol }.to_json();
                            if self.send(frame).await {
                                self.unanswered.push_back(Unanswered::Unsubscribe(symbol));
                            }
                        }
                    }
                    Some(HandlerCommand::Disconnect) | None => break,
                },
                raw = self.raw_rx.recv() => match raw {
                    Some((epoch, message)) => self.receive(epoch, message).await,
                    None => break,
                },
            }
        }
        if let Some(client) = self.client.take() {
            client.disconnect().await;
        }
    }

    async fn receive(&mut self, epoch: u64, message: Message) {
        let Message::Text(text) = message else {
            return;
        };
        // A replacement's frames can come before the transport's
        // `RECONNECTED`, its welcome among them: whichever comes first
        // starts the new connection.
        if epoch > self.epoch {
            self.reconnected(epoch);
        }
        if &text[..] == RECONNECTED.as_bytes() || epoch < self.epoch {
            // Its connection started, or replaced.
            return;
        }
        self.last_frame = Some(get_atomic_clock_realtime().get_time_ns());
        let name = self.socket.name();
        let frame = match serde_json::from_slice::<LseWsFrame>(&text) {
            Ok(frame) => frame,
            Err(e) => {
                log::warn!(
                    "a frame of LSE's {name} not read ({} bytes): {e}",
                    text.len()
                );
                return;
            }
        };
        match frame {
            LseWsFrame::Welcome => {
                let frame = LseWsAction::Auth {
                    api_key: self.credential.api_key(),
                }
                .to_json();
                self.send(frame).await;
            }
            LseWsFrame::Authenticated(authenticated) => self.authenticated(authenticated).await,
            LseWsFrame::Subscribed { symbol } => {
                self.unanswered.pop_front();
                self.subscriptions.confirm_subscribe(&symbol);
            }
            LseWsFrame::Unsubscribed { symbol } => {
                self.unanswered.pop_front();
                self.subscriptions.confirm_unsubscribe(&symbol);
            }
            LseWsFrame::Tick(tick) => self.pass(LseWsMessage::Tick(tick)),
            LseWsFrame::Pong | LseWsFrame::Symbols => {}
            LseWsFrame::Error(refusal) => self.refused(refusal),
        }
    }

    /// The key is authenticated on the connection: the first time, the
    /// client is connected; after a reconnect, the gap is passed on. Then
    /// every subscription is sent.
    async fn authenticated(&mut self, authenticated: LseWsAuthenticated) {
        self.authenticated = true;
        if let Some(waiting) = self.authenticating.take() {
            let _ = waiting.send(Ok(authenticated));
        }
        if let Some(last) = self.dropped.take() {
            self.pass(LseWsMessage::Reconnected {
                last,
                at: get_atomic_clock_realtime().get_time_ns(),
            });
        }
        for symbol in self.subscriptions.reset_after_reconnect() {
            self.send_subscribe(symbol).await;
        }
    }

    /// LSE refused the key, before it was authenticated, or else the
    /// oldest subscription or unsubscription not answered yet.
    fn refused(&mut self, refusal: LseWsRefusal) {
        let name = self.socket.name();
        if !self.authenticated {
            match self.authenticating.take() {
                Some(waiting) => {
                    let _ = waiting.send(Err(refusal));
                }
                None => log::error!("LSE's {name} refused the key: {refusal}"),
            }
            return;
        }
        match self.unanswered.pop_front() {
            Some(Unanswered::Subscribe(symbol)) => {
                // Kept for the next connection.
                self.subscriptions.mark_failure(&symbol);
                self.pass(LseWsMessage::Refused { symbol, refusal });
            }
            Some(Unanswered::Unsubscribe(symbol)) => {
                self.subscriptions.confirm_unsubscribe(&symbol);
                log::warn!("LSE's {name} refused the unsubscription of {symbol}: {refusal}");
            }
            None => log::error!("LSE's {name} refused: {refusal}"),
        }
    }

    /// The transport replaced the connection with that of `epoch`, a later
    /// one, which LSE welcomes: the key is authenticated on it again.
    fn reconnected(&mut self, epoch: u64) {
        log::warn!(
            "LSE's {} reconnected: authenticating the key again",
            self.socket.name()
        );
        self.epoch = epoch;
        self.authenticated = false;
        self.unanswered.clear();
        if self.dropped.is_none() {
            self.dropped = Some(self.last_frame);
        }
    }

    async fn send_subscribe(&mut self, symbol: String) {
        let frame = LseWsAction::Subscribe { symbol: &symbol }.to_json();
        if self.send(frame).await {
            self.unanswered.push_back(Unanswered::Subscribe(symbol));
        }
    }

    /// Sends `frame` on the connection, if it is still the transport's
    /// and live; whether it was sent. One not sent never is: the
    /// subscriptions wait in their state for the next connection, which
    /// LSE welcomes again. The frame is not logged: an auth holds the key.
    async fn send(&self, frame: String) -> bool {
        let Some(client) = self.client.as_ref().filter(|client| client.is_active()) else {
            return false;
        };
        match client
            .send_text_on_connection(frame, None, self.epoch)
            .await
        {
            Ok(()) => true,
            Err(e) => {
                log::debug!("a frame not sent to LSE's {}: {e}", self.socket.name());
                false
            }
        }
    }

    fn pass(&self, message: LseWsMessage) {
        // The receiver is gone once the client stopped listening.
        let _ = self.out.send(message);
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::string::secret::SecretString;
    use serde_json::{Value, json};

    use super::*;
    use crate::testing::{
        API_KEY, MockLseSocket, SocketAction, WS_INVALID_KEY, WS_LIMIT_REACHED, WS_TICKS_GOLD,
        WS_TOO_MANY_CONNECTIONS, data_ws_answer,
    };

    type Out = tokio::sync::mpsc::UnboundedReceiver<LseWsMessage>;

    fn credential() -> LseCredential {
        LseCredential::resolve(Some(SecretString::from(API_KEY)), |_| None).unwrap()
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
}
