//! The client of LSE's CME Globex stream: an outer client and the handler
//! task that owns Nautilus' `WebSocketClient`, as Nautilus' adapter guide
//! lays one out ("WebSocket client patterns").
//!
//! The CME key goes in the `X-API-Key` header of the opening request, a
//! reconnect's too. A connection carries one subscription, the contracts
//! by their names at LSE at Level 1, which the client sends first on each
//! connection (`send_text_on_connection`, never replayed on another), from
//! the contracts it carries (Nautilus' `SubscriptionState`, topics: the
//! names). LSE answers with `hello`, then opens each contract, its last
//! best bid and offer, and streams its records; it refuses a subscription
//! by closing the connection with 1008 and a reason, such as `symbol
//! unavailable: NGZ6`. `connect` returns once LSE says `hello`, or fails
//! with LSE's close.
//!
//! The handler passes each contract's records on with their sequence, the
//! opening ones marked, and keeps each contract's sequence. A sequence
//! beyond the last but one is a gap, passed on and left: the records
//! between did not come, as when the stream drops what a slow reader could
//! not take; a record not beyond the last is passed over, as is an opening
//! that brings nothing new. A contract the client does not carry yet is
//! carried on a new connection: the handler asks Nautilus' transport to
//! reconnect (`request_reconnect`), and the new connection's subscription
//! names it too, LSE taking no second subscription on a connection. A new
//! subscriber of a contract carried already is passed on in its place
//! among the records, for whoever keeps the contract's latest record to
//! open it again.
//!
//! Nautilus' transport reconnects a dropped connection with backoff. On the
//! new one each contract opens again: an opening beyond the contract's
//! last record is passed on, and the records between are a gap. A new
//! `epoch` in `hello` is a reset of LSE's feed: its sequences start again,
//! so each contract's is forgotten, and what came between is a gap left as
//! it is. A close for a refusal a new connection would not lift, 1008 (the
//! subscription refused) or 4001 (the key replaced or revoked, or its plan
//! ended), is not reconnected; every other is (LSE's documentation,
//! "Refusals and closes": 4003 the account's connections held, 1012 the
//! feed's connection reset, 1013 a contract not ready).

use std::sync::Arc;

use ahash::AHashMap;
use anyhow::Context;
use dbn::Mbp1Msg;
use nautilus_common::live::dst::time::{self, Duration};
use nautilus_live::task::{TaskJoinOutcome, TaskSlot, finish_task};
use nautilus_network::{
    Message, RECONNECTED,
    websocket::{
        CloseHandler, EpochMessageHandler, SubscriptionState, WebSocketClient, WebSocketConfig,
    },
};

use super::messages::{CmeRecordFrame, CmeSubscription, CmeText};
use crate::common::credential::LseCredential;

/// How long LSE may take to answer the subscription.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the client pings: as often as LSE pings it, every 15 s; a
/// connection silent for three pings is replaced.
const PING_SECS: u64 = 15;

/// The transport's backoff between reconnect attempts.
const RECONNECT_DELAY_INITIAL_MS: u64 = 1_000;
const RECONNECT_DELAY_MAX_MS: u64 = 30_000;
const RECONNECT_BACKOFF_FACTOR: f64 = 2.0;
const RECONNECT_JITTER_MS: u64 = 500;

/// How long `disconnect` waits for the handler to close the transport,
/// then for its abort.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const ABORT_TIMEOUT: Duration = Duration::from_secs(2);

/// The closes of a refusal a new connection would not lift: the
/// subscription refused (1008), the key replaced or revoked, or its plan
/// ended (4001).
const TERMINAL: [u16; 2] = [1008, 4001];

/// The delimiter of the subscription state's topics, which no contract's
/// name holds: a topic is one name.
const TOPIC_DELIMITER: char = ':';

/// The header the key goes in.
const API_KEY_HEADER: &str = "X-API-Key";

/// What the client passes on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CmeWsMessage {
    /// A Level 1 record of the contract `symbol`, at its `sequence`;
    /// `opening` if it opens the contract, LSE's last best bid and offer,
    /// and so carries no trade of its own.
    Record {
        symbol: String,
        sequence: u64,
        record: Mbp1Msg,
        opening: bool,
    },
    /// The records of `symbol` from `from` to `to`, both included, did not
    /// come.
    Gap { symbol: String, from: u64, to: u64 },
    /// A new subscriber of the contract `symbol`, carried already, after
    /// the records passed on before it: the contract's latest record opens
    /// it for the subscriber.
    Reopen { symbol: String },
}

/// What the transport passes the handler: a frame of a connection, or the
/// close of one, with its code and reason if LSE gave them.
enum Inbound {
    Frame(u64, Message),
    Closed(Option<(u16, String)>),
}

/// A connection to LSE's CME stream, carrying the contracts subscribed.
#[derive(Debug)]
pub(crate) struct CmeStreamClient {
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
    /// The handler task, explicitly singular: `disconnect` joins it.
    handler: TaskSlot<()>,
}

/// Subscribes contracts on a connection: cloned to whoever subscribes.
#[derive(Debug, Clone)]
pub(crate) struct CmeSubscriber {
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
}

impl CmeStreamClient {
    /// Connects to the stream at `url` with the CME key of `credential`,
    /// subscribing the contracts `symbols`, by their names at LSE, at Level
    /// 1, and passing their records and gaps to `out`.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails (a key LSE does not take is
    /// refused with HTTP 401), or if LSE refuses the subscription, with
    /// its close's code and reason, or does not answer it within
    /// `OPEN_TIMEOUT`.
    pub(crate) async fn connect(
        url: &str,
        credential: &LseCredential,
        symbols: &[String],
        out: tokio::sync::mpsc::UnboundedSender<CmeWsMessage>,
    ) -> anyhow::Result<Self> {
        let (raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel::<Inbound>();
        let closes = raw_tx.clone();
        let message_handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            // The receiver is gone only once the handler has stopped.
            let _ = raw_tx.send(Inbound::Frame(epoch, message));
        });
        let close_handler: CloseHandler = Arc::new(move |frame| {
            let close = frame.map(|frame| (frame.code, frame.reason));
            let reconnect = close
                .as_ref()
                .is_none_or(|(code, _)| !TERMINAL.contains(code));
            let _ = closes.send(Inbound::Closed(close));
            reconnect
        });
        let config = WebSocketConfig::builder()
            .url(url.to_owned())
            .headers(vec![(
                API_KEY_HEADER.to_owned(),
                credential.api_key().to_owned(),
            )])
            .heartbeat_interval_secs(PING_SECS)
            .reconnect_delay_initial_ms(RECONNECT_DELAY_INITIAL_MS)
            .reconnect_delay_max_ms(RECONNECT_DELAY_MAX_MS)
            .reconnect_backoff_factor(RECONNECT_BACKOFF_FACTOR)
            .reconnect_jitter_ms(RECONNECT_JITTER_MS)
            .build()
            .context("the config of LSE's CME stream")?;
        let client = WebSocketClient::epoch_builder()
            .config(config)
            .epoch_handler(message_handler)
            .close_handler(close_handler)
            .connect()
            .await
            .context("connecting to LSE's CME stream")?;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        cmd_tx
            .send(HandlerCommand::SetClient(client))
            .ok()
            .context("the handler of LSE's CME stream is gone")?;
        let subscriptions = SubscriptionState::new(TOPIC_DELIMITER);
        for symbol in symbols {
            subscriptions.mark_subscribe(symbol);
        }
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let handler = CmeHandler {
            client: None,
            cmd_rx,
            raw_rx,
            out,
            opening: Some(opened_tx),
            subscriptions,
            epoch: 0,
            feed: None,
            sequences: AHashMap::new(),
            names: AHashMap::new(),
        };
        let mut task = TaskSlot::new();
        task.spawn(handler.run())
            .map_err(|e| anyhow::anyhow!("starting the handler of LSE's CME stream: {e}"))?;
        let connected = Self {
            cmd_tx,
            handler: task,
        };
        let refused = match time::timeout(OPEN_TIMEOUT, opened_rx).await {
            Ok(Ok(Ok(()))) => return Ok(connected),
            Ok(Ok(Err(close))) => {
                format!("LSE's CME stream refused the subscription of {symbols:?}: {close}")
            }
            Ok(Err(_)) => "LSE's CME stream stopped before answering the subscription".to_string(),
            Err(_) => format!(
                "LSE's CME stream did not answer the subscription of {symbols:?} within \
                 {OPEN_TIMEOUT:?}"
            ),
        };
        connected.disconnect().await;
        anyhow::bail!(refused)
    }

    /// Who subscribes contracts on the connection.
    pub(crate) fn subscriber(&self) -> CmeSubscriber {
        CmeSubscriber {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    /// Closes the connection, joining the handler within
    /// `DISCONNECT_TIMEOUT`, then aborting it.
    pub(crate) async fn disconnect(mut self) {
        // The handler is gone once it has stopped.
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        match finish_task(&mut self.handler, DISCONNECT_TIMEOUT, ABORT_TIMEOUT).await {
            None | Some(TaskJoinOutcome::Completed(())) => {}
            Some(TaskJoinOutcome::Aborted) => {
                log::warn!("the handler of LSE's CME stream did not stop in time: aborted");
            }
            Some(TaskJoinOutcome::Failed(e)) => {
                log::error!("the handler of LSE's CME stream failed: {e}");
            }
            Some(TaskJoinOutcome::Incomplete) => {
                log::error!("the handler of LSE's CME stream did not stop after its abort");
            }
        }
    }
}

impl CmeSubscriber {
    /// Has the connection carry the contract `symbol`, its name at LSE: if
    /// it carries it already and `reopen`, the new subscriber is passed on
    /// (`CmeWsMessage::Reopen`); if not, a new connection carries it,
    /// opening it.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is disconnected.
    pub(crate) fn subscribe(&self, symbol: &str, reopen: bool) -> anyhow::Result<()> {
        self.cmd_tx
            .send(HandlerCommand::Subscribe {
                symbol: symbol.to_owned(),
                reopen,
            })
            .ok()
            .context("the connection to LSE's CME stream is closed")
    }
}

/// Commands from the outer client to the handler.
enum HandlerCommand {
    /// The connected client; the first command, queued before the command
    /// sender is shared, so no subscription can overtake it.
    SetClient(WebSocketClient),
    Subscribe {
        symbol: String,
        reopen: bool,
    },
    Disconnect,
}

/// The handler task: the one owner of the `WebSocketClient`.
struct CmeHandler {
    client: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    out: tokio::sync::mpsc::UnboundedSender<CmeWsMessage>,
    /// Where the first `hello`, or the refusal of the first subscription,
    /// goes, until it comes.
    opening: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    /// The contracts carried, by their names at LSE.
    subscriptions: SubscriptionState,
    /// The transport's connection the handler speaks on: 0, then each
    /// replacement's.
    epoch: u64,
    /// The feed's epoch, as the last `hello` gave it.
    feed: Option<u64>,
    /// The sequence of each contract's last record, by its name.
    sequences: AHashMap<String, u64>,
    /// The contracts' names by the stream's number for them.
    names: AHashMap<u32, String>,
}

impl CmeHandler {
    /// Runs until disconnected or the outer client is dropped.
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                command = self.cmd_rx.recv() => match command {
                    Some(HandlerCommand::SetClient(client)) => {
                        self.client = Some(client);
                        let symbols = self.subscriptions.all_topics();
                        self.send_subscription(symbols).await;
                    }
                    Some(HandlerCommand::Subscribe { symbol, reopen }) => {
                        self.subscribe(symbol, reopen);
                    }
                    Some(HandlerCommand::Disconnect) | None => break,
                },
                inbound = self.raw_rx.recv() => match inbound {
                    Some(Inbound::Frame(epoch, message)) => self.receive(epoch, message).await,
                    Some(Inbound::Closed(close)) => self.closed(close),
                    None => break,
                },
            }
        }
        if let Some(client) = self.client.take() {
            client.disconnect().await;
        }
    }

    async fn receive(&mut self, epoch: u64, message: Message) {
        if epoch < self.epoch {
            // Of a connection replaced.
            return;
        }
        if epoch > self.epoch {
            self.reconnected(epoch).await;
        }
        match message {
            Message::Text(text) if &text[..] == RECONNECTED.as_bytes() => {}
            Message::Text(text) => match serde_json::from_slice::<CmeText>(&text) {
                Ok(text) => self.text(text),
                // A type the client does not read.
                Err(e) => log::debug!(
                    "a frame of LSE's CME stream not read ({} bytes): {e}",
                    text.len()
                ),
            },
            Message::Binary(bytes) => match CmeRecordFrame::parse(&bytes) {
                Ok(frame) => self.record(frame),
                Err(e) => log::warn!(
                    "a record of LSE's CME stream not read ({} bytes): {e:#}",
                    bytes.len()
                ),
            },
            _ => {}
        }
    }

    fn text(&mut self, text: CmeText) {
        match text {
            CmeText::Hello { epoch } => {
                if let Some(feed) = self.feed
                    && feed != epoch
                {
                    log::warn!(
                        "LSE's CME feed was reset (epoch {feed}, now {epoch}): its sequences \
                         start again, and what came between is a gap left as it is"
                    );
                    self.sequences.clear();
                }
                self.feed = Some(epoch);
                if let Some(opening) = self.opening.take() {
                    let _ = opening.send(Ok(()));
                }
            }
            CmeText::SnapshotBegin {
                symbol,
                instrument_id,
                ..
            } => {
                self.names.insert(instrument_id, symbol);
            }
            CmeText::SnapshotEnd { symbol } => self.subscriptions.confirm_subscribe(&symbol),
        }
    }

    /// Passes `frame`'s record on, if it is beyond its contract's last,
    /// and the gap before it, if one.
    fn record(&mut self, frame: CmeRecordFrame) {
        let Some(symbol) = self.names.get(&frame.instrument_id).cloned() else {
            log::warn!(
                "a record of LSE's CME stream of a contract not opened ({}) passed over",
                frame.instrument_id
            );
            return;
        };
        if let Some(&last) = self.sequences.get(&symbol) {
            if frame.sequence <= last {
                if !frame.snapshot {
                    log::debug!(
                        "a record of {symbol} of LSE's CME stream at {}, not beyond {last}, \
                         passed over",
                        frame.sequence
                    );
                }
                return;
            }
            // An opening stands for its own record, whose trade, if one,
            // did not come.
            let to = if frame.snapshot {
                frame.sequence
            } else {
                frame.sequence - 1
            };
            if to > last {
                let _ = self.out.send(CmeWsMessage::Gap {
                    symbol: symbol.clone(),
                    from: last + 1,
                    to,
                });
            }
        }
        self.sequences.insert(symbol.clone(), frame.sequence);
        let _ = self.out.send(CmeWsMessage::Record {
            symbol,
            sequence: frame.sequence,
            record: frame.record,
            opening: frame.snapshot,
        });
    }

    /// Carries `symbol`: if carried already and `reopen`, the new
    /// subscriber is passed on; if not, a new connection carries it.
    fn subscribe(&mut self, symbol: String, reopen: bool) {
        if self.subscriptions.try_mark_subscribe(&symbol) {
            log::info!(
                "LSE's CME stream carries {symbol} from its next connection, one subscription a \
                 connection: reconnecting"
            );
            if let Some(client) = &self.client {
                let _ = client.request_reconnect();
            }
            return;
        }
        if reopen {
            let _ = self.out.send(CmeWsMessage::Reopen { symbol });
        }
    }

    /// The transport replaced the connection with that of `epoch`, a later
    /// one: the subscription is sent on it.
    async fn reconnected(&mut self, epoch: u64) {
        log::warn!("LSE's CME stream reconnected: subscribing again");
        self.epoch = epoch;
        let symbols = self.subscriptions.reset_after_reconnect();
        self.send_subscription(symbols).await;
    }

    /// The transport's connection closed: before LSE answered the first
    /// subscription, its refusal fails `connect`; else it is logged.
    fn closed(&mut self, close: Option<(u16, String)>) {
        let described = close.as_ref().map_or_else(
            || "no close frame".to_string(),
            |(code, reason)| format!("{code} {reason}"),
        );
        if let Some(opening) = self.opening.take() {
            let _ = opening.send(Err(described));
            return;
        }
        match close {
            Some((code, _)) if TERMINAL.contains(&code) => log::error!(
                "LSE's CME stream closed the connection ({described}): not reconnecting, so no \
                 CME data streams"
            ),
            _ => log::warn!("LSE's CME stream closed the connection ({described}): reconnecting"),
        }
    }

    /// Sends the subscription of `symbols` on the connection, if it is
    /// still the transport's and live. One not sent never is: the next
    /// connection sends its own.
    async fn send_subscription(&self, mut symbols: Vec<String>) {
        let Some(client) = self.client.as_ref().filter(|client| client.is_active()) else {
            return;
        };
        symbols.sort();
        let frame = CmeSubscription::level_1(&symbols).to_json();
        if let Err(e) = client
            .send_text_on_connection(frame, None, self.epoch)
            .await
        {
            log::debug!("the subscription not sent to LSE's CME stream: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::string::secret::SecretString;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        common::credential::LseProduct,
        testing::{
            CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7, CME_SYMBOL_UNAVAILABLE, CmeAction,
            MockCmeStream, dbn_frames, frames, sent,
        },
    };

    type Out = tokio::sync::mpsc::UnboundedReceiver<CmeWsMessage>;

    fn credential(key: &str) -> LseCredential {
        LseCredential::resolve(LseProduct::Cme, Some(SecretString::from(key)), |_| None).unwrap()
    }

    async fn connect(
        peer: &MockCmeStream,
        symbols: &[&str],
    ) -> anyhow::Result<(CmeStreamClient, Out)> {
        let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
        let symbols: Vec<String> = symbols.iter().map(ToString::to_string).collect();
        let client =
            CmeStreamClient::connect(peer.url(), &credential(CME_API_KEY), &symbols, out_tx)
                .await?;
        Ok((client, out))
    }

    async fn next(out: &mut Out) -> CmeWsMessage {
        tokio::time::timeout(std::time::Duration::from_secs(10), out.recv())
            .await
            .expect("nothing passed on within 10 s")
            .expect("the client stopped")
    }

    /// A record passed on: its contract, sequence, whether it opens the
    /// contract, and its action.
    fn record(message: &CmeWsMessage) -> (&str, u64, bool, u8) {
        let CmeWsMessage::Record {
            symbol,
            sequence,
            record,
            opening,
        } = message
        else {
            panic!("not a record: {message:?}")
        };
        (symbol, *sequence, *opening, record.action as u8)
    }

    fn subscription(symbols: &[&str]) -> Value {
        json!({"op": "subscribe", "symbols": symbols, "level": 1, "encoding": "dbn"})
    }

    /// The captured opening of GCZ6 and GCG7 at CME's Friday close.
    #[tokio::test]
    async fn the_key_goes_in_the_header_and_each_contract_opens_with_its_best_bid_and_offer() {
        let peer = MockCmeStream::start(|_, _| sent(frames(CME_GCZ6_GCG7)));

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
        let peer = MockCmeStream::start(|_, _| sent(dbn_frames(CME_ESZ6_DOCUMENTED)));

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
        let peer = MockCmeStream::start(|_, _| {
            let mut frames = dbn_frames(CME_ESZ6_DOCUMENTED);
            frames.remove(4);
            sent(frames)
        });

        let (_client, mut out) = connect(&peer, &["ESZ6"]).await.unwrap();

        assert_eq!(record(&next(&mut out).await), ("ESZ6", 554, true, b'M'));
        assert_eq!(
            next(&mut out).await,
            CmeWsMessage::Gap {
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
        let peer = MockCmeStream::start(move |_, _| vec![CmeAction::Close(code, reason.clone())]);

        let err = connect(&peer, &["GCZ6", "NGZ6"])
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("1008 symbol unavailable: NGZ6"), "{err}");
    }

    /// LSE answers a key that is no CME Group key with HTTP 401.
    #[tokio::test]
    async fn a_key_lse_does_not_take_fails_the_connect_without_showing_the_key() {
        let peer = MockCmeStream::start(|_, _| sent(frames(CME_GCZ6_GCG7)));
        let (out_tx, _out) = tokio::sync::mpsc::unbounded_channel();
        let other = "lse-other-test-key-0123456789";

        let err = CmeStreamClient::connect(
            peer.url(),
            &credential(other),
            &["GCZ6".to_string()],
            out_tx,
        )
        .await
        .unwrap_err();

        let err = format!("{err:#}");
        assert!(err.contains("401"), "{err}");
        assert!(!err.contains(other), "{err}");
    }

    /// GCZ6 opened, then GCG7 asked: a second connection carries both, and
    /// GCZ6's opening there brings nothing new.
    #[tokio::test]
    async fn a_contract_not_carried_yet_is_carried_from_a_new_connection() {
        let peer = MockCmeStream::start(|_, subscription| {
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

    /// On the same connection, after the records passed on before it.
    #[tokio::test]
    async fn a_new_subscriber_of_a_contract_carried_is_passed_on_after_its_records() {
        let peer = MockCmeStream::start(|_, _| sent(dbn_frames(CME_ESZ6_DOCUMENTED)));
        let (client, mut out) = connect(&peer, &["ESZ6"]).await.unwrap();
        for _ in 0..3 {
            next(&mut out).await;
        }

        client.subscriber().subscribe("ESZ6", true).unwrap();

        assert_eq!(
            next(&mut out).await,
            CmeWsMessage::Reopen {
                symbol: "ESZ6".into()
            }
        );
        assert_eq!(peer.received().len(), 1);
    }

    /// The peer drops the first connection after ESZ6's record 555; the
    /// second replays LSE's documented session from its opening, of which
    /// only the trade 556 is new.
    #[tokio::test]
    async fn a_dropped_connection_subscribes_again_and_passes_on_only_what_is_new() {
        let peer = MockCmeStream::start(|connection, _| {
            let mut frames = sent(dbn_frames(CME_ESZ6_DOCUMENTED));
            if connection == 0 {
                frames.truncate(5);
                frames.push(CmeAction::Close(1001, "going away".to_string()));
            }
            frames
        });
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
        assert_eq!(
            peer.received(),
            [(0, subscription(&["ESZ6"])), (1, subscription(&["ESZ6"]))]
        );
    }
}
