//! The client of an LSE exchange stream, CME Globex's or Nasdaq TotalView's, one protocol (LSE's
//! WebSocket documentation, "The CME Group stream").
//!
//! An outer client and the handler task that owns Nautilus' `WebSocketClient` ("WebSocket client
//! patterns" in Nautilus' adapter guide).
//!
//! A connection carries one subscription of one level: the contracts subscribed (Nautilus'
//! `SubscriptionState`, topics: their names at LSE), sent first on each connection and never
//! replayed on another (`send_text_on_connection`). The handler connects once it has a contract to
//! name, and its first subscription names those it was started with and every contract subscribed
//! by then. A contract it was started with is named on the first connection alone, for LSE to check
//! it: a later connection names only the contracts subscribed, so an expired one never taken does
//! not have LSE refuse the whole subscription, and with none to name it sends none.
//!
//! The key goes in the `X-API-Key` header of the opening request, a reconnect's too.
//!
//! LSE answers with `hello`, then opens each contract (its last best bid and offer at Level 1, its
//! last ten levels at Level 2, no record at Level 3) and streams its records. It refuses a
//! subscription by closing with 1008 and a reason, such as `symbol unavailable: NGZ6` or `CME
//! Levels 2 and 3 need CME Standard or above`. `connect` returns once LSE says `hello`, or fails
//! with LSE's close.
//!
//! Records pass on with their sequence, openings marked. A sequence beyond the last but one is a
//! gap, passed on and left: the records between did not come, as when the stream drops what a slow
//! reader could not take. A record not beyond the last, or an opening that brings nothing new, is
//! passed over.
//!
//! A contract subscribed but not carried is carried on a new connection, which the handler requests
//! (`request_reconnect`): LSE takes no second subscription on a connection. A new subscriber of a
//! carried contract gets `Reopen`, for whoever keeps the latest record to open it again. A contract
//! no longer carried is left out of the next connection's subscription, LSE taking no
//! unsubscription.
//!
//! After a reconnect each contract opens again: an opening beyond the last record is passed on, and
//! the records between are a gap. A new `epoch` in `hello` is a reset of LSE's feed: sequences
//! start again, so each is forgotten, and what came between is a gap left as it is.
//!
//! A close a new connection would not lift, 1008 or 4001 (key replaced or revoked, or plan ended),
//! is not reconnected and stops the handler. Every other close is reconnected (LSE's documentation,
//! "Refusals and closes": 4003 the account's connections held, 1012 the feed's connection reset,
//! 1013 a contract not ready).

use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use nautilus_common::live::dst::time::{self, Duration};
use nautilus_live::task::TaskSlot;
use nautilus_network::{
    Message, RECONNECTED,
    websocket::{
        CloseHandler, EpochMessageHandler, SubscriptionState, WebSocketClient, WebSocketConfig,
    },
};

use super::messages::{
    ExchangeLevel, ExchangeRecord, ExchangeRecordFrame, ExchangeSubscription, ExchangeText,
};
use crate::{
    common::{consts::LseExchange, credential::LseCredential},
    websocket::{
        RECONNECT_BACKOFF_FACTOR, RECONNECT_DELAY_INITIAL_MS, RECONNECT_DELAY_MAX_MS,
        RECONNECT_JITTER_MS, finish_handler,
    },
};

/// How long LSE may take to answer the subscription.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// LSE pings every 15 s, and so does the client. A connection silent for three pings is replaced.
const PING_SECS: u64 = 15;

const TERMINAL: [u16; 2] = [1008, 4001];

/// A delimiter no contract's name holds, so a topic is one name.
const TOPIC_DELIMITER: char = ':';

const API_KEY_HEADER: &str = "X-API-Key";

/// What the client passes on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeWsMessage {
    /// A record of the contract `symbol`. An `opening` is LSE's last best bid and offer or ten
    /// levels, and carries no event of its own.
    Record {
        symbol: String,
        sequence: u64,
        record: ExchangeRecord,
        opening: bool,
    },
    /// The records of `symbol` from `from` to `to`, both included, did not come.
    Gap { symbol: String, from: u64, to: u64 },
    /// A new subscriber of the carried contract `symbol`: its latest record opens it for them.
    Reopen { symbol: String },
}

/// The answer to a connection's first subscription: `Ok` once LSE says `hello`, else why not.
pub type ExchangeOpening = tokio::sync::oneshot::Receiver<Result<(), String>>;

enum Inbound {
    Frame(u64, Message),
    Closed(Option<(u16, String)>),
}

/// A connection to an LSE exchange stream at one level, carrying the contracts subscribed.
#[derive(Debug)]
pub struct ExchangeStreamClient {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
    handler: TaskSlot<()>,
}

/// Subscribes contracts on a connection: cloned to whoever subscribes.
#[derive(Debug, Clone)]
pub struct ExchangeSubscriber {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
}

impl ExchangeStreamClient {
    /// Starts the client of `exchange`'s stream at `url` with the key of `credential`, at `level`,
    /// passing the records and gaps of its contracts to `out`.
    ///
    /// The handler connects once it has a contract to name, from `symbols` or the first subscribed.
    /// The opening returned gets LSE's answer.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport's config is refused or the handler does not start.
    pub fn start(
        exchange: LseExchange,
        url: &str,
        credential: &LseCredential,
        level: ExchangeLevel,
        symbols: &[String],
        out: tokio::sync::mpsc::UnboundedSender<ExchangeWsMessage>,
    ) -> anyhow::Result<(Self, ExchangeOpening)> {
        let (raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel::<Inbound>();
        let closes = raw_tx.clone();
        let messages: EpochMessageHandler = Arc::new(move |epoch, message| {
            // The receiver is gone only once the handler has stopped
            let _ = raw_tx.send(Inbound::Frame(epoch, message));
        });
        let closed: CloseHandler = Arc::new(move |frame| {
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
            .with_context(|| format!("the config of LSE's {exchange} stream"))?;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let subscriptions = SubscriptionState::new(TOPIC_DELIMITER);
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();
        let handler = ExchangeHandler {
            exchange,
            level,
            transport: Some(Transport {
                config,
                messages,
                closed,
            }),
            client: None,
            cmd_rx,
            raw_rx,
            out,
            opening: Some(opened_tx),
            first: symbols.to_vec(),
            subscriptions,
            carried: AHashSet::new(),
            epoch: 0,
            feed: None,
            sequences: AHashMap::new(),
            names: AHashMap::new(),
        };
        let mut task = TaskSlot::new();
        task.spawn(handler.run())
            .map_err(|e| anyhow::anyhow!("starting the handler of LSE's {exchange} stream: {e}"))?;
        Ok((
            Self {
                exchange,
                cmd_tx,
                handler: task,
            },
            opened_rx,
        ))
    }

    /// Connects as `start` does, the first subscription naming `symbols`, which LSE checks.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails (a key LSE does not take is refused with HTTP 401),
    /// or if LSE refuses the subscription, with its close's code and reason, or does not answer it
    /// within `OPEN_TIMEOUT`.
    pub async fn connect(
        exchange: LseExchange,
        url: &str,
        credential: &LseCredential,
        level: ExchangeLevel,
        symbols: &[String],
        out: tokio::sync::mpsc::UnboundedSender<ExchangeWsMessage>,
    ) -> anyhow::Result<Self> {
        let (connected, opening) = Self::start(exchange, url, credential, level, symbols, out)?;
        let refused = match time::timeout(OPEN_TIMEOUT, opening).await {
            Ok(Ok(Ok(()))) => return Ok(connected),
            Ok(Ok(Err(refused))) => refused,
            Ok(Err(_)) => {
                format!("LSE's {exchange} stream stopped before answering the subscription")
            }
            Err(_) => format!(
                "LSE's {exchange} stream did not answer the {level} subscription of {symbols:?} \
                 within {OPEN_TIMEOUT:?}"
            ),
        };
        connected.disconnect().await;
        anyhow::bail!(refused)
    }

    /// Returns who subscribes contracts on the connection.
    pub fn subscriber(&self) -> ExchangeSubscriber {
        ExchangeSubscriber {
            exchange: self.exchange,
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    /// Returns whether the handler stopped on a close a new connection would not lift.
    pub fn is_closed(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// Closes the connection, joining the handler within `DISCONNECT_TIMEOUT`, then aborting it.
    pub async fn disconnect(mut self) {
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        finish_handler(&mut self.handler, &format!("{} stream", self.exchange)).await;
    }
}

impl ExchangeSubscriber {
    /// Has the connection carry the contract `symbol`, its name at LSE, now and on every later
    /// connection. If it is carried already and `reopen`, the new subscriber is passed on
    /// (`ExchangeWsMessage::Reopen`).
    ///
    /// # Errors
    ///
    /// Returns an error if the client is disconnected, or stopped on a close a new connection would
    /// not lift (see `is_closed`).
    pub fn subscribe(&self, symbol: &str, reopen: bool) -> anyhow::Result<()> {
        self.cmd_tx
            .send(HandlerCommand::Subscribe {
                symbol: symbol.to_owned(),
                reopen,
            })
            .ok()
            .with_context(|| format!("the connection to LSE's {} stream is closed", self.exchange))
    }

    /// Has the connection carry `symbol` no longer: the next connection's subscription leaves it
    /// out. LSE takes no unsubscription, so this connection carries it on until replaced. A stopped
    /// handler carries nothing.
    pub fn unsubscribe(&self, symbol: &str) {
        let _ = self.cmd_tx.send(HandlerCommand::Unsubscribe {
            symbol: symbol.to_owned(),
        });
    }
}

enum HandlerCommand {
    Subscribe { symbol: String, reopen: bool },
    Unsubscribe { symbol: String },
    Disconnect,
}

struct Transport {
    config: WebSocketConfig,
    messages: EpochMessageHandler,
    closed: CloseHandler,
}

struct ExchangeHandler {
    exchange: LseExchange,
    level: ExchangeLevel,
    transport: Option<Transport>,
    client: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    out: tokio::sync::mpsc::UnboundedSender<ExchangeWsMessage>,
    opening: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    /// The contracts the first subscription names beside those subscribed, until it is sent.
    first: Vec<String>,
    subscriptions: SubscriptionState,
    carried: AHashSet<String>,
    /// The transport's connection the handler speaks on: 0, then each replacement's.
    epoch: u64,
    /// The feed's epoch, as the last `hello` gave it.
    feed: Option<u64>,
    sequences: AHashMap<String, u64>,
    names: AHashMap<u32, String>,
}

impl ExchangeHandler {
    async fn run(mut self) {
        if self.open().await {
            loop {
                tokio::select! {
                    biased;
                    command = self.cmd_rx.recv() => match command {
                        Some(HandlerCommand::Subscribe { symbol, reopen }) => {
                            self.subscribe(symbol, reopen);
                        }
                        Some(HandlerCommand::Unsubscribe { symbol }) => self.unsubscribe(&symbol),
                        Some(HandlerCommand::Disconnect) | None => break,
                    },
                    inbound = self.raw_rx.recv() => match inbound {
                        Some(Inbound::Frame(epoch, message)) => self.receive(epoch, message).await,
                        Some(Inbound::Closed(close)) => {
                            if !self.closed(close.as_ref()) {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
        }
        if let Some(client) = self.client.take() {
            client.disconnect().await;
        }
    }

    /// Connects once it has a contract to name and sends the first subscription.
    ///
    /// Returns whether it did, the opening told if the transport does not connect.
    async fn open(&mut self) -> bool {
        while self.first.is_empty() && self.subscriptions.is_empty() {
            match self.cmd_rx.recv().await {
                Some(HandlerCommand::Subscribe { symbol, .. }) => {
                    self.subscriptions.mark_subscribe(&symbol);
                }
                Some(HandlerCommand::Unsubscribe { symbol }) => self.unsubscribe(&symbol),
                Some(HandlerCommand::Disconnect) | None => return false,
            }
        }
        let Transport {
            config,
            messages,
            closed,
        } = self.transport.take().expect("the transport connects once");
        let connected = WebSocketClient::epoch_builder()
            .config(config)
            .epoch_handler(messages)
            .close_handler(closed)
            .connect()
            .await;
        match connected {
            Ok(client) => self.client = Some(client),
            Err(e) => {
                let failed = anyhow::Error::from(e)
                    .context(format!("connecting to LSE's {} stream", self.exchange));
                if let Some(opening) = self.opening.take() {
                    let _ = opening.send(Err(format!("{failed:#}")));
                }
                return false;
            }
        }
        while let Ok(command) = self.cmd_rx.try_recv() {
            match command {
                HandlerCommand::Subscribe { symbol, .. } => {
                    self.subscriptions.mark_subscribe(&symbol);
                }
                HandlerCommand::Unsubscribe { symbol } => self.unsubscribe(&symbol),
                HandlerCommand::Disconnect => return false,
            }
        }
        let mut symbols = std::mem::take(&mut self.first);
        for symbol in self.subscriptions.all_topics() {
            if !symbols.contains(&symbol) {
                symbols.push(symbol);
            }
        }
        self.send_subscription(symbols).await;
        true
    }

    async fn receive(&mut self, epoch: u64, message: Message) {
        if epoch < self.epoch {
            // Of a connection replaced
            return;
        }
        if epoch > self.epoch {
            self.reconnected(epoch).await;
        }
        match message {
            Message::Text(text) if &text[..] == RECONNECTED.as_bytes() => {}
            Message::Text(text) => match serde_json::from_slice::<ExchangeText>(&text) {
                Ok(text) => self.text(text),
                // A type the client does not read
                Err(e) => log::debug!(
                    "a frame of LSE's {} stream not read ({} bytes): {e}",
                    self.exchange,
                    text.len()
                ),
            },
            Message::Binary(bytes) => match ExchangeRecordFrame::parse(&bytes, self.level) {
                Ok(frame) => self.record(frame),
                Err(e) => log::warn!(
                    "a record of LSE's {} stream not read ({} bytes): {e:#}",
                    self.exchange,
                    bytes.len()
                ),
            },
            _ => {}
        }
    }

    fn text(&mut self, text: ExchangeText) {
        match text {
            ExchangeText::Hello { epoch } => {
                if let Some(feed) = self.feed
                    && feed != epoch
                {
                    log::warn!(
                        "LSE's {} feed was reset (epoch {feed}, now {epoch}): its sequences \
                         start again, and what came between is a gap left as it is",
                        self.exchange
                    );
                    self.sequences.clear();
                }
                self.feed = Some(epoch);
                if let Some(opening) = self.opening.take() {
                    let _ = opening.send(Ok(()));
                }
            }
            ExchangeText::SnapshotBegin {
                symbol,
                instrument_id,
                ..
            } => {
                self.names.insert(instrument_id, symbol);
            }
            ExchangeText::SnapshotEnd { symbol } => self.subscriptions.confirm_subscribe(&symbol),
        }
    }

    fn record(&mut self, frame: ExchangeRecordFrame) {
        let Some(symbol) = self.names.get(&frame.instrument_id).cloned() else {
            log::warn!(
                "a record of LSE's {} stream of a contract not opened ({}) passed over",
                self.exchange,
                frame.instrument_id
            );
            return;
        };
        if let Some(&last) = self.sequences.get(&symbol) {
            if frame.sequence <= last {
                if !frame.snapshot {
                    log::debug!(
                        "a record of {symbol} of LSE's {} stream at {}, not beyond {last}, \
                         passed over",
                        self.exchange,
                        frame.sequence
                    );
                }
                return;
            }

            // An opening stands for its own record, whose trade, if one, did not come
            let to = if frame.snapshot {
                frame.sequence
            } else {
                frame.sequence - 1
            };
            if to > last {
                let _ = self.out.send(ExchangeWsMessage::Gap {
                    symbol: symbol.clone(),
                    from: last + 1,
                    to,
                });
            }
        }
        self.sequences.insert(symbol.clone(), frame.sequence);
        let _ = self.out.send(ExchangeWsMessage::Record {
            symbol,
            sequence: frame.sequence,
            record: frame.record,
            opening: frame.snapshot,
        });
    }

    fn subscribe(&self, symbol: String, reopen: bool) {
        let newly = self.subscriptions.try_mark_subscribe(&symbol);
        if newly && self.carried.contains(&symbol) {
            // Named on the first connection before it was subscribed, so its opening comes there
            self.subscriptions.confirm_subscribe(&symbol);
        } else if newly {
            log::info!(
                "LSE's {} stream carries {symbol} from its next connection, one subscription a \
                 connection: reconnecting",
                self.exchange
            );
            if let Some(client) = &self.client {
                let _ = client.request_reconnect();
            }
            return;
        }
        if reopen {
            let _ = self.out.send(ExchangeWsMessage::Reopen { symbol });
        }
    }

    fn unsubscribe(&self, symbol: &str) {
        self.subscriptions.mark_unsubscribe(symbol);
        self.subscriptions.confirm_unsubscribe(symbol);
    }

    async fn reconnected(&mut self, epoch: u64) {
        log::warn!(
            "LSE's {} stream reconnected: subscribing again",
            self.exchange
        );
        self.epoch = epoch;
        let symbols = self.subscriptions.reset_after_reconnect();
        self.send_subscription(symbols).await;
    }

    /// A refusal before LSE answers the first subscription goes to the opening, else it is logged.
    ///
    /// Returns whether the handler goes on: not after a close a new connection would not lift.
    fn closed(&mut self, close: Option<&(u16, String)>) -> bool {
        let described = close.map_or_else(
            || "no close frame".to_string(),
            |(code, reason)| format!("{code} {reason}"),
        );
        let terminal = close.is_some_and(|(code, _)| TERMINAL.contains(code));
        if let Some(opening) = self.opening.take() {
            let mut symbols: Vec<&String> = self.carried.iter().collect();
            symbols.sort();
            let _ = opening.send(Err(format!(
                "LSE's {} stream refused the {} subscription of {symbols:?}: {described}",
                self.exchange, self.level
            )));
        } else if terminal {
            log::error!(
                "LSE's {} stream closed the {} connection ({described}): not reconnecting, so \
                 none of its contracts streams at that level",
                self.exchange,
                self.level
            );
        } else {
            log::warn!(
                "LSE's {} stream closed the {} connection ({described}): reconnecting",
                self.exchange,
                self.level
            );
        }
        !terminal
    }

    /// Sends the subscription of `symbols` if the connection is live, and takes them as what it
    /// carries. One not sent never is: the next connection sends its own.
    async fn send_subscription(&mut self, mut symbols: Vec<String>) {
        self.carried = symbols.iter().cloned().collect();
        if symbols.is_empty() {
            log::info!(
                "LSE's {} stream carries no contract at {}: no subscription until one is \
                 subscribed",
                self.exchange,
                self.level
            );
            return;
        }
        let Some(client) = self.client.as_ref().filter(|client| client.is_active()) else {
            return;
        };
        symbols.sort();
        let frame = ExchangeSubscription::new(self.level, &symbols).to_json();
        if let Err(e) = client
            .send_text_on_connection(frame, None, self.epoch)
            .await
        {
            log::debug!(
                "the subscription not sent to LSE's {} stream: {e}",
                self.exchange
            );
        }
    }
}
