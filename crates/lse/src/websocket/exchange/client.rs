//! The client of an LSE exchange stream for CME Globex or Nasdaq TotalView. Both streams use one
//! protocol (LSE's WebSocket documentation, "The CME Group stream").
//!
//! The client is an outer client plus the handler task that owns Nautilus' `WebSocketClient`
//! ("WebSocket client patterns" in Nautilus' adapter guide).
//!
//! A connection carries one subscription at one level. Nautilus' `SubscriptionState` holds the
//! subscribed contracts, and its topics are their names at LSE. The client sends the subscription
//! first on each connection and never replays it on another (`send_text_on_connection`).
//!
//! The handler names contracts like this:
//! - It connects once it has a contract to name.
//! - Its first subscription names the contracts it was started with and every contract subscribed
//!   by then.
//! - The first connection alone names a contract it was started with, for LSE to check it.
//! - A later connection names only the contracts subscribed. An expired contract that was never
//!   taken then does not make LSE refuse the whole subscription.
//! - With no contract to name, the handler sends no subscription.
//!
//! The key goes in the `X-API-Key` header of the opening request and of every reconnect.
//!
//! LSE answers with `hello`, then opens each contract and streams its records. An opening is the
//! last best bid and offer at Level 1, the last ten levels at Level 2, and no record at Level 3.
//! LSE refuses a subscription by closing with 1008 and a reason, such as `symbol unavailable:
//! NGZ6` or `CME Levels 2 and 3 need CME Standard or above`. `connect` returns once LSE says
//! `hello`, or fails with LSE's close.
//!
//! The client passes records on with their sequence and marks openings. It treats sequences like
//! this:
//! - A sequence beyond the last but one is a gap. The client passes it on and leaves it. The
//!   records between did not come, as when the stream drops what a slow reader could not take.
//! - A record not beyond the last is passed over.
//! - An opening that brings nothing new is passed over.
//!
//! LSE takes no second subscription and no unsubscription on a connection, so:
//! - The handler requests a new connection (`request_reconnect`) to carry a contract that is
//!   subscribed but not carried.
//! - A new subscriber of a carried contract gets `Reopen`, for whoever keeps the latest record to
//!   open it again.
//! - The next connection's subscription leaves out a contract no longer carried.
//!
//! After a reconnect each contract opens again. The client passes on an opening beyond the last
//! record, and the records between are a gap. A new `epoch` in `hello` means LSE reset the feed.
//! Sequences start again, so the client forgets each one, and what came between is a gap left as
//! it is.
//!
//! A close that a new connection would not lift stops the handler and is not reconnected. Those
//! closes are 1008 and 4001 (key replaced or revoked, or plan ended). Every other close is
//! reconnected (LSE's documentation, "Refusals and closes"):
//! - 4003: the account's connections are held.
//! - 1012: the feed's connection was reset.
//! - 1013: a contract is not ready.

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

/// The longest LSE may take to answer the subscription.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// LSE pings every 15 s, and so does the client. A connection silent for three pings is replaced.
const PING_SECS: u64 = 15;

const TERMINAL: [u16; 2] = [1008, 4001];

/// A delimiter that no contract's name contains, so a topic is one name.
const TOPIC_DELIMITER: char = ':';

const API_KEY_HEADER: &str = "X-API-Key";

/// What the client passes on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeWsMessage {
    /// A record of the contract `symbol`. An `opening` is LSE's last best bid and offer or last ten
    /// levels, and it carries no event of its own.
    Record {
        symbol: String,
        sequence: u64,
        record: ExchangeRecord,
        opening: bool,
    },
    /// The records of `symbol` from `from` to `to`, both included, did not come.
    Gap { symbol: String, from: u64, to: u64 },
    /// A new subscriber joined the carried contract `symbol`. Its latest record opens the contract
    /// for them.
    Reopen { symbol: String },
}

/// The answer to a connection's first subscription: `Ok` once LSE says `hello`, else the reason it
/// did not.
pub type ExchangeOpening = tokio::sync::oneshot::Receiver<Result<(), String>>;

enum Inbound {
    Frame(u64, Message),
    Closed(Option<(u16, String)>),
}

/// A connection to an LSE exchange stream at one level. It carries the subscribed contracts.
#[derive(Debug)]
pub struct ExchangeStreamClient {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
    handler: TaskSlot<()>,
}

/// Subscribes contracts on a connection. Each subscriber gets a clone.
#[derive(Debug, Clone)]
pub struct ExchangeSubscriber {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
}

impl ExchangeStreamClient {
    /// Starts the client for `exchange`'s stream at `url` and `level`, using the key in
    /// `credential`.
    ///
    /// The client passes the records and gaps of its contracts to `out`. The handler connects once
    /// it has a contract to name, from `symbols` or the first one subscribed. The opening returned
    /// gets LSE's answer.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport refuses its config or the handler does not start.
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

    /// Connects as `start` does, and the first subscription names `symbols` for LSE to check.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The connection fails. LSE refuses a key it does not take with HTTP 401.
    /// - LSE refuses the subscription, with the code and reason of its close.
    /// - LSE does not answer the subscription within `OPEN_TIMEOUT`.
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

    /// Returns the subscriber for contracts on the connection.
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

    /// Closes the connection. It joins the handler within `DISCONNECT_TIMEOUT`, then aborts it.
    pub async fn disconnect(mut self) {
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        finish_handler(&mut self.handler, &format!("{} stream", self.exchange)).await;
    }
}

impl ExchangeSubscriber {
    /// Has the connection carry the contract `symbol`, its name at LSE, now and on every later
    /// connection.
    ///
    /// If the contract is already carried and `reopen` is set, the client passes on the new
    /// subscriber (`ExchangeWsMessage::Reopen`).
    ///
    /// # Errors
    ///
    /// Returns an error if the client is disconnected, or stopped on a close that a new connection
    /// would not lift (see `is_closed`).
    pub fn subscribe(&self, symbol: &str, reopen: bool) -> anyhow::Result<()> {
        self.cmd_tx
            .send(HandlerCommand::Subscribe {
                symbol: symbol.to_owned(),
                reopen,
            })
            .ok()
            .with_context(|| format!("the connection to LSE's {} stream is closed", self.exchange))
    }

    /// Has the connection stop carrying `symbol`.
    ///
    /// The next connection's subscription leaves it out. LSE takes no unsubscription, so this
    /// connection carries it until replaced. A stopped handler carries nothing.
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
    /// The contracts the first subscription names in addition to those subscribed, until it is
    /// sent.
    first: Vec<String>,
    subscriptions: SubscriptionState,
    carried: AHashSet<String>,
    /// The number of the transport connection the handler speaks on: 0, then one more for each
    /// replacement.
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
    /// Returns whether it did. If the transport does not connect, it tells the opening.
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
            // The frame is from a replaced connection
            return;
        }

        if epoch > self.epoch {
            self.reconnected(epoch).await;
        }

        match message {
            Message::Text(text) if &text[..] == RECONNECTED.as_bytes() => {}
            Message::Text(text) => match serde_json::from_slice::<ExchangeText>(&text) {
                Ok(text) => self.text(text),
                // The client does not read this type
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

            // An opening stands for its own record, and that record's trade, if any, did not come
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
            // The first connection named it before it was subscribed, so its opening comes there
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

    /// Sends a refusal to the opening if LSE has not yet answered the first subscription, else
    /// logs it.
    ///
    /// Returns whether the handler goes on. It does not after a close that a new connection would
    /// not lift.
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

    /// Sends the subscription to `symbols` if the connection is live, and records them as carried.
    ///
    /// A subscription that is not sent is never sent later. The next connection sends its own.
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
