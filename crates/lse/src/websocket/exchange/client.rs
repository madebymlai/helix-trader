//! The client of an LSE exchange stream, CME Globex's or Nasdaq
//! TotalView's, which speak one protocol (LSE's WebSocket documentation,
//! "The CME Group stream": the Nasdaq stream "sends the same messages for
//! stocks"): an outer client and the handler task that owns Nautilus'
//! `WebSocketClient`, as Nautilus' adapter guide lays one out ("WebSocket
//! client patterns"). Its contracts are the stream's instruments, a CME
//! contract or a Nasdaq stock.
//!
//! A connection carries one subscription, of one level: the contracts by
//! their names at LSE at Level 1, 2 or 3, which the handler sends first on
//! each connection (`send_text_on_connection`, never replayed on another),
//! from the contracts subscribed (Nautilus' `SubscriptionState`, topics:
//! the names). The handler connects once it has a contract to name, those
//! it was started with or the first subscribed, and its first subscription
//! names them and every contract subscribed by then; no subscription
//! overtakes the connection, each waiting in the handler's commands. A
//! contract it was started with is named on the first connection alone,
//! for LSE to check it, unless subscribed: a later connection names only
//! the contracts subscribed, so that one never taken, once expired, does
//! not have LSE refuse the whole subscription, and a connection with none
//! to name sends no subscription. The key of the
//! exchange's plan goes in the `X-API-Key` header of the opening request, a
//! reconnect's too. LSE
//! answers with `hello`, then opens each contract, with its last best bid
//! and offer at Level 1, its last ten levels at Level 2 and no record at
//! Level 3, and streams its records; it refuses a subscription by closing
//! the connection with 1008 and a reason, such as `symbol unavailable:
//! NGZ6`, or `CME Levels 2 and 3 need CME Standard or above` for a level
//! the key's plan lacks. The answer to the first subscription, `hello` or
//! the refusal, goes to the connection's opening. `connect` returns once
//! LSE says `hello`, or fails with LSE's close.
//!
//! The handler passes each contract's records on with their sequence, the
//! opening ones marked, and keeps each contract's sequence. A sequence
//! beyond the last but one is a gap, passed on and left: the records
//! between did not come, as when the stream drops what a slow reader could
//! not take; a record not beyond the last is passed over, as is an opening
//! that brings nothing new. A contract subscribed that the connection does
//! not carry is carried on a new connection: the handler asks Nautilus'
//! transport to
//! reconnect (`request_reconnect`), and the new connection's subscription
//! names it too, LSE taking no second subscription on a connection. A new
//! subscriber of a contract carried already is passed on in its place
//! among the records, for whoever keeps the contract's latest record to
//! open it again. A contract carried no longer is left out of the next
//! connection's subscription, the current one carrying it on, LSE taking
//! no unsubscription: an expired contract named there would have LSE
//! refuse the whole subscription.
//!
//! Nautilus' transport reconnects a dropped connection with backoff. On the
//! new one each contract opens again: an opening beyond the contract's
//! last record is passed on, and the records between are a gap. A new
//! `epoch` in `hello` is a reset of LSE's feed: its sequences start again,
//! so each contract's is forgotten, and what came between is a gap left as
//! it is. A close for a refusal a new connection would not lift, 1008 (the
//! subscription refused) or 4001 (the key replaced or revoked, or its plan
//! ended), is not reconnected, and stops the handler, whose contracts a
//! new client carries; every other is reconnected (LSE's documentation,
//! "Refusals and closes": 4003 the account's connections held, 1012 the
//! feed's connection reset, 1013 a contract not ready).

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
use crate::common::{consts::LseExchange, credential::LseCredential};
use crate::websocket::{
    RECONNECT_BACKOFF_FACTOR, RECONNECT_DELAY_INITIAL_MS, RECONNECT_DELAY_MAX_MS,
    RECONNECT_JITTER_MS, finish_handler,
};

/// How long LSE may take to answer the subscription.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the client pings: as often as LSE pings it, every 15 s; a
/// connection silent for three pings is replaced.
const PING_SECS: u64 = 15;

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
pub enum ExchangeWsMessage {
    /// A record of the contract `symbol`, of the connection's level, at its
    /// `sequence`; `opening` if it opens the contract, LSE's last best bid
    /// and offer or ten levels, and so carries no event of its own.
    Record {
        symbol: String,
        sequence: u64,
        record: ExchangeRecord,
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

/// The answer to a connection's first subscription: `Ok` once LSE says
/// `hello`, or why it did not, LSE's refusal with its close's code and
/// reason among them.
pub type ExchangeOpening = tokio::sync::oneshot::Receiver<Result<(), String>>;

/// What the transport passes the handler: a frame of a connection, or the
/// close of one, with its code and reason if LSE gave them.
enum Inbound {
    Frame(u64, Message),
    Closed(Option<(u16, String)>),
}

/// A connection to an LSE exchange stream at one level, carrying the
/// contracts subscribed.
#[derive(Debug)]
pub struct ExchangeStreamClient {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
    /// The handler task, explicitly singular: `disconnect` joins it.
    handler: TaskSlot<()>,
}

/// Subscribes contracts on a connection: cloned to whoever subscribes.
#[derive(Debug, Clone)]
pub struct ExchangeSubscriber {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
}

impl ExchangeStreamClient {
    /// Starts the client of `exchange`'s stream at `url` with the key of
    /// `credential`, at `level`, passing the records and gaps of its
    /// contracts to `out`: its handler connects once it has a contract to
    /// name, `symbols`, by their names at LSE, or the first subscribed
    /// (`ExchangeSubscriber::subscribe`), and its first subscription names
    /// `symbols` and every contract subscribed by then. A later connection
    /// names only the contracts subscribed, `symbols` among them only if
    /// subscribed. The opening returned gets LSE's answer.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport's config is refused or the handler
    /// does not start.
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
            // The receiver is gone only once the handler has stopped.
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

    /// Connects to `exchange`'s stream at `url` with the key of
    /// `credential`, its first subscription naming the contracts `symbols`,
    /// by their names at LSE, at `level`, which LSE checks, and passing their
    /// records and gaps to `out`; a later connection names those subscribed
    /// alone (see `start`).
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails (a key LSE does not take is
    /// refused with HTTP 401), or if LSE refuses the subscription, with
    /// its close's code and reason, or does not answer it within
    /// `OPEN_TIMEOUT`.
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

    /// Who subscribes contracts on the connection.
    pub fn subscriber(&self) -> ExchangeSubscriber {
        ExchangeSubscriber {
            exchange: self.exchange,
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    /// Whether the handler has stopped, on a close a new connection would
    /// not lift, such as LSE's refusal of the subscription: no contract is
    /// carried on it any more.
    pub fn is_closed(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// Closes the connection, joining the handler within
    /// `DISCONNECT_TIMEOUT`, then aborting it.
    pub async fn disconnect(mut self) {
        // The handler is gone once it has stopped.
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        finish_handler(&mut self.handler, &format!("{} stream", self.exchange)).await;
    }
}

impl ExchangeSubscriber {
    /// Has the connection carry the contract `symbol`, its name at LSE, on
    /// this connection and every later one: if it carries it already and
    /// `reopen`, the new subscriber is passed on (`ExchangeWsMessage::Reopen`);
    /// if not, the connection's first subscription names it, if not sent
    /// yet, or a new connection carries it, opening it.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is disconnected, or stopped on a
    /// close a new connection would not lift (see `is_closed`).
    pub fn subscribe(&self, symbol: &str, reopen: bool) -> anyhow::Result<()> {
        self.cmd_tx
            .send(HandlerCommand::Subscribe {
                symbol: symbol.to_owned(),
                reopen,
            })
            .ok()
            .with_context(|| format!("the connection to LSE's {} stream is closed", self.exchange))
    }

    /// Has the connection carry the contract `symbol` no longer: the next
    /// connection's subscription leaves it out, LSE refusing a whole
    /// subscription that names a contract it does not carry, such as one
    /// expired. LSE takes no unsubscription on a connection, so this one
    /// carries it on until replaced. A stopped handler carries nothing.
    pub fn unsubscribe(&self, symbol: &str) {
        // The handler is gone once it has stopped.
        let _ = self.cmd_tx.send(HandlerCommand::Unsubscribe {
            symbol: symbol.to_owned(),
        });
    }
}

/// Commands from the outer client to the handler.
enum HandlerCommand {
    Subscribe { symbol: String, reopen: bool },
    Unsubscribe { symbol: String },
    Disconnect,
}

/// What the handler connects the transport with.
struct Transport {
    config: WebSocketConfig,
    messages: EpochMessageHandler,
    closed: CloseHandler,
}

/// The handler task: the one owner of the `WebSocketClient`.
struct ExchangeHandler {
    /// The exchange whose stream it is.
    exchange: LseExchange,
    /// The level the connection's subscription asks.
    level: ExchangeLevel,
    /// What the transport connects with, until it does.
    transport: Option<Transport>,
    client: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    out: tokio::sync::mpsc::UnboundedSender<ExchangeWsMessage>,
    /// Where the first `hello`, or the refusal of the first subscription,
    /// goes, until it comes.
    opening: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    /// The contracts the first connection's subscription names beside those
    /// subscribed, until it is sent.
    first: Vec<String>,
    /// The contracts subscribed, by their names at LSE, which every
    /// connection's subscription names.
    subscriptions: SubscriptionState,
    /// The contracts the connection's subscription named, which it carries.
    carried: AHashSet<String>,
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

impl ExchangeHandler {
    /// Runs until disconnected, the outer client dropped, or a close a new
    /// connection would not lift.
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
                            if !self.closed(close) {
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

    /// Connects once it has a contract to name, and sends the subscription
    /// of the contracts it was started with and those subscribed by then,
    /// while it connected among them. Whether it did: not if disconnected
    /// first, nor if the transport does not connect, which the opening is
    /// told.
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
            // Of a connection replaced.
            return;
        }
        if epoch > self.epoch {
            self.reconnected(epoch).await;
        }
        match message {
            Message::Text(text) if &text[..] == RECONNECTED.as_bytes() => {}
            Message::Text(text) => match serde_json::from_slice::<ExchangeText>(&text) {
                Ok(text) => self.text(text),
                // A type the client does not read.
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

    /// Passes `frame`'s record on, if it is beyond its contract's last,
    /// and the gap before it, if one.
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
            // An opening stands for its own record, whose trade, if one,
            // did not come.
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

    /// Carries `symbol` on every connection: if carried already and
    /// `reopen`, the new subscriber is passed on; if not, a new connection
    /// carries it.
    fn subscribe(&mut self, symbol: String, reopen: bool) {
        let newly = self.subscriptions.try_mark_subscribe(&symbol);
        if newly && self.carried.contains(&symbol) {
            // Named on this connection, the first, before it was subscribed:
            // its opening came, or comes, here.
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

    /// Carries `symbol` no longer: the subscription of the next connection
    /// leaves it out (Nautilus' `SubscriptionState`). LSE takes no
    /// unsubscription on a connection, so none is sent.
    fn unsubscribe(&mut self, symbol: &str) {
        self.subscriptions.mark_unsubscribe(symbol);
        self.subscriptions.confirm_unsubscribe(symbol);
    }

    /// The transport replaced the connection with that of `epoch`, a later
    /// one: the subscription is sent on it.
    async fn reconnected(&mut self, epoch: u64) {
        log::warn!(
            "LSE's {} stream reconnected: subscribing again",
            self.exchange
        );
        self.epoch = epoch;
        let symbols = self.subscriptions.reset_after_reconnect();
        self.send_subscription(symbols).await;
    }

    /// The transport's connection closed: before LSE answered the first
    /// subscription, its refusal goes to the opening; else it is logged.
    /// Whether the handler goes on: not after a close a new connection would
    /// not lift, which the transport does not reconnect.
    fn closed(&mut self, close: Option<(u16, String)>) -> bool {
        let described = close.as_ref().map_or_else(
            || "no close frame".to_string(),
            |(code, reason)| format!("{code} {reason}"),
        );
        let terminal = close
            .as_ref()
            .is_some_and(|(code, _)| TERMINAL.contains(code));
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

    /// Sends the subscription of `symbols` on the connection, if it is
    /// still the transport's and live, and takes them as what it carries;
    /// none if there are none, a later subscription carrying its contract
    /// on a new connection. One not sent never is: the next connection
    /// sends its own.
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
