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
//! from the contracts it carries (Nautilus' `SubscriptionState`, topics:
//! the names). The handler connects once it carries a contract, those it
//! was started with or the first subscribed, and its first subscription
//! names every contract subscribed by then; no subscription overtakes the
//! connection, each waiting in the handler's commands. The key of the
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
//! that brings nothing new. A contract the client does not carry yet is
//! carried on a new connection: the handler asks Nautilus' transport to
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

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::live::dst::time::{self, Duration};
use nautilus_live::task::{TaskJoinOutcome, TaskSlot, finish_task};
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

/// How long LSE may take to answer the subscription.
pub(crate) const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

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
pub(crate) enum ExchangeWsMessage {
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
pub(crate) type ExchangeOpening = tokio::sync::oneshot::Receiver<Result<(), String>>;

/// What the transport passes the handler: a frame of a connection, or the
/// close of one, with its code and reason if LSE gave them.
enum Inbound {
    Frame(u64, Message),
    Closed(Option<(u16, String)>),
}

/// A connection to an LSE exchange stream at one level, carrying the
/// contracts subscribed.
#[derive(Debug)]
pub(crate) struct ExchangeStreamClient {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
    /// The handler task, explicitly singular: `disconnect` joins it.
    handler: TaskSlot<()>,
}

/// Subscribes contracts on a connection: cloned to whoever subscribes.
#[derive(Debug, Clone)]
pub(crate) struct ExchangeSubscriber {
    exchange: LseExchange,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
}

impl ExchangeStreamClient {
    /// Starts the client of `exchange`'s stream at `url` with the key of
    /// `credential`, carrying the contracts `symbols`, by their names at
    /// LSE, at `level`, and passing their records and gaps to `out`: its
    /// handler connects once it carries a contract, `symbols` or the first
    /// subscribed (`ExchangeSubscriber::subscribe`), and subscribes every
    /// contract it carries by then. The opening returned gets LSE's answer.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport's config is refused or the handler
    /// does not start.
    pub(crate) fn start(
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
        for symbol in symbols {
            subscriptions.mark_subscribe(symbol);
        }
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
            subscriptions,
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
    /// `credential`, subscribing the contracts `symbols`, by their names at
    /// LSE, at `level`, and passing their records and gaps to `out`.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails (a key LSE does not take is
    /// refused with HTTP 401), or if LSE refuses the subscription, with
    /// its close's code and reason, or does not answer it within
    /// `OPEN_TIMEOUT`.
    pub(crate) async fn connect(
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
    pub(crate) fn subscriber(&self) -> ExchangeSubscriber {
        ExchangeSubscriber {
            exchange: self.exchange,
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    /// Whether the handler has stopped, on a close a new connection would
    /// not lift, such as LSE's refusal of the subscription: no contract is
    /// carried on it any more.
    pub(crate) fn is_closed(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// Closes the connection, joining the handler within
    /// `DISCONNECT_TIMEOUT`, then aborting it.
    pub(crate) async fn disconnect(mut self) {
        // The handler is gone once it has stopped.
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        let exchange = self.exchange;
        match finish_task(&mut self.handler, DISCONNECT_TIMEOUT, ABORT_TIMEOUT).await {
            None | Some(TaskJoinOutcome::Completed(())) => {}
            Some(TaskJoinOutcome::Aborted) => {
                log::warn!("the handler of LSE's {exchange} stream did not stop in time: aborted");
            }
            Some(TaskJoinOutcome::Failed(e)) => {
                log::error!("the handler of LSE's {exchange} stream failed: {e}");
            }
            Some(TaskJoinOutcome::Incomplete) => {
                log::error!("the handler of LSE's {exchange} stream did not stop after its abort");
            }
        }
    }
}

impl ExchangeSubscriber {
    /// Has the connection carry the contract `symbol`, its name at LSE: if
    /// it carries it already and `reopen`, the new subscriber is passed on
    /// (`ExchangeWsMessage::Reopen`); if not, the connection's first
    /// subscription names it, if not sent yet, or a new connection carries
    /// it, opening it.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is disconnected, or stopped on a
    /// close a new connection would not lift (see `is_closed`).
    pub(crate) fn subscribe(&self, symbol: &str, reopen: bool) -> anyhow::Result<()> {
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
    pub(crate) fn unsubscribe(&self, symbol: &str) {
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

    /// Connects once a contract is carried, and sends the subscription of
    /// the contracts carried by then, those subscribed while it connected
    /// among them. Whether it did: not if disconnected first, nor if the
    /// transport does not connect, which the opening is told.
    async fn open(&mut self) -> bool {
        while self.subscriptions.is_empty() {
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
        let symbols = self.subscriptions.all_topics();
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

    /// Carries `symbol`: if carried already and `reopen`, the new
    /// subscriber is passed on; if not, a new connection carries it.
    fn subscribe(&mut self, symbol: String, reopen: bool) {
        if self.subscriptions.try_mark_subscribe(&symbol) {
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
            let mut symbols = self.subscriptions.all_topics();
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
    /// still the transport's and live. One not sent never is: the next
    /// connection sends its own.
    async fn send_subscription(&self, mut symbols: Vec<String>) {
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

#[cfg(test)]
mod tests {
    use nautilus_common::testing::wait_until_async;
    use nautilus_core::string::secret::SecretString;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        common::credential::LseProduct,
        testing::{
            CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7, CME_GCZ6_L2_DOCUMENTED,
            CME_GCZ6_L3_DOCUMENTED, CME_LEVEL_REQUIRED, CME_SYMBOL_UNAVAILABLE, MockExchangeStream,
            StreamAction, dbn_frames, frames, sent,
        },
    };

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
        let peer =
            MockExchangeStream::cme(move |_, _| vec![StreamAction::Close(code, reason.clone())]);

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

    /// GCZ6 opened, then GCG7 asked: a second connection carries both, and
    /// GCZ6's opening there brings nothing new.
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

    /// GCZ6 and GCG7 carried, then GCG7 no longer, as a roll leaves its old
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

    /// The peer drops the first connection after ESZ6's record 555; the
    /// second replays LSE's documented session from its opening, of which
    /// only the trade 556 is new.
    #[tokio::test]
    async fn a_dropped_connection_subscribes_again_and_passes_on_only_what_is_new() {
        let peer = MockExchangeStream::cme(|connection, _| {
            let mut frames = sent(dbn_frames(CME_ESZ6_DOCUMENTED));
            if connection == 0 {
                frames.truncate(5);
                frames.push(StreamAction::Close(1001, "going away".to_string()));
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
    /// and the transport reconnects, the new connection's opening coming.
    #[tokio::test]
    async fn a_client_whose_first_subscription_is_closed_for_now_carries_it_from_its_next_connection()
     {
        let peer = MockExchangeStream::cme(|connection, _| {
            if connection == 0 {
                vec![StreamAction::Close(1013, "GCZ6 not ready".to_string())]
            } else {
                sent(frames(CME_GCZ6_GCG7)[..4].to_vec())
            }
        });
        let (out_tx, mut out) = tokio::sync::mpsc::unbounded_channel();
        let (_client, opening) = ExchangeStreamClient::start(
            LseExchange::Cme,
            peer.url(),
            &credential(CME_API_KEY),
            ExchangeLevel::Mbp1,
            &["GCZ6".to_string()],
            out_tx,
        )
        .unwrap();

        let refused = opening.await.unwrap().unwrap_err();

        assert!(refused.contains("1013 GCZ6 not ready"), "{refused}");
        assert_eq!(record(&next(&mut out).await), ("GCZ6", 1794, true, b'M'));
    }
}
