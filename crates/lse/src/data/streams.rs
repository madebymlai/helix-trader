//! The data client's live data: the sockets of the `LSEDGE` instruments it
//! streams and their quotes, and LSE's CME stream of the `GLBX` contracts it
//! streams (see `cme`).
//!
//! `open` connects to the socket each instrument's market streams on (see
//! `LseSymbol::stream`), one connection a socket whatever the key allows,
//! and fails, naming them, unless the socket lists every instrument's
//! symbol to the key and allows them all on that connection, as LSE stated
//! when it authenticated the key (`max_subscriptions`): LSE accepts a
//! subscription to a symbol it does not list and sends nothing, and
//! refuses one beyond its limit (`LIMIT_REACHED`) only once asked, after
//! the node started. A quotes subscription reads the instrument's
//! precision from the cache, then subscribes its symbol; each tick of it
//! becomes a quote (see `parse_quote`). LSE's refusal of a subscription is
//! logged as an error, a reconnect's gap as a warning.

use std::sync::Arc;

use ahash::AHashMap;
use anyhow::{Context, bail};
use jiff::{Timestamp, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    live::{dst::time::Duration, sender::EventSender},
    messages::DataEvent,
};
use nautilus_core::{collections::AtomicMap, time::AtomicTime};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::Data,
    identifiers::{InstrumentId, Venue},
    instruments::Instrument,
};

use super::cme::{CmeAccess, CmeKind, CmeStreams};
use crate::{
    common::{
        consts::{LSE_API_KEY, LseSocket},
        credential::LseCredential,
        symbol::LseInstrument,
    },
    config::LseDataClientConfig,
    http::listing::CmeListings,
    websocket::{
        client::{KEEPALIVE_SECS, LseWebSocketClient, LseWsMessage},
        parse::parse_quote,
    },
};

/// How long the tick tasks get to finish at a close, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// An instrument whose quotes are subscribed: its id and precisions.
#[derive(Debug, Clone, Copy)]
struct Quoted {
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
}

/// The instruments whose quotes are subscribed, by their socket and their
/// symbol there in capitals: read on each tick, written on each
/// subscription.
type QuotedBySymbol = Arc<AtomicMap<(LseSocket, String), Quoted>>;

/// The sockets of the instruments the client streams.
#[derive(Debug)]
pub(super) struct LseStreams {
    /// The LSE key, which the sockets take; none if no instrument is
    /// streamed.
    credential: Option<LseCredential>,
    /// The address of every socket, if not LSE's own.
    url: Option<String>,
    /// Each instrument streamed, with its socket and its symbol there.
    streamed: Vec<(InstrumentId, LseSocket, String)>,
    sockets: AHashMap<LseSocket, LseWebSocketClient>,
    quoted: QuotedBySymbol,
    /// The tasks turning each socket's ticks into quotes.
    tasks: TaskGroup,
    /// The `GLBX` contracts, on LSE's CME stream.
    cme: CmeStreams,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl LseStreams {
    /// The streams of the instruments `config` streams: `LSEDGE` series on
    /// the key of `credential`, the LSE key, and `GLBX` contracts on `cme`,
    /// the CME key, looked up in `listings`; none open.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if an instrument is neither an `LSEDGE`
    /// series nor a `GLBX` contract, or is a series of a market streamed on
    /// no socket here, and naming the variable of the key if an instrument
    /// is streamed with no key of its product.
    pub(super) fn new(
        config: &LseDataClientConfig,
        credential: Option<LseCredential>,
        cme: Option<CmeAccess>,
        listings: Arc<CmeListings>,
        data_sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<Self> {
        let year = Timestamp::from(clock.get_time_ns())
            .to_zoned(TimeZone::UTC)
            .year();
        let (mut streamed, mut contracts) = (Vec::new(), Vec::new());
        for &id in &config.stream_ids {
            match LseInstrument::from_instrument_id(id, year)? {
                LseInstrument::Series(series) => {
                    let (socket, symbol) =
                        series.stream().with_context(|| format!("streaming {id}"))?;
                    streamed.push((id, socket, symbol));
                }
                LseInstrument::Cme(contract) => contracts.push(contract),
            }
        }
        let cme = CmeStreams::new(
            contracts,
            cme,
            listings,
            config.base_url_ws_cme.clone(),
            data_sender.clone(),
            clock,
        )?;
        if credential.is_none() && !streamed.is_empty() {
            bail!(
                "streaming {} needs the LSE key: set {LSE_API_KEY}, or give the config's",
                streamed
                    .iter()
                    .map(|(id, ..)| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        Ok(Self {
            credential,
            url: config.base_url_ws.clone(),
            streamed,
            sockets: AHashMap::new(),
            quoted: Arc::default(),
            tasks: TaskGroup::new(),
            cme,
            data_sender,
            clock,
        })
    }

    /// Connects to the socket of each instrument streamed and checks the
    /// socket lists them all to the key and allows them on one connection;
    /// starts turning their ticks into quotes. Then opens the CME stream of
    /// the contracts streamed (see `CmeStreams::open`). Nothing to do if
    /// none is streamed.
    ///
    /// # Errors
    ///
    /// Returns an error if a connection fails or LSE refuses the key (see
    /// `LseWebSocketClient::connect`), naming the instruments a socket does
    /// not list, or naming the limit if the instruments of a socket exceed
    /// it, or as `CmeStreams::open`; then no socket is left open.
    pub(super) async fn open(&mut self) -> anyhow::Result<()> {
        if !self.tasks.is_open() {
            self.close().await?;
            self.tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("reopening the LSE tick tasks: {e}"))?;
        }
        let mut sockets: Vec<LseSocket> = Vec::new();
        for (_, socket, _) in &self.streamed {
            if !sockets.contains(socket) {
                sockets.push(*socket);
            }
        }
        for socket in sockets {
            if let Err(e) = self.open_socket(socket).await {
                self.close().await?;
                return Err(e);
            }
        }
        if let Err(e) = self.cme.open().await {
            self.close().await?;
            return Err(e);
        }
        Ok(())
    }

    async fn open_socket(&mut self, socket: LseSocket) -> anyhow::Result<()> {
        let name = socket.name();
        let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
        let url = self.url.as_deref().unwrap_or(socket.url());
        let credential = self
            .credential
            .as_ref()
            .context("an instrument streamed with no LSE key")?;
        let client =
            LseWebSocketClient::connect(socket, url, credential, KEEPALIVE_SECS, out_tx).await?;
        let streamed: Vec<_> = self
            .streamed
            .iter()
            .filter(|(_, of, _)| *of == socket)
            .collect();
        let unlisted: Vec<String> = streamed
            .iter()
            .filter(|(_, _, symbol)| !client.lists(symbol))
            .map(|(id, _, symbol)| format!("{id} ({symbol})"))
            .collect();
        let checked = if !unlisted.is_empty() {
            Err(anyhow::anyhow!(
                "LSE's {name} does not list {} to the key: LSE would accept its subscription \
                 and send nothing",
                unlisted.join(", ")
            ))
        } else if let Some(max) = client.max_subscriptions()
            && streamed.len() > max
        {
            Err(anyhow::anyhow!(
                "LSE's {name} allows {max} symbols on a connection of the key \
                 (max_subscriptions), and one connection streams them all: {} are streamed",
                streamed.len()
            ))
        } else {
            Ok(())
        };
        if let Err(e) = checked {
            client.disconnect().await;
            return Err(e);
        }
        self.sockets.insert(socket, client);
        self.spawn_quotes(socket, out)
    }

    /// Turns the ticks `out` passes on into quotes of the instruments
    /// subscribed, and logs refusals and gaps.
    fn spawn_quotes(
        &self,
        socket: LseSocket,
        mut out: tokio::sync::mpsc::UnboundedReceiver<LseWsMessage>,
    ) -> anyhow::Result<()> {
        let name = socket.name();
        let quoted = Arc::clone(&self.quoted);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancelled = self.tasks.cancellation_token();
        self.tasks
            .spawn(async move {
                loop {
                    let message = tokio::select! {
                        biased;
                        () = cancelled.cancelled() => return,
                        message = out.recv() => match message {
                            Some(message) => message,
                            None => return,
                        },
                    };
                    match message {
                        LseWsMessage::Tick(tick) => {
                            let key = (socket, tick.symbol.to_ascii_uppercase());
                            let Some(of) = quoted.get_cloned(&key) else {
                                // Of a subscription just ended.
                                continue;
                            };
                            match parse_quote(
                                &tick,
                                of.instrument_id,
                                of.price_precision,
                                of.size_precision,
                                clock.get_time_ns(),
                            ) {
                                Ok(quote) => send(&sender, DataEvent::Data(Data::Quote(quote))),
                                Err(e) => log::warn!(
                                    "a tick of {} from LSE's {name} skipped: {e:#}",
                                    of.instrument_id
                                ),
                            }
                        }
                        LseWsMessage::Refused { symbol, refusal } => log::error!(
                            "LSE's {name} refused the subscription of {symbol}: {refusal}; \
                             no quotes of it stream"
                        ),
                        LseWsMessage::Reconnected { last, at } => log::warn!(
                            "LSE's {name} reconnected and resubscribed: nothing came from {} \
                             to {at}, a gap left as it is",
                            last.map_or_else(|| "the connection".to_string(), |l| l.to_string())
                        ),
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("starting the ticks of LSE's {name}: {e}"))
    }

    /// Subscribes to the quotes of `id`, at the precision of its definition
    /// in `cache`: a `GLBX` contract's on the CME stream (see
    /// `CmeStreams::subscribe`), an `LSEDGE` series' on its socket.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if the client does not stream it, if its
    /// socket is not open, or if `cache` has no definition of it: LSE
    /// publishes none, and its prices are read at the definition's
    /// precision.
    pub(super) fn subscribe_quotes(
        &self,
        id: InstrumentId,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        if id.venue == Venue::GLBX() {
            return self.cme.subscribe(id, CmeKind::Quotes, cache);
        }
        let (socket, symbol) = self.streamed_as(id)?;
        let client = self
            .sockets
            .get(&socket)
            .with_context(|| format!("LSE's {} is not connected", socket.name()))?;
        let quoted = {
            let cache = cache.borrow();
            let instrument = cache.instrument(&id).with_context(|| {
                format!(
                    "{id} is not in the cache: LSE publishes no definitions, and its prices are \
                     read at the definition's precision, so it must come from elsewhere, a \
                     catalog"
                )
            })?;
            Quoted {
                instrument_id: id,
                price_precision: instrument.price_precision(),
                size_precision: instrument.size_precision(),
            }
        };
        self.quoted
            .insert((socket, symbol.to_ascii_uppercase()), quoted);
        client.subscribe(symbol)
    }

    /// Ends the quotes of `id`.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if the client does not stream it, or if
    /// its socket is not open.
    pub(super) fn unsubscribe_quotes(&self, id: InstrumentId) -> anyhow::Result<()> {
        if id.venue == Venue::GLBX() {
            return self.cme.unsubscribe(id, CmeKind::Quotes);
        }
        let (socket, symbol) = self.streamed_as(id)?;
        self.quoted.remove(&(socket, symbol.to_ascii_uppercase()));
        self.sockets
            .get(&socket)
            .with_context(|| format!("LSE's {} is not connected", socket.name()))?
            .unsubscribe(symbol)
    }

    /// Subscribes to the trades of `id`, a `GLBX` contract, at the
    /// precision of its definition in `cache` (see `CmeStreams::subscribe`).
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is not a `GLBX` contract, LSE's
    /// sockets streaming quotes alone, or as `CmeStreams::subscribe`.
    pub(super) fn subscribe_trades(
        &self,
        id: InstrumentId,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        ensure_cme_trades(id)?;
        self.cme.subscribe(id, CmeKind::Trades, cache)
    }

    /// Ends the trades of `id`, a `GLBX` contract.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is not a `GLBX` contract or is not
    /// subscribed.
    pub(super) fn unsubscribe_trades(&self, id: InstrumentId) -> anyhow::Result<()> {
        ensure_cme_trades(id)?;
        self.cme.unsubscribe(id, CmeKind::Trades)
    }

    /// The socket of `id` and its symbol there.
    fn streamed_as(&self, id: InstrumentId) -> anyhow::Result<(LseSocket, &str)> {
        match self.streamed.iter().find(|(of, _, _)| *of == id) {
            Some((_, socket, symbol)) => Ok((*socket, symbol)),
            None => bail!(
                "{id} is not among the instruments the LSE client streams (its config's \
                 stream_ids), whose sockets it opens"
            ),
        }
    }

    /// Begins the shutdown of the tick and record tasks; `close` finishes
    /// it.
    pub(super) fn begin_shutdown(&self) {
        self.tasks.begin_shutdown();
        self.cme.begin_shutdown();
    }

    /// Stops the tick and record tasks and disconnects every socket and
    /// the CME stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the tasks do not stop.
    pub(super) async fn close(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        let (stopped, cme) = tokio::join!(
            self.tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
            self.cme.close()
        );
        for (_, client) in self.sockets.drain() {
            client.disconnect().await;
        }
        self.quoted.rcu(|quoted| quoted.clear());
        stopped.map_err(|e| anyhow::anyhow!("stopping the LSE tick tasks: {e}"))?;
        cme
    }
}

/// Checks `id` is a `GLBX` contract, whose trades LSE's CME stream carries.
fn ensure_cme_trades(id: InstrumentId) -> anyhow::Result<()> {
    if id.venue != Venue::GLBX() {
        bail!(
            "LSE streams no trades of {id}: its sockets' ticks are quotes; LSE's CME stream \
             carries the trades of GLBX contracts"
        );
    }
    Ok(())
}

/// Sends `event` to the node; a closed channel means the node is stopping.
fn send(sender: &EventSender<DataEvent>, event: DataEvent) {
    if let Err(e) = sender.send(event) {
        log::warn!("data event not sent: {e}");
    }
}
