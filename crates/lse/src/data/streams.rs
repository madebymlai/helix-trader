//! The data client's live data: the sockets of the instruments it streams,
//! the quotes of `LSEDGE` series and the trades of `OPRA` contracts, and
//! LSE's CME stream of the `GLBX` contracts it streams, their quotes,
//! trades, book depth and book deltas (see `cme`).
//!
//! `open` connects to the socket each instrument streams on (see
//! `LseSymbol::stream` and `OpraContract::stream`), one connection a socket
//! whatever the key allows, and fails, naming them, unless the socket lists
//! every series' symbol to the key and allows all its subscriptions on that
//! connection, as LSE stated when it authenticated the key
//! (`max_subscriptions`): LSE accepts a subscription to a symbol it does not
//! list and sends nothing, and refuses one beyond its limit
//! (`LIMIT_REACHED`) only once asked, after the node started. An option
//! contract streams on data-ws in its underlying's chain, every listed
//! contract of the underlying in one subscription, which LSE refuses for an
//! underlying with no listed options (`INVALID_UNDERLYING`); a socket lists
//! no contract. A quotes subscription reads the series' precision from the
//! cache, then subscribes its symbol; each tick of it becomes a quote (see
//! `parse_quote`). A trades subscription reads the contract's precision
//! from the cache, then subscribes its underlying's chain if no other
//! contract of it is subscribed; each tick of a contract subscribed becomes
//! a trade (see `parse_trade`), the chain's others are dropped, and the
//! chain ends with its last contract's subscription. LSE's refusal of a
//! subscription is logged as an error, a reconnect's gap as a warning.

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
        consts::{LSE_API_KEY, LseSocket, OPRA_VENUE},
        credential::LseCredential,
        symbol::{LseInstrument, OpraContract},
    },
    config::LseDataClientConfig,
    http::{listing::CmeListings, parse::TradeIds},
    websocket::{
        client::{KEEPALIVE_SECS, LseWebSocketClient, LseWsMessage},
        parse::{parse_quote, parse_trade},
    },
};

/// How long the tick tasks get to finish at a close, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// An instrument whose ticks are subscribed: its id and precisions.
#[derive(Debug, Clone, Copy)]
struct Subscribed {
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
}

/// The series whose quotes are subscribed, by their socket and their
/// symbol there in capitals: read on each tick, written on each
/// subscription.
type QuotedBySymbol = Arc<AtomicMap<(LseSocket, String), Subscribed>>;

/// The option contracts whose trades are subscribed, by their name on
/// data-ws in capitals: read on each tick, written on each subscription.
type TradedByName = Arc<AtomicMap<String, Subscribed>>;

/// The sockets of the instruments the client streams.
#[derive(Debug)]
pub(super) struct LseStreams {
    /// The LSE key, which the sockets take; none if no instrument is
    /// streamed.
    credential: Option<LseCredential>,
    /// Each series streamed, with its socket and its symbol there.
    streamed: Vec<(InstrumentId, LseSocket, String)>,
    /// Each option contract streamed, on data-ws in its underlying's chain.
    options: Vec<(InstrumentId, OpraContract)>,
    /// The sockets of the instruments streamed, in the order first named,
    /// each with its address.
    urls: Vec<(LseSocket, String)>,
    sockets: AHashMap<LseSocket, LseWebSocketClient>,
    quoted: QuotedBySymbol,
    traded: TradedByName,
    /// How many contracts of each underlying's chain, by its root, have
    /// their trades subscribed: the chain is subscribed while any has.
    chains: AHashMap<String, usize>,
    /// The tasks turning each socket's ticks into quotes and trades.
    tasks: TaskGroup,
    /// The `GLBX` contracts, on LSE's CME stream.
    cme: CmeStreams,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl LseStreams {
    /// The streams of the instruments `config` streams: `LSEDGE` series and
    /// `OPRA` contracts on the key of `credential`, the LSE key, and `GLBX`
    /// contracts on `cme`, the CME key, looked up in `listings`; none open.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if an instrument is neither an `LSEDGE`
    /// series, an `OPRA` contract nor a `GLBX` contract, or is a series of a
    /// market streamed on no socket here, and naming the variable of the
    /// key if an instrument is streamed with no key of its product.
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
        let (mut streamed, mut options, mut contracts) = (Vec::new(), Vec::new(), Vec::new());
        for &id in &config.stream_ids {
            match LseInstrument::from_instrument_id(id, year)? {
                LseInstrument::Series(series) => {
                    let (socket, symbol) =
                        series.stream().with_context(|| format!("streaming {id}"))?;
                    streamed.push((id, socket, symbol));
                }
                LseInstrument::Opra(contract) => options.push((id, contract)),
                LseInstrument::Cme(contract) => contracts.push(contract),
            }
        }
        let cme = CmeStreams::new(
            contracts,
            cme,
            listings,
            config.cme_stream_url().to_owned(),
            data_sender.clone(),
            clock,
        )?;
        if credential.is_none() && !(streamed.is_empty() && options.is_empty()) {
            bail!(
                "streaming {} needs the LSE key: set {LSE_API_KEY}, or give the config's",
                streamed
                    .iter()
                    .map(|(id, ..)| id)
                    .chain(options.iter().map(|(id, _)| id))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        let sockets = streamed
            .iter()
            .map(|&(_, socket, _)| socket)
            .chain(options.iter().map(|(_, contract)| contract.stream().0));
        let mut urls: Vec<(LseSocket, String)> = Vec::new();
        for socket in sockets {
            if !urls.iter().any(|(of, _)| *of == socket) {
                urls.push((socket, config.ws_url(socket).to_owned()));
            }
        }
        Ok(Self {
            credential,
            streamed,
            options,
            urls,
            sockets: AHashMap::new(),
            quoted: Arc::default(),
            traded: Arc::default(),
            chains: AHashMap::new(),
            tasks: TaskGroup::new(),
            cme,
            data_sender,
            clock,
        })
    }

    /// Connects to the socket of each instrument streamed and checks the
    /// socket lists every series to the key and allows the subscriptions of
    /// them all on one connection, a symbol each series and a chain each
    /// underlying of the contracts; starts turning their ticks into quotes
    /// and trades. Then opens the CME stream of the contracts streamed (see
    /// `CmeStreams::open`). Nothing to do if none is streamed.
    ///
    /// # Errors
    ///
    /// Returns an error if a connection fails or LSE refuses the key (see
    /// `LseWebSocketClient::connect`), naming the series a socket does not
    /// list, or naming the limit if the subscriptions of a socket exceed
    /// it, or as `CmeStreams::open`; then no socket is left open.
    pub(super) async fn open(&mut self) -> anyhow::Result<()> {
        if !self.tasks.is_open() {
            self.close().await?;
            self.tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("reopening the LSE tick tasks: {e}"))?;
        }
        for (socket, url) in self.urls.clone() {
            if let Err(e) = self.open_socket(socket, &url).await {
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

    async fn open_socket(&mut self, socket: LseSocket, url: &str) -> anyhow::Result<()> {
        let name = socket.name();
        let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
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
        let mut chains: Vec<&str> = self
            .options
            .iter()
            .filter(|(_, contract)| contract.stream().0 == socket)
            .map(|(_, contract)| contract.root())
            .collect();
        chains.sort_unstable();
        chains.dedup();
        let subscriptions = streamed.len() + chains.len();
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
            && subscriptions > max
        {
            Err(anyhow::anyhow!(
                "LSE's {name} allows {max} symbols on a connection of the key \
                 (max_subscriptions), an underlying's chain of options taking one, and one \
                 connection streams them all: {subscriptions} are streamed, {} symbols and {} \
                 chains",
                streamed.len(),
                chains.len()
            ))
        } else {
            Ok(())
        };
        if let Err(e) = checked {
            client.disconnect().await;
            return Err(e);
        }
        self.sockets.insert(socket, client);
        self.spawn_ticks(socket, out)
    }

    /// Turns the ticks `out` passes on into quotes of the series and trades
    /// of the option contracts subscribed, and logs refusals and gaps.
    fn spawn_ticks(
        &self,
        socket: LseSocket,
        mut out: tokio::sync::mpsc::UnboundedReceiver<LseWsMessage>,
    ) -> anyhow::Result<()> {
        let name = socket.name();
        let quoted = Arc::clone(&self.quoted);
        let traded = Arc::clone(&self.traded);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancelled = self.tasks.cancellation_token();
        self.tasks
            .spawn(async move {
                // The ids of each contract's trades, in the order they come.
                let mut trade_ids: AHashMap<InstrumentId, TradeIds> = AHashMap::new();
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
                            let symbol = tick.symbol.to_ascii_uppercase();
                            let (of, parsed) =
                                if let Some(of) = quoted.get_cloned(&(socket, symbol.clone())) {
                                    let quote = parse_quote(
                                        &tick,
                                        of.instrument_id,
                                        of.price_precision,
                                        of.size_precision,
                                        clock.get_time_ns(),
                                    );
                                    (of, quote.map(Data::Quote))
                                } else if let Some(of) = traded.get_cloned(&symbol) {
                                    let trade = parse_trade(
                                        &tick,
                                        of.instrument_id,
                                        of.price_precision,
                                        of.size_precision,
                                        trade_ids.entry(of.instrument_id).or_default(),
                                        clock.get_time_ns(),
                                    );
                                    (of, trade.map(Data::Trade))
                                } else {
                                    // Of a subscription just ended, or of a
                                    // contract of a chain not subscribed.
                                    continue;
                                };
                            match parsed {
                                Ok(data) => send(&sender, DataEvent::Data(data)),
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
                        LseWsMessage::OptionsRefused {
                            underlying,
                            refusal,
                        } => log::error!(
                            "LSE's {name} refused the chain of {underlying}: {refusal}; no \
                             trades of its contracts stream"
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
        &mut self,
        id: InstrumentId,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        if id.venue == Venue::GLBX() {
            return self.cme.subscribe(id, CmeKind::Quotes, cache);
        }
        let (socket, symbol) = self.streamed_as(id)?;
        let client = self.socket(socket)?;
        let quoted = defined(id, cache)?;
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
        self.socket(socket)?.unsubscribe(symbol)
    }

    /// Subscribes to the trades of `id`, at the precision of its definition
    /// in `cache`: a `GLBX` contract's on the CME stream (see
    /// `CmeStreams::subscribe`); an `OPRA` contract's on its socket,
    /// subscribing its underlying's chain unless a contract of it is
    /// subscribed already; nothing to do
    /// if its trades are.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is no `OPRA` contract, LSE's
    /// sockets streaming a series' ticks as quotes, if the client does not
    /// stream it, if its socket is not open, or if `cache` has no definition
    /// of it.
    pub(super) fn subscribe_trades(
        &mut self,
        id: InstrumentId,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        if id.venue == Venue::GLBX() {
            return self.cme.subscribe(id, CmeKind::Trades, cache);
        }
        let contract = self.contract(id)?;
        let (socket, name) = contract.stream();
        let root = contract.root().to_owned();
        self.socket(socket)?;
        if self.traded.contains_key(&name) {
            return Ok(());
        }
        let traded = defined(id, cache)?;
        self.traded.insert(name, traded);
        let subscribed = self.chains.entry(root.clone()).or_default();
        *subscribed += 1;
        if *subscribed == 1 {
            self.socket(socket)?.subscribe_options(&root)?;
        }
        Ok(())
    }

    /// Ends the trades of `id`: a `GLBX` contract's, which the CME stream
    /// carries on; an `OPRA` contract's, and its underlying's chain with the
    /// trades of its last contract subscribed, nothing to do if its trades
    /// are not subscribed.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is no `OPRA` contract, if the client
    /// does not stream it, or if its socket is not open.
    pub(super) fn unsubscribe_trades(&mut self, id: InstrumentId) -> anyhow::Result<()> {
        if id.venue == Venue::GLBX() {
            return self.cme.unsubscribe(id, CmeKind::Trades);
        }
        let contract = self.contract(id)?;
        let (socket, name) = contract.stream();
        let root = contract.root().to_owned();
        self.socket(socket)?;
        if !self.traded.contains_key(&name) {
            return Ok(());
        }
        self.traded.remove(&name);
        let subscribed = self
            .chains
            .get_mut(&root)
            .expect("a contract subscribed counts in its chain");
        *subscribed -= 1;
        if *subscribed == 0 {
            self.chains.remove(&root);
            self.socket(socket)?.unsubscribe_options(&root)?;
        }
        Ok(())
    }

    /// Subscribes to the book depth of `id`, a `GLBX` contract's ten levels
    /// a side on LSE's CME stream at Level 2, at the price precision of its
    /// definition in `cache` (see `CmeStreams::subscribe`).
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is no `GLBX` contract, or as
    /// `CmeStreams::subscribe`.
    pub(super) fn subscribe_book_depth(
        &mut self,
        id: InstrumentId,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        booked(id)?;
        self.cme.subscribe(id, CmeKind::Depth, cache)
    }

    /// Ends the book depth of `id`, which the CME stream carries on.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is no `GLBX` contract, or is not
    /// subscribed.
    pub(super) fn unsubscribe_book_depth(&self, id: InstrumentId) -> anyhow::Result<()> {
        booked(id)?;
        self.cme.unsubscribe(id, CmeKind::Depth)
    }

    /// Subscribes to the book deltas of `id`, a `GLBX` contract's order
    /// events on LSE's CME stream at Level 3, at the price precision of its
    /// definition in `cache` (see `CmeStreams::subscribe`).
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is no `GLBX` contract, or as
    /// `CmeStreams::subscribe`.
    pub(super) fn subscribe_book_deltas(
        &mut self,
        id: InstrumentId,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        booked(id)?;
        self.cme.subscribe(id, CmeKind::Deltas, cache)
    }

    /// Ends the book deltas of `id`, which the CME stream carries on.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is no `GLBX` contract, or is not
    /// subscribed.
    pub(super) fn unsubscribe_book_deltas(&self, id: InstrumentId) -> anyhow::Result<()> {
        booked(id)?;
        self.cme.unsubscribe(id, CmeKind::Deltas)
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

    /// The contract `id` is, an `OPRA` contract the client streams.
    fn contract(&self, id: InstrumentId) -> anyhow::Result<&OpraContract> {
        if id.venue != *OPRA_VENUE {
            bail!(
                "LSE streams no trades of {id}: its sockets' ticks of a series are quotes (a \
                 futures tick's price is its bid); it streams the trades of US option \
                 contracts, under OPRA, and of CME Globex contracts, under GLBX"
            );
        }
        match self.options.iter().find(|(of, _)| *of == id) {
            Some((_, contract)) => Ok(contract),
            None => bail!(
                "{id} is not among the instruments the LSE client streams (its config's \
                 stream_ids), whose sockets it opens"
            ),
        }
    }

    /// The client of `socket`, connected.
    fn socket(&self, socket: LseSocket) -> anyhow::Result<&LseWebSocketClient> {
        self.sockets
            .get(&socket)
            .with_context(|| format!("LSE's {} is not connected", socket.name()))
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
        self.traded.rcu(|traded| traded.clear());
        self.chains.clear();
        stopped.map_err(|e| anyhow::anyhow!("stopping the LSE tick tasks: {e}"))?;
        cme
    }
}

/// Checks that LSE streams a book of `id`: a `GLBX` contract's, LSE's
/// sockets streaming a series' ticks alone.
///
/// # Errors
///
/// Returns an error naming it if it is no `GLBX` contract.
fn booked(id: InstrumentId) -> anyhow::Result<()> {
    if id.venue != Venue::GLBX() {
        bail!(
            "LSE streams no book of {id}: its sockets stream a series' ticks alone; it streams \
             the book of CME Globex contracts, under GLBX, Level 2's ten levels a side as depth \
             and Level 3's orders as deltas"
        );
    }
    Ok(())
}

/// `id` with the precisions of its definition in `cache`.
///
/// # Errors
///
/// Returns an error naming it if `cache` has no definition of it: LSE
/// publishes none, and its prices are read at the definition's precision.
fn defined(id: InstrumentId, cache: &CacheView) -> anyhow::Result<Subscribed> {
    let cache = cache.borrow();
    let instrument = cache.instrument(&id).with_context(|| {
        format!(
            "{id} is not in the cache: LSE publishes no definitions, and its prices are read at \
             the definition's precision, so it must come from elsewhere, a catalog"
        )
    })?;
    Ok(Subscribed {
        instrument_id: id,
        price_precision: instrument.price_precision(),
        size_precision: instrument.size_precision(),
    })
}

/// Sends `event` to the node; a closed channel means the node is stopping.
fn send(sender: &EventSender<DataEvent>, event: DataEvent) {
    if let Err(e) = sender.send(event) {
        log::warn!("data event not sent: {e}");
    }
}
