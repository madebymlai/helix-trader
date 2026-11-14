//! The data client's exchange data: the `GLBX` contracts of CME Globex and
//! the `XNAS` stocks of Nasdaq TotalView it streams on their exchange's
//! stream, LSE's CME stream or its Nasdaq stream, which speak one protocol
//! (LSE's WebSocket documentation, "The CME Group stream": the Nasdaq stream
//! "sends the same messages for stocks"), at Level 1 as quotes and trades,
//! at Level 2 as book depth and at Level 3 as book deltas. Each exchange's
//! instruments, its contracts here, are streamed by an `ExchangeStreams` of
//! its own, on its plan's key.
//!
//! `open` names each contract the config streams as the stream takes it,
//! a CME contract by its listing's symbol, which it looks up in LSE's
//! catalogue with the CME key (see `http::listing`), a Nasdaq stock by its
//! ticker, then connects to the stream subscribing them all at Level 1 (see
//! `ExchangeStreamClient`), and fails with LSE's refusal of a lookup or of
//! the subscription. A subscription reads the contract's price precision
//! from the cache and has the stream carry the contract at the level of its
//! data: quotes and trades at Level 1, depth at Level 2 and deltas at Level
//! 3, each level on a connection of its own, LSE taking one level a
//! connection ("to change the contracts or the level, open another
//! connection"). The first depth or deltas subscription opens its level's
//! connection; LSE's refusal of it, such as of a level the key's plan
//! lacks, is logged as an error, and a later subscription of the level asks
//! LSE again on a new connection. A contract carried already opens for a
//! new quotes or depth subscriber with its latest best bid and offer or ten
//! levels, unless they went out already, and one not carried yet at the
//! level, a later contract of a rolled root or a stock the config does not
//! stream, is named, a CME contract looked up, and carried from the level's
//! next connection, which opens it. A contract whose last subscription at a
//! level ends is left out of the level's next connection, so that one
//! rolled out of and expired, which LSE would refuse (`symbol unavailable`,
//! closing the connection with 1008, not reconnected), does not stop the
//! level's data.
//!
//! Each record of a contract subscribed becomes Nautilus data through
//! Nautilus' Databento decoder, LSE's records being Databento's, at the
//! contract's price precision. At Level 1 (`decode_mbp1_msg`): its best
//! bid and offer a quote, with the sizes LSE sends, and its trade, if it is
//! one and not an opening, a trade with the aggressor side of LSE's side,
//! none for the `N` LSE's documentation gives every trade ("the feed does
//! not say which side took it"). At
//! Level 2 (`decode_mbp10_msg`): its ten levels a side, after the record's
//! change, an `OrderBookDepth` with each level's count of orders, an empty
//! level none, the contract opening with its last ten levels. At Level 3
//! (`decode_mbo_msg`): the order it adds (`A`), changes (`M`) or removes
//! (`C`), an `OrderBookDelta`; an event of no action (`N`) changes no order
//! and gives none. LSE's Level 3 opens with no book ("the stream carries
//! order events and no opening book"), so a contract's deltas start at its
//! first order event and none is invented. LSE flags the last event of each
//! exchange message (128, `F_LAST`): the deltas are gathered until it, then
//! sent as one `OrderBookDeltas`, as Nautilus' Databento client sends a
//! live event's; a flagged event of no delta ends its message too, its
//! `F_LAST` put on the message's last delta, as Nautilus' Databento decoder
//! does on history (`MboDeltaBuffer`), so that every event ends `F_LAST`
//! (Nautilus' adapter guide: "Each logical update ends with `F_LAST`"). The
//! decoder stamps each at the record's `ts_recv` (`ts_event`), when LSE
//! received it, as Nautilus' Databento client stamps its data, and
//! `ts_init` when the record came. It numbers a trade, a depth and a delta
//! by the record's own sequence, which LSE leaves 0; each takes the
//! stream's sequence of its record instead, one of its contract's and
//! level's alone. A gap of a contract's sequence is logged and left.

use std::{fmt, sync::Arc};

use ahash::AHashMap;
use anyhow::{Context, bail};
use dbn::{MboMsg, Mbp1Msg, Mbp10Msg};
use jiff::{Timestamp, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    live::{dst::time, sender::EventSender},
    messages::DataEvent,
};
use nautilus_core::{collections::AtomicMap, time::AtomicTime};
use nautilus_databento::decode::{decode_mbo_msg, decode_mbp1_msg, decode_mbp10_msg};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{Data, OrderBookDelta, OrderBookDeltas},
    enums::RecordFlag,
    identifiers::{InstrumentId, TradeId},
};

use super::{TASK_FORCE, TASK_GRACE, precisions, send};
use crate::{
    common::{
        consts::LseExchange,
        credential::LseCredential,
        symbol::{CmeContract, LseInstrument, NasdaqStock},
    },
    http::{client::LseHttpClient, listing::CmeListings},
    websocket::exchange::{
        client::{
            ExchangeOpening, ExchangeStreamClient, ExchangeSubscriber, ExchangeWsMessage,
            OPEN_TIMEOUT,
        },
        messages::{ExchangeLevel, ExchangeRecord},
    },
};

/// The data of a contract a subscription takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamKind {
    Quotes,
    Trades,
    Depth,
    Deltas,
}

impl StreamKind {
    /// The level of LSE's stream that carries the data.
    const fn level(self) -> ExchangeLevel {
        match self {
            Self::Quotes | Self::Trades => ExchangeLevel::Mbp1,
            Self::Depth => ExchangeLevel::Mbp10,
            Self::Deltas => ExchangeLevel::Mbo,
        }
    }

    /// Whether a new subscriber gets the contract's latest record: its best
    /// bid and offer, or its ten levels.
    const fn reopens(self) -> bool {
        matches!(self, Self::Quotes | Self::Depth)
    }
}

/// A contract subscribed: its id, its price precision and the data taken.
#[derive(Debug, Clone, Copy)]
struct Subscribed {
    instrument_id: InstrumentId,
    price_precision: u8,
    quotes: bool,
    trades: bool,
    depth: bool,
    deltas: bool,
}

impl Subscribed {
    /// Whether data of `level` is taken.
    const fn takes_at(&self, level: ExchangeLevel) -> bool {
        match level {
            ExchangeLevel::Mbp1 => self.quotes || self.trades,
            ExchangeLevel::Mbp10 => self.depth,
            ExchangeLevel::Mbo => self.deltas,
        }
    }

    /// Takes `kind`, or stops taking it.
    const fn set(&mut self, kind: StreamKind, taken: bool) {
        match kind {
            StreamKind::Quotes => self.quotes = taken,
            StreamKind::Trades => self.trades = taken,
            StreamKind::Depth => self.depth = taken,
            StreamKind::Deltas => self.deltas = taken,
        }
    }
}

/// The contracts subscribed, by their names on the stream: read on each
/// record, written on each subscription.
type SubscribedByName = Arc<AtomicMap<String, Subscribed>>;

/// A contract's latest record at Level 1 or 2, whose levels are its best
/// bid and offer or its ten levels, its sequence, and whether those went
/// out.
struct Latest {
    record: ExchangeRecord,
    sequence: u64,
    sent: bool,
}

/// The key of an exchange's plan, and LSE's API on it, which looks a CME
/// contract up.
#[derive(Debug)]
pub(super) struct ExchangeAccess {
    pub(super) credential: LseCredential,
    pub(super) http: Arc<LseHttpClient>,
}

/// A contract of an exchange's stream: a CME Globex contract, which the
/// stream names by its listing's symbol, or a Nasdaq TotalView stock, which
/// it names by its ticker.
#[derive(Debug, Clone)]
pub(super) enum ExchangeInstrument {
    Cme(CmeContract),
    Nasdaq(NasdaqStock),
}

impl ExchangeInstrument {
    /// The contract `id` stands for, a CME contract's two-digit year
    /// resolved against `reference_year`.
    ///
    /// # Errors
    ///
    /// Returns an error naming `id` if it is neither a `GLBX` contract nor
    /// an `XNAS` stock (see `LseInstrument::from_instrument_id`).
    fn of(id: InstrumentId, reference_year: i16) -> anyhow::Result<Self> {
        match LseInstrument::from_instrument_id(id, reference_year)? {
            LseInstrument::Cme(contract) => Ok(Self::Cme(contract)),
            LseInstrument::Nasdaq(stock) => Ok(Self::Nasdaq(stock)),
            LseInstrument::Series(_) | LseInstrument::Opra(_) => {
                bail!("{id} streams on no exchange stream of LSE's, of GLBX and XNAS")
            }
        }
    }

    fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::Cme(contract) => contract.instrument_id(),
            Self::Nasdaq(stock) => stock.instrument_id(),
        }
    }

    /// The contract's name on its stream, if known without LSE's catalogue:
    /// a stock's ticker. A CME contract's is its listing's (see `name`).
    fn ticker(&self) -> Option<&str> {
        match self {
            Self::Cme(_) => None,
            Self::Nasdaq(stock) => Some(stock.ticker()),
        }
    }

    /// The contract's name on its stream: a CME contract's listing's
    /// symbol, looked up in `listings` through `http` (LSE's WebSocket
    /// documentation, "Contracts": the names "the HTTP API downloads the
    /// same contracts by"), a stock's ticker.
    ///
    /// # Errors
    ///
    /// Returns an error naming a CME contract whose listing LSE delisted:
    /// its symbol names a contract every ten years, so the stream would
    /// carry the later one's records under its id (`ESZ16.GLBX`, delisted
    /// in 2016, is `ESZ6`, which names 2026's contract). Or as
    /// `CmeListings::listing`.
    async fn name(&self, listings: &CmeListings, http: &LseHttpClient) -> anyhow::Result<String> {
        match self {
            Self::Cme(contract) => {
                let listing = listings.listing(http, contract).await?;
                if listing.delisted_date.is_some() {
                    bail!(
                        "{contract} is LSE's listing {listing}, delisted: LSE's CME stream takes \
                         a contract by its listing's symbol, {}, which names a contract every ten \
                         years, so it would stream a later contract's records as {contract}'s; \
                         its history is asked by its listing's number",
                        listing.symbol
                    );
                }
                Ok(listing.symbol)
            }
            Self::Nasdaq(stock) => Ok(stock.ticker().to_owned()),
        }
    }
}

impl fmt::Display for ExchangeInstrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cme(contract) => fmt::Display::fmt(contract, f),
            Self::Nasdaq(stock) => fmt::Display::fmt(stock, f),
        }
    }
}

/// The contracts the client streams on an exchange's stream.
#[derive(Debug)]
pub(super) struct ExchangeStreams {
    exchange: LseExchange,
    /// The key of the exchange's plan and LSE's API on it; none if the
    /// client has no such key.
    access: Option<ExchangeAccess>,
    listings: Arc<CmeListings>,
    /// The stream's address.
    url: String,
    /// The contracts the config streams, which `open` subscribes at Level
    /// 1.
    streamed: Vec<ExchangeInstrument>,
    /// The connection of each level: Level 1's from `open`, the others'
    /// from their first subscription.
    clients: AHashMap<ExchangeLevel, ExchangeStreamClient>,
    /// The names on the stream of the contracts subscribed.
    names: Arc<AtomicMap<InstrumentId, String>>,
    subscribed: SubscribedByName,
    /// The tasks turning records into data, logging each connection's
    /// answer, and looking up the CME contracts subscribed after `open`.
    tasks: TaskGroup,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl ExchangeStreams {
    /// The streams of `streamed` on `exchange`'s stream at `url`, on
    /// `access`, the key of its plan, looking each CME contract up in
    /// `listings`; none open.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variable of the plan's key and the
    /// contracts if any is streamed with no such key.
    pub(super) fn new(
        exchange: LseExchange,
        streamed: Vec<ExchangeInstrument>,
        access: Option<ExchangeAccess>,
        listings: Arc<CmeListings>,
        url: String,
        data_sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<Self> {
        if access.is_none() && !streamed.is_empty() {
            let product = exchange.product();
            bail!(
                "streaming {} needs a {product} key: set {}, or give the config's",
                streamed
                    .iter()
                    .map(|instrument| instrument.instrument_id().to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                product.api_key_var()
            );
        }
        Ok(Self {
            exchange,
            access,
            listings,
            url,
            streamed,
            clients: AHashMap::new(),
            names: Arc::default(),
            subscribed: Arc::default(),
            tasks: TaskGroup::new(),
            data_sender,
            clock,
        })
    }

    /// Names the contracts streamed as the stream takes them, looking a CME
    /// contract up, and connects to the stream subscribing them all at
    /// Level 1; starts turning their records into data. Nothing to do if
    /// none is streamed.
    ///
    /// # Errors
    ///
    /// Returns an error if a lookup fails (LSE's refusal in the error), or
    /// as `ExchangeStreamClient::connect`, LSE's refusal of the subscription
    /// among them; then the stream is left closed.
    pub(super) async fn open(&mut self) -> anyhow::Result<()> {
        if self.streamed.is_empty() {
            return Ok(());
        }
        let exchange = self.exchange;
        if !self.tasks.is_open() {
            self.close().await?;
            self.tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("reopening the LSE {exchange} tasks: {e}"))?;
        }
        let access = self.access()?;
        let mut symbols = Vec::new();
        for instrument in &self.streamed {
            let name = instrument
                .name(&self.listings, &access.http)
                .await
                .with_context(|| format!("streaming {instrument}"))?;
            self.names.insert(instrument.instrument_id(), name.clone());
            symbols.push(name);
        }
        let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
        let client = ExchangeStreamClient::connect(
            exchange,
            &self.url,
            &access.credential,
            ExchangeLevel::Mbp1,
            &symbols,
            out_tx,
        )
        .await?;
        self.clients.insert(ExchangeLevel::Mbp1, client);
        if let Err(e) = self.spawn_records(ExchangeLevel::Mbp1, out) {
            self.close().await?;
            return Err(e);
        }
        Ok(())
    }

    /// The subscriber of the connection of `level`: its open one, or a new
    /// one carrying the contracts subscribed at the level, which connects
    /// with its first contract, and whose records and answer are taken up
    /// here.
    ///
    /// # Errors
    ///
    /// Returns an error if the client has no key of the exchange's plan, or
    /// a new connection's client or tasks do not start.
    fn subscriber(&mut self, level: ExchangeLevel) -> anyhow::Result<ExchangeSubscriber> {
        if let Some(client) = self.clients.get(&level)
            && !client.is_closed()
        {
            return Ok(client.subscriber());
        }
        let access = self.access()?;
        let carried: Vec<String> = self
            .subscribed
            .load()
            .iter()
            .filter(|(_, of)| of.takes_at(level))
            .map(|(symbol, _)| symbol.clone())
            .collect();
        let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
        let (client, opening) = ExchangeStreamClient::start(
            self.exchange,
            &self.url,
            &access.credential,
            level,
            &carried,
            out_tx,
        )?;
        let subscriber = client.subscriber();
        // A closed client's handler has stopped.
        self.clients.insert(level, client);
        self.spawn_records(level, out)?;
        self.spawn_opening(level, opening)?;
        Ok(subscriber)
    }

    /// The key of the exchange's plan and LSE's API on it.
    ///
    /// # Errors
    ///
    /// Returns an error naming the exchange if the client has no such key.
    fn access(&self) -> anyhow::Result<&ExchangeAccess> {
        self.access.as_ref().with_context(|| {
            format!(
                "{} contracts streamed with no {} key",
                self.exchange,
                self.exchange.product()
            )
        })
    }

    /// Logs LSE's answer to the first subscription of the connection of
    /// `level`, which `opening` gets: its refusal as an error.
    fn spawn_opening(&self, level: ExchangeLevel, opening: ExchangeOpening) -> anyhow::Result<()> {
        let exchange = self.exchange;
        self.tasks
            .spawn(async move {
                match time::timeout(OPEN_TIMEOUT, opening).await {
                    Ok(Ok(Ok(()))) => log::info!("LSE's {exchange} stream opened at {level}"),
                    Ok(Ok(Err(refused))) => log::error!(
                        "{refused}: no data of its contracts streams at {level} until a later \
                         subscription of the level is answered"
                    ),
                    // Disconnected before LSE answered.
                    Ok(Err(_)) => {}
                    Err(_) => log::warn!(
                        "LSE's {exchange} stream did not answer the {level} subscription within \
                         {OPEN_TIMEOUT:?}"
                    ),
                }
            })
            .map_err(|e| anyhow::anyhow!("awaiting LSE's {exchange} answer at {level}: {e}"))
    }

    /// Turns the records `out` passes on, of the connection of `level`, into
    /// the data of the contracts subscribed, and logs gaps.
    fn spawn_records(
        &self,
        level: ExchangeLevel,
        mut out: tokio::sync::mpsc::UnboundedReceiver<ExchangeWsMessage>,
    ) -> anyhow::Result<()> {
        let exchange = self.exchange;
        let subscribed = Arc::clone(&self.subscribed);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancelled = self.tasks.cancellation_token();
        self.tasks
            .spawn(async move {
                let mut latest: AHashMap<String, Latest> = AHashMap::new();
                // Each contract's deltas of the exchange message under way.
                let mut events: AHashMap<String, Vec<OrderBookDelta>> = AHashMap::new();
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
                        ExchangeWsMessage::Record {
                            symbol,
                            sequence,
                            record: ExchangeRecord::Mbo(record),
                            ..
                        } => {
                            let event = events.entry(symbol.clone()).or_default();
                            match subscribed.get_cloned(&symbol).filter(|of| of.deltas) {
                                Some(of) => gather(&sender, clock, of, &record, sequence, event),
                                // Of a contract whose deltas are not taken.
                                None => event.clear(),
                            }
                        }
                        ExchangeWsMessage::Record {
                            symbol,
                            sequence,
                            record,
                            opening,
                        } => {
                            let sent = subscribed.get_cloned(&symbol).is_some_and(|of| {
                                emit(&sender, clock, of, &record, sequence, opening)
                            });
                            latest.insert(
                                symbol,
                                Latest {
                                    record,
                                    sequence,
                                    sent,
                                },
                            );
                        }
                        ExchangeWsMessage::Reopen { symbol } => {
                            if let (Some(of), Some(latest)) =
                                (subscribed.get_cloned(&symbol), latest.get_mut(&symbol))
                                && !latest.sent
                            {
                                latest.sent =
                                    emit(&sender, clock, of, &latest.record, latest.sequence, true);
                            }
                        }
                        ExchangeWsMessage::Gap { symbol, from, to } => {
                            let contract = subscribed
                                .get_cloned(&symbol)
                                .map_or_else(|| symbol.clone(), |of| of.instrument_id.to_string());
                            log::warn!(
                                "LSE's {exchange} stream: the {level} records {from} to {to} of \
                                 {contract} ({symbol}) did not come, a gap left as it is"
                            );
                        }
                    }
                }
            })
            .map_err(|e| {
                anyhow::anyhow!("starting the {level} records of LSE's {exchange} stream: {e}")
            })
    }

    /// Subscribes to `kind` of the contract `id`, at the price precision of
    /// its definition in `cache`: has the stream carry it at `kind`'s
    /// level, opening the level's connection if none is, and looking a CME
    /// contract up first if no subscription has yet.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if the stream is not open, if it is no
    /// contract of an exchange stream, or if `cache` has no definition of
    /// it: LSE publishes none, and its prices are read at the definition's
    /// precision.
    pub(super) fn subscribe(
        &mut self,
        id: InstrumentId,
        kind: StreamKind,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        // `open` opens Level 1 for the contracts the config streams.
        if !self.clients.contains_key(&ExchangeLevel::Mbp1) {
            bail!(
                "{id} is not streamed: LSE's {} stream opens only for the contracts of its \
                 venue the LSE client's config streams (its stream_ids)",
                self.exchange
            );
        }
        let year = Timestamp::from(self.clock.get_time_ns())
            .to_zoned(TimeZone::UTC)
            .year();
        let instrument = ExchangeInstrument::of(id, year)?;
        let (price_precision, _) = precisions(cache, id)?;
        let subscriber = self.subscriber(kind.level())?;
        let reopen = kind.reopens();
        let named = self
            .names
            .get_cloned(&id)
            .or_else(|| instrument.ticker().map(ToOwned::to_owned));
        if let Some(symbol) = named {
            self.names.insert(id, symbol.clone());
            take(&self.subscribed, &symbol, id, price_precision, kind);
            return subscriber.subscribe(&symbol, reopen);
        }
        let exchange = self.exchange;
        let http = Arc::clone(&self.access()?.http);
        let listings = Arc::clone(&self.listings);
        let (names, subscribed) = (Arc::clone(&self.names), Arc::clone(&self.subscribed));
        self.tasks
            .spawn(async move {
                match instrument.name(&listings, &http).await {
                    Ok(symbol) => {
                        names.insert(id, symbol.clone());
                        take(&subscribed, &symbol, id, price_precision, kind);
                        if let Err(e) = subscriber.subscribe(&symbol, reopen) {
                            log::error!("{id} streams nothing from LSE's {exchange} stream: {e:#}");
                        }
                    }
                    Err(e) => {
                        log::error!("{id} streams nothing from LSE's {exchange} stream: {e:#}");
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("looking up {id} to stream it: {e}"))
    }

    /// Ends `kind` of the contract `id`. Each level's connection it takes no
    /// data at then, `kind`'s or another, carries it no longer from its next
    /// connection (see `ExchangeSubscriber::unsubscribe`): the stream carries
    /// it on until then.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is not subscribed.
    pub(super) fn unsubscribe(&self, id: InstrumentId, kind: StreamKind) -> anyhow::Result<()> {
        let symbol = self
            .names
            .get_cloned(&id)
            .with_context(|| format!("{id} is not subscribed on LSE's {} stream", self.exchange))?;
        self.subscribed.rcu(|subscribed| {
            if let Some(of) = subscribed.get_mut(&symbol) {
                of.set(kind, false);
            }
        });
        let of = self.subscribed.get_cloned(&symbol);
        for (level, client) in &self.clients {
            if !of.is_some_and(|of| of.takes_at(*level)) {
                client.subscriber().unsubscribe(&symbol);
            }
        }
        Ok(())
    }

    /// Begins the shutdown of the record tasks; `close` finishes it.
    pub(super) fn begin_shutdown(&self) {
        self.tasks.begin_shutdown();
    }

    /// Stops the record tasks and disconnects the stream's connections.
    ///
    /// # Errors
    ///
    /// Returns an error if the record tasks do not stop.
    pub(super) async fn close(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        let stopped = self.tasks.finish_shutdown(TASK_GRACE, TASK_FORCE).await;
        for (_, client) in self.clients.drain() {
            client.disconnect().await;
        }
        self.subscribed.rcu(|subscribed| subscribed.clear());
        stopped.map_err(|e| anyhow::anyhow!("stopping the LSE {} tasks: {e}", self.exchange))
    }
}

/// Sends the data of `record`, of the contract `of`, at its `sequence`,
/// that `of` takes, an `opening` carrying no event of its own. Whether the
/// data a new subscriber opens with went out: a quote, or a depth.
fn emit(
    sender: &EventSender<DataEvent>,
    clock: &'static AtomicTime,
    of: Subscribed,
    record: &ExchangeRecord,
    sequence: u64,
    opening: bool,
) -> bool {
    match record {
        ExchangeRecord::Mbp1(record) => emit_mbp1(sender, clock, of, record, sequence, opening),
        ExchangeRecord::Mbp10(record) => emit_mbp10(sender, clock, of, record, sequence),
        // Gathered into events (see `gather`); a contract opens with none.
        ExchangeRecord::Mbo(_) => false,
    }
}

/// Sends `record`'s best bid and offer as a quote, and its trade, if it is
/// one and not an `opening`, as a trade with `sequence` as its id. Whether
/// a quote went out.
fn emit_mbp1(
    sender: &EventSender<DataEvent>,
    clock: &'static AtomicTime,
    of: Subscribed,
    record: &Mbp1Msg,
    sequence: u64,
    opening: bool,
) -> bool {
    let decoded = decode_mbp1_msg(
        record,
        of.instrument_id,
        of.price_precision,
        Some(clock.get_time_ns()),
        !opening,
    );
    let (quote, trade) = match decoded {
        Ok(decoded) => decoded,
        Err(e) => {
            log::warn!(
                "a record of {} from LSE's stream skipped: {e:#}",
                of.instrument_id
            );
            return false;
        }
    };
    let quote = quote.filter(|_| of.quotes);
    let quoted = quote.is_some();
    if let Some(quote) = quote {
        send(sender, DataEvent::Data(Data::Quote(quote)));
    }
    if let Some(mut trade) = trade.filter(|_| of.trades) {
        trade.trade_id = TradeId::new(sequence.to_string());
        send(sender, DataEvent::Data(Data::Trade(trade)));
    }
    quoted
}

/// Sends `record`'s ten levels a side as a depth numbered `sequence`, if
/// `of` takes depth. Whether it went out.
fn emit_mbp10(
    sender: &EventSender<DataEvent>,
    clock: &'static AtomicTime,
    of: Subscribed,
    record: &Mbp10Msg,
    sequence: u64,
) -> bool {
    if !of.depth {
        return false;
    }
    match decode_mbp10_msg(
        record,
        of.instrument_id,
        of.price_precision,
        Some(clock.get_time_ns()),
    ) {
        Ok(mut depth) => {
            depth.sequence = sequence;
            send(sender, DataEvent::Data(Data::BookDepth(Box::new(depth))));
            true
        }
        Err(e) => {
            log::warn!(
                "a record of {} from LSE's stream skipped: {e:#}",
                of.instrument_id
            );
            false
        }
    }
}

/// Adds the delta of `record`, the contract `of`'s order event, numbered
/// `sequence`, to `event`, the deltas of its exchange message so far; at
/// the message's last event, which LSE flags `F_LAST`, sends them as one
/// `OrderBookDeltas`, the last flagged `F_LAST`, if any came.
fn gather(
    sender: &EventSender<DataEvent>,
    clock: &'static AtomicTime,
    of: Subscribed,
    record: &MboMsg,
    sequence: u64,
    event: &mut Vec<OrderBookDelta>,
) {
    match decode_mbo_msg(
        record,
        of.instrument_id,
        of.price_precision,
        Some(clock.get_time_ns()),
        false,
    ) {
        Ok((Some(mut delta), _)) => {
            delta.sequence = sequence;
            event.push(delta);
        }
        // An event of no action.
        Ok((None, _)) => {}
        Err(e) => log::warn!(
            "a record of {} from LSE's stream skipped: {e:#}",
            of.instrument_id
        ),
    }
    if record.flags.is_last()
        && let Some(last) = event.last_mut()
    {
        last.flags |= RecordFlag::F_LAST as u8;
        let deltas = OrderBookDeltas::new(of.instrument_id, std::mem::take(event));
        send(sender, DataEvent::Data(Data::BookDeltas(Box::new(deltas))));
    }
}

/// Records that `kind` of `id`, named `symbol` on the stream, is taken.
fn take(
    subscribed: &SubscribedByName,
    symbol: &str,
    id: InstrumentId,
    price_precision: u8,
    kind: StreamKind,
) {
    subscribed.rcu(|subscribed| {
        let of = subscribed.entry(symbol.to_owned()).or_insert(Subscribed {
            instrument_id: id,
            price_precision,
            quotes: false,
            trades: false,
            depth: false,
            deltas: false,
        });
        of.price_precision = price_precision;
        of.set(kind, true);
    });
}
