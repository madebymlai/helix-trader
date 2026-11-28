//! Streams the data client's exchange data: the `GLBX` contracts of CME Globex and the `XNAS`
//! stocks of Nasdaq TotalView. Each exchange has its own stream.
//!
//! LSE's CME and Nasdaq streams speak one protocol. LSE's WebSocket documentation says so in "The
//! CME Group stream": the Nasdaq stream "sends the same messages for stocks". They carry Level 1
//! as quotes and trades, Level 2 as book depth and Level 3 as book deltas. Each exchange gets its
//! own `ExchangeStreams`, which streams that exchange's contracts on its plan's key.
//!
//! `open` names each configured contract as the stream takes it:
//! - A CME contract is named by its listing's symbol, looked up in LSE's catalogue with the CME
//!   key (see `http::listing`).
//! - A Nasdaq stock is named by its ticker.
//!
//! It then connects and names all contracts in Level 1's first subscription (see
//! `ExchangeStreamClient`). LSE checks that subscription. `open` fails with LSE's refusal of a
//! lookup or of the subscription.
//!
//! A subscription reads the contract's price precision from the cache. It has the stream carry the
//! contract at the level of its data:
//! - Quotes and trades at Level 1.
//! - Depth at Level 2.
//! - Deltas at Level 3.
//!
//! Each level has its own connection, because LSE takes one level per connection ("to change the
//! contracts or the level, open another connection").
//!
//! The first depth or deltas subscription opens its level's connection. If LSE refuses it, for
//! example because the key's plan lacks the level, the refusal is logged as an error. A later
//! subscription to the level asks again on a new connection.
//!
//! A new quotes or depth subscriber gets an opening from a contract the stream already carries:
//! its latest best bid and offer, or its ten levels. It gets none if they went out already. A
//! contract not yet carried at the level is named, and a CME contract is looked up. It is carried
//! from the level's next connection.
//!
//! A level's later connections name only the contracts that take data at that level. A configured
//! contract that no subscription takes is named on Level 1's first connection alone. A contract
//! whose subscriptions end is left out of every level where it then takes no data. So a rolled-out,
//! expired contract does not stop the level's data. LSE would refuse such a contract (`symbol
//! unavailable`, closing the connection with 1008, not reconnected).
//!
//! Nautilus' Databento decoder turns each record into Nautilus data, because LSE's records are
//! Databento's. It uses the contract's price precision:
//! - Level 1 (`decode_mbp1_msg`): the best bid and offer becomes a quote, with LSE's sizes. A trade
//!   that is not an opening becomes a trade. It has LSE's aggressor side, which is none for the `N`
//!   LSE gives every trade ("the feed does not say which side took it").
//! - Level 2 (`decode_mbp10_msg`): the ten levels on each side, after the record's change, become
//!   an `OrderBookDepth` with each level's order count. An empty level has none. The contract
//!   opens with its last ten levels.
//! - Level 3 (`decode_mbo_msg`): an order added (`A`), changed (`M`) or removed (`C`) becomes an
//!   `OrderBookDelta`. An event with no action (`N`) gives none.
//!
//! LSE's Level 3 opens with no book ("the stream carries order events and no opening book"). So a
//! contract's deltas start at its first order event, and none is invented. LSE flags the last
//! event of each exchange message (128, `F_LAST`). The deltas are gathered until that event, then
//! sent as one `OrderBookDeltas`, as Nautilus' Databento client sends a live event's. A flagged
//! event with no delta also ends its message. Its `F_LAST` goes on the message's last delta, as
//! the decoder does on history (`MboDeltaBuffer`). So every event ends `F_LAST` (Nautilus' adapter
//! guide: "Each logical update ends with `F_LAST`").
//!
//! Data is stamped `ts_event` at the record's `ts_recv`, when LSE received it, as Nautilus'
//! Databento client does. It is stamped `ts_init` when the record came. LSE leaves the record's
//! own sequence at 0. So a trade, depth or delta takes the stream's sequence for its record, one
//! per contract and level. A gap in a contract's sequence is logged and left.

use std::{fmt::Display, sync::Arc};

use ahash::AHashMap;
use anyhow::Context;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamKind {
    Quotes,
    Trades,
    Depth,
    Deltas,
}

impl StreamKind {
    const fn level(self) -> ExchangeLevel {
        match self {
            Self::Quotes | Self::Trades => ExchangeLevel::Mbp1,
            Self::Depth => ExchangeLevel::Mbp10,
            Self::Deltas => ExchangeLevel::Mbo,
        }
    }

    // Whether a new subscriber gets the contract's latest record, either its best bid and offer or
    // its ten levels.
    const fn reopens(self) -> bool {
        matches!(self, Self::Quotes | Self::Depth)
    }
}

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
    const fn takes_at(&self, level: ExchangeLevel) -> bool {
        match level {
            ExchangeLevel::Mbp1 => self.quotes || self.trades,
            ExchangeLevel::Mbp10 => self.depth,
            ExchangeLevel::Mbo => self.deltas,
        }
    }

    const fn set(&mut self, kind: StreamKind, taken: bool) {
        match kind {
            StreamKind::Quotes => self.quotes = taken,
            StreamKind::Trades => self.trades = taken,
            StreamKind::Depth => self.depth = taken,
            StreamKind::Deltas => self.deltas = taken,
        }
    }
}

// The subscribed contracts, keyed by their names on the stream
type SubscribedByName = Arc<AtomicMap<String, Subscribed>>;

// A contract's latest Level 1 or 2 record, with its sequence and whether its data went out
struct Latest {
    record: ExchangeRecord,
    sequence: u64,
    sent: bool,
}

#[derive(Debug)]
pub(super) struct ExchangeAccess {
    pub(super) credential: LseCredential,
    pub(super) http: Arc<LseHttpClient>,
}

/// A contract on an exchange stream.
///
/// The stream names a CME Globex contract by its listing's symbol, and a Nasdaq TotalView stock by
/// its ticker.
#[derive(Debug, Clone)]
pub(super) enum ExchangeInstrument {
    Cme(CmeContract),
    Nasdaq(NasdaqStock),
}

impl ExchangeInstrument {
    /// Returns the contract `id` stands for.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is neither a `GLBX` contract nor an `XNAS` stock (see
    /// `LseInstrument::from_instrument_id`).
    fn of(id: InstrumentId, reference_year: i16) -> anyhow::Result<Self> {
        match LseInstrument::from_instrument_id(id, reference_year)? {
            LseInstrument::Cme(contract) => Ok(Self::Cme(contract)),
            LseInstrument::Nasdaq(stock) => Ok(Self::Nasdaq(stock)),
            LseInstrument::Series(_) | LseInstrument::Opra(_) => {
                anyhow::bail!("{id} streams on no exchange stream of LSE's, of GLBX and XNAS")
            }
        }
    }

    fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::Cme(contract) => contract.instrument_id(),
            Self::Nasdaq(stock) => stock.instrument_id(),
        }
    }

    // A stock's ticker, which names it on the stream without LSE's catalogue, or none for a CME
    // contract (see `name`).
    fn ticker(&self) -> Option<&str> {
        match self {
            Self::Cme(_) => None,
            Self::Nasdaq(stock) => Some(stock.ticker()),
        }
    }

    /// Returns the contract's name on its stream.
    ///
    /// A CME contract is named by its listing's symbol, looked up in `listings` (LSE's WebSocket
    /// documentation, "Contracts": the names "the HTTP API downloads the same contracts by"). A
    /// stock is named by its ticker.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - LSE delisted the CME contract's listing. Its symbol recurs every ten years, so the stream
    ///   would carry the later contract's records under its id. For example, `ESZ16.GLBX` was
    ///   delisted in 2016, and its symbol `ESZ6` names 2026's contract.
    /// - `CmeListings::listing` fails.
    async fn name(&self, listings: &CmeListings, http: &LseHttpClient) -> anyhow::Result<String> {
        match self {
            Self::Cme(contract) => {
                let listing = listings.listing(http, contract).await?;
                if listing.delisted_date.is_some() {
                    anyhow::bail!(
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

impl Display for ExchangeInstrument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cme(contract) => Display::fmt(contract, f),
            Self::Nasdaq(stock) => Display::fmt(stock, f),
        }
    }
}

#[derive(Debug)]
pub(super) struct ExchangeStreams {
    exchange: LseExchange,
    access: Option<ExchangeAccess>,
    listings: Arc<CmeListings>,
    url: String,
    streamed: Vec<ExchangeInstrument>,
    clients: AHashMap<ExchangeLevel, ExchangeStreamClient>,
    names: Arc<AtomicMap<InstrumentId, String>>,
    subscribed: SubscribedByName,
    tasks: TaskGroup,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl ExchangeStreams {
    /// Creates the streams for `streamed` on `exchange`'s stream at `url`. None is open.
    ///
    /// # Errors
    ///
    /// Returns an error if a contract is streamed and the plan has no key. The error names the
    /// key's variable and the contracts.
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
            anyhow::bail!(
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

    /// Names the streamed contracts as the stream takes them, then connects.
    ///
    /// It names them all in Level 1's first subscription, which LSE checks. It does nothing if no
    /// contract is streamed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A lookup fails.
    /// - `ExchangeStreamClient::connect` fails, including when LSE refuses the subscription.
    ///
    /// The stream stays closed after an error.
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

    /// Returns the subscriber for `level`'s connection.
    ///
    /// That is the open subscriber, or a new one subscribed with the contracts taken at the level.
    /// A new one connects with its first contract.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The client has no key for the exchange's plan.
    /// - A new connection's client or tasks do not start.
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
            &[],
            out_tx,
        )?;
        let subscriber = client.subscriber();
        for symbol in &carried {
            subscriber.subscribe(symbol, false)?;
        }

        // A closed client's handler has stopped
        self.clients.insert(level, client);
        self.spawn_records(level, out)?;
        self.spawn_opening(level, opening)?;
        Ok(subscriber)
    }

    /// Returns the exchange plan's key and LSE's API on it.
    ///
    /// # Errors
    ///
    /// Returns an error if the client has no such key. The error names the exchange.
    fn access(&self) -> anyhow::Result<&ExchangeAccess> {
        self.access.as_ref().with_context(|| {
            format!(
                "{} contracts streamed with no {} key",
                self.exchange,
                self.exchange.product()
            )
        })
    }

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

                    // Disconnected before LSE answered
                    Ok(Err(_)) => {}
                    Err(_) => log::warn!(
                        "LSE's {exchange} stream did not answer the {level} subscription within \
                         {OPEN_TIMEOUT:?}"
                    ),
                }
            })
            .map_err(|e| anyhow::anyhow!("awaiting LSE's {exchange} answer at {level}: {e}"))
    }

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

                                // The contract does not take deltas
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

    /// Subscribes to `kind` of the contract `id`.
    ///
    /// It uses the price precision of the contract's definition in `cache`. It opens the level's
    /// connection if none is open.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The stream is not open.
    /// - `id` is no contract of an exchange stream.
    /// - `cache` has no definition of `id`. LSE publishes none, and prices are read at its
    ///   precision.
    ///
    /// Each error names `id`.
    pub(super) fn subscribe(
        &mut self,
        id: InstrumentId,
        kind: StreamKind,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        // `open` opens Level 1 for the contracts the config streams
        if !self.clients.contains_key(&ExchangeLevel::Mbp1) {
            anyhow::bail!(
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

    /// Ends `kind` of the contract `id`.
    ///
    /// Each level's connection that then takes no data stops carrying the contract from its next
    /// connection (see `ExchangeSubscriber::unsubscribe`). It carries the contract until then.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is not subscribed.
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

// Sends the data of `record` that `of` takes, treats an `opening` as having no event of its own,
// and returns whether the data a new subscriber opens with, a quote or a depth, went out.
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

        // Gathered into events (see `gather`), so an opening has none
        ExchangeRecord::Mbo(_) => false,
    }
}

// Sends `record`'s best bid and offer as a quote, and its trade unless the record is an `opening`,
// using `sequence` as the id, and returns whether a quote went out.
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

// Sends `record`'s ten levels on each side as a depth numbered `sequence` if `of` takes depth, and
// returns whether it went out.
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

// Adds the delta of `record` to `event`, which holds the deltas of its exchange message so far,
// and sends them as one `OrderBookDeltas` at the message's last event, which LSE flags `F_LAST`.
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

        // An event of no action
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
