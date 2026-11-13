//! The data client's CME Globex data: the `GLBX` contracts it streams on
//! LSE's CME stream at Level 1, as quotes and trades.
//!
//! `open` looks each contract the config streams up in LSE's catalogue
//! with the CME key (see `http::listing`), its listing's symbol being its
//! name on the stream, then connects to the stream subscribing them all
//! (see `CmeStreamClient`), and fails with LSE's refusal of a lookup or of
//! the subscription. A quotes or trades subscription reads the contract's
//! price precision from the cache and has the stream carry the contract:
//! one carried already opens for a new quotes subscriber with its latest
//! best bid and offer, unless that went out already, and one not carried
//! yet, a later contract of a rolled root, is looked up and carried from
//! the stream's next connection, which opens it. Each record
//! of a contract subscribed becomes Nautilus data through Nautilus'
//! Databento decoder (`decode_mbp1_msg`), LSE's records being Databento's:
//! its best bid and offer a quote, with the sizes LSE sends, and its trade,
//! if it is one and not an opening, a trade with no aggressor side, LSE
//! sending side `N` for every trade ("the feed does not say which side took
//! it"). The decoder stamps both at the record's `ts_recv` (`ts_event`),
//! when LSE received it, as Nautilus' Databento client stamps its data, and
//! `ts_init` when the record came. The decoder takes a trade's id from the
//! record's own sequence, which LSE leaves 0; the trade takes the stream's
//! sequence of its record instead, one of its contract's alone. A gap of a
//! contract's sequence is logged and left.

use std::sync::Arc;

use ahash::AHashMap;
use anyhow::{Context, bail};
use dbn::Mbp1Msg;
use jiff::{Timestamp, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    live::{dst::time::Duration, sender::EventSender},
    messages::DataEvent,
};
use nautilus_core::{collections::AtomicMap, time::AtomicTime};
use nautilus_databento::decode::decode_mbp1_msg;
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::Data,
    identifiers::{InstrumentId, TradeId},
    instruments::Instrument,
};

use crate::{
    common::{
        consts::{CME_STREAM_URL, LSE_CME_API_KEY},
        credential::LseCredential,
        symbol::CmeContract,
    },
    http::{client::LseHttpClient, listing::CmeListings},
    websocket::cme::client::{CmeStreamClient, CmeWsMessage},
};

/// How long the record tasks get to finish at a close, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// The data of a contract a subscription takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CmeKind {
    Quotes,
    Trades,
}

/// A contract subscribed: its id, its price precision and the data taken.
#[derive(Debug, Clone, Copy)]
struct Subscribed {
    instrument_id: InstrumentId,
    price_precision: u8,
    quotes: bool,
    trades: bool,
}

/// The contracts subscribed, by their names on the stream: read on each
/// record, written on each subscription.
type SubscribedByName = Arc<AtomicMap<String, Subscribed>>;

/// A contract's latest record, whose levels are its best bid and offer, its
/// sequence, and whether its quote went out.
struct Latest {
    record: Mbp1Msg,
    sequence: u64,
    quoted: bool,
}

/// The CME key, and LSE's API on it, which looks the contracts up.
#[derive(Debug)]
pub(super) struct CmeAccess {
    pub(super) credential: LseCredential,
    pub(super) http: Arc<LseHttpClient>,
}

/// The contracts the client streams on LSE's CME stream.
#[derive(Debug)]
pub(super) struct CmeStreams {
    /// The CME key and LSE's API on it; none if the client has no CME key.
    access: Option<CmeAccess>,
    listings: Arc<CmeListings>,
    /// The stream's address, if not LSE's own.
    url: Option<String>,
    /// The contracts the config streams, which `open` subscribes.
    streamed: Vec<CmeContract>,
    client: Option<CmeStreamClient>,
    /// The names on the stream of the contracts subscribed.
    names: Arc<AtomicMap<InstrumentId, String>>,
    subscribed: SubscribedByName,
    /// The tasks turning records into quotes and trades, and looking up the
    /// contracts subscribed after `open`.
    tasks: TaskGroup,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl CmeStreams {
    /// The streams of `streamed`, on `access`, the CME key, looking each
    /// contract up in `listings`; none open.
    ///
    /// # Errors
    ///
    /// Returns an error naming `LSE_CME_API_KEY` and the contracts if any
    /// is streamed with no CME key.
    pub(super) fn new(
        streamed: Vec<CmeContract>,
        access: Option<CmeAccess>,
        listings: Arc<CmeListings>,
        url: Option<String>,
        data_sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<Self> {
        if access.is_none() && !streamed.is_empty() {
            bail!(
                "streaming {} needs a CME Group key: set {LSE_CME_API_KEY}, or give the config's",
                streamed
                    .iter()
                    .map(|contract| contract.instrument_id().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        Ok(Self {
            access,
            listings,
            url,
            streamed,
            client: None,
            names: Arc::default(),
            subscribed: Arc::default(),
            tasks: TaskGroup::new(),
            data_sender,
            clock,
        })
    }

    /// Looks the contracts streamed up and connects to the stream
    /// subscribing them all; starts turning their records into data.
    /// Nothing to do if none is streamed.
    ///
    /// # Errors
    ///
    /// Returns an error if a lookup fails (LSE's refusal in the error), or
    /// as `CmeStreamClient::connect`, LSE's refusal of the subscription
    /// among them; then the stream is left closed.
    pub(super) async fn open(&mut self) -> anyhow::Result<()> {
        if self.streamed.is_empty() {
            return Ok(());
        }
        if !self.tasks.is_open() {
            self.close().await?;
            self.tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("reopening the LSE CME tasks: {e}"))?;
        }
        let access = self
            .access
            .as_ref()
            .context("CME contracts streamed with no CME key")?;
        let mut symbols = Vec::new();
        for contract in &self.streamed {
            let listing = self
                .listings
                .listing(&access.http, contract)
                .await
                .with_context(|| format!("streaming {contract}"))?;
            self.names
                .insert(contract.instrument_id(), listing.symbol.clone());
            symbols.push(listing.symbol);
        }
        let (out_tx, out) = tokio::sync::mpsc::unbounded_channel();
        let url = self.url.as_deref().unwrap_or(CME_STREAM_URL);
        let client = CmeStreamClient::connect(url, &access.credential, &symbols, out_tx).await?;
        self.client = Some(client);
        if let Err(e) = self.spawn_records(out) {
            self.close().await?;
            return Err(e);
        }
        Ok(())
    }

    /// Turns the records `out` passes on into quotes and trades of the
    /// contracts subscribed, and logs gaps.
    fn spawn_records(
        &self,
        mut out: tokio::sync::mpsc::UnboundedReceiver<CmeWsMessage>,
    ) -> anyhow::Result<()> {
        let subscribed = Arc::clone(&self.subscribed);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancelled = self.tasks.cancellation_token();
        self.tasks
            .spawn(async move {
                let mut latest: AHashMap<String, Latest> = AHashMap::new();
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
                        CmeWsMessage::Record {
                            symbol,
                            sequence,
                            record,
                            opening,
                        } => {
                            let quoted = match subscribed.get_cloned(&symbol) {
                                Some(of) => emit(&sender, clock, of, &record, sequence, opening),
                                None => false,
                            };
                            latest.insert(
                                symbol,
                                Latest {
                                    record,
                                    sequence,
                                    quoted,
                                },
                            );
                        }
                        CmeWsMessage::Reopen { symbol } => {
                            if let (Some(of), Some(latest)) =
                                (subscribed.get_cloned(&symbol), latest.get_mut(&symbol))
                                && !latest.quoted
                            {
                                latest.quoted =
                                    emit(&sender, clock, of, &latest.record, latest.sequence, true);
                            }
                        }
                        CmeWsMessage::Gap { symbol, from, to } => {
                            let contract = subscribed
                                .get_cloned(&symbol)
                                .map_or_else(|| symbol.clone(), |of| of.instrument_id.to_string());
                            log::warn!(
                                "LSE's CME stream: the records {from} to {to} of {contract} \
                                 ({symbol}) did not come, a gap left as it is"
                            );
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("starting the records of LSE's CME stream: {e}"))
    }

    /// Subscribes to `kind` of the contract `id`, at the price precision of
    /// its definition in `cache`: has the stream carry it, looking it up
    /// first if no subscription has yet.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if the stream is not open, if it is not a
    /// CME contract, or if `cache` has no definition of it: LSE publishes
    /// none, and its prices are read at the definition's precision.
    pub(super) fn subscribe(
        &self,
        id: InstrumentId,
        kind: CmeKind,
        cache: &CacheView,
    ) -> anyhow::Result<()> {
        let (client, access) = self
            .client
            .as_ref()
            .zip(self.access.as_ref())
            .with_context(|| {
                format!(
                    "{id} is not streamed: LSE's CME stream opens only for the CME contracts the \
                 LSE client's config streams (its stream_ids)"
                )
            })?;
        let year = Timestamp::from(self.clock.get_time_ns())
            .to_zoned(TimeZone::UTC)
            .year();
        let contract = CmeContract::from_instrument_id(id, year)?;
        let price_precision = cache
            .borrow()
            .instrument(&id)
            .with_context(|| {
                format!(
                    "{id} is not in the cache: LSE publishes no definitions, and its prices are \
                     read at the definition's precision, so it must come from elsewhere, a \
                     catalog or a broker"
                )
            })?
            .price_precision();
        let subscriber = client.subscriber();
        let reopen = kind == CmeKind::Quotes;
        if let Some(symbol) = self.names.get_cloned(&id) {
            take(&self.subscribed, &symbol, id, price_precision, kind);
            return subscriber.subscribe(&symbol, reopen);
        }
        let (listings, http) = (Arc::clone(&self.listings), Arc::clone(&access.http));
        let (names, subscribed) = (Arc::clone(&self.names), Arc::clone(&self.subscribed));
        self.tasks
            .spawn(async move {
                match listings.listing(&http, &contract).await {
                    Ok(listing) => {
                        names.insert(id, listing.symbol.clone());
                        take(&subscribed, &listing.symbol, id, price_precision, kind);
                        if let Err(e) = subscriber.subscribe(&listing.symbol, reopen) {
                            log::error!("{id} streams nothing from LSE's CME stream: {e:#}");
                        }
                    }
                    Err(e) => log::error!("{id} streams nothing from LSE's CME stream: {e:#}"),
                }
            })
            .map_err(|e| anyhow::anyhow!("looking up {id} to stream it: {e}"))
    }

    /// Ends `kind` of the contract `id`; the stream carries it on.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if it is not subscribed.
    pub(super) fn unsubscribe(&self, id: InstrumentId, kind: CmeKind) -> anyhow::Result<()> {
        let symbol = self
            .names
            .get_cloned(&id)
            .with_context(|| format!("{id} is not subscribed on LSE's CME stream"))?;
        self.subscribed.rcu(|subscribed| {
            if let Some(of) = subscribed.get_mut(&symbol) {
                match kind {
                    CmeKind::Quotes => of.quotes = false,
                    CmeKind::Trades => of.trades = false,
                }
            }
        });
        Ok(())
    }

    /// Begins the shutdown of the record tasks; `close` finishes it.
    pub(super) fn begin_shutdown(&self) {
        self.tasks.begin_shutdown();
    }

    /// Stops the record tasks and disconnects the stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the record tasks do not stop.
    pub(super) async fn close(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        let stopped = self.tasks.finish_shutdown(TASK_GRACE, TASK_FORCE).await;
        if let Some(client) = self.client.take() {
            client.disconnect().await;
        }
        self.subscribed.rcu(|subscribed| subscribed.clear());
        stopped.map_err(|e| anyhow::anyhow!("stopping the LSE CME tasks: {e}"))
    }
}

/// Sends the data of `record`, of the contract `of`, at its `sequence`,
/// that `of` takes: its best bid and offer as a quote, and its trade, if it
/// is one and not an `opening`, as a trade with that sequence as its id.
/// Whether a quote went out.
fn emit(
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
                "a record of {} from LSE's CME stream skipped: {e:#}",
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

/// Records that `kind` of `id`, named `symbol` on the stream, is taken.
fn take(
    subscribed: &SubscribedByName,
    symbol: &str,
    id: InstrumentId,
    price_precision: u8,
    kind: CmeKind,
) {
    subscribed.rcu(|subscribed| {
        let of = subscribed.entry(symbol.to_owned()).or_insert(Subscribed {
            instrument_id: id,
            price_precision,
            quotes: false,
            trades: false,
        });
        of.price_precision = price_precision;
        match kind {
            CmeKind::Quotes => of.quotes = true,
            CmeKind::Trades => of.trades = true,
        }
    });
}

/// Sends `event` to the node; a closed channel means the node is stopping.
fn send(sender: &EventSender<DataEvent>, event: DataEvent) {
    if let Err(e) = sender.send(event) {
        log::warn!("data event not sent: {e}");
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, num::NonZeroU32, rc::Rc, time::Duration};

    use nautilus_common::{
        cache::Cache,
        clients::DataClient,
        clock::{Clock, VirtualClock},
        factories::DataClientFactory,
        live::runner::replace_data_event_sender,
        messages::data::{SubscribeQuotes, SubscribeTrades},
    };
    use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{QuoteTick, TradeTick},
        enums::AggressorSide,
        identifiers::{ClientId, Symbol},
        instruments::InstrumentAny,
        types::{Price, Quantity},
    };
    use serde_json::{Value, json};
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        LseDataClientConfig, LseDataClientFactory,
        testing::{
            API_KEY, CME_API_KEY, CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7, CME_SYMBOL_UNAVAILABLE,
            CmeAction, ESZ26, GCZ26, MockCmeStream, MockLse, PAIRS_ESZ6, PAIRS_GCG7, PAIRS_GCZ6,
            Reply, dbn_frames, esz26, frames, gcz26, held, sent,
        },
    };

    type Events = mpsc::UnboundedReceiver<DataEvent>;

    /// GCG7 of 2027, `GCG27.GLBX`, as GCZ26's definition with its id.
    const GCG27: &str = "GCG27.GLBX";

    fn gcg27() -> InstrumentAny {
        let InstrumentAny::FuturesContract(mut contract) = gcz26() else {
            unreachable!("a futures contract")
        };
        contract.id = InstrumentId::from(GCG27);
        contract.raw_symbol = Symbol::from("GCG7");
        InstrumentAny::FuturesContract(contract)
    }

    /// The client the factory creates with the CME key alone, LSE's API on
    /// `lse` and its CME stream on `stream`, streaming `streamed`, its cache
    /// holding `instruments`, and the node's data events, once it connected
    /// or failed to.
    async fn streaming(
        lse: &MockLse,
        stream: &MockCmeStream,
        streamed: &[&str],
        instruments: Vec<InstrumentAny>,
    ) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
        let (tx, events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let mut cache = Cache::default();
        for instrument in instruments {
            cache.add_instrument(instrument).unwrap();
        }
        let config = LseDataClientConfig::builder()
            .cme_api_key(SecretString::from(CME_API_KEY))
            .base_url_http(lse.url().to_string())
            .base_url_ws_cme(stream.url().to_string())
            .cme_calls_per_minute(NonZeroU32::new(6000).unwrap())
            .stream_ids(streamed.iter().map(|&id| InstrumentId::from(id)).collect())
            .build();
        let factory = LseDataClientFactory::new();
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
        let mut client = factory
            .create(
                factory.name(),
                &config,
                Rc::new(RefCell::new(cache)).into(),
                clock,
            )
            .unwrap();
        let connected = client.connect().await;
        (client, connected, events)
    }

    fn ids(ids: &[&str]) -> Vec<InstrumentId> {
        ids.iter().map(|&id| InstrumentId::from(id)).collect()
    }

    /// The quotes of `of`, as the data engine asks the client for them.
    fn quotes_of(of: &[&str]) -> SubscribeQuotes {
        SubscribeQuotes::new(
            ids(of),
            Some(ClientId::from("LSE")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    /// The trades of `of`, as the data engine asks the client for them.
    fn trades_of(of: &[&str]) -> SubscribeTrades {
        SubscribeTrades::new(
            ids(of),
            Some(ClientId::from("LSE")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    async fn next(events: &mut Events) -> Data {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("no data event within 10 s")
            .unwrap();
        let DataEvent::Data(data) = event else {
            panic!("not data: {event:?}")
        };
        data
    }

    /// A quote of ESZ26, received by LSE at `ts_recv`, made at `ts_init`.
    fn es_quote(
        (bid, bid_size): (&str, u64),
        (ask, ask_size): (&str, u64),
        ts_recv: u64,
        ts_init: UnixNanos,
    ) -> Data {
        Data::Quote(QuoteTick::new(
            InstrumentId::from(ESZ26),
            Price::from(bid),
            Price::from(ask),
            Quantity::from(bid_size),
            Quantity::from(ask_size),
            UnixNanos::from(ts_recv),
            ts_init,
        ))
    }

    fn ts_init(data: &Data) -> UnixNanos {
        match data {
            Data::Quote(quote) => quote.ts_init,
            Data::Trade(trade) => trade.ts_init,
            other => panic!("neither a quote nor a trade: {other:?}"),
        }
    }

    /// LSE's documented ESZ6 at Level 1: its opening, bid 7569.75 x 2, ask
    /// 7570.00 x 11, comes with the quotes subscription; its next records,
    /// a change of the best bid and offer and a trade, come after: a quote,
    /// then the trade's record's quote and its trade, 1 lot at 7570.00, of
    /// no aggressor side, its id the record's sequence. Each is stamped when
    /// LSE received the record (`ts_recv`), as Nautilus' Databento decoder
    /// stamps it.
    #[tokio::test]
    async fn a_glbx_contract_receives_its_opening_best_bid_and_offer_then_its_quotes_and_trades() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6)]);
        let stream = MockCmeStream::start(|_, _| held(dbn_frames(CME_ESZ6_DOCUMENTED), 4));
        let (mut client, connected, mut events) =
            streaming(&lse, &stream, &[ESZ26], vec![esz26()]).await;
        connected.unwrap();

        client.subscribe_quotes(quotes_of(&[ESZ26])).unwrap();
        client.subscribe_trades(trades_of(&[ESZ26])).unwrap();
        let opening = next(&mut events).await;
        stream.release();
        let live = [
            next(&mut events).await,
            next(&mut events).await,
            next(&mut events).await,
        ];

        assert_eq!(
            opening,
            es_quote(
                ("7569.75", 2),
                ("7570.00", 11),
                1_794_272_217_707_781_000,
                ts_init(&opening)
            )
        );
        assert_eq!(
            live,
            [
                es_quote(
                    ("7569.50", 18),
                    ("7570.00", 12),
                    1_794_272_218_208_093_000,
                    ts_init(&live[0])
                ),
                es_quote(
                    ("7569.50", 18),
                    ("7570.00", 12),
                    1_794_272_218_213_015_000,
                    ts_init(&live[1])
                ),
                Data::Trade(TradeTick::new(
                    InstrumentId::from(ESZ26),
                    Price::from("7570.00"),
                    Quantity::from(1),
                    AggressorSide::NoAggressor,
                    TradeId::new("556"),
                    UnixNanos::from(1_794_272_218_213_015_000),
                    ts_init(&live[2]),
                )),
            ]
        );
        let [lookup] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (
                lookup.path.as_str(),
                lookup.param("symbol"),
                lookup.api_key.as_deref()
            ),
            ("/v3/pairs", Some("ESZ6"), Some(CME_API_KEY))
        );
        assert_eq!(
            stream.received(),
            [(
                0,
                json!({"op": "subscribe", "symbols": ["ESZ6"], "level": 1, "encoding": "dbn"})
            )]
        );
    }

    /// Synthetic: LSE's documented session without its record 555. The gap
    /// is logged and left: the trade after it comes, and nothing stands in
    /// for the record missed.
    #[tokio::test]
    async fn the_records_after_a_gap_come_and_nothing_stands_in_for_those_missed() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_ESZ6)]);
        let stream = MockCmeStream::start(|_, _| {
            let mut frames = dbn_frames(CME_ESZ6_DOCUMENTED);
            frames.remove(4);
            held(frames, 4)
        });
        let (mut client, connected, mut events) =
            streaming(&lse, &stream, &[ESZ26], vec![esz26()]).await;
        connected.unwrap();
        client.subscribe_trades(trades_of(&[ESZ26])).unwrap();

        stream.release();

        let Data::Trade(trade) = next(&mut events).await else {
            panic!("not the trade")
        };
        assert_eq!(trade.trade_id, TradeId::new("556"));
        client.disconnect().await.unwrap();
        assert!(events.try_recv().is_err(), "data beside the trade");
    }

    /// GCZ6 of 2026 streamed from the start; GCG7 of 2027, its next
    /// contract, subscribed later, as a roll subscribes it: looked up, then
    /// carried from a new connection, which opens it with its captured best
    /// bid and offer, bid 3890.0 x 1 and ask 3932.0 x 1.
    #[tokio::test]
    async fn a_contract_subscribed_after_the_start_is_looked_up_and_carried_from_a_new_connection()
    {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::json(PAIRS_GCG7)]);
        let stream = MockCmeStream::start(|_, subscription| {
            let mut frames = frames(CME_GCZ6_GCG7);
            if subscription["symbols"] == json!(["GCZ6"]) {
                frames.truncate(4);
            }
            sent(frames)
        });
        let (mut client, connected, mut events) =
            streaming(&lse, &stream, &[GCZ26], vec![gcz26(), gcg27()]).await;
        connected.unwrap();

        client.subscribe_quotes(quotes_of(&[GCG27])).unwrap();

        let Data::Quote(quote) = next(&mut events).await else {
            panic!("not a quote")
        };
        assert_eq!(
            (
                quote.instrument_id,
                quote.bid_price,
                quote.bid_size,
                quote.ask_price,
                quote.ask_size,
                quote.ts_event
            ),
            (
                InstrumentId::from(GCG27),
                Price::from("3890.00"),
                Quantity::from(1),
                Price::from("3932.00"),
                Quantity::from(1),
                UnixNanos::from(1_794_607_200_675_397_000)
            )
        );
        let subscriptions: Vec<Value> = stream
            .received()
            .into_iter()
            .map(|(_, frame)| frame["symbols"].clone())
            .collect();
        assert_eq!(subscriptions, [json!(["GCZ6"]), json!(["GCG7", "GCZ6"])]);
        let looked_up: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| call.param("symbol").unwrap().to_string())
            .collect();
        assert_eq!(looked_up, ["GCZ6", "GCG7"]);
    }

    /// LSE's captured close of a subscription naming a contract the stream
    /// does not carry.
    #[tokio::test]
    async fn lse_s_refusal_of_the_subscription_fails_the_connect_with_its_reason() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
        let close: Value = serde_json::from_str(CME_SYMBOL_UNAVAILABLE).unwrap();
        let reason = close["reason"].as_str().unwrap().to_string();
        let stream = MockCmeStream::start(move |_, _| vec![CmeAction::Close(1008, reason.clone())]);

        let (_client, connected, _) = streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;

        let err = format!("{:#}", connected.unwrap_err());
        assert!(err.contains("1008 symbol unavailable: NGZ6"), "{err}");
    }

    /// LSE's sockets stream quotes alone.
    #[tokio::test]
    async fn the_trades_of_an_lsedge_series_are_refused_naming_it() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
        let stream = MockCmeStream::start(|_, _| sent(frames(CME_GCZ6_GCG7)));
        let (mut client, connected, _) = streaming(&lse, &stream, &[GCZ26], vec![gcz26()]).await;
        connected.unwrap();

        let err = client
            .subscribe_trades(trades_of(&["GOLD-FUTURES.LSEDGE"]))
            .unwrap_err();

        assert!(
            format!("{err:#}").contains("GOLD-FUTURES.LSEDGE"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn a_glbx_contract_streamed_with_no_cme_key_is_refused_naming_its_variable() {
        let (tx, _events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .stream_ids(ids(&[GCZ26]))
            .build();
        let factory = LseDataClientFactory::new();
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));

        let Err(err) = factory.create(
            factory.name(),
            &config,
            Rc::new(RefCell::new(Cache::default())).into(),
            clock,
        ) else {
            panic!("created a client streaming GCZ26 with no CME key")
        };

        let err = format!("{err:#}");
        assert!(
            err.contains(GCZ26) && err.contains("LSE_CME_API_KEY"),
            "{err}"
        );
    }
}
