//! The data client: Nautilus' `DataClient` on LSE's REST API and sockets.
//!
//! It serves the instruments LSE serves, each asked of its market with its
//! product's key (see `LseInstrument`): an `LSEDGE` series' trades, its
//! tick slices, and bars, LSE's candles 1 second to 4 hours wide, each
//! stamped at its close, by its symbol, with the LSE key; a `GLBX`
//! contract's trades and quotes, CME Globex's Level 1, its book depth, ten
//! levels a side, Level 2, and its book deltas, Level 3, by the number of
//! its listing, looked up once in LSE's catalogue (see `http::listing`),
//! with the CME key; an `XNAS` stock's, Nasdaq TotalView's Levels 1 to 3
//! alike, by its ticker, with the Nasdaq key; and an `OPRA` contract's
//! trades, its tick slices, and
//! bars, its candles, as an `LSEDGE` series', by its OSI symbol, in LSE's
//! US options, with the LSE key. An `LSEDGE` series' or an `OPRA`
//! contract's quotes are asked too, and LSE refuses them, serving their
//! past as timeframes alone; an index's are the values of its tick slice
//! instead, each a quote of no spread and no sizes, as its socket streams
//! them, and its trades are refused, an index having none. Exports are
//! asked a UTC day an export, a book or orders a UTC hour, one download at
//! a time on each key, an export LSE refuses as too wide asked again in
//! halves (see `http::export`). It serves the rows of LSE's reference
//! collections, its macro series of releases and of yields and an
//! instrument's splits and dividends as custom data (`LseRow`) too, its
//! source and symbol named in the metadata of the request's data type,
//! each row stamped when its values are known, with the LSE key (see
//! `http::rows`); its definitions request fails, LSE publishing none; a
//! request of LSE's options chain fails, LSE serving it as it stands
//! alone. Live, it streams the quotes of the `LSEDGE` series
//! its config names, on their sockets, the trades of its `OPRA` contracts,
//! through one subscription of each underlying's chain on data-ws, and, of
//! its `GLBX` contracts and `XNAS` stocks, the quotes and trades, Level 1,
//! the book depth, Level 2, and the book deltas, Level 3, on LSE's CME
//! stream with the CME key and on its Nasdaq stream with the Nasdaq key,
//! which speak one protocol, a connection a level (see `streams` and
//! `exchange`). Every
//! other request and subscription is left to Nautilus' defaults: a request
//! fails as unsupported, a subscription is logged as not implemented. A
//! request is answered from a task, in one completed response envelope, the
//! data engine writing it back to the catalog if asked (`update_catalog`);
//! a failure is the envelope's, LSE's refusal with its code, message and
//! required plan. A request reaching into the span LSE has not loaded yet,
//! the current UTC day and the day before until LSE holds rows of it, is
//! answered with what LSE holds in a plain response, which completes the
//! request and which the data engine writes to no catalog
//! (`DataEngine::response`: "Payload-only replies do not certify coverage
//! for catalog write-back"), as Nautilus' OKX client answers a window it
//! cannot confirm, so that a fill asks for it again; the span is logged.

mod exchange;
mod streams;

use std::{collections::HashMap, future::Future, num::NonZeroUsize, sync::Arc};

use anyhow::{Context, bail, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, execute_instrument_subscriptions},
    live::{dst::time::Duration, runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            BarsResponse, BookDeltasResponse, BookDepthResponse, CustomDataResponse,
            DataResponseEnvelope, QuotesResponse, RequestBars, RequestBookDeltas, RequestBookDepth,
            RequestCustomData, RequestInstrument, RequestQuotes, RequestTrades,
            SubscribeBookDeltas, SubscribeBookDepth, SubscribeQuotes, SubscribeTrades,
            TradesResponse, UnsubscribeBookDeltas, UnsubscribeBookDepth, UnsubscribeQuotes,
            UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    UUID4, UnixNanos,
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{CustomData, CustomDataTrait, DEPTH10_LEN},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};

pub use self::streams::check_streamed;
use self::{exchange::ExchangeAccess, streams::LseStreams};
use crate::{
    common::{
        consts::{CME_MARKET, LseExchange},
        credential::{LseCredential, LseProduct},
        symbol::LseInstrument,
    },
    config::LseDataClientConfig,
    http::{
        client::LseHttpClient,
        export::fetch,
        listing::CmeListings,
        parse::{
            parse_book, parse_candles, parse_index_quotes, parse_orders, parse_quotes,
            parse_trade_slice,
        },
        query::{CandleWidth, ExportQuery, Rows},
        rows::{self, Fetched, RowSource},
    },
    types::{LseRow, SOURCE, SYMBOL},
};

/// How long the client's tasks get to finish at a close, then to abort:
/// its requests', its sockets' ticks' and its exchange streams' records'.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// What reads an export's file into Nautilus data, at an instrument's
/// price and size precisions.
type Reader<T> = fn(Bytes, InstrumentId, u8, u8) -> anyhow::Result<Vec<T>>;

/// The data client: LSE's history on the keys given and its live data on
/// the LSE key, on no venue, so asked by the requests naming it or by the
/// venue routes its node gives it.
#[derive(Debug)]
pub(crate) struct LseDataClient {
    client_id: ClientId,
    /// LSE's API on each key given, by the product it opens.
    http: HashMap<LseProduct, Arc<LseHttpClient>>,
    /// The listings of the CME contracts asked for so far.
    listings: Arc<CmeListings>,
    /// The instruments, whose precisions LSE's prices are converted at.
    cache: CacheView,
    tasks: TaskGroup,
    streams: LseStreams,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    connected: bool,
}

impl LseDataClient {
    /// The client `client_id` on the keys, API and sockets of `config`,
    /// each key given or, if none is, read by `var` from its variable,
    /// `LSE_API_KEY`, `LSE_CME_API_KEY` and `LSE_NASDAQ_API_KEY`, streaming
    /// the instruments it
    /// names on the LSE key; not connected.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variables if no key is given or in
    /// them, if an HTTP client cannot be built, or if an instrument
    /// streamed is not an `LSEDGE` series streamed on a socket, or is
    /// streamed with no LSE key (see `LseStreams::new`). Registers `LseRow`
    /// with Nautilus.
    ///
    /// # Panics
    ///
    /// Panics if the node's data event sender is not set on this thread,
    /// as it is when the node's factory creates the client.
    pub(crate) fn new(
        client_id: ClientId,
        config: &LseDataClientConfig,
        cache: CacheView,
        var: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        let mut credentials = HashMap::new();
        let mut http = HashMap::new();
        for product in LseProduct::ALL {
            let Some(credential) =
                LseCredential::resolve(product, config.api_key_of(product), &var)
            else {
                continue;
            };
            let client = LseHttpClient::new(
                config.http_base_url().to_owned(),
                &credential,
                config.calls_per_minute_of(product),
            )?;
            http.insert(product, Arc::new(client));
            credentials.insert(product, credential);
        }
        ensure!(
            !http.is_empty(),
            "no LSE key: set {}, or give the config's",
            LseProduct::ALL.map(LseProduct::api_key_var).join(" or ")
        );
        // The data engine writes the rows the client answers with back to
        // a catalog by their type's registered encoding.
        LseRow::register();
        let data_sender = get_data_event_sender();
        let clock = get_atomic_clock_realtime();
        let listings = Arc::<CmeListings>::default();
        let mut access = |exchange: LseExchange| {
            let product = exchange.product();
            credentials
                .remove(&product)
                .zip(http.get(&product).cloned())
                .map(|(credential, http)| ExchangeAccess { credential, http })
        };
        let exchanges = (access(LseExchange::Cme), access(LseExchange::Nasdaq));
        let streams = LseStreams::new(
            config,
            credentials.remove(&LseProduct::Global),
            exchanges,
            Arc::clone(&listings),
            data_sender.clone(),
            clock,
        )?;
        Ok(Self {
            client_id,
            http,
            listings,
            cache,
            tasks: TaskGroup::new(),
            streams,
            data_sender,
            clock,
            connected: false,
        })
    }

    /// Answers the request `request_id` with no data and `reason`, its
    /// failure.
    fn fail(&self, request_id: UUID4, reason: String) {
        log::error!("{} request failed: {reason}", self.client_id);
        send(
            &self.data_sender,
            DataEvent::ResponseEnvelope(DataResponseEnvelope::terminal(request_id, Some(reason))),
        );
    }

    /// LSE's API on the key of `product`.
    fn http(&self, product: LseProduct) -> anyhow::Result<Arc<LseHttpClient>> {
        self.http.get(&product).cloned().with_context(|| {
            format!(
                "no {product} key: set {}, or give the config's",
                product.api_key_var()
            )
        })
    }

    /// Runs `future`, a request's, on a task of the client.
    fn spawn_request<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let cancelled = self.tasks.cancellation_token();
        let future = async move {
            tokio::select! {
                biased;
                () = cancelled.cancelled() => {}
                () = future => {}
            }
        };
        if let Err(e) = self.tasks.spawn(future) {
            log::warn!(
                "{} request skipped after shutdown began: {e}",
                self.client_id
            );
        }
    }

    /// Answers the request `request_id` of `id`'s rows over `[start, end]`
    /// (end: now if none), the export of the rows `select` chooses for what
    /// `id` stands for at LSE, each file read by the reader it chooses with
    /// them at the precisions of `id`'s definition (see `answer_export`),
    /// the export asked of the instrument's market with its product's key:
    /// a derived series' by its symbol, a CME contract's by its listing's
    /// number, looked up first (see `http::listing`), a Nasdaq stock's by
    /// its ticker, a US option's by its OSI symbol. A failure is the envelope's, with no rows: an instrument
    /// LSE does not serve, one whose rows `select` refuses, no key of its
    /// product, no definition in the cache or no start, before LSE is
    /// asked; LSE's refusal of the lookup, or as `answer_export`.
    fn answer_rows<T: Send + 'static>(
        &self,
        request_id: UUID4,
        id: InstrumentId,
        (start, end): (Option<Timestamp>, Option<Timestamp>),
        limit: Option<NonZeroUsize>,
        select: impl FnOnce(&LseInstrument) -> anyhow::Result<(Rows, Reader<T>)>,
        respond: impl FnOnce(Vec<T>, UnixNanos) -> DataResponse + Send + 'static,
    ) {
        let now = Timestamp::from(self.clock.get_time_ns());
        let checked = (|| {
            let instrument =
                LseInstrument::from_instrument_id(id, now.to_zoned(TimeZone::UTC).year())?;
            let (rows, read) = select(&instrument)?;
            let http = self.http(instrument.product())?;
            let precisions = precisions(&self.cache, id)?;
            let start = start.with_context(|| format!("a request of {id} needs a start"))?;
            Ok::<_, anyhow::Error>((instrument, rows, read, http, precisions, start))
        })();
        let (instrument, rows, read, http, precisions, start) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request_id, format!("{e:#}"));
                return;
            }
        };
        let (price_precision, size_precision) = precisions;
        // `[start, end]` as LSE takes a span, `[from, to)`.
        let to = end.unwrap_or(now) + SignedDuration::from_nanos(1);
        let listings = Arc::clone(&self.listings);
        let lookup = Arc::clone(&http);
        let query = async move {
            Ok(Some(match instrument {
                LseInstrument::Series(series) => ExportQuery::series(&series, rows, start, to),
                LseInstrument::Cme(contract) => {
                    let listing = listings.listing(&lookup, &contract).await?;
                    ExportQuery::listing(CME_MARKET, listing.pair_id, rows, start, to)
                }
                LseInstrument::Nasdaq(stock) => ExportQuery::stock(&stock, rows, start, to),
                LseInstrument::Opra(contract) => ExportQuery::option(&contract, rows, start, to),
            }))
        };
        self.answer_export(
            request_id,
            http,
            query,
            now,
            limit,
            move |file| read(file, id, price_precision, size_precision),
            respond,
        );
    }

    /// Answers the request `request_id`, from a task, with the rows of the
    /// export `query` gives (see `http::export::fetch`), asked through
    /// `http`, each file read by `read`, the newest `limit` if one is
    /// given, in `respond`'s response of the rows and the time it is made;
    /// no export answers with no rows, asking nothing. The response comes
    /// in one completed response envelope, or, if the export reaches into
    /// the span LSE has not loaded as of `now`, in a plain response, which
    /// completes the request and which the data engine writes to no
    /// catalog, the span logged. A failure, `query`'s, LSE's refusal of an
    /// export or a file not read, is the envelope's, with no rows.
    #[expect(clippy::too_many_arguments, reason = "one request's parts")]
    fn answer_export<T: Send + 'static>(
        &self,
        request_id: UUID4,
        http: Arc<LseHttpClient>,
        query: impl Future<Output = anyhow::Result<Option<ExportQuery>>> + Send + 'static,
        now: Timestamp,
        limit: Option<NonZeroUsize>,
        read: impl Fn(Bytes) -> anyhow::Result<Vec<T>> + Send + Sync + 'static,
        respond: impl FnOnce(Vec<T>, UnixNanos) -> DataResponse + Send + 'static,
    ) {
        let sender = self.data_sender.clone();
        let client_id = self.client_id;
        let clock = self.clock;
        self.spawn_request(async move {
            let fetched = async {
                let Some(query) = query.await? else {
                    return Ok((Vec::new(), None, None));
                };
                let exported = fetch(&http, &query, now).await?;
                let mut rows = Vec::new();
                for (export, file) in exported.files {
                    rows.extend(read(file).with_context(|| format!("reading {export}"))?);
                }
                Ok::<_, anyhow::Error>((rows, exported.unloaded_from, Some(query)))
            }
            .await;
            let event = match fetched {
                Ok((mut rows, unloaded_from, query)) => {
                    if let Some(limit) = limit {
                        let excess = rows.len().saturating_sub(limit.get());
                        rows.drain(..excess);
                    }
                    let count = rows.len();
                    let response = respond(rows, clock.get_time_ns());
                    match (unloaded_from, query) {
                        (Some(from), Some(query)) => {
                            log::warn!(
                                "{client_id}: LSE has not loaded {query} from {from} yet, a UTC \
                                 day being stored the morning after it: answered with the \
                                 {count} rows it holds, written to no catalog"
                            );
                            DataEvent::Response(response)
                        }
                        _ => DataEvent::ResponseEnvelope(DataResponseEnvelope::new(
                            response, true, None,
                        )),
                    }
                }
                Err(e) => {
                    let reason = format!("{e:#}");
                    log::error!("{client_id} request failed: {reason}");
                    DataEvent::ResponseEnvelope(DataResponseEnvelope::terminal(
                        request_id,
                        Some(reason),
                    ))
                }
            };
            send(&sender, event);
        });
    }

    async fn teardown(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        let (requests, streams) = tokio::join!(
            self.tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
            self.streams.close(),
        );
        requests
            .map_err(|e| anyhow::anyhow!("stopping the {} request tasks: {e}", self.client_id))?;
        streams
    }
}

#[async_trait(?Send)]
impl DataClient for LseDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// None: LSE serves several venues, which a node routes to the client.
    fn venue(&self) -> Option<Venue> {
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::debug!("Starting {}", self.client_id);
        Ok(())
    }

    /// Begins the shutdown of the request and tick tasks; `disconnect`, or
    /// the next `connect`, finishes it.
    fn stop(&mut self) -> anyhow::Result<()> {
        log::debug!("Stopping {}", self.client_id);
        self.tasks.begin_shutdown();
        self.streams.begin_shutdown();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    /// Connected: connected, and its tasks not shut down.
    fn is_connected(&self) -> bool {
        self.connected && self.tasks.is_open()
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    /// Opens the sockets of the instruments it streams, none if it streams
    /// none: every other call is a request of its own. Reopens the request
    /// tasks a stop closed. A failure is logged too, as the node logs none
    /// of its reason.
    ///
    /// # Errors
    ///
    /// As `LseStreams::open`: a connection failing, LSE refusing the key,
    /// a symbol a socket does not list or more of them than it allows.
    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if !self.tasks.is_open() {
            self.teardown().await?;
            self.tasks.start_generation().map_err(|e| {
                anyhow::anyhow!("reopening the {} request tasks: {e}", self.client_id)
            })?;
        }
        if let Err(e) = self.streams.open().await {
            log::error!("{} not connected: {e:#}", self.client_id);
            return Err(e);
        }
        self.connected = true;
        log::info!("Connected {}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        let finished = self.teardown().await;
        log::info!("Disconnected {}", self.client_id);
        finished
    }

    /// Streams the quotes of each instrument, of those it streams, at the
    /// precision of its definition in the cache: an `LSEDGE` series' bid
    /// and ask with no sizes, a `GLBX` contract's or an `XNAS` stock's best
    /// bid and offer with LSE's sizes, opening with its latest (see
    /// `LseStreams`).
    ///
    /// # Errors
    ///
    /// Returns an error naming an instrument it does not stream or the
    /// cache does not define.
    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.subscribe_quotes(id, &self.cache)
        })
    }

    /// Ends the quotes of each instrument.
    ///
    /// # Errors
    ///
    /// Returns an error naming an instrument it does not stream.
    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_quotes(id)
        })
    }

    /// Streams the trades of each instrument, of those it streams, at the
    /// precision of its definition in the cache, with no aggressor side: a
    /// `GLBX` contract's on LSE's CME stream, an `XNAS` stock's on its
    /// Nasdaq stream, an `OPRA` contract's through
    /// one subscription of its underlying's chain, every listed contract of
    /// the underlying (see `LseStreams`).
    ///
    /// # Errors
    ///
    /// Returns an error naming an instrument that is none of them, LSE's
    /// sockets streaming a series' ticks as quotes, that it does not stream,
    /// or that the cache does not define.
    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.subscribe_trades(id, &self.cache)
        })
    }

    /// Ends the trades of each instrument, and an `OPRA` contract's
    /// underlying's chain with its last contract's.
    ///
    /// # Errors
    ///
    /// Returns an error naming an instrument that is neither a `GLBX` nor an
    /// `OPRA` contract nor an `XNAS` stock, or that it does not stream.
    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_trades(id)
        })
    }

    /// Streams the book depth of each instrument, a `GLBX` contract's or an
    /// `XNAS` stock's ten levels a side, at the price precision of its
    /// definition in the cache, from its exchange's stream at Level 2,
    /// opening with its last ten levels (see `LseStreams`). The first depth subscription opens the
    /// level's connection; LSE's refusal of it, such as of a key whose plan
    /// lacks Level 2, is logged as an error.
    ///
    /// # Errors
    ///
    /// Returns an error naming it if the depth asked is not ten, the levels
    /// LSE streams, as Nautilus' Databento client serves MBP-10's alone, or
    /// naming an instrument that is neither, that the client does not
    /// stream, or that the cache does not define.
    fn subscribe_book_depth(&mut self, cmd: SubscribeBookDepth) -> anyhow::Result<()> {
        if let Some(depth) = cmd.depth.filter(|depth| depth.get() != DEPTH10_LEN) {
            bail!(
                "{}: LSE's exchange streams hold ten levels a side at Level 2, not a depth of \
                 {depth}",
                cmd.instrument_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.subscribe_book_depth(id, &self.cache)
        })
    }

    /// Ends the book depth of each instrument.
    ///
    /// # Errors
    ///
    /// Returns an error naming an instrument that is neither a `GLBX`
    /// contract nor an `XNAS` stock, or that is not subscribed.
    fn unsubscribe_book_depth(&mut self, cmd: &UnsubscribeBookDepth) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_book_depth(id)
        })
    }

    /// Streams the book deltas of each instrument, a `GLBX` contract's or an
    /// `XNAS` stock's order events, at the price precision of its definition
    /// in the cache, from its exchange's stream at Level 3, which opens with
    /// no book, so its
    /// deltas start at its first order event, each exchange message's
    /// ending `F_LAST` (see `LseStreams`). The first deltas subscription
    /// opens the level's connection; LSE's refusal of it, such as of a key
    /// whose plan lacks Level 3, is logged as an error. The book type asked
    /// is not read: LSE's orders are Level 3, as Nautilus' Databento client
    /// serves MBO for any.
    ///
    /// # Errors
    ///
    /// Returns an error naming an instrument that is neither a `GLBX`
    /// contract nor an `XNAS` stock, that the client does not stream, or that
    /// the cache does not define.
    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.subscribe_book_deltas(id, &self.cache)
        })
    }

    /// Ends the book deltas of each instrument.
    ///
    /// # Errors
    ///
    /// Returns an error naming an instrument that is neither a `GLBX`
    /// contract nor an `XNAS` stock, or that is not subscribed.
    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_book_deltas(id)
        })
    }

    /// Fails naming the instruments, in the response envelope: LSE
    /// publishes no definitions. The data engine asks the client only for
    /// one its catalogs lack (`DataEngine::dispatch_instrument_request`).
    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let ids: Vec<String> = request
            .instrument_ids
            .iter()
            .map(ToString::to_string)
            .collect();
        self.fail(
            request.request_id,
            format!(
                "{}: LSE publishes no instrument definitions; each must come from a catalog",
                ids.join(", ")
            ),
        );
        Ok(())
    }

    /// Answers with the trades over `[start, end]` (end: now if none), the
    /// newest `limit` if one is given (see `answer_rows`): an `LSEDGE`
    /// series' or an `OPRA` contract's tick slices, a `GLBX` contract's or an
    /// `XNAS` stock's trades, each with no aggressor side, LSE sending none. An index's
    /// fail naming it before LSE is asked: an index has no trades, its
    /// ticks being its values, served as quotes.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one instrument.
    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let id = request.single_instrument_id()?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let (request_id, params) = (request.request_id, request.params);
        let (start, end) = (
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
        );
        self.answer_rows(
            request_id,
            id,
            (request.start, request.end),
            request.limit,
            |instrument| match instrument {
                LseInstrument::Series(series) if series.is_index() => bail!(
                    "{id} is an index, which has no trades: its ticks are quotes, its candles \
                     bars"
                ),
                LseInstrument::Series(_) | LseInstrument::Opra(_) => {
                    Ok((Rows::TradeSlice, parse_trade_slice))
                }
                LseInstrument::Cme(_) | LseInstrument::Nasdaq(_) => {
                    Ok((Rows::Trades, parse_trade_slice))
                }
            },
            move |trades, ts_init| {
                DataResponse::Trades(TradesResponse::new(
                    request_id, client_id, id, trades, start, end, ts_init, params,
                ))
            },
        );
        Ok(())
    }

    /// Answers with the quotes over `[start, end]` (end: now if none), the
    /// newest `limit` if one is given (see `answer_rows`), with the sizes
    /// LSE sends: a `GLBX` contract's or an `XNAS` stock's best bid and
    /// offer, CME Globex's or Nasdaq TotalView's. LSE
    /// refuses an `LSEDGE` series' and an `OPRA` contract's (`400
    /// data-unavailable`), serving their past as timeframes alone, but for
    /// an index, whose quotes are the values of its tick slice, each a quote
    /// whose bid and ask are the value, with no sizes, as its socket streams
    /// them (`http::parse::parse_index_quotes`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one instrument.
    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let id = request.single_instrument_id()?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let (request_id, params) = (request.request_id, request.params);
        let (start, end) = (
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
        );
        self.answer_rows(
            request_id,
            id,
            (request.start, request.end),
            request.limit,
            |instrument| {
                Ok(match instrument {
                    LseInstrument::Series(series) if series.is_index() => {
                        (Rows::TradeSlice, parse_index_quotes)
                    }
                    _ => (Rows::Quotes, parse_quotes),
                })
            },
            move |quotes, ts_init| {
                DataResponse::Quotes(QuotesResponse::new(
                    request_id, client_id, id, quotes, start, end, ts_init, params,
                ))
            },
        );
        Ok(())
    }

    /// Answers with the depth snapshots over `[start, end]` (end: now if
    /// none), the newest `limit` if one is given (see `answer_rows`), ten
    /// levels a side: a `GLBX` contract's or an `XNAS` stock's book, CME
    /// Globex's or Nasdaq TotalView's Level 2, each
    /// row of it the book after its event, the last of each time ending
    /// the event (`http::parse::parse_book`), asked a UTC hour an export. A
    /// depth other than ten, the levels of LSE's book, fails naming it
    /// before LSE is asked, as Nautilus' Databento client serves MBP-10's
    /// alone. LSE refuses a key whose plan does not sell Level 2
    /// (`level-required`) and an `LSEDGE` series'.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one instrument.
    fn request_book_depth(&self, request: RequestBookDepth) -> anyhow::Result<()> {
        let id = request.single_instrument_id()?;
        if let Some(depth) = request.depth.filter(|depth| depth.get() != DEPTH10_LEN) {
            self.fail(
                request.request_id,
                format!("{id}: LSE's book holds ten levels a side, not a depth of {depth}"),
            );
            return Ok(());
        }
        let client_id = request.client_id.unwrap_or(self.client_id);
        let (request_id, params) = (request.request_id, request.params);
        let (start, end) = (
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
        );
        self.answer_rows(
            request_id,
            id,
            (request.start, request.end),
            request.limit,
            |_| Ok((Rows::Book, parse_book)),
            move |depths, ts_init| {
                DataResponse::BookDepth(BookDepthResponse::new(
                    request_id, client_id, id, depths, start, end, ts_init, params,
                ))
            },
        );
        Ok(())
    }

    /// Answers with the book deltas over `[start, end]` (end: now if none),
    /// the newest `limit` if one is given (see `answer_rows`): a `GLBX`
    /// contract's or an `XNAS` stock's orders, CME Globex's or Nasdaq
    /// TotalView's Level 3, each order added, changed or
    /// removed a delta, the last of each time ending the event
    /// (`http::parse::parse_orders`), asked a UTC hour an export. LSE
    /// refuses a key whose plan does not sell Level 3 (`level-required`)
    /// and an `LSEDGE` series'.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one instrument.
    fn request_book_deltas(&self, request: RequestBookDeltas) -> anyhow::Result<()> {
        let id = request.single_instrument_id()?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let (request_id, params) = (request.request_id, request.params);
        let (start, end) = (
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
        );
        self.answer_rows(
            request_id,
            id,
            (request.start, request.end),
            request.limit,
            |_| Ok((Rows::Orders, parse_orders)),
            move |deltas, ts_init| {
                DataResponse::BookDeltas(BookDeltasResponse::new(
                    request_id, client_id, id, deltas, start, end, ts_init, params,
                ))
            },
        );
        Ok(())
    }

    /// Answers with the rows of the source and the series the request's
    /// data type names (`LseRow::data_type`: `source` and `symbol` in its
    /// metadata) known over `[start, end]` (end: now if none), each stamped
    /// when its values are known, the newest `limit` if one is given, each
    /// an `LseRow` of that data type, in one completed response envelope;
    /// or in a plain response, logging the span, if LSE may not have loaded
    /// them all yet (see `http::rows`). Its sources are LSE's reference
    /// collections, its macro series of releases and of yields, and an
    /// instrument's splits and dividends, LSE's factors. A failure, the
    /// data type not `LseRow`'s or naming no source or symbol, a source the
    /// client does not stamp, LSE's macro observations or its options chain
    /// (`options_chain`), which LSE serves as it stands alone, with no
    /// history, refused before LSE is asked, or LSE's refusal of a call, is
    /// the envelope's, with no rows: LSE's refusal carries its code,
    /// message and required plan.
    fn request_data(&self, request: RequestCustomData) -> anyhow::Result<()> {
        let data_type = request.data_type.clone();
        let checked = (|| {
            let rows = LseRow::type_name_static();
            if data_type.type_name() != rows {
                bail!(
                    "{} serves no custom data {}: only {rows}",
                    self.client_id,
                    data_type.type_name()
                );
            }
            let named = |key| {
                data_type
                    .metadata()
                    .and_then(|metadata| metadata.get_str(key))
                    .with_context(|| format!("{data_type:?} names no {key} in its metadata"))
            };
            let source = RowSource::of(named(SOURCE)?, named(SYMBOL)?)?;
            let start = request
                .start
                .with_context(|| format!("a request of {data_type} needs a start"))?;
            let http = self.http(LseProduct::Global)?;
            Ok::<_, anyhow::Error>((source, start, http))
        })();
        let (source, start, http) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request.request_id, format!("{e:#}"));
                return Ok(());
            }
        };
        let now = Timestamp::from(self.clock.get_time_ns());
        let end = request.end.unwrap_or(now);
        let sender = self.data_sender.clone();
        let client_id = request.client_id;
        let clock = self.clock;
        self.spawn_request(async move {
            let event = match rows::fetch(&http, &source, start, end, now).await {
                Ok(Fetched {
                    rows,
                    unloaded_from,
                }) => {
                    let mut rows: Vec<_> = rows
                        .into_iter()
                        .map(|row| CustomData::new(Arc::new(row), data_type.clone()))
                        .collect();
                    if let Some(limit) = request.limit {
                        let excess = rows.len().saturating_sub(limit.get());
                        rows.drain(..excess);
                    }
                    let count = rows.len();
                    let response = DataResponse::Data(CustomDataResponse::new(
                        request.request_id,
                        client_id,
                        None,
                        data_type.clone(),
                        rows,
                        datetime_to_unix_nanos(request.start),
                        datetime_to_unix_nanos(request.end),
                        clock.get_time_ns(),
                        request.params,
                    ));
                    match unloaded_from {
                        None => DataEvent::ResponseEnvelope(DataResponseEnvelope::new(
                            response, true, None,
                        )),
                        Some(from) => {
                            log::warn!(
                                "{client_id}: LSE may not have loaded {source} known over \
                                 [{from}, {end}] yet, a date being stored the morning after it \
                                 and a release filled once out: answered with the {count} rows \
                                 it holds, written to no catalog"
                            );
                            DataEvent::Response(response)
                        }
                    }
                }
                Err(e) => {
                    let reason = format!("{e:#}");
                    log::error!("{client_id} request failed: {reason}");
                    DataEvent::ResponseEnvelope(DataResponseEnvelope::terminal(
                        request.request_id,
                        Some(reason),
                    ))
                }
            };
            send(&sender, event);
        });
        Ok(())
    }

    /// Answers with the bars of the bar type that close over `[start, end]`
    /// (end: now if none), the newest `limit` if one is given, from LSE's
    /// candles of the instrument (see `http::export`), an `LSEDGE` series'
    /// by its symbol or an `OPRA` contract's by its OSI symbol, with the
    /// LSE key, in one completed response envelope, or in a plain response,
    /// logging the span, if the request reaches into the span LSE has not
    /// loaded yet. LSE stamps a candle at its open; its bar is stamped at
    /// its close, as Nautilus' adapters stamp bars by default (Databento's
    /// `bars_timestamp_on_close`). Only whole candles are asked, LSE folding
    /// a candle from the rows inside the export alone, and none if no bar
    /// closes in the window. A failure, the instrument neither an `LSEDGE`
    /// series nor an `OPRA` contract, the bars not of LSE's candles, the
    /// instrument not in the cache or LSE's refusal of an export, is the
    /// envelope's, with no bars.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one bar type.
    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.single_bar_type()?;
        let id = bar_type.instrument_id();
        let now = Timestamp::from(self.clock.get_time_ns());
        let end = request.end.unwrap_or(now);
        let checked = (|| {
            let instrument =
                LseInstrument::from_instrument_id(id, now.to_zoned(TimeZone::UTC).year())?;
            let width = CandleWidth::of(bar_type)?;
            let start = request
                .start
                .with_context(|| format!("a request of the {bar_type} bars needs a start"))?;
            let candles = width.candles_closing(start, end);
            let query = match instrument {
                LseInstrument::Series(series) => candles
                    .map(|(from, to)| ExportQuery::series(&series, Rows::Candles(width), from, to)),
                LseInstrument::Opra(contract) => candles.map(|(from, to)| {
                    ExportQuery::option(&contract, Rows::Candles(width), from, to)
                }),
                LseInstrument::Cme(contract) => bail!(
                    "{bar_type}: the client asks LSE for the candles of LSEDGE series and OPRA \
                     contracts, not of {contract}"
                ),
                LseInstrument::Nasdaq(stock) => bail!(
                    "{bar_type}: the client asks LSE for the candles of LSEDGE series and OPRA \
                     contracts, not of {stock}, a Nasdaq TotalView stock"
                ),
            };
            let precisions = precisions(&self.cache, id)?;
            let http = self.http(LseProduct::Global)?;
            Ok::<_, anyhow::Error>((query, precisions, http))
        })();
        let (query, (price_precision, size_precision), http) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request.request_id, format!("{e:#}"));
                return Ok(());
            }
        };
        let client_id = request.client_id.unwrap_or(self.client_id);
        let (request_id, params) = (request.request_id, request.params);
        let (start, end) = (
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
        );
        self.answer_export(
            request_id,
            http,
            async move { Ok(query) },
            now,
            request.limit,
            move |file| parse_candles(file, bar_type, price_precision, size_precision),
            move |bars, ts_init| {
                DataResponse::Bars(BarsResponse::new(
                    request_id, client_id, bar_type, bars, start, end, ts_init, params,
                ))
            },
        );
        Ok(())
    }
}

/// The price and size precisions of `id`, its definition's in `cache`.
///
/// # Errors
///
/// Returns an error naming it if `cache` has no definition of it: LSE
/// publishes none, and its prices are read at the definition's precision.
fn precisions(cache: &CacheView, id: InstrumentId) -> anyhow::Result<(u8, u8)> {
    let cache = cache.borrow();
    let instrument = cache.instrument(&id).with_context(|| {
        format!(
            "{id} is not in the cache: LSE publishes no definitions, and its prices are read at \
             the definition's precision, so it must come from elsewhere, a catalog or a broker's \
             client"
        )
    })?;
    Ok((instrument.price_precision(), instrument.size_precision()))
}

/// Sends `event` to the node; a closed channel means the node is stopping.
fn send(sender: &EventSender<DataEvent>, event: DataEvent) {
    if let Err(e) = sender.send(event) {
        log::warn!("data event not sent: {e}");
    }
}

#[cfg(test)]
mod tests {
    //! The client made with no environment (`LseDataClient::new`, `var`
    //! answering nothing), which its factory, reading the process's, does
    //! not let a test inject: its keys are the config's alone. Every other
    //! test of the client goes through its factory, in `tests/`.

    use std::{cell::RefCell, num::NonZeroU32, rc::Rc};

    use nautilus_common::{cache::Cache, live::runner::replace_data_event_sender};
    use nautilus_core::string::secret::SecretString;
    use nautilus_model::instruments::InstrumentAny;
    use tokio::sync::mpsc;

    use super::*;
    use crate::testing::{
        API_KEY, CME_API_KEY, MockLse, NASDAQ_API_KEY, NVDA, NVDA_TRADES, PAIRS_GCZ6, Reply, gcz26,
        nvda,
    };

    type Events = mpsc::UnboundedReceiver<DataEvent>;

    /// A request of a window, as Nautilus' constructors of them take it.
    type WindowRequest<R> = fn(
        InstrumentId,
        Option<Timestamp>,
        Option<Timestamp>,
        Option<NonZeroUsize>,
        Option<ClientId>,
        UUID4,
        UnixNanos,
        Option<nautilus_core::Params>,
    ) -> R;

    /// The config of a client on `lse`, with every key.
    fn config(lse: &MockLse) -> LseDataClientConfig {
        LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .cme_api_key(SecretString::from(CME_API_KEY))
            .nasdaq_api_key(SecretString::from(NASDAQ_API_KEY))
            .base_url_http(lse.url().to_string())
            .calls_per_minute(NonZeroU32::new(6000).unwrap())
            .cme_calls_per_minute(NonZeroU32::new(6000).unwrap())
            .nasdaq_calls_per_minute(NonZeroU32::new(6000).unwrap())
            .build()
    }

    fn cache_of(instruments: Vec<InstrumentAny>) -> CacheView {
        let mut cache = Cache::default();
        for instrument in instruments {
            cache.add_instrument(instrument).unwrap();
        }
        Rc::new(RefCell::new(cache)).into()
    }

    /// The node's data events, as the client sends them.
    fn events() -> Events {
        let (tx, events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        events
    }

    /// The next event, a completed response envelope.
    async fn next_envelope(events: &mut Events) -> DataResponseEnvelope {
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), events.recv())
            .await
            .expect("no data event within 10 s")
            .expect("the data events ended");
        let DataEvent::ResponseEnvelope(envelope) = event else {
            panic!("not a response envelope: {event:?}");
        };
        assert!(envelope.completed());
        envelope
    }

    /// The request of `id` over the second from 14:30 on 12 November 2026.
    fn second_of<R>(id: &str, request: WindowRequest<R>) -> R {
        request(
            InstrumentId::from(id),
            Some("2026-11-12T14:30:00Z".parse().unwrap()),
            Some("2026-11-12T14:30:00.999999999Z".parse().unwrap()),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn gcz26_second<R>(request: WindowRequest<R>) -> R {
        second_of(crate::testing::GCZ26, request)
    }

    /// The client holds the LSE key alone, and the environment none.
    #[tokio::test]
    async fn a_cme_contract_asked_with_no_cme_key_fails_naming_its_variable_unasked() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6)]);
        let mut events = events();
        let config = LseDataClientConfig {
            cme_api_key: None,
            ..config(&lse)
        };
        let client = LseDataClient::new(
            ClientId::from("LSE"),
            &config,
            cache_of(vec![gcz26()]),
            |_| None,
        )
        .unwrap();

        client
            .request_trades(gcz26_second(RequestTrades::new))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        assert!(reason.contains("LSE_CME_API_KEY"), "{reason}");
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
    }

    #[tokio::test]
    async fn a_client_with_no_key_at_all_is_not_created_naming_every_variable() {
        let lse = MockLse::start(vec![]);
        let config = LseDataClientConfig {
            api_key: None,
            cme_api_key: None,
            nasdaq_api_key: None,
            ..config(&lse)
        };

        let err = LseDataClient::new(ClientId::from("LSE"), &config, cache_of(vec![]), |_| None)
            .unwrap_err()
            .to_string();

        for variable in ["LSE_API_KEY", "LSE_CME_API_KEY", "LSE_NASDAQ_API_KEY"] {
            assert!(err.contains(variable), "{variable}: {err}");
        }
    }

    /// The client holds the LSE and CME keys, and the environment none.
    #[tokio::test]
    async fn an_xnas_stock_asked_with_no_nasdaq_key_fails_naming_its_variable_unasked() {
        let lse = MockLse::start(vec![Reply::file(NVDA_TRADES)]);
        let mut events = events();
        let config = LseDataClientConfig {
            nasdaq_api_key: None,
            ..config(&lse)
        };
        let client = LseDataClient::new(
            ClientId::from("LSE"),
            &config,
            cache_of(vec![nvda()]),
            |_| None,
        )
        .unwrap();

        client
            .request_trades(second_of(NVDA, RequestTrades::new))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        assert!(reason.contains("LSE_NASDAQ_API_KEY"), "{reason}");
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
    }
}
