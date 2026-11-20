//! The data client: Nautilus' `DataClient` on LSE's REST API and sockets.
//!
//! It serves the instruments LSE serves, each asked of its market with its product's key (see
//! `LseInstrument`):
//! - An `LSEDGE` series: trades (its tick slices) and bars (LSE's candles, 1 second to 4 hours
//!   wide, each stamped at its close), by symbol, with the LSE key.
//! - A `GLBX` contract: trades and quotes (CME Globex's Level 1), book depth (ten levels a side,
//!   Level 2) and book deltas (Level 3), by its listing's number, looked up once in LSE's catalogue
//!   (see `http::listing`), with the CME key.
//! - An `XNAS` stock: Nasdaq TotalView's Levels 1 to 3 alike, by ticker, with the Nasdaq key.
//! - An `OPRA` contract: trades (tick slices) and bars (candles) as an `LSEDGE` series', by OSI
//!   symbol, in LSE's US options, with the LSE key.
//!
//! LSE refuses the quotes of an `LSEDGE` series and of an `OPRA` contract, serving their past as
//! timeframes alone. An index's quotes are the values of its tick slice, each a quote of no spread
//! and no sizes, as its socket streams them, and its trades are refused, an index having none.
//!
//! Exports are asked a UTC day per export, and a book or orders a UTC hour, one download at a time
//! on each key. An export LSE refuses as too wide is asked again in halves (see `http::export`).
//!
//! It also serves the rows of LSE's reference collections as custom data (`LseRow`): its macro
//! series of releases and of yields, and an instrument's splits and dividends. The request's data
//! type names the source and symbol in its metadata, each row is stamped when its values are known,
//! and the LSE key is used (see `http::rows`). A definitions request fails, LSE publishing none,
//! and so does a request of LSE's options chain, which LSE serves as it stands, with no history.
//!
//! Live, it streams:
//! - The quotes of the `LSEDGE` series its config names, on their sockets.
//! - The trades of its `OPRA` contracts, through one subscription of each underlying's chain on
//!   data-ws.
//! - The quotes and trades (Level 1), book depth (Level 2) and book deltas (Level 3) of its `GLBX`
//!   contracts and `XNAS` stocks, on LSE's CME stream with the CME key and its Nasdaq stream with
//!   the Nasdaq key. The two streams speak one protocol, with a connection a level (see `streams`
//!   and `exchange`).
//!
//! Every other request and subscription is left to Nautilus' defaults: a request fails as
//! unsupported, and a subscription is logged as not implemented.
//!
//! A request is answered from a task in one completed response envelope, which the data engine
//! writes back to the catalog if asked (`update_catalog`). A failure is the envelope's, carrying
//! LSE's refusal with its code, message and required plan.
//!
//! A request reaching into the span LSE has not loaded yet, the current UTC day and the day before
//! until LSE holds rows of it, is answered with what LSE holds in a plain response. That response
//! completes the request, and the data engine writes it to no catalog (`DataEngine::response`:
//! "Payload-only replies do not certify coverage for catalog write-back"), as Nautilus' OKX client
//! answers a window it cannot confirm, so a fill asks for it again. The span is logged.

mod exchange;
mod served;
mod streams;

use std::{
    cell::RefCell, collections::HashMap, future::Future, num::NonZeroUsize, rc::Rc, sync::Arc,
};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, execute_instrument_subscriptions},
    clock::Clock,
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
    data::{CustomData, DEPTH10_LEN},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};

use self::{exchange::ExchangeAccess, streams::LseStreams};
pub use self::{
    served::{serves, serves_custom},
    streams::{streams, streams_custom},
};
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
        rows::{self, Fetched},
    },
};

/// How long the client's tasks get to finish at a close, then to abort: its requests', its sockets'
/// ticks' and its exchange streams' records'.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// Reads an export's file into Nautilus data, at an instrument's price and size precisions.
type Reader<T> = fn(Bytes, InstrumentId, u8, u8) -> anyhow::Result<Vec<T>>;

/// The data client: LSE's history on the keys given and its live data on the LSE key, on no venue,
/// so it is asked by the requests naming it or by the venue routes its node gives it.
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
    /// The node's clock: the time a request is asked at, which judges what LSE has loaded.
    node_clock: Rc<RefCell<dyn Clock>>,
    /// Nautilus' realtime clock, which a request's task stamps its response by and the streams
    /// their data.
    clock: &'static AtomicTime,
    connected: bool,
}

impl LseDataClient {
    // panics-doc-ok (transitive via get_data_event_sender)
    /// Creates the client `client_id` on the keys, API and sockets of `config`, not connected.
    ///
    /// Each key is the config's or, if none is given, read by `var` from its variable:
    /// `LSE_API_KEY`, `LSE_CME_API_KEY` or `LSE_NASDAQ_API_KEY`. The instruments the config names
    /// stream on the LSE key, and requests are asked at the times `node_clock` reads.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No key is given or in the variables, naming them.
    /// - An HTTP client cannot be built.
    /// - An instrument streamed is not an `LSEDGE` series streamed on a socket, or is streamed with
    ///   no LSE key (see `LseStreams::new`).
    ///
    /// # Panics
    ///
    /// Panics if the node's data event sender is not set on this thread, as it is when the node's
    /// factory creates the client.
    pub(crate) fn new(
        client_id: ClientId,
        config: &LseDataClientConfig,
        cache: CacheView,
        node_clock: Rc<RefCell<dyn Clock>>,
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
        anyhow::ensure!(
            !http.is_empty(),
            "no LSE key: set {}, or give the config's",
            LseProduct::ALL.map(LseProduct::api_key_var).join(" or ")
        );
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
            &listings,
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
            node_clock,
            clock,
            connected: false,
        })
    }

    /// Returns now, on the node's clock.
    fn now(&self) -> Timestamp {
        self.node_clock.borrow().utc_now()
    }

    /// Answers the request `request_id` with no data and `reason`, its failure.
    fn fail(&self, request_id: UUID4, reason: String) {
        log::error!("{} request failed: {reason}", self.client_id);
        send(
            &self.data_sender,
            DataEvent::ResponseEnvelope(DataResponseEnvelope::terminal(request_id, Some(reason))),
        );
    }

    /// Returns LSE's API on the key of `product`.
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

    /// Answers the request `request_id` of `id`'s rows over `[start, end]` (end: now if none).
    ///
    /// `select` chooses the export of the rows for what `id` stands for at LSE, and the reader of
    /// each file, at the precisions of `id`'s definition (see `answer_export`). The export is asked
    /// of the instrument's market with its product's key: a derived series' by its symbol, a CME
    /// contract's by its listing's number (looked up first, see `http::listing`), a Nasdaq stock's
    /// by its ticker, a US option's by its OSI symbol.
    ///
    /// A failure is the envelope's, with no rows. It comes before LSE is asked if the instrument is
    /// not served, `select` refuses its rows, its product has no key, the cache has no definition
    /// or the request has no start. Otherwise it is LSE's refusal of the lookup, or as
    /// `answer_export`.
    fn answer_rows<T: Send + 'static>(
        &self,
        request_id: UUID4,
        id: InstrumentId,
        (start, end): (Option<Timestamp>, Option<Timestamp>),
        limit: Option<NonZeroUsize>,
        select: impl FnOnce(&LseInstrument) -> anyhow::Result<(Rows, Reader<T>)>,
        respond: impl FnOnce(Vec<T>, UnixNanos) -> DataResponse + Send + 'static,
    ) {
        let now = self.now();
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

        // `[start, end]` as LSE takes a span, `[from, to)`
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

    /// Answers the request `request_id` from a task, with the rows of the export `query` gives (see
    /// `http::export::fetch`).
    ///
    /// The export is asked through `http` and each file read by `read`, keeping the newest `limit`
    /// if given. `respond` builds the response from the rows and the time it is made. No export
    /// answers with no rows, asking nothing.
    ///
    /// The response is one completed response envelope. If the export reaches into the span LSE has
    /// not loaded as of `now`, it is a plain response instead, which completes the request, is
    /// written to no catalog by the data engine, and logs the span.
    ///
    /// A failure is the envelope's, with no rows: `query`'s, LSE's refusal of an export, or a file
    /// not read.
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

    /// Returns `None`: LSE serves several venues, which a node routes to the client.
    fn venue(&self) -> Option<Venue> {
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::debug!("Starting {}", self.client_id);
        Ok(())
    }

    /// Begins the shutdown of the request and tick tasks, which `disconnect` or the next `connect`
    /// finishes.
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

    /// Returns whether the client is connected and its tasks are not shut down.
    fn is_connected(&self) -> bool {
        self.connected && self.tasks.is_open()
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    /// Opens the sockets of the instruments it streams, none if it streams none, and reopens the
    /// request tasks a stop closed.
    ///
    /// Every other call is a request of its own. A failure is logged too, as the node logs none of
    /// its reason.
    ///
    /// # Errors
    ///
    /// Returns an error if the request tasks a stop closed cannot be reopened, or if
    /// `LseStreams::open` fails: a connection fails, LSE refuses the key, a symbol is not listed on
    /// a socket, or more symbols are asked than a socket allows.
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

    /// Streams the quotes of each instrument, at the precision of its definition in the cache,
    /// opening with its latest (see `LseStreams`).
    ///
    /// An `LSEDGE` series' quote has a bid and ask with no sizes. A `GLBX` contract's or an `XNAS`
    /// stock's is its best bid and offer with LSE's sizes.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument, which it names, is not streamed by the client or not
    /// defined in the cache.
    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.subscribe_quotes(id, &self.cache)
        })
    }

    /// Ends the quotes of each instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument, which it names, is not streamed by the client.
    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_quotes(id)
        })
    }

    /// Streams the trades of each instrument, at the precision of its definition in the cache, with
    /// no aggressor side (see `LseStreams`).
    ///
    /// A `GLBX` contract's trades come on LSE's CME stream, an `XNAS` stock's on its Nasdaq stream,
    /// and an `OPRA` contract's through one subscription of its underlying's chain, every listed
    /// contract of the underlying.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument, which it names, is an `LSEDGE` series, LSE's sockets
    /// streaming a series' ticks as quotes, or is not streamed by the client or not defined in the
    /// cache.
    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.subscribe_trades(id, &self.cache)
        })
    }

    /// Ends the trades of each instrument, and an `OPRA` contract's underlying's chain with its
    /// last contract's.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument, which it names, is neither a `GLBX` nor an `OPRA`
    /// contract nor an `XNAS` stock, or is not streamed by the client.
    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_trades(id)
        })
    }

    /// Streams the book depth of each instrument, at the price precision of its definition in the
    /// cache, from its exchange's stream at Level 2, opening with its last ten levels (see
    /// `LseStreams`).
    ///
    /// A `GLBX` contract's or an `XNAS` stock's depth is ten levels a side. The first depth
    /// subscription opens the level's connection, and LSE's refusal of it, such as of a key whose
    /// plan lacks Level 2, is logged as an error.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The depth asked is not ten, the levels LSE streams, as Nautilus' Databento client serves
    ///   MBP-10's alone, naming the instruments.
    /// - An instrument, which it names, is neither a `GLBX` contract nor an `XNAS` stock, or is not
    ///   streamed by the client or not defined in the cache.
    fn subscribe_book_depth(&mut self, cmd: SubscribeBookDepth) -> anyhow::Result<()> {
        if let Some(depth) = cmd.depth.filter(|depth| depth.get() != DEPTH10_LEN) {
            anyhow::bail!(
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
    /// Returns an error if an instrument, which it names, is neither a `GLBX` contract nor an
    /// `XNAS` stock, or is not subscribed.
    fn unsubscribe_book_depth(&mut self, cmd: &UnsubscribeBookDepth) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_book_depth(id)
        })
    }

    /// Streams the book deltas of each instrument, a `GLBX` contract's or an `XNAS` stock's order
    /// events, at the price precision of its definition in the cache, from its exchange's stream at
    /// Level 3 (see `LseStreams`).
    ///
    /// The stream opens with no book, so its deltas start at its first order event, each exchange
    /// message's ending `F_LAST`. The first deltas subscription opens the level's connection, and
    /// LSE's refusal of it, such as of a key whose plan lacks Level 3, is logged as an error. The
    /// book type asked is not read: LSE's orders are Level 3, as Nautilus' Databento client serves
    /// MBO for any.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument, which it names, is neither a `GLBX` contract nor an
    /// `XNAS` stock, or is not streamed by the client or not defined in the cache.
    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.subscribe_book_deltas(id, &self.cache)
        })
    }

    /// Ends the book deltas of each instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument, which it names, is neither a `GLBX` contract nor an
    /// `XNAS` stock, or is not subscribed.
    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.streams.unsubscribe_book_deltas(id)
        })
    }

    /// Fails naming the instruments, in the response envelope: LSE publishes no definitions.
    ///
    /// The data engine asks the client only for one its catalogs lack
    /// (`DataEngine::dispatch_instrument_request`).
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

    /// Answers with the trades over `[start, end]` (end: now if none), keeping the newest `limit`
    /// if given (see `answer_rows`).
    ///
    /// An `LSEDGE` series' or an `OPRA` contract's trades are its tick slices, and a `GLBX`
    /// contract's or an `XNAS` stock's its trades, each with no aggressor side, LSE sending none.
    /// An index's request fails naming it before LSE is asked: an index has no trades, its ticks
    /// being its values, served as quotes.
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
                LseInstrument::Series(series) if series.is_index() => anyhow::bail!(
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

    /// Answers with the quotes over `[start, end]` (end: now if none), keeping the newest `limit`
    /// if given (see `answer_rows`), with the sizes LSE sends.
    ///
    /// A `GLBX` contract's or an `XNAS` stock's quotes are its best bid and offer, CME Globex's or
    /// Nasdaq TotalView's. LSE refuses an `LSEDGE` series' and an `OPRA` contract's (`400
    /// data-unavailable`), serving their past as timeframes alone, but for an index, whose quotes
    /// are the values of its tick slice, each a quote whose bid and ask are the value, with no
    /// sizes, as its socket streams them (`http::parse::parse_index_quotes`).
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

    /// Answers with the depth snapshots over `[start, end]` (end: now if none), keeping the newest
    /// `limit` if given (see `answer_rows`), ten levels a side.
    ///
    /// A `GLBX` contract's or an `XNAS` stock's depth is CME Globex's or Nasdaq TotalView's Level 2
    /// book, asked a UTC hour an export. Each row is the book after its event, the last of each
    /// time ending the event (`http::parse::parse_book`). A depth other than ten, the levels of
    /// LSE's book, fails naming it before LSE is asked, as Nautilus' Databento client serves
    /// MBP-10's alone. LSE refuses a key whose plan does not sell Level 2 (`level-required`) and an
    /// `LSEDGE` series'.
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

    /// Answers with the book deltas over `[start, end]` (end: now if none), keeping the newest
    /// `limit` if given (see `answer_rows`).
    ///
    /// A `GLBX` contract's or an `XNAS` stock's deltas are CME Globex's or Nasdaq TotalView's Level
    /// 3 orders, asked a UTC hour an export. Each order added, changed or removed is a delta, the
    /// last of each time ending the event (`http::parse::parse_orders`). LSE refuses a key whose
    /// plan does not sell Level 3 (`level-required`) and an `LSEDGE` series'.
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

    /// Answers with the rows of the source and the series the request's data type names (`source`
    /// and `symbol` in its metadata, see `served::row_source`), known over `[start, end]` (end: now
    /// if none), keeping the newest `limit` if given.
    ///
    /// Each row is an `LseRow` of the data type as asked, stamped when its values are known, with
    /// the caller's identifier. The response is one completed response envelope, which the data
    /// engine writes back under that identifier, or a plain response, logging the span, if LSE may
    /// not have loaded all rows yet (see `http::rows`).
    ///
    /// The sources are LSE's reference collections: its macro series of releases and of yields, and
    /// an instrument's splits and dividends, LSE's factors.
    ///
    /// A failure is the envelope's, with no rows. It comes before LSE is asked if the data type is
    /// not `LseRow`'s, names no source or symbol or another key, names a source the client does not
    /// stamp, or asks LSE's macro observations or options chain (`options_chain`), which LSE serves
    /// as it stands, with no history (`serves_custom` checks the same). Otherwise it is LSE's
    /// refusal of a call, carrying its code, message and required plan.
    fn request_data(&self, request: RequestCustomData) -> anyhow::Result<()> {
        let data_type = request.data_type.clone();
        let checked = (|| {
            let source = served::row_source(&data_type)?;
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
        let now = self.now();
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

    /// Answers with the bars of the bar type that close over `[start, end]` (end: now if none),
    /// keeping the newest `limit` if given.
    ///
    /// The bars are built from LSE's candles of the instrument (see `http::export`), an `LSEDGE`
    /// series' by its symbol or an `OPRA` contract's by its OSI symbol, with the LSE key. The
    /// response is one completed response envelope, or a plain response, logging the span, if the
    /// request reaches into the span LSE has not loaded yet.
    ///
    /// LSE stamps a candle at its open, and its bar is stamped at its close, as Nautilus' adapters
    /// stamp bars by default (Databento's `bars_timestamp_on_close`). Only whole candles are asked,
    /// LSE folding a candle from the rows inside the export alone, and none if no bar closes in the
    /// window.
    ///
    /// A failure is the envelope's, with no bars: the instrument is neither an `LSEDGE` series nor
    /// an `OPRA` contract, the bars are not of LSE's candles, the instrument is not in the cache,
    /// or LSE refuses an export.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one bar type.
    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.single_bar_type()?;
        let id = bar_type.instrument_id();
        let now = self.now();
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
                LseInstrument::Cme(contract) => anyhow::bail!(
                    "{bar_type}: the client asks LSE for the candles of LSEDGE series and OPRA \
                     contracts, not of {contract}"
                ),
                LseInstrument::Nasdaq(stock) => anyhow::bail!(
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

/// Returns the price and size precisions of `id`, its definition's in `cache`.
///
/// # Errors
///
/// Returns an error if `cache` has no definition of `id`, naming it: LSE publishes none, and its
/// prices are read at the definition's precision.
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

/// Sends `event` to the node, a closed channel meaning the node is stopping.
fn send(sender: &EventSender<DataEvent>, event: DataEvent) {
    if let Err(e) = sender.send(event) {
        log::warn!("data event not sent: {e}");
    }
}

#[cfg(test)]
mod tests {
    //! The client made with no environment (`LseDataClient::new`, `var` answering nothing), which
    //! its factory, reading the process's, does not let a test inject, so its keys are the config's
    //! alone. Every other test of the client goes through its factory, in `tests/`.

    use std::{cell::RefCell, num::NonZeroU32, rc::Rc};

    use nautilus_common::{
        cache::Cache, clock::VirtualClock, live::runner::replace_data_event_sender,
    };
    use nautilus_core::string::secret::SecretString;
    use nautilus_model::instruments::InstrumentAny;
    use rstest::rstest;

    use super::*;
    use crate::testing::{
        API_KEY, CME_API_KEY, MockLse, NASDAQ_API_KEY, NVDA, NVDA_TRADES, PAIRS_GCZ6, Reply, gcz26,
        nvda,
    };

    type Events = tokio::sync::mpsc::UnboundedReceiver<DataEvent>;

    /// A request of a window, as Nautilus' constructors take it.
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

    /// The node's clock, Nautilus' virtual clock, at 12:00 on 16 November 2026, days after the
    /// second the tests ask for.
    fn node_clock() -> Rc<RefCell<dyn Clock>> {
        let clock = VirtualClock::new();
        clock.set_time(UnixNanos::from(
            "2026-11-16T12:00:00Z".parse::<Timestamp>().unwrap(),
        ));
        Rc::new(RefCell::new(clock))
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
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
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

    /// The config of a client on `lse` with every key but `product`'s.
    fn config_without(lse: &MockLse, product: LseProduct) -> LseDataClientConfig {
        let mut config = config(lse);
        match product {
            LseProduct::Global => config.api_key = None,
            LseProduct::Cme => config.cme_api_key = None,
            LseProduct::Nasdaq => config.nasdaq_api_key = None,
        }
        config
    }

    /// The client holds every key but the instrument's product's, and the environment none. LSE
    /// would answer the request, the contract's listings or the stock's trades, were it asked.
    #[rstest]
    #[case::a_cme_contract(
        LseProduct::Cme,
        gcz26(),
        crate::testing::GCZ26,
        Reply::json(PAIRS_GCZ6),
        "LSE_CME_API_KEY"
    )]
    #[case::an_xnas_stock(
        LseProduct::Nasdaq,
        nvda(),
        NVDA,
        Reply::file(NVDA_TRADES),
        "LSE_NASDAQ_API_KEY"
    )]
    #[tokio::test]
    async fn an_instrument_asked_with_no_key_of_its_product_fails_naming_its_variable_unasked(
        #[case] product: LseProduct,
        #[case] instrument: InstrumentAny,
        #[case] id: &str,
        #[case] answer: Reply,
        #[case] variable: &str,
    ) {
        let lse = MockLse::start(vec![answer]);
        let mut events = events();
        let client = LseDataClient::new(
            ClientId::from("LSE"),
            &config_without(&lse, product),
            cache_of(vec![instrument]),
            node_clock(),
            |_| None,
        )
        .unwrap();

        client
            .request_trades(second_of(id, RequestTrades::new))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        assert!(reason.contains(variable), "{reason}");
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
    }

    #[rstest]
    fn a_client_with_no_key_at_all_is_not_created_naming_every_variable() {
        let err = LseDataClient::new(
            ClientId::from("LSE"),
            &LseDataClientConfig::default(),
            cache_of(vec![]),
            node_clock(),
            |_| None,
        )
        .unwrap_err()
        .to_string();

        for variable in ["LSE_API_KEY", "LSE_CME_API_KEY", "LSE_NASDAQ_API_KEY"] {
            assert!(err.contains(variable), "{variable}: {err}");
        }
    }

    /// The client streams the instrument holding every key but its product's, the config's alone,
    /// the environment holding none.
    #[rstest]
    #[case::a_glbx_contract(
        crate::testing::GCZ26,
        LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .nasdaq_api_key(SecretString::from(NASDAQ_API_KEY))
            .stream_ids(vec![InstrumentId::from(crate::testing::GCZ26)])
            .build(),
        "LSE_CME_API_KEY"
    )]
    #[case::an_xnas_stock(
        NVDA,
        LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .cme_api_key(SecretString::from(CME_API_KEY))
            .stream_ids(vec![InstrumentId::from(NVDA)])
            .build(),
        "LSE_NASDAQ_API_KEY"
    )]
    fn an_instrument_streamed_with_no_key_of_its_product_is_refused_naming_its_variable(
        #[case] id: &str,
        #[case] config: LseDataClientConfig,
        #[case] variable: &str,
    ) {
        let _events = events();

        let Err(e) = LseDataClient::new(
            ClientId::from("LSE"),
            &config,
            cache_of(vec![]),
            node_clock(),
            |_| None,
        ) else {
            panic!("created a client streaming {id} with no key of its product")
        };

        let err = format!("{e:#}");
        assert!(err.contains(id) && err.contains(variable), "{err}");
    }
}
