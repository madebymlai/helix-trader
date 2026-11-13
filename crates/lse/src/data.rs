//! The data client: Nautilus' `DataClient` on LSE's REST API.
//!
//! It serves the instruments LSE serves, each asked of its market with its
//! product's key (see `LseInstrument`): an `LSEDGE` series' trades, its
//! tick slices, and bars, LSE's candles 1 second to 4 hours wide, each
//! stamped at its close, by its symbol, with the LSE key; a `GLBX`
//! contract's trades and quotes, CME Globex's Level 1, by the number of its
//! listing, looked up once in LSE's catalogue (see `http::listing`), with
//! the CME key. An `LSEDGE` series' quotes are asked too, and LSE refuses
//! them, serving a derived market's past as timeframes alone. Exports are
//! asked a UTC day an export, one download at a time on each key, an
//! export LSE refuses as too wide asked again in halves (see
//! `http::export`). It serves the rows of LSE's reference collections as
//! custom data (`LseRow`) too, its source and symbol named in the metadata
//! of the request's data type, with the LSE key; its definitions request
//! fails, LSE publishing none. Every other request and every
//! subscription is left to Nautilus' defaults: a request fails as
//! unsupported, a subscription is logged as not implemented. A request is
//! answered from a task, in one completed response envelope, the data
//! engine writing it back to the catalog if asked (`update_catalog`); a
//! failure is the envelope's, LSE's refusal with its code, message and
//! required plan. A request of trades, quotes or bars reaching into the span LSE
//! has not loaded yet, the current UTC day and the day before until LSE
//! holds rows of it, is answered with what LSE holds in a plain response,
//! which completes the request and which the data engine writes to no
//! catalog (`DataEngine::response`: "Payload-only replies do not certify
//! coverage for catalog write-back"), as Nautilus' OKX client answers a
//! window it cannot confirm, so that a fill asks for it again; the span is
//! logged.

use std::{collections::HashMap, future::Future, num::NonZeroUsize, sync::Arc};

use anyhow::{Context, bail, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp, civil::Date, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    live::{dst::time::Duration, runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            BarsResponse, CustomDataResponse, DataResponseEnvelope, QuotesResponse, RequestBars,
            RequestCustomData, RequestInstrument, RequestQuotes, RequestTrades, TradesResponse,
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
    data::{CustomData, CustomDataTrait},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};

use crate::{
    common::{
        consts::CME_MARKET,
        credential::{LseCredential, LseProduct},
        symbol::{LseInstrument, LseSymbol},
    },
    config::LseDataClientConfig,
    http::{
        client::LseHttpClient,
        export::fetch,
        listing::CmeListings,
        parse::{parse_candles, parse_quotes, parse_rows, parse_trade_slice},
        query::{CandleWidth, ExportQuery, ReferenceQuery, Rows},
    },
    types::{LseRow, SOURCE, SYMBOL},
};

/// How long the request tasks get to finish at a disconnect, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// The data client: LSE's history on the keys given, on no venue, so asked
/// by the requests naming it or by the venue routes its node gives it.
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
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    connected: bool,
}

impl LseDataClient {
    /// The client `client_id` on the keys and API of `config`, each key
    /// given or, if none is, read by `var` from its variable,
    /// `LSE_API_KEY` and `LSE_CME_API_KEY`; not connected.
    ///
    /// # Errors
    ///
    /// Returns an error naming the variables if no key is given or in
    /// them, or if an HTTP client cannot be built. Registers `LseRow` with
    /// Nautilus.
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
        }
        ensure!(
            !http.is_empty(),
            "no LSE key: set {}, or give the config's",
            LseProduct::ALL.map(LseProduct::api_key_var).join(" or ")
        );
        // The data engine writes the rows the client answers with back to
        // a catalog by their type's registered encoding.
        LseRow::register();
        Ok(Self {
            client_id,
            http,
            listings: Arc::default(),
            cache,
            tasks: TaskGroup::new(),
            data_sender: get_data_event_sender(),
            clock: get_atomic_clock_realtime(),
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

    /// The price and size precisions of `id`, the instrument's in the cache.
    fn precisions(&self, id: InstrumentId) -> anyhow::Result<(u8, u8)> {
        let cache = self.cache.borrow();
        let instrument = cache.instrument(&id).with_context(|| {
            format!(
                "{id} is not in the cache: LSE publishes no definitions, and its prices are \
                 read at the definition's precision, so it must come from elsewhere, a catalog"
            )
        })?;
        Ok((instrument.price_precision(), instrument.size_precision()))
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
    /// (end: now if none), the export of the rows `rows` selects of what
    /// `id` stands for at LSE, each file read by `read` at the precisions
    /// of `id`'s definition (see `answer_export`), the export asked of the
    /// instrument's market with its product's key: a derived series' by its
    /// symbol, a CME contract's by its listing's number, looked up first
    /// (see `http::listing`). A failure is the envelope's, with no rows: an
    /// instrument LSE does not serve, no key of its product, no definition
    /// in the cache or no start, before LSE is asked; LSE's refusal of the
    /// lookup, or as `answer_export`.
    #[expect(clippy::too_many_arguments, reason = "one request's parts")]
    fn answer_rows<T: Send + 'static>(
        &self,
        request_id: UUID4,
        id: InstrumentId,
        (start, end): (Option<Timestamp>, Option<Timestamp>),
        limit: Option<NonZeroUsize>,
        rows: impl FnOnce(&LseInstrument) -> Rows,
        read: impl Fn(Bytes, InstrumentId, u8, u8) -> anyhow::Result<Vec<T>> + Send + Sync + 'static,
        respond: impl FnOnce(Vec<T>, UnixNanos) -> DataResponse + Send + 'static,
    ) {
        let now = Timestamp::from(self.clock.get_time_ns());
        let checked = (|| {
            let instrument =
                LseInstrument::from_instrument_id(id, now.to_zoned(TimeZone::UTC).year())?;
            let http = self.http(instrument.product())?;
            let precisions = self.precisions(id)?;
            let start = start.with_context(|| format!("a request of {id} needs a start"))?;
            Ok::<_, anyhow::Error>((instrument, http, precisions, start))
        })();
        let (instrument, http, (price_precision, size_precision), start) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request_id, format!("{e:#}"));
                return;
            }
        };
        let rows = rows(&instrument);
        // `[start, end]` as LSE takes a span, `[from, to)`.
        let to = end.unwrap_or(now) + SignedDuration::from_nanos(1);
        let listings = Arc::clone(&self.listings);
        let lookup = Arc::clone(&http);
        let query = async move {
            Ok(Some(match instrument {
                LseInstrument::Series(series) => ExportQuery::series(&series, rows, start, to),
                LseInstrument::Cme(contract) => {
                    let pair_id = listings.pair_id(&lookup, &contract).await?;
                    ExportQuery::listing(CME_MARKET, pair_id, rows, start, to)
                }
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
        self.tasks
            .finish_shutdown(TASK_GRACE, TASK_FORCE)
            .await
            .map_err(|e| anyhow::anyhow!("stopping the {} request tasks: {e}", self.client_id))
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

    /// Begins the shutdown of the request tasks; `disconnect`, or the next
    /// `connect`, finishes it.
    fn stop(&mut self) -> anyhow::Result<()> {
        log::debug!("Stopping {}", self.client_id);
        self.tasks.begin_shutdown();
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

    /// Nothing to open: every call is a request of its own. Reopens the
    /// request tasks a stop closed.
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
    /// series' tick slices, a `GLBX` contract's trades, each with no
    /// aggressor side, LSE sending none.
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
                LseInstrument::Series(_) => Rows::TradeSlice,
                LseInstrument::Cme(_) => Rows::Trades,
            },
            parse_trade_slice,
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
    /// LSE sends: a `GLBX` contract's, CME Globex's best bid and offer. LSE
    /// refuses an `LSEDGE` series' (`400 data-unavailable`), serving a
    /// derived market's past as timeframes alone.
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
            |_| Rows::Quotes,
            parse_quotes,
            move |quotes, ts_init| {
                DataResponse::Quotes(QuotesResponse::new(
                    request_id, client_id, id, quotes, start, end, ts_init, params,
                ))
            },
        );
        Ok(())
    }

    /// Answers with the rows of the reference collection and the symbol
    /// the request's data type names (`LseRow::data_type`: `source` and
    /// `symbol` in its metadata) whose time is in `[start, end]` (end: now
    /// if none), the newest `limit` if one is given, each an `LseRow` of
    /// that data type, in one completed response envelope. LSE is asked
    /// its catalogue, for the column of the collection's time, then the
    /// collection's export over the UTC dates the window spans. A failure,
    /// the data type not `LseRow`'s or naming no source or symbol, the
    /// source no collection LSE lists, or LSE's refusal of a call, is the
    /// envelope's, with no rows: LSE's refusal carries its code, message
    /// and required plan.
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
                    .map(str::to_owned)
                    .with_context(|| format!("{data_type:?} names no {key} in its metadata"))
            };
            let start = request
                .start
                .with_context(|| format!("a request of {data_type} needs a start"))?;
            let http = self.http(LseProduct::Global)?;
            Ok::<_, anyhow::Error>((named(SOURCE)?, named(SYMBOL)?, start, http))
        })();
        let (source, symbol, start, http) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request.request_id, format!("{e:#}"));
                return Ok(());
            }
        };
        let end = request
            .end
            .unwrap_or_else(|| Timestamp::from(self.clock.get_time_ns()));
        let sender = self.data_sender.clone();
        let client_id = request.client_id;
        let clock = self.clock;
        self.spawn_request(async move {
            let window = UnixNanos::from(start)..=UnixNanos::from(end);
            let fetched = async {
                let collections = http
                    .reference_collections()
                    .await
                    .context("reading LSE's reference catalogue")?;
                let collection = collections
                    .into_iter()
                    .find(|collection| collection.dataset == source)
                    .with_context(|| format!("LSE lists no reference collection {source}"))?;
                let query = ReferenceQuery::new(&source, &symbol, utc_date(start), utc_date(end));
                let file = http
                    .reference_export(&query)
                    .await
                    .with_context(|| format!("exporting {query}"))?;
                let rows = match file {
                    Some(file) => parse_rows(file, &source, &symbol, &collection.time_column)
                        .with_context(|| format!("reading {query}"))?,
                    None => Vec::new(),
                };
                Ok::<_, anyhow::Error>(
                    rows.into_iter()
                        .filter(|row| window.contains(&row.ts_init))
                        .map(|row| CustomData::new(Arc::new(row), data_type.clone()))
                        .collect::<Vec<_>>(),
                )
            }
            .await;
            let envelope = match fetched {
                Ok(mut rows) => {
                    if let Some(limit) = request.limit {
                        let excess = rows.len().saturating_sub(limit.get());
                        rows.drain(..excess);
                    }
                    let response = CustomDataResponse::new(
                        request.request_id,
                        client_id,
                        None,
                        data_type,
                        rows,
                        datetime_to_unix_nanos(request.start),
                        datetime_to_unix_nanos(request.end),
                        clock.get_time_ns(),
                        request.params,
                    );
                    DataResponseEnvelope::new(DataResponse::Data(response), true, None)
                }
                Err(e) => {
                    let reason = format!("{e:#}");
                    log::error!("{client_id} request failed: {reason}");
                    DataResponseEnvelope::terminal(request.request_id, Some(reason))
                }
            };
            send(&sender, DataEvent::ResponseEnvelope(envelope));
        });
        Ok(())
    }

    /// Answers with the bars of the bar type that close over `[start, end]`
    /// (end: now if none), the newest `limit` if one is given, from LSE's
    /// candles of the instrument's series (see `http::export`), in one
    /// completed response envelope, or in a plain response, logging the
    /// span, if the request reaches into the span LSE has not loaded yet.
    /// LSE stamps a candle at its open; its bar is stamped at its close, as
    /// Nautilus' adapters stamp bars by default (Databento's
    /// `bars_timestamp_on_close`). Only whole candles are asked, LSE folding
    /// a candle from the rows inside the export alone, and none if no bar
    /// closes in the window. A failure, the series not an LSEDGE one, the
    /// bars not of LSE's candles, the instrument not in the cache or LSE's
    /// refusal of an export, is the envelope's, with no bars.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one bar type.
    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.single_bar_type()?;
        let id = bar_type.instrument_id();
        let checked = (|| {
            let series = LseSymbol::from_instrument_id(id)?;
            let width = CandleWidth::of(bar_type)?;
            let precisions = self.precisions(id)?;
            let start = request
                .start
                .with_context(|| format!("a request of the {bar_type} bars needs a start"))?;
            let http = self.http(LseProduct::Global)?;
            Ok::<_, anyhow::Error>((series, width, precisions, start, http))
        })();
        let (series, width, (price_precision, size_precision), start, http) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request.request_id, format!("{e:#}"));
                return Ok(());
            }
        };
        let now = Timestamp::from(self.clock.get_time_ns());
        let end = request.end.unwrap_or(now);
        let query = width
            .candles_closing(start, end)
            .map(|(from, to)| ExportQuery::candles(&series, width, from, to));
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

/// The UTC date of `at`.
fn utc_date(at: Timestamp) -> Date {
    at.to_zoned(TimeZone::UTC).date()
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

    use jiff::Timestamp;
    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        factories::DataClientFactory,
        live::runner::replace_data_event_sender,
        messages::{
            DataResponse,
            data::{
                CustomDataResponse, DataResponseEnvelope, RequestCustomData, RequestFundingRates,
            },
        },
    };
    use nautilus_core::{Params, UUID4, UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{BarType, CustomData, CustomDataTrait, DataType, HasTsInit},
        identifiers::InstrumentId,
        instruments::InstrumentAny,
        types::{Price, Quantity},
    };
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        LseDataClientFactory, LseRow,
        testing::{
            API_KEY, CME_API_KEY, DATA_UNAVAILABLE, DATA_UNAVAILABLE_QUOTES,
            DATA_UNAVAILABLE_TODAY, DATE_NOT_TRADING, GCZ26, GCZ26_QUOTES, GCZ26_TRADES,
            GOLD_FUTURES, GOLD_FUTURES_4H, GOLD_SPOT_MINUTES, GOLD_TRADES, HISTORY_LIMIT, MockLse,
            PAIRS_GCZ6, PLAN_REQUIRED, REFERENCE_CATALOGUE, REFERENCE_DATA_UNAVAILABLE, Reply,
            SYMBOL_UNKNOWN, US10Y_BOND_YIELDS, gcz26, gold_futures, gold_spot,
        },
    };

    type Events = mpsc::UnboundedReceiver<DataEvent>;

    /// The client the factory creates on `lse`, its cache holding the
    /// definitions of `GOLD_FUTURES` and `GOLD_SPOT` if `defined`,
    /// connected, and the node's data events.
    async fn connected(lse: &MockLse, defined: bool) -> (Box<dyn DataClient>, Events) {
        let (tx, events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let mut cache = Cache::default();
        if defined {
            cache.add_instrument(gold_futures()).unwrap();
            cache.add_instrument(gold_spot()).unwrap();
        }
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .base_url_http(lse.url().to_string())
            .calls_per_minute(std::num::NonZeroU32::new(6000).unwrap())
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
        client.connect().await.unwrap();
        (client, events)
    }

    /// A config of the client on `lse` with both keys, calls unthrottled.
    fn config(lse: &MockLse) -> LseDataClientConfig {
        LseDataClientConfig {
            api_key: Some(SecretString::from(API_KEY)),
            cme_api_key: Some(SecretString::from(CME_API_KEY)),
            base_url_http: Some(lse.url().to_string()),
            calls_per_minute: NonZeroU32::new(6000).unwrap(),
            cme_calls_per_minute: NonZeroU32::new(6000).unwrap(),
        }
    }

    /// The cache holding `instruments`.
    fn cache_of(instruments: Vec<InstrumentAny>) -> CacheView {
        let mut cache = Cache::default();
        for instrument in instruments {
            cache.add_instrument(instrument).unwrap();
        }
        Rc::new(RefCell::new(cache)).into()
    }

    /// The node's data events, from now on.
    fn events() -> Events {
        let (tx, events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        events
    }

    /// The client the factory creates from `config`, its cache holding
    /// `instruments`, connected, and the node's data events.
    async fn connected_with(
        config: LseDataClientConfig,
        instruments: Vec<InstrumentAny>,
    ) -> (Box<dyn DataClient>, Events) {
        let events = events();
        let factory = LseDataClientFactory::new();
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
        let mut client = factory
            .create(factory.name(), &config, cache_of(instruments), clock)
            .unwrap();
        client.connect().await.unwrap();
        (client, events)
    }

    async fn next_event(events: &mut Events) -> DataEvent {
        tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("no data event within 10 s")
            .expect("the data events ended")
    }

    /// The next event, a completed response envelope.
    async fn next_envelope(events: &mut Events) -> DataResponseEnvelope {
        let event = next_event(events).await;
        let DataEvent::ResponseEnvelope(envelope) = event else {
            panic!("not a response envelope: {event:?}");
        };
        assert!(envelope.completed());
        envelope
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    /// The trades of `GOLD_FUTURES` over `[from, to]`, as the data engine
    /// asks for a window, naming no client.
    fn trades(from: &str, to: &str) -> RequestTrades {
        RequestTrades::new(
            InstrumentId::from(GOLD_FUTURES),
            Some(at(from)),
            Some(at(to)),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    /// The captured second, as the data engine asks for it.
    fn captured_second() -> RequestTrades {
        trades("2026-10-21T10:00:00Z", "2026-10-21T10:00:00.999999999Z")
    }

    fn trades_of(envelope: &DataResponseEnvelope) -> TradesResponse {
        match envelope.response() {
            Some(DataResponse::Trades(response)) => response.clone(),
            other => panic!("not a trades response: {other:?}"),
        }
    }

    /// The bars of `bar_type` closing over `[from, to]`, as the data
    /// engine asks for a window, naming no client.
    fn bars(bar_type: &str, from: &str, to: &str) -> RequestBars {
        RequestBars::new(
            BarType::from(bar_type),
            Some(at(from)),
            Some(at(to)),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn bars_of(envelope: &DataResponseEnvelope) -> BarsResponse {
        match envelope.response() {
            Some(DataResponse::Bars(response)) => response.clone(),
            other => panic!("not a bars response: {other:?}"),
        }
    }

    /// The `from` and `to` of each call `lse` received.
    fn asked_windows(lse: &MockLse) -> Vec<(String, String)> {
        lse.calls()
            .iter()
            .map(|call| {
                (
                    call.param("from").unwrap().to_string(),
                    call.param("to").unwrap().to_string(),
                )
            })
            .collect()
    }

    const GOLD_SPOT_MINUTE_BARS: &str = "XAU/USD-COMMODITIES.LSEDGE-1-MINUTE-LAST-EXTERNAL";

    #[tokio::test]
    async fn the_factory_creates_the_client_named_lse_on_no_venue() {
        let lse = MockLse::start(vec![]);

        let (client, _) = connected(&lse, true).await;

        assert_eq!(client.client_id(), ClientId::from("LSE"));
        assert_eq!(client.venue(), None);
        assert!(client.is_connected());
    }

    /// LSE publishes no definitions: the data engine asks the client for
    /// one only when its catalogs lack it.
    #[tokio::test]
    async fn an_instrument_request_fails_naming_the_instrument_unasked() {
        let lse = MockLse::start(vec![]);
        let (client, mut events) = connected(&lse, false).await;
        let request = RequestInstrument::new(
            InstrumentId::from(GOLD_FUTURES),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let id = request.request_id;

        client.request_instrument(request).unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(*envelope.correlation_id(), id);
        assert!(envelope.response().is_none());
        let reason = envelope.failure_reason().expect("a failure");
        for part in [GOLD_FUTURES, "LSE publishes no instrument definitions"] {
            assert!(reason.contains(part), "{part}: {reason}");
        }
        assert!(lse.calls().is_empty());
    }

    #[tokio::test]
    async fn a_trades_request_is_answered_with_lse_s_trades_in_one_complete_envelope() {
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
        let (client, mut events) = connected(&lse, true).await;
        let request = captured_second();
        let id = request.request_id;

        client.request_trades(request).unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        let response = trades_of(&envelope);
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.client_id, ClientId::from("LSE"));
        assert_eq!(response.data.len(), 6);
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (call.param("from"), call.param("to")),
            (Some("2026-10-21T10:00:00Z"), Some("2026-10-21T10:00:01Z"))
        );
    }

    /// Nautilus' adapter guide, "Connection lifecycle": a stopped client
    /// connects again.
    #[tokio::test]
    async fn a_stopped_client_connects_again_and_answers_requests() {
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
        let (mut client, mut events) = connected(&lse, true).await;

        client.stop().unwrap();
        assert!(!client.is_connected());
        client.connect().await.unwrap();

        assert!(client.is_connected());
        client.request_trades(captured_second()).unwrap();
        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        assert_eq!(trades_of(&envelope).data.len(), 6);
    }

    /// 23:00 on the 20th to 00:59:59.999999999 on the 21st: the 20th's
    /// hour, which LSE holds no session of, then the 21st's.
    #[tokio::test]
    async fn a_window_over_midnight_is_asked_a_utc_day_at_a_time() {
        let lse = MockLse::start(vec![
            Reply::refusal(404, DATE_NOT_TRADING),
            Reply::file(GOLD_TRADES),
        ]);
        let (client, mut events) = connected(&lse, true).await;

        client
            .request_trades(trades(
                "2026-10-20T23:00:00Z",
                "2026-10-21T00:59:59.999999999Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        assert_eq!(trades_of(&envelope).data.len(), 6);
        let asked: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| {
                (
                    call.param("from").unwrap().to_string(),
                    call.param("to").unwrap().to_string(),
                )
            })
            .collect();
        let day = |from: &str, to: &str| (from.to_string(), to.to_string());
        assert_eq!(
            asked,
            [
                day("2026-10-20T23:00:00Z", "2026-10-21T00:00:00Z"),
                day("2026-10-21T00:00:00Z", "2026-10-21T01:00:00Z"),
            ]
        );
    }

    /// A day as the data engine asks for one, its window closed at the
    /// next midnight: one export, through the midnight's instant.
    #[tokio::test]
    async fn a_day_window_ending_at_a_midnight_is_one_export() {
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
        let (client, mut events) = connected(&lse, true).await;

        client
            .request_trades(trades("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z"))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (call.param("from"), call.param("to")),
            (
                Some("2026-10-20T00:00:00Z"),
                Some("2026-10-21T00:00:00.000000001Z")
            )
        );
    }

    /// From the start of the day before until now: LSE sends the day
    /// before's trades and none of the current day's, which it stores the
    /// morning after. A plain response completes the request, and the data
    /// engine writes it to no catalog (`DataEngine::response`), so that a
    /// fill asks for the window again.
    #[tokio::test]
    async fn a_trades_request_reaching_into_the_day_lse_has_not_loaded_is_answered_unwritten() {
        let lse = MockLse::start(vec![
            Reply::file(GOLD_TRADES),
            Reply::refusal(404, DATA_UNAVAILABLE_TODAY),
            // Should the request come after midnight.
            Reply::refusal(404, DATA_UNAVAILABLE_TODAY),
        ]);
        let (client, mut events) = connected(&lse, true).await;
        let today = Timestamp::from_second(Timestamp::now().as_second() / 86_400 * 86_400).unwrap();
        let day_before = today - SignedDuration::from_hours(24);
        let request = RequestTrades::new(
            InstrumentId::from(GOLD_FUTURES),
            Some(day_before),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let id = request.request_id;

        client.request_trades(request).unwrap();

        let event = next_event(&mut events).await;
        let DataEvent::Response(DataResponse::Trades(response)) = event else {
            panic!("not a plain trades response: {event:?}");
        };
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.data.len(), 6);
        let day_before = day_before.to_string();
        assert_eq!(
            lse.calls().first().and_then(|call| call.param("from")),
            Some(day_before.as_str())
        );
    }

    /// Prices are converted at the instrument's precision, which LSE does
    /// not give.
    #[tokio::test]
    async fn a_trades_request_of_an_instrument_not_in_the_cache_fails_naming_it_unasked() {
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
        let (client, mut events) = connected(&lse, false).await;

        client.request_trades(captured_second()).unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        assert!(reason.contains(GOLD_FUTURES), "{reason}");
        assert!(lse.calls().is_empty());
    }

    #[tokio::test]
    async fn lse_s_refusal_is_the_request_s_failure_with_its_code_message_and_required_plan() {
        for (status, refusal, parts) in [
            (
                403,
                PLAN_REQUIRED,
                &[
                    "plan-required",
                    "This feed requires a subscription",
                    "CME Group Lite",
                ][..],
            ),
            (
                404,
                SYMBOL_UNKNOWN,
                &["symbol-unknown", "symbol is not available"][..],
            ),
        ] {
            let lse = MockLse::start(vec![Reply::refusal(status, refusal)]);
            let (client, mut events) = connected(&lse, true).await;

            client.request_trades(captured_second()).unwrap();

            let envelope = next_envelope(&mut events).await;
            let reason = envelope.failure_reason().expect("a failure");
            for part in parts {
                assert!(reason.contains(part), "{part}: {reason}");
            }
        }
    }

    /// US10Y's daily bond yields over `[from, to]`, as the data engine asks
    /// for them, naming the client.
    fn bond_yields(from: &str, to: &str) -> RequestCustomData {
        rows_of(LseRow::data_type("bond_yields", "US10Y"), from, to)
    }

    /// The custom data of `data_type` over `[from, to]`.
    fn rows_of(data_type: DataType, from: &str, to: &str) -> RequestCustomData {
        RequestCustomData::new(
            ClientId::from("LSE"),
            data_type,
            Some(at(from)),
            Some(at(to)),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    fn rows_in(envelope: &DataResponseEnvelope) -> CustomDataResponse {
        match envelope.response() {
            Some(DataResponse::Data(response)) => response.clone(),
            other => panic!("not a custom data response: {other:?}"),
        }
    }

    fn custom_data(response: &CustomDataResponse) -> Vec<CustomData> {
        response
            .data
            .downcast_ref::<Vec<CustomData>>()
            .expect("custom data")
            .clone()
    }

    fn day(date: &str) -> UnixNanos {
        UnixNanos::from(at(&format!("{date}T00:00:00Z")))
    }

    /// The captured rows: of the 9th, 10th and 12th, each at its date.
    #[tokio::test]
    async fn a_custom_data_request_is_answered_with_lse_s_rows_in_one_complete_envelope() {
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);
        let (client, mut events) = connected(&lse, false).await;
        let mut request = bond_yields("2026-11-09T00:00:00Z", "2026-11-12T23:59:59.999999999Z");
        let params = Params::new([("update_catalog".to_string(), serde_json::json!(true))].into());
        request.params = Some(params.clone());
        let id = request.request_id;

        client.request_data(request).unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        let response = rows_in(&envelope);
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.params, Some(params));
        let data_type = LseRow::data_type("bond_yields", "US10Y");
        assert_eq!(response.data_type, data_type);
        let rows = custom_data(&response);
        assert!(rows.iter().all(|row| row.data_type == data_type));
        let times: Vec<_> = rows.iter().map(CustomData::ts_init).collect();
        assert_eq!(
            times,
            [day("2026-11-09"), day("2026-11-10"), day("2026-11-12")]
        );
        let [catalogue, export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(catalogue.path, "/v3/reference");
        assert_eq!(export.path, "/v3/reference/export");
        assert_eq!(
            (
                export.param("dataset"),
                export.param("symbol"),
                export.param("from"),
                export.param("to")
            ),
            (
                Some("bond_yields"),
                Some("US10Y"),
                Some("2026-11-09"),
                Some("2026-11-12")
            )
        );
    }

    /// LSE selects rows by date: the 10th's export, which the mock answers
    /// with the three days, keeps the 10th's row alone.
    #[tokio::test]
    async fn rows_outside_the_request_s_window_are_left_out() {
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);
        let (client, mut events) = connected(&lse, false).await;

        client
            .request_data(bond_yields(
                "2026-11-10T00:00:00Z",
                "2026-11-10T23:59:59.999999999Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        let times: Vec<_> = custom_data(&rows_in(&envelope))
            .iter()
            .map(CustomData::ts_init)
            .collect();
        assert_eq!(times, [day("2026-11-10")]);
        let export = &lse.calls()[1];
        assert_eq!(
            (export.param("from"), export.param("to")),
            (Some("2026-11-10"), Some("2026-11-10"))
        );
    }

    #[tokio::test]
    async fn a_selection_lse_holds_no_row_of_is_answered_complete_with_no_rows() {
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::refusal(404, REFERENCE_DATA_UNAVAILABLE),
        ]);
        let (client, mut events) = connected(&lse, false).await;

        client
            .request_data(bond_yields(
                "2026-11-07T00:00:00Z",
                "2026-11-08T23:59:59.999999999Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        assert_eq!(custom_data(&rows_in(&envelope)), []);
    }

    #[tokio::test]
    async fn lse_s_refusal_of_the_export_is_the_request_s_failure() {
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::refusal(404, SYMBOL_UNKNOWN),
        ]);
        let (client, mut events) = connected(&lse, false).await;

        client
            .request_data(bond_yields(
                "2026-11-09T00:00:00Z",
                "2026-11-12T23:59:59.999999999Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        assert!(reason.contains("symbol-unknown"), "{reason}");
    }

    /// LSE is asked its catalogue, which lists no such collection, and no
    /// export.
    #[tokio::test]
    async fn a_source_that_is_no_reference_collection_of_lse_fails_naming_it() {
        let lse = MockLse::start(vec![Reply::json(REFERENCE_CATALOGUE)]);
        let (client, mut events) = connected(&lse, false).await;

        client
            .request_data(rows_of(
                LseRow::data_type("no_such_collection", "US10Y"),
                "2026-11-09T00:00:00Z",
                "2026-11-12T23:59:59.999999999Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        assert!(reason.contains("no_such_collection"), "{reason}");
        assert_eq!(lse.calls().len(), 1);
    }

    /// Neither is asked of LSE.
    #[tokio::test]
    async fn custom_data_not_of_lse_s_rows_or_naming_no_source_fails_naming_its_type_unasked() {
        let no_source = DataType::new(LseRow::type_name_static(), None, Some("US10Y".into()));
        for data_type in [
            DataType::new("Imbalance", None, Some("GC.v.0.GLBX".into())),
            no_source,
        ] {
            let lse = MockLse::start(vec![]);
            let (client, mut events) = connected(&lse, false).await;

            client
                .request_data(rows_of(
                    data_type.clone(),
                    "2026-11-09T00:00:00Z",
                    "2026-11-12T23:59:59.999999999Z",
                ))
                .unwrap();

            let envelope = next_envelope(&mut events).await;
            let reason = envelope.failure_reason().expect("a failure");
            assert!(reason.contains(data_type.type_name()), "{reason}");
            assert!(lse.calls().is_empty(), "{:?}", lse.calls());
        }
    }

    /// Nautilus' default for a request a client does not implement.
    #[tokio::test]
    async fn a_kind_lse_has_no_call_for_is_refused_as_unsupported() {
        let lse = MockLse::start(vec![]);
        let (client, _) = connected(&lse, true).await;
        let request = RequestFundingRates::new(
            InstrumentId::from(GOLD_FUTURES),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );

        let err = client.request_funding_rates(request).unwrap_err();

        assert!(err.to_string().contains("does not support"), "{err}");
    }

    /// The captured five minutes, candles LSE stamps at 10:00 to 10:04,
    /// their open, as pyarrow reads them.
    #[tokio::test]
    async fn a_bars_request_is_answered_with_lse_s_candles_stamped_at_their_close() {
        let lse = MockLse::start(vec![Reply::file(GOLD_SPOT_MINUTES)]);
        let (client, mut events) = connected(&lse, true).await;
        let request = bars(
            GOLD_SPOT_MINUTE_BARS,
            "2026-10-21T10:01:00Z",
            "2026-10-21T10:05:00Z",
        );
        let id = request.request_id;

        client.request_bars(request).unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        let response = bars_of(&envelope);
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.client_id, ClientId::from("LSE"));
        assert_eq!(response.bar_type, BarType::from(GOLD_SPOT_MINUTE_BARS));
        let closes: Vec<_> = response
            .data
            .iter()
            .map(|bar| (bar.ts_event, bar.ts_init, bar.close))
            .collect();
        let close = |time: &str, price| {
            let time = UnixNanos::from(at(&format!("2026-10-21T{time}Z")));
            (time, time, Price::from(price))
        };
        assert_eq!(
            closes,
            [
                close("10:01:00", "4118.01"),
                close("10:02:00", "4116.94"),
                close("10:03:00", "4116.17"),
                close("10:04:00", "4116.12"),
                close("10:05:00", "4118.16"),
            ]
        );
        assert_eq!(
            asked_windows(&lse),
            [(
                "2026-10-21T10:00:00Z".to_string(),
                "2026-10-21T10:05:00Z".to_string()
            )]
        );
    }

    /// LSE adjusts prices and volumes for splits unless told not to.
    #[tokio::test]
    async fn candles_are_asked_of_the_series_as_published() {
        let lse = MockLse::start(vec![Reply::file(GOLD_SPOT_MINUTES)]);
        let (client, mut events) = connected(&lse, true).await;

        client
            .request_bars(bars(
                GOLD_SPOT_MINUTE_BARS,
                "2026-10-21T10:01:00Z",
                "2026-10-21T10:05:00Z",
            ))
            .unwrap();

        next_envelope(&mut events).await;
        let [call] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(call.path, "/v3/export");
        let mut query = call.query.clone();
        query.sort();
        let expected = [
            ("adjusted", "0"),
            ("format", "parquet"),
            ("from", "2026-10-21T10:00:00Z"),
            ("market", "commodities"),
            ("symbol", "XAU/USD"),
            ("timeframe", "1m"),
            ("to", "2026-10-21T10:05:00Z"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        assert_eq!(query, expected);
    }

    /// Bars closing from 02:00: the first closes at 04:00 and opens at
    /// 00:00. LSE folds a candle from the rows inside the export alone, so
    /// an export from 22:00 or 02:00 would cut it.
    #[tokio::test]
    async fn only_whole_candles_are_asked() {
        let lse = MockLse::start(vec![Reply::file(GOLD_FUTURES_4H)]);
        let (client, mut events) = connected(&lse, true).await;

        client
            .request_bars(bars(
                &format!("{GOLD_FUTURES}-4-HOUR-LAST-EXTERNAL"),
                "2026-10-21T02:00:00Z",
                "2026-10-21T12:00:00Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        assert_eq!(bars_of(&envelope).data.len(), 3);
        assert_eq!(
            asked_windows(&lse),
            [(
                "2026-10-21T00:00:00Z".to_string(),
                "2026-10-21T12:00:00Z".to_string()
            )]
        );
    }

    /// Bars closing at 23:59 to 00:01: candles opening at 23:58 and 23:59
    /// on the 20th, then at 00:00 on the 21st.
    #[tokio::test]
    async fn candles_over_midnight_are_asked_a_utc_day_at_a_time() {
        let lse = MockLse::start(vec![
            Reply::refusal(404, DATA_UNAVAILABLE),
            Reply::refusal(404, DATA_UNAVAILABLE),
        ]);
        let (client, mut events) = connected(&lse, true).await;

        client
            .request_bars(bars(
                GOLD_SPOT_MINUTE_BARS,
                "2026-10-20T23:59:00Z",
                "2026-10-21T00:01:00Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        let day = |from: &str, to: &str| (from.to_string(), to.to_string());
        assert_eq!(
            asked_windows(&lse),
            [
                day("2026-10-20T23:58:00Z", "2026-10-21T00:00:00Z"),
                day("2026-10-21T00:00:00Z", "2026-10-21T00:01:00Z"),
            ]
        );
    }

    /// No 1-minute bar closes between 10:00:30 and 10:00:50.
    #[tokio::test]
    async fn a_window_no_bar_closes_in_is_answered_with_none_unasked() {
        let lse = MockLse::start(vec![]);
        let (client, mut events) = connected(&lse, true).await;

        client
            .request_bars(bars(
                GOLD_SPOT_MINUTE_BARS,
                "2026-10-21T10:00:30Z",
                "2026-10-21T10:00:50Z",
            ))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        assert_eq!(bars_of(&envelope).data.len(), 0);
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
    }

    /// LSE's candle widths below a day (LSE's API docs, "The selection",
    /// `timeframe`), each a bar specification of Nautilus'.
    #[tokio::test]
    async fn each_lse_candle_width_is_asked_for_its_bar_specification() {
        let widths = [
            ("1-SECOND", "1s"),
            ("5-SECOND", "5s"),
            ("15-SECOND", "15s"),
            ("30-SECOND", "30s"),
            ("1-MINUTE", "1m"),
            ("3-MINUTE", "3m"),
            ("5-MINUTE", "5m"),
            ("15-MINUTE", "15m"),
            ("30-MINUTE", "30m"),
            ("1-HOUR", "1h"),
            ("4-HOUR", "4h"),
        ];
        let lse = MockLse::start(
            widths
                .iter()
                .map(|_| Reply::refusal(404, DATA_UNAVAILABLE))
                .collect(),
        );
        let (client, mut events) = connected(&lse, true).await;

        for (spec, _) in widths {
            client
                .request_bars(bars(
                    &format!("{GOLD_FUTURES}-{spec}-LAST-EXTERNAL"),
                    "2026-10-21T10:00:00Z",
                    "2026-10-21T14:00:00Z",
                ))
                .unwrap();
            let envelope = next_envelope(&mut events).await;
            assert_eq!(envelope.failure_reason(), None, "{spec}");
        }

        let asked: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| call.param("timeframe").unwrap().to_string())
            .collect();
        assert_eq!(asked, widths.map(|(_, timeframe)| timeframe));
    }

    /// A width LSE has no candles of; LSE's daily, weekly and monthly
    /// candles, which follow exchange sessions; a price LSE's candles do not
    /// carry; and bars Nautilus builds.
    #[tokio::test]
    async fn a_bar_type_lse_does_not_serve_is_refused_naming_it_unasked() {
        for spec in [
            "2-MINUTE-LAST-EXTERNAL",
            "1-DAY-LAST-EXTERNAL",
            "1-WEEK-LAST-EXTERNAL",
            "1-MONTH-LAST-EXTERNAL",
            "100-TICK-LAST-EXTERNAL",
            "1-MINUTE-MID-EXTERNAL",
            "1-MINUTE-LAST-INTERNAL",
        ] {
            let bar_type = format!("{GOLD_FUTURES}-{spec}");
            let lse = MockLse::start(vec![Reply::file(GOLD_FUTURES_4H)]);
            let (client, mut events) = connected(&lse, true).await;

            client
                .request_bars(bars(
                    &bar_type,
                    "2026-10-21T10:00:00Z",
                    "2026-10-21T14:00:00Z",
                ))
                .unwrap();

            let envelope = next_envelope(&mut events).await;
            let reason = envelope.failure_reason().expect("a failure");
            assert!(reason.contains(&bar_type), "{reason}");
            assert!(lse.calls().is_empty(), "{bar_type}: {:?}", lse.calls());
        }
    }

    /// Bars closing from 00:01 the day before the current one: LSE holds the
    /// day before's candles and none of the current day's, which it stores
    /// the morning after. A plain response completes the request, and the
    /// data engine writes it to no catalog, so that a fill asks for the
    /// window again.
    #[tokio::test]
    async fn a_bars_request_reaching_into_the_day_lse_has_not_loaded_is_answered_unwritten() {
        let lse = MockLse::start(vec![
            Reply::file(GOLD_SPOT_MINUTES),
            Reply::refusal(404, DATA_UNAVAILABLE_TODAY),
            // Should the request come after midnight.
            Reply::refusal(404, DATA_UNAVAILABLE_TODAY),
        ]);
        let (client, mut events) = connected(&lse, true).await;
        let today = Timestamp::from_second(Timestamp::now().as_second() / 86_400 * 86_400).unwrap();
        let day_before = today - SignedDuration::from_hours(24);
        let request = RequestBars::new(
            BarType::from(GOLD_SPOT_MINUTE_BARS),
            Some(day_before + SignedDuration::from_mins(1)),
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );
        let id = request.request_id;

        client.request_bars(request).unwrap();

        let event = next_event(&mut events).await;
        let DataEvent::Response(DataResponse::Bars(response)) = event else {
            panic!("not a plain bars response: {event:?}");
        };
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.data.len(), 5);
        let day_before = day_before.to_string();
        assert_eq!(
            lse.calls().first().and_then(|call| call.param("from")),
            Some(day_before.as_str())
        );
    }

    /// The captured second of GCZ6 of 2026, as the data engine asks for a
    /// window, naming no client.
    fn gcz26_second<R>(
        request: impl FnOnce(
            InstrumentId,
            Option<Timestamp>,
            Option<Timestamp>,
            Option<std::num::NonZeroUsize>,
            Option<ClientId>,
            UUID4,
            UnixNanos,
            Option<nautilus_core::Params>,
        ) -> R,
    ) -> R {
        request(
            InstrumentId::from(GCZ26),
            Some(at("2026-11-12T14:30:00Z")),
            Some(at("2026-11-12T14:30:00.999999999Z")),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    /// The client with GCZ26's definition, as Databento gives it, in its
    /// cache.
    async fn defining_gcz26(lse: &MockLse) -> (Box<dyn DataClient>, Events) {
        connected_with(config(lse), vec![gcz26()]).await
    }

    /// A call: its path, its pair id or symbol, its rows and the key it
    /// carried.
    type Asked = (String, Option<String>, Option<String>, Option<String>);

    /// The calls LSE received, in order.
    fn asked(lse: &MockLse) -> Vec<Asked> {
        lse.calls()
            .iter()
            .map(|call| {
                let param = |name| call.param(name).map(ToString::to_string);
                (
                    call.path.clone(),
                    param("pair_id").or_else(|| param("symbol")),
                    param("data").or_else(|| param("timeframe")),
                    call.api_key.clone(),
                )
            })
            .collect()
    }

    fn call(path: &str, instrument: &str, rows: Option<&str>, key: &str) -> Asked {
        (
            path.to_string(),
            Some(instrument.to_string()),
            rows.map(ToString::to_string),
            Some(key.to_string()),
        )
    }

    /// GCZ6 is looked up in LSE's catalogue, its listing of 2026 asked for
    /// its trades, both with the CME key.
    #[tokio::test]
    async fn a_cme_contract_s_trades_are_asked_by_its_listing_with_the_cme_key() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::file(GCZ26_TRADES)]);
        let (client, mut events) = defining_gcz26(&lse).await;
        let request = gcz26_second(RequestTrades::new);
        let id = request.request_id;

        client.request_trades(request).unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        let response = trades_of(&envelope);
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.data.len(), 38);
        assert_eq!(response.data[0].price, Price::from("3908.10"));
        assert_eq!(
            asked(&lse),
            [
                call("/v3/pairs", "GCZ6", None, CME_API_KEY),
                call("/v3/export", "4000000002", Some("trades"), CME_API_KEY),
            ]
        );
    }

    /// LSE's quotes of the captured second, with the sizes LSE sends.
    #[tokio::test]
    async fn a_cme_contract_s_quotes_are_its_best_bid_and_offer_with_lse_s_sizes() {
        let lse = MockLse::start(vec![Reply::json(PAIRS_GCZ6), Reply::file(GCZ26_QUOTES)]);
        let (client, mut events) = defining_gcz26(&lse).await;
        let request = gcz26_second(RequestQuotes::new);
        let id = request.request_id;

        client.request_quotes(request).unwrap();

        let envelope = next_envelope(&mut events).await;
        assert_eq!(envelope.failure_reason(), None);
        let Some(DataResponse::Quotes(response)) = envelope.response() else {
            panic!("not a quotes response: {:?}", envelope.response());
        };
        assert_eq!(response.correlation_id, id);
        assert_eq!(response.client_id, ClientId::from("LSE"));
        assert_eq!(response.data.len(), 636);
        let first = &response.data[0];
        assert_eq!(
            (
                first.bid_price,
                first.ask_price,
                first.bid_size,
                first.ask_size
            ),
            (
                Price::from("3908.10"),
                Price::from("3908.30"),
                Quantity::from(2),
                Quantity::from(9)
            )
        );
        assert_eq!(
            asked(&lse)[1],
            call("/v3/export", "4000000002", Some("quotes"), CME_API_KEY)
        );
    }

    /// Trades, then quotes: LSE's catalogue is asked once.
    #[tokio::test]
    async fn a_contract_s_listing_is_looked_up_once_for_all_its_requests() {
        let lse = MockLse::start(vec![
            Reply::json(PAIRS_GCZ6),
            Reply::file(GCZ26_TRADES),
            Reply::file(GCZ26_QUOTES),
        ]);
        let (client, mut events) = defining_gcz26(&lse).await;

        client
            .request_trades(gcz26_second(RequestTrades::new))
            .unwrap();
        next_envelope(&mut events).await;
        client
            .request_quotes(gcz26_second(RequestQuotes::new))
            .unwrap();
        next_envelope(&mut events).await;

        let paths: Vec<_> = asked(&lse).into_iter().map(|(path, ..)| path).collect();
        assert_eq!(paths, ["/v3/pairs", "/v3/export", "/v3/export"]);
    }

    /// LSE's refusal of the CME key's rows older than its plan's history.
    #[tokio::test]
    async fn lse_s_refusal_of_a_cme_contract_s_rows_is_the_request_s_failure() {
        let lse = MockLse::start(vec![
            Reply::json(PAIRS_GCZ6),
            Reply::refusal(403, HISTORY_LIMIT),
        ]);
        let (client, mut events) = defining_gcz26(&lse).await;

        client
            .request_trades(gcz26_second(RequestTrades::new))
            .unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        for part in ["history-limit", "12 years of L1 history", "pair 4000000002"] {
            assert!(reason.contains(part), "{part}: {reason}");
        }
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
    async fn a_client_with_no_key_at_all_is_not_created_naming_both_variables() {
        let lse = MockLse::start(vec![]);
        let config = LseDataClientConfig {
            api_key: None,
            cme_api_key: None,
            ..config(&lse)
        };

        let err = LseDataClient::new(ClientId::from("LSE"), &config, cache_of(vec![]), |_| None)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("LSE_API_KEY") && err.contains("LSE_CME_API_KEY"),
            "{err}"
        );
    }

    /// LSE serves a derived market's past as timeframes alone: it is asked,
    /// and its refusal is the answer.
    #[tokio::test]
    async fn a_derived_series_quotes_are_asked_and_lse_s_refusal_fails_them() {
        let lse = MockLse::start(vec![Reply::refusal(400, DATA_UNAVAILABLE_QUOTES)]);
        let (client, mut events) = connected(&lse, true).await;
        let request = RequestQuotes::new(
            InstrumentId::from(GOLD_FUTURES),
            Some(at("2026-10-21T10:00:00Z")),
            Some(at("2026-10-21T10:00:00.999999999Z")),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );

        client.request_quotes(request).unwrap();

        let envelope = next_envelope(&mut events).await;
        let reason = envelope.failure_reason().expect("a failure");
        assert!(reason.contains("data-unavailable"), "{reason}");
        assert_eq!(
            asked(&lse),
            [call("/v3/export", "GOLD", Some("quotes"), API_KEY)]
        );
    }
}
