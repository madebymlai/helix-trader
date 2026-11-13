//! The data client: Nautilus' `DataClient` on LSE's REST API.
//!
//! It serves the trades and the quotes of the instruments LSE serves, each
//! asked of its market with its product's key (see `LseInstrument`): an
//! `LSEDGE` series' trades, its tick slices, by its symbol, with the LSE
//! key; a `GLBX` contract's trades and quotes, CME Globex's Level 1, by the
//! number of its listing, looked up once in LSE's catalogue (see
//! `http::listing`), with the CME key. An `LSEDGE` series' quotes are asked
//! too, and LSE refuses them, serving a derived market's past as
//! timeframes alone. Exports are asked a UTC day an export, one download at
//! a time on each key, an export LSE refuses as too wide asked again in
//! halves (see `http::export`); the definitions request answers none, LSE
//! publishing none. Every other request and every subscription is left to
//! Nautilus' defaults: a request fails as unsupported, a subscription is
//! logged as not implemented. A request is answered from a task, in one
//! completed response envelope, the data engine writing it back to the
//! catalog if asked (`update_catalog`); a failure is the envelope's, LSE's
//! refusal with its code, message and required plan. A request reaching
//! into the span LSE has not loaded yet, the current UTC day and the day
//! before until LSE holds rows of it, is answered with what LSE holds in a
//! plain response, which completes the request and which the data engine
//! writes to no catalog (`DataEngine::response`: "Payload-only replies do
//! not certify coverage for catalog write-back"), as Nautilus' OKX client
//! answers a window it cannot confirm, so that a fill asks for it again;
//! the span is logged.

use std::{collections::HashMap, future::Future, num::NonZeroUsize, sync::Arc};

use anyhow::{Context, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    live::{dst::time::Duration, runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            DataResponseEnvelope, QuotesResponse, RequestInstrument, RequestQuotes, RequestTrades,
            TradesResponse,
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
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};

use crate::{
    common::{
        consts::CME_MARKET,
        credential::{LseCredential, LseProduct},
        symbol::LseInstrument,
    },
    config::LseDataClientConfig,
    http::{
        client::LseHttpClient,
        export::fetch,
        listing::CmeListings,
        parse::{parse_quotes, parse_trade_slice},
        query::{ExportQuery, Rows},
    },
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
    /// them, or if an HTTP client cannot be built.
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
    /// (end: now if none), from a task: the export of the rows `rows`
    /// selects of what `id` stands for at LSE (see `http::export::fetch`),
    /// each file read by `read` at the precisions of `id`'s definition, the
    /// newest `limit` if one is given, in `respond`'s response of the rows
    /// and the time it is made. The response comes in one completed
    /// response envelope, or, if the export reaches into the span LSE has
    /// not loaded yet, in a plain response, which completes the request and
    /// which the data engine writes to no catalog, the span logged. A
    /// failure is the envelope's, with no rows: an instrument LSE does not
    /// serve, no key of its product, no definition in the cache, no start,
    /// before LSE is asked; LSE's refusal of a lookup or of an export, or a
    /// file not read.
    #[expect(clippy::too_many_arguments, reason = "one request's parts")]
    fn answer_export<T: Send + 'static>(
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
        let sender = self.data_sender.clone();
        let client_id = self.client_id;
        let clock = self.clock;
        self.spawn_request(async move {
            let fetched = async {
                let query = match &instrument {
                    LseInstrument::Series(series) => ExportQuery::series(series, rows, start, to),
                    LseInstrument::Cme(contract) => {
                        let pair_id = listings.pair_id(&http, contract).await?;
                        ExportQuery::listing(CME_MARKET, pair_id, rows, start, to)
                    }
                };
                let exported = fetch(&http, &query, now).await?;
                let mut data = Vec::new();
                for (export, file) in exported.files {
                    data.extend(
                        read(file, id, price_precision, size_precision)
                            .with_context(|| format!("reading {export}"))?,
                    );
                }
                Ok::<_, anyhow::Error>((data, exported.unloaded_from, query))
            }
            .await;
            let event = match fetched {
                Ok((mut data, unloaded_from, query)) => {
                    if let Some(limit) = limit {
                        let excess = data.len().saturating_sub(limit.get());
                        data.drain(..excess);
                    }
                    let count = data.len();
                    let response = respond(data, clock.get_time_ns());
                    match unloaded_from {
                        None => DataEvent::ResponseEnvelope(DataResponseEnvelope::new(
                            response, true, None,
                        )),
                        Some(from) => {
                            log::warn!(
                                "{client_id}: LSE has not loaded {query} from {from} yet, a UTC \
                                 day being stored the morning after it: answered with the \
                                 {count} rows it holds, written to no catalog"
                            );
                            DataEvent::Response(response)
                        }
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

    /// Answers with no definition, in one completed response envelope: LSE
    /// publishes none. A catalog the data engine holds answers first.
    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        send(
            &self.data_sender,
            DataEvent::ResponseEnvelope(DataResponseEnvelope::terminal(request.request_id, None)),
        );
        Ok(())
    }

    /// Answers with the trades over `[start, end]` (end: now if none), the
    /// newest `limit` if one is given (see `answer_export`): an `LSEDGE`
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
        self.answer_export(
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
    /// newest `limit` if one is given (see `answer_export`), with the sizes
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
        self.answer_export(
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
            data::{DataResponseEnvelope, RequestFundingRates, TradesResponse},
        },
    };
    use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        identifiers::InstrumentId,
        instruments::InstrumentAny,
        types::{Price, Quantity},
    };
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        LseDataClientFactory,
        testing::{
            API_KEY, CME_API_KEY, DATA_UNAVAILABLE_QUOTES, DATA_UNAVAILABLE_TODAY,
            DATE_NOT_TRADING, GCZ26, GCZ26_QUOTES, GCZ26_TRADES, GOLD_FUTURES, GOLD_TRADES,
            HISTORY_LIMIT, MockLse, PAIRS_GCZ6, PLAN_REQUIRED, Reply, SYMBOL_UNKNOWN, gcz26,
            gold_futures,
        },
    };

    type Events = mpsc::UnboundedReceiver<DataEvent>;

    /// The client the factory creates on `lse`, its cache holding
    /// `GOLD_FUTURES`'s definition if `defined`, connected, and the
    /// node's data events.
    async fn connected(lse: &MockLse, defined: bool) -> (Box<dyn DataClient>, Events) {
        let instruments = if defined {
            vec![gold_futures()]
        } else {
            vec![]
        };
        connected_with(config(lse), instruments).await
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

    #[tokio::test]
    async fn the_factory_creates_the_client_named_lse_on_no_venue() {
        let lse = MockLse::start(vec![]);

        let (client, _) = connected(&lse, true).await;

        assert_eq!(client.client_id(), ClientId::from("LSE"));
        assert_eq!(client.venue(), None);
        assert!(client.is_connected());
    }

    /// LSE publishes no definitions.
    #[tokio::test]
    async fn an_instrument_request_is_answered_complete_with_no_definition() {
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
        assert_eq!(envelope.failure_reason(), None);
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
        let lse = MockLse::start(vec![Reply::answer(PAIRS_GCZ6), Reply::file(GCZ26_TRADES)]);
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
        let lse = MockLse::start(vec![Reply::answer(PAIRS_GCZ6), Reply::file(GCZ26_QUOTES)]);
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
            Reply::answer(PAIRS_GCZ6),
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
            Reply::answer(PAIRS_GCZ6),
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
        let lse = MockLse::start(vec![Reply::answer(PAIRS_GCZ6)]);
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
