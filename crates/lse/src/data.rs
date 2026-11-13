//! The data client: Nautilus' `DataClient` on LSE's REST API.
//!
//! It serves trades, the tick slices of LSE's derived markets, asked a UTC
//! day an export, one download at a time, and the rows of LSE's reference
//! collections as custom data (`LseRow`), its source and symbol named in the
//! metadata of the request's data type; its definitions request answers
//! none, LSE publishing none. Every other request and every subscription is
//! left to Nautilus' defaults: a request fails as unsupported, a
//! subscription is logged as not implemented. A request is answered from a
//! task, in one completed response envelope, the data engine writing it
//! back to the catalog if asked (`update_catalog`); a failure is the
//! envelope's, LSE's refusal with its code, message and required plan.

use std::{future::Future, sync::Arc};

use anyhow::{Context, bail};
use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp, civil::Date, tz::TimeZone};
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    live::{dst::time::Duration, runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            CustomDataResponse, DataResponseEnvelope, RequestCustomData, RequestInstrument,
            RequestTrades, TradesResponse,
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
    common::{credential::LseCredential, symbol::LseSymbol},
    config::LseDataClientConfig,
    http::{
        client::LseHttpClient,
        parse::{parse_rows, parse_trade_slice},
        query::{ExportQuery, ReferenceQuery},
    },
    types::{LseRow, SOURCE, SYMBOL},
};

/// How long the request tasks get to finish at a disconnect, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// A UTC day: LSE's exports are asked a day at a time.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// The data client: LSE's history on one key, on no venue, so asked by
/// the requests naming it or by the venue routes its node gives it.
#[derive(Debug)]
pub(crate) struct LseDataClient {
    client_id: ClientId,
    http: Arc<LseHttpClient>,
    /// The instruments, whose precisions LSE's prices are converted at.
    cache: CacheView,
    tasks: TaskGroup,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    connected: bool,
}

impl LseDataClient {
    /// The client `client_id` on the key and API of `config`, the key given
    /// or read from `LSE_API_KEY`; not connected.
    ///
    /// # Errors
    ///
    /// Returns an error if no key is given or in `LSE_API_KEY`, or if the
    /// HTTP client cannot be built. Registers `LseRow` with Nautilus.
    ///
    /// # Panics
    ///
    /// Panics if the node's data event sender is not set on this thread,
    /// as it is when the node's factory creates the client.
    pub(crate) fn new(
        client_id: ClientId,
        config: &LseDataClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Self> {
        let credential =
            LseCredential::resolve(config.api_key.clone(), |name| std::env::var(name).ok())?;
        let http = LseHttpClient::new(
            config.http_base_url().to_owned(),
            &credential,
            config.calls_per_minute,
        )?;
        // The data engine writes the rows the client answers with back to
        // a catalog by their type's registered encoding.
        LseRow::register();
        Ok(Self {
            client_id,
            http: Arc::new(http),
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
    /// newest `limit` if one is given, from LSE's tick slices of the
    /// instrument's series, a UTC day an export, in one completed response
    /// envelope. A failure, the series not an LSEDGE one, the instrument
    /// not in the cache or LSE's refusal of an export, is the envelope's,
    /// with no trades: LSE's refusal carries its code, message and
    /// required plan.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is of more than one instrument.
    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let id = request.single_instrument_id()?;
        let checked = (|| {
            let series = LseSymbol::from_instrument_id(id)?;
            let precisions = self.precisions(id)?;
            let start = request
                .start
                .with_context(|| format!("a request of the trades of {id} needs a start"))?;
            Ok::<_, anyhow::Error>((series, precisions, start))
        })();
        let (series, (price_precision, size_precision), start) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request.request_id, format!("{e:#}"));
                return Ok(());
            }
        };
        let end = request
            .end
            .unwrap_or_else(|| Timestamp::from(self.clock.get_time_ns()));
        let http = Arc::clone(&self.http);
        let sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;
        self.spawn_request(async move {
            let fetched = async {
                let mut trades = Vec::new();
                for (from, to) in utc_days(start, end) {
                    let query = ExportQuery::trades(&series, from, to);
                    let file = http
                        .export(&query)
                        .await
                        .with_context(|| format!("exporting {query}"))?;
                    if let Some(file) = file {
                        trades.extend(
                            parse_trade_slice(file, id, price_precision, size_precision)
                                .with_context(|| format!("reading {query}"))?,
                        );
                    }
                }
                Ok::<_, anyhow::Error>(trades)
            }
            .await;
            let envelope = match fetched {
                Ok(mut trades) => {
                    if let Some(limit) = request.limit {
                        let excess = trades.len().saturating_sub(limit.get());
                        trades.drain(..excess);
                    }
                    let response = TradesResponse::new(
                        request.request_id,
                        client_id,
                        id,
                        trades,
                        datetime_to_unix_nanos(request.start),
                        datetime_to_unix_nanos(request.end),
                        clock.get_time_ns(),
                        request.params,
                    );
                    DataResponseEnvelope::new(DataResponse::Trades(response), true, None)
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
            Ok::<_, anyhow::Error>((named(SOURCE)?, named(SYMBOL)?, start))
        })();
        let (source, symbol, start) = match checked {
            Ok(checked) => checked,
            Err(e) => {
                self.fail(request.request_id, format!("{e:#}"));
                return Ok(());
            }
        };
        let end = request
            .end
            .unwrap_or_else(|| Timestamp::from(self.clock.get_time_ns()));
        let http = Arc::clone(&self.http);
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
}

/// The UTC date of `at`.
fn utc_date(at: Timestamp) -> Date {
    at.to_zoned(TimeZone::UTC).date()
}

/// `[start, end]`, closed, as the UTC days it spans, each as LSE takes two
/// timestamps, `[from, to)`: the first from `start`, the last to just past
/// `end`.
fn utc_days(start: Timestamp, end: Timestamp) -> Vec<(Timestamp, Timestamp)> {
    let end = end + SignedDuration::from_nanos(1);
    let mut days = Vec::new();
    let mut from = start;
    while from < end {
        let midnight = Timestamp::from_second(from.as_second().div_euclid(86_400) * 86_400)
            .expect("a midnight of a timestamp is one");
        let to = (midnight + DAY).min(end);
        days.push((from, to));
        from = to;
    }
    days
}

/// Sends `event` to the node; a closed channel means the node is stopping.
fn send(sender: &EventSender<DataEvent>, event: DataEvent) {
    if let Err(e) = sender.send(event) {
        log::warn!("data event not sent: {e}");
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, time::Duration};

    use jiff::Timestamp;
    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        factories::DataClientFactory,
        live::runner::replace_data_event_sender,
        messages::{
            DataResponse,
            data::{
                CustomDataResponse, DataResponseEnvelope, RequestCustomData, RequestQuotes,
                TradesResponse,
            },
        },
    };
    use nautilus_core::{Params, UUID4, UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{CustomData, CustomDataTrait, DataType, HasTsInit},
        identifiers::InstrumentId,
    };
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        LseDataClientFactory, LseRow,
        testing::{
            API_KEY, DATE_NOT_TRADING, GOLD_FUTURES, GOLD_TRADES, MockLse, PLAN_REQUIRED,
            REFERENCE_CATALOGUE, REFERENCE_DATA_UNAVAILABLE, Reply, SYMBOL_UNKNOWN,
            US10Y_BOND_YIELDS, gold_futures,
        },
    };

    type Events = mpsc::UnboundedReceiver<DataEvent>;

    /// The client the factory creates on `lse`, its cache holding
    /// `GOLD_FUTURES`'s definition if `defined`, connected, and the
    /// node's data events.
    async fn connected(lse: &MockLse, defined: bool) -> (Box<dyn DataClient>, Events) {
        let (tx, events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let mut cache = Cache::default();
        if defined {
            cache.add_instrument(gold_futures()).unwrap();
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

    /// The next event, a completed response envelope.
    async fn next_envelope(events: &mut Events) -> DataResponseEnvelope {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("no data event within 10 s")
            .expect("the data events ended");
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
        let request = RequestQuotes::new(
            InstrumentId::from(GOLD_FUTURES),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        );

        let err = client.request_quotes(request).unwrap_err();

        assert!(err.to_string().contains("does not support"), "{err}");
    }
}
