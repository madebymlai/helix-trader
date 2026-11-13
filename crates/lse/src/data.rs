//! The data client: Nautilus' `DataClient` on LSE's REST API and sockets.
//!
//! It serves trades, the tick slices of LSE's derived markets, asked a UTC
//! day an export, one download at a time; its definitions request answers
//! none, LSE publishing none. Live, it streams the quotes of the
//! instruments its config names, on their sockets (see `streams`). Every
//! other request and subscription is left to Nautilus' defaults: a request
//! fails as unsupported, a subscription is logged as not implemented. A
//! request is answered from a task, in one completed response envelope,
//! the data engine writing it back to the catalog if asked
//! (`update_catalog`); a failure is the envelope's, LSE's refusal with its
//! code, message and required plan.

#[cfg(test)]
mod data_tester;
mod streams;

use std::{future::Future, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, execute_instrument_subscriptions},
    live::{dst::time::Duration, runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            DataResponseEnvelope, RequestInstrument, RequestTrades, SubscribeQuotes,
            TradesResponse, UnsubscribeQuotes,
        },
    },
};
use nautilus_core::{
    UUID4,
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};

use self::streams::LseStreams;
use crate::{
    common::{credential::LseCredential, symbol::LseSymbol},
    config::LseDataClientConfig,
    http::{client::LseHttpClient, parse::parse_trade_slice, query::ExportQuery},
};

/// How long the request tasks get to finish at a disconnect, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// A UTC day: LSE's exports are asked a day at a time.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// The data client: LSE's history and live data on one key, on no venue,
/// so asked by the requests naming it or by the venue routes its node
/// gives it.
#[derive(Debug)]
pub(crate) struct LseDataClient {
    client_id: ClientId,
    http: Arc<LseHttpClient>,
    /// The instruments, whose precisions LSE's prices are converted at.
    cache: CacheView,
    tasks: TaskGroup,
    streams: LseStreams,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    connected: bool,
}

impl LseDataClient {
    /// The client `client_id` on the key, API and sockets of `config`, the
    /// key given or read from `LSE_API_KEY`, streaming the instruments it
    /// names; not connected.
    ///
    /// # Errors
    ///
    /// Returns an error if no key is given or in `LSE_API_KEY`, if the HTTP
    /// client cannot be built, or if an instrument streamed is not an
    /// `LSEDGE` series streamed on a socket (see `LseStreams::new`).
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
        let data_sender = get_data_event_sender();
        let clock = get_atomic_clock_realtime();
        let streams = LseStreams::new(config, credential, data_sender.clone(), clock)?;
        Ok(Self {
            client_id,
            http: Arc::new(http),
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
    /// precision of its definition in the cache: bid and ask, with no
    /// sizes (see `LseStreams`).
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
            data::{DataResponseEnvelope, RequestQuotes, TradesResponse},
        },
    };
    use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
    use nautilus_model::identifiers::InstrumentId;
    use tokio::sync::mpsc;

    use nautilus_model::{
        data::{Data, QuoteTick},
        types::{Price, Quantity},
    };
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        LseDataClientFactory,
        testing::{
            API_KEY, DATE_NOT_TRADING, GOLD_FUTURES, GOLD_TRADES, MockLse, MockLseSocket,
            PLAN_REQUIRED, Reply, SYMBOL_UNKNOWN, data_ws_answer, gold_futures,
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

    /// The client the factory creates on `socket`, streaming `streamed`,
    /// its cache holding `GOLD_FUTURES`'s definition if `defined`, and the
    /// node's data events, once it connected or failed to.
    async fn streaming(
        socket: &MockLseSocket,
        streamed: &[&str],
        defined: bool,
    ) -> (Box<dyn DataClient>, anyhow::Result<()>, Events) {
        let (tx, events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let mut cache = Cache::default();
        if defined {
            cache.add_instrument(gold_futures()).unwrap();
        }
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .base_url_ws(socket.url().to_string())
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

    /// The quotes of `ids`, as the data engine asks the client for them.
    fn quotes_of(ids: &[&str]) -> SubscribeQuotes {
        SubscribeQuotes::new(
            ids.iter()
                .map(|&id| InstrumentId::from(id))
                .collect::<Vec<_>>(),
            Some(ClientId::from("LSE")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    /// The frames `socket` received, keys aside.
    fn sent(socket: &MockLseSocket) -> Vec<Value> {
        socket
            .received()
            .into_iter()
            .map(|(_, frame)| frame)
            .filter(|frame| frame["action"] != "auth")
            .collect()
    }

    /// The futures data-ws lists, in its order.
    const LISTED_FUTURES: [&str; 17] = [
        "AUDUSD",
        "BRENT",
        "CADUSD",
        "CHFUSD",
        "COPPER",
        "CORN",
        "CRUDEOIL",
        "DOW",
        "EURUSD",
        "GASOLINE",
        "GBPUSD",
        "GOLD",
        "HEATINGOIL",
        "JPYUSD",
        "LEANHOGS",
        "LIVECATTLE",
        "SILVER",
    ];

    /// The first tick captured, GOLD.F's at 16:44:55.499567 on 13 November
    /// 2026: bid 3888.0, ask 3888.2.
    #[tokio::test]
    async fn a_subscribed_lsedge_instrument_receives_lse_s_quotes_of_bid_and_ask_with_no_sizes() {
        let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
        let (mut client, connected, mut events) = streaming(&socket, &[GOLD_FUTURES], true).await;
        connected.unwrap();

        client.subscribe_quotes(quotes_of(&[GOLD_FUTURES])).unwrap();

        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("no data event within 10 s")
            .unwrap();
        let DataEvent::Data(Data::Quote(quote)) = event else {
            panic!("not a quote: {event:?}")
        };
        let at: UnixNanos = "2026-11-13T16:44:55.499567Z"
            .parse::<Timestamp>()
            .unwrap()
            .into();
        assert_eq!(
            quote,
            QuoteTick::new(
                InstrumentId::from(GOLD_FUTURES),
                Price::from("3888.0"),
                Price::from("3888.2"),
                Quantity::from(0),
                Quantity::from(0),
                at,
                quote.ts_init,
            )
        );
        assert_eq!(
            sent(&socket),
            [json!({"action": "subscribe", "symbol": "GOLD.F"})]
        );
    }

    #[tokio::test]
    async fn an_unsubscription_of_quotes_goes_to_lse() {
        let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
        let (mut client, connected, _) = streaming(&socket, &[GOLD_FUTURES], true).await;
        connected.unwrap();
        client.subscribe_quotes(quotes_of(&[GOLD_FUTURES])).unwrap();
        let unsubscribe = UnsubscribeQuotes::new(
            InstrumentId::from(GOLD_FUTURES),
            Some(ClientId::from("LSE")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        client.unsubscribe_quotes(&unsubscribe).unwrap();

        socket.wait_until(|received| received.len() == 3).await;
        assert_eq!(
            sent(&socket)[1],
            json!({"action": "unsubscribe", "symbol": "GOLD.F"})
        );
    }

    /// LSE would accept its subscription and send nothing.
    #[tokio::test]
    async fn a_symbol_its_socket_does_not_list_fails_the_connect_naming_it_before_subscribing() {
        let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));

        let (client, connected, _) =
            streaming(&socket, &[GOLD_FUTURES, "NOSUCH-FUTURES.LSEDGE"], true).await;

        let err = format!("{:#}", connected.unwrap_err());
        assert!(err.contains("NOSUCH-FUTURES.LSEDGE (NOSUCH.F)"), "{err}");
        assert!(!err.contains(GOLD_FUTURES), "{err}");
        assert!(client.is_disconnected());
        assert_eq!(sent(&socket), Vec::<Value>::new());
    }

    /// data-ws allows the free key 16 symbols on its one connection.
    #[tokio::test]
    async fn more_instruments_than_a_connection_allows_fail_the_connect_naming_lse_s_limit() {
        let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
        let ids: Vec<String> = LISTED_FUTURES
            .iter()
            .map(|symbol| format!("{symbol}-FUTURES.LSEDGE"))
            .collect();
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();

        let (_, connected, _) = streaming(&socket, &ids, true).await;

        let err = format!("{:#}", connected.unwrap_err());
        assert!(err.contains("allows 16 symbols"), "{err}");
        assert!(err.contains("17 are streamed"), "{err}");
        assert_eq!(sent(&socket), Vec::<Value>::new());
    }

    #[tokio::test]
    async fn quotes_of_an_instrument_the_cache_does_not_define_fail_naming_it_unasked() {
        let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
        let (mut client, connected, _) = streaming(&socket, &[GOLD_FUTURES], false).await;
        connected.unwrap();

        let err = client
            .subscribe_quotes(quotes_of(&[GOLD_FUTURES]))
            .unwrap_err();

        let err = format!("{err:#}");
        assert!(
            err.contains(&format!("{GOLD_FUTURES} is not in the cache")),
            "{err}"
        );
        assert_eq!(sent(&socket), Vec::<Value>::new());
    }

    #[tokio::test]
    async fn quotes_of_an_instrument_the_client_does_not_stream_fail_naming_it() {
        let socket = MockLseSocket::start(|_, frame| data_ws_answer(frame));
        let (mut client, connected, _) = streaming(&socket, &[GOLD_FUTURES], true).await;
        connected.unwrap();

        let err = client
            .subscribe_quotes(quotes_of(&["SILVER-FUTURES.LSEDGE"]))
            .unwrap_err();

        let err = format!("{err:#}");
        assert!(err.contains("SILVER-FUTURES.LSEDGE is not among"), "{err}");
        assert_eq!(sent(&socket), Vec::<Value>::new());
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
