//! IC's data client for the live node: Nautilus' `DataClient` on a cTrader
//! session.
//!
//! On connect it authenticates, loads its instruments (as `IcHistory`
//! does) and sends them to the node. Quote subscriptions become spot
//! subscriptions, their spot events quotes (see `spots`); the session
//! sends them again after a reconnect. Quote requests are answered from
//! the tick history, merged as a fill merges it, up to the request's end
//! or now. Bars are left to Nautilus, built from the quotes.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use anyhow::Context;
use async_trait::async_trait;
use jiff::Timestamp;
use nautilus_common::live::dst::time::Duration;
use nautilus_common::{
    clients::{DataClient, execute_instrument_subscriptions},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{QuotesResponse, RequestQuotes, SubscribeQuotes, UnsubscribeQuotes},
    },
};
use nautilus_core::{
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::Data,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};

use super::spots::SpotQuotes;
use crate::data::ic::{
    IcHistory, IcSymbol,
    config::{IcConnection, IcCredential},
    parse::IC_VENUE,
    websocket::HEARTBEAT_INTERVAL,
};

/// How long tasks get to finish at a disconnect, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// IC's data client: quotes live and historical of the instruments it
/// loads, on venue `ICMARKETS`.
#[derive(Debug)]
pub(super) struct IcDataClient {
    client_id: ClientId,
    url: String,
    credential: IcCredential,
    instrument_ids: Vec<InstrumentId>,
    session: Option<Session>,
    session_tasks: TaskGroup,
    command_tasks: TaskGroup,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
}

/// A connected session and what it loaded.
#[derive(Debug)]
struct Session {
    history: Arc<IcHistory>,
    symbols: HashMap<InstrumentId, IcSymbol>,
    quotes: Arc<Mutex<SpotQuotes>>,
}

impl IcDataClient {
    /// A client on the environment of `connection` loading
    /// `instrument_ids`, IC ids, at connect; not connected.
    ///
    /// # Panics
    ///
    /// Panics if the node's data event sender is not set on this thread,
    /// as it is when the node's factory creates the client.
    #[must_use]
    pub(super) fn new(
        client_id: ClientId,
        connection: &IcConnection,
        instrument_ids: Vec<InstrumentId>,
    ) -> Self {
        let url = connection.environment.url().to_owned();
        Self::with_endpoint(
            client_id,
            url,
            connection.credential.clone(),
            instrument_ids,
        )
    }

    fn with_endpoint(
        client_id: ClientId,
        url: String,
        credential: IcCredential,
        instrument_ids: Vec<InstrumentId>,
    ) -> Self {
        Self {
            client_id,
            url,
            credential,
            instrument_ids,
            session: None,
            session_tasks: TaskGroup::new(),
            command_tasks: TaskGroup::new(),
            data_sender: get_data_event_sender(),
            clock: get_atomic_clock_realtime(),
        }
    }

    fn session(&self) -> anyhow::Result<&Session> {
        self.session
            .as_ref()
            .with_context(|| format!("{} is not connected", self.client_id))
    }

    /// Connects, loads the instruments and sends them to the node, then
    /// starts turning spot events into quotes.
    async fn open_session(&self) -> anyhow::Result<Session> {
        let (history, mut spots) =
            IcHistory::connect_to(&self.url, self.credential.clone(), HEARTBEAT_INTERVAL).await?;
        let loaded = async {
            let symbols = history.symbols(&self.instrument_ids).await?;
            let instruments: Vec<_> = symbols
                .iter()
                .map(|s| (s.instrument.clone(), s.symbol_id))
                .collect();
            Ok::<_, anyhow::Error>((symbols, SpotQuotes::new(&instruments)?))
        }
        .await;
        let (symbols, quotes) = match loaded {
            Ok(loaded) => loaded,
            Err(e) => {
                history.disconnect().await;
                return Err(e);
            }
        };
        for symbol in &symbols {
            send(
                &self.data_sender,
                DataEvent::Instrument(symbol.instrument.clone()),
            );
        }
        let quotes = Arc::new(Mutex::new(quotes));
        let stream = Arc::clone(&quotes);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancelled = self.session_tasks.cancellation_token();
        self.session_tasks.spawn(async move {
            loop {
                let spot = tokio::select! {
                    biased;
                    () = cancelled.cancelled() => return,
                    spot = spots.recv() => match spot {
                        Some(spot) => spot,
                        None => return,
                    },
                };
                let mut quotes = lock(&stream);
                match quotes.spot(&spot, clock.get_time_ns()) {
                    Ok(Some(quote)) => send(&sender, DataEvent::Data(Data::Quote(quote))),
                    Ok(None) => {}
                    Err(e) => log::warn!("IC spot event skipped: {e:#}"),
                }
            }
        })?;
        Ok(Session {
            history: Arc::new(history),
            symbols: symbols
                .into_iter()
                .map(|s| (s.instrument.id(), s))
                .collect(),
            quotes,
        })
    }

    fn spawn_command<F>(&self, description: &'static str, future: F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let cancelled = self.command_tasks.cancellation_token();
        let future = async move {
            tokio::select! {
                biased;
                () = cancelled.cancelled() => {}
                result = future => {
                    if let Err(e) = result {
                        log::error!("{description} failed: {e:#}");
                    }
                }
            }
        };
        if let Err(e) = self.command_tasks.spawn(future) {
            log::warn!("{description} skipped after shutdown began: {e}");
        }
    }

    /// Closes admission and cancels the tasks, and asks the session to
    /// close: `teardown` awaits both.
    fn begin_shutdown(&self) {
        self.session_tasks.begin_shutdown();
        self.command_tasks.begin_shutdown();
        if let Some(session) = &self.session {
            session.history.begin_shutdown();
        }
    }

    /// Drains the tasks, then closes the session, so a token renewal out
    /// at `stop` is still saved.
    async fn teardown(&mut self) -> anyhow::Result<()> {
        self.begin_shutdown();
        let (session, command) = tokio::join!(
            self.session_tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
            self.command_tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
        );
        if let Some(open) = self.session.take() {
            // The tasks are done, so this is the last handle.
            if let Ok(history) = Arc::try_unwrap(open.history) {
                history.disconnect().await;
            }
        }
        session.map_err(|e| anyhow::anyhow!("stopping the IC spot task: {e}"))?;
        command.map_err(|e| anyhow::anyhow!("stopping the IC request tasks: {e}"))?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl DataClient for IcDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(Venue::new(IC_VENUE))
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::debug!("Starting {}", self.client_id);
        Ok(())
    }

    /// Begins the shutdown; `disconnect`, or the next `connect`, finishes
    /// it.
    fn stop(&mut self) -> anyhow::Result<()> {
        log::debug!("Stopping {}", self.client_id);
        self.begin_shutdown();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    /// Connected: a session, and its tasks not shut down.
    fn is_connected(&self) -> bool {
        self.session.is_some() && self.session_tasks.is_open()
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        log::info!("Connecting {}", self.client_id);
        if self.session.is_some() || !self.session_tasks.is_open() || !self.command_tasks.is_open()
        {
            self.teardown().await?;
            self.session_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("reopening the IC spot task: {e}"))?;
            self.command_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("reopening the IC request tasks: {e}"))?;
        }
        self.session = Some(self.open_session().await?);
        log::info!("Connected {}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        log::info!("Disconnecting {}", self.client_id);
        let finished = self.teardown().await;
        log::info!("Disconnected {}", self.client_id);
        finished
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let session = self.session()?;
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            let symbol_id = lock(&session.quotes).subscribe(id)?;
            session.history.subscribe_spots(symbol_id)
        })
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let session = self.session()?;
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            let symbol_id = lock(&session.quotes).unsubscribe(id)?;
            session.history.unsubscribe_spots(symbol_id)
        })
    }

    /// Answers with the quotes over `[start, end]` (end: now if none) from
    /// the tick history, the newest `limit` if one is given. The live
    /// quotes of the instrument wait for the response (see `spots`).
    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let session = self.session()?;
        let id = request.instrument_id;
        let symbol = session
            .symbols
            .get(&id)
            .cloned()
            .with_context(|| format!("{id} is not an instrument the IC data client loaded"))?;
        let start = request
            .start
            .with_context(|| format!("a request of the quotes of {id} needs a start"))?;
        let end = request
            .end
            .unwrap_or_else(|| Timestamp::from(self.clock.get_time_ns()));
        lock(&session.quotes).hold(id)?;
        let history = Arc::clone(&session.history);
        let quotes = Arc::clone(&session.quotes);
        let sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;
        self.spawn_command("IC quotes request", async move {
            let fetched = history.quote_ticks(&symbol, start, end).await;
            // Under the lock, so no live quote of the instrument goes
            // between the response and the quotes it held.
            let mut live = lock(&quotes);
            let (answered, result) = match fetched {
                Ok(mut ticks) => {
                    if let Some(limit) = request.limit {
                        let excess = ticks.len().saturating_sub(limit.get());
                        ticks.drain(..excess);
                    }
                    let answered = ticks.last().map(|q| q.ts_event);
                    let response = QuotesResponse::new(
                        request.request_id,
                        client_id,
                        id,
                        ticks,
                        datetime_to_unix_nanos(request.start),
                        datetime_to_unix_nanos(request.end),
                        clock.get_time_ns(),
                        request.params,
                    );
                    send(&sender, DataEvent::Response(DataResponse::Quotes(response)));
                    (answered, Ok(()))
                }
                Err(e) => (None, Err(e)),
            };
            for quote in live.release(id, answered) {
                send(&sender, DataEvent::Data(Data::Quote(quote)));
            }
            result
        });
        Ok(())
    }
}

/// The spot quotes, locked; a panic elsewhere left them consistent, as
/// each change is a single step.
fn lock(quotes: &Mutex<SpotQuotes>) -> MutexGuard<'_, SpotQuotes> {
    quotes.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Sends `event` to the node; a closed channel means the node is stopping.
fn send(sender: &EventSender<DataEvent>, event: DataEvent) {
    if let Err(e) = sender.send(event) {
        log::warn!("IC data event not sent: {e}");
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use nautilus_common::live::runner::replace_data_event_sender;
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        instruments::Instrument,
        types::{Price, Quantity},
    };
    use prost::Message as _;
    use tokio::sync::mpsc;

    use super::*;
    use crate::data::ic::{
        messages::{
            Payload, ProtoMessage, ProtoOaAssetListReq, ProtoOaAssetListRes,
            ProtoOaGetDynamicLeverageByIdReq, ProtoOaGetTickDataReq, ProtoOaRefreshTokenReq,
            ProtoOaSpotEvent, ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes,
            ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq,
            ProtoOaSymbolsListRes, ProtoOaTraderReq, ProtoOaTraderRes, ProtoOaUnsubscribeSpotsReq,
            ProtoOaUnsubscribeSpotsRes,
        },
        testing::{
            self, ASSET_LIST_RES, Action, Received, SPOT_EVENT_EURUSD_FIRST,
            SPOT_EVENT_XAUUSD_FIRST, SUBSCRIBE_SPOTS_RES, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES,
            TRADER_RES, UNSUBSCRIBE_SPOTS_RES, accept_auth, dynamic_leverage_answer, replay, reply,
            tick_answer,
        },
        ticks::Tick,
    };

    const XAUUSD: &str = "XAUUSD.ICMARKETS";
    const EURUSD: &str = "EURUSD.ICMARKETS";

    /// cTrader's XAUUSD and EURUSD symbol ids on the server the fixtures
    /// come from.
    const XAUUSD_SYMBOL_ID: i64 = 41;

    /// The captured first spot event of `symbol_id`, as a frame.
    fn first_spot(symbol_id: i64) -> Action {
        let payload = if symbol_id == XAUUSD_SYMBOL_ID {
            SPOT_EVENT_XAUUSD_FIRST
        } else {
            SPOT_EVENT_EURUSD_FIRST
        };
        Action::Send(
            ProtoMessage {
                payload_type: ProtoOaSpotEvent::TYPE,
                payload: Some(payload.to_vec()),
                client_msg_id: None,
            }
            .encode_to_vec(),
        )
    }

    /// A cTrader serving the captured assets, symbols and details, the
    /// XAUUSD ticks `bids` and `asks` (oldest first), and spot
    /// subscriptions: the captured answer, then each symbol's captured
    /// first spot event.
    fn peer(bids: Vec<Tick>, asks: Vec<Tick>) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> {
        move |_, request| {
            if let Some(actions) = accept_auth(request) {
                return actions;
            }
            let payload = request.payload.as_deref().unwrap_or_default();
            match request.payload_type {
                t if t == ProtoOaAssetListReq::TYPE => {
                    vec![replay::<ProtoOaAssetListRes>(request, ASSET_LIST_RES)]
                }
                t if t == ProtoOaSymbolsListReq::TYPE => {
                    vec![replay::<ProtoOaSymbolsListRes>(request, SYMBOLS_LIST_RES)]
                }
                t if t == ProtoOaSymbolByIdReq::TYPE => {
                    vec![replay::<ProtoOaSymbolByIdRes>(request, SYMBOL_BY_ID_RES)]
                }
                t if t == ProtoOaTraderReq::TYPE => {
                    vec![replay::<ProtoOaTraderRes>(request, TRADER_RES)]
                }
                t if t == ProtoOaGetDynamicLeverageByIdReq::TYPE => {
                    vec![dynamic_leverage_answer(request)]
                }
                t if t == ProtoOaGetTickDataReq::TYPE => {
                    let asked = ProtoOaGetTickDataReq::decode(payload).unwrap();
                    vec![reply(request, &tick_answer(&asked, &bids, &asks, 10_000))]
                }
                t if t == ProtoOaSubscribeSpotsReq::TYPE => {
                    let asked = ProtoOaSubscribeSpotsReq::decode(payload).unwrap();
                    let mut actions = vec![replay::<ProtoOaSubscribeSpotsRes>(
                        request,
                        SUBSCRIBE_SPOTS_RES,
                    )];
                    actions.extend(asked.symbol_id.iter().map(|&id| first_spot(id)));
                    actions
                }
                t if t == ProtoOaUnsubscribeSpotsReq::TYPE => {
                    vec![replay::<ProtoOaUnsubscribeSpotsRes>(
                        request,
                        UNSUBSCRIBE_SPOTS_RES,
                    )]
                }
                other => panic!("unexpected request {other}"),
            }
        }
    }

    type Events = mpsc::UnboundedReceiver<DataEvent>;

    /// A client of XAUUSD and EURUSD on the peer `script` plays, connected,
    /// and the node's data events.
    async fn connected(
        script: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
    ) -> (IcDataClient, Events, Received) {
        let (url, received) = testing::start(script);
        let (tx, events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let (credential, _tokens) = testing::credential();
        let mut client = IcDataClient::with_endpoint(
            ClientId::from("ICMARKETS"),
            url,
            credential,
            vec![InstrumentId::from(XAUUSD), InstrumentId::from(EURUSD)],
        );
        client.connect().await.unwrap();
        (client, events, received)
    }

    async fn next_event(events: &mut Events) -> DataEvent {
        tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("no data event within 10 s")
            .expect("the data events ended")
    }

    /// The next event, a quote.
    async fn next_quote(events: &mut Events) -> nautilus_model::data::QuoteTick {
        match next_event(events).await {
            DataEvent::Data(Data::Quote(quote)) => quote,
            other => panic!("not a quote: {other:?}"),
        }
    }

    /// Skips the two instruments sent at connect.
    async fn skip_instruments(events: &mut Events) {
        for _ in 0..2 {
            assert!(matches!(next_event(events).await, DataEvent::Instrument(_)));
        }
    }

    fn subscribe(id: &str) -> SubscribeQuotes {
        SubscribeQuotes::new(
            InstrumentId::from(id),
            None,
            Some(Venue::new(IC_VENUE)),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    /// The requests of `payload_type` received.
    fn received_of<M: Payload>(received: &Received) -> Vec<M> {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f)| f.payload_type == M::TYPE)
            .map(|(_, f)| M::decode(f.payload.as_deref().unwrap_or_default()).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn connecting_sends_the_instruments_to_the_node_on_icmarkets() {
        let (mut client, mut events, _) = connected(peer(vec![], vec![])).await;

        let mut ids = Vec::new();
        for _ in 0..2 {
            let DataEvent::Instrument(instrument) = next_event(&mut events).await else {
                panic!("not an instrument");
            };
            ids.push(instrument.id());
        }

        assert_eq!(
            ids,
            [InstrumentId::from(XAUUSD), InstrumentId::from(EURUSD)]
        );
        assert!(client.is_connected());
        assert_eq!(client.venue(), Some(Venue::new("ICMARKETS")));
        client.disconnect().await.unwrap();
        assert!(client.is_disconnected());
    }

    #[tokio::test]
    async fn subscribed_quotes_come_from_the_spot_events_at_their_time() {
        let (mut client, mut events, received) = connected(peer(vec![], vec![])).await;
        skip_instruments(&mut events).await;

        client.subscribe_quotes(subscribe(XAUUSD)).unwrap();

        let quote = next_quote(&mut events).await;
        // As captured: bid 401796000, ask 401801000 at 1793017475790 ms;
        // 10,000 oz, XAUUSD's maximum order, as the history's quotes.
        assert_eq!(quote.instrument_id, InstrumentId::from(XAUUSD));
        assert_eq!(quote.bid_price, Price::from("4017.96"));
        assert_eq!(quote.ask_price, Price::from("4018.01"));
        assert_eq!(quote.ts_event, UnixNanos::new(1_793_017_475_790_000_000));
        assert!(quote.ts_init >= quote.ts_event);
        assert_eq!(quote.bid_size, Quantity::from(10_000));
        let asked: Vec<ProtoOaSubscribeSpotsReq> = received_of(&received);
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].symbol_id, [XAUUSD_SYMBOL_ID]);
        assert_eq!(asked[0].subscribe_to_spot_timestamp, Some(true));
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn an_unsubscription_ends_the_spot_subscription() {
        let (mut client, mut events, received) = connected(peer(vec![], vec![])).await;
        skip_instruments(&mut events).await;
        client.subscribe_quotes(subscribe(XAUUSD)).unwrap();
        next_quote(&mut events).await;

        client
            .unsubscribe_quotes(&UnsubscribeQuotes::new(
                InstrumentId::from(XAUUSD),
                None,
                Some(Venue::new(IC_VENUE)),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();

        testing::wait_until(&received, |frames| {
            frames
                .iter()
                .any(|(_, f)| f.payload_type == ProtoOaUnsubscribeSpotsReq::TYPE)
        })
        .await;
        let asked: Vec<ProtoOaUnsubscribeSpotsReq> = received_of(&received);
        assert_eq!(asked[0].symbol_id, [XAUUSD_SYMBOL_ID]);
        client.disconnect().await.unwrap();
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn tick(at_ms: i64, price: i64) -> Tick {
        Tick { ms: at_ms, price }
    }

    fn request(start: Option<Timestamp>, end: Timestamp, limit: Option<usize>) -> RequestQuotes {
        RequestQuotes::new(
            InstrumentId::from(XAUUSD),
            start,
            Some(end),
            limit.and_then(NonZeroUsize::new),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        )
    }

    /// (ms, bid, ask) of `quotes`.
    fn prices(quotes: &[nautilus_model::data::QuoteTick]) -> Vec<(u64, String, String)> {
        quotes
            .iter()
            .map(|q| {
                (
                    q.ts_event.as_u64() / 1_000_000,
                    q.bid_price.to_string(),
                    q.ask_price.to_string(),
                )
            })
            .collect()
    }

    fn quote(ms: i64, bid: &str, ask: &str) -> (u64, String, String) {
        (ms.try_into().unwrap(), bid.to_owned(), ask.to_owned())
    }

    /// One tick of each side before the window, two bids and an ask in it.
    fn window_ticks() -> (i64, Vec<Tick>, Vec<Tick>) {
        let s = at("2026-10-01T00:00:00Z").as_millisecond();
        let bids = vec![
            tick(s - 60_000, 400_000_000),
            tick(s + 1_000, 400_010_000),
            tick(s + 3_000, 400_020_000),
        ];
        let asks = vec![tick(s - 30_000, 400_050_000), tick(s + 2_000, 400_060_000)];
        (s, bids, asks)
    }

    #[tokio::test]
    async fn a_quote_request_is_answered_from_the_tick_history_of_its_window() {
        let (s, bids, asks) = window_ticks();
        let (mut client, mut events, _) = connected(peer(bids, asks)).await;
        skip_instruments(&mut events).await;
        let asked = request(
            Some(at("2026-10-01T00:00:00Z")),
            at("2026-10-01T01:00:00Z"),
            None,
        );
        let request_id = asked.request_id;

        client.request_quotes(asked).unwrap();

        let DataEvent::Response(DataResponse::Quotes(response)) = next_event(&mut events).await
        else {
            panic!("not a quotes response");
        };
        assert_eq!(response.correlation_id, request_id);
        assert_eq!(response.instrument_id, InstrumentId::from(XAUUSD));
        assert_eq!(
            prices(&response.data),
            [
                quote(s + 1_000, "4000.10", "4000.50"),
                quote(s + 2_000, "4000.10", "4000.60"),
                quote(s + 3_000, "4000.20", "4000.60"),
            ]
        );
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn a_limit_keeps_the_newest_quotes() {
        let (s, bids, asks) = window_ticks();
        let (mut client, mut events, _) = connected(peer(bids, asks)).await;
        skip_instruments(&mut events).await;

        client
            .request_quotes(request(
                Some(at("2026-10-01T00:00:00Z")),
                at("2026-10-01T01:00:00Z"),
                Some(1),
            ))
            .unwrap();

        let DataEvent::Response(DataResponse::Quotes(response)) = next_event(&mut events).await
        else {
            panic!("not a quotes response");
        };
        assert_eq!(
            prices(&response.data),
            [quote(s + 3_000, "4000.20", "4000.60")]
        );
        client.disconnect().await.unwrap();
    }

    /// The peer sends a live spot while the request's ticks are fetched.
    #[tokio::test]
    async fn live_quotes_arriving_during_a_request_come_after_its_response() {
        let (_, bids, asks) = window_ticks();
        let mut answer = peer(bids, asks);
        let mut subscribed = false;
        let mut spot_sent = false;
        let script = move |connection, request: &ProtoMessage| {
            let mut actions = Vec::new();
            if request.payload_type == ProtoOaSubscribeSpotsReq::TYPE {
                // The subscription's own spot comes during the request.
                subscribed = true;
                return vec![replay::<ProtoOaSubscribeSpotsRes>(
                    request,
                    SUBSCRIBE_SPOTS_RES,
                )];
            }
            if request.payload_type == ProtoOaGetTickDataReq::TYPE && subscribed && !spot_sent {
                spot_sent = true;
                actions.push(first_spot(XAUUSD_SYMBOL_ID));
            }
            actions.extend(answer(connection, request));
            actions
        };
        let (mut client, mut events, received) = connected(script).await;
        skip_instruments(&mut events).await;
        client.subscribe_quotes(subscribe(XAUUSD)).unwrap();
        testing::wait_until(&received, |frames| {
            frames
                .iter()
                .any(|(_, f)| f.payload_type == ProtoOaSubscribeSpotsReq::TYPE)
        })
        .await;

        client
            .request_quotes(request(
                Some(at("2026-10-01T00:00:00Z")),
                at("2026-10-01T01:00:00Z"),
                None,
            ))
            .unwrap();

        assert!(matches!(
            next_event(&mut events).await,
            DataEvent::Response(DataResponse::Quotes(_))
        ));
        let live = next_quote(&mut events).await;
        assert_eq!(live.ts_event, UnixNanos::new(1_793_017_475_790_000_000));
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn after_a_reconnect_the_quotes_are_subscribed_again_and_flow() {
        let mut answer = peer(vec![], vec![]);
        let script = move |connection, request: &ProtoMessage| {
            let mut actions = answer(connection, request);
            if connection == 0 && request.payload_type == ProtoOaSubscribeSpotsReq::TYPE {
                actions.push(Action::Close);
            }
            actions
        };
        let (mut client, mut events, received) = connected(script).await;
        skip_instruments(&mut events).await;

        client.subscribe_quotes(subscribe(XAUUSD)).unwrap();

        let before = next_quote(&mut events).await;
        let after = next_quote(&mut events).await;
        assert_eq!(before.instrument_id, InstrumentId::from(XAUUSD));
        assert_eq!(after.instrument_id, InstrumentId::from(XAUUSD));
        let again: Vec<Vec<i64>> = received
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, f)| *c == 1 && f.payload_type == ProtoOaSubscribeSpotsReq::TYPE)
            .map(|(_, f)| {
                ProtoOaSubscribeSpotsReq::decode(f.payload.as_deref().unwrap())
                    .unwrap()
                    .symbol_id
            })
            .collect();
        assert_eq!(again, [vec![XAUUSD_SYMBOL_ID]]);
        client.disconnect().await.unwrap();
    }

    /// The renewal is answered 300 ms after its request, after `stop`.
    #[tokio::test]
    async fn a_token_renewal_in_flight_at_stop_is_saved_by_the_disconnect() {
        let mut answer = peer(vec![], vec![]);
        let (url, received) = testing::start(move |connection, request| {
            if request.payload_type == ProtoOaRefreshTokenReq::TYPE {
                return vec![testing::slow_renewal_answer(
                    request,
                    Duration::from_millis(300),
                )];
            }
            answer(connection, request)
        });
        let (tx, _events) = mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let (credential, _dir) = testing::credential();
        let mut client = IcDataClient::with_endpoint(
            ClientId::from("ICMARKETS"),
            url,
            credential.clone(),
            vec![InstrumentId::from(XAUUSD)],
        );
        client.connect().await.unwrap();
        client.session().unwrap().history.renew_token();
        testing::wait_until(&received, |frames| {
            frames
                .iter()
                .any(|(_, f)| f.payload_type == ProtoOaRefreshTokenReq::TYPE)
        })
        .await;

        client.stop().unwrap();
        client.disconnect().await.unwrap();

        assert_eq!(
            testing::stored_access_token(&credential),
            testing::renewed_access(1)
        );
        assert!(client.is_disconnected());
    }

    #[tokio::test]
    async fn a_quote_request_without_a_start_is_refused() {
        let (mut client, _events, _) = connected(peer(vec![], vec![])).await;

        let err = client
            .request_quotes(request(None, at("2026-10-01T01:00:00Z"), None))
            .unwrap_err();

        assert!(err.to_string().contains("needs a start"), "{err}");
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn quotes_of_an_instrument_not_loaded_are_refused() {
        let (mut client, _events, _) = connected(peer(vec![], vec![])).await;

        let err = client
            .subscribe_quotes(subscribe("GBPUSD.ICMARKETS"))
            .unwrap_err();

        assert!(format!("{err:#}").contains("GBPUSD.ICMARKETS"), "{err:#}");
        client.disconnect().await.unwrap();
    }
}
