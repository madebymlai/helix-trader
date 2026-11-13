//! A live warmup at its seam: a `Warmup` on a Nautilus node whose fake data
//! client answers as a venue does, its bars built by Nautilus' own data
//! engine and cached as Nautilus caches them. The tests read what the venue
//! was asked and what the warmup handed its indicator. Hermetic: no venue.
//! The node's clock is the real one, so the tests read the windows and the
//! bars against each other, not against a time. Each test runs its nodes on
//! its own thread, as Nautilus' message bus is thread-local.
//!
//! ```sh
//! cargo test --test node_warmup
//! ```

use std::{
    any::Any,
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};

use helix_trader::{
    backtest::Commission,
    model::{
        decide::DecisionRule,
        targets::{SideTarget, Target, TargetConfig, TargetSetter},
    },
    rolls::Roller,
    strategy::{
        DataRequirements, TickSeries,
        warmup::{self, BookIndicator, Warmup},
    },
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{
        DataActor, DataActorConfig, DataActorCore, DataActorNative,
        indicators::{ActorIndicator, SharedActorIndicator},
    },
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            BarsResponse, BookDeltasResponse, DataRequestFailed, DataResponseEnvelope,
            QuotesResponse, RequestBars, RequestBookDeltas, RequestQuotes, RequestTrades,
            SubscribeQuotes, SubscribeTrades, TradesResponse,
        },
    },
    nautilus_actor,
};
use nautilus_core::{UUID4, UnixNanos, datetime::datetime_to_unix_nanos};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveNodeConfig, RoutingConfig},
};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, Data, NautilusDataType, OrderBookDelta, OrderBookDeltas,
        QuoteTick, TradeTick,
    },
    enums::{AggressorSide, BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, TradeId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::futures_contract_es},
    orderbook::OrderBook,
    types::{Money, Price, Quantity},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};

const GCZ26: &str = "GCZ26.GLBX";
/// Hour bars built from the venue's minute bars.
const COMPOSITE: &str = "GCZ26.GLBX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";
/// Minute bars built from the venue's quotes.
const MID_MINUTES: &str = "GCZ26.GLBX-1-MINUTE-MID-INTERNAL";

const HOUR_NS: u64 = 3_600_000_000_000;
const MINUTE_NS: u64 = 60_000_000_000;
const DAY_NS: u64 = 24 * HOUR_NS;
const WARMUP: SignedDuration = SignedDuration::from_hours(72);
/// How long after its close a node builds a composite bar, which is stamped
/// then (`warmup::bar_build_delay`).
const COMPOSITE_DELAY_NS: u64 = 5_000_000_000;

fn gcz26() -> InstrumentAny {
    let mut contract = futures_contract_es(None, None);
    contract.id = InstrumentId::from(GCZ26);
    InstrumentAny::FuturesContract(contract)
}

/// The venue's price at the minute closing at `ns`: its number since the
/// epoch, in dollars, moving by one a minute, so that a bar's prices tell
/// which minutes built it and every hour's true range is 60.
fn price_at(ns: u64) -> Price {
    Price::from(format!("{}.00", ns / MINUTE_NS).as_str())
}

/// The closes of the minutes in `[start, end]`, outside `closed`.
fn minutes(start: u64, end: u64, closed: Option<(u64, u64)>) -> impl Iterator<Item = u64> {
    (start.div_ceil(MINUTE_NS)..=end / MINUTE_NS)
        .map(|minute| minute * MINUTE_NS)
        .filter(move |&ns| closed.is_none_or(|(from, to)| !(from..=to).contains(&ns)))
}

/// How the fake venue answers a request.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Answer {
    /// A completed envelope, as cTrader's, Databento's and OKX's clients.
    Envelope,
    /// A response without an envelope, as IB's client: Nautilus takes it as
    /// complete and writes nothing back.
    Plain,
    /// As `Envelope` until the request of this index, which fails with the
    /// rows it got.
    FailedAt(usize),
}

/// What the fake venue holds and was asked.
#[derive(Debug)]
struct FakeVenue {
    answer: Answer,
    /// A span it holds no data over, in ns: a closed market.
    closed: Option<(u64, u64)>,
    /// How long it takes to answer a request, as over a network; none
    /// answers at once.
    latency: Option<Duration>,
    /// If a subscription resends the warmup's last tick first, made as it
    /// was and stamped on receipt, as IC does its latest: the warmup's
    /// length, from which and the start of the first window asked, an empty
    /// catalog's, the venue tells the warmup's end, and so its last minute.
    resends: Option<SignedDuration>,
    /// Each request's window, in ns.
    asked: Vec<(u64, u64)>,
    /// Each live tick it sent.
    live: Vec<Live>,
}

/// A live tick the fake venue sent.
#[derive(Debug, Clone, Copy)]
struct Live {
    /// When it was made (`ts_event`), in ns.
    made: u64,
    /// How many windows the venue had been asked by then: fewer than it is
    /// asked in all, the tick came before the warmup's last window.
    asked: usize,
}

impl FakeVenue {
    fn new(answer: Answer) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            answer,
            closed: None,
            latency: None,
            resends: None,
            asked: vec![],
            live: vec![],
        }))
    }
}

#[derive(Debug)]
struct FakeConfig;

impl ClientConfig for FakeConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
struct FakeFactory(Rc<RefCell<FakeVenue>>);

impl DataClientFactory for FakeFactory {
    fn create(
        &self,
        name: &str,
        _config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        Ok(Box::new(FakeClient {
            id: ClientId::from(name),
            venue: Rc::clone(&self.0),
            sender: get_data_event_sender(),
            connected: false,
        }))
    }

    fn name(&self) -> &str {
        "FAKE"
    }

    fn config_type(&self) -> &str {
        "FakeConfig"
    }
}

/// A data client answering as the fake venue says; on no venue, so routed by
/// the node's routing.
struct FakeClient {
    id: ClientId,
    venue: Rc<RefCell<FakeVenue>>,
    sender: EventSender<DataEvent>,
    connected: bool,
}

impl FakeClient {
    /// Records the request `request_id` over `window`, then sends `response`
    /// as the venue says.
    fn answer(
        &self,
        request_id: UUID4,
        window: (u64, u64),
        response: DataResponse,
    ) -> anyhow::Result<()> {
        let mut venue = self.venue.borrow_mut();
        let index = venue.asked.len();
        venue.asked.push(window);
        let event = match venue.answer {
            Answer::FailedAt(failing) if index == failing => {
                DataEvent::RequestFailed(DataRequestFailed {
                    request_id,
                    reason: "venue cut the history".to_string(),
                    response: Some(response),
                })
            }
            Answer::Plain => DataEvent::Response(response),
            Answer::Envelope | Answer::FailedAt(_) => {
                DataEvent::ResponseEnvelope(DataResponseEnvelope::new(response, true, None))
            }
        };
        match venue.latency {
            None => self.sender.send(event)?,
            Some(latency) => {
                let sender = self.sender.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(latency).await;
                    sender.send(event).unwrap();
                });
            }
        }
        Ok(())
    }

    /// Sends the live ticks of a subscription to `instrument_id`, as `tick`
    /// makes them of the time made and the time stamped, in ns: at once, one
    /// made and stamped now, after the warmup's last minute's, made as it was
    /// and stamped now, if the venue resends it (`FakeVenue::resends`): made
    /// at or before the warmup's end, whatever the time now, so the history
    /// holds it.
    ///
    /// # Panics
    ///
    /// Panics if the venue resends before it was asked a window.
    fn subscribed(
        &self,
        instrument_id: InstrumentId,
        tick: impl Fn(InstrumentId, u64, u64) -> Data,
    ) -> anyhow::Result<()> {
        let now = UnixNanos::from(Timestamp::now()).as_u64();
        let mut venue = self.venue.borrow_mut();
        let mut made = vec![];
        if let Some(length) = venue.resends {
            let (start, _) = *venue
                .asked
                .first()
                .expect("a window asked before the subscription");
            let end = start + u64::try_from(length.as_nanos()).unwrap();
            made.push((end / MINUTE_NS) * MINUTE_NS);
        }
        made.push(now);
        for made in made {
            let asked = venue.asked.len();
            venue.live.push(Live { made, asked });
            self.sender
                .send(DataEvent::Data(tick(instrument_id, made, now)))?;
        }
        Ok(())
    }
}

/// A quote of `instrument_id` at the venue's price, made at `made` and
/// stamped at `stamped`, in ns.
fn quote_at(instrument_id: InstrumentId, made: u64, stamped: u64) -> QuoteTick {
    let price = price_at(made);
    QuoteTick::new(
        instrument_id,
        price,
        price,
        Quantity::from(1),
        Quantity::from(1),
        made.into(),
        stamped.into(),
    )
}

/// A trade of `instrument_id` at the venue's price, made at `made` and
/// stamped at `stamped`, in ns, its id the time made.
fn trade_at(instrument_id: InstrumentId, made: u64, stamped: u64) -> TradeTick {
    TradeTick::new(
        instrument_id,
        price_at(made),
        Quantity::from(1),
        AggressorSide::Buy,
        TradeId::new(made.to_string()),
        made.into(),
        stamped.into(),
    )
}

fn window(start: Option<Timestamp>, end: Option<Timestamp>) -> (u64, u64) {
    (
        datetime_to_unix_nanos(start).unwrap().as_u64(),
        datetime_to_unix_nanos(end).unwrap().as_u64(),
    )
}

#[async_trait::async_trait(?Send)]
impl DataClient for FakeClient {
    fn client_id(&self) -> ClientId {
        self.id
    }
    fn venue(&self) -> Option<Venue> {
        None
    }
    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn is_connected(&self) -> bool {
        self.connected
    }
    fn is_disconnected(&self) -> bool {
        !self.connected
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        self.connected = true;
        Ok(())
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    /// The venue's minute bars in the request's window, one a minute,
    /// stamped at their close.
    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let bar_type = request.single_bar_type()?;
        let closed = self.venue.borrow().closed;
        let rows: Vec<Bar> = minutes(start, end, closed)
            .map(|ns| {
                let price = price_at(ns);
                Bar::new(
                    bar_type,
                    price,
                    price,
                    price,
                    price,
                    Quantity::from(1),
                    ns.into(),
                    ns.into(),
                )
            })
            .collect();
        let response = DataResponse::Bars(BarsResponse::new(
            request.request_id,
            self.id,
            bar_type,
            rows,
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params.clone(),
        ));
        self.answer(request.request_id, (start, end), response)
    }

    /// The venue's deltas in the request's window, a batch on each minute.
    fn request_book_deltas(&self, request: RequestBookDeltas) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let id = request.single_instrument_id()?;
        let closed = self.venue.borrow().closed;
        let rows: Vec<OrderBookDelta> = minutes(start, end, closed)
            .flat_map(|ns| deltas_at(id, ns))
            .collect();
        let response = DataResponse::BookDeltas(BookDeltasResponse::new(
            request.request_id,
            self.id,
            id,
            rows,
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params.clone(),
        ));
        self.answer(request.request_id, (start, end), response)
    }

    /// The venue's quotes in the request's window, one on each minute.
    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let id = request.single_instrument_id()?;
        let closed = self.venue.borrow().closed;
        let rows: Vec<QuoteTick> = minutes(start, end, closed)
            .map(|ns| quote_at(id, ns, ns))
            .collect();
        let response = DataResponse::Quotes(QuotesResponse::new(
            request.request_id,
            self.id,
            id,
            rows,
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params.clone(),
        ));
        self.answer(request.request_id, (start, end), response)
    }

    /// The venue's trades in the request's window, one on each minute.
    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let id = request.single_instrument_id()?;
        let closed = self.venue.borrow().closed;
        let rows: Vec<TradeTick> = minutes(start, end, closed)
            .map(|ns| trade_at(id, ns, ns))
            .collect();
        let response = DataResponse::Trades(TradesResponse::new(
            request.request_id,
            self.id,
            id,
            rows,
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params.clone(),
        ));
        self.answer(request.request_id, (start, end), response)
    }

    /// A quote of each instrument at once, as IC and IB send one on a
    /// subscription (`FakeClient::subscribed`).
    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        for id in cmd.instrument_ids {
            self.subscribed(id, |id, made, stamped| {
                Data::Quote(quote_at(id, made, stamped))
            })?;
        }
        Ok(())
    }

    /// A trade of each instrument at once (`FakeClient::subscribed`).
    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        for id in cmd.instrument_ids {
            self.subscribed(id, |id, made, stamped| {
                Data::Trade(trade_at(id, made, stamped))
            })?;
        }
        Ok(())
    }
}

/// The venue's deltas at the minute closing at `ns`, as a book feed carries
/// them: one bid added, its order id the minute, after a clear if the minute
/// opens a UTC day, a snapshot of that bid alone.
fn deltas_at(id: InstrumentId, ns: u64) -> Vec<OrderBookDelta> {
    let order = BookOrder::new(
        OrderSide::Buy,
        price_at(ns),
        Quantity::from(1),
        ns / MINUTE_NS,
    );
    let last = RecordFlag::F_LAST as u8;
    if ns.is_multiple_of(DAY_NS) {
        let snapshot = RecordFlag::F_SNAPSHOT as u8;
        vec![
            OrderBookDelta::clear(id, 0, ns.into(), ns.into()),
            OrderBookDelta::new(
                id,
                BookAction::Add,
                order,
                snapshot | last,
                0,
                ns.into(),
                ns.into(),
            ),
        ]
    } else {
        vec![OrderBookDelta::new(
            id,
            BookAction::Add,
            order,
            last,
            0,
            ns.into(),
            ns.into(),
        )]
    }
}

/// A book a warmup's deltas are applied to, as a feature keeping one does.
#[derive(Debug)]
struct Books {
    book: RefCell<OrderBook>,
    /// The stamp of each delta of the feed, a snapshot's aside.
    fed: RefCell<Vec<u64>>,
}

impl BookIndicator for Books {
    fn handle_depth(&self, _depth: &nautilus_model::data::OrderBookDepth) {}

    fn handle_deltas(&self, deltas: &OrderBookDeltas) {
        let of_the_feed = deltas
            .deltas
            .iter()
            .filter(|delta| !RecordFlag::F_SNAPSHOT.matches(delta.flags))
            .map(|delta| delta.ts_event.as_u64());
        self.fed.borrow_mut().extend(of_the_feed);
        self.book.borrow_mut().apply_deltas(deltas).unwrap();
    }
}

/// An indicator recording each bar the warmup hands it, and when each quote
/// and trade it reads was made.
#[derive(Debug, Default)]
struct Fed {
    bars: RefCell<Vec<Bar>>,
    quotes: RefCell<Vec<u64>>,
    trades: RefCell<Vec<u64>>,
}

impl ActorIndicator for Fed {
    fn key(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn initialized(&self) -> anyhow::Result<bool> {
        Ok(true)
    }

    fn handle_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.quotes.borrow_mut().push(quote.ts_event.as_u64());
        Ok(())
    }

    fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        self.trades.borrow_mut().push(trade.ts_event.as_u64());
        Ok(())
    }

    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        self.bars.borrow_mut().push(*bar);
        Ok(())
    }
}

/// Starts a warmup as a strategy does, hands it its responses, and stops the
/// node when it ends.
#[derive(Debug)]
struct Warming {
    core: DataActorCore,
    warmup: Rc<Warmup>,
    /// The roller of the tick series the warmup's features read, started
    /// after the warmup, as a model handle starts it.
    roller: Option<Roller>,
    /// The target, started as a model handle does, and read for the long
    /// side when the warmup ends.
    traded: Option<Rc<TargetSetter>>,
    exits: Rc<RefCell<Option<SideTarget>>>,
    failure: Rc<RefCell<Option<String>>>,
}

nautilus_actor!(Warming);

impl DataActor for Warming {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.cache_rc().borrow_mut().add_instrument(gcz26())?;
        Rc::clone(&self.warmup).start(self)?;
        if let Some(roller) = self.roller.clone() {
            roller.start(self)?;
        }
        if let Some(traded) = self.traded.clone() {
            traded.start(self);
        }
        Ok(())
    }

    fn on_historical_book_deltas(&mut self, deltas: &[OrderBookDelta]) -> anyhow::Result<()> {
        self.warmup.on_historical_book_deltas(deltas);
        Ok(())
    }

    /// The ticks, of the warmup and live, go to the warmup, as a model
    /// strategy hands them (`model_data_hooks!`).
    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        self.warmup.on_historical_quotes(quotes)
    }

    fn on_historical_trades(&mut self, trades: &[TradeTick]) -> anyhow::Result<()> {
        self.warmup.on_historical_trades(trades)
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.warmup.on_quote(quote)
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.warmup.on_trade(trade)
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if let Err(e) = Rc::clone(&self.warmup).on_data_response(self, response) {
            *self.failure.borrow_mut() = Some(format!("{e:#}"));
        }
        if !self.warmup.is_warming() {
            if let Some(traded) = self.traded.clone() {
                *self.exits.borrow_mut() = traded.at(self)?.ok().map(|sides| sides.long);
            }
            self.shutdown_system(Some("the warmup ended".to_string()));
        }
        Ok(())
    }
}

/// What a warmup did.
#[derive(Debug)]
struct Warmed {
    /// The bars handed to the indicator, in order.
    bars: Vec<Bar>,
    failure: Option<String>,
}

/// Runs a warmup of `length` on `bar_type` on a node with the catalog in
/// `dir`, its history asked of `venue`.
fn warm(
    dir: &Path,
    venue: &Rc<RefCell<FakeVenue>>,
    bar_type: &str,
    length: SignedDuration,
) -> Warmed {
    let requirements = DataRequirements {
        bar_types: vec![BarType::from(bar_type)],
        ticks: vec![],
        custom: vec![],
        warmup: length,
    };
    let fed = Rc::new(Fed::default());
    let warmup = Warmup::live(&requirements).feeding(Rc::clone(&fed) as SharedActorIndicator);
    let (failure, _) = run(dir, venue, &requirements, warmup, None, None);
    Warmed {
        bars: fed.bars.borrow().clone(),
        failure,
    }
}

/// Runs `warmup`, `roller` and `traded` of a strategy of `requirements` on a
/// node with the catalog in `dir`, its history asked of `venue`: why it
/// failed, if it did, and the long side's target when it ended.
fn run(
    dir: &Path,
    venue: &Rc<RefCell<FakeVenue>>,
    requirements: &DataRequirements,
    warmup: Warmup,
    roller: Option<Roller>,
    traded: Option<Rc<TargetSetter>>,
) -> (Option<String>, Option<SideTarget>) {
    let warmup = Rc::new(warmup);
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("WARMUP-001"),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(
            dir.to_str().unwrap().to_string(),
            None,
            None,
        )],
        cache: Some(warmup.cache_config().unwrap()),
        data_engine: warmup::live_data_engine_config(&requirements.bar_types),
        ..Default::default()
    };
    let failure = Rc::new(RefCell::new(None));
    let exits = Rc::new(RefCell::new(None));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_name("WARMUP")
            .with_delay_post_stop_secs(0)
            .add_data_client_with_routing(
                None,
                Box::new(FakeFactory(Rc::clone(venue))),
                Box::new(FakeConfig),
                RoutingConfig {
                    default: false,
                    venues: Some(vec!["GLBX".to_string()]),
                },
            )
            .unwrap()
            .build()
            .unwrap();
        node.add_actor(Warming {
            core: DataActorCore::new(DataActorConfig::default()),
            warmup,
            roller,
            traded,
            exits: Rc::clone(&exits),
            failure: Rc::clone(&failure),
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(60), node.run())
            .await
            .expect("the warmup ends within a minute")
            .unwrap();
        node.dispose();
    });
    let failure = failure.borrow().clone();
    let exits = exits.take();
    (failure, exits)
}

/// The time a bar closes, in ns.
fn close_of(bar: &Bar) -> u64 {
    bar.ts_event.as_u64()
}

/// Whether `bars` are one every `step` ns, oldest first, so each once.
fn one_every(bars: &[Bar], step: u64) -> bool {
    bars.windows(2)
        .all(|pair| close_of(&pair[1]) == close_of(&pair[0]) + step)
}

/// A warmup of 72 hours is asked of the venue one window at a time, none
/// holding more than a day, each starting 1 ns after the one before, from
/// the start of the warmup to its end: no time is asked twice or left out.
#[test]
fn a_long_warmup_is_asked_in_windows_under_a_day_covering_it_all() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);

    warm(dir.path(), &venue, COMPOSITE, WARMUP);

    let asked = venue.borrow().asked.clone();
    let day = 24 * HOUR_NS;
    assert_eq!(asked.len(), 4, "{asked:?}");
    assert!(asked.iter().all(|&(from, to)| to - from < day), "{asked:?}");
    assert!(
        asked.windows(2).all(|pair| pair[1].0 == pair[0].1 + 1),
        "{asked:?}"
    );
    let (first, last) = (asked[0].0, asked[3].1);
    assert_eq!(last - first, 72 * HOUR_NS, "{asked:?}");
}

/// Nautilus builds a composite bar type's bars from the source bars of every
/// window into one aggregator and caches them under its standard type: the
/// warmup hands each of them to the indicator, once, oldest first. Over 72
/// hours, dropping the first, partial, bar: 71 closes on the hour, or 72 or
/// 70 if the run lands on an hour's edge; each closes at its last minute's
/// price and opens at its first, the venue's 60 minutes up to its close.
#[test]
fn a_composite_bar_types_warmup_hands_every_bar_once_oldest_first() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);

    let warmed = warm(dir.path(), &venue, COMPOSITE, WARMUP);

    assert_eq!(warmed.failure, None);
    let bars = warmed.bars;
    assert!((70..=72).contains(&bars.len()), "{} bars", bars.len());
    assert!(one_every(&bars, HOUR_NS), "{bars:?}");
    for bar in &bars {
        let close = close_of(bar) - COMPOSITE_DELAY_NS;
        assert_eq!(close % HOUR_NS, 0, "{bar:?}");
        assert_eq!(bar.close, price_at(close), "{bar:?}");
        assert_eq!(bar.open, price_at(close - 59 * MINUTE_NS), "{bar:?}");
    }
}

/// So are the bars Nautilus builds from a window's quotes: a warmup of 30
/// hours spans two windows.
#[test]
fn bars_built_from_quotes_are_handed_over_once_each_across_windows() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);

    let warmed = warm(
        dir.path(),
        &venue,
        MID_MINUTES,
        SignedDuration::from_hours(30),
    );

    assert_eq!(warmed.failure, None);
    assert_eq!(venue.borrow().asked.len(), 2);
    assert!(warmed.bars.len() >= 29 * 60, "{} bars", warmed.bars.len());
    assert!(one_every(&warmed.bars, MINUTE_NS));
}

/// A market closed over several windows, as a weekend is: Nautilus moves to
/// the next duration after an empty window and ends a request, reporting
/// success, when they run out. The windows after the empty ones are asked
/// all the same, and the warmup reaches now.
#[test]
fn a_run_of_empty_windows_does_not_end_the_warmup() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    let now = UnixNanos::from(Timestamp::now()).as_u64();
    venue.borrow_mut().closed = Some((now - 66 * HOUR_NS, now - 6 * HOUR_NS));

    let warmed = warm(dir.path(), &venue, COMPOSITE, WARMUP);

    assert_eq!(warmed.failure, None);
    assert_eq!(venue.borrow().asked.len(), 4);
    let last = close_of(warmed.bars.last().expect("bars"));
    assert!(now - last <= 2 * HOUR_NS, "the last bar closes at {last}");
}

/// IB's client answers without an envelope: Nautilus takes the answer as
/// complete, so the next window is asked. Without that, a warmup on IB would
/// wait for ever.
#[test]
fn an_answer_without_an_envelope_moves_to_the_next_window() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Plain);

    let warmed = warm(dir.path(), &venue, COMPOSITE, WARMUP);

    assert_eq!(warmed.failure, None);
    assert_eq!(venue.borrow().asked.len(), 4);
    assert!((70..=72).contains(&warmed.bars.len()));
    assert!(one_every(&warmed.bars, HOUR_NS));
}

/// The venue cuts the history at the third window: no later window is
/// asked, the warmup ends naming the reason, and the indicator holds what
/// was built up to there, each bar once.
#[test]
fn a_window_failing_ends_the_warmup_with_what_was_built_handed_over() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::FailedAt(2));

    let warmed = warm(dir.path(), &venue, COMPOSITE, WARMUP);

    assert_eq!(venue.borrow().asked.len(), 3);
    let failure = warmed.failure.expect("a failed warmup");
    assert!(failure.contains("venue cut the history"), "{failure}");
    assert!(
        (60..=72).contains(&warmed.bars.len()),
        "{}",
        warmed.bars.len()
    );
    assert!(one_every(&warmed.bars, HOUR_NS));
}

/// The usual start, on a catalog already filled: the first warmup's answers
/// are written back window by window, and the second reads them from the
/// catalog, asking the venue only for the time since, and hands over the
/// same bars once each.
#[test]
fn a_warmup_the_catalog_holds_asks_the_venue_only_for_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let first = FakeVenue::new(Answer::Envelope);
    warm(
        dir.path(),
        &first,
        MID_MINUTES,
        SignedDuration::from_hours(30),
    );
    let first_end = first.borrow().asked.last().unwrap().1;
    let second = FakeVenue::new(Answer::Envelope);

    let warmed = warm(
        dir.path(),
        &second,
        MID_MINUTES,
        SignedDuration::from_hours(30),
    );

    assert_eq!(warmed.failure, None);
    let asked = second.borrow().asked.clone();
    assert!(
        asked.iter().all(|&(from, _)| from > first_end),
        "{asked:?} after {first_end}"
    );
    assert!(warmed.bars.len() >= 29 * 60, "{} bars", warmed.bars.len());
    assert!(one_every(&warmed.bars, MINUTE_NS));
}

/// `dir` by a path relative to the working directory: up to the root, then
/// down to it.
fn relative(dir: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let mut relative: PathBuf = std::iter::repeat_n("..", cwd.components().count() - 1).collect();
    relative.push(dir.strip_prefix("/").unwrap());
    relative
}

/// So it does with the catalog given by a path relative to the working
/// directory, as `live --catalog` takes one and `Live` hands it to the node.
#[test]
fn a_warmup_reads_what_a_catalog_given_relative_to_the_working_directory_holds() {
    let dir = tempfile::tempdir().unwrap();
    let first = FakeVenue::new(Answer::Envelope);
    warm(
        dir.path(),
        &first,
        MID_MINUTES,
        SignedDuration::from_hours(30),
    );
    let first_end = first.borrow().asked.last().unwrap().1;
    let second = FakeVenue::new(Answer::Envelope);

    let warmed = warm(
        &relative(dir.path()),
        &second,
        MID_MINUTES,
        SignedDuration::from_hours(30),
    );

    assert_eq!(warmed.failure, None);
    let asked = second.borrow().asked.clone();
    assert!(
        asked.iter().all(|&(from, _)| from > first_end),
        "{asked:?} after {first_end}"
    );
    assert!(warmed.bars.len() >= 29 * 60, "{} bars", warmed.bars.len());
    assert!(one_every(&warmed.bars, MINUTE_NS));
}

/// The long side's target when a warmup of 72 hours of `requirements`' ends:
/// a bracket's take profit two ATRs away, its stop one, the ATR of model_ema's
/// config, on hourly bars.
fn bracket_of(bar_types: Vec<BarType>, ticks: Vec<TickSeries>) -> SideTarget {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    let requirements = DataRequirements {
        bar_types,
        ticks,
        custom: vec![],
        warmup: WARMUP,
    };
    let spec: Target = toml::from_str(
        r#"
        limit = "45m"
        [bracket]
        take_profit = { atr = 2.0 }
        stop = { atr = 1.0 }
        atr = { period = 14, ma_type = "WILDER" }
        "#,
    )
    .unwrap();
    let target = TargetConfig {
        instrument_id: InstrumentId::from(GCZ26),
        quantity: Quantity::from(1),
        spec,
    };
    let hourly = DecisionRule::Every(SignedDuration::from_hours(1));
    // No order is sent: nothing is charged.
    let commission = Commission::PerOrder(Money::from("0 USD"));
    let traded = Rc::new(TargetSetter::new(&target, hourly, &requirements, commission).unwrap());
    let warmup = traded.warmed(Warmup::live(&requirements));
    let (failure, exits) = run(
        dir.path(),
        &venue,
        &requirements,
        warmup,
        None,
        Some(traded),
    );
    assert_eq!(failure, None);
    assert_eq!(venue.borrow().asked.len(), 4);
    exits.expect("a bracket: the ATR is warm when the warmup ends")
}

/// Every hour's true range is 60 and each bar reaches the ATR once, in order,
/// so the bracket is 3 ATRs wide, 180, within what the first bar's range, 59,
/// still weighs after a Wilder's 70 bars. A bar handed over again would throw
/// the true range back by hundreds, and the Wilder's memory would hold it.
fn assert_three_atrs_wide(exits: &SideTarget) {
    let bracket = exits.bracket.unwrap();
    let width = bracket.take_profit.as_f64() - bracket.stop_loss.as_f64();
    assert!((179.0..=181.0).contains(&width), "{exits:?}");
}

/// A strategy reading quotes: the ATR's bars are built from them, by the one
/// request of the quotes, across its windows.
#[test]
fn a_warmup_of_quotes_warms_the_atr_of_a_bracket() {
    let exits = bracket_of(
        vec![],
        vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(GCZ26),
        }],
    );

    assert_three_atrs_wide(&exits);
}

/// A strategy reading composite bars: the ATR is on the same hour bars, which
/// a node caches under their standard type.
#[test]
fn a_warmup_of_composite_bars_warms_the_atr_of_a_bracket() {
    let exits = bracket_of(vec![BarType::from(COMPOSITE)], vec![]);

    assert_three_atrs_wide(&exits);
}

/// The `data` of GCZ26.
fn of_gcz26(data: NautilusDataType) -> TickSeries {
    TickSeries {
        data,
        instrument_id: InstrumentId::from(GCZ26),
    }
}

/// Runs a live warmup of `length` of `series` on a node with the catalog in
/// `dir`, its history asked of `venue`, as a model handle starts one: the
/// warmup hands features its ticks, of the history and live, and the roller
/// subscribes to the series after the warmup's start. The features, once the
/// warmup has ended.
fn read_live(
    dir: &Path,
    venue: &Rc<RefCell<FakeVenue>>,
    series: TickSeries,
    length: SignedDuration,
) -> Rc<Fed> {
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![series.clone()],
        custom: vec![],
        warmup: length,
    };
    let features = Rc::new(Fed::default());
    let warmup = Warmup::live(&requirements)
        .reading(vec![series.clone()])
        .feeding_ticks(Rc::clone(&features) as SharedActorIndicator);
    let roller = Roller::live(vec![series]);
    let (failure, _) = run(dir, venue, &requirements, warmup, Some(roller), None);
    assert_eq!(failure, None);
    features
}

/// Asserts the venue sent its first live tick before it was asked its last
/// window: the tick came while the warmup was pending.
fn assert_live_before_the_last_window(venue: &FakeVenue) {
    let first = venue.live.first().expect("a live tick");
    assert!(
        first.asked < venue.asked.len(),
        "the live tick came once {} of {} windows were asked",
        first.asked,
        venue.asked.len()
    );
}

/// The venue answers each window after a while, as over a network, and sends
/// a quote at once on the subscription, while the warmup is not answered yet:
/// the features read every quote of the warmup, two windows of 23 hours over
/// 30, then the live one, in time order.
#[test]
fn features_read_every_warmup_quote_before_a_live_one() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().latency = Some(Duration::from_millis(20));

    let features = read_live(
        dir.path(),
        &venue,
        of_gcz26(NautilusDataType::QuoteTick),
        SignedDuration::from_hours(30),
    );

    let venue = venue.borrow();
    assert_eq!(venue.asked.len(), 2);
    assert_live_before_the_last_window(&venue);
    let read = features.quotes.borrow();
    assert!(read.len() >= 29 * 60, "{} quotes", read.len());
    let older = read.windows(2).position(|pair| pair[1] < pair[0]);
    assert_eq!(older, None, "a quote older than the one read before it");
    assert_eq!(
        read.last().copied(),
        venue.live.first().map(|live| live.made)
    );
}

/// So with trades: the features read every trade of the warmup, then the
/// live one the venue sends on the subscription.
#[test]
fn features_read_every_warmup_trade_before_a_live_one() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().latency = Some(Duration::from_millis(20));

    let features = read_live(
        dir.path(),
        &venue,
        of_gcz26(NautilusDataType::TradeTick),
        SignedDuration::from_hours(30),
    );

    let venue = venue.borrow();
    assert_eq!(venue.asked.len(), 2);
    assert_live_before_the_last_window(&venue);
    let read = features.trades.borrow();
    assert!(read.len() >= 29 * 60, "{} trades", read.len());
    let older = read.windows(2).position(|pair| pair[1] < pair[0]);
    assert_eq!(older, None, "a trade older than the one read before it");
    assert_eq!(
        read.last().copied(),
        venue.live.first().map(|live| live.made)
    );
}

/// The venue resends its latest quote on the subscription, as IC does: made
/// at the warmup's last minute, which the history holds, stamped on receipt;
/// then sends a new one. The features read that minute once, from the
/// warmup, then the new quote.
#[test]
fn the_venues_resend_of_the_warmups_last_quote_is_read_once() {
    let dir = tempfile::tempdir().unwrap();
    let length = SignedDuration::from_hours(30);
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().latency = Some(Duration::from_millis(20));
    venue.borrow_mut().resends = Some(length);

    let features = read_live(
        dir.path(),
        &venue,
        of_gcz26(NautilusDataType::QuoteTick),
        length,
    );

    let venue = venue.borrow();
    assert_live_before_the_last_window(&venue);
    let [resent, new] = venue.live[..] else {
        panic!("{:?}", venue.live)
    };
    let (_, end) = *venue.asked.last().expect("a window asked");
    assert_eq!(
        resent.made,
        (end / MINUTE_NS) * MINUTE_NS,
        "the resend is of the history's last minute, the last window ending at {end}"
    );
    let read = features.quotes.borrow();
    let of_the_minute = read.iter().filter(|&&made| made == resent.made).count();
    assert_eq!(
        of_the_minute, 1,
        "the resent minute read {of_the_minute} times"
    );
    assert_eq!(read.last(), Some(&new.made));
    let older = read.windows(2).position(|pair| pair[1] < pair[0]);
    assert_eq!(older, None, "a quote older than the one read before it");
}

/// A catalog holds the oldest windows of a warmup of 72 hours, its quotes up
/// to 30 hours ago: of the four windows of 23 hours, the first is read from
/// it, the second from it up to its end, and the venue is asked the rest of
/// the second and the two after, each answered after a while, the live quote
/// coming while they are pending. The features read every minute of the
/// warmup once, oldest first, the catalog's and the venue's alike, then the
/// live quote.
#[test]
fn features_read_a_catalogs_windows_then_the_venues_before_a_live_quote() {
    let dir = tempfile::tempdir().unwrap();
    let now = UnixNanos::from(Timestamp::now()).as_u64();
    let (from, to) = (now - 73 * HOUR_NS, now - 30 * HOUR_NS);
    let id = InstrumentId::from(GCZ26);
    let quotes: Vec<QuoteTick> = minutes(from, to, None)
        .map(|ns| quote_at(id, ns, ns))
        .collect();
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_to_parquet(&quotes, Some(from.into()), Some(to.into()), None)
        .unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().latency = Some(Duration::from_millis(20));

    let features = read_live(
        dir.path(),
        &venue,
        of_gcz26(NautilusDataType::QuoteTick),
        WARMUP,
    );

    let venue = venue.borrow();
    assert_eq!(venue.asked.len(), 3, "{:?}", venue.asked);
    assert!(
        venue.asked.iter().all(|&(asked_from, _)| asked_from > to),
        "{:?} asked of the venue, the catalog ending at {to}",
        venue.asked
    );
    assert_live_before_the_last_window(&venue);
    let read = features.quotes.borrow();
    let (live, warmup) = read.split_last().expect("quotes");
    assert!(warmup.len() >= 72 * 60, "{} quotes", warmup.len());
    assert!(
        warmup.windows(2).all(|pair| pair[1] == pair[0] + MINUTE_NS),
        "a minute repeated, lost or out of order"
    );
    assert_eq!(Some(*live), venue.live.first().map(|live| live.made));
}

/// A warmup of 72 hours of deltas on `dir`'s catalog: the book they build, how
/// many batches cleared it, and what the venue was asked.
fn warm_deltas(dir: &Path) -> (Rc<Books>, Vec<(u64, u64)>) {
    let venue = FakeVenue::new(Answer::Envelope);
    let id = InstrumentId::from(GCZ26);
    let ticks = vec![TickSeries {
        data: NautilusDataType::OrderBookDelta,
        instrument_id: id,
    }];
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: ticks.clone(),
        custom: vec![],
        warmup: WARMUP,
    };
    let books = Rc::new(Books {
        book: RefCell::new(OrderBook::new(id, BookType::L3_MBO)),
        fed: RefCell::new(vec![]),
    });
    let warmup = Warmup::live(&requirements)
        .reading(ticks)
        .feeding_books(Rc::clone(&books) as Rc<dyn BookIndicator>);
    let (failure, _) = run(dir, &venue, &requirements, warmup, None, None);
    assert_eq!(failure, None);
    let asked = venue.borrow().asked.clone();
    (books, asked)
}

/// The book holds one bid for each minute since the last UTC midnight, the
/// day's snapshot among them, up to the latest minute, as the venue's feed
/// builds it, whatever snapshots the windows opened with; and each delta of
/// the feed came once, in order, a minute after the one before or two across
/// a midnight's snapshot. A repeat would not show in the book: an order added
/// twice is one order.
fn assert_the_days_book(books: &Books) {
    let book = books.book.borrow();
    let best = book.best_bid_price().expect("a bid");
    let minute = best.as_f64() as u64;
    let now = UnixNanos::from(Timestamp::now()).as_u64();
    assert!(
        now / MINUTE_NS - minute <= 2,
        "the best bid is of minute {minute}"
    );
    let since_midnight = minute - (minute * MINUTE_NS / DAY_NS) * DAY_NS / MINUTE_NS;
    let orders: usize = book.bids(None).map(|level| level.len()).sum();
    assert_eq!(orders as u64, since_midnight + 1);
    let fed = books.fed.borrow();
    assert!(
        fed.windows(2)
            .all(|pair| [MINUTE_NS, 2 * MINUTE_NS].contains(&(pair[1] - pair[0]))),
        "a delta repeated, lost or out of order"
    );
}

/// The venue answers the first window of an empty catalog, and each after it
/// from its day's start as the windows before wrote it back, with the rest
/// asked of the venue.
#[test]
fn deltas_a_venue_answers_in_windows_build_the_days_book() {
    let dir = tempfile::tempdir().unwrap();

    let (books, asked) = warm_deltas(dir.path());

    assert_eq!(asked.len(), 4);
    assert_the_days_book(&books);
}

/// The catalog holds them: each window is read from its day's start and opens
/// with a snapshot of the book at its own start.
#[test]
fn deltas_a_catalog_answers_in_windows_build_the_days_book_across_their_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    warm_deltas(dir.path());

    let (books, _) = warm_deltas(dir.path());

    assert_the_days_book(&books);
}

/// What `Streaming` saw when LSE's first quote of its gold series came: the
/// quote, the definitions in the cache then, and why the warmup failed, if
/// it did.
#[derive(Debug, Clone)]
struct Streamed {
    quote: QuoteTick,
    gold: Option<InstrumentAny>,
    gcz26: Option<InstrumentAny>,
}

/// Starts a warmup and its roller as a model handle does, GCZ26's
/// definition in the cache as its broker's client puts it there; stops the
/// node on LSE's first quote of its gold series.
#[derive(Debug)]
struct Streaming {
    core: DataActorCore,
    warmup: Rc<Warmup>,
    roller: Roller,
    streamed: Rc<RefCell<Option<Streamed>>>,
    failure: Rc<RefCell<Option<String>>>,
}

nautilus_actor!(Streaming);

impl DataActor for Streaming {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.cache_rc().borrow_mut().add_instrument(gcz26())?;
        Rc::clone(&self.warmup).start(self)?;
        self.roller.clone().start(self)
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        let gold = InstrumentId::from(lse::testing::GOLD_FUTURES);
        if quote.instrument_id != gold || self.streamed.borrow().is_some() {
            return Ok(());
        }
        let streamed = {
            let cache = self.cache();
            Streamed {
                quote: *quote,
                gold: cache.instrument(&gold),
                gcz26: cache.instrument(&InstrumentId::from(GCZ26)),
            }
        };
        *self.streamed.borrow_mut() = Some(streamed);
        self.shutdown_system(Some("LSE's first quote came".to_string()));
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if let Err(e) = Rc::clone(&self.warmup).on_data_response(self, response) {
            *self.failure.borrow_mut() = Some(format!("{e:#}"));
        }
        Ok(())
    }
}

/// A live start reading LSE's gold futures quotes and GCZ26's, on a node
/// whose catalog holds the gold series' definition, written by hand as LSE
/// publishes none, and an older GCZ26 than the broker's: the LSE client,
/// against the mock LSE socket and routed LSEDGE as the live node routes
/// it, beside a venue's routed GLBX. The warmup asks the gold series'
/// definition, which the catalog answers and the data engine caches before
/// the roller's subscription reaches the LSE client: that reads the
/// definition's precision, and LSE's quote streams at it, bid and ask with
/// no sizes. GCZ26's, which the cache holds, is not asked, so the broker's
/// stays. LSE serves no quotes of the gold series' past: LSE's mock peer
/// refuses them as LSE does (`400 data-unavailable`), and the warmup ends
/// with what the venue served and LSE's refusal.
#[test]
fn an_lsedge_definition_the_cache_lacks_comes_from_the_catalog_before_lse_streams_it() {
    let dir = tempfile::tempdir().unwrap();
    let older = {
        let InstrumentAny::FuturesContract(mut contract) = gcz26() else {
            unreachable!("a futures contract")
        };
        contract.ts_init = UnixNanos::from(1);
        InstrumentAny::FuturesContract(contract)
    };
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(vec![lse::testing::gold_futures(), older])
        .unwrap();
    let socket = lse::testing::MockLseSocket::start(|_, frame| lse::testing::data_ws_answer(frame));
    let history = lse::testing::MockLse::start(vec![lse::testing::Reply::refusal(
        400,
        lse::testing::DATA_UNAVAILABLE_QUOTES,
    )]);
    let gold = InstrumentId::from(lse::testing::GOLD_FUTURES);
    let ticks = vec![
        TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: gold,
        },
        of_gcz26(NautilusDataType::QuoteTick),
    ];
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: ticks.clone(),
        custom: vec![],
        warmup: SignedDuration::from_hours(1),
    };
    let warmup = Rc::new(Warmup::live(&requirements).reading(ticks.clone()));
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("WARMUP-001"),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
        )],
        cache: Some(warmup.cache_config().unwrap()),
        data_engine: warmup::live_data_engine_config(&requirements.bar_types),
        ..Default::default()
    };
    let lse = lse::LseDataClientConfig::builder()
        .api_key(nautilus_core::string::secret::SecretString::from(
            lse::testing::API_KEY,
        ))
        .base_url_http(history.url().to_string())
        .base_url_ws_data(socket.url().to_string())
        .stream_ids(vec![gold])
        .build();
    let streamed = Rc::new(RefCell::new(None));
    let failure = Rc::new(RefCell::new(None));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_name("WARMUP")
            .with_delay_post_stop_secs(0)
            .add_data_client_with_routing(
                None,
                Box::new(FakeFactory(FakeVenue::new(Answer::Envelope))),
                Box::new(FakeConfig),
                RoutingConfig {
                    default: false,
                    venues: Some(vec!["GLBX".to_string()]),
                },
            )
            .unwrap()
            .add_data_client_with_routing(
                None,
                Box::new(lse::LseDataClientFactory::new()),
                Box::new(lse),
                RoutingConfig {
                    default: false,
                    venues: Some(vec!["LSEDGE".to_string()]),
                },
            )
            .unwrap()
            .build()
            .unwrap();
        node.add_actor(Streaming {
            core: DataActorCore::new(DataActorConfig::default()),
            warmup,
            roller: Roller::live(ticks),
            streamed: Rc::clone(&streamed),
            failure: Rc::clone(&failure),
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(60), node.run())
            .await
            .expect("LSE's quote comes within a minute")
            .unwrap();
        node.dispose();
    });

    let streamed = streamed
        .borrow()
        .clone()
        .expect("a quote of the gold series");
    assert_eq!(
        streamed.gold.map(|gold| gold.id()),
        Some(gold),
        "the gold series' definition in the cache"
    );
    assert_eq!(
        streamed.gcz26.map(|gcz26| gcz26.ts_init()),
        Some(gcz26().ts_init()),
        "the broker's GCZ26"
    );
    let quote = streamed.quote;
    assert_eq!(
        (
            quote.bid_price,
            quote.ask_price,
            quote.bid_size,
            quote.ask_size
        ),
        (
            Price::from("3888.0"),
            Price::from("3888.2"),
            Quantity::from(0),
            Quantity::from(0)
        )
    );
    let failure = failure
        .borrow()
        .clone()
        .expect("LSE serves no quotes of the past");
    assert!(failure.contains("data-unavailable"), "{failure}");
}

/// What `GlbxStreaming` saw of ESZ26 on the node.
#[derive(Debug, Default)]
struct GlbxSeen {
    quotes: Vec<QuoteTick>,
    trades: Vec<TradeTick>,
}

/// Starts a warmup and its roller of ESZ26's quotes and trades, as a model
/// handle does, ESZ26's definition in the cache as its broker's client puts
/// it there; lets the mock CME stream send its live records once the first
/// quote came, and stops the node on the first trade.
#[derive(Debug)]
struct GlbxStreaming {
    core: DataActorCore,
    warmup: Rc<Warmup>,
    roller: Roller,
    stream: Rc<lse::testing::MockCmeStream>,
    seen: Rc<RefCell<GlbxSeen>>,
}

nautilus_actor!(GlbxStreaming);

impl DataActor for GlbxStreaming {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.cache_rc()
            .borrow_mut()
            .add_instrument(lse::testing::esz26())?;
        Rc::clone(&self.warmup).start(self)?;
        self.roller.clone().start(self)
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.seen.borrow_mut().quotes.push(*quote);
        self.stream.release();
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.seen.borrow_mut().trades.push(*trade);
        self.shutdown_system(Some("LSE's first trade came".to_string()));
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        // The warmup's outcome is not under test here.
        let _ = Rc::clone(&self.warmup).on_data_response(self, response);
        Ok(())
    }
}

/// A live start reading ESZ26's quotes and trades, `GLBX`, as the live node
/// routes them: LSE's client routed GLBX, against the mock LSE peer and the
/// mock CME stream replaying LSE's documented ESZ6, beside another client
/// on no venue, as IB's is. The roller's subscriptions reach LSE's client,
/// which streams the contract's opening best bid and offer, then its
/// records: LSE's quotes and trade, not the other client's. The warmup's
/// two days of the contract's history are asked of LSE too, which answers
/// that it holds none (`404 data-unavailable`); the other client is asked
/// nothing. The other client is not made the default route, as the live
/// node makes IB's: Nautilus' node builder makes the first client on no
/// venue it registers the default, in no set order, and then refuses
/// another made the default ("default client already registered").
#[test]
fn glbx_data_comes_from_lse_s_cme_stream_routed_glbx_beside_another_client() {
    let dir = tempfile::tempdir().unwrap();
    let stream = Rc::new(lse::testing::MockCmeStream::start(|_, _| {
        lse::testing::held(
            lse::testing::dbn_frames(lse::testing::CME_ESZ6_DOCUMENTED),
            4,
        )
    }));
    let not_loaded = lse::testing::Reply::refusal(404, lse::testing::DATA_UNAVAILABLE_TODAY);
    let history = lse::testing::MockLse::start(
        std::iter::once(lse::testing::Reply::json(lse::testing::PAIRS_ESZ6))
            .chain(std::iter::repeat_n(not_loaded, 16))
            .collect(),
    );
    let es = InstrumentId::from(lse::testing::ESZ26);
    let ticks: Vec<TickSeries> = [NautilusDataType::QuoteTick, NautilusDataType::TradeTick]
        .into_iter()
        .map(|data| TickSeries {
            data,
            instrument_id: es,
        })
        .collect();
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: ticks.clone(),
        custom: vec![],
        warmup: SignedDuration::from_hours(48),
    };
    let warmup = Rc::new(Warmup::live(&requirements).reading(ticks.clone()));
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("WARMUP-001"),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
        )],
        cache: Some(warmup.cache_config().unwrap()),
        data_engine: warmup::live_data_engine_config(&requirements.bar_types),
        ..Default::default()
    };
    let lse = lse::LseDataClientConfig::builder()
        .cme_calls_per_minute(std::num::NonZeroU32::new(6000).unwrap())
        .cme_api_key(nautilus_core::string::secret::SecretString::from(
            lse::testing::CME_API_KEY,
        ))
        .base_url_http(history.url().to_string())
        .base_url_ws_cme(stream.url().to_string())
        .stream_ids(vec![es])
        .build();
    let other = FakeVenue::new(Answer::Envelope);
    let seen = Rc::new(RefCell::new(GlbxSeen::default()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_name("WARMUP")
            .with_delay_post_stop_secs(0)
            .add_data_client_with_routing(
                None,
                Box::new(FakeFactory(Rc::clone(&other))),
                Box::new(FakeConfig),
                RoutingConfig {
                    default: false,
                    venues: None,
                },
            )
            .unwrap()
            .add_data_client_with_routing(
                None,
                Box::new(lse::LseDataClientFactory::new()),
                Box::new(lse),
                RoutingConfig {
                    default: false,
                    venues: Some(vec!["GLBX".to_string()]),
                },
            )
            .unwrap()
            .build()
            .unwrap();
        node.add_actor(GlbxStreaming {
            core: DataActorCore::new(DataActorConfig::default()),
            warmup,
            roller: Roller::live(ticks),
            stream: Rc::clone(&stream),
            seen: Rc::clone(&seen),
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(60), node.run())
            .await
            .expect("LSE's trade comes within a minute")
            .unwrap();
        node.dispose();
    });

    let seen = seen.borrow();
    let quotes: Vec<_> = seen
        .quotes
        .iter()
        .map(|quote| {
            (
                quote.bid_price,
                quote.bid_size,
                quote.ask_price,
                quote.ask_size,
            )
        })
        .collect();
    let level = |bid: &str, bid_size: u64, ask: &str, ask_size: u64| {
        (
            Price::from(bid),
            Quantity::from(bid_size),
            Price::from(ask),
            Quantity::from(ask_size),
        )
    };
    assert_eq!(
        quotes,
        [
            level("7569.75", 2, "7570.00", 11),
            level("7569.50", 18, "7570.00", 12),
            level("7569.50", 18, "7570.00", 12),
        ]
    );
    let trade = seen.trades[0];
    assert_eq!(
        (trade.price, trade.size, trade.aggressor_side),
        (
            Price::from("7570.00"),
            Quantity::from(1),
            AggressorSide::NoAggressor
        )
    );
    let exported: Vec<_> = history
        .calls()
        .iter()
        .filter(|call| call.path == "/v3/export")
        .map(|call| {
            (
                call.param("pair_id").map(ToString::to_string),
                call.param("data").map(ToString::to_string),
            )
        })
        .collect();
    for data in ["quotes", "trades"] {
        assert!(
            exported.contains(&(Some("4000000001".to_string()), Some(data.to_string()))),
            "the warmup's {data} of ESZ26 not asked of LSE: {exported:?}"
        );
    }
    let other = other.borrow();
    assert!(
        other.asked.is_empty() && other.live.is_empty(),
        "the other client was asked of GLBX: {:?} {:?}",
        other.asked,
        other.live
    );
}
