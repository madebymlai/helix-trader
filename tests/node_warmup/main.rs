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
    backtest::{Backtest, Commission},
    model::{
        dataset::DatasetVenue,
        decide::DecisionRule,
        targets::{SideTarget, Target, TargetConfig, TargetSetter},
    },
    strategy::{
        requirements::{DataRequirements, TickSeries},
        subscriptions::Subscriptions,
        warmup::{self, BookIndicator, Warmup},
    },
    venues,
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
    live::{get_runtime, runner::get_data_event_sender, sender::EventSender},
    logging::logger::LoggerConfig,
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
use nautilus_core::{
    UUID4, UnixNanos, datetime::datetime_to_unix_nanos, string::secret::SecretString,
    time::get_atomic_clock_realtime,
};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveNodeConfig, RoutingConfig},
};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, Data, NautilusDataType, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth, QuoteTick, TradeTick,
    },
    enums::{AggressorSide, BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, StrategyId, TradeId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::futures_contract_es},
    orderbook::OrderBook,
    types::{Money, Price, Quantity},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;

const GCZ26: &str = "GCZ26.GLBX";
/// CME's E-mini S&P 500 of December 2026, of a venue whose history, IB's,
/// serves its own minute bars, which GLBX's, LSE's, does not
/// (`venues::serves`).
const ESZ26: &str = "ESZ26.CME";
/// Hour bars built from the venue's minute bars.
const COMPOSITE: &str = "ESZ26.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";
/// Minute bars built from the venue's quotes.
const MID_MINUTES: &str = "GCZ26.GLBX-1-MINUTE-MID-INTERNAL";
/// Minute bars of ESZ26 built from the venue's quotes.
const ES_MID_MINUTES: &str = "ESZ26.CME-1-MINUTE-MID-INTERNAL";

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

fn esz26() -> InstrumentAny {
    let mut contract = futures_contract_es(None, None);
    contract.id = InstrumentId::from(ESZ26);
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
    /// How long it takes to answer a request of quotes, if not `latency`.
    quotes_latency: Option<Duration>,
    /// How long it takes to answer each window of a series, given its
    /// kind (as `kinds`), its instrument and how many windows of it the
    /// venue was asked before, if not as the latencies above say.
    window_latency: Option<fn(&str, InstrumentId, usize) -> Duration>,
    /// How long before each minute's close its trade is made, in ns: none,
    /// on the close, as its quote.
    trades_early: u64,
    /// If a subscription resends the warmup's last tick first, made as it
    /// was and stamped on receipt, as IC does its latest: the warmup's
    /// length, from which and the start of the first window asked, an empty
    /// catalog's, the venue tells the warmup's end, and so its last minute.
    resends: Option<SignedDuration>,
    /// Each request's window, in ns.
    asked: Vec<(u64, u64)>,
    /// What each request asked, in the order asked: `bar`, `deltas`,
    /// `quote` or `trade`.
    kinds: Vec<&'static str>,
    /// The instrument of each request, in the order asked.
    instruments: Vec<InstrumentId>,
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
            quotes_latency: None,
            window_latency: None,
            trades_early: 0,
            resends: None,
            asked: vec![],
            kinds: vec![],
            instruments: vec![],
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

    fn name(&self) -> &'static str {
        "FAKE"
    }

    fn config_type(&self) -> &'static str {
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
        let (kind, latency) = match &response {
            DataResponse::Quotes(_) => ("quote", venue.quotes_latency.or(venue.latency)),
            DataResponse::Trades(_) => ("trade", venue.latency),
            DataResponse::BookDeltas(_) => ("deltas", venue.latency),
            _ => ("bar", venue.latency),
        };
        let instrument_id = match &response {
            DataResponse::Quotes(quotes) => quotes.instrument_ids[0],
            DataResponse::Trades(trades) => trades.instrument_ids[0],
            DataResponse::BookDeltas(deltas) => deltas.instrument_id,
            DataResponse::Bars(bars) => bars.bar_type.instrument_id(),
            other => unreachable!("the fake venue answers no {other:?}"),
        };
        let before = venue
            .kinds
            .iter()
            .zip(&venue.instruments)
            .filter(|&(&of, &id)| of == kind && id == instrument_id)
            .count();
        let latency = venue.window_latency.map_or(latency, |of_window| {
            Some(of_window(kind, instrument_id, before))
        });
        venue.kinds.push(kind);
        venue.instruments.push(instrument_id);
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

        match latency {
            None => self.sender.send(event)?,
            Some(latency) => {
                let sender = self.sender.clone();
                get_runtime().spawn(async move {
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
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
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

    /// The venue's trades in the request's window, one on each minute,
    /// made `FakeVenue::trades_early` before it.
    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let id = request.single_instrument_id()?;
        let rows: Vec<TradeTick> = trades_made(&self.venue.borrow(), start, end)
            .map(|made| trade_at(id, made, made))
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

/// When the trades `venue` holds in `[start, end]` were made, in ns: one a
/// minute, `FakeVenue::trades_early` before its close.
fn trades_made(venue: &FakeVenue, start: u64, end: u64) -> impl Iterator<Item = u64> {
    let early = venue.trades_early;
    minutes(start + early, end, venue.closed).map(move |ns| ns - early)
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
    fn handle_depth(&self, _depth: &OrderBookDepth) {}

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
    /// The subscriptions to the bars of the requirements and of the
    /// target's ATR and to the tick series the features read, started from
    /// where the warmup ends, as a model handle starts them; none of the
    /// series rolls.
    subscriptions: Subscriptions,
    /// The target, warmed as a model handle does, and read for the long
    /// side when the warmup ends.
    traded: Option<Rc<TargetSetter>>,
    exits: Rc<RefCell<Option<SideTarget>>>,
    failure: Rc<RefCell<Option<String>>>,
    /// The close of each live bar the strategy got, in ns, and whether the
    /// warmup was pending then.
    live_bars: Rc<RefCell<Vec<(u64, bool)>>>,
}

nautilus_actor!(Warming);

impl DataActor for Warming {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.cache_rc().borrow_mut().add_instrument(gcz26())?;
        self.cache_rc().borrow_mut().add_instrument(esz26())?;
        let now = Rc::clone(&self.warmup).start(self)?;
        self.subscriptions.clone().start(self, now);
        Ok(())
    }

    /// The deltas, of the warmup and live, go to the warmup, as a model
    /// strategy hands them (`model_data_hooks!`).
    fn on_historical_book_deltas(&mut self, deltas: &[OrderBookDelta]) -> anyhow::Result<()> {
        self.warmup.on_historical_book_deltas(deltas)
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        self.warmup.on_book_deltas(deltas)
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

    /// The live bars go to the warmup, as a model strategy hands them
    /// (`model_data_hooks!`); each recorded, with whether the warmup was
    /// still pending.
    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let warming = self.warmup.is_warming();
        self.live_bars
            .borrow_mut()
            .push((bar.ts_event.as_u64(), warming));
        self.warmup.on_bar(bar)
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
    let warmup = Warmup::live(&requirements).feeding(
        &requirements.bar_types,
        Rc::clone(&fed) as SharedActorIndicator,
    );
    let Ran { failure, .. } = run(dir, venue, &requirements, warmup, vec![], None);
    Warmed {
        bars: fed.bars.borrow().clone(),
        failure,
    }
}

/// Runs `warmup`, the subscriptions to the bars of `requirements` and of
/// `traded`'s ATR and to `ticks`, and `traded` of a strategy of
/// `requirements` on a node with the catalog in `dir`, its history asked of
/// `venue`: what it did (`Ran`).
fn run(
    dir: &Path,
    venue: &Rc<RefCell<FakeVenue>>,
    requirements: &DataRequirements,
    warmup: Warmup,
    ticks: Vec<TickSeries>,
    traded: Option<Rc<TargetSetter>>,
) -> Ran {
    let atr_bars = traded.as_ref().and_then(|traded| traded.atr_bar_type());
    let bar_types = requirements.bar_types.iter().copied().chain(atr_bars);
    let subscriptions = Subscriptions::new(bar_types.collect(), ticks, vec![]);
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
    let live_bars = Rc::new(RefCell::new(vec![]));
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
                    venues: Some(vec!["GLBX".to_string(), "CME".to_string()]),
                },
            )
            .unwrap()
            .build()
            .unwrap();
        node.add_actor(Warming {
            core: DataActorCore::new(DataActorConfig::default()),
            warmup,
            subscriptions,
            traded,
            exits: Rc::clone(&exits),
            failure: Rc::clone(&failure),
            live_bars: Rc::clone(&live_bars),
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(60), node.run())
            .await
            .expect("the warmup ends within a minute")
            .unwrap();
        node.dispose();
    });
    let failure = failure.borrow().clone();
    Ran {
        failure,
        exits: exits.take(),
        live_bars: live_bars.take(),
    }
}

/// What a run of a warmup did.
#[derive(Debug)]
struct Ran {
    /// Why it failed, if it did.
    failure: Option<String>,
    /// The long side's target when it ended.
    exits: Option<SideTarget>,
    /// The close of each live bar the strategy got, in ns, and whether the
    /// warmup was pending then.
    live_bars: Vec<(u64, bool)>,
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

/// A warmup of 72 hours is asked of the venue one window at a time, each
/// starting 1 ns after the one before, from the start of the warmup to its
/// end: no time is asked twice or left out.
#[rstest]
fn a_long_warmup_is_asked_in_windows_covering_it_all() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);

    warm(dir.path(), &venue, COMPOSITE, WARMUP);

    let asked = venue.borrow().asked.clone();
    assert_eq!(asked.len(), 4, "{asked:?}");
    assert!(
        asked.windows(2).all(|pair| pair[1].0 == pair[0].1 + 1),
        "{asked:?}"
    );
    let (first, last) = (asked[0].0, asked[3].1);
    assert_eq!(last - first, 72 * HOUR_NS, "{asked:?}");
}

/// Each window, with the source bar before its start, which IB's client
/// asks too to keep the bar opening before the start, spans at most a day:
/// ibapi asks such a span in seconds, up to 86,400, and a longer one in
/// sessions, one more than the days it spans, so that a window of a day
/// and a minute fetches three sessions to keep one day's bars.
#[rstest]
fn each_window_with_the_bar_before_it_spans_at_most_a_day() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);

    warm(dir.path(), &venue, COMPOSITE, WARMUP);

    let asked = venue.borrow().asked.clone();
    assert!(!asked.is_empty());
    assert!(
        asked
            .iter()
            .all(|&(from, to)| to - from + MINUTE_NS <= DAY_NS),
        "{asked:?}"
    );
}

/// Nautilus builds a composite bar type's bars from the source bars of every
/// window into one aggregator and caches them under its standard type: the
/// warmup hands each of them to the indicator, once, oldest first. Over 72
/// hours, dropping the first, partial, bar: 71 closes on the hour, or 72 or
/// 70 if the run lands on an hour's edge; each closes at its last minute's
/// price and opens at its first, the venue's 60 minutes up to its close.
#[rstest]
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
#[rstest]
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
#[rstest]
fn a_run_of_empty_windows_does_not_end_the_warmup() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    let now = get_atomic_clock_realtime().get_time_ns().as_u64();
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
#[rstest]
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
#[rstest]
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
/// same bars once each; so it does with the catalog given by a path relative
/// to the working directory, as `live --catalog` takes one and `Live` hands
/// it to the node.
#[rstest]
#[case::absolute(Path::to_path_buf)]
#[case::relative(relative)]
fn a_warmup_the_catalog_holds_asks_the_venue_only_for_the_rest(
    #[case] catalog_path: fn(&Path) -> PathBuf,
) {
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
        &catalog_path(dir.path()),
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

/// The long side's target when a warmup of 72 hours of `requirements`' ends,
/// traded on the instrument they read first: a bracket's take profit two ATRs
/// away, its stop one, the ATR of model_ema's config, on hourly bars.
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
    let target = TargetConfig::builder()
        .instrument_id(requirements.instrument_ids()[0])
        .risk("0.5%".parse().unwrap())
        .spec(spec)
        .build();
    let hourly = DecisionRule::Every(SignedDuration::from_hours(1));
    // No order is sent: nothing is charged.
    let commission = Commission::PerOrder(Money::from("0 USD"));
    let dataset = DatasetVenue {
        commission,
        balance: Money::from("100000 USD"),
        latency: None,
    };
    let traded = Rc::new(TargetSetter::new(&target, hourly, &requirements, &dataset).unwrap());
    let warmup = traded.warmed(Warmup::live(&requirements));
    let Ran { failure, exits, .. } = run(
        dir.path(),
        &venue,
        &requirements,
        warmup,
        vec![],
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
/// request of the quotes, across its windows. A strategy reading composite
/// bars: the ATR is on the same hour bars, which a node caches under their
/// standard type.
#[rstest]
#[case::quotes(vec![], vec![of_gcz26(NautilusDataType::QuoteTick)])]
#[case::composite_bars(vec![BarType::from(COMPOSITE)], vec![])]
fn a_warmup_warms_the_atr_of_a_bracket(
    #[case] bar_types: Vec<BarType>,
    #[case] ticks: Vec<TickSeries>,
) {
    let exits = bracket_of(bar_types, ticks);

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
/// warmup hands features its ticks, of the history and live, and the
/// subscriptions start on the series from where the warmup ends. The
/// features, once the warmup has ended.
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
    let warmup = Warmup::live(&requirements).feeding_ticks(
        &requirements.ticks,
        Rc::clone(&features) as SharedActorIndicator,
    );
    let Ran { failure, .. } = run(dir, venue, &requirements, warmup, vec![series], None);
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
/// a tick at once on the subscription, while the warmup is not answered yet:
/// the features read every quote, or every trade, of the warmup, two windows
/// of 23 hours over 30, then the live one, in time order.
#[rstest]
#[case::quotes(NautilusDataType::QuoteTick, |fed: &Fed| fed.quotes.borrow().clone())]
#[case::trades(NautilusDataType::TradeTick, |fed: &Fed| fed.trades.borrow().clone())]
fn features_read_every_warmup_tick_before_a_live_one(
    #[case] data: NautilusDataType,
    #[case] ticks_read: fn(&Fed) -> Vec<u64>,
) {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().latency = Some(Duration::from_millis(20));

    let features = read_live(
        dir.path(),
        &venue,
        of_gcz26(data),
        SignedDuration::from_hours(30),
    );

    let venue = venue.borrow();
    assert_eq!(venue.asked.len(), 2);
    assert_live_before_the_last_window(&venue);
    let read = ticks_read(&features);
    assert!(read.len() >= 29 * 60, "{} ticks", read.len());
    let older = read.windows(2).position(|pair| pair[1] < pair[0]);
    assert_eq!(older, None, "a tick older than the one read before it");
    assert_eq!(
        read.last().copied(),
        venue.live.first().map(|live| live.made)
    );
}

/// The venue resends its latest quote on the subscription, as IC does: made
/// at the warmup's last minute, which the history holds, stamped on receipt;
/// then sends a new one. The features read that minute once, from the
/// warmup, then the new quote.
#[rstest]
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
#[rstest]
fn features_read_a_catalogs_windows_then_the_venues_before_a_live_quote() {
    let dir = tempfile::tempdir().unwrap();
    let now = get_atomic_clock_realtime().get_time_ns().as_u64();
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

/// A feature reading several series, recording each quote, trade and bar
/// it reads, in one log, so that the order across them shows.
#[derive(Debug, Default)]
struct Flow {
    read: RefCell<Vec<(&'static str, u64)>>,
}

impl ActorIndicator for Flow {
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
        self.read
            .borrow_mut()
            .push(("quote", quote.ts_event.as_u64()));
        Ok(())
    }

    fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        self.read
            .borrow_mut()
            .push(("trade", trade.ts_event.as_u64()));
        Ok(())
    }

    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        self.read.borrow_mut().push(("bar", bar.ts_event.as_u64()));
        Ok(())
    }
}

/// A feature on GCZ26's quotes and trades, two series the warmup asks in
/// windows of their own, two of 23 hours over 30: the venue answers the
/// quotes' windows after a while and the trades' at once, so both of the
/// trades' windows reach the strategy before the quotes' first does. The
/// trades are made 30 seconds before each minute's quote. The feature
/// reads the history as a replay of the same data hands it, every quote and
/// trade once in time order, then the live ones.
#[rstest]
fn a_feature_on_two_series_reads_their_history_in_time_order_however_it_arrives() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().quotes_latency = Some(Duration::from_millis(200));
    venue.borrow_mut().trades_early = 30_000_000_000;
    let series = vec![
        of_gcz26(NautilusDataType::QuoteTick),
        of_gcz26(NautilusDataType::TradeTick),
    ];
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: series.clone(),
        custom: vec![],
        warmup: SignedDuration::from_hours(30),
    };
    let flow = Rc::new(Flow::default());
    let warmup = Warmup::live(&requirements).feeding_ticks(
        &requirements.ticks,
        Rc::clone(&flow) as SharedActorIndicator,
    );

    let Ran { failure, .. } = run(dir.path(), &venue, &requirements, warmup, series, None);

    assert_eq!(failure, None);
    let venue = venue.borrow();
    assert_eq!(venue.kinds, ["quote", "trade", "trade", "quote"]);
    let mut replayed: Vec<(&str, u64)> = venue
        .kinds
        .iter()
        .zip(&venue.asked)
        .flat_map(|(&kind, &(start, end))| -> Vec<(&str, u64)> {
            match kind {
                "quote" => minutes(start, end, None).map(|ns| ("quote", ns)).collect(),
                _ => trades_made(&venue, start, end)
                    .map(|ns| ("trade", ns))
                    .collect(),
            }
        })
        .collect();
    replayed.sort_by_key(|&(_, made)| made);
    let read = flow.read.borrow();
    let (history, live) = read.split_at(read.len() - venue.live.len());
    assert!(history.len() >= 2 * 29 * 60, "{} ticks", history.len());
    assert!(
        history == replayed.as_slice(),
        "the first tick read out of time order: {:?}",
        history
            .iter()
            .zip(&replayed)
            .find(|(read, replayed)| read != replayed)
    );
    let live_made: Vec<u64> = live.iter().map(|&(_, made)| made).collect();
    let sent: Vec<u64> = venue.live.iter().map(|live| live.made).collect();
    assert_eq!(live_made, sent);
}

/// GCZ26's bars of a second, which a warmup builds from its trades.
const LAST_SECONDS: &str = "GCZ26.GLBX-1-SECOND-LAST-INTERNAL";

/// A feature on GCZ26's bars of a second, built from its trades, and on its
/// quotes, the warmup asking the trades and the quotes in two windows each
/// of 23 hours over 30: the venue answers the trades at once and the
/// quotes' windows after 1.5 s each, so the trades' request completes, its
/// bars built, while the quotes' is pending, and Nautilus' aggregator goes
/// live on the venue's live trade, building a live bar a second on, which
/// the strategy hands the warmup while the quotes are still pending. The
/// trades are made 30 seconds before each minute's quote, so each bar
/// closes then. The feature, reading the trades too, reads the history in
/// time order, the bars among the ticks window by window, then the live
/// ticks and bar.
#[rstest]
fn a_feature_on_built_bars_and_quotes_reads_them_in_time_order_however_they_arrive() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().quotes_latency = Some(Duration::from_millis(1500));
    venue.borrow_mut().trades_early = 30_000_000_000;
    let quotes = of_gcz26(NautilusDataType::QuoteTick);
    let requirements = DataRequirements {
        bar_types: vec![BarType::from(LAST_SECONDS)],
        ticks: vec![quotes.clone()],
        custom: vec![],
        warmup: SignedDuration::from_hours(30),
    };
    let flow = Rc::new(Flow::default());
    let warmup = Warmup::live(&requirements)
        .feeding(
            &requirements.bar_types,
            Rc::clone(&flow) as SharedActorIndicator,
        )
        .feeding_ticks(
            &[quotes.clone(), of_gcz26(NautilusDataType::TradeTick)],
            Rc::clone(&flow) as SharedActorIndicator,
        );

    let ran = run(
        dir.path(),
        &venue,
        &requirements,
        warmup,
        vec![quotes],
        None,
    );

    assert_eq!(ran.failure, None);
    let venue = venue.borrow();
    assert_eq!(venue.kinds.last(), Some(&"quote"), "{:?}", venue.kinds);
    let (live_bar, _) = *ran
        .live_bars
        .iter()
        .find(|&&(_, warming)| warming)
        .expect("a live bar while the quotes were pending");
    let read = flow.read.borrow();
    let older = read.windows(2).find(|pair| pair[1].1 < pair[0].1);
    assert_eq!(older, None, "read older than the one before it");
    let made = |kind: &str| -> Vec<u64> {
        read.iter()
            .filter(|(of, _)| *of == kind)
            .map(|&(_, made)| made)
            .collect()
    };
    let asked = |kind: &str| -> Vec<u64> {
        let mut made: Vec<u64> = venue
            .kinds
            .iter()
            .zip(&venue.asked)
            .filter(|(of, _)| **of == kind)
            .flat_map(|(_, &(start, end))| -> Vec<u64> {
                match kind {
                    "quote" => minutes(start, end, None).collect(),
                    _ => trades_made(&venue, start, end).collect(),
                }
            })
            .collect();
        made.sort_unstable();
        made
    };
    // The history's, then the live one.
    let quotes_read = made("quote");
    assert_eq!(quotes_read[..quotes_read.len() - 1], asked("quote"));
    let trades = asked("trade");
    assert_eq!(made("trade"), trades);
    // Each of the history's closes on the second of its one trade, the
    // first, partial, dropped; then the live one.
    let bars = made("bar");
    assert!(bars.len() >= 29 * 60, "{} bars", bars.len());
    assert_eq!(bars[..bars.len() - 1], trades[1..]);
    assert_eq!(bars.last(), Some(&live_bar));
}

/// What a feature handled, whole, so that a replay and a live warmup are
/// compared item for item.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Handled {
    Quote(QuoteTick),
    Trade(TradeTick),
    Bar(Bar),
}

impl Handled {
    fn ts_init(&self) -> u64 {
        match self {
            Self::Quote(quote) => quote.ts_init.as_u64(),
            Self::Trade(trade) => trade.ts_init.as_u64(),
            Self::Bar(bar) => bar.ts_init.as_u64(),
        }
    }
}

/// A feature recording everything it handles, in order: every quote and
/// trade the warmup hands it, of the series it reads, and the bars of
/// `bar_types`, as a feature set routes bars by their type, the warmup
/// handing each indicator it feeds bars every bar.
#[derive(Debug, Default)]
struct Handling {
    bar_types: Vec<BarType>,
    handled: RefCell<Vec<Handled>>,
}

impl ActorIndicator for Handling {
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
        self.handled.borrow_mut().push(Handled::Quote(*quote));
        Ok(())
    }

    fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        self.handled.borrow_mut().push(Handled::Trade(*trade));
        Ok(())
    }

    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        if self.bar_types.contains(&bar.bar_type) {
            self.handled.borrow_mut().push(Handled::Bar(*bar));
        }
        Ok(())
    }
}

/// The warmup of `requirements` handing everything to `handling`, which
/// reads all its series, as a model handle builds a feature set's, and
/// building GCZ26's minute MID bars for a target's ATR, as
/// `TargetSetter::warmed` builds its bars, which no feature reads.
fn handing(requirements: &DataRequirements, warmup: Warmup, handling: &Rc<Handling>) -> Warmup {
    let atr = Rc::new(Handling::default());
    warmup
        .building(BarType::from(MID_MINUTES), atr as SharedActorIndicator)
        .feeding(
            &requirements.bar_types,
            Rc::clone(handling) as SharedActorIndicator,
        )
        .feeding_ticks(
            &requirements.ticks,
            Rc::clone(handling) as SharedActorIndicator,
        )
}

/// A strategy in a backtest, its warmup replayed, its subscriptions those of
/// its requirements from its start, handing its warmup what it gets, as a
/// model strategy does (`model_data_hooks!`).
#[derive(Debug)]
struct Replaying {
    core: StrategyCore,
    warmup: Rc<Warmup>,
    subscriptions: Subscriptions,
}

nautilus_strategy!(Replaying);

impl DataActor for Replaying {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let now = Rc::clone(&self.warmup).start(self)?;
        self.subscriptions.clone().start(self, now);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.warmup.on_quote(quote)
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.warmup.on_trade(trade)
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.warmup.on_bar(bar)
    }
}

/// What a feature reading every series of `requirements` handles in a
/// backtest over `[start, end]`, in ns, of the catalog in `dir`, run on a
/// thread of its own, as a node's message bus is thread-local.
fn replayed(dir: &Path, requirements: &DataRequirements, start: u64, end: u64) -> Vec<Handled> {
    let (dir, requirements) = (dir.to_path_buf(), requirements.clone());
    let at = |ns: u64| Timestamp::from_nanosecond(i128::from(ns)).unwrap();

    std::thread::spawn(move || {
        let handling = Rc::new(Handling {
            bar_types: requirements.bar_types.clone(),
            ..Handling::default()
        });
        let warmup = handing(&requirements, Warmup::replayed(&requirements), &handling);
        let strategy = Replaying {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("REPLAY-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            warmup: Rc::new(warmup),
            subscriptions: Subscriptions::new(
                requirements.bar_types.clone(),
                requirements.ticks.clone(),
                vec![],
            ),
        };
        let backtest = Backtest {
            catalog: dir,
            bar_types: requirements.bar_types.clone(),
            ticks: requirements.ticks.clone(),
            custom: vec![],
            start: at(start),
            end: at(end),
            commission: Commission::PerOrder(Money::from("0 USD")),
            latency: None,
            pairs: vec![],
            balance: Money::from("1000000 USD"),
            venue_oms: venues::oms_type,
            logging: LoggerConfig::default(),
        };
        backtest.run(strategy).unwrap();
        handling.handled.take()
    })
    .join()
    .unwrap()
}

/// How long the venue takes to answer each window of each series of
/// `a_live_warmup_hands_a_feature_its_history_as_a_replay_of_the_same_data_does`,
/// the window of the index given: GCZ26's quotes 300 ms then at once,
/// ESZ26's quotes 100 ms then 400 ms, and GCZ26's trades at once then 700
/// ms, so that the windows arrive in an order of their own, GCZ26's quotes,
/// the ATR's bars built from them, completing while the others are pending
/// and the trades completing last.
fn shuffled(kind: &str, instrument_id: InstrumentId, window: usize) -> Duration {
    let millis: &[u64] = match (kind, instrument_id.symbol.as_str()) {
        ("quote", "GCZ26") => &[300, 0],
        ("quote", _) => &[100, 400],
        _ => &[0, 700],
    };
    Duration::from_millis(millis[window.min(millis.len() - 1)])
}

/// The acceptance check of the warmup's merge: a feature on GCZ26's trades
/// and quotes, `[data]` listing them so, on ESZ26's quotes, and on ESZ26's
/// minute MID bars, built from its quotes, reads a live warmup's history as
/// a backtest replaying the same data hands it, item for item; the warmup
/// builds GCZ26's minute MID bars for a target's ATR too, which no feature
/// reads and a backtest reads no series for. The venue holds a quote of
/// each instrument and a trade of GCZ26 on every minute, so every minute
/// is a tie of the three ticks and the bars closing then; it answers each
/// series' two windows of 23 hours over 30 after delays of their own
/// (`shuffled`).
/// The backtest replays the same quotes and trades, put in a catalog, over
/// the span the live warmup asked. A live warmup drops the first bar, which
/// misses the data before the warmup's start (`skip_first_non_full_bar`):
/// the replay's first bar is left out of the comparison, and nothing else.
#[rstest]
fn a_live_warmup_hands_a_feature_its_history_as_a_replay_of_the_same_data_does() {
    let live_dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    venue.borrow_mut().window_latency = Some(shuffled);
    let requirements = DataRequirements {
        bar_types: vec![BarType::from(ES_MID_MINUTES)],
        ticks: vec![
            of_gcz26(NautilusDataType::TradeTick),
            of_gcz26(NautilusDataType::QuoteTick),
            TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(ESZ26),
            },
        ],
        custom: vec![],
        warmup: SignedDuration::from_hours(30),
    };
    let handling = Rc::new(Handling {
        bar_types: requirements.bar_types.clone(),
        ..Handling::default()
    });
    let warmup = handing(&requirements, Warmup::live(&requirements), &handling);

    let ran = run(
        live_dir.path(),
        &venue,
        &requirements,
        warmup,
        requirements.ticks.clone(),
        None,
    );
    assert_eq!(ran.failure, None);
    let venue = venue.borrow();
    assert_eq!(venue.kinds.len(), 6, "{:?}", venue.kinds);
    let start = venue.asked.iter().map(|&(start, _)| start).min().unwrap();
    let end = venue.asked.iter().map(|&(_, end)| end).max().unwrap();
    let replay_dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(replay_dir.path(), None, None, None, None);
    catalog.write_instruments(vec![gcz26(), esz26()]).unwrap();

    for instrument_id in [GCZ26, ESZ26].map(InstrumentId::from) {
        let quotes: Vec<QuoteTick> = minutes(start, end, None)
            .map(|ns| quote_at(instrument_id, ns, ns))
            .collect();
        catalog
            .write_to_parquet(&quotes, Some(start.into()), Some(end.into()), None)
            .unwrap();
    }
    let trades: Vec<TradeTick> = trades_made(&venue, start, end)
        .map(|ns| trade_at(InstrumentId::from(GCZ26), ns, ns))
        .collect();
    catalog
        .write_to_parquet(&trades, Some(start.into()), Some(end.into()), None)
        .unwrap();

    let replay = replayed(replay_dir.path(), &requirements, start, end);

    let live: Vec<Handled> = handling
        .handled
        .borrow()
        .iter()
        .copied()
        .filter(|handled| handled.ts_init() <= end)
        .collect();
    let first_bar = replay
        .iter()
        .position(|handled| matches!(handled, Handled::Bar(_)))
        .expect("a bar replayed");
    let mut replay = replay;
    let partial = replay.remove(first_bar);
    assert_eq!(
        partial.ts_init(),
        start.div_ceil(MINUTE_NS) * MINUTE_NS,
        "the replay's first bar closes on the warmup's first minute"
    );
    assert!(live.len() >= 4 * 29 * 60, "{} items", live.len());
    let differs = live
        .iter()
        .zip(&replay)
        .position(|(live, replayed)| live != replayed);
    assert!(
        live == replay,
        "{} live, {} replayed; the first to differ: {:?}",
        live.len(),
        replay.len(),
        differs.map(|at| (
            at,
            &live[at.saturating_sub(3)..(at + 4).min(live.len())],
            &replay[at.saturating_sub(3)..(at + 4).min(replay.len())]
        ))
    );
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
        ticks,
        custom: vec![],
        warmup: WARMUP,
    };
    let books = Rc::new(Books {
        book: RefCell::new(OrderBook::new(id, BookType::L3_MBO)),
        fed: RefCell::new(vec![]),
    });
    let warmup = Warmup::live(&requirements).feeding_books(
        &requirements.ticks,
        Rc::clone(&books) as Rc<dyn BookIndicator>,
    );
    let Ran { failure, .. } = run(dir, &venue, &requirements, warmup, vec![], None);
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
    let now = get_atomic_clock_realtime().get_time_ns().as_u64();
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
#[rstest]
fn deltas_a_venue_answers_in_windows_build_the_days_book() {
    let dir = tempfile::tempdir().unwrap();

    let (books, asked) = warm_deltas(dir.path());

    assert_eq!(asked.len(), 4);
    assert_the_days_book(&books);
}

/// The catalog holds them: each window is read from its day's start and opens
/// with a snapshot of the book at its own start.
#[rstest]
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

/// Starts a warmup and its subscriptions as a model handle does, none of
/// the series rolling, GCZ26's definition in the cache as its broker's
/// client puts it there; stops the node on LSE's first quote of its gold
/// series.
#[derive(Debug)]
struct Streaming {
    core: DataActorCore,
    warmup: Rc<Warmup>,
    subscriptions: Subscriptions,
    streamed: Rc<RefCell<Option<Streamed>>>,
    failure: Rc<RefCell<Option<String>>>,
}

nautilus_actor!(Streaming);

impl DataActor for Streaming {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.cache_rc().borrow_mut().add_instrument(gcz26())?;
        let now = Rc::clone(&self.warmup).start(self)?;
        self.subscriptions.clone().start(self, now);
        Ok(())
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
/// the strategy's subscription reaches the LSE client: that reads the
/// definition's precision, and LSE's quote streams at it, bid and ask with
/// no sizes. GCZ26's, which the cache holds, is not asked, so the broker's
/// stays. LSE serves no quotes of the gold series' past: LSE's mock peer
/// refuses them as LSE does (`400 data-unavailable`), and the warmup ends
/// with what the venue served and LSE's refusal.
#[rstest]
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
    let warmup = Rc::new(Warmup::live(&requirements));
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
        .api_key(SecretString::from(lse::testing::API_KEY))
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
            subscriptions: Subscriptions::new(requirements.bar_types.clone(), ticks, vec![]),
            warmup,
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

/// Starts a warmup and its subscriptions to ESZ26's quotes and trades, as a
/// model handle does, ESZ26's definition in the cache as its broker's client
/// puts it there; lets the mock CME stream send its live records once the
/// first quote came, and stops the node on the first trade.
#[derive(Debug)]
struct GlbxStreaming {
    core: DataActorCore,
    warmup: Rc<Warmup>,
    subscriptions: Subscriptions,
    stream: Rc<lse::testing::MockExchangeStream>,
    seen: Rc<RefCell<GlbxSeen>>,
}

nautilus_actor!(GlbxStreaming);

impl DataActor for GlbxStreaming {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.cache_rc()
            .borrow_mut()
            .add_instrument(lse::testing::esz26())?;
        let now = Rc::clone(&self.warmup).start(self)?;
        self.subscriptions.clone().start(self, now);
        Ok(())
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
/// on no venue, as IB's is. The strategy's subscriptions reach LSE's client,
/// which streams the contract's opening best bid and offer, then its
/// records: LSE's quotes and trade, not the other client's. The warmup's
/// two days of the contract's history are asked of LSE too, which answers
/// that it holds none (`404 data-unavailable`); the other client is asked
/// nothing. Neither is made the default route, as on the live node, where
/// IB's data client is routed the venues of its live data: Nautilus' node
/// builder makes the first client on no venue it registers the default, in
/// no set order, and refuses another made the default after it.
#[rstest]
fn glbx_data_comes_from_lse_s_cme_stream_routed_glbx_beside_another_client() {
    let dir = tempfile::tempdir().unwrap();
    let stream = Rc::new(lse::testing::MockExchangeStream::cme(|_, _| {
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
    let warmup = Rc::new(Warmup::live(&requirements));
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
        .cme_api_key(SecretString::from(lse::testing::CME_API_KEY))
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
            subscriptions: Subscriptions::new(requirements.bar_types.clone(), ticks, vec![]),
            warmup,
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

#[path = "../common/mod.rs"]
mod common;

/// A model strategy's first decision on a live node: its model handle's own
/// warmup, warming the features and the target's ATR, through Nautilus' data
/// engine from the fake venue's quotes of GCZ26, and its decision as the
/// warmup ends.
mod model_handle {
    use helix_trader::{
        model::{
            blueprint::ModelBlueprint,
            config::{Margins, ModelConfig},
            features::{FeatureSet, side_names},
            handle::{Decision, Mode, ModelHandle},
            learner::{LearnerConfig, TrainingSet},
            targets::Side,
        },
        strategy::{Declared, EmaArgs, IndicatorsConfig, Series, risk::R},
    };
    use nautilus_common::messages::data::DataResponseEnvelope;
    use nautilus_model::identifiers::StrategyId;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };

    use super::*;
    use crate::common::dataset::{baseline_stored, model_set};

    const STRATEGY: &str = "WARMUP-MODEL-001";

    /// The last mid of GCZ26, Nautilus' exponential average of one,
    /// `[indicators.mid_0]`.
    const MID: Declared<EmaArgs> = Declared::new("mid_0");

    /// Decides once, as its handle's warmup ends, and stops the node.
    #[derive(Debug)]
    struct Deciding {
        core: StrategyCore,
        model: Rc<ModelHandle>,
        /// Each decision made, a trade or none: the one when the warmup ended.
        decisions: Rc<RefCell<Vec<Option<Decision>>>>,
        failure: Rc<RefCell<Option<String>>>,
    }

    nautilus_strategy!(Deciding);

    impl DataActor for Deciding {
        fn on_start(&mut self) -> anyhow::Result<()> {
            self.cache_rc().borrow_mut().add_instrument(gcz26())?;
            Rc::clone(&self.model).start(self)
        }

        /// The quotes, of the warmup and live, go to the warmup, as a model
        /// strategy hands them (`model_data_hooks!`).
        fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
            self.model.warmup().on_historical_quotes(quotes)
        }

        fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
            self.model.warmup().on_quote(quote)
        }

        fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
            let warmup = self.model.warmup();
            if let Err(e) = Rc::clone(&warmup).on_data_response(self, response) {
                *self.failure.borrow_mut() = Some(format!("{e:#}"));
            }

            if !warmup.is_warming() {
                match Rc::clone(&self.model).decide(self) {
                    Ok(decision) => self.decisions.borrow_mut().push(decision),
                    Err(e) => *self.failure.borrow_mut() = Some(format!("{e:#}")),
                }
                self.shutdown_system(Some("the warmup ended".to_string()));
            }
            Ok(())
        }
    }

    /// Hourly decisions on GCZ26 of a bracket whose take profit is two ATRs
    /// away and its stop one, the ATR of model_ema's config, on hourly bars,
    /// the long traded from an expected R of nought, on the baseline learner,
    /// never retrained.
    fn config() -> ModelConfig {
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
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .target(
                TargetConfig::builder()
                    .instrument_id(InstrumentId::from(GCZ26))
                    .risk("0.5%".parse().unwrap())
                    .spec(spec)
                    .build(),
            )
            .dataset(DatasetVenue {
                // No order is sent: nothing is charged.
                commission: Commission::PerOrder(Money::from("0 USD")),
                balance: Money::from("100000 USD"),
                latency: None,
            })
            .window(SignedDuration::from_hours(48))
            .calibration(SignedDuration::from_hours(12))
            .margins(Margins {
                long: Some(R::new(0.0)),
                short: None,
            })
            .min_rows(1)
            .learner(LearnerConfig::Baseline {})
            .build()
    }

    /// The strategy's one feature, the last mid of GCZ26.
    fn features() -> Rc<FeatureSet> {
        let tables: IndicatorsConfig =
            toml::from_str(r#"mid_0 = { period = 1, price_type = "MID" }"#).unwrap();
        FeatureSet::builder()
            .feature(&[Series::quotes(InstrumentId::from(GCZ26))], MID, &tables)
            .build()
            .unwrap()
    }

    /// What the strategy reads: the quotes of GCZ26, with a warmup of 72
    /// hours.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GCZ26),
            }],
            custom: vec![],
            warmup: WARMUP,
        }
    }

    /// The model of the strategy deciding on `features` as `config` says.
    fn blueprint() -> ModelBlueprint {
        ModelBlueprint::new(
            StrategyId::from(STRATEGY),
            features(),
            config(),
            requirements(),
        )
        .unwrap()
    }

    /// Publishes under `models` the long model of `blueprint`, valid from
    /// the epoch, a baseline expecting 0.4R of every row.
    fn publish_long(blueprint: &ModelBlueprint, models: &Path) {
        let config = config();
        let names = side_names(features().names(), &config.target.payoff_names());
        let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![0.4]).unwrap();
        let long = baseline_stored(
            blueprint.fingerprint().unwrap().to_string(),
            names,
            &config,
            Some(Side::Long),
            &set,
        );
        blueprint
            .model_store(models)
            .unwrap()
            .publish(&model_set(&config, Timestamp::UNIX_EPOCH, vec![long], None))
            .unwrap();
    }

    /// Runs the model strategy of `blueprint`, trading live on the models
    /// under `dir`, on a node with the catalog in `dir`, its history asked of
    /// `venue`: why it failed, if it did, and the decisions it made, the one
    /// when its warmup ended.
    fn first_decision(
        dir: &Path,
        venue: &Rc<RefCell<FakeVenue>>,
        blueprint: &ModelBlueprint,
    ) -> (Option<String>, Vec<Option<Decision>>) {
        let model = ModelHandle::new(
            blueprint,
            &Mode::Live {
                models: dir.join("models"),
            },
        )
        .unwrap();
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
            cache: Some(model.warmup().cache_config().unwrap()),
            data_engine: warmup::live_data_engine_config(&requirements().bar_types),
            ..Default::default()
        };
        let failure = Rc::new(RefCell::new(None));
        let decisions = Rc::new(RefCell::new(vec![]));
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
            node.add_strategy(Deciding {
                core: StrategyCore::new(StrategyConfig {
                    strategy_id: Some(StrategyId::from(STRATEGY)),
                    order_id_tag: Some("001".to_string()),
                    ..Default::default()
                }),
                model,
                decisions: Rc::clone(&decisions),
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
        let decisions = decisions.take();
        (failure, decisions)
    }

    /// Worked by hand. The venue's mids rise by one a minute, so every hour's
    /// true range is 60, and Wilder's ATR of 14 hourly bars, warmed over the
    /// 72 hours of the handle's warmup, is 60 within a third of a dollar, the
    /// weight the first bar's range, 59, still has (as
    /// `assert_three_atrs_wide` reads its bracket of 3 ATRs within a dollar
    /// of 180). The first decision, as the warmup ends, trades the long, of
    /// 0.4R, its stop one ATR from the mid: one R on one contract of GCZ26,
    /// whose multiplier is 1, is worth the ATR in USD. Without the warmup
    /// the ATR would be cold and the decision no trade.
    #[rstest]
    fn a_live_warmup_sets_the_first_decisions_stop_from_the_targets_atr() {
        let dir = tempfile::tempdir().unwrap();
        let venue = FakeVenue::new(Answer::Envelope);
        let blueprint = blueprint();
        publish_long(&blueprint, &dir.path().join("models"));

        let (failure, decisions) = first_decision(dir.path(), &venue, &blueprint);

        assert_eq!(failure, None);
        let [Some(decision)] = decisions[..] else {
            panic!("not one trade, the target's ATR warm: {decisions:?}");
        };
        assert_eq!(decision.side, OrderSide::Buy);
        let one_r = decision
            .trade()
            .risk(&gcz26(), Quantity::from(1))
            .unwrap()
            .as_f64();
        assert!((59.66..=60.34).contains(&one_r), "{one_r}: {decision:?}");
    }
}
