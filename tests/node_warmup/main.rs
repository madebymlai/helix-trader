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

use std::{any::Any, cell::RefCell, path::Path, rc::Rc, time::Duration};

use helix_trader::{
    model::{
        decide::DecisionRule,
        targets::{Barrier, BarrierExits, Side, Stops, TargetsConfig},
    },
    strategy::{
        DataRequirements, TickSeries,
        warmup::{self, Warmup},
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
            BarsResponse, DataRequestFailed, DataResponseEnvelope, QuotesResponse, RequestBars,
            RequestQuotes,
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
    data::{Bar, BarType, NautilusDataType, QuoteTick, TradeTick},
    identifiers::{ClientId, InstrumentId, TraderId, Venue},
    instruments::{InstrumentAny, stubs::futures_contract_es},
    types::{Price, Quantity},
};
use nautilus_persistence::config::DataCatalogConfig;

const GCZ26: &str = "GCZ26.GLBX";
/// Hour bars built from the venue's minute bars.
const COMPOSITE: &str = "GCZ26.GLBX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";
/// Minute bars built from the venue's quotes.
const MID_MINUTES: &str = "GCZ26.GLBX-1-MINUTE-MID-INTERNAL";

const HOUR_NS: u64 = 3_600_000_000_000;
const MINUTE_NS: u64 = 60_000_000_000;
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
    /// Each request's window, in ns.
    asked: Vec<(u64, u64)>,
}

impl FakeVenue {
    fn new(answer: Answer) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            answer,
            closed: None,
            asked: vec![],
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
        self.sender.send(event)?;
        Ok(())
    }
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

    /// The venue's quotes in the request's window, one on each minute.
    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let id = request.single_instrument_id()?;
        let closed = self.venue.borrow().closed;
        let rows: Vec<QuoteTick> = minutes(start, end, closed)
            .map(|ns| {
                let price = price_at(ns);
                QuoteTick::new(
                    id,
                    price,
                    price,
                    Quantity::from(1),
                    Quantity::from(1),
                    ns.into(),
                    ns.into(),
                )
            })
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
}

/// An indicator recording each bar the warmup hands it.
#[derive(Debug, Default)]
struct Fed {
    bars: RefCell<Vec<Bar>>,
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

    fn handle_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
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
    /// The barrier's stops, started as a model handle does, and read for the
    /// long side when the warmup ends.
    stops: Option<Rc<Stops>>,
    exits: Rc<RefCell<Option<BarrierExits>>>,
    failure: Rc<RefCell<Option<String>>>,
}

nautilus_actor!(Warming);

impl DataActor for Warming {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.cache_rc().borrow_mut().add_instrument(gcz26())?;
        Rc::clone(&self.warmup).start(self)?;
        if let Some(stops) = self.stops.clone() {
            stops.start(self);
        }
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if let Err(e) = Rc::clone(&self.warmup).on_data_response(self, response) {
            *self.failure.borrow_mut() = Some(format!("{e:#}"));
        }
        if !self.warmup.is_warming() {
            if let Some(stops) = self.stops.clone() {
                *self.exits.borrow_mut() = stops.at(self, Side::Long)?;
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
        warmup: length,
    };
    let fed = Rc::new(Fed::default());
    let warmup = Warmup::live(&requirements).feeding(Rc::clone(&fed) as SharedActorIndicator);
    let (failure, _) = run(dir, venue, &requirements, warmup, None);
    Warmed {
        bars: fed.bars.borrow().clone(),
        failure,
    }
}

/// Runs `warmup`, and `stops`, of a strategy of `requirements` on a node with
/// the catalog in `dir`, its history asked of `venue`: why it failed, if it
/// did, and the long side's barrier when it ended.
fn run(
    dir: &Path,
    venue: &Rc<RefCell<FakeVenue>>,
    requirements: &DataRequirements,
    warmup: Warmup,
    stops: Option<Rc<Stops>>,
) -> (Option<String>, Option<BarrierExits>) {
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
            stops,
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
    let exits = exits.borrow().clone();
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

/// The long side's barrier when a warmup of 72 hours of `requirements`' ends:
/// a bracket's take profit two ATRs away, its stop one, the ATR of model_ema's
/// config, on hourly bars.
fn bracket_of(bar_types: Vec<BarType>, ticks: Vec<TickSeries>) -> BarrierExits {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Envelope);
    let requirements = DataRequirements {
        bar_types,
        ticks,
        warmup: WARMUP,
    };
    let barrier: Barrier = toml::from_str(
        r#"
        take_profit = { atr = 2.0 }
        stop = { atr = 1.0 }
        limit = "45m"
        atr = { period = 14, ma_type = "WILDER" }
        "#,
    )
    .unwrap();
    let targets = TargetsConfig {
        instrument_id: InstrumentId::from(GCZ26),
        quantity: Quantity::from(1),
        barrier: Some(barrier),
        horizon: None,
    };
    let hourly = DecisionRule::Every(SignedDuration::from_hours(1));
    let stops = Rc::new(
        Stops::new(&targets, hourly, &requirements)
            .unwrap()
            .expect("a barrier"),
    );
    let warmup = stops.warmed(Warmup::live(&requirements));
    let (failure, exits) = run(dir.path(), &venue, &requirements, warmup, Some(stops));
    assert_eq!(failure, None);
    assert_eq!(venue.borrow().asked.len(), 4);
    exits.expect("a bracket: the ATR is warm when the warmup ends")
}

/// Every hour's true range is 60 and each bar reaches the ATR once, in order,
/// so the bracket is 3 ATRs wide, 180, within what the first bar's range, 59,
/// still weighs after a Wilder's 70 bars. A bar handed over again would throw
/// the true range back by hundreds, and the Wilder's memory would hold it.
fn assert_three_atrs_wide(exits: &BarrierExits) {
    let width = exits.take_profit.as_f64() - exits.stop.as_f64();
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
