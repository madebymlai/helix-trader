//! fill-catalog's fill through a Nautilus node, at its seam: what the
//! catalog lacks (`catalog::missing`), filled by a `NodeFill` whose node holds
//! a fake data client answering as Databento's does (a completed
//! `DataResponseEnvelope`, `RequestFailed` on an error), and the catalog.
//! The data engine writes each answer back; the tests read the catalog,
//! what the client was asked, and what the fill returns. Hermetic: no
//! venue. Each test runs its nodes on its own thread, as Nautilus' message
//! bus is thread-local.
//!
//! ```sh
//! cargo test --test node_fill
//! ```

use std::{
    any::Any,
    cell::RefCell,
    path::Path,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use helix_trader::{
    catalog::{FillData, NodeClient, NodeFill, missing},
    strategy::{DataRequirements, TickSeries},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            BookDeltasResponse, BookDepthResponse, DataRequestFailed, DataResponseEnvelope,
            InstrumentResponse, RequestBookDeltas, RequestBookDepth, RequestInstrument,
            RequestTrades, TradesResponse,
        },
    },
};
use nautilus_core::{Params, UnixNanos, datetime::datetime_to_unix_nanos};
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::{
        BookOrder, NautilusDataType, OrderBookDelta, OrderBookDepth, TradeTick, stubs::stub_depth10,
    },
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, TradeId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::futures_contract_es},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const GCZ6: &str = "GCZ6.GLBX";

/// Friday 17 October 2025 to Tuesday 21 October: a weekend without data
/// inside.
const FRI: &str = "2025-10-17T00:00:00Z";
const SAT: &str = "2025-10-18T00:00:00Z";
const SUN: &str = "2025-10-19T00:00:00Z";
const MON: &str = "2025-10-20T00:00:00Z";
const TUE: &str = "2025-10-21T00:00:00Z";
const WED: &str = "2025-10-22T00:00:00Z";

const DAY: SignedDuration = SignedDuration::from_hours(24);

fn ts(s: &str) -> Timestamp {
    s.parse().unwrap()
}

fn ns(s: &str) -> u64 {
    UnixNanos::from(ts(s)).as_u64()
}

/// 1 ns before `s`: the end of the window before it.
fn before(s: &str) -> u64 {
    ns(s) - 1
}

/// The time 1 ns before `s`.
fn end_before(s: &str) -> Timestamp {
    ts(s) - SignedDuration::from_nanos(1)
}

fn gcz6() -> InstrumentAny {
    let mut contract = futures_contract_es(None, None);
    contract.id = InstrumentId::from(GCZ6);
    InstrumentAny::FuturesContract(contract)
}

fn depth(at: &str) -> OrderBookDepth {
    let mut depth = stub_depth10();
    depth.instrument_id = InstrumentId::from(GCZ6);
    depth.ts_event = ts(at).into();
    depth.ts_init = ts(at).into();
    depth
}

/// A bid of one contract at 4000.00 added to GCZ6's book at `at`, the
/// last delta of its event.
fn delta(at: &str) -> OrderBookDelta {
    OrderBookDelta::new(
        InstrumentId::from(GCZ6),
        BookAction::Add,
        BookOrder::new(OrderSide::Buy, Price::from("4000.00"), Quantity::from(1), 1),
        RecordFlag::F_LAST as u8,
        1,
        ts(at).into(),
        ts(at).into(),
    )
}

fn trade(id: &str, at: &str) -> TradeTick {
    TradeTick::new(
        InstrumentId::from(id),
        Price::from("4000.00"),
        Quantity::from(1),
        AggressorSide::Buy,
        TradeId::new("1"),
        ts(at).into(),
        ts(at).into(),
    )
}

/// The book of GCZ6 the venue has: a snapshot on Friday, Monday and
/// Tuesday, none on the weekend.
fn book() -> Vec<OrderBookDepth> {
    vec![
        depth("2025-10-17T10:00:00Z"),
        depth("2025-10-20T10:00:00Z"),
        depth("2025-10-21T10:00:00Z"),
    ]
}

/// How the fake venue answers a data request.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Answer {
    /// A completed envelope, at once.
    Data,
    /// A completed envelope, 50 ms later from a task, as Databento's
    /// client answers from its HTTP request.
    Later,
    /// `RequestFailed` with the rows got, as Databento's client on an
    /// error.
    Failed,
    /// A completed envelope with a failure reason, as IC's client.
    FailureEnvelope,
    /// The request refused (`Err`).
    Refused,
    /// Trades of GCQ6 to a request of GCZ6's: rows the engine cannot
    /// write back.
    OtherContract,
}

/// What the fake venue holds and was asked.
#[derive(Debug)]
struct FakeVenue {
    book: Vec<OrderBookDepth>,
    answer: Answer,
    /// Each request: what, from, to (ns), and its `price_precision` param.
    asked: Vec<(FillData, u64, u64, Option<u64>)>,
    /// Requests asked and not answered yet, and the most at once.
    in_flight: Arc<AtomicUsize>,
    most_in_flight: usize,
}

impl FakeVenue {
    fn new(answer: Answer) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            book: book(),
            answer,
            asked: vec![],
            in_flight: Arc::default(),
            most_in_flight: 0,
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

/// A data client answering as the fake venue says; on no venue, so
/// routed by the node's routing, as Databento's.
struct FakeClient {
    id: ClientId,
    venue: Rc<RefCell<FakeVenue>>,
    sender: EventSender<DataEvent>,
    connected: bool,
}

impl FakeClient {
    /// Records the request of `data` over `[start, end]` with `params`,
    /// then answers it with `response` as the venue says.
    fn answer(
        &self,
        data: FillData,
        request_id: nautilus_core::UUID4,
        (start, end): (u64, u64),
        params: Option<&Params>,
        response: DataResponse,
    ) -> anyhow::Result<()> {
        let mut venue = self.venue.borrow_mut();
        let precision = params.and_then(|p| p.get_u64("price_precision"));
        venue.asked.push((data, start, end, precision));
        let in_flight = venue.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        venue.most_in_flight = venue.most_in_flight.max(in_flight);
        let done = Arc::clone(&venue.in_flight);
        let envelope = |response| DataResponseEnvelope::new(response, true, None);
        let event = match venue.answer {
            Answer::Data | Answer::OtherContract => DataEvent::ResponseEnvelope(envelope(response)),
            Answer::Later => {
                let sender = self.sender.clone();
                let event = DataEvent::ResponseEnvelope(envelope(response));
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    done.fetch_sub(1, Ordering::SeqCst);
                    sender.send(event).unwrap();
                });
                return Ok(());
            }
            Answer::Failed => DataEvent::RequestFailed(DataRequestFailed {
                request_id,
                reason: "venue cut the history".to_string(),
                response: Some(response),
            }),
            Answer::FailureEnvelope => DataEvent::ResponseEnvelope(
                envelope(response).with_failure("venue cut the history".to_string()),
            ),
            Answer::Refused => anyhow::bail!("venue refused the request"),
        };
        done.fetch_sub(1, Ordering::SeqCst);
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

    /// The definition, at once, whatever the venue's answer to data.
    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let precision = request
            .params
            .as_ref()
            .and_then(|p| p.get_u64("price_precision"));
        self.venue
            .borrow_mut()
            .asked
            .push((FillData::Instrument, start, end, precision));
        let response = InstrumentResponse::new(
            request.request_id,
            self.id,
            request.single_instrument_id()?,
            gcz6(),
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params,
        );
        let envelope =
            DataResponseEnvelope::new(DataResponse::Instrument(Box::new(response)), true, None);
        self.sender.send(DataEvent::ResponseEnvelope(envelope))?;
        Ok(())
    }

    fn request_book_depth(&self, request: RequestBookDepth) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let rows: Vec<OrderBookDepth> = self
            .venue
            .borrow()
            .book
            .iter()
            .filter(|d| (start..=end).contains(&d.ts_init.as_u64()))
            .cloned()
            .collect();
        let response = DataResponse::BookDepth(BookDepthResponse::new(
            request.request_id,
            self.id,
            request.single_instrument_id()?,
            rows,
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params.clone(),
        ));
        self.answer(
            FillData::Depth,
            request.request_id,
            (start, end),
            request.params.as_ref(),
            response,
        )
    }

    /// One delta, on Monday at 10:00, if the window holds it.
    fn request_book_deltas(&self, request: RequestBookDeltas) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let rows: Vec<OrderBookDelta> = [delta("2025-10-20T10:00:00Z")]
            .into_iter()
            .filter(|d| (start..=end).contains(&d.ts_init.as_u64()))
            .collect();
        let response = DataResponse::BookDeltas(BookDeltasResponse::new(
            request.request_id,
            self.id,
            request.single_instrument_id()?,
            rows,
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params.clone(),
        ));
        self.answer(
            FillData::Deltas,
            request.request_id,
            (start, end),
            request.params.as_ref(),
            response,
        )
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let (start, end) = window(request.start, request.end);
        let of = if self.venue.borrow().answer == Answer::OtherContract {
            "GCQ6.GLBX"
        } else {
            GCZ6
        };
        let response = DataResponse::Trades(TradesResponse::new(
            request.request_id,
            self.id,
            request.instrument_ids.clone(),
            vec![trade(of, "2025-10-17T10:00:00Z")],
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            UnixNanos::from(end),
            request.params.clone(),
        ));
        self.answer(
            FillData::Trades,
            request.request_id,
            (start, end),
            request.params.as_ref(),
            response,
        )
    }
}

/// The requirements reading `data` of GCZ6.
fn reading(data: NautilusDataType) -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data,
            instrument_id: InstrumentId::from(GCZ6),
        }],
        warmup: SignedDuration::ZERO,
    }
}

/// fill-catalog's fill through the node of the series `requirements` reads
/// over `[start, end]` into the catalog in `dir`: what the catalog lacks,
/// asked of the fake `venue` in windows of `period`.
fn fill(
    dir: &Path,
    venue: &Rc<RefCell<FakeVenue>>,
    requirements: &DataRequirements,
    (start, end): (Timestamp, Timestamp),
    period: SignedDuration,
) -> anyhow::Result<()> {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let requests = missing(&catalog, &requirements.slices(start, end))?;
    let mut node = NodeFill::new();
    node.add(
        NodeClient {
            factory: Box::new(FakeFactory(Rc::clone(venue))),
            config: Box::new(FakeConfig),
            routing: RoutingConfig {
                default: false,
                venues: Some(vec!["GLBX".to_string()]),
            },
        },
        &requests,
        |_| period,
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(node.run(dir))
}

fn depth_intervals(dir: &Path) -> Vec<(u64, u64)> {
    ParquetDataCatalog::new(dir, None, None, None, None)
        .get_intervals(&NautilusDataType::OrderBookDepth.into(), Some(GCZ6))
        .unwrap()
}

/// What the catalog in `dir` lacks of GCZ6's book over `[from, to]`.
fn depth_lacking(dir: &Path, (from, to): (&str, &str)) -> Vec<(u64, u64)> {
    ParquetDataCatalog::new(dir, None, None, None, None)
        .get_missing_intervals_for_request(
            ns(from),
            ns(to),
            &NautilusDataType::OrderBookDepth.into(),
            Some(GCZ6),
        )
        .unwrap()
}

fn stored_book(dir: &Path) -> Vec<u64> {
    let mut catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog
        .order_book_depths(Some(vec![GCZ6.to_string()]), None, None)
        .unwrap()
        .iter()
        .map(|d| d.ts_init.as_u64())
        .collect()
}

#[test]
fn each_day_is_asked_and_written_back_and_a_wider_fill_asks_only_the_new_day() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Data);
    let book_of = reading(NautilusDataType::OrderBookDepth);

    fill(
        dir.path(),
        &venue,
        &book_of,
        (ts(FRI), end_before(TUE)),
        DAY,
    )
    .unwrap();
    fill(dir.path(), &venue, &book_of, (ts(FRI), ts(WED)), DAY).unwrap();
    let asked_twice = venue.borrow().asked.len();
    fill(dir.path(), &venue, &book_of, (ts(FRI), ts(WED)), DAY).unwrap();

    // The definition over the window, then a day a request, each with the
    // definition's price precision; the weekend asked once, answered
    // empty. Then only what Tuesday adds; then nothing.
    let precision = Some(2);
    assert_eq!(
        venue.borrow().asked,
        [
            (FillData::Instrument, ns(FRI), before(TUE), None),
            (FillData::Depth, ns(FRI), before(SAT), precision),
            (FillData::Depth, ns(SAT), before(SUN), precision),
            (FillData::Depth, ns(SUN), before(MON), precision),
            (FillData::Depth, ns(MON), before(TUE), precision),
            // The definition stored, the catalog answers it.
            (FillData::Depth, ns(TUE), before(WED), precision),
            (FillData::Depth, ns(WED), ns(WED), precision),
        ]
    );
    assert_eq!(asked_twice, 7);
    // The empty weekend and the empty instant of Wednesday covered too.
    assert_eq!(depth_lacking(dir.path(), (FRI, WED)), []);
    assert_eq!(
        stored_book(dir.path()),
        book()
            .iter()
            .map(|d| d.ts_init.as_u64())
            .collect::<Vec<_>>()
    );
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let stored = catalog
        .instruments(Some(&[GCZ6.to_string()]), None, None)
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].id(), InstrumentId::from(GCZ6));
}

/// The book stored over Friday to Monday but for 0.2 s before Friday's
/// midnight, as a live node's daily promotions leave a sliver between
/// their files. The venue holds nothing in the sliver.
#[test]
fn a_sliver_between_stored_files_is_asked_once_and_closed_by_its_write_back() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog
        .write_to_parquet(
            &[depth("2025-10-17T10:00:00Z")],
            Some(ts(FRI).into()),
            Some(ts("2025-10-17T23:59:59.5Z").into()),
            None,
        )
        .unwrap();
    catalog
        .write_to_parquet(
            &[depth("2025-10-20T10:00:00Z")],
            Some(ts("2025-10-17T23:59:59.7Z").into()),
            Some(end_before(TUE).into()),
            None,
        )
        .unwrap();
    let venue = FakeVenue::new(Answer::Data);
    let book_of = reading(NautilusDataType::OrderBookDepth);

    for _ in 0..2 {
        fill(
            dir.path(),
            &venue,
            &book_of,
            (ts(FRI), end_before(TUE)),
            DAY,
        )
        .unwrap();
    }

    // The sliver and its definition, by the first fill only.
    let sliver = (
        ns("2025-10-17T23:59:59.500000001Z"),
        ns("2025-10-17T23:59:59.699999999Z"),
    );
    assert_eq!(
        venue.borrow().asked,
        [
            (FillData::Instrument, sliver.0, sliver.1, None),
            (FillData::Depth, sliver.0, sliver.1, Some(2)),
        ]
    );
}

/// Friday from midnight to noon in windows of three hours: the book's
/// snapshot at 10:00 is in the last.
#[test]
fn a_slow_client_is_asked_one_window_at_a_time_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Later);
    let three_hours = SignedDuration::from_hours(3);
    let morning = (
        ts("2025-10-17T00:00:00Z"),
        end_before("2025-10-17T12:00:00Z"),
    );

    fill(
        dir.path(),
        &venue,
        &reading(NautilusDataType::OrderBookDepth),
        morning,
        three_hours,
    )
    .unwrap();

    let windows: Vec<(u64, u64)> = venue.borrow().asked[1..]
        .iter()
        .map(|&(_, from, to, _)| (from, to))
        .collect();
    assert_eq!(
        windows,
        [
            (ns("2025-10-17T00:00:00Z"), before("2025-10-17T03:00:00Z")),
            (ns("2025-10-17T03:00:00Z"), before("2025-10-17T06:00:00Z")),
            (ns("2025-10-17T06:00:00Z"), before("2025-10-17T09:00:00Z")),
            (ns("2025-10-17T09:00:00Z"), before("2025-10-17T12:00:00Z")),
        ]
    );
    assert_eq!(venue.borrow().most_in_flight, 1);
}

#[test]
fn a_failed_request_fails_the_fill_naming_it_and_writes_nothing() {
    let friday = "the book depths of GCZ6.GLBX over \
                  [2025-10-17T00:00:00Z, 2025-10-17T23:59:59.999999999Z]";
    for (answer, failure) in [
        // The venue's reason.
        (Answer::Failed, "failed: venue cut the history"),
        (Answer::FailureEnvelope, "failed: venue cut the history"),
        // Nautilus' reason, not the client's.
        (Answer::Refused, "failed: "),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let venue = FakeVenue::new(answer);

        let err = fill(
            dir.path(),
            &venue,
            &reading(NautilusDataType::OrderBookDepth),
            (ts(FRI), end_before(SUN)),
            DAY,
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains(&format!("{friday} {failure}")),
            "{answer:?}: {err}"
        );
        // The first day failed: the second never asked.
        assert_eq!(venue.borrow().asked.len(), 2, "{answer:?}");
        assert_eq!(depth_intervals(dir.path()), [], "{answer:?}");
    }
}

#[test]
fn rows_the_engine_does_not_write_back_fail_the_coverage_check() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::OtherContract);

    let err = fill(
        dir.path(),
        &venue,
        &reading(NautilusDataType::TradeTick),
        (ts(FRI), end_before(SAT)),
        DAY,
    )
    .unwrap_err()
    .to_string();

    assert!(
        err.contains("the trades of GCZ6.GLBX over [2025-10-17T00:00:00Z"),
        "{err}"
    );
    assert!(err.contains("write-back failed"), "{err}");
}

#[test]
fn book_deltas_are_asked_a_day_a_request_and_written_back() {
    let dir = tempfile::tempdir().unwrap();
    let venue = FakeVenue::new(Answer::Data);

    fill(
        dir.path(),
        &venue,
        &reading(NautilusDataType::OrderBookDelta),
        (ts(MON), end_before(WED)),
        DAY,
    )
    .unwrap();

    let precision = Some(2);
    assert_eq!(
        venue.borrow().asked,
        [
            (FillData::Instrument, ns(MON), before(WED), None),
            (FillData::Deltas, ns(MON), before(TUE), precision),
            (FillData::Deltas, ns(TUE), before(WED), precision),
        ]
    );
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let stored: Vec<OrderBookDelta> = catalog
        .order_book_deltas(Some(vec![GCZ6.to_string()]), None, None)
        .unwrap();
    assert_eq!(stored, [delta("2025-10-20T10:00:00Z")]);
}
