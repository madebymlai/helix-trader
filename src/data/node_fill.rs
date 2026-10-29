//! Fills the catalog through a Nautilus node: the sources whose data client
//! answers a request with a completed `DataResponseEnvelope` (Databento's,
//! IC's), which the data engine writes back to the catalog (`update_catalog`).
//!
//! `missing` turns series slices (`strategy::SeriesSlice`) into what the
//! catalog lacks of them, by Nautilus' own count
//! (`get_missing_intervals_for_request`): per contract its definitions,
//! then each series' missing windows. A `NodeFill` gathers each source's
//! data client with its requests, cut into windows of the source's period,
//! and `run` builds a live node with those clients and the catalog, and an
//! actor asking for one window at a time with `update_catalog`. The data
//! engine serves what the catalog holds, asks the client for the rest and
//! writes the answer back over the window asked for. The actor records the
//! first failure (a failed request, a failure envelope, no answer within
//! the timeout) and stops the node; `run` returns it. See `README.md`,
//! "The node fill".

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt,
    path::Path,
    rc::Rc,
};

use anyhow::{Context, bail, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    factories::{ClientConfig, DataClientFactory},
    messages::data::{DataResponse, DataResponseEnvelope},
    nautilus_actor,
    timer::TimeEvent,
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveNodeConfig, RoutingConfig},
};
use nautilus_model::{
    data::NautilusDataType,
    identifiers::{InstrumentId, TraderId},
    instruments::Instrument,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
use serde_json::json;

use super::catalog_path;
use crate::strategy::SeriesSlice;

/// How long `run` waits for the answer to one request at most, by the wall
/// clock: Nautilus' requests have no timeout, so a client that never
/// answers would hold the run forever. A week of IC's gold ticks takes
/// about 40 s, three hours of Databento's MBP-10 about a million rows.
pub const REQUEST_TIMEOUT: SignedDuration = SignedDuration::from_mins(30);

/// The book depth of an `OrderBookDepth` request: MBP-10's.
const DEPTH: usize = 10;

/// The name of the actor's timer of the request in flight.
const DEADLINE: &str = "fill-request-deadline";

/// What a fill asks a source for: an instrument's definition, or one of
/// its series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillData {
    /// The instrument, its definition (`request_instrument`).
    Instrument,
    /// `QuoteTick`s (`request_quotes`).
    Quotes,
    /// `TradeTick`s (`request_trades`).
    Trades,
    /// `OrderBookDepth` snapshots of 10 levels (`request_book_depth`).
    Depth,
    /// `OrderBookDelta`s (`request_book_deltas`).
    Deltas,
}

impl FillData {
    /// The series of `data_type`; an error for a data type no node source
    /// serves here (bars, funding, custom data).
    fn of(data_type: &NautilusDataType) -> anyhow::Result<Self> {
        Ok(match data_type {
            NautilusDataType::QuoteTick => Self::Quotes,
            NautilusDataType::TradeTick => Self::Trades,
            NautilusDataType::OrderBookDepth => Self::Depth,
            NautilusDataType::OrderBookDelta => Self::Deltas,
            other => bail!("no source filled through the node serves {other}"),
        })
    }

    /// The Nautilus data type of a series; none for the instrument.
    #[must_use]
    pub fn data_type(self) -> Option<NautilusDataType> {
        match self {
            Self::Instrument => None,
            Self::Quotes => Some(NautilusDataType::QuoteTick),
            Self::Trades => Some(NautilusDataType::TradeTick),
            Self::Depth => Some(NautilusDataType::OrderBookDepth),
            Self::Deltas => Some(NautilusDataType::OrderBookDelta),
        }
    }
}

impl fmt::Display for FillData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Instrument => "definition",
            Self::Quotes => "quotes",
            Self::Trades => "trades",
            Self::Depth => "book depths",
            Self::Deltas => "book deltas",
        })
    }
}

/// A request of a fill: `data` of `instrument_id` over the closed window
/// `[from, to]`, as the catalog stores intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillRequest {
    pub data: FillData,
    pub instrument_id: InstrumentId,
    pub from: Timestamp,
    pub to: Timestamp,
}

impl fmt::Display for FillRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the {} of {} over [{}, {}]",
            self.data, self.instrument_id, self.from, self.to
        )
    }
}

impl FillRequest {
    /// This request cut into the windows of `period` it touches, aligned on
    /// multiples of `period` from the Unix epoch (a day: each UTC midnight),
    /// each ending 1 ns before the next starts, the first and last cut to
    /// the request.
    fn windows(self, period: SignedDuration) -> Vec<Self> {
        let period_ns = period.as_nanos();
        let mut windows = Vec::new();
        let mut from = self.from;
        while from <= self.to {
            let ns = from.as_nanosecond();
            let next = (ns.div_euclid(period_ns) + 1) * period_ns;
            let to = Timestamp::from_nanosecond(next - 1)
                .expect("a period end within jiff's range")
                .min(self.to);
            windows.push(Self { from, to, ..self });
            from = to + SignedDuration::from_nanos(1);
        }
        windows
    }
}

/// What `catalog` lacks of `slices`, contract by contract in the order they
/// first come: the contract's definition over the span of what it lacks,
/// then each of its series' missing windows (Nautilus'
/// `get_missing_intervals_for_request`), in time order. The slices are
/// `DataRequirements::slices`, which reads no window of a series twice. A
/// contract lacking nothing is left out.
///
/// # Errors
///
/// Returns an error if a series is not the quotes, trades, book depths or
/// book deltas of its slice's contract, or if the catalog cannot be read.
pub fn missing(
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
) -> anyhow::Result<Vec<FillRequest>> {
    let mut contracts: Vec<(InstrumentId, Vec<FillRequest>)> = Vec::new();
    for SeriesSlice {
        series: (data_type, identifier),
        slice,
    } in slices
    {
        let id = slice.instrument_id;
        let data =
            FillData::of(data_type).with_context(|| format!("the {data_type} of {identifier}"))?;
        ensure!(
            *identifier == id.to_string(),
            "the {data} of {identifier} on a slice of {id}"
        );
        let gaps = catalog
            .get_missing_intervals_for_request(
                nanos(slice.from),
                nanos(slice.to),
                &data_type.clone().into(),
                Some(identifier),
            )
            .with_context(|| format!("reading the stored {data} of {id}"))?;
        let requests = gaps.into_iter().map(|(from, to)| FillRequest {
            data,
            instrument_id: id,
            from: at(from),
            to: at(to),
        });
        match contracts.iter_mut().find(|(contract, _)| *contract == id) {
            Some((_, of_contract)) => of_contract.extend(requests),
            None => contracts.push((id, requests.collect())),
        }
    }
    Ok(contracts
        .into_iter()
        .filter(|(_, requests)| !requests.is_empty())
        .flat_map(|(id, requests)| {
            let from = requests.iter().map(|r| r.from).min().expect("a request");
            let to = requests.iter().map(|r| r.to).max().expect("a request");
            let definition = FillRequest {
                data: FillData::Instrument,
                instrument_id: id,
                from,
                to,
            };
            std::iter::once(definition).chain(requests)
        })
        .collect())
}

fn nanos(at: Timestamp) -> u64 {
    UnixNanos::from(at).as_u64()
}

fn at(ns: u64) -> Timestamp {
    Timestamp::from(UnixNanos::from(ns))
}

/// A source's data client for the node: its factory and config, and the
/// venues Nautilus routes its requests to it by (none for a client naming
/// its venue, as IC's).
pub struct NodeClient {
    pub factory: Box<dyn DataClientFactory>,
    pub config: Box<dyn ClientConfig>,
    pub routing: RoutingConfig,
}

/// The data clients of a fill through the node, and the requests asked of
/// them, in order.
#[derive(Default)]
pub struct NodeFill {
    clients: Vec<NodeClient>,
    requests: Vec<FillRequest>,
}

impl NodeFill {
    /// A fill asking nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `requests` (see `missing`), asked of `client`, each series cut
    /// into the windows of `period(data)` (see `FillRequest::windows`): the
    /// most of it one request holds. A definition is asked whole. No
    /// request leaves the client out of the node, so it never connects.
    pub fn add(
        &mut self,
        client: NodeClient,
        requests: &[FillRequest],
        period: impl Fn(FillData) -> SignedDuration,
    ) {
        if requests.is_empty() {
            return;
        }
        self.clients.push(client);
        for &request in requests {
            match request.data {
                FillData::Instrument => self.requests.push(request),
                data => self.requests.extend(request.windows(period(data))),
            }
        }
    }

    /// Fills the catalog in `catalog_dir` through a live node with the
    /// clients and the catalog, one request at a time, each with
    /// `update_catalog`, so the data engine writes each answer back over
    /// its window. Each series request carries the price precision of its
    /// instrument's definition, asked first (`price_precision`: Databento's
    /// client then asks for no definitions of its own). Nothing asked
    /// builds no node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to build or run; naming the
    /// request, if one fails, its client answers with a failure, or no
    /// answer comes within `timeout`; if the node stops before every
    /// request is answered; or if a window answered with data is not
    /// covered in the catalog after the run (see `check_coverage`). What was
    /// written before stays.
    pub async fn run(self, catalog_dir: &Path, timeout: SignedDuration) -> anyhow::Result<()> {
        let Self { clients, requests } = self;
        if requests.is_empty() {
            log::info!("The catalog holds every window asked for: no node");
            return Ok(());
        }
        log::info!(
            "Filling the catalog through the node: {} requests",
            requests.len()
        );
        let config = LiveNodeConfig {
            environment: Environment::Live,
            trader_id: TraderId::from("FILL-001"),
            load_state: false,
            save_state: false,
            catalogs: vec![DataCatalogConfig::new(
                catalog_path(catalog_dir)?,
                None,
                None,
            )],
            ..Default::default()
        };
        let mut builder = LiveNodeBuilder::from_config(config)?
            .with_name("FILL")
            .with_delay_post_stop_secs(0);
        for client in clients {
            builder = builder.add_data_client_with_routing(
                None,
                client.factory,
                client.config,
                client.routing,
            )?;
        }
        let mut node = builder.build()?;
        let outcome = Rc::new(RefCell::new(Outcome::default()));
        let total = requests.len();
        node.add_actor(FillActor {
            core: DataActorCore::new(DataActorConfig::default()),
            requests: requests.into(),
            pending: None,
            precisions: HashMap::new(),
            timeout,
            outcome: Rc::clone(&outcome),
        })?;
        let ran = node.run().await;
        node.dispose();
        ran?;
        let Outcome {
            answered,
            failure,
            done,
        } = outcome.take();
        if let Some(failure) = failure {
            bail!(failure);
        }
        ensure!(
            done,
            "the node stopped with {} of {total} requests answered",
            answered.len()
        );
        check_coverage(
            &ParquetDataCatalog::new(catalog_dir, None, None, None, None),
            &answered,
        )
    }
}

/// Checks that each request of `answered` that came back with data is
/// covered in `catalog`: Nautilus only logs a failed write-back
/// (`update_catalog_from_response`), so the requester sees a success.
/// Temporary: delete once nautilus_trader#5474 (a write-back failure
/// reported to the requester) is fixed.
///
/// # Errors
///
/// Returns an error naming the first such request the catalog does not
/// cover, and what it lacks.
fn check_coverage(
    catalog: &ParquetDataCatalog,
    answered: &[(FillRequest, usize)],
) -> anyhow::Result<()> {
    for &(request, rows) in answered {
        let Some(data_type) = request.data.data_type() else {
            continue;
        };
        if rows == 0 {
            continue;
        }
        let missing = catalog
            .get_missing_intervals_for_request(
                nanos(request.from),
                nanos(request.to),
                &data_type.into(),
                Some(&request.instrument_id.to_string()),
            )
            .with_context(|| format!("reading the coverage of {request}"))?;
        ensure!(
            missing.is_empty(),
            "{request} came back with {rows} rows, but the catalog lacks {:?} of it: \
             Nautilus' write-back failed (see its log)",
            missing
                .iter()
                .map(|&(from, to)| (at(from), at(to)))
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

/// How the actor's requests ended: those answered, with their rows, the
/// first failure, and whether every request was answered.
#[derive(Debug, Default)]
struct Outcome {
    answered: Vec<(FillRequest, usize)>,
    failure: Option<String>,
    done: bool,
}

/// Asks for `requests` one at a time, the next once the one before is
/// answered, and stops the node after the last answer or the first
/// failure.
#[derive(Debug)]
struct FillActor {
    core: DataActorCore,
    requests: VecDeque<FillRequest>,
    /// The request in flight and its id.
    pending: Option<(UUID4, FillRequest)>,
    /// The price precision of each instrument whose definition came back.
    precisions: HashMap<InstrumentId, u8>,
    timeout: SignedDuration,
    outcome: Rc<RefCell<Outcome>>,
}

impl FillActor {
    /// Asks for the next request, with its deadline, or, all answered,
    /// stops the node.
    fn next(&mut self) {
        let Some(request) = self.requests.pop_front() else {
            self.outcome.borrow_mut().done = true;
            self.shutdown_system(Some("every fill request answered".to_string()));
            return;
        };
        log::info!("Asking for {request}");
        let deadline = self.clock().utc_now() + self.timeout;
        let issued = self.issue(request).and_then(|id| {
            self.clock()
                .set_time_alert(DEADLINE, deadline, None, None)?;
            Ok(id)
        });
        match issued {
            Ok(id) => self.pending = Some((id, request)),
            Err(e) => self.fail(format!("asking for {request}: {e:#}")),
        }
    }

    /// Sends `request` to the data engine, with `update_catalog`.
    fn issue(&mut self, request: FillRequest) -> anyhow::Result<UUID4> {
        let FillRequest {
            data,
            instrument_id,
            from,
            to,
        } = request;
        let mut params = vec![("update_catalog".to_string(), json!(true))];
        if data != FillData::Instrument
            && let Some(precision) = self.precisions.get(&instrument_id)
        {
            params.push(("price_precision".to_string(), json!(precision)));
        }
        let params = Some(Params::new(params.into_iter().collect()));
        let (from, to) = (Some(from), Some(to));
        match data {
            FillData::Instrument => self.request_instrument(instrument_id, from, to, None, params),
            FillData::Quotes => self.request_quotes(instrument_id, from, to, None, None, params),
            FillData::Trades => self.request_trades(instrument_id, from, to, None, None, params),
            FillData::Depth => self.request_book_depth(
                instrument_id,
                from,
                to,
                None,
                std::num::NonZeroUsize::new(DEPTH),
                None,
                params,
            ),
            FillData::Deltas => {
                self.request_book_deltas(instrument_id, from, to, None, None, params)
            }
        }
    }

    /// Records `reason` as the fill's failure and stops the node.
    fn fail(&mut self, reason: String) {
        log::error!("{reason}");
        self.pending = None;
        self.outcome.borrow_mut().failure = Some(reason.clone());
        self.shutdown_system(Some(reason));
    }
}

impl DataActor for FillActor {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.next();
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        let Some((id, request)) = self.pending else {
            return Ok(());
        };
        if *response.correlation_id() != id {
            return Ok(());
        }
        self.pending = None;
        self.clock().cancel_timer(DEADLINE);
        if let Some(reason) = response.failure_reason() {
            self.fail(format!("{request} failed: {reason}"));
            return Ok(());
        }
        let rows = match response.response() {
            Some(DataResponse::Instrument(answer)) => {
                self.precisions
                    .insert(answer.instrument_id, answer.data.price_precision());
                1
            }
            Some(DataResponse::Quotes(answer)) => answer.data.len(),
            Some(DataResponse::Trades(answer)) => answer.data.len(),
            Some(DataResponse::BookDepth(answer)) => answer.data.len(),
            Some(DataResponse::BookDeltas(answer)) => answer.data.len(),
            _ => 0,
        };
        log::info!("{request}: {rows} rows");
        self.outcome.borrow_mut().answered.push((request, rows));
        self.next();
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if event.name.as_str() == DEADLINE
            && let Some((_, request)) = self.pending
        {
            self.fail(format!("{request}: no answer within {:#}", self.timeout));
        }
        Ok(())
    }
}
nautilus_actor!(FillActor);

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::TradeTick,
        enums::AggressorSide,
        identifiers::TradeId,
        types::{Price, Quantity},
    };

    use super::*;
    use crate::rolls::Slice;

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn request(data: FillData, id: &str, from: &str, to: &str) -> FillRequest {
        FillRequest {
            data,
            instrument_id: InstrumentId::from(id),
            from: ts(from),
            to: ts(to),
        }
    }

    fn slice(data_type: NautilusDataType, id: &str, from: &str, to: &str) -> SeriesSlice {
        SeriesSlice {
            series: (data_type, id.to_string()),
            slice: Slice {
                instrument_id: InstrumentId::from(id),
                from: ts(from),
                to: ts(to),
            },
        }
    }

    fn windows(of: FillRequest, period: SignedDuration) -> Vec<(String, String)> {
        of.windows(period)
            .iter()
            .map(|w| (w.from.to_string(), w.to.to_string()))
            .collect()
    }

    #[test]
    fn a_request_is_cut_on_the_period_s_multiples_from_the_epoch() {
        let three_hours = SignedDuration::from_hours(3);
        let asked = request(
            FillData::Depth,
            "GCZ6.GLBX",
            "2026-10-21T02:00:00Z",
            "2026-10-21T08:30:00Z",
        );

        assert_eq!(
            windows(asked, three_hours),
            [
                ("2026-10-21T02:00:00Z", "2026-10-21T02:59:59.999999999Z"),
                ("2026-10-21T03:00:00Z", "2026-10-21T05:59:59.999999999Z"),
                ("2026-10-21T06:00:00Z", "2026-10-21T08:30:00Z"),
            ]
            .map(|(a, b)| (a.to_string(), b.to_string()))
        );
    }

    #[test]
    fn a_request_within_one_period_is_one_window_and_a_day_ends_at_midnight() {
        let day = SignedDuration::from_hours(24);
        let within = request(
            FillData::Trades,
            "GCZ6.GLBX",
            "2026-10-21T00:00:00Z",
            "2026-10-21T23:59:59.999999999Z",
        );
        let across = request(
            FillData::Trades,
            "GCZ6.GLBX",
            "2026-10-21T12:00:00Z",
            "2026-10-22T00:00:00Z",
        );

        assert_eq!(windows(within, day).len(), 1);
        assert_eq!(
            windows(across, day),
            [
                ("2026-10-21T12:00:00Z", "2026-10-21T23:59:59.999999999Z"),
                ("2026-10-22T00:00:00Z", "2026-10-22T00:00:00Z"),
            ]
            .map(|(a, b)| (a.to_string(), b.to_string()))
        );
    }

    #[test]
    fn an_empty_catalog_lacks_each_series_over_its_slice_after_the_definition() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let slices = [
            slice(
                NautilusDataType::OrderBookDepth,
                "GCQ6.GLBX",
                "2026-07-01T00:00:00Z",
                "2026-07-30T23:59:59.999999999Z",
            ),
            slice(
                NautilusDataType::OrderBookDepth,
                "GCZ6.GLBX",
                "2026-07-31T00:00:00Z",
                "2026-08-10T00:00:00Z",
            ),
            slice(
                NautilusDataType::TradeTick,
                "GCZ6.GLBX",
                "2026-08-01T00:00:00Z",
                "2026-08-10T00:00:00Z",
            ),
        ];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked,
            [
                request(
                    FillData::Instrument,
                    "GCQ6.GLBX",
                    "2026-07-01T00:00:00Z",
                    "2026-07-30T23:59:59.999999999Z"
                ),
                request(
                    FillData::Depth,
                    "GCQ6.GLBX",
                    "2026-07-01T00:00:00Z",
                    "2026-07-30T23:59:59.999999999Z"
                ),
                request(
                    FillData::Instrument,
                    "GCZ6.GLBX",
                    "2026-07-31T00:00:00Z",
                    "2026-08-10T00:00:00Z"
                ),
                request(
                    FillData::Depth,
                    "GCZ6.GLBX",
                    "2026-07-31T00:00:00Z",
                    "2026-08-10T00:00:00Z"
                ),
                request(
                    FillData::Trades,
                    "GCZ6.GLBX",
                    "2026-08-01T00:00:00Z",
                    "2026-08-10T00:00:00Z"
                ),
            ]
        );
    }

    fn trade(id: &str, at: &str) -> TradeTick {
        TradeTick::new(
            InstrumentId::from(id),
            Price::from("4000.0"),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::new("1"),
            ts(at).into(),
            ts(at).into(),
        )
    }

    #[test]
    fn a_stored_series_lacks_only_what_is_outside_its_stored_intervals() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        // Stored over Wednesday, a weekend inside it covered as well.
        catalog
            .write_to_parquet(
                &[trade("GCZ6.GLBX", "2026-10-21T10:00:00Z")],
                Some(ts("2026-10-21T00:00:00Z").into()),
                Some(ts("2026-10-21T23:59:59.999999999Z").into()),
                None,
            )
            .unwrap();
        let slices = [slice(
            NautilusDataType::TradeTick,
            "GCZ6.GLBX",
            "2026-10-20T00:00:00Z",
            "2026-10-23T00:00:00Z",
        )];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked[1..],
            [
                request(
                    FillData::Trades,
                    "GCZ6.GLBX",
                    "2026-10-20T00:00:00Z",
                    "2026-10-20T23:59:59.999999999Z"
                ),
                request(
                    FillData::Trades,
                    "GCZ6.GLBX",
                    "2026-10-22T00:00:00Z",
                    "2026-10-23T00:00:00Z"
                ),
            ]
        );
        assert_eq!(asked[0].data, FillData::Instrument);
        assert_eq!(
            (asked[0].from, asked[0].to),
            (ts("2026-10-20T00:00:00Z"), ts("2026-10-23T00:00:00Z"))
        );
    }

    #[test]
    fn a_covered_window_asks_for_nothing_not_even_the_definition() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        catalog
            .write_to_parquet(
                &[trade("GCZ6.GLBX", "2026-10-21T10:00:00Z")],
                Some(ts("2026-10-21T00:00:00Z").into()),
                Some(ts("2026-10-21T23:59:59.999999999Z").into()),
                None,
            )
            .unwrap();
        let slices = [slice(
            NautilusDataType::TradeTick,
            "GCZ6.GLBX",
            "2026-10-21T06:00:00Z",
            "2026-10-21T18:00:00Z",
        )];

        assert_eq!(missing(&catalog, &slices).unwrap(), []);
    }

    #[test]
    fn bars_are_refused_naming_the_series() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bars = "GCZ6.GLBX-1-MINUTE-LAST-EXTERNAL";
        let slices = [SeriesSlice {
            series: (NautilusDataType::Bar, bars.to_string()),
            slice: Slice {
                instrument_id: InstrumentId::from("GCZ6.GLBX"),
                from: ts("2026-10-20T00:00:00Z"),
                to: ts("2026-10-21T00:00:00Z"),
            },
        }];

        let err = missing(&catalog, &slices).unwrap_err();

        assert!(format!("{err:#}").contains(bars), "{err:#}");
    }

    #[test]
    fn book_deltas_are_asked_as_deltas_over_their_slice() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let slices = [slice(
            NautilusDataType::OrderBookDelta,
            "GCZ6.GLBX",
            "2026-10-21T00:00:00Z",
            "2026-10-22T00:00:00Z",
        )];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked[1],
            request(
                FillData::Deltas,
                "GCZ6.GLBX",
                "2026-10-21T00:00:00Z",
                "2026-10-22T00:00:00Z"
            )
        );
    }

    #[test]
    fn custom_data_is_refused_naming_its_type_and_instrument() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let slices = [slice(
            NautilusDataType::Custom {
                type_name: "Imbalance".to_string(),
            },
            "GCZ6.GLBX",
            "2026-10-21T00:00:00Z",
            "2026-10-22T00:00:00Z",
        )];

        let err = format!("{:#}", missing(&catalog, &slices).unwrap_err());

        assert!(err.contains("Imbalance"), "{err}");
        assert!(err.contains("GCZ6.GLBX"), "{err}");
    }

    #[test]
    fn a_window_answered_with_rows_but_not_stored_fails_the_coverage_check() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let depth = request(
            FillData::Depth,
            "GCZ6.GLBX",
            "2026-10-21T00:00:00Z",
            "2026-10-21T02:59:59.999999999Z",
        );
        let empty = request(
            FillData::Depth,
            "GCZ6.GLBX",
            "2026-10-24T00:00:00Z",
            "2026-10-24T02:59:59.999999999Z",
        );
        let definition = request(
            FillData::Instrument,
            "GCZ6.GLBX",
            "2026-10-21T00:00:00Z",
            "2026-10-24T00:00:00Z",
        );

        check_coverage(&catalog, &[(definition, 1), (empty, 0)]).unwrap();
        let err = check_coverage(&catalog, &[(depth, 5)]).unwrap_err();

        assert!(err.to_string().contains(&depth.to_string()), "{err}");
    }
}
