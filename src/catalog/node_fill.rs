//! Fills the catalog through a Nautilus node: the sources whose data client answers a request
//! with a completed `DataResponseEnvelope`, which the data engine writes back (`update_catalog`).
//!
//! `missing` turns series slices into what the catalog lacks of them, by Nautilus' own count
//! (`get_missing_intervals_for_request`): per contract its definition, then each series' missing
//! windows, the slivers between a live node's promoted files among them. `missing_custom` does the
//! same for custom series, with no definition.
//!
//! `run` builds a live node with each source's data client and an actor asking one request at a
//! time, naming the client it was added with, which the data engine sends it to before any venue
//! route (`DataEngine::get_client`). Each series goes by Nautilus' time-range pipeline
//! (`time_range_generator`), a window of the period at a time; a custom data request is asked
//! whole, as the pipeline windows none.
//!
//! The actor waits for each answer (`on_data_response`) with no deadline, as Nautilus' requests
//! have none: each client answers every request, a failed one in its response envelope. It records
//! the first failure and stops the node; `run` returns it. See `README.md`, "Fill paths".

use std::{cell::RefCell, collections::VecDeque, fmt::Display, path::Path, rc::Rc};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    clock::Clock,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory},
    messages::data::{DataResponse, DataResponseEnvelope},
    nautilus_actor,
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_model::{
    data::{BarType, CustomData, DataType, NautilusDataType},
    identifiers::{ClientId, InstrumentId, TraderId},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use serde_json::json;

use super::catalog_config;
use crate::{
    strategy::requirements::{CatalogSeries, CustomSlice, SeriesSlice},
    time_range, venues,
};

/// What a fill asks a source for: an instrument's definition, or one of its series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillData {
    /// The instrument's definition (`request_instrument`).
    Instrument(InstrumentId),
    /// `QuoteTick`s (`request_quotes`).
    Quotes(InstrumentId),
    /// `TradeTick`s (`request_trades`).
    Trades(InstrumentId),
    /// `OrderBookDepth` snapshots of its venue's book levels (`venues::book_format`).
    Depth(InstrumentId),
    /// `OrderBookDelta`s (`request_book_deltas`).
    Deltas(InstrumentId),
    /// The `Bar`s of a bar type (`request_bars`).
    Bars(BarType),
    /// `FundingRateUpdate`s (`request_funding_rates`).
    Funding(InstrumentId),
}

impl FillData {
    /// The contract the request targets.
    #[must_use]
    pub fn instrument_id(self) -> InstrumentId {
        match self {
            Self::Instrument(id)
            | Self::Quotes(id)
            | Self::Trades(id)
            | Self::Depth(id)
            | Self::Deltas(id)
            | Self::Funding(id) => id,
            Self::Bars(bar_type) => bar_type.instrument_id(),
        }
    }

    fn of(series: &CatalogSeries) -> anyhow::Result<Self> {
        let tick = match series {
            CatalogSeries::Bars(bar_type) => return Ok(Self::Bars(*bar_type)),
            CatalogSeries::Ticks(tick) => tick,
        };
        let id = tick.instrument_id;
        Ok(match &tick.data {
            NautilusDataType::QuoteTick => Self::Quotes(id),
            NautilusDataType::TradeTick => Self::Trades(id),
            NautilusDataType::OrderBookDepth => Self::Depth(id),
            NautilusDataType::OrderBookDelta => Self::Deltas(id),
            NautilusDataType::FundingRateUpdate => Self::Funding(id),
            other => anyhow::bail!(
                "no source filled through the node serves the {other} of an instrument"
            ),
        })
    }
}

impl Display for FillData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Instrument(_) => f.write_str("definition"),
            Self::Quotes(_) => f.write_str("quotes"),
            Self::Trades(_) => f.write_str("trades"),
            Self::Depth(_) => f.write_str("book depths"),
            Self::Deltas(_) => f.write_str("book deltas"),
            Self::Bars(bar_type) => write!(
                f,
                "{}-{} bars",
                bar_type.spec(),
                bar_type.aggregation_source()
            ),
            Self::Funding(_) => f.write_str("funding rates"),
        }
    }
}

/// A request of a fill: `data` over the closed window `[from, to]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillRequest {
    pub data: FillData,
    pub from: Timestamp,
    pub to: Timestamp,
}

impl Display for FillRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the {} of {} over [{}, {}]",
            self.data,
            self.data.instrument_id(),
            self.from,
            self.to
        )
    }
}

/// What `catalog` lacks of `slices` (`DataRequirements::slices`), contract by contract in the order
/// they first come: the definition over the span of what it lacks, then each series' missing
/// windows, in time order. A contract lacking nothing is left out.
///
/// # Errors
///
/// Returns an error if a series is not the quotes, trades, book depths, book deltas, bars or
/// funding of its slice's contract, or if the catalog cannot be read.
pub fn missing(
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
) -> anyhow::Result<Vec<FillRequest>> {
    let mut contracts: Vec<(InstrumentId, Vec<FillRequest>)> = Vec::new();

    for slice in slices {
        let id = slice.series.instrument_id();
        let (data_type, identifier) = &slice.series.key();
        let data = FillData::of(&slice.series)
            .with_context(|| format!("the {data_type} of {identifier}"))?;
        let (from, to) = (nanos(slice.from), nanos(slice.to));
        let gaps = catalog
            .get_missing_intervals_for_request(
                from,
                to,
                &data_type.clone().into(),
                Some(identifier),
            )
            .with_context(|| format!("reading the stored {data} of {id}"))?;
        let requests = gaps.into_iter().map(|(from, to)| FillRequest {
            data,
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
                data: FillData::Instrument(id),
                from,
                to,
            };
            std::iter::once(definition).chain(requests)
        })
        .collect())
}

/// A request of a fill of a custom series: its `data_type` over the closed window `[from, to]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomFillRequest {
    pub data_type: DataType,
    pub from: Timestamp,
    pub to: Timestamp,
}

impl Display for CustomFillRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the {} over [{}, {}]",
            self.data_type, self.from, self.to
        )
    }
}

/// What `catalog` lacks of each of `slices` (`DataRequirements::custom_slices`), in time order, the
/// slices in the order given. No definition: a custom series brings no instrument.
///
/// # Errors
///
/// Returns an error if the catalog cannot be read.
pub fn missing_custom(
    catalog: &ParquetDataCatalog,
    slices: &[CustomSlice],
) -> anyhow::Result<Vec<CustomFillRequest>> {
    let mut requests = Vec::new();
    for CustomSlice { series, from, to } in slices {
        let stored = NautilusDataType::Custom {
            type_name: series.type_name().to_string(),
        };
        let gaps = catalog
            .get_missing_intervals_for_request(
                nanos(*from),
                nanos(*to),
                &stored.into(),
                Some(series.identifier()),
            )
            .with_context(|| format!("reading the stored {series}"))?;
        requests.extend(gaps.into_iter().map(|(from, to)| CustomFillRequest {
            data_type: series.data_type().clone(),
            from: at(from),
            to: at(to),
        }));
    }
    Ok(requests)
}

fn nanos(at: Timestamp) -> u64 {
    UnixNanos::from(at).as_u64()
}

fn at(ns: u64) -> Timestamp {
    Timestamp::from(UnixNanos::from(ns))
}

/// A source's data client for the node. It takes no venue route, as each request names its client,
/// so two sources of one venue share a node.
pub struct NodeClient {
    pub factory: Box<dyn DataClientFactory>,
    pub config: Box<dyn ClientConfig>,
}

/// The data clients of a fill and the requests asked of them, in order.
#[derive(Default)]
pub struct NodeFill {
    clients: Vec<NodeClient>,
    requests: Vec<Asked>,
}

/// A request of the fill, its client, and the period of its windows, none for a definition or
/// custom data.
#[derive(Debug, Clone)]
struct Asked {
    request: Request,
    client: ClientId,
    period: Option<SignedDuration>,
}

#[derive(Debug, Clone)]
enum Request {
    Series(FillRequest),
    Custom(CustomFillRequest),
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Series(request) => Display::fmt(request, f),
            Self::Custom(request) => Display::fmt(request, f),
        }
    }
}

impl NodeFill {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `requests` (see `missing`), asked of `client`, each series in windows of
    /// `period(data)`, the most one answer holds. A definition is asked whole. With no request the
    /// client stays out of the node and never connects.
    pub fn add(
        &mut self,
        client: NodeClient,
        requests: &[FillRequest],
        period: impl Fn(FillData) -> SignedDuration,
    ) {
        self.add_with_custom(client, requests, &[], period);
    }

    /// Adds `requests` as `add` does, then `custom`, each whole: Nautilus' time-range pipeline
    /// windows no custom data request (`data/src/engine/time_range.rs`,
    /// `is_time_range_pipeline_variant`).
    pub fn add_with_custom(
        &mut self,
        client: NodeClient,
        requests: &[FillRequest],
        custom: &[CustomFillRequest],
        period: impl Fn(FillData) -> SignedDuration,
    ) {
        if requests.is_empty() && custom.is_empty() {
            return;
        }
        let id = ClientId::new(client.factory.name());
        self.clients.push(client);
        self.requests.extend(requests.iter().map(|&request| Asked {
            request: Request::Series(request),
            client: id,
            period: match request.data {
                FillData::Instrument(_) => None,
                data => Some(period(data)),
            },
        }));
        self.requests.extend(custom.iter().map(|request| Asked {
            request: Request::Custom(request.clone()),
            client: id,
            period: None,
        }));
    }

    #[cfg(test)]
    pub(crate) fn asked(&self) -> Vec<(FillRequest, Option<SignedDuration>)> {
        self.requests
            .iter()
            .filter_map(|asked| match asked.request {
                Request::Series(request) => Some((request, asked.period)),
                Request::Custom(_) => None,
            })
            .collect()
    }

    /// Fills the catalog in `catalog_dir` through a live node, one request at a time with
    /// `update_catalog`, so what was written stays if a later window fails.
    ///
    /// The node runs on the clocks `clock` makes (`LiveNodeBuilder::with_clock_factory`): a fill of
    /// history passes Nautilus' `LiveClock`. Nothing asked builds no node; a node built is disposed
    /// whatever comes of it (`concepts/architecture.md`, "Processes and threads").
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The node fails to build or run.
    /// - A request fails, its client answers with a failure or its answer is not written back, the
    ///   error naming the request.
    /// - The node stops before every request is answered.
    ///
    /// What was written before stays.
    pub async fn run(
        self,
        catalog_dir: &Path,
        clock: impl Fn() -> Rc<RefCell<dyn Clock>> + 'static,
    ) -> anyhow::Result<()> {
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
            catalogs: vec![catalog_config(catalog_dir)?],
            ..Default::default()
        };
        let mut builder = LiveNodeBuilder::from_config(config)?
            .with_name("FILL")
            .with_delay_post_stop_secs(0)
            .with_clock_factory(clock);

        for client in clients {
            builder = builder.add_data_client(None, client.factory, client.config)?;
        }
        let mut node = builder.build()?;
        let outcome = Rc::new(RefCell::new(Outcome::default()));
        let total = requests.len();
        let ran = match node.add_actor(FillActor {
            core: DataActorCore::new(DataActorConfig::default()),
            requests: requests.into(),
            pending: None,
            rows: 0,
            outcome: Rc::clone(&outcome),
        }) {
            Ok(()) => node.run().await,
            Err(e) => Err(e),
        };
        node.dispose();
        ran?;
        let Outcome {
            answered,
            failure,
            done,
        } = outcome.take();

        if let Some(failure) = failure {
            anyhow::bail!(failure);
        }
        anyhow::ensure!(
            done,
            "the node stopped with {answered} of {total} requests answered"
        );
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Outcome {
    answered: usize,
    failure: Option<String>,
    done: bool,
}

#[derive(Debug)]
struct FillActor {
    core: DataActorCore,
    requests: VecDeque<Asked>,
    pending: Option<(UUID4, Request)>,
    rows: usize,
    outcome: Rc<RefCell<Outcome>>,
}

impl FillActor {
    fn next(&mut self) {
        let Some(asked) = self.requests.pop_front() else {
            self.outcome.borrow_mut().done = true;
            self.shutdown_system(Some("every fill request answered".to_string()));
            return;
        };
        let request = asked.request.clone();
        log::info!("Asking {} for {request}", asked.client);
        self.rows = 0;
        match self.issue(asked) {
            Ok(id) => self.pending = Some((id, request)),
            Err(e) => self.fail(format!("asking for {request}: {e:#}")),
        }
    }

    /// Sends `asked` to the data engine with `update_catalog`. Depth is asked at the levels of its
    /// venue's book (`venues::book_format`), the one format of the catalog's series of it.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus refuses the request, or, naming the instrument, if it asks the
    /// depth of a venue whose book has none.
    fn issue(&mut self, asked: Asked) -> anyhow::Result<UUID4> {
        let Asked {
            request,
            client,
            period,
        } = asked;
        let (from, to) = match &request {
            Request::Series(request) => (request.from, request.to),
            Request::Custom(request) => (request.from, request.to),
        };
        let mut params = vec![("update_catalog".to_string(), json!(true))];
        if let Some(period) = period {
            params.extend(time_range::params(from, to, period));
        }
        let params = Some(Params::new(params.into_iter().collect()));
        let data = match request {
            Request::Series(request) => request.data,
            Request::Custom(request) => {
                return self.request_data(
                    request.data_type,
                    client,
                    Some(from),
                    Some(to),
                    None,
                    params,
                );
            }
        };
        let (from, to, client) = (Some(from), Some(to), Some(client));

        match data {
            FillData::Instrument(instrument_id) => {
                self.request_instrument(instrument_id, from, to, client, params)
            }
            FillData::Quotes(instrument_id) => {
                self.request_quotes(instrument_id, from, to, None, client, params)
            }
            FillData::Trades(instrument_id) => {
                self.request_trades(instrument_id, from, to, None, client, params)
            }
            FillData::Depth(instrument_id) => {
                let levels = venues::book_format(instrument_id.venue)
                    .and_then(|book| book.depth)
                    .with_context(|| {
                        format!(
                            "the book of {} has no depth: none of {instrument_id} is asked",
                            instrument_id.venue
                        )
                    })?;
                self.request_book_depth(instrument_id, from, to, None, Some(levels), client, params)
            }
            FillData::Deltas(instrument_id) => {
                self.request_book_deltas(instrument_id, from, to, None, client, params)
            }
            FillData::Bars(bar_type) => self.request_bars(bar_type, from, to, None, client, params),
            FillData::Funding(instrument_id) => {
                self.request_funding_rates(instrument_id, from, to, None, client, params)
            }
        }
    }

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
        let Some((id, request)) = &self.pending else {
            return Ok(());
        };
        if response.correlation_id() != id {
            return Ok(());
        }
        let request = request.to_string();
        if let Some(reason) = response.failure_reason() {
            self.fail(format!("{request} failed: {reason}"));
            return Ok(());
        }
        self.rows += match response.response() {
            Some(DataResponse::Instrument(_)) => 1,
            Some(DataResponse::Quotes(answer)) => answer.data.len(),
            Some(DataResponse::Trades(answer)) => answer.data.len(),
            Some(DataResponse::BookDepth(answer)) => answer.data.len(),
            Some(DataResponse::BookDeltas(answer)) => answer.data.len(),
            Some(DataResponse::Bars(answer)) => answer.data.len(),
            Some(DataResponse::FundingRates(answer)) => answer.data.len(),
            Some(DataResponse::Data(answer)) => answer
                .data
                .downcast_ref::<Vec<CustomData>>()
                .map_or(0, Vec::len),
            _ => 0,
        };
        // A window of the request, written back; the next is on its way
        if !response.completed() {
            return Ok(());
        }
        self.pending = None;
        log::info!("{request}: {} rows", self.rows);
        self.outcome.borrow_mut().answered += 1;
        self.next();
        Ok(())
    }
}
nautilus_actor!(FillActor);

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::{Bar, BarType, DataType, TradeTick},
        enums::AggressorSide,
        identifiers::TradeId,
        types::{Price, Quantity},
    };
    use nautilus_persistence::backend::parquet::paths::timestamps_to_filename;
    use rstest::rstest;

    use super::*;
    use crate::strategy::requirements::{CustomSeries, TickSeries};

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn request(data: FillData, from: &str, to: &str) -> FillRequest {
        FillRequest {
            data,
            from: ts(from),
            to: ts(to),
        }
    }

    fn slice(data_type: NautilusDataType, id: &str, from: &str, to: &str) -> SeriesSlice {
        SeriesSlice {
            series: CatalogSeries::Ticks(TickSeries {
                data: data_type,
                instrument_id: InstrumentId::from(id),
            }),
            from: ts(from),
            to: ts(to),
        }
    }

    #[rstest]
    fn an_empty_catalog_lacks_each_series_over_its_slice_after_the_definition() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let slices = [
            slice(
                NautilusDataType::OrderBookDepth,
                "GCQ26.GLBX",
                "2026-07-01T00:00:00Z",
                "2026-07-30T23:59:59.999999999Z",
            ),
            slice(
                NautilusDataType::OrderBookDepth,
                "GCZ26.GLBX",
                "2026-07-31T00:00:00Z",
                "2026-08-10T00:00:00Z",
            ),
            slice(
                NautilusDataType::TradeTick,
                "GCZ26.GLBX",
                "2026-08-01T00:00:00Z",
                "2026-08-10T00:00:00Z",
            ),
        ];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked,
            [
                request(
                    FillData::Instrument(InstrumentId::from("GCQ26.GLBX")),
                    "2026-07-01T00:00:00Z",
                    "2026-07-30T23:59:59.999999999Z"
                ),
                request(
                    FillData::Depth(InstrumentId::from("GCQ26.GLBX")),
                    "2026-07-01T00:00:00Z",
                    "2026-07-30T23:59:59.999999999Z"
                ),
                request(
                    FillData::Instrument(InstrumentId::from("GCZ26.GLBX")),
                    "2026-07-31T00:00:00Z",
                    "2026-08-10T00:00:00Z"
                ),
                request(
                    FillData::Depth(InstrumentId::from("GCZ26.GLBX")),
                    "2026-07-31T00:00:00Z",
                    "2026-08-10T00:00:00Z"
                ),
                request(
                    FillData::Trades(InstrumentId::from("GCZ26.GLBX")),
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

    fn store_trades_over_wednesday(catalog: &ParquetDataCatalog) {
        catalog
            .write_to_parquet(
                &[trade("GCZ26.GLBX", "2026-10-21T10:00:00Z")],
                Some(ts("2026-10-21T00:00:00Z").into()),
                Some(ts("2026-10-21T23:59:59.999999999Z").into()),
                None,
            )
            .unwrap();
    }

    fn store_bars_over_wednesday(catalog: &ParquetDataCatalog) {
        let price = Price::from("4000.0");
        let close = ts("2026-10-21T10:00:00Z");
        let bar = Bar::new(
            BarType::from(BARS),
            price,
            price,
            price,
            price,
            Quantity::from(1),
            (close - SignedDuration::from_mins(1)).into(),
            close.into(),
        );
        catalog
            .write_to_parquet(
                &[bar],
                Some(ts("2026-10-21T00:00:00Z").into()),
                Some(ts("2026-10-21T23:59:59.999999999Z").into()),
                None,
            )
            .unwrap();
    }

    /// A slice from Tuesday of a series stored over Wednesday lacks Tuesday and what follows.
    #[rstest]
    #[case::trades(
        store_trades_over_wednesday,
        slice(
            NautilusDataType::TradeTick,
            "GCZ26.GLBX",
            "2026-10-20T00:00:00Z",
            "2026-10-23T00:00:00Z"
        ),
        FillData::Trades(InstrumentId::from("GCZ26.GLBX")),
        "2026-10-23T00:00:00Z"
    )]
    #[case::bars(
        store_bars_over_wednesday,
        bars_slice(BARS, "2026-10-20T00:00:00Z", "2026-10-22T00:00:00Z"),
        FillData::Bars(BarType::from(BARS)),
        "2026-10-22T00:00:00Z"
    )]
    fn a_stored_series_lacks_only_what_is_outside_its_stored_intervals(
        #[case] store: fn(&ParquetDataCatalog),
        #[case] slice: SeriesSlice,
        #[case] data: FillData,
        #[case] to: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        store(&catalog);

        let asked = missing(&catalog, &[slice]).unwrap();

        assert_eq!(
            asked,
            [
                request(
                    FillData::Instrument(InstrumentId::from("GCZ26.GLBX")),
                    "2026-10-20T00:00:00Z",
                    to
                ),
                request(
                    data,
                    "2026-10-20T00:00:00Z",
                    "2026-10-20T23:59:59.999999999Z"
                ),
                request(data, "2026-10-22T00:00:00Z", to),
            ]
        );
    }

    #[rstest]
    fn a_covered_window_asks_for_nothing_not_even_the_definition() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        catalog
            .write_to_parquet(
                &[trade("GCZ26.GLBX", "2026-10-21T10:00:00Z")],
                Some(ts("2026-10-21T00:00:00Z").into()),
                Some(ts("2026-10-21T23:59:59.999999999Z").into()),
                None,
            )
            .unwrap();
        let slices = [slice(
            NautilusDataType::TradeTick,
            "GCZ26.GLBX",
            "2026-10-21T06:00:00Z",
            "2026-10-21T18:00:00Z",
        )];

        assert_eq!(missing(&catalog, &slices).unwrap(), []);
    }

    /// Empty quote files named as a live node's promotion names them; coverage reads their names.
    fn promoted_quotes(dir: &Path, id: &str, files: &[(&str, &str)]) {
        let quotes = dir.join("data/quotes").join(id.replace('/', ""));
        std::fs::create_dir_all(&quotes).unwrap();
        for &(first, last) in files {
            let name = timestamps_to_filename(ts(first).into(), ts(last).into())
                .replace(".parquet", "_0123456789abcdef.parquet");
            std::fs::write(quotes.join(name), b"").unwrap();
        }
    }

    /// The slivers between a live node's daily promoted files are asked with the definition over
    /// them. A run-start directory an older node left is ignored.
    #[rstest]
    fn a_sliver_between_promoted_files_is_asked_with_its_definition() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("live-runs/1792540800000000000")).unwrap();
        promoted_quotes(
            dir.path(),
            "XAUUSD.ICMARKETS",
            &[
                ("2026-10-21T00:00:00Z", "2026-10-21T23:59:59.5Z"),
                ("2026-10-21T23:59:59.7Z", "2026-10-22T23:59:59Z"),
            ],
        );
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let slices = [slice(
            NautilusDataType::QuoteTick,
            "XAUUSD.ICMARKETS",
            "2026-10-21T00:00:00Z",
            "2026-10-22T23:59:59Z",
        )];

        let asked = missing(&catalog, &slices).unwrap();

        let sliver = request(
            FillData::Quotes(InstrumentId::from("XAUUSD.ICMARKETS")),
            "2026-10-21T23:59:59.500000001Z",
            "2026-10-21T23:59:59.699999999Z",
        );
        assert_eq!(
            asked,
            [
                FillRequest {
                    data: FillData::Instrument(InstrumentId::from("XAUUSD.ICMARKETS")),
                    ..sliver
                },
                sliver
            ]
        );
    }

    const BARS: &str = "GCZ26.GLBX-1-MINUTE-LAST-EXTERNAL";

    fn bars_slice(bar_type: &str, from: &str, to: &str) -> SeriesSlice {
        SeriesSlice {
            series: CatalogSeries::Bars(BarType::from(bar_type)),
            from: ts(from),
            to: ts(to),
        }
    }

    /// An empty catalog lacks a series over its slice, asked after the definition.
    #[rstest]
    #[case::bars(
        bars_slice(BARS, "2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z"),
        FillData::Bars(BarType::from(BARS))
    )]
    #[case::funding(
        slice(
            NautilusDataType::FundingRateUpdate,
            "BTC-USDT-SWAP.OKX",
            "2026-10-20T00:00:00Z",
            "2026-10-21T00:00:00Z"
        ),
        FillData::Funding(InstrumentId::from("BTC-USDT-SWAP.OKX"))
    )]
    #[case::book_deltas(
        slice(
            NautilusDataType::OrderBookDelta,
            "GCZ26.GLBX",
            "2026-10-20T00:00:00Z",
            "2026-10-21T00:00:00Z"
        ),
        FillData::Deltas(InstrumentId::from("GCZ26.GLBX"))
    )]
    fn a_series_is_asked_as_its_kind_over_its_slice_after_the_definition(
        #[case] slice: SeriesSlice,
        #[case] data: FillData,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let id = data.instrument_id();
        let (from, to) = ("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z");

        let asked = missing(&catalog, &[slice]).unwrap();

        assert_eq!(
            asked,
            [
                request(FillData::Instrument(id), from, to),
                request(data, from, to),
            ]
        );
    }

    fn bond_yields_metadata() -> Params {
        Params::new([("source".to_string(), json!("bond_yields"))].into())
    }

    #[rstest]
    fn a_custom_series_is_lacking_over_its_window_alone() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, to) = (
            ts("2026-11-09T00:00:00Z"),
            ts("2026-11-12T23:59:59.999999999Z"),
        );
        let bond_yields =
            CustomSeries::new("LseRow", "US10Y".to_string(), Some(bond_yields_metadata())).unwrap();

        let asked = missing_custom(&catalog, &bond_yields.slices(from, to)).unwrap();

        assert_eq!(
            asked,
            [CustomFillRequest {
                data_type: DataType::new(
                    "LseRow",
                    Some(bond_yields_metadata()),
                    Some("US10Y".to_string())
                ),
                from,
                to,
            }]
        );
    }

    /// The bond yields under GC's id, which rolls from GCQ26 into GCZ26 at 00:00 UTC on 29 July
    /// 2026 (`rolls/README.md`): each contract's series lacks over its slice, the contract's id its
    /// identifier.
    #[rstest]
    fn a_roots_custom_series_is_lacking_contract_by_contract_over_its_slices() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, roll, to) = (
            ts("2026-07-28T00:00:00Z"),
            ts("2026-07-29T00:00:00Z"),
            ts("2026-07-30T00:00:00Z"),
        );
        let bond_yields = CustomSeries::new(
            "LseRow",
            "GC.GLBX".to_string(),
            Some(bond_yields_metadata()),
        )
        .unwrap();

        let asked = missing_custom(&catalog, &bond_yields.slices(from, to)).unwrap();

        let of = |contract: &str| {
            DataType::new(
                "LseRow",
                Some(bond_yields_metadata()),
                Some(contract.to_string()),
            )
        };
        assert_eq!(
            asked,
            [
                CustomFillRequest {
                    data_type: of("GCQ26.GLBX"),
                    from,
                    to: roll - SignedDuration::from_nanos(1),
                },
                CustomFillRequest {
                    data_type: of("GCZ26.GLBX"),
                    from: roll,
                    to,
                },
            ]
        );
    }
}
