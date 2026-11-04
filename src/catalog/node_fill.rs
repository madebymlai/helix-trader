//! Fills the catalog through a Nautilus node: the sources whose data client
//! answers a request with a completed `DataResponseEnvelope`, which the
//! data engine writes back to the catalog (`update_catalog`).
//!
//! `missing` turns series slices (`strategy::SeriesSlice`) into what the
//! catalog lacks of them, by Nautilus' own count
//! (`get_missing_intervals_for_request`): per contract its definitions, then
//! each series' missing windows, the slivers between a live node's promoted
//! files among them, each asked once and closed by its write-back. A
//! `NodeFill` gathers each source's data client with its requests and the
//! source's period, and `run` builds a live node with those clients and the
//! catalog, and an actor asking for one request at a time with
//! `update_catalog`, each series by Nautilus' time-range pipeline
//! (`time_range_generator`): the data engine asks for a window of the period
//! at a time, serves what the catalog holds, asks the client for the rest and
//! writes each answer back over its window. The actor waits for Nautilus'
//! answer to each request (`on_data_response`) with no deadline of its own,
//! as Nautilus' requests have none: each client the fill asks answers every
//! request, a failed one with the failure in its response envelope, and one
//! that never answered would hold the run until it is interrupted. The actor
//! records the first failure (a failed request, a failure envelope) and stops
//! the node; `run` returns it. See `README.md`, "Fill paths".

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
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveNodeConfig, RoutingConfig},
};
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::{InstrumentId, TraderId},
    instruments::Instrument,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
use serde_json::json;

use super::catalog_path;
use crate::{strategy::SeriesSlice, time_range};

/// The book depth of an `OrderBookDepth` request: MBP-10's.
const DEPTH: usize = 10;

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
    /// The `Bar`s of a bar type (`request_bars`).
    Bars(BarType),
    /// `FundingRateUpdate`s (`request_funding_rates`).
    Funding,
}

impl FillData {
    /// The series of `data_type` keyed by `identifier`, as the catalog
    /// keys it (see `strategy::catalog_series`): the bar type of bars, the
    /// instrument id of anything else; an error for custom data, which no
    /// node source serves here.
    fn of(data_type: &NautilusDataType, identifier: &str) -> anyhow::Result<Self> {
        Ok(match data_type {
            NautilusDataType::QuoteTick => Self::Quotes,
            NautilusDataType::TradeTick => Self::Trades,
            NautilusDataType::OrderBookDepth => Self::Depth,
            NautilusDataType::OrderBookDelta => Self::Deltas,
            NautilusDataType::Bar => Self::Bars(identifier.parse()?),
            NautilusDataType::FundingRateUpdate => Self::Funding,
            other => bail!("no source filled through the node serves {other}"),
        })
    }

    /// The instrument whose series of this kind `identifier` keys.
    fn instrument_of(self, identifier: &str) -> String {
        match self {
            Self::Bars(bar_type) => bar_type.instrument_id().to_string(),
            _ => identifier.to_string(),
        }
    }
}

impl fmt::Display for FillData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Instrument => f.write_str("definition"),
            Self::Quotes => f.write_str("quotes"),
            Self::Trades => f.write_str("trades"),
            Self::Depth => f.write_str("book depths"),
            Self::Deltas => f.write_str("book deltas"),
            Self::Bars(bar_type) => write!(
                f,
                "{}-{} bars",
                bar_type.spec(),
                bar_type.aggregation_source()
            ),
            Self::Funding => f.write_str("funding rates"),
        }
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

/// What `catalog` lacks of `slices`, contract by contract in the order they
/// first come: the contract's definition over the span of what it lacks,
/// then each of its series' missing windows (Nautilus'
/// `get_missing_intervals_for_request`), in time order. The slices are
/// `DataRequirements::slices`, which reads no window of a series twice. A
/// contract lacking nothing is left out.
///
/// # Errors
///
/// Returns an error if a series is not the quotes, trades, book depths,
/// book deltas, bars or funding of its slice's contract, or if the catalog
/// cannot be read.
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
        let data = FillData::of(data_type, identifier)
            .with_context(|| format!("the {data_type} of {identifier}"))?;
        ensure!(
            data.instrument_of(identifier) == id.to_string(),
            "the {data} of {identifier} on a slice of {id}"
        );
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
/// its venue, as a cTrader broker's).
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
    requests: Vec<Asked>,
}

/// A request of the fill, and the period of the windows the data engine asks
/// it in; a definition has none, being asked whole.
#[derive(Debug, Clone, Copy)]
struct Asked {
    request: FillRequest,
    period: Option<SignedDuration>,
}

impl NodeFill {
    /// A fill asking nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `requests` (see `missing`), asked of `client`, each series in
    /// windows of `period(data)`: the most of it one answer holds, and one
    /// write-back covers. A definition is asked whole. No request leaves the
    /// client out of the node, so it never connects.
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
        self.requests.extend(requests.iter().map(|&request| Asked {
            request,
            period: match request.data {
                FillData::Instrument => None,
                data => Some(period(data)),
            },
        }));
    }

    /// Fills the catalog in `catalog_dir` through a live node with the
    /// clients and the catalog, one request at a time, each with
    /// `update_catalog`, so the data engine writes each answer back over
    /// its window, and what was written stays if a later window fails. Each
    /// series request carries the price precision of its
    /// instrument's definition, asked first (`price_precision`: Databento's
    /// client then asks for no definitions of its own). Nothing asked
    /// builds no node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to build or run; naming the
    /// request, if one fails, its client answers with a failure or its
    /// answer is not written back; or if the node stops before every
    /// request is answered. What was written before stays.
    pub async fn run(self, catalog_dir: &Path) -> anyhow::Result<()> {
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
            rows: 0,
            precisions: HashMap::new(),
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
            "the node stopped with {answered} of {total} requests answered"
        );
        Ok(())
    }
}

/// How the actor's requests ended: how many were answered, the first
/// failure, and whether every request was answered.
#[derive(Debug, Default)]
struct Outcome {
    answered: usize,
    failure: Option<String>,
    done: bool,
}

/// Asks for `requests` one at a time, the next once the one before is
/// answered, and stops the node after the last answer or the first
/// failure.
#[derive(Debug)]
struct FillActor {
    core: DataActorCore,
    requests: VecDeque<Asked>,
    /// The request in flight and its id.
    pending: Option<(UUID4, FillRequest)>,
    /// The rows the request in flight has got so far, over its windows.
    rows: usize,
    /// The price precision of each instrument whose definition came back.
    precisions: HashMap<InstrumentId, u8>,
    outcome: Rc<RefCell<Outcome>>,
}

impl FillActor {
    /// Asks for the next request or, all answered, stops the node.
    fn next(&mut self) {
        let Some(asked) = self.requests.pop_front() else {
            self.outcome.borrow_mut().done = true;
            self.shutdown_system(Some("every fill request answered".to_string()));
            return;
        };
        let request = asked.request;
        log::info!("Asking for {request}");
        self.rows = 0;
        match self.issue(asked) {
            Ok(id) => self.pending = Some((id, request)),
            Err(e) => self.fail(format!("asking for {request}: {e:#}")),
        }
    }

    /// Sends `asked` to the data engine, with `update_catalog`, a series in
    /// windows of its period.
    fn issue(&mut self, asked: Asked) -> anyhow::Result<UUID4> {
        let request = asked.request;
        let FillRequest {
            data,
            instrument_id,
            from,
            to,
        } = request;
        let mut params = vec![("update_catalog".to_string(), json!(true))];
        if let Some(period) = asked.period {
            params.extend(time_range::params(from, to, period));
        }
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
            FillData::Bars(bar_type) => self.request_bars(bar_type, from, to, None, None, params),
            FillData::Funding => {
                self.request_funding_rates(instrument_id, from, to, None, None, params)
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
        if let Some(reason) = response.failure_reason() {
            self.fail(format!("{request} failed: {reason}"));
            return Ok(());
        }
        self.rows += match response.response() {
            Some(DataResponse::Instrument(answer)) => {
                self.precisions
                    .insert(answer.instrument_id, answer.data.price_precision());
                1
            }
            Some(DataResponse::Quotes(answer)) => answer.data.len(),
            Some(DataResponse::Trades(answer)) => answer.data.len(),
            Some(DataResponse::BookDepth(answer)) => answer.data.len(),
            Some(DataResponse::BookDeltas(answer)) => answer.data.len(),
            Some(DataResponse::Bars(answer)) => answer.data.len(),
            Some(DataResponse::FundingRates(answer)) => answer.data.len(),
            _ => 0,
        };
        // A window of the request, written back; the next one is on its way.
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
        data::{Bar, BarType, TradeTick},
        enums::AggressorSide,
        identifiers::TradeId,
        types::{Price, Quantity},
    };

    use nautilus_persistence::backend::parquet::paths::timestamps_to_filename;

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

    #[test]
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
                    FillData::Instrument,
                    "GCQ26.GLBX",
                    "2026-07-01T00:00:00Z",
                    "2026-07-30T23:59:59.999999999Z"
                ),
                request(
                    FillData::Depth,
                    "GCQ26.GLBX",
                    "2026-07-01T00:00:00Z",
                    "2026-07-30T23:59:59.999999999Z"
                ),
                request(
                    FillData::Instrument,
                    "GCZ26.GLBX",
                    "2026-07-31T00:00:00Z",
                    "2026-08-10T00:00:00Z"
                ),
                request(
                    FillData::Depth,
                    "GCZ26.GLBX",
                    "2026-07-31T00:00:00Z",
                    "2026-08-10T00:00:00Z"
                ),
                request(
                    FillData::Trades,
                    "GCZ26.GLBX",
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
                &[trade("GCZ26.GLBX", "2026-10-21T10:00:00Z")],
                Some(ts("2026-10-21T00:00:00Z").into()),
                Some(ts("2026-10-21T23:59:59.999999999Z").into()),
                None,
            )
            .unwrap();
        let slices = [slice(
            NautilusDataType::TradeTick,
            "GCZ26.GLBX",
            "2026-10-20T00:00:00Z",
            "2026-10-23T00:00:00Z",
        )];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked[1..],
            [
                request(
                    FillData::Trades,
                    "GCZ26.GLBX",
                    "2026-10-20T00:00:00Z",
                    "2026-10-20T23:59:59.999999999Z"
                ),
                request(
                    FillData::Trades,
                    "GCZ26.GLBX",
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

    /// Quote files of `id` in the catalog in `dir` over the intervals of
    /// `files`, each named as a live node's promotion names it: its
    /// interval and a replay identity. The files are empty: coverage reads
    /// their names.
    fn promoted_quotes(dir: &Path, id: &str, files: &[(&str, &str)]) {
        let quotes = dir.join("data/quotes").join(id.replace('/', ""));
        std::fs::create_dir_all(&quotes).unwrap();
        for &(first, last) in files {
            let name = timestamps_to_filename(ts(first).into(), ts(last).into())
                .replace(".parquet", "_0123456789abcdef.parquet");
            std::fs::write(quotes.join(name), b"").unwrap();
        }
    }

    /// The slivers a live node's daily promotions leave between their files
    /// are what the catalog lacks, each asked with the definition over it.
    /// A run-start directory an older node left is ignored.
    #[test]
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
            FillData::Quotes,
            "XAUUSD.ICMARKETS",
            "2026-10-21T23:59:59.500000001Z",
            "2026-10-21T23:59:59.699999999Z",
        );
        assert_eq!(
            asked,
            [
                FillRequest {
                    data: FillData::Instrument,
                    ..sliver
                },
                sliver
            ]
        );
    }

    const BARS: &str = "GCZ26.GLBX-1-MINUTE-LAST-EXTERNAL";

    fn bars_slice(bar_type: &str, id: &str, from: &str, to: &str) -> SeriesSlice {
        SeriesSlice {
            series: (NautilusDataType::Bar, bar_type.to_string()),
            slice: Slice {
                instrument_id: InstrumentId::from(id),
                from: ts(from),
                to: ts(to),
            },
        }
    }

    #[test]
    fn bars_are_asked_as_their_bar_type_over_their_slice_after_the_definition() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, to) = ("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z");
        let slices = [bars_slice(BARS, "GCZ26.GLBX", from, to)];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked,
            [
                request(FillData::Instrument, "GCZ26.GLBX", from, to),
                request(FillData::Bars(BarType::from(BARS)), "GCZ26.GLBX", from, to),
            ]
        );
    }

    #[test]
    fn stored_bars_lack_only_what_is_outside_their_bar_type_s_stored_interval() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
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
        let slices = [bars_slice(
            BARS,
            "GCZ26.GLBX",
            "2026-10-20T00:00:00Z",
            "2026-10-22T00:00:00Z",
        )];

        let asked = missing(&catalog, &slices).unwrap();

        let bars = FillData::Bars(BarType::from(BARS));
        assert_eq!(
            asked[1..],
            [
                request(
                    bars,
                    "GCZ26.GLBX",
                    "2026-10-20T00:00:00Z",
                    "2026-10-20T23:59:59.999999999Z"
                ),
                request(
                    bars,
                    "GCZ26.GLBX",
                    "2026-10-22T00:00:00Z",
                    "2026-10-22T00:00:00Z"
                ),
            ]
        );
    }

    #[test]
    fn bars_on_a_slice_of_another_contract_are_refused_naming_both() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let slices = [bars_slice(
            BARS,
            "GCQ26.GLBX",
            "2026-10-20T00:00:00Z",
            "2026-10-21T00:00:00Z",
        )];

        let err = format!("{:#}", missing(&catalog, &slices).unwrap_err());

        assert!(err.contains(BARS), "{err}");
        assert!(err.contains("GCQ26.GLBX"), "{err}");
    }

    #[test]
    fn funding_is_asked_over_its_slice_after_the_definition() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, to) = ("2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z");
        let slices = [slice(
            NautilusDataType::FundingRateUpdate,
            "BTC-USDT-SWAP.OKX",
            from,
            to,
        )];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked,
            [
                request(FillData::Instrument, "BTC-USDT-SWAP.OKX", from, to),
                request(FillData::Funding, "BTC-USDT-SWAP.OKX", from, to),
            ]
        );
    }

    #[test]
    fn book_deltas_are_asked_as_deltas_over_their_slice() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let slices = [slice(
            NautilusDataType::OrderBookDelta,
            "GCZ26.GLBX",
            "2026-10-21T00:00:00Z",
            "2026-10-22T00:00:00Z",
        )];

        let asked = missing(&catalog, &slices).unwrap();

        assert_eq!(
            asked[1],
            request(
                FillData::Deltas,
                "GCZ26.GLBX",
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
            "GCZ26.GLBX",
            "2026-10-21T00:00:00Z",
            "2026-10-22T00:00:00Z",
        )];

        let err = format!("{:#}", missing(&catalog, &slices).unwrap_err());

        assert!(err.contains("Imbalance"), "{err}");
        assert!(err.contains("GCZ26.GLBX"), "{err}");
    }
}
