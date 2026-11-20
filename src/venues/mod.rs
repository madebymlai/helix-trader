//! Holds the routing table. Each venue keeps its history, live clients and command-line settings
//! in a module of its own.
//!
//! Each venue has a row (`Row`). The row says where the venue's instrument definitions, history
//! and live data come from, each on its own. It also names the broker that trades the
//! instruments, if there is one. Everything else asks the row and names no venue.
//!
//! `serves`, `streams` and `book_format` copy what a venue's data client does for any account, so
//! that a fill, a warmup and a live start agree with the client. The venue enforces an account's
//! market data permissions and limits, and the client logs its refusals.
//!
//! A custom series has no venue. Only its type's row (`CustomRow`) routes it, as Nautilus routes a
//! custom data command by its client id alone. No venue's row answers for custom data.
//!
//! A type with no row, or no history source, belongs to the catalog. Its data is written into the
//! catalog from outside helix, and nothing streams it:
//! - Fill-catalog skips it.
//! - A live warmup asks the catalog alone.
//! - A live start refuses it.
//!
//! Helix registers every custom data type it compiles with Nautilus once, as the process starts
//! (`register_custom_data`).
//!
//! The cTrader brokers are one kind (`CTraderBroker`). Each broker's module names its venue and
//! account, and its row is a line of `CTRADER_BROKERS`.
//!
//! No trait stands for a venue. Nautilus' factories, configs and `RoutingConfig` are the seam,
//! and the venues' needs differ too much for a trait to hide them.

pub mod databento;
pub mod ib;
pub mod ic;
pub mod lse;
pub mod okx;
pub mod pepperstone;

use std::{cell::RefCell, num::NonZeroUsize, path::Path, rc::Rc};

use ::lse::{LseDataClientConfig, LseDataClientFactory, LseRow};
use anyhow::Context;
use clap::Args;
use ctrader::{
    CTraderAccountVars, CTraderConnection, CTraderDataClientConfig, CTraderDataClientFactory,
    CTraderExecutionClientConfig, CTraderExecutionClientFactory,
};
use jiff::Timestamp;
use nautilus_common::{factories::DataClientFactory, live::clock::LiveClock};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_databento::types::register_databento_custom_data;
use nautilus_interactive_brokers::factories::InteractiveBrokersDataClientFactory;
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::{BarSpecification, BarType, CustomDataTrait, NautilusDataType},
    enums::{AggregationSource, BarAggregation, BookType, OmsType, PriceType},
    identifiers::{ClientId, InstrumentId, Venue},
};
use nautilus_okx::factories::OKXDataClientFactory;
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use self::{databento::DatabentoHistory, ib::IbHistory, okx::OkxHistory};
use crate::{
    catalog::{NodeClient, NodeFill, missing},
    live::node::{Clients, ExecClient},
    strategy::{
        Series,
        requirements::{
            CatalogSeries, CustomSeries, CustomSlice, DataRequirements, SeriesSlice, TickSeries,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CTraderBroker {
    venue: &'static str,
    account: CTraderAccountVars,
}

impl CTraderBroker {
    /// # Errors
    ///
    /// Returns an error if `CTraderConnection::from_vars` fails.
    fn connection(self) -> anyhow::Result<CTraderConnection> {
        CTraderConnection::from_vars(Venue::new(self.venue), self.account, |name| {
            std::env::var(name).ok()
        })
    }
}

/// The brokers, in the order a fill asks them and a node takes their clients.
const CTRADER_BROKERS: &[CTraderBroker] = &[ic::BROKER, pepperstone::BROKER];

fn ctrader_broker(venue: Venue) -> Option<CTraderBroker> {
    CTRADER_BROKERS
        .iter()
        .copied()
        .find(|broker| venue.as_str() == broker.venue)
}

fn ctrader_brokers_present(
    ids: &[(CTraderBroker, InstrumentId)],
) -> Vec<(CTraderBroker, Vec<InstrumentId>)> {
    CTRADER_BROKERS
        .iter()
        .map(|&broker| {
            let of_broker: Vec<InstrumentId> = ids
                .iter()
                .filter(|(of, _)| *of == broker)
                .map(|&(_, id)| id)
                .collect();
            (broker, of_broker)
        })
        .filter(|(_, of_broker)| !of_broker.is_empty())
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DefinitionsSource {
    /// Definitions are written by hand, and the data engine answers a fill's requests from them
    /// before any client.
    Catalog,
    /// Definitions are written before the node starts (`OkxHistory::write_definitions`).
    Okx,
    CTrader(CTraderBroker),
    /// Datasets: `GLBX.MDP3` for `GLBX`, `XNAS.ITCH` for `XNAS`, `OPRA.PILLAR` for `OPRA`.
    Databento,
    Ib,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistorySource {
    Okx,
    CTrader(CTraderBroker),
    Lse,
    Ib,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveDataSource {
    Okx,
    CTrader(CTraderBroker),
    /// Streams LSE's quotes, Levels 1 to 3 of CME Globex and Nasdaq TotalView, and US options'
    /// trades.
    Lse,
    Ib,
}

/// The format of a venue's book as its live data streams it (`book_format`).
///
/// L3_MBO deltas are orders. L2_MBP deltas are levels: a price level on OKX, a market depth row
/// on IB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BookFormat {
    pub(crate) deltas: BookType,
    /// The levels on each side of its depth snapshots, none if it streams no depth.
    pub(crate) depth: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Broker {
    Okx,
    /// A cTrader broker's execution client, which trades on a hedging account.
    CTrader(CTraderBroker),
    Ib,
}

/// Where a trade reads an account's equity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EquitySource {
    /// Balance totals plus the P&L of open positions, for a balance that leaves that P&L out.
    Portfolio,
    /// Balance totals alone (`Account::balances_total`), for a balance that already holds the open
    /// P&L.
    BalanceTotals,
}

impl Broker {
    /// Copies Nautilus' answer, which only a built execution client gives. A test pins the
    /// cTrader brokers' and IB's modes by building the client through its factory, not OKX's.
    /// OKX's derivatives net in the net position mode that live requires.
    #[must_use]
    pub(crate) fn oms_type(self) -> OmsType {
        match self {
            Self::CTrader(_) => OmsType::Hedging,
            Self::Okx | Self::Ib => OmsType::Netting,
        }
    }

    /// Copies the adapters. The cTrader (`account_state`) and OKX (`parse_account_state`)
    /// balances leave open positions out. IB reports net liquidation (`execution/account.rs`,
    /// `AccountSummaryBalance`), which already holds the open P&L, so `Portfolio::equity` would
    /// add it again, as Nautilus documents (nautilus_trader#3565, closed as not planned).
    #[must_use]
    pub(crate) fn equity_source(self) -> EquitySource {
        match self {
            Self::CTrader(_) | Self::Okx => EquitySource::Portfolio,
            Self::Ib => EquitySource::BalanceTotals,
        }
    }

    /// Copies the adapters, as Nautilus answers only in an account state reported once connected.
    /// The cTrader account state carries the deposit currency (`account_state`). OKX's reports
    /// none (`parse_account_state`), and neither does IB's config (`ib::live`).
    #[must_use]
    pub(crate) fn has_base_currency(self) -> bool {
        match self {
            Self::CTrader(_) => true,
            Self::Okx | Self::Ib => false,
        }
    }

    /// Copies IB's adapter (`crates/adapters/interactive_brokers/src/execution`):
    /// - `core.rs` `validate_order` denies a reduce-only order, and a list holding one.
    /// - `commands.rs` `prepare_submit_order` denies a post-only order sent alone, and sends one
    ///   in a list as a plain order.
    ///
    /// No test pins it, as Nautilus' `ExecutionClient` gives no such answer. Nautilus' bracket and
    /// `close_position` make both unless told otherwise.
    #[must_use]
    pub(crate) fn takes_reduce_only_and_post_only(self) -> bool {
        match self {
            Self::CTrader(_) | Self::Okx => true,
            Self::Ib => false,
        }
    }

    /// Returns the name its factory registers. The cTrader adapter names each client after its
    /// venue.
    fn data_client(self) -> ClientId {
        match self {
            Self::Okx => ClientId::new(OKXDataClientFactory::new().name()),
            Self::CTrader(broker) => ClientId::new(broker.venue),
            Self::Ib => ClientId::new(InteractiveBrokersDataClientFactory::new().name()),
        }
    }
}

impl LiveDataSource {
    fn client(self) -> ClientId {
        match self {
            Self::Okx => Broker::Okx.data_client(),
            Self::CTrader(broker) => Broker::CTrader(broker).data_client(),
            Self::Lse => ClientId::new(LseDataClientFactory::new().name()),
            Self::Ib => Broker::Ib.data_client(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Row {
    definitions: DefinitionsSource,
    history: HistorySource,
    live_data: LiveDataSource,
    broker: Option<Broker>,
}

impl Row {
    const fn new(
        definitions: DefinitionsSource,
        history: HistorySource,
        live_data: LiveDataSource,
        broker: Option<Broker>,
    ) -> Self {
        Self {
            definitions,
            history,
            live_data,
            broker,
        }
    }

    /// Returns the row of `venue`.
    ///
    /// GLBX, XNAS and OPRA, as Databento names them, take their sources as follows:
    /// - Definitions come from Databento, as LSE publishes none.
    /// - History and live data come from LSE.
    /// - Orders go through IB, whose adapter maps XNAS to NASDAQ.
    ///
    /// LSEDGE, LSE's own series, has no broker. A venue without a row takes everything from IB.
    #[rustfmt::skip]
    fn of(venue: Venue) -> Self {
        use {Broker as B, DefinitionsSource as D, HistorySource as H, LiveDataSource as L};
        if let Some(broker) = ctrader_broker(venue) {
            return Self::new(
                D::CTrader(broker), H::CTrader(broker), L::CTrader(broker), Some(B::CTrader(broker)),
            );
        }
        match venue.as_str() {
            //                    definitions   history       live data  broker
            "OKX"    => Self::new(D::Okx,       H::Okx,       L::Okx,    Some(B::Okx)),
            "GLBX"   => Self::new(D::Databento, H::Lse,       L::Lse,    Some(B::Ib)),
            "XNAS"   => Self::new(D::Databento, H::Lse,       L::Lse,    Some(B::Ib)),
            "OPRA"   => Self::new(D::Databento, H::Lse,       L::Lse,    Some(B::Ib)),
            "LSEDGE" => Self::new(D::Catalog,   H::Lse,       L::Lse,    None),
            _        => Self::new(D::Ib,        H::Ib,        L::Ib,     Some(B::Ib)),
        }
    }
}

/// A custom data type's row, keyed by type name alone, as Nautilus routes a custom data command by
/// its client id alone.
///
/// A type with no row belongs to the catalog.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CustomRow {
    history: Option<HistorySource>,
    live_data: Option<LiveDataSource>,
}

impl CustomRow {
    fn table() -> [(&'static str, Self); 1] {
        use HistorySource as H;
        use LiveDataSource as L;
        [(
            LseRow::type_name_static(),
            Self {
                history: Some(H::Lse),
                live_data: Some(L::Lse),
            },
        )]
    }

    fn of(series: &CustomSeries) -> Self {
        Self::table()
            .into_iter()
            .find_map(|(name, row)| (name == series.type_name()).then_some(row))
            .unwrap_or_default()
    }
}

/// The history source of `series` if its type's row has one that serves it, None for the
/// catalog's. LSE's check is its client's (`lse::serves_custom`); the other sources serve none.
///
/// # Errors
///
/// Returns an error if its type's history source does not serve `series`, naming its data type and
/// why.
pub(crate) fn serves_custom(series: &CustomSeries) -> anyhow::Result<Option<HistorySource>> {
    let data_type = series.data_type();
    let Some(source) = CustomRow::of(series).history else {
        return Ok(None);
    };
    match source {
        HistorySource::Lse => ::lse::serves_custom(data_type)?,
        HistorySource::Okx => anyhow::bail!("OKX's history serves no custom data, {data_type}"),
        HistorySource::CTrader(broker) => anyhow::bail!(
            "{}'s history serves no custom data, {data_type}",
            broker.venue
        ),
        HistorySource::Ib => anyhow::bail!("IB's history serves no custom data, {data_type}"),
    }
    Ok(Some(source))
}

/// Checks that the live data source of `series` streams it.
///
/// LSE's client streams no custom data (`lse::streams_custom`), and neither do the others.
///
/// # Errors
///
/// Returns an error if:
/// - Its type has no row, or one with no live data source, naming `series` and its type.
/// - Its live data source does not stream it, naming its data type and why.
pub(crate) fn streams_custom(series: &CustomSeries) -> anyhow::Result<()> {
    let data_type = series.data_type();
    match CustomRow::of(series).live_data {
        Some(LiveDataSource::Lse) => ::lse::streams_custom(data_type),
        Some(LiveDataSource::Okx) => {
            anyhow::bail!("OKX's live data streams no custom data, {data_type}")
        }
        Some(LiveDataSource::CTrader(broker)) => anyhow::bail!(
            "{}'s live data streams no custom data, {data_type}",
            broker.venue
        ),
        Some(LiveDataSource::Ib) => {
            anyhow::bail!("IB's live data streams no custom data, {data_type}")
        }
        None => anyhow::bail!(
            "no live data source streams the custom series {series}: none streams custom data \
             of the type {}",
            series.type_name()
        ),
    }
}

/// Returns the data client of `series` on the live node, by the name the node registers it under,
/// or None if nothing streams it.
pub(crate) fn custom_data_client(series: &CustomSeries) -> Option<ClientId> {
    CustomRow::of(series).live_data.map(LiveDataSource::client)
}

/// Registers every custom data type helix compiles with Nautilus, before any config loads
/// (`main`).
///
/// Nautilus decodes and encodes custom data by the registered schema of its type
/// (`ensure_custom_data_registered`). Nautilus' Databento adapter registers neither
/// `DatabentoImbalance` nor `DatabentoStatistics` itself (`register_databento_custom_data`). No
/// row routes them, so they belong to the catalog. Idempotent.
pub fn register_custom_data() {
    LseRow::register();
    register_databento_custom_data();
}

/// Returns how the accounts of `venue` keep positions, as its broker's accounts do.
///
/// Returns `OmsType::Unspecified` if no broker trades the venue. A backtest simulates each venue
/// this way, as Nautilus' docs ask.
#[must_use]
pub fn oms_type(venue: Venue) -> OmsType {
    Row::of(venue)
        .broker
        .map_or(OmsType::Unspecified, Broker::oms_type)
}

/// Returns whether a broker trades the instruments of `venue`. Every venue has one but LSEDGE,
/// which is only read.
#[must_use]
pub fn has_broker(venue: Venue) -> bool {
    Row::of(venue).broker.is_some()
}

/// Returns whether the broker's account on `venue` has a base currency. If so, a backtest's
/// account takes the balance's currency as its base.
#[must_use]
pub fn has_base_currency(venue: Venue) -> bool {
    Row::of(venue).broker.is_some_and(Broker::has_base_currency)
}

/// Returns the series that supply the rates of `pair`, an FX pair a strategy converts equity
/// through (`[trade] conversions`).
///
/// The rates come from the first form the history of the pair's venue serves (`serves`): 1-minute
/// BID and ASK bars, then quotes. Bars come first for two reasons:
/// - A rate needs only the latest value. For a pair with no quote cached, Nautilus reads it at
///   the close of the latest BID and ASK bars (`Cache::get_xrate`).
/// - Bars are lighter to fill.
///
/// Rates come from one form only, so that a backtest and live agree, as Nautilus reads a pair's
/// quotes before its bars, however old.
///
/// # Errors
///
/// Returns an error if the venue's history serves neither form, as OKX's does not. The error
/// names the pair, its venue and why, since no rate of the pair could be read.
pub fn rate_series(pair: InstrumentId) -> anyhow::Result<Vec<Series>> {
    let bars = [PriceType::Bid, PriceType::Ask].map(|price_type| {
        BarType::new(
            pair,
            BarSpecification::new(1, BarAggregation::Minute, price_type),
            AggregationSource::External,
        )
    });
    let quotes = TickSeries {
        data: NautilusDataType::QuoteTick,
        instrument_id: pair,
    };
    let no_bars = match bars
        .iter()
        .try_for_each(|bars| serves(&CatalogSeries::Bars(*bars)))
    {
        Ok(()) => return Ok(bars.map(Series::Bars).to_vec()),
        Err(e) => e,
    };
    let no_quotes = match serves(&CatalogSeries::Ticks(quotes.clone())) {
        Ok(()) => return Ok(vec![Series::Ticks(quotes)]),
        Err(e) => e,
    };
    anyhow::bail!(
        "no rate of {pair} can be read: {venue}'s history serves neither its 1-minute BID and \
         ASK bars ({no_bars:#}) nor its quotes ({no_quotes:#})",
        venue = pair.venue
    )
}

/// Checks that the history of the venue of `source` serves it.
///
/// This copies what the data client that its fill asks requests:
/// - cTrader requests quotes alone (`ctrader::quoted`).
/// - OKX requests trades and candles (`okx::serves`).
/// - LSE requests what `lse::serves` allows.
/// - IB requests time bars, quotes and trades (`ib::serves`).
///
/// # Errors
///
/// Returns an error if the venue's history does not serve `source`. The error names it and says
/// why.
pub(crate) fn serves(source: &CatalogSeries) -> anyhow::Result<()> {
    match Row::of(source.instrument_id().venue).history {
        HistorySource::CTrader(broker) => {
            ctrader::quoted(Venue::new(broker.venue), &[source.key()]).map(drop)
        }
        HistorySource::Okx => okx::serves(source),
        HistorySource::Lse => ::lse::serves(&source.key()),
        HistorySource::Ib => ib::serves(source),
    }
}

/// Checks that the live data of the venue of `source` streams it as a run subscribes to it.
///
/// This copies the subscriptions of the venue's data client:
/// - cTrader streams quotes alone (`ctrader::quoted`).
/// - OKX streams quotes, trades, candles and book (`okx::streams`).
/// - LSE streams what `lse::streams` allows.
/// - IB streams quotes, trades, time bars and book deltas (`ib::streams`).
///
/// A book is checked in its venue's format (`book_format`).
///
/// A subscription that a client does not take does not fail the run. Nautilus' `DataClient`
/// defaults log "Handler not implemented", and the data engine logs only the error of a
/// subscription (`crates/data/src/engine/mod.rs:875-877`). The strategy would then run deaf to
/// it.
///
/// # Errors
///
/// Returns an error if the venue's live data does not stream `source`. The error names it and
/// says why.
pub(crate) fn streams(source: &CatalogSeries) -> anyhow::Result<()> {
    match Row::of(source.instrument_id().venue).live_data {
        LiveDataSource::CTrader(broker) => {
            ctrader::quoted(Venue::new(broker.venue), &[source.key()]).map(drop)
        }
        LiveDataSource::Okx => okx::streams(source),
        LiveDataSource::Lse => ::lse::streams(&source.key()),
        LiveDataSource::Ib => ib::streams(source),
    }
}

/// Returns the format of the book of `venue`'s instruments as its live data source streams it.
///
/// This copies the data client:
/// - LSE streams orders and depth of ten levels a side (`lse::BOOK`).
/// - IB streams market depth rows and no depth (`ib::BOOK`).
/// - OKX streams price levels and depth of five levels a side (`okx::BOOK`).
/// - A cTrader broker streams no book.
///
/// A venue has one format, the one its live data streams. The catalog keeps one series of a
/// venue's book, so the live data of each row is LSE's, the one history that serves a book
/// (`serves`).
pub(crate) fn book_format(venue: Venue) -> Option<BookFormat> {
    match Row::of(venue).live_data {
        LiveDataSource::CTrader(_) => None,
        LiveDataSource::Okx => Some(okx::BOOK),
        LiveDataSource::Lse => Some(lse::BOOK),
        LiveDataSource::Ib => Some(ib::BOOK),
    }
}

/// Returns where a trade on `venue` reads its broker's account equity, or `Portfolio::equity` with
/// no broker.
#[must_use]
pub fn equity_source(venue: Venue) -> EquitySource {
    Row::of(venue)
        .broker
        .map_or(EquitySource::Portfolio, Broker::equity_source)
}

/// Returns the data client a live node asks for the definitions of `venue`'s instruments.
///
/// It is the broker's client, else the live data source's (LSE publishes none for LSEDGE, see
/// `strategy::warmup`). The catalog answers first.
#[must_use]
pub fn live_definitions_client(venue: Venue) -> ClientId {
    let row = Row::of(venue);
    row.broker
        .map_or_else(|| row.live_data.client(), Broker::data_client)
}

/// Returns whether the broker of `venue` takes reduce-only and post-only orders.
///
/// It returns false if no broker trades the venue. A backtest asks it as live does, so that both
/// send the same orders.
#[must_use]
pub fn takes_reduce_only_and_post_only(venue: Venue) -> bool {
    Row::of(venue)
        .broker
        .is_some_and(Broker::takes_reduce_only_and_post_only)
}

/// The venue settings of a live node: the IB account and client id.
#[derive(Debug, Clone, Args)]
pub struct LiveVenueArgs {
    #[command(flatten)]
    ib: ib::IbLiveArgs,
}

/// The venue settings of a catalog fill: the IB client id.
#[derive(Debug, Args)]
pub struct HistoryVenueArgs {
    #[command(flatten)]
    ib: ib::IbHistoryArgs,
}

#[must_use]
fn ctrader_node_client(
    connection: CTraderConnection,
    instrument_ids: Vec<InstrumentId>,
) -> NodeClient {
    let config = CTraderDataClientConfig::new(connection, instrument_ids);
    NodeClient {
        factory: Box::new(CTraderDataClientFactory::new(&config)),
        config: Box::new(config),
    }
}

/// Adds a fill of `slices` to the node.
///
/// The fill requests cTrader quotes for what the catalog misses, in windows of at most one tick
/// request's span. The connection is read only if something is missing.
///
/// # Errors
///
/// Returns an error if:
/// - The catalog cannot be read.
/// - `connection` fails.
fn add_ctrader_fill(
    node: &mut NodeFill,
    catalog: &ParquetDataCatalog,
    connection: impl FnOnce() -> anyhow::Result<CTraderConnection>,
    instrument_ids: Vec<InstrumentId>,
    slices: &[SeriesSlice],
) -> anyhow::Result<()> {
    let requests = missing(catalog, slices)?;
    if !requests.is_empty() {
        node.add(
            ctrader_node_client(connection()?, instrument_ids),
            &requests,
            |_| ctrader::TICK_REQUEST_SPAN,
        );
    }
    Ok(())
}

fn of_source(slices: &[SeriesSlice], source: HistorySource) -> Vec<SeriesSlice> {
    slices
        .iter()
        .filter(|slice| Row::of(slice.series.instrument_id().venue).history == source)
        .cloned()
        .collect()
}

fn of_custom_source(custom: &[CustomSlice], source: HistorySource) -> Vec<CustomSlice> {
    custom
        .iter()
        .filter(|slice| CustomRow::of(&slice.series).history == Some(source))
        .cloned()
        .collect()
}

fn of_definitions_source(slices: &[SeriesSlice], source: DefinitionsSource) -> Vec<SeriesSlice> {
    slices
        .iter()
        .filter(|slice| Row::of(slice.series.instrument_id().venue).definitions == source)
        .cloned()
        .collect()
}

/// Fills the catalog in `catalog_dir` with the instruments and series `requirements` reads over
/// `[start, end]`.
///
/// Each contract is filled over its slice, from the history source of its row. Custom series
/// come from the row of their type (`CustomRow::of`).
///
/// The cTrader brokers, OKX and LSE fill through a Nautilus node (`catalog::NodeFill`). IB fills
/// the others without one, until its client answers with response envelopes, and connects only if
/// it has instruments.
///
/// Definitions come from the definitions source of each row. The GLBX, XNAS and OPRA definitions
/// are asked of Databento's data client in LSE's fill, each within the end of its dataset.
///
/// Every series is checked against its history (`serves`, `serves_custom`, `ib::check_held`)
/// before anything connects. A source with nothing missing reads no credential, apart from IB's
/// connection.
///
/// # Errors
///
/// Returns an error if:
/// - A venue's history or a custom series' type's does not serve a series, before anything
///   connects.
/// - IB holds no bars of a series as old as its slice, before anything connects.
/// - The catalog cannot be read.
/// - `DATABENTO_API_KEY` is missing while Databento has a definition to give.
/// - `add_ctrader_fill`, `OkxHistory::write_definitions`, `lse::add_fill`, `NodeFill::run` (a
///   missing `LSE_API_KEY`, `LSE_CME_API_KEY` or `LSE_NASDAQ_API_KEY` among them),
///   `IbHistory::connect` or `IbHistory::fill` fails.
pub async fn fill_history(
    requirements: &DataRequirements,
    start: Timestamp,
    end: Timestamp,
    venues: &HistoryVenueArgs,
    catalog_dir: &Path,
) -> anyhow::Result<()> {
    for series in &requirements.custom {
        serves_custom(series)?;
    }
    for source in requirements.catalog_series() {
        serves(&source)?;
    }

    // A rolled root's series is filled contract by contract, each over its slice
    let slices = requirements.slices(start, end);
    let ib_slices = of_source(&slices, HistorySource::Ib);
    let mut catalog = ParquetDataCatalog::new(catalog_dir, None, None, None, None);
    ib::check_held(
        &catalog,
        &ib_slices,
        Timestamp::from(get_atomic_clock_realtime().get_time_ns()),
    )?;
    let (mut okx_ids, mut ctrader_ids, mut lse_ids, mut ib_ids) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for id in requirements.instrument_ids() {
        match Row::of(id.venue).history {
            HistorySource::Okx => okx_ids.push(id),
            HistorySource::CTrader(broker) => ctrader_ids.push((broker, id)),
            HistorySource::Lse => lse_ids.push(id),
            HistorySource::Ib => ib_ids.push(id),
        }
    }
    let mut node = NodeFill::new();
    for (broker, ids) in ctrader_brokers_present(&ctrader_ids) {
        add_ctrader_fill(
            &mut node,
            &catalog,
            || broker.connection(),
            ids,
            &of_source(&slices, HistorySource::CTrader(broker)),
        )?;
    }
    let okx_definitions = of_definitions_source(&slices, DefinitionsSource::Okx);
    if !okx_definitions.is_empty() {
        OkxHistory::default()
            .write_definitions(&catalog, &okx_definitions)
            .await?;
    }
    if !okx_ids.is_empty() {
        let slices = of_source(&slices, HistorySource::Okx);
        let requests = okx::requests(&catalog, &slices)?;
        if !requests.is_empty() {
            node.add(okx::node_client(&okx_ids)?, &requests, okx::period);
        }
    }
    let custom = requirements.custom_slices(start, end);
    let lse_custom = of_custom_source(&custom, HistorySource::Lse);
    if !lse_ids.is_empty() || !lse_custom.is_empty() {
        lse::add_fill(
            &mut node,
            &catalog,
            &of_source(&slices, HistorySource::Lse),
            &lse_custom,
            LseDataClientConfig::default(),
            || {
                let api_key =
                    std::env::var("DATABENTO_API_KEY").context("reading DATABENTO_API_KEY")?;
                DatabentoHistory::new(&api_key)
            },
        )
        .await?;
    }
    node.run(catalog_dir, || Rc::new(RefCell::new(LiveClock::default())))
        .await?;

    // Temporary until nautilus_trader#5472 is fixed: IB then fills through the node
    if !ib_ids.is_empty() {
        let ib = IbHistory::connect(
            venues.ib.client_id,
            end,
            Timestamp::from(get_atomic_clock_realtime().get_time_ns()),
        )
        .await?;
        ib.fill(&mut catalog, &ib_slices).await?;
    }
    Ok(())
}

/// Returns the data and execution clients of a cTrader broker, both on its venue so Nautilus
/// routes the broker's data and orders to them without a route of ours.
fn ctrader_clients(connection: CTraderConnection, instrument_ids: Vec<InstrumentId>) -> Clients {
    let data = CTraderDataClientConfig::new(connection.clone(), instrument_ids.clone());
    let exec = CTraderExecutionClientConfig::new(connection, instrument_ids);
    Clients {
        data_factory: Box::new(CTraderDataClientFactory::new(&data)),
        data_config: Box::new(data),
        data_routing: RoutingConfig::default(),
        exec: Some(ExecClient {
            factory: Box::new(CTraderExecutionClientFactory::new(&exec)),
            config: Box::new(exec),
            routing: RoutingConfig::default(),
        }),
    }
}

/// The routes of a live node: the instruments of each adapter's clients, the venues routed to IB's
/// data client, and the live data sources of the custom series.
#[derive(Debug, Default, PartialEq, Eq)]
struct LiveRoutes {
    okx: Vec<InstrumentId>,
    ctrader: Vec<(CTraderBroker, InstrumentId)>,
    lse: Vec<InstrumentId>,
    ib: Vec<InstrumentId>,
    ib_data_venues: Vec<Venue>,
    custom: Vec<LiveDataSource>,
}

impl LiveRoutes {
    /// Routes `instrument_ids` and `custom` to the live clients.
    ///
    /// - Each instrument goes to the clients of its live data source, and to IB's too if IB is
    ///   its broker. IB's execution client resolves the instruments of its orders from those its
    ///   data client loads (`load_ids`), so GLBX, XNAS and OPRA go to both.
    /// - IB's data client is routed the venues whose live data is IB's, once each.
    /// - Each custom series goes to the live data source of its type, once, whether or not an
    ///   instrument routes to it.
    fn of(instrument_ids: &[InstrumentId], custom: &[CustomSeries]) -> Self {
        let mut routes = Self::default();
        for &id in instrument_ids {
            let row = Row::of(id.venue);
            match row.live_data {
                LiveDataSource::Okx => routes.okx.push(id),
                LiveDataSource::CTrader(broker) => routes.ctrader.push((broker, id)),
                LiveDataSource::Lse => routes.lse.push(id),
                LiveDataSource::Ib => {
                    if !routes.ib_data_venues.contains(&id.venue) {
                        routes.ib_data_venues.push(id.venue);
                    }
                }
            }
            if row.live_data == LiveDataSource::Ib || row.broker == Some(Broker::Ib) {
                routes.ib.push(id);
            }
        }
        for series in custom {
            if let Some(source) = CustomRow::of(series).live_data
                && !routes.custom.contains(&source)
            {
                routes.custom.push(source);
            }
        }
        routes
    }

    /// LSE's client is set up if an instrument or a custom series routes to it, and streams no
    /// instrument if none does (`lse::live_clients`). The other adapters stream no custom data,
    /// so a live start refuses their custom series before this (`streams_custom`).
    ///
    /// # Errors
    ///
    /// Returns an error if `live_clients` would, apart from its checks of what the run reads.
    async fn clients(self, venues: &LiveVenueArgs, catalog: &Path) -> anyhow::Result<Vec<Clients>> {
        let Self {
            okx: okx_ids,
            ctrader: ctrader_ids,
            lse: lse_ids,
            ib: ib_ids,
            ib_data_venues,
            custom,
        } = self;
        let ctrader = ctrader_brokers_present(&ctrader_ids)
            .into_iter()
            .map(|(broker, ids)| broker.connection().map(|connection| (connection, ids)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut clients = Vec::new();
        if !okx_ids.is_empty() {
            let okx = okx::clients(&okx_ids, catalog).await?;
            clients.push(okx.into_node_clients());
        }
        for (connection, ids) in ctrader {
            connection.check_account().await?;
            clients.push(ctrader_clients(connection, ids));
        }
        if !lse_ids.is_empty() || custom.contains(&LiveDataSource::Lse) {
            clients.push(lse::live_clients(&lse_ids));
        }
        if !ib_ids.is_empty() {
            let account = venues.ib.account.as_ref().context(
                "the strategy trades IB instruments: give the IB account \
                 (--account or NAUTILUS_IB_ACCOUNT_ID)",
            )?;
            let ib = ib::clients(account, venues.ib.client_id, &ib_ids).await?;
            clients.push(ib.into_node_clients(&ib_data_venues));
        }
        Ok(clients)
    }
}

/// Returns the live node's clients for the instruments and custom series `requirements` reads
/// (see `LiveRoutes`).
///
/// The clients come in this order: OKX's, each cTrader broker's, LSE's, then IB's. Each but LSE's
/// is a data client with the execution client of its adapter on its connection. LSE's is a data
/// client alone (`lse::live_clients`). It is set up for a custom series with no venue, as the
/// subscription names the client (`custom_data_client`).
///
/// Every venue the run reads has a data route, so helix makes no data client the default.
/// Nautilus makes the first data client on no venue the default, in no set order, and would
/// refuse IB's if it became the default after LSE's (see `live::node::Clients`). IB's execution
/// client takes the default route of orders, which is every order but OKX's and the cTrader
/// brokers'.
///
/// Every series the run subscribes to is checked against its venue's live data (`streams`,
/// `streams_custom`) before anything connects. Otherwise the client would log the subscription
/// and the run would go deaf. OKX's clients are set up once its account trades each instrument at
/// the leverage that its definition in `catalog` holds. IB's clients trade the IB account of
/// `venues`.
///
/// # Errors
///
/// Returns an error if:
/// - `requirements` reads a series that its venue's live data does not stream, or a custom series
///   that its type's does not (see `streams`, `streams_custom`). The error names it and comes
///   before anything connects.
/// - A cTrader broker's environment or credential is missing (see `CTraderConnection::from_vars`),
///   before anything connects.
/// - OKX's clients cannot be set up or the OKX account does not suit the run (see `okx::clients`).
/// - cTrader is unreachable, or a broker's account is not hedging or not the credential's (see
///   `CTraderConnection::check_account`).
/// - The strategy trades IB instruments and no account is given, or IB's clients cannot be set up
///   (see `ib::clients`).
pub(crate) async fn live_clients(
    requirements: &DataRequirements,
    instrument_ids: &[InstrumentId],
    venues: &LiveVenueArgs,
    catalog: &Path,
) -> anyhow::Result<Vec<Clients>> {
    for series in requirements.catalog_series() {
        streams(&series)?;
    }
    for series in &requirements.custom {
        streams_custom(series)?;
    }
    LiveRoutes::of(instrument_ids, &requirements.custom)
        .clients(venues, catalog)
        .await
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        ffi::OsStr,
        num::NonZeroUsize,
        os::unix::ffi::OsStrExt,
        rc::Rc,
    };

    use ::lse::LSEDGE_VENUE;
    use clap::{Command, FromArgMatches};
    use ctrader::CTraderEnvironment;
    use jiff::SignedDuration;
    use nautilus_common::{
        actor::DataActor,
        cache::Cache,
        clients::DataClient,
        clock::VirtualClock,
        enums::Environment,
        live::runner::{replace_data_event_sender, replace_exec_event_sender},
        messages::data::{
            DataCommand, RequestBars, RequestBookDeltas, RequestBookDepth, RequestCommand,
            RequestQuotes, RequestTrades, SubscribeCommand,
        },
        msgbus::{self, MessagingSwitchboard, stubs::get_typed_into_message_saving_handler},
    };
    use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
    use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig, node::LiveNode};
    use nautilus_model::{
        data::{BarType, FundingRateUpdate, QuoteTick, TradeTick},
        enums::{AggressorSide, BookType, OrderSide, OrderType},
        identifiers::{AccountId, ClientId, StrategyId, TradeId, TraderId},
        instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
        orders::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use nautilus_okx::{common::consts::OKX_VENUE, config::OKXDataClientConfig};
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        catalog::{FillData, FillRequest},
        strategy::subscriptions::Subscriptions,
    };

    fn demo_connection(broker: CTraderBroker) -> CTraderConnection {
        let map: HashMap<&str, &str> = HashMap::from([
            ("CTRADER_CLIENT_ID", "12345_clientIdValue"),
            ("CTRADER_CLIENT_SECRET", "clientSecretValue"),
            ("CTRADER_ACCESS_TOKEN", "accessTokenValue"),
            ("CTRADER_REFRESH_TOKEN", "refreshTokenValue"),
            ("HOME", "/home/u"),
            ("IC_ENVIRONMENT", "demo"),
            ("IC_ACCOUNT_ID", "1000001"),
            ("PEPPERSTONE_ENVIRONMENT", "demo"),
            ("PEPPERSTONE_ACCOUNT_ID", "2000002"),
        ]);
        let var = |name: &str| map.get(name).map(ToString::to_string);
        CTraderConnection::from_vars(Venue::new(broker.venue), broker.account, var).unwrap()
    }

    #[rstest]
    fn each_ctrader_venue_reads_its_own_account_beside_the_login_s_credential() {
        let ic = demo_connection(ic::BROKER);
        let pepperstone = demo_connection(pepperstone::BROKER);

        assert_eq!(ic.account_id(), AccountId::from("ICMARKETS-DEMO"));
        assert_eq!(ic.environment, CTraderEnvironment::Demo);
        assert_eq!(ic.credential.account_id.get(), 1_000_001);
        assert_eq!(
            pepperstone.account_id(),
            AccountId::from("PEPPERSTONE-DEMO")
        );
        assert_eq!(pepperstone.environment, CTraderEnvironment::Demo);
        assert_eq!(pepperstone.credential.account_id.get(), 2_000_002);
        assert_eq!(ic.credential.token_file, pepperstone.credential.token_file);
    }

    fn reading(bars: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: bars.iter().map(|bars| BarType::from(*bars)).collect(),
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    #[rstest]
    #[case::no_instruments(vec![], vec![])]
    #[case::one_broker_s(
        vec![(pepperstone::BROKER, "XAUUSD.PEPPERSTONE")],
        vec![(pepperstone::BROKER, vec!["XAUUSD.PEPPERSTONE"])]
    )]
    #[case::two_brokers_out_of_order(
        vec![
            (pepperstone::BROKER, "XAUUSD.PEPPERSTONE"),
            (ic::BROKER, "XAUUSD.ICMARKETS"),
            (pepperstone::BROKER, "EURUSD.PEPPERSTONE"),
        ],
        vec![
            (ic::BROKER, vec!["XAUUSD.ICMARKETS"]),
            (pepperstone::BROKER, vec!["XAUUSD.PEPPERSTONE", "EURUSD.PEPPERSTONE"]),
        ]
    )]
    fn the_ctrader_brokers_present_come_in_their_table_s_order_each_with_its_instruments_in_theirs(
        #[case] ids: Vec<(CTraderBroker, &str)>,
        #[case] present: Vec<(CTraderBroker, Vec<&str>)>,
    ) {
        let ids: Vec<(CTraderBroker, InstrumentId)> = ids
            .into_iter()
            .map(|(broker, id)| (broker, InstrumentId::from(id)))
            .collect();
        let present: Vec<(CTraderBroker, Vec<InstrumentId>)> = present
            .into_iter()
            .map(|(broker, ids)| (broker, instrument_ids(&ids)))
            .collect();

        assert_eq!(ctrader_brokers_present(&ids), present);
    }

    #[rstest]
    fn a_ctrader_broker_s_clients_take_the_instruments_of_its_bars_and_of_its_quotes_read_alone() {
        let (gold, eurusd) = (
            InstrumentId::from("XAUUSD.PEPPERSTONE"),
            InstrumentId::from("EURUSD.PEPPERSTONE"),
        );
        let requirements = DataRequirements {
            ticks: vec![TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: eurusd,
            }],
            ..reading(&[
                "XAUUSD.PEPPERSTONE-1-HOUR-MID-INTERNAL",
                "XAUUSD.PEPPERSTONE-1-DAY-MID-INTERNAL",
            ])
        };

        let routes = LiveRoutes::of(&requirements.instrument_ids(), &requirements.custom);

        assert_eq!(
            routes.ctrader,
            [(pepperstone::BROKER, gold), (pepperstone::BROKER, eurusd)]
        );
    }

    #[rstest]
    fn the_node_gets_factories_that_create_the_broker_s_clients_on_its_venue() {
        let (data_tx, _data_events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let (exec_tx, _exec_events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(exec_tx);
        let connection = demo_connection(pepperstone::BROKER);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));

        let clients = ctrader_clients(connection, vec![InstrumentId::from("XAUUSD.PEPPERSTONE")]);

        let data = clients
            .data_factory
            .create(
                clients.data_factory.name(),
                clients.data_config.as_ref(),
                cache.clone().into(),
                clock.clone(),
            )
            .unwrap();
        assert_eq!(data.client_id(), ClientId::from("PEPPERSTONE"));
        assert_eq!(data.venue(), Some(Venue::new("PEPPERSTONE")));
        assert!(data.is_disconnected());
        let ExecClient {
            factory: exec_factory,
            config: exec_config,
            ..
        } = clients.exec.unwrap();
        let exec = exec_factory
            .create(
                TraderId::from("HELIX-001"),
                exec_factory.name(),
                exec_config.as_ref(),
                cache.into(),
                clock,
            )
            .unwrap();
        assert_eq!(exec.client_id(), ClientId::from("PEPPERSTONE"));
        assert_eq!(exec.venue(), Venue::new("PEPPERSTONE"));
        assert_eq!(exec.oms_type(), OmsType::Hedging);
        assert_eq!(exec.account_id(), AccountId::from("PEPPERSTONE-DEMO"));
        assert!(!exec.is_connected());
    }

    fn parsed<A: Args + FromArgMatches>(argv: &[&str]) -> Result<A, clap::Error> {
        let matches = A::augment_args(Command::new("venues"))
            .try_get_matches_from(["venues"].into_iter().chain(argv.iter().copied()))?;
        A::from_arg_matches(&matches)
    }

    #[rstest]
    #[case::the_client_id_given(&["--account", "DU1234567", "--client-id", "7"], 7)]
    #[case::the_adapter_s_default_client(&["--account", "DU1234567"], 1)]
    fn live_takes_the_ib_account_and_client_id(#[case] argv: &[&str], #[case] client_id: i32) {
        let venues: LiveVenueArgs = parsed(argv).unwrap();
        assert_eq!(venues.ib.account, Some("DU1234567".parse().unwrap()));
        assert_eq!(venues.ib.client_id, client_id);
    }

    /// Reads the fallback from the argument definition, as setting the variable would race the
    /// other tests. It is not required: an OKX-only run has no IB account.
    #[rstest]
    fn the_live_account_falls_back_on_nautilus_ib_account_id() {
        let live = LiveVenueArgs::augment_args(Command::new("live"));
        let account = live
            .get_arguments()
            .find(|arg| arg.get_id() == "account")
            .unwrap();
        assert_eq!(
            account.get_env(),
            Some(std::ffi::OsStr::new("NAUTILUS_IB_ACCOUNT_ID"))
        );
        assert!(!account.is_required_set());
    }

    #[rstest]
    #[case::by_default(&[], 10)]
    #[case::the_client_id_given(&["--client-id", "12"], 12)]
    fn fill_catalog_is_ib_client_10_unless_told_otherwise(
        #[case] argv: &[&str],
        #[case] client_id: i32,
    ) {
        let venues: HistoryVenueArgs = parsed(argv).unwrap();
        assert_eq!(venues.ib.client_id, client_id);
    }

    #[rstest]
    #[case::an_ib_instrument(ticks(&[]), "SXR8.IBIS")]
    #[case::glbx(ticks(&[(NautilusDataType::QuoteTick, "GCZ26.GLBX")]), "GCZ26.GLBX")]
    #[case::an_option(
        ticks(&[(NautilusDataType::TradeTick, "AAPL  261120C00355000.OPRA")]),
        "AAPL  261120C00355000.OPRA"
    )]
    #[tokio::test]
    async fn a_run_trading_ib_instruments_without_an_account_is_refused_naming_where_to_give_it(
        #[case] requirements: DataRequirements,
        #[case] loaded: &str,
    ) {
        let Err(e) = live_clients(
            &requirements,
            &[InstrumentId::from(loaded)],
            &live_args(),
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up {loaded}'s clients without IB's account");
        };
        let err = e.to_string();
        assert!(
            err.contains("--account") && err.contains("NAUTILUS_IB_ACCOUNT_ID"),
            "{err}"
        );
    }

    pub(in crate::venues) fn created_oms_type(clients: Clients) -> OmsType {
        let (exec_tx, _exec_events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(exec_tx);
        let ExecClient {
            factory, config, ..
        } = clients.exec.unwrap();
        let client = factory
            .create(
                TraderId::from("HELIX-001"),
                factory.name(),
                config.as_ref(),
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap();
        assert!(!client.is_connected());
        client.oms_type()
    }

    #[rstest]
    fn each_ctrader_broker_s_execution_client_keeps_positions_as_the_broker_answers() {
        for &broker in CTRADER_BROKERS {
            let id = InstrumentId::from(format!("XAUUSD.{}", broker.venue).as_str());

            let created = created_oms_type(ctrader_clients(demo_connection(broker), vec![id]));

            assert_eq!(created, Broker::CTrader(broker).oms_type(), "{broker:?}");
        }
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    const FILL_START: &str = "2026-10-01T00:00:00Z";
    /// An end that IB's connection refuses as not yet closed (`IbHistory::connect`).
    const FILL_END: &str = "2099-01-01T00:00:00Z";
    const IB_REFUSES: &str = "within a minute of now";
    const SWAP: &str = "BTC-USDT-SWAP.OKX";

    fn ticks(series: &[(NautilusDataType, &str)]) -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: series
                .iter()
                .map(|(data, id)| TickSeries {
                    data: data.clone(),
                    instrument_id: InstrumentId::from(*id),
                })
                .collect(),
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    fn history_args() -> HistoryVenueArgs {
        parsed(&[]).unwrap()
    }

    fn quote(id: &str) -> QuoteTick {
        let (px, qty) = (Price::from("100.00"), Quantity::from(1));
        let ts = at(FILL_START).into();
        QuoteTick::new(InstrumentId::from(id), px, px, qty, qty, ts, ts)
    }

    fn trade(id: &str) -> TradeTick {
        let ts = at(FILL_START).into();
        TradeTick::new(
            InstrumentId::from(id),
            Price::from("100.00"),
            Quantity::from(1),
            AggressorSide::Sell,
            TradeId::new("1"),
            ts,
            ts,
        )
    }

    fn store_quotes(catalog: &ParquetDataCatalog, quotes: &[QuoteTick]) {
        catalog
            .write_to_parquet(
                quotes,
                Some(at(FILL_START).into()),
                Some(at(FILL_END).into()),
                None,
            )
            .unwrap();
    }

    fn store_okx_definition(catalog: &ParquetDataCatalog, id: &str) {
        let mut swap = crypto_perpetual_ethusdt();
        swap.id = InstrumentId::from(id);
        catalog
            .write_instruments(vec![InstrumentAny::CryptoPerpetual(swap)])
            .unwrap();
    }

    #[tokio::test]
    async fn a_complete_catalog_fills_with_no_credential_no_node_and_no_ib() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, to) = (Some(at(FILL_START).into()), Some(at(FILL_END).into()));
        store_okx_definition(&catalog, SWAP);
        store_quotes(&catalog, &[quote("XAUUSD.ICMARKETS")]);
        store_quotes(&catalog, &[quote("XAUUSD.PEPPERSTONE")]);
        catalog
            .write_to_parquet(&[trade("GCZ26.GLBX")], from, to, None)
            .unwrap();
        catalog
            .write_to_parquet(&[trade(SWAP)], from, to, None)
            .unwrap();
        let funding = FundingRateUpdate::new(
            InstrumentId::from(SWAP),
            Decimal::new(1, 4),
            Some(480),
            None,
            at(FILL_START).into(),
            at(FILL_START).into(),
        );
        catalog
            .write_to_parquet(&[funding], from, to, None)
            .unwrap();
        let requirements = ticks(&[
            (NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
            (NautilusDataType::QuoteTick, "XAUUSD.PEPPERSTONE"),
            (NautilusDataType::TradeTick, "GCZ26.GLBX"),
            (NautilusDataType::TradeTick, SWAP),
        ]);

        fill_history(
            &requirements,
            at(FILL_START),
            at(FILL_END),
            &history_args(),
            dir.path(),
        )
        .await
        .unwrap();
    }

    /// A type that no row routes belongs to the catalog, so a fill hands it to no source.
    #[rstest]
    #[case::an_instrument_s_custom_data(vec![imbalances("GCZ26.GLBX"), imbalances("GC.GLBX")])]
    #[case::a_custom_series_of_no_instrument(vec![signal()])]
    #[tokio::test]
    async fn a_fill_hands_a_custom_series_of_a_type_with_no_row_to_no_source(
        #[case] custom: Vec<CustomSeries>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, to) = (Some(at(FILL_START).into()), Some(at(FILL_END).into()));
        catalog
            .write_to_parquet(&[trade("GCZ26.GLBX")], from, to, None)
            .unwrap();
        let requirements = DataRequirements {
            custom,
            ..ticks(&[(NautilusDataType::TradeTick, "GCZ26.GLBX")])
        };

        fill_history(
            &requirements,
            at(FILL_START),
            at(FILL_END),
            &history_args(),
            dir.path(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_fill_with_an_instrument_of_ib_connects_ib() {
        let dir = tempfile::tempdir().unwrap();

        let err = fill_history(
            &reading(&["SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"]),
            at(FILL_START),
            at(FILL_END),
            &history_args(),
            dir.path(),
        )
        .await
        .unwrap_err();

        assert!(format!("{err:#}").contains(FILL_END), "{err:#}");
    }

    /// A non-UTF-8 catalog path fails the fill at its first step, before IB would refuse the end.
    #[tokio::test]
    async fn the_node_s_fill_runs_before_ib_connects() {
        let dir = tempfile::tempdir().unwrap();
        let catalog_dir = dir.path().join(OsStr::from_bytes(b"not-utf8-\xFF"));

        // The catalog opens its path after converting it to UTF-8
        std::fs::create_dir(catalog_dir.to_string_lossy().as_ref()).unwrap();
        let catalog = ParquetDataCatalog::new(&catalog_dir, None, None, None, None);
        store_okx_definition(&catalog, SWAP);
        let requirements = ticks(&[
            (NautilusDataType::TradeTick, SWAP),
            (NautilusDataType::TradeTick, "SXR8.IBIS"),
        ]);

        let err = fill_history(
            &requirements,
            at(FILL_START),
            at(FILL_END),
            &history_args(),
            &catalog_dir,
        )
        .await
        .unwrap_err();

        // `Path::display` replaces the invalid byte
        assert!(format!("{err:#}").contains("not-utf8-\u{FFFD}"), "{err:#}");
    }

    #[tokio::test]
    async fn a_custom_series_its_type_s_source_does_not_serve_fails_the_fill_with_its_answer_before_anything_connects()
     {
        let dir = tempfile::tempdir().unwrap();
        let observations = lse_rows("macro_observation", "US.core_inflation_rate");
        let requirements = DataRequirements {
            custom: vec![observations],
            ..ticks(&[(NautilusDataType::TradeTick, "SXR8.IBIS")])
        };

        let err = fill_history(
            &requirements,
            at(FILL_START),
            at(FILL_END),
            &history_args(),
            dir.path(),
        )
        .await
        .unwrap_err();

        let err = format!("{err:#}");
        assert!(
            err.contains("macro_observation US.core_inflation_rate")
                && err.contains("macro_release")
                && !err.contains(IB_REFUSES),
            "{err}"
        );
    }

    fn signal() -> CustomSeries {
        CustomSeries::new("Signal", "GOLD-SIGNAL".to_string(), None).unwrap()
    }

    #[rstest]
    #[case::lse_s_rows(
        lse_rows("bond_yields", "US10Y"),
        Some(HistorySource::Lse),
        Some("LSE")
    )]
    #[case::a_type_with_no_row(signal(), None, None)]
    fn a_custom_series_is_served_by_its_type_s_source_and_has_its_live_data_client(
        #[case] series: CustomSeries,
        #[case] source: Option<HistorySource>,
        #[case] client: Option<&str>,
    ) {
        assert_eq!(serves_custom(&series).unwrap(), source);
        assert_eq!(custom_data_client(&series), client.map(ClientId::from));
    }

    #[rstest]
    #[case::okx_s_quotes(
        ticks(&[
            (NautilusDataType::QuoteTick, SWAP),
            (NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
            (NautilusDataType::TradeTick, "SXR8.IBIS"),
        ]),
        SWAP
    )]
    #[case::a_ctrader_broker_s_bars(
        reading(&[
            "XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL",
            "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL",
        ]),
        "XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL"
    )]
    #[tokio::test]
    async fn a_series_its_venue_does_not_serve_fails_the_fill_before_anything_connects(
        #[case] requirements: DataRequirements,
        #[case] refused: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();

        let err = fill_history(
            &requirements,
            at(FILL_START),
            at(FILL_END),
            &history_args(),
            dir.path(),
        )
        .await
        .unwrap_err();

        let err = format!("{err:#}");
        assert!(err.contains(refused) && !err.contains(IB_REFUSES), "{err}");
    }

    #[tokio::test]
    async fn a_fill_of_small_ib_bars_older_than_six_months_fails_before_ib_connects() {
        let dir = tempfile::tempdir().unwrap();
        let bars = "SXR8.IBIS-5-SECOND-LAST-EXTERNAL";

        let err = fill_history(
            &reading(&[bars]),
            at("2020-01-01T00:00:00Z"),
            at(FILL_END),
            &history_args(),
            dir.path(),
        )
        .await
        .unwrap_err();

        let err = format!("{err:#}");
        assert!(err.contains(bars) && err.contains("six months"), "{err}");
    }

    fn gold_slices(venue: &str) -> (Vec<InstrumentId>, Vec<SeriesSlice>) {
        let requirements = ticks(&[(
            NautilusDataType::QuoteTick,
            format!("XAUUSD.{venue}").as_str(),
        )]);
        (
            requirements.instrument_ids(),
            requirements.slices(at(FILL_START), at(FILL_END)),
        )
    }

    #[rstest]
    fn a_ctrader_fill_asks_what_the_catalog_lacks_in_tick_requests_and_a_definition_whole() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (ids, slices) = gold_slices(pepperstone::VENUE);
        let mut node = NodeFill::new();

        add_ctrader_fill(
            &mut node,
            &catalog,
            || Ok(demo_connection(pepperstone::BROKER)),
            ids,
            &slices,
        )
        .unwrap();

        let asked = |data| FillRequest {
            data,
            from: at(FILL_START),
            to: at(FILL_END),
        };
        let id = InstrumentId::from("XAUUSD.PEPPERSTONE");
        assert_eq!(
            node.asked(),
            [
                (asked(FillData::Instrument(id)), None),
                (
                    asked(FillData::Quotes(id)),
                    Some(ctrader::TICK_REQUEST_SPAN)
                ),
            ]
        );
    }

    #[rstest]
    fn a_ctrader_fill_on_a_complete_catalog_asks_nothing_and_reads_no_connection() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        store_quotes(&catalog, &[quote("XAUUSD.PEPPERSTONE")]);
        let (ids, slices) = gold_slices(pepperstone::VENUE);
        let mut node = NodeFill::new();

        add_ctrader_fill(
            &mut node,
            &catalog,
            || panic!("the connection was read with nothing missing"),
            ids,
            &slices,
        )
        .unwrap();

        assert_eq!(node.asked(), []);
    }

    /// GC.GLBX rolls from GCQ26 to GCZ26 at 00:00 UTC on 29 July 2026.
    #[rstest]
    fn each_history_source_takes_the_slices_of_its_own_contracts() {
        let requirements = ticks(&[
            (NautilusDataType::TradeTick, "GC.GLBX"),
            (NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
            (NautilusDataType::QuoteTick, "XAUUSD.PEPPERSTONE"),
            (NautilusDataType::TradeTick, "BTC-USDT-SWAP.OKX"),
            (NautilusDataType::TradeTick, "SXR8.IBIS"),
            (NautilusDataType::TradeTick, "GOLD-FUTURES.LSEDGE"),
        ]);
        let slices = requirements.slices(at("2026-07-28T00:00:00Z"), at("2026-07-29T12:00:00Z"));
        let contracts = |source| {
            of_source(&slices, source)
                .iter()
                .map(|slice| slice.series.instrument_id().to_string())
                .collect::<Vec<_>>()
        };

        assert_eq!(
            contracts(HistorySource::Lse),
            ["GCQ26.GLBX", "GCZ26.GLBX", "GOLD-FUTURES.LSEDGE"]
        );
        assert_eq!(
            contracts(HistorySource::CTrader(ic::BROKER)),
            ["XAUUSD.ICMARKETS"]
        );
        assert_eq!(
            contracts(HistorySource::CTrader(pepperstone::BROKER)),
            ["XAUUSD.PEPPERSTONE"]
        );
        assert_eq!(contracts(HistorySource::Okx), ["BTC-USDT-SWAP.OKX"]);
        assert_eq!(contracts(HistorySource::Ib), ["SXR8.IBIS"]);
    }

    #[rstest]
    #[case::icmarkets("ICMARKETS", true)]
    #[case::okx("OKX", true)]
    #[case::glbx("GLBX", true)]
    #[case::ibis("IBIS", true)]
    #[case::lsedge("LSEDGE", false)]
    fn every_venue_has_a_broker_but_lsedge(#[case] venue: &str, #[case] has: bool) {
        assert_eq!(has_broker(Venue::new(venue)), has);
    }

    fn live_args() -> LiveVenueArgs {
        LiveVenueArgs {
            ib: ib::IbLiveArgs {
                account: None,
                client_id: 1,
            },
        }
    }

    /// Returns the error of a live start that reads `read` and an OKX swap's quotes.
    async fn refused_at_start(read: DataRequirements) -> String {
        let requirements = DataRequirements {
            ticks: [
                read.ticks,
                ticks(&[(NautilusDataType::QuoteTick, SWAP)]).ticks,
            ]
            .concat(),
            ..read
        };
        let Err(e) = live_clients(
            &requirements,
            &requirements.instrument_ids(),
            &live_args(),
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up clients for {requirements:?}");
        };
        format!("{e:#}")
    }

    #[rstest]
    #[case::the_last_bars_a_composite_is_built_from(
        reading(&["XAUUSD.PEPPERSTONE-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"]),
        "XAUUSD.PEPPERSTONE-1-MINUTE-LAST-EXTERNAL"
    )]
    #[case::its_bid_bars(
        reading(&["XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL"]),
        "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL"
    )]
    #[case::bars_built_from_its_trades(
        reading(&["XAUUSD.PEPPERSTONE-1-HOUR-LAST-INTERNAL"]),
        "XAUUSD.PEPPERSTONE"
    )]
    #[case::its_trades(
        ticks(&[(NautilusDataType::TradeTick, "XAUUSD.ICMARKETS")]),
        "XAUUSD.ICMARKETS"
    )]
    #[tokio::test]
    async fn a_run_reading_a_ctrader_series_but_its_quotes_is_refused_naming_it_before_anything_connects(
        #[case] read: DataRequirements,
        #[case] series: &str,
    ) {
        let err = refused_at_start(read).await;

        assert!(err.contains(series), "{err}");
    }

    /// No broker trades LSEDGE, so it keeps Nautilus' venue default.
    #[rstest]
    #[case::icmarkets("ICMARKETS", OmsType::Hedging)]
    #[case::pepperstone("PEPPERSTONE", OmsType::Hedging)]
    #[case::glbx("GLBX", OmsType::Netting)]
    #[case::okx("OKX", OmsType::Netting)]
    #[case::ibis("IBIS", OmsType::Netting)]
    #[case::cme("CME", OmsType::Netting)]
    #[case::nasdaq("NASDAQ", OmsType::Netting)]
    #[case::lseetf("LSEETF", OmsType::Netting)]
    #[case::idealpro("IDEALPRO", OmsType::Netting)]
    #[case::binance("BINANCE", OmsType::Netting)]
    #[case::lsedge("LSEDGE", OmsType::Unspecified)]
    fn each_venue_keeps_positions_as_the_account_of_its_live_broker_does(
        #[case] venue: &str,
        #[case] kept: OmsType,
    ) {
        assert_eq!(oms_type(Venue::new(venue)), kept);
    }

    #[rstest]
    #[case::icmarkets("ICMARKETS", EquitySource::Portfolio)]
    #[case::pepperstone("PEPPERSTONE", EquitySource::Portfolio)]
    #[case::okx("OKX", EquitySource::Portfolio)]
    #[case::glbx("GLBX", EquitySource::BalanceTotals)]
    #[case::ibis("IBIS", EquitySource::BalanceTotals)]
    #[case::idealpro("IDEALPRO", EquitySource::BalanceTotals)]
    #[case::lsedge("LSEDGE", EquitySource::Portfolio)]
    fn each_venue_reads_its_equity_as_its_live_broker_reports_it(
        #[case] venue: &str,
        #[case] source: EquitySource,
    ) {
        assert_eq!(equity_source(Venue::new(venue)), source);
    }

    #[rstest]
    #[case::icmarkets("ICMARKETS", true)]
    #[case::pepperstone("PEPPERSTONE", true)]
    #[case::okx("OKX", false)]
    #[case::glbx("GLBX", false)]
    #[case::ibis("IBIS", false)]
    #[case::idealpro("IDEALPRO", false)]
    #[case::lsedge("LSEDGE", false)]
    fn each_venue_has_a_base_currency_as_the_account_of_its_live_broker_has(
        #[case] venue: &str,
        #[case] based: bool,
    ) {
        assert_eq!(has_base_currency(Venue::new(venue)), based);
    }

    #[rstest]
    #[case::icmarkets("EURUSD.ICMARKETS", quotes_of("EURUSD.ICMARKETS"))]
    #[case::pepperstone("EURUSD.PEPPERSTONE", quotes_of("EURUSD.PEPPERSTONE"))]
    #[case::idealpro("EUR/USD.IDEALPRO", bid_and_ask_bars_of("EUR/USD.IDEALPRO"))]
    fn each_venue_s_pairs_are_read_in_the_first_form_its_history_serves(
        #[case] pair: &str,
        #[case] read: Vec<Series>,
    ) {
        assert_eq!(rate_series(InstrumentId::from(pair)).unwrap(), read);
    }

    fn quotes_of(pair: &str) -> Vec<Series> {
        vec![Series::Ticks(TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(pair),
        })]
    }

    fn bid_and_ask_bars_of(pair: &str) -> Vec<Series> {
        ["BID", "ASK"]
            .map(|side| Series::Bars(BarType::from(format!("{pair}-1-MINUTE-{side}-EXTERNAL"))))
            .to_vec()
    }

    #[rstest]
    fn a_pair_whose_venue_serves_neither_form_has_no_rate_series() {
        let err = rate_series(InstrumentId::from("USDC-USDT.OKX"))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("USDC-USDT.OKX") && err.contains("OKX's history"),
            "{err}"
        );
    }

    fn bars_of(bar_type: &str) -> CatalogSeries {
        CatalogSeries::Bars(BarType::from(bar_type))
    }

    fn ticks_of(data: NautilusDataType, instrument_id: &str) -> CatalogSeries {
        CatalogSeries::Ticks(TickSeries {
            data,
            instrument_id: InstrumentId::from(instrument_id),
        })
    }

    fn assert_serves(served: &[CatalogSeries], refused: &[CatalogSeries]) {
        for source in served {
            if let Err(e) = serves(source) {
                panic!("{source} refused: {e:#}");
            }
        }

        for source in refused {
            let Err(e) = serves(source) else {
                panic!("{source} served");
            };
            let (_, identifier) = source.key();
            assert!(format!("{e:#}").contains(&identifier), "{source}: {e:#}");
        }
    }

    /// Nautilus' OKX data client requests trades and candles alone (`request_trades`,
    #[rstest]
    fn okx_s_history_serves_its_trades_and_its_candles() {
        use NautilusDataType as D;
        assert_serves(
            &[
                ticks_of(D::TradeTick, SWAP),
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-5-MINUTE-LAST-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-4-HOUR-LAST-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-1-DAY-LAST-EXTERNAL"),
            ],
            &[
                ticks_of(D::QuoteTick, SWAP),
                ticks_of(D::OrderBookDepth, SWAP),
                ticks_of(D::OrderBookDelta, SWAP),
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-MID-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-BID-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-MARK-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-4-MINUTE-LAST-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-LAST-INTERNAL"),
            ],
        );
    }

    #[rstest]
    fn lse_s_history_serves_what_its_client_asks_lse_for() {
        use NautilusDataType as D;
        assert_serves(
            &[
                ticks_of(D::QuoteTick, "GCZ26.GLBX"),
                ticks_of(D::TradeTick, "GCZ26.GLBX"),
                ticks_of(D::OrderBookDepth, "GCZ26.GLBX"),
                ticks_of(D::OrderBookDelta, "NVDA.XNAS"),
                ticks_of(D::TradeTick, "GOLD-FUTURES.LSEDGE"),
                ticks_of(D::QuoteTick, "SPX-INDICES.LSEDGE"),
                ticks_of(D::TradeTick, "AAPL  261120C00355000.OPRA"),
                bars_of("GOLD-FUTURES.LSEDGE-1-MINUTE-LAST-EXTERNAL"),
                bars_of("AAPL  261120C00355000.OPRA-5-MINUTE-LAST-EXTERNAL"),
            ],
            &[
                ticks_of(D::TradeTick, "SPX-INDICES.LSEDGE"),
                bars_of("GCZ26.GLBX-1-MINUTE-LAST-EXTERNAL"),
                bars_of("NVDA.XNAS-1-MINUTE-LAST-EXTERNAL"),
                bars_of("GOLD-FUTURES.LSEDGE-1-DAY-LAST-EXTERNAL"),
                bars_of("GOLD-FUTURES.LSEDGE-1-MINUTE-MID-EXTERNAL"),
                bars_of("GOLD-FUTURES.LSEDGE-4-MINUTE-LAST-EXTERNAL"),
            ],
        );
    }

    fn assert_streams(streamed: &[CatalogSeries], refused: &[CatalogSeries]) {
        for source in streamed {
            if let Err(e) = streams(source) {
                panic!("{source} refused: {e:#}");
            }
        }

        for source in refused {
            let Err(e) = streams(source) else {
                panic!("{source} streamed");
            };
            let (_, identifier) = source.key();
            assert!(format!("{e:#}").contains(&identifier), "{source}: {e:#}");
        }
    }

    /// Nautilus' IB data client streams quotes, trades and bars (`subscribe_quotes`,
    /// `subscribe_trades`, `subscribe_bars`) and L2_MBP book deltas.
    #[rstest]
    fn ib_s_client_streams_its_quotes_trades_bars_and_book_deltas() {
        use NautilusDataType as D;
        assert_streams(
            &[
                ticks_of(D::QuoteTick, "SXR8.IBIS"),
                ticks_of(D::QuoteTick, "EUR/USD.IDEALPRO"),
                ticks_of(D::TradeTick, "ESZ26.CME"),
                ticks_of(D::OrderBookDelta, "ESZ26.CME"),
                ticks_of(D::OrderBookDelta, "EUR/USD.IDEALPRO"),
                bars_of("SXR8.IBIS-1-MINUTE-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-5-SECOND-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-30-SECOND-BID-EXTERNAL"),
                bars_of("EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL"),
                bars_of("EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL"),
                bars_of("ESZ26.CME-8-HOUR-MID-EXTERNAL"),
                bars_of("SXR8.IBIS-1-MONTH-LAST-EXTERNAL"),
                bars_of("BTC/USD.PAXOS-1-MINUTE-LAST-EXTERNAL"),
            ],
            &[
                ticks_of(D::TradeTick, "EUR/USD.IDEALPRO"),
                ticks_of(D::TradeTick, "BTC/USD.PAXOS"),
                ticks_of(D::TradeTick, "BTC/USD.ZEROHASH"),
                ticks_of(D::OrderBookDepth, "SXR8.IBIS"),
                bars_of("EUR/USD.IDEALPRO-1-MINUTE-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-SECOND-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-4-MINUTE-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-MINUTE-MARK-EXTERNAL"),
                bars_of("SXR8.IBIS-100-TICK-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-MINUTE-LAST-INTERNAL"),
            ],
        );
    }

    /// IB's refusal of bars of a size it does not stream offers the sizes Nautilus' IB client asks
    /// for (`bar_type_to_ib_bar_size`), except 1 second, which IB does not keep up to date.
    #[rstest]
    fn ib_s_refusal_of_bars_offers_exactly_the_sizes_it_streams() {
        let units = ["SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH"];
        let err = format!(
            "{:#}",
            streams(&bars_of("SXR8.IBIS-10-SECOND-LAST-EXTERNAL")).unwrap_err()
        );

        let offered: Vec<&str> = err
            .split([' ', ',', '(', ')', ':'])
            .filter(|word| {
                word.split_once('-').is_some_and(|(step, unit)| {
                    step.parse::<usize>().is_ok() && units.contains(&unit)
                })
            })
            .collect();

        assert_eq!(
            offered,
            [
                "5-SECOND",
                "15-SECOND",
                "30-SECOND",
                "1-MINUTE",
                "2-MINUTE",
                "3-MINUTE",
                "5-MINUTE",
                "10-MINUTE",
                "15-MINUTE",
                "20-MINUTE",
                "30-MINUTE",
                "1-HOUR",
                "2-HOUR",
                "3-HOUR",
                "4-HOUR",
                "8-HOUR",
                "1-DAY",
                "1-WEEK",
                "1-MONTH",
            ],
            "{err}"
        );
    }

    /// Nautilus' OKX data client streams quotes, trades and trade-priced candles
    /// (`subscribe_quotes`, `subscribe_trades`, `subscribe_bars` over `bar_spec_as_okx_channel`).
    #[rstest]
    fn okx_s_client_streams_its_quotes_trades_candles_and_book() {
        use NautilusDataType as D;
        assert_streams(
            &[
                ticks_of(D::QuoteTick, SWAP),
                ticks_of(D::TradeTick, SWAP),
                ticks_of(D::OrderBookDelta, SWAP),
                ticks_of(D::OrderBookDepth, SWAP),
                bars_of("BTC-USDT-SWAP.OKX-1-SECOND-LAST-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-4-HOUR-LAST-EXTERNAL"),
            ],
            &[
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-MID-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-4-MINUTE-LAST-EXTERNAL"),
                bars_of("BTC-USDT-SWAP.OKX-1-MINUTE-LAST-INTERNAL"),
            ],
        );
    }

    #[rstest]
    #[case::glbx("GLBX", Some(lse_book()))]
    #[case::xnas("XNAS", Some(lse_book()))]
    #[case::opra("OPRA", Some(lse_book()))]
    #[case::lsedge("LSEDGE", Some(lse_book()))]
    #[case::ibis("IBIS", Some(ib_book()))]
    #[case::cme("CME", Some(ib_book()))]
    #[case::idealpro("IDEALPRO", Some(ib_book()))]
    #[case::nasdaq("NASDAQ", Some(ib_book()))]
    #[case::okx("OKX", Some(okx_book()))]
    #[case::icmarkets("ICMARKETS", None)]
    #[case::pepperstone("PEPPERSTONE", None)]
    fn each_venue_s_book_is_in_the_format_its_live_data_client_streams(
        #[case] venue: &str,
        #[case] format: Option<BookFormat>,
    ) {
        assert_eq!(book_format(Venue::new(venue)), format);
    }

    fn lse_book() -> BookFormat {
        BookFormat {
            deltas: BookType::L3_MBO,
            depth: NonZeroUsize::new(10),
        }
    }

    fn ib_book() -> BookFormat {
        BookFormat {
            deltas: BookType::L2_MBP,
            depth: None,
        }
    }

    fn okx_book() -> BookFormat {
        BookFormat {
            deltas: BookType::L2_MBP,
            depth: NonZeroUsize::new(5),
        }
    }

    fn imbalances(identifier: &str) -> CustomSeries {
        CustomSeries::new("Imbalance", identifier.to_string(), None).unwrap()
    }

    /// A history request of each kind a data client takes, with the kind's name and the source it
    /// asks for.
    fn requests_of(id: InstrumentId) -> Vec<(&'static str, CatalogSeries, RequestCommand)> {
        use NautilusDataType as D;
        let (start, end): (Timestamp, Timestamp) = (
            at(FILL_START),
            at(FILL_START) + SignedDuration::from_hours(1),
        );
        let bars = BarType::from(format!("{id}-1-MINUTE-LAST-EXTERNAL").as_str());
        let ticks = |data| {
            CatalogSeries::Ticks(TickSeries {
                data,
                instrument_id: id,
            })
        };
        let ts = UnixNanos::from(end);
        vec![
            (
                "quotes",
                ticks(D::QuoteTick),
                RequestCommand::Quotes(RequestQuotes::new(
                    id,
                    Some(start),
                    Some(end),
                    None,
                    None,
                    UUID4::new(),
                    ts,
                    None,
                )),
            ),
            (
                "trades",
                ticks(D::TradeTick),
                RequestCommand::Trades(RequestTrades::new(
                    id,
                    Some(start),
                    Some(end),
                    None,
                    None,
                    UUID4::new(),
                    ts,
                    None,
                )),
            ),
            (
                "bars",
                CatalogSeries::Bars(bars),
                RequestCommand::Bars(RequestBars::new(
                    bars,
                    Some(start),
                    Some(end),
                    None,
                    None,
                    UUID4::new(),
                    ts,
                    None,
                )),
            ),
            (
                "book deltas",
                ticks(D::OrderBookDelta),
                RequestCommand::BookDeltas(RequestBookDeltas::new(
                    id,
                    Some(start),
                    Some(end),
                    None,
                    None,
                    UUID4::new(),
                    ts,
                    None,
                )),
            ),
            (
                "book depth",
                ticks(D::OrderBookDepth),
                RequestCommand::BookDepth(RequestBookDepth::new(
                    id,
                    Some(start),
                    Some(end),
                    None,
                    None,
                    None,
                    UUID4::new(),
                    ts,
                    None,
                )),
            ),
        ]
    }

    /// Nautilus' `DataClient` default refusal (`does not support`).
    fn unsupported(client: &dyn DataClient, request: RequestCommand) -> bool {
        let answer = match request {
            RequestCommand::Quotes(request) => client.request_quotes(request),
            RequestCommand::Trades(request) => client.request_trades(request),
            RequestCommand::Bars(request) => client.request_bars(request),
            RequestCommand::BookDeltas(request) => client.request_book_deltas(request),
            RequestCommand::BookDepth(request) => client.request_book_depth(request),
            other => panic!("no history request: {other:?}"),
        };
        answer.is_err_and(|e| e.to_string().contains("does not support"))
    }

    fn instrument_ids(ids: &[&str]) -> Vec<InstrumentId> {
        ids.iter().map(|&id| InstrumentId::from(id)).collect()
    }

    const UNREACHABLE: &str = "http://127.0.0.1:1";

    /// Each client is built as its fill builds it and left unconnected. A bar type that a client
    /// takes but `serves` refuses (a price, a width) is not checked.
    fn request_kinds_against_serves(clients: Vec<(NodeClient, Vec<InstrumentId>)>) -> Vec<String> {
        let (data_tx, _data_events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let mut disagreeing = Vec::new();

        for (client, venues) in clients {
            let name = client.factory.name().to_string();
            let created = client
                .factory
                .create(
                    &name,
                    client.config.as_ref(),
                    cache.clone().into(),
                    clock.clone(),
                )
                .unwrap();
            assert!(created.is_disconnected(), "{name}");

            for (i, (kind, _, request)) in requests_of(venues[0]).into_iter().enumerate() {
                let served = venues
                    .iter()
                    .any(|&id| serves(&requests_of(id).swap_remove(i).1).is_ok());
                if unsupported(created.as_ref(), request) == served {
                    disagreeing.push(format!("{name}'s {kind}: served {served}"));
                }
            }
        }
        disagreeing
    }

    /// Pins the kinds of history request that Nautilus' OKX data client implements, which
    /// `okx::serves` copies. They are trades and bars (`crates/adapters/okx/src/data.rs:3005`,
    /// `:3055`). The `DataClient` default refuses every other kind
    /// (`crates/common/src/clients/data.rs:622-699`).
    #[tokio::test]
    async fn drift_okx_s_client_takes_the_kinds_of_request_its_history_serves() {
        let okx = NodeClient {
            factory: Box::new(OKXDataClientFactory::new()),
            config: Box::new(OKXDataClientConfig {
                base_url_http: Some(UNREACHABLE.to_string()),
                ..Default::default()
            }),
        };

        let disagreeing = request_kinds_against_serves(vec![(okx, instrument_ids(&[SWAP]))]);

        assert_eq!(disagreeing, Vec::<String>::new());
    }

    #[tokio::test]
    async fn the_ctrader_and_lse_clients_take_the_kinds_of_request_their_history_serves() {
        let ctrader = ctrader_node_client(
            demo_connection(ic::BROKER),
            instrument_ids(&["XAUUSD.ICMARKETS"]),
        );
        let lse = NodeClient {
            factory: Box::new(LseDataClientFactory::new()),
            config: Box::new(
                LseDataClientConfig::builder()
                    .api_key(SecretString::from(::lse::testing::API_KEY))
                    .cme_api_key(SecretString::from(::lse::testing::CME_API_KEY))
                    .nasdaq_api_key(SecretString::from(::lse::testing::NASDAQ_API_KEY))
                    .base_url_http(UNREACHABLE.to_string())
                    .build(),
            ),
        };

        let disagreeing = request_kinds_against_serves(vec![
            (ctrader, instrument_ids(&["XAUUSD.ICMARKETS"])),
            (
                lse,
                instrument_ids(&[
                    "GOLD-FUTURES.LSEDGE",
                    "GCZ26.GLBX",
                    "NVDA.XNAS",
                    "AAPL  261120C00355000.OPRA",
                ]),
            ),
        ]);

        assert_eq!(disagreeing, Vec::<String>::new());
    }

    #[derive(Debug)]
    struct Subscriber {
        core: StrategyCore,
    }

    nautilus_strategy!(Subscriber);

    impl DataActor for Subscriber {}

    /// Each subscription is captured as the command sent to the data engine. Custom data is not
    /// tried, as it goes by its type (`custom_data_client`, `streams_custom`).
    fn subscriptions_of(
        id: InstrumentId,
    ) -> Vec<(&'static str, CatalogSeries, Option<SubscribeCommand>)> {
        use NautilusDataType as D;
        let bars = BarType::from(format!("{id}-1-MINUTE-LAST-EXTERNAL").as_str());
        let ticks = |data| TickSeries {
            data,
            instrument_id: id,
        };
        let kinds = [
            ("quotes", D::QuoteTick),
            ("trades", D::TradeTick),
            ("book deltas", D::OrderBookDelta),
            ("book depth", D::OrderBookDepth),
        ];
        let (handler, sent) = get_typed_into_message_saving_handler::<DataCommand>(None);
        msgbus::register_data_command_endpoint(
            MessagingSwitchboard::data_engine_queue_execute(),
            handler,
        );
        let mut subscriber = Subscriber {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("SUBSCRIBER-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
        };
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(FILL_START)), true);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        subscriber
            .core
            .register(TraderId::from("HELIX-001"), clock, cache, portfolio)
            .unwrap();
        Subscriptions::new(
            vec![bars],
            kinds.iter().map(|(_, data)| ticks(data.clone())).collect(),
            vec![],
        )
        .start(&mut subscriber, at(FILL_START));
        let mut sent: Vec<SubscribeCommand> = sent
            .get_messages()
            .into_iter()
            .map(|command| match command {
                DataCommand::Subscribe(subscription) => subscription,
                other => panic!("no subscription: {other:?}"),
            })
            .collect();
        let mut take = |of: fn(&SubscribeCommand) -> bool| {
            let at = sent.iter().position(of)?;
            Some(sent.remove(at))
        };
        let mut subscriptions = vec![(
            "bars",
            CatalogSeries::Bars(bars),
            take(|command| matches!(command, SubscribeCommand::Bars(_))),
        )];
        for (kind, data) in kinds {
            let command = match data {
                D::QuoteTick => take(|command| matches!(command, SubscribeCommand::Quotes(_))),
                D::TradeTick => take(|command| matches!(command, SubscribeCommand::Trades(_))),
                D::OrderBookDelta => {
                    take(|command| matches!(command, SubscribeCommand::BookDeltas(_)))
                }
                D::OrderBookDepth => {
                    take(|command| matches!(command, SubscribeCommand::BookDepth(_)))
                }
                other => unreachable!("{other} is no kind of subscription tried"),
            };
            subscriptions.push((kind, CatalogSeries::Ticks(ticks(data)), command));
        }
        assert_eq!(sent, [], "a subscription of no kind tried");
        subscriptions
    }

    /// A subscription that streams nothing answers `Ok` unconnected, as Nautilus' `DataClient`
    /// defaults do after logging "Handler not implemented", or as IB's client does for L3_MBO book
    /// deltas.
    fn streams_none(client: &mut dyn DataClient, subscription: Option<SubscribeCommand>) -> bool {
        let Some(subscription) = subscription else {
            return true;
        };
        let answer = match subscription {
            SubscribeCommand::Quotes(cmd) => client.subscribe_quotes(cmd),
            SubscribeCommand::Trades(cmd) => client.subscribe_trades(cmd),
            SubscribeCommand::Bars(cmd) => client.subscribe_bars(cmd),
            SubscribeCommand::BookDeltas(cmd) => client.subscribe_book_deltas(cmd),
            SubscribeCommand::BookDepth(cmd) => client.subscribe_book_depth(cmd),
            other => panic!("no subscription of a strategy: {other:?}"),
        };
        answer.is_ok()
    }

    /// Each client is created as the node creates it and left unconnected. A series that a client
    /// takes but `streams` refuses (a width, a currency pair's trades) is not checked.
    #[tokio::test]
    async fn the_ctrader_and_lse_clients_take_the_kinds_of_subscription_they_stream() {
        let (data_tx, _data_events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let ids = |ids: &[&str]| {
            ids.iter()
                .map(|&id| InstrumentId::from(id))
                .collect::<Vec<_>>()
        };
        let lse_venues = ids(&[
            "GOLD-FUTURES.LSEDGE",
            "GCZ26.GLBX",
            "NVDA.XNAS",
            "AAPL  261120C00355000.OPRA",
        ]);
        let [lse, _ib] = <[Clients; 2]>::try_from(lse_and_ib_clients(&lse_venues))
            .unwrap_or_else(|all| panic!("{} clients", all.len()));
        let ctrader = ctrader_clients(demo_connection(ic::BROKER), ids(&["XAUUSD.ICMARKETS"]));
        let mut drift = Vec::new();

        for (client, venues) in [(ctrader, ids(&["XAUUSD.ICMARKETS"])), (lse, lse_venues)] {
            let name = client.data_factory.name().to_string();
            let mut created = client
                .data_factory
                .create(
                    &name,
                    client.data_config.as_ref(),
                    cache.clone().into(),
                    clock.clone(),
                )
                .unwrap();
            assert!(created.is_disconnected(), "{name}");
            for (i, (kind, _, subscription)) in subscriptions_of(venues[0]).into_iter().enumerate()
            {
                let streamed = venues
                    .iter()
                    .any(|&id| streams(&subscriptions_of(id).swap_remove(i).1).is_ok());
                if streams_none(created.as_mut(), subscription) == streamed {
                    drift.push(format!("{name}'s {kind}: streamed {streamed}"));
                }
            }
        }

        assert_eq!(drift, Vec::<String>::new());
    }

    #[rstest]
    #[case::icmarkets("ICMARKETS", true)]
    #[case::pepperstone("PEPPERSTONE", true)]
    #[case::okx("OKX", true)]
    #[case::glbx("GLBX", false)]
    #[case::ibis("IBIS", false)]
    #[case::idealpro("IDEALPRO", false)]
    #[case::lsedge("LSEDGE", false)]
    fn each_venue_takes_reduce_only_and_post_only_orders_as_its_live_broker_does(
        #[case] venue: &str,
        #[case] takes: bool,
    ) {
        assert_eq!(takes_reduce_only_and_post_only(Venue::new(venue)), takes);
    }

    #[rstest]
    #[case::an_lsedge_series("GOLD-FUTURES.LSEDGE", lsedge_row())]
    #[case::another_lsedge_series("US10Y-FUTURES.LSEDGE", lsedge_row())]
    #[case::an_okx_swap("BTC-USDT-SWAP.OKX", okx_row())]
    #[case::ic_s_gold("XAUUSD.ICMARKETS", ctrader_row(ic::BROKER))]
    #[case::pepperstone_s_gold("XAUUSD.PEPPERSTONE", ctrader_row(pepperstone::BROKER))]
    #[case::a_glbx_contract("GCZ26.GLBX", exchange_row())]
    #[case::a_glbx_root("GC.GLBX", exchange_row())]
    #[case::a_databento_continuous_symbol("GC.v.0.GLBX", exchange_row())]
    #[case::an_xnas_stock("NVDA.XNAS", exchange_row())]
    #[case::an_xnas_stock_with_a_space("BRK B.XNAS", exchange_row())]
    #[case::an_opra_contract("AAPL  261120C00355000.OPRA", exchange_row())]
    #[case::an_ibis_etf("SXR8.IBIS", ib_row())]
    #[case::a_cme_contract("ESZ26.CME", ib_row())]
    #[case::a_comex_contract("GCZ26.XCEC", ib_row())]
    #[case::an_idealpro_pair("EUR/USD.IDEALPRO", ib_row())]
    #[case::a_venue_of_no_row("BTC-USDT-SWAP.BINANCE", ib_row())]
    fn each_instrument_routes_to_the_row_of_its_venue(#[case] id: &str, #[case] row: Row) {
        assert_eq!(Row::of(InstrumentId::from(id).venue), row);
    }

    fn okx_row() -> Row {
        Row {
            definitions: DefinitionsSource::Okx,
            history: HistorySource::Okx,
            live_data: LiveDataSource::Okx,
            broker: Some(Broker::Okx),
        }
    }

    fn ctrader_row(broker: CTraderBroker) -> Row {
        Row {
            definitions: DefinitionsSource::CTrader(broker),
            history: HistorySource::CTrader(broker),
            live_data: LiveDataSource::CTrader(broker),
            broker: Some(Broker::CTrader(broker)),
        }
    }

    /// A row belongs to the venue, so a GLBX root and a Databento continuous symbol take the GLBX
    /// row too, though LSE's client refuses the latter, as it serves dated contracts alone.
    fn exchange_row() -> Row {
        Row {
            definitions: DefinitionsSource::Databento,
            history: HistorySource::Lse,
            live_data: LiveDataSource::Lse,
            broker: Some(Broker::Ib),
        }
    }

    fn ib_row() -> Row {
        Row {
            definitions: DefinitionsSource::Ib,
            history: HistorySource::Ib,
            live_data: LiveDataSource::Ib,
            broker: Some(Broker::Ib),
        }
    }

    fn lsedge_row() -> Row {
        Row {
            definitions: DefinitionsSource::Catalog,
            history: HistorySource::Lse,
            live_data: LiveDataSource::Lse,
            broker: None,
        }
    }

    #[rstest]
    fn each_instrument_goes_to_the_clients_of_its_live_data_and_of_its_broker() {
        let routes = LiveRoutes::of(
            &[
                InstrumentId::from("GCZ26.GLBX"),
                InstrumentId::from("GOLD-FUTURES.LSEDGE"),
                InstrumentId::from("SXR8.IBIS"),
                InstrumentId::from("NVDA.XNAS"),
                InstrumentId::from("AAPL  261120C00355000.OPRA"),
                InstrumentId::from("BTC-USDT-SWAP.OKX"),
                InstrumentId::from("XAUUSD.ICMARKETS"),
            ],
            &[],
        );

        assert_eq!(
            routes,
            LiveRoutes {
                okx: vec![InstrumentId::from("BTC-USDT-SWAP.OKX")],
                ctrader: vec![(ic::BROKER, InstrumentId::from("XAUUSD.ICMARKETS"))],
                lse: vec![
                    InstrumentId::from("GCZ26.GLBX"),
                    InstrumentId::from("GOLD-FUTURES.LSEDGE"),
                    InstrumentId::from("NVDA.XNAS"),
                    InstrumentId::from("AAPL  261120C00355000.OPRA")
                ],
                ib: vec![
                    InstrumentId::from("GCZ26.GLBX"),
                    InstrumentId::from("SXR8.IBIS"),
                    InstrumentId::from("NVDA.XNAS"),
                    InstrumentId::from("AAPL  261120C00355000.OPRA")
                ],
                ib_data_venues: vec![Venue::new("IBIS")],
                custom: vec![],
            }
        );
    }

    #[rstest]
    fn a_custom_series_routes_its_type_s_live_data_source_to_the_node_with_no_instrument_of_it() {
        let routes = LiveRoutes::of(
            &[InstrumentId::from("SXR8.IBIS")],
            &[
                lse_rows("bond_yields", "US10Y"),
                lse_rows("bond_yields", "US02Y"),
                signal(),
            ],
        );

        assert_eq!(
            routes,
            LiveRoutes {
                ib: vec![InstrumentId::from("SXR8.IBIS")],
                ib_data_venues: vec![Venue::new("IBIS")],
                custom: vec![LiveDataSource::Lse],
                ..LiveRoutes::default()
            }
        );
    }

    #[tokio::test]
    async fn a_run_reading_a_custom_series_alone_has_lse_s_client_on_its_node_streaming_no_instrument()
     {
        let routes = LiveRoutes::of(&[], &[lse_rows("bond_yields", "US10Y")]);

        let clients = routes
            .clients(&live_args(), Path::new("catalog"))
            .await
            .unwrap();

        let [mut lse] =
            <[Clients; 1]>::try_from(clients).unwrap_or_else(|all| panic!("{} clients", all.len()));
        assert_eq!(lse.data_factory.name(), "LSE");
        assert!(lse.exec.is_none());
        assert_eq!(
            (lse.data_routing.default, lse.data_routing.venues.clone()),
            (false, Some(vec![]))
        );
        let config = lse
            .data_config
            .as_any()
            .downcast_ref::<LseDataClientConfig>()
            .unwrap()
            .clone();
        assert_eq!(config.stream_ids, []);
        lse.data_config = Box::new(LseDataClientConfig {
            api_key: Some(SecretString::from(::lse::testing::API_KEY)),
            ..config
        });
        let mut node = live_node(vec![lse]).unwrap();
        let on_node = node
            .kernel()
            .data_engine
            .borrow_mut()
            .get_client(Some(&ClientId::from("LSE")), None)
            .map(|client| client.client_id);
        node.dispose();
        assert_eq!(on_node, Some(ClientId::from("LSE")));
    }

    #[rstest]
    #[case::glbx(&["GCZ26.GLBX", "GCG27.GLBX"], "GLBX")]
    #[case::opra(&["AAPL  261120C00355000.OPRA", "AAPL  261120P00355000.OPRA"], "OPRA")]
    fn lse_s_client_streams_exchange_contracts_routed_their_venue_with_no_execution_client(
        #[case] ids: &[&str],
        #[case] venue: &str,
    ) {
        let clients = lse::live_clients(&instrument_ids(ids));

        assert_eq!(clients.data_factory.name(), "LSE");
        assert!(clients.exec.is_none());
        assert_eq!(
            (clients.data_routing.default, clients.data_routing.venues),
            (false, Some(vec![venue.to_string()]))
        );
        let config = clients
            .data_config
            .as_any()
            .downcast_ref::<LseDataClientConfig>()
            .unwrap();
        assert_eq!(config.stream_ids, instrument_ids(ids));
    }

    /// The node has no streaming, and its writer drops through Nautilus' runtime when a test's
    /// thread ends.
    fn live_node(clients: Vec<Clients>) -> anyhow::Result<LiveNode> {
        let config = LiveNodeConfig {
            environment: Environment::Live,
            trader_id: TraderId::from("HELIX-001"),
            ..Default::default()
        };
        crate::live::node::add_clients(LiveNodeBuilder::from_config(config)?, clients)?.build()
    }

    /// LSE's has the test keys, and IB's uses a gateway that is not started.
    fn lse_and_ib_clients(ids: &[InstrumentId]) -> Vec<Clients> {
        let routes = LiveRoutes::of(ids, &[]);
        let mut lse = lse::live_clients(&routes.lse);
        let streamed = lse
            .data_config
            .as_any()
            .downcast_ref::<LseDataClientConfig>()
            .unwrap()
            .clone();
        lse.data_config = Box::new(LseDataClientConfig {
            api_key: Some(SecretString::from(::lse::testing::API_KEY)),
            cme_api_key: Some(SecretString::from(::lse::testing::CME_API_KEY)),
            nasdaq_api_key: Some(SecretString::from(::lse::testing::NASDAQ_API_KEY)),
            ..streamed
        });
        vec![
            lse,
            ib::offline_clients(&routes.ib).into_node_clients(&routes.ib_data_venues),
        ]
    }

    fn data_clients(node: &LiveNode, venue: &str) -> (Option<ClientId>, Option<ClientId>) {
        let mut engine = node.kernel().data_engine.borrow_mut();
        (
            engine
                .get_client(None, Some(&Venue::new(venue)))
                .map(|client| client.client_id),
            engine.get_client(None, None).map(|client| client.client_id),
        )
    }

    fn order_client(node: &LiveNode, instrument_id: &str) -> Vec<ClientId> {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(InstrumentId::from(instrument_id))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .build();
        node.kernel()
            .exec_engine()
            .borrow()
            .get_clients_for_orders(&[order])
            .iter()
            .map(|client| client.client_id())
            .collect()
    }

    /// The builder registers LSE's and IB's clients in no set order (`LiveNodeBuilder`'s clients
    /// are a `HashMap`). The first client on no venue that it registers becomes the default
    /// (`DataEngine::register_client`), and both are on no venue.
    fn assert_routed_whichever_registers_first(ids: &[&str], routed: &[(&str, &str, &str, &str)]) {
        let ids: Vec<InstrumentId> = ids.iter().map(|&id| InstrumentId::from(id)).collect();
        let mut first = HashSet::new();
        for _ in 0..64 {
            let mut node = live_node(lse_and_ib_clients(&ids))
                .unwrap_or_else(|e| panic!("the node of {ids:?} not built: {e:#}"));
            let clients: Vec<_> = routed
                .iter()
                .map(|&(venue, instrument_id, _, _)| {
                    (
                        data_clients(&node, venue),
                        order_client(&node, instrument_id),
                    )
                })
                .collect();
            node.dispose();

            for (&(venue, _, data, orders), ((client, default), order_clients)) in
                routed.iter().zip(clients)
            {
                assert_eq!(client, Some(ClientId::from(data)), "{venue}'s data");
                assert_eq!(order_clients, [ClientId::from(orders)], "{venue}'s orders");
                first.extend(default);
            }

            if first.len() == 2 {
                return;
            }
        }
        panic!("one client registered first in 64 builds: {first:?}");
    }

    #[rstest]
    #[case::an_exchange_run(
        &["GCZ26.GLBX", "GCG27.GLBX", "NVDA.XNAS", "AAPL  261120C00355000.OPRA"],
        &[
            ("GLBX", "GCZ26.GLBX", "LSE", "IB"),
            ("XNAS", "NVDA.XNAS", "LSE", "IB"),
            ("OPRA", "AAPL  261120C00355000.OPRA", "LSE", "IB"),
        ]
    )]
    #[case::an_lsedge_and_ib_run(
        &["GOLD-FUTURES.LSEDGE", "SXR8.IBIS"],
        &[
            ("LSEDGE", "GOLD-FUTURES.LSEDGE", "LSE", "IB"),
            ("IBIS", "SXR8.IBIS", "IB", "IB"),
        ]
    )]
    #[tokio::test]
    async fn a_run_s_node_routes_each_venue_s_data_and_orders_to_its_clients_whichever_registers_first(
        #[case] ids: &[&str],
        #[case] routed: &[(&str, &str, &str, &str)],
    ) {
        assert_routed_whichever_registers_first(ids, routed);
    }

    #[rstest]
    fn the_table_s_venues_are_their_adapters() {
        assert_eq!(Row::of(*OKX_VENUE).history, HistorySource::Okx);
        assert_eq!(
            Row::of(Venue::GLBX()).definitions,
            DefinitionsSource::Databento
        );
        assert_eq!(Row::of(*LSEDGE_VENUE).history, HistorySource::Lse);
    }

    #[tokio::test]
    async fn a_run_reading_lsedge_instruments_streams_them_from_lse_routed_lsedge_with_no_broker() {
        let requirements = ticks(&[
            (NautilusDataType::QuoteTick, "GOLD-FUTURES.LSEDGE"),
            (NautilusDataType::QuoteTick, "US10Y-FUTURES.LSEDGE"),
        ]);

        let clients = live_clients(
            &requirements,
            &requirements.instrument_ids(),
            &live_args(),
            Path::new("catalog"),
        )
        .await
        .unwrap();
        let [clients] =
            <[Clients; 1]>::try_from(clients).unwrap_or_else(|all| panic!("{} clients", all.len()));

        assert_eq!(clients.data_factory.name(), "LSE");
        assert!(clients.exec.is_none());
        assert_eq!(
            (clients.data_routing.default, clients.data_routing.venues),
            (false, Some(vec!["LSEDGE".to_string()]))
        );
        let config = clients
            .data_config
            .as_any()
            .downcast_ref::<LseDataClientConfig>()
            .unwrap();
        assert_eq!(
            config.stream_ids,
            [
                InstrumentId::from("GOLD-FUTURES.LSEDGE"),
                InstrumentId::from("US10Y-FUTURES.LSEDGE"),
            ]
        );
    }

    #[rstest]
    #[case::okx("BTC-USDT-SWAP.OKX", "OKX")]
    #[case::icmarkets("XAUUSD.ICMARKETS", "ICMARKETS")]
    #[case::pepperstone("XAUUSD.PEPPERSTONE", "PEPPERSTONE")]
    #[case::glbx("GCZ26.GLBX", "IB")]
    #[case::xnas("NVDA.XNAS", "IB")]
    #[case::opra("AAPL  261120C00355000.OPRA", "IB")]
    #[case::ibis("SXR8.IBIS", "IB")]
    #[case::lsedge("GOLD-FUTURES.LSEDGE", "LSE")]
    fn a_definition_is_asked_of_the_venue_s_broker_or_else_of_its_live_data_client(
        #[case] id: &str,
        #[case] client: &str,
    ) {
        assert_eq!(
            live_definitions_client(InstrumentId::from(id).venue),
            ClientId::from(client)
        );
        let lse = lse::live_clients(&[InstrumentId::from("GOLD-FUTURES.LSEDGE")]);
        assert_eq!(lse.data_factory.name(), "LSE");
    }

    /// LSE streams none of these series, so each is refused before anything connects, where OKX's
    /// clients would fail on their missing environment.
    #[rstest]
    #[case::a_glbx_contract_s_external_bars(
        reading(&["GCZ26.GLBX-1-MINUTE-LAST-EXTERNAL"]),
        "GCZ26.GLBX-1-MINUTE-LAST-EXTERNAL"
    )]
    #[case::an_lsedge_series_trades(
        ticks(&[(NautilusDataType::TradeTick, "GOLD-FUTURES.LSEDGE")]),
        "GOLD-FUTURES.LSEDGE"
    )]
    #[case::bars_built_from_an_lsedge_series_trades(
        reading(&["GOLD-FUTURES.LSEDGE-1-MINUTE-LAST-INTERNAL"]),
        "GOLD-FUTURES.LSEDGE"
    )]
    #[case::an_option_s_quotes(
        ticks(&[(NautilusDataType::QuoteTick, "AAPL  261120C00355000.OPRA")]),
        "AAPL  261120C00355000.OPRA"
    )]
    #[case::an_lsedge_series_book(
        ticks(&[(NautilusDataType::OrderBookDepth, "GOLD-FUTURES.LSEDGE")]),
        "GOLD-FUTURES.LSEDGE"
    )]
    #[tokio::test]
    async fn a_run_reading_a_series_lse_does_not_stream_is_refused_naming_it_before_anything_connects(
        #[case] read: DataRequirements,
        #[case] series: &str,
    ) {
        let requirements = DataRequirements {
            ticks: [
                read.ticks,
                ticks(&[(NautilusDataType::QuoteTick, SWAP)]).ticks,
            ]
            .concat(),
            ..read
        };
        let loaded = [
            InstrumentId::from(SWAP),
            InstrumentId::from("GCZ26.GLBX"),
            InstrumentId::from("GCG27.GLBX"),
            InstrumentId::from("GOLD-FUTURES.LSEDGE"),
            InstrumentId::from("AAPL  261120C00355000.OPRA"),
        ];

        let Err(e) = live_clients(&requirements, &loaded, &live_args(), Path::new("catalog")).await
        else {
            panic!("set up clients for {series}");
        };

        let err = format!("{e:#}");
        assert!(err.contains(series), "{err}");
    }

    #[rstest]
    #[case::an_ib_instrument_s_book_depth(
        ticks(&[(NautilusDataType::OrderBookDepth, "SXR8.IBIS")]),
        "SXR8.IBIS"
    )]
    #[case::a_forex_pair_s_trades(
        ticks(&[(NautilusDataType::TradeTick, "EUR/USD.IDEALPRO")]),
        "EUR/USD.IDEALPRO"
    )]
    #[case::bars_built_from_a_forex_pair_s_trades(
        reading(&["EUR/USD.IDEALPRO-1-HOUR-LAST-INTERNAL"]),
        "EUR/USD.IDEALPRO"
    )]
    #[case::the_1_second_bars_a_composite_is_built_from(
        reading(&["SXR8.IBIS-1-MINUTE-LAST-INTERNAL@1-SECOND-EXTERNAL"]),
        "SXR8.IBIS-1-SECOND-LAST-EXTERNAL"
    )]
    #[case::an_okx_swap_s_mid_bars_a_composite_is_built_from(
        reading(&["BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL@1-MINUTE-EXTERNAL"]),
        "BTC-USDT-SWAP.OKX-1-MINUTE-MID-EXTERNAL"
    )]
    #[tokio::test]
    async fn a_run_reading_a_series_ib_or_okx_does_not_stream_is_refused_naming_it_before_anything_connects(
        #[case] read: DataRequirements,
        #[case] series: &str,
    ) {
        let err = refused_at_start(read).await;

        assert!(err.contains(series), "{err}");
    }

    /// The run gets past the check to set up OKX's clients, which fail on the swap's missing
    /// definition.
    #[tokio::test]
    async fn a_run_reading_ib_s_book_deltas_and_okx_s_book_gets_past_the_streams_check() {
        use NautilusDataType as D;

        let err = refused_at_start(ticks(&[
            (D::OrderBookDelta, "ESZ26.CME"),
            (D::OrderBookDelta, SWAP),
            (D::OrderBookDepth, SWAP),
        ]))
        .await;

        assert!(
            err.contains("fill the catalog before trading it live"),
            "{err}"
        );
    }

    fn lse_rows(source: &str, symbol: &str) -> CustomSeries {
        let metadata = nautilus_core::Params::new(
            [
                ("source".to_string(), serde_json::json!(source)),
                ("symbol".to_string(), serde_json::json!(symbol)),
            ]
            .into(),
        );
        CustomSeries::new("LseRow", symbol.to_string(), Some(metadata)).unwrap()
    }

    #[tokio::test]
    async fn a_run_reading_a_custom_series_is_refused_by_its_type_s_streams_answer_before_anything_connects()
     {
        let us10y = lse_rows("bond_yields", "US10Y");
        let requirements = DataRequirements {
            custom: vec![us10y.clone()],
            ..ticks(&[(NautilusDataType::QuoteTick, "BTC-USDT-SWAP.OKX")])
        };

        let Err(e) = live_clients(
            &requirements,
            &requirements.instrument_ids(),
            &live_args(),
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up clients for a custom series");
        };

        let err = format!("{e:#}");
        assert!(err.contains(&us10y.to_string()), "{err}");
    }

    #[rstest]
    #[case::the_tests_signal(signal())]
    #[case::a_root_s_imbalances(imbalances("GC.GLBX"))]
    #[case::a_contract_s_imbalances(imbalances("GCZ26.GLBX"))]
    #[tokio::test]
    async fn a_run_reading_a_custom_series_of_a_type_with_no_row_is_refused_naming_it_before_anything_connects(
        #[case] series: CustomSeries,
    ) {
        let requirements = DataRequirements {
            custom: vec![series.clone()],
            ..ticks(&[(NautilusDataType::QuoteTick, "BTC-USDT-SWAP.OKX")])
        };

        let Err(e) = live_clients(
            &requirements,
            &requirements.instrument_ids(),
            &live_args(),
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up clients for {series}, the catalog's");
        };

        let err = format!("{e:#}");
        assert!(err.contains(&series.to_string()), "{err}");
    }
}
