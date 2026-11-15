//! The venues: each one's history, live clients and command-line settings in a
//! module of its own, and the routing table, a row per venue (`Row`) saying,
//! each on its own, where its instruments' definitions (`DefinitionsSource`),
//! history (`HistorySource`) and live data (`LiveDataSource`) come from, and
//! which broker trades them (`Broker`), if one does. Everything else asks here
//! and names no venue, each caller the row's part it needs: fill-catalog for
//! its fill (`fill_history`: the definitions and history), the live runner for
//! its node's clients (`live_clients`: the live data), a live warmup for the
//! client it asks a definition of (`live_definitions_client`: the broker, or
//! the live data), a backtest for how a venue keeps positions (`oms_type`,
//! its broker's answer), whether it has money (`has_broker`: a venue no
//! broker trades has none) and whether its account has a base currency
//! (`has_base_currency`, its broker's answer), a trade where it reads its
//! account's equity (`equity_source`, its broker's answer), whether its
//! orders may be reduce-only and post-only
//! (`takes_reduce_only_and_post_only`, its broker's answer) and the tag
//! linking a bracket's exits at its broker (`bracket_exits_tag`, IB's OCA
//! group), fill-catalog and a live warmup whether a venue's history serves
//! a series (`serves`, its history source's answer, each a copy of the data
//! client its fill asks, so that a fill and a warmup agree on what history
//! there is), and a strategy the series an FX pair it converts through is
//! read in (`rate_series`: the first form `serves` takes, the pair's
//! 1-minute BID and ASK bars, then its quotes); venue code checks its own
//! instruments against the table. Custom data of no instrument has no venue
//! and no row: its type names its source, LSE's rows LSE (`fill_history`).
//! The settings each command takes of the venues are composed here,
//! `LiveVenueArgs` for live and `HistoryVenueArgs` for fill-catalog, which
//! the commands flatten.
//!
//! The cTrader brokers are one kind (`CTraderBroker`), which both dispatches
//! loop over: each one's module names its venue and account, and its row of
//! the table is a line of `CTRADER_BROKERS`, all another such broker takes.
//! What they share is the adapter's (`ctrader`), its clients built for helix
//! here (`ctrader_node_client`, `ctrader_clients`).
//!
//! No trait stands for a venue: Nautilus' factories, configs and
//! `RoutingConfig` are already the seam, and the venues' needs differ too much
//! for a trait to hide them. OKX needs its account's leverage, IB an account, a client id
//! and its fill after the node's, Databento an API key and its own check,
//! LSE a key per market and, for its own series, definitions written into
//! the catalog by hand.

pub mod databento;
pub mod ib;
pub mod ic;
pub mod lse;
pub mod okx;
pub mod pepperstone;

use std::path::Path;

use ::lse::{LseDataClientConfig, LseDataClientFactory, LseRow};
use anyhow::{Context, bail, ensure};
use clap::Args;
use ctrader::{
    CTraderAccountVars, CTraderConnection, CTraderDataClientConfig, CTraderDataClientFactory,
    CTraderExecutionClientConfig, CTraderExecutionClientFactory,
};
use jiff::Timestamp;
use nautilus_common::factories::DataClientFactory;
use nautilus_interactive_brokers::factories::InteractiveBrokersDataClientFactory;
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::{BarSpecification, BarType, CustomDataTrait, DataType, NautilusDataType},
    enums::{AggregationSource, BarAggregation, OmsType, PriceType},
    identifiers::{ClientId, ClientOrderId, InstrumentId, Venue},
};
use nautilus_okx::factories::OKXDataClientFactory;
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use ustr::Ustr;

use self::{databento::DatabentoHistory, ib::IbHistory, okx::OkxHistory};
use crate::{
    catalog::{NodeClient, NodeFill, missing},
    live::node::{Clients, ExecClient},
    strategy::{
        Series,
        requirements::{CatalogSeries, DataRequirements, SeriesSlice, TickSeries},
    },
};

/// A cTrader broker: the venue of its instruments, which names its clients
/// too, and the variables of its account. Its module declares it and
/// `CTRADER_BROKERS` routes it; the adapter (`ctrader`) does the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CTraderBroker {
    venue: &'static str,
    account: CTraderAccountVars,
}

impl CTraderBroker {
    /// The broker's connection, from the process' environment (or `.env`,
    /// once read): see `CTraderConnection::from_vars`.
    ///
    /// # Errors
    ///
    /// As `CTraderConnection::from_vars`.
    fn connection(self) -> anyhow::Result<CTraderConnection> {
        CTraderConnection::from_vars(Venue::new(self.venue), self.account, |name| {
            std::env::var(name).ok()
        })
    }
}

/// The cTrader brokers, each a row of the routing table (see `row`), in the
/// order a fill asks them and a node takes their clients.
const CTRADER_BROKERS: &[CTraderBroker] = &[ic::BROKER, pepperstone::BROKER];

/// The cTrader broker of `venue`, if one of `CTRADER_BROKERS` is on it.
fn ctrader_broker(venue: Venue) -> Option<CTraderBroker> {
    CTRADER_BROKERS
        .iter()
        .copied()
        .find(|broker| venue.as_str() == broker.venue)
}

/// The cTrader brokers among `ids`, in the order of `CTRADER_BROKERS` whatever
/// the order of `ids`, each with its instruments in the order they come.
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

/// The source of a venue's instrument definitions in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DefinitionsSource {
    /// The catalog alone: definitions written into it by hand, which
    /// Nautilus' data engine answers a fill's definition requests with
    /// before asking any client.
    Catalog,
    /// OKX's public API, written into the catalog before the node starts
    /// (`OkxHistory::write_definitions`).
    Okx,
    /// A cTrader broker's symbols.
    CTrader(CTraderBroker),
    /// Databento's datasets: `GLBX.MDP3` of `GLBX`'s, `XNAS.ITCH` of
    /// `XNAS`'s, `OPRA.PILLAR` of `OPRA`'s.
    Databento,
    /// Interactive Brokers' contract details.
    Ib,
}

/// The source of a venue's history in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistorySource {
    /// OKX's public API, through Nautilus' OKX data client.
    Okx,
    /// A cTrader broker's tick history.
    CTrader(CTraderBroker),
    /// LSE's exports, through the LSE adapter's data client.
    Lse,
    /// Interactive Brokers' historical bars and ticks.
    Ib,
}

/// The source of a venue's live data on the live node: a data client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveDataSource {
    /// OKX's data client.
    Okx,
    /// A cTrader broker's data client.
    CTrader(CTraderBroker),
    /// The LSE adapter's data client, streaming LSE's quotes, CME Globex's
    /// and Nasdaq TotalView's Levels 1 to 3 and US options' trades.
    Lse,
    /// Interactive Brokers' data client, on the gateway.
    Ib,
}

/// The broker trading a venue's instruments: its execution client on the
/// live node, whose account a backtest simulates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Broker {
    /// OKX's execution client.
    Okx,
    /// A cTrader broker's execution client, on a hedging account.
    CTrader(CTraderBroker),
    /// Interactive Brokers' execution client, on the gateway.
    Ib,
}

/// Where a trade reads the equity of an account as its broker reports it
/// (`Broker::equity_source`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EquitySource {
    /// Nautilus' `Portfolio::equity`: the account's balance totals and its
    /// open positions' P&L, for a broker whose balance leaves them out.
    Portfolio,
    /// The account's balance totals alone (`Account::balances_total`), for
    /// a broker whose balance holds its open positions' P&L already.
    BalanceTotals,
}

impl Broker {
    /// How the broker's accounts keep positions. Nautilus answers this only
    /// on an execution client it has built, so this is a copy of its answer,
    /// each broker's pinned by a test creating that client through its
    /// factory from the config the node gets: the cTrader brokers' accounts
    /// hedge, each trade a position of its own; OKX's derivatives net, in
    /// the net position mode live requires; IB's account nets.
    #[must_use]
    pub(crate) fn oms_type(self) -> OmsType {
        match self {
            Self::CTrader(_) => OmsType::Hedging,
            Self::Okx | Self::Ib => OmsType::Netting,
        }
    }

    /// Where a trade reads the equity of the broker's account as its
    /// execution client reports it. A copy of the adapters: the cTrader
    /// adapter reports an account's balance, its deposits and realized P&L
    /// (`ctrader`'s execution client, `account_state`), and OKX's its cash
    /// (`parse_account_state`), leaving open positions out, so their equity
    /// is Nautilus' `Portfolio::equity`, which adds the open positions'
    /// P&L. IB's adapter reports IB's net liquidation as the balance's
    /// total (`execution/account.rs`, `AccountSummaryBalance`), which holds
    /// that P&L already, so its equity is the account's balance totals
    /// alone: `Portfolio::equity` adds a reported margin account's open P&L
    /// to them again, as Nautilus documents it, and will not change
    /// (nautilus_trader#3565, closed as not planned).
    #[must_use]
    pub(crate) fn equity_source(self) -> EquitySource {
        match self {
            Self::CTrader(_) | Self::Okx => EquitySource::Portfolio,
            Self::Ib => EquitySource::BalanceTotals,
        }
    }

    /// Whether the broker's accounts have a base currency, one currency
    /// Nautilus books their margins, commissions and P&L in, converting
    /// through the rates of the traded instrument's venue. Nautilus answers
    /// this only in an account state the execution client reports once
    /// connected, so this is a copy of the adapters: the cTrader adapter's
    /// account state carries the account's deposit currency
    /// (`ctrader`'s execution client, `account_state`); OKX's adapter
    /// reports none (`parse_account_state`), its accounts holding each
    /// currency apart, nor does IB's execution client, whose config sets
    /// none (`ib::live`).
    #[must_use]
    pub(crate) fn has_base_currency(self) -> bool {
        match self {
            Self::CTrader(_) => true,
            Self::Okx | Self::Ib => false,
        }
    }

    /// Whether the broker's execution client takes reduce-only and
    /// post-only orders, which Nautilus' bracket makes of its exits and take
    /// profit and `close_position` of its close unless told otherwise. A
    /// copy of IB's adapter (`crates/adapters/interactive_brokers/src/
    /// execution`): `core.rs` `validate_order` denies a reduce-only order,
    /// and a list holding one whole, and `commands.rs` `prepare_submit_order`
    /// a post-only order sent alone, one in a list going to IB as a plain
    /// order. Unlike `oms_type` no test pins this copy by building the
    /// client: Nautilus' `ExecutionClient` gives no such answer.
    #[must_use]
    pub(crate) fn takes_reduce_only_and_post_only(self) -> bool {
        match self {
            Self::CTrader(_) | Self::Okx => true,
            Self::Ib => false,
        }
    }

    /// The tag of a bracket's exits linking them at the broker, the bracket
    /// named by `entry`, its entry's client order id: on IB their OCA group
    /// (`ib::bracket_exits_tag`); none on the others, whose execution
    /// clients read no IB tag.
    #[must_use]
    pub(crate) fn bracket_exits_tag(self, entry: ClientOrderId) -> Option<Ustr> {
        match self {
            Self::CTrader(_) | Self::Okx => None,
            Self::Ib => Some(ib::bracket_exits_tag(entry)),
        }
    }

    /// The broker's data client on the live node, by the name the node
    /// registers it under, its factory's: OKX's and IB's adapters name
    /// theirs, the cTrader adapter names each broker's after its venue.
    fn data_client(self) -> ClientId {
        match self {
            Self::Okx => ClientId::new(OKXDataClientFactory::new().name()),
            Self::CTrader(broker) => ClientId::new(broker.venue),
            Self::Ib => ClientId::new(InteractiveBrokersDataClientFactory::new().name()),
        }
    }
}

impl LiveDataSource {
    /// The source's data client on the live node, by the name the node
    /// registers it under, its factory's.
    fn client(self) -> ClientId {
        match self {
            Self::Okx => Broker::Okx.data_client(),
            Self::CTrader(broker) => Broker::CTrader(broker).data_client(),
            Self::Lse => ClientId::new(LseDataClientFactory::new().name()),
            Self::Ib => Broker::Ib.data_client(),
        }
    }
}

/// A venue's row of the routing table: where its instruments' definitions,
/// history and live data come from, each on its own, and the broker
/// trading them, if one does.
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

    /// The row of `venue`. A venue of `CTRADER_BROKERS` takes everything
    /// from its broker, and OKX from OKX. GLBX, CME Globex as Databento
    /// names it, takes its definitions from Databento, LSE publishing
    /// none, its history and live data from LSE's CME Globex, and its
    /// orders from IB; XNAS, Nasdaq TotalView as Databento names it, its
    /// definitions from Databento, its history and live data from LSE's
    /// Nasdaq TotalView, and its orders from IB, whose adapter maps XNAS to
    /// its NASDAQ; OPRA, US options as Databento names them, its
    /// definitions from Databento, its history and live data from LSE's US
    /// options, and its orders from IB. LSEDGE,
    /// LSE's own series, takes its definitions from the catalog alone, its
    /// history and live data from LSE, and has no broker: LSE trades
    /// nothing. Every venue without a row of its own takes everything from
    /// IB.
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

/// Checks that a source serves custom data of no instrument of the type
/// `type_name`: such data has no venue, its type naming its source, and LSE
/// alone serves any, its rows (`lse::LseRow`).
fn custom_source(type_name: &str) -> anyhow::Result<()> {
    let rows = LseRow::type_name_static();
    ensure!(
        type_name == rows,
        "no source fills the custom data {type_name} of no instrument: LSE fills its {rows}"
    );
    Ok(())
}

/// The data type of custom data of no instrument of the type `type_name`
/// and the `metadata` a strategy's config gives it, keyed as its source
/// keys it: LSE's rows of a `source` and a `symbol`, both strings, keyed by
/// `lse::LseRow::data_type`.
///
/// # Errors
///
/// Returns an error naming the type if no source serves it (see
/// `fill_history`), or the key if the metadata lacks `source` or `symbol`,
/// holds another, or holds one not a string.
pub fn custom_data_type(
    type_name: &str,
    metadata: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<DataType> {
    custom_source(type_name)?;
    let text = |key: &str| {
        metadata
            .get(key)
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("{type_name} names its {key}, a string, in its metadata"))
    };
    let (source, symbol) = (text("source")?, text("symbol")?);
    if let Some(other) = metadata
        .keys()
        .find(|key| !["source", "symbol"].contains(&key.as_str()))
    {
        bail!("{type_name} takes a source and a symbol in its metadata, not {other}");
    }
    Ok(LseRow::data_type(source, symbol))
}

/// Registers with Nautilus the custom data types a source serves, LSE's
/// rows, so that a catalog's are read: Nautilus decodes custom data by its
/// type's name. Idempotent.
pub fn register_custom_data() {
    LseRow::register();
}

/// How the accounts of `venue` keep positions: as those of its broker do
/// (`Broker::oms_type`), so every venue without a row of its own as IB's;
/// a venue no broker trades as Nautilus' venue default
/// (`OmsType::Unspecified`). A backtest's normal run simulates each venue
/// so, as Nautilus' docs ask of a backtest venue.
#[must_use]
pub fn oms_type(venue: Venue) -> OmsType {
    Row::of(venue)
        .broker
        .map_or(OmsType::Unspecified, Broker::oms_type)
}

/// Whether a broker trades the instruments of `venue`: every venue's but
/// LSEDGE's, which is only read. A backtest gives a venue no broker trades
/// no money.
#[must_use]
pub fn has_broker(venue: Venue) -> bool {
    Row::of(venue).broker.is_some()
}

/// Whether the account of `venue`'s broker has a base currency
/// (`Broker::has_base_currency`), so every venue without a row of its own
/// as IB's has none; a venue no broker trades has none. A backtest's
/// simulated account of the venue takes the balance's currency as its base
/// if so, as the live account keeps its own.
#[must_use]
pub fn has_base_currency(venue: Venue) -> bool {
    Row::of(venue).broker.is_some_and(Broker::has_base_currency)
}

/// The series the rates of `pair`, an FX pair a strategy converts equity
/// through (`[trade] conversions`), are read from: the first form of the
/// pair its venue's history serves (`serves`), its 1-minute BID and ASK
/// bars, then its quotes; an IB pair's bars, a cTrader pair's quotes.
/// Bars first: a rate needs the pair's latest value alone, which Nautilus
/// reads of a pair with no quote cached at the closes of its latest BID
/// and ASK bars (`Cache::get_xrate`), and bars are the lighter history to
/// fill and warm up on. The strategy's data requirements carry them
/// (`StandardConfig::data_requirements`), so that fill-catalog fills them,
/// a backtest replays them and a live warmup requests them as any series
/// served, and the rates are read from one form of the pair, the same in
/// a backtest as live: Nautilus reads a pair's quotes before its bars,
/// however old.
///
/// # Errors
///
/// Returns an error naming the pair and its venue, and why, if its venue's
/// history serves neither form, OKX's: no rate of it could be read.
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
    bail!(
        "no rate of {pair} can be read: {venue}'s history serves neither its 1-minute BID and \
         ASK bars ({no_bars:#}) nor its quotes ({no_quotes:#})",
        venue = pair.venue
    )
}

/// Checks that the history of the venue of `source`'s instrument serves
/// it, as its row's history source answers (`Row::of`, so every venue
/// without a row of its own as IB's), each a copy of what the data client
/// its fill asks requests: a cTrader broker's quotes alone
/// (`ctrader::quoted`); OKX's trades and candles (`okx::serves`); the
/// series LSE's client asks LSE for (`lse::serves`); IB's time bars,
/// quotes and trades (`ib::serves`). fill-catalog checks every series it
/// fills here before anything connects, and a live warmup requests the
/// history of a series only if served (`strategy::warmup`), so the two
/// agree; a conversion pair is read in a form served (`rate_series`).
/// Custom data of an instrument is the catalog's: a fill fills it from no
/// source, and a warmup asks the catalog alone.
///
/// # Errors
///
/// Returns an error naming `source` and saying why if its venue's history
/// does not serve it.
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

/// Where a trade on `venue` reads the equity of its broker's account as the
/// broker reports it (`Broker::equity_source`), so every venue without a
/// row of its own as IB; a venue no broker trades, whose account is
/// Nautilus' alone, from `Portfolio::equity`.
#[must_use]
pub fn equity_source(venue: Venue) -> EquitySource {
    Row::of(venue)
        .broker
        .map_or(EquitySource::Portfolio, Broker::equity_source)
}

/// The data client a live node asks the definition of an instrument of
/// `venue` of: its broker's, if a broker trades it, its live data
/// source's otherwise, LSEDGE's LSE, which publishes none (see
/// `strategy::warmup`). The node's catalog answers first either way.
#[must_use]
pub fn live_definitions_client(venue: Venue) -> ClientId {
    let row = Row::of(venue);
    row.broker
        .map_or_else(|| row.live_data.client(), Broker::data_client)
}

/// Whether the broker of `venue` takes reduce-only and post-only orders
/// (`Broker::takes_reduce_only_and_post_only`), so every venue without a
/// row of its own as IB; a venue no broker trades takes none. A trade asks
/// it in a backtest as live, so that a backtest sends the orders live
/// sends.
#[must_use]
pub fn takes_reduce_only_and_post_only(venue: Venue) -> bool {
    Row::of(venue)
        .broker
        .is_some_and(Broker::takes_reduce_only_and_post_only)
}

/// The tag a bracket's exits on `venue` carry besides Nautilus' own,
/// linking them at its broker (`Broker::bracket_exits_tag`), the bracket
/// named by `entry`, its entry's client order id: so every venue without a
/// row of its own as IB; a venue no broker trades has none. A trade tags
/// them in a backtest as live, the simulated venue reading no tag.
#[must_use]
pub fn bracket_exits_tag(venue: Venue, entry: ClientOrderId) -> Option<Ustr> {
    Row::of(venue)
        .broker
        .and_then(|broker| broker.bracket_exits_tag(entry))
}

/// The venues' settings of a live node, the live command's flags, each
/// venue's declared in its module: IB's account and client id. The live
/// runner holds them and hands them to `live_clients`.
#[derive(Debug, Clone, Args)]
pub struct LiveVenueArgs {
    #[command(flatten)]
    ib: ib::IbLiveArgs,
}

/// The venues' settings of a catalog fill, fill-catalog's flags, each
/// venue's declared in its module: IB's client id.
#[derive(Debug, Args)]
pub struct HistoryVenueArgs {
    #[command(flatten)]
    ib: ib::IbHistoryArgs,
}

/// The data client of a cTrader broker for a fill through the node (see
/// `NodeFill::add`), on `connection`, loading `instrument_ids`.
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

/// Adds to `node` the fill of `slices`, the quotes of `instrument_ids`,
/// instruments of a cTrader broker, which its history serves (`serves`),
/// over what the catalog misses of them, a window holding at most what one
/// tick request spans, the broker's data client loading `instrument_ids`.
/// The connection is read only if something is missing, so a complete
/// catalog needs no credential.
///
/// # Errors
///
/// Returns an error if the catalog cannot be read, or as `connection`.
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

/// Those of `slices` whose contract `source` fills.
fn of_source(slices: &[SeriesSlice], source: HistorySource) -> Vec<SeriesSlice> {
    slices
        .iter()
        .filter(|slice| Row::of(slice.slice.instrument_id.venue).history == source)
        .cloned()
        .collect()
}

/// Those of `slices` whose contract `source` defines.
fn of_definitions_source(slices: &[SeriesSlice], source: DefinitionsSource) -> Vec<SeriesSlice> {
    slices
        .iter()
        .filter(|slice| Row::of(slice.slice.instrument_id.venue).definitions == source)
        .cloned()
        .collect()
}

/// Fills the catalog in `catalog_dir` with the instruments and the series
/// `requirements` reads over `[start, end]`, those its bars are built from
/// and its ticks (see `DataRequirements::catalog_keys`), each on a contract
/// over its slice (`DataRequirements::slices`), every history source
/// (`Row::of`, `HistorySource`) given its slices, and its custom data of no
/// instrument (`DataRequirements::custom`), which has no venue, from the
/// source its type names: LSE's rows (`lse::LseRow`) from LSE, the only
/// such type a source serves. The cTrader brokers', OKX's and LSE's fill
/// through a Nautilus node (see `catalog::NodeFill`), each request naming
/// the client that answers it: each cTrader broker's data client for its
/// own instruments (`add_ctrader_fill`), OKX's for OKX instruments (their
/// series and funding) and LSE's for LSEDGE, GLBX, XNAS and OPRA
/// instruments and LSE's rows (`lse::add_fill`). Then IB, connected only if it has
/// instruments, on the client id of `venues`, fills the others, without a
/// node until its client answers with response envelopes. Each contract's
/// definition comes from its row's definitions source
/// (`DefinitionsSource`): the cTrader brokers and IB ask the definition of
/// each contract they fill the series of; OKX writes the definitions it
/// gives before the node starts; GLBX's, XNAS's and OPRA's are asked of
/// Databento's data client in LSE's fill, each within the end of its
/// dataset; LSEDGE's are the catalog's alone, which the data
/// engine answers LSE's client's definition requests from.
/// Before anything connects, every series is checked against its venue's
/// history (`serves`), custom data of an instrument aside, which each
/// source refuses as it reaches it, and IB's against what IB holds
/// (`ib::check_held`). A source with nothing missing reads no credential,
/// IB's connection aside.
///
/// # Errors
///
/// Returns an error if custom data of no instrument is of a type no source
/// fills, if a venue's history does not serve a series, or if IB holds no
/// bars of a series as old as its slice, each before anything connects; if
/// the catalog cannot be read, if
/// `DATABENTO_API_KEY` is missing while Databento has a definition to give,
/// or as `add_ctrader_fill`, `OkxHistory::write_definitions`,
/// `lse::add_fill` (`DatabentoHistory::new` and
/// `DatabentoHistory::within_datasets` among them), `NodeFill::run` (`LSE_API_KEY`, `LSE_CME_API_KEY` or
/// `LSE_NASDAQ_API_KEY` missing while LSE has something to fill of its
/// market among them), `IbHistory::connect` and `IbHistory::fill`.
pub async fn fill_history(
    requirements: &DataRequirements,
    start: Timestamp,
    end: Timestamp,
    venues: &HistoryVenueArgs,
    catalog_dir: &Path,
) -> anyhow::Result<()> {
    for data_type in &requirements.custom {
        custom_source(data_type.type_name())?;
    }
    for source in requirements.catalog_series() {
        if !matches!(
            source,
            CatalogSeries::Ticks(TickSeries {
                data: NautilusDataType::Custom { .. },
                ..
            })
        ) {
            serves(&source)?;
        }
    }
    // Every source takes the series slices of its instruments: a rolled
    // root's series contract by contract, each over its slice.
    let slices = requirements.slices(start, end);
    let ib_slices = of_source(&slices, HistorySource::Ib);
    let mut catalog = ParquetDataCatalog::new(catalog_dir, None, None, None, None);
    ib::check_held(&catalog, &ib_slices, Timestamp::now())?;
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
            .write_definitions(&mut catalog, &okx_definitions)
            .await?;
    }
    if !okx_ids.is_empty() {
        let slices = of_source(&slices, HistorySource::Okx);
        let requests = okx::requests(&catalog, &slices)?;
        if !requests.is_empty() {
            node.add(okx::node_client(&okx_ids)?, &requests, okx::period);
        }
    }
    if !lse_ids.is_empty() || !requirements.custom.is_empty() {
        lse::add_fill(
            &mut node,
            &catalog,
            &of_source(&slices, HistorySource::Lse),
            &requirements.custom,
            start,
            end,
            LseDataClientConfig::default(),
            || {
                let api_key =
                    std::env::var("DATABENTO_API_KEY").context("reading DATABENTO_API_KEY")?;
                DatabentoHistory::new(&api_key)
            },
        )
        .await?;
    }
    node.run(catalog_dir).await?;
    // Temporary until nautilus_trader#5472 is fixed: IB then fills through
    // the node.
    if !ib_ids.is_empty() {
        let ib = IbHistory::connect(venues.ib.client_id, end, Timestamp::now()).await?;
        ib.fill(&mut catalog, &ib_slices).await?;
    }
    Ok(())
}

/// The clients of a node trading `instrument_ids` of a cTrader broker, on
/// `connection`. Both are on its venue, so Nautilus routes the broker's data
/// and orders to them without a route of ours.
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

/// What a node needs of the cTrader broker of `venue` to trade its
/// instruments `ids`: the connection `connection` reads, and the instruments
/// of the series `requirements` reads of them, those its bars are built from
/// and its ticks, as a fill reads them (`DataRequirements::catalog_keys`),
/// their quotes being all cTrader serves: an instrument read as its quotes
/// alone, a conversion pair's, is loaded, so that the strategy's
/// subscription to them is served. Nothing connects.
///
/// # Errors
///
/// Returns an error if a series is not the quotes of an instrument on
/// `venue` (see `ctrader::quoted`), a bar type not built from them or ticks
/// of another kind, before the connection is read, or as `connection`.
fn ctrader_read(
    requirements: &DataRequirements,
    ids: &[InstrumentId],
    venue: &str,
    connection: impl FnOnce() -> anyhow::Result<CTraderConnection>,
) -> anyhow::Result<(CTraderConnection, Vec<InstrumentId>)> {
    let instrument_ids = ctrader::quoted(
        Venue::new(venue),
        &requirements.of_instruments(ids).catalog_keys(),
    )?;
    Ok((connection()?, instrument_ids))
}

/// The instruments of each adapter's clients on the live node, and the
/// venues IB's data client is routed.
#[derive(Debug, Default, PartialEq, Eq)]
struct LiveRoutes {
    okx: Vec<InstrumentId>,
    ctrader: Vec<(CTraderBroker, InstrumentId)>,
    lse: Vec<InstrumentId>,
    ib: Vec<InstrumentId>,
    ib_data_venues: Vec<Venue>,
}

impl LiveRoutes {
    /// Each of `instrument_ids` to the clients of its row's live data
    /// source, and to IB's too if IB is its broker: IB's clients come as a
    /// pair, the data client, which loads the instruments it is given
    /// (`load_ids`), and the execution client, which resolves the
    /// instruments of the orders it takes from them; so GLBX, XNAS and OPRA,
    /// whose live data is LSE's and whose orders go to IB, are given to
    /// both. IB's
    /// data client is routed the venues whose live data is IB's, each once.
    /// OKX's and the cTrader brokers' rows take their live data from their
    /// broker.
    fn of(instrument_ids: &[InstrumentId]) -> Self {
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
        routes
    }
}

/// The live node's clients of `instrument_ids` (see `LiveRoutes`), for the
/// instruments `requirements` reads: OKX's, each cTrader broker's
/// (`ctrader_brokers_present`), LSE's, then IB's. OKX's, the cTrader
/// brokers' and IB's are each a data client with the execution client of
/// the same adapter on its connection; LSE's is a data client alone, routed
/// the venues of its instruments, LSEDGE, which no broker trades, and GLBX,
/// XNAS and OPRA, whose orders go to IB's execution client (see
/// `lse::live_clients`), its keys read when the node creates it. IB's data
/// client is routed the venues whose live data is IB's, and its execution
/// client takes the default route of orders, every order but OKX's and the
/// cTrader brokers'. So every venue the run reads has a route of its data,
/// and helix makes no data client the default: Nautilus makes the first
/// data client on no venue it registers the default itself, LSE's or IB's
/// in no set order, and would refuse IB's made the default after LSE's
/// (see `live::node::Clients`); nothing depends on which, every venue read
/// having a route. No source streams custom data of no
/// instrument, LSE's rows, so a run reading any is refused before anything
/// connects, and so is one reading a series of LSE's venues LSE's client
/// does not stream (`lse::check_streamed`): external bars, LSE streaming no
/// candles, a GLBX or XNAS contract's among them, which IB streamed before
/// LSE took their live data; an LSEDGE series' trades; an OPRA contract's
/// quotes. The cTrader brokers' series, bars and ticks, are checked and
/// their environments and credential read before anything connects; OKX's
/// clients are set up once its account trades each instrument at the
/// leverage its definition in `catalog` holds, IB's trading the IB account
/// of `venues` with its client id.
///
/// # Errors
///
/// Returns an error naming them if `requirements` reads custom data of no
/// instrument or a series LSE's client does not stream of a venue whose
/// live data is LSE's, if a cTrader broker's series is not its quotes or
/// built from them, or its environment or credential is missing (see `ctrader_read`,
/// checked before anything connects), if OKX's clients cannot be set up or the OKX
/// account does not suit the run (see `okx::clients`), if cTrader is
/// unreachable or a broker's account is not hedging or not the
/// credential's (see
/// `CTraderConnection::check_account`), if the strategy trades IB
/// instruments and no account is given, or if IB's clients cannot be set up
/// (see `ib::clients`).
pub(crate) async fn live_clients(
    requirements: &DataRequirements,
    instrument_ids: &[InstrumentId],
    venues: &LiveVenueArgs,
    catalog: &Path,
) -> anyhow::Result<Vec<Clients>> {
    let LiveRoutes {
        okx: okx_ids,
        ctrader: ctrader_ids,
        lse: lse_ids,
        ib: ib_ids,
        ib_data_venues,
    } = LiveRoutes::of(instrument_ids);
    ensure!(
        requirements.custom.is_empty(),
        "no source streams custom data of no instrument live yet: {}",
        requirements
            .custom
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    // Read before anything connects: what LSE's client is to stream, of
    // the instruments read, a rolled root's among them, whose contracts it
    // streams.
    let lse_read: Vec<InstrumentId> = requirements
        .instrument_ids()
        .into_iter()
        .filter(|id| Row::of(id.venue).live_data == LiveDataSource::Lse)
        .collect();
    ::lse::check_streamed(&requirements.of_instruments(&lse_read).catalog_keys())?;
    let ctrader = ctrader_brokers_present(&ctrader_ids)
        .into_iter()
        .map(|(broker, ids)| ctrader_read(requirements, &ids, broker.venue, || broker.connection()))
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
    if !lse_ids.is_empty() {
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

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        ffi::OsStr,
        os::unix::ffi::OsStrExt,
        rc::Rc,
    };

    use ::lse::LSEDGE_VENUE;
    use clap::{Command, FromArgMatches};
    use ctrader::CTraderEnvironment;
    use jiff::SignedDuration;
    use nautilus_common::{
        cache::Cache,
        clients::DataClient,
        clock::VirtualClock,
        enums::Environment,
        live::runner::{replace_data_event_sender, replace_exec_event_sender},
        messages::data::{
            RequestBars, RequestBookDeltas, RequestBookDepth, RequestCommand, RequestQuotes,
            RequestTrades,
        },
    };
    use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
    use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig, node::LiveNode};
    use nautilus_model::{
        data::{BarType, DataType, FundingRateUpdate, QuoteTick, TradeTick},
        enums::{AggressorSide, OrderSide, OrderType},
        identifiers::{AccountId, ClientId, TradeId, TraderId},
        instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
        orders::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use nautilus_okx::common::consts::OKX_VENUE;

    use super::*;
    use crate::catalog::{FillData, FillRequest};

    /// A demo account of `broker`, through its account's variables, beside
    /// the login's credential.
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

    /// Each broker's account by its own variables, on the login's one
    /// credential and token file.
    #[test]
    fn each_ctrader_venue_reads_its_own_account_beside_the_login_s_credential() {
        let ic = demo_connection(ic::BROKER);
        let pepperstone = demo_connection(pepperstone::BROKER);

        assert_eq!(ic.account_id(), AccountId::from("ICMARKETS-DEMO"));
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

    /// The bars are checked first: a bad one fails the start without the
    /// environment, naming the bars.
    #[test]
    fn bars_not_built_from_quotes_are_refused_before_the_connection_is_read() {
        for bars in [
            "XAUUSD.PEPPERSTONE-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            "XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL",
            "XAUUSD.PEPPERSTONE-1-HOUR-LAST-INTERNAL",
        ] {
            let Err(err) = ctrader_read(
                &reading(&[bars]),
                &[InstrumentId::from("XAUUSD.PEPPERSTONE")],
                pepperstone::VENUE,
                || panic!("the connection was read before the bars were checked"),
            ) else {
                panic!("{bars} passed");
            };

            assert!(err.to_string().contains("serves quotes only"), "{err}");
        }
    }

    /// A broker without instruments is not among those present, the only
    /// ones whose connection either dispatch reads.
    #[test]
    fn a_ctrader_broker_without_instruments_is_not_present() {
        let gold = InstrumentId::from("XAUUSD.PEPPERSTONE");

        assert_eq!(ctrader_brokers_present(&[]), []);
        assert_eq!(
            ctrader_brokers_present(&[(pepperstone::BROKER, gold)]),
            [(pepperstone::BROKER, vec![gold])]
        );
    }

    #[test]
    fn the_ctrader_brokers_present_come_in_their_table_s_order_each_with_its_instruments_in_theirs()
    {
        let eurusd = InstrumentId::from("EURUSD.PEPPERSTONE");
        let gold = InstrumentId::from("XAUUSD.PEPPERSTONE");
        let ic_gold = InstrumentId::from("XAUUSD.ICMARKETS");

        let present = ctrader_brokers_present(&[
            (pepperstone::BROKER, gold),
            (ic::BROKER, ic_gold),
            (pepperstone::BROKER, eurusd),
        ]);

        assert_eq!(
            present,
            [
                (ic::BROKER, vec![ic_gold]),
                (pepperstone::BROKER, vec![gold, eurusd]),
            ]
        );
    }

    #[test]
    fn a_ctrader_venue_loads_the_instruments_of_its_bars() {
        let requirements = reading(&[
            "XAUUSD.PEPPERSTONE-1-HOUR-MID-INTERNAL",
            "XAUUSD.PEPPERSTONE-1-DAY-MID-INTERNAL",
            "EURUSD.PEPPERSTONE-1-HOUR-BID-INTERNAL",
        ]);
        let ids = [
            InstrumentId::from("EURUSD.PEPPERSTONE"),
            InstrumentId::from("XAUUSD.PEPPERSTONE"),
        ];

        let (_, loaded) = ctrader_read(&requirements, &ids, pepperstone::VENUE, || {
            Ok(demo_connection(pepperstone::BROKER))
        })
        .unwrap();

        assert_eq!(loaded, ids);
    }

    /// A pair of `[trade] conversions` is read as its quotes alone, no bars
    /// built from them: the broker's clients load it beside the gold of the
    /// bars, so that the strategy's subscription to its quotes is served.
    #[test]
    fn a_ctrader_venue_loads_an_instrument_read_as_quotes_alone() {
        let eurusd = InstrumentId::from("EURUSD.PEPPERSTONE");
        let requirements = DataRequirements {
            ticks: vec![TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: eurusd,
            }],
            ..reading(&["XAUUSD.PEPPERSTONE-1-HOUR-MID-INTERNAL"])
        };
        let ids = [InstrumentId::from("XAUUSD.PEPPERSTONE"), eurusd];

        let (_, loaded) = ctrader_read(&requirements, &ids, pepperstone::VENUE, || {
            Ok(demo_connection(pepperstone::BROKER))
        })
        .unwrap();

        assert_eq!(
            loaded.iter().collect::<HashSet<_>>(),
            ids.iter().collect::<HashSet<_>>()
        );
    }

    /// Ticks of a cTrader instrument are checked as its bars are: its
    /// trades, which cTrader does not serve, fail the start without the
    /// environment, naming them.
    #[test]
    fn ticks_but_quotes_are_refused_before_the_connection_is_read() {
        let gold = InstrumentId::from("XAUUSD.PEPPERSTONE");
        let requirements = DataRequirements {
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: gold,
            }],
            ..reading(&[])
        };

        let Err(err) = ctrader_read(&requirements, &[gold], pepperstone::VENUE, || {
            panic!("the connection was read before the ticks were checked")
        }) else {
            panic!("trades passed");
        };

        assert!(err.to_string().contains("serves quotes only"), "{err}");
    }

    /// As the node does: each client created by its factory, under the
    /// factory's name, from its config.
    #[test]
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

    /// `A` from the flags `argv`, as a command flattening it parses them.
    fn parsed<A: Args + FromArgMatches>(argv: &[&str]) -> Result<A, clap::Error> {
        let matches = A::augment_args(Command::new("venues"))
            .try_get_matches_from(["venues"].into_iter().chain(argv.iter().copied()))?;
        A::from_arg_matches(&matches)
    }

    #[test]
    fn live_takes_the_ib_account_and_client_id() {
        let venues: LiveVenueArgs =
            parsed(&["--account", "DU1234567", "--client-id", "7"]).unwrap();
        assert_eq!(venues.ib.account, Some("DU1234567".parse().unwrap()));
        assert_eq!(venues.ib.client_id, 7);
    }

    /// The account given, since the developer's environment may set
    /// `NAUTILUS_IB_ACCOUNT_ID`, which the flag falls back on.
    #[test]
    fn live_is_the_adapters_default_ib_client_unless_told_otherwise() {
        let venues: LiveVenueArgs = parsed(&["--account", "DU1234567"]).unwrap();
        assert_eq!(venues.ib.client_id, 1);
    }

    /// Read from the arguments' definition: setting the variable would race
    /// the other tests, and the developer's environment may have it. Not
    /// required: a run on OKX alone has no IB account.
    #[test]
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

    #[test]
    fn fill_catalog_is_ib_client_10_unless_told_otherwise() {
        let venues: HistoryVenueArgs = parsed(&[]).unwrap();
        assert_eq!(venues.ib.client_id, 10);
        let venues: HistoryVenueArgs = parsed(&["--client-id", "12"]).unwrap();
        assert_eq!(venues.ib.client_id, 12);
    }

    /// Refused before anything connects: the run has no OKX or IC
    /// instrument, and IB's gateway starts only once the account is known.
    #[tokio::test]
    async fn a_run_trading_ib_instruments_without_an_account_is_refused_naming_where_to_give_it() {
        let venues = LiveVenueArgs {
            ib: ib::IbLiveArgs {
                account: None,
                client_id: 1,
            },
        };
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let Err(err) = live_clients(
            &requirements,
            &[InstrumentId::from("SXR8.IBIS")],
            &venues,
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up IB's clients without an account");
        };
        let err = err.to_string();
        assert!(
            err.contains("IB account")
                && err.contains("--account")
                && err.contains("NAUTILUS_IB_ACCOUNT_ID"),
            "{err}"
        );
    }

    /// How the execution client of `clients` keeps positions, created as the
    /// node creates it, from the config the node gets, connecting to nothing.
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

    /// Against the adapter's client: the broker's answer is our copy of it.
    #[test]
    fn each_ctrader_broker_s_execution_client_keeps_positions_as_the_broker_answers() {
        for &broker in CTRADER_BROKERS {
            let id = InstrumentId::from(format!("XAUUSD.{}", broker.venue).as_str());

            let created = created_oms_type(ctrader_clients(demo_connection(broker), vec![id]));

            assert_eq!(created, Broker::CTrader(broker).oms_type(), "{broker:?}");
        }
    }

    #[test]
    fn each_ctrader_broker_is_on_the_venue_its_module_offers_its_callers() {
        assert_eq!((ic::BROKER.venue, ic::VENUE), ("ICMARKETS", "ICMARKETS"));
        assert_eq!(
            (pepperstone::BROKER.venue, pepperstone::VENUE),
            ("PEPPERSTONE", "PEPPERSTONE")
        );
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    const FILL_START: &str = "2026-10-01T00:00:00Z";
    /// An end IB's connection refuses, before it starts a gateway, as not
    /// yet closed (`IbHistory::connect`): a fill reaching IB says so at once,
    /// with no gateway, and a fill not reaching it does not say so.
    const FILL_END: &str = "2099-01-01T00:00:00Z";
    const IB_REFUSES: &str = "within a minute of now";
    const SWAP: &str = "BTC-USDT-SWAP.OKX";

    /// Requirements reading each of `series`, a data type of an instrument.
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

    /// Stores `quotes` over the whole of a fill's window.
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

    /// Stores the definition of the OKX swap `id`, so that a fill asks OKX's
    /// API for none.
    fn store_okx_definition(catalog: &mut ParquetDataCatalog, id: &str) {
        let mut swap = crypto_perpetual_ethusdt();
        swap.id = InstrumentId::from(id);
        catalog
            .write_instruments(vec![InstrumentAny::CryptoPerpetual(swap)])
            .unwrap();
    }

    /// Every window of every source but IB's stored: no node starts, and
    /// with no IB instrument IB is not connected, though it would refuse this
    /// end. A credential read where none is set would fail the fill.
    #[tokio::test]
    async fn a_complete_catalog_fills_with_no_credential_no_node_and_no_ib() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, to) = (Some(at(FILL_START).into()), Some(at(FILL_END).into()));
        store_okx_definition(&mut catalog, SWAP);
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

        assert!(err.to_string().contains(IB_REFUSES), "{err:#}");
    }

    /// The node's fill fails at its first step, before any node: its catalog
    /// path is not UTF-8 (`NodeFill::run`). OKX has something to ask, its
    /// definition stored, and IB would refuse the end. The fill stops on the
    /// node's failure, so IB comes after the node's run.
    #[tokio::test]
    async fn the_node_s_fill_runs_before_ib_connects() {
        let dir = tempfile::tempdir().unwrap();
        let catalog_dir = dir.path().join(OsStr::from_bytes(b"not-utf8-\xFF"));
        // Where the catalog opens it: its path, made UTF-8.
        std::fs::create_dir(catalog_dir.to_string_lossy().as_ref()).unwrap();
        let mut catalog = ParquetDataCatalog::new(&catalog_dir, None, None, None, None);
        store_okx_definition(&mut catalog, SWAP);
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

        assert!(err.to_string().contains("not UTF-8"), "{err:#}");
    }

    /// Before anything connects: an IB instrument beside it would connect
    /// IB, which would refuse the end.
    #[tokio::test]
    async fn custom_data_of_no_instrument_that_is_not_lse_s_rows_fails_the_fill_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let requirements = DataRequirements {
            custom: vec![DataType::new(
                "Imbalance",
                None,
                Some("GC.v.0.GLBX".to_string()),
            )],
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

        let err = err.to_string();
        assert!(
            err.contains("Imbalance") && err.contains("no instrument"),
            "{err}"
        );
    }

    /// Every series of a fill is checked against its venue's history
    /// before anything connects: OKX's quotes, which its client does not
    /// request, and a cTrader broker's bars, its history being quotes, each
    /// fail the fill naming them, before OKX writes a definition, which
    /// would read its account's environment, missing here, before a
    /// cTrader broker's connection is read, and before IB, which would
    /// refuse the end.
    #[tokio::test]
    async fn a_series_its_venue_does_not_serve_fails_the_fill_before_anything_connects() {
        for (requirements, refused) in [
            (
                ticks(&[
                    (NautilusDataType::QuoteTick, SWAP),
                    (NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
                    (NautilusDataType::TradeTick, "SXR8.IBIS"),
                ]),
                SWAP,
            ),
            (
                reading(&[
                    "XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL",
                    "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL",
                ]),
                "XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL",
            ),
        ] {
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
            assert!(
                err.contains(refused) && err.contains("serves") && !err.contains(IB_REFUSES),
                "{refused}: {err}"
            );
        }
    }

    /// IB holds no bars of 30 seconds or less older than six months: a
    /// fill of them reaching earlier fails naming that before IB connects,
    /// which would refuse the end.
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

    /// The instruments and slices of a fill of gold's quotes on `venue`.
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

    #[test]
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
            instrument_id: InstrumentId::from("XAUUSD.PEPPERSTONE"),
            from: at(FILL_START),
            to: at(FILL_END),
        };
        assert_eq!(
            node.asked(),
            [
                (asked(FillData::Instrument), None),
                (asked(FillData::Quotes), Some(ctrader::TICK_REQUEST_SPAN)),
            ]
        );
    }

    #[test]
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

    /// GC.GLBX rolls from GCQ26 to GCZ26 at 00:00 UTC on 29 July 2026: its
    /// slices are LSE's, contract by contract, as LSEDGE's are.
    #[test]
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
                .map(|slice| slice.slice.instrument_id.to_string())
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

    /// Only a venue no broker trades, LSEDGE, has none.
    #[test]
    fn every_venue_has_a_broker_but_lsedge() {
        for (venue, has) in [
            ("ICMARKETS", true),
            ("OKX", true),
            ("GLBX", true),
            ("IBIS", true),
            ("LSEDGE", false),
        ] {
            assert_eq!(has_broker(Venue::new(venue)), has, "{venue}");
        }
    }

    fn live_args() -> LiveVenueArgs {
        LiveVenueArgs {
            ib: ib::IbLiveArgs {
                account: None,
                client_id: 1,
            },
        }
    }

    /// Named Pepperstone first, IC is read first; and no OKX client is set
    /// up, OKX coming first of the node's clients: its environment, which
    /// this would otherwise fail on, is never asked for.
    #[tokio::test]
    async fn a_run_checks_the_ctrader_brokers_bars_in_their_table_s_order_before_anything_connects()
    {
        let requirements = reading(&[
            "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL",
            "XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL",
            "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL",
        ]);
        let ids = [
            InstrumentId::from("BTC-USDT-SWAP.OKX"),
            InstrumentId::from("XAUUSD.PEPPERSTONE"),
            InstrumentId::from("XAUUSD.ICMARKETS"),
        ];

        let Err(err) = live_clients(&requirements, &ids, &live_args(), Path::new("catalog")).await
        else {
            panic!("set up clients for bars cTrader does not serve");
        };

        assert!(
            err.to_string().contains("ICMARKETS serves quotes only"),
            "{err:#}"
        );
    }

    /// The cTrader brokers' accounts hedge; IB's and OKX's net, whether the
    /// venue has a row of its own, as GLBX has, or none; LSEDGE, which no
    /// broker trades, keeps positions as Nautilus' venue default.
    #[test]
    fn each_venue_keeps_positions_as_the_account_of_its_live_broker_does() {
        for (venue, kept) in [
            ("ICMARKETS", OmsType::Hedging),
            ("PEPPERSTONE", OmsType::Hedging),
            ("GLBX", OmsType::Netting),
            ("OKX", OmsType::Netting),
            ("IBIS", OmsType::Netting),
            ("CME", OmsType::Netting),
            ("NASDAQ", OmsType::Netting),
            ("LSEETF", OmsType::Netting),
            ("IDEALPRO", OmsType::Netting),
            ("BINANCE", OmsType::Netting),
            ("LSEDGE", OmsType::Unspecified),
        ] {
            assert_eq!(oms_type(Venue::new(venue)), kept, "{venue}");
        }
    }

    /// IB's adapter reports IB's net liquidation as its account's balance,
    /// holding its open positions' P&L, GLBX's and every venue's without a
    /// row of its own; a cTrader broker's balance and OKX's cash leave
    /// them out; LSEDGE, which no broker trades, has no account of a
    /// broker.
    #[test]
    fn each_venue_reads_its_equity_as_its_live_broker_reports_it() {
        for (venue, source) in [
            ("ICMARKETS", EquitySource::Portfolio),
            ("PEPPERSTONE", EquitySource::Portfolio),
            ("OKX", EquitySource::Portfolio),
            ("GLBX", EquitySource::BalanceTotals),
            ("IBIS", EquitySource::BalanceTotals),
            ("IDEALPRO", EquitySource::BalanceTotals),
            ("LSEDGE", EquitySource::Portfolio),
        ] {
            assert_eq!(equity_source(Venue::new(venue)), source, "{venue}");
        }
    }

    /// A cTrader account's state carries its deposit currency as its base
    /// (`ctrader`'s execution client); OKX's adapter reports none, nor does
    /// IB's execution client as helix configures it, GLBX's and every
    /// venue's without a row of its own; LSEDGE, which no broker trades,
    /// has no account of a broker.
    #[test]
    fn each_venue_has_a_base_currency_as_the_account_of_its_live_broker_has() {
        for (venue, based) in [
            ("ICMARKETS", true),
            ("PEPPERSTONE", true),
            ("OKX", false),
            ("GLBX", false),
            ("IBIS", false),
            ("IDEALPRO", false),
            ("LSEDGE", false),
        ] {
            assert_eq!(has_base_currency(Venue::new(venue)), based, "{venue}");
        }
    }

    /// A pair's rates are read in the first form its venue's history serves
    /// of its 1-minute BID and ASK bars, then its quotes: a cTrader
    /// broker's as its quotes, serving no bars; IB's, an IDEALPRO pair's,
    /// IDEALPRO having no row of its own, as its bars, which IB serves
    /// beside its quotes.
    #[test]
    fn each_venue_s_pairs_are_read_in_the_first_form_its_history_serves() {
        let quotes = |pair: &str| {
            vec![Series::Ticks(TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(pair),
            })]
        };
        let bars = |pair: &str| {
            ["BID", "ASK"]
                .map(|side| Series::Bars(BarType::from(format!("{pair}-1-MINUTE-{side}-EXTERNAL"))))
                .to_vec()
        };
        for (pair, read) in [
            ("EURUSD.ICMARKETS", quotes("EURUSD.ICMARKETS")),
            ("EURUSD.PEPPERSTONE", quotes("EURUSD.PEPPERSTONE")),
            ("EUR/USD.IDEALPRO", bars("EUR/USD.IDEALPRO")),
        ] {
            assert_eq!(
                rate_series(InstrumentId::from(pair)).unwrap(),
                read,
                "{pair}"
            );
        }
    }

    /// OKX's history serves no BID or ASK bars, its candles being of trades,
    /// and no quotes: no rate of an OKX pair can be read, which names the
    /// pair and its venue.
    #[test]
    fn a_pair_whose_venue_serves_neither_form_has_no_rate_series() {
        let err = rate_series(InstrumentId::from("USDC-USDT.OKX"))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("USDC-USDT.OKX") && err.contains("OKX's history"),
            "{err}"
        );
    }

    /// The external bars of `bar_type`, a source as it is.
    fn bars_of(bar_type: &str) -> CatalogSeries {
        CatalogSeries::Bars(BarType::from(bar_type))
    }

    /// The `data` of `instrument_id`, a source as it is.
    fn ticks_of(data: NautilusDataType, instrument_id: &str) -> CatalogSeries {
        CatalogSeries::Ticks(TickSeries {
            data,
            instrument_id: InstrumentId::from(instrument_id),
        })
    }

    /// Asserts each of `served` is served and each of `refused` refused,
    /// the error naming it.
    fn assert_serves(served: &[CatalogSeries], refused: &[CatalogSeries]) {
        for source in served {
            if let Err(err) = serves(source) {
                panic!("{source} refused: {err:#}");
            }
        }
        for source in refused {
            let Err(err) = serves(source) else {
                panic!("{source} served");
            };
            let (_, identifier) = source.key();
            assert!(
                format!("{err:#}").contains(&identifier),
                "{source}: {err:#}"
            );
        }
    }

    /// A cTrader broker's history is its tick history, of quotes alone, as
    /// its adapter serves (`ctrader::quoted`).
    #[test]
    fn a_ctrader_broker_s_history_serves_its_quotes_alone() {
        use NautilusDataType as D;
        assert_serves(
            &[
                ticks_of(D::QuoteTick, "XAUUSD.ICMARKETS"),
                ticks_of(D::QuoteTick, "EURUSD.PEPPERSTONE"),
            ],
            &[
                ticks_of(D::TradeTick, "XAUUSD.ICMARKETS"),
                ticks_of(D::OrderBookDelta, "XAUUSD.PEPPERSTONE"),
                bars_of("XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL"),
                bars_of("EURUSD.PEPPERSTONE-1-MINUTE-LAST-EXTERNAL"),
            ],
        );
    }

    /// Nautilus' OKX data client requests trades and candles alone
    /// (`request_trades`, `request_bars`), no quotes and no book: its
    /// candles are of trade prices, at a width OKX has a candle of
    /// (`bar_spec_as_okx_channel`), whatever price a bar type names.
    #[test]
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

    /// LSE's client asks LSE for the quotes, trades and books of an
    /// instrument of its venues, but an index's trades, which it has none
    /// of, and for its candles of an LSEDGE series and an OPRA contract
    /// alone, EXTERNAL bars of the LAST price below a day wide
    /// (`lse::serves`).
    #[test]
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

    /// Nautilus' IB client serves IB's own time bars of every size IB has
    /// but a month's, which its history client parses none of, priced
    /// LAST, BID, ASK or MID (its TRADES, BID, ASK and MIDPOINT),
    /// and IB's historical ticks, BID_ASK as quotes and TRADES as trades,
    /// every venue without a row of its own; no book, no other price and
    /// no size IB has not.
    #[test]
    fn ib_s_history_serves_its_time_bars_quotes_and_trades() {
        use NautilusDataType as D;
        assert_serves(
            &[
                bars_of("SXR8.IBIS-1-MINUTE-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-MINUTE-MID-EXTERNAL"),
                bars_of("EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL"),
                bars_of("EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL"),
                bars_of("SXR8.IBIS-1-SECOND-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-30-SECOND-BID-EXTERNAL"),
                bars_of("ESZ26.CME-2-MINUTE-LAST-EXTERNAL"),
                bars_of("ESZ26.CME-20-MINUTE-LAST-EXTERNAL"),
                bars_of("ESZ26.CME-8-HOUR-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-DAY-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-WEEK-LAST-EXTERNAL"),
                ticks_of(D::QuoteTick, "SXR8.IBIS"),
                ticks_of(D::QuoteTick, "EUR/USD.IDEALPRO"),
                ticks_of(D::TradeTick, "ESZ26.CME"),
            ],
            &[
                bars_of("SXR8.IBIS-1-MINUTE-MARK-EXTERNAL"),
                bars_of("SXR8.IBIS-4-MINUTE-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-2-SECOND-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-MONTH-LAST-EXTERNAL"),
                bars_of("SXR8.IBIS-1-MINUTE-LAST-INTERNAL"),
                bars_of("SXR8.IBIS-100-TICK-LAST-EXTERNAL"),
                ticks_of(D::OrderBookDepth, "SXR8.IBIS"),
                ticks_of(D::OrderBookDelta, "ESZ26.CME"),
            ],
        );
    }

    /// A history request of each kind a data client takes, of `id`, its
    /// bars `id`'s 1-minute LAST bars, over an hour, as a fill or a warmup
    /// sends it: the kind's name, the source it asks for and the request.
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

    /// Whether `client` refuses `request` as a kind it does not take:
    /// Nautilus' `DataClient` default refusal (`does not support`), which
    /// a client implementing the kind never gives.
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

    /// `serves` against the data clients its answers copy: each history
    /// source's client, built as its fill builds it and unconnected, on
    /// unreachable addresses, refuses as a kind it does not take exactly
    /// the kinds of request of which `serves` refuses the source of every
    /// instrument of its venues tried; a kind it takes the source of one
    /// of them, it does not. Each kind is tried on the 1-minute LAST bars
    /// of each instrument, its quotes, its trades and its book; a bar type
    /// a client takes but `serves` refuses (a price, a width) is not
    /// checked here.
    #[tokio::test]
    async fn each_history_source_s_client_takes_the_kinds_of_request_its_history_serves() {
        let (data_tx, _data_events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let unreachable = "http://127.0.0.1:1".to_string();
        let ids = |ids: &[&str]| {
            ids.iter()
                .map(|&id| InstrumentId::from(id))
                .collect::<Vec<_>>()
        };
        let okx = NodeClient {
            factory: Box::new(OKXDataClientFactory::new()),
            config: Box::new(nautilus_okx::config::OKXDataClientConfig {
                base_url_http: Some(unreachable.clone()),
                ..Default::default()
            }),
        };
        let lse = NodeClient {
            factory: Box::new(LseDataClientFactory::new()),
            config: Box::new(
                LseDataClientConfig::builder()
                    .api_key(SecretString::from(::lse::testing::API_KEY))
                    .cme_api_key(SecretString::from(::lse::testing::CME_API_KEY))
                    .nasdaq_api_key(SecretString::from(::lse::testing::NASDAQ_API_KEY))
                    .base_url_http(unreachable)
                    .build(),
            ),
        };
        let ib = ib::offline_clients(&ids(&["SXR8.IBIS"])).into_node_clients(&[]);
        let ib = NodeClient {
            factory: ib.data_factory,
            config: ib.data_config,
        };
        let ctrader = ctrader_node_client(demo_connection(ic::BROKER), ids(&["XAUUSD.ICMARKETS"]));
        let mut drift = Vec::new();
        for (client, venues) in [
            (ctrader, ids(&["XAUUSD.ICMARKETS"])),
            (okx, ids(&[SWAP])),
            (
                lse,
                ids(&[
                    "GOLD-FUTURES.LSEDGE",
                    "GCZ26.GLBX",
                    "NVDA.XNAS",
                    "AAPL  261120C00355000.OPRA",
                ]),
            ),
            (ib, ids(&["SXR8.IBIS"])),
        ] {
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
                    drift.push(format!("{name}'s {kind}: served {served}"));
                }
            }
        }

        assert_eq!(drift, Vec::<String>::new());
    }

    /// IB's execution client denies reduce-only and post-only orders, GLBX's
    /// and every venue's without a row of its own.
    #[test]
    fn each_venue_takes_reduce_only_and_post_only_orders_as_its_live_broker_does() {
        for (venue, takes) in [
            ("ICMARKETS", true),
            ("PEPPERSTONE", true),
            ("OKX", true),
            ("GLBX", false),
            ("IBIS", false),
            ("IDEALPRO", false),
            ("LSEDGE", false),
        ] {
            assert_eq!(
                takes_reduce_only_and_post_only(Venue::new(venue)),
                takes,
                "{venue}"
            );
        }
    }

    /// IB's execution client links a bracket's exits in an OCA group only
    /// when they are tagged with one, GLBX's and every venue's without a
    /// row of its own; no other broker reads IB's tags, and LSEDGE, which
    /// no broker trades, sends no order.
    #[test]
    fn each_venue_tags_a_brackets_exits_as_its_live_broker_links_them() {
        let entry = ClientOrderId::from("O-1");
        for (venue, tagged) in [
            ("ICMARKETS", false),
            ("PEPPERSTONE", false),
            ("OKX", false),
            ("GLBX", true),
            ("IBIS", true),
            ("IDEALPRO", true),
            ("LSEDGE", false),
        ] {
            assert_eq!(
                bracket_exits_tag(Venue::new(venue), entry).is_some(),
                tagged,
                "{venue}"
            );
        }
    }

    /// GLBX takes its definitions from Databento, its history and live data
    /// from LSE and its orders from IB, and XNAS and OPRA the same; OKX and
    /// each cTrader broker take all four from themselves; LSEDGE its
    /// definitions from the catalog and its history and live data from LSE,
    /// with no broker; and every other venue all four from IB.
    #[test]
    fn each_instrument_routes_to_the_row_of_its_venue() {
        let okx = Row {
            definitions: DefinitionsSource::Okx,
            history: HistorySource::Okx,
            live_data: LiveDataSource::Okx,
            broker: Some(Broker::Okx),
        };
        let ctrader = |broker| Row {
            definitions: DefinitionsSource::CTrader(broker),
            history: HistorySource::CTrader(broker),
            live_data: LiveDataSource::CTrader(broker),
            broker: Some(Broker::CTrader(broker)),
        };
        // An exchange's listings, under the ids Databento gives them, CME
        // Globex's, Nasdaq TotalView's and US options', streamed from LSE;
        // a row is its venue's, so a GLBX root and a Databento continuous
        // symbol take GLBX's too, which LSE's client refuses the latter of,
        // serving dated contracts alone.
        let exchange = Row {
            definitions: DefinitionsSource::Databento,
            history: HistorySource::Lse,
            live_data: LiveDataSource::Lse,
            broker: Some(Broker::Ib),
        };
        let ib = Row {
            definitions: DefinitionsSource::Ib,
            history: HistorySource::Ib,
            live_data: LiveDataSource::Ib,
            broker: Some(Broker::Ib),
        };
        let lsedge = Row {
            definitions: DefinitionsSource::Catalog,
            history: HistorySource::Lse,
            live_data: LiveDataSource::Lse,
            broker: None,
        };
        for (id, row) in [
            ("GOLD-FUTURES.LSEDGE", lsedge),
            ("US10Y-FUTURES.LSEDGE", lsedge),
            ("BTC-USDT-SWAP.OKX", okx),
            ("XAUUSD.ICMARKETS", ctrader(ic::BROKER)),
            ("XAUUSD.PEPPERSTONE", ctrader(pepperstone::BROKER)),
            ("GCZ26.GLBX", exchange),
            ("GC.GLBX", exchange),
            ("GC.v.0.GLBX", exchange),
            ("NVDA.XNAS", exchange),
            ("BRK B.XNAS", exchange),
            ("AAPL  261120C00355000.OPRA", exchange),
            ("SXR8.IBIS", ib),
            ("ESZ26.CME", ib),
            ("GCZ26.XCEC", ib),
            ("EUR/USD.IDEALPRO", ib),
            ("BTC-USDT-SWAP.BINANCE", ib),
        ] {
            let id = InstrumentId::from(id);
            assert_eq!(Row::of(id.venue), row, "{id}");
        }
    }

    /// GLBX's, XNAS's and OPRA's data from LSE's client and their orders
    /// through IB's, which loads their instruments; LSEDGE's data from LSE's
    /// alone; every other venue's from its broker's, IB's routed its venues
    /// alone.
    #[test]
    fn each_instrument_goes_to_the_clients_of_its_live_data_and_of_its_broker() {
        let routes = LiveRoutes::of(&[
            InstrumentId::from("GCZ26.GLBX"),
            InstrumentId::from("GOLD-FUTURES.LSEDGE"),
            InstrumentId::from("SXR8.IBIS"),
            InstrumentId::from("NVDA.XNAS"),
            InstrumentId::from("AAPL  261120C00355000.OPRA"),
            InstrumentId::from("BTC-USDT-SWAP.OKX"),
            InstrumentId::from("XAUUSD.ICMARKETS"),
        ]);

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
            }
        );
    }

    /// Its data is LSE's, but its orders IB's: refused before anything
    /// connects, as an IB instrument's run is.
    #[tokio::test]
    async fn a_run_reading_glbx_needs_the_ib_account_its_orders_go_to() {
        let requirements = ticks(&[(NautilusDataType::QuoteTick, "GCZ26.GLBX")]);

        let Err(err) = live_clients(
            &requirements,
            &requirements.instrument_ids(),
            &live_args(),
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up GLBX's clients without IB's account");
        };

        assert!(err.to_string().contains("IB account"), "{err:#}");
    }

    /// LSE's data client, routed GLBX, streams the contract.
    #[test]
    fn lse_s_client_streams_glbx_contracts_routed_glbx_with_no_execution_client() {
        let clients = lse::live_clients(&[
            InstrumentId::from("GCZ26.GLBX"),
            InstrumentId::from("GCG27.GLBX"),
        ]);

        assert_eq!(clients.data_factory.name(), "LSE");
        assert!(clients.exec.is_none());
        assert_eq!(
            (clients.data_routing.default, clients.data_routing.venues),
            (false, Some(vec!["GLBX".to_string()]))
        );
        let config = clients
            .data_config
            .as_any()
            .downcast_ref::<LseDataClientConfig>()
            .unwrap();
        assert_eq!(
            config.stream_ids,
            [
                InstrumentId::from("GCZ26.GLBX"),
                InstrumentId::from("GCG27.GLBX")
            ]
        );
    }

    /// Its data is LSE's, but its orders IB's: refused before anything
    /// connects, as an IB instrument's run is.
    #[tokio::test]
    async fn a_run_reading_an_option_needs_the_ib_account_its_orders_go_to() {
        let requirements = ticks(&[(NautilusDataType::TradeTick, "AAPL  261120C00355000.OPRA")]);

        let Err(err) = live_clients(
            &requirements,
            &requirements.instrument_ids(),
            &live_args(),
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up OPRA's clients without IB's account");
        };

        assert!(err.to_string().contains("IB account"), "{err:#}");
    }

    /// LSE's data client, routed OPRA, streams the contracts, the trades of
    /// each through its underlying's chain.
    #[test]
    fn lse_s_client_streams_option_contracts_routed_opra_with_no_execution_client() {
        let clients = lse::live_clients(&[
            InstrumentId::from("AAPL  261120C00355000.OPRA"),
            InstrumentId::from("AAPL  261120P00355000.OPRA"),
        ]);

        assert_eq!(clients.data_factory.name(), "LSE");
        assert!(clients.exec.is_none());
        assert_eq!(
            (clients.data_routing.default, clients.data_routing.venues),
            (false, Some(vec!["OPRA".to_string()]))
        );
        let config = clients
            .data_config
            .as_any()
            .downcast_ref::<LseDataClientConfig>()
            .unwrap();
        assert_eq!(
            config.stream_ids,
            [
                InstrumentId::from("AAPL  261120C00355000.OPRA"),
                InstrumentId::from("AAPL  261120P00355000.OPRA")
            ]
        );
    }

    /// A live node with `clients`, added as `Live` adds them
    /// (`live::node::add_clients`), on Nautilus' defaults otherwise: no
    /// streaming, whose writer drops through Nautilus' runtime when a test's
    /// thread ends.
    fn live_node(clients: Vec<Clients>) -> anyhow::Result<LiveNode> {
        let config = LiveNodeConfig {
            environment: Environment::Live,
            trader_id: TraderId::from("HELIX-001"),
            ..Default::default()
        };
        crate::live::node::add_clients(LiveNodeBuilder::from_config(config)?, clients)?.build()
    }

    /// The clients `live_clients` sets up for a run reading `ids`, LSE's and
    /// IB's, IB's on a gateway that is not started and LSE's given the test
    /// keys a run reads from the environment: created, they connect to
    /// nothing.
    fn lse_and_ib_clients(ids: &[InstrumentId]) -> Vec<Clients> {
        let routes = LiveRoutes::of(ids);
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

    /// The client the node routes `venue`'s data to, and the one it takes
    /// for the default.
    fn data_clients(node: &LiveNode, venue: &str) -> (Option<ClientId>, Option<ClientId>) {
        let mut engine = node.kernel().data_engine.borrow_mut();
        (
            engine
                .get_client(None, Some(&Venue::new(venue)))
                .map(|client| client.client_id),
            engine.get_client(None, None).map(|client| client.client_id),
        )
    }

    /// The client the node sends an order of `instrument_id` to.
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

    /// Builds the live node of a run reading `ids`, LSE's and IB's clients
    /// among its own, until Nautilus' builder has registered each of the
    /// two first, which it does in no set order (`LiveNodeBuilder`'s clients
    /// are a `HashMap`): each build must succeed and route each venue's
    /// data and orders as `routed` says, a venue and the client of its data
    /// and of its orders. The first client on no venue the builder
    /// registers becomes the default (Nautilus' `DataEngine::register_client`),
    /// and LSE's and IB's are both on no venue.
    fn assert_routed_whichever_registers_first(ids: &[&str], routed: &[(&str, &str, &str, &str)]) {
        let ids: Vec<InstrumentId> = ids.iter().map(|&id| InstrumentId::from(id)).collect();
        let mut first = HashSet::new();
        for _ in 0..64 {
            let mut node = live_node(lse_and_ib_clients(&ids))
                .unwrap_or_else(|e| panic!("the node of {ids:?} not built: {e:#}"));
            for &(venue, instrument_id, data, orders) in routed {
                let (client, default) = data_clients(&node, venue);
                assert_eq!(client, Some(ClientId::from(data)), "{venue}'s data");
                assert_eq!(
                    order_client(&node, instrument_id),
                    [ClientId::from(orders)],
                    "{venue}'s orders"
                );
                first.extend(default);
            }
            node.dispose();
            if first.len() == 2 {
                return;
            }
        }
        panic!("one client registered first in 64 builds: {first:?}");
    }

    /// GLBX's, XNAS's and OPRA's data from LSE's client, their orders
    /// through IB's.
    #[tokio::test]
    async fn an_exchange_run_s_node_routes_its_data_to_lse_and_its_orders_to_ib_whichever_registers_first()
     {
        assert_routed_whichever_registers_first(
            &[
                "GCZ26.GLBX",
                "GCG27.GLBX",
                "NVDA.XNAS",
                "AAPL  261120C00355000.OPRA",
            ],
            &[
                ("GLBX", "GCZ26.GLBX", "LSE", "IB"),
                ("XNAS", "NVDA.XNAS", "LSE", "IB"),
                ("OPRA", "AAPL  261120C00355000.OPRA", "LSE", "IB"),
            ],
        );
    }

    /// LSEDGE's data from LSE's client, an IB venue's data and orders from
    /// IB's.
    #[tokio::test]
    async fn an_lsedge_and_ib_run_s_node_routes_each_venue_to_its_client_whichever_registers_first()
    {
        assert_routed_whichever_registers_first(
            &["GOLD-FUTURES.LSEDGE", "SXR8.IBIS"],
            &[
                ("LSEDGE", "GOLD-FUTURES.LSEDGE", "LSE", "IB"),
                ("IBIS", "SXR8.IBIS", "IB", "IB"),
            ],
        );
    }

    /// The table names its venues as their adapters do.
    #[test]
    fn the_table_s_venues_are_their_adapters() {
        assert_eq!(Row::of(*OKX_VENUE).history, HistorySource::Okx);
        assert_eq!(
            Row::of(Venue::GLBX()).definitions,
            DefinitionsSource::Databento
        );
        assert_eq!(Row::of(*LSEDGE_VENUE).history, HistorySource::Lse);
    }

    /// LSE's data client alone, on the route of LSEDGE, not made the
    /// default; its key is read when the node creates it, so nothing is read
    /// or connected here.
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

    /// The broker's data client where a broker trades the venue, GLBX's and
    /// XNAS's IB among them; LSEDGE's LSE, which streams it. Each by the
    /// name its factory gives the client, which the node registers it under.
    #[test]
    fn a_definition_is_asked_of_the_venue_s_broker_or_else_of_its_live_data_client() {
        for (id, client) in [
            ("BTC-USDT-SWAP.OKX", "OKX"),
            ("XAUUSD.ICMARKETS", "ICMARKETS"),
            ("XAUUSD.PEPPERSTONE", "PEPPERSTONE"),
            ("GCZ26.GLBX", "IB"),
            ("NVDA.XNAS", "IB"),
            ("AAPL  261120C00355000.OPRA", "IB"),
            ("SXR8.IBIS", "IB"),
            ("GOLD-FUTURES.LSEDGE", "LSE"),
        ] {
            let id = InstrumentId::from(id);

            assert_eq!(
                live_definitions_client(id.venue),
                ClientId::from(client),
                "{id}"
            );
        }
        let lse = lse::live_clients(&[InstrumentId::from("GOLD-FUTURES.LSEDGE")]);
        assert_eq!(lse.data_factory.name(), "LSE");
    }

    /// LSE streams none of them: each refused naming it before anything
    /// connects, OKX's clients among them, which would fail on its missing
    /// environment. A GLBX contract's external bars, which IB streamed
    /// before LSE took GLBX's live data, an LSEDGE series' trades, LSE's
    /// sockets giving quotes, an option's quotes, LSE streaming its trades
    /// alone, an LSEDGE series' book, and custom data of a rolled root, read
    /// on the contracts the run loads.
    #[tokio::test]
    async fn a_run_reading_a_series_lse_does_not_stream_is_refused_naming_it_before_anything_connects()
     {
        let gc_imbalances = TickSeries {
            data: NautilusDataType::Custom {
                type_name: "Imbalance".to_string(),
            },
            instrument_id: InstrumentId::from("GC.GLBX"),
        };
        for (read, series) in [
            (
                reading(&["GCZ26.GLBX-1-MINUTE-LAST-EXTERNAL"]),
                "GCZ26.GLBX-1-MINUTE-LAST-EXTERNAL",
            ),
            (
                ticks(&[(NautilusDataType::TradeTick, "GOLD-FUTURES.LSEDGE")]),
                "GOLD-FUTURES.LSEDGE",
            ),
            (
                reading(&["GOLD-FUTURES.LSEDGE-1-MINUTE-LAST-INTERNAL"]),
                "GOLD-FUTURES.LSEDGE",
            ),
            (
                ticks(&[(NautilusDataType::QuoteTick, "AAPL  261120C00355000.OPRA")]),
                "AAPL  261120C00355000.OPRA",
            ),
            (
                ticks(&[(NautilusDataType::OrderBookDepth, "GOLD-FUTURES.LSEDGE")]),
                "GOLD-FUTURES.LSEDGE",
            ),
            (
                DataRequirements {
                    ticks: vec![gc_imbalances],
                    ..ticks(&[])
                },
                "GC.GLBX",
            ),
        ] {
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

            let Err(err) =
                live_clients(&requirements, &loaded, &live_args(), Path::new("catalog")).await
            else {
                panic!("set up clients for {series}");
            };

            let err = format!("{err:#}");
            assert!(
                err.contains(series) && err.contains("LSE streams no"),
                "{series}: {err}"
            );
        }
    }

    /// LSE streams none yet: refused before anything connects, OKX's
    /// clients among them, which would fail on its missing environment.
    #[tokio::test]
    async fn a_run_reading_custom_data_of_no_instrument_is_refused_naming_it_before_anything_connects()
     {
        let us10y = LseRow::data_type("bond_yields", "US10Y");
        let requirements = DataRequirements {
            custom: vec![us10y.clone()],
            ..ticks(&[(NautilusDataType::QuoteTick, "BTC-USDT-SWAP.OKX")])
        };

        let Err(err) = live_clients(
            &requirements,
            &requirements.instrument_ids(),
            &live_args(),
            Path::new("catalog"),
        )
        .await
        else {
            panic!("set up clients for custom data of no instrument");
        };

        assert!(err.to_string().contains(&us10y.to_string()), "{err:#}");
    }
}
