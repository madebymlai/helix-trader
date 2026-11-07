//! The venues: each one's history, live clients and command-line settings
//! in a module of its own, and the routing table, which says which history
//! source fills an instrument's series (`HistorySource`) and which broker
//! trades it live (`LiveBroker`), both by its venue. Everything else asks
//! here and names no venue: fill-catalog for its fill (`fill_history`), the
//! live runner for its node's clients (`live_clients`), a backtest for how a
//! venue keeps positions (`oms_type`, its broker's answer), a trade whether
//! its orders may be reduce-only and post-only
//! (`takes_reduce_only_and_post_only`, its broker's answer); venue code checks
//! its own instruments against the table. The settings each command takes of
//! the venues are composed here, `LiveVenueArgs` for live and
//! `HistoryVenueArgs` for fill-catalog, which the commands flatten.
//!
//! The cTrader brokers are one kind (`CTraderBroker`), which both dispatches
//! loop over: each one's module names its venue and account, and its row of
//! the table is a line of `CTRADER_BROKERS`, all another such broker takes.
//! What they share is the adapter's (`ctrader`), its clients built for helix
//! here (`ctrader_node_client`, `ctrader_clients`).
//!
//! No trait stands for a venue: Nautilus' factories, configs and
//! `RoutingConfig` are already the seam, and the venues' needs differ too much
//! for a trait to hide them. OKX needs a leverage, IB an account, a client id
//! and its fill after the node's, Databento an API key and its own check.

pub mod databento;
pub mod ib;
pub mod ic;
pub mod okx;
pub mod pepperstone;

use std::path::Path;

use anyhow::Context;
use clap::Args;
use ctrader::{
    CTraderAccountVars, CTraderConnection, CTraderDataClientConfig, CTraderDataClientFactory,
    CTraderExecutionClientConfig, CTraderExecutionClientFactory,
};
use jiff::Timestamp;
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::NautilusDataType,
    enums::OmsType,
    identifiers::{InstrumentId, Venue},
};
use nautilus_okx::common::consts::OKX_VENUE;
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

use self::{databento::DatabentoHistory, ib::IbHistory, okx::OkxHistory};
use crate::{
    catalog::{NodeClient, NodeFill, missing},
    live::node::Clients,
    strategy::{DataRequirements, SeriesSlice, catalog_series},
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

/// The source of an instrument's history in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistorySource {
    /// OKX's public API, through Nautilus' OKX data client.
    Okx,
    /// A cTrader broker's tick history.
    CTrader(CTraderBroker),
    /// Databento's `GLBX.MDP3` dataset.
    Databento,
    /// Interactive Brokers' historical bars.
    Ib,
}

/// The broker of an instrument on the live node: its data and execution
/// clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveBroker {
    /// OKX's clients.
    Okx,
    /// A cTrader broker's clients, on a hedging account.
    CTrader(CTraderBroker),
    /// Interactive Brokers' clients, on the gateway.
    Ib,
}

impl HistorySource {
    /// The history source of `id`: its venue's row (see `row`).
    #[must_use]
    pub(crate) fn of(id: InstrumentId) -> Self {
        row(id.venue).0
    }
}

impl LiveBroker {
    /// The live broker of `id`: its venue's row (see `row`).
    #[must_use]
    pub(crate) fn of(id: InstrumentId) -> Self {
        row(id.venue).1
    }

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
}

/// The routing table's row of `venue`: its history source and its live
/// broker. A venue of `CTRADER_BROKERS` has both from its broker. GLBX, CME
/// Globex as Databento names it, has its history from Databento and its live
/// broker in IB, Databento having no live clients here; every venue without
/// a row of its own is IB's, history and live.
fn row(venue: Venue) -> (HistorySource, LiveBroker) {
    if venue == *OKX_VENUE {
        (HistorySource::Okx, LiveBroker::Okx)
    } else if let Some(&broker) = CTRADER_BROKERS
        .iter()
        .find(|broker| venue.as_str() == broker.venue)
    {
        (HistorySource::CTrader(broker), LiveBroker::CTrader(broker))
    } else if venue == Venue::GLBX() {
        (HistorySource::Databento, LiveBroker::Ib)
    } else {
        (HistorySource::Ib, LiveBroker::Ib)
    }
}

/// How the accounts of `venue` keep positions: as those of its live broker
/// do (`LiveBroker::oms_type`), so every venue without a row of its own as
/// IB's. A backtest's normal run simulates each venue so, as Nautilus' docs
/// ask of a backtest venue.
#[must_use]
pub fn oms_type(venue: Venue) -> OmsType {
    row(venue).1.oms_type()
}

/// Whether the live broker of `venue` takes reduce-only and post-only
/// orders (`LiveBroker::takes_reduce_only_and_post_only`), so every venue
/// without a row of its own as IB. A trade asks it in a backtest as live,
/// so that a backtest sends the orders live sends.
#[must_use]
pub fn takes_reduce_only_and_post_only(venue: Venue) -> bool {
    row(venue).1.takes_reduce_only_and_post_only()
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
/// `NodeFill::add`), on `connection`, loading `instrument_ids`. It names its
/// venue, so Nautilus routes the broker's requests to it without a route of
/// ours.
#[must_use]
fn ctrader_node_client(
    connection: CTraderConnection,
    instrument_ids: Vec<InstrumentId>,
) -> NodeClient {
    let config = CTraderDataClientConfig::new(connection, instrument_ids);
    NodeClient {
        factory: Box::new(CTraderDataClientFactory::new(&config)),
        config: Box::new(config),
        routing: RoutingConfig::default(),
    }
}

/// Adds to `node` the fill of `series`, the quotes of the instruments of the
/// cTrader broker of `venue`, over what the catalog misses of `slices`, a
/// window holding at most what one tick request spans. The connection is
/// read only if something is missing, so a complete catalog needs no
/// credential.
///
/// # Errors
///
/// Returns an error if a series is not the quotes of an instrument on
/// `venue`, if the catalog cannot be read, or as `connection`.
fn add_ctrader_fill(
    node: &mut NodeFill,
    catalog: &ParquetDataCatalog,
    venue: &str,
    connection: impl FnOnce() -> anyhow::Result<CTraderConnection>,
    series: &[(NautilusDataType, String)],
    slices: &[SeriesSlice],
) -> anyhow::Result<()> {
    let instrument_ids = ctrader::quoted(Venue::new(venue), series)?;
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
        .filter(|slice| HistorySource::of(slice.slice.instrument_id) == source)
        .cloned()
        .collect()
}

/// Fills the catalog in `catalog_dir` with the instruments and the series
/// `requirements` reads over `[start, end]`, those its bars are built from
/// and its ticks (see `DataRequirements::catalog_series`), each on a contract
/// over its slice (`DataRequirements::slices`), every history source
/// (`HistorySource`) given its slices. Databento's, the cTrader brokers' and
/// OKX's fill through a Nautilus node (see `catalog::NodeFill`), by Databento's
/// data client for GLBX instruments (the series declared, checked against the
/// dataset's end before the node starts), each cTrader broker's for its own
/// (`add_ctrader_fill`) and OKX's for OKX instruments (their series and
/// funding, their definitions written before the node starts). Then IB,
/// connected only if it has instruments, on the client id of `venues`, fills
/// the others, without a node until its client answers with response
/// envelopes. The cTrader brokers and IB refuse a series they do not serve. A
/// source with nothing missing reads no credential, IB's connection aside.
///
/// # Errors
///
/// Returns an error if the catalog cannot be read, if `DATABENTO_API_KEY` is
/// missing while Databento has something to fill, or as
/// `DatabentoHistory::new`, `DatabentoHistory::check`, `add_ctrader_fill`,
/// `OkxHistory::write_definitions`, `NodeFill::run`, `IbHistory::connect` and
/// `IbHistory::fill`.
pub async fn fill_history(
    requirements: &DataRequirements,
    start: Timestamp,
    end: Timestamp,
    venues: &HistoryVenueArgs,
    catalog_dir: &Path,
) -> anyhow::Result<()> {
    let mut catalog = ParquetDataCatalog::new(catalog_dir, None, None, None, None);
    let (mut okx_ids, mut databento_ids, mut ctrader_ids, mut ib_ids) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for id in requirements.instrument_ids() {
        match HistorySource::of(id) {
            HistorySource::Okx => okx_ids.push(id),
            HistorySource::Databento => databento_ids.push(id),
            HistorySource::CTrader(broker) => ctrader_ids.push((broker, id)),
            HistorySource::Ib => ib_ids.push(id),
        }
    }
    // Every source takes the series slices of its instruments: a rolled
    // root's series contract by contract, each over its slice.
    let slices = requirements.slices(start, end);
    let mut node = NodeFill::new();
    if !databento_ids.is_empty() {
        let requests = missing(&catalog, &of_source(&slices, HistorySource::Databento))?;
        if !requests.is_empty() {
            let api_key =
                std::env::var("DATABENTO_API_KEY").context("reading DATABENTO_API_KEY")?;
            let databento = DatabentoHistory::new(&api_key)?;
            databento.check(&requests).await?;
            node.add(databento.node_client(), &requests, DatabentoHistory::period);
        }
    }
    for (broker, ids) in ctrader_brokers_present(&ctrader_ids) {
        add_ctrader_fill(
            &mut node,
            &catalog,
            broker.venue,
            || broker.connection(),
            &requirements.of_instruments(&ids).catalog_series(),
            &of_source(&slices, HistorySource::CTrader(broker)),
        )?;
    }
    if !okx_ids.is_empty() {
        let slices = of_source(&slices, HistorySource::Okx);
        OkxHistory::default()
            .write_definitions(&mut catalog, &slices)
            .await?;
        let requests = okx::requests(&catalog, &slices)?;
        if !requests.is_empty() {
            node.add(okx::node_client(&okx_ids)?, &requests, okx::period);
        }
    }
    node.run(catalog_dir).await?;
    // Temporary until nautilus_trader#5472 is fixed: IB then fills through
    // the node.
    if !ib_ids.is_empty() {
        let ib = IbHistory::connect(venues.ib.client_id, end, Timestamp::now()).await?;
        ib.fill(&mut catalog, &of_source(&slices, HistorySource::Ib))
            .await?;
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
        exec: Some((
            Box::new(CTraderExecutionClientFactory::new(&exec)),
            Box::new(exec),
        )),
        routing: RoutingConfig::default(),
    }
}

/// What a node needs of the cTrader broker of `venue` to trade its
/// instruments `ids`: the connection `connection` reads, and the instruments
/// of the bars `requirements` reads of them, their quotes being all cTrader
/// serves. Nothing connects.
///
/// # Errors
///
/// Returns an error if a bar type is not built from the quotes of an
/// instrument on `venue` (see `ctrader::quoted`), before the connection is
/// read, or as `connection`.
fn ctrader_read(
    requirements: &DataRequirements,
    ids: &[InstrumentId],
    venue: &str,
    connection: impl FnOnce() -> anyhow::Result<CTraderConnection>,
) -> anyhow::Result<(CTraderConnection, Vec<InstrumentId>)> {
    let instrument_ids = ctrader::quoted(
        Venue::new(venue),
        &catalog_series(&requirements.of_instruments(ids).bar_types),
    )?;
    Ok((connection()?, instrument_ids))
}

/// The live node's clients of `instrument_ids`, those of each one's live
/// broker (`LiveBroker`), for the instruments `requirements` reads: OKX's,
/// each cTrader broker's (`ctrader_brokers_present`), then IB's. The cTrader
/// brokers' bar types are checked and their environments and credential read
/// before anything connects; OKX's clients are set up at `leverage`, the
/// run's, IB's trading the IB account of `venues` with its client id.
///
/// # Errors
///
/// Returns an error if a cTrader broker's bar type is not built from quotes
/// or its environment or credential is missing (see `ctrader_read`, checked
/// before anything connects), if OKX's clients cannot be set up or the OKX
/// account does not suit the run (see `okx::clients`), if cTrader is
/// unreachable or a broker's account is not hedging or not the credential's
/// (see `CTraderConnection::check_account`), if the strategy trades IB
/// instruments and no account is given, or if IB's clients cannot be set up
/// (see `ib::clients`).
pub(crate) async fn live_clients(
    requirements: &DataRequirements,
    instrument_ids: &[InstrumentId],
    venues: &LiveVenueArgs,
    leverage: Decimal,
) -> anyhow::Result<Vec<Clients>> {
    let (mut okx_ids, mut ctrader_ids, mut ib_ids) = (Vec::new(), Vec::new(), Vec::new());
    for &id in instrument_ids {
        match LiveBroker::of(id) {
            LiveBroker::Okx => okx_ids.push(id),
            LiveBroker::CTrader(broker) => ctrader_ids.push((broker, id)),
            LiveBroker::Ib => ib_ids.push(id),
        }
    }
    // Read before anything connects.
    let ctrader = ctrader_brokers_present(&ctrader_ids)
        .into_iter()
        .map(|(broker, ids)| ctrader_read(requirements, &ids, broker.venue, || broker.connection()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut clients = Vec::new();
    if !okx_ids.is_empty() {
        let okx = okx::clients(&okx_ids, leverage).await?;
        clients.push(okx.into_node_clients());
    }
    for (connection, ids) in ctrader {
        connection.check_account().await?;
        clients.push(ctrader_clients(connection, ids));
    }
    if !ib_ids.is_empty() {
        let account = venues.ib.account.as_ref().context(
            "the strategy trades IB instruments: give the IB account \
             (--account or NAUTILUS_IB_ACCOUNT_ID)",
        )?;
        let ib = ib::clients(account, venues.ib.client_id, &ib_ids).await?;
        clients.push(ib.into_node_clients());
    }
    Ok(clients)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashMap, ffi::OsStr, os::unix::ffi::OsStrExt, rc::Rc};

    use clap::{Command, FromArgMatches};
    use ctrader::CTraderEnvironment;
    use jiff::SignedDuration;
    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        live::runner::{replace_data_event_sender, replace_exec_event_sender},
    };
    use nautilus_model::{
        data::{BarType, FundingRateUpdate, QuoteTick, TradeTick},
        enums::AggressorSide,
        identifiers::{AccountId, ClientId, TradeId, TraderId},
        instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
        types::{Price, Quantity},
    };

    use super::*;
    use crate::{
        catalog::{FillData, FillRequest},
        strategy::TickSeries,
    };

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
        let (exec_factory, exec_config) = clients.exec.unwrap();
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
            warmup: SignedDuration::ZERO,
        };
        let Err(err) = live_clients(
            &requirements,
            &[InstrumentId::from("SXR8.IBIS")],
            &venues,
            Decimal::ONE,
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
        let (factory, config) = clients.exec.unwrap();
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

            assert_eq!(
                created,
                LiveBroker::CTrader(broker).oms_type(),
                "{broker:?}"
            );
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

    /// Named Pepperstone first, IC is asked first. Refused before any
    /// connection is read, and before IB, which would refuse the end.
    #[tokio::test]
    async fn a_fill_asks_the_ctrader_brokers_in_their_table_s_order_and_before_ib() {
        let dir = tempfile::tempdir().unwrap();
        let requirements = reading(&[
            "XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL",
            "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL",
            "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL",
        ]);

        let err = fill_history(
            &requirements,
            at(FILL_START),
            at(FILL_END),
            &history_args(),
            dir.path(),
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("ICMARKETS serves quotes only"),
            "{err:#}"
        );
    }

    /// The series and slices of a fill of gold's quotes on `venue`.
    fn gold_slices(venue: &str) -> (Vec<(NautilusDataType, String)>, Vec<SeriesSlice>) {
        let requirements = ticks(&[(
            NautilusDataType::QuoteTick,
            format!("XAUUSD.{venue}").as_str(),
        )]);
        (
            requirements.catalog_series(),
            requirements.slices(at(FILL_START), at(FILL_END)),
        )
    }

    #[test]
    fn a_ctrader_fill_asks_what_the_catalog_lacks_in_tick_requests_and_a_definition_whole() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (series, slices) = gold_slices(pepperstone::VENUE);
        let mut node = NodeFill::new();

        add_ctrader_fill(
            &mut node,
            &catalog,
            pepperstone::VENUE,
            || Ok(demo_connection(pepperstone::BROKER)),
            &series,
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
        let (series, slices) = gold_slices(pepperstone::VENUE);
        let mut node = NodeFill::new();

        add_ctrader_fill(
            &mut node,
            &catalog,
            pepperstone::VENUE,
            || panic!("the connection was read with nothing missing"),
            &series,
            &slices,
        )
        .unwrap();

        assert_eq!(node.asked(), []);
    }

    #[test]
    fn a_series_a_ctrader_broker_does_not_serve_is_refused_before_the_connection_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let requirements = reading(&["XAUUSD.PEPPERSTONE-1-MINUTE-BID-EXTERNAL"]);

        let err = add_ctrader_fill(
            &mut NodeFill::new(),
            &catalog,
            pepperstone::VENUE,
            || panic!("the connection was read before the series were checked"),
            &requirements.catalog_series(),
            &requirements.slices(at(FILL_START), at(FILL_END)),
        )
        .unwrap_err();

        assert!(err.to_string().contains("serves quotes only"), "{err:#}");
    }

    /// GC.GLBX rolls from GCQ26 to GCZ26 at 00:00 UTC on 29 July 2026: its
    /// slices are Databento's, contract by contract.
    #[test]
    fn each_history_source_takes_the_slices_of_its_own_contracts() {
        let requirements = ticks(&[
            (NautilusDataType::TradeTick, "GC.GLBX"),
            (NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
            (NautilusDataType::QuoteTick, "XAUUSD.PEPPERSTONE"),
            (NautilusDataType::TradeTick, "BTC-USDT-SWAP.OKX"),
            (NautilusDataType::TradeTick, "SXR8.IBIS"),
        ]);
        let slices = requirements.slices(at("2026-07-28T00:00:00Z"), at("2026-07-29T12:00:00Z"));
        let contracts = |source| {
            of_source(&slices, source)
                .iter()
                .map(|slice| slice.slice.instrument_id.to_string())
                .collect::<Vec<_>>()
        };

        assert_eq!(
            contracts(HistorySource::Databento),
            ["GCQ26.GLBX", "GCZ26.GLBX"]
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

        let Err(err) = live_clients(&requirements, &ids, &live_args(), Decimal::ONE).await else {
            panic!("set up clients for bars cTrader does not serve");
        };

        assert!(
            err.to_string().contains("ICMARKETS serves quotes only"),
            "{err:#}"
        );
    }

    /// The cTrader brokers' accounts hedge; IB's and OKX's net, whether the
    /// venue has a row of its own, as GLBX has, or none.
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
        ] {
            assert_eq!(oms_type(Venue::new(venue)), kept, "{venue}");
        }
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
        ] {
            assert_eq!(
                takes_reduce_only_and_post_only(Venue::new(venue)),
                takes,
                "{venue}"
            );
        }
    }

    #[test]
    fn each_instrument_routes_to_the_row_of_its_venue() {
        use HistorySource as H;
        use LiveBroker as L;
        for (id, history, live) in [
            ("BTC-USDT-SWAP.OKX", H::Okx, L::Okx),
            (
                "XAUUSD.ICMARKETS",
                H::CTrader(ic::BROKER),
                L::CTrader(ic::BROKER),
            ),
            (
                "XAUUSD.PEPPERSTONE",
                H::CTrader(pepperstone::BROKER),
                L::CTrader(pepperstone::BROKER),
            ),
            ("GCZ26.GLBX", H::Databento, L::Ib),
            ("GC.GLBX", H::Databento, L::Ib),
            ("GC.v.0.GLBX", H::Databento, L::Ib),
            ("SXR8.IBIS", H::Ib, L::Ib),
            ("ESZ26.CME", H::Ib, L::Ib),
            ("GCZ26.XCEC", H::Ib, L::Ib),
            ("EUR/USD.IDEALPRO", H::Ib, L::Ib),
            ("BTC-USDT-SWAP.BINANCE", H::Ib, L::Ib),
        ] {
            let id = InstrumentId::from(id);
            assert_eq!(
                (HistorySource::of(id), LiveBroker::of(id)),
                (history, live),
                "{id}"
            );
        }
    }
}
