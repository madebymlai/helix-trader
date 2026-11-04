//! The venues: each one's history, live clients and command-line settings
//! in a module of its own, and the routing table, which says which history
//! source fills an instrument's series (`HistorySource`) and which broker
//! trades it live (`LiveBroker`), both by its venue. Fill-catalog and the
//! live runner partition the instruments through it, the live runner by
//! asking for the node's clients (`live_clients`), and venue code checks
//! its own instruments against it. The settings each command takes of the
//! venues are composed here, `LiveVenueArgs` for live and
//! `HistoryVenueArgs` for fill-catalog, which the commands flatten. The
//! cTrader brokers' modules name each one's venue and account; what they
//! share is the adapter's (`ctrader`), its clients built for helix here
//! (`ctrader_node_client`, `ctrader_clients`).

pub mod databento;
pub mod ib;
pub mod ic;
pub mod okx;
pub mod pepperstone;

use anyhow::Context;
use clap::Args;
use ctrader::{
    CTraderConnection, CTraderDataClientConfig, CTraderDataClientFactory,
    CTraderExecutionClientConfig, CTraderExecutionClientFactory,
};
use jiff::Timestamp;
use nautilus_live::config::RoutingConfig;
use nautilus_model::identifiers::{InstrumentId, Venue};
use nautilus_okx::common::consts::OKX_VENUE;
use rust_decimal::Decimal;

use self::ib::IbHistory;
use crate::{
    catalog::NodeClient,
    live::node::Clients,
    strategy::{DataRequirements, catalog_series},
};

/// The source of an instrument's history in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySource {
    /// OKX's public API, through Nautilus' OKX data client.
    Okx,
    /// IC Markets' cTrader tick history.
    Ic,
    /// Pepperstone's cTrader tick history.
    Pepperstone,
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
    /// IC Markets' cTrader clients, on a hedging account.
    Ic,
    /// Pepperstone's cTrader clients, on a hedging account.
    Pepperstone,
    /// Interactive Brokers' clients, on the gateway.
    Ib,
}

impl HistorySource {
    /// The history source of `id`: its venue's row (see `row`).
    #[must_use]
    pub fn of(id: InstrumentId) -> Self {
        row(id.venue).0
    }
}

impl LiveBroker {
    /// The live broker of `id`: its venue's row (see `row`).
    #[must_use]
    pub(crate) fn of(id: InstrumentId) -> Self {
        row(id.venue).1
    }
}

/// The routing table's row of `venue`: its history source and its live
/// broker. GLBX, CME Globex as Databento names it, has its history from
/// Databento and its live broker in IB, Databento having no live clients
/// here; every venue without a row of its own is IB's, history and live.
fn row(venue: Venue) -> (HistorySource, LiveBroker) {
    if venue == *OKX_VENUE {
        (HistorySource::Okx, LiveBroker::Okx)
    } else if venue == Venue::new(ic::VENUE) {
        (HistorySource::Ic, LiveBroker::Ic)
    } else if venue == Venue::new(pepperstone::VENUE) {
        (HistorySource::Pepperstone, LiveBroker::Pepperstone)
    } else if venue == Venue::GLBX() {
        (HistorySource::Databento, LiveBroker::Ib)
    } else {
        (HistorySource::Ib, LiveBroker::Ib)
    }
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

impl HistoryVenueArgs {
    /// Connects to IB for history with the fill's client id, once IB can
    /// serve closed bars over a window ending at `end` (see
    /// `IbHistory::connect`).
    ///
    /// # Errors
    ///
    /// As `IbHistory::connect`.
    pub async fn connect_ib(&self, end: Timestamp, now: Timestamp) -> anyhow::Result<IbHistory> {
        IbHistory::connect(self.ib.client_id, end, now).await
    }
}

/// The data client of a cTrader broker for a fill through the node (see
/// `NodeFill::add`), on `connection`, loading `instrument_ids`. It names its
/// venue, so Nautilus routes the broker's requests to it without a route of
/// ours.
#[must_use]
pub fn ctrader_node_client(
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

/// What a node needs of the cTrader broker of `venue`, if it trades any of
/// its instruments `ids`: the connection `connection` reads, and the
/// instruments of the bars `requirements` reads of them, their quotes being
/// all cTrader serves. Nothing connects.
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
) -> anyhow::Result<Option<(CTraderConnection, Vec<InstrumentId>)>> {
    if ids.is_empty() {
        return Ok(None);
    }
    let instrument_ids = ctrader::quoted(
        Venue::new(venue),
        &catalog_series(&requirements.of_instruments(ids).bar_types),
    )?;
    Ok(Some((connection()?, instrument_ids)))
}

/// The live node's clients of `instrument_ids`, those of each one's live
/// broker (`LiveBroker`), for the instruments `requirements` reads: OKX's,
/// IC's, Pepperstone's, then IB's. The cTrader brokers' bar types are
/// checked and their environments and credential read before anything
/// connects; OKX's clients are set up at `leverage`, the run's, IB's trading
/// the IB account of `venues` with its client id.
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
    let (mut okx_ids, mut ic_ids, mut pepperstone_ids, mut ib_ids) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for &id in instrument_ids {
        match LiveBroker::of(id) {
            LiveBroker::Okx => okx_ids.push(id),
            LiveBroker::Ic => ic_ids.push(id),
            LiveBroker::Pepperstone => pepperstone_ids.push(id),
            LiveBroker::Ib => ib_ids.push(id),
        }
    }
    // Read before anything connects.
    let ic = ctrader_read(requirements, &ic_ids, ic::VENUE, ic::connection)?;
    let pepperstone = ctrader_read(
        requirements,
        &pepperstone_ids,
        pepperstone::VENUE,
        pepperstone::connection,
    )?;
    let mut clients = Vec::new();
    if !okx_ids.is_empty() {
        let okx = okx::clients(&okx_ids, leverage).await?;
        clients.push(okx.into_node_clients());
    }
    for (connection, ids) in [ic, pepperstone].into_iter().flatten() {
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
    use std::{cell::RefCell, collections::HashMap, rc::Rc};

    use clap::{Command, FromArgMatches};
    use ctrader::{CTraderAccountVars, CTraderEnvironment};
    use jiff::SignedDuration;
    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        live::runner::{replace_data_event_sender, replace_exec_event_sender},
    };
    use nautilus_model::{
        data::BarType,
        enums::OmsType,
        identifiers::{AccountId, ClientId, TraderId},
    };

    use super::*;

    /// A demo account of `venue`, through `account`'s variables, beside the
    /// login's credential.
    fn demo_connection(venue: &str, account: CTraderAccountVars) -> CTraderConnection {
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
        CTraderConnection::from_vars(Venue::new(venue), account, var).unwrap()
    }

    /// Each broker's account by its own variables, on the login's one
    /// credential and token file.
    #[test]
    fn each_ctrader_venue_reads_its_own_account_beside_the_login_s_credential() {
        let ic = demo_connection(ic::VENUE, ic::ACCOUNT);
        let pepperstone = demo_connection(pepperstone::VENUE, pepperstone::ACCOUNT);

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

    #[test]
    fn a_ctrader_venue_without_instruments_reads_no_connection() {
        let read = ctrader_read(&reading(&[]), &[], pepperstone::VENUE, || {
            panic!("the connection was read for no instrument")
        });

        assert!(read.unwrap().is_none());
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
            Ok(demo_connection(pepperstone::VENUE, pepperstone::ACCOUNT))
        })
        .unwrap()
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
        let connection = demo_connection(pepperstone::VENUE, pepperstone::ACCOUNT);
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

    #[test]
    fn each_instrument_routes_to_the_row_of_its_venue() {
        use HistorySource as H;
        use LiveBroker as L;
        for (id, history, live) in [
            ("BTC-USDT-SWAP.OKX", H::Okx, L::Okx),
            ("XAUUSD.ICMARKETS", H::Ic, L::Ic),
            ("XAUUSD.PEPPERSTONE", H::Pepperstone, L::Pepperstone),
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
