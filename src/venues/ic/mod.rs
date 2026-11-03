//! IC Markets, over Spotware's cTrader Open API: the instruments of IC's
//! forex and metals and their quotes, from cTrader's tick history, for the
//! catalog, which fill-catalog fills through the node by IC's data client
//! (see `catalog::NodeFill`), and the live node's clients on a hedging
//! account. IC's instruments are on venue `ICMARKETS` (`XAUUSD.ICMARKETS`).
//! See `README.md`.
//!
//! Nautilus has no cTrader adapter, so the submodules are one, laid out as
//! Nautilus' adapters are (`docs/developer_guide/adapters.md`) and private
//! to this module: `common` (Spotware's protobuf messages from the
//! vendored `proto/`, the parsing of symbols into instruments, the account
//! check, the token pair and its token file), `config` (the environment,
//! the credential and the client configs), `websocket` (the connection: an
//! outer client and its handler task), `session` (`IcHistory`, the cTrader
//! session history and both clients share: the tick history merged into
//! quotes, the spot subscriptions, the trade requests), `data` (the data
//! client, its live quotes from the spot events), `execution` (the
//! execution client: the trade requests of orders, the dispatch of trade
//! events, the reconciliation reports) and `factories`.
//!
//! This module itself holds IC's couplings to helix: which instruments a
//! fill's IC data client loads, from the strategy's catalog series
//! (`quoted`), the client (`node_client`) and the period of its requests
//! (`period`), and the live node's clients (`clients`), on the environment
//! `IC_ENVIRONMENT` names, `demo` or `live`, required, with the credential
//! from `IC_*`, both read before anything connects.

use std::str::FromStr;

use anyhow::{Context, ensure};
use jiff::SignedDuration;
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::InstrumentId,
};

use self::common::parse;
pub(crate) use self::common::parse::IC_VENUE;
pub use self::{
    config::{IcConnection, IcDataClientConfig, IcExecutionClientConfig},
    factories::{IcDataClientFactory, IcExecutionClientFactory},
    session::IcHistory,
};
use crate::{
    catalog::{FillData, NodeClient},
    live::node::Clients,
    strategy::catalog_series,
};

mod common;
mod config;
mod data;
mod execution;
mod factories;
mod session;
#[cfg(test)]
mod testing;
mod websocket;

/// The instruments of `series`, each the quotes of an IC instrument:
/// those IC's data client loads to fill them through the node.
///
/// # Errors
///
/// Returns an error naming the first series that is not the quotes of
/// an IC instrument: IC serves nothing else.
pub fn quoted(series: &[(NautilusDataType, String)]) -> anyhow::Result<Vec<InstrumentId>> {
    series
        .iter()
        .map(|(data_type, identifier)| quotes_of(data_type, identifier))
        .collect()
}

/// The IC instrument whose quotes the series of `data_type` and
/// `identifier` is; an error for any other series.
fn quotes_of(data_type: &NautilusDataType, identifier: &str) -> anyhow::Result<InstrumentId> {
    ensure!(
        *data_type == NautilusDataType::QuoteTick,
        "IC serves quotes only, not the {data_type} of {identifier}"
    );
    let id = InstrumentId::from_str(identifier)
        .with_context(|| format!("{identifier} is no instrument id"))?;
    parse::ensure_ic(id)?;
    Ok(id)
}

/// IC's data client for a fill through the node (see `NodeFill::add`),
/// loading `instrument_ids` (see `quoted`), on the connection of the
/// environment (`IcConnection::from_env`). It names its venue, so
/// Nautilus routes IC's requests to it without a route of ours.
///
/// # Errors
///
/// As `IcConnection::from_env`.
pub fn node_client(instrument_ids: Vec<InstrumentId>) -> anyhow::Result<NodeClient> {
    Ok(NodeClient {
        factory: Box::new(IcDataClientFactory),
        config: Box::new(IcDataClientConfig {
            connection: IcConnection::from_env()?,
            instrument_ids,
        }),
        routing: RoutingConfig::default(),
    })
}

/// The window one request holds at most in a fill through the node
/// (see `NodeFill::add`): a week, the most one tick request spans,
/// about 0.7 M BID and 0.7 M ASK ticks of XAUUSD (`README.md`, "Tick
/// history").
#[must_use]
pub fn period(_data: FillData) -> SignedDuration {
    SignedDuration::from_hours(7 * 24)
}

/// The IC client configs of a node trading IC instruments.
#[derive(Debug, Clone)]
pub(crate) struct IcClients {
    data: IcDataClientConfig,
    exec: IcExecutionClientConfig,
}

/// The client configs of the IC instruments `bar_types` are built from,
/// their quotes (IC serves nothing else), on the connection `var` reads
/// (`IcConnection::from_vars`), before anything connects.
///
/// # Errors
///
/// Returns an error if a bar type is not built from the quotes of an IC
/// instrument (see `strategy::catalog_series`), if `IC_ENVIRONMENT` is not
/// `demo` or `live`, or if a credential variable is missing or invalid.
pub(crate) fn clients(
    bar_types: &[BarType],
    var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<IcClients> {
    let instrument_ids = quoted(&catalog_series(bar_types))?;
    let connection = IcConnection::from_vars(var)?;
    Ok(IcClients {
        data: IcDataClientConfig {
            connection: connection.clone(),
            instrument_ids: instrument_ids.clone(),
        },
        exec: IcExecutionClientConfig {
            connection,
            instrument_ids,
        },
    })
}

impl IcClients {
    /// Checks the account the clients trade, on a session of its own
    /// closed before the node connects: hedging and `IC_ACCOUNT_ID`'s (see
    /// `IcHistory::check_account`). Run before the node is built, so a
    /// wrong account fails the start with its reason.
    ///
    /// # Errors
    ///
    /// Returns an error if cTrader cannot be reached or refuses the
    /// credential, or if the account does not suit (see
    /// `IcHistory::check_account`).
    pub(crate) async fn check_account(&self) -> anyhow::Result<()> {
        let ic = IcHistory::connect_as(&self.data.connection).await?;
        let checked = ic.check_account().await;
        ic.disconnect().await;
        checked
    }

    /// The factories and configs, for the node. Both clients are on venue
    /// `ICMARKETS`, so Nautilus routes IC's data and orders to them without
    /// a route of ours.
    pub(crate) fn into_node_clients(self) -> Clients {
        Clients {
            data_factory: Box::new(IcDataClientFactory),
            data_config: Box::new(self.data),
            exec: Some((Box::new(IcExecutionClientFactory), Box::new(self.exec))),
            routing: RoutingConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashMap, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        factories::{DataClientFactory, ExecutionClientFactory},
        live::runner::{replace_data_event_sender, replace_exec_event_sender},
    };
    use nautilus_model::{
        enums::OmsType,
        identifiers::{AccountId, ClientId, TraderId, Venue},
    };

    use super::{factories::NAME, *};

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    const CREDENTIAL: [(&str, &str); 6] = [
        ("IC_CLIENT_ID", "12345_clientIdValue"),
        ("IC_CLIENT_SECRET", "clientSecretValue"),
        ("IC_ACCESS_TOKEN", "accessTokenValue"),
        ("IC_REFRESH_TOKEN", "refreshTokenValue"),
        ("IC_ACCOUNT_ID", "1000001"),
        ("HOME", "/home/u"),
    ];

    fn demo() -> Vec<(&'static str, &'static str)> {
        let mut pairs = CREDENTIAL.to_vec();
        pairs.push(("IC_ENVIRONMENT", "demo"));
        pairs
    }

    /// Gold's hourly bars, built from its quotes.
    fn gold_bars() -> BarType {
        BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")
    }

    #[test]
    fn a_missing_or_unknown_environment_is_refused_before_anything_connects() {
        for environment in [None, Some("paper")] {
            let mut pairs = CREDENTIAL.to_vec();
            pairs.extend(environment.map(|e| ("IC_ENVIRONMENT", e)));

            let err = clients(&[gold_bars()], vars(&pairs)).unwrap_err();

            assert!(err.to_string().contains("IC_ENVIRONMENT"), "{err}");
        }
    }

    #[test]
    fn a_missing_credential_variable_is_refused_before_anything_connects() {
        let err = clients(&[gold_bars()], vars(&[("IC_ENVIRONMENT", "demo")])).unwrap_err();

        assert!(err.to_string().contains("IC_CLIENT_ID"), "{err}");
    }

    #[test]
    fn an_instrument_not_on_icmarkets_is_refused() {
        let bars = BarType::from("EUR/USD.IDEALPRO-1-HOUR-MID-INTERNAL");

        let err = clients(&[bars], vars(&demo())).unwrap_err();

        assert!(err.to_string().contains("EUR/USD.IDEALPRO"), "{err}");
    }

    #[test]
    fn bars_not_built_from_quotes_are_refused_before_anything_connects() {
        for bars in [
            "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL",
            "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL",
        ] {
            let err = clients(&[BarType::from(bars)], vars(&demo())).unwrap_err();

            assert!(err.to_string().contains("IC serves quotes only"), "{err}");
        }
    }

    #[test]
    fn the_clients_load_the_instruments_of_the_bars() {
        let bars = [
            gold_bars(),
            BarType::from("XAUUSD.ICMARKETS-1-DAY-MID-INTERNAL"),
            BarType::from("EURUSD.ICMARKETS-1-HOUR-BID-INTERNAL"),
        ];

        let clients = clients(&bars, vars(&demo())).unwrap();

        let ids = [
            InstrumentId::from("EURUSD.ICMARKETS"),
            InstrumentId::from("XAUUSD.ICMARKETS"),
        ];
        assert_eq!(clients.data.instrument_ids, ids);
        assert_eq!(clients.exec.instrument_ids, ids);
    }

    #[test]
    fn the_factory_creates_an_unconnected_data_client_on_icmarkets() {
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(tx);
        let config = clients(&[gold_bars()], vars(&demo())).unwrap().data;
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));

        let client = IcDataClientFactory
            .create(NAME, &config, cache.into(), clock)
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("ICMARKETS"));
        assert_eq!(client.venue(), Some(Venue::new("ICMARKETS")));
        assert!(client.is_disconnected());
    }

    #[test]
    fn the_factory_creates_an_unconnected_hedging_execution_client_on_icmarkets() {
        let (tx, _events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let config = clients(&[gold_bars()], vars(&demo())).unwrap().exec;
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));

        let client = IcExecutionClientFactory
            .create(
                TraderId::from("HELIX-001"),
                NAME,
                &config,
                cache.into(),
                clock,
            )
            .unwrap();

        assert_eq!(client.client_id(), ClientId::from("ICMARKETS"));
        assert_eq!(client.venue(), Venue::new("ICMARKETS"));
        assert_eq!(client.oms_type(), OmsType::Hedging);
        assert_eq!(client.account_id(), AccountId::from("ICMARKETS-DEMO"));
        assert!(!client.is_connected());
    }

    /// As the node does: each client created by its factory, under the
    /// factory's name, from its config.
    #[test]
    fn the_node_gets_factories_that_create_ic_s_clients_from_their_configs() {
        let (data_tx, _data_events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let (exec_tx, _exec_events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(exec_tx);
        let clients = clients(&[gold_bars()], vars(&demo())).unwrap();
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));

        let node_clients = clients.into_node_clients();

        let data = node_clients.data_factory.create(
            node_clients.data_factory.name(),
            node_clients.data_config.as_ref(),
            cache.clone().into(),
            clock.clone(),
        );
        assert!(data.is_ok(), "{:#}", data.err().unwrap());
        let (exec_factory, exec_config) = node_clients.exec.unwrap();
        let exec = exec_factory.create(
            TraderId::from("HELIX-001"),
            exec_factory.name(),
            exec_config.as_ref(),
            cache.into(),
            clock,
        );
        assert!(exec.is_ok(), "{:#}", exec.err().unwrap());
    }

    const XAUUSD: &str = "XAUUSD.ICMARKETS";

    #[test]
    fn a_series_other_than_the_quotes_of_an_ic_instrument_is_refused() {
        let quotes = (NautilusDataType::QuoteTick, XAUUSD.to_owned());
        let bars = (
            NautilusDataType::Bar,
            "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL".to_owned(),
        );
        let elsewhere = (NautilusDataType::QuoteTick, "EUR/USD.IDEALPRO".to_owned());

        assert_eq!(
            quoted(std::slice::from_ref(&quotes)).unwrap(),
            [InstrumentId::from(XAUUSD)]
        );
        let err = quoted(&[quotes, bars]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "IC serves quotes only, not the Bar of XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL"
        );
        let err = quoted(&[elsewhere]).unwrap_err();
        assert!(err.to_string().contains("EUR/USD.IDEALPRO"), "{err}");
    }

    #[test]
    fn book_deltas_and_custom_data_are_refused() {
        let imbalance = NautilusDataType::Custom {
            type_name: "Imbalance".to_string(),
        };
        for data_type in [NautilusDataType::OrderBookDelta, imbalance] {
            let named = data_type.to_string();
            let err = quoted(&[(data_type, XAUUSD.to_owned())]).unwrap_err();
            assert!(err.to_string().contains(&named), "{err}");
        }
    }
}
