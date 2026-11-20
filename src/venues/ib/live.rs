//! Interactive Brokers clients for the live node, what their data client streams (`streams`), and
//! the live command's IB flags (`IbLiveArgs`).
//!
//! Provides the Nautilus client factories and configs from `nautilus_interactive_brokers`:
//! `InteractiveBrokersDataClientFactory`, `InteractiveBrokersExecutionClientFactory` and their
//! configs, on the gateway of the account's trading mode (`IbGateway`); the account id goes in the
//! execution config.

use clap::Args;
use jiff::SignedDuration;
use nautilus_interactive_brokers::{
    config::{
        InteractiveBrokersDataClientConfig, InteractiveBrokersExecutionClientConfig,
        InteractiveBrokersInstrumentProviderConfig,
    },
    data::convert::bar_type_to_ib_bar_size,
    factories::{InteractiveBrokersDataClientFactory, InteractiveBrokersExecutionClientFactory},
};
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::{BarSpecification, BarType, NautilusDataType},
    enums::{AggregationSource, BarAggregation, BookType, PriceType},
    identifiers::{InstrumentId, Venue},
};

use super::{
    BAR_PRICES, CURRENCY_PAIR_VENUES, FOREX_VENUE, IbAccount, IbGateway, data_client_config,
};
use crate::{
    live::node::{Clients, ExecClient},
    strategy::requirements::{CatalogSeries, TickSeries},
    venues::BookFormat,
};

/// IB's settings of a live node, the live command's flags: the account traded and the client id of
/// the node's IB clients (see `clients`).
#[derive(Debug, Clone, Args)]
pub(crate) struct IbLiveArgs {
    /// The IB account traded, for example DU1234567 (paper); needed only for IB instruments.
    #[arg(long, env = "NAUTILUS_IB_ACCOUNT_ID")]
    pub(in crate::venues) account: Option<IbAccount>,
    /// IB client id of the node, the adapter default.
    #[arg(long, default_value_t = 1)]
    pub(in crate::venues) client_id: i32,
}

/// The IB data and execution client configs of a node trading `account` on `instrument_ids` through
/// `gateway`, both on one connection.
#[derive(Debug, Clone)]
pub(crate) struct IbClients {
    pub(super) data: InteractiveBrokersDataClientConfig,
    pub(super) exec: InteractiveBrokersExecutionClientConfig,
}

/// Checks `client_id`, then starts or reuses the gateway of `account`'s trading mode and waits for
/// its login (see `IbGateway::of_account`), and returns the client configs on it (see `configs`).
///
/// # Errors
///
/// Returns an error if:
/// - The client id is a multiple of 1000 (IB reserves those for the execution client), checked
///   before any gateway starts.
/// - The gateway does not start and log in.
pub(crate) async fn clients(
    account: &IbAccount,
    client_id: i32,
    instrument_ids: &[InstrumentId],
) -> anyhow::Result<IbClients> {
    anyhow::ensure!(
        client_id % 1000 != 0,
        "client id {client_id} is a multiple of 1000, which the IB execution client rejects"
    );
    let gateway = IbGateway::of_account(account, client_id).await?;
    Ok(configs(&gateway, account, instrument_ids))
}

/// The client configs for `account` on `gateway`, loading `instrument_ids` at startup.
///
/// The data client is the history's (`data_client_config`: the full session, the symbology and
/// venues of the catalog's instruments), so its 1-minute bars, requested for the warmup and
/// subscribed to live, are those `fill-catalog` stores. Quotes a strategy subscribes to come tick
/// by tick (`batch_quotes: false`; the adapter falls back to `reqMktData` where IB refuses them).
/// Data and exec share `gateway`'s client id, hence one connection.
fn configs(gateway: &IbGateway, account: &IbAccount, instrument_ids: &[InstrumentId]) -> IbClients {
    let instrument_provider = InteractiveBrokersInstrumentProviderConfig {
        load_ids: instrument_ids.iter().copied().collect(),
        ..Default::default()
    };
    IbClients {
        data: InteractiveBrokersDataClientConfig {
            batch_quotes: false,
            instrument_provider: instrument_provider.clone(),
            ..data_client_config(gateway.clone())
        },
        exec: InteractiveBrokersExecutionClientConfig {
            host: gateway.host.clone(),
            port: gateway.port,
            client_id: gateway.client_id,
            account_id: Some(account.to_string()),
            instrument_provider,
            ..Default::default()
        },
    }
}

impl IbClients {
    /// The factories and configs, for the node: the data client routed the venues of `data_venues`,
    /// those whose live data is IB's, and none if none is, helix making it not the default, which
    /// Nautilus may make it, nothing depending on that (see `live::node::Clients`); the execution
    /// client the default route of orders, since IB trades many venues, GLBX among them, whose data
    /// is LSE's.
    pub(crate) fn into_node_clients(self, data_venues: &[Venue]) -> Clients {
        Clients {
            data_factory: Box::new(InteractiveBrokersDataClientFactory::new()),
            data_config: Box::new(self.data),
            data_routing: RoutingConfig {
                default: false,
                venues: (!data_venues.is_empty())
                    .then(|| data_venues.iter().map(ToString::to_string).collect()),
            },
            exec: Some(ExecClient {
                factory: Box::new(InteractiveBrokersExecutionClientFactory::new()),
                config: Box::new(self.exec),
                routing: RoutingConfig::builder().default(true).build(),
            }),
        }
    }
}

/// IB's book as Nautilus' IB data client streams it (`venues::book_format`): its deltas of an
/// L2_MBP book, the rows of IB's market depth, each by its position, and by the exchange posting it
/// on IB's SMART depth, the client's default, an update able to move a row to another price
/// (`subscribe_book_deltas`, `data/core.rs:1249`, `:1301-1306`; `data/streams.rs:1952-2027`). The
/// client logs an L3_MBO subscription as unpublished by IB and streams nothing
/// (`core.rs:1262-1268`). It streams no depth snapshots, which it does not subscribe to (Nautilus'
/// `DataClient` default, `common/src/clients/data.rs:171-174`). A deltas subscription of no depth
/// asks IB for 20 rows a side (`core.rs:1299`).
pub(in crate::venues) const BOOK: BookFormat = BookFormat {
    deltas: BookType::L2_MBP,
    depth: None,
};

/// The narrowest bars IB keeps up to date: its historical bars streamed with `keepUpToDate`, "only
/// ... with bar sizes 5 seconds or greater" (TWS API, "Historical Bar Data"), and its real-time
/// bars, 5 seconds wide.
const KEPT_UP_TO_DATE: SignedDuration = SignedDuration::from_secs(5);

/// Checks that Nautilus' IB data client, the live node's, streams `source`, the series of an
/// instrument of a venue whose live data is IB's, as a run subscribes to it:
/// - Its quotes (`subscribe_quotes`, `data/core.rs:724`).
/// - Its trades (`subscribe_trades`, `:979`), but a currency pair's, which the client logs as
///   unsupported and streams none of (`:1000-1006`, see `CURRENCY_PAIR_VENUES`).
/// - IB's own (`EXTERNAL`) time bars (`subscribe_bars`, `:1093`) of a size IB has
///   (`bar_type_to_ib_bar_size`, on which the client's subscription task fails any other) but 1
///   second: its 5-second bars as IB's real-time bars (`:1166`), the others as its historical bars
///   kept up to date (`data/streams.rs:817-821`), which IB keeps only at 5 seconds or more
///   (`KEPT_UP_TO_DATE`), each priced at one of `BAR_PRICES`, a forex pair's LAST bars aside, IB
///   having no TRADES of them.
/// - Its book deltas, which a run subscribes to in IB's format (`BOOK`, through
///   `venues::book_format`), of an L2_MBP book.
///
/// It subscribes to no book depth, `BOOK` having none: Nautilus' `DataClient` default logs "Handler
/// not implemented" and streams nothing (`common/src/clients/data.rs:171-174`).
///
/// # Errors
///
/// Returns an error if the client does not stream `source`, naming it and saying why.
pub(crate) fn streams(source: &CatalogSeries) -> anyhow::Result<()> {
    use NautilusDataType as D;
    match source {
        CatalogSeries::Bars(bar_type) => {
            anyhow::ensure!(
                bar_type.is_externally_aggregated()
                    && BAR_PRICES.contains(&bar_type.spec().price_type)
                    && streamed_size(bar_type),
                "IB streams its own (EXTERNAL) time bars of a size IB has and keeps up to date, \
                 {}, priced LAST, BID, ASK or MID, not {bar_type}",
                streamed_sizes(bar_type.instrument_id()).join(", ")
            );
            anyhow::ensure!(
                !(bar_type.spec().price_type == PriceType::Last
                    && bar_type.instrument_id().venue.as_str() == FOREX_VENUE),
                "IB has no TRADES bars of a forex pair, so it streams no {bar_type}: read its \
                 BID, ASK or MID bars"
            );
        }
        CatalogSeries::Ticks(TickSeries {
            data: D::QuoteTick | D::OrderBookDelta,
            ..
        }) => {}
        CatalogSeries::Ticks(TickSeries {
            data: D::TradeTick,
            instrument_id,
        }) => anyhow::ensure!(
            !CURRENCY_PAIR_VENUES.contains(&instrument_id.venue.as_str()),
            "Nautilus' IB client streams no trades of a currency pair, so IB streams no trades \
             of {instrument_id}: read its quotes"
        ),
        CatalogSeries::Ticks(TickSeries {
            data: D::OrderBookDepth,
            instrument_id,
        }) => anyhow::bail!(
            "Nautilus' IB client streams no book depth, so IB streams none of {instrument_id}: \
             read its book deltas"
        ),
        CatalogSeries::Ticks(ticks) => {
            anyhow::bail!(
                "Nautilus' IB client streams quotes, trades, bars and book deltas, not the {ticks}"
            )
        }
    }
    Ok(())
}

/// Whether IB streams bars of `bar_type`'s size: one IB has (`bar_type_to_ib_bar_size`, on which
/// the client's subscription task fails any other) and keeps up to date (`KEPT_UP_TO_DATE`).
fn streamed_size(bar_type: &BarType) -> bool {
    bar_type_to_ib_bar_size(bar_type).is_ok() && bar_type.spec().timedelta() >= KEPT_UP_TO_DATE
}

/// The sizes IB streams bars of `instrument_id` of (`streamed_size`), as Nautilus writes a bar's
/// step and aggregation (`5-SECOND`), shortest first: each valid step to 60 of each time
/// aggregation tried, so that the sizes a refusal offers are the rule's.
fn streamed_sizes(instrument_id: InstrumentId) -> Vec<String> {
    use BarAggregation as A;
    [
        A::Millisecond,
        A::Second,
        A::Minute,
        A::Hour,
        A::Day,
        A::Week,
        A::Month,
        A::Year,
    ]
    .into_iter()
    .flat_map(|aggregation| {
        (1..=60).filter_map(move |step| {
            BarSpecification::new_checked(step, aggregation, PriceType::Last).ok()
        })
    })
    .filter(|&spec| {
        streamed_size(&BarType::new(
            instrument_id,
            spec,
            AggregationSource::External,
        ))
    })
    .map(|spec| format!("{}-{}", spec.step, spec.aggregation))
    .collect()
}

/// IB's clients of a node trading `instrument_ids` on the paper account DU1234567, on a gateway at
/// 127.0.0.1:4002 that is not started: created, they connect to nothing.
#[cfg(test)]
pub(in crate::venues) fn offline_clients(instrument_ids: &[InstrumentId]) -> IbClients {
    let gateway = IbGateway {
        host: "127.0.0.1".to_string(),
        port: 4002,
        client_id: 11,
    };
    configs(&gateway, &"DU1234567".parse().unwrap(), instrument_ids)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::venues::{Broker, tests::created_oms_type};

    fn gateway() -> IbGateway {
        IbGateway {
            host: "127.0.0.1".to_string(),
            port: 4002,
            client_id: 11,
        }
    }

    fn paper() -> IbAccount {
        "DU1234567".parse().unwrap()
    }

    fn sxr8() -> InstrumentId {
        InstrumentId::from("SXR8.IBIS")
    }

    /// Pins that Nautilus' IB execution client keeps positions netting, whatever its config
    /// (`crates/adapters/interactive_brokers/src/factories.rs:182`): `Broker::oms_type` copies it,
    /// which a backtest simulates every venue IB trades by (`venues::oms_type`).
    ///
    /// Against the adapter's client: the broker's answer is our copy of it. The client needs no
    /// credential and connects to nothing.
    #[rstest]
    fn drift_the_execution_client_keeps_positions_as_the_broker_answers() {
        let clients = configs(&gateway(), &paper(), &[sxr8()]);

        let created = created_oms_type(clients.into_node_clients(&[]));

        assert_eq!(created, Broker::Ib.oms_type());
    }

    /// Not made the default of data: Nautilus makes the first data client on no venue the default
    /// itself, and refuses another made it after.
    #[rstest]
    fn the_data_client_is_routed_its_venues_and_the_execution_client_takes_every_order() {
        let ibis = || configs(&gateway(), &paper(), &[sxr8()]);

        let routed = ibis().into_node_clients(&[Venue::new("IBIS")]);
        let none = ibis().into_node_clients(&[]);

        assert_eq!(
            routed.data_routing,
            RoutingConfig {
                default: false,
                venues: Some(vec!["IBIS".to_string()])
            }
        );
        assert_eq!(none.data_routing, RoutingConfig::default());
        for clients in [routed, none] {
            assert_eq!(
                clients.exec.unwrap().routing,
                RoutingConfig::builder().default(true).build()
            );
        }
    }

    #[rstest]
    fn the_data_client_takes_the_full_session_and_tick_by_tick_quotes() {
        let data = configs(&gateway(), &paper(), &[sxr8()]).data;
        assert!(!data.batch_quotes);
        assert!(!data.use_regular_trading_hours);
        assert_eq!(
            (data.host.as_str(), data.port, data.client_id),
            ("127.0.0.1", 4002, 11)
        );
        assert_eq!(data.instrument_provider.load_ids, [sxr8()].into());
    }

    #[rstest]
    fn the_exec_client_trades_the_account_on_the_same_connection() {
        let exec = configs(&gateway(), &paper(), &[sxr8()]).exec;
        assert_eq!(exec.account_id.as_deref(), Some("DU1234567"));
        assert_eq!(
            (exec.host.as_str(), exec.port, exec.client_id),
            ("127.0.0.1", 4002, 11)
        );
        assert_eq!(exec.instrument_provider.load_ids, [sxr8()].into());
    }

    /// Starting the gateway would fail without credentials or Docker, or start a container: the
    /// client id is checked first.
    #[tokio::test]
    async fn rejects_a_client_id_ib_reserves_before_starting_the_gateway() {
        let Err(e) = clients(&paper(), 2000, &[sxr8()]).await else {
            panic!("started");
        };
        assert!(e.to_string().contains("client id 2000"), "{e}");
    }
}
