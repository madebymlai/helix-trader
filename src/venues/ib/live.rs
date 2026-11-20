//! Interactive Brokers clients for the live node.
//!
//! This module provides `nautilus_interactive_brokers`' data and execution client factories and
//! configs, on the gateway for the account's trading mode (`IbGateway`). It also defines what the
//! data client streams (`streams`) and the IB flags of the live command (`IbLiveArgs`).

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

#[derive(Debug, Clone, Args)]
pub(crate) struct IbLiveArgs {
    /// The IB account traded, for example DU1234567 (paper); needed only for IB instruments.
    #[arg(long, env = "NAUTILUS_IB_ACCOUNT_ID")]
    pub(in crate::venues) account: Option<IbAccount>,
    /// IB client id of the node, the adapter default.
    #[arg(long, default_value_t = 1)]
    pub(in crate::venues) client_id: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct IbClients {
    pub(super) data: InteractiveBrokersDataClientConfig,
    pub(super) exec: InteractiveBrokersExecutionClientConfig,
}

/// Checks `client_id`, starts or reuses `account`'s gateway and returns the client configs on it.
///
/// # Errors
///
/// Returns an error if:
/// - The client id is a multiple of 1000, which IB reserves for the execution client (checked
///   before any gateway starts).
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

// The data client is the one history uses (`data_client_config`), so its bars are those
// `fill-catalog` stores, quotes come tick by tick (`batch_quotes: false`, and the adapter falls
// back to `reqMktData` where IB refuses them), and data and exec share `gateway`'s client id, so
// they use one connection.
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
    /// Returns the node clients for IB.
    ///
    /// - The data client is routed the venues in `data_venues` whose live data is IB's, or none if
    ///   there are none. It is never the default (see `live::node::Clients`).
    /// - The execution client is the default route for orders, since IB trades venues whose data is
    ///   LSE's.
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

/// IB's book as Nautilus' IB data client streams it (`venues::book_format`).
///
/// - L2_MBP deltas, the rows of IB's market depth by position.
/// - On SMART depth the rows are by posting exchange, so an update can move a row to another price
///   (`subscribe_book_deltas`, `data/core.rs:1249`, `:1301-1306`; `data/streams.rs:1952-2027`).
/// - An L3_MBO subscription streams nothing (`core.rs:1262-1268`).
/// - There are no depth snapshots (`common/src/clients/data.rs:171-174`).
/// - A deltas subscription with no depth asks for 20 rows a side (`core.rs:1299`).
pub(in crate::venues) const BOOK: BookFormat = BookFormat {
    deltas: BookType::L2_MBP,
    depth: None,
};

/// The narrowest bars IB keeps up to date: "only ... with bar sizes 5 seconds or greater" (TWS API,
/// "Historical Bar Data"). Real-time bars are 5 seconds.
const KEPT_UP_TO_DATE: SignedDuration = SignedDuration::from_secs(5);

/// Checks that Nautilus' IB data client streams `source`, as a run subscribes to it.
///
/// The client streams:
/// - Quotes (`subscribe_quotes`, `data/core.rs:724`).
/// - Trades (`:979`), but not a currency pair's. The client logs those as unsupported
///   (`:1000-1006`, see `CURRENCY_PAIR_VENUES`).
/// - IB's own (`EXTERNAL`) time bars (`:1093`), priced at one of `BAR_PRICES`, a forex pair's LAST
///   bars aside:
///   - The size must be one IB has (`bar_type_to_ib_bar_size`), and not 1 second. The subscription
///     task fails any other size.
///   - 5 seconds is streamed as real-time bars (`:1166`).
///   - Other sizes are streamed as historical bars kept up to date (`data/streams.rs:817-821`,
///     `KEPT_UP_TO_DATE`).
/// - L2_MBP book deltas (`BOOK`).
///
/// The client streams no book depth. Nautilus' `DataClient` default logs "Handler not implemented"
/// (`common/src/clients/data.rs:171-174`).
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

fn streamed_size(bar_type: &BarType) -> bool {
    bar_type_to_ib_bar_size(bar_type).is_ok() && bar_type.spec().timedelta() >= KEPT_UP_TO_DATE
}

// Lists each valid step up to 60 of each time aggregation, so a refusal offers the rule's sizes
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
    /// (`crates/adapters/interactive_brokers/src/factories.rs:182`).
    /// `Broker::oms_type` copies that setting.
    #[rstest]
    fn drift_the_execution_client_keeps_positions_as_the_broker_answers() {
        let clients = configs(&gateway(), &paper(), &[sxr8()]);

        let created = created_oms_type(clients.into_node_clients(&[]));

        assert_eq!(created, Broker::Ib.oms_type());
    }

    /// Nautilus makes the first data client on no venue the default and refuses another after it.
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

    #[tokio::test]
    async fn rejects_a_client_id_ib_reserves_before_starting_the_gateway() {
        let Err(e) = clients(&paper(), 2000, &[sxr8()]).await else {
            panic!("started");
        };
        assert!(e.to_string().contains("client id 2000"), "{e}");
    }
}
