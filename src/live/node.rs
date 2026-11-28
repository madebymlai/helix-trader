//! Builds and runs the live node, and names no broker.
//!
//! The module uses `LiveNodeBuilder::from_config`, since the engine configs are `LiveNodeConfig`
//! fields. It follows the IB adapter's `node_exec_tester` example. Once the node is built, the
//! module:
//! - Promotes what the last run left staged (`promote_staged`).
//! - Adds the retrain actor and the strategy.
//! - Claims the external orders of the instruments the strategy trades.
//! - Runs until SIGINT or SIGTERM.
//! - Disposes the node, also when a step fails.

use std::fmt::Debug;

use nautilus_common::{
    actor::DataActorNative,
    cache::{CacheConfig, database::CacheDatabaseFactory},
    component::Component,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
    logging::logger::LoggerConfig,
};
use nautilus_core::UUID4;
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveDataEngineConfig, LiveExecutionEngineConfig, LiveNodeConfig, RoutingConfig},
    node::LiveNode,
};
use nautilus_model::identifiers::TraderId;
use nautilus_persistence::config::{DataCatalogConfig, StreamingConfig};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::recording::promote_staged;
use crate::model::launcher::RetrainActor;

/// The clients of one adapter on the node.
///
/// They are the adapter's data client (factory, config and routing) and its execution client, if
/// the node trades there.
///
/// Nautilus' data engine makes the first data client with no venue the default
/// (`DataEngine::register_client`, crates/data/src/engine/registry.rs:44). It registers clients in
/// the node builder's `HashMap` order (crates/live/src/node/builder.rs:94, :642). It refuses to
/// make a second client the default (`set_default_client`, registry.rs:101). Helix routes every
/// venue a run reads, so nothing depends on which client becomes the default.
pub struct Clients {
    pub data_factory: Box<dyn DataClientFactory>,
    pub data_config: Box<dyn ClientConfig>,
    pub data_routing: RoutingConfig,
    pub exec: Option<ExecClient>,
}

/// The execution client of an adapter on the node: its factory, config and order routing.
pub struct ExecClient {
    pub factory: Box<dyn ExecutionClientFactory>,
    pub config: Box<dyn ClientConfig>,
    pub routing: RoutingConfig,
}

/// Builds the live node of `trader_id`, as `instance_id`, on the clients of each adapter in
/// `clients`, with `cache` sized for the warmup and `data_engine`.
///
/// Then promotes what the last run of the instance left staged (`promote_staged`), before
/// anything reads or writes the catalog. A node that cannot promote it is disposed. The cache is
/// persisted to `cache_database`, if given (see `redis::start`). `catalog` answers the historical
/// requests first. Then the data clients answer, and their answers are written back (see
/// `strategy::warmup`).
///
/// # Errors
///
/// Returns an error if:
/// - The node fails to build, including when two clients claim the same route.
/// - What the last run staged cannot be promoted.
#[expect(clippy::too_many_arguments)]
pub fn build(
    trader_id: TraderId,
    instance_id: UUID4,
    logging: LoggerConfig,
    cache: CacheConfig,
    data_engine: LiveDataEngineConfig,
    streaming: StreamingConfig,
    catalog: DataCatalogConfig,
    cache_database: Option<Box<dyn CacheDatabaseFactory>>,
    clients: Vec<Clients>,
) -> anyhow::Result<LiveNode> {
    let config = config(
        trader_id,
        instance_id,
        logging,
        cache,
        data_engine,
        streaming,
        catalog,
    );
    let mut builder = LiveNodeBuilder::from_config(config)?
        .with_name("HELIX")
        .with_delay_post_stop_secs(5);

    if let Some(cache_database) = cache_database {
        builder = builder.with_cache_database_factory(cache_database);
    }
    let mut node = add_clients(builder, clients)?.build()?;
    match promote_staged(&mut node) {
        Ok(()) => Ok(node),
        Err(e) => {
            node.dispose();
            Err(e)
        }
    }
}

/// Returns `builder` with the data client of each adapter in `clients`, and its execution client if
/// it has one.
///
/// # Errors
///
/// Returns an error if two clients take one name.
pub(crate) fn add_clients(
    mut builder: LiveNodeBuilder,
    clients: Vec<Clients>,
) -> anyhow::Result<LiveNodeBuilder> {
    for clients in clients {
        builder = builder.add_data_client_with_routing(
            None,
            clients.data_factory,
            clients.data_config,
            clients.data_routing,
        )?;

        if let Some(exec) = clients.exec {
            builder = builder.add_exec_client_with_routing(
                None,
                exec.factory,
                exec.config,
                exec.routing,
            )?;
        }
    }
    Ok(builder)
}

/// Adds `retraining`, then `strategy`, to `node`, runs it until SIGINT or SIGTERM, then disposes
/// it. The node is disposed also when any of those steps fails.
///
/// # Errors
///
/// Returns an error if:
/// - The node fails to load its cache.
/// - The retrain actor or the strategy cannot be added.
/// - The strategy cannot claim its instruments.
/// - The node fails to start, run or stop, including when orders are still unresolved after the
///   post-stop delay.
pub(super) async fn run<S>(
    mut node: LiveNode,
    strategy: S,
    retraining: Option<RetrainActor>,
) -> anyhow::Result<()>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    let result = match add(&mut node, strategy, retraining) {
        Ok(()) => node.run().await,
        Err(e) => Err(e),
    };
    // Disposing closes the cache database
    node.dispose();
    result
}

fn add<S>(node: &mut LiveNode, strategy: S, retraining: Option<RetrainActor>) -> anyhow::Result<()>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    if let Some(retraining) = retraining {
        node.add_actor(retraining)?;
    }
    node.add_strategy(strategy)
}

/// Every `PURGE_INTERVAL_MINS`, closed orders, closed positions and account events older than
/// `PURGE_AFTER_MINS` (a week) are purged from memory. Redis keeps them.
const PURGE_INTERVAL_MINS: u32 = 60;
const PURGE_AFTER_MINS: u32 = 7 * 24 * 60;

fn config(
    trader_id: TraderId,
    instance_id: UUID4,
    logging: LoggerConfig,
    cache: CacheConfig,
    data_engine: LiveDataEngineConfig,
    streaming: StreamingConfig,
    catalog: DataCatalogConfig,
) -> LiveNodeConfig {
    LiveNodeConfig {
        catalogs: vec![catalog],
        streaming: Some(streaming),
        environment: Environment::Live,
        trader_id,
        instance_id: Some(instance_id),
        logging,
        cache: Some(cache),
        data_engine,
        exec_engine: LiveExecutionEngineConfig {
            open_check_interval_secs: Some(10.0),
            position_check_interval_secs: Some(30.0),
            purge_closed_orders_interval_mins: Some(PURGE_INTERVAL_MINS),
            purge_closed_orders_buffer_mins: Some(PURGE_AFTER_MINS),
            purge_closed_positions_interval_mins: Some(PURGE_INTERVAL_MINS),
            purge_closed_positions_buffer_mins: Some(PURGE_AFTER_MINS),
            purge_account_events_interval_mins: Some(PURGE_INTERVAL_MINS),
            purge_account_events_lookback_mins: Some(PURGE_AFTER_MINS),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use ahash::AHashSet;
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;
    use crate::{
        live::Recording,
        strategy::ema::{EmaCross, EmaCrossConfig},
    };

    const INSTANCE_ID: &str = "2d89666b-1a1e-4a75-b193-4eb3b454c757";

    fn offline_node() -> LiveNode {
        let config = LiveNodeConfig {
            environment: Environment::Live,
            trader_id: TraderId::from("HELIX-001"),
            ..Default::default()
        };
        LiveNodeBuilder::from_config(config)
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn a_strategy_claims_the_external_orders_of_the_instruments_it_trades_alone() {
        let config = EmaCrossConfig::load(
            r#"
            [data]
            series = [
                { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
            ]
            warmup = "1h"

            [indicators]
            fast = { period = 1 }
            slow = { period = 2 }

            [trade]
            instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
            risk = "1%"
            conversions = ["EURUSD.ICMARKETS"]
            "#,
        )
        .unwrap();
        let mut node = offline_node();
        let strategy = EmaCross::live(config).unwrap();
        let gold = InstrumentId::from("XAUUSD.ICMARKETS");

        let added = add(&mut node, strategy, None);

        let claimed = node
            .kernel()
            .cache()
            .borrow()
            .external_order_claim_instrument_ids(None);
        node.dispose();
        added.unwrap();
        assert_eq!(claimed, AHashSet::from_iter([gold]));
    }

    fn node_config() -> LiveNodeConfig {
        let catalog = DataCatalogConfig::new("/data/catalog".to_string(), None, None);
        let recording = Recording {
            staging: "/data/staging".into(),
        };
        config(
            TraderId::from("HELIX-001"),
            UUID4::from(INSTANCE_ID),
            LoggerConfig::default(),
            CacheConfig::default(),
            LiveDataEngineConfig::default(),
            recording.streaming(&catalog).unwrap(),
            catalog,
        )
    }

    #[rstest]
    fn open_orders_and_positions_are_checked_and_closed_ones_purged_hourly_after_a_week() {
        let exec = node_config().exec_engine;
        let week = Some(7 * 24 * 60);
        assert_eq!(exec.open_check_interval_secs, Some(10.0));
        assert_eq!(exec.position_check_interval_secs, Some(30.0));
        assert_eq!(exec.purge_closed_orders_interval_mins, Some(60));
        assert_eq!(exec.purge_closed_orders_buffer_mins, week);
        assert_eq!(exec.purge_closed_positions_interval_mins, Some(60));
        assert_eq!(exec.purge_closed_positions_buffer_mins, week);
        assert_eq!(exec.purge_account_events_interval_mins, Some(60));
        assert_eq!(exec.purge_account_events_lookback_mins, week);
        assert!(!exec.purge_from_database);
    }

    #[rstest]
    fn the_catalog_is_registered_writable() {
        let [catalog] = node_config().catalogs.try_into().unwrap();
        assert_eq!(catalog.path(), "/data/catalog");
        assert!(!catalog.read_only());
    }

    #[rstest]
    fn the_node_streams_into_its_catalog_under_its_kept_instance_id() {
        let config = node_config();
        assert_eq!(config.instance_id, Some(UUID4::from(INSTANCE_ID)));
        let streaming = config.streaming.unwrap();
        assert_eq!(streaming.writer_path, "/data/staging");
        assert_eq!(streaming.catalog.unwrap().path(), "/data/catalog");
    }
}
