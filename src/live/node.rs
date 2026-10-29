//! Live node: builds and runs it.
//!
//! Uses `LiveNodeBuilder::from_config` (`nautilus_live`), since the
//! engine configs are `LiveNodeConfig` fields, with the data and execution clients of
//! each adapter the instruments need, each with its routing, and the cache
//! persisted to Redis (`with_cache_database_factory`, see `redis`),
//! promotes what the last run left staged once built (`promote_staged`), adds
//! the strategy (`add_strategy`), claims its instruments' external
//! orders, calls `run`, which stops on SIGINT or SIGTERM, and disposes the
//! node, which closes the cache database. Follows the IB adapter's
//! `node_exec_tester` example.
//! Never names a broker.

use std::fmt::Debug;

use anyhow::Context;
use nautilus_common::{
    actor::DataActorNative,
    cache::CacheConfig,
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
use nautilus_model::identifiers::{InstrumentId, TraderId};
use nautilus_persistence::config::{DataCatalogConfig, StreamingConfig};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{recording::promote_staged, redis};

/// One adapter's data and execution client factories and configs, and
/// how Nautilus routes data and orders to them.
pub(super) struct Clients {
    pub(super) data_factory: Box<dyn DataClientFactory>,
    pub(super) data_config: Box<dyn ClientConfig>,
    pub(super) exec_factory: Box<dyn ExecutionClientFactory>,
    pub(super) exec_config: Box<dyn ClientConfig>,
    pub(super) routing: RoutingConfig,
}

/// Builds the live node of `trader_id`, as its `instance_id`, on the
/// clients of each adapter in `clients`, routed as each says, with `cache`
/// configuring the cache (sized for the warmup) and `data_engine`
/// the data engine (see `warmup::live_data_engine_config`), then promotes
/// what the last run of its instance left staged (`promote_staged`), so
/// that the catalog holds it before anything reads or writes it.
///
/// The cache is persisted to Redis, started or reused first (see
/// `redis::start`), under `trader_id`'s keys: the node loads the orders,
/// positions and accounts of its earlier runs at start, then reconciles
/// them with the venues, and checks open orders and positions as it runs.
/// `catalog` is registered, the directory fill-catalog fills and backtests
/// read: it answers the historical requests first, the data clients
/// what it lacks, and answers asked to are written back to it (see
/// `strategy::warmup`). The data the node receives is streamed into it as
/// `streaming` says (see `Recording`).
///
/// # Errors
///
/// Returns an error if Redis cannot be started, if the node fails to build
/// (two clients claiming the same route included), or what the last run
/// staged cannot be promoted.
#[expect(clippy::too_many_arguments)]
pub(super) async fn build(
    trader_id: TraderId,
    instance_id: UUID4,
    logging: LoggerConfig,
    cache: CacheConfig,
    data_engine: LiveDataEngineConfig,
    streaming: StreamingConfig,
    catalog: DataCatalogConfig,
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
    let cache_database = redis::start().await?;
    let mut builder = LiveNodeBuilder::from_config(config)?
        .with_name("HELIX")
        .with_cache_database_factory(Box::new(cache_database))
        .with_delay_post_stop_secs(5);
    for clients in clients {
        builder = builder
            .add_data_client_with_routing(
                None,
                clients.data_factory,
                clients.data_config,
                clients.routing.clone(),
            )?
            .add_exec_client_with_routing(
                None,
                clients.exec_factory,
                clients.exec_config,
                clients.routing,
            )?;
    }
    let mut node = builder.build()?;
    promote_staged(&mut node)?;
    Ok(node)
}

/// Adds `strategy` to `node` claiming the external orders of
/// `instrument_ids`, and runs it until SIGINT or SIGTERM, then disposes
/// it.
///
/// # Errors
///
/// Returns an error if the node fails to load its cache, the strategy
/// cannot be added or claim its instruments, or the node fails to start,
/// run or stop (orders still unresolved after the post-stop delay
/// included).
pub(super) async fn run<S>(
    mut node: LiveNode,
    instrument_ids: &[InstrumentId],
    strategy: S,
) -> anyhow::Result<()>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    let strategy_id = strategy
        .strategy_id()
        .context("the strategy has no id to claim its instruments with")?;
    // Its config may claim some already; `add_strategy` registers those.
    let claimed = strategy.external_order_instrument_ids().unwrap_or_default();
    let unclaimed: Vec<InstrumentId> = instrument_ids
        .iter()
        .filter(|id| !claimed.contains(id))
        .copied()
        .collect();
    node.add_strategy(strategy)?;
    node.register_external_order_claims(strategy_id, &unclaimed)?;
    // As Nautilus documents it: disposing closes the cache database.
    let result = node.run().await;
    node.dispose();
    result
}

/// How often Nautilus purges the cache of closed orders, closed positions
/// and account events older than `PURGE_AFTER_MINS`, a week; never an open
/// order or position. Memory only: Nautilus' Rust node refuses
/// `purge_from_database`, so Redis keeps them.
const PURGE_INTERVAL_MINS: u32 = 60;
const PURGE_AFTER_MINS: u32 = 7 * 24 * 60;

/// The config of the live node: `trader_id` and its kept `instance_id`,
/// `logging`, `cache`, `data_engine`, the one `catalog`, `streaming` into
/// it, and the execution engine's reconciliation and checks, after loading
/// the cache from its database (`load_cache`, Nautilus' default), and its
/// purges (`PURGE_INTERVAL_MINS`).
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
            reconciliation: true,
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
    use std::time::Duration;

    use super::*;
    use crate::live::Recording;

    const INSTANCE_ID: &str = "2d89666b-1a1e-4a75-b193-4eb3b454c757";

    fn node_config() -> LiveNodeConfig {
        let catalog = DataCatalogConfig::new("/data/catalog".to_string(), None, None);
        let recording = Recording {
            staging: "/data/staging".into(),
            promotion_interval: Duration::from_secs(5 * 60),
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

    #[test]
    fn closed_orders_positions_and_account_events_are_purged_hourly_after_a_week() {
        let exec = node_config().exec_engine;
        let week = Some(7 * 24 * 60);
        assert_eq!(exec.purge_closed_orders_interval_mins, Some(60));
        assert_eq!(exec.purge_closed_orders_buffer_mins, week);
        assert_eq!(exec.purge_closed_positions_interval_mins, Some(60));
        assert_eq!(exec.purge_closed_positions_buffer_mins, week);
        assert_eq!(exec.purge_account_events_interval_mins, Some(60));
        assert_eq!(exec.purge_account_events_lookback_mins, week);
        assert!(!exec.purge_from_database);
    }

    #[test]
    fn the_catalog_is_registered_writable() {
        let [catalog] = node_config().catalogs.try_into().unwrap();
        assert_eq!(catalog.path(), "/data/catalog");
        assert!(!catalog.read_only());
    }

    #[test]
    fn the_node_streams_into_its_catalog_under_its_kept_instance_id() {
        let config = node_config();
        assert_eq!(config.instance_id, Some(UUID4::from(INSTANCE_ID)));
        let streaming = config.streaming.unwrap();
        assert_eq!(streaming.writer_path, "/data/staging");
        assert_eq!(streaming.catalog.unwrap().path(), "/data/catalog");
    }
}
