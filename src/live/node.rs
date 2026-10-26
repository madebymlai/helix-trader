//! Live node: builds and runs it.
//!
//! Uses `LiveNodeBuilder::from_config` (`nautilus_live`), since catalogs
//! are a `LiveNodeConfig` field, with the data and execution clients of
//! each adapter the instruments need, each with its routing, and the cache
//! persisted to Redis (`with_cache_database_factory`, see `redis`), adds the
//! strategy (`add_strategy`),
//! claims its instruments' external orders, and calls `run`, which stops on
//! SIGINT or SIGTERM. Follows the IB adapter's `node_exec_tester` example.
//! Never names a broker.

use std::{fmt::Debug, path::Path};

use anyhow::Context;
use nautilus_common::{
    actor::DataActorNative,
    cache::CacheConfig,
    component::Component,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
    logging::logger::LoggerConfig,
};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveDataEngineConfig, LiveExecutionEngineConfig, LiveNodeConfig, RoutingConfig},
};
use nautilus_model::identifiers::{InstrumentId, TraderId};
use nautilus_persistence::config::DataCatalogConfig;
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::redis;

/// One adapter's data and execution client factories and configs, and
/// how Nautilus routes data and orders to them.
pub(super) struct Clients {
    pub(super) data_factory: Box<dyn DataClientFactory>,
    pub(super) data_config: Box<dyn ClientConfig>,
    pub(super) exec_factory: Box<dyn ExecutionClientFactory>,
    pub(super) exec_config: Box<dyn ClientConfig>,
    pub(super) routing: RoutingConfig,
}

/// Builds the live node of `trader_id` on the clients of each adapter in
/// `clients`, routed as each says, with the catalog at `catalog` serving historical requests,
/// `cache` configuring the cache (sized for the warmup) and `data_engine`
/// the data engine (see `warmup::live_data_engine_config`), adds
/// `strategy` claiming the external orders of `instrument_ids`, and runs it
/// until SIGINT or SIGTERM.
///
/// The cache is persisted to Redis, started or reused first (see
/// `redis::start`), under `trader_id`'s keys: the node loads the orders,
/// positions and accounts of its earlier runs at start, then reconciles
/// them with the venues, and checks open orders and positions as it runs.
/// With the catalog registered, the data engine answers a request from it
/// and asks the data client only for the intervals it misses.
///
/// # Errors
///
/// Returns an error if Redis cannot be started, if the node fails to build
/// (two clients claiming the same route included), fails to load its cache, the strategy cannot be
/// added or claim its instruments, or the node fails to start, run or stop
/// (orders still unresolved after the post-stop delay included).
#[expect(clippy::too_many_arguments, reason = "the parts of one node")]
pub(super) async fn run<S>(
    trader_id: TraderId,
    catalog: &Path,
    logging: LoggerConfig,
    cache: CacheConfig,
    data_engine: LiveDataEngineConfig,
    clients: Vec<Clients>,
    instrument_ids: &[InstrumentId],
    strategy: S,
) -> anyhow::Result<()>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    let config = config(trader_id, catalog, logging, cache, data_engine)?;
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
    node.run().await
}

/// The config of the live node: `trader_id`, `logging`, `cache`, the
/// catalog at `catalog`, `data_engine` and the execution engine's
/// reconciliation and checks, after loading the cache from its database
/// (`load_cache`, Nautilus' default).
fn config(
    trader_id: TraderId,
    catalog: &Path,
    logging: LoggerConfig,
    cache: CacheConfig,
    data_engine: LiveDataEngineConfig,
) -> anyhow::Result<LiveNodeConfig> {
    // Absolute: the node's DataFusion reader finds no object store for a
    // relative path ("file://catalog/...").
    let catalog = std::path::absolute(catalog)?;
    let catalog = catalog
        .to_str()
        .with_context(|| format!("catalog path {} is not UTF-8", catalog.display()))?;
    Ok(LiveNodeConfig {
        environment: Environment::Live,
        trader_id,
        logging,
        catalogs: vec![DataCatalogConfig::new(catalog.to_string(), None, None)],
        cache: Some(cache),
        data_engine,
        exec_engine: LiveExecutionEngineConfig {
            reconciliation: true,
            open_check_interval_secs: Some(10.0),
            position_check_interval_secs: Some(30.0),
            ..Default::default()
        },
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn catalog_of(catalog: &Path) -> String {
        let config = config(
            TraderId::from("HELIX-001"),
            catalog,
            LoggerConfig::default(),
            CacheConfig::default(),
            LiveDataEngineConfig::default(),
        )
        .unwrap();
        assert_eq!(config.catalogs.len(), 1);
        config.catalogs[0].path().to_string()
    }

    #[test]
    fn the_catalog_is_registered_for_warmup() {
        assert_eq!(catalog_of(&PathBuf::from("/data/catalog")), "/data/catalog");
    }

    #[test]
    fn a_relative_catalog_is_made_absolute() {
        let absolute = std::env::current_dir().unwrap().join("catalog");
        assert_eq!(
            catalog_of(&PathBuf::from("catalog")),
            absolute.to_str().unwrap()
        );
    }
}
