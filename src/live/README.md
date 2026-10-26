# Running live

The `live` module builds and runs a Nautilus `LiveNode` for the strategy, on
a paper then a real account (build step 3). It adds no trading logic: it
sets up the clients of the instruments' venues, fills the catalog up to now
for the IB instruments, then wires the clients, the strategy and the node.
Clients go by venue: OKX instruments to OKX's clients, all others to IB's,
and a strategy trading both gets both, routed by Nautilus.

APIs were read in the `nautilus-*` crates at `0.66.0`; the build uses the
`develop` commit in `docs/ARCHITECTURE.md` (Version), which also holds the
process-wide rules (one node per process, Nautilus owns the logger). Open
points are under "Unverified".

## Layout

```
live/
  mod.rs    `Live`: the start sequence (clients by venue, catalog filling,
            logger), the entry point
  node.rs   builds and runs the LiveNode, broker-agnostic
  redis.rs  the Redis container behind the node's cache
  ib/       IB client factories and configs, on the gateway `data`'s IB
            part starts; README.md: IB live
  okx/      OKX client factories and configs, and the account checks;
            README.md: OKX live
```

Each broker gets its own folder next to `ib/`, returning the same shape: a
data factory and config, an exec factory and config, and their routing
(`node::Clients`). `node.rs` takes a list of them as `Box<dyn
DataClientFactory>` and friends and never names a broker.

## The command

```sh
helix-trader live ema --config ema.toml [--account DU1234567] \
  [--catalog catalog] [--client-id 1] [--leverage 1]
```

It runs one strategy until Ctrl-C (SIGINT) or SIGTERM, starting the
gateway itself, so it needs no prior `fill-catalog` run. In code:
`live::Live { trader_id, catalog, account, client_id, leverage, logging
}.run(&requirements, strategy)`, generic over the strategy like
`Backtest::run`. The command's trader is `HELIX-001`, whose cache every run
loads (see "Cache in Redis").


- `--account` is the bare IB account id, else `NAUTILUS_IB_ACCOUNT_ID`,
  needed only for IB instruments; its mode (paper for `D...`) picks the
  gateway (`ib/README.md`, "Gateway").
- `--leverage` (default 1) is the leverage the run expects, as in a
  backtest, and only a check: OKX applies the account's leverage and the
  start refuses an OKX instrument at another one (`okx/README.md`); IB does
  not check it.
- `--client-id` defaults to the adapter's 1, so `fill-catalog` (10) can run
  at the same time; a multiple of 1000 is refused before any gateway starts.
- `NAUTILUS_LOG` configures the logger as for the other commands.
- The command reads `.env` at start (see `.env.example`), and a variable
  already set wins. Credentials come only from the environment, never from
  code or git.

## Start sequence

1. Install Nautilus' logger, with the node's trader id and `LoggerConfig`,
   and the `tracing` bridge: the gateway and the fill log before the node
   exists, and the node reuses that logger when built.
2. OKX instruments, if any: read `OKX_ENVIRONMENT` (`demo` or `live`),
   build the OKX client configs on it, check the credentials are set, and
   check the account (net position mode, each instrument's leverage equal
   to `--leverage`). Nothing is filled: OKX's data client serves the warmup
   (`okx/README.md`).
3. IB instruments, if any: check the client id, start or reuse the Docker
   gateway of the account's mode, wait for its login, and build the IB
   client configs on it.
4. Fill the catalog for the IB instruments' data requirements, with the
   same `IbHistory` and `fill_catalog` as `fill-catalog`: instruments, then
   each instrument's quotes over `[now - warmup, now - 1 minute]`, resuming
   after what the catalog holds, through the paper gateway (started or
   reused). The end stops a minute short because IB history refuses an end
   within the minute still in progress. The history connection closes here,
   so the node can connect with the same client id.
5. Start or reuse the Redis container (`redis.rs`), then build the node
   (`node.rs`) with its cache persisted there, the catalog in
   `LiveNodeConfig::catalogs`,
   the clients of each venue (IB's routed by default, OKX's routed by
   Nautilus as the only clients of venue OKX), reconciliation on, open-order
   and position checks every 10 and 30 s (as the adapter's
   `node_exec_tester`), and a 5 s post-stop delay.
6. Add the strategy, claim the external orders of its instruments that its
   config does not claim already (`register_external_order_claims`), then
   `run()`.

The strategy's warmup request of an IB instrument (`strategy::warmup`) is
then served by the catalog; the data engine sends IB only the interval the
catalog misses, from the fill's end to now, as historical BID_ASK ticks.
Only the catalog part reaches the internal bars: the IB ticks are dropped
(see "Known limits").

## Cache in Redis

The node persists Nautilus' cache to Redis, as Nautilus recommends for live
trading (Concepts › Cache, "Database configuration"; How-to › Configure live
trading, "Cache database configuration"): `RedisCacheConfig` from
`nautilus_infrastructure`, a `CacheDatabaseFactory`, given to
`LiveNodeBuilder::with_cache_database_factory`. The node opens it at start,
loads it (`LiveExecutionEngineConfig::load_cache`, Nautilus' default) and
only then connects the clients and reconciles, so reconciliation adds only
what the venues did since the last run. It is the node's, so it covers every
venue the node trades.

**What.** Currencies, instruments, accounts, orders and positions, with
their ids and every event since they opened, and the cache's general
entries; written as they change (`CacheConfig` defaults: JSON, no
buffering). Market data is not persisted (`save_market_data` off), nor is
strategy state (`load_state`/`save_state` off: the strategies rebuild
theirs from the warmup).

**Where.** The Docker container `helix-redis`, started or reused before the
node is built, the way the IB Gateway's is (`redis.rs`): image
`docker.io/library/redis:7.4.11-alpine` (Nautilus needs 6.2 or later), to
pull once beforehand; port 6379 on 127.0.0.1 only, so no password; restart
policy `always`; data in Redis' append-only file on the named volume
`helix-redis-data`, which outlives the container. Keys are
`trader-<trader id>:...` (`use_trader_prefix`, without the instance id), so
a run loads what the last run of the same trader left: the command's trader
is `HELIX-001`; tests use their own (`HELIXTEST-...`) and delete their keys.

**Reset.** Stop the node, then delete the trader's keys:

```sh
docker exec helix-redis redis-cli --scan --pattern 'trader-HELIX-001:*' \
  | xargs -r docker exec -i helix-redis redis-cli del
```

`CacheConfig::flush_on_start` is not the way: on Redis it flushes the whole
database, every trader's keys.

**Observed on IB paper** (`tests/ib_restart.rs`, 2026-10-26, EXS1.IBIS):

- **Restart with an open position.** Two overlapping buys of 1 make one
  netted position of 2 at 209.75; after a restart it comes back from the
  cache with the same id, quantity, average price and its two orders, and
  no other position; startup reconciliation adds nothing.
- **Position closed while the node was down** (sold behind its back
  through IB's API). Reconciliation takes IB's sell as an external order
  (`PERM-<perm id>`) claimed by the strategy, applies its fill, and the
  cached position closes before the strategy starts.
- **Order in flight when the node died, working at IB.** Startup
  reconciliation finds it among IB's open orders: `SUBMITTED` to
  `ACCEPTED`. It can then be canceled as usual.
- **Order in flight when the node died, rejected by IB meanwhile** (IB
  lists rejected orders nowhere). It stays `SUBMITTED`: no IB report names
  it, Nautilus' in-flight check covers only orders sent in the current run,
  and its open-order check resolves an order missing at the venue only in
  full-history mode (`open_check_open_only = false`), not in the default
  open-only mode (Concepts › Execution › Reconciliation, "Full-history
  checks"). A strategy that waits for no order in flight on the instrument,
  as the EMA does, then never trades it: reset the cache, or delete that
  order's keys.
- **Empty cache** (a new trader). Reconciliation rebuilds from the
  executions IB reports, the current day's: the strategy's own orders come
  back by their order reference, others as external orders claimed by the
  strategy.

## Known limits

Both are Nautilus bugs, present in `0.65.0`, `0.66.0` and the pinned
`develop` commit; there is no documented way around them, so the code keeps
the documented usage.

- **The IB part of the warmup is lost.** The adapter stamps historical
  BID_ASK ticks with `ts_init` = the time it received them
  (`nautilus-interactive-brokers` `data/core.rs`, `request_quotes`), and the
  data engine trims each response to `[start, end]` on `ts_init`
  (`DataResponse::trim_to_bounds`). The IB leg's `end` is the time the
  request was sent, so every tick falls after it and is dropped. The
  strategy's warmup therefore ends at the fill's end, a minute or so before
  the start, and the first live bar opens on the last catalog quote: its
  open and high or low can be stale, and its volume counts that quote's
  nominal size. The bars after it are right. Upstream:
  [nautilus_trader#5394](https://github.com/nautechsystems/nautilus_trader/issues/5394).
- **A live start can panic during the warmup.** `warmup::start` requests
  the quotes with `update_subscriptions` and then subscribes to the bars, as
  the Nautilus docs show. A live quote that arrives before the warmup
  response is passed to the aggregator while it is still in historical
  mode, moves its clock to now, and the response's older quotes then fail
  `VirtualClock::advance_time`'s "time must be non-decreasing" check: the
  node exits with code 101. It depends on timing, so it is more likely when
  IB history is slow or the instrument ticks fast; starting again usually
  works. Upstream:
  [nautilus_trader#5393](https://github.com/nautechsystems/nautilus_trader/issues/5393).

`0.65.0` also stops paging historical ticks after one page when a request
has both a start and an end (IB anchors on the start); the gap sent to IB
here is about a minute, well below one page, and `0.66.0` pages from the
end only.

## node.rs

`node::run` builds the node through `LiveNodeBuilder::from_config`, because
the builder has no catalog method and catalogs are a `LiveNodeConfig`
field. `nautilus-live`'s `streaming` feature is what enables
`LiveNodeConfig::catalogs` (and `with_streaming_config`).

```rust
let config = LiveNodeConfig {
    environment: Environment::Live,
    trader_id,                                   // TraderId::from("HELIX-001")
    logging,
    catalogs: vec![DataCatalogConfig::new(absolute_catalog, None, None)],
    cache: Some(warmup::cache_config(&bar_types, warmup)?), // holds the warmup bars
    data_engine: warmup::live_data_engine_config(&bar_types), // see the strategy README
    exec_engine: LiveExecutionEngineConfig {
        reconciliation: true,
        open_check_interval_secs: Some(10.0),
        position_check_interval_secs: Some(30.0),
        ..Default::default()
    },
    ..Default::default()
};
let mut node = LiveNodeBuilder::from_config(config)?
    .with_name("HELIX")
    .with_delay_post_stop_secs(5)
    .add_data_client_with_routing(None, data_factory, data_config, routing.clone())?
    .add_exec_client_with_routing(None, exec_factory, exec_config, routing)?
    .build()?;
node.add_strategy(strategy)?;
node.register_external_order_claims(strategy_id, &unclaimed)?;
node.run().await?;
```

- **Environment** is `Live`, also for a paper account: the broker client
  decides paper or live, not the node. `Backtest` is rejected; `Sandbox`
  exists for simulated exec clients and we do not use it.
- **TraderId** is `"NAME-TAG"`: ASCII with at least one hyphen, split on
  the last one, both parts non-empty. `TraderId::from` panics on bad input,
  `new_checked` returns the error.
- `with_exec_engine_config(LiveExecutionEngineConfig)` replaces the whole
  exec config, including an earlier `with_reconciliation`, so call it
  before the other `with_*` methods.

### Catalog for warmup

Warmup is described in the strategy README ("Warmup and training"). With a
catalog registered, the data engine serves a request from the catalog and
asks the adapter only for the missing intervals; without one, the adapter
answers alone.

Point the config at the catalog `fill_catalog` wrote, as an absolute path:
the node's DataFusion reader finds no object store for a relative one
(`file://catalog/...`). Catalog names must be unique. The directory must
exist, so the start creates it, empty for a run on OKX alone.

### Clients and routing

```rust
fn add_data_client_with_routing(self, name: Option<String>,
    factory: Box<dyn DataClientFactory>, config: Box<dyn ClientConfig>,
    routing: RoutingConfig) -> anyhow::Result<Self>
fn add_exec_client_with_routing(/* same shape, Box<dyn ExecutionClientFactory> */)
```

- `name: None` uses the factory's name, which becomes the `ClientId`; a
  repeated name is an error.
- `RoutingConfig { default: bool, venues: Option<Vec<String>> }` is a `bon`
  builder whose `default()` has `default: false`. For exec clients an
  explicit venue route wins, the only client for a venue routes it, and
  several clients for one venue with no route and no default client make
  `build()` fail.

### Strategy and external orders

```rust
fn add_strategy<T>(&mut self, strategy: T) -> anyhow::Result<()>
where T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static
```

`add_strategy` has the same bound as the backtest engine's `add_strategy`
(reached through `BacktestNode::get_engine_mut`), so the same strategy type
goes to both. Everything goes in before `run()`: clients, the strategy and
claims; `add_strategy` fails unless the node is `Idle`, and a duplicate
strategy id is an error.

Positions are kept as the venue keeps them: strategies leave `oms_type`
unset, so Nautilus takes each execution client's OMS (IB specifics in
`ib/README.md`, "Execution client config").

Orders found at the venue that no strategy sent belong to `EXTERNAL`. A
strategy claims them per instrument, through its config or
`register_external_order_claims`:

```rust
StrategyConfig { external_order_instrument_ids: Some(vec![instrument_id]), ..Default::default() }
```

Any node that trades reconciles and claims every instrument its strategy
trades (the whole-book rule), so reconciled orders and positions land on
the strategy. Two claims on one instrument are an error.

### Reconciliation and timeouts

| Builder method | Default |
| --- | --- |
| `with_reconciliation(bool)` | `true` |
| `with_reconciliation_lookback_mins(u32)` | no limit |
| `with_timeout_connection(u64)` (s) | 60 |
| `with_timeout_reconciliation(u64)` (s) | 30 |
| `with_delay_post_stop_secs(u64)` | 10 |
| `with_delay_shutdown_secs(u64)` | 5 |

`run()` connects the data clients, loads their instruments, connects the
exec clients, reconciles, then starts the trader; a reconciliation failure
aborts startup. Continuous open-order and position checks are off unless
`open_check_interval_secs` / `position_check_interval_secs` are set.

### Run and shutdown

```rust
async fn run(&mut self) -> anyhow::Result<()>        // NodeRunMode::Owned
fn handle(&self) -> LiveNodeHandle                   // Clone, thread-safe
LiveNodeHandle::stop(&self)
```

- `run()` installs SIGINT and SIGTERM handlers (`Owned` mode), so the
  process is the node and code stops it through `handle().stop()`, which
  works from any thread or task. `NodeRunMode::Hosted` leaves signals to the
  host.
- The loop runs on the calling thread: call `run()` from `#[tokio::main]`,
  not from a spawned task.
- `run()` can be called once.
- On stop, strategies stop (and cancel orders in `on_stop`), events keep
  flowing for `delay_post_stop`, then clients disconnect. `run()` returns an
  error if order submissions are still unresolved then, so set
  `delay_post_stop` above the time the strategy needs to flatten.

### Recording live data

`with_streaming_config(StreamingConfig)` writes live data and events to
Feather files under `writer_path`; with its `catalog` set it promotes them
into that catalog. `StreamingConfig::new(writer_path, catalog:
Option<DataCatalogConfig>, flush_interval_ms, replace_existing,
rotation_config)`; `RotationConfig` is by size, interval, scheduled dates,
or none.

### Logging

`LiveNodeConfig::logging` or `with_logging(LoggerConfig)`. Defaults:
`stdout_level: Info`, `fileout_level: Off`, `use_tracing: false`.

```rust
LoggerConfig {
    fileout_level: LevelFilter::Debug,
    file_config: Some(FileWriterConfig::new(
        Some("logs".into()), None, Some("json".into()), Some((10_000_000, 5)))),
    ..Default::default()
}
```

`FileWriterConfig::new(directory, file_name, file_format, rotate:
Option<(max_bytes, max_backups)>)`. Per-component and per-module levels:
`component_level`, `module_level`. Set `use_tracing: true` to bring adapter
logs written through `tracing` into the same output.

## Interactive Brokers

`ib/`: the IB clients on the Docker gateway, the account and its tests: see
[`ib/README.md`](ib/README.md).

## OKX

`okx/`: the OKX clients, the EEA environment, the account checks and their
tests: see [`okx/README.md`](okx/README.md).

## Tests

- Unit tests sit in a `#[cfg(test)] mod tests` at the bottom of each file,
  for what needs no node and no broker.
- No integration test runs a live node without a broker yet (the venue
  tests below run `Live` against IB and OKX). One would sit in `tests/`, one
  node per file: a node with no broker client or a test data client, the
  strategy added, stopped through `handle().stop()` from a spawned task,
  asserting on the result of `run()`, under
  `#[tokio::test(flavor = "current_thread")]`.
- Venue end-to-end tests are under "IB tests" and "OKX tests" in the venue
  READMEs; `ib_restart.rs` there checks the cache in Redis across
  restarts.

## Unverified

- How `limit` behaves when a request is served by a catalog plus the adapter.
- Whether warmup can read a catalog while streaming promotes into it.
- Whether IB reports ES's exchange as `CME` (the venue comes from that
  field), and which history depths IB serves (see `data/ib/README.md`).
