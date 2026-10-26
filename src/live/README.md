# Running live

The `live` module builds and runs a Nautilus `LiveNode` for the strategy, on
a paper then a real account (build step 3). It adds no trading logic: it
sets up the clients of the instruments' venues, then wires the clients, the
strategy and the node. The venues' data clients serve the warmup: the node
registers no catalog.
Clients go by venue: OKX instruments to OKX's clients, all others to IB's,
and a strategy trading both gets both, routed by Nautilus.

APIs were read in the `nautilus-*` crates at `0.66.0`; the build uses the
`develop` commit in `docs/ARCHITECTURE.md` (Version), which also holds the
process-wide rules (one node per process, Nautilus owns the logger). Open
points are under "Unverified".

## Layout

```
live/
  mod.rs    `Live`: the start sequence (clients by venue, logger), the
            entry point
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
  [--client-id 1] [--leverage 1] [--trader-id HELIX-001]
```

It runs one strategy until Ctrl-C (SIGINT) or SIGTERM, starting the
gateway itself, so it needs no prior `fill-catalog` run. In code:
`live::Live { trader_id, account, client_id, leverage, logging
}.run(&requirements, strategy)`, generic over the strategy like
`Backtest::run`. It starts or reuses the Redis container that holds the
node's cache (see "Cache in Redis").


- `--account` is the bare IB account id, else `NAUTILUS_IB_ACCOUNT_ID`,
  needed only for IB instruments; its mode (paper for `D...`) picks the
  gateway (`ib/README.md`, "Gateway").
- `--leverage` (default 1) is the leverage the run expects, as in a
  backtest, and only a check: OKX applies the account's leverage and the
  start refuses an OKX instrument at another one (`okx/README.md`); IB does
  not check it.
- `--trader-id` (default `HELIX-001`, checked by Nautilus: a name and a tag
  joined by a hyphen) is the node's trader, whose cache in Redis each run
  loads. Live processes running at the same time each need their own
  trader id, since the Redis keys are per trader.
- `--client-id` defaults to the adapter's 1, so `fill-catalog` (10) can run
  at the same time; a multiple of 1000 is refused before any gateway starts.
- `NAUTILUS_LOG` configures the logger as for the other commands.
- The command reads `.env` at start (see `.env.example`), and a variable
  already set wins. Credentials come only from the environment, never from
  code or git.

## Start sequence

1. Install Nautilus' logger, with the node's trader id and `LoggerConfig`,
   and the `tracing` bridge: the gateway logs before the node exists, and
   the node reuses that logger when built.
2. OKX instruments, if any: read `OKX_ENVIRONMENT` (`demo` or `live`),
   build the OKX client configs on it, check the credentials are set, and
   check the account (net position mode, each instrument's leverage equal
   to `--leverage`). OKX's data client serves the warmup (`okx/README.md`).
3. IB instruments, if any: check the client id, start or reuse the Docker
   gateway of the account's mode, wait for its login, and build the IB
   client configs on it. IB's data client serves the warmup (below).
4. Start or reuse the Redis container (`redis.rs`), then build the node
   (`node.rs`) with its cache persisted there, the clients of each venue
   (IB's routed by default, OKX's routed by Nautilus as the only clients of
   venue OKX), reconciliation on, open-order and position checks every 10
   and 30 s (as the adapter's `node_exec_tester`), and a 5 s post-stop
   delay.
5. Add the strategy, claim the external orders of its instruments that its
   config does not claim already (`register_external_order_claims`), then
   `run()`.

### Warmup and live bars on IB

A strategy on IB trades composite bars of IB's 1-minute bars, such as
`SXR8.IBIS-1-HOUR-MID-INTERNAL@1-MINUTE-EXTERNAL` (`strategy/README.md`,
"Bar types"). Its warmup request (`strategy::warmup`) asks for
`SXR8.IBIS-1-MINUTE-MID-EXTERNAL` from `now - warmup`, with no end. With no
catalog registered, the data engine passes it to IB's data client
(`request_bars`, `adapters/interactive_brokers/src/data/core.rs`), which
splits the window into IB durations (`calculate_duration_segments`) and
answers with IB's bars up to now, stamped at their close. The subscription
to the bars then goes to IB's `subscribe_bars`, which for any bar size but 5
seconds runs a keepUpToDate historical request (`streams.rs`,
`handle_historical_bars_subscription`): it starts at the subscription and
publishes each minute once, when IB's update after its close arrives, or
2 s after the close at the latest (`HistoricalBarCompletion`).

Measured on paper (`tests/ib_minute_bars.rs`, three runs on 2026-10-26
between 15:29 and 15:43 UTC, SXR8.IBIS, 2-hour warmup): the warmup answer
came 0.1 to 0.2 s after the request, 120 or 121 bars; the 12 live minute
bars arrived 0.4 to 1.3 s after their close, under the 5 s the node waits
before building a bar of source bars (`strategy/README.md`, "Composite
bars"). The hand-off repeats one minute (see "Known limits").

Without a catalog, `fill-catalog` and `live` share nothing: `live` never
reads the catalog, and a node registering it would serve the warmup from
it and start the subscription of a stored source bar type where the
catalog ends (the data engine prefills the subscription's `start_ns` from
the catalog, `data/src/engine/streaming.rs`), replaying every bar since as
live.

## Cache in Redis

The node persists Nautilus' cache to Redis, as Nautilus recommends for live
trading (Concepts › Cache, "Database configuration"; How-to › Configure live
trading, "Cache database configuration"): `RedisCacheConfig` from
`nautilus_infrastructure`, a `CacheDatabaseFactory`, given to
`LiveNodeBuilder::with_cache_database_factory`. The node opens it at start,
loads it (`LiveExecutionEngineConfig::load_cache`, Nautilus' default) and
only then connects the clients and reconciles, so reconciliation adds only
what the venues did since the last run. It is the node's, so it covers every
venue the node trades. When `run` returns, the node is disposed, which
closes the cache database, as Nautilus documents.

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
a run loads what the last run of the same trader left (`--trader-id`);
tests use their own (`HELIXTEST-...`) and delete their keys.

**Purging.** Every hour, Nautilus purges closed orders, closed positions
and account events older than 7 days (`LiveExecutionEngineConfig`
`purge_*_interval_mins` 60, buffers and lookback 10080; Concepts › Cache,
"Automatic purging in live trading"); open orders and positions never.
Memory only: the pinned Nautilus refuses `purge_from_database` in the Rust
live runtime ("not supported by the Rust live runtime yet", its config
validation), so it stays off and Redis keeps them (see "Reset").

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

Observed at the pinned `develop` commit; the code keeps the documented
usage.

- **The hand-off repeats the minute in progress.** IB's answer to the
  warmup's `request_bars` ends with the minute still in progress at the
  request, a bar closing after it, and the response, which has no end,
  keeps it (`DataResponse::trim_to_bounds` trims only to a given end). The
  live stream then publishes that minute again, complete. The composite
  aggregator takes both into the bar spanning the hand-off: its open,
  high, low and close are those it would have without the repeat (the
  partial minute lies within the complete one, which comes last), but its
  volume counts the partial minute too (nothing on MID bars, whose volume
  is 0). Requesting the warmup with an explicit `end` of now drops it
  (Nautilus trims the response to `[start, end]`): measured, the warmup
  then ended on the last closed minute and the live bars followed it with
  no gap and no duplicate.
- **A live start can panic during the warmup.** `warmup::start` requests
  the source bars with `update_subscriptions` and then subscribes to the
  bars, as the Nautilus docs show. A live bar that arrives before the
  warmup response is passed to the aggregator while it is still in
  historical mode, moves its clock to now, and the response's older bars
  then fail `VirtualClock::advance_time`'s "time must be non-decreasing"
  check: the node exits with code 101. IB answered a 2-hour warmup in
  0.2 s, but a 720-hour one in 67 s, so a minute bar always came first and
  the node panicked (paper, 2026-10-26, SXR8.IBIS: `to_time_ns`
  2026-09-26 < `from_time_ns` 2026-10-26 15:49). On IB a warmup must be
  answered within the minute: hours, not weeks. Upstream:
  [nautilus_trader#5393](https://github.com/nautechsystems/nautilus_trader/issues/5393).
- **A warmup request fails its last, 1-second segment.** The window runs
  from `now - warmup` to the client's own `now`, a moment later, and
  `calculate_duration_segments` rounds the fraction of a second up into a
  `1 S` segment, which IB refuses (error 321, "Historical data requested
  duration is invalid"). The data client logs a warning and answers with
  the bars of the earlier segments; the `1 S` segment holds none anyway.
  Seen with the 720-hour warmup above (its `30 D` segment raised no
  error).
- **A warmup of a day or more may miss bars.** The data client splits the
  window into years, days and seconds (`calculate_duration_segments`), and
  IB counts `D` in trading sessions, not 24 h, so bars between the days and
  seconds segments can go missing without an error, as the same split did
  for history (`data/ib/README.md`, "Walking back, not segments";
  nautilus_trader#5384). Not observed live yet.

## node.rs

`node::run` builds the node through `LiveNodeBuilder::from_config`, with
the engine configs as `LiveNodeConfig` fields. It registers no catalog
(`LiveNodeConfig::catalogs`, enabled by `nautilus-live`'s `streaming`
feature, stays empty; see "Warmup and live bars on IB").

```rust
let config = LiveNodeConfig {
    environment: Environment::Live,
    trader_id,                                   // TraderId::from("HELIX-001")
    logging,
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

- Whether IB's answer to a `30 D` request of 1-minute bars is complete
  (it raised no error, but the node panicked before the bars could be
  counted).
- Whether IB reports ES's exchange as `CME` (the venue comes from that
  field), and which history depths IB serves (see `data/ib/README.md`).
