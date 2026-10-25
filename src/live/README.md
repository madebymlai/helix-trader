# Running live

The `live` module builds and runs a Nautilus `LiveNode` for the strategy, on
a paper then a real account (build step 3). It adds no trading logic: it
sets up the clients of the instruments' venues, fills the catalog up to now
for the IB ones, then wires the clients, the strategy and the node. OKX
instruments go to OKX's clients, all others to IB's; a strategy trading
both gets both. Filling the catalog needs no node (see the data README).

APIs were read in the `nautilus-*` crates at `0.66.0`; the build uses the
`develop` commit in `docs/ARCHITECTURE.md` (Version). Open points are under
"Unverified". Process-wide rules (one node per process, Nautilus owns the
logger) are in `docs/ARCHITECTURE.md`.

## The command

```sh
helix-trader live ema --config ema.toml [--account DU1234567] \
  [--catalog catalog] [--client-id 1] [--leverage 1]
```

Runs one strategy until Ctrl-C (SIGINT) or SIGTERM. `--account` is the bare
IB account id, else `NAUTILUS_IB_ACCOUNT_ID`, needed only for IB
instruments; its mode (paper for `D...`) picks the gateway (see `ib/README.md`, "Gateway").
`--leverage` (default 1) is the leverage the run expects, as in a backtest;
it is a check only: OKX applies the leverage set on the account, and the
start refuses an OKX instrument at another one (see `okx/README.md`). IB
does not check it. A client id that is a multiple of 1000
is refused before any gateway starts. The default client id is the
adapter's, 1, so `fill-catalog` (10) can run at the same time.
`NAUTILUS_LOG` configures the logger as for the other commands. The
command reads `.env` at start (see `.env.example`); a variable already set
wins.

No `fill-catalog` run is needed first, nor a gateway: the command starts
it. In code: `live::Live { catalog, account, client_id, leverage, logging
}.run(&requirements, strategy)`, generic over the strategy like
`Backtest::run`.

### Start sequence

1. Install Nautilus' logger, with the node's trader id and `LoggerConfig`,
   and the `tracing` bridge: the gateway and the fill log before the node
   exists, and the node reuses that logger when built.
2. OKX instruments, if any: read `OKX_ENVIRONMENT` (`demo` or `live`),
   build the OKX client configs on it, check the credentials are set,
   check the account (net position mode, each instrument's leverage equal
   to `--leverage`). Nothing is filled: OKX's data client serves the warmup
   (see `okx/README.md`).
3. IB instruments, if any: check the client id, start or reuse the Docker
   gateway of the account's mode, wait for its login, and build the IB
   client configs on it.
4. Fill the catalog for the IB instruments' data requirements, with the same
   `IbHistory` and `fill_catalog` as `fill-catalog`: instruments, then each
   instrument's quotes over `[now - warmup, now - 1 minute]`, resuming after
   what the catalog holds, through the paper gateway (started or reused). IB history refuses an end within a minute of now
   (the minute still in progress). The history connection closes here, so
   the node can connect with the same client id.
5. Build the node with the catalog in `LiveNodeConfig::catalogs`, the
   clients of each venue (IB's routed by default, OKX's routed by Nautilus
   as the only clients of venue OKX), reconciliation on, open-order and
   position checks every 10 and 30 s (as the adapter's `node_exec_tester`),
   and a 5 s post-stop delay.
6. Add the strategy and claim the external orders of its instruments (the
   ones its config does not claim already), then `run()`.

The strategy's warmup request of an IB instrument (`strategy::warmup`) is then served by the
catalog; the data engine sends IB only the interval the catalog misses,
from the fill's end to now, as historical BID_ASK ticks. Only the catalog
part reaches the internal bars: the IB ticks are dropped (see "Known
limits").

### Known limits

Both are Nautilus bugs, present in `0.65.0`, `0.66.0` and the pinned
`develop` commit; there is no
documented way around them, so the code keeps the documented usage.

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

### IB quote subscription

The data client sets `batch_quotes: false`: quotes are subscribed tick by
tick (`reqTickByTickData` BID_ASK); the adapter falls back to `reqMktData`
where IB refuses, and always uses it for BAG spreads. Internal bars are then
built from every quote change. It also sets `use_regular_trading_hours:
false`, as the history in the catalog does.

## Layout

```
live/
  mod.rs    `Live`: the start sequence (clients by venue, catalog filling,
            logger), the entry point
  node.rs   builds and runs the LiveNode, broker-agnostic
  ib/       Interactive Brokers client factories and configs, on the
            gateway `data`'s IB part starts; README.md: IB live
  okx/      OKX client factories and configs, and the account checks;
            README.md: OKX live
```

Another broker is another folder next to `ib/` returning the same shape: a
data factory and config, an exec factory and config, and their routing.
`node.rs` takes a list of them as `Box<dyn DataClientFactory>` and friends
and never names a broker.

## Cargo

```toml
nautilus-live = { git = "https://github.com/nautechsystems/nautilus_trader", rev = "a51ae015c150ef68f62d9295e3efa9534f492a4a", features = ["streaming"] }
nautilus-common = { git = "https://github.com/nautechsystems/nautilus_trader", rev = "a51ae015c150ef68f62d9295e3efa9534f492a4a" }
nautilus-system = { git = "https://github.com/nautechsystems/nautilus_trader", rev = "a51ae015c150ef68f62d9295e3efa9534f492a4a" }
nautilus-persistence = { git = "https://github.com/nautechsystems/nautilus_trader", rev = "a51ae015c150ef68f62d9295e3efa9534f492a4a" }
nautilus-trading = { git = "https://github.com/nautechsystems/nautilus_trader", rev = "a51ae015c150ef68f62d9295e3efa9534f492a4a" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`streaming` enables catalogs on the node (`LiveNodeConfig::catalogs`) and
recording live data (`with_streaming_config`).

## node.rs

### Build

```rust
LiveNode::builder(trader_id: TraderId, environment: Environment) -> anyhow::Result<LiveNodeBuilder>
LiveNodeBuilder::from_config(config: LiveNodeConfig) -> anyhow::Result<LiveNodeBuilder>
LiveNodeBuilder::build(self) -> anyhow::Result<LiveNode>
```

```rust
let mut node = LiveNode::builder(TraderId::from("HELIX-001"), Environment::Live)?
    .with_name("HELIX-PAPER")
    .with_exec_engine_config(exec_engine)      // first: it replaces the whole exec config
    .with_reconciliation(true)
    .with_delay_post_stop_secs(5)
    .with_logging(logger)
    .add_data_client_with_routing(None, data_factory, data_config, routing.clone())?
    .add_exec_client_with_routing(None, exec_factory, exec_config, routing)?
    .build()?;
node.add_strategy(strategy)?;
node.run().await?;
```

- **Environment.** `Live`, also for a paper account: the broker client
  decides paper or live, not the node. `Backtest` is rejected; `Sandbox`
  exists for simulated exec clients and we do not use it.
- **TraderId.** `"NAME-TAG"`, one hyphen, both parts non-empty.
  `TraderId::from` panics on bad input, `new_checked` returns the error.

### Clients and routing

```rust
fn add_data_client_with_routing(self, name: Option<String>,
    factory: Box<dyn DataClientFactory>, config: Box<dyn ClientConfig>,
    routing: RoutingConfig) -> anyhow::Result<Self>
fn add_exec_client_with_routing(/* same shape, Box<dyn ExecutionClientFactory> */)
```

- `name: None` uses the factory's name, which becomes the `ClientId`. A
  repeated name is an error.
- `RoutingConfig { default: bool, venues: Option<Vec<String>> }` (`bon`
  builder; `default()` is `default: false`). An explicit venue route wins;
  the only client for a venue routes it; several clients for one venue with
  no route or default make `build()` fail.

### Strategy

```rust
fn add_strategy<T>(&mut self, strategy: T) -> anyhow::Result<()>
where T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static
```

- `add_strategy` has the same bound as the backtest engine's `add_strategy`
  (reached through `BacktestNode::get_engine_mut`), so the same strategy
  type goes to both.
- Add it before `run()`: it fails unless the node is `Idle`. A
  duplicate strategy id is an error.

### Reconciliation and timeouts

| Builder method | Default |
| --- | --- |
| `with_reconciliation(bool)` | `true` |
| `with_reconciliation_lookback_mins(u32)` | no limit |
| `with_timeout_connection(u64)` (s) | 60 |
| `with_timeout_reconciliation(u64)` (s) | 30 |
| `with_delay_post_stop_secs(u64)` | 10 |
| `with_delay_shutdown_secs(u64)` | 5 |

- `run()` connects the data clients, loads their instruments, connects the
  exec clients, reconciles, then starts the trader. A reconciliation failure
  aborts startup.
- `with_exec_engine_config(LiveExecutionEngineConfig)` replaces the whole
  exec config, including an earlier `with_reconciliation`: call it first.
  Continuous open-order and position checks are off unless
  `open_check_interval_secs` / `position_check_interval_secs` are set.

### External orders

Orders found at the venue that no strategy sent belong to `EXTERNAL`. The
strategy claims them per instrument through its config:

```rust
StrategyConfig { external_order_instrument_ids: Some(vec![instrument_id]), ..Default::default() }
```

With the whole-book rule the strategy claims every instrument it trades, so
reconciled orders and positions land on it. Two claims on one instrument
are an error.

### Run and shutdown

```rust
async fn run(&mut self) -> anyhow::Result<()>        // NodeRunMode::Owned
fn handle(&self) -> LiveNodeHandle                   // Clone, thread-safe
LiveNodeHandle::stop(&self)
```

- `run()` installs SIGINT and SIGTERM handlers (`Owned` mode); the process
  is the node. `NodeRunMode::Hosted` leaves signals to the host.
- The loop runs on the calling thread: call `run()` from `#[tokio::main]`,
  do not spawn it.
- `handle().stop()` works from any thread or task.
- On stop, strategies stop (and cancel orders in `on_stop`), events keep
  flowing for `delay_post_stop`, then clients disconnect. `run()` returns an
  error if order submissions are still unresolved then: set
  `delay_post_stop` above the time the strategy needs to flatten.
- `run()` can be called once.

### Catalog for warmup

Warmup is described in the strategy README. With a catalog registered, the
data engine serves a request from the catalog and asks the adapter only for
the missing intervals; without one, the adapter answers alone.

The builder has no catalog method: catalogs are a `LiveNodeConfig` field, so
build through `from_config`:

```rust
let config = LiveNodeConfig {
    environment: Environment::Live,
    trader_id,
    catalogs: vec![DataCatalogConfig::new("data/catalog".to_string(), None, None)],
    cache: Some(warmup::cache_config(&bar_types, warmup)?), // holds the warmup bars
    data_engine: warmup::live_data_engine_config(&bar_types), // see the strategy README
    ..Default::default()
};
let builder = LiveNodeBuilder::from_config(config)?;   // then the same with_* chain
```

Point it at the catalog `fill_catalog` wrote. Catalog names must be unique.
The directory must exist: the start creates it, empty for a run on OKX
alone.

### Recording live data

`with_streaming_config(StreamingConfig)` writes live data and events to
Feather files under `writer_path`; with a catalog in the config it promotes
them into the catalog. `StreamingConfig::new(writer_path, catalog,
flush_interval_ms, replace_existing, rotation_config)`; rotation by size,
interval or scheduled dates.

### Logging

`with_logging(LoggerConfig)`. Defaults: `stdout_level: Info`,
`fileout_level: Off`, `use_tracing: false`.

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

`ib/`: the IB clients on the Docker gateway, the account and its
tests: see [`ib/README.md`](ib/README.md).

## OKX

`okx/`: the OKX clients, the EEA environment, the account checks
and their tests: see [`okx/README.md`](okx/README.md).

## Rules

- **Everything before `run()`**: clients, the strategy, claims.
- **Reconcile on** for any node that trades, and claim the strategy's
  instruments.
- **Owned run mode**: Nautilus handles signals; code stops through
  `handle().stop()`.
- **One broker per folder**; `node.rs` stays broker-agnostic.
- **Clients by venue**: OKX instruments to OKX's clients, the others to
  IB's; mixed venues get both, routed by Nautilus.
- **No secrets in code or git**: credentials come from the environment.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file, for
  what needs no node and no broker.
- Integration tests in `tests/`, one node per file: a node with no broker
  client or a test data client, the strategy added, stopped through
  `handle().stop()` from a spawned task, asserting on the result of `run()`.
  Use `#[tokio::test(flavor = "current_thread")]`.

## Unverified

- How `limit` behaves when a request is served by a catalog plus the adapter.
- Whether warmup can read a catalog while streaming promotes into it.
- Whether IB reports ES's exchange as `CME` (the venue comes from that
  field), and which history depths IB serves (see `data/ib/README.md`).
