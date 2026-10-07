# Running live

The `live` module builds and runs a Nautilus `LiveNode` for the strategy, on
a paper then a real account (build step 3). It adds no trading logic: it
wires broker clients, the strategy and the node. Filling the catalog needs
no node (see the data README).

APIs are those of the `nautilus-*` crates at `0.65.0`; open points are under
"Unverified". Process-wide rules (one node per process, Nautilus owns the
logger) are in `docs/ARCHITECTURE.md`.

## Layout

```
live/
  mod.rs    declares the submodules, exposes the entry points
  node.rs   builds and runs the LiveNode, broker-agnostic
  ib.rs     Interactive Brokers client factories and configs
```

Another broker is another file next to `ib.rs` returning the same shape: a
data factory and config, an optional exec factory and config. `node.rs` takes
them as `Box<dyn DataClientFactory>` and friends and never names a broker.

## Cargo

```toml
nautilus-live = { version = "=0.65.0", features = ["streaming"] }
nautilus-common = "=0.65.0"
nautilus-system = "=0.65.0"
nautilus-persistence = "=0.65.0"
nautilus-trading = "=0.65.0"
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
    ..Default::default()
};
let builder = LiveNodeBuilder::from_config(config)?;   // then the same with_* chain
```

Point it at the catalog `fill_catalog` wrote. Catalog names must be unique.

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

## Interactive Brokers specifics (live)

`nautilus_interactive_brokers::{config, factories}`; Cargo:
`nautilus-interactive-brokers = "=0.65.0"`. The default `execution` feature
is enough; `gateway` adds a Docker-managed gateway.

```rust
InteractiveBrokersDataClientFactory::new()       // name() == "IB"
InteractiveBrokersExecutionClientFactory::new()  // name() == "IB"
```

Both configs implement `ClientConfig` and `Default`; build them with
`..Default::default()`.

### Data client config

`InteractiveBrokersDataClientConfig`:

| Field | Default |
| --- | --- |
| `host` | `"127.0.0.1"` |
| `port` | `4002` |
| `client_id` | `1` |
| `use_regular_trading_hours` | `true` |
| `market_data_type` | `Realtime` |
| `request_timeout` (s) | `60` |
| `connection_timeout` (s) | `300` |
| `instrument_provider` | default provider config |

The history side (`data/ib.rs`) uses the same config type for its
historical client. `use_regular_trading_hours` must equal the `use_rth` of
the history requests, or backtest and live bars differ.

### Execution client config

`InteractiveBrokersExecutionClientConfig`: `host`, `port`, `client_id` as
above, `account_id: Option<String>`, timeouts, `instrument_provider`.

- The factory fixes `OmsType::Netting` and `AccountType::Margin`; leave the
  strategy's `oms_type` unset or `Netting`.
- The exec `client_id` must not be a multiple of 1000.
- Data and exec with the same `(host, port, client_id)` share one
  connection. A second process needs its own `client_id`.

### Paper or live: the port

Only the port switches the mode. The adapter default is the paper gateway.

| Application | Paper | Live |
| --- | --- | --- |
| IB Gateway | 4002 (default) | 4001 |
| TWS | 7497 | 7496 |

The node stays `Environment::Live` either way. Port and `account_id` come
from the command line or the environment; reject a paper port with a live
account (or the reverse) at startup.

### Account id and the client name

The factory turns `account_id` into an `AccountId`:

- with a `-` (`IB-DU1234567`): used as is;
- without (`DU1234567`): prefixed with the client name, `IB-DU1234567`;
- `None`: `IB-001`, not a real account. Always set it.

Orders carry the part after the first `-`. A client name with a hyphen
corrupts it: name `IB-PAPER` plus `DU1234567` sends account `PAPER-DU1234567`.
So use the default name with a bare account, or any name with
`IB-DU1234567`.

### Routing

IB spans many venues, so register it as the default client for both data
and exec: `RoutingConfig::builder().default(true).build()`.

### Instrument provider

`InteractiveBrokersInstrumentProviderConfig`, inside each client config;
give both the same one.

| Field | Default |
| --- | --- |
| `symbology_method` | `Simplified` (or `Raw`) |
| `load_ids: HashSet<InstrumentId>` | empty |
| `load_contracts: Vec<serde_json::Value>` | empty |
| `build_futures_chain`, `min_expiry_days`, `max_expiry_days` | `None` |
| `cache_path`, `cache_validity_days` | `None` (no cache) |

Startup loads `load_ids`, then `load_contracts`; one that does not resolve
fails startup. `load_contracts` entries are IB contract JSON:

```rust
serde_json::json!({
    "secType": "FUT", "symbol": "ES", "exchange": "CME", "currency": "USD",
    "build_futures_chain": true, "min_expiry_days": 0, "max_expiry_days": 120,
})
```

Other keys: `localSymbol`, `tradingClass`, `lastTradeDateOrContractMonth`,
`conId`, `multiplier`, `includeExpired`, `primaryExchange`.

Instrument ids with `Simplified`: the IB local symbol and the contract's
exchange as venue. A future is `ESM4.CME` (the year can be 1, 2 or 4
digits); a stock `AAPL.SMART`; forex `EUR/USD.IDEALPRO`. `Raw` keeps the
security type (`AAPL=STK.SMART`). Config, bar types and catalog must use the
same symbology and venue; the venue rules (never
`convert_exchange_to_mic_venue`) are in the data README.

### Gateway

Run IB Gateway or TWS with the socket API enabled, or enable the `gateway`
feature and use `DockerizedIBGateway` (needs Docker and the credentials in
the environment). Its `read_only_api` defaults to `true`; whether that
blocks orders is unverified, so set it to `false` for trading and check on
paper.

### IB rules

- **Mode by port.** Paper and live differ only in `port` and `account_id`.
- **Always set `account_id`.**
- **Default routing** for both clients.
- **Same symbology** in provider, bar types and catalog.
- **Use `use_tracing: true`**: IB and `ibapi` log through `tracing`.

### IB tests

- Unit tests for the pure construction in `ib.rs`: configs from CLI values,
  port to mode, account id forms, rejecting a paper port with a live account.
- Against a real gateway, by hand, not in CI: run against the paper port
  with a paper account, watch reconciliation at startup and a clean stop on
  Ctrl-C. The IB crate's `node_data_tester` and `node_exec_tester` examples
  (`--features examples`) are the reference programs.

## Rules

- **Everything before `run()`**: clients, the strategy, claims.
- **Reconcile on** for any node that trades, and claim the strategy's
  instruments.
- **Owned run mode**: Nautilus handles signals; code stops through
  `handle().stop()`.
- **One broker per file**; `node.rs` stays broker-agnostic.
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
  field), and which history depths IB serves (see the data README).
