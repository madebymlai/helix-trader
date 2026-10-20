# Running live

The `live` module builds and runs a Nautilus `LiveNode` for the strategy, on
a paper then a real account (build step 3). It adds no trading logic: it
fills the catalog up to now, then wires broker clients, the strategy and the
node. Filling the catalog needs no node (see the data README).

APIs are those of the `nautilus-*` crates at `0.66.0`; open points are under
"Unverified". Process-wide rules (one node per process, Nautilus owns the
logger) are in `docs/ARCHITECTURE.md`.

## The command

```sh
helix-trader live ema --config ema.toml [--account DU1234567] \
  [--catalog catalog] [--client-id 1]
```

Runs one strategy until Ctrl-C (SIGINT) or SIGTERM. `--account` is the bare
IB account id, else `NAUTILUS_IB_ACCOUNT_ID`; its mode (paper for `D...`)
picks the gateway (see "Gateway"). A client id that is a multiple of 1000
is refused before any gateway starts. The default client id is the
adapter's, 1, so `fill-catalog` (10) can run at the same time.
`NAUTILUS_LOG` configures the logger as for the other commands. The
command reads `.env` at start (see `.env.example`); a variable already set
wins.

No `fill-catalog` run is needed first, nor a gateway: the command starts
it. In code: `live::Live { catalog, account, client_id, logging
}.run(&requirements, strategy)`, generic over the strategy like
`Backtest::run`.

### Start sequence

1. Install Nautilus' logger, with the node's trader id and `LoggerConfig`,
   and the `tracing` bridge: the gateway and the fill log before the node
   exists, and the node reuses that logger when built.
2. Check the client id, start or reuse the Docker gateway of the account's
   mode, wait for its login, and build the IB client configs on it.
3. Fill the catalog for the strategy's data requirements, with the same
   `IbHistory` and `fill_catalog` as `fill-catalog`: instruments, then each
   instrument's quotes over `[now - warmup, now - 1 minute]`, resuming after
   what the catalog holds, through the paper gateway (started or reused). IB history refuses an end within a minute of now
   (the minute still in progress). The history connection closes here, so
   the node can connect with the same client id.
4. Build the node with the catalog in `LiveNodeConfig::catalogs`, IB data
   and exec clients routed by default, reconciliation on, open-order and
   position checks every 10 and 30 s (as the adapter's `node_exec_tester`),
   and a 5 s post-stop delay.
5. Add the strategy and claim the external orders of its instruments (the
   ones its config does not claim already), then `run()`.

The strategy's warmup request (`strategy::warmup`) is then served by the
catalog; the data engine sends IB only the interval the catalog misses,
from the fill's end to now, as historical BID_ASK ticks. Only the catalog
part reaches the internal bars: the IB ticks are dropped (see "Known
limits").

### Known limits

Both are Nautilus bugs, present in `0.65.0` and `0.66.0`; there is no
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
  mod.rs    `Live`: the start sequence (catalog filling, logger), the entry point
  node.rs   builds and runs the LiveNode, broker-agnostic
  ib.rs     Interactive Brokers client factories and configs, on the
            gateway `data`'s IB part starts
```

Another broker is another file next to `ib.rs` returning the same shape: a
data factory and config, an optional exec factory and config. `node.rs` takes
them as `Box<dyn DataClientFactory>` and friends and never names a broker.

## Cargo

```toml
nautilus-live = { version = "=0.66.0", features = ["streaming"] }
nautilus-common = "=0.66.0"
nautilus-system = "=0.66.0"
nautilus-persistence = "=0.66.0"
nautilus-trading = "=0.66.0"
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
`nautilus-interactive-brokers = "=0.66.0"`, features `execution` and
`gateway` (the Docker-managed gateway, through `bollard`).

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
| `batch_quotes` | `true` (`reqMktData`); set `false` (see "IB quote subscription") |

The history side (`data/ib/`) uses the same config type for its
historical client. `use_regular_trading_hours` must equal the `use_rth` of
the history requests, or backtest and live bars differ. History uses the
full session (`use_regular_trading_hours: false`), so the live data client
must set `false` too, overriding the default: with regular hours, IB's
first bar of a futures day is partial (13:30-14:00 UTC for ES) and the
adapter stamps it at open + bar size, half an hour after its real close
(see "Full session, not regular hours" in the data README).

### Execution client config

`InteractiveBrokersExecutionClientConfig`: `host`, `port`, `client_id` as
above, `account_id: Option<String>`, timeouts, `instrument_provider`.

- The factory fixes `OmsType::Netting` and `AccountType::Margin`; leave the
  strategy's `oms_type` unset or `Netting`.
- The exec `client_id` must not be a multiple of 1000.
- Data and exec with the same `(host, port, client_id)` share one
  connection. A second process needs its own `client_id`.

### Paper or live: the account

The account's mode picks the gateway: a paper account (`D...`) the paper
container, on port 4002, any other the live one, on 4001 (the adapter's
ports by `TradingMode`). The node stays `Environment::Live` either way. The
account comes from `--account`, else `NAUTILUS_IB_ACCOUNT_ID`, as the
adapter's `node_exec_tester` reads it.

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

`live` and `fill-catalog` start the gateway themselves, with Nautilus'
`DockerizedIBGateway` (`data/ib/gateway.rs`), as Nautilus documents it:
started before the clients, which take its `host` and `port`. The data
client config's `dockerized_gateway` is reserved and rejected. No `--host`
or `--port`: an IB Gateway or TWS run by hand is not used.

- **One container per mode**, `nautilus-ib-gateway-paper` (port 4002) or
  `nautilus-ib-gateway-live` (4001), image `ghcr.io/gnzsnz/ib-gateway`
  (IBC logs in). `safe_start` reuses a running container and replaces a
  stopped or created one. The container stays up after the command, with
  restart policy `always`; the next command reuses it.
- **Login wait.** `start` waits for the login of a container it creates,
  but returns at once for one still starting, so `IbGateway` polls
  `container_status()` every second until `Ready`, within the adapter's
  start timeout (300 s). Since 0.66.0 `Ready` comes from the log since the
  container's last start, so a restarted container is not taken as logged
  in from an earlier session's log.
- **Credentials**: `TWS_USERNAME` and `TWS_PASSWORD`, read by the adapter
  from the environment (or `.env`); they never enter our configs or logs.
  `DOCKER_HOST` points the adapter's Docker client (bollard) at a socket
  other than `/var/run/docker.sock`, such as the user's podman socket.
- **History always on paper**: the catalog fill uses the paper container
  even for a live account, so it never needs a live login. A paper `live`
  thus uses one container for both.
- **`read_only_api: false`** on every container: the paper container
  `fill-catalog` starts also serves a paper `live`, whose orders a
  read-only API would block (below).
- **Live account**: the login needs the 2FA approved in IBKR Mobile within
  the start timeout.
- **Other sessions**: the container logs in with
  `EXISTING_SESSION_DETECTED_ACTION=primary`, ending any other session of
  the same IB user (an IB Gateway or TWS run by hand); and it takes the
  mode's port, so a gateway run by hand on 4002 or 4001 must be closed.
- **Two commands starting the same container at the same moment** can race
  in the adapter (one sees the other's container just created and replaces
  it): start the second once the first is logged in.
- **Gateway restart**: the node does not run a full reconciliation again
  after the container restarts; only the periodic open-order and position
  checks run.

A gateway with the API in read-only mode (IB Gateway: Configure, Settings,
API, Settings, "Read-Only API") blocks more than orders: the execution
client's first request (the next valid order id) fails with IB error 321
"The API interface is currently in Read-Only mode", the client never
connects, and the node aborts its startup after the connection timeout
("Engine readiness timed out"). Seen on a paper gateway run by hand; the
containers we start have it off.

### IB rules

- **Mode by account.** The account's mode picks the container, hence the
  port.
- **Always set `account_id`.**
- **Default routing** for both clients.
- **Same symbology** in provider, bar types and catalog.
- **Use `use_tracing: true`**: IB and `ibapi` log through `tracing`.

### IB tests

- Unit tests for the pure construction in `ib.rs` (configs on a gateway,
  the client id refused before any gateway starts) and in
  `data/ib/gateway.rs` (account forms and mode, the container config, the
  login wait on fake statuses).
- End to end, `tests/ib_gateway.rs`, against the Docker paper container:
  history starting or reusing it, `fill_catalog` through it, and `Live`
  on a paper account, stopped by SIGINT, with a session that holds no bar
  so the strategy never trades. Gated as the IB crate's `node_data_tester`
  and `node_exec_tester` examples are: each test returns at once, saying it
  was skipped, unless `NAUTILUS_IB_RUN=1` is set in the shell (not in
  `.env`); anything placing orders would also need
  `NAUTILUS_IB_LIVE_ORDERS=1` (none does). The account comes from
  `NAUTILUS_IB_ACCOUNT_ID` and must be a paper one. Run:

  ```sh
  NAUTILUS_IB_RUN=1 cargo test --test ib_gateway
  ```

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
