# Interactive Brokers live

`live/ib/mod.rs` builds the IB data and execution clients of the live node,
from `nautilus_interactive_brokers::{config, factories}`, on the Docker
gateway of the account's mode. The node, the start sequence and the routing
between venues are in `live/README.md`, with how the warmup and the live
bars come from IB's 1-minute bars ("Warmup and live bars on IB"); the IB
history the catalog is filled from is in `data/ib/README.md`.

```rust
InteractiveBrokersDataClientFactory::new()       // name() == "IB"
InteractiveBrokersExecutionClientFactory::new()  // name() == "IB"
```

Both configs implement `ClientConfig` and `Default`; build them with
`..Default::default()`. Both clients log through `tracing` (as does `ibapi`),
so the logger needs `use_tracing: true` to show them.

## The account

The account's mode picks the gateway: a paper account (`D...`) gets the paper
container on port 4002, any other the live one on 4001 (the adapter's ports
by `TradingMode`). The node stays `Environment::Live` either way. The account
comes from `--account`, else `NAUTILUS_IB_ACCOUNT_ID`, as the adapter's
`node_exec_tester` reads it.

The execution factory turns `account_id` into an `AccountId`: with a `-`
(`IB-DU1234567`) it is used as is; without (`DU1234567`) it is prefixed with
the client name, giving `IB-DU1234567`; `None` gives `IB-001`, not a real
account, so always set it. Orders and account requests carry the IB account
code (`ib_account_code` in the adapter's `execution/account.rs`): a bare
configured code as is, otherwise the part of the `AccountId` after its last
`-`, since IB codes hold no hyphen. A client name with a hyphen is therefore
safe: name `IB-PAPER` plus `DU1234567` gives `AccountId` `IB-PAPER-DU1234567`
and sends `DU1234567`.

## Gateway

`live` and `fill-catalog` start the gateway themselves, with Nautilus'
`DockerizedIBGateway` (`data/ib/gateway.rs`), as Nautilus documents it:
started before the clients, which take its `host` and `port`. The Rust client
configs have no `dockerized_gateway` field; the Python bindings accept one and
reject it as not wired. There is no `--host`
or `--port`: an IB Gateway or TWS run by hand is not used.

- **One container per mode**: `nautilus-ib-gateway-paper` (port 4002) or
  `nautilus-ib-gateway-live` (4001), image `ghcr.io/gnzsnz/ib-gateway` (IBC
  logs in). `safe_start` reuses a running container and replaces a stopped or
  created one. The container stays up after the command, with restart policy
  `always`, and the next command reuses it. The adapter creates the container
  without pulling the image: pull it once beforehand
  (`docker pull ghcr.io/gnzsnz/ib-gateway:stable`).
- **Login wait.** `start` waits for the login of a container it creates but
  returns at once for one still starting, so `IbGateway` polls
  `container_status()` every second until `Ready`, within the adapter's start
  timeout (300 s). Since 0.66.0 `Ready` comes from the log since the
  container's last start, so a restarted container is not taken as logged in
  from an earlier session's log.
- **Credentials**: `TWS_USERNAME` and `TWS_PASSWORD`, read by the adapter from
  the environment (or `.env`); they never enter our configs or logs.
  `DOCKER_HOST` points the adapter's Docker client (bollard) at a socket other
  than `/var/run/docker.sock`, such as the user's podman socket.
- **History always on paper**: `fill-catalog` uses the paper container, so
  it never needs a live login. A paper `live` uses the same container; a
  live node's warmup comes from its own data client, on the live gateway.
- **`read_only_api: false`** on every container, overriding the adapter's
  default `true`, because the paper container `fill-catalog` starts also
  serves a paper `live`, whose orders a read-only API would block (see
  "Read-only API" below).
- **Live account**: the login needs the 2FA approved in IBKR Mobile within the
  start timeout.
- **Other sessions**: the container logs in with
  `EXISTING_SESSION_DETECTED_ACTION=primary`, ending any other session of the
  same IB user (an IB Gateway or TWS run by hand), and it takes the mode's
  port, so a gateway run by hand on 4002 or 4001 must be closed.
- **Concurrent starts**: two commands starting the same container at the same
  moment can race in the adapter (one sees the other's just-created container
  and replaces it); start the second once the first is logged in.
- **Gateway restart**: the node does not run a full reconciliation again after
  the container restarts; only the periodic open-order and position checks
  run.

### Read-only API

A gateway with the API in read-only mode (IB Gateway: Configure, Settings,
API, Settings, "Read-Only API") blocks more than orders: the execution
client's first request (the next valid order id) fails with IB error 321 "The
API interface is currently in Read-Only mode", the client never connects, and
the node aborts its startup after the connection timeout ("Engine readiness
timed out"). Seen on a paper gateway run by hand; the containers we start
have it off.

## Clients

Data and execution clients with the same `(host, port, client_id)` share one
connection (the adapter's shared-client registry; the first creator's
`max_reconnect_attempts` wins), which is how `mod.rs` builds them; a second
process needs its own `client_id`. Both are registered as the node's default client
(`RoutingConfig::builder().default(true).build()`), since IB spans many
venues.

### Data client config

`InteractiveBrokersDataClientConfig` is the history's config
(`data::data_client_config`), so the 1-minute bars the node requests and
subscribes to are those `fill-catalog` stores, with `batch_quotes: false`:
quotes a strategy subscribes to come tick by tick (`reqTickByTickData`
BID_ASK). The adapter falls back to `reqMktData` where IB refuses, and
always uses it for BAG spreads. Adapter defaults:

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
| `batch_quotes` | `true` (`reqMktData`) |

`use_regular_trading_hours` must equal the `use_rth` of the history requests,
or backtest and live bars differ. History uses the full session, so the live
data client sets `false` too, overriding the default: with regular hours,
IB's first bar of a futures day is partial (13:30-14:00 UTC for ES) and the
adapter stamps it at open + bar size, half an hour after its real close (see
"Full session, not regular hours" in `data/ib/README.md`).

### Execution client config

`InteractiveBrokersExecutionClientConfig`: `host`, `port`, `client_id` as
above, `account_id: Option<String>` (see "The account"), timeouts,
`instrument_provider`. The factory fixes `OmsType::Netting` and
`AccountType::Margin`; a strategy leaves its `oms_type` unset and nets.
Nautilus' `bracket()` makes its exits reduce-only and the IB execution
client denies reduce-only orders (`UNSUPPORTED_REDUCE_ONLY`), so a bracket
is denied on IB (`adapters/interactive_brokers/src/execution/core.rs`): the
EMA crossover, which trades in brackets, cannot trade live on IB. Its
backtests on IB data are unaffected.
The exec `client_id` must not be a multiple of 1000: the adapter partitions
order ids by `client_id % 1000` and rejects one. Since data and exec share the
client id, `mod.rs` refuses it before any gateway starts.

### Instrument provider

`InteractiveBrokersInstrumentProviderConfig` sits inside each client config;
give both the same one (`mod.rs` sets `load_ids` to the strategy's
instruments).

| Field | Default |
| --- | --- |
| `symbology_method` | `Simplified` (or `Raw`) |
| `load_ids: HashSet<InstrumentId>` | empty |
| `load_contracts: Vec<ConfiguredContract>` | empty |
| `build_futures_chain`, `min_expiry_days`, `max_expiry_days` | `None` |
| `cache_path`, `cache_validity_days` | `None` (no cache) |

Startup loads `load_ids`, then `load_contracts`; one that does not resolve
fails startup. A `ConfiguredContract` is an IB `Contract` plus per-contract
chain overrides; in serialized config it is IB contract JSON:

```rust
serde_json::json!({
    "secType": "FUT", "symbol": "ES", "exchange": "CME", "currency": "USD",
    "build_futures_chain": true, "min_expiry_days": 0, "max_expiry_days": 120,
})
```

Other contract keys: `localSymbol`, `tradingClass`,
`lastTradeDateOrContractMonth`, `conId`, `multiplier`, `includeExpired`,
`primaryExchange`, `right`, `strike`, `secId`, `issuerId`, `description`,
`comboLegsDescrip`; other overrides: `build_options_chain`,
`options_chain_exchange`.

With `Simplified`, an instrument id is the IB local symbol with the
contract's exchange as venue: a future `ESM4.CME` (the provider builds the
1-digit year of the local symbol; an id given to it may write the year in 1,
2 or 4 digits), forex `EUR/USD.IDEALPRO`. A stock resolved by the provider
takes its primary exchange (`AAPL.NASDAQ`), `SMART` only when it has none.
`Raw` keeps the security type (`AAPL=STK.NASDAQ`). Provider, bar types and
catalog must use the same symbology and venue; the venue rules (never
`convert_exchange_to_mic_venue`) are in `data/ib/README.md`.

## IB tests

- Unit tests for the pure construction in `mod.rs` (configs on a gateway, the
  client id refused before any gateway starts) and in `data/ib/gateway.rs`
  (account forms and mode, the container config, the login wait on fake
  statuses).
- End to end, `tests/live/ib_gateway.rs`, against the Docker paper container:
  history starting or reusing it, `fill_catalog` of the 1-minute TRADES
  bars through it, and `Live` on a paper account, stopped by SIGINT,
  with an EMA crossover never warm enough to trade.
- End to end, `tests/live/ib_minute_bars.rs`: a probe on `Live` warming up a
  composite bar type from IB's `request_bars`, then getting the live minute
  bars from `subscribe_bars`: no gap and no duplicate at the hand-off
  (`live/README.md`, "Warmup and live bars on IB"), and how long after its
  close each live minute arrives. No order; SXR8 must be quoted.
- End to end, `tests/live/ib_restart.rs`, placing orders on the paper account:
  the node's cache in Redis across restarts, each node in a child process,
  1 or 2 EXS1 at a time, then IB read back flat (live README, "Cache in
  Redis", for what it shows). Xetra must be open.

The end-to-end tests are gated as the IB crate's `node_data_tester` and
`node_exec_tester` examples are: each returns at once, saying it was skipped,
unless `NAUTILUS_IB_RUN=1` is set in the shell (not in `.env`). Those placing
orders, `ib_restart.rs`, also need `NAUTILUS_IB_LIVE_ORDERS=1`. The account
comes from `NAUTILUS_IB_ACCOUNT_ID` and must be a paper one.

```sh
NAUTILUS_IB_RUN=1 cargo test --test live ib_gateway::
NAUTILUS_IB_RUN=1 cargo test --test live ib_minute_bars:: -- --nocapture
NAUTILUS_IB_RUN=1 NAUTILUS_IB_LIVE_ORDERS=1 cargo test --test live ib_restart:: -- --test-threads=1
```
