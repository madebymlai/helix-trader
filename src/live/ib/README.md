# Interactive Brokers live

`nautilus_interactive_brokers::{config, factories}`; Cargo:
`nautilus-interactive-brokers`, pinned as the others, features `execution`
and `gateway` (the Docker-managed gateway, through `bollard`).

```rust
InteractiveBrokersDataClientFactory::new()       // name() == "IB"
InteractiveBrokersExecutionClientFactory::new()  // name() == "IB"
```

Both configs implement `ClientConfig` and `Default`; build them with
`..Default::default()`.

## Data client config

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
| `batch_quotes` | `true` (`reqMktData`); set `false` (see "IB quote subscription" in `live/README.md`) |

The history side (`data/ib/`) uses the same config type for its
historical client. `use_regular_trading_hours` must equal the `use_rth` of
the history requests, or backtest and live bars differ. History uses the
full session (`use_regular_trading_hours: false`), so the live data client
must set `false` too, overriding the default: with regular hours, IB's
first bar of a futures day is partial (13:30-14:00 UTC for ES) and the
adapter stamps it at open + bar size, half an hour after its real close
(see "Full session, not regular hours" in `data/ib/README.md`).

## Execution client config

`InteractiveBrokersExecutionClientConfig`: `host`, `port`, `client_id` as
above, `account_id: Option<String>`, timeouts, `instrument_provider`.

- The factory fixes `OmsType::Netting` and `AccountType::Margin`; leave the
  strategy's `oms_type` unset or `Netting`.
- The exec `client_id` must not be a multiple of 1000.
- Data and exec with the same `(host, port, client_id)` share one
  connection. A second process needs its own `client_id`.

## Paper or live: the account

The account's mode picks the gateway: a paper account (`D...`) the paper
container, on port 4002, any other the live one, on 4001 (the adapter's
ports by `TradingMode`). The node stays `Environment::Live` either way. The
account comes from `--account`, else `NAUTILUS_IB_ACCOUNT_ID`, as the
adapter's `node_exec_tester` reads it.

## Account id and the client name

The factory turns `account_id` into an `AccountId`:

- with a `-` (`IB-DU1234567`): used as is;
- without (`DU1234567`): prefixed with the client name, `IB-DU1234567`;
- `None`: `IB-001`, not a real account. Always set it.

Orders carry the part after the first `-`. A client name with a hyphen
corrupts it: name `IB-PAPER` plus `DU1234567` sends account `PAPER-DU1234567`.
So use the default name with a bare account, or any name with
`IB-DU1234567`.

## Routing

IB spans many venues, so register it as the default client for both data
and exec: `RoutingConfig::builder().default(true).build()`.

## Instrument provider

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
`convert_exchange_to_mic_venue`) are in `data/ib/README.md`.

## Gateway

`live` and `fill-catalog` start the gateway themselves, with Nautilus'
`DockerizedIBGateway` (`data/ib/gateway.rs`), as Nautilus documents it:
started before the clients, which take its `host` and `port`. The data
client config's `dockerized_gateway` is reserved and rejected. No `--host`
or `--port`: an IB Gateway or TWS run by hand is not used.

- **One container per mode**, `nautilus-ib-gateway-paper` (port 4002) or
  `nautilus-ib-gateway-live` (4001), image `ghcr.io/gnzsnz/ib-gateway`
  (IBC logs in). `safe_start` reuses a running container and replaces a
  stopped or created one. The container stays up after the command, with
  restart policy `always`; the next command reuses it. The adapter creates
  the container without pulling the image: pull it once beforehand
  (`docker pull ghcr.io/gnzsnz/ib-gateway:stable`).
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

## IB rules

- **Mode by account.** The account's mode picks the container, hence the
  port.
- **Always set `account_id`.**
- **Default routing** for both clients.
- **Same symbology** in provider, bar types and catalog.
- **Use `use_tracing: true`**: IB and `ibapi` log through `tracing`.

## IB tests

- Unit tests for the pure construction in `mod.rs` (configs on a gateway,
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
