# Running live

The `live` module builds and runs a Nautilus `LiveNode` for the strategy, on
a paper then a real account (build step 3). It adds no trading logic: it
sets up the clients of the instruments' venues, then wires the clients, the
strategy and the node. The node registers the catalog fill-catalog fills
and backtests read (`--catalog`, default `catalog`): it serves the warmup
first, the venues' data clients what it lacks, and IC's answers are
written back to it. The node streams the market data it receives into it,
so that a retrain the node starts replays up to the present (see
"Recording into the catalog").
Clients go by venue: OKX instruments to OKX's clients, IC instruments
(`….ICMARKETS`) to IC's, all others to IB's, and a strategy trading several
venues gets their clients, routed by Nautilus: a venue's own client
before IB's, the default, so IC's data and orders go to IC's clients with
IB's in the node ("Clients and routing" under `node.rs`). IC's account is
hedging: an order opens a position of its own unless it names one (see
`ic/README.md`, "Hedging positions").

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
  recording.rs
            `Recording`: the streaming of the live data into the
            catalog, and the trader's kept instance id
  redis.rs  the Redis container behind the node's cache
  ib/       IB client factories and configs, on the gateway `data`'s IB
            part starts; README.md: IB live
  ic/       IC's data and execution clients, their factories and
            configs, on the cTrader session `data`'s IC part holds;
            README.md: IC live
  okx/      OKX client factories and configs, and the account checks;
            README.md: OKX live
```

Each broker gets its own folder next to `ib/`, returning the same shape: a
data factory and config, an exec factory and config, and
their routing (`node::Clients`). `node.rs` takes a list of them as `Box<dyn
DataClientFactory>` and friends and never names a broker.

## The command

```sh
helix-trader live ema --config ema.toml [--account DU1234567] \
  [--client-id 1] [--leverage 1] [--trader-id HELIX-001] [--catalog catalog] \
  [--staging staging] [--promotion-minutes 5] [--models models] \
  [--retrain-commission-rate 0.00003 --retrain-balance "10000000 USD"]
```

It runs one strategy until Ctrl-C (SIGINT) or SIGTERM, starting the
gateway itself, so it needs no prior `fill-catalog` run. In code:
`live::Live { trader_id, account, client_id, leverage, logging, catalog,
recording }.run(&requirements, strategy)`, generic over the strategy like
`Backtest::run`. It starts or reuses the Redis container that holds the
node's cache (see "Cache in Redis").


- `--account` is the bare IB account id, else `NAUTILUS_IB_ACCOUNT_ID`,
  needed only for IB instruments; its mode (paper for `D...`) picks the
  gateway (`ib/README.md`, "Gateway").
- `--leverage` (default 1) is the leverage the run expects, as in a
  backtest, and only a check: OKX applies the account's leverage and the
  start refuses an OKX instrument at another one (`okx/README.md`); IB and
  IC do not check it.
- `--trader-id` (default `HELIX-001`, checked by Nautilus: a name and a tag
  joined by a hyphen) is the node's trader, whose cache in Redis each run
  loads. Live processes running at the same time each need their own
  trader id, since the Redis keys are per trader.
- `--client-id` defaults to the adapter's 1, so `fill-catalog` (10) can run
  at the same time; a multiple of 1000 is refused before any gateway starts.
- `--staging` (default `staging`) is the directory the node stages the
  data it streams, outside the catalog (a staging directory inside it is
  refused); it also keeps each trader's instance id. `--promotion-minutes`
  (default 5, at least 1, at most as many as fit in u64 milliseconds) is
  how often the staged data is promoted into the catalog (see "Recording
  into the catalog").
- `--models` (default `models`) is the model store a strategy trading on a
  model reads, its newest model valid at each decision (strategy README,
  "Features and decisions", "Model handle"); the EMA reads none.
- `--retrain-commission` or `--retrain-commission-rate`, and
  `--retrain-balance`, are the costs of the dataset runs of the retrains
  a strategy trading on a model starts, as `train` takes them (see
  "Retraining"); such a strategy refuses to start without them, the EMA
  ignores them.
- `NAUTILUS_LOG` configures the logger as for the other commands.
- The command reads `.env` at start (see `.env.example`), and a variable
  already set wins. Credentials come only from the environment, never from
  code or git.

## Start sequence

1. Read the trader's instance id from the staging directory, or keep a new
   one there (see "Recording into the catalog"), then install Nautilus'
   logger, with the node's trader and instance ids and `LoggerConfig`,
   and the `tracing` bridge: the gateway logs before the node exists, and
   the node reuses that logger when built.
2. OKX instruments, if any: read `OKX_ENVIRONMENT` (`demo` or `live`),
   build the OKX client configs on it, check the credentials are set, and
   check the account (net position mode, each instrument's leverage equal
   to `--leverage`). OKX's data client serves the warmup (`okx/README.md`).
3. IC instruments, if any: read `IC_ENVIRONMENT` (`demo` or `live`) and the IC credential (all read
   before anything connects, step 2's OKX included), build IC's client
   configs on them, then check the account on a session of its own, closed
   before the node connects: hedging and `IC_ACCOUNT_ID`'s, so a wrong
   account fails the start with its reason. IC's data client serves the
   warmup from cTrader's tick history (`ic/README.md`, "Warmup").
4. IB instruments, if any: check the client id, start or reuse the Docker
   gateway of the account's mode, wait for its login, and build the IB
   client configs on it. IB's data client serves the warmup (below).
5. Start or reuse the Redis container (`redis.rs`), then build the node
   (`node.rs`) with its cache persisted there, the clients of each venue
   (IB's routed by default, OKX's and IC's routed by Nautilus as the only
   clients of their venue), reconciliation on, open-order and position checks every 10
   and 30 s (as the adapter's `node_exec_tester`), a 5 s post-stop
   delay, and the streaming of its data into the catalog; then promote
   what the last run of its instance left staged (`promote_staged`, see
   "Instance id").
6. Join the gaps the last runs streamed across in the series of the
   warmup, and record the run's start in the catalog (see "Recording
   into the catalog").
7. Add the strategy, claim the external orders of its instruments that its
   config does not claim already (`register_external_order_claims`), then
   `run()`.

### Warmup and live bars on IB

A strategy on IB trades composite bars of IB's 1-minute bars, such as
`SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` (`strategy/README.md`,
"Bar types"). Its warmup request (`strategy::warmup`) asks for
`SXR8.IBIS-1-MINUTE-LAST-EXTERNAL`, IB's TRADES bars, over
`[now - warmup, now]`. The data engine answers it from the catalog's
minute bars (fill-catalog's) and passes the rest of the window to IB's
data client (`request_bars`,
`adapters/interactive_brokers/src/data/core.rs`), which splits it into IB
durations (`calculate_duration_segments`) and answers with IB's bars up
to now, stamped at their close; the engine merges the two into one
response. IB's answer is not written back (the client replies with the
payload alone, see `data/README.md`, "Nautilus persists only certified
responses"). The subscription to the bars then goes to IB's
`subscribe_bars`, which for any bar size but 5 seconds runs a
keepUpToDate historical request (`streams.rs`,
`handle_historical_bars_subscription`): it starts at the subscription's
`start_ns` param, which the warmup sets to its `now`, and publishes each
minute once, when IB's update after its close arrives, or 2 s after the
close at the latest (`HistoricalBarCompletion`). Without that param the
data engine, having a catalog, would set it to 1 ns after the catalog's
last minute bar (`subscribe_command_with_prefilled_start_ns`,
`data/src/engine/streaming.rs`), and IB would send every bar since as
live; until the bump to `04f766b` the node registered no catalog for that
reason.

The hand-off has no gap and no duplicate because the request ends at
`now`. IB's answer ends with the minute still in progress, a partial bar
stamped at its close, after `now`; the data engine trims the response to
the request's `[start, end]` (`DataResponse::trim_to_bounds`), so the
warmup ends on the last closed minute, and the live stream, which starts
at the subscription, sends the minute in progress once, complete. With no
end (as until 2026-10-26) nothing was trimmed and that minute came twice:
partial in the warmup, complete live. The same end keeps the window a
whole number of seconds: open-ended, the client's own later `now` added a
`1 S` segment to a warmup of a day or more, which IB refused (error 321,
logged, the bars of the other segments kept).

A market order needs no quote subscription: the risk engine prices it from
the latest cached external LAST bar (`risk/src/engine/mod.rs`,
`market_order_price`), here IB's last minute bar, as it does in a
backtest (`backtest_ib_minute_bars.rs`). It would deny the order
(`MARKET_PRICE_UNAVAILABLE`) on MID bars alone.

Measured on paper (`tests/live/ib_minute_bars.rs`, SXR8.IBIS, 2-hour warmup):
four runs on 2026-10-26 between 15:29 and 16:35 UTC, three on MID bars,
the last on the TRADES bars. The warmup answer came 0.1 to 0.2 s after the
request, 120 or 121 bars; the 16 live minute bars arrived 0.4 to 1.3 s
after their close, under the 5 s the node waits before building a bar of
source bars (`strategy/README.md`, "Composite bars"). In the last run,
with the warmup ending at now, the warmup requested at 16:31:23 ended on
the bar closing 16:31:00 and the first live bar closed 16:32:00: no gap,
no duplicate. Without the end, two runs had the minute in progress
(closing 15:30 and 15:40) in both the warmup, partial, and the live
stream.

On `04f766b`, with the node on the catalog (paper, 2026-10-27, 07:11 UTC,
SXR8.IBIS, the catalog's minute bars ending 30 min before now): the
warmup came 0.3 s after the request, 41 bars, 11 of them the catalog's and
the rest IB's; it ended on the bar closing 07:11:00 and the first live bar
closed 07:12:00: no gap, no duplicate, and no bar since the catalog's end
replayed as live.

## Rolled roots

A strategy reading a futures root (`GC.GLBX`, see `rolls/README.md`)
trades and reads its dated contracts, as in a backtest. At the start,
`Live::run` takes for each instrument the strategy reads the contracts it
needs up to its next roll (`rolls::contracts_at`, from the same calendar
the backtest's table is built from): a root gives the contract standing
for it now and the one it rolls into next, a plain instrument itself. Those are the node's
instruments: each goes to the client of its venue like any instrument,
loaded at connect, and the strategy claims its external orders. A config
without a root loads what it did before.

The strategy's `Roller` subscribes to the root's ticks on the contract in
front and switches them to the next one at the roll (`rolls/README.md`,
"The roller"). The node already holds that contract, since it loaded it at
the start. A run spans one roll: after it the node does not hold the
contract after the next one, so restart it before the root's second roll
(GC rolls two to four months apart).

The contracts are loaded at the start, not requested by the roller as it
rolls. A request reaches the venue's client only if it skips the catalog
(`force_instrument_update`, `data/src/engine/streaming.rs`
`dispatch_instrument_request`), and a backtest's data client answers no
instrument request (`backtest/src/data_client.rs` has no
`request_instrument`, and the trait's default refuses it). An instrument
answered from the catalog reaches the cache but not the client: IB's
`subscribe_trades` looks the contract up in its own provider and fails
without it (`adapters/interactive_brokers/src/data/core.rs`). One roller
cannot do both.

Bars on a root are refused at the start, naming the contract to read
instead. Nautilus' continuous bars take every roll's prices up front, in
the subscription's params (`rolls/README.md`, "Continuous bars"). A live
roll's post price is the post contract's first trade after the roll, so
it is unknown when the subscription is made. Resubscribing at each roll
would also move every earlier bar of a backward mode into the new frame
under a running strategy, and a warmup of hours usually spans no roll,
which Nautilus' continuous bars need. Read bars on the contract, or the
root's ticks through the roller.

No live client serves GLBX yet: a GLBX contract goes to IB's clients, the
default, and IB names CME futures on `CME` (`data/ib/README.md`). Running a
GLBX root live needs a client for that venue.

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

**Observed on IB paper** (`tests/live/ib_restart.rs`, 2026-10-26, EXS1.IBIS):

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
  checks"). A strategy that waits for no order in flight on the instrument
  then never trades it: reset the cache, or delete that order's keys.
- **Empty cache** (a new trader). Reconciliation rebuilds from the
  executions IB reports, the current day's: the strategy's own orders come
  back by their order reference, others as external orders claimed by the
  strategy.

## Recording into the catalog

The node streams the market data it receives into the catalog it
registers, so that a retrain the node starts replays up to the present
(`recording.rs`, `Recording`): Nautilus' streaming writer
(`LiveNodeConfig::streaming`, a `StreamingConfig`) appends what the
message bus carries to Feather files under the staging directory
(`--staging`), in `live/<instance id>/`, flushed every second, and
promotes them into the catalog every `--promotion-minutes` and when the
node is disposed after a clean stop (`promote_on_close`), each staging
file deleted once promoted (`delete_feather_after_promotion`). Promotion
writes parquet files beside fill-catalog's, named by their interval.

**What.** Quotes, trades, book deltas and depth, bars, mark and index
prices, funding rates, and instrument definitions (`data_types`); not
order and position events, account states or custom data, which the
writer would stream unfiltered: the catalog holds market data. The
venues' data clients publish their instruments' definitions at connect
(IB's from its provider, OKX's as loaded, IC's as `ic/` loads them), so
the catalog holds the definitions a backtest over the streamed data
looks up, one per run, stamped when loaded.

**Bars.** Bars the node builds (`…-INTERNAL`) are stored under the
`EXTERNAL` aggregation source, as fill-catalog's venue bars are: a
retrain reads the bars a live strategy built as
`AUD/USD.SIM-1-SECOND-LAST-EXTERNAL`, not `…-INTERNAL`. The warmup's bars
are not streamed: Nautilus builds them into the cache only (see
`strategy::warmup`).

**Readers.** Another process reading the catalog while the node runs
sees each series up to the last promotion, a clean prefix of what the
node received: no gap, no duplicate (`tests/live/streaming.rs`).

**Coverage.** Promotion names each file by the `ts_init` of its first
and last row, so the files of a series leave a gap between them, from
one's last row to the next one's first, which Nautilus counts as missing
(a 1 ns gap is a gap): a warmup over it asks the venue for every gap,
and fill-catalog would fetch each again. A backtest, or a retrain's
replay, reads the rows and not the intervals, and runs through the gaps
(backtest README, "No check before the run"). The node records the start of each run in the catalog before it is
built (`data::record_run_start`, `live-runs/<ns>` beside `data/`), and a
reader joins the gaps a run streamed across before it reads
(`data::live_runs::join`, with Nautilus' `extend_file_name`):
fill-catalog's node fill, and the node's own start over the series of
its warmup. A gap is joined when the file after it was promoted, the file
before it ends after the run's start (the run's own, or the warmup it
wrote back), and no run started inside it. A join covers the whole
series at once, every gap judged by the file names before it: the file
before a joined gap is renamed by its interval alone, no longer named as
promoted, so a later join of a narrower window would leave the gap
before that file. A crash or a stop stays a
gap, as does the time before a run's first row. The node itself renames
nothing while it streams: a reader never loses a file under it.

**Instance id.** Nautilus names the staging run directory after the
node's instance id and, at start, recovers and promotes only the files
of its own directory. The node of a trader keeps one instance id across
runs, in `<staging>/<trader id>.instance-id`, written once (temporary
file, then rename): a run after a crash writes to the same directory,
seals the files the crashed run left (`.feather.partial`, up to their
last complete batch), and the start promotes them as soon as the node is
built (`promote_staged`: the writer closed, which promotes what it
holds, `promote_on_close`, and reopened, empty, in the same directory),
before the gaps are joined and the warmup reads the catalog and writes
IC's answer back after them. Left to the first promotion, a promotion
interval later, they would lie under that answer, which Nautilus would
refuse to promote them over (non-disjoint intervals), losing them, and
every start after would fail the same way. A crash loses at most the last
second, unflushed. A staged file the catalog already holds rows over
fails the start, naming it. The same id is the logger's. A file
that holds no instance id fails the start rather than being replaced:
with a new id, what the old one left unpromoted would stay in staging.
Two live processes at once each need their own trader (see "Cache in
Redis"), so their staging runs differ.

**Disk.** Staging holds at most a promotion interval of data. The tick
directories of the catalog gain a file per promotion: consolidate them
from time to time (`ParquetDataCatalog::consolidate_catalog` and its
kin).

## Retraining

A strategy trading on a model retrains live on its model section's
`retrain` interval: its model handle (`Mode::Live`) sets a Nautilus timer
named after the strategy, `RETRAIN-<strategy id>`, on the epoch-aligned
grid of the interval (`model::schedule`, shared with the backtest), and each firing starts this binary's `train` at
the grid time as a child process (`model::launcher`), with the node's
`--config`, `--catalog`, `--models` and `--leverage` and the retrain
costs: the replay runs a Nautilus node of its own, which this process,
holding the live one, cannot. The timer fires
`Recording::catalog_delay` after the grid time, a promotion interval and
a minute: by then the data the node received up to that time is in the
catalog for the replay. One retrain per strategy runs at a time: one due
while another runs is skipped, with a warning. The strategy polls the
child at each decision: a finished child is logged, as info when it
published (exit status success), as an error with the error it printed
last when it kept the current model or failed (with a meta model, the
child trains the primary, then the meta, and succeeds only if it
published both, its error naming each one's outcome); the child's standard
output and error are the node's. A published model is used from the
next decision, and a node starting with models in the store trades on
the latest valid from its first decision (strategy README, "Live
retraining"). A child still running when the node stops runs to its
end; its model is there for the next start.

## Known limits

Observed at the pinned `develop` commit; the code keeps the documented
usage.

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
- **A warmup of a day or more may miss bars.** The data client splits the
  window into years, days and seconds (`calculate_duration_segments`), and
  IB counts `D` in trading sessions, not 24 h, so bars between the days and
  seconds segments can go missing without an error, as the same split did
  for history (`data/ib/README.md`, "Walking back, not segments";
  nautilus_trader#5384). Not observed live yet.

## node.rs

`node::run` builds the node through `LiveNodeBuilder::from_config`, with
the engine configs as `LiveNodeConfig` fields. It registers the one
catalog, writable (`LiveNodeConfig::catalogs`, enabled by
`nautilus-live`'s `streaming` feature; its path absolute, as the
backtest's), so that the warmup reads it and IC's answers are written back
(see "Warmup and live bars on IB" and `ic/README.md`), and streams into it
under the trader's kept instance id (see "Recording into the catalog").

```rust
let config = LiveNodeConfig {
    environment: Environment::Live,
    trader_id,                                   // TraderId::from("HELIX-001")
    instance_id: Some(instance_id),              // kept in the staging directory
    logging,
    catalogs: vec![DataCatalogConfig::new(catalog, None, None)], // absolute path
    streaming: Some(recording.streaming(&catalog)?), // see "Recording into the catalog"
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
- A data client on one venue (OKX's, IC's) routes that venue when
  registered; IB's has none (`venue()` is `None`, IB spans many) and is
  the default.
- An order goes to the client its `client_id` names (ours name none),
  else its account's issuer's client, else the client routed for its
  instrument's venue, else the default
  (`ExecutionEngine::find_client_for_command`); a subscription or request
  to the client its `client_id` names, else its venue's, else the default
  (`DataEngine::get_client`). So with IB's clients the default, an
  `….ICMARKETS` order or quote subscription goes to IC's and an
  `….IBIS` one to IB's.

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

`LiveNodeConfig::streaming` (or `with_streaming_config(StreamingConfig)`)
writes live data and events to Feather files under
`<writer_path>/live/<instance id>/`; with its `catalog` set it promotes
them into that catalog. `StreamingConfig::new(writer_path, catalog:
Option<DataCatalogConfig>, flush_interval_ms, replace_existing,
rotation_config)`, then the public fields: `promotion_interval_ms`,
`promote_on_close` (default on), `delete_feather_after_promotion`
(default off), and the filters `data_types`, `record_types`,
`instrument_types` and `record_filters` (unset: everything the bus
carries). `RotationConfig` is by size, interval, scheduled dates, or none;
with none, each promotion seals the files it promotes. The kernel creates
the writer when the node is built (refusing an invalid config,
`StreamingConfig::validate`), which recovers what a crashed run of the
instance left but promotes it only at its first promotion; flushes it at
stop and closes it, promoting the rest, at dispose.
`NautilusKernel::close_streaming_writer` closes it, promoting, and
`reopen_streaming_writer` opens it again in the same run directory
(`LiveNode::kernel_mut`). `replace_existing` empties the run directory first:
off, so that a restart keeps what a crash left.

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

## IC Markets

`ic/`: IC's clients, their environment and credential, the account check
and reconciliation, the warmup and their tests: see
[`ic/README.md`](ic/README.md).

## OKX

`okx/`: the OKX clients, the EEA environment, the account checks and their
tests: see [`okx/README.md`](okx/README.md).

## Tests

- Unit tests sit in a `#[cfg(test)] mod tests` at the bottom of each file,
  for what needs no node and no broker.
- `tests/live/streaming.rs` runs a node without a broker, hermetic and
  ungated: a stub data client on `SIM` sends numbered quotes, trades and
  depth, an actor subscribes to them and to 1-second bars built from the
  trades, and the node streams as `Recording` configures it. The node
  runs in a child process (the test binary, filtered to `streamed_node`),
  stopping itself through `handle().stop()` or killed, and the test reads
  the catalog from its own process: a clean prefix of each series while
  the node runs, everything after a clean stop with no staging file left,
  the built bars under `EXTERNAL`, the instrument definition, and after a
  crash and a restart the crashed run's staged data promoted, before an
  answer written back over what the catalog lacks up to the start, as
  IC's warmup writes it back, lands after it. The
  binary's backtest over data streamed across several promotions runs
  through the gaps between them, which it leaves in the catalog, and
  across a crash and a restart through the crash's gap.
- Retraining needs no node: the launcher and the handle's retrains are
  unit tests (`model::launcher`, `model::handle`) on an in-process
  launcher, a `sh` child and a clock driven by hand, and
  `tests/backtest/backtest_learned.rs` starts the real `train` as a live
  node does and backtests on the model it publishes.
- Venue end-to-end tests are under "IB tests", "OKX tests" and "IC tests"
  in the venue READMEs; `ib_restart.rs` there checks the cache in Redis
  across restarts.

## Unverified

- A live retrain against a real venue: none has run. A backtest over
  streamed data running through the gaps between promoted files is shown
  on the stub client only.
- A disconnect inside a run reads as covered once joined, as it does
  inside one promoted file: Nautilus names a file by its rows, and no
  client tells the node it lost data. A crash or a stop reads as a gap.
- Joining renames files (Nautilus' `extend_file_name`): a reader that
  listed a series just before another process joined it (a backtest
  and a start, two fills) can fail to open a file and must read again. Promotion and the node's retrains, one at a time, do not race
  it.
- Streaming on IB and OKX: only IC's demo has streamed so far
  (`ic/README.md`, "IC tests", `ic_live_demo.rs`), and a crash and
  restart only on the stub client.
- A promotion overlapping what the catalog already holds fails (Nautilus
  refuses non-disjoint intervals), with a warning, and its file stays in
  staging: observed for a definition re-sent with the time of one already
  promoted (a stub stamping it at 0).
  IC's warmup answer, written back up to its `now`, cannot overlap the
  run's first promoted quotes, stamped at their receipt after the
  subscription the warmup sends after its request (`ic/README.md`,
  "Warmup"), nor what a crashed run left staged, promoted when the
  restart's node is built, before the warmup (see "Instance id"; the stub
  client's restart writes an answer back as IC's, `streaming.rs`).
- Whether IB's answer to a `30 D` request of 1-minute bars is complete
  (it raised no error, but the node panicked before the bars could be
  counted).
- Whether IB reports ES's exchange as `CME` (the venue comes from that
  field), and which history depths IB serves (see `data/ib/README.md`).
