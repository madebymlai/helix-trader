# Backtesting

The backtest module runs one strategy over the catalog, with costs: build
step 2. It describes the run as a Nautilus `BacktestRunConfig` and runs it on
a `BacktestNode`, the plain Nautilus API, and adds nothing the node already
does. One run per process; there are no parameter sweeps.

APIs are those of the `nautilus-*` crates at `0.65.0`; *unverified* marks
what the sources did not confirm. Process-wide rules (one node per process,
Nautilus owns the logger) are in `docs/ARCHITECTURE.md`.

## Layout

```
backtest/
  mod.rs      Backtest: the one public type
  node.rs     the BacktestRunConfig and the BacktestNode run
  coverage.rs the check that the catalog can serve the run
  costs.rs    fee and fill models of the simulated venue
  report.rs   report(&BacktestResult) -> String: the results as text
```

Dependencies, all pinned at `=0.65.0`:

- `nautilus-backtest` with the `streaming` feature, which enables the
  `node` module and catalog registration.
- `nautilus-execution` (fee and fill models), `nautilus-persistence`
  (`DataCatalogConfig`), `nautilus-analysis` for statistics not in the
  default set (see Results).

## The entry point

```rust
pub struct Backtest {
    pub catalog: PathBuf,          // bars, instruments, warmup
    pub bar_types: Vec<BarType>,   // instruments and venue derive from them
    pub start: Timestamp,
    pub end: Timestamp,
    pub warmup: SignedDuration,    // the strategy's
    pub commission: Money,         // per order
    pub balance: Money,
    pub logging: LoggerConfig,     // the node installs the logger with it
}

impl Backtest {
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<BacktestResult>
    where S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static;
}
```

Only Nautilus types cross it. `run` fails before building the node on no
bar types, bar types on several venues, a negative commission or warmup,
`start` after `end`, or a catalog that cannot serve the run (see Coverage
check). A relative catalog path is made absolute: the node's DataFusion
reader finds no object store for `file://catalog/...`.

## The run

```rust
let mut node = BacktestNode::new(vec![run_config])?;
node.build()?;                                         // engine, venue, instruments
node.get_engine_mut(RUN_ID)
    .context("no engine for the run id")?
    .add_strategy(strategy)?;                          // strategies are not config
let results: Vec<BacktestResult> = node.run()?;       // loads the data, runs
```

`nautilus_backtest::node::BacktestNode`:

```rust
pub fn new(configs: Vec<BacktestRunConfig>) -> anyhow::Result<Self>;
pub fn build(&mut self) -> anyhow::Result<()>;
pub fn get_engine_mut(&mut self, id: &str) -> Option<&mut BacktestEngine>;
pub fn run(&mut self) -> anyhow::Result<Vec<BacktestResult>>;
pub fn dispose(&mut self);
```

- `build` creates the engine, adds the venue and loads the instruments from
  the catalog. `run` loads the data and runs.
- The strategy goes in between: a run config holds no strategy. The engine's
  `add_strategy` takes any type with the bound
  `Strategy + StrategyNative + DataActorNative + Component + Debug +
  'static`, so `node.rs` is generic over the strategy and never names one.
- One run config per node: `new` rejects more ("kernel MessageBus is a
  thread-local singleton"). One node per process.
- With the default `dispose_on_completion = true` the node is spent after
  `run`; the process ends there anyway.

## The run config

`BacktestRunConfig` (`nautilus_backtest::config`), `bon` builder; `build()`
validates and returns a `Result`.

| Field | Use |
|---|---|
| `id: String` | the run id, used by `get_engine_mut` |
| `venues: Vec<BacktestVenueConfig>` | the simulated venue |
| `data: Vec<BacktestDataConfig>` | what to load from the catalog |
| `engine: BacktestEngineConfig` | logging, warmup catalog |
| `start`, `end: Option<UnixNanos>` | the run window |
| `chunk_size: Option<usize>` | stream the data in chunks instead of loading it all |
| `raise_exception` | default `false`: set it `true` (see below) |
| `dispose_on_completion` | default `true` |

With `raise_exception = false`, a build or run error is only logged, the
run is skipped, and `node.run()` returns `Ok` with no result. We set it
`true`, so the error is returned.

```rust
let run_config = BacktestRunConfig::builder()
    .id(RUN_ID.to_string())
    .venues(vec![venue_config])
    .data(vec![data_config])
    .engine(engine_config)
    .start(start)
    .end(end)
    .raise_exception(true)
    .build()?;
```

### Venue

`BacktestVenueConfig`, `bon` builder:

| Field | Value |
|---|---|
| `name` | the bar types' venue, for example `"IBIS"` |
| `oms_type` | `OmsType::Netting` |
| `account_type` | `AccountType::Margin` (the EMA strategy goes short) |
| `book_type` | `BookType::L1_MBP` (`new` rejects deeper books without book data) |
| `starting_balances: Vec<String>` | the balance, `Money::to_string()`, for example `"10000.00 EUR"` |
| `fee_model: Option<FeeModelAny>` | required: the node's `build` fails without it (the builder's `build()` does not), so a missing fee never becomes zero |
| `fill_model: Option<FillModelAny>` | slippage (see Costs) |

Flags worth knowing: `bar_execution` (default `true`, bars drive matching),
`bar_adaptive_high_low_ordering` (default `false`, we set `true`; per bar,
visit the extreme closest to the open first), `use_reduce_only` (`true`),
`allow_cash_borrowing` (`false`). Optional: `margin_model`,
`latency_model`, `default_leverage`, `leverages`, `base_currency`.

### Data

`BacktestDataConfig`, `bon` builder, one per data stream; ours is one for
all the bar types:

```rust
let data_config = BacktestDataConfig::builder()
    .data_type(NautilusDataType::Bar)
    .catalog_path(catalog_path)
    .bar_types(bar_types.iter().map(ToString::to_string).collect())
    .build()?;
```

- `bar_types` are queried as given; with `instrument_id(s)` and `bar_spec`
  instead, the node queries `{instrument_id}-{bar_spec}-EXTERNAL`. The
  engine replays `EXTERNAL` bars only; `INTERNAL` bar types (continuous
  futures) are built by the data engine from a subscription, never loaded
  (*unverified*).
- The node queries the catalog over the run window only (the intersection
  with `start_time`, `end_time` if set): bars before the run `start` are not
  loaded. Indicators warm up from the strategy's request, not the replay.
- The node loads the instruments of the catalog in `build`, so
  `fill_catalog` must have written them.

### Engine config

`BacktestEngineConfig` (`nautilus_backtest::config`), `bon` builder and
`Default`. Fields we set:

| Field | Use |
|---|---|
| `catalogs: Vec<DataCatalogConfig>` | serves strategy warmup requests |
| `logging: LoggerConfig` | the logger the node installs (`Backtest::logging`) |

The node's kernel installs Nautilus's logger from `logging` when it is
built. Installing one before (`init_logging`, as `fill-catalog` does) would
not fail, the kernel would reuse it, but its own config would be ignored:
so the command passes the `NAUTILUS_LOG` config through `Backtest` and
installs nothing itself.

## Warmup catalog

Warmup is described in the strategy README. On the backtest side, register
the catalog in `BacktestEngineConfig::catalogs`:

```rust
DataCatalogConfig::new(path: String, fs_protocol: Option<String>,
                       catalog_backend: Option<CatalogBackendType>) -> DataCatalogConfig
```

The request window is clamped to simulated now, which is the run `start`
when `on_start` runs, so warmup data must predate the run start; later bars
arrive through the replay. What the strategy receives, as the runs showed:

- **Catalog covers the window** (its stored intervals for the bar type span
  `[start − warmup, start]`): one `on_historical_bars` with the bars of the
  window, before the first `on_bar`. A covered window holding no bars still
  calls it, with an empty slice.
- **Catalog not registered, or a gap in the window**: the data engine asks
  the venue's backtest data client for the missing part. That client does
  not implement bar requests (a `WARN` is logged), no response ever comes,
  and `on_historical_bars` is never called, not even with the stored part.
  The run goes on with cold indicators.

So the catalog must cover the whole warmup window for every bar type.

## Coverage check

Before building the node, `run` checks the catalog (`coverage.rs`) and
fails on the first miss:

- every instrument of the bar types is stored, else the error names the
  instrument id;
- every bar type is covered over `[start − warmup, end]`, else the error
  names the bar type and its first missing range. A bar type not stored
  misses the whole range.

Coverage is the bar type's stored intervals (`get_intervals`, inclusive
nanoseconds; `fill_catalog` records each fill over the window asked for),
with gaps computed by Nautilus's `missing_intervals`, the function behind
the data engine's own catalog lookup. Adjacent intervals (`end + 1 ==
start`, as a resumed fill writes them) and overlapping ones cover as one;
a 1 ns gap is a gap. An interval ending exactly at `end` covers it. The
range is checked whole: the warmup request asks for `[start − warmup,
start]`, the replay reads the run window.

## Results

`node.run()` returns one `BacktestResult` (`nautilus_backtest::result`) per
run config; all fields are public:

```rust
iterations: usize, total_events: usize, total_orders: usize, total_positions: usize,
summary: AHashMap<String, String>,
stats_pnls: AHashMap<String, AHashMap<String, f64>>,   // currency -> stat -> value
stats_returns: AHashMap<String, f64>,
stats_general: AHashMap<String, f64>,
returns_series: BTreeMap<UnixNanos, f64>,
// plus ids, run timestamps and elapsed_time_secs (simulated, not wall time)
```

Default statistics:

- `stats_pnls[currency]`: `PnL (total)`, `PnL% (total)`, `Max Winner`,
  `Avg Winner`, `Min Winner`, `Min Loser`, `Avg Loser`, `Max Loser`,
  `Expectancy`, `Win Rate`. With no winning position the winner
  statistics are `NaN`.
- `stats_returns`: `Returns Volatility (252 days)`, `Returns Skewness`,
  `Returns Kurtosis`, `Average (Return)`, `Average Loss (Return)`,
  `Average Win (Return)`, `Sharpe Ratio (252 days)`,
  `Sortino Ratio (252 days)`, `Profit Factor`, `Tail Ratio`,
  `Risk Return Ratio`.
- `stats_general`: `Long Ratio`.
- `summary`: `orders.total`, `orders.open`, `orders.closed`,
  `positions.total`, `positions.open`, `positions.snapshots`, and per venue
  `account.{venue}.balance.{currency}.total` (also `.free`, `.locked`), for
  example `"9942.00 EUR"`.

PnL is net of commissions: a position's realized PnL subtracts the
commission of each fill in its settlement currency, the open position's
included. The result has no commission figure of its own; with a fixed fee
per order it is `total_orders × commission`. `total_positions` counts the
snapshots of positions a netting flip closed, while `positions.total` in the
summary counts only cached positions (one per instrument when netting).

`Max Drawdown`, `CAGR (252 days)`, `Calmar Ratio (252 days)` are not in the
default set. Each is a `PortfolioStatistic` (`nautilus_analysis`)
computed from `returns_series`; a `PortfolioAnalyzer` only loops over the
statistics registered, so `report.rs` calls the one it prints directly:

```rust
let max_drawdown = MaxDrawdown::new().calculate_from_returns(&result.returns_series); // Some(-0.05): 5%
```

The engine also logs its statistics at the end of the run.

## The command

```
helix-trader backtest ema --config ema.toml --start 2026-01-01 --end 2026-10-01 \
  [--catalog catalog] --commission "3 EUR" --balance "10000 EUR"
```

`main` loads and validates the strategy's config, builds the `Backtest`
from its bar types and warmup, runs the strategy and prints `report`:
`Orders`, `Positions`, then one section per currency of PnL statistics,
the return statistics with `Max Drawdown`, the general statistics, and the
final balances (`account.*.balance.*.total` of the summary), names sorted,
values to six decimals. `--commission` and `--balance` are required
Nautilus `Money` (`"<amount> <currency>"`), parsed with `Money::from_str`;
a malformed one fails at argument parsing. A negative commission
(`--commission="-3 EUR"`) fails before the node is built. The engine log
goes to stdout before the results, configured by `NAUTILUS_LOG`.

On the fixture catalog of the tests (EMA 5/15, 10 shares, warmup 48h, 3 EUR
commission) it prints `Orders: 9`, `PnL (total): -58.000000` (the
fixture's -31 without commission, less 9 × 3 EUR) and a final balance of
`9942.00 EUR`.

## Costs

`costs.rs` builds the two models the venue config takes
(`nautilus_execution::models`):

```rust
let fee = FeeModelAny::Fixed(FixedFeeModel::new(commission, Some(true))?);
let fill = FillModelAny::OneTickSlippage(OneTickSlippageFillModel::new(1.0, 0.0, Some(FILL_SEED))?);
```

### Fees

| Variant | Constructor | Charges |
|---|---|---|
| `Fixed` | `FixedFeeModel::new(Money, Option<bool>)` | flat per order, on its first fill only by default (`Some(true)`); `Err` if negative. Ours |
| `PerContract` | `PerContractFeeModel::new(Money)` | per contract; `Err` if negative |
| `MakerTaker` | `MakerTakerFeeModel::new(maker, taker)`, `::zero()` | notional rate by liquidity side |

The other variants are for options. A fixed fee matches IB Fixed pricing on
Xetra below EUR 6,000 an order, where the EUR 3 minimum always applies.

### Fills

Every fill model is built from `(prob_fill_on_limit: f64, prob_slippage: f64,
random_seed: Option<u64>)`, probabilities in `[0, 1]`. Always pass a seed.

| `FillModelAny` variant | Simulates |
|---|---|
| `Default` | the matching engine's own logic |
| `BestPrice` | everything at the best price, unlimited size |
| `OneTickSlippage` | every fill one tick worse than the best price |
| `Probabilistic` | 50/50 per fill: best price or one tick worse |
| `TwoTier` | first 10 contracts at best, the rest one tick worse |
| `ThreeTier` | 50 at best, 30 at +1 tick, 20 at +2 ticks |
| `LimitOrderPartialFill` | at most 5 contracts at best, the rest one tick worse |
| `SizeAware` | small orders at best, larger ones partly one tick worse |
| `CompetitionAware` | liquidity scaled by a `liquidity_factor` |
| `VolumeSensitive`, `MarketHours` | depend on setters the node never calls: avoid |

`OneTickSlippage` quotes unlimited size one tick beyond the best bid and
ask, so it slips whatever `prob_slippage`; with `prob_fill_on_limit = 1.0`
the seed draws nothing that changes a result. With bars and no quotes the
best price is the bar's: a market order sent in `on_bar` fills at the
same timestamp, at that bar's close plus (buy) or minus (sell) one tick.

## Rules

- **The plain Nautilus API.** A run is a `BacktestRunConfig` on a
  `BacktestNode`; no engine wrapper.
- **One run per process**, no parameter sweeps.
- **Costs always explicit.** A fee model always, even a zero one.
- **Warmup before the start.** Warmup bars must predate the run start, and
  the catalog must cover the whole warmup window: `run` refuses otherwise.
- **Seed all randomness**: the fill model and the strategy config.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file, for
  pure code: the run config from our inputs, the cost models (a negative
  commission is rejected, the commission is charged on the first fill only),
  the coverage check (gaps on the intervals alone, then on a temporary
  catalog: missing instrument, missing bar type, 1 ns short at either end),
  the report of a hand-built `BacktestResult` (sections, sorted names, max
  drawdown). The command line is tested in `main.rs`: required flags,
  malformed `Money`, the window check.
- Integration tests in `tests/`, one node per file, on a fixture catalog
  (`tests/common`: SXR8.IBIS in EUR, synthetic hourly bars on a wave)
  written with `ParquetDataCatalog::write_to_parquet` and stored up to the
  run `end`:
  - `backtest_warmup.rs`: a probe strategy receives the warmup bars, then
    every replayed bar; its one market order fills one tick above the close
    and pays the commission once.
  - `backtest_ema_cross.rs`: the EMA strategy trades, its PnL is lower by
    `orders × commission` than without commission, and it reproduces the
    figures recorded in the test.
  - `backtest_ema_cross_free.rs`: the same run without commission, the
    reference for the commission check.
  - `backtest_coverage.rs`: a warmup longer than the stored history is
    refused before the node is built, naming the bar type.
