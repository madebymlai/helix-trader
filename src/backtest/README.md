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
  mod.rs      the step 2 entry point
  node.rs     the BacktestRunConfig and the BacktestNode run
  costs.rs    fee and fill models of the simulated venue
```

Dependencies, all pinned at `=0.65.0`:

- `nautilus-backtest` with the `streaming` feature, which enables the
  `node` module and catalog registration.
- `nautilus-execution` (fee and fill models), `nautilus-persistence`
  (`DataCatalogConfig`), `nautilus-analysis` (extra statistics),
  `nautilus-common` (`LoggerConfig`).

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
- One run config per node, one node per process.
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
| `dispose_on_completion` | default `true` |

```rust
let run_config = BacktestRunConfig::builder()
    .id(RUN_ID.to_string())
    .venues(vec![venue_config])
    .data(vec![data_config])
    .engine(engine_config)
    .start(start)
    .end(end)
    .build()?;
```

### Venue

`BacktestVenueConfig`, `bon` builder:

| Field | Value |
|---|---|
| `name` | the venue, for example `"CME"` |
| `oms_type` | `OmsType::Netting` |
| `account_type` | `AccountType::Margin` |
| `book_type` | `BookType::L1_MBP` (deeper fails with bars only) |
| `starting_balances: Vec<String>` | for example `vec!["100_000 USD".into()]` |
| `fee_model: Option<FeeModelAny>` | required: `build()` fails without it, so a missing fee never becomes zero |
| `fill_model: Option<FillModelAny>` | slippage (see Costs) |

Flags worth knowing: `bar_execution` (default `true`, bars drive matching),
`bar_adaptive_high_low_ordering` (default `false`; per bar, visit the
extreme closest to the open first), `use_reduce_only` (`true`),
`allow_cash_borrowing` (`false`). Optional: `margin_model`,
`latency_model`, `default_leverage`, `leverages`, `base_currency`.

```rust
let venue_config = BacktestVenueConfig::builder()
    .name("CME")
    .oms_type(OmsType::Netting)
    .account_type(AccountType::Margin)
    .book_type(BookType::L1_MBP)
    .starting_balances(vec!["100_000 USD".to_string()])
    .fee_model(fee_model)      // from costs.rs
    .fill_model(fill_model)    // from costs.rs
    .bar_adaptive_high_low_ordering(true)
    .build()?;
```

### Data

`BacktestDataConfig`, `bon` builder, one per data stream:

```rust
let data_config = BacktestDataConfig::builder()
    .data_type(NautilusDataType::Bar)
    .catalog_path(catalog_path)
    .instrument_ids(instrument_ids)          // or .instrument_id(id)
    .bar_spec(spec)                          // or .bar_types(vec![..])
    .build()?;
```

- With `bar_spec`, the node loads `{instrument_id}-{bar_spec}-EXTERNAL`
  bars. The engine replays `EXTERNAL` bars only; `INTERNAL` bar types
  (continuous futures) are built by the data engine from a subscription,
  never loaded.
- `start_time`, `end_time` narrow what is loaded. Data before the run
  `start` is loaded but skipped by the replay.
- The node loads the instruments of the catalog in `build`, so
  `fill_catalog` must have written them.

### Engine config

`BacktestEngineConfig` (`nautilus_backtest::config`), `bon` builder and
`Default`. Fields we set:

| Field | Use |
|---|---|
| `logging: LoggerConfig` | log levels and files |
| `catalogs: Vec<DataCatalogConfig>` | serves strategy warmup requests |

## Warmup catalog

Warmup is described in the strategy README. On the backtest side, register
the catalog in `BacktestEngineConfig::catalogs`:

```rust
DataCatalogConfig::new(path: String, fs_protocol: Option<String>,
                       catalog_backend: Option<CatalogBackendType>) -> DataCatalogConfig
```

Without it the warmup response is empty. The request window is clamped to
simulated now, which is the run `start` when `on_start` runs, so warmup data
must predate the run start; later bars arrive through the replay. Catalog
names must be unique, and a bad path fails the build.

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
  `Expectancy`, `Win Rate`, `Profit Factor`.
- `stats_returns`: `Returns Volatility (252 days)`, `Returns Skewness`,
  `Returns Kurtosis`, `Average (Return)`, `Average Loss (Return)`,
  `Average Win (Return)`, `Sharpe Ratio (252 days)`,
  `Sortino Ratio (252 days)`, `Tail Ratio`, `Risk Return Ratio`.
- `stats_general`: `Long Ratio`.

`Max Drawdown`, `CAGR (252 days)`, `Calmar Ratio (252 days)` are not in the
default set. Compute them from `returns_series` with a `PortfolioAnalyzer`
(`nautilus_analysis`):

```rust
let mut analyzer = PortfolioAnalyzer::default();
analyzer.register_statistic(Arc::new(MaxDrawdown::new()));
analyzer.register_statistic(Arc::new(CAGR::new(None)));
for (ts, r) in &result.returns_series { analyzer.add_return(*ts, *r); }
let returns = analyzer.get_performance_stats_returns();   // AHashMap<String, f64>
```

The engine also logs its statistics at the end of the run.

## Costs

`costs.rs` builds the two models the venue config takes
(`nautilus_execution::models`):

```rust
let fee = FeeModelAny::PerContract(PerContractFeeModel::new(commission)?);
let fill = FillModelAny::OneTickSlippage(OneTickSlippageFillModel::new(1.0, 0.0, Some(seed))?);
```

### Fees

| Variant | Constructor | Charges |
|---|---|---|
| `PerContract` | `PerContractFeeModel::new(Money)` | per contract, the broker model; `Err` if negative |
| `Fixed` | `FixedFeeModel::new(Money, Option<bool>)` | flat per order, first fill only by default |
| `MakerTaker` | `MakerTakerFeeModel::new(maker, taker)`, `::zero()` | notional rate by liquidity side |

`MakerTakerFeeModel::zero()` is the explicit zero fee for tests. The other
variants are for options.

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

With bars and no quotes, slippage is the cost model: `OneTickSlippage` is the
conservative fixed cost, `Probabilistic` half of it on average.

## Rules

- **The plain Nautilus API.** A run is a `BacktestRunConfig` on a
  `BacktestNode`; no engine wrapper.
- **One run per process**, no parameter sweeps.
- **Costs always explicit.** A fee model always, even a zero one.
- **Warmup before the start.** Warmup bars must predate the run start.
- **Seed all randomness**: the fill model and the strategy config.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file, for
  pure code: the run config from our inputs, the cost models
  (`PerContractFeeModel::new` rejects a negative commission, fill models
  reject a probability outside `[0, 1]`).
- Integration tests in `tests/`, one node per file, on a small fixture
  catalog written with `ParquetDataCatalog::write_to_parquet`:
  - a run produces fills and the expected commission;
  - warmup bars arrive before the first `on_bar`;
  - a seeded run reproduces the PnL figures recorded in the test.
