# Backtesting

The backtest module runs one strategy over the catalog, with costs: build
step 2. It describes the run as a Nautilus `BacktestRunConfig` and runs it on
a `BacktestNode`, the plain Nautilus API, and adds nothing the node already
does. One run per process; there are no parameter sweeps.

APIs are those of the `nautilus-*` crates at `0.66.0`; *unverified* marks
what the sources did not confirm. Process-wide rules (one node per process,
Nautilus owns the logger) are in `docs/ARCHITECTURE.md`.

## Layout

```
backtest/
  mod.rs      Backtest: the one public type
  node.rs     the BacktestRunConfig and the BacktestNode run
  coverage.rs the check that the catalog can serve the run
  costs.rs    fee and fill models of the simulated venue
  report.rs   Outcome, the result and the costs; report(&Outcome) -> String
```

Dependencies, all pinned at `=0.66.0`:

- `nautilus-backtest` with the `streaming` feature, which enables the
  `node` module and catalog registration.
- `nautilus-execution` (fee and fill models), `nautilus-persistence`
  (`DataCatalogConfig`), `nautilus-analysis` for statistics not in the
  default set (see Results).

## The entry point

```rust
pub struct Backtest {
    pub catalog: PathBuf,                // instruments, quotes, warmup
    pub bar_types: Vec<BarType>,   // the strategy's: their instruments' quotes replayed,
                                   // the venue derived from them, the cache sized for their warmup
    pub start: Timestamp,
    pub end: Timestamp,
    pub warmup: SignedDuration,    // the strategy's
    pub fee_model: FeeModelAny,    // commission_per_order or commission_rate
    pub balance: Money,
    pub logging: LoggerConfig,     // the node installs the logger with it
}

impl Backtest {
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<Outcome>
    where S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static;
}

pub struct Outcome {
    pub result: BacktestResult,    // Nautilus'
    // and the costs of the fills, private: report prints them (see Results)
}
```

The fields of `Backtest` are Nautilus types; we add no types of our own.
`run` returns `Outcome`: Nautilus' `BacktestResult` plus the costs of the
fills. `commission_per_order(Money)` and `commission_rate(Decimal)` build
the fee model (see Costs).

`run` fails before building the node when:

- there are no instruments, or they are on more than one venue;
- the warmup is negative;
- `start` is not before `end`;
- the cache cannot hold the warmup (see the strategy README);
- the catalog cannot serve the run (see Coverage check).

A relative catalog path is made absolute: with a relative one, the node's
DataFusion reader finds no object store for `file://catalog/...`.

The run replays the instruments' quotes and nothing else: the strategy's
bars are internal bars the data engine builds from those quotes (see the
strategy README), and orders fill on them (see Fills).

## The run

```rust
let mut node = BacktestNode::new(vec![run_config])?;
node.build()?;                                         // engine, venue, instruments
node.get_engine_mut(RUN_ID)
    .context("no engine for the run id")?
    .add_strategy(strategy)?;                          // strategies are not config
let results: Vec<BacktestResult> = node.run()?;       // loads the data, runs
let engine = node.get_engine(RUN_ID)?;                 // kept: dispose_on_completion(false)
// the fills from engine.kernel().cache, the quotes loaded again (see Results)
node.dispose();
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
- By default (`dispose_on_completion = true`) the node throws the engine
  away at the end of `run`, cache included. We set it `false`, as Nautilus
  documents for reading the results after a run. Then we read the fills
  from the engine's cache and call `dispose` ourselves.

## The run config

`BacktestRunConfig` (`nautilus_backtest::config`), `bon` builder; `build()`
validates and returns a `Result`.

| Field | Use |
|---|---|
| `id: String` | the run id, used by `get_engine_mut` |
| `venues: Vec<BacktestVenueConfig>` | the simulated venue |
| `data: Vec<BacktestDataConfig>` | what to load from the catalog |
| `engine: BacktestEngineConfig` | logging, warmup catalog, cache and data engine from `strategy::warmup` |
| `start`, `end: Option<UnixNanos>` | the engine's window: `start − 1 ns` to `end` (see below) |
| `chunk_size: Option<usize>` | stream the data in chunks instead of loading it all |
| `raise_exception` | default `false`: set it `true` (see below) |
| `dispose_on_completion` | default `true`: set it `false`, so that the cache outlives `run` |

With `raise_exception = false`, a build or run error is only logged, the
run is skipped, and `node.run()` returns `Ok` with no result. We set it
`true`, so the error is returned.

```rust
let run_config = BacktestRunConfig::builder()
    .id(RUN_ID.to_string())
    .venues(vec![venue_config])
    .data(data_configs)
    .engine(engine_config)
    .start(engine_start)      // start − 1 ns
    .end(end)
    .raise_exception(true)
    .dispose_on_completion(false)
    .build()?;
```

**The engine start.** The engine starts 1 ns before `start`
(`Backtest::engine_start`). It skips data before its start, and a minute's
quote is stamped 1 ns before the minute's close: a run starting exactly on
a bar's close would otherwise skip the quote closing that bar and reject an
order sent on the bar (`No market`). The strategy's clock, and so its
warmup request, starts there too: the warmup covers `[start − 1 ns −
warmup, start − 1 ns]` and the coverage check follows (see Coverage check).
The quote stamped `start − 1 ns` is in both the warmup and the replay: the
bar spanning them counts it twice in its volume, which nothing reads; its
prices are right.

**Why not Nautilus' build delay.** Nautilus documents
`DataEngineConfig::time_bars_build_delay` for data stamped on a bar
boundary (Concepts › Backtesting › Bar execution, "Internal bar
aggregation timing"). A probe on 0.66.0 with quotes stamped at the close
and a 1 µs delay passed for a run starting off the hour but failed for one
starting on a bar close: the warmup lost its first full bar
(`skip_first_non_full_bar` drops it once the delay shifts the first
close), and when the warmup request's aggregator moved to the engine clock
it restarted its timer at `start`, merging the bar closing at `start` into
the next one, so the order decided on it went an hour late. The delay also
stamps every bar 1 µs after its close, which the session would read as
past the close, and loses the bar closing at `end`. Stamping the quote
1 ns early has none of these effects.

### Venue

`BacktestVenueConfig`, `bon` builder:

| Field | Value |
|---|---|
| `name` | the instruments' venue, for example `"IBIS"` |
| `oms_type` | `OmsType::Netting` |
| `account_type` | `AccountType::Margin` (the EMA strategy goes short) |
| `book_type` | `BookType::L1_MBP` (`new` rejects deeper books without book data) |
| `starting_balances: Vec<String>` | the balance, `Money::to_string()`, for example `"10000.00 EUR"` |
| `fee_model: Option<FeeModelAny>` | required: the node's `build` fails without it (the builder's `build()` does not), so a missing fee never becomes zero |
| `fill_model: Option<FillModelAny>` | `Default`, no slippage (see Fills) |
| `bar_execution` | `false`: only quotes move the book |

Flags worth knowing: `liquidity_consumption` (default `false`, left off:
every order sees the full quote size), `bar_adaptive_high_low_ordering`
(bar execution only), `use_reduce_only` (`true`), `allow_cash_borrowing`
(`false`). Optional: `margin_model`, `latency_model`, `default_leverage`,
`leverages`, `base_currency`.

### Data

`BacktestDataConfig`, `bon` builder, one per data stream; ours are one per
instrument for its quotes, and no bars:

```rust
let quotes = BacktestDataConfig::builder()          // per instrument
    .data_type(NautilusDataType::QuoteTick)
    .catalog_path(catalog_path.clone())
    .instrument_id(instrument_id)
    .build()?;
```

- The quotes feed both the venue (fills) and the data engine, which
  aggregates them into the internal bars the strategy subscribed to.
- The node queries the catalog over the engine's window only (the
  intersection with `start_time`, `end_time` if set): quotes before it are
  not loaded. Indicators warm up from the strategy's request, not the
  replay.
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

The request window is clamped to simulated now, which is the engine start
(`start − 1 ns`) when `on_start` runs, so warmup data must predate the run
start; later bars are built from the replay. The warmup asks for quotes
(`strategy::warmup`), which the data engine aggregates into the
strategy's bars before answering. What the strategy receives:

- **Catalog covers the window** (the stored intervals of the instrument's
  quotes span `[start − 1 ns − warmup, start − 1 ns]`): one
  `on_historical_quotes` per instrument before the first `on_bar`, the
  warmup bars in the cache, the partial oldest one dropped. The bar
  spanning the warmup end and the run start comes once, complete, in
  `on_bar`.
- **Catalog not registered, or a gap in the window**: the data engine asks
  the venue's backtest data client for the missing part, which answers
  nothing (seen with bar requests, before quotes: no response ever came,
  not even with the stored part). The run would go on with cold
  indicators.

So the catalog must cover the whole warmup window with every instrument's
quotes.

## Coverage check

Before building the node, `run` checks the catalog (`coverage.rs`) and
fails on the first miss:

- every instrument is stored, else the error names the instrument id;
- every instrument's quotes are covered over `[start − 1 ns − warmup,
  end]`, the warmup and the run, else the error names the instrument
  (`the quotes of SXR8.IBIS`) and the first missing range.

Quotes not stored miss the whole range. Coverage is the stored intervals
of the instrument's `QuoteTick`s (`get_intervals`,
inclusive nanoseconds; `fill_catalog` records each fill over the window
asked for), with gaps computed by Nautilus's `missing_intervals`, the
function behind the data engine's own catalog lookup. Adjacent intervals
(`end + 1 == start`, as a resumed fill writes them) and overlapping ones
cover as one; a 1 ns gap is a gap. An interval ending exactly at `end`
covers it. The ranges start at the engine start (see The run config) and
are checked whole: the warmup request asks for `[start − 1 ns − warmup,
start − 1 ns]`, the replay reads the engine's window. Coverage is of
ranges, not of single hours: an hour without a quote inside a covered range
passes (see Fills).

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

PnL is after commissions: each position's realized PnL subtracts the
commission of its fills, the still open position's included. `PnL (total)`
is how much the account balance changed; it counts realized PnL only. The
result does not show the costs; the report adds them (see Costs in the
report below).

`total_positions` also counts the positions a flip closed (netting keeps
one position per instrument and snapshots the closed one).
`positions.total` in the summary counts only the positions in the cache:
one per instrument when netting.

`Max Drawdown`, `CAGR (252 days)`, `Calmar Ratio (252 days)` are not in the
default set. Each is a `PortfolioStatistic` (`nautilus_analysis`)
computed from `returns_series`; a `PortfolioAnalyzer` only loops over the
statistics registered, so `report.rs` calls the one it prints directly:

```rust
let max_drawdown = MaxDrawdown::new().calculate_from_returns(&result.returns_series); // Some(-0.05): 5%
```

The engine also logs its statistics at the end of the run.

### Costs in the report

The result has no costs, so `run` returns an `Outcome`: the result plus
the costs of the fills. `report.rs` works them out after the run, from
two inputs:

- **Fills.** Every `OrderFilled` event of the orders in the engine's cache
  (`cache.orders(..)`, then `order.events()`). From each it takes the
  price (`last_px`), the quantity (`last_qty`), the price's currency and
  the commission. Nautilus documents reading the cache after the run with
  `dispose_on_completion` off (Concepts › Reports; the v2 migration notes
  on post-run inspection).
- **Quotes.** After the run the engine no longer has the replayed quotes:
  the node clears its data, and the cache keeps only the last
  `tick_capacity` quotes. So `node.rs` loads them again with
  `BacktestNode::load_data_config(data, start, end)`, the same loader and
  window the run used. Each fill gets the last quote of its instrument
  stamped at or before it: the quote it filled on (see Fills).

Per currency, `report` prints a `Costs` section after the PnL statistics:

| Figure | |
|---|---|
| `Commission` | the sum of the fills' commissions |
| `Spread` | for each fill, the distance between its price and the quote's mid, times its quantity (and the instrument's multiplier): the half spread paid, in the price's currency |
| `Cost per trade` | (commission + spread) / number of fills |
| `Gross PnL` | `PnL (total)` + commission + spread: the PnL before costs |

`PnL (total)` counts realized PnL only. A position still open at the end
has paid the half spread to open it, but that loss is not realized yet. The
gross PnL adds it back anyway, so it is too high by that amount: the
open position's size times half the spread.

An inverse instrument is refused: its value is not linear in the price.

## The command

```
helix-trader backtest ema --config ema.toml --start 2026-01-01 --end 2026-10-01 \
  [--catalog catalog] (--commission "3 EUR" | --commission-rate 0.0005) \
  --balance "10000 EUR"
```

`main` loads and validates the strategy's config, builds the `Backtest`
from its data requirements (instruments and warmup), runs the strategy and
prints `report`, in this order:

1. `Orders` and `Positions`;
2. the PnL statistics, one section per currency;
3. the costs, one section per currency;
4. the return statistics, with `Max Drawdown`;
5. the general statistics;
6. the final balances (`account.*.balance.*.total` of the summary).

Names are sorted, values printed to six decimals.

`--balance` and `--commission` are Nautilus `Money` (`"<amount>
<currency>"`), parsed with `Money::from_str`; `--commission-rate` is a
`Decimal` (0.0005 is 0.05%). `--balance` is required, and exactly one of
`--commission` and `--commission-rate`. Each commission flag is turned
into the fee model at argument parsing, so a malformed or negative one
(`--commission="-3 EUR"`, `--commission-rate=-0.0005`) fails there. The
engine log goes to stdout before the results, configured by
`NAUTILUS_LOG`.

On the wave of the tests (EMA 5/15 on 1-HOUR-MID internal bars, 10
shares, warmup 48h, Xetra's session, 3 EUR commission) it prints
`Orders: 4`, `PnL (total): -246.000000` (the wave's -234.00 without
commission, less 4 × 3 EUR) and a final balance of `9754.00 EUR`.

## Costs

`costs.rs` builds the two models the venue config takes
(`nautilus_execution::models`): the fee model, from the command's
commission flag, one of

```rust
let fee = FeeModelAny::Fixed(FixedFeeModel::new(commission, Some(true))?); // commission_per_order
let fee = FeeModelAny::MakerTaker(MakerTakerFeeModel::new(rate, rate));    // commission_rate
```

and the fill model:

```rust
let fill = FillModelAny::Default(DefaultFillModel::new(1.0, 0.0, Some(FILL_SEED))?);
```

### Fees

| Variant | Constructor | Charges |
|---|---|---|
| `Fixed` | `FixedFeeModel::new(Money, Option<bool>)` | flat per order, on its first fill only by default (`Some(true)`); `Err` if negative. Ours |
| `PerContract` | `PerContractFeeModel::new(Money)` | per contract; `Err` if negative |
| `MakerTaker` | `MakerTakerFeeModel::new(maker, taker)`, `::zero()` | notional rate by liquidity side, on every fill; a negative rate is a rebate. Ours, the same rate on both sides (IB does not distinguish), negative rejected |

The other variants are for options. A fixed fee matches IB Fixed pricing on
Xetra below EUR 6,000 an order, where the EUR 3 minimum always applies.
Above it the fee understates IB's: a strategy that flips from short to long
sends twice its trade size, so EMA with `trade_size = 5` on SXR8 (EUR
590-745) sends orders of EUR 5,900-7,300 (see "Verified on IB data").
There a rate of 0.0005 (IB's 0.05%) matches instead. A rate has no
minimum: IB's EUR 3 an order cannot be expressed on a `BacktestNode` until
Nautilus adds one
([nautilus_trader#5398](https://github.com/nautechsystems/nautilus_trader/issues/5398)),
so a rate understates IB's fee on orders below EUR 6,000. Pick
`--commission "3 EUR"` or `--commission-rate 0.0005` according to which
side of EUR 6,000 the strategy's orders fall.

### Fills

The venue fills on quotes. With `bar_execution` off, the instruments'
`QuoteTick`s alone set the L1 book. A market buy fills at the ask, a sell
at the bid, of the last quote replayed before the order: the catalog holds
a quote a minute, stamped 1 ns before the minute's close (data README), so
an order sent in `on_bar` fills, in full and stamped at the bar's close, on
the quote of the bar's last minute, the one that closed the bar. The fill
model is `Default` with `(1.0, 0.0, FILL_SEED)`: nothing on top of the
quote, so the spread is the cost, and no slippage constant to guess.
Verified in "Fills on quotes" below, and by the integration tests.

Known limits:

- **Resting orders** (stop, limit) are checked once per quote, every
  minute, never against the bars: a stop fills on the first minute quote
  crossing it (`backtest_stop.rs`), but one the price crosses and leaves
  within a minute fills only if a minute's closing quote crosses it.
- **Missing quotes.** A minute without a quote fills on the last earlier
  one, silently, stale by the gap (overnight, at the open); with no quote
  yet in the run the order is rejected (`No market`). The coverage check
  sees ranges, not single minutes.
- **Nominal sizes.** Quote sizes are stated, not observed (IB gives none):
  no depth, so large orders on illiquid instruments look too easy.

Every fill model is built from `(prob_fill_on_limit: f64, prob_slippage: f64,
random_seed: Option<u64>)`, probabilities in `[0, 1]`. Always pass a seed.

| `FillModelAny` variant | Simulates |
|---|---|
| `Default` | the matching engine's own logic. Ours |
| `BestPrice` | everything at the best price, unlimited size |
| `OneTickSlippage` | every fill one tick worse than the best price |
| `Probabilistic` | 50/50 per fill: best price or one tick worse |
| `TwoTier` | first 10 contracts at best, the rest one tick worse |
| `ThreeTier` | 50 at best, 30 at +1 tick, 20 at +2 ticks |
| `LimitOrderPartialFill` | at most 5 contracts at best, the rest one tick worse |
| `SizeAware` | small orders at best, larger ones partly one tick worse |
| `CompetitionAware` | liquidity scaled by a `liquidity_factor` |
| `VolumeSensitive`, `MarketHours` | depend on setters the node never calls: avoid |

With `prob_fill_on_limit = 1.0` and `prob_slippage = 0.0` the seed draws
nothing that changes a result.

### Fills on BID and ASK bars

Can the matching engine charge the real spread from IB's BID and ASK
bars? Tested on 2026-10-08 with a throwaway probe (market buy on one
decision bar, sell on a later one, six orders over 2026-09-01 to 09-03,
`OrderFilled` prices recorded) over a scratch catalog of SXR8 1-HOUR
LAST, BID, ASK and MID bars (see the data README). Answer: **not with
IB's bars as stored**.

How the engine picks the bars it executes on
(`execution/src/matching_engine/mod.rs`, `process_bar`, around line
1856):

- Per instrument it keeps one execution bar type
  (`execution_bar_types`): the first one seen. A bar of another type
  replaces it if its step (`execution_bar_deltas`) is not longer, else
  it is ignored. LAST, BID, ASK and MID of one cadence therefore all
  execute.
- LAST and MID bars become trade ticks (`process_trade_ticks_from_bar`);
  on an L1 book a trade tick sets bid and ask to the trade price. BID
  and ASK bars are held until both of the same `ts_init` arrived, then
  become quote ticks (`process_quote_ticks_from_bar`).
- Tick sizes come from the bar volume (`BarTickSizes::from_volume`). A
  zero-volume quote clears the book (`process_bar_quote_tick`); a
  zero-volume trade only sets the last price. IB's BID, ASK and MID bars
  all have volume 0, so they never set a bid or an ask.
- The backtest engine (`backtest/src/engine.rs`, the replay loop) gives
  each item to the venue, then to the data engine (the strategy's
  `on_bar`), then settles the venue: an order sent in `on_bar` fills
  before the next item, even one with the same timestamp. Equal
  timestamps replay in the order the streams were added
  (`BacktestEngine::add_data`: the first stream wins ties).

What the probe got (decision at 2026-09-02 09:00 UTC: LAST close 709.54,
bid/ask close 709.44/709.48, previous hour 710.22/710.29):

| Replayed (tie order), decision bars | Fills |
|---|---|
| LAST, BID, ASK as stored (`Backtest`, or the engine in either order), LAST | the LAST close (± one tick with `OneTickSlippage`): 709.54 |
| the same with BID and ASK volume set non-zero, LAST | the LAST close: LAST trade ticks override the quotes |
| BID, ASK as stored, BID or MID | none: `No market for SXR8.IBIS` |
| BID, ASK with volume, decision on the first of them | the **previous** hour's ask/bid: 710.29 |
| ASK, BID with volume, decision on BID | this hour's ask/bid close: 709.48 |
| BID, ASK with volume, then MID as stored, decision on MID | this hour's ask/bid close: 709.48 |
| MID as stored first, then BID, ASK with volume | the previous hour's ask/bid: 710.29 |

All six fills of each run followed the row. The strategy received its
own bar type only, every bar in order, whatever else was replayed.

So fills on IB's bars as such would need LAST out of the replay, a
volume put on the BID and ASK bars, the strategy deciding on MID (or
BID) bars added after them, and control of the tie order. The way that
works is to turn each BID and ASK pair into one quote tick and switch
`bar_execution` off: see "Fills on quotes", now the way the backtest
fills.

### Fills on quotes

Can orders fill on quotes while bars drive the strategy? Tested on
2026-10-08 with a throwaway probe (a scratch crate on the `nautilus-*`
crates, nothing kept). Answer: **yes**, with the venue's `bar_execution`
off and each quote stamped 1 ns before its bar's close.

Setup:

- A scratch catalog: SXR8.IBIS (price precision 4, size precision 0,
  tick 0.0001), its 1-HOUR-LAST bars, and one `QuoteTick` per BID and
  ASK bar pair of IB (see the data README) over the run window: bid =
  BID close, ask = ASK close, size 1,000,000 on both sides, `ts_event =
  ts_init =` the bars' close − 1 ns, written with `write_to_parquet`.
- The node as in `node.rs` (netting, margin, L1 book, fixed fee) but
  `.bar_execution(false)`, and a second `BacktestDataConfig` with
  `.data_type(NautilusDataType::QuoteTick)` and `.instrument_id(..)`.
  Run 2026-09-01 to 09-26, warmup 72 h.
- A probe subscribed to the LAST bars only: it asks for its warmup,
  sends 12 market orders of 10 shares on chosen bars (in and out of
  Xetra's session, one with a crossed quote, one whose quote was
  removed), and on the 2026-09-03 14:00 UTC bar a buy limit and a sell
  stop, both at 710.00, canceled at 17:00.

What the probe got, fill model `Default` (a selection; all 12 orders
followed the rule):

| Decision bar (UTC) | Side | LAST close | Bid/ask, this hour | Bid/ask, previous hour | Fill |
|---|---|---|---|---|---|
| 09-02 09:00 | buy | 709.54 | 709.44/709.48 | 710.22/710.29 | 709.48 |
| 09-02 12:00 | sell | 711.00 | 711.04/711.12 | 709.54/709.62 | 711.04 |
| 09-02 19:00 | buy | 713.36 | 712.96/713.36 | 713.24/713.64 | 713.36 |
| 09-02 21:00 | sell | 713.96 | 712.48/713.18 | 713.22/713.54 | 712.48 |
| 09-03 06:30 | buy | 712.32 | 712.58/712.28, crossed | 712.48/713.18 | 712.28 |
| 09-04 10:00 | buy | 719.38 | removed | 719.16/719.18 | 719.18 |
| 09-18 15:00 | buy | 717.56 | 717.91/718.00 | 717.92/717.95 | 718.00 |

Every market order filled in full, in one fill stamped at the decision
bar's close, at that hour's ask (buy) or bid (sell), even where the LAST
close lies outside the quote (2,328 of the year's 4,078 hours); the
hour without a quote filled on the previous one. The same orders in
variants:

| Variant | Market orders fill at |
|---|---|
| as above, bars data config first | this hour's quote |
| quotes data config first | this hour's quote |
| quotes stamped at the close, bars first | the **previous** hour's quote |
| quotes stamped at the close, quotes first | this hour's quote |
| `BestPrice` fill model | the same prices as `Default` |
| quote size 5 | 5 at the quote, 5 one tick worse |
| quote prices at precision 2 | nothing: every quote skipped (`Invalid bid_price precision 2, expected 4`, an `ERROR` after 20), every order rejected `No market` |
| `bar_execution` on | the LAST close (two orders only partly, the rest one tick worse); the limit and the stop filled at 710.00 |
| run `start` on a bar's close | that bar: rejected `No market`; its quote (start − 1 ns) precedes the run and is skipped, even when stored. Starting 1 ns earlier fills it |

- **Bars.** In every variant `on_bar` received all 302 LAST bars of the
  window, in order, with their closes.
  With `bar_execution` off `process_bar` returns at once: bars never
  touch the book.
- **The 1 ns stamp.** The quote sorts strictly before its bar, so the
  venue holds the hour's closing quote when the strategy decides on the
  bar, whatever order the data configs are in. Stamped at the close it
  ties with the bar and the tie goes to the config added first (see
  above): bars first means every fill one hour stale. The close quote is
  the last of its hour, so 1 ns earlier adds no look-ahead.
- **Fill model.** `Default` with `(1.0, 0.0, seed)` adds nothing to the
  quote. `BestPrice` gave the same market fills, but ignores the quote
  size and fills limit orders at or inside the spread at their own price
  (`fill_limit_inside_spread`): `Default` it is.
- **Size.** `liquidity_consumption` is off by default: every order sees
  the full quote size, nothing is used up between orders. A market order
  larger than the size is not refused: the rest fills one tick worse
  (`apply_fills`, exhausted L1 volume). At 1,000,000 shares the size
  never limits an order.
- **Warmup.** The warmup request is answered from the catalog's bars as
  before (16 bars before the first `on_bar`), with quotes stored only
  from the start or also before it: warmup needs no quotes.
- **Crossed quotes** (ask < bid) are taken as they are (a warning in
  debug builds only): a buy fills at the ask, below the bid. In the year,
  178 of 4,087 pairs are crossed: 176 on the bars closing 08:30 and 09:00
  Frankfurt time (pre-opening), 2 at 14:00 on 24 and 31 December (half
  days, after the close; 659.40/628.53 on the 24th). 6 are locked (ask =
  bid).
- **Missing quotes.** An hour without a quote fills on the last earlier
  one, silently; with no quote yet the order is rejected `No market`.
  In the year every LAST bar has its BID and ASK pair (9 pairs have no
  LAST bar: first bars of the day).

Applied (see Fills, The run config):

- Venue as in `node.rs` with `.bar_execution(false)`;
  `liquidity_consumption` left off; the quotes of every instrument as a
  `QuoteTick` data config next to the bars.
- Fill model `FillModelAny::Default(DefaultFillModel::new(1.0, 0.0,
  Some(seed)))`.
- A run `start` on a bar's close left that bar without its quote (and the
  warmup response also held the bar at `start`, which the replay delivered
  again): the engine starts 1 ns earlier.

For the data side (see the data README):

- Quotes: bid and ask at the instrument's price precision (IB's bars
  already are), size 1,000,000 at its size precision, `ts_event = ts_init
  =` the bars' close − 1 ns.
- Crossed pairs dropped by the conversion, like a pair missing a side:
  the hour then fills on the previous quote rather than on an impossible
  price.

The probe's resting orders show the first known limit (see Fills): the
buy limit and the sell stop at 710.00 were not filled although the next
hour's LAST low was 702.01, its closing quote 715.50/715.54; with
`bar_execution` on both filled at 710.00.

## Verified on IB data

Paper gateway, 2026-10-08, SXR8.IBIS 1-HOUR-LAST, EMA 10/30 (the
defaults), `trade_size = 5`, `warmup = "720h"`, before fills on quotes
(bar execution, one tick of slippage):

```
helix-trader fill-catalog ema --config sxr8.toml --catalog sxr8-cat \
  --start 2025-10-01 --end 2026-10-03
helix-trader backtest ema --config sxr8.toml --catalog sxr8-cat \
  --start 2026-01-01 --end 2026-10-01 --commission "3 EUR" --balance "10000 EUR"
```

It prints `Orders: 113`, `PnL (total): -525.70`, a final balance of
`9474.30 EUR`, no `NaN`. Checked against the stored bars, outside Nautilus:

- **Trade count.** EMA 10/30 recomputed on the closes (SMA seed, as
  Nautilus' EMA) over the 295 warmup bars of `[2025-12-02, 2026-01-01]`
  then the 3,047 replayed bars gives 113 orders: the first on the first
  replayed bar (warm from the start), then one per crossing. All 113 match
  the fills in bar, side and size (5, then 10 per flip).
- **Commission.** The same run at `"0 EUR"` has the same fills and ends at
  `9813.30 EUR` (PnL -186.70): the difference is 339.00 = 113 × 3 EUR.
  IB Fixed would have charged 377.32: 112 of the 113 orders are flips of
  10 shares, above EUR 6,000 (see Fees).
- **Fills.** All 113 (`OrderFilled` with `NAUTILUS_LOG=stdout=Info`) are
  at their bar's close plus (buy) or minus (sell) `0.0001`, the
  instrument's tick, stamped at the bar's `ts_event`.
- **Session.** 58 of the 113 orders were sent on bars that close outside
  Xetra's continuous trading (09:00-17:30 Frankfurt time): 9 on the first
  bar of the day (a half-hour bar, see the data README), 9 on the
  pre-opening hour, 40 after the close, 33 of them on the 19:00-23:00
  bars (median volume 154 shares a bar, 12 under 100, one with 0). Bars
  in continuous trading had a median 6,429. Those fills are at prices with
  little or no volume behind them, which a live order on Xetra might not
  get; the thin bars also move the EMAs. The results above are therefore
  optimistic about fills.
- `Long Ratio: 0.500000` with 57 long of 113 positions: Nautilus rounds it
  to two decimals.

On internal bars, 2026-10-20: the catalog of SXR8.IBIS 1-minute quotes
filled from the paper gateway (2026-08-24 to 10-01, see the data README),
EMA 5/15 on `1-HOUR-MID` bars, `trade_size = 10`, `warmup = "168h"`,
Xetra's session, September 2026, 3 EUR an order. The warmup request asked
for the quotes from 2026-08-24 23:59:59.999999999 with the three params of
`strategy::warmup`; the first order went on the run's first bar inside the
session (09-01 08:00 UTC), so the EMAs were warm from the start. It prints
`Orders: 16`, `PnL (total): -30.000000`, a final balance of
`9970.00 EUR`, no error. The same month on the stored 1-HOUR-LAST bars
gave 14 orders and `9935.60 EUR`: the mid of the closing quote is not the
last trade.

With costs and the session, 2026-10-21: the same window again, now with
the commission, fills on quotes and Xetra's session. The catalog of
SXR8.IBIS 1-minute quotes was filled from the paper gateway over the
warmup and the window (194,609 quotes, 973 crossed pairs and 8 one-sided
bars dropped, 21 minutes), EMA 10/30 on `1-HOUR-MID` bars,
`trade_size = 5`, `warmup = "720h"`, Xetra's session (Europe/Berlin
09:00-17:30):

```toml
instrument_ids = ["SXR8.IBIS"]
bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
trade_size = "5"
warmup = "720h"
session = { time_zone = "Europe/Berlin", open = "09:00", close = "17:30" }
```

```
helix-trader fill-catalog ema --config sxr8.toml --catalog sxr8-cat \
  --start 2025-12-01 --end 2026-10-01
helix-trader backtest ema --config sxr8.toml --catalog sxr8-cat \
  --start 2026-01-01 --end 2026-10-01 --commission-rate 0.0005 --balance "10000 EUR"
```

| | Before (LAST bars, 3 EUR) | `--commission-rate 0.0005` | `--commission "3 EUR"` |
|---|---|---|---|
| Orders (filled) | 113 (113) | 57 (57) | 57 (57) |
| PnL (total) | -525.70 | -223.40 | -203.70 |
| Commission | 339.00 | 190.70 | 171.00 |
| Spread | about 0 (one tick, 0.0001) | 16.05 | 16.05 |
| Cost per trade | 3.00 | 3.63 | 3.28 |
| Gross PnL | -186.70 | -16.65 | -16.65 |
| Final balance | 9474.30 EUR | 9776.60 EUR | 9796.30 EUR |

Both new runs have the same fills; only the commission differs. Before,
the strategy traded every bar of the day, in the session or not, on LAST
closes. Now it trades in the session only, on the mid of the closing
quote, and pays the spread. Checked against the stored quotes, outside
Nautilus:

- **Trade count.** EMA 10/30 recomputed by hand:
  1. Take the mid of the last quote of each hour.
  2. Keep only the hours that have a quote and close inside the session.
  3. Seed each EMA with the average of its first 10 or 30 bars, as
     Nautilus does.
  4. Warm up from 2025-12-01 23:59:59.999999999.

  This gives the same 57 orders, all filled, none rejected. They match
  the fills in hour, side and size: one of 5 shares, then 56 flips of 10.
- **Session.** Every fill is on a bar closing 10:00 to 17:00 Frankfurt
  time, Monday to Friday. No fill on the weekdays the exchange was closed
  (no quote all day): 1 January, 3 and 6 April, 1 May.
- **Fills.** All 57 at the ask (buy) or the bid (sell) of the quote
  stamped 1 ns before the decision bar's close. Half the spread a fill
  paid: median 0.32 bps, at most 1.27.
- **Commission.** Each fill's commission is 0.05% of its value to the cent
  (`--commission-rate`), or 3.00 EUR (`--commission`). Values run from EUR
  3,146 (the first order, 5 shares) to 7,306. Only that first order is
  below EUR 6,000, where IB charges its EUR 3 minimum instead of 1.57: the
  rate understates IB by 1.43 on it. IB Fixed would charge 192.11 in all,
  1.41 more than the rate run and 21.11 more than the flat 3 EUR.
- **Report.** Spread recomputed on the fills: 16.05 EUR, as reported.
  Gross PnL = PnL (total) + commission + spread, in both runs.

**Weekends and holidays, found and fixed.** The first run of this check
traded on days the exchange was closed. The session checks the time of
day only, and Nautilus built a bar for every hour, flat at the last close
when no quote came. So the 10:00-17:00 bars of Saturdays, Sundays and
holidays counted as session bars. With the rate, that run sent 86 orders:
78 filled, 5 of them on Saturdays at Friday's last quote (11 to 18 hours
old), and 8 rejected on New Year's Day. It ended at 9364.42 EUR (PnL
-635.58, commission 256.08, spread 35.15). Now an hour without any quote
builds no bar, in the backtest and live alike (see the strategy README,
"Hours without quotes"): 57 orders, the figures above. The September run
on internal bars above had one such fill too (Saturday 5 September, on a
quote 13 hours old). Re-run on this catalog with the fix: 16 orders, none
on a weekend, PnL -111.80, a final balance of 9888.20 EUR instead of
9970.00.

## Rules

- **The plain Nautilus API.** A run is a `BacktestRunConfig` on a
  `BacktestNode`; no engine wrapper.
- **One run per process**, no parameter sweeps.
- **Costs always explicit.** A fee model always, even a zero one.
- **Warmup before the start.** Warmup quotes must predate the run start,
  and the catalog must cover the whole warmup window: `run` refuses
  otherwise.
- **Seed all randomness**: the fill model and the strategy config.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file, for
  pure code: the run config from our inputs, the cost models (a negative
  commission or rate is rejected, a fixed commission is charged on the first
  fill only, a rate on the value of every fill, maker and taker alike),
  the coverage check (gaps on the intervals alone, then on a temporary
  catalog: missing instrument, quotes 1 ns short at either end, missing,
  or with a gap), the report of a hand-built `Outcome` (sections, sorted
  names, max drawdown, the costs and the gross PnL). The command line is
  tested in `main.rs`: required flags, exactly one commission flag,
  malformed `Money`, the window check.
- Integration tests in `tests/`, one node per file, run the real EMA
  strategy through `Backtest::run` on a fixture catalog (`tests/common`:
  SXR8.IBIS in EUR and its 1-minute quotes only, bid and ask at the mid
  ± 0.05 EUR, stamped 1 ns before each minute's close, written with
  `ParquetDataCatalog::write_to_parquet`). The strategy trades on
  1-HOUR-MID internal bars. Two kinds of quotes:
  - a wave over days, on which the EMA 5/15 crosses several times:
    `backtest_ema_cross.rs` checks that its PnL is lower by `orders ×
    commission` than in `backtest_ema_cross_free.rs`, and its final
    balance;
  - steps, a level per hour chosen so that the shortest crossover (1/2)
    trades at hours worked out by hand, and so that what is tested changes
    the orders and the final balance: `backtest_warmup.rs` (a run off an
    hour: warmup from the cache, the partial oldest bar dropped, the first
    bar of the run traded, buys at the ask and sells at the bid of the quote
    1 ns before the bar's close), `backtest_start_on_close.rs` (the same on
    a run starting exactly on an hour's close: the one-quote oldest bar
    dropped, the bar at the start traded on its closing quote),
    `backtest_session.rs` (bars outside the session neither move the EMAs
    nor trade; overnight, without quotes, there are no bars),
    `backtest_commission_rate.rs` (the orders of a run without commission,
    each fill charged the rate on its value, and the report's costs: the
    half spread of each fill, the gross PnL).
  - `backtest_coverage.rs`: a warmup longer than the stored quotes, or a
    catalog without quotes, is refused before the node is built, naming
    the instrument's quotes and the missing interval.
  - `backtest_inputs.rs`: a negative warmup, or a start on or after the
    end, is refused before the node is built.

  The EMA reads bar closes only, so the bars' opens, highs and lows, and
  stops, are tested with a recording strategy (`tests/common/recorder.rs`)
  that warms up through `strategy::warmup` like a real one, records every
  bar it gets and can rest a sell stop:
  - `backtest_bars.rs` and `backtest_bars_on_close.rs` (noise, a mid of
    its own every minute; runs from 12:30 and from 13:00): every warmup and
    replayed bar has the open, high, low and close worked out from its
    hour's minute quotes, the partial oldest bar left out, and the bar
    spanning the warmup end comes once, complete, as the first replayed
    bar. With `update_subscriptions` off both fail: that bar comes out
    holding only the replayed half hour, or one quote on a run starting on
    its close.
  - `backtest_stop.rs` (a fall of 0.50 a minute): a sell stop rested on
    the 13:00 bar fills at the bid of the first minute quote at or below
    it, 13:39:59.999999999, not at the next bar.
  - `backtest_quote_gap.rs` (a mid of its own every minute, but no quote
    in one hour of the warmup and one of the replay): neither hour yields
    a bar; every other bar is as worked out from its hour's quotes.
