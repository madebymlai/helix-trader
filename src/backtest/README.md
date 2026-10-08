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

On the fixture catalog of the tests (EMA 5/15, 10 shares, warmup 48h,
Xetra's session, 3 EUR commission) it prints `Orders: 4`,
`PnL (total): -243.600000` (the fixture's -231.60 without commission, less
4 × 3 EUR) and a final balance of `9756.40 EUR`.

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
Above it the fee understates IB's: a strategy that flips from short to long
sends twice its trade size, so EMA with `trade_size = 5` on SXR8 (EUR
590-745) sends orders of EUR 5,900-7,300 (see "Verified on IB data").

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
The tick is the instrument's `price_increment`, from IB's minimum tick:
`0.0001` for SXR8.IBIS (price precision 4) although its trades print in
whole cents, so on SXR8 one tick of slippage costs next to nothing.

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
`bar_execution` off: see "Fills on quotes". A slippage in basis points
of the price (0.5 bps for SXR8, the median half spread in continuous
trading rounded up, data README) stays only as the fallback.

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

Recommended:

- Venue as in `node.rs` with `.bar_execution(false)`;
  `liquidity_consumption` left off; the quotes of every instrument as a
  `QuoteTick` data config next to the bars.
- Fill model `FillModelAny::Default(DefaultFillModel::new(1.0, 0.0,
  Some(seed)))`.
- Quotes: bid and ask at the instrument's price precision (IB's bars
  already are), size 1,000,000 at its size precision, `ts_event = ts_init
  =` the bars' close − 1 ns.
- Crossed pairs dropped by the conversion, like a pair missing a side:
  the hour then fills on the previous quote rather than on an impossible
  price.
- A run `start` on a bar's close leaves that bar without its quote:
  start the run 1 ns earlier, or accept that an order on it is rejected.
  (At such a `start` the warmup response also holds the bar at `start`,
  which the replay delivers again.)

Known limits:

- **Resting orders are checked only against quotes**, once an hour. The
  buy limit and the sell stop at 710.00 were not filled although the
  next hour's LAST low was 702.01, its closing quote 715.50/715.54; with
  `bar_execution` on both filled at 710.00.
- A missing quote leaves the previous one in force, up to an hour or
  more stale.
- Quote sizes are nominal: no depth, so large orders on illiquid
  instruments look too easy.

## Verified on IB data

Paper gateway, 2026-10-08, SXR8.IBIS 1-HOUR-LAST, EMA 10/30 (the
defaults), `trade_size = 5`, `warmup = "720h"`:

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
