# Backtesting

The backtest module runs one strategy over the catalog, with costs (build
step 2). It describes the run as a Nautilus `BacktestRunConfig`, runs it on
a `BacktestNode`, and adds nothing the node already does. One run per
process, one node per process, Nautilus owns the logger, and the pinned
Nautilus commit: `docs/ARCHITECTURE.md` ("Nautilus v2 constraints",
"Version"). The Nautilus APIs below were checked against that commit's
sources; *unverified* marks what the sources did not confirm.

## Layout

```
backtest/
  mod.rs      Backtest: the one public type
  node.rs     the BacktestRunConfig and the BacktestNode run
  coverage.rs the check that the catalog can serve the run
  costs.rs    fee and fill models of the simulated venue
  report.rs   Outcome, the result and the costs; report(&Outcome) -> String
```

`nautilus-backtest` needs its `streaming` feature, which enables the `node`
module and catalog registration.

## The command

```
helix-trader backtest ema --config ema.toml --start 2026-01-01 --end 2026-10-01 \
  [--catalog catalog] (--commission "3 EUR" | --commission-rate 0.0005) \
  --balance "10000 EUR" [--leverage 10]
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

Names are sorted, values printed to six decimals. The engine log goes to
stdout before the results, configured by `NAUTILUS_LOG`.

`--balance` (required) and `--commission` are Nautilus `Money`
(`"<amount> <currency>"`, `Money::from_str`); `--commission-rate` is a
`Decimal` (0.0005 is 0.05%); exactly one commission flag is required. The
commission flag becomes the fee model at argument parsing, so a malformed
or negative one (`--commission="-3 EUR"`, `--commission-rate=-0.0005`)
fails there. `--leverage` is a `Decimal`, 1 (no leverage) if not given;
Nautilus refuses one that is not positive before the run.

On the time exit of the tests (`common::trades`, `TIME_EXIT`: EMA 1/2 on
1-HOUR-MID internal bars, 10 shares, stop loss and take profit 100 bps,
held an hour, warmup 3h, Xetra's session, 3 EUR commission) it prints
`Orders: 4` (the entry, its two legs, the close), `PnL (total):
33.000000` (39.00 without commission, less 2 filled orders × 3 EUR) and a
final balance of `10033.00 EUR` (`backtest_commission_per_order.rs`).

## The entry point

`Backtest` (`mod.rs`) holds Nautilus types only, documented field by field
in the code; `commission_per_order(Money)` and `commission_rate(Decimal)`
build its fee model (see Costs).

```rust
impl Backtest {
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<Outcome>
    where S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static;
}
```

`Outcome` is Nautilus' `BacktestResult` (public `result`) plus the costs of
the fills, private: `report` prints them (see Costs in the report).

`run` fails before building the node when there are no instruments, the
warmup is negative, `start` is not before
`end`, the leverage is not positive (Nautilus' check), the cache cannot
hold the warmup (`strategy/README.md`, "Warmup and training"), or the
catalog cannot serve the run (see Coverage check).

A relative catalog path is made absolute: with a relative one, the node's
DataFusion reader finds no object store for `file://catalog/...`.

The run replays the series the strategy's bars are built from
(`strategy::catalog_series`, `data/README.md`): the source bars of a
composite bar type (`...-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` replays
`...-1-MINUTE-LAST-EXTERNAL`, IB's TRADES bars or OKX's candles), or the
quotes of their instruments. The data engine builds the strategy's
internal bars from them, and orders fill on them (see Fills). It also
replays the book depth of each instrument whose catalog stores one (see
Book data) and the funding of each instrument whose catalog stores funding
(`Backtest::series`; see OKX on candles), and nothing else. The
instruments may be on several venues, each simulated (see Venue).

## The run

```rust
let mut node = BacktestNode::new(vec![run_config])?;
node.build()?;                                         // engine, venue, instruments
node.get_engine_mut(RUN_ID)
    .context("no engine for the run id")?
    .add_strategy(strategy)?;                          // strategies are not config
let results: Vec<BacktestResult> = node.run()?;       // loads the data, runs
let engine = node.get_engine(RUN_ID).context("no engine for the run")?; // kept: dispose_on_completion(false)
// the fills from engine.kernel().cache, the quotes loaded again (see Results)
node.dispose();
```

`nautilus_backtest::node::BacktestNode`:

```rust
pub fn new(configs: Vec<BacktestRunConfig>) -> anyhow::Result<Self>;
pub fn build(&mut self) -> anyhow::Result<()>;
pub fn get_engine_mut(&mut self, id: &str) -> Option<&mut BacktestEngine>;
pub fn get_engine(&self, id: &str) -> Option<&BacktestEngine>;
pub fn run(&mut self) -> anyhow::Result<Vec<BacktestResult>>;
pub fn load_data_config(config: &BacktestDataConfig, start: Option<UnixNanos>,
                        end: Option<UnixNanos>) -> anyhow::Result<Vec<Data>>;
pub fn dispose(&mut self);
```

- `build` creates the engine, adds the venue and loads the instruments from
  the catalog; `run` loads the data and runs.
- The strategy goes in between, since a run config holds no strategy.
  `add_strategy` takes any type with the bound above, so `node.rs` is
  generic over the strategy and never names one.
- `new` rejects more than one run config ("kernel MessageBus is a
  thread-local singleton").
- With the default `dispose_on_completion = true` the node disposes the
  engine at the end of `run`, cache included. We set it `false`, as Nautilus
  documents for reading results after a run: `run` then only clears the
  engine's data, we read the fills from its cache and call `dispose`
  ourselves.

## The run config

`BacktestRunConfig` (`nautilus_backtest::config`), `bon` builder whose
`build()` validates and returns a `Result`:

```rust
let run_config = BacktestRunConfig::builder()
    .id(RUN_ID.to_string())       // read back by get_engine_mut
    .venues(venue_configs)        // one simulated venue per venue of the instruments
    .data(data_configs)           // what to load from the catalog
    .engine(engine_config)        // logging, warmup catalog, cache and data engine from strategy::warmup
    .start(engine_start)          // start − 1 ns (see The engine start)
    .end(end)
    .chunk_size(CHUNK_SIZE)       // 100,000: streamed, not loaded at once
    .raise_exception(true)
    .dispose_on_completion(false) // the cache outlives run
    .build()?;
```

`chunk_size` streams the data: the node merges the series from the
catalog and loads and replays `CHUNK_SIZE` (100,000) data points at a
time instead of loading them all first (Nautilus' streaming mode,
`run_streaming` in `backtest/src/node.rs`). A day of CME gold's book is
7.4 million snapshots; loaded at once it went past 16 GB and the run was
killed. The order of the replay is the same: the suite's results did not
change. With the default `raise_exception = false`, a build or run error is
only logged, the run is skipped and `node.run()` returns `Ok` with no
result; we set it `true` so the error is returned.

**The engine start.** The engine starts 1 ns before `start`
(`Backtest::engine_start`). It skips data before its start, and in a
catalog of quotes (the test fixtures; `fill-catalog` stored IB's that way
until 2026-10-26) a minute's quote is stamped 1 ns before the minute's
close: a run starting exactly on
a bar's close would otherwise skip the quote closing that bar and reject an
order sent on the bar (`No market`). The strategy's clock, and so its
warmup request, starts there too: the warmup covers `[start − 1 ns −
warmup, start − 1 ns]` and the coverage check follows. The quote stamped
`start − 1 ns` is in both the warmup and the replay: the bar spanning them
counts it twice in its volume, which nothing reads; its prices are right.

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

One per venue of the instruments (`BacktestRunConfig::venues` is a
list), all alike but for the name and the position mode:

| Field | Value |
|---|---|
| `name` | the venue, for example `"IBIS"` |
| `oms_type` | `Backtest::venue_oms` of the venue: the real venue's, `venue_oms_type` (see below) |
| `account_type` | `AccountType::Margin` (the EMA strategy goes short) |
| `book_type` | `BookType::L1_MBP` (`new` rejects L2 or L3 books without book data) |
| `starting_balances: Vec<String>` | the balance, `Money::to_string()`, for example `"10000.00 EUR"`, the same at every venue |
| `default_leverage: Option<Decimal>` | `--leverage` (see Leverage) |
| `fee_model: Option<FeeModelAny>` | required: the node's `build` fails without it (the builder's `build()` does not), so a missing fee never becomes zero |
| `fill_model: Option<FillModelAny>` | `Default`, no slippage (see Fills) |
| `bar_execution` | Nautilus' default, `true`: only `EXTERNAL` bars drive fills, which are IB's minute TRADES bars and OKX's minute candles |
| `bar_adaptive_high_low_ordering` | Nautilus' default, `false`: a bar replays open, high, low, close (see Fills on candles) |

The simulated venue keeps positions as the real one does, so that a
strategy, its `oms_type` unset, trades as it does live (strategy README,
"Position mode: the venue's"). `venue_oms_type` declares each venue's mode
in one place, as Nautilus' execution clients declare it live: `Hedging`
for the venues in `HEDGING_VENUES` (`ICMARKETS`, IC's hedging cTrader
account, `live/ic/README.md`, "Hedging positions"), `Netting` for every
other, IB's exchanges (Nautilus' IB
execution client nets) and OKX (its client nets derivatives, in the net
position mode `live` requires). `main` passes `venue_oms_type` as
`Backtest::venue_oms`, which tests replace to run one venue both ways.
With an unset strategy OMS, Nautilus takes the venue's: two
overlapping buys make one position at their average on a netting venue,
two positions on a hedging one (`backtest_netting_venue.rs`,
`backtest_hedging_venue.rs`).

**Several venues.** A strategy reading one venue's data and trading on
another (CME gold's book on `GLBX`, IC's gold on `ICMARKETS`) gets a
simulated venue for each, each keeping positions as it does
(`GLBX` netting, `ICMARKETS` hedging), each account starting with the
run's one balance at the run's one leverage and fee model: enough while a
strategy trades on one venue, the other's account left untouched. The
report shows each account's balances (see Results).
`backtest_two_venues.rs` pins it.

Defaults left alone: `liquidity_consumption` `false` (every order sees the
full quote or bar size), `use_reduce_only` `true`, `allow_cash_borrowing`
`false`, `liquidation_enabled` `false`. Left unset: `margin_model`
(Nautilus' default, see Leverage), `latency_model`, `leverages`,
`base_currency` (see OKX on candles).

### Leverage

Leverage comes from one source: the venue's, carried by the instrument's
margins where the venue states them, or the run's `--leverage` where it
does not. The venue gets `--leverage` as `default_leverage`, for every
instrument; unset, Nautilus uses 10 on a margin account (1 on a cash one).

- **IC**: its instruments carry IC's own margin (`data/ic/README.md`,
  "Margins": 1 / the leverage IC applies, 1:30 on the demo), so an IC
  backtest runs at `--leverage 1`, the default. Another leverage would
  divide IC's margin again (`--leverage 30` on the demo's 1/30: 1/900).
- **OKX**: its instruments carry margins of 1, and the leverage is the
  account's, set by hand per instrument at OKX: `--leverage` says which.

The margin model is Nautilus' default, `LeveragedMarginModel` (Concepts ›
Accounting, "Margin models"): an order locks notional / leverage × the
instrument's `margin_init`; an open position locks notional / leverage ×
`margin_maint`, its value at entry. OKX instruments are stored with both
margins at 1 (`data/okx/README.md`), so a position locks notional /
leverage, as on OKX. IC's carry IC's margin and lock, at 1x, what IC
locks. IB's instruments have margins of 0 and lock nothing at any leverage.

Liquidation is off, so OKX's own maintenance rate (BTC X-Perp: 0.65%),
which only decides when OKX liquidates, is not modelled: a run never
closes a position for lack of margin.

The risk engine denies an order whose initial margin exceeds the free
balance (`InitialMarginExceedsFreeBalance`, `nautilus-risk`
`engine/mod.rs` `check_margin_balance`); position-reducing orders skip the
check. So at 1x an OKX order worth more than the free balance is denied,
and leverage can change the fills of large orders.

### Data

`BacktestDataConfig`, `bon` builder, one per series, keyed as the catalog
keys it:

```rust
let quotes = BacktestDataConfig::builder()          // quotes: by instrument
    .data_type(NautilusDataType::QuoteTick)
    .catalog_path(catalog_path.clone())
    .instrument_id(instrument_id)
    .build()?;
let bars = BacktestDataConfig::builder()            // bars: by bar type
    .data_type(NautilusDataType::Bar)
    .catalog_path(catalog_path.clone())
    .bar_types(vec!["SXR8.IBIS-1-MINUTE-LAST-EXTERNAL".to_string()])
    .build()?;
```

- The series feed both the venue (fills) and the data engine, which
  aggregates them into the internal bars the strategy subscribed to.
- Set `bar_types` for bars: without it the node reads every bar type, and
  with `bar_spec` it builds `{instrument}-{spec}-EXTERNAL`
  (`BacktestDataConfig::query_identifiers`).
- The node queries the catalog over the engine's window only (intersected
  with `start_time`, `end_time` if set): quotes before it are not loaded.
  Indicators warm up from the strategy's request, not the replay.
- The node loads the catalog's instruments in `build`, so `fill_catalog`
  must have written them.

### Book data

For each instrument the catalog stores book depth snapshots of (Databento's
MBP-10, 10 levels a side, `data/databento/README.md`), the run also loads
them, keyed by the instrument id:

```rust
let book = BacktestDataConfig::builder()            // book depth: by instrument
    .data_type(NautilusDataType::OrderBookDepth)
    .catalog_path(catalog_path.clone())
    .instrument_id(InstrumentId::from("GC.v.0.GLBX"))
    .build()?;
```

No config field asks for it: the strategy lists the instrument among its
`instrument_ids`, and the catalog decides. Nautilus replays the snapshots
with the other data, in time order: to the instrument's venue, and to the
data engine, which publishes them to a strategy subscribed with
`subscribe_book_depth` (`on_book_depth`) and, the subscription managed,
keeps the cache's book (`cache.order_book`) as the last snapshot
(`backtest_book_depth.rs`). The coverage check asks the snapshots over the
window, as every series replayed.

An instrument read for its book alone has its bars left out: when none of
the series its bars are built from is stored but its book is (CME gold's
MID bars, with no quotes of gold), no bar type of it is replayed or
checked. Its warmup request, which the catalog cannot answer, goes to
the venue's backtest data client, which refuses it ("does not support
RequestQuotes"): the warmup's request group ends failed, which a strategy
calling `warmup::finished` logs as an error ("the warmup is incomplete"),
and the run goes on. An instrument with neither is refused by the
coverage check, as before.

### Engine config

`BacktestEngineConfig` (`nautilus_backtest::config`), `bon` builder and
`Default`. We set `catalogs: Vec<DataCatalogConfig>` (serves warmup
requests, see Warmup catalog), `logging: LoggerConfig`
(`Backtest::logging`), `cache` (`warmup::cache_config`) and `data_engine`
(`warmup::data_engine_config`, no bar for an hour without quotes; see
"Weekends and holidays, found and fixed").

The node's kernel installs Nautilus' logger from `logging` when it is
built. Installing one before (`init_logging`, as `fill-catalog` does)
would not fail, the kernel would reuse it, but its own config would be
ignored; so the command passes the `NAUTILUS_LOG` config through
`Backtest` and installs nothing itself.

## Warmup catalog

Warmup itself is in `strategy/README.md`, "Warmup and training". On the
backtest side the catalog is registered in
`BacktestEngineConfig::catalogs`, read-only, so that no answer is ever
written back to it (the warmup asks for write-back, which live IC
answers take):

```rust
DataCatalogConfig::new(path: String, fs_protocol: Option<String>,
                       catalog_backend: Option<CatalogBackendType>) -> DataCatalogConfig
config.with_read_only(true)    // rejects response write-back
```

The request window is clamped to simulated now, the engine start
(`start − 1 ns`) when `on_start` runs, so warmup data must predate the run
start; later bars are built from the replay. What the strategy receives:

- **Catalog covers the window** (the stored intervals of every series span
  `[start − 1 ns − warmup, start − 1 ns]`): one `on_historical_quotes`
  (`on_historical_bars` for source bars) per instrument before the first
  `on_bar`, the warmup bars in the cache, the partial oldest one dropped,
  then the warmup's request group ends complete in `on_data_response`
  (`backtest_long_warmup.rs`, `backtest_composite_warmup.rs`). The bar
  spanning the warmup end and the run start comes once, complete, in
  `on_bar`.
- **Catalog not registered, or a gap in the window**: the data engine asks
  the venue's backtest data client for the missing part, which answers
  nothing (seen with bar requests, before quotes: no response ever came,
  not even with the stored part). The run would go on with cold
  indicators, which is why the coverage check refuses it.

## Coverage check

Before building the node, `run` checks the catalog (`coverage.rs`) and
fails on the first miss: an instrument not stored (the error names its
id), or a series not covered over `[start − 1 ns − warmup, end]`, the
warmup and the run (the error names the series, such as `the quotes of
SXR8.IBIS`, `the bars of SXR8.IBIS-1-MINUTE-LAST-EXTERNAL`, `the
funding_rates of BTC-USDT-SWAP.OKX`, and the first missing range).

Coverage is the stored intervals of the series, by data type and
identifier (inclusive nanoseconds; `fill_catalog` records each fill over
the window asked for). Gaps come from the catalog's
`get_missing_intervals_for_request`, which goes through the
`missing_intervals` behind the data engine's own catalog lookup. A series
not stored misses the whole range. Adjacent intervals (`end + 1 == start`,
as a resumed fill writes them) and overlapping ones cover as one; a 1 ns
gap is a gap; an interval ending exactly at `end` covers it. The two
ranges are checked whole because the warmup request asks for all of
`[start − 1 ns − warmup, start − 1 ns]` and the replay reads the engine's
window. Coverage is of ranges, not of single minutes: a minute without a
quote inside a covered range passes (see Fills).

## Costs

`costs.rs` builds the two models the venue config takes
(`nautilus_execution::models`): the fee model from the command's
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
| `MakerTaker` | `MakerTakerFeeModel::new(maker, taker)`, `::zero()` | notional rate by liquidity side, on every fill; a negative rate is a rebate. Ours, the same rate on both sides (IB does not distinguish), negative rejected by `costs.rs` |

The other variants are for options and outcome shares. A fixed fee matches
IB Fixed pricing on Xetra below EUR 6,000 an order, where the EUR 3
minimum always applies. Above it the fee understates IB's: a strategy that
flips from short to long sends twice its trade size, as the EMA did when it
rebalanced, so with `trade_size = 5` on SXR8 (EUR 590-745) it sent orders
of EUR 5,900-7,300 (see Verified on IB data), where a rate of 0.0005 (IB's
0.05%) matches instead. A rate has no minimum, and IB's EUR 3 an order cannot be
expressed on a `BacktestNode` until Nautilus adds one
([nautilus_trader#5398](https://github.com/nautechsystems/nautilus_trader/issues/5398),
open), so a rate understates IB's fee on orders below EUR 6,000. Pick
`--commission "3 EUR"` or `--commission-rate 0.0005` by which side of EUR
6,000 the strategy's orders fall.

### Fills

Orders fill on the `EXTERNAL` bars replayed (`bar_execution` on; the
matching engine skips `INTERNAL` bars): IB's 1-minute TRADES bars and
OKX's candles, both LAST, the same way (see Fills on candles). A composite
hour bar is built after every minute bar closing at its close is replayed
(the timer of a timestamp fires after its data), so an order sent in
`on_bar` fills at the close of the hour's last minute, the trade price,
with no spread: slightly optimistic, accepted while IB is not traded. The
risk engine prices the market order from the cached LAST bar
(`risk/src/engine/mod.rs`, `market_order_price`), live as in a backtest.

On IB the minute's volume limits the fill as on OKX's thin minutes: an
order fills at most the close's share of the minute's volume at the close
and the rest one tick worse (0.0001 on SXR8). Outside Xetra's continuous
trading most TRADES bars have volume 0 (they carry the last trade price):
such a minute sets only the last price, and an order on it fills against
the book the last traded minute left. `backtest_ib_minute_bars.rs` and
Verified on IB data.

IB's BID, ASK and MID bars are not stored: they have volume 0, and the
matching engine takes a bar quote of size zero for an empty side
(`process_bar_quote_tick`), so every market order on them was rejected
`No market` (Verified on IB data).

A catalog of quotes (the test fixtures) fills as before: a market buy at
the ask, a sell at the bid, of the last quote replayed before the order,
the quote of the bar's last minute, stamped 1 ns before its close. The
fill model adds nothing on top of the quote, so the spread is the cost and
there is no slippage constant to guess. Verified in Fills on quotes and by
the integration tests.

Known limits of fills on quotes:

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

Every fill model is built from `(prob_fill_on_limit: f64, prob_slippage:
f64, random_seed: Option<u64>)`, probabilities in `[0, 1]`; always pass a
seed. With `prob_fill_on_limit = 1.0` and `prob_slippage = 0.0` the seed
draws nothing that changes a result: probabilities of 0 and 1 never reach
the RNG. That is also the node's model when `fill_model` is unset, but
unseeded; ours stays explicit, by the rule to seed all randomness (Rules).

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

### Fills on BID and ASK bars

Probe, 2026-10-08 (on `0.65.0`; the engine's bar handling below is
unchanged at the pinned commit): can the matching engine charge the real
spread from IB's BID and ASK bars? A throwaway strategy bought on one
decision bar and
sold on a later one, six orders over 2026-09-01 to 09-03, over a scratch
catalog of SXR8 1-HOUR LAST, BID, ASK and MID bars (`data/ib/README.md`).
Answer: **not with IB's bars as stored**.

How the engine executes bars (`execution/src/matching_engine/mod.rs`,
`process_bar`):

- Per instrument it keeps one execution bar type (`execution_bar_types`),
  the first one seen; a bar of another type replaces it if its step
  (`execution_bar_deltas`) is not longer, else it is ignored. LAST, BID,
  ASK and MID of one cadence therefore all execute.
- LAST and MID bars become trade ticks (`process_trade_ticks_from_bar`);
  on an L1 book a trade tick sets bid and ask to the trade price. BID and
  ASK bars are held until both of the same `ts_init` arrived, then become
  quote ticks (`process_quote_ticks_from_bar`).
- Tick sizes come from the bar volume (`BarTickSizes::from_volume`). A
  zero-volume quote clears the book (`process_bar_quote_tick`); a
  zero-volume trade only sets the last price. IB's BID, ASK and MID bars
  all have volume 0, so they never set a bid or an ask.
- The backtest engine's replay loop (`backtest/src/engine.rs`) gives each
  item to the venue, then to the data engine (the strategy's `on_bar`),
  then settles the venue: an order sent in `on_bar` fills before the next
  item, even one with the same timestamp. Equal timestamps replay in the
  order the streams were added (`BacktestEngine::add_data` appends: the
  first stream wins ties).

Decision at 2026-09-02 09:00 UTC: LAST close 709.54, bid/ask close
709.44/709.48, previous hour 710.22/710.29.

| Replayed (tie order), decision bars | Fills |
|---|---|
| LAST, BID, ASK as stored (`Backtest`, or the engine in either order), LAST | the LAST close (± one tick with `OneTickSlippage`): 709.54 |
| the same with BID and ASK volume set non-zero, LAST | the LAST close: LAST trade ticks override the quotes |
| BID, ASK as stored, BID or MID | none: `No market for SXR8.IBIS` |
| BID, ASK with volume, decision on the first of them | the **previous** hour's ask/bid: 710.29 |
| ASK, BID with volume, decision on BID | this hour's ask/bid close: 709.48 |
| BID, ASK with volume, then MID as stored, decision on MID | this hour's ask/bid close: 709.48 |
| MID as stored first, then BID, ASK with volume | the previous hour's ask/bid: 710.29 |

All six fills of each run followed the row. The strategy received its own
bar type only, every bar in order, whatever else was replayed. Fills on
IB's bars would need LAST out of the replay, a volume on the BID and ASK
bars, the strategy deciding on MID (or BID) bars added after them, and
control of the tie order. Turning each BID and ASK pair into one quote
tick worked instead (Fills on quotes), until 2026-10-26: the catalog now
stores IB's TRADES bars and no quote is made from bars (see Fills).

### Fills on quotes

This is how IB runs filled while `fill-catalog` made a quote a minute from
IB's BID and ASK closes (until 2026-10-26); it still holds for a catalog
of quotes.

Probe, 2026-10-08 (a scratch crate on the `nautilus-*` crates, nothing
kept): can orders fill on quotes while bars drive the strategy? Answer:
**yes**, with each quote stamped 1 ns before its bar's close; the probe
also had the venue's `bar_execution` off, needed while the catalog held
`EXTERNAL` bars (an IB catalog now holds none, so it is left on).

Setup:

- A scratch catalog: SXR8.IBIS (price precision 4, size precision 0, tick
  0.0001), its 1-HOUR-LAST bars, and one `QuoteTick` per BID and ASK bar
  pair of IB over the run window: bid = BID close, ask = ASK close, size
  1,000,000 on both sides, `ts_event = ts_init =` the bars' close − 1 ns,
  written with `write_to_parquet`.
- The node as in `node.rs` (netting, margin, L1 book, fixed fee) with
  `.bar_execution(false)` and a second `BacktestDataConfig` for the
  quotes. Run 2026-09-01 to 09-26, warmup 72 h.
- A probe strategy subscribed to the LAST bars only: it asks for its
  warmup, sends 12 market orders of 10 shares on chosen bars (in and out of
  Xetra's session, one with a crossed quote, one whose quote was removed),
  and on the 2026-09-03 14:00 UTC bar a buy limit and a sell stop, both at
  710.00, canceled at 17:00.

Fill model `Default`, a selection (all 12 orders followed the rule):

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
close lies outside the quote (2,328 of the year's 4,078 hours); the hour
without a quote filled on the previous one. Variants:

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

What the probe established:

- **Bars.** In every variant `on_bar` received all 302 LAST bars of the
  window, in order, with their closes. With `bar_execution` off
  `process_bar` returns at once: bars never touch the book.
- **The 1 ns stamp.** The quote sorts strictly before its bar, so the venue
  holds the hour's closing quote when the strategy decides on the bar,
  whatever order the data configs are in. Stamped at the close it ties with
  the bar and the tie goes to the config added first: bars first means
  every fill one hour stale. The closing quote is the last of its hour, so
  1 ns earlier adds no look-ahead.
- **Fill model.** `Default` with `(1.0, 0.0, seed)` adds nothing to the
  quote. `BestPrice` gave the same market fills but ignores the quote size
  and fills limit orders at or inside the spread at their own price
  (`fill_limit_inside_spread`), so `Default` it is.
- **Size.** With `liquidity_consumption` off every order sees the full
  quote size and nothing is used up between orders. A market order larger
  than the size is not refused: the rest fills one tick worse
  (`apply_fills`, exhausted L1 volume). At 1,000,000 shares the size never
  limits an order.
- **Warmup.** The warmup request was answered from the catalog's bars (16
  bars before the first `on_bar`) whether quotes were stored only from the
  start or also before it: warmup needs no quotes.
- **Crossed quotes** (ask < bid) are taken as they are (a warning in debug
  builds only): a buy fills at the ask, below the bid. In the year 178 of
  4,087 pairs are crossed: 176 on the bars closing 08:30 and 09:00
  Frankfurt time (pre-opening), 2 at 14:00 on 24 and 31 December (half
  days, after the close; 659.40/628.53 on the 24th). 6 are locked (ask =
  bid). The data side therefore drops crossed pairs, so the hour fills on
  the previous quote rather than on an impossible price.
- **Missing quotes.** In the year every LAST bar has its BID and ASK pair
  (9 pairs have no LAST bar: first bars of the day).
- **Resting orders.** The buy limit and the sell stop at 710.00 were not
  filled although the next hour's LAST low was 702.01 and its closing
  quote 715.50/715.54; with `bar_execution` on both filled at 710.00. This
  is the first known limit in Fills.

The run start on a bar's close also showed the warmup response holding the
bar at `start`, which the replay delivered again: both are why the engine
starts 1 ns earlier (see The engine start). How the quotes are built from
IB's bars is in `data/ib/README.md`.

## IC on ticks

An IC Markets instrument (`XAUUSD.ICMARKETS`, `EURUSD.ICMARKETS`)
backtests through the same code as IB's, on its quotes: the catalog holds a
`CurrencyPair` with IC's margins (1 / 30 on the demo) and one quote per
tick time, merged from
cTrader's BID and ASK ticks (`data/ic/README.md`). The strategy trades
internal bars built from them, such as `XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL`.

- **Fills.** A market order sent in `on_bar` fills at the ask (buy) or bid
  (sell) of the last quote at or before the bar's close, as on IB's
  quotes: a tick stamped exactly on the close reaches the venue before the
  order (checked by hand on `backtest_ic_quotes.rs` with a quote at
  10:00:00.000: the buy filled at its ask). Ticks keep their own
  milliseconds; nothing is stamped 1 ns early.
- **Size.** Quotes are 100,000 units, or the instrument's maximum order if
  smaller (XAUUSD 10,000 oz): an XAUUSD order always fills whole at the
  quote, an EURUSD order above one lot fills the excess one tick worse,
  and a bar holds at most 18,446,744,073 / size quotes, beyond which
  Nautilus panics building its volume (`data/ic/README.md`, "Quote size").
- **Commission.** IC's Raw account charges 30 USD per million USD traded
  a side on XAUUSD and EURUSD: `--commission-rate 0.00003`, charged on
  each fill's value in USD.
- **Venue and balance.** Named `ICMARKETS`; the balance in USD
  (`--balance "10000 USD"`), unleveraged (`--leverage 1`, the default):
  IC's leverage is in the instrument's margins (see Leverage).
- **Hedging.** The venue keeps positions as IC's hedging account does
  (`venue_oms_type`): an order opens a position of its own unless it names
  one, so a strategy reduces with `close_position` (`live/ic/README.md`,
  "Hedging positions").

`backtest_ic_quotes.rs` checks the fills, the commission and the spread in
the report on gold quotes written through `fill_catalog`;
`backtest_ic_hedging.rs` two overlapping trades kept as two positions;
`backtest_ic_margin.rs` IC's margin locked and an order beyond it denied.

## OKX on candles

An OKX swap (`BTC-USDT-SWAP.OKX`) or X-Perp (`BTC-USD_UM_XPERP-310404.OKX`,
the contract an EEA account trades) backtests through the same code as
IB's instruments, on its 1-minute candles. The catalog holds a
`CryptoPerpetual` or a `CryptoFuture` and the bars of
`…-1-MINUTE-LAST-EXTERNAL` (`data/okx/README.md`). The strategy trades a
composite bar type built from them, such as
`BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`
(`strategy/README.md`), and the run replays the minute bars and the
funding, nothing else. A catalog filled before candles, with OKX quotes,
must be filled again into a new directory (`data/okx/README.md`).

- **Venue.** Named `OKX` after the instruments' venue, with the same
  config as for IB. The balance is in the settlement currency: USDT for a
  swap (`--balance "10000 USDT"`), USD for an X-Perp (Nautilus' USD, two
  decimals: each fill's value, commission and PnL rounded to the cent).
  `base_currency` is left unset, as for IB.
- **Margins.** A position locks its value / `--leverage` (see Leverage).
  OKX Europe caps retail accounts at 10x.
- **Contracts.** BTC-USDT-SWAP: 0.01 BTC a contract, lots of 0.01. The
  X-Perp: 0.0001 BTC, whole contracts; Nautilus rejects its orders before
  its listing (2026-03-30) or after its expiry (2031-04-04).
- **Commission.** `--commission-rate 0.0005` is OKX's taker fee, 0.05% of
  each fill's value (contracts × contract size × price), no minimum.
- **Hour bars.** A minute bar is stamped at its open (`ts_event`) and
  replayed at its close (`ts_init`), so the hour bar closing 10:00 holds
  the minutes opening 09:00 to 09:59 and is built at 10:00, after its last
  minute closed: nothing of 10:00-10:01 is in it (`backtest_okx_swap.rs`).
  Since the engine starts 1 ns before `start`, a run starting on the hour
  replays the minute closing then: its first hour bar holds that minute
  alone.
- **Warmup.** The warmup of a composite bar type requests its minute bars
  (`strategy/README.md`), so the catalog must hold them from `start -
  warmup`, as for quotes.

### Funding

OKX settles funding on open swap and X-Perp positions (BTC every 8 hours,
00:00, 08:00 and 16:00 UTC, about 0.01% of the position each time). The
catalog stores their funding rates (`data/okx/README.md`), the run replays
them by instrument id, and Nautilus' simulated venue settles them; we add
no code of our own. An update whose `ts_event` is on its interval's
boundary makes one `FundingSettlement` at that time on the open position:
rate × the position's value at the settlement price, paid by a long and
received by a short when the rate is positive, the reverse when negative.
With no mark price in the catalog the price is the mid of the book, which
the bars set to the last trade: the close of the minute ending at the
funding time (OKX uses its mark price; the difference is negligible). The
settlement adjusts the position's realized PnL and the balance, so the
report's PnL and final balance include it.

A position opened or flipped on an hour bar closing at a funding time pays
that time's funding: the fill and the settlement share the timestamp and
Nautilus settles after the fill, while on OKX an order sent after the
00:00 close comes after the settlement.

The stored funding must cover the whole window, as the bars do, else the
run is refused naming `the funding_rates of …` and the missing range.
The stored funding reaches the last funding time before the catalog fill
ran, but an X-Perp's starts only about 3 months before its first fill (all
OKX keeps, `data/okx/README.md`): use the swap for longer studies.
`backtest_okx_swap_funding.rs` and `backtest_okx_xperp_funding.rs` check a
long paying and receiving at two funding times, and a window the funding
does not cover refused.

### Fills on candles

The minute bars drive the venue through Nautilus' bar execution (Concepts ›
Backtesting › Bar execution), on by default, on the L1 book the venue
already uses.

- **Four trades a minute.** Each minute bar becomes trades at its open,
  high, low and close, a quarter of its volume each (the close takes the
  remainder; a bar of 1 to 3 lots splits differently,
  `BarTickSizes::from_volume`), and resting orders are matched after each.
  An order sent in `on_bar` arrives after the minute's close and fills, as
  a market order, at that close: the last price, no spread, stamped at the
  hour's close.
- **Order of high and low: open, high, low, close.** Nautilus' default
  (`bar_adaptive_high_low_ordering` off). The adaptive order visits the
  extreme nearer the open first; Nautilus calls it a heuristic with no
  general accuracy rate, and it only decides which of a stop and a target
  inside one minute fills first, as a bracket's stop loss and take profit
  can.
- **Fill model unchanged**: no slippage. LAST bars carry no bid or ask, so
  the run pays no spread and the report prints `Spread: n/a (no quotes)`.
  On BTC the book is one tick (0.1) wide nearly all the time: the
  quote-based runs this replaced paid 0.16 USDT of spread over 157 fills,
  against 128 USDT of commission.
- **Size.** `liquidity_consumption` is off, but an order fills at most the
  close's share of the minute's volume at the close, and the rest one tick
  worse. On the X-Perp, whose minutes can trade few contracts, that happens
  (below).

### Verified on OKX data

2026-10-22: scratch catalogs filled from OKX's archive for archive days
2026-10-11 to 10-21 (`--start 2026-10-11 --end 2026-10-21`), EMA 10/30 on
the composite hour bars warmed up over 48 hours, no session, 0.01 BTC a
trade. Funding not included (filled before funding was stored).

```toml
instrument_ids = ["BTC-USDT-SWAP.OKX"]     # or BTC-USD_UM_XPERP-310404.OKX
bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
trade_size = "1"                           # "100" on the X-Perp
warmup = "48h"
```

```
helix-trader backtest ema --config swap.toml --catalog okx-cat \
  --start 2026-10-14 --end 2026-10-21 --commission-rate 0.0005 --balance "10000 USDT"
```

| | Swap | X-Perp |
|---|---|---|
| Orders (fills) | 9 (9) | 9 (11) |
| PnL (total) | -58.06 USDT | -58.10 USD |
| Commission | 6.98 USDT | 6.98 USD |
| Spread | n/a | n/a |
| Gross PnL | -51.08 USDT | -51.12 USD |
| Max drawdown | 0.66% | 0.66% |
| Final balance | 9941.94 USDT | 9941.90 USD |

Warm from the start: the first order fills at 2026-10-14 00:00, on the
first hour bar of the run. Before the warmup served composite bar types,
the same window started cold and gave 8 orders, -65.06 USDT and -64.86
USD. Two X-Perp buys of 200 contracts met thin minutes: 10 and 18
contracts filled at the close, the rest (190, 182) one tick higher.

Checked against the stored minute bars, outside Nautilus, on the cold runs
(same fills code):

- **Fills.** Every swap fill, and every X-Perp order's first fill, at the
  close of the minute closing at the fill's time (the hour's close).
- **Thin minutes.** 8 and 17 contracts at the close, the rest one tick
  higher.
- **Commission.** 0.05% of each fill's value, to the eighth decimal on the
  swap, to the cent on the X-Perp.

**With funding**, 2026-10-23: a scratch catalog filled for
BTC-USDT-SWAP.OKX with `--start 2026-06-01 --end 2026-10-01` (candles to
2026-10-01T00:00Z, funding 2026-06 to 2026-09 from 4 monthly files, 366
rates), the same EMA config, `--start 2026-06-03 --end
2026-09-30T15:00:00Z`: 104 orders, 359 funding settlements (every 00:00,
08:00 and 16:00 UTC of the window, a position being always open),
-0.716571 USDT in all (+5.94 received, -6.65 paid). On the catalog without
its funding: the same 104 orders, a final balance of 10064.36 USDT,
0.71657135 more than with funding (10063.65), and PnL (total) 64.36
against 63.65. The first settlement, 2026-06-03 00:00 on a short of 1
contract opened that minute, is 0.01 BTC × 66,722.6 (the close of the
minute ending 00:00) × 0.0000865678874748 (the archive's rate) =
+0.05776035 USDT, as Nautilus settled it.

## Results

`node.run()` returns one `BacktestResult` (`nautilus_backtest::result`)
per run config; all fields are public:

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
  `Expectancy`, `Win Rate`. With no winning position the winner statistics
  are `NaN`.
- `stats_returns`: `Returns Volatility (252 days)`, `Returns Skewness`,
  `Returns Kurtosis`, `Average (Return)`, `Average Loss (Return)`,
  `Average Win (Return)`, `Sharpe Ratio (252 days)`, `Sortino Ratio (252
  days)`, `Profit Factor`, `Tail Ratio`, `Risk Return Ratio`.
- `stats_general`: `Long Ratio`, rounded by Nautilus to two decimals.
- `summary`: `orders.total`, `orders.open`, `orders.closed`,
  `positions.total`, `positions.open`, `positions.snapshots`, and per venue
  `account.{venue}.balance.{currency}.total` (also `.free`, `.locked`),
  for example `"9942.00 EUR"`.

PnL is after commissions: each position's realized PnL subtracts the
commission of its fills, the still open position's included. `PnL
(total)` is how much the account balance changed; it counts realized PnL
only.

`total_positions` also counts the positions a flip closed (netting keeps
one position per instrument and snapshots the closed one);
`positions.total` in the summary counts only the positions in the cache,
one per instrument when netting.

`Max Drawdown`, `CAGR (252 days)` and `Calmar Ratio (252 days)` are not in
the default set. Each is a `PortfolioStatistic` (`nautilus_analysis`)
computed from the returns series. `node.rs` registers `MaxDrawdown` with
the engine's portfolio before the run, so the portfolio computes it with
the default statistics and the result holds it in `stats_returns`, named
`Max Drawdown` (a negative fraction: -0.05 is 5%):

```rust
engine.kernel_mut().portfolio.borrow_mut().register_statistic(Arc::new(MaxDrawdown::new()));
```

A registration outlives the analyzer's resets (`Portfolio::register_statistic`).
The engine also logs its statistics, `Max Drawdown` included, at the end of
the run.

### Costs in the report

The result has no costs, so `report.rs` works them out after the run from
two inputs:

- **Fills.** Every `OrderFilled` event of the orders in the engine's cache
  (`cache.orders(..)`, then `order.events()`): the price (`last_px`), the
  quantity (`last_qty`), the price's currency and the commission. Nautilus
  documents reading the cache after the run with `dispose_on_completion`
  off (Concepts › Reports; the v2 migration notes on post-run inspection).
- **Quotes.** After the run the engine no longer has the replayed quotes:
  the node clears its data, and the cache keeps only the last
  `tick_capacity` quotes. So `node.rs` loads them again with
  `BacktestNode::load_data_config(data, start, end)`, the same loader and
  window the run used. Each fill gets the last quote of its instrument
  stamped at or before it: the quote it filled on (see Fills).

The spread rule is by data, not by venue: a fill of an instrument the run
replayed no quote of has no spread (a run on OKX's candles, or on IB's
minute TRADES bars, prints `Spread: n/a (no quotes)`, with the cost per
trade and the gross PnL on the commission alone); a fill of one with quotes and none before it is an
error. An inverse instrument is refused: its value is not linear in the
price.

Per currency, `report` prints a `Costs` section after the PnL statistics:

| Figure | |
|---|---|
| `Commission` | the sum of the fills' commissions |
| `Spread` | for each fill, the distance between its price and the quote's mid (Nautilus' `extract_price(PriceType::Mid)`, as for MID bars), times its quantity (and the instrument's multiplier): the half spread paid, in the price's currency |
| `Cost per trade` | (commission + spread) / number of fills |
| `Gross PnL` | `PnL (total)` + commission + spread: the PnL before costs |

A position still open at the end has paid the half spread to open it, but
that loss is not realized yet; the gross PnL adds it back anyway, so it is
too high by the open position's size times half the spread.

## Verified on IB data

The runs below, and those on OKX data above, were made with the EMA as it
was then: a rebalancer, long or short `trade_size` on every listed
instrument, turning at each cross with no stop loss, take profit or hold.
Their configs predate `trade_instrument_id` and the exits.

The runs on internal MID bars further below filled on quotes
`fill-catalog` made from IB's BID and ASK closes until 2026-10-26; a
catalog filled since holds IB's 1-minute TRADES bars as they are.

**IB's minute TRADES bars**, 2026-10-26: the catalog of SXR8.IBIS
`1-MINUTE-LAST-EXTERNAL` bars filled from the paper gateway over
2026-09-04 to 09-12 (5,524 bars, 2,659 of them with volume 0, nearly all
outside Xetra's continuous trading), EMA 5/15 on
`1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` bars, `trade_size = 10`,
`warmup = "48h"`, no session (refused with composite bars), 2026-09-07 to
09-12, 3 EUR an order:

```
helix-trader fill-catalog ema --config sxr8.toml --catalog sxr8-cat \
  --start 2026-09-04 --end 2026-09-12
helix-trader backtest ema --config sxr8.toml --catalog sxr8-cat \
  --start 2026-09-07 --end 2026-09-12 --commission "3 EUR" --balance "10000 EUR"
```

It printed `Orders: 4`, `PnL (total): 24.200000`, commission 12.00, gross
PnL 36.20, `Spread: n/a (no quotes)` and a final balance of `10024.20 EUR`.
Every order filled at the close of the minute closing at its decision bar's
close (714.32, 714.88, 713.68, 708.30). The flips of 20 met thin or empty
minutes: on 09-08 12:00 (4 shares traded) and 09-11 07:00 (none) 1 share
filled at the close and 19 one tick higher.

The same window on IB's MID, BID and ASK minute bars instead (13,950 bars,
every one with volume 0, 4,650 of each series, MID composite bars) sent 66
orders, all rejected `No market for SXR8.IBIS`, and ended at
`10000.00 EUR`.

**LAST bars, bar execution.** Paper gateway, 2026-10-08, SXR8.IBIS
1-HOUR-LAST, EMA 10/30 (the defaults), `trade_size = 5`, `warmup =
"720h"`, before fills on quotes (bar execution, one tick of slippage):

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
  `9813.30 EUR` (PnL -186.70): the difference is 339.00 = 113 × 3 EUR. IB
  Fixed would have charged 377.32: 112 of the 113 orders are flips of 10
  shares, above EUR 6,000 (see Fees).
- **Fills.** All 113 (`OrderFilled` with `NAUTILUS_LOG=stdout=Info`) are at
  their bar's close plus (buy) or minus (sell) `0.0001`, the instrument's
  tick, stamped at the bar's `ts_event`.
- **Session.** 58 of the 113 orders were sent on bars that close outside
  Xetra's continuous trading (09:00-17:30 Frankfurt time): 9 on the first
  bar of the day (a half-hour bar, see `data/ib/README.md`), 9 on the
  pre-opening hour, 40 after the close, 33 of them on the 19:00-23:00 bars
  (median volume 154 shares a bar, 12 under 100, one with 0). Bars in
  continuous trading had a median 6,429. Those fills are at prices with
  little or no volume behind them, which a live order on Xetra might not
  get, and the thin bars also move the EMAs: these results are optimistic
  about fills.
- `Long Ratio: 0.500000` with 57 long of 113 positions: Nautilus rounds it
  to two decimals.

**Internal MID bars**, 2026-10-20: the catalog of SXR8.IBIS 1-minute quotes
filled from the paper gateway (2026-08-24 to 10-01, see
`data/ib/README.md`), EMA 5/15 on `1-HOUR-MID` bars, `trade_size = 10`,
`warmup = "168h"`, Xetra's session, September 2026, 3 EUR an order. The
warmup request asked for the quotes from 2026-08-24 23:59:59.999999999
with the three params of `strategy::warmup`; the first order went on the
run's first bar inside the session (09-01 08:00 UTC), so the EMAs were
warm from the start. It printed `Orders: 16`, `PnL (total): -30.000000`, a
final balance of `9970.00 EUR`, no error, including one weekend fill (see
Weekends and holidays below). The same month on the stored 1-HOUR-LAST
bars gave 14 orders and `9935.60 EUR`: the mid of the closing quote is not
the last trade.

**With costs and the session**, 2026-10-21: the same window as the LAST
run, now with the commission, fills on quotes and Xetra's session. The
catalog of SXR8.IBIS 1-minute quotes was filled from the paper gateway
over the warmup and the window (194,609 quotes, 973 crossed pairs and 8
one-sided bars dropped, 21 minutes):

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
the strategy traded every bar of the day on LAST closes; now it trades in
the session only, on the mid of the closing quote, and pays the spread.
Checked against the stored quotes, outside Nautilus:

- **Trade count.** EMA 10/30 recomputed by hand:
  1. Take the mid of the last quote of each hour.
  2. Keep only the hours that have a quote and close inside the session.
  3. Seed each EMA with the average of its first 10 or 30 bars, as
     Nautilus does.
  4. Warm up from 2025-12-01 23:59:59.999999999.

  This gives the same 57 orders, all filled, none rejected, matching the
  fills in hour, side and size: one of 5 shares, then 56 flips of 10.
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
- **Report.** Spread recomputed on the fills: 16.05 EUR, as reported. Gross
  PnL = PnL (total) + commission + spread, in both runs.

**Weekends and holidays, found and fixed.** The first run of this check
traded on days the exchange was closed: the session checks the time of day
only, and Nautilus built a bar for every hour, flat at the last close when
no quote came, so the 10:00-17:00 bars of Saturdays, Sundays and holidays
counted as session bars. With the rate, that run sent 86 orders: 78
filled, 5 of them on Saturdays at Friday's last quote (11 to 18 hours
old), and 8 rejected on New Year's Day. It ended at 9364.42 EUR (PnL
-635.58, commission 256.08, spread 35.15). Now an hour without any quote
builds no bar, in backtest and live alike (`strategy/README.md`, "Hours
without quotes"): 57 orders, the figures above. The September run on
internal MID bars had one such fill (Saturday 5 September, on a quote 13
hours old); re-run on this catalog with the fix it gives 16 orders, none
on a weekend, PnL -111.80, a final balance of 9888.20 EUR instead of
9970.00.

## Rules

- **Costs always explicit.** A fee model always, even a zero one.
- **Warmup before the start.** Warmup data must predate the run start,
  and the catalog must cover the whole warmup window: `run` refuses
  otherwise.
- **Seed all randomness**: the fill model and the strategy config.
- The plain Nautilus API and one run per process, no parameter sweeps:
  `docs/ARCHITECTURE.md`.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file, for
  pure code: the run config from our inputs; the cost models (a negative
  commission or rate rejected, a fixed commission charged on the first
  fill only, a rate on the value of every fill, maker and taker alike);
  the coverage check (gaps on the intervals alone, then on a temporary
  catalog: missing instrument, quotes 1 ns short at either end, missing,
  or with a gap, the source bars of a composite bar type, stored funding
  short of the window); the run's series (the stored funding of a traded
  instrument added, none without; the stored book depth of an
  instrument added beside the source of its bars, or alone when none is
  stored); the data configs (quotes by instrument,
  a composite bar type's source bars by bar type); the report of a
  hand-built `Outcome` (sections, sorted names, max drawdown, the costs and
  the gross PnL); the venue at the run's leverage with no margin model of
  ours; instruments on two venues a venue each, as each keeps positions,
  with the run's balance, leverage and fee model; a leverage not positive
  refused. The command line is tested in
  `main.rs`: required flags, exactly one commission flag, malformed
  `Money`, the window check, `--leverage` 1 unless given.
- Integration tests in `tests/backtest/`, one node per test, run the real EMA
  strategy through `Backtest::run` on a fixture catalog (`tests/common`:
  SXR8.IBIS in EUR and its 1-minute quotes only, bid and ask at the mid
  ± 0.05 EUR, stamped 1 ns before each minute's close, written with
  `ParquetDataCatalog::write_to_parquet`). The strategy trades on
  1-HOUR-MID internal bars, the shortest crossover (1/2) on steps, a level
  per hour chosen so that every cross, exit and fill is worked out by hand
  (`common::trades`, three hours of warmup, the run from 12:30), and so
  that what is tested changes the orders and the final balance:
  - the EMA's trades: `backtest_ema_entries.rs` (one bracket per cross, in
    its direction, none on a bar keeping the side), `backtest_ema_exits.rs`
    (a long ended by its take profit, a short by its stop loss, each on
    the first minute quote reaching it, the other leg cancelled),
    `backtest_ema_time_exit.rs` (a trade closed its hold after the entry
    filled, at the market then), `backtest_ema_overlap.rs` (on a hedging
    venue a short opened beside a long still held, each closed by its own
    alert), `backtest_ema_read_only_instrument.rs` (a second instrument
    read, its bars crossing every hour, sends no order);
  - the warmup: `backtest_warmup.rs` (a run off an hour: warmup from the
    cache, the partial oldest bar dropped, the bar spanning the warmup end
    traded, buys at the ask and sells at the bid of the quote 1 ns before
    the bar's close), `backtest_start_on_close.rs` (the same on a run
    starting exactly on an hour's close: the one-quote oldest bar dropped,
    the bar at the start traded on its closing quote);
  - the session: `backtest_session.rs` (bars outside the session neither
    move the EMAs nor trade; overnight, without quotes, there are no bars),
    `backtest_without_session.rs` (without a session every bar counts, the
    one spanning midnight included);
  - the costs: `backtest_commission_per_order.rs` (charged on each filled
    order, not on the legs cancelled), `backtest_commission_rate.rs` (each
    fill charged the rate on its value, and the report's costs: the half
    spread of each fill, the gross PnL), `backtest_trade_size_off_grid.rs`
    (10.5 shares trade as 10);
  - a wave over days, on which the EMA 5/15 crosses several times:
    `backtest_max_drawdown.rs` checks that the result's `stats_returns`
    hold `Max Drawdown`, the largest fall from a peak of the equity
    compounded over its returns series.
  - `backtest_coverage.rs`: a warmup longer than the stored quotes, or a
    catalog without quotes, is refused before the node is built, naming
    the instrument's quotes and the missing interval.
  - `backtest_inputs.rs`: a negative warmup, or a start on or after the
    end, is refused before the node is built.

  The EMA reads bar closes only, so the bars' opens, highs and lows, and
  orders of a chosen side and size, are tested with a recording strategy
  (`tests/common/recorder.rs`)
  that warms up through `strategy::warmup` like a real one, records every
  bar it gets and can rest a sell stop or send market orders:
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
  - `backtest_ib_minute_bars.rs`: `fill_catalog` stores the one series
    `catalog_series` lists for a composite
    `1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` bar type, minute bars stamped
    at their close with a volume, as IB gives TRADES bars; the hour bars
    have the prices of their minutes, and two market orders fill in full at
    the close of the minute closing their decision bar (see Fills).
  - `backtest_composite_bars.rs`: `fill_catalog` stores the external
    minute bars a fake source returns for a composite
    `1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` bar type, and a recorder
    without warmup gets hour bars with the open, high, low and close of
    their minute bars.
  - `backtest_okx_swap.rs`: BTC-USDT-SWAP.OKX, a `CryptoPerpetual` with
    margins of 1, on minute bars stamped as `fill-catalog` stores OKX's
    candles (open, close). A recorder without warmup on the composite hour
    bars buys 2 contracts on the first and sells 3 on the second: each
    hour bar holds the minutes opening in its hour (a minute moved by one
    shows in a high or a low), the fills at the close of the hour's last
    minute, 0.05% on each fill's value, at 1x the short left open locking
    its whole value at entry, and `Spread: n/a` in the report's costs.
  - `backtest_okx_swap_funding.rs`: the same swap held long across two
    funding times with stored funding: a Nautilus funding settlement at
    each, -0.2 USDT at +0.01% on 2,000 USDT, +0.404 at -0.02% on 2,020, in
    the final balance; and a window the stored funding does not cover,
    refused naming it and the missing range.
  - `backtest_okx_xperp.rs`: the same bars and orders on
    BTC-USD_UM_XPERP-310404.OKX, a `CryptoFuture` of 0.0001 BTC a contract
    settled in USD, with margins of 1: 200 contracts bought and 300 sold,
    the commission and PnL in USD at two decimals, at 10x the short left
    open locking a tenth of its value.
  - `backtest_ic_quotes.rs`: XAUUSD.ICMARKETS, a `CurrencyPair` with
    IC's margins on the demo (1 / 30), unleveraged, on irregular millisecond quotes sized 10,000 as IC's fill
    stores them, written through `fill_catalog` (`common::ic`). A recorder
    without warmup on hour MID bars buys 10,000 oz on the first bar and
    closes that position by its id on the second (`close_position`), the
    venue hedging: each fills whole at the ask, then the bid, of the last quote
    before the bar's close, 30 USD per million is charged on each fill's
    value, and the report shows the spread paid (half each quote's spread
    on 10,000 oz) and the gross PnL (the mids' move).
  - `backtest_ic_hedging.rs`: on the same quotes, the venue
    `venue_oms_type(ICMARKETS)`, two buys of 10 oz on two bars are two
    open positions, at 4006.10 and 4012.15.
  - `backtest_book_depth.rs`: CME gold, GC.v.0.GLBX, a `FuturesContract`
    with book depth snapshots of 10 levels and no quotes (`common::glbx`):
    a strategy subscribed to its book depth, managed, gets the snapshots
    of the run's window in order, the cache's book holding each as it
    arrives; gold's MID bars, without a source, are left out.
  - `backtest_two_venues.rs`: gold's book from `GLBX` and IC's gold
    quotes (`common::ic`) in one catalog: a strategy reading the book buys
    10 oz of XAUUSD on IC's first hour bar; two venues, the trade on
    `ICMARKETS`, its commission off that account alone, the report
    showing both accounts at the run's balance.
  - `backtest_ic_margin.rs`: on the same quotes and 1,000 USD, a buy of
    5 oz at 4006.10 (20,030.50 USD) fills, its position locking IC's
    margin, 667.68 USD; a second of 5 oz at 4012.15 is denied by the risk
    engine (`INITIAL_MARGIN_EXCEEDS_FREE_BALANCE: free=331.72 USD,
    margin=668.69 USD`).
