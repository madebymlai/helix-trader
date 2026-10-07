# The data module

`data` is build step 1. It fills the Nautilus Parquet catalog with the
history of the instruments the strategy trades; backtest reads that catalog.
It adds what Nautilus does not provide: filling the catalog from a
historical source, and the roll dates.

APIs are those of the `nautilus-*` crates at `0.65.0`; **unverified** marks
what the sources did not confirm. Examples use the future `ESM4.CME`, its
predecessor `ESH4.CME` and the continuous root `ES.CME`.

## Layout

```
data/
  mod.rs           declares the modules, exposes only what others call
  fill_catalog.rs  source-agnostic: resume, check, sort, write
  ib.rs            Interactive Brokers history: instruments and bars
  rolls.rs         roll dates -> continuous_future_transitions params
```

No node and no port of our own. A source is an adapter's historical
client, which returns Nautilus types (`Vec<InstrumentAny>`, `Vec<Bar>`);
`ParquetDataCatalog` is the storage.

```
strategy config ──► fill_catalog ──► ib.rs ──(HistoricalInteractiveBrokersClient)──► IB Gateway
                         └──► ParquetDataCatalog ──► backtest
rolls ◄── catalog bars of each contract ──► params for the INTERNAL bar type
```

## Instruments and bar types

Nautilus has no universe type, and neither do we. The strategy config is the
single source of the instruments and the bar type (see the strategy README).
`main` reads them from it and passes Nautilus types to `fill_catalog`, backtest and
live: `Vec<InstrumentId>` and `BarType`. This module never names an
instrument. The history window is a run argument of `fill-catalog`
(`--start`, `--end`), not config.

Two bar types matter: the `EXTERNAL` one per contract (what `fill_catalog` requests
and the catalog stores) and the `INTERNAL` one per continuous root (what the
strategy subscribes to).

### Bar type format

```
{instrument_id}-{step}-{aggregation}-{price_type}-{source}
ESM4.CME-1-HOUR-LAST-EXTERNAL
```

```rust
BarSpecification::new_checked(step, aggregation, price_type) -> anyhow::Result<BarSpecification>
BarType::new(instrument_id, spec, aggregation_source) -> BarType
"ESM4.CME-1-HOUR-LAST-EXTERNAL".parse::<BarType>()   // use for config input
```

`BarSpecification::new` and `BarType::from(&str)` panic on bad input; use
the checked forms for anything read from config.

**Step rules** (enforced by `new_checked` and parsing):

| Aggregation | Step must |
|---|---|
| `SECOND`, `MINUTE` | divide 60, not equal 60 |
| `HOUR` | divide 24, not equal 24 |
| `MILLISECOND` | divide 1000, not equal 1000 |
| `MONTH` | divide 12 |
| `DAY`, `WEEK`, `YEAR` | any non-zero step |

So `60-MINUTE` is rejected: write `1-HOUR`. Valid for Nautilus is not valid
for the adapter: each adapter serves its own bar sizes (for IB see below),
and a cadence the adapter cannot serve is rejected before fill_catalog starts.

**Aggregation source**, the last token:

- `EXTERNAL`: bars made by the venue or adapter: what `request_bars`
  returns, what the catalog stores, what `subscribe_bars` streams.
- `INTERNAL`: bars built by the Nautilus data engine from ticks or other
  bars, as continuous futures are.

**Composite** bar types build a target from a finer bar type:
`ES.CME-1-HOUR-LAST-INTERNAL@1-HOUR-EXTERNAL`. The target must be
`INTERNAL`. `request_bars` rejects a composite `bar_type` (pass it through
the `bar_types` param instead); `subscribe_bars` takes it directly.

### Instrument ids across sources

The id a source gives a contract must be the id the strategy config, the
catalog and the live broker use. Sources disagree on the venue:

| Source and setting | June 2024 ES |
|---|---|
| IB, default (`Simplified`, venue = the contract's exchange) | `ESM4.CME` |
| IB, `symbol_to_mic_venue: {"ES": "XCME"}` | `ESM4.XCME` |
| Databento, default | `ESM4.GLBX` |
| Databento, `use_exchange_as_venue` | `ESM4.XCME` |

- The symbol matches (`ESM4`); only the venue differs. No default setting
  makes two sources agree: matching IB and Databento takes `XCME` on both.
- **One source per instrument** in a catalog, with the ids of the live
  broker. Changing source or venue means new config ids and a refilled
  catalog.
- **The decade is not in the id.** `ESM4` is June 2014 and June 2024 alike,
  in both sources, and Nautilus has no option to add it. The catalog keeps
  the two apart in time, but one run cannot hold both: the cache has one
  instrument per id. With IB as the source it should not arise (about two
  years of history, unverified). A longer history needs one
  catalog per decade, or a symbol rewrite that breaks the match with the
  live broker.

## fill_catalog.rs

Fills the catalog for a list of bar types over one window. No node: a
source's historical client is a plain async call returning Nautilus types,
the same pattern the Nautilus docs use for every source (a client or loader,
then `catalog.write_*`). Filling the catalog is its own process run.

### Why not a node

The generic history path in Rust is `DataClient` behind a `LiveNode`:
`DataClient::request_bars` returns `()` and the bars reach an actor's
`on_historical_bars` through the data engine. That callback carries no
request id (an empty response arrives as an empty slice), and an `Err` from
it is only logged, so a failed fill would still exit cleanly. The historical
clients return the data and propagate errors with `?`.

### Nautilus does not persist responses

In Rust, a requested response is never written to a catalog by Nautilus.
The data engine serves a request from a registered catalog and asks the
adapter for the missing intervals, but writes nothing back; the
`update_catalog` param only routes a request to the client. The streaming
writer records published live data only, and history responses are not
published. Filling the catalog is ours.

### Interface

```rust
pub async fn fill_catalog(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,           // from the source, before any bars
    bar_types: &[BarType],
    start: Timestamp,
    end: Timestamp,
    fetch_bars: impl AsyncFnMut(BarType, Timestamp, Timestamp) -> anyhow::Result<Vec<Bar>>,
) -> anyhow::Result<()>
```

`fetch_bars` is the seam: `ib.rs` supplies it now, another source another
function. No trait until a second source exists. Tests pass a closure with
canned bars.

1. **Instruments.** Write those not yet in the catalog
   (`catalog.instruments(ids)`), so a re-run adds no file.
2. **Resume.** Per bar type, one at a time, from the stored range of that
   exact bar type (`get_intervals` with the bar type string, so other bar
   types of the same instrument do not count). A `start` before the stored
   range is an error naming the bar type and the range, raised before
   fetching: extending history backwards is not supported; use a new
   catalog. Otherwise the resume point is the end of the stored range;
   request from 1 ns after it (or `start`, if later) to `end`. A window
   already covered (resume point at or after `end`) asks the source for
   nothing and is not an error.
3. **Check.** Refuse an empty response (also after a resume: a window
   not yet covered must have bars) and any bar of another bar type.
   Drop bars at or before the resume point, sort by `ts_init`, drop
   duplicate `ts_init`. If nothing is left, nothing is written and that is
   not an error.
4. **Write** one bar type per call with `write_to_parquet`, recording the
   file's interval as the window asked for, not the bars' own span: from
   the request start (or the oldest bar, if earlier) to `end` (or the
   newest bar, if later). The stored range then spans the windows already
   asked for, so a re-run with the same `start` and `end` asks for nothing,
   even when the bars start after `start` or close before `end`. A response whose bars were
   all dropped records no coverage, so the next run asks for that tail
   again.

Any error stops the run. What was written stays valid; a re-run resumes.

### Writing

Inherent methods of `ParquetDataCatalog` (`nautilus_persistence::backend::parquet::catalog`):

```rust
ParquetDataCatalog::new(base_path: &Path, None, None, None, None)   // panics on a bad path
catalog.write_instruments(instruments: Vec<InstrumentAny>) -> anyhow::Result<Vec<PathBuf>>
catalog.write_to_parquet(data: &[Bar], start: Option<UnixNanos>, end: Option<UnixNanos>,
                         skip_disjoint_check: Option<bool>) -> anyhow::Result<PathBuf>
catalog.get_intervals(&NautilusDataType::Bar.into(), Some(bar_type_str))
    -> anyhow::Result<Vec<(u64, u64)>>             // stored [start, end] per file, sorted
```

`write_to_parquet` returns an empty path for empty data and writes nothing.
The trait `CatalogWriter` (`write_data` over `&[Data]`) groups mixed data
for you; with one bar type per call the inherent method is enough.

Write constraints:

- **Ascending `ts_init`.** Sort before writing; a descent is an error.
- **One bar type per file.** `write_to_parquet` refuses mixed bar types.
- **Disjoint intervals.** Each write makes one file named after its
  `[first, last]` `ts_init`. A new interval must not touch an existing one:
  a shared timestamp is an overlap and an error. Appending a later window
  works; rewriting an overlapping one fails. `fill_catalog` passes the
  window asked for as the interval (see step 4). Never pass
  `skip_disjoint_check`: an overlap means a bug in the windows.
- **Instruments** go one file per id, named by the instrument's `ts_init`;
  a later definition adds a file.

Layout on disk:

```
{catalog}/data/bars/ESM4.CME-1-HOUR-LAST-EXTERNAL/{start}_{end}.parquet
{catalog}/data/futures_contract/ESM4.CME/{ts}_{ts}.parquet
```

### Reading back

```rust
catalog.bars(ids: Option<Vec<String>>, start: Option<UnixNanos>, end: Option<UnixNanos>)
    -> anyhow::Result<Vec<Bar>>                      // &mut self; ordered by ts_init
catalog.instruments(ids: Option<&[String]>, start, end) -> anyhow::Result<Vec<InstrumentAny>>
catalog.get_intervals(..)                            // the stored range a re-run resumes from
```

For bars an id is a full bar type string or an instrument id (which matches
all its bar types).

### Interactive Brokers specifics (history)

`ib.rs` wraps `HistoricalInteractiveBrokersClient`
(`nautilus_interactive_brokers::historical`).

```rust
HistoricalInteractiveBrokersClient::connect(config: InteractiveBrokersDataClientConfig)
    .await -> anyhow::Result<Self>
client.request_instruments(instrument_ids: Option<Vec<InstrumentId>>,
    contracts: Option<Vec<Contract>>) .await -> anyhow::Result<Vec<InstrumentAny>>
client.request_bars(bar_specifications: Vec<&str>, end_date_time: Timestamp,
    start_date_time: Option<Timestamp>, duration: Option<&str>,
    contracts: Option<Vec<Contract>>, instrument_ids: Option<Vec<InstrumentId>>,
    use_rth: bool, timeout: u64) .await -> anyhow::Result<Vec<Bar>>
```

- **No node.** `connect` opens the IB socket through the adapter's shared
  client registry and initialises an instrument provider; no message bus,
  cache or clock. It needs a Tokio runtime and a running IB Gateway or TWS.
  The config is the data client config of the live README.
- **Instruments first.** Bars of a contract the provider does not hold come
  out with a fallback precision (5, 0). Call `request_instruments` with the
  config's ids before `request_bars`.
- **Bar spec strings** are `"{step}-{aggregation}-{price_type}"`, for example
  `"1-HOUR-LAST"`. The historical client parses SECOND, MINUTE, HOUR, DAY and
  WEEK only (no MONTH). IB itself serves 1/5/15/30 SECOND;
  1/2/3/5/10/15/20/30 MINUTE; 1/2/3/4/8 HOUR; 1 DAY; 1 WEEK. Anything else
  fails. Only time bars.
- **Segments.** With a start, the client splits the window into segments and
  requests them in sequence. A failed request or a timeout fails the whole
  call. A contract that cannot be resolved is logged and skipped, so its
  answer is empty: `fill_catalog` refuses it.
- **Timestamps.** IB gives the bar open; the adapter stamps
  `ts_event = ts_init` = open + bar duration, the close. Bars come back
  sorted by `ts_event`. All intervals and roll dates are on the close.
- **Regular hours.** `use_rth` must equal the live data client's
  `use_regular_trading_hours`, or backtest and live bars differ.
- **Venue.** `Simplified` symbology takes the contract's IB exchange as the
  venue (`ESM4.CME`). For a MIC venue set `symbol_to_mic_venue` per root.
  Never set `convert_exchange_to_mic_venue`: it looks the exchange up in a
  `std::collections::HashMap` where `CME` belongs to four MICs (`XCME`,
  `XCEC`, `XFXS`, `GLBX`), so the venue can change from one process to the
  next.
- **Limits.** IB's own limits (duration per bar size, request pacing, how
  far back history goes) are **unverified**. Request modest windows (months
  for intraday bars).
- **Expired futures and depth.** `includeExpired: true` in a contract spec
  exists, but whether an expired contract loads end to end is
  **unverified**. IB documents history for expired futures only for about
  two years after expiry (**unverified** against a gateway), so IB as the
  source gives roughly two years of per-contract history, and that bounds
  the backtest. First manual check: request bars of contracts that expired
  one and three years ago. For more depth use another source (see "Other
  sources").
- **`CONTFUT`.** IB's own continuous future is no substitute for the
  per-contract bars, even for older history: it is one series with IB's roll
  rule and adjustment, which the Nautilus continuous future (contract bars
  plus our transitions) cannot use and live cannot trade. The client also
  drops the end date for it and returns a single segment anchored at now.
  Do not store it.

### Other sources

Databento (`nautilus-databento`, not used yet) has the same shape:
`DatabentoHistoricalClient::get_range_instruments(RangeQueryParams)` and
`get_range_bars(params, aggregation, timestamp_on_close)`, returning
`Vec<InstrumentAny>` and `Vec<Bar>`. A `databento.rs` next to `ib.rs` would
supply its own `fetch_bars`. Its ids are on `GLBX` unless
`use_exchange_as_venue` is set (see "Instrument ids across sources").

## rolls.rs

Nautilus stitches contracts into a continuous future but does not find the
rolls. This module finds them and renders them as the params that
`subscribe_bars` and `request_bars` take on an `INTERNAL` bar type. Pure
computation: bars in, values out.

1. **Input:** the catalog's `EXTERNAL` bars of each contract of one root,
   as `&[Bar]` per `InstrumentId`, read by the caller. A calendar rule also
   takes the contracts' expiries, as values.
2. **Detect** each roll between consecutive contracts: the
   `transition_time_ns`, and the two contracts' prices at that same moment.
   The rule is ours (for example the first close at which the next
   contract's volume exceeds the expiring one's); Nautilus has none.
3. **Render** the list as `nautilus_core::Params`.

### The params

```json
{
  "continuous_future_transitions": [
    {
      "transition_time_ns": 1710460800000000000,
      "pre_instrument_id":  "ESH4.CME",
      "post_instrument_id": "ESM4.CME",
      "pre_price":  "5001.00",
      "post_price": "4995.50"
    }
  ],
  "continuous_future_adjustment_mode": "BACKWARD_SPREAD",
  "first_pre_instrument_id": "ESH4.CME",
  "last_post_instrument_id": "ESM4.CME"
}
```

- `transition_time_ns`: a JSON integer, not a string.
- Prices: decimal strings, from `Price::as_decimal()` or `Display`, never
  from `f64`.
- `continuous_future_adjustment_mode`: optional, default `BACKWARD_SPREAD`.
- `first_pre_instrument_id`, `last_post_instrument_id`: optional bounds on
  which transitions accumulate.
- Build with `serde_json::json!` and `serde_json::from_value::<Params>`
  (needs `serde_json` and `rust_decimal` in `Cargo.toml`).

The engine rejects at request or subscribe time:

- an empty list, or times not **strictly increasing**;
- a broken chain: each `pre_instrument_id` must be the previous
  `post_instrument_id`;
- a contract on another venue than the target bar type;
- a non-`INTERNAL` target;
- non-positive prices in a ratio mode.

Segments: the first contract covers up to the first transition, each next
one from its transition to the next. A bar stamped exactly at
`transition_time_ns` belongs to the new contract. Pick roll times on bar
closes.

### Adjustment modes

`ContinuousFutureAdjustmentType` (`nautilus_model::enums`):

| Mode | Applied to segment k of N transitions |
|---|---|
| `BACKWARD_SPREAD` (default) | sum over i in [k, N) of (post_i - pre_i) |
| `FORWARD_SPREAD` | sum over i in [0, k) of (pre_i - post_i) |
| `BACKWARD_RATIO` | product over i in [k, N) of (post_i / pre_i) |
| `FORWARD_RATIO` | product over i in [0, k) of (pre_i / post_i) |

Backward modes keep the latest contract unadjusted and move old prices into
its frame; forward modes anchor on the first contract. Spreads keep price
differences (and can go negative over long histories); ratios keep returns.

### What every segment needs

- Each contract's instrument in the cache and its `EXTERNAL` bars available:
  in backtest, instruments added to the engine and bars in the catalog; in
  live, from the adapter or the catalog. A missing segment is a hole, not an
  error.
- The root (`ES.CME`) is not a real contract. If absent, the engine builds it
  from the first segment's `FuturesContract`.
- To build from bars rather than ticks, request the composite form
  `ES.CME-1-HOUR-LAST-INTERNAL@1-HOUR-EXTERNAL`, passed through the
  `bar_types` param since `request_bars` refuses a composite `bar_type`. A
  standard `INTERNAL` target with `LAST` prices is built from trade ticks.
  The actor-level request with `bar_types` is **unverified** until a test of
  ours passes.
- `limit` applies per segment, not to the total.

### Live rolls

`subscribe_bars` with the same params sets a timer per future transition and
switches contracts at each. The roll list must therefore already contain the
**next** roll when the node starts: detect it ahead from the calendar, since
a volume rule only knows the past. New dates need a restart. How the first
bar after a roll behaves when the new contract has not ticked yet is
**unverified**.

## Rules

- **The strategy config is the only source** of instruments and bar types;
  this module receives them as Nautilus types.
- **Only Nautilus types**: no parallel `Symbol`, `Bar` or `Roll` types
  beyond the params this module returns.
- **`fill_catalog` writes, nobody else** writes the catalog.
- **Sort, split, append**: sorted by `ts_init`, one bar type per file,
  resume from the stored range (`get_intervals`).
- **No node** for filling the catalog: historical clients only.
- **One source per instrument**, with the ids of the live broker.
- **Check before writing**: refuse an empty response or a foreign bar type.
- **Instruments before bars.**
- **Per-contract bars are the stored truth**: never write an adjusted series;
  continuous series are built at read time.
- **Rolls are values**: backtest and live pass the same params to the same
  engine call.
- **No clock and no I/O** in `rolls.rs`: time comes in as
  arguments.
- **Cadence checked against the adapter** before `fill_catalog` starts.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file:
  - `rolls.rs`: detection on small synthetic series (clean roll, a gap, no
    overlap); params checked as JSON (increasing times, contiguous chain,
    decimal strings, each mode).
  - `fill_catalog.rs`: on a temp catalog, with a closure for `fetch_bars`:
    bars written and read back; a re-run requests only after the last bar
    and writes no duplicate instrument; an empty response or a foreign bar
    type is an error; unsorted and duplicate bars are stored sorted and
    unique.
  - `ib.rs`: bar type to bar spec string; a cadence IB cannot serve is
    rejected before connecting.
- Integration tests in `tests/`, one node per file: a backtest of a
  continuous future over a fixture catalog and roll params, which also
  proves the engine accepts our params.
- Anything against a real gateway is a manual check: fill a small window of
  one contract on the paper gateway and read it back.
