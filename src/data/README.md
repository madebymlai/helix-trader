# The data module

`data` is build step 1. It fills the Nautilus Parquet catalog that backtest
reads, with three things only: the instruments the strategies trade, the
series their bars are built from, and the funding of OKX swaps and X-Perps,
which backtests settle. The strategies' bars themselves are internal, built
by the Nautilus data engine (see `src/strategy/README.md`, "Bars built from
quotes"): from quotes (one a minute from IB) or, for a composite bar type,
from its source bars (OKX's 1-minute candles). The module adds what Nautilus
lacks: filling the catalog from a historical source, and the roll dates.

APIs were read in the `nautilus-*` crates at `0.66.0` and their signatures
and engine checks rechecked at the `develop` commit the build pins (see
`docs/ARCHITECTURE.md`, "Version"). **Unverified** marks what the sources
did not confirm. Examples use the future `ESM4.CME`, its
predecessor `ESH4.CME` and the continuous root `ES.CME`.

## Layout

```
data/
  mod.rs           declares the modules, exposes only what others call
  fill_catalog.rs  source-agnostic, any Nautilus data type: resume, check, sort, write
  ib/
    mod.rs         Interactive Brokers history: instruments, quotes
    gateway.rs     the IB account and the Docker gateway, started and logged in
    README.md      IB history: requests, the gateway, depths, tests
  okx/
    mod.rs         OKX history: instruments, the archive, the fill one archive month or day at a time
    archive.rs     a candle month or day file -> bars, a funding month file -> funding rates
    testdata/      samples of real archive files, a funding history page and real instruments
    README.md      OKX history: candles, funding, the archive, tests
  rolls.rs         roll dates -> continuous_future_transitions params
```

The module runs no node and opens no port of its own; the IB part starts the
Docker IB Gateway it connects to (see `ib/README.md`, "The gateway"). A
source is an adapter's historical client returning Nautilus types:
`Vec<InstrumentAny>`, and a series' data as `Vec<Data>` (IB's quotes, made
from bars; OKX's candles, as bars). `ParquetDataCatalog` is the storage.

```
strategy config ──► fill_catalog ──► ib/ ──(HistoricalInteractiveBrokersClient)──► IB Gateway (Docker)
        └──────────► okx/ ──► fill_catalog, one call per archive month or day ──(OKX public API, archive files)──► OKX
                         └──► ParquetDataCatalog ──► backtest
rolls ◄── catalog quotes of each contract ──► params for the INTERNAL bar type
```

Only Nautilus types cross this module: no parallel `Symbol`, `Bar` or `Roll`
types beyond the params `rolls.rs` returns. `fill_catalog` is the only
writer of the catalog.

## Instruments and bar types

Nautilus has no universe type, and neither do we: the strategy's data
requirements are the single source of the instruments (see
`src/strategy/README.md`). `main` reads them and passes Nautilus types to
`fill_catalog`: `Vec<InstrumentAny>` loaded from the source by id, and the
series the strategy's bar types are built from (`strategy::catalog_series`).
This module never names an instrument. The history window is a run argument
of `fill-catalog` (`--start`, `--end`), not config.

A series is a Nautilus data type and its catalog identifier, as the catalog
keys it:

| Strategy bar type | Series stored |
|---|---|
| `SXR8.IBIS-1-HOUR-MID-INTERNAL` | quotes of `SXR8.IBIS` |
| `X.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` (composite) | bars of `X.OKX-1-MINUTE-LAST-EXTERNAL` |
| `X.OKX-1-MINUTE-LAST-EXTERNAL` | bars of that bar type |
| `X.OKX-1-HOUR-LAST-INTERNAL` | trades of `X.OKX` (no source serves them yet) |

The rule is the data engine's: a composite bar type is built from its source
bars, an internal one priced LAST from trades, any other from quotes. `ib/`
serves quotes only, made from IB's `EXTERNAL` 1-MINUTE BID and ASK bars,
which it never stores. `okx/` serves only the bars of
`X.OKX-1-MINUTE-LAST-EXTERNAL`, OKX's 1-minute candles.

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

`BarSpecification::new` and `BarType::from(&str)` panic on bad input, so
anything read from config goes through the checked forms, which also
enforce the step rules:

| Aggregation | Step must |
|---|---|
| `SECOND`, `MINUTE` | divide 60, not equal 60 |
| `HOUR` | divide 24, not equal 24 |
| `MILLISECOND` | divide 1000, not equal 1000 |
| `MONTH` | divide 12 |
| `DAY`, `WEEK`, `YEAR` | any non-zero step |

So `60-MINUTE` is rejected: write `1-HOUR`.

The last token is the aggregation source. `EXTERNAL` bars are made by the
venue or adapter: what `request_bars` returns and `subscribe_bars` streams;
`ib/` requests them to make quotes and does not store them, while a
composite bar type's source bars are stored as they are. `INTERNAL` bars are
built by the Nautilus data engine from ticks or other bars: the strategies'
bars, from the stored quotes or source bars, and continuous futures.

A composite bar type builds a target from a finer bar type:
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

The symbol matches (`ESM4`); only the venue differs. No default setting
makes two sources agree: matching IB and Databento takes `XCME` on both.

A catalog holds one source per instrument, with the ids of the live broker.
Changing source or venue means new config ids and a refilled catalog.

The decade is not in the id. `ESM4` is June 2014 and June 2024 alike, in
both sources, and Nautilus has no option to add it. The catalog keeps the
two apart in time, but one run cannot hold both: the cache has one
instrument per id. With IB as the source it should not arise (about two
years of history, unverified). A longer history needs one catalog per
decade, or a symbol rewrite that breaks the match with the live broker.

## fill_catalog.rs

Fills the catalog over one window with the instruments and the series their
bars are built from (Nautilus `Data`: quotes, or bars for a composite bar
type; the backtest builds the strategies' bars from them and fills orders on
them, see `src/backtest/README.md`). A source's historical client is a plain
async call returning Nautilus types, the pattern the Nautilus docs use for
every source (a client or loader, then `catalog.write_*`). Filling the
catalog is its own process run.

### Why not a node

The generic history path in Rust is `DataClient` behind a `LiveNode`:
`DataClient::request_bars` returns `anyhow::Result<()>`, an acknowledgement
without data, and the bars reach an actor's
`on_historical_bars` through the data engine. That callback carries no
request id (an empty response arrives as an empty slice), and an `Err` from
it is only logged, so a failed fill would still exit cleanly. The historical
clients return the data and propagate errors with `?`.

### Nautilus does not persist responses

In Rust, Nautilus never writes a requested response to a catalog. The data
engine serves a request from a registered catalog and asks the adapter for
the missing intervals, but writes nothing back; the `update_catalog` param
only routes a request to the client. The streaming writer records published
live data only, and history responses are not published. Filling the
catalog is ours.

### Interface

```rust
pub async fn fill_catalog(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,           // from the source, before any data
    series: &[(NautilusDataType, String)],     // data type, catalog identifier
    start: Timestamp,
    end: Timestamp,
    fetch: impl AsyncFnMut(&NautilusDataType, &str, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()>
```

`fetch` is the seam: `ib/` supplies `IbHistory::data` over the whole window,
`okx/` one per archive month or day (see `okx/README.md`); tests pass
closures with canned data. A fetch over `[from, to]` returns the items of
the series whose `ts_init` lies in it, the closed interval stored
(Nautilus' catalog intervals are closed, the next contiguous when it starts
1 ns after the end). How the source makes the items (IB's quotes come from
bars) and at what cadence (one a minute for IB, see `ib/README.md`) is its
own business: no caller passes the cadence, and `ib/` checks `end` against
it before starting the gateway. A source refuses a data type it does not
serve.

The steps say quotes and instruments; bars go the same way, keyed by their
bar type instead of the instrument id.

1. **Instruments.** Write those not yet in the catalog
   (`catalog.instruments(ids)`), before any data, so a re-run adds no file.
2. **Resume.** Per instrument, one at a time, from the stored range of its
   quotes (`query_first_timestamp` and `query_last_timestamp` with the
   instrument id, under `data/quotes/`). A `start` before the stored range
   is an error naming the instrument and the range, raised before fetching:
   extending history backwards is not supported; use a new catalog.
   Otherwise request from 1 ns after the stored end to `end`, even when
   `start` is later, so no hole opens between the stored range and `start`.
   `start` counts only for an instrument's first fill and for the backwards
   check. A window already covered (request start at or after `end`) asks
   the source for nothing and is not an error.
3. **Check.** Refuse an empty response on an instrument's first fill (no
   stored range: a contract with no data is never silently missing) and any
   quote of another instrument. Once an instrument's quotes are stored, an
   empty response means nothing new (an expired contract, a weekend):
   nothing is written, no coverage is recorded, no error, and the next
   instruments run. Drop quotes at or before the resume point, sort by
   `ts_init`, drop duplicate `ts_init`. If nothing is left, nothing is
   written and that is not an error.
4. **Write** one series per call with `write_data_enum`, recording the
   file's interval as the window asked for, not the quotes' own span: from
   the request start (or the oldest quote, if earlier) to `end` (or the
   newest quote, if later). The stored range then spans the windows already
   asked for, so a re-run with the same `start` and `end` asks for nothing,
   even when the quotes start after `start` or end before `end`. A response
   whose quotes were all dropped, or an empty one after a resume, records no
   coverage, so the next run asks for that tail again.

Any error stops the run. What was written stays valid; a re-run resumes.

Coverage is the window asked for, not what the source had. When the source
has less (IB's history depth), the missing part is never asked again. That
is deliberate: the historical client fails the whole call on a failed
request, so a short answer that is not an error means the source has no
more.

A fill over `[start, end]` therefore records quotes over `[start, end]`,
whatever their stamps. IB's quotes sit 1 ns before their bar's close, so the
last quote of a window ending on a close is at `end − 1 ns`, yet the
interval reaches `end`; and since the fetch is over `(from, end]` in quote
stamps, the quote of the bar closing just after `end` (stamped `end`
itself) belongs to this fill, not the next. The backtest requires quotes
over `[start − 1 ns, end]` of its run, so start the fill (or the earlier
fill the catalog resumes from) at least the warmup before the run (see
`src/backtest/README.md`, "Coverage check").

### Writing

Inherent methods of `ParquetDataCatalog` (`nautilus_persistence::backend::parquet::catalog`):

```rust
ParquetDataCatalog::new(base_path: &Path, None, None, None, None)   // panics on a bad path
catalog.write_instruments(instruments: Vec<InstrumentAny>) -> anyhow::Result<Vec<PathBuf>>
catalog.write_data_enum(data: &[Data], start: Option<UnixNanos>, end: Option<UnixNanos>,
                        skip_disjoint_check: Option<bool>) -> anyhow::Result<()>
catalog.get_intervals(&NautilusDataType::QuoteTick.into(), Some(identifier))
    -> anyhow::Result<Vec<(u64, u64)>>             // stored [start, end] per file, sorted
catalog.query_first_timestamp(&data_type, Some(identifier))
    -> anyhow::Result<Option<u64>>                 // start of the first of those intervals
catalog.query_last_timestamp(&data_type, Some(identifier))
    -> anyhow::Result<Option<u64>>                 // end of the last of those intervals
```

`write_data_enum` groups `Data` by type, then by identifier (the bar type of
a bar, the instrument id of anything else), and writes each group with
`write_to_parquet` over the given interval: for quotes, the same file as
writing them directly.

The catalog's write constraints:

- `ts_init` ascending: sort before writing; a descent is an error.
- One series per file: `write_to_parquet` refuses items of mixed
  instruments or bar types, which is why `write_data_enum` groups first.
- Disjoint intervals. Each write makes one file named after its interval:
  the `start` and `end` passed, else the first and last `ts_init`. A new
  interval must not touch an existing one: a shared timestamp is an overlap
  and an error. Appending a later window works; rewriting an overlapping
  one fails. Never pass `skip_disjoint_check`: an overlap means a bug in the
  windows.
- A write whose file name already exists (the same interval again) is
  skipped with an info log and returns `Ok`, not an error.
- Instruments go one file per id, named by the instrument's `ts_init`; a
  later definition adds a file.

Layout on disk:

```
{catalog}/data/quotes/ESM4.CME/{start}_{end}.parquet
{catalog}/data/bars/{bar type}/{start}_{end}.parquet      // composite bar types' sources only
{catalog}/data/futures_contract/ESM4.CME/{ts}_{ts}.parquet
```

### Reading back

```rust
catalog.quote_ticks(ids: Option<Vec<String>>, start: Option<UnixNanos>, end: Option<UnixNanos>)
    -> anyhow::Result<Vec<QuoteTick>>                // &mut self; ids: instrument ids; ordered by ts_init
catalog.instruments(ids: Option<&[String]>, start, end) -> anyhow::Result<Vec<InstrumentAny>>
catalog.query_last_timestamp(..)                     // the stored end a re-run resumes from
```

### Choosing the source

`fill-catalog` picks each instrument's source from its venue
(`OkxHistory::serves`): OKX's archive for `….OKX` ids, IB for every other
venue. A config may hold both; IB's gateway starts only if an IB instrument
is in it.

- `ib/` fills IB instruments' quotes through the Docker IB Gateway it
  starts: see [`ib/README.md`](ib/README.md).
- `okx/` fills OKX swaps' and X-Perps' candles and funding from OKX's
  archive and API: see [`okx/README.md`](okx/README.md).

### Other sources

Databento (`nautilus-databento`, not used yet) has the same shape:
`DatabentoHistoricalClient::get_range_instruments(RangeQueryParams)` returns
`Vec<InstrumentAny>`, and
`get_range_quotes(RangeQueryParams, Some("bbo-1m".into()))` returns the
1-minute `QuoteTick`s (the schema defaults to `mbp-1`, so pass it). A `databento.rs` next to `ib/` would supply its
own `fetch`. Its ids are on `GLBX` unless `use_exchange_as_venue` is set
(see "Instrument ids across sources").

## rolls.rs

Nautilus stitches contracts into a continuous future but does not find the
rolls. This module finds them and renders them as the params that
`subscribe_bars` and `request_bars` take on an `INTERNAL` bar type. Not
built yet: continuous futures are out of scope for now, and this section
predates the quotes-only catalog.

`rolls.rs` is pure computation, with no clock and no I/O: prices and time
come in as arguments, values go out.

1. **Input:** the catalog's quotes of each contract of one root, per
   `InstrumentId`, read by the caller. A calendar rule also takes the
   contracts' expiries, as values.
2. **Detect** each roll between consecutive contracts: the
   `transition_time_ns`, and the two contracts' prices at that same moment.
   The rule is ours (for example the first close at which the next
   contract's volume exceeds the expiring one's); Nautilus has none.
3. **Render** the list as `nautilus_core::Params`.

Per-contract quotes are the stored truth: an adjusted series is never
written, continuous series are built at read time. Rolls are values, so
backtest and live pass the same params to the same engine call.

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
  from `f64` (the engine also parses JSON numbers; we do not send them).
- `continuous_future_adjustment_mode`: optional, default `BACKWARD_SPREAD`.
- `first_pre_instrument_id`, `last_post_instrument_id`: optional bounds on
  which transitions accumulate; each must appear in the chain.
- Build with `serde_json::json!` and `serde_json::from_value::<Params>`
  (needs `serde_json` and `rust_decimal` in `Cargo.toml`).

The engine rejects at request or subscribe time:

- an empty list, or times not strictly increasing;
- a broken chain: each `pre_instrument_id` must be the previous
  `post_instrument_id`;
- a contract or bound on another venue than the target bar type, or a
  bound absent from the chain;
- a non-`INTERNAL` target;
- non-positive prices in a ratio mode;
- on `subscribe_bars` only, a `bar_types` param: a subscription takes the
  target (composite or not) as its bar type.

The first contract covers up to the first transition, each next one from its
transition to the next. A bar stamped exactly at `transition_time_ns`
belongs to the new contract, so pick roll times on bar closes.

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

- Each contract's instrument in the cache and its source data available
  (`EXTERNAL` bars, trades or quotes, by the rule below): in backtest,
  instruments added to the engine and that data in the catalog; in
  live, from the adapter or the catalog. A missing segment is a hole, not an
  error. A request builds no synthetic bars over roll gaps and weekends;
  a subscription does unless the data engine config turns
  `time_bars_build_with_no_updates` off, as ours does (strategy README,
  "Hours without quotes").
- The root (`ES.CME`) is not a real contract. If absent, the engine builds it
  from the first segment's `FuturesContract`; if that is not in the cache
  it only logs a warning.
- To build from bars rather than ticks, request the composite form
  `ES.CME-1-HOUR-LAST-INTERNAL@1-HOUR-EXTERNAL` through the `bar_types`
  param (see "Bar type format"); each segment then requests its own
  contract's `EXTERNAL` bars. A standard `INTERNAL` target is built from
  trade ticks when priced `LAST`, from quotes otherwise (so a `MID` target
  builds from the quotes this catalog stores). The actor-level request with
  `bar_types` is **unverified** until a test of ours passes.
- `limit` applies per segment, not to the total.

### Live rolls

`subscribe_bars` with the same params sets a timer for the next transition,
switches contracts when it fires, then schedules the one after. The roll
list must therefore already contain the next roll when the node starts: detect it ahead from the calendar, since a
volume rule only knows the past. New dates need a restart. How the first bar
after a roll behaves when the new contract has not ticked yet is
**unverified**.

## Tests

Unit tests sit in a `#[cfg(test)] mod tests` at the bottom of each file.

- `fill_catalog.rs`, on a temp catalog with a closure for `fetch`:
  - only instruments and quotes on disk, quote files byte for byte those
    `write_to_parquet` makes; quotes read back per instrument, sorted and
    unique;
  - bars stored under their bar type and resumed after their range;
  - errors: data of another series (a quote of another instrument, another
    bar type), an empty first response (quotes or bars), a start before the
    stored range (refused before fetching); an empty resumed response is
    not an error;
  - coverage over the window asked for; a re-run requests only after each
    instrument's stored range, writes no duplicate instrument, and fetches
    a covered window not again;
  - a failure keeps the instruments filled before it;
  - a minute-cadence source stored every minute and resumed with only the
    new minutes.
- `rolls.rs`, once built: detection on small synthetic series (clean roll,
  a gap, no overlap); params checked as JSON (increasing times, contiguous
  chain, decimal strings, each mode).
- `ib/`, `ib/gateway.rs` and `okx/`: see their READMEs.

Integration tests live in `tests/`, one node per file. Planned with
`rolls.rs`: a backtest of a continuous future over a fixture catalog and
roll params, which also proves the engine accepts our params.
