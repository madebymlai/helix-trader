# The data module

`data` is build step 1. It fills the Nautilus Parquet catalog that backtest
reads, with four things only: the instruments the strategies trade, the
series their bars are built from, the funding of OKX swaps and X-Perps,
which backtests settle, and the book and trades of CME instruments read as
signals (Databento's MBP-10 and trades). The strategies' bars themselves are internal, built
by the Nautilus data engine (see `src/strategy/README.md`, "Bars built from
quotes"): for a composite bar type from its source bars (IB's 1-minute
TRADES bars, OKX's 1-minute candles), otherwise from quotes (one per tick
from IC). The module adds what Nautilus lacks: filling the catalog from a
historical source.

APIs were read in the `nautilus-*` crates at `0.66.0` and their signatures
and engine checks rechecked at the `develop` commit the build pins (see
`docs/ARCHITECTURE.md`, "Version"). **Unverified** marks what the sources
did not confirm. Examples use the future `ESM4.CME`.

## Layout

```
data/
  mod.rs           declares the modules, exposes only what others call
  fill_catalog.rs  source-agnostic, any Nautilus data type: resume, check, sort, write
  databento/
    mod.rs         Databento history of GLBX instruments: definitions, MBP-10, trades, a day per request
    publishers.json Nautilus' Databento publishers, read by the client
    README.md      Databento history: schemas, instrument ids, resume, cost, tests
  ib/
    mod.rs         Interactive Brokers history: instruments, 1-minute TRADES bars
    gateway.rs     the IB account and the Docker gateway, started and logged in
    README.md      IB history: requests, the gateway, depths, tests
  ic/
    mod.rs         IC Markets history: the session, instruments, the fill
    account.rs     the account checked (hedging, IC_ACCOUNT_ID's) and its state
    config.rs      the environment and the credential, from IC_*
    messages.rs    Spotware's protobuf messages, their payload types, the envelope
    parse.rs       cTrader symbols -> CurrencyPair, ids
    ticks.rs       tick history: decoding, paging, the merge into quotes, their size
    token.rs       the token pair and the user-only token file that keeps the latest one
    websocket/     the cTrader connection: outer client, handler task, spot subscriptions, token renewal
    testing.rs     a mock cTrader peer (a local WebSocket server) for the tests
    proto/         Spotware's four .proto files, vendored (build.rs compiles them)
    testdata/      responses captured from IC's demo, scrubbed, text and wire forms
    README.md      IC history: the cTrader session, credentials, instruments, tick quotes, the account, tests
  okx/
    mod.rs         OKX history: instruments, the archive, the fill one archive month or day at a time
    archive.rs     a candle month or day file -> bars, a funding month file -> funding rates
    testdata/      samples of real archive files, a funding history page and real instruments
    README.md      OKX history: candles, funding, the archive, tests
```

The module runs no node and opens no port of its own; the IB part starts the
Docker IB Gateway it connects to (see `ib/README.md`, "The gateway"). A
source is an adapter's historical client returning Nautilus types:
`Vec<InstrumentAny>`, and a series' data as `Vec<Data>` (IB's 1-minute
bars and OKX's candles, as bars; IC's quotes, merged from ticks). Nautilus
has no cTrader adapter, so `ic/` is its own client (see `ic/README.md`).
`ParquetDataCatalog` is the storage.

```
strategy config ──► fill_catalog ──► ib/ ──(HistoricalInteractiveBrokersClient)──► IB Gateway (Docker)
        ├──────────► ic/ ──► fill_catalog ──(cTrader Open API, protobuf over WebSocket)──► IC Markets
        └──────────► okx/ ──► fill_catalog, one call per archive month or day ──(OKX public API, archive files)──► OKX
                         └──► ParquetDataCatalog ──► backtest
```

Only Nautilus types cross this module: no parallel `Symbol` or `Bar`
types. `fill_catalog` writes the catalog, except Databento's history,
which `databento/` writes day by day (see "Choosing the source").

## Instruments and bar types

Nautilus has no universe type, and neither do we: the strategy's data
requirements are the single source of the instruments (see
`src/strategy/README.md`). `main` reads them and passes Nautilus types to
`fill_catalog`: `Vec<InstrumentAny>` loaded from the source by id, and the
series the strategy reads (`DataRequirements::catalog_series`): those its
bar types are built from (`strategy::catalog_series`) and the tick series it
declares, each keyed by its instrument id.
This module never names an instrument. The history window is a run argument
of `fill-catalog` (`--start`, `--end`), not config.

A series is a Nautilus data type and its catalog identifier, as the catalog
keys it:

| Strategy bar type | Series stored |
|---|---|
| `SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` (composite) | bars of `SXR8.IBIS-1-MINUTE-LAST-EXTERNAL` |
| `X.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` (composite) | bars of `X.OKX-1-MINUTE-LAST-EXTERNAL` |
| `X.OKX-1-MINUTE-LAST-EXTERNAL` | bars of that bar type |
| `XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL` | quotes of `XAUUSD.ICMARKETS` |
| `SXR8.IBIS-1-HOUR-MID-INTERNAL` | quotes of `SXR8.IBIS` (no source serves them) |
| `X.OKX-1-HOUR-LAST-INTERNAL` | trades of `X.OKX` (no source serves them yet) |

The rule is the data engine's: a composite bar type is built from its source
bars, an internal one priced LAST from trades, any other from quotes.
`ib/` serves only IB's `EXTERNAL` 1-MINUTE-LAST bars (its TRADES bars),
stored as IB gives them; `ic/` quotes only, one per tick time, merged from
cTrader's BID and ASK ticks; `okx/` only the bars of
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
`ib/` and `okx/` store them as they are. `INTERNAL` bars are built by the
Nautilus data engine from ticks or other bars: the strategies' bars, from
the stored source bars (or quotes).

A composite bar type builds a target from a finer bar type:
`ESM4.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`. The target must be
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
bars are built from (Nautilus `Data`: bars for a composite bar type, or
quotes; the backtest builds the strategies' bars from
them and fills orders on them, see `src/backtest/README.md`). A source's historical client is a plain
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

### Nautilus persists only certified responses

The data engine serves a request from a registered catalog and asks the
adapter for the missing intervals; with the `update_catalog` param it
writes an adapter's answer back to the catalog, over the request's window,
but only an answer the adapter sends as an explicit completed response
envelope without a failure (`DataResponseEnvelope`; the engine's
`update_catalog_from_response`, `data/src/engine/streaming.rs`): IC's data
client does (`live/ic/README.md`), IB's and OKX's reply with the payload
alone, which Nautilus does not take as complete coverage. So a live node
fills the catalog with IC's quotes as its warmups fetch them, and nothing
else; filling it with a window of history, from any source, is ours.

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
`ic/` one per week (see `ic/README.md`, "The fill"), `okx/` one per
archive month or day (see `okx/README.md`); tests pass
closures with canned data. A fetch over `[from, to]` returns the items of
the series whose `ts_init` lies in it, the closed interval stored
(Nautilus' catalog intervals are closed, the next contiguous when it starts
1 ns after the end). The series' bar type states its cadence; `ib/`
serves 1-MINUTE-LAST bars only and checks `end` against the minute in progress
before starting the gateway (see `ib/README.md`). A source refuses a series
it does not serve.

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
   stored range: a contract with no data is never silently missing; the
   error names the series and the window asked for) and any
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

A fill over `[start, end]` therefore records a series over `[start, end]`,
whatever its stamps. IB's bars are stamped at their close, so the bar
closing at `end` belongs to this fill, not the next. The backtest requires
every series over `[start − 1 ns − warmup, end]` of its run, so start the
fill (or the earlier fill the catalog resumes from) at least the warmup
before the run (see `src/backtest/README.md`, "Coverage check").

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
(`OkxHistory::serves`, `DatabentoHistory::serves`, `IcHistory::serves`):
OKX's archive for `….OKX` ids, Databento for `….GLBX` ids, IC's cTrader
for `….ICMARKETS`, IB for every other venue. A config may hold several;
IB's gateway starts, and IC connects, only if one of their instruments is
in it.

- `ib/` fills IB instruments' 1-minute TRADES bars through the Docker IB Gateway
  it starts: see [`ib/README.md`](ib/README.md).
- `ic/` fills IC Markets' forex and metals instruments and their quotes,
  merged from tick history, through Spotware's cTrader Open API: see
  [`ic/README.md`](ic/README.md).
- `okx/` fills OKX swaps' and X-Perps' candles and funding from OKX's
  archive and API: see [`okx/README.md`](okx/README.md).
- `databento/` fills GLBX instruments' MBP-10 book (`OrderBookDepth`) and
  trades, with their definitions, from Databento's historical API, one
  UTC day per request after logging Databento's quotes. It takes the
  GLBX series of `DataRequirements::slices`, so a rolled root (`GC.GLBX`)
  is filled contract by contract, each over its slice. It writes the
  catalog itself rather than through `fill_catalog`, which drops items
  sharing a `ts_init` (book updates and trades often do) and holds a whole
  window in memory (a day of MBP-10 is about 7 GB decoded): see
  [`databento/README.md`](databento/README.md). A backtest replays the
  book of each instrument the strategy lists, from the catalog
  (`backtest/README.md`, "Book data").

## Tests

Unit tests sit in a `#[cfg(test)] mod tests` at the bottom of each file.

- `fill_catalog.rs`, on a temp catalog with a closure for `fetch`:
  - only instruments and quotes on disk, quote files byte for byte those
    `write_to_parquet` makes; quotes read back per instrument, sorted and
    unique;
  - bars stored under their bar type and resumed after their range;
  - errors: data of another series (a quote of another instrument, another
    bar type), an empty first response (quotes or bars, naming the window),
    a start before the
    stored range (refused before fetching); an empty resumed response is
    not an error;
  - coverage over the window asked for; a re-run requests only after each
    instrument's stored range, writes no duplicate instrument, and fetches
    a covered window not again;
  - a failure keeps the instruments filled before it;
  - a minute-cadence source stored every minute and resumed with only the
    new minutes.
- `ib/`, `ib/gateway.rs`, `ic/`, `okx/` and `databento/`: see their READMEs.
