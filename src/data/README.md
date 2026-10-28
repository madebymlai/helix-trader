# The data module

`data` is build step 1. It fills the Nautilus Parquet catalog that backtest
reads, with four things only: the instruments the strategies trade, the
series their bars are built from, the funding of OKX swaps and X-Perps,
which backtests settle, and the book and trades of CME instruments read as
signals (Databento's MBP-10 and trades). The strategies' bars themselves are internal, built
by the Nautilus data engine (see `src/strategy/README.md`, "Bars built from
quotes"): for a composite bar type from its source bars (IB's 1-minute
TRADES bars, OKX's 1-minute candles), otherwise from quotes (one per tick
from IC). The module fills the catalog from each instrument's historical source:
through a Nautilus node where the source's data client answers in a way
Nautilus writes back (Databento, IC; see "The node fill"), and by its own
writer where it does not yet (IB, OKX; see "fill_catalog.rs").

APIs were read in the `nautilus-*` crates at `0.66.0` and their signatures
and engine checks rechecked at the `develop` commit the build pins (see
`docs/ARCHITECTURE.md`, "Version"). **Unverified** marks what the sources
did not confirm. Examples use the future `ESM4.CME`.

## Layout

```
data/
  mod.rs           declares the modules, exposes only what others call
  node_fill.rs     the fill through a node: what the catalog lacks, the node, the actor asking one window at a time
  fill_catalog.rs  source-agnostic, any Nautilus data type: resume, check, sort, write (IB, OKX)
  databento/
    mod.rs         Databento of GLBX instruments: its data client for the node, cost quotes, the dataset's end
    publishers.json Nautilus' Databento publishers, read by the client
    README.md      Databento history through the node: series, cost quotes, periods, instrument ids, tests
  ib/
    mod.rs         Interactive Brokers history: instruments, 1-minute TRADES bars
    gateway.rs     the IB account and the Docker gateway, started and logged in
    README.md      IB history: requests, the gateway, depths, tests
  ic/
    mod.rs         IC Markets history: the session, instruments, tick quotes
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

Databento and IC fill through a Nautilus live node built for the run
(`node_fill.rs`): their data clients (Nautilus' `DatabentoDataClient`,
our `IcDataClient` in `live/ic`) answer its requests, and its data engine
writes the answers to the catalog. IB and OKX fill without a node: a
source is an adapter's historical client returning Nautilus types,
`Vec<InstrumentAny>` and a series' data as `Vec<Data>` (IB's 1-minute
bars, OKX's candles), written by `fill_catalog`; the IB part starts the
Docker IB Gateway it connects to (see `ib/README.md`, "The gateway").
Nautilus has no cTrader adapter, so `ic/` and `live/ic` are its own
client (see `ic/README.md`). `ParquetDataCatalog` is the storage.

```
strategy config ──► node_fill: a live node, one request at a time, update_catalog
        │              ├──► DatabentoDataClient ──(historical API)──► Databento (GLBX)
        │              └──► IcDataClient ──(cTrader Open API)──► IC Markets
        ├──────────► fill_catalog ──► ib/ ──(HistoricalInteractiveBrokersClient)──► IB Gateway (Docker)
        └──────────► okx/ ──► fill_catalog, one call per archive month or day ──(OKX public API, archive files)──► OKX
                         └──► ParquetDataCatalog ──► backtest
```

Only Nautilus types cross this module: no parallel `Symbol` or `Bar`
types.

## Instruments and bar types

Nautilus has no universe type, and neither do we: the strategy's data
requirements are the single source of the instruments (see
`src/strategy/README.md`). `main` reads them and passes Nautilus types on:
the series the strategy reads (`DataRequirements::catalog_series`), those
its bar types are built from (`strategy::catalog_series`) and the tick
series it declares, each keyed by its instrument id; to the node fill as
slices, each series on a contract over its slice
(`DataRequirements::slices`), to `fill_catalog` with the
`Vec<InstrumentAny>` loaded from the source by id.
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

## The node fill

`node_fill.rs` fills the catalog through a live node built for the run,
with the data clients of the sources asked and the catalog registered
(writable), and an actor asking for one window at a time.

```rust
pub fn missing(catalog: &ParquetDataCatalog, slices: &[SeriesSlice]) -> anyhow::Result<Vec<FillRequest>>;
pub struct FillRequest { pub data: FillData, pub instrument_id: InstrumentId, pub from: Timestamp, pub to: Timestamp }
pub enum FillData { Instrument, Quotes, Trades, Depth }
pub struct NodeClient { pub factory: Box<dyn DataClientFactory>, pub config: Box<dyn ClientConfig>, pub routing: RoutingConfig }
impl NodeFill {
    pub fn new() -> Self;
    pub fn add(&mut self, client: NodeClient, requests: &[FillRequest], period: impl Fn(FillData) -> SignedDuration);
    pub async fn run(self, catalog_dir: &Path, timeout: SignedDuration) -> anyhow::Result<()>;
}
```

- **What is missing.** `missing` asks the catalog what it lacks of each
  slice, by Nautilus' own count (`get_missing_intervals_for_request`,
  the call behind the data engine's catalog lookup): contract by contract,
  its definition over the span of what it lacks, then each series'
  missing intervals. A rolled root's contracts come each over its slice;
  `DataRequirements::slices` has already merged one contract's slices of
  a series from two tables, so no window is asked twice. A
  covered window asks for nothing: no node is built. Quotes, trades and
  book depths only (`FillData`): a source filling bars or funding through
  the node does not exist yet.
- **Windows.** `add` cuts each series request into windows of the
  source's period (`DatabentoHistory::period`: a day; `IcHistory::period`:
  a week), aligned on multiples
  of the period from the Unix epoch (a day: each UTC midnight), each
  ending 1 ns before the next: the most one request, and one catalog
  file, holds. A source with nothing missing is left out of the node.
- **The node.** Environment `Live`, trader `FILL-001`, no execution
  client, no cache database, no state saved. Databento's client is routed
  by venue (`GLBX`), IC's names its own. Neither opens a live feed:
  Databento's `connect` starts nothing (its gateway opens on a
  subscription only), IC's connects its cTrader session and loads its
  instruments.
- **The actor** asks for each request in order, the next once the one
  before is answered, with `update_catalog`: the data engine asks the
  client for what the catalog lacks of the window and writes the answer
  back over that window. The definition comes first: its price precision
  goes with each data request of the instrument (`price_precision`, so
  Databento's client asks for no definitions of its own). A definition
  already stored is served from the catalog; a new one is written once
  per id.
- **Errors.** The first failure stops the node, and `run` returns it,
  naming the request: a client answering with a failure (Databento's
  `RequestFailed`, IC's failure envelope; nothing of the window is written,
  even the rows it got), a request the client refuses, or no answer within
  the timeout (`REQUEST_TIMEOUT`, 30 minutes by the wall clock: Nautilus'
  requests have none). So fill-catalog exits non-zero. What was written
  before stays; a re-run asks only for what is still missing.
- **Coverage check.** Nautilus only logs a failed write-back, so the
  actor would see success: after the run, every window that came back
  with rows must be covered in the catalog, or `run` fails naming it.
  Temporary: to be deleted once nautilus_trader#5474 (write-back failure
  reported to the requester) is fixed.

What Nautilus records:

- An answer is stored over its window, whatever its rows' span, so a
  weekend inside a window is covered and never asked again.
- An empty answer records coverage only when it touches a stored interval
  (its start 1 ns after one's end, or its end 1 ns before one's start);
  otherwise nothing, and the next fill asks for it again. Asked in order,
  the empty windows after stored data (a weekend, a Saturday at the end of
  a fill) join it; empty windows before any stored data of the series
  (before IC's history, a contract's quiet first hours) are asked again on
  each fill, the last of them joining the data after it each time.
- A first fill with no data at all records nothing and is no error.
- One request at a time: two windows the catalog lacks, asked together,
  would each be asked whole, and an empty one would record nothing, since
  the window next to it is not stored yet; and windows meeting on an
  instant (`[d, d + 1 day]`) would both cover it, the second write failing
  as an overlap (`write_to_parquet`'s disjoint check).

### Memory

A window's rows are held by the client's response, the data engine
(without a copy for a window the catalog lacks entirely) and the
catalog's write, which copies them once more
(`write_grouped_to_parquet`). Three hours of COMEX gold MBP-10 are about a
million snapshots, about 1.1 GB decoded, twice that while written; a
whole day (about 7 million) would be some 7.5 GB, twice that while
written. The window, not the fill, bounds the memory, and a window is one
catalog file.

## fill_catalog.rs

Fills the catalog for IB and OKX, until nautilus_trader#5472 (IB) and
#5473 (OKX) are fixed and they fill through the node (see "Which sources
fill without a node"), over one window with the instruments and the
series their bars are built from (Nautilus `Data`: bars for a composite bar type, or
quotes; the backtest builds the strategies' bars from
them and fills orders on them, see `src/backtest/README.md`). A source's historical client is a plain
async call returning Nautilus types, the pattern the Nautilus docs use for
every source (a client or loader, then `catalog.write_*`). Filling the
catalog is its own process run.

### Which sources fill without a node

Nautilus' own path for history is a request to a data client behind a
node: the data engine serves what a registered catalog holds, asks the
client for the missing intervals, and, with the `update_catalog` param,
writes the answer back over the window asked for. It writes back only an
answer the client sends as an explicit completed response envelope
without a failure (`DataResponseEnvelope`; the engine's
`update_catalog_from_response`, `data/src/engine/streaming.rs`).
Databento's client and IC's do, so they fill through the node (see "The
node fill"). IB's and OKX's reply with the payload alone, which Nautilus
does not take as complete coverage, so they keep `fill_catalog` until
nautilus_trader#5472 (IB) and #5473 (OKX) are fixed.

### Interface

```rust
pub async fn fill_slices(
    catalog: &mut ParquetDataCatalog,
    instruments: &[InstrumentAny],             // the slices' contracts, from the source
    slices: &[SeriesSlice],                    // DataRequirements::slices, as the node fill's
    fetch: impl AsyncFnMut(&NautilusDataType, &str, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()>                         // one fill_catalog per slice, in order

pub async fn fill_catalog(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,           // from the source, before any data
    series: &[(NautilusDataType, String)],     // data type, catalog identifier
    start: Timestamp,
    end: Timestamp,
    fetch: impl AsyncFnMut(&NautilusDataType, &str, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()>
```

Every source gets the same shape from `fill-catalog`: the series slices
of its instruments (`DataRequirements::slices`). `ib/` passes its slices
to `fill_slices`; `okx/` fills each slice by its archive's periods.

`fetch` is the seam: `ib/` supplies `IbHistory::data` over each slice,
`okx/` one per archive month or day (see `okx/README.md`); tests pass
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
in it and, for IC and Databento, something of it is missing. The node runs
after OKX's fill and before IB's.

- `ib/` fills IB instruments' 1-minute TRADES bars through the Docker IB Gateway
  it starts: see [`ib/README.md`](ib/README.md).
- IC Markets' forex and metals instruments and their quotes, merged from
  tick history through Spotware's cTrader Open API, fill through the node
  by IC's data client (`live/ic`), a week a request: see
  [`ic/README.md`](ic/README.md). IC serves quotes only
  (`IcHistory::quoted`).
- `okx/` fills OKX swaps' and X-Perps' candles and funding from OKX's
  archive and API: see [`okx/README.md`](okx/README.md).
- GLBX instruments' series as declared (MBP-10 book as `OrderBookDepth`,
  trades, quotes), with their definitions, fill through the node by
  Nautilus' Databento data client, a day a request, after `databento/` checks the dataset's end and
  logs Databento's quote for every request: see
  [`databento/README.md`](databento/README.md). The GLBX series come from
  `DataRequirements::slices`, so a rolled root (`GC.GLBX`) is filled
  contract by contract, each over its slice. A backtest replays the book
  of each instrument the strategy lists, from the catalog
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
    new minutes;
  - `fill_slices`: each slice fetched over its window on its contract, its
    instrument stored; a slice of a contract not loaded refused after the
    slices before it, which stay stored.
- `node_fill.rs`, on temp catalogs: the windows of a period, cut on its
  multiples from the epoch (three hours, a day ending at midnight); what an
  empty catalog lacks (a definition, then each series over its slice, a
  rolled root's contracts each over its own), only what is outside a
  stored interval, nothing (not even a definition) for a covered window;
  bars refused, naming the
  series; the coverage check failing a window answered with rows but not
  stored, not a definition or an empty window.
- `tests/node_fill/`, fill-catalog's fill at its seam: `missing`, then a
  `NodeFill` whose live node holds a fake data client answering as
  Databento's, and the catalog:
  - a day a request, each written back, the definition first and its
    price precision on each data request, the empty weekend after Friday
    joining it; a wider fill asking only the new day (the definition from
    the catalog); the same fill again building no node;
  - a client answering 50 ms later asked one window at a time, in order;
    empty windows before any stored data recorded only once they touch it;
  - a failed request (`RequestFailed`, a failure envelope, a refused
    request) failing the fill naming the request and its reason, writing
    nothing, asking nothing after it; a silent client failing it after the
    timeout;
  - rows the engine does not write back (another contract's) failing the
    coverage check; a covered window asking nothing.
- `ib/`, `ib/gateway.rs`, `ic/`, `okx/` and `databento/`: see their READMEs.
