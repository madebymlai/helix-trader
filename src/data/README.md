# The data module

`data` is build step 1. It fills the Nautilus Parquet catalog with the
instruments the strategy trades and the data their bars are built from,
and nothing else. The strategies' bars are internal, built by the
Nautilus data engine (see the strategy README): from quotes (one a minute
from IB), or, for a composite bar type, from its source bars (OKX's
1-minute candles). Backtest reads that catalog.
It adds what Nautilus does not provide: filling the catalog from a
historical source, and the roll dates.

APIs were read in the `nautilus-*` crates at `0.66.0`; the build uses the
`develop` commit in `docs/ARCHITECTURE.md` (Version). **Unverified** marks
what the sources did not confirm. Examples use the future `ESM4.CME`, its
predecessor `ESH4.CME` and the continuous root `ES.CME`.

## Layout

```
data/
  mod.rs           declares the modules, exposes only what others call
  fill_catalog.rs  source-agnostic, any Nautilus data type: resume, check, sort, write
  ib/
    mod.rs         Interactive Brokers history: instruments, quotes
    gateway.rs     the IB account and the Docker gateway, started and logged in
  okx/
    mod.rs         OKX history: instruments, the archive, the fill one archive month or day at a time
    archive.rs     a candle month or day file -> bars
    testdata/      samples of real archive files and real instruments
  rolls.rs         roll dates -> continuous_future_transitions params
```

No node and no port of our own. The IB part starts the Docker IB Gateway
it connects to (see "The gateway" below). A source is an adapter's historical
client, which returns Nautilus types (`Vec<InstrumentAny>`, and the data
of a series as `Vec<Data>`: IB's quotes, made from bars; OKX's
candles, as bars);
`ParquetDataCatalog` is the storage.

```
strategy config ──► fill_catalog ──► ib/ ──(HistoricalInteractiveBrokersClient)──► IB Gateway (Docker)
        └──────────► okx/ ──► fill_catalog, one call per archive month or day ──(OKX public API, archive files)──► OKX
                         └──► ParquetDataCatalog ──► backtest
rolls ◄── catalog quotes of each contract ──► params for the INTERNAL bar type
```

## Instruments and bar types

Nautilus has no universe type, and neither do we. The strategy's data
requirements are the single source of the instruments (see the strategy
README). `main` reads them and passes Nautilus types to `fill_catalog`:
`Vec<InstrumentAny>` loaded from the source by id, and the series the
strategy's bar types are built from (`strategy::catalog_series`). This
module never names an instrument. The history window is a run argument of
`fill-catalog` (`--start`, `--end`), not config.

A series is a Nautilus data type and its catalog identifier, as the
catalog keys it:

| Strategy bar type | Series stored |
|---|---|
| `SXR8.IBIS-1-HOUR-MID-INTERNAL` | quotes of `SXR8.IBIS` |
| `X.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL` (composite) | bars of `X.OKX-1-MINUTE-LAST-EXTERNAL` |
| `X.OKX-1-MINUTE-LAST-EXTERNAL` | bars of that bar type |
| `X.OKX-1-HOUR-LAST-INTERNAL` | trades of `X.OKX` (no source serves them yet) |

The rule is the data engine's: a composite bar type is built from its
source bars, an internal one priced LAST from trades, any other from
quotes. `ib/` serves quotes only, made from IB's `EXTERNAL` 1-MINUTE BID
and ASK bars, which it never stores. `okx/` serves the bars of
`X.OKX-1-MINUTE-LAST-EXTERNAL` only, OKX's 1-minute candles.

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

So `60-MINUTE` is rejected: write `1-HOUR`.

**Aggregation source**, the last token:

- `EXTERNAL`: bars made by the venue or adapter: what `request_bars`
  returns and `subscribe_bars` streams. `ib/` requests them to make
  quotes and does not store them; the source bars of a composite bar type
  are stored as they are.
- `INTERNAL`: bars built by the Nautilus data engine from ticks or other
  bars: the strategies' bars, from the stored quotes or source bars, and
  continuous futures.

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

Fills the catalog over one window with the instruments and the series
their bars are built from (Nautilus `Data`: quotes, or bars for a
composite bar type; the backtest builds the strategies' bars from them and
fills orders on them, see the backtest README). No node: a
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
    instruments: Vec<InstrumentAny>,           // from the source, before any data
    series: &[(NautilusDataType, String)],     // data type, catalog identifier
    start: Timestamp,
    end: Timestamp,
    fetch: impl AsyncFnMut(&NautilusDataType, &str, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()>
```

`fetch` is the seam: `ib/` supplies `IbHistory::data` over the whole
window, `okx/` one per archive month or day (see "OKX specifics"). Tests pass closures
with canned data. A fetch over `[from, to]` returns the items of the
series whose `ts_init` lies in it, the closed interval stored (Nautilus'
catalog intervals are closed, the next contiguous when it starts 1 ns
after the end); how the source makes them (IB's quotes come from bars) and
at what cadence (one a minute for IB, see the IB quotes below) is its own
business. A source refuses a data type it does not serve.

The steps below say quotes and instruments; bars go the same way, keyed
by their bar type instead of the instrument id.

1. **Instruments.** Write those not yet in the catalog
   (`catalog.instruments(ids)`), so a re-run adds no file.
2. **Resume.** Per instrument, one at a time, from the stored range of its
   quotes (`query_first_timestamp` and `query_last_timestamp` with the
   instrument id, under `data/quotes/`).
   A `start` before the stored range is an error naming the instrument and
   the range, raised before fetching: extending history backwards is not
   supported; use a new catalog. Otherwise the resume point is the end of
   the stored range; request from 1 ns after it to `end`, even when
   `start` is later, so no hole opens between the stored range and
   `start`. `start` counts only for an instrument's first fill and for the
   backwards check. A window already covered (request start at or after
   `end`) asks the source for nothing and is not an error.
3. **Check.** Refuse an empty response on an instrument's first fill (no
   stored range: a contract with no data is never silently missing) and
   any quote of another instrument. Once an instrument's quotes are
   stored, an empty response means nothing new (an expired contract, a
   weekend): nothing is written, no coverage is recorded, no error, and
   the next instruments run. Drop quotes at or before the resume point,
   sort by `ts_init`, drop duplicate `ts_init`. If nothing is left,
   nothing is written and that is not an error.
4. **Write** one series per call with `write_data_enum`,
   recording the file's interval as the window asked for, not the quotes'
   own span: from the request start (or the oldest quote, if earlier) to
   `end` (or the newest quote, if later). The stored range then spans the
   windows already asked for, so a re-run with the same `start` and `end`
   asks for nothing, even when the quotes start after `start` or end
   before `end`. A response whose quotes were all dropped, or an empty one
   after a resume, records no coverage, so the next run asks for that tail
   again.

   Coverage is the window asked for, not what the source had. When the
   source has less (IB's history depth), the missing part is never asked
   again. That is deliberate: the historical client fails the whole call
   on a failed request, so a short answer that is not an error means the
   source has no more.

   **Quotes' coverage.** A fill over `[start, end]` records the quotes
   over `[start, end]`, whatever their stamps. IB's quotes sit
   1 ns before their bar's close, so the last quote of a window ending on
   a close is at `end − 1 ns`, yet the interval reaches `end`; and since
   the fetch is over `(from, end]` in quote stamps, the quote of the bar
   closing just after `end` (stamped `end` itself) belongs to this fill,
   not the next. The backtest requires quotes over `[start − 1 ns, end]`
   of its run: start the fill (or the earlier fill the catalog resumes
   from) at least the warmup before the run, as the backtest's coverage
   check requires.

Any error stops the run. What was written stays valid; a re-run resumes.

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

`write_data_enum` groups `Data` by type, then by identifier (the bar type
of a bar, the instrument id of anything else), and writes each group with
`write_to_parquet` over the given interval: for quotes, the same file as
writing them directly.

Write constraints:

- **Ascending `ts_init`.** Sort before writing; a descent is an error.
- **One series per file.**
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

### Interactive Brokers specifics (history)

`ib/` wraps `HistoricalInteractiveBrokersClient`
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

Our wrapper, `IbHistory` (exported with `IbAccount` from `data`):

```rust
IbHistory::connect(client_id: i32, end: Timestamp, now: Timestamp)
    .await -> anyhow::Result<IbHistory>   // checks end, then starts or reuses
                                          // the paper gateway, then connects
history.instruments(ids: &[InstrumentId]).await -> anyhow::Result<Vec<InstrumentAny>>
history.data(&data_type, identifier, start, end)
    .await -> anyhow::Result<Vec<Data>>        // the fetch of fill_catalog: quotes only,
                                               // any other data type is an error
history.quotes(instrument_id, start, end)
    .await -> anyhow::Result<Vec<QuoteTick>>   // [start, end], one a minute, sorted
```

- **Checked before the gateway starts.** `end` must be at least a minute (the
  quote cadence, see below) before `now`: IB answers a window ending
  within one bar of now with the bar still in progress, and `fill_catalog`
  would store its quote and resume past it. `main` reads the clock.
- **Every instrument or none.** `request_instruments` answers an unknown or
  expired id with nothing and only a warning. `instruments` asks again for
  each missing future as an explicit contract (root symbol, local symbol,
  exchange, `includeExpired: true`), the form that resolved the expired
  `ESU5` on the paper gateway; then it fails naming the ids still missing,
  before any quote request.
- **Market data subscription.** Historical bars through the API need the
  Level 1 real-time subscription of the exchange (for ES, "CME Real-Time
  (NP,L1)") with "Market Data API access" enabled, shared with the paper
  account. Without it every bar request fails with IB error 2188, also for
  old windows and with delayed data (tested on the paper gateway).
- **Client id.** `fill-catalog` defaults to IB client id 10, not the adapter
  default 1, so it can run beside a live node.
- **Logging.** The adapter warns through `tracing`. `fill-catalog` runs no
  node, so `main` installs Nautilus' own logger and tracing bridge for that
  command, as a node would (`NAUTILUS_LOG`, `RUST_LOG`).
- **No node.** `connect` opens the IB socket through the adapter's shared
  client registry and initialises an instrument provider; no message bus,
  cache or clock. It needs a Tokio runtime. The config is the data client
  config of the live README.
- **The gateway.** `connect` first starts Nautilus' `DockerizedIBGateway`
  in paper mode, or reuses its running container
  (`nautilus-ib-gateway-paper`, port 4002), and waits for its login
  (`ib/gateway.rs`; details in the live README, "Gateway"). History always
  runs on paper, so it never needs a live login or 2FA. It needs Docker
  (`DOCKER_HOST` for a socket other than `/var/run/docker.sock`, such as
  podman's) and the paper login in `TWS_USERNAME` and `TWS_PASSWORD`, which
  `fill-catalog` also reads from `.env`. The container stays up afterwards;
  the next command reuses it. It takes port 4002 and logs in as the primary
  session, so an IB Gateway run by hand on paper must be closed first.
- **Instruments first.** `request_bars` fails a contract the provider does
  not hold (`Instrument … is missing from the provider`; up to 0.65.0 its
  bars came out at a fallback precision (5, 0)). Call `request_instruments`
  with the config's ids before `request_bars`.
- **Bar spec strings** are `"{step}-{aggregation}-{price_type}"`, for example
  `"1-MINUTE-BID"`, the `Display` of `BarSpecification` that the historical
  client splits on `-`. It parses SECOND, MINUTE, HOUR, DAY and WEEK only
  (no MONTH). `ib/` asks only for the 1-MINUTE BID and ASK bars quotes
  are made from (`quoted_bar_spec`).
- **Walking back, not segments.** Given a start, the client splits a
  window into an `N D` segment ending at `end` and a seconds segment
  ending N x 24 h earlier. IB counts `D` in trading sessions, not 24 h, so
  bars between the two segments go missing without an error (paper
  gateway 2026-10-07: ESZ6 1-HOUR over `[10-05T00:00Z, 10-07T18:00Z]` lacked
  10-05 19:00-21:00). The live data client shares that segmentation
  (`data/convert.rs` in the adapter; unchanged in 0.66.0 and the pinned
  commit, nautechsystems/nautilus_trader#5384 is open). `quoted_bars` never passes a start: it
  walks back from `end` (`walk_back`), each request with an explicit
  duration (one segment, `start_date_time: None`) ending at the oldest
  bar so far, so consecutive answers overlap by a bar and leave no hole.
  It stops once a bar at or before `start` arrived, on an empty answer, or
  when an answer brings nothing older than the request's end (no endless
  loop when IB has no more). It returns the bars in `(start, end]`,
  sorted by `ts_init`, duplicates at the joins dropped. An empty first
  answer is returned empty; `fill_catalog` decides whether that is an
  error. A failed request or a timeout fails the whole call. A contract
  that cannot be resolved is logged and skipped, so its answer is empty:
  `fill_catalog` refuses it on a first fill, and on a resumed window takes
  it as nothing new.
- **Duration per request** (`QUOTED_BARS_PER_REQUEST`): `86400 S`, within
  IB's documented maximum duration for 1-MINUTE bars. Seconds, not `1 D`:
  `D` counts whole trading days, so a `1 D` request ending in a session's
  first minute answers that one bar and the walk back stops there;
  `86400 S` answers the 1,440 bars before the end, across sessions and
  weekends. About 165 requests per price type for a year (about 252
  sessions).

- **Pacing.** IB allows at most 60 historical requests in any ten minutes,
  no identical request within 15 seconds, and no more than six requests
  for the same contract within two seconds; a violation fails the request
  (IB error 162), which fails the run. Bars over 30 seconds are, per IB,
  paced softly (throttled rather than refused), small bars strictly, so
  the 1-MINUTE bars quotes are made from are throttled at worst. A long
  fill can still be split into several runs (each resumes where the last
  stopped). Verified on the paper gateway (2026-10-09), SXR8 1-MINUTE
  LAST, BID and ASK: September and August 2026 back to back, 84 requests
  in about six minutes, no pacing error and no slowdown (3-4 s per
  request, 1,440 bars each); each month complete (930 bars a session);
  about 3.5 minutes per instrument-month for the three types, so about
  40 minutes per instrument-year.
- **Timestamps.** IB gives the bar open; the adapter stamps
  `ts_event = ts_init` = open + bar duration, the close. Bars come back
  sorted by `ts_event`. All intervals and roll dates are on the close.
  Verified on the paper gateway (2026-10-07): 1-HOUR bars stamp
  `ts_event = ts_init` = the bar close, on the hour. Weekly bars break the
  rule: the adapter's `bar_close_from_open` returns the open for WEEK;
  `ib/` asks for minute bars only.
- **Full session, not regular hours.** The data client config built in
  `ib/` sets `use_regular_trading_hours: false`, and every bar request
  passes that same setting as `use_rth`. With regular hours on (the adapter
  default), IB's first regular-hours bar of a day is partial (13:30-14:00
  UTC for ES) while the adapter stamps every bar at open + bar size, so
  that bar is stamped 14:30, half an hour after its close (seen on the
  paper gateway 2026-10-07: ESZ6 1-HOUR bars stamped 14:30 then 15:00).
  With the full session every bar is on the hour. Futures trade nearly
  around the clock, so the full session is also the real market. The live
  data client must use the same setting, or backtest and live bars differ.
- **Venue.** `Simplified` symbology takes the contract's IB exchange as the
  venue (`ESM4.CME`). For a MIC venue set `symbol_to_mic_venue` per root.
  Never set `convert_exchange_to_mic_venue`: it looks the exchange up in a
  `std::collections::HashMap` where `CME` belongs to four MICs (`XCME`,
  `XCEC`, `XFXS`, `GLBX`), so the venue can change from one process to the
  next.
- **Limits.** How far back IB's history goes is **unverified** beyond the
  expired futures below.
- **Expired futures and depth.** On the paper gateway (2026-10-07), an
  expired future resolves only as an explicit contract with
  `includeExpired: true`, and only back about 13 to 15 months: `ESZ5`
  (expired 10 months earlier) and `ESU5` (12.6 months) resolved; `ESM5`
  (15.5 months), `ESH5`, `ESZ4` and `ESZ3` did not, by id or by contract,
  although IB documents two years. Once loaded that way, `request_bars` by
  instrument id returns the bars (`ESU5.CME`, precision 2), so
  `quoted_bars` always asks by id; the contract-month form returned them under another id
  (`ES202509.CME`) at the fallback precision 5 (0.65.0). **IB as the source gives
  about one year of expired ES contracts**, and that bounds the backtest;
  for more depth use another source (see "Other sources").
- **After expiry.** IB keeps answering with flat, zero-volume bars after a
  contract's last trade (`ESU5` on 2025-09-19: flat bars from 15:00 to
  21:00 UTC). Rolls happen before expiry, so a continuous series never
  uses them; a single-contract backtest through expiry would.
- **Verified end to end** (paper gateway, 2026-10-07, `fill-catalog` on
  `ESZ6.CME` and `ESU5.CME`, 1-HOUR-LAST, when it still stored bars; the
  resume algorithm is the one quotes use now): a fill reads back sorted and
  unique; a re-run asks for nothing; a window filled in two runs equals the
  same window filled in one (110 bars, identical); bars are stamped on the
  hour, 23 per day over the full session; an end after a contract's expiry
  works.
- **Xetra ETF (`SXR8.IBIS`).** Verified on the paper gateway when
  `fill-catalog` still stored bars (2026-10-08,
  1-HOUR-LAST, 4,078 bars from 2025-10-01 to 2026-10-02; a fresh fill of
  2026-06-01 to 2026-10-01 was identical, and its re-run asked for
  nothing). Unlike ES:
  - The full session runs 07:30-23:00 Frankfurt time, so the UTC hours
    move with daylight saving: bars stamped 06:30-21:00 UTC in summer,
    07:30-22:00 UTC in winter, 16 a day (6 on 24 and 31 December).
    Continuous trading is 09:00-17:30: median volume per bar 3,800-9,200
    shares for the bars closing 10:00-18:00, 30-200 for the others.
  - The first bar of the day is a half-hour bar (07:30-08:00, equal to the
    first 30-MINUTE bar) stamped open + 1 h, at 08:30: 30 minutes after its
    close, the partial-bar effect of regular hours on ES. Every other bar
    is stamped at its close, on the hour.
  - IB's minimum tick is `0.0001`, so the instrument has price precision 4
    although trades print in whole cents.
  - **BID, ASK and MID bars.** IB serves all three for SXR8 (filled then
    through `fill-catalog` with a config's `bar_spec` price type): 1,440 bars
    each over 2026-06-01 to 2026-10-03 (paper gateway, 2026-10-08), the
    same 16 stamps a day as LAST (LAST: 1,434, six hours without a
    trade). Every BID, ASK and MID bar has **volume 0**, which the
    matching engine cannot use as is (see the backtest README). The MID
    close equals (bid close + ask close) / 2 within half a cent in
    continuous trading. Closes are aligned with LAST: the median gap
    between the LAST close and the mid close of the same stamp is 0.70
    bps, against 8 bps one hour off.
  - **Spread**, ask close − bid close per bar, over the same window (90
    bars per hour of the day; mid EUR 679-742). Continuous trading means
    the bars closing 10:00-17:00 Frankfurt time:

    | Bars | n | p10 | p25 | median | p75 | p90 | p99 |
    |---|---|---|---|---|---|---|---|
    | continuous trading, EUR | 720 | 0.02 | 0.02 | 0.04 | 0.08 | 0.10 | 0.20 |
    | continuous trading, bps of mid | 720 | 0.28 | 0.28 | 0.56 | 1.11 | 1.43 | 2.84 |
    | outside, EUR | 720 | 0.16 | 0.28 | 0.36 | 0.50 | 0.74 | 1.67 |
    | outside, bps of mid | 720 | 2.19 | 3.92 | 4.99 | 7.11 | 10.45 | 23.82 |

    In continuous trading the mean is 0.054 EUR (0.76 bps), the spread is
    one cent in 1% of the bars, and every hour has a median of 0.4-0.6 bps except
    the bar closing 16:00 (1.14 bps, the US open). Outside, the evening
    bars (closing 18:00-23:00) have a median of 5.0 bps, the 23:00 bar
    10.3; the pre-opening bars (closing 08:30 and 09:00) have a median of
    4.6 bps and 54 crossed closes (ask below bid, the opening auction).
    So half the typical spread in continuous trading, the cost of a
    market order, is about 0.3 bps (2 cents on a 10-share order of EUR
    7,000: EUR 0.20).
- **Quotes from BID and ASK bars.** IB has no usable quote history, so
  `quotes` fetches the 1-MINUTE BID and ASK bars of the instrument
  (`quoted_bar_type`), walking back (`quoted_bars`), never stores them,
  and turns each pair of the same close into one `QuoteTick`
  (`quotes_from_bars`, pure and unit-tested): bid = BID close, ask = ASK
  close, at the bars' price precision (the instrument's, since the
  instruments are loaded first); size 1,000,000 on both sides at the bars'
  size precision (IB gives none; that size never limits an order);
  `ts_event = ts_init` = close − 1 ns, so the quote sorts before a bar of
  the same close and an order decided on that bar fills on its quote (see
  the backtest README, "Fills on quotes"; "The engine start" says why not
  Nautilus' bar build delay instead). The quotes over `[start, end]`
  are thus those of the bars closing over `[start + 1 ns, end + 1 ns]`
  (`quoted_bars_window`). Dropped, counted and logged at `INFO`, not
  errors: a bar without the other side's bar of the same close, and a
  crossed pair (ask < bid, mostly the pre-opening auction); that minute
  then fills on the previous quote. A locked pair (ask = bid) is kept. Only
  `ib/` knows quotes come from bars; a source with quotes (Databento's
  `bbo-1m`) would return `QuoteTick`s directly.
- **Quote cadence: 1 minute, fixed.** `quotes` has no cadence parameter:
  the cadence is a detail of this source, not of the strategy or the CLI.
  One quote a minute lets the backtest fill a market order on the quote of
  the minute closing with its bar and check resting orders every minute,
  whatever the strategy's bar size; 1 minute is the finest bar size IB
  paces softly. `fill-catalog` stores the instruments and these quotes
  only, resuming from their stored range: the strategies' bars are
  internal, built from the quotes (see the strategy README). The check
  before connecting uses this cadence too: `end` at least a minute before
  now.
- **Cost of minute quotes.** Two 1-MINUTE bar types per instrument (BID
  and ASK), 1,440 bars per `86400 S` request: about 28 requests per
  instrument-month and 330 per instrument-year.
  Measured below: about 1.8 minutes per instrument-month, so 20-25 minutes
  per instrument-year, with no pacing error (soft throttling only). An
  instrument-month of Xetra quotes is about 20,000 rows (930 minutes a
  session).
- **Quotes verified** on the paper gateway (2026-10-08, SXR8.IBIS
  1-HOUR, 2026-09-01 to 09-26): 304 BID and 304 ASK bars, no unpaired
  bar, 4 crossed pairs dropped (bid above ask by 0.16-3.48 EUR), 300
  quotes stored over `[09-01, 09-26]`, each equal to the BID and ASK
  closes of the bars filled separately, stamped close − 1 ns, precision 4,
  size 1,000,000 at precision 0. Of the 302 LAST bars, the 4 crossed
  hours have no quote. A re-run fetched nothing (no request in the debug
  log) and wrote nothing.
- **Minute quotes verified** on the paper gateway (2026-10-20, SXR8.IBIS,
  2026-08-24 to 10-01, 28 sessions, with the 1-HOUR LAST bars): 26,040
  BID and 26,040 ASK bars (930 a session), no unpaired bar, 99 crossed
  pairs dropped, 25,941 quotes stored one a minute; the whole fill took
  2 min 13 s, and a re-run fetched nothing. A backtest over September
  (EMA 5/15, Xetra's session) filled all 14 market orders at the bid
  (sells) or ask (buys) of the quote stamped 1 ns before the decision
  bar's close, the previous quote 60 s earlier.
- **`CONTFUT`.** IB's own continuous future is no substitute for the
  per-contract quotes, even for older history: it is one series with IB's roll
  rule and adjustment, which the Nautilus continuous future (contract bars
  plus our transitions) cannot use and live cannot trade. The client also
  drops the end date for it and returns a single segment anchored at now.
  Do not store it.

### Choosing the source

`fill-catalog` picks each instrument's source from its venue
(`OkxHistory::serves`): OKX's archive for `….OKX` ids, IB for every other
venue. A config may hold both; IB's gateway starts only if an IB
instrument is in it.

### OKX specifics (history)

`okx/` serves OKX swaps (`BTC-USDT-SWAP.OKX`) and X-Perps
(`BTC-USD_UM_XPERP-310404.OKX`, the contracts an EEA account trades): their
1-minute candles, stored as the bars of the instrument's
`1-MINUTE-LAST-EXTERNAL` bar type. A strategy trades OKX on a composite
bar type built from them, such as
`BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`; any other
series (quotes, trades, other bars) is refused before any request. It
uses OKX's public endpoints only, with no key.

```rust
OkxHistory::new() -> anyhow::Result<OkxHistory>
history.fill(&mut catalog, series: &[(NautilusDataType, String)], start, end)
    .await -> anyhow::Result<()>        // per series: its instrument, then bars month by month, then day by day
```

- **Instruments.** The symbol gives the type (Nautilus'
  `okx_instrument_type_from_symbol`: `…-SWAP` a swap, `…-310404` a
  future) and the family (the symbol without its last part: `BTC-USDT`,
  `BTC-USD_UM_XPERP`). `OKXRawHttpClient::get_instruments`
  (`/public/instruments`) gives OKX's definition; `parse_instrument_any`
  turns a swap into a `CryptoPerpetual` and a future into a
  `CryptoFuture` (contract value as the multiplier, tick and lot sizes as
  increments). A dated future is refused.
- **Margins of 1.** The adapter leaves the margins at 0, which would lock
  nothing; `okx/` stores 1 for both. OKX locks the position's value
  divided by the leverage, and the leverage is the account's choice, not
  the instrument's: the backtest takes it as `--leverage` and Nautilus
  divides the value by it, times the instrument's margin (see the backtest
  README, "Leverage"). OKX's own maintenance rates (BTC X-Perp: 0.65%)
  only decide liquidation, which the backtest does not simulate, so they
  are not stored.
- **X-Perps.** A `CryptoFuture` expiring in 2031, linear, settled in USD
  (Nautilus' USD, two decimals). BTC: 0.0001 BTC a contract, whole
  contracts, tick 0.1. Funding is not filled (out of scope, as for swaps).
- **The archive.** `GET /api/v5/public/market-data-history` (module 2,
  1-minute candles; `instType` `SWAP` or `FUTURES`, the family;
  `dateAggrType` `monthly` or `daily`; 5 requests per 2 s, kept to one
  every 600 ms) lists the files of up to 10 months, or 10 days, per
  request (OKX's errors 50077 and 50076 beyond), each keyed by its
  `dateTs`. A file is a zip of one CSV, about 1.2 MB a month and 0.04 MB
  a day for BTC, downloaded from `static.okx.com` (no quota) into memory.
  A swap has its own file (`BTC-USDT-SWAP-candlesticks-<month or day>.zip`); a
  future is in its family's chain file
  (`BTC-USD_UM_XPERP-futureschain-candlesticks-<month or day>.zip`), whose rows of
  other contracts are skipped. Every row is a confirmed candle: an
  unconfirmed one is an error.
- **Months and days of UTC+8.** The archive's month and day are those of
  UTC+8: the file of 2026-10-19 holds the 1,440 minutes opening from
  2026-10-18T16:00Z to 2026-10-19T15:59Z, that of 2026-09 the 43,200
  opening from 2026-08-31T16:00Z to 2026-09-30T15:59Z. Files are chosen
  by those, not by the UTC dates. A month's CSV holds the same rows as its
  days' CSVs (2026-09 of BTC-USDT-SWAP, checked 2026-10-23: byte for byte
  the 30 daily files' rows), so the same bars.
- **Bars as the adapter makes them.** Each row becomes the adapter's
  `OKXCandlestick` and its `parse_candlestick` makes the bar, at the
  instrument's precisions: the prices, and the volume in contracts (OKX's
  `vol`), of the bars Nautilus' OKX clients make from the API.
- **Stamped at the open, seen at the close.** `ts_event` is the candle's
  open, as OKX and the adapter stamp it; `ts_init` is its close (open +
  1 minute). This is Nautilus' convention for bars stamped at their open
  (Concepts › Backtesting › Bar execution, "Bar timestamp convention":
  `ts_init` must be the close, else the whole bar is visible before it
  formed), and what the adapter does for its own historical bars from the
  release after 0.66.0 (nautilus_trader#5339, commit `fc94ea1`, in the
  pinned commit); 0.66.0 stamped them at the request time. The backtest
  replays by `ts_init` and the data engine aggregates by it, so the hour bar closing
  10:00 holds the minutes opening 09:00 to 09:59 and is built at 10:00
  (`backtest_okx_swap.rs` checks both).
- **Months, then days.** `fill` calls `fill_catalog` once per archive
  month or day and series, over the bars closing in `(period start, period
  end]` (UTC+8 midnights), closed and contiguous with the next one's, as
  Nautilus' catalog intervals. From the month it resumes in, it reads
  monthly files, one listing per 10 months, up to the first month without
  one; from there daily files, one listing per 10 days. The first window
  starts with the day of `start`, which the files cover whole, so the
  stored range may begin before `start`. Each call resumes where the
  catalog stopped; a stopped run picks up after its last stored day, from
  the month's file if it has been published since. A period starting at
  `end` is left to the next run.
- **Archive start and lag.** No file on the first day of a fill (before
  the listing, or before the archive) is an empty first response, an
  error naming the series. OKX publishes a month's file on the first days
  of the next month (2026-09's on 2026-10-01) and a day's about two days
  after it ends; a later day with no file stops the series without
  recording it, and the next run asks for it again.
- **Catalogs filled before candles.** A catalog filled when `okx/` stored
  quotes rebuilt from the order book must be filled again, fresh, into a
  new directory: its quotes are no longer filled or extended, and nothing
  deletes them.
- **Candles against the API.** One archive day (2026-10-19, 1,440
  minutes) compared minute by minute with OKX's
  `/api/v5/market/history-candles` (`bar=1m`, public, 2026-10-22), for
  BTC-USDT-SWAP and BTC-USD_UM_XPERP-310404: the same 1,440 minutes on
  both, no difference in open, high, low, close, `vol`, `volCcy`,
  `volCcyQuote` or `confirm`.
- **Verified** (2026-10-22, release build, `fill-catalog` with an EMA
  config on each id, `--start 2026-10-13 --end 2026-10-21`): archive days
  10-13 to 10-20 of 1,440 bars each per instrument, then a stop at 10-21,
  not yet published; later that day 10-21 was out and the same fill took
  its first 480 minutes, up to `end`. About 6 s per instrument; the
  catalog holds about 50 KB a day per instrument. A re-run asked for
  nothing. Four fills back to back got no HTTP 429 at one listing every
  600 ms (one every 400 ms did, now and then).
- **Verified, months** (2026-10-23, release build, `fill-catalog` with an
  EMA config on BTC-USDT-SWAP.OKX, `--start 2024-10-23 --end
  2026-10-23`, a new catalog): 24 monthly files (2024-10 to 2026-09) and
  21 daily ones (2026-10-01 to 10-21), then a stop at 10-22, not yet
  published; 6 listings, no HTTP 429; 1,049,760 bars (729 days of 1,440)
  in 45 contiguous intervals, 34 MB, in 12.6 s. A re-run took 1.2 s (two
  listings, nothing new). The monthly files of those 24 months hold, byte
  for byte, the rows of their daily files.

### Other sources

Databento (`nautilus-databento`, not used yet) has the same shape:
`DatabentoHistoricalClient::get_range_instruments(RangeQueryParams)`
returns `Vec<InstrumentAny>`, and its quote requests take the 1-minute
`bbo-1m` schema and return `QuoteTick`s (per the Nautilus Databento docs;
the Rust method is **unverified**). A `databento.rs` next to
`ib/` would supply its own `fetch`. Its ids are on `GLBX` unless
`use_exchange_as_venue` is set (see "Instrument ids across sources").

## rolls.rs

Nautilus stitches contracts into a continuous future but does not find the
rolls. This module finds them and renders them as the params that
`subscribe_bars` and `request_bars` take on an `INTERNAL` bar type. Pure
computation: prices in, values out. Not built yet: continuous futures are
out of scope for now, and this section predates the quotes-only catalog.

1. **Input:** the catalog's quotes of each contract of one root, per
   `InstrumentId`, read by the caller. A calendar rule also takes the
   contracts' expiries, as values.
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

- **The strategy's data requirements are the only source** of
  instruments; this module receives them as Nautilus types.
- **Only Nautilus types**: no parallel `Symbol`, `Bar` or `Roll` types
  beyond the params this module returns.
- **`fill_catalog` writes, nobody else** writes the catalog.
- **Sort, split, append**: sorted by `ts_init`, one instrument's quotes
  per file, resume from the stored range (`query_first_timestamp`,
  `query_last_timestamp`).
- **No node** for filling the catalog: historical clients only.
- **One source per instrument**, with the ids of the live broker.
- **Check before writing**: refuse data of another series, and an
  empty response on a series' first fill.
- **Instruments before data.**
- **Instruments and the series the strategies' bars are built from**:
  quotes, or a composite bar type's source bars; nothing else.
- **Per-contract quotes are the stored truth**: never write an adjusted
  series; continuous series are built at read time.
- **Rolls are values**: backtest and live pass the same params to the same
  engine call.
- **No clock and no I/O** in `rolls.rs`: time comes in as
  arguments.
- **The cadence is the source's**: no caller passes it; `ib/` checks
  `end` against it before starting the gateway.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file:
  - `rolls.rs`: detection on small synthetic series (clean roll, a gap, no
    overlap); params checked as JSON (increasing times, contiguous chain,
    decimal strings, each mode).
  - `fill_catalog.rs`: on a temp catalog, with a closure for `fetch`:
    only instruments and quotes stored (no other kind of data on disk);
    quote files byte for byte those `write_to_parquet` makes; bars stored
    under their bar type and resumed after their range; data of another
    series (a quote, another bar type) or an empty first response for bars
    an error; quotes written and read back per instrument, sorted and
    unique; coverage over the window asked for; a re-run requests only
    after each instrument's stored range and writes no duplicate
    instrument; a covered window fetched not again; an empty first
    response or a quote of another instrument is an error, an empty
    resumed response is not; a failure keeps the instruments filled
    before it; a start before the stored range refused before fetching;
    a minute-cadence source stored every minute and resumed with only the
    new minutes.
  - `ib/`: BID and ASK bars to quotes (prices and precision, the 1 ns
    stamp, the nominal size at the bars' size precision, a side without
    the other dropped, a crossed pair dropped and a locked one kept, empty
    input, different precisions an error), the bar window of a quote
    window and the 1-MINUTE BID and ASK bars quotes are made from; an
    `end` within a minute of now rejected before the gateway starts; the walk back
    over canned answers (no hole at the joins, stops at start, on an empty
    answer and on no progress, trims to `(start, end]`, sorted and
    unique); the data client config and the bar requests use the full
    session; a series other than quotes refused.
  - `okx/`: the samples of a real swap file and a real X-Perp chain file
    (the first 61 minutes and the last of 2026-10-19) to bars: stamped at
    the open, seen at the close, prices and volume in contracts, the
    minutes of an hour; the first and last hours of 2026-09 from the
    swap's month file equal to those from its first and last days' files,
    and bounded by the month of UTC+8; on small hand-made files an unconfirmed candle
    named, the rows of other contracts skipped, a file without the
    contract an error; the real BTC swap and X-Perp to a `CryptoPerpetual`
    and a `CryptoFuture` with margins of 1, a dated future refused; the
    type and family of a symbol; only minute candles served; the archive
    listing of a day, of months and of a chain, keyed by period; months
    and days of UTC+8; 10 periods per listing; the fill with a fake
    archive (complete months from monthly files and the rest from daily
    files, contiguous windows; two years in 3 monthly and 3 daily
    listings; a failed day keeping the days before and the next run
    resuming from the month's file published since, as one uninterrupted
    fill; a start on a day's end; a day not published; no file on the
    first day).
  - `ib/gateway.rs`: account forms and their mode, the container config
    (the mode, the API open to orders), the login wait on fake statuses
    (ready at once, ready after a wait, the deadline naming the last
    status, a vanished container, a failed status).
- Integration tests in `tests/`, one node per file: a backtest of a
  continuous future over a fixture catalog and roll params, which also
  proves the engine accepts our params.
- Against the Docker paper gateway, `tests/ib_gateway.rs` (gated by
  `NAUTILUS_IB_RUN=1`, see the live README, "IB tests"): starting or
  reusing it, and a day of SXR8.IBIS quotes filled through it.
