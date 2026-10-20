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
  ib.rs            Interactive Brokers history: instruments, bars, quotes
  rolls.rs         roll dates -> continuous_future_transitions params
```

No node and no port of our own. A source is an adapter's historical
client, which returns Nautilus types (`Vec<InstrumentAny>`, `Vec<Bar>`,
`Vec<QuoteTick>`); `ParquetDataCatalog` is the storage.

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

Fills the catalog over one window with the bars of a list of bar types
and the quotes of the instruments (Nautilus `QuoteTick`s, which the
backtest fills orders on; see the backtest README). No node: a
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
    fetch_quotes: impl AsyncFnMut(InstrumentId, Timestamp, Timestamp) -> anyhow::Result<Vec<QuoteTick>>,
) -> anyhow::Result<()>
```

`fetch_bars` and `fetch_quotes` are the seam: `ib.rs` supplies them now,
another source others. No trait until a second source exists. Tests pass
closures with canned data. A fetch over `(from, to]` returns the items
whose `ts_init` lies in it; how the source makes them (IB's quotes come
from bars) is its own business.

**Series.** Bars are stored per bar type, quotes per instrument (one
series per id of `instruments`; their cadence is the source's, one a minute
for IB, see the IB quotes below). Both go through one generic algorithm (`fill_series`) over a
private `Series` trait implemented for `Bar` (key `BarType`) and
`QuoteTick` (key `InstrumentId`). Bar types run first, then quotes.

1. **Instruments.** Write those not yet in the catalog
   (`catalog.instruments(ids)`), so a re-run adds no file.
2. **Resume.** Per series, one at a time, from the stored range of that
   series (bars: `get_intervals` with the bar type string, so other bar
   types of the same instrument do not count; quotes: with the instrument
   id, under `data/quotes/`, apart from the bars). Steps 2 to 4 say "bar
   type" and "bar"; for quotes read "instrument" and "quote". A `start`
   before the stored range is an error naming the series and the range, raised before
   fetching: extending history backwards is not supported; use a new
   catalog. Otherwise the resume point is the end of the stored range;
   request from 1 ns after it to `end`, even when `start` is later, so no
   hole opens between the stored range and `start`. `start` counts only
   for a bar type's first fill and for the backwards check. A window
   already covered (request start at or after `end`) asks the source for
   nothing and is not an error.
3. **Check.** Refuse an empty response on a bar type's first fill (no
   stored range: a contract with no data is never silently missing) and
   any bar of another bar type. Once a bar type is stored, an empty
   response means nothing new (an expired contract, a weekend): nothing
   is written, no coverage is recorded, no error, and the next bar types
   run. Drop bars at or before the resume point, sort by `ts_init`, drop
   duplicate `ts_init`. If nothing is left, nothing is written and that is
   not an error.
4. **Write** one bar type per call with `write_to_parquet`, recording the
   file's interval as the window asked for, not the bars' own span: from
   the request start (or the oldest bar, if earlier) to `end` (or the
   newest bar, if later). The stored range then spans the windows already
   asked for, so a re-run with the same `start` and `end` asks for nothing,
   even when the bars start after `start` or close before `end`. A response whose bars were
   all dropped, or an empty one after a resume, records no coverage, so
   the next run asks for that tail again.

   Coverage is the window asked for, not what the source had. When the
   source has less (IB's history depth), the missing part is never asked
   again. That is deliberate: the historical client fails the whole call
   on a failed request, so a short answer that is not an error means the
   source has no more.

   **Quotes' coverage.** A fill over `[start, end]` records the quotes
   over `[start, end]` like bars, whatever their stamps. IB's quotes sit
   1 ns before their bar's close, so the last quote of a window ending on
   a close is at `end − 1 ns`, yet the interval reaches `end`; and since
   the fetch is over `(from, end]` in quote stamps, the quote of the bar
   closing just after `end` (stamped `end` itself) belongs to this fill,
   not the next. The backtest requires quotes over `[start − 1 ns, end]`
   of its run: start the fill (or the earlier fill the catalog resumes
   from) before the run, as the warmup already requires for bars.

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
{catalog}/data/quotes/ESM4.CME/{start}_{end}.parquet
{catalog}/data/futures_contract/ESM4.CME/{ts}_{ts}.parquet
```

### Reading back

```rust
catalog.bars(ids: Option<Vec<String>>, start: Option<UnixNanos>, end: Option<UnixNanos>)
    -> anyhow::Result<Vec<Bar>>                      // &mut self; ordered by ts_init
catalog.quote_ticks(ids, start, end) -> anyhow::Result<Vec<QuoteTick>>   // ids: instrument ids
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

Our wrapper, `IbHistory` (exported with `IbGateway` from `data`):

```rust
IbHistory::connect(gateway: IbGateway, bar_types: &[BarType], end: Timestamp, now: Timestamp)
    .await -> anyhow::Result<IbHistory>   // checks cadence and end before connecting
history.instruments(ids: &[InstrumentId]).await -> anyhow::Result<Vec<InstrumentAny>>
history.bars(bar_type, start, end).await -> anyhow::Result<Vec<Bar>>   // the fetch_bars of fill_catalog;
                                                                       // (start, end], sorted, unique
history.quotes(instrument_id, start, end)
    .await -> anyhow::Result<Vec<QuoteTick>>   // the fetch_quotes of fill_catalog; (start, end],
                                               // one a minute, sorted
```

- **Checked before connecting.** Every bar type must be one IB serves (see
  bar spec strings below, `EXTERNAL`, priced `LAST`/`BID`/`ASK`/`MID`), and
  `end` must be at least the longest bar before `now`: IB answers a window
  ending within one bar of now with the bar still in progress, and
  `fill_catalog` would store it and resume past it. `main` reads the clock.
- **Every instrument or none.** `request_instruments` answers an unknown or
  expired id with nothing and only a warning. `instruments` asks again for
  each missing future as an explicit contract (root symbol, local symbol,
  exchange, `includeExpired: true`), the form that resolved the expired
  `ESU5` on the paper gateway; then it fails naming the ids still missing,
  before any bar request.
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
  cache or clock. It needs a Tokio runtime and a running IB Gateway or TWS.
  The config is the data client config of the live README.
- **Instruments first.** Bars of a contract the provider does not hold come
  out with a fallback precision (5, 0). Call `request_instruments` with the
  config's ids before `request_bars`.
- **Bar spec strings** are `"{step}-{aggregation}-{price_type}"`, for example
  `"1-HOUR-LAST"`. The historical client parses SECOND, MINUTE, HOUR, DAY and
  WEEK only (no MONTH). IB itself serves 1/5/15/30 SECOND;
  1/2/3/5/10/15/20/30 MINUTE; 1/2/3/4/8 HOUR; 1 DAY; 1 WEEK. `ib.rs`
  accepts all but WEEK (see "Timestamps"). Anything else fails. Only time
  bars.
- **Walking back, not segments.** Given a start, the client splits a
  window into an `N D` segment ending at `end` and a seconds segment
  ending N x 24 h earlier. IB counts `D` in trading sessions, not 24 h, so
  bars between the two segments go missing without an error (paper
  gateway 2026-10-07: ESZ6 1-HOUR over `[10-05T00:00Z, 10-07T18:00Z]` lacked
  10-05 19:00-21:00). The live data client shares that segmentation
  (`data/convert.rs` in the adapter). `bars` never passes a start: it
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
- **Duration per request** (`request_duration`), by bar size, within IB's
  documented maximum duration per bar size (`S` seconds, `D` whole
  trading days, `Y` years). 1 MINUTE uses seconds, not `1 D`: `D` counts
  whole trading days, so a `1 D` request ending in a session's first
  minute answers that one bar and the walk back stops there; `86400 S`
  answers the 1,440 bars before the end, across sessions and weekends.

  | Bar size | Duration | Requests for a year (about 252 sessions) |
  |---|---|---|
  | 1 SECOND | `1800 S` | thousands: short windows only |
  | 5 SECOND | `3600 S` | thousands: short windows only |
  | 15 SECOND | `14400 S` | about 1500: short windows only |
  | 30 SECOND | `28800 S` | about 750: short windows only |
  | 1 MINUTE | `86400 S` (1,440 bars) | about 165 per bar type |
  | 2 MINUTE | `2 D` | about 126 |
  | 3, 5, 10 MINUTE | `5 D` | about 51 |
  | 15, 20 MINUTE | `10 D` | about 26 |
  | 30 MINUTE, 1/2/3/4/8 HOUR | `20 D` | about 13 |
  | 1 DAY | `1 Y` | 1 |

- **Pacing.** IB allows at most 60 historical requests in any ten minutes,
  no identical request within 15 seconds, and no more than six requests
  for the same contract within two seconds; a violation fails the request
  (IB error 162), which fails the run. Bars over 30 seconds are, per IB,
  paced softly (throttled rather than refused), small bars strictly. A
  year of 1-HOUR bars takes about 13 requests; windows of minute bars
  beyond a few months, or of second bars beyond a few hours, can reach the
  limit. Split such a fill into several runs (each resumes where the last
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
  rule: the adapter's `bar_close_from_open` returns the open for WEEK, so
  `ib.rs` refuses WEEK before connecting.
- **Full session, not regular hours.** The data client config built in
  `ib.rs` sets `use_regular_trading_hours: false`, and every bar request
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
  instrument id returns the bars (`ESU5.CME`, precision 2), so `bars`
  always asks by id; the contract-month form returned them under another id
  (`ES202509.CME`) at the fallback precision 5. **IB as the source gives
  about one year of expired ES contracts**, and that bounds the backtest;
  for more depth use another source (see "Other sources").
- **After expiry.** IB keeps answering with flat, zero-volume bars after a
  contract's last trade (`ESU5` on 2025-09-19: flat bars from 15:00 to
  21:00 UTC). Rolls happen before expiry, so a continuous series never
  uses them; a single-contract backtest through expiry would.
- **Verified end to end** (paper gateway, 2026-10-07, `fill-catalog` on
  `ESZ6.CME` and `ESU5.CME`, 1-HOUR-LAST): a fill reads back sorted and
  unique; a re-run asks for nothing; a window filled in two runs equals the
  same window filled in one (110 bars, identical); bars are stamped on the
  hour, 23 per day over the full session; an end after a contract's expiry
  works.
- **Xetra ETF (`SXR8.IBIS`).** Verified on the paper gateway (2026-10-08,
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
  - **BID, ASK and MID bars.** IB serves all three for SXR8 through
    `fill-catalog` (a config whose `bar_spec` has `price_type = "BID"`,
    `"ASK"` or `"MID"`; the EMA config accepts any price type): 1,440 bars
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
  (`quoted_bar_type`) with the same walk back as `bars`,
  and turns each pair of the same close into one `QuoteTick`
  (`quotes_from_bars`, pure and unit-tested): bid = BID close, ask = ASK
  close, at the bars' price precision (the instrument's, since the
  instruments are loaded first); size 1,000,000 on both sides at the bars'
  size precision (IB gives none; that size never limits an order);
  `ts_event = ts_init` = close − 1 ns, so the quote sorts before a bar of
  the same close and an order decided on that bar fills on its quote (see
  the backtest README, "Fills on quotes"). The quotes over `(start, end]`
  are thus those of the bars closing over `(start + 1 ns, end + 1 ns]`
  (`quoted_bars_window`). Dropped, counted and logged at `INFO`, not
  errors: a bar without the other side's bar of the same close, and a
  crossed pair (ask < bid, mostly the pre-opening auction); that minute
  then fills on the previous quote. A locked pair (ask = bid) is kept. Only
  `ib.rs` knows quotes come from bars; a source with quotes (Databento's
  `bbo-1m`) would return `QuoteTick`s directly.
- **Quote cadence: 1 minute, fixed.** `quotes` has no cadence parameter:
  the cadence is a detail of this source, not of the strategy or the CLI.
  One quote a minute lets the backtest fill a market order on the quote of
  the minute closing with its bar and check resting orders every minute,
  whatever the strategy's bar size; 1 minute is the finest bar size IB
  paces softly. `fill-catalog` stores the strategy's bars and these quotes,
  each resuming from its own stored range. The check before connecting
  covers the strategy's bar types; the minute bars close within the
  longest of them.
- **Cost of minute quotes.** Two 1-MINUTE bar types per instrument (BID
  and ASK), 1,440 bars per `86400 S` request: about 28 requests per
  instrument-month and 330 per instrument-year (see the duration table),
  at 3-4 s each, about 2.3 minutes per instrument-month and 27 minutes per
  instrument-year with no pacing error (soft throttling only). An
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
- **Sort, split, append**: sorted by `ts_init`, one bar type (or one
  instrument's quotes) per file, resume from the stored range
  (`get_intervals`).
- **No node** for filling the catalog: historical clients only.
- **One source per instrument**, with the ids of the live broker.
- **Check before writing**: refuse a foreign bar type or instrument, and
  an empty response on a series' first fill.
- **Instruments before bars and quotes.**
- **Quotes with the bars**: `fill-catalog` always stores both; quotes one
  a minute, whatever the bars' cadence.
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
    and writes no duplicate instrument; an empty first response or a
    foreign bar type is an error, an empty resumed response is not;
    unsorted and duplicate bars are stored sorted and unique. The same for
    quotes with a closure for `fetch_quotes` (per instrument, coverage over
    the window asked for, a foreign instrument refused, a minute-cadence
    source stored every minute and resumed with only the new minutes), and bars and
    quotes of one instrument resuming each from their own range. The bar
    tests' quote source answers a quote at the epoch, so quotes never
    fail them.
  - `ib.rs`: BID and ASK bars to quotes (prices and precision, the 1 ns
    stamp, the nominal size at the bars' size precision, a side without
    the other dropped, a crossed pair dropped and a locked one kept, empty
    input, different precisions an error), the bar window of a quote
    window and the 1-MINUTE BID and ASK bars quotes are made from; bar type to bar spec string and to request duration; a
    cadence IB cannot serve is rejected before connecting; the walk back
    over canned answers (no hole at the joins, stops at start, on an empty
    answer and on no progress, trims to `(start, end]`, sorted and
    unique); the data client config and the bar requests use the full
    session.
- Integration tests in `tests/`, one node per file: a backtest of a
  continuous future over a fixture catalog and roll params, which also
  proves the engine accepts our params.
- Anything against a real gateway is a manual check: fill a small window of
  one contract on the paper gateway and read it back.
