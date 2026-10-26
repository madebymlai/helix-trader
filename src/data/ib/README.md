# Interactive Brokers history

`data/ib/` fetches IB instruments and quotes for `fill_catalog` (the
`fill-catalog` command, and `live` catching the catalog up before it
starts). IB has no usable quote history, so the quotes are made here from
1-MINUTE BID and ASK bars, one quote a minute; IB serves nothing else to
the catalog. Every request goes through the Docker paper gateway this
module starts or reuses.

Neighbouring docs: the fill algorithm (resume, coverage, writing) is in
`data/README.md`, "fill_catalog.rs"; the live data and execution clients,
their config and the gateway container are in `live/ib/README.md`.

## Layout

```
ib/
  mod.rs       IbHistory: connect, instruments, quotes; the data client config
  gateway.rs   IbAccount and IbGateway: the Docker gateway, started or reused
```

## Interface

The adapter's client, `HistoricalInteractiveBrokersClient`
(`nautilus_interactive_brokers::historical`):

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
IbHistory::latest_end(now: Timestamp) -> anyhow::Result<Timestamp>  // now − 1 minute
history.instruments(ids: &[InstrumentId]).await -> anyhow::Result<Vec<InstrumentAny>>
history.data(&data_type, identifier, start, end)
    .await -> anyhow::Result<Vec<Data>>        // the fetch of fill_catalog: quotes only,
                                               // any other data type is an error
history.quotes(instrument_id, start, end)
    .await -> anyhow::Result<Vec<QuoteTick>>   // [start, end], one a minute, sorted
```

## Connecting

### The gateway

`connect` first starts Nautilus' `DockerizedIBGateway` in paper mode, or
reuses its running container, and waits for its login (`gateway.rs`; the
container, credentials, `DOCKER_HOST` and the other-session rules are in
`live/ib/README.md`, "Gateway"). History always runs on paper, even for a
live account, so it never needs a live login or 2FA. `fill-catalog` also
reads `TWS_USERNAME` and `TWS_PASSWORD` from `.env`.

`connect` checks `end` before the gateway starts: `end` must be at or
before `latest_end(now)`, a minute (the quote cadence) before now, because
IB answers a window ending within one bar of now with the bar still in
progress, and `fill_catalog` would store its quote and resume past it.
`main` reads the clock.

### No node

`connect` opens the IB socket through the adapter's shared client registry
and initialises an instrument provider; no message bus, cache or clock. It
needs a Tokio runtime. Its config is the data client config of
`live/ib/README.md` (`data_client_config`), with the full session (see
"Full session, not regular hours").

The adapter warns through `tracing`. `fill-catalog` runs no node, so `main`
installs Nautilus' own logger and tracing bridge for that command, as a
node would (`NAUTILUS_LOG`, `RUST_LOG`); see `docs/ARCHITECTURE.md`,
"Nautilus v2 constraints".

`fill-catalog` defaults to IB client id 10, not the adapter default 1, so
it can run beside a live node.

### Market data subscription

Historical bars through the API need the exchange's Level 1 real-time
subscription (for ES, "CME Real-Time (NP,L1)") with "Market Data API
access" enabled, shared with the paper account. Without it every bar
request fails with IB error 2188, also for old windows and with delayed
data (tested on the paper gateway).

## Instruments

`request_instruments` answers an unknown or expired id with nothing and
only a warning, so `instruments` loads every instrument or fails: it asks
again for each missing future as an explicit contract (root symbol, local
symbol, exchange, `includeExpired: true`), the form that resolved the
expired `ESU5` on the paper gateway, then fails naming the ids still
missing, before any quote request.

Load instruments before requesting bars. Given an instrument id,
`request_bars` first tries to fetch its contract details itself and skips,
with a warning, an id it cannot resolve; a contract whose instrument the
provider still does not hold fails the call (`Instrument … is missing from
the provider`; up to 0.65.0 its bars came out at a fallback precision
(5, 0)).

`Simplified` symbology takes the contract's IB exchange as the venue
(`ESM4.CME`); for a MIC venue set `symbol_to_mic_venue` per root. Never set
`convert_exchange_to_mic_venue`: it looks the exchange up in a
`std::collections::HashMap` where `CME` belongs to four MICs (`XCME`,
`XCEC`, `XFXS`, `GLBX`), so the venue can change from one process to the
next.

## Quotes from BID and ASK bars

`quotes` fetches the 1-MINUTE BID and ASK bars of the instrument
(`quoted_bar_type`) by walking back (`quoted_bars`, see "Walking back, not
segments"), never stores them, and turns each pair of the same close into
one `QuoteTick` (`quotes_from_bars`, pure):

- bid = BID close, ask = ASK close, at the bars' price precision (the
  instrument's, since the instruments are loaded first);
- size 1,000,000 on both sides at the bars' size precision: IB gives none,
  and that size never limits an order;
- `ts_event = ts_init` = close − 1 ns, so the quote sorts before a bar of
  the same close and an order decided on that bar fills on its quote (see
  `backtest/README.md`, "Fills on quotes"; "The engine start" there says
  why not Nautilus' bar build delay instead).

The quotes over `[start, end]` are thus those of the bars closing over
`[start + 1 ns, end + 1 ns]` (`quoted_bars_window`). Two cases are dropped,
counted and logged at `INFO`, not errors, and that minute then fills on
the previous quote: a bar without the other side's bar of the same close,
and a crossed pair (ask < bid, mostly the pre-opening auction). A locked
pair (ask = bid) is kept.

Only `ib/` knows quotes come from bars; a source with quotes (Databento's
`bbo-1m`) would return `QuoteTick`s directly.

### Quote cadence: 1 minute, fixed

`quotes` has no cadence parameter: the cadence is a detail of this source,
not of the strategy or the CLI. One quote a minute lets the backtest fill a
market order on the quote of the minute closing with its bar and check
resting orders every minute, whatever the strategy's bar size; 1 minute is
the finest bar size IB paces softly. `fill-catalog` stores the instruments
and these quotes only; the strategies' bars are built from the quotes (see
`strategy/README.md`, "Bars built from quotes").

## Bar requests

Bar spec strings are `"{step}-{aggregation}-{price_type}"`, e.g.
`"1-MINUTE-BID"`, the `Display` of `BarSpecification`, which the historical
client splits on `-`. It parses SECOND, MINUTE, HOUR, DAY and WEEK only
(no MONTH). `ib/` asks only for the 1-MINUTE BID and ASK bars quotes are
made from (`quoted_bar_spec`).

### Walking back, not segments

`request_bars` takes a start or a duration, not both. Given a start, it
splits the window (`calculate_duration_segments`) into up to three
segments: whole 365-day years as `N Y` ending at `end`, the remaining days
as `M D` ending N x 365 x 24 h earlier, and the remaining seconds as `S`
ending M x 24 h before that. IB counts `D` in trading sessions, not 24 h,
so bars between the days and seconds segments go missing without an error
(paper gateway 2026-10-07: ESZ6 1-HOUR over `[10-05T00:00Z, 10-07T18:00Z]`
lacked 10-05 19:00-21:00). The live data
client shares that segmentation (`data/convert.rs` in the adapter; still
so at the pinned commit; nautechsystems/nautilus_trader#5384 is open).

So `quoted_bars` never passes a start. It walks back from `end`
(`walk_back`), each request one explicit duration (`start_date_time:
None`) ending at the oldest bar so far, so consecutive answers overlap by a
bar and leave no hole. It stops once a bar at or before `start` arrived, on
an empty answer, or when an answer brings nothing older than the request's
end (no endless loop when IB has no more). It returns the bars in
`(start, end]`, sorted by `ts_init`, duplicates at the joins dropped.

An empty first answer is returned empty; `fill_catalog` decides whether
that is an error. A failed request or a timeout fails the whole call. So
does an instrument id the adapter cannot resolve to a contract: it skips
the id with a warning, and with no contract left `request_bars` fails (`No
valid contracts found after conversion`), since `quoted_bars` asks for one
id at a time.

### Duration per request

`QUOTED_BARS_PER_REQUEST` is `86400 S`, within IB's documented maximum
duration for 1-MINUTE bars. Seconds, not `1 D`: `D` counts whole trading
days, so a `1 D` request ending in a session's first minute answers that
one bar and the walk back stops there; `86400 S` answers the 1,440 bars
before the end, across sessions and weekends. That is about 165 requests
per price type for a year (about 252 sessions), so about 28 requests per
instrument-month and 330 per instrument-year for BID and ASK together.

### Pacing

IB allows at most 60 historical requests in any ten minutes, no identical
request within 15 seconds, and no more than six requests for the same
contract within two seconds; a violation fails the request (IB error 162),
which fails the run. Per IB, bars over 30 seconds are paced softly
(throttled rather than refused) and smaller bars strictly, so the 1-MINUTE
bars are throttled at worst. A long fill can still be split into several
runs, each resuming where the last stopped.

Measured on the paper gateway (2026-10-09), SXR8 1-MINUTE LAST, BID and
ASK: September and August 2026 back to back, 84 requests in about six
minutes, no pacing error and no slowdown (3-4 s per request, 1,440 bars
each), each month complete (930 bars a session); about 3.5 minutes per
instrument-month for the three types, so about 40 minutes per
instrument-year. For quotes (BID and ASK only), the minute-quote run under
"Verified runs" gives about 1.8 minutes per instrument-month, so 20-25
minutes per instrument-year, soft throttling only. An instrument-month of
Xetra quotes is about 20,000 rows (930 minutes a session).

### Timestamps

IB gives the bar open; the adapter stamps `ts_event = ts_init` = open + bar
duration, the close. Bars come back sorted by `ts_event`. All intervals and
roll dates are on the close. Verified on the paper gateway (2026-10-07):
1-HOUR bars stamp `ts_event = ts_init` = the bar close, on the hour. Weekly
bars break the rule (the adapter's `bar_close_from_open` returns the open
for WEEK); DAY bars are stamped open + 1 day − 1 ns. `ib/` asks for minute
bars only.

### Full session, not regular hours

The data client config built in `ib/` sets
`use_regular_trading_hours: false`, and every bar request passes that same
setting as `use_rth`. With regular hours on (the adapter default), IB's
first regular-hours bar of a day is partial (13:30-14:00 UTC for ES) while
the adapter stamps every bar at open + bar size, so that bar is stamped
14:30, half an hour after its close (paper gateway 2026-10-07: ESZ6 1-HOUR
bars stamped 14:30 then 15:00). With the full session every bar is on the
hour. Futures trade nearly around the clock, so the full session is also
the real market. The live data client must use the same setting, or
backtest and live bars differ.

## History depth

How far back IB's history goes is **unverified** beyond the expired futures
below.

On the paper gateway (2026-10-07), an expired future resolves only as an
explicit contract with `includeExpired: true`, and only back about 13 to 15
months, although IB documents two years: `ESZ5` (expired 10 months earlier)
and `ESU5` (12.6 months) resolved; `ESM5` (15.5 months), `ESH5`, `ESZ4` and
`ESZ3` did not, by id or by contract. Once loaded that way, `request_bars`
by instrument id returns the bars (`ESU5.CME`, precision 2), so
`quoted_bars` always asks by id; the contract-month form returned them
under another id (`ES202509.CME`) at the fallback precision 5 (0.65.0).
**IB as the source gives about one year of expired ES contracts**, and that
bounds the backtest; for more depth use another source (see `data/README.md`,
"Other sources").

IB keeps answering with flat, zero-volume bars after a contract's last
trade (`ESU5` on 2025-09-19: flat bars from 15:00 to 21:00 UTC). Rolls
happen before expiry, so a continuous series never uses them; a
single-contract backtest through expiry would.

IB's own continuous future, `CONTFUT`, is no substitute for the
per-contract quotes, even for older history: it is one series with IB's
roll rule and adjustment, which the Nautilus continuous future (contract
bars plus our transitions) cannot use and live cannot trade. The client
also drops the end date for it (IB error 10339) and returns only the first
segment, anchored at now, with a warning. Do not store it.

## Xetra ETF (SXR8.IBIS)

Observed on the paper gateway when `fill-catalog` still stored bars
(2026-10-08, 1-HOUR-LAST, 4,078 bars from 2025-10-01 to 2026-10-02; a fresh
fill of 2026-06-01 to 2026-10-01 was identical, and its re-run asked for
nothing). Unlike ES:

- The full session runs 07:30-23:00 Frankfurt time, so the UTC hours move
  with daylight saving: bars stamped 06:30-21:00 UTC in summer, 07:30-22:00
  UTC in winter, 16 a day (6 on 24 and 31 December). Continuous trading is
  09:00-17:30: median volume per bar 3,800-9,200 shares for the bars
  closing 10:00-18:00, 30-200 for the others.
- The first bar of the day is a half-hour bar (07:30-08:00, equal to the
  first 30-MINUTE bar) stamped open + 1 h, at 08:30: 30 minutes after its
  close, the partial-bar effect of regular hours on ES, here in the full
  session. Every other bar is stamped at its close, on the hour.
- IB's minimum tick is `0.0001`, so the instrument has price precision 4
  although trades print in whole cents.

### BID, ASK and MID bars

IB serves all three for SXR8 (filled then through `fill-catalog` with a
config's `bar_spec` price type): 1,440 bars each over 2026-06-01 to
2026-10-03 (paper gateway, 2026-10-08), the same 16 stamps a day as LAST
(LAST: 1,434, six hours without a trade). Every BID, ASK and MID bar has
**volume 0**, which the matching engine cannot use as is (see
`backtest/README.md`, "Fills on BID and ASK bars"). The MID close equals
(bid close + ask close) / 2 within half a cent in continuous trading.
Closes are aligned with LAST: the median gap between the LAST close and the
mid close of the same stamp is 0.70 bps, against 8 bps one hour off.

### Spread

Ask close − bid close per bar, over the same window (90 bars per hour of
the day; mid EUR 679-742). Continuous trading means the bars closing
10:00-17:00 Frankfurt time:

| Bars | n | p10 | p25 | median | p75 | p90 | p99 |
|---|---|---|---|---|---|---|---|
| continuous trading, EUR | 720 | 0.02 | 0.02 | 0.04 | 0.08 | 0.10 | 0.20 |
| continuous trading, bps of mid | 720 | 0.28 | 0.28 | 0.56 | 1.11 | 1.43 | 2.84 |
| outside, EUR | 720 | 0.16 | 0.28 | 0.36 | 0.50 | 0.74 | 1.67 |
| outside, bps of mid | 720 | 2.19 | 3.92 | 4.99 | 7.11 | 10.45 | 23.82 |

In continuous trading the mean is 0.054 EUR (0.76 bps), the spread is one
cent in 1% of the bars, and every hour has a median of 0.4-0.6 bps except
the bar closing 16:00 (1.14 bps, the US open). Outside, the evening bars
(closing 18:00-23:00) have a median of 5.0 bps, the 23:00 bar 10.3; the
pre-opening bars (closing 08:30 and 09:00) have a median of 4.6 bps and 54
crossed closes (ask below bid, the opening auction). So half the typical
spread in continuous trading, the cost of a market order, is about 0.3 bps
(2 cents on a 10-share order of EUR 7,000: EUR 0.20).

## Verified runs

All on the paper gateway.

- **End to end on ES** (2026-10-07, `fill-catalog` on `ESZ6.CME` and
  `ESU5.CME`, 1-HOUR-LAST, when it still stored bars; the resume algorithm
  is the one quotes use now): a fill reads back sorted and unique; a re-run
  asks for nothing; a window filled in two runs equals the same window
  filled in one (110 bars, identical); bars are stamped on the hour, 23 per
  day over the full session; an end after a contract's expiry works.
- **Quotes at an hourly cadence** (2026-10-08, SXR8.IBIS 1-HOUR, 2026-09-01
  to 09-26): 304 BID and 304 ASK bars, no unpaired bar, 4 crossed pairs
  dropped (bid above ask by 0.16-3.48 EUR), 300 quotes stored over
  `[09-01, 09-26]`, each equal to the BID and ASK closes of the bars filled
  separately, stamped close − 1 ns, precision 4, size 1,000,000 at
  precision 0. Of the 302 LAST bars, the 4 crossed hours have no quote. A
  re-run fetched nothing (no request in the debug log) and wrote nothing.
- **Minute quotes** (2026-10-20, SXR8.IBIS, 2026-08-24 to 10-01, 28
  sessions, with the 1-HOUR LAST bars): 26,040 BID and 26,040 ASK bars (930
  a session), no unpaired bar, 99 crossed pairs dropped, 25,941 quotes
  stored one a minute; the whole fill took 2 min 13 s, and a re-run fetched
  nothing. A backtest over September (EMA 5/15, Xetra's session) filled all
  14 market orders at the bid (sells) or ask (buys) of the quote stamped
  1 ns before the decision bar's close, the previous quote 60 s earlier.

## Tests

- `ib/mod.rs`: BID and ASK bars to quotes (prices and precision, the 1 ns
  stamp, the nominal size at the bars' size precision, a side without the
  other dropped, a crossed pair dropped and a locked one kept, empty input,
  different precisions an error); the bar window of a quote window; the
  1-MINUTE BID and ASK bars quotes are made from; the latest end a minute
  before now, and an `end` within a minute of now rejected before the
  gateway starts; the walk back over canned answers (no hole at the joins,
  stops at start, on an empty answer and on no progress, trims to
  `(start, end]`, sorted and unique); the data client config and the bar
  requests use the full session; a series other than quotes refused.
- `ib/gateway.rs`: account forms and their mode, the container config (the
  mode, the API open to orders), the login wait on fake statuses (ready at
  once, ready after a wait, the deadline naming the last status, a vanished
  container, a failed status).
- `tests/ib_gateway.rs`, against the Docker paper gateway (gated by
  `NAUTILUS_IB_RUN=1`, see `live/ib/README.md`, "IB tests"): starting or
  reusing it, and a day of SXR8.IBIS quotes filled through it.
