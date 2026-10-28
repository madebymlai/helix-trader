# OKX history

`okx/` fills the catalog for OKX swaps (`BTC-USDT-SWAP.OKX`) and X-Perps
(`BTC-USD_UM_XPERP-310404.OKX`, the contracts an EEA account trades): each
one's instrument, its 1-minute candles and its funding. It uses OKX's
public endpoints only, with no key. The catalog, `fill_catalog` and the
bar type rules are in `../README.md`; `fill-catalog` routes `….OKX` ids
here (`../README.md`, "Choosing the source").

Candles are stored as the bars of the instrument's
`1-MINUTE-LAST-EXTERNAL` bar type. A strategy trades OKX on a composite
bar type built from them, such as
`BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`; any other
series (quotes, trades, other bars) is refused before any request.

```rust
OkxHistory::new() -> anyhow::Result<OkxHistory>
OkxHistory::serves(id: InstrumentId) -> bool   // the OKX venue
history.fill(&mut catalog, slices: &[SeriesSlice]).await -> anyhow::Result<()>
```

`fill` takes the OKX series of `DataRequirements::slices`, the shape
`fill-catalog` gives every source (the node fill's too): each series on
its instrument over its slice, the whole window for an OKX instrument. It
is temporary: OKX's data client answers without a `DataResponseEnvelope`,
which Nautilus does not write back; once nautilus_trader#5473 is fixed,
OKX fills through the node (`../README.md`, "The node fill").

Per slice, `fill` stores, in this order:

1. the instrument (see "Instruments");
2. a swap's funding from the archive, month by month (see "Funding of swaps");
3. the funding after that, or an X-Perp's, from the API (see "Funding from the API");
4. the bars from the archive, month by month, then day by day (see "Months, then days").

A failed funding fill stores no new bars, so a backtest over them is
refused for want of funding rather than run without it.

## Instruments

The symbol gives the type (Nautilus' `okx_instrument_type_from_symbol`:
`…-SWAP` a swap, `…-310404` a future) and the family (the symbol without
its last part: `BTC-USDT`, `BTC-USD_UM_XPERP`).
`OKXRawHttpClient::get_instruments` (`/public/instruments`), on the raw
client of the `OKXHttpClient` that also requests the funding (see Funding
from the API), gives OKX's definition; `parse_instrument_any` turns a
swap into a `CryptoPerpetual` and a future into a `CryptoFuture` (`ctVal`
× `ctMult` as the multiplier, tick and lot sizes as increments). A future
whose rule type is not `xperp` (the adapter's `is_xperp_rule_type`) is a
dated future and is refused.

An X-Perp is a `CryptoFuture` expiring in 2031, linear, settled in USD
(Nautilus' USD, two decimals). BTC: 0.0001 BTC a contract, whole
contracts, tick 0.1.

### Margins of 1

`parse_instrument_any(raw, margin_init, margin_maint, ts_init)` leaves a
margin it is given as `None` at 0, which would lock nothing; `okx/` passes
1 for both. OKX locks the position's value divided by the leverage,
and the leverage is the account's choice, not the instrument's: the
backtest takes it as `--leverage` and Nautilus divides the value by it,
times the instrument's margin (see `src/backtest/README.md`, "Leverage").
OKX's own maintenance rates (BTC X-Perp: 0.65%) only decide liquidation,
which the backtest does not simulate, so they are not stored.

## The archive

Candles, and a swap's funding, come from OKX's archive.
`GET /api/v5/public/market-data-history` lists the files of up to 10
months, or 10 days, per request (OKX's errors 50077 and 50076 beyond),
each keyed by its `dateTs`. Its parameters: the module (2 for 1-minute
candles, 3 for funding rates), `instType` `SWAP` or `FUTURES`, the family,
`dateAggrType` `monthly` or `daily`. Its quota is 5 requests per 2 s;
`okx/` keeps to one every 600 ms (one every 400 ms got an HTTP 429 now and
then).

A file is a zip of one CSV, about 1.2 MB a month and 0.04 MB a day for BTC
candles, downloaded from `static.okx.com` (no quota) into memory. A swap
has its own file (`BTC-USDT-SWAP-candlesticks-<month or day>.zip`); a
future is in its family's chain file
(`BTC-USD_UM_XPERP-futureschain-candlesticks-<month or day>.zip`), whose
rows of other contracts are skipped. Every row is a confirmed candle: an
unconfirmed one is an error.

### Months and days of UTC+8

The archive's month and day are those of UTC+8, and files are chosen by
those, not by the UTC dates: the file of 2026-10-19 holds the 1,440
minutes opening from 2026-10-18T16:00Z to 2026-10-19T15:59Z, that of
2026-09 the 43,200 opening from 2026-08-31T16:00Z to 2026-09-30T15:59Z. A
month's CSV holds the same rows as its days' CSVs, so the same bars
(2026-09 of BTC-USDT-SWAP, checked 2026-10-23: byte for byte the 30 daily
files' rows; likewise the 24 months of the run in "Verified fills").

### Months, then days

`fill` calls `fill_catalog` once per archive month or day and series,
over the bars closing in `(period start, period end]` (UTC+8 midnights),
closed and contiguous with the next one's, as Nautilus' catalog
intervals. From the month it resumes in, it reads monthly files, one
listing per 10 months, up to the first month without one; from there
daily files, one listing per 10 days.

The first window starts with the day of `start`, which the files cover
whole, so the stored range may begin before `start`. Each call resumes
where the catalog stopped; a stopped run picks up after its last stored
day, from the month's file if it has been published since. A period
starting at `end` is left to the next run.

### Archive start and lag

No file on the first day of a fill (before the listing, or before the
archive) is an empty first response, an error naming the series. OKX
publishes a month's file on the first days of the next month (2026-09's
on 2026-10-01) and a day's about two days after it ends; a later day with
no file stops the series without recording it, and the next run asks for
it again.

## Candles to bars

Each row becomes the adapter's `OKXCandlestick` and its
`parse_candlestick` makes the bar, at the instrument's precisions: the
prices, and the volume in contracts (OKX's `vol`), of the bars Nautilus'
OKX clients make from the API.

One archive day (2026-10-19, 1,440 minutes) was compared minute by minute
with OKX's `/api/v5/market/history-candles` (`bar=1m`, public,
2026-10-22), for BTC-USDT-SWAP and BTC-USD_UM_XPERP-310404: the same 1,440
minutes on both, no difference in open, high, low, close, `vol`, `volCcy`,
`volCcyQuote` or `confirm`.

### Stamped at the open, seen at the close

`ts_event` is the candle's open, as OKX and the adapter stamp it;
`ts_init` is its close (open + 1 minute). This is Nautilus' convention for
bars stamped at their open (Concepts › Backtesting › Bar execution, "Bar
timestamp convention": `ts_init` must be the close, else the whole bar is
visible before it formed), and what the adapter does for its own
historical bars from the release after 0.66.0 (nautilus_trader#5339,
commit `fc94ea1`, in the pinned commit); 0.66.0 stamped them at the
request time. The backtest replays by `ts_init` and the data engine
aggregates by it, so the hour bar closing 10:00 holds the minutes opening
09:00 to 09:59 and is built at 10:00 (`backtest_okx_swap.rs` checks
both).

## Funding

Funding is stored as the instrument's `FundingRateUpdate`s (series
`FundingRateUpdate`, keyed by the instrument id). Each update is parsed by
the adapter's `parse_funding_rate`, as Nautilus' OKX client does from the
API: the rate, `ts_event` = `ts_init` = the funding time, the interval the
minutes since the funding time before, no `next_funding_ns`. The backtest
needs the funding over its whole window (see `src/backtest/README.md`,
"OKX on candles").

### Funding of swaps

The archive holds the funding of swaps only, in module 3 (`dateAggrType`
`monthly` only; `BTC-USDT-SWAP-fundingrates-<month>.zip`, a CSV of
`instrument_name,funding_rate,funding_time`, one row per funding time,
from the month's start in UTC+8 to before its end: 2026-09 runs from
2026-08-31T16:00Z to 2026-09-30T08:00Z). Each row becomes the adapter's
`OKXFundingRateHistory`. The first row of a file takes the next row's
interval, its row before being in the month before. The archive's
`funding_rate` equals the API's `fundingRate` and `realizedRate` (checked
2026-10-23).

Windows are the month's funding times, `[month start, month end − 1 ns]`,
contiguous. The fill resumes after the stored end and stops at the first
month not published, so the archive's funding ends with the last complete
month while the bars go on day by day. No file for the first month is an
error naming the series, so a swap's `start` in a month the archive has
not published is an error.

### Funding from the API

After the archive's months for a swap, and for an X-Perp from `start`,
`fill` stores the funding from Nautilus'
`OKXHttpClient::request_funding_rates` (`/public/funding-rate-history`,
public, its own quota of 5 requests a second). It parses as the archive's
rows (`parse_funding_rate`), except that the oldest update of a response
is left without an interval, since its time before is not in the
response: it would not settle. So pages go backwards from `end`, each
asking for the times before the oldest of the one before + 1 ms (OKX's
`after`, exclusive), which brings that time back with its interval, until
a page reaches before the window (or the stored end, plus 1 ns) or holds
only that time; only updates with an interval are stored.

OKX keeps about 3 months (from 2026-07-20T16:00Z on 2026-10-23, the same
for BTC-USDT-SWAP and the BTC X-Perp, in one response of 283 times), so an
older `start` stores the X-Perp's funding from its second oldest time.
The stored range ends on the newest funding time, so a re-run asks for
what OKX settled since; a window the stored funding already covers to
`end` (a swap's archive months) asks for nothing. A resumed fill OKX no
longer reaches back to (a catalog left more than 3 months) is an error,
not a hole; no funding on a first fill is an error naming the series.

Verified 2026-10-23 (release build, `fill-catalog` from 2026-08-01 for
the swap, 2026-07-01 for the X-Perp, to now): the swap's funding stored
over 2026-07-31T16:00Z to 2026-09-30T15:59:59.999999999Z from the archive
and on to 2026-10-22T16:00Z from the API (one request, 283 times); the
X-Perp's over 2026-07-21T00:00Z to 2026-10-22T16:00Z (two requests). A
re-run wrote nothing new.

## Catalogs filled before candles

A catalog filled when `okx/` stored quotes rebuilt from the order book
must be filled again, fresh, into a new directory: its quotes are no
longer filled or extended, and nothing deletes them.

## Verified fills

Days (2026-10-22, release build, `fill-catalog` with an EMA config on
each id, `--start 2026-10-13 --end 2026-10-21`): archive days 10-13 to
10-20 of 1,440 bars each per instrument, then a stop at 10-21, not yet
published; later that day 10-21 was out and the same fill took its first
480 minutes, up to `end`. About 6 s per instrument; the catalog holds
about 50 KB a day per instrument. A re-run asked for nothing. Four fills
back to back got no HTTP 429 at one listing every 600 ms.

Months (2026-10-23, release build, `fill-catalog` with an EMA config on
BTC-USDT-SWAP.OKX, `--start 2024-10-23 --end 2026-10-23`, a new catalog):
24 monthly files (2024-10 to 2026-09) and 21 daily ones (2026-10-01 to
10-21), then a stop at 10-22, not yet published; 6 listings, no HTTP 429;
1,049,760 bars (729 days of 1,440) in 45 contiguous intervals, 34 MB, in
12.6 s. A re-run took 1.2 s (two listings, nothing new). The monthly
files of those 24 months hold, byte for byte, the rows of their daily
files.

## Tests

Instruments: the real BTC swap and X-Perp to a `CryptoPerpetual` and a
`CryptoFuture` with margins of 1; a dated future refused; the type and
family of a symbol; only minute candles served; the slices of a strategy
on a composite bar type filled with its minute candles over their window,
a slice of another series refused.

Candles: samples of a real swap file and a real X-Perp chain file (the
first 61 minutes and the last of 2026-10-19) to bars, stamped at the open,
seen at the close, prices and volume in contracts, the minutes of an hour;
the first and last hours of 2026-09 from the swap's month file equal to
those from its first and last days' files, and bounded by the month of
UTC+8; on small hand-made files an unconfirmed candle named, the rows of
other contracts skipped, a file without the contract an error.

Archive: the listing of a day, of months and of a chain, keyed by period;
months and days of UTC+8; 10 periods per listing. The fill with a fake
archive: complete months from monthly files and the rest from daily
files, contiguous windows; two years in 3 monthly and 3 daily listings; a
failed day keeping the days before and the next run resuming from the
month's file published since, as one uninterrupted fill; a start on a
day's end; a day not published; no file on the first day.

Funding from the archive: the sample of a real month file (its first two
days) to updates at the funding times with an 8-hour interval, the first
row's from the row after; a window's updates; intervals from the rows'
times; rows out of order, alone, or with no row of the swap an error; the
listing of funding months; an X-Perp's funding refused. The fill with a
fake archive: monthly files only, windows `[month start, month end −
1 ns]`, a stop at the first month not published, resumed after the stored
end as one uninterrupted fill, no file for the first month an error
naming the series.

Funding from the API: a real API page (8 funding times of the X-Perp)
served locally to Nautilus' client, paged back to the window's start with
every stored update's interval, or as far as OKX keeps it; resumed after
the stored end as one uninterrupted fill; a resume OKX no longer reaches,
and no funding on a first fill, errors; a window the stored funding
covers asking for nothing.

Order: the funding filled before the bars; a failed one (a swap's
unpublished first month, an X-Perp's window older than OKX keeps) storing
no bars.
