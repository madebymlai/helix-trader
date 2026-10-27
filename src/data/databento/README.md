# Databento history

`databento/` fills the catalog for `GLBX` instruments (CME Globex,
dataset `GLBX.MDP3`) from Databento's historical API, through Nautilus'
`DatabentoHistoricalClient` (`nautilus-databento`,
`crates/adapters/databento/src/historical.rs`, at the pinned commit). It
decodes; this module picks the windows, checks and writes.

| Databento schema | Client call | Stored as |
|---|---|---|
| `definition` | `get_range_instruments` | the instrument, once per definition received |
| `mbp-10` | `get_range_order_book_depth(params, Some(10))` | `OrderBookDepth` (10 levels a side), `data/order_book_depths/{id}/` |
| `trades` | `get_range_trades(params, None)` | `TradeTick`, `data/trades/{id}/` |

`fill-catalog` picks this source for every `….GLBX` instrument of the
strategy config (`DatabentoHistory::serves`), and stores its MBP-10 and
trades whatever the strategy's bar type: the book is the signal the
strategy reads, the trades its flow. A strategy's bars still need the
series of their bar type (quotes for `MID` bars), which Databento does not
fill here. The API key is `DATABENTO_API_KEY`, from the environment or
`.env`; it is never logged.

## Interface

```rust
DatabentoHistory::new(api_key: &str) -> anyhow::Result<DatabentoHistory>
DatabentoHistory::serves(id: InstrumentId) -> bool
history.fill(&self, catalog: &ParquetDataCatalog, ids: &[InstrumentId],
             start: Timestamp, end: Timestamp) -> anyhow::Result<()>
```

`new` builds Nautilus' client with `publishers.json`, Nautilus' map of
Databento publishers to venues, copied from `crates/adapters/databento/`
at the pinned commit (the client reads it from a path; Nautilus ships it
beside its own binaries), and Databento's own client (`databento`, the
version Nautilus uses) for the metadata Nautilus' client does not give:
cost quotes and the dataset's end.

## Instrument ids

The client resolves each record's id from the response's symbology
(`symbology.rs`, `decode_nautilus_instrument_id`): the symbol is the
mapping's `raw_symbol` for the record's Databento instrument id and date,
the venue the publisher's (`publishers.json`: publisher 1, `GLBX.MDP3`,
is `GLBX`). The request's symbol is the id's (`GC.v.0`), its symbology
inferred from it (`infer_symbology_type`: three parts, the last numeric,
is `continuous`).

For a continuous symbol the mapping's `raw_symbol` is the symbol itself
(dbn `PitSymbolMap::from_metadata`: with `stype_out = instrument_id` the
map goes from the instrument id to the requested symbol). Every record,
definitions included, is therefore `GC.v.0.GLBX`, whichever contract the
series holds that day: the whole series under one id, natively. Nautilus'
Databento docs use the same form (`ES.c.0.GLBX`). Backtests list
`GC.v.0.GLBX`; live configs name the dated contract. The definitions are
the contract the series holds (its expiry: 2026-12-29 for the four weeks
loaded, December's), under the series' id and with `GC.v.0` as raw symbol;
price increment 0.10, precision 2. `use_exchange_as_venue` (venue `XCEC`) is off: the venue stays `GLBX`.

A definition fetch also caches the symbol's price precision in the client
(`cache_price_precisions`), which the MBP-10 and trades requests then
resolve per record.

## Resume, days and cost

Per instrument, each series (MBP-10, trades) resumes as `fill_catalog`
does: from 1 ns after its stored range (`query_last_timestamp`), whatever
`start`, to `end`; a `start` before the stored range is an error (extending
history backwards is not supported). A covered window asks for nothing: a
`fill-catalog` over the stored weeks makes no request at all.

Otherwise, before any data request:

1. An `end` past the data Databento has (`metadata.get_dataset_range`;
   GLBX.MDP3 lags real time, on 2026-10-26 at 18:42 UTC it ended at 18:30)
   is an error: a request over it fails (422, `data_end_after_available_end`).
2. Databento's quote for each series' missing window is logged
   (`metadata.get_cost`, free).
3. The definitions over the missing span are fetched (a few kilobytes,
   quoted $0.00 for four weeks) and those later than any stored written.

Then each series is fetched one UTC day per request (the window cut at
each midnight; the first and last day may be partial) and stored over the
day asked for, so consecutive days are contiguous in the catalog. A day of
GC MBP-10 is about 6.8 million snapshots: one request holds one day, some
7.5 GB decoded. A day is written in catalog files of at most 1,000,000
rows, each ending 1 ns before the next starts and never splitting the
records of one timestamp. A record of another instrument or outside its
day is an error. Any error stops the run; the days stored stay, and a
re-run resumes after them.

Saturdays have no data (CME Globex is closed), so they write nothing and
have no coverage: expected, not a gap to fill. A Saturday at the end of a
window is asked for again by the next fill; once a later day is stored, the
resume point is past it.

The quote is logged, not enforced: a fill fetches, and pays for, every
missing day of its window. Nautilus' client collects each response before
returning it.

## Tests

Without network:

- `serves`: `GLBX` ids only;
- resume window: a series not stored over the whole window; a stored one
  after its range, whatever `start`; a covered window (as the loaded
  weeks) fetching nothing; a `start` before the stored range refused;
- an `end` past the dataset's end refused;
- the day split: cut at each UTC midnight, a window inside a day is one
  day, a request's end exclusive;
- a day's write, on a temp catalog: stored over the day, split into
  contiguous files between timestamps, an empty day writing nothing, a
  record of another instrument or outside the day refused.

The requests themselves are not unit-tested: no test places a request,
paid or free.

## The first load (2026-10-26)

The four weeks 2026-09-28 to 2026-10-23 were loaded from downloaded DBN
files with Nautilus' file loader (`DatabentoDataLoader::read_records`),
each file stored over its metadata window (midnight to midnight UTC), the
definitions from `get_range_instruments` as above. That path is gone; the
catalog it wrote has the same ids, layout and coverage as this one, so a
fill resumes after it.

## The backfill (2026-10-27)

The four weeks before, 2026-08-31 to 09-28, filled by `fill-catalog` into
a scratch catalog (quoted and charged $18.86 for MBP-10, $2.63 for trades)
and its files moved into `catalog/`: a fill does not extend history
backwards. The scratch fill wrote, as its last depth file, the one
snapshot at the request end, 09-28 00:00:00, which the first load holds
too; it was dropped. The catalog now holds 2026-08-31 to 10-23, its files
contiguous but for the Saturdays. No roll in the span (`GC.v.0` last
rolled 2026-07-31).
