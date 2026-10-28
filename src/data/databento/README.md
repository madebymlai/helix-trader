# Databento history

`GLBX` instruments (CME Globex, dataset `GLBX.MDP3`) fill the catalog
through fill-catalog's node (`../README.md`, "The node fill") with
Nautilus' Databento data client (`DatabentoDataClient`,
`nautilus-databento` with its `live` feature,
`crates/adapters/databento/src/data.rs`, at the pinned commit). It answers
each request with a completed `DataResponseEnvelope`, or `RequestFailed`
on an error, so the data engine writes its answers back. `databento/`
does what that client does not: the cost quotes and the dataset's end,
before the node starts, and the period each series is asked in.

| Series (`FillData`) | Client call | Databento schema | Stored as |
|---|---|---|---|
| `Instrument` | `request_instrument` | `definition` | the instrument, the latest definition of the window, once per id |
| `Depth` | `request_book_depth`, depth 10 | `mbp-10` | `OrderBookDepth` (10 levels a side), `data/order_book_depths/{id}/` |
| `Trades` | `request_trades` | `trades` | `TradeTick`, `data/trades/{id}/` |
| `Quotes` | `request_quotes` | `mbp-1` | `QuoteTick`, `data/quotes/{id}/` |

`fill-catalog` picks this source for every `….GLBX` instrument of the
strategy config (`DatabentoHistory::serves`) and fills the series the
config declares for it, no other: the book (`depth`) and trades of a tick
series, or the quotes or trades its bars are built from. The API key is
`DATABENTO_API_KEY`, from the environment or `.env`; it is never logged.

## Interface

```rust
DatabentoHistory::new(api_key: &str) -> anyhow::Result<DatabentoHistory>
DatabentoHistory::serves(id: InstrumentId) -> bool
DatabentoHistory::period(data: FillData) -> SignedDuration
history.quote(&self, requests: &[FillRequest]) -> anyhow::Result<()>
history.node_client(&self) -> NodeClient
```

`fill-catalog` takes the GLBX series of `DataRequirements::slices(start,
end)` (`strategy/README.md`): each series on a contract over its slice. A
plain instrument's slice is the whole window on itself; a rolled root
(`GC.GLBX`) has one slice per contract that stands for it in the window
(`rolls/README.md`). `node_fill::missing` turns them into what the catalog
lacks, contract by contract: the definition, then each series' missing
windows. Then, before the node is built:

1. `quote` refuses a request of another venue, and an end past the data
   Databento has (`metadata.get_dataset_range`; GLBX.MDP3 lags real time,
   on 2026-10-26 at 18:42 UTC it ended at 18:30): a request over it fails
   (422, `data_end_after_available_end`).
2. `quote` logs Databento's quote for every request (`metadata.get_cost`,
   free; definitions too), then their total. The quote is logged, not
   enforced: the fill then asks for, and pays for, every request.

`node_client` gives the node Nautilus' client, its config built by `new`
with `publishers.json`, Nautilus' map of Databento publishers to venues,
copied from `crates/adapters/databento/` at the pinned commit (the client
reads it from a path; Nautilus ships it beside its own binaries), routed
the requests of venue `GLBX`. It opens no live feed: its `connect` starts
nothing; a subscription would open the gateway, and the fill subscribes
to nothing. `new` also builds Databento's own client (`databento`, the
version Nautilus uses) for the metadata.

## Periods

`period` is what one request holds at most: a UTC day of any series,
windows cut on multiples of the period from the Unix epoch (so at UTC
midnight). A busy day of GC MBP-10 is about 6.8 million snapshots, some
7.5 GB decoded, and Nautilus' write copies them once more; Databento
answers each request after a wait, which shorter windows only multiply
(three-hour windows fetched a quarter of the data an hour). Each
window is one request, one answer and one catalog file. Nautilus' client
collects each response before returning it.

Saturdays have no data (CME Globex is closed): their windows come back
empty. Nautilus records them as covered when they touch stored data, as
after a Friday (`../README.md`, "The node fill").

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

A dated contract (`GCZ6`, a rolled root's contract, from its slice) is a
raw symbol to `infer_symbology_type`, so it is requested, and quoted, with
raw symbology: its records and definitions come back under its own id
(`GCZ6.GLBX`), the definitions with `GCZ6` as raw symbol. A rolled root
thus fills one series and one set of definitions per contract, each over
its slice, and the root itself is never stored.

A data request carries the price precision of the instrument's
definition, asked first (`price_precision`, see `../README.md`, "The node
fill"), so the client fetches no definitions of its own to learn it
(`seed_price_precision_if_needed`, `data.rs`).

## Tests

Without network:

- `serves`: `GLBX` ids only; `period`: a day of MBP-10, trades and
  quotes; the schema each series is quoted in; a quote's range
  ending 1 ns after its request;
- an `end` past the dataset's end refused; a request of another venue
  refused before any request;
- what the catalog lacks across GC's roll of 2026-07-29 (`node_fill::missing`
  on a temp catalog): a rolled root (`GC.GLBX`) asked contract by
  contract, `GCQ6` to 1 ns before the roll and `GCZ6` from it, each with
  its definition first; a contract's trades stored to 13:00 asked only
  after; a contract declared as itself and through its root asked once,
  over both.

The client's requests are Nautilus' and are tested at fill-catalog's seam
with a fake client answering as Databento's (`tests/node_fill/`). No test
places a request to Databento, paid or free; every quote comes before the
node is built, the order of `run_fill_catalog` in `main.rs`.

## The first load (2026-10-26)

The four weeks 2026-09-28 to 2026-10-23 were loaded from downloaded DBN
files with Nautilus' file loader (`DatabentoDataLoader::read_records`),
each file stored over its metadata window (midnight to midnight UTC), the
definitions from `get_range_instruments` as above. That path is gone; the
catalog it wrote has the same ids, layout and coverage as a fill through
the node, so a fill asks only for what it lacks.
