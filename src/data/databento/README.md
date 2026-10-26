# Databento history

`databento/` loads Databento DBN files of one instrument into the catalog
with Nautilus' own loader, `DatabentoDataLoader` (`nautilus-databento`,
`crates/adapters/databento/src/loader.rs`, at the pinned commit). It
decodes; this module only checks and writes. The files are downloaded
outside this repo (COMEX gold, `GLBX.MDP3`, continuous symbol `GC.v.0`,
one file per schema and UTC day).

| DBN schema | Loader call | Stored as |
|---|---|---|
| `mbp-10` | `read_records::<Mbp10Msg>` | `OrderBookDepth` (10 levels a side), `data/order_book_depths/{id}/` |
| `trades` | `read_records::<TradeMsg>` | `TradeTick`, `data/trades/{id}/` |

Any other schema is refused. `read_records` is the streaming form of
`load_order_book_depth` and `load_trades` (the same decoder,
`decode_record`): a day of GC MBP-10 is about 6.8 million snapshots, 7.5 GB
when collected at once.

## Interface

```rust
DatabentoFiles::new() -> anyhow::Result<DatabentoFiles>
databento.load(&mut self, catalog: &ParquetDataCatalog, instrument: InstrumentAny,
               files: &[PathBuf]) -> anyhow::Result<()>
```

`new` builds the loader with `publishers.json`, Nautilus' map of Databento
publishers to venues, copied from `crates/adapters/databento/` at the
pinned commit (the loader reads it from a path; Nautilus ships it beside
its own binaries). `load` writes the instrument if the catalog lacks it,
then each file in turn. Files are paths passed in; the module reads them
only through the loader and the DBN metadata, and never writes beside
them.

## Instrument ids

The loader is called with no `instrument_id`, so it resolves each record's
id from the file's own symbology (`symbology.rs`,
`decode_nautilus_instrument_id`): the symbol is the mapping's `raw_symbol`
for the record's Databento instrument id and date, the venue the
publisher's (`publishers.json`: publisher 1, `GLBX.MDP3`, is `GLBX`).

For a file requested with `stype_in = continuous`, the mapping's
`raw_symbol` is the continuous symbol itself (dbn
`PitSymbolMap::from_metadata`: with `stype_out = instrument_id` the map
goes from the instrument id to the requested symbol). Every record of the
GC files is therefore `GC.v.0.GLBX`, whichever contract the series holds
that day: the whole series under one id, natively. Nautilus' Databento
docs use the same form (`ES.c.0.GLBX`). Checked on 2026-09-28: 142,053
trades and 6,826,995 snapshots, all `GC.v.0.GLBX`.

`use_exchange_as_venue` (venue `XCEC` for COMEX) does not reach these
records: the loader applies it only to definition files, keyed by their
dated symbol (`GCZ6`), never `GC.v.0`.

The loader takes prices' precision from the instrument
(`set_price_precision` on its symbol); a record of another symbol fails to
decode, and one of the same symbol on another venue is refused.

## Coverage

Each file is stored over its request window, `[start, end − 1 ns]` from
its DBN metadata (a daily file: midnight to midnight UTC), not its
records' span, so consecutive daily files are contiguous in the catalog.
Before decoding, `load` asks the catalog for the missing part of the
window (`get_missing_intervals_for_request`):

- none: the file is stored, nothing is decoded or written;
- the window's tail: a load stopped midway, resumed after the stored part;
- anything else: an error, raised before writing.

A file is written in catalog files of at most 1,000,000 rows, each ending
1 ns before the next starts and never splitting the records of one
timestamp. A record outside the file's window is an error. A file with no
record in its window records no coverage.

Days the download has no file for (Saturdays) leave gaps in coverage.

## The instrument (open)

The catalog needs the instrument under `GC.v.0.GLBX`. Nautilus gives it
natively only through the historical API: `DatabentoHistoricalClient::
get_range_instruments` with symbol `GC.v.0` resolves definitions through
the metadata as above (`historical.rs`), so they come back as
`GC.v.0.GLBX`. A definition file loaded with `load_instruments` takes each
record's own dated symbol instead (`GCZ6.GLBX`), which does not match the
data. The CLI step is wired once the source of the instrument is chosen.

## Tests

On a temp catalog, with Nautilus' DBN fixtures in `testdata/` (ESH1,
`GLBX.MDP3`, 2020-12-28; copied from `crates/adapters/databento/test_data/`):

- the MBP-10 snapshots and trades read back from the catalog under the
  instrument, which is stored;
- each file covered over its metadata window;
- a second load writes nothing;
- a file split into contiguous catalog files, and resumed after the part
  stored;
- records of the same symbol on another venue, and a schema not stored
  (`mbp-1`), refused.
