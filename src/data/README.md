# The data module

`data` is build step 1. It fills the Nautilus Parquet catalog with the
history of the instruments the strategy trades; backtest reads that catalog.
It adds what Nautilus does not provide: the ingest actor and the roll dates.

APIs are those of the `nautilus-*` crates at `0.65.0`; **unverified** marks
what the sources did not confirm. Examples use the future `ESM4.CME`, its
predecessor `ESH4.CME` and the continuous root `ES.CME`.

## Layout

```
data/
  mod.rs        declares the modules, exposes only what others call
  ingest.rs     the DataActor that fills the catalog
  rolls.rs      roll dates -> continuous_future_transitions params
```

No port of our own: the Nautilus `DataClient` trait is the port for history
(any adapter implements it), `CatalogWriter` the port for storage.

```
strategy config ──► ingest ──(DataClient)──► adapter
                      └─────(CatalogWriter)──► ParquetDataCatalog ──► backtest
rolls ◄── catalog bars of each contract ──► params for the INTERNAL bar type
```

## Instruments and bar types

Nautilus has no universe type, and neither do we. The strategy config is the
single source of the instruments and the bar type (see the strategy README).
`main` reads them from it and passes Nautilus types to ingest, backtest and
live: `Vec<InstrumentId>` and `BarType`. This module never names an
instrument.

Two bar types matter: the `EXTERNAL` one per contract (what ingest requests
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
and a cadence the adapter cannot serve is rejected before ingest starts.

**Aggregation source**, the last token:

- `EXTERNAL`: bars made by the venue or adapter: what `request_bars`
  returns, what the catalog stores, what `subscribe_bars` streams.
- `INTERNAL`: bars built by the Nautilus data engine from ticks or other
  bars, as continuous futures are.

**Composite** bar types build a target from a finer bar type:
`ES.CME-1-HOUR-LAST-INTERNAL@1-HOUR-EXTERNAL`. The target must be
`INTERNAL`. `request_bars` rejects a composite `bar_type` (pass it through
the `bar_types` param instead); `subscribe_bars` takes it directly.

## ingest.rs

A Nautilus `DataActor` on a `LiveNode` with a data client and no execution
client (the data-only node, see the live README). It requests instruments
and bars and writes each response into the catalog. It stops the node when
every request has answered. Ingest is its own process run.

### The actor

```rust
#[derive(Debug)]
pub struct Ingest {
    core: DataActorCore,            // DataActorCore::new(DataActorConfig)
    catalog: ParquetDataCatalog,
    // pending requests, per bar type the last stored ts_init
}
nautilus_common::nautilus_actor!(Ingest);   // the field must be named `core`
impl DataActor for Ingest { .. }
```

### Requests and callbacks

On `DataActor` (`nautilus_common::actor`); times are `jiff::Timestamp`, not
after now, `start <= end`:

```rust
fn request_instrument(&mut self, instrument_id: InstrumentId, start: Option<Timestamp>,
    end: Option<Timestamp>, client_id: Option<ClientId>, params: Option<Params>)
    -> anyhow::Result<UUID4>
fn request_instruments(&mut self, venue: Option<Venue>, start: Option<Timestamp>,
    end: Option<Timestamp>, client_id: Option<ClientId>, params: Option<Params>)
    -> anyhow::Result<UUID4>
fn request_bars(&mut self, bar_type: BarType, start: Option<Timestamp>,
    end: Option<Timestamp>, limit: Option<NonZeroUsize>, client_id: Option<ClientId>,
    params: Option<Params>) -> anyhow::Result<UUID4>
```

| Request | Callback |
|---|---|
| `request_instrument`, `request_instruments` | `on_instrument(&InstrumentAny)`, once per instrument |
| `request_bars` | `on_historical_bars(&[Bar])`, once per response, the whole batch |

- The callbacks carry no request id. Attribute bars by `bars[0].bar_type`,
  or run one request at a time.
- `on_instrument` also fires for live instrument updates; do not assume
  every call answers a request.
- An empty response may or may not reach `on_historical_bars`
  (**unverified**): track pending requests and treat a missing answer as an
  error.
- `Params` (`nautilus_core::Params`) is a map of `serde_json::Value`.

### Nautilus does not persist responses

In Rust, a requested response is never written to a catalog by Nautilus.
The `update_catalog` param only routes a request to the client instead of a
registered catalog. The ingest actor is the persistence step, and the ingest
node registers no catalog, so every request reaches the adapter.

### Writing

`CatalogWriter` (`nautilus_persistence::catalog::traits`), implemented by
`ParquetDataCatalog`:

```rust
fn write_instruments(&mut self, instruments: &[InstrumentAny]) -> anyhow::Result<()>;
fn write_data(&mut self, data: &[Data], start: Option<UnixNanos>, end: Option<UnixNanos>,
              params: Option<Params>) -> anyhow::Result<()>;   // bars as Data::Bar
```

```rust
ParquetDataCatalog::new(base_path: &Path, None, None, None, None)   // panics on a bad path
ParquetDataCatalog::from_uri(uri: &str, ..)                          // Result; also s3://, gs://
```

Write constraints:

- **Ascending `ts_init`.** Sort before writing; a descent is an error.
- **One bar type per file.** `write_data` groups a mixed slice for you;
  the lower-level `write_to_parquet` refuses mixed bar types.
- **Disjoint intervals.** Each write makes one file named after its
  `[first, last]` `ts_init`. A new interval must not touch an existing one:
  a shared timestamp is an overlap and an error. Appending a later window
  works; rewriting an overlapping one fails. Never pass
  `skip_disjoint_check`: an overlap means a bug in the windows.
- **Instruments** go one file per id, named by the instrument's `ts_init`;
  a later definition adds a file.

Layout on disk:

```
{catalog}/data/bars/ESM4.CME-1-HOUR-LAST-EXTERNAL/{start}_{end}.parquet
{catalog}/data/futures_contract/ESM4.CME/{ts}_{ts}.parquet
```

### Reading back

```rust
catalog.bars(ids: Option<Vec<String>>, start: Option<UnixNanos>, end: Option<UnixNanos>)
    -> anyhow::Result<Vec<Bar>>                      // &mut self; ordered by ts_init
catalog.instruments(ids: Option<&[String]>, start, end) -> anyhow::Result<Vec<InstrumentAny>>
catalog.query_last_timestamp(..)                     // where a re-run resumes
```

For bars an id is a full bar type string or an instrument id (which matches
all its bar types).

### Interactive Brokers specifics (history)

- **Bar sizes.** IB serves only 1/5/15/30 SECOND; 1/2/3/5/10/15/20/30
  MINUTE; 1/2/3/4/8 HOUR; 1 DAY; 1 WEEK; 1 MONTH. Anything else fails
  ("Unsupported bar aggregation/step combination"). Only time bars.
- **Node.** The data-only node of the live README with
  `InteractiveBrokersDataClientFactory`, an `InteractiveBrokersDataClientConfig`
  and default routing. The IB crate's `node_data_tester` example is the same
  shape.
- **Instruments first.** `request_bars` fails ("Instrument ... not found in
  provider") unless the provider already holds the contract: load it through
  `load_ids` / `load_contracts` in the provider config (see the live README).
- **Instrument requests.** `request_instrument` loads an unknown contract and
  answers with it; it ignores `start` and `end`. `request_instruments` is not
  "list all": it returns only the contracts named in the `ib_contracts`
  param, and nothing without it. Use `request_instrument` per contract.
- **Timestamps.** IB gives the bar open; the adapter stamps
  `ts_event = ts_init` = open + bar duration, the close. All intervals and
  roll dates are on that close.
- **Gaps.** The adapter splits a long range into segments and runs them in
  sequence, with no pacing and no retry. **A failed segment is logged and
  skipped, and the response arrives with a hole.** Check coverage (first and
  last bar, expected count) before writing.
- **Limits.** IB's own limits (duration per bar size, request pacing, how
  far back history goes) are **unverified**. Request modest windows (months
  for intraday bars), one at a time, and treat a short answer as an error.
- **Regular hours.** `use_regular_trading_hours` in the data client config
  applies to every request; history and live must agree.
- **Expired futures.** `includeExpired: true` in a contract spec exists, but
  whether an expired contract loads end to end is **unverified**. Test it
  against a gateway before planning the history on it.
- **`CONTFUT`.** IB's own continuous future is one back-adjusted series with
  no explicit end. Do not ingest it: rolls need the per-contract bars.
- **Fallback.** `HistoricalInteractiveBrokersClient`
  (`nautilus_interactive_brokers::historical`) fetches bars without a node:

  ```rust
  HistoricalInteractiveBrokersClient::connect(config).await?
      .request_bars(bar_specs: Vec<&str>, end: Timestamp, start: Option<Timestamp>,
          duration: Option<&str>, contracts: Option<Vec<Contract>>,
          instrument_ids: Option<Vec<InstrumentId>>, use_rth: bool, timeout: u64)
          .await?   // Vec<Bar>, ready for write_data
  ```

  It ties `ingest.rs` to IB, so use it only if the node path proves awkward.

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
- **Ingest writes, nobody else** writes the catalog.
- **Sort, split, append**: sorted by `ts_init`, one bar type per file,
  resume from `query_last_timestamp`.
- **Check before writing**: refuse a short or empty response.
- **Instruments before bars.**
- **Per-contract bars are the stored truth**: never write an adjusted series;
  continuous series are built at read time.
- **Rolls are values**: backtest and live pass the same params to the same
  engine call.
- **No clock and no I/O** in `rolls.rs`: time comes in as
  arguments.
- **Cadence checked against the adapter** before ingest starts.

## Tests

- Unit tests in a `#[cfg(test)] mod tests` at the bottom of each file:
  - `rolls.rs`: detection on small synthetic series (clean roll, a gap, no
    overlap); params checked as JSON (increasing times, contiguous chain,
    decimal strings, each mode).
  - `ingest.rs`: the pure parts: windows, ordering, coverage check, resume
    point.
- Integration tests in `tests/`, one node per file:
  - catalog round trip on a temp dir: write two bar types and an instrument,
    read them back; overlapping and mixed writes fail;
  - the ingest actor on a node with a stub `DataClient` returning canned
    responses;
  - a backtest of a continuous future over a fixture catalog and roll params,
    which also proves the engine accepts our params.
- Anything against a real gateway is a manual check.
