# LSE

A NautilusTrader adapter for London Strategic Edge's market data (LSE Data 3.0), laid out as Nautilus' adapters are: a data client only, as LSE trades nothing, naming no symbol. It serves the trades of LSE's derived markets, their tick slices, as `TradeTick`s, and LSE's candles, 1 second to 4 hours wide, as external `Bar`s stamped at their close; every other request fails as Nautilus' data client default does (unsupported), and every subscription is logged as not implemented. It decides nothing a key may have: it asks LSE, and LSE's refusal is the request's failure.

## Instruments

A series of a derived market (futures, stocks, indices, forex, commodities, crypto) is one instrument under the venue `LSEDGE`, not `LSE`, which is IB's London Stock Exchange. Its symbol is LSE's symbol, a hyphen and LSE's market in capitals: GOLD in `futures` is `GOLD-FUTURES.LSEDGE` (`LseSymbol`). LSE lists one symbol in several markets, and its market names have no hyphen, so the last hyphen splits symbol from market. An exchange market's listings (`cme`, `nasdaq`, `us_options`) are not `LSEDGE` series.

LSE publishes no instrument definitions. The client answers an instrument request with none, a completed envelope with no payload, and converts LSE's prices and sizes at the precisions of the instrument in its cache, so the definition must reach the cache from elsewhere, such as a catalog Nautilus answers the instrument request from first. A trades request of an instrument the cache lacks fails naming it, before LSE is asked.

## Credentials

The key is `LSE_API_KEY`, or the config's `api_key`, read when the node creates the client. It goes in the `X-API-Key` header of every call; no `Debug` output and no error holds it, and redirects are refused, so that it goes nowhere else.

## Exports

A trades request asks LSE's tick slice (`/v3/export`, `timeframe=tick`) as Parquet, at prices as published (`adjusted=0`), a UTC day an export over two timestamps, `[from, to)`. An export LSE refuses as too wide (`413 export-too-wide`: past the 2,500,000 rows one export may read on every plan, or not counted within LSE's 10 seconds) is asked again in halves, and a half LSE still refuses is halved again, down to a second, past which LSE's refusal fails the request: no second of any series LSE serves holds that many rows. LSE answers an export with its file: since 6 November 2026 it keeps no prepared file (`/v3/jobs` answers `export-unknown`), so there is no job to poll. A download cut short is fetched again, as LSE's docs say. A whole file whose SHA-256 is not the one LSE gives with it (`x-checksum-sha256`) fails the request, naming the export by its selection and by LSE's id of it (`x-export-id`), for LSE to look into: LSE builds a file whole before sending it, so that its checksum is exact. One export downloads at a time, as LSE allows one a key.

Calls are spaced to stay within the calls a minute of the key's plan in any minute: the config's `calls_per_minute`, by default the free plan's 60, the fewest of any plan; set the plan's figure (`/v3/usage`, `calls_per_minute`) to go faster. A refusal for the moment, a 429 of calls or of the one download (`calls-exhausted`, `connections-exhausted`, `exports-busy`) or a busy 503, is asked again after the `Retry-After` LSE gives, or a backoff, ten times at most. A 429 of a quota (`allowance-exhausted`, `rows-exhausted`) fails the request at once with LSE's message: it lasts until Monday or until 00:00 UTC, and what the request did not get is the caller's to ask for again once it resets. A selection LSE holds no row of (`data-unavailable`, `date-not-trading`) answers no trades or bars. Every other refusal fails the request with LSE's status, code, message and required plan, for example `403 plan-required: This feed requires a subscription; required plan: CME Group Lite`.

LSE stores a UTC day after it ends, the morning after it (its book and orders later still), and answers a day it has not loaded as one it holds no row of (`data-unavailable`: "the dates asked may not be loaded yet (or nothing traded in them)"). So the client takes the current UTC day as not loaded, whatever LSE answers, and the day before as not loaded until LSE answers rows of it. A request reaching into that span is answered with what LSE holds in a plain `DataResponse` rather than a completed `DataResponseEnvelope`, and the span is logged: the request completes, but Nautilus' data engine writes a payload-only reply to no catalog, as it certifies no coverage, so that a fill asks for the window again on its next run rather than recording a day LSE had not loaded as one with no rows. Nautilus' OKX client answers a window it cannot confirm the same way.

A bars request asks LSE's candles (`/v3/export`, `data=candles`, LSE's default) of the bar type's width, as published and as Parquet, as trades are. LSE serves candles 1, 5, 15 and 30 seconds, 1, 3, 5, 15 and 30 minutes, and 1 and 4 hours wide, as `1-SECOND-LAST-EXTERNAL` to `4-HOUR-LAST-EXTERNAL` bars (`CandleWidth`), each of LSE's widths one bar specification: `60-MINUTE` is not `1-HOUR`. A candle has one price, the last: of trades where its market has trades, and of LSE's stored quotes in forex and commodities, which store no trades (LSE's docs; gold spot's candles have no volume). Any other bar type is refused naming it, before LSE is asked: a width LSE does not serve, another price, bars Nautilus builds (`INTERNAL`), and LSE's daily, weekly and monthly candles, which follow exchange sessions (each stamped at its session's first traded minute and folded from the sessions inside the dates asked), so that no close of theirs is known. LSE stamps a candle at its open, on a grid of its width from 1970 in UTC; its bar is stamped at its close, its open plus its width (Nautilus' `BarSpecification::timedelta`), both `ts_event` and `ts_init`, as Nautilus' adapters stamp bars by default (Databento's `bars_timestamp_on_close`). A request over `[start, end]` gets the bars closing in it. LSE folds a candle from the rows inside the export alone, so a candle cut by the export's start comes partial: the client asks only whole candles, from the first one's open to the last one's close, a UTC day an export as trades are, and nothing if no candle closes in the window; an export of candles LSE refuses as too wide is halved between the two candles nearest its middle, and one of a single candle, which no width holds too many rows for, is not.

LSE sends prices and sizes as doubles. Each is converted once, through its shortest decimal form, which must fit the instrument's precision: a finer price fails the request rather than being rounded, as a definition too coarse would round it; a candle's volume is a size. A trade is stamped at LSE's time, both `ts_event` and `ts_init`, with no aggressor side, LSE sending none; its trade id is a hash (FNV-1a, as Nautilus' Databento adapter derives ids) of its instrument, time, price, size and its place among the trades alike before it in the file, so trades alike keep ids apart and a refill gives the same ids.

## Tests

The tests run against a mock LSE peer (`testing`) replaying payloads captured from LSE with the free key, and LSE's documented answers where none was captured (`test_data/`, see its README). With the `test-support` feature the peer and its payloads serve a caller's tests of its own fill through the adapter. They run with the workspace's, or alone:

```sh
cargo test -p lse
```

A test against LSE itself needs a key, so its caller holds and gates it, as this repository's caller does (`NAUTILUS_LSE_RUN=1`, see the root README).

## Data testing

`examples/node_data_tester.rs` runs Nautilus' `DataTester` on a live node with the client, against LSE itself with the key of the environment or `.env`, for the cases of Nautilus' [data testing spec](https://github.com/nautechsystems/nautilus_trader/blob/8b184c8af85068d788b591e10cb900fa2aba2cf2/docs/developer_guide/spec_data_testing.md) the adapter supports: TC-D01 (instruments), answered by the node's catalog, LSE publishing no definitions, into which the tester writes the crate's test definitions; TC-D31 (historical trades), of gold futures; and TC-D41 (historical bars), gold spot's 1-minute candles, over a past hour. The adapter streams nothing, so every subscription case is skipped, and every other request case, LSE having no call for it. The node runs until interrupted:

```sh
cargo run -p lse --features test-support --example lse-data-tester
```

Run on 13 November 2026 with the free key, over 10:00 to 11:00 on 12 November: TC-D01 answered both definitions from the catalog, the tester warning that their timestamps are 0, the test definitions being stamped at 1970; TC-D31 got 2,285 trades of gold futures, and none of gold spot, a commodity, which LSE stores no trades of; TC-D41 got 61 bars of gold spot, closing a minute apart from 10:00 to 11:00, at the nanosecond scale, with valid OHLC values.
