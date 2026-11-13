# LSE

A NautilusTrader adapter for London Strategic Edge's market data (LSE Data 3.0), laid out as Nautilus' adapters are: a data client only, as LSE trades nothing, naming no symbol. It serves the trades of LSE's derived markets, their tick slices, as `TradeTick`s; every other request fails as Nautilus' data client default does (unsupported), and every subscription is logged as not implemented. It decides nothing a key may have: it asks LSE, and LSE's refusal is the request's failure.

## Instruments

A series of a derived market (futures, stocks, indices, forex, commodities, crypto) is one instrument under the venue `LSEDGE`, not `LSE`, which is IB's London Stock Exchange. Its symbol is LSE's symbol, a hyphen and LSE's market in capitals: GOLD in `futures` is `GOLD-FUTURES.LSEDGE` (`LseSymbol`). LSE lists one symbol in several markets, and its market names have no hyphen, so the last hyphen splits symbol from market. An exchange market's listings (`cme`, `nasdaq`, `us_options`) are not `LSEDGE` series.

LSE publishes no instrument definitions. The client answers an instrument request with none, a completed envelope with no payload, and converts LSE's prices and sizes at the precisions of the instrument in its cache, so the definition must reach the cache from elsewhere, such as a catalog Nautilus answers the instrument request from first. A trades request of an instrument the cache lacks fails naming it, before LSE is asked.

## Credentials

The key is `LSE_API_KEY`, or the config's `api_key`, read when the node creates the client. It goes in the `X-API-Key` header of every call; no `Debug` output and no error holds it, and redirects are refused, so that it goes nowhere else.

## Exports

A trades request asks LSE's tick slice (`/v3/export`, `timeframe=tick`) as Parquet, at prices as published (`adjusted=0`), a UTC day an export over two timestamps, `[from, to)`. LSE answers an export with its file: since 6 November 2026 it keeps no prepared file (`/v3/jobs` answers `export-unknown`), so there is no job to poll. A file is checked against the SHA-256 LSE gives with it (`x-checksum-sha256`), and one cut short is fetched again. One export downloads at a time, as LSE allows one a key.

Calls are spaced to stay within the calls a minute of the key's plan in any minute: the config's `calls_per_minute`, by default the free plan's 60, the fewest of any plan; set the plan's figure (`/v3/usage`, `calls_per_minute`) to go faster. A refusal for the moment, a 429 of calls or of the one download (`calls-exhausted`, `connections-exhausted`, `exports-busy`) or a busy 503, is asked again after the `Retry-After` LSE gives, or a backoff, ten times at most. A 429 of a quota (`allowance-exhausted`, `rows-exhausted`) fails the request at once: it lasts until Monday or until 00:00 UTC. A selection LSE holds no row of (`data-unavailable`, `date-not-trading`) answers no trades. Every other refusal fails the request with LSE's status, code, message and required plan, for example `403 plan-required: This feed requires a subscription; required plan: CME Group Lite`.

LSE sends prices and sizes as doubles. Each is converted once, through its shortest decimal form, which must fit the instrument's precision: a finer price fails the request rather than being rounded, as a definition too coarse would round it. A trade is stamped at LSE's time, both `ts_event` and `ts_init`, with no aggressor side, LSE sending none; its trade id is a hash (FNV-1a, as Nautilus' Databento adapter derives ids) of its instrument, time, price, size and its place among the trades alike before it in the file, so trades alike keep ids apart and a refill gives the same ids.

## Tests

The tests run against a mock LSE peer (`testing`) replaying payloads captured from LSE with the free key, and LSE's documented answers where none was captured (`test_data/`, see its README). With the `test-support` feature the peer and its payloads serve a caller's tests of its own fill through the adapter. They run with the workspace's, or alone:

```sh
cargo test -p lse
```

A test against LSE itself needs a key, so its caller holds and gates it, as this repository's caller does (`NAUTILUS_LSE_RUN=1`, see the root README).
