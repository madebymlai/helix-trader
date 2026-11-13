# LSE

A NautilusTrader adapter for London Strategic Edge's market data (LSE Data 3.0), laid out as Nautilus' adapters are: a data client only, as LSE trades nothing, naming no symbol. It serves the trades of LSE's derived markets, their tick slices, as `TradeTick`s, and streams their quotes, a futures series' on data-ws, as `QuoteTick`s; every other request fails as Nautilus' data client default does (unsupported), and every other subscription is logged as not implemented. It decides nothing a key may have: it asks LSE, and LSE's refusal is the request's failure.

## Instruments

A series of a derived market (futures, stocks, indices, forex, commodities, crypto) is one instrument under the venue `LSEDGE`, not `LSE`, which is IB's London Stock Exchange. Its symbol is LSE's symbol, a hyphen and LSE's market in capitals: GOLD in `futures` is `GOLD-FUTURES.LSEDGE` (`LseSymbol`). LSE lists one symbol in several markets, and its market names have no hyphen, so the last hyphen splits symbol from market. An exchange market's listings (`cme`, `nasdaq`, `us_options`) are not `LSEDGE` series.

LSE publishes no instrument definitions. The client answers an instrument request with none, a completed envelope with no payload, and converts LSE's prices and sizes at the precisions of the instrument in its cache, so the definition must reach the cache from elsewhere, such as a catalog Nautilus answers the instrument request from first. A trades request of an instrument the cache lacks fails naming it, before LSE is asked.

## Credentials

The key is `LSE_API_KEY`, or the config's `api_key`, read when the node creates the client. It goes in the `X-API-Key` header of every call; no `Debug` output and no error holds it, and redirects are refused, so that it goes nowhere else.

## Exports

A trades request asks LSE's tick slice (`/v3/export`, `timeframe=tick`) as Parquet, at prices as published (`adjusted=0`), a UTC day an export over two timestamps, `[from, to)`. LSE answers an export with its file: since 6 November 2026 it keeps no prepared file (`/v3/jobs` answers `export-unknown`), so there is no job to poll. A file is checked against the SHA-256 LSE gives with it (`x-checksum-sha256`), and one cut short is fetched again. One export downloads at a time, as LSE allows one a key.

Calls are spaced to stay within the calls a minute of the key's plan in any minute: the config's `calls_per_minute`, by default the free plan's 60, the fewest of any plan; set the plan's figure (`/v3/usage`, `calls_per_minute`) to go faster. A refusal for the moment, a 429 of calls or of the one download (`calls-exhausted`, `connections-exhausted`, `exports-busy`) or a busy 503, is asked again after the `Retry-After` LSE gives, or a backoff, ten times at most. A 429 of a quota (`allowance-exhausted`, `rows-exhausted`) fails the request at once: it lasts until Monday or until 00:00 UTC. A selection LSE holds no row of (`data-unavailable`, `date-not-trading`) answers no trades. Every other refusal fails the request with LSE's status, code, message and required plan, for example `403 plan-required: This feed requires a subscription; required plan: CME Group Lite`.

LSE sends prices and sizes as doubles. Each is converted once, through its shortest decimal form, which must fit the instrument's precision: a finer price fails the request rather than being rounded, as a definition too coarse would round it. A trade is stamped at LSE's time, both `ts_event` and `ts_init`, with no aggressor side, LSE sending none; its trade id is a hash (FNV-1a, as Nautilus' Databento adapter derives ids) of its instrument, time, price, size and its place among the trades alike before it in the file, so trades alike keep ids apart and a refill gives the same ids.

## Live quotes

The config names the instruments the client streams (`stream_ids`); none on a fill's node, which opens no socket. `connect` opens the socket each one's market streams on, one connection a socket, as LSE's limits ask: a futures series on data-ws, as its symbol suffixed `.F` (`GOLD-FUTURES.LSEDGE` is `GOLD.F`, as LSE's socket documentation spells it); LSE's other markets stream nowhere here yet, and naming one fails the client's creation. It answers the socket's welcome with the key and is connected once LSE authenticates it, as Nautilus' adapter guide asks of `connect`. It fails, closing what it opened, with LSE's code and message if LSE refuses the key (`INVALID_KEY`) or a connection beyond the key's (`TOO_MANY_CONNECTIONS`, giving `max_connections`), naming the instruments the socket does not list to the key, LSE accepting a subscription to a symbol it does not list and sending nothing, and naming LSE's limit if the instruments of a socket are more than the subscriptions LSE says one connection of the key allows (`max_subscriptions`, 16 on the free key): LSE refuses one beyond it (`LIMIT_REACHED`) only once asked, after the node started.

A quotes subscription reads the instrument's precisions from the cache, failing naming it if the cache has no definition, then subscribes its symbol. Each tick becomes a quote of its bid and ask at the instrument's precision, each through its shortest decimal form, which must fit it, as an export's prices; a finer one is skipped with a warning rather than rounded. Its sizes are zero, LSE's socket ticks carrying none, and its price and volume are dropped: a futures tick's price is its bid. It is stamped at LSE's time of it (`ts_event`) and when it came (`ts_init`). LSE opens a subscription with the symbol's last tick, which comes through as any other. LSE's refusal of a subscription, such as `LIMIT_REACHED`, is logged as an error with its code and message, and the subscription is sent again on the next connection.

Each socket is a client on Nautilus' `WebSocketClient`, an outer client and the handler task owning it, as Nautilus' adapter guide lays one out ("WebSocket client patterns"). The application keepalive LSE asks for, `{"action": "ping"}`, goes every 25 s, as LSE's Python client sends it. A dropped connection is reconnected by Nautilus' transport with backoff (1 s, doubling to 30 s, with jitter); the client authenticates the key again, sends every subscription again (Nautilus' `SubscriptionState`), and logs the gap, from the dropped connection's last frame to the new one's authentication, leaving it as it is: nothing is fetched to fill it. A connection LSE closes for a refusal a new one cannot lift (`CONNECTION_REPLACED`, `KEY_IN_USE`, `KEY_REVOKED`, `QUOTA_EXCEEDED`) is not reconnected; one closed as beyond the key's connections is, with backoff, as LSE may still count the connection dropped.

## Tests

The tests run against mock LSE peers (`testing`), an HTTP server and a socket, replaying payloads and frames captured from LSE with the free key, and LSE's documented answers where none was captured (`test_data/`, see its README). With the `test-support` feature the peers and their payloads serve a caller's tests of its own fill and live node through the adapter. The streamed quotes are tested as Nautilus' data testing specification asks (`docs/developer_guide/spec_data_testing.md`), its `DataTester` driving the client on a node against the mock socket: TC-D20 (quotes after start) and TC-D70 (their subscription ended on stop); TC-D21 is skipped, LSE having no quotes of a derived market's past. They run with the workspace's, or alone:

```sh
cargo test -p lse
```

A test against LSE itself needs a key, so its caller holds and gates it, as this repository's caller does (`NAUTILUS_LSE_RUN=1`, see the root README).
