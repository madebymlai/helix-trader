# cTrader

A NautilusTrader adapter for Spotware's cTrader Open API, naming no broker: forex and metals definitions, quotes from cTrader's tick history and live from its spot events, and an execution client on a hedging account, all on one cTrader session. The caller names the broker: its venue (`CTraderConnection::venue`, the venue of the instruments and of the account `<venue>-DEMO` or `<venue>-LIVE`), its clients' id (`client_id` of each client config, the factories' names too), the environment, the credential with its token file, and the names of the settings it read them from, which the errors asking to check one cite (`CTraderSettingNames`). Spotware's demo and live endpoints are selected by the environment. Only quotes are served (`SERVED_DATA`), and a tick request spans a week at most (`TICK_REQUEST_SPAN`). Building the vendored protobufs (`proto/`, Spotware's, MIT-licensed) requires `protoc`.

## Session and token file

A session authenticates the app, then the account, with the token file's pair: the credential's pair seeds the file, which then keeps the latest pair; the settings the credential came from are never written, and their pair is spent after renewal. A refresh invalidates the old pair. The file's directory is mode 0700 and the file 0600. Renewal holds an advisory lock across the request and atomic file replacement, allowing sessions of several processes sharing the file to use the latest pair rather than spend the same refresh token. Keep the file private and shared by processes using the same credential. If renewal is refused, or the connection or process dies between the renewal request and its answer, a new pair from the cTrader Playground replaces both tokens of the credential; the changed seed replaces the file on the next connection. A running session renews before expiry and reauthenticates when necessary.

After reconnect the session reauthenticates and resubscribes; in-flight requests fail. Trade outcomes require live reconciliation. Nautilus does not yet bind binary sends to a connection: a frame already in the writer at a drop can be replayed before authentication on the new connection (nautilus_trader#5526). Historical requests go one every 250 ms: one every 200 ms was rate limited on a demo.

## Tick history

cTrader history provides BID and ASK ticks separately, not sizes. The session pages backwards in at most week-long windows, retaining ticks at page boundaries; a full page in one millisecond fails rather than silently losing data. It merges ticks into one quote per millisecond, carrying the last value of the other side, including a seed from before the window. No quote is emitted before both sides are known; crossed quotes are dropped and locked quotes kept. Tick milliseconds become both `ts_event` and `ts_init` in nanoseconds.

Synthetic quote size is 100,000 units or the instrument's maximum order size if smaller. Orders exceeding that size can fill partly at a worse price in backtests. Conversely, Nautilus adds quote sizes into aggregated bar volume; enough ticks can overflow its quantity limit and panic. Long or unusually active bars therefore need particular care: increasing quote size reduces the overflow threshold. A request holds a window and its preceding seed ticks in memory.

## Live quotes

The data client's live quotes are the spot events, merged by the rule of the tick history: on a demo the spot events of a minute were the ticks the history serves for it. It serves quotes, not external or composite bars; bars are built internally from its quotes. It holds live quotes while a history request is answered and releases them after the response, excluding duplicates. Failed history requests report the failure and are not written back. A market closed throughout a short warmup can produce an empty response and a cold strategy.

## Instruments and account

An instrument id is the symbol's name in cTrader on the venue (`XAUUSD.<venue>`). Symbols with known currency assets become currency pairs; indices and shares are refused. cTrader symbol IDs differ between servers and are resolved by name. Initial and maintenance margins use the reciprocal of the lower of account leverage and the symbol's first dynamic-leverage tier; higher tiers can require more margin at the broker than this single rate models. Margins are fixed at connect.

The account must be hedged, be the credential's, and use a deposit currency Nautilus knows: the execution client checks it on connect, and the caller can check it before (`CTraderSession::check_account`). Account balance and margin state are published at connect but are not refreshed after fills, so they can become stale.

## Execution client

On a hedging account an order without a position ID opens a separate position, even on the opposite side. Close or reduce a particular position by naming it; a reduce-only order without a position ID is denied. Reconciliation reports each position separately, including its protections and deals; incomplete deal history is marked incomplete rather than applied as a false position.

The client supports market, limit and stop-market orders and supported brackets. Unsupported order types, time-in-force values and flags are denied. Bracket protections live on the cTrader position; amendments preserve the other protection because cTrader can otherwise remove it. Requests sent without an answer can remain in flight until venue events or reconciliation resolve them. Partial-close protection updates have only mock-peer coverage, not venue verification. Absolute protections on a market entry were accepted on a demo despite the protocol's stated restriction; do not assume the same behavior on every account.

## Tests

The tests run against a mock cTrader peer replaying payloads captured from a broker's demo (`testdata/`, scrubbed), under a made-up venue, client id and setting names, so a broker's name left in the adapter fails a test. They run with the workspace's, or alone:

```sh
cargo test -p ctrader
```

The end-to-end test of a session's token renewal on a real demo needs a broker's credential, so its caller holds and gates it, as this repository's caller does in its tests; it **rotates the real token pair**. The `test-support` feature exposes the hooks it needs, as Nautilus' crates expose theirs: a forced renewal (`CTraderSession::renew_token`), a session's spot events as cTrader sends them (`CTraderSession::connect_with_spots`) and the pair the token file holds (`CTraderCredential::stored_tokens`).
