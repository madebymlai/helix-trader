# cTrader

A NautilusTrader adapter for Spotware's cTrader Open API, naming no broker: forex and metals definitions, quotes from cTrader's tick history and live from its spot events, and an execution client on a hedging account, all on one cTrader session. The caller names the broker: its venue (the venue of the instruments and of the account `<venue>-DEMO` or `<venue>-LIVE`, its clients named after it) and the variables of its account (`CTraderAccountVars`), which `CTraderConnection::from_vars` reads beside the login's credential. Spotware's demo and live endpoints are selected by the account's environment. Only quotes are served (`SERVED_DATA`; `quoted` refuses any other series), and a tick request spans a week at most (`TICK_REQUEST_SPAN`). Building the vendored protobufs (`proto/`, Spotware's, MIT-licensed) requires `protoc`.

## Credentials

The credential is the cTrader login's, not a broker's: one Open API app and one token pair serve the accounts the login holds at every broker, and a renewal invalidates the old pair, so every broker's connection shares them and their token file. Set `CTRADER_CLIENT_ID`, `CTRADER_CLIENT_SECRET`, `CTRADER_ACCESS_TOKEN` and `CTRADER_REFRESH_TOKEN` in the process' environment. The app's client id and secret come from openapi.ctrader.com, the token pair from the app's Playground, which also lists the accounts it grants, each with its ctidTraderAccountId and broker. Each account's environment (`demo` or `live`) and id are in the variables its caller names.

The environment's pair seeds `$XDG_STATE_HOME/ctrader/token.json`, or `~/.local/state/ctrader/token.json` by default, shared by fills and live sessions of every broker. The file keeps the latest pair; the environment's pair is spent after renewal, and the settings it came from are never written. The directory is mode 0700 and the file 0600. Renewal holds an advisory lock across the request and atomic file replacement, allowing processes sharing the file to use the latest pair rather than spend the same refresh token. Keep the file private and shared by processes using the same credential. If renewal is refused, or the connection or process dies between the renewal request and its answer, obtain a new pair from the cTrader Playground and replace both tokens of the credential; the changed seed replaces the file on the next connection.

## Session and token file

A session authenticates the app, then the account, with the token file's pair. A running session renews before expiry and reauthenticates when necessary.

After reconnect the session reauthenticates and resubscribes; in-flight requests fail. Each client's socket reports its availability to the node through Nautilus' `SocketControl`, as `SocketStateChanged` events of the client, and takes the node's `reconnect_socket` of its endpoint, after which its session authenticates again as after a drop. A broker's two clients share its client id, so each socket has its own endpoint: `ctrader-open-api-data` for the data client's, `ctrader-open-api-trading` for the execution client's. Trade outcomes require live reconciliation. Each frame is bound to the connection it was sent on and is never replayed on another: one refused at a drop waits for the new session's authentication, and one whose write timed out or failed after it began counts as sent, its outcome left to reconciliation. Historical requests go one every 250 ms: one every 200 ms was rate limited on a demo.

## Tick history

cTrader history provides BID and ASK ticks separately, not sizes. The session pages backwards in at most week-long windows, retaining ticks at page boundaries; a full page in one millisecond fails rather than silently losing data. It merges ticks into one quote per millisecond, carrying the last value of the other side, including a seed from before the window. No quote is emitted before both sides are known; crossed quotes are dropped and locked quotes kept. Tick milliseconds become both `ts_event` and `ts_init` in nanoseconds.

Synthetic quote size is 100,000 units or the instrument's maximum order size if smaller. Orders exceeding that size can fill partly at a worse price in backtests. Conversely, Nautilus adds quote sizes into aggregated bar volume; enough ticks can overflow its quantity limit and panic. Long or unusually active bars therefore need particular care: increasing quote size reduces the overflow threshold. A request holds a window and its preceding seed ticks in memory.

## Live quotes

The data client's live quotes are the spot events, merged by the rule of the tick history: on a demo the spot events of a minute were the ticks the history serves for it. It serves quotes, not external or composite bars; bars are built internally from its quotes. Failed history requests report the failure and are not written back. A market closed throughout a short warmup can produce an empty response and a cold strategy.

## Instruments and account

An instrument id is the symbol's name in cTrader on the venue (`XAUUSD.<venue>`). Symbols with known currency assets become currency pairs; indices and shares are refused. cTrader symbol IDs differ between servers and are resolved by name. Initial and maintenance margins use the reciprocal of the lower of account leverage and the symbol's first dynamic-leverage tier; higher tiers can require more margin at the broker than this single rate models. Margins are fixed at connect for the clients' own use; an instrument request asks cTrader for the instrument anew, so its answer holds the symbol's definition and margin of the moment. The spot events and the tick history are still read at the definition loaded at connect: after cTrader changes a symbol's definition, an instrument request's answer can differ from the instrument the quotes are built for until the client reconnects.

The account must be hedged, be the credential's, and use a deposit currency Nautilus knows: the execution client checks it on connect, and the caller can check it before (`CTraderSession::check_account`). Account balance and margin state are published at connect but are not refreshed after fills, so they can become stale.

## Execution client

On a hedging account an order without a position ID opens a separate position, even on the opposite side. Close or reduce a particular position by naming it; a reduce-only order without a position ID is denied. Reconciliation reports each position separately, including its protections and deals; incomplete deal history is marked incomplete rather than applied as a false position. An order's fills are its deals alone (`requires_authoritative_fills`): reconciliation waits for them rather than infer a fill from an order's status.

The client supports market, limit and stop-market orders and supported brackets. Unsupported order types, time-in-force values and flags are denied. Bracket protections live on the cTrader position; amendments preserve the other protection because cTrader can otherwise remove it. Partial-close protection updates have only mock-peer coverage, not venue verification. Absolute protections on a market entry were accepted on a demo despite the protocol's stated restriction; do not assume the same behavior on every account.

A failed order command is one of Nautilus' `CommandFailure` classes and follows its policy. A request the session never sent is rejected (a new order or a close) or denied (a modify or cancel). One cTrader refuses is rejected; a refused modify or cancel leaves the order working. One of unknown outcome emits nothing and stays in flight until venue events, a query or reconciliation resolve it: sent without an answer, its session gone without saying whether it was sent, or answered with a rate limit (`REQUEST_FREQUENCY_EXCEEDED`, `BLOCKED_PAYLOAD_TYPE`), an unreachable or maintained server (`CANT_ROUTE_REQUEST`, `CH_SERVER_NOT_REACHABLE`, `SERVER_IS_UNDER_MAINTENANCE`) or `UNKNOWN_ERROR`. A move or removal of a protection of unknown outcome stays asked even once Nautilus resolves the command, so the next amendment of the other protection of that position carries it; this matters only to a strategy that modifies its exits.

A fill's commission is the charge its deal states. A deal that states none is not booked at zero: a report request that needs it fails, and its trade event emits nothing and is applied by a later replay or reconciliation. The demo accounts the fixtures come from charge commission, so every deal there states it; a live run on a commission-free account must confirm that its deals state a commission of zero rather than omit it, since otherwise none of its fills is booked.

## Tests

The tests run against a mock cTrader peer replaying payloads captured from a broker's demo (`testdata/`, scrubbed), under a made-up venue, client id and setting names, so a broker's name left in the adapter fails a test. They run with the workspace's, or alone:

```sh
cargo test -p ctrader
```

The end-to-end test of a session's token renewal on a real demo needs a broker's credential, so its caller holds and gates it, as this repository's caller does in its tests; it **rotates the real token pair**. The `test-support` feature exposes the hooks it needs, as Nautilus' crates expose theirs: a forced renewal (`CTraderSession::renew_token`), a session's spot events as cTrader sends them (`CTraderSession::connect_with_spots`) and the pair the token file holds (`CTraderCredential::stored_tokens`).
