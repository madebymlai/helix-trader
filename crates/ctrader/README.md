# cTrader

A NautilusTrader adapter for Spotware's cTrader Open API, naming no broker: forex and metals definitions, quotes from cTrader's tick history and live from its spot events, and an execution client on a hedging account, all on one cTrader session. The caller names the broker: its venue (the venue of the instruments and of the account `<venue>-DEMO` or `<venue>-LIVE`, its clients named after it) and the variables of its account (`CTraderAccountVars`), which `CTraderConnection::from_vars` reads beside the login's credential. Spotware's demo and live endpoints are selected by the account's environment. Only quotes are served (`SERVED_DATA`; `quoted` refuses any other series), and a tick request spans a week at most (`TICK_REQUEST_SPAN`). Building the vendored protobufs (`proto/`, Spotware's, MIT-licensed) requires `protoc`.

## Credentials

The credential is the cTrader login's, not a broker's: one Open API app and one token pair serve the accounts the login holds at every broker, and a renewal invalidates the old pair, so every broker's connection shares them and their token file. Set `CTRADER_CLIENT_ID`, `CTRADER_CLIENT_SECRET`, `CTRADER_ACCESS_TOKEN` and `CTRADER_REFRESH_TOKEN` in the process' environment. The app's client id and secret come from openapi.ctrader.com, the token pair from the app's Playground, which also lists the accounts it grants, each with its ctidTraderAccountId and broker. Each account's environment (`demo` or `live`) and id are in the variables its caller names.

The token pair seeds the token file, `$XDG_STATE_HOME/ctrader/token.json`, or `~/.local/state/ctrader/token.json` by default, which fills and live sessions of every broker share; the environment's pair is spent after renewal.

## Session and token file

A session authenticates the app, then the account, with the token file's pair: the credential's pair seeds the file, which then keeps the latest pair; the settings the credential came from are never written. A refresh invalidates the old pair. The file's directory is mode 0700 and the file 0600. Renewal holds an advisory lock across the request and atomic file replacement, allowing sessions of several processes sharing the file to use the latest pair rather than spend the same refresh token. Keep the file private and shared by processes using the same credential. If renewal is refused, or the connection or process dies between the renewal request and its answer, a new pair from the cTrader Playground replaces both tokens of the credential; the changed seed replaces the file on the next connection. A running session renews before expiry and reauthenticates when necessary.

After reconnect the session reauthenticates and resubscribes; in-flight requests fail. Trade outcomes require live reconciliation. Each frame is bound to the connection it was sent on and is never replayed on another: one refused at a drop waits for the new session's authentication, and one whose write timed out counts as sent, its outcome left to reconciliation. Historical requests go one every 250 ms: one every 200 ms was rate limited on a demo.

## Tick history

cTrader history provides BID and ASK ticks separately, not sizes. The session pages backwards in at most week-long windows, retaining ticks at page boundaries; a full page in one millisecond fails rather than silently losing data. It merges ticks into one quote per millisecond, carrying the last value of the other side, including a seed from before the window. No quote is emitted before both sides are known; crossed quotes are dropped and locked quotes kept. Tick milliseconds become both `ts_event` and `ts_init` in nanoseconds.

Synthetic quote size is 100,000 units or the instrument's maximum order size if smaller. Orders exceeding that size can fill partly at a worse price in backtests. Conversely, Nautilus adds quote sizes into aggregated bar volume; enough ticks can overflow its quantity limit and panic. Long or unusually active bars therefore need particular care: increasing quote size reduces the overflow threshold. A request holds a window and its preceding seed ticks in memory.

## Live quotes

The data client's live quotes are the spot events, merged by the rule of the tick history: on a demo the spot events of a minute were the ticks the history serves for it. It serves quotes, not external or composite bars; bars are built internally from its quotes. Failed history requests report the failure and are not written back. A market closed throughout a short warmup can produce an empty response and a cold strategy.

## Instruments and account

An instrument id is the symbol's name in cTrader on the venue (`XAUUSD.<venue>`). Symbols with known currency assets become currency pairs; indices and shares are refused. cTrader symbol IDs differ between servers and are resolved by name. Initial and maintenance margins use the reciprocal of the lower of account leverage and the symbol's first dynamic-leverage tier; higher tiers can require more margin at the broker than this single rate models. Margins are fixed at connect.

The account must be hedged, be the credential's, and use a deposit currency Nautilus knows: the execution client checks it on connect, and the caller can check it before (`CTraderSession::check_account`). Account balance and margin state are published at connect but are not refreshed after fills, so they can become stale.

## Execution client

On a hedging account an order without a position ID opens a separate position, even on the opposite side. Close or reduce a particular position by naming it; a reduce-only order without a position ID is denied. Reconciliation reports each position separately, including its protections and deals; incomplete deal history is marked incomplete rather than applied as a false position. An order's fills are its deals alone (`requires_authoritative_fills`): reconciliation waits for them rather than infer a fill from an order's status.

The client supports market, limit and stop-market orders and supported brackets. Unsupported order types, time-in-force values and flags are denied. Bracket protections live on the cTrader position; amendments preserve the other protection because cTrader can otherwise remove it. Requests sent without an answer can remain in flight until venue events or reconciliation resolve them. Partial-close protection updates have only mock-peer coverage, not venue verification. Absolute protections on a market entry were accepted on a demo despite the protocol's stated restriction; do not assume the same behavior on every account.

## Tests

The tests run against a mock cTrader peer replaying payloads captured from a broker's demo (`testdata/`, scrubbed), under a made-up venue, client id and setting names, so a broker's name left in the adapter fails a test. They run with the workspace's, or alone:

```sh
cargo test -p ctrader
```

The end-to-end test of a session's token renewal on a real demo needs a broker's credential, so its caller holds and gates it, as this repository's caller does in its tests; it **rotates the real token pair**. The `test-support` feature exposes the hooks it needs, as Nautilus' crates expose theirs: a forced renewal (`CTraderSession::renew_token`), a session's spot events as cTrader sends them (`CTraderSession::connect_with_spots`) and the pair the token file holds (`CTraderCredential::stored_tokens`).
