# cTrader

A NautilusTrader adapter for Spotware's cTrader Open API, naming no broker: forex and metals definitions, quotes from cTrader's tick history and live from its spot events, and an execution client on a hedging account, all on one cTrader session. The caller names the broker: its venue (`CTraderConnection::venue`, the venue of the instruments and of the account `<venue>-DEMO` or `<venue>-LIVE`), its clients' id (`client_id` of each client config, the factories' names too), the environment, the credential with its token file, and the names of the settings it read them from, which the errors asking to check one cite (`CTraderSettingNames`). Spotware's demo and live endpoints are selected by the environment. Only quotes are served (`SERVED_DATA`), and a tick request spans a week at most (`TICK_REQUEST_SPAN`). Building the vendored protobufs (`proto/`, Spotware's, MIT-licensed) requires `protoc`.

## Session and token file

A session authenticates the app, then the account, with the token file's pair: the credential's pair seeds the file, which then keeps the latest pair; the settings the credential came from are never written, and their pair is spent after renewal. A refresh invalidates the old pair. The file's directory is mode 0700 and the file 0600. Renewal holds an advisory lock across the request and atomic file replacement, allowing sessions of several processes sharing the file to use the latest pair rather than spend the same refresh token. Keep the file private and shared by processes using the same credential. If renewal is refused, or the connection or process dies between the renewal request and its answer, a new pair from the cTrader Playground replaces both tokens of the credential; the changed seed replaces the file on the next connection. A running session renews before expiry and reauthenticates when necessary.

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

On a hedging account an order without a position ID opens a separate position, even on the opposite side. Close or reduce a particular position by naming it; a reduce-only order without a position ID is denied. Reconciliation reports each position separately, including its protections and deals; incomplete deal history is marked incomplete rather than applied as a false position.

The client supports market, limit and stop-market orders, trailing stops as a position's stop loss, and supported brackets. Unsupported order types, time-in-force values and flags are denied. Bracket protections live on the cTrader position; amendments preserve the other protection because cTrader can otherwise remove it. Requests sent without an answer can remain in flight until venue events or reconciliation resolve them. Partial-close protection updates have only mock-peer coverage, not venue verification. Absolute protections on a market entry were accepted on a demo despite the protocol's stated restriction; do not assume the same behavior on every account.

### Trailing stops

cTrader trails only a position's stop loss (`trailingStopLoss`), so a trailing stop market goes two ways:

- **As a bracket's stop loss**, on the entry, relative (`relativeStopLoss`): cTrader sets it at the entry's fill less the offset (plus, for a sell). cTrader takes no mix of absolute and relative protections on an entry ("SL/TP in absolute values and in pips are not allowed together"), so the bracket's take profit goes relative too, its distance from the entry's price (a market entry's: the node's last quote). A market entry's take profit thus lands off the strategy's price by the entry's slippage; once the position holds the protections, both legs take cTrader's levels. cTrader states a pending entry's relative protections relative only, so its legs take the levels they imply at the entry's price.
- **On an open position it names** (`position_id`, closing the position's whole quantity), absolute (`ProtoOAAmendPositionSLTPReq`): at its offset from the node's last bid, for a sell, or ask, for a buy.

The offset is by price, or in basis points of the entry's price or of that quote, rounded to the tick. Other offset types, an activation price, a time in force but GTC, a trailing stop naming no position and one without a quote to set it from are denied. A position takes one stop loss order: Nautilus' cache keeps the venue order id `<positionId>-SL` its stop loss order's, cancelled or not, so a stop on a position that has had one is denied; a modify of the trigger price moves it to a new level, which cTrader then trails at its new distance. A cancel removes the stop; a trailing leg of a pending entry has no level to move. Each move cTrader reports (`ProtoOATrailingSLChangedEvent`) updates the stop's trigger price; a move of a stop this session did not set is reported at its new level. A stop set on a position whose request goes unanswered is concluded by a query from the position: accepted at the trailing stop loss it holds, else rejected. cTrader never states the distance a stop trails at, so reconciliation reports a trailing stop loss as a trailing stop market at its level with an offset of 0. An amendment of a position's protections sends its trailing stop at its last reported level: a move cTrader made after that one is undone, and the trail restarts from there.

cTrader's trailing rule, measured on a demo (2026-11-04, longs of 1 oz of XAUUSD): the stop trails at the distance between the bid and the stop when it is set or amended. It moves on each new high of the bid, tick by tick, to that bid less the distance, and never loosens. A bracket's market buy fills at the ask, so its stop starts at the ask less the offset and trails the fill's spread closer than the offset (offset 0.50, spread 0.11: 0.39 behind the bid), moving as soon as the bid passes the bid at the fill. A stop hit fills at market: the demo's filled 0.25 below its level.

## Tests

The tests run against a mock cTrader peer replaying payloads captured from a broker's demo (`testdata/`, scrubbed), under a made-up venue, client id and setting names, so a broker's name left in the adapter fails a test. They run with the workspace's, or alone:

```sh
cargo test -p ctrader
```

The end-to-end test of a session's token renewal on a real demo needs a broker's credential, so its caller holds and gates it, as this repository's caller does in its tests; it **rotates the real token pair**. The `test-support` feature exposes the hooks it needs, as Nautilus' crates expose theirs: a forced renewal (`CTraderSession::renew_token`), a session's spot events as cTrader sends them (`CTraderSession::connect_with_spots`) and the pair the token file holds (`CTraderCredential::stored_tokens`).
