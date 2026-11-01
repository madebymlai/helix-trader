# IC Markets history and cTrader session

IC forex and metals definitions and quotes fill through the [node](../README.md), using the cTrader Open API. Only quote series are served. The shared session also supports the [live IC clients](../../live/ic/README.md); building the vendored protobufs requires `protoc`.

## Credentials and renewal

Set `IC_ENVIRONMENT` (`demo` or `live`), `IC_CLIENT_ID`, `IC_CLIENT_SECRET`, `IC_ACCOUNT_ID`, `IC_ACCESS_TOKEN` and `IC_REFRESH_TOKEN` in the environment or `.env`. The token pair seeds the state file; `.env` is not updated and its pair is spent after renewal.

The latest pair belongs to the user state file at `$XDG_STATE_HOME/helix-trader/ic-token.json`, or `~/.local/state/helix-trader/ic-token.json` by default. The directory is mode 0700 and the file 0600. Renewal holds an advisory lock across the request and atomic file replacement, allowing fill and live sessions sharing that file to use the latest pair rather than spend the same refresh token. Keep the state file private and shared by processes using the same credentials.

A refresh invalidates the old pair. If renewal is refused, or the connection or process dies between the renewal request and its answer, generate a new pair in the cTrader Playground and replace both token variables; the changed seed replaces the file on the next connection. A running session renews before expiry and reauthenticates when necessary.

After reconnect the session reauthenticates and resubscribes; in-flight requests fail. Trade outcomes require live reconciliation. Nautilus does not yet bind binary sends to a connection: a frame already in the writer at a drop can be replayed before authentication on the new connection (nautilus_trader#5526). See [live IC](../../live/ic/README.md) for runtime operation.

## Quotes

cTrader history provides BID and ASK ticks separately, not sizes. The client pages backwards in at most week-long windows, retaining ticks at page boundaries; a full page in one millisecond fails rather than silently losing data. It merges ticks into one quote per millisecond, carrying the last value of the other side, including a seed from before the window. No quote is emitted before both sides are known; crossed quotes are dropped and locked quotes kept. Tick milliseconds become both `ts_event` and `ts_init` in nanoseconds.

Synthetic quote size is 100,000 units or the instrument's maximum order size if smaller. Orders exceeding that size can fill partly at a worse price in backtests. Conversely, Nautilus adds quote sizes into aggregated bar volume; enough ticks can overflow its quantity limit and panic. Long or unusually active bars therefore need particular care: increasing quote size reduces the overflow threshold. The fill holds a window and its preceding seed ticks in memory. Check source depth separately from catalog coverage as described in [shared fill semantics](../README.md).

## Instruments and account

Symbols with known currency assets become currency pairs; indices and shares are refused. cTrader symbol IDs differ between servers and are resolved by name. The account must be hedged, match `IC_ACCOUNT_ID`, and use a deposit currency Nautilus knows.

Instrument initial and maintenance margins use the reciprocal of the lower of account leverage and the symbol's first dynamic-leverage tier. Higher tiers can require more margin at IC than this single rate models. Both catalog and live instruments use these margins; IC backtests use leverage 1. A catalog created before those margins were populated requires a fresh fill into a new directory.

## Tests

Offline tests use fixtures and a mock peer in `src/data/ic`. Demo tests are gated by `NAUTILUS_IC_RUN=1`; see [live IC](../../live/ic/README.md) for test isolation and execution warnings. The library renewal test `the_demo_session_renews_its_token_and_keeps_receiving_quotes` **rotates the real token pair**; run it only when prepared to manage the shared state file and recover credentials if renewal is interrupted.
