# IC Markets

IC Markets serves `ICMARKETS` instruments, history and live (see the [routing table](../mod.rs)), over the cTrader Open API: forex and metals definitions and quotes fill through the node, and the live clients trade a hedging account on the same cTrader session. Only quote series are served; building the vendored protobufs requires `protoc`. See [catalog](../../catalog/README.md) for shared fill semantics and [live](../../live/README.md) for startup, routing, reconciliation and recording. Sessions can renew tokens, including during tests.

## Credentials and renewal

Set `IC_ENVIRONMENT` (`demo` or `live`), `IC_CLIENT_ID`, `IC_CLIENT_SECRET`, `IC_ACCOUNT_ID`, `IC_ACCESS_TOKEN` and `IC_REFRESH_TOKEN` in the environment or `.env`. The token pair seeds the state file; `.env` is not updated and its pair is spent after renewal.

The latest pair belongs to the user state file at `$XDG_STATE_HOME/helix-trader/ic-token.json`, or `~/.local/state/helix-trader/ic-token.json` by default. The directory is mode 0700 and the file 0600. Renewal holds an advisory lock across the request and atomic file replacement, allowing fill and live sessions sharing that file to use the latest pair rather than spend the same refresh token. Keep the state file private and shared by processes using the same credentials.

A refresh invalidates the old pair. If renewal is refused, or the connection or process dies between the renewal request and its answer, generate a new pair in the cTrader Playground and replace both token variables; the changed seed replaces the file on the next connection. A running session renews before expiry and reauthenticates when necessary.

After reconnect the session reauthenticates and resubscribes; in-flight requests fail. Trade outcomes require live reconciliation. Nautilus does not yet bind binary sends to a connection: a frame already in the writer at a drop can be replayed before authentication on the new connection (nautilus_trader#5526). See [account and trading](#account-and-trading) for runtime operation.

## Quotes

cTrader history provides BID and ASK ticks separately, not sizes. The client pages backwards in at most week-long windows, retaining ticks at page boundaries; a full page in one millisecond fails rather than silently losing data. It merges ticks into one quote per millisecond, carrying the last value of the other side, including a seed from before the window. No quote is emitted before both sides are known; crossed quotes are dropped and locked quotes kept. Tick milliseconds become both `ts_event` and `ts_init` in nanoseconds.

Synthetic quote size is 100,000 units or the instrument's maximum order size if smaller. Orders exceeding that size can fill partly at a worse price in backtests. Conversely, Nautilus adds quote sizes into aggregated bar volume; enough ticks can overflow its quantity limit and panic. Long or unusually active bars therefore need particular care: increasing quote size reduces the overflow threshold. The fill holds a window and its preceding seed ticks in memory. Check source depth separately from catalog coverage as described in [shared fill semantics](../../catalog/README.md).

## Instruments and account

Symbols with known currency assets become currency pairs; indices and shares are refused. cTrader symbol IDs differ between servers and are resolved by name. The account must be hedged, match `IC_ACCOUNT_ID`, and use a deposit currency Nautilus knows.

Instrument initial and maintenance margins use the reciprocal of the lower of account leverage and the symbol's first dynamic-leverage tier. Higher tiers can require more margin at IC than this single rate models. Both catalog and live instruments use these margins; IC backtests use leverage 1. A catalog created before those margins were populated requires a fresh fill into a new directory.

## Account and trading

`IC_ENVIRONMENT` must be `demo` or `live`. Startup checks credentials and opens a separate account-check session before building the node. It refuses the wrong account, a netting or spread-betting account, and a token that cannot be renewed. The execution client checks the account again on connect.

IC uses a hedging account: an order without a position ID opens a separate position, even on the opposite side. Close or reduce a particular position by naming it; a reduce-only order without a position ID is denied. Reconciliation reports each position separately, including its protections and deals; incomplete deal history is marked incomplete rather than applied as a false position.

The adapter supports market, limit and stop-market orders and supported brackets. Unsupported order types, time-in-force values and flags are denied. Bracket protections live on the cTrader position; amendments preserve the other protection because cTrader can otherwise remove it. Requests sent without an answer can remain in flight until venue events or reconciliation resolve them. Partial-close protection updates have only mock-peer coverage, not venue verification. Absolute protections on a market entry were accepted on demo despite the protocol's stated restriction; do not assume the same behavior on every account.

## Data, warmup and risk

IC serves quotes, not external or composite bars; use bars built internally from its quotes. The client holds live quotes while a history request is answered and releases them after the response, excluding duplicates. This avoids the live-before-warmup race described for [IB](../ib/README.md). Failed history requests report the failure and are not written back. A market closed throughout a short warmup can produce an empty response and a cold strategy.

Quote sizes are synthetic liquidity, not available market depth; see [quotes](#quotes) before relying on fills or backtests. For a EUR account trading USD-quoted instruments, subscribe to `EURUSD.ICMARKETS` quotes as well: Nautilus needs the USD/EUR conversion rate for margin validation and denies orders without it.

Instrument margins are fixed at connect; see [instruments and account](#instruments-and-account) for leverage-tier limits. Account balance and margin state are published at connect but are not refreshed after fills, so they can become stale.

## Tests

Offline tests use fixtures and a mock peer in `src/venues/ic`. The demo tests `tests/live/ic_demo.rs`, `tests/live/ic_live_demo.rs` and `tests/live/ic_orders_demo.rs` require `NAUTILUS_IC_RUN=1` in the shell and a demo account. **The orders test places demo-account orders**; verify the account and check it afterwards. Tests may renew shared tokens; see [credentials and renewal](#credentials-and-renewal) for token safety. The library renewal test `the_demo_session_renews_its_token_and_keeps_receiving_quotes` **rotates the real token pair**; run it only when prepared to manage the shared state file and recover credentials if renewal is interrupted. Run live-node tests in separate processes.

```sh
NAUTILUS_IC_RUN=1 cargo test --test live ic_demo:: -- --nocapture
NAUTILUS_IC_RUN=1 cargo test --test live ic_live_demo:: -- --nocapture
NAUTILUS_IC_RUN=1 cargo test --test live ic_orders_demo:: -- --nocapture
```
