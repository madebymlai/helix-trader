# IC Markets live

See [live](../README.md) for startup, routing, reconciliation and recording. Configure the cTrader account, credentials, token file and session as described in [IC data](../../data/ic/README.md); sessions can renew tokens, including during tests.

## Account and trading

`IC_ENVIRONMENT` must be `demo` or `live`. Startup checks credentials and opens a separate account-check session before building the node. It refuses the wrong account, a netting or spread-betting account, and a token that cannot be renewed. The execution client checks the account again on connect.

IC uses a hedging account: an order without a position ID opens a separate position, even on the opposite side. Close or reduce a particular position by naming it; a reduce-only order without a position ID is denied. Reconciliation reports each position separately, including its protections and deals; incomplete deal history is marked incomplete rather than applied as a false position.

The adapter supports market, limit and stop-market orders and supported brackets. Unsupported order types, time-in-force values and flags are denied. Bracket protections live on the cTrader position; amendments preserve the other protection because cTrader can otherwise remove it. Requests sent without an answer can remain in flight until venue events or reconciliation resolve them. Partial-close protection updates have only mock-peer coverage, not venue verification. Absolute protections on a market entry were accepted on demo despite the protocol's stated restriction; do not assume the same behavior on every account.

## Data, warmup and risk

IC serves quotes, not external or composite bars; use bars built internally from its quotes. The client holds live quotes while a history request is answered and releases them after the response, excluding duplicates. This avoids the live-before-warmup race described for [IB](../ib/README.md). Failed history requests report the failure and are not written back. A market closed throughout a short warmup can produce an empty response and a cold strategy.

Quote sizes are synthetic liquidity, not available market depth; see [IC data](../../data/ic/README.md) before relying on fills or backtests. For a EUR account trading USD-quoted instruments, subscribe to `EURUSD.ICMARKETS` quotes as well: Nautilus needs the USD/EUR conversion rate for margin validation and denies orders without it.

Instrument margins are fixed at connect; see [IC data](../../data/ic/README.md) for leverage-tier limits. Account balance and margin state are published at connect but are not refreshed after fills, so they can become stale.

## Tests

`tests/live/ic_live_demo.rs` and `tests/live/ic_orders_demo.rs` require `NAUTILUS_IC_RUN=1` in the shell and a demo account. **The orders test places demo-account orders**; verify the account and check it afterwards. Tests may renew shared tokens; follow [IC data](../../data/ic/README.md) for token safety. Run live-node tests in separate processes.

```sh
NAUTILUS_IC_RUN=1 cargo test --test live ic_live_demo:: -- --nocapture
NAUTILUS_IC_RUN=1 cargo test --test live ic_orders_demo:: -- --nocapture
```
