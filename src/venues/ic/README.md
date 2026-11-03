# IC Markets

IC Markets serves `ICMARKETS` instruments, history and live (see the [routing table](../mod.rs)), through the [cTrader adapter](../../../crates/ctrader/README.md), which this module configures: venue `ICMARKETS`, clients named `ICMARKETS`, account `ICMARKETS-DEMO` or `ICMARKETS-LIVE`, and the connection read from the environment. Forex and metals definitions and quotes fill through the node, and the live clients trade a hedging account on the same cTrader session; see the adapter for the session, the tick history, live quotes and the execution client. Only quote series are served; building the vendored protobufs requires `protoc`. See [catalog](../../catalog/README.md) for shared fill semantics and [live](../../live/README.md) for startup, routing, reconciliation and recording. Sessions can renew tokens, including during tests.

## Credentials

Set `IC_ENVIRONMENT` (`demo` or `live`), `IC_CLIENT_ID`, `IC_CLIENT_SECRET`, `IC_ACCOUNT_ID`, `IC_ACCESS_TOKEN` and `IC_REFRESH_TOKEN` in the environment or `.env`. The app's client id and secret come from openapi.ctrader.com, the token pair and the account's ctidTraderAccountId from the app's Playground. The token pair seeds the token file; `.env` is not updated and its pair is spent after renewal.

The latest pair belongs to the user state file at `$XDG_STATE_HOME/helix-trader/ic-token.json`, or `~/.local/state/helix-trader/ic-token.json` by default, which fills and live sessions share; see [the adapter's token file](../../../crates/ctrader/README.md#session-and-token-file) for its locking and renewal. If renewal is refused, or the connection or process dies between the renewal request and its answer, generate a new pair in the cTrader Playground and replace both token variables; the changed seed replaces the file on the next connection.

## Account, margins and costs

`IC_ENVIRONMENT` must be `demo` or `live`. Startup checks credentials and opens a separate account-check session before building the node. It refuses the wrong account, a netting or spread-betting account, and a token that cannot be renewed. The execution client checks the account again on connect. IC accounts are hedging by default.

Instrument margins are IC's: the lower of the account's leverage and the symbol's first dynamic-leverage tier (on the demo account, 1:30 under gold's 1:200 and EUR/USD's 1:1000). IC's demo symbols have one tier, which covers every size. Both catalog and live instruments use these margins; IC backtests use leverage 1. A catalog created before those margins were populated requires a fresh fill into a new directory. The demo account's commission is IC's Raw charge, 30 USD per million per side, charged in the account's currency (checked by `tests/live/ic_orders_demo.rs`): a backtest's `--commission-rate 0.00003`.

For a EUR account trading USD-quoted instruments, subscribe to `EURUSD.ICMARKETS` quotes as well: Nautilus needs the USD/EUR conversion rate for margin validation and denies orders without it. The data client holds live quotes while a history request is answered, which avoids the live-before-warmup race described for [IB](../ib/README.md).

## Tests

Offline tests of the environment and the clients' configuration are in `src/venues/ic`; the adapter's are in [the crate](../../../crates/ctrader/README.md#tests). The demo tests `tests/live/ic_demo.rs`, `tests/live/ic_live_demo.rs`, `tests/live/ic_orders_demo.rs` and `tests/live/ic_token_demo.rs` require `NAUTILUS_IC_RUN=1` in the shell and a demo account. **The orders test places demo-account orders**; verify the account and check it afterwards. Tests may renew shared tokens; see [credentials](#credentials) for token safety. The renewal test `ic_token_demo::the_demo_session_renews_its_token_and_keeps_receiving_quotes` **rotates the real token pair** in the token file; run it only when prepared to manage the shared state file and recover credentials if renewal is interrupted. Run live-node tests in separate processes.

```sh
NAUTILUS_IC_RUN=1 cargo test --test live ic_demo:: -- --nocapture
NAUTILUS_IC_RUN=1 cargo test --test live ic_live_demo:: -- --nocapture
NAUTILUS_IC_RUN=1 cargo test --test live ic_orders_demo:: -- --nocapture
NAUTILUS_IC_RUN=1 cargo test --test live ic_token_demo:: -- --nocapture
```
