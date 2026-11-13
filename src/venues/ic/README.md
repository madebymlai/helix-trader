# IC Markets

IC Markets serves `ICMARKETS` instruments, history and live (see the [routing table](../mod.rs), where it is a cTrader broker, a line of `CTRADER_BROKERS`), through the [cTrader adapter](../../../crates/ctrader/README.md), which this module configures: venue `ICMARKETS`, clients named `ICMARKETS`, account `ICMARKETS-DEMO` or `ICMARKETS-LIVE`, and the variables of its account. Forex and metals definitions and quotes fill through the node, and the live clients trade a hedging account on the same cTrader session; see the adapter for the session, the tick history, live quotes and the execution client. Only quote series are served; building the vendored protobufs requires `protoc`. See [catalog](../../catalog/README.md) for shared fill semantics and [live](../../live/README.md) for startup, routing, reconciliation and recording. Sessions can renew tokens, including during tests.

## Account, margins and costs

The app and the token pair are the cTrader login's, shared with every broker: see [the adapter's credentials](../../../crates/ctrader/README.md#credentials). Set `IC_ACCOUNT_ID` to the account's ctidTraderAccountId and `IC_ENVIRONMENT` to `demo` or `live`. Startup checks credentials and opens a separate account-check session before building the node. It refuses the wrong account, a netting or spread-betting account, and a token that cannot be renewed. The execution client checks the account again on connect. IC accounts are hedging by default.

Instrument margin is the reciprocal of the lower of the account's leverage and the symbol's first dynamic-leverage tier (on the demo account, leverage 1:30 under gold's 1:200 and EUR/USD's 1:1000, giving a margin fraction of 1/30). IC's demo symbols have one tier, which covers every size; higher tiers elsewhere can require more margin. Both catalog and live instruments use these margins; IC backtests use leverage 1. A catalog created before those margins were populated requires a fresh fill into a new directory. The demo account's commission is IC's Raw charge, 30 USD per million per side, charged in the account's currency: a backtest's `--commission-rate 0.00003`.

For a EUR account trading USD-quoted instruments, subscribe to `EURUSD.ICMARKETS` quotes as well: Nautilus needs the USD/EUR conversion rate for margin validation and denies orders without it.

## Tests

Offline tests of the clients' wiring are in `src/venues`; those of the environment, the credential and the rest of the adapter are in [the crate](../../../crates/ctrader/README.md#tests). The demo tests `tests/live/ctrader_demo.rs`, `tests/live/ctrader_live_demo.rs`, `tests/live/ctrader_orders_demo.rs` and `tests/live/ctrader_token_demo.rs` run on the broker `NAUTILUS_CTRADER_RUN` names in the shell, here `ic`, and on its demo account only. **The orders test places demo-account orders**; verify the account and check it afterwards. Tests may renew shared tokens; see [the adapter's credentials](../../../crates/ctrader/README.md#credentials) for token safety. The renewal test **rotates the login's real token pair**, every broker's, in the token file; run it only when prepared to manage the shared state file and recover credentials if renewal is interrupted. Run live-node tests in separate processes.

```sh
NAUTILUS_CTRADER_RUN=ic cargo test --test live ctrader_demo:: -- --nocapture
NAUTILUS_CTRADER_RUN=ic cargo test --test live ctrader_live_demo:: -- --nocapture
NAUTILUS_CTRADER_RUN=ic cargo test --test live ctrader_orders_demo:: -- --nocapture
NAUTILUS_CTRADER_RUN=ic cargo test --test live ctrader_token_demo:: -- --nocapture
```
