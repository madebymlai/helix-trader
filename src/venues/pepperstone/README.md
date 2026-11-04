# Pepperstone

Pepperstone serves `PEPPERSTONE` instruments, history and live (see the [routing table](../mod.rs)), through the [cTrader adapter](../../../crates/ctrader/README.md), as [IC Markets](../ic/README.md) does: venue `PEPPERSTONE`, clients named `PEPPERSTONE`, account `PEPPERSTONE-DEMO` or `PEPPERSTONE-LIVE`. Forex and metals definitions and quotes fill through the node, and the live clients trade a hedging account on one cTrader session; see the adapter for the session, the tick history, live quotes and the execution client. Only quote series are served. See [catalog](../../catalog/README.md) for shared fill semantics and [live](../../live/README.md) for startup, routing, reconciliation and recording.

## Account

The app and the token pair are the cTrader login's, shared with every broker: see [the adapter's credentials](../../../crates/ctrader/README.md#credentials). Set `PEPPERSTONE_ACCOUNT_ID` to the account's ctidTraderAccountId and `PEPPERSTONE_ENVIRONMENT` to `demo` or `live`. The token must grant the account: the app's Playground lists what it grants, and the broker of each. Startup checks credentials and opens a separate account-check session before building the node. It refuses the wrong account, a netting or spread-betting account, and a token that cannot be renewed. The execution client checks the account again on connect.

## Margins and costs

The maintained account is the *Pepperstone - Europe* demo, in USD at 1:30. Instrument margins are the broker's: the lower of the account's leverage and the symbol's first dynamic-leverage tier, so 1:20 on gold (its tier, under the account's) and 1:30 on EUR/USD. Both catalog and live instruments use these margins; backtests use leverage 1. Gold's maximum order, and so its synthetic quote size, is 5,000 oz.

Commissions are per lot, not per million as at IC: 3.5 USD per lot per side on gold (a lot is 100 oz) and 3 USD on EUR/USD, charged in the account's currency and rounded to its cents, so a side of 1 oz of gold costs 0.04 USD (checked by `tests/live/ctrader_orders_demo.rs`). A backtest's `--commission-rate` is a share of value, so it only approximates them: 3.5 USD on 100 oz at 4,000 USD is `0.00000875`, moving with the price.

## Tests

The demo tests in `tests/live/ctrader_*.rs` run on the broker `NAUTILUS_CTRADER_RUN` names, here `pepperstone`, and on its demo only. **The orders test places demo-account orders**; verify the account and check it afterwards. Run live-node tests in separate processes. The renewal test, which rotates the login's real token pair, is the adapter's and needs no second run here; see [IC Markets' tests](../ic/README.md#tests).

```sh
NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_demo:: -- --nocapture
NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_live_demo:: -- --nocapture
NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_orders_demo:: -- --nocapture
```
