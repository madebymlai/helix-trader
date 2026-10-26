# IC Markets live

`live/ic/mod.rs` builds IC's clients for the live node: the factories and
configs of the adapter in `data/ic/` (`IcDataClient`, `IcExecutionClient`).
The node, the start sequence and the routing between venues are in
`live/README.md`; the cTrader session, the spot events, the merge into
quotes, the quote requests, the account and the reconciliation reports in
`data/ic/README.md`.

```rust
IcDataClientFactory                  // name() == "ICMARKETS", config_type() == "IcDataClientConfig"
IcDataClientConfig { connection: IcConnection, instrument_ids: Vec<InstrumentId> }
IcExecutionClientFactory             // name() == "ICMARKETS", config_type() == "IcExecutionClientConfig"
IcExecutionClientConfig { connection: IcConnection, instrument_ids: Vec<InstrumentId> }
```

Each factory creates its client named after the client
(`ClientId("ICMARKETS")`) on venue `ICMARKETS`, so the data and execution
engines route IC instruments and orders to them with no route of ours
(`RoutingConfig::default()`). The execution client is hedging
(`OmsType::Hedging`), a margin account `ICMARKETS-DEMO` or `ICMARKETS-LIVE`:
named after the environment, since the account's id and login are
credential identity, never logged; stable, so a persisted cache finds it
again.

## Environment and credential

`ic::clients` reads `IC_ENVIRONMENT` (`demo` or `live`, required, no
default) and the credential (`IC_CLIENT_ID`, `IC_CLIENT_SECRET`,
`IC_ACCESS_TOKEN`, `IC_REFRESH_TOKEN`, `IC_ACCOUNT_ID`, and the token
file's place from `XDG_STATE_HOME` or `HOME`) into an `IcConnection` before anything
connects, as `OKX_ENVIRONMENT` is read for OKX: a missing or unknown value
fails the start naming the variable, never a value. The config carries the
connection; `Debug` redacts the credential.

Renewal is automatic: the access token (about 30 days) is renewed by
every session, the start's fill and the node's data and execution clients
alike, at start when due, a day before it expires while the node runs, and
when cTrader refuses or invalidates it, re-authenticating the account on
the open connection. The latest pair lives in the token file
(`~/.local/state/helix-trader/ic-token.json`, mode 0600), seeded once from
`.env`, which is never written; the sessions share it under a lock
(`data/ic/README.md`, "Token file and renewal").

## Refused before the node runs

The start's catalog fill opens a cTrader session before the node is built,
and checks the account there (`IcHistory::check_account`), so each of these
fails `Live::run` with its reason, before any node or strategy:

- a token pair cTrader no longer renews (revoked, or lost): `cTrader
  refused to renew the access token (CH_ACCESS_TOKEN_INVALID): generate a
  new token pair in the Playground ...`, or the same remedy after
  `cTrader refused the account authentication (...) even with a renewed
  access token`;
- another account than `IC_ACCOUNT_ID` (the account auth or the trader of
  another account): `... another account than IC_ACCOUNT_ID`;
- a netting (or spread betting) account: `the IC account is NETTED, not
  HEDGED: the adapter trades a hedging account only ...`.

The execution client checks the same at connect, and logs the reason of
a failed connect: the node logs only "connection failed", then times out. On the demo, 2026-10-26: the
account is `HEDGED`, its deposit currency EUR.

## What the execution client does

- **Connect**: authenticates, checks the account, loads the run's
  instruments, publishes the account state (balance, and the margin the
  open positions use, in the deposit currency), then waits until the
  node's cache holds the account before reporting connected.
- **Startup reconciliation**: one report per cTrader position, its
  `positionId` the venue position id; the stop loss and take profit as the
  position's reduce-only orders; pending orders; the deals as fills (see
  `data/ic/README.md`, "Reconciliation").
- **Periodic checks** (open orders every 10 s, positions every 30 s, as the
  node is configured): the reconcile request each time.
- **Orders**: none reaches IC yet. A submission is denied (`OrderDenied`,
  "IC's execution client places no order yet"), a modification or cancel
  rejected; the next slice of the adapter adds the order flow.

## What the data client does

- **Connect**: authenticates (application, then account), loads the run's
  instruments from cTrader's symbols and sends them to the node, then
  reports connected.
- **Quotes**: `subscribe_quotes` / `unsubscribe_quotes` become spot
  (un)subscriptions; spot events become `QuoteTick`s by the history's merge,
  stamped at the spot's time, sized as the catalog's (XAUUSD 10,000 oz,
  EURUSD 100,000 EUR). On the demo the spots were the history's ticks, so
  live and backtest quotes match.
- **Requests**: `request_quotes` answers from the tick history up to the
  request's end or now; live quotes of the instrument wait for the response.
- **Bars**: built by Nautilus from the quotes (internal bar types such as
  `XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL`). `request_bars` and
  `subscribe_bars` of external bars are not served, so composite bar types
  are refused at the start's fill ("IC serves quotes only").
- **Reconnect**: the transport reconnects, the session authenticates again
  and subscribes again to every symbol subscribed.
- **Heartbeat**: a `ProtoHeartbeatEvent` every 10 s.

## Warmup

The start fills the catalog with the IC instruments' quotes over `[now -
warmup, now]` (`IcHistory::fill`, as `fill-catalog`). The strategy's warmup
request is then served by the catalog, and the data engine asks the data
client for the seconds since the fill. Unlike IB's (`live/README.md`, "Known
limits"), those quotes are stamped with the tick's time and kept. The
client sends no live quote of an instrument before the response to its
quote request, which keeps the warmup panic of nautilus_trader#5393 away as
long as the data engine asks the client for a part of the warmup, as it
does when the catalog ends before the node starts.

## Known limits

- **No order flow.** An order on an IC instrument is denied by IC's
  execution client; nothing reaches IC. A strategy trading IC instruments
  with IB ones is still refused at the start (kept from before IC had an
  execution client; IC's client now gets IC's orders by venue).
- **Market closed over the whole warmup** (a weekend with a short warmup):
  the start's fill fails, naming the series and the window, as
  `fill-catalog` does.
- **The first spot** after a subscription is the last price, even with the
  market closed; when the warmup response already holds it, it is dropped.

## IC tests

- Unit tests in `mod.rs`: a missing or unknown `IC_ENVIRONMENT` and a missing
  credential variable refused before anything connects, an instrument of
  another venue refused, the factories creating unconnected clients on
  `ICMARKETS` (the execution client hedging, on `ICMARKETS-DEMO`), IC's
  execution client in the node's clients.
- The data client against the mock cTrader peer: `data/ic/README.md`,
  "Tests".
- End to end, `tests/ic_live_demo.rs`: `Live` on the demo's XAUUSD, a
  recording strategy getting its warmup, live quotes and the minute bars
  built from them, the node reconciling the account through IC's execution
  client at start; no order. Passed again on 2026-10-26 (13:15-13:17 UTC,
  155 s): account state 1,000,000.00 EUR, no margin; mass status empty,
  reconciled; 29 warmup bars, 2 live bars. Skipped unless `NAUTILUS_IC_RUN=1` is set in
  the shell:

  ```sh
  NAUTILUS_IC_RUN=1 cargo test --test ic_live_demo -- --nocapture
  ```
