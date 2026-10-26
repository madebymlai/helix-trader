# IC Markets live

`live/ic/` holds IC's clients for the live node, the data client
(`IcDataClient`) and the execution client (`IcExecutionClient`), with
their factories and configs, on the cTrader session of `data/ic/`
(`IcHistory`, imported as `live/ib/` imports `data::IbGateway`). The node,
the start sequence and the routing between venues are in
`live/README.md`; the cTrader session, the credential and token file, the
tick history and its merge into quotes, and the account check in
`data/ic/README.md`.

## Layout

```
ic/
  mod.rs        the factories and configs, ic::clients, the node's IC clients
  data.rs       IcDataClient: Nautilus' DataClient on a cTrader session
  spots.rs      live quotes: spot events merged into quotes, held during a request
  execution.rs  IcExecutionClient: Nautilus' ExecutionClient, up to orders
  reports.rs    positions, orders and deals as reconciliation reports
```

## Interface

```rust
IcDataClientFactory                  // name() == "ICMARKETS", config_type() == "IcDataClientConfig"
IcDataClientConfig { connection: IcConnection, instrument_ids: Vec<InstrumentId> }
IcExecutionClientFactory             // name() == "ICMARKETS", config_type() == "IcExecutionClientConfig"
IcExecutionClientConfig { connection: IcConnection, instrument_ids: Vec<InstrumentId> }

IcDataClient::new(client_id, &connection, instrument_ids) // a DataClient; the node connects it
IcExecutionClient::new(core, &connection, instrument_ids) // an ExecutionClient; the node connects it
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
  "Reconciliation").
- **Periodic checks** (open orders every 10 s, positions every 30 s, as the
  node is configured): the reconcile request each time.
- **Orders**: none reaches IC yet. A submission is denied (`OrderDenied`,
  "IC's execution client places no order yet"), a modification or cancel
  rejected; the next slice of the adapter adds the order flow.

## Reconciliation

`IcExecutionClient` reports the account to Nautilus' reconciliation
(`reports.rs`, Nautilus' `concepts/execution/reconciliation.md`):

- **Positions** (`ProtoOAReconcileReq`, open positions): one
  `PositionStatusReport` per cTrader position, its `positionId` the venue
  position id, `price` (the VWAP of its executions) the entry average,
  unrounded. Two positions of one symbol are two reports. The request lists
  every open position, so an instrument without one is flat.
- **Stop loss and take profit**: cTrader holds them on the position
  (`stopLoss`, `takeProfit`, amended by position id), so the reconcile
  request is sent with `returnProtectionOrders` false and each becomes an
  open reduce-only GTC order of the position, its venue order id the
  position's with `-SL` or `-TP`: a stop market order at the stop loss
  (trigger `TRADE` as `BID_ASK`, `DOUBLE_TRADE` as `DOUBLE_BID_ASK`, the
  `OPPOSITE` methods, which Nautilus lacks, as `DEFAULT`), a limit order at
  the take profit, for the position's volume.
- **Pending orders** (the same request): an `OrderStatusReport` each, by
  `orderId`, with its `clientOrderId` and the position it opens into or
  closes. Market, limit and stop orders; a stop limit (its limit a
  slippage in points) is an error, so the start fails rather than drop it.
- **Fills** (`ProtoOADealListReq`): a `FillReport` per filled deal, by
  `dealId`, of order `orderId` and position `positionId`, at
  `executionPrice`, the commission cTrader states (negative, a charge) as a
  positive cost in the deposit currency, no liquidity side. Nautilus builds
  the orders of these fills (`generate_missing_orders`), each with its
  position id; closed orders are not listed (`ProtoOAOrderListReq` unused).
- **The window.** The startup mass status reads the deals since the
  earliest of: the node's lookback, the opening of a position open at IC,
  the opening of a position the node's cache holds open. So a fresh start
  gets every deal of the open positions, and a restart on a persisted cache
  the closing deals of a position closed while it was down. A position's
  deals go in only when they add up to it (to its volume, or to nothing for
  a closed one); else its history starts before the window and applying
  them would open a position IC does not hold: they are left out, logged,
  and the report window is marked incomplete. The window's start, in whole
  ms, is the declared `lookback_start`.
- **Deal pages.** A window cTrader answers only in part (`hasMore`) is asked
  again in two halves overlapping by 1 ms, deals kept once by id: neither
  the page size nor the order of the deals is documented. A window not
  ending after its start is refused by cTrader (`INCORRECT_BOUNDARIES`,
  demo, 2026-10-26), so none is asked. Deal requests go through the
  historical limiter (see `data/ic/README.md`, "Rate").
- **Scope.** Records of a symbol the run did not load are dropped, logged at
  debug (the margin of their positions still counts in the account state).
- **Prices** come as doubles: a deal's, an order's, a stop loss and a take
  profit are read at the instrument's precision, a finer one an error.
  Volumes are in cents of a unit, as the instruments'.
- **Periodic checks**: open orders and positions from the reconcile request
  each time; fills from the deal list over the asked window. A single-order
  query finds open orders only, and answers an error, not an absence, for
  any other.

The same positions come back whether the node starts from a persisted cache
or from scratch: cTrader positions carry their own ids, which are the
Nautilus position ids either way.

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

## Live quotes: spot events

`IcDataClient::subscribe_quotes` subscribes to a symbol's spot events
(`ProtoOASubscribeSpotsReq`, `subscribeToSpotTimestamp: true`); the
handler keeps the subscription for the session and sends it again after a
reconnect. `unsubscribe_quotes` ends it (`ProtoOAUnsubscribeSpotsReq`);
spots cTrader still sends after it are dropped. cTrader's answer comes
after the call returns, so a refusal is logged, not returned.

A spot event (`ProtoOASpotEvent`) carries the bid and the ask in
1/100,000, each only when it changed, and the time of the change in Unix
ms. The first one after a subscription carries both sides and the last
session close, even with the market closed.

- **Measured on the demo (2026-10-26, 60 s, XAUUSD and EURUSD):** the spot
  events were the ticks the tick history serves for the same minute, side
  for side and millisecond for millisecond (XAUUSD 123 BID and 122 ASK,
  EURUSD 36 and 47). Spots arrived about 20 ms after their time.
- **The merge** is the history's (`ticks::QuoteMerge`, see `data/ic/README.md`,
  "The merge"): the other side carried, none before both are known, a
  crossed quote dropped, a locked one kept, sizes as in "Quote size" (XAUUSD 10,000 oz,
  EURUSD 100,000 EUR). So a live run gets the quotes a fill stores, and the
  bars Nautilus builds from them are the backtest's.
- **Stamps**: `ts_event` the spot's time, `ts_init` its receipt (the fill
  stamps both with the tick's time).
- **Bars**: none from cTrader (live trendbars are not used); Nautilus
  builds the strategy's internal bars from the quotes.

## Warmup: quote requests

`IcDataClient::request_quotes` answers with the quotes over `[start, end]`
(`end` absent: now) from the tick history, merged as a fill merges them, the
newest `limit` if one is given; a request without `start` is refused. With
the catalog registered, the node asks the client only for what the catalog
misses: `live` fills the catalog up to its start, so that is the few
seconds since (`live/README.md`, "Start sequence").

While a request of an instrument is answered (four rate-limited tick
requests, about a second), its live quotes are held, then sent after the
response, under one lock, without those at or before the response's last
quote. So the node never gets a live quote before the warmup response,
which would panic it (nautilus_trader#5393, `live/README.md`, "Known
limits"), nor a quote twice. A failed request is logged; the node gets no
response, and the held quotes go out.

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
- Against the mock cTrader peer of `data/ic/testing.rs`, on the responses
  in `data/ic/testdata/` (positions, orders and deals synthetic):
  - `spots.rs`: the captured spot events into quotes (both sides at their
    time and receipt, sized as history; one side carried; none before both
    sides; crossed dropped; nothing when unsubscribed, the sides kept);
    quotes held by requests and released after them without those
    answered; a spot of a symbol not loaded refused;
  - `execution.rs`, the `ExecutionClient` against the mock peer: the
    captured account state published at connect (EUR, the balance, no
    margin), connected once the node registers it; the margin of open
    positions locked; a netting account, an expired token cTrader refuses
    to renew and another account refused at connect, nothing published; on two open XAUUSD
    positions (a long with stop loss and take profit, a short of two deals
    reduced once, with a double-trade stop loss), a US500 position out of
    scope and a pending EURUSD limit: one position report per position by
    its id, side, volume and VWAP; the stop losses and the take profit as
    reduce-only orders of their positions; the pending order; the deals as
    fills by position with the commission as a cost; the deals of a
    position opened before the window left out and the window incomplete;
    the deals read from the lookback's start, and from the opening of a
    position the cache holds open; the captured empty account reporting
    nothing; the periodic checks of one instrument; an order denied, no
    frame sent;
  - `data.rs`, the `DataClient` against the mock peer: the instruments sent
    at connect on venue `ICMARKETS`; subscribed quotes from the captured
    spot events; an unsubscription sent; a quote request answered from the
    tick history of its window, and its limit; a live quote arriving during
    a request sent after the response; after the peer closes the
    connection, the quotes subscribed again and flowing; a request without
    start and an instrument not loaded refused.
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
