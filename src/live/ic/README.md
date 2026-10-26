# IC Markets live

`live/ic/` holds IC's clients for the live node, the data client
(`IcDataClient`) and the execution client (`IcExecutionClient`), with
their factories and configs, on the cTrader session of `data/ic/`
(`IcHistory`, imported as `live/ib/` imports `data::IbGateway`). The node,
the start sequence and the routing between venues are in
`live/README.md`; the cTrader session, the credential and token file, the
tick history and its merge into quotes, and the account check in
`data/ic/README.md`, with the audit of the Nautilus primitives both use
("Nautilus primitives").

## Layout

```
ic/
  mod.rs        the factories and configs, ic::clients, the node's IC clients
  data.rs       IcDataClient: Nautilus' DataClient on a cTrader session
  spots.rs      live quotes: spot events merged into quotes, held during a request
  execution.rs  IcExecutionClient: Nautilus' ExecutionClient
  orders.rs     Nautilus orders as cTrader trade requests, and their tags
  dispatch.rs   cTrader's trade events as Nautilus order events and reports
  reports.rs    positions, orders and deals as reconciliation reports
  execution/tests/order_flow.rs  the order flow against the mock peer
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
every session, the start's account check and the node's data and
execution clients alike, at start when due, a day before it expires while the node runs, and
when cTrader refuses or invalidates it, re-authenticating the account on
the open connection. The latest pair lives in the token file
(`~/.local/state/helix-trader/ic-token.json`, mode 0600), seeded once from
`.env`, which is never written; the sessions share it under a lock
(`data/ic/README.md`, "Token file and renewal").

## Refused before the node runs

`ic::clients` refuses a bar type not built from the quotes of an IC
instrument ("IC serves quotes only": a composite bar type, external bars,
LAST bars), before anything connects. The start then opens a cTrader
session of its own before the node is built, checks the account there
(`IcClients::check_account`, on `IcHistory::check_account`) and closes it,
so each of these fails `Live::run` with its reason, before any node or
strategy:

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
  instruments, listens to the account's trade events, publishes the
  account state (balance, and the margin the open positions use, in the
  deposit currency), then waits until the node's cache holds the account
  before reporting connected.
- **Startup reconciliation**: one report per cTrader position, its
  `positionId` the venue position id; the stop loss and take profit as the
  position's reduce-only orders; pending orders; the deals as fills (see
  "Reconciliation").
- **Periodic checks** (open orders every 10 s, positions every 30 s, as the
  node is configured): the reconcile request each time.
- **Orders**: market, limit and stop market orders, brackets, closes by
  position, amendments and cancels (see "Orders"), their outcomes from
  cTrader's trade events (see "Trade events").

## Hedging positions

The account is hedging, and so is the venue to Nautilus (`OmsType::Hedging`,
and `backtest::venue_oms_type` for `ICMARKETS`): every order that names no
position opens one of its own, its cTrader `positionId` the Nautilus
position id, carried by every fill (`OrderFilled.position_id`). Two buys
of gold are two positions, each at its price; a sell does not reduce a
long, it opens a short beside it. A strategy reduces a position by naming
it: `close_position` (a market order on the closing side with the
position's id), or any order with `position_id` set. The EMA crossover
closes each trade's position that way at the end of its hold
(`strategy/README.md`, "Orders and positions").

## Orders

`orders.rs` turns a Nautilus order into a cTrader request; `execution.rs`
checks it, registers it with the dispatch (`dispatch.rs`), reports it
submitted and sends it.

- **New orders** (`ProtoOANewOrderReq`): market, limit (`limitPrice`) and
  stop market (`STOP`, `stopPrice`, the trigger `DEFAULT`/`BID_ASK` as
  `TRADE`, `DOUBLE_BID_ASK` as `DOUBLE_TRADE`) orders; GTC, GTD (its
  expiry in ms), IOC and FOK; a market order keeps cTrader's own time in
  force (IOC). The `clientOrderId` is the Nautilus one. Denied before
  anything is sent (`OrderDenied`): another order type (stop limit,
  trailing, if-touched), `DAY` and other times in force, post-only,
  quote-quantity and iceberg orders, an instrument the client did not
  load, a quantity off the cent grid of a unit.
- **Brackets** (`submit_order_list` of an entry and its contingent legs, as
  Nautilus' `bracket()` builds them): the entry (market, limit or stop
  market) carries the stop loss (the stop market leg's trigger) and the
  take profit (the limit leg's price) as absolute `stopLoss` and
  `takeProfit`; one request. Spotware's proto says absolute protections
  are not supported for market orders; the demo took them on a market
  entry and set them on its position (2026-10-26). Once the entry fills,
  cTrader holds them on the position as one `STOP_LOSS_TAKE_PROFIT` order,
  and the legs are accepted under the venue order ids `<positionId>-SL`
  and `<positionId>-TP`, as reconciliation names them. The legs must be a
  stop market and a limit (either may be left out), for the entry's
  quantity, on the other side; any other list with contingencies is
  denied whole; a list without contingencies goes as its orders.
- **Closing a position**: an order naming a position (`SubmitOrder`'s
  `position_id`, as `close_position` sends) and on its closing side, at
  market, is `ProtoOAClosePositionReq` for its quantity: the position
  closes, or shrinks, by its id, never past flat (a larger quantity is
  denied), and no other position is touched. cTrader makes the request's
  `clientMsgId` the closing order's `clientOrderId`. Any other order naming
  a position carries its `positionId` (a limit closing it, an order adding
  to it). A reduce-only order naming no position is denied: on a hedging
  account it would open one.
- **Modify**: a pending order is amended (`ProtoOAAmendOrderReq`: volume,
  limit or stop price, and the bracket's protections, which an amendment
  otherwise drops: the demo's amended limit lost its own); a protection is
  moved on its position (`ProtoOAAmendPositionSLTPReq`) with the other
  protection as last asked, since cTrader drops a price left out (the
  demo: amending the stop loss alone removed the take profit). A leg of a
  bracket whose entry is pending is moved on the entry.
  A protection's quantity is the position's: a modification of it is
  rejected.
- **Cancel**: a pending order by its `orderId` (`ProtoOACancelOrderReq`); a
  protection by amending its position without it, the other kept (both
  legs of one position cancelled together go in one amendment); a leg of a
  pending entry by amending the entry. A refused removal rejects the
  cancel of each protection it removed.
- **Protections as last asked**: a move or a cancel of a protection is
  held by the dispatch until cTrader's protection shows it done, or it is
  refused, and every amendment of the same position or pending entry
  carries it. So a cancel of the take profit followed, before IC answers,
  by a move of the stop loss sends the stop loss alone, and two moves in a
  row (stop loss, then take profit) send both new prices.
- **Unknown outcomes**: a request is sent under a `clientMsgId` naming the
  operation and the order (`N:`, `A:`, `X:`, `C:`, `P:`, `U:` and the
  client order id). One the session never sent (it failed, or the
  connection closed first) is rejected at once (`OrderRejected`,
  `OrderModifyRejected` or `OrderCancelRejected`, "not sent to IC: ...").
  One sent and never answered (a disconnect) stays in flight: the client
  declares `retain_unresolved_submissions`, so Nautilus never times it out
  into a rejection; the venue's later events, `query_order` (the order by
  its id or, without one, among the orders cTrader updated since it was
  created, with its deals) and the periodic checks resolve it. An order
  with no venue order id that cTrader's order list over its window
  (its creation, a minute of margin each side, to now) does not hold never
  reached IC: `query_order` reports it rejected (an `OrderStatusReport`,
  `REJECTED`, which Nautilus applies as `OrderRejected`), with a bracket
  entry's legs, its reason "IC holds no such order: its request never
  reached IC". Not while its request may still wait in the session's
  queue (sent only once the session is authenticated): until the session
  wrote it, the query concludes nothing. A bracket's leg is never
  concluded on its own (IC holds it on its entry or position), nor is a
  protection (`-SL`, `-TP`) queried. cTrader's deals carry no client order
  id, so the deal list cannot find an order the order list lacks.

## Trade events

cTrader sends every execution event and order error of the account to each
session; the execution client's session passes them on (the data client's
drops them). Its first events for a request come under the request's
`clientMsgId`; events of the server (a stop loss hit) come under the
order's `clientOrderId`. `dispatch.rs` maps them (captured on the demo,
2026-10-26, 1 oz of gold):

| cTrader | Nautilus |
|---|---|
| `ORDER_ACCEPTED` | `OrderAccepted`, venue order id the `orderId` (once) |
| `ORDER_FILLED`, `ORDER_PARTIAL_FILL` | `OrderFilled` per deal (once, by `dealId`): `executionPrice`, `filledVolume`, `positionId`, the commission (a positive cost in the deposit currency), no liquidity side; preceded by `OrderAccepted` if none came |
| `ORDER_REPLACED` | `OrderUpdated`: volume, limit or stop price |
| `ORDER_CANCELLED`, `ORDER_EXPIRED` | `OrderCanceled`, `OrderExpired`, and a pending entry's legs cancelled |
| `ORDER_REJECTED`, `ProtoOAOrderErrorEvent`, an error answer | `OrderRejected` of a new order or a close (and a bracket entry's legs), `OrderModifyRejected` of an amendment, `OrderCancelRejected` of a cancel, the reason cTrader's code and description (`TRADING_BAD_VOLUME: Order volume = 0.50 is smaller than minimum allowed volume = 1.00.`) |
| `STOP_LOSS_TAKE_PROFIT` accepted or replaced | the legs accepted, or updated to its prices and the position's volume; a price gone cancels its leg |
| `STOP_LOSS_TAKE_PROFIT` filled | the leg whose price is nearer the deal's filled, on the position (closing it), the other leg cancelled |
| `STOP_LOSS_TAKE_PROFIT` cancelled | the legs cancelled (the position closed otherwise) |

An event of an order this client did not send (or sent before a restart)
becomes reports instead, which Nautilus reconciles: the order's status
with its fill, or the status of a position's protections under their
`-SL`/`-TP` ids. A modify or cancel of such an order (a protection
reconciled after a restart) tracks it before the request goes, so IC's
refusal rejects the modification or the cancel. A terminal order's repeated events are dropped; a new
deal after it goes as a report.

**Commission.** A fill carries cTrader's charge of its deal: IC's Raw
commission, 30 USD per million per side, in EUR on the demo account. On
the demo (2026-10-26) 1 oz of gold at 4,018.30 and 4,018.20 (0.1205 USD)
was charged 0.11 EUR each side, as the deals state it; the backtest charges
the same rate (`commission_rate(0.00003)`) in USD.

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
  each time; fills from the deal list over the asked window.
- **A single order** (`generate_order_status_report`, `query_order`): open,
  from the reconcile request; else by its venue order id, or, without one,
  among the orders cTrader updated since the cached order was created
  (`ProtoOAOrderListReq`, a minute of margin each side, through the
  historical limiter), from its details (`ProtoOAOrderDetailsReq`, the
  order and its deals). An order cTrader holds nowhere in that window is
  `None`, an absence, to `generate_order_status_report`, and rejected by
  `query_order` (see "Unknown outcomes"); a failed request an error. The
  protections (`-SL`, `-TP`) are not queried one by one: the open-order
  check reads them.

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
  are refused at the start (`ic::clients`, "IC serves quotes only").
- **Reconnect**: the transport reconnects, the session authenticates again
  and subscribes again to every symbol subscribed; after cTrader ends the
  session (`ProtoOAClientDisconnectEvent`) the session asks the transport
  for the reconnect (`data/ic/README.md`, "Session").
- **Stop**: `stop` begins the shutdown (the tasks cancelled, the session
  told to close) and `disconnect`, or the next `connect`, finishes it: the
  tasks drained, the session's handler joined, a token renewal in flight
  answered and saved first.
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
newest `limit` if one is given; a request without `start` is refused. The
node registers no catalog, so the data engine passes the strategy's whole
warmup request to the client (see "Warmup").

While a request of an instrument is answered (four rate-limited tick
requests, about a second), its live quotes are held, then sent after the
response, under one lock, without those at or before the response's last
quote. So the node never gets a live quote before the warmup response,
which would panic it (nautilus_trader#5393, `live/README.md`, "Known
limits"), nor a quote twice. A failed request is logged; the node gets no
response, and the held quotes go out.

## Warmup

The strategy's warmup request (`strategy::warmup`) asks for the quotes of
each IC instrument over `[now - warmup, now]`. The node registers no
catalog (`live/README.md`, "Warmup and live bars on IB", for why), so the
data engine passes it to the data client, which answers from cTrader's
tick history, as `fill-catalog` fetches it (`IcHistory::quote_ticks`):
both sides side by side, about 2 pages a second, 312 pages a side for a
month of gold (`data/ic/README.md`, "Probe"), so a warmup of hours takes
seconds and one of a month minutes.

The hand-off has no gap and no duplicate. The request ends at `now`, the
tick history up to it; the spot subscription, sent after the request,
starts at the subscription, and its first spot carries both sides, so the
quote at the hand-off is the current one. The client holds the
instrument's live quotes from the request until its response is sent,
then sends them without those at or before the response's last quote.
The node never gets a live quote before the warmup response, which keeps
the warmup panic of nautilus_trader#5393 (`live/README.md`, "Known
limits") away from IC, however long the history takes.

## Known limits

- **EUR/USD quotes for the risk engine.** The account is in EUR, gold and
  EUR/USD are quoted in USD: Nautilus' risk engine converts an order's
  margin at the cache's USD/EUR rate, from EUR/USD's quotes, and denies the
  order without them (`VALIDATION_FAILED: Account currency conversion
  failed: No exchange rate from USD to EUR`, demo, 2026-10-26). A strategy
  trading IC instruments quoted in USD subscribes to EURUSD.ICMARKETS's
  quotes too (its bars or a `subscribe_quotes`), as `ic_orders_demo.rs`
  does.
- **Margins.** IC's instruments carry margins of 1 (`data/ic/README.md`,
  "Currency pairs"), while the demo account runs 1:30: Nautilus' risk
  engine checks an order's margin as its notional / the account's leverage
  in Nautilus (1 unless set), 30 times IC's, so it may deny an order IC
  would take when the free balance is short. The backtest applies
  `--leverage` (backtest README, "Leverage"). Not changed: a live client
  cannot set the account's leverage natively (`data/ic/README.md`,
  "Nautilus primitives", "Venue leverage on the margin account").
- **Account state after fills.** The account state is published at
  connect; balances and margins are not read again after a fill (the
  closing deal's `balance` is not used).
- **Partial closes and protections.** After a partial close the legs of the
  position's bracket are updated to the position's remaining volume
  (tested against the mock peer only: the demo's minimum, 1 oz, cannot be
  closed in part).
- **IC with IB.** A strategy trading IC instruments with IB ones is
  refused at the start.
- **Market closed over the whole warmup** (a weekend with a short warmup):
  the warmup response is empty and the strategy starts cold; nothing fails.
- **The first spot** after a subscription is the last price, even with the
  market closed; when the warmup response already holds it, it is dropped.

## IC tests

- Unit tests in `mod.rs`: a missing or unknown `IC_ENVIRONMENT` and a missing
  credential variable refused before anything connects, an instrument of
  another venue and bars not built from quotes refused, the clients loading
  the instruments of the bars, the factories creating unconnected clients on
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
    nothing; the periodic checks of one instrument;
  - `execution/tests/order_flow.rs`, the order flow on the trade events
    captured on the demo (`data/ic/testdata/execution_*`, `order_error_*`),
    the node played (orders cached, events applied): a market order sent
    with its id and volume, accepted and filled at 4022.78 on position
    690004734, charged 0.11 EUR; a limit order accepted, amended and
    cancelled; a rejection and a refused cancel with IC's reasons; a fill
    before the acceptance and events twice making one acceptance and one
    fill; an unanswered order staying submitted until a query finds it
    filled; an unanswered order IC's order list lacks reported rejected by
    a query; a bracket riding on its entry, its legs accepted as
    `690004734-SL` and `-TP`, a moved stop loss sent with the take profit
    kept, a removed take profit keeping the stop loss; a move after an
    unanswered cancel or move carrying it, on a position and on a pending
    entry; a refused cancel of both protections rejecting the cancel of
    each; a refused move and cancel of a protection reconciled before a
    restart rejected; the demo's stop loss
    hit filling the stop loss and cancelling the take profit, a take profit
    hit the reverse; one of two positions closed by its id, a partial
    close, a close of a position IC no longer holds rejected, a close
    larger than its position denied, a protected position closed cancelling
    its legs;
  - `orders.rs`: tags and protection ids read back; market, bracket entry
    and stop requests; order types and flags cTrader lacks denied;
  - `dispatch.rs`: an order whose request may still wait to go out not
    concluded absent, one this session never sent concluded, an absent
    entry taking its legs, a leg alone not concluded;
  - `data.rs`, the `DataClient` against the mock peer: the instruments sent
    at connect on venue `ICMARKETS`; subscribed quotes from the captured
    spot events; an unsubscription sent; a quote request answered from the
    tick history of its window, and its limit; a live quote arriving during
    a request sent after the response; after the peer closes the
    connection, the quotes subscribed again and flowing; a token renewal in
    flight at `stop` saved by the `disconnect` that follows; a request
    without start and an instrument not loaded refused.
- End to end, `tests/ic_live_demo.rs`: `Live` on the demo's XAUUSD, a
  recording strategy getting its warmup, live quotes and the minute bars
  built from them, the node reconciling the account through IC's execution
  client at start; no order. With no catalog, it checks that the warmup
  is one response of the data client starting within a minute of
  `now - warmup`, and that the first live quote comes after its last.
  Passed on 2026-10-26 (16:50-16:53 UTC, 155 s): the account checked
  before the node was built; account state 999,997.27 EUR, no margin; mass
  status empty, reconciled; the warmup answered in 0.87 s (3,472 quotes),
  29 warmup bars, 3 live bars, the first the minute after the last warmup
  bar (`data/ic/README.md`, "Tests"). Skipped unless `NAUTILUS_IC_RUN=1` is set in
  the shell:

  ```sh
  NAUTILUS_IC_RUN=1 cargo test --test ic_live_demo -- --nocapture
  ```
- End to end with orders, `tests/ic_orders_demo.rs` (same gate, demo only):
  `Live` on XAUUSD and EURUSD, a strategy sending, on gold's first quote
  after a EUR/USD one, one bracket of 1 oz (a market buy, its stop loss
  30.00 under the bid, its take profit 30.00 over the ask) and closing the
  position it opens at once (`close_position`); then IC's execution client
  reads the account: no position, no order open. Passed on 2026-10-26
  (15:39 UTC, 13 s): the entry accepted and filled at 4018.30 on position
  690014736, the legs accepted as `690014736-SL` (3988.21) and `-TP`
  (4048.31), the close filled at 4018.20, the legs cancelled with the
  position, each fill charged 0.11 EUR; the account flat after.

  ```sh
  NAUTILUS_IC_RUN=1 cargo test --test ic_orders_demo -- --nocapture
  ```
