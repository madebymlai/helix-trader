# IC Markets history and the cTrader session

`data/ic/` fetches IC Markets' forex and metals instruments and their
quotes, merged from cTrader's BID and ASK tick history, for `fill_catalog`,
and holds what the live node's clients in `live/ic/` share with it: the
cTrader session (`IcHistory`), its connection, messages and credential,
the tick merge and the account check. All of it goes through Spotware's
cTrader Open API: binary protobuf frames over Nautilus'
`WebSocketClient`, on `wss://demo.ctraderapi.com:5035` or
`wss://live.ctraderapi.com:5035`. `fill-catalog` routes `….ICMARKETS` ids
here (`../README.md`, "Choosing the source"), and so does `live`
(`../../live/ic/README.md`).

Nautilus has no cTrader adapter; this one follows its adapter guide
(`docs/developer_guide/adapters.md`: outer client and handler task,
`SetClient` first, `AuthTracker`, `SubscriptionState`) and uses Nautilus'
own primitives wherever one fits cTrader's protocol (see "Nautilus
primitives"). The messages are Spotware's `.proto`
files, vendored in `proto/` (the commit is in `build.rs`) and compiled with
prost; `build.rs` needs `protoc`.

Neighbouring docs: the fill algorithm (resume, coverage, writing) is in
`data/README.md`, "fill_catalog.rs"; the data and execution clients (live
quotes, quote requests, reconciliation), their factories, configs and
place in the live node in `live/ic/README.md`.

## Layout

```
ic/
  mod.rs        IcHistory: connect, instruments, the fill, the account,
                open positions, deals and orders, trade requests and events
  account.rs    the account checked (hedging, IC_ACCOUNT_ID's) and its state
  config.rs     IcEnvironment, IcCredential and IcConnection, from IC_*
  messages.rs   Spotware's protobuf messages, their payload types, the envelope
  parse.rs      cTrader symbols -> CurrencyPair, ids
  ticks.rs      tick history: decoding, paging, the merge into quotes, their size
  token.rs      the token pair and the token file that keeps the latest one
  websocket/    the cTrader connection: outer client, handler task, spot
                subscriptions, trade requests and events
  testing.rs    a mock cTrader peer (a local WebSocket server) for the tests
  proto/        Spotware's four .proto files, vendored
  testdata/     responses captured from IC's demo, scrubbed, text and wire forms
```

## Interface

```rust
IcConnection::from_env() -> anyhow::Result<IcConnection>  // IC_ENVIRONMENT and the credential
IcHistory::connect().await -> anyhow::Result<IcHistory>   // reads IC_*, authenticates
IcHistory::connect_as(&connection).await                  // the same, on a connection read before
IcHistory::serves(id: InstrumentId) -> bool               // the ICMARKETS venue
history.instruments(&ids).await -> anyhow::Result<Vec<InstrumentAny>>
history.fill(&mut catalog, series, start, end).await      // instruments, then quotes
history.check_account().await -> anyhow::Result<()>       // hedging, IC_ACCOUNT_ID's
history.disconnect().await
connection.account_id() -> AccountId                      // ICMARKETS-DEMO or ICMARKETS-LIVE
```

`fill` refuses a series other than quotes before any request, then fills
each instrument's quotes a week at a time (see "The fill").

## Connecting

### Credentials

In `.env`:

- `IC_ENVIRONMENT`, `demo` or `live`, required.
- `IC_CLIENT_ID` and `IC_CLIENT_SECRET`: the app registered on
  openapi.ctrader.com (Applications, Credentials).
- `IC_ACCESS_TOKEN`, `IC_REFRESH_TOKEN` and `IC_ACCOUNT_ID`: from the
  app's Playground, the first token pair and the `ctidTraderAccountId` of
  the IC account. They only seed the token file (below): from then on the
  adapter renews the pair by itself.

None is ever printed: they are `SecretString`s, `Debug` redacts them,
errors name variables and cTrader's error codes only. The plaintext copies
an auth or renewal frame is built from are wiped (`zeroize`) once it is
encoded; the frame itself goes to the transport.

### Token file and renewal

The access token lasts 2,628,000 s (about 30 days); the refresh token
renews it with `ProtoOARefreshTokenReq`, whose answer holds a new access
token, its lifetime (`expiresIn`, in seconds) and a new refresh token, the
old pair invalidated at once (Spotware's docs: the refresh token has no
expiry but is valid until used). So the latest pair lives in a token file,
outside the repository, the user's only:

- where: `$XDG_STATE_HOME/helix-trader/ic-token.json` (XDG base directory
  convention for user state), by default
  `~/.local/state/helix-trader/ic-token.json`; a relative
  `XDG_STATE_HOME` is ignored, as the specification says;
- what: JSON with both tokens, the access token's expiry and a SHA-256
  fingerprint of the `.env` pair that seeded it; mode 0600, its directory
  0700;
- seeded from `.env` once: at the first connect, or when `.env` holds
  another pair than the one that seeded it (a new one from the
  Playground). `.env` is never written; after the first renewal its pair
  is spent;
- written atomically (a temporary file renamed over it), so a reader sees
  a whole pair; a renewal holds an advisory lock (`flock` on
  `ic-token.json.lock`) from before its request until the new pair is
  written, so sessions of several processes (a fill and a live node) never
  spend the same refresh token: the one that finds the file renewed takes
  that pair instead.

Every session (fill, data client, execution client) renews by itself:

- at start, when the expiry is unknown (a pair just seeded) or less than
  a day away (`RENEW_BEFORE`);
- on schedule while it runs, a day before the expiry;
- when cTrader refuses the token at the account auth
  (`CH_ACCESS_TOKEN_INVALID`, `OA_AUTH_TOKEN_EXPIRED`), once;
- when cTrader ends the account session
  (`ProtoOAAccountsTokenInvalidatedEvent`, `ProtoOAAccountDisconnectEvent`:
  the token renewed by another session, or expired): the account
  authenticates again with the file's pair, renewed first if refused.

The renewal's answer is the only copy of the new pair: it is written to
the file before anything else, whatever the session went through while
it was out (an event ending the account session, an expired wait), known
by its `clientMsgId` or its payload type; a disconnect waits for it up to
30 s. Then the account authenticates again on the open connection:
requests in flight keep awaiting their answers, new ones wait. The spot
subscriptions stay: cTrader keeps them on the connection across a new
account auth, and answers `ALREADY_SUBSCRIBED` to one sent again (demo,
2026-10-26); only those subscribed or unsubscribed meanwhile are sent,
once the account is authenticated again. A new connection subscribes them
all again.

A refused renewal fails the session naming the code and the remedy:
generate a new pair in the Playground, set `IC_ACCESS_TOKEN` and
`IC_REFRESH_TOKEN` in `.env` (which seeds the file again). The pair is
lost, and the same remedy applies, if the connection drops or the process
dies between the renewal request and its answer.

### Session

Application auth, then account auth (the token renewed first if due, see
above). A refusal names the code and what to check, as does an account
other than `IC_ACCOUNT_ID`. A `ProtoHeartbeatEvent` goes out every
10 s (the transport's Ping is off). Requests carry a `clientMsgId` and get
the response that echoes it.

After a reconnect the session authenticates again, then subscribes again
to the spots of every symbol subscribed, before serving the requests that
waited; requests in flight fail.

The spot subscriptions are tracked in Nautilus' `SubscriptionState`
(`nautilus_network::websocket`), topics `spot:<symbolId>`: a subscription
or unsubscription is marked when asked, cTrader's answer confirms it, a
refused subscription is marked failed (logged, and kept for the next
connection, which sends it again), and `reset_after_reconnect` gives what
a new connection subscribes. Changes asked while the account
authenticates again on the same connection wait, the last of each symbol,
and are sent once it is authenticated if they change the state. cTrader ending the client's session
(`ProtoOAClientDisconnectEvent`) has the handler ask the transport for a
new connection (`WebSocketClient::request_reconnect`, as Lighter's handler
does on its venue's shutdown frame): requests wait for the new session,
which authenticates again, and those in flight fail. Should the transport
take no reconnect (it is closing), the session fails. An account session
ended is authenticated again (see above).

On the demo (2026-10-26) cTrader sent no heartbeat of its own in 25 s
while ours ran, and none is needed: the transport's reconnect handles a
dropped connection.

Trade requests (a new order, an amendment, a cancel, a close, a position's
protections amended) go out under the `clientMsgId` their caller chose
(`IcHistory::trade`, the account filled in); the caller learns only whether
the frame was written: one waiting when the session fails or the
connection closes was never sent, and says so. Their answers, and every
execution event and order error of the account, come as trade events
(`IcHistory::trade_events`, from the moment it is called), with the
`clientMsgId` they came under: cTrader answers a request with execution
events under its id, the server's own (a stop loss hit) under the order's
`clientOrderId` (demo, 2026-10-26). A session nobody listens to (the data
client's) drops them. A trade request in flight at a reconnect gets no
answer: its outcome is left to the execution client's reconciliation
(`../../live/ic/README.md`, "Orders").

### Nautilus primitives

`docs/ARCHITECTURE.md` asks for plain Nautilus API: each primitive the
adapter needs is Nautilus' own unless Nautilus has none for cTrader's
protocol. Audited 2026-10-26 against the sources then pinned (`a51ae01`)
and checked again at the pin `04f766b`, which changed none of the files
below but a test of `network/src/websocket/client.rs` and the PnL of
`model/src/accounts/margin.rs` (paths relative to
`~/.cargo/git/checkouts/nautilus_trader-*/04f766b/crates/`), the adapter guide (`docs/developer_guide/adapters.md`,
"Shared baseline", "WebSocket client patterns", "Task management") and the
networking guide (`docs/concepts/networking.md`), and how the Binance,
OKX and AX adapters use them:

| Primitive | Ours today | Nautilus native equivalent (path) | Verdict | Reason |
| --- | --- | --- | --- | --- |
| Spot subscription tracking | Nautilus' (before: two symbol sets in the handler, wanted and held on the connection) | `SubscriptionState` (`network/src/websocket/subscription.rs`; guide, "Subscription management") | migrated | Marked when asked, confirmed or failed by cTrader's answer, `reset_after_reconnect` for a new connection. The changes asked during a new account auth on the same connection wait in the handler, last of each symbol: cTrader keeps the connection's subscriptions then (`ALREADY_SUBSCRIBED`), and the state has no "not yet sent" mark. |
| Authentication state | Nautilus' `AuthTracker`, begun per connection, succeeded or failed on cTrader's answers | `AuthTracker` and `WebSocketClient::set_auth_tracker` (`network/src/websocket/auth.rs`, `network/src/websocket/client.rs`; networking guide, "Liveness and recovery") | migrated | Now registered with the transport, which invalidates it when the connection drops and fails it at disconnect (the handler's own failure at close removed). Registered with `reconnect_buffer_waits_for_auth` false: the auth frames are binary, and the transport binds only text sends to a connection (`send_text_on_connection`), so they would wait behind the buffer they unblock. |
| Reconnection | Nautilus' handler-mode `WebSocketClient` (backoff, `RECONNECTED`) | `WebSocketClient` (`network/src/websocket/client.rs`; networking guide, "Connection modes") | already native | The handler re-authenticates and replays on `RECONNECTED`, as the guide's "Reconnection and shutdown" asks. |
| Rate limiting | Nautilus' keyed quota on the transport, `historical`, one per 250 ms | `Quota` and `keyed_quotas` (`network/src/ratelimiter/quota.rs`, `network/src/websocket/client.rs`) | already native | |
| Application heartbeat | ours: a `ProtoHeartbeatEvent` every 10 s from the handler | `WebSocketConfig::heartbeat_interval_secs` / `heartbeat_payload` (`network/src/websocket/config.rs`) | keep: no native equivalent | The transport's heartbeat is a Ping or a text frame; cTrader counts only its binary protobuf `ProtoHeartbeatEvent`. The guide: "Keep application heartbeat messages in the venue handler". |
| Request correlation | ours: `clientMsgId` per request, a map to its caller, requests waiting while the session authenticates | none (guide, "Subscription management": "The adapter provides the wire correlation"; "Tracked and external execution updates") | keep: no native equivalent | Nautilus leaves wire correlation to the adapter. |
| Request timeout | ours: `tokio::time::timeout` of 60 s around a response | `RetryManager` (`network/src/retry.rs`) times out retried operations only | keep: no native equivalent | No request is retried, so there is no retry operation to bound; OKX's WebSocket client bounds its waits with a plain timeout too (`adapters/okx/src/websocket/client.rs`). |
| Retries | none | `RetryManager` (`network/src/retry.rs`) | keep: nothing to migrate | No retry loop: the deal window split in halves and the renewal after a refused token are protocol steps, not retries of a transient failure. |
| Token renewal | ours: refresh token request, token file under `flock`, schedule a day before expiry | none (guide, "Authentication": "Refreshable tokens ... need adapter-specific state") | keep: no native equivalent | |
| Secrets | Nautilus' `SecretString`; plaintext copies wiped with `zeroize` | `SecretString` (`core/src/string/secret.rs`; guide, "Credentials and secret handling") | already native | `zeroize` is the guide's "Bound plaintext lifetime". |
| Handler task | Nautilus' `TaskSlot`, spawned gated, joined by `finish_task` at `disconnect` (40 s, then aborted, 2 s) | `TaskSlot` and `finish_task` (`live/src/task.rs`; guide, "Task management", "explicitly singular"), as Binance's spot streams client (`adapters/binance/src/spot/websocket/streams/client.rs`) | migrated | A `TaskSlot` aborts its task when dropped, so the clients' `stop` no longer drops the session: it begins the shutdown (`begin_shutdown`: task groups closed, the handler told to close, as Binance's `stop` does), and `disconnect`, or the next `connect`, drains the tasks and joins the handler, which saves a token renewal's answer first (the only copy of the new pair). |
| Reconnect on cTrader's end of the session | Nautilus': after `ProtoOAClientDisconnectEvent` the handler calls `WebSocketClient::request_reconnect` | `WebSocketClient::request_reconnect` (`network/src/websocket/client.rs`; guide, "Reconnect ownership"), as Lighter's handler on its venue's shutdown frame (`adapters/lighter/src/websocket/handler.rs`) | migrated | Accepted: requests wait for the new session, authenticated again on `RECONNECTED`; refused (the transport closing), the session fails as before. The transport opens the new connection before it drops the old one (`reconnect_with_outcome`), so cTrader need not close the socket. |
| Time, tasks and wall clock (DST seams) | Nautilus': timers, intervals and timeouts from `nautilus_common::live::dst::time`, `biased;` on every `select!`, the wall clock through `nautilus_core::time` (`get_atomic_clock_realtime`), no `spawn_blocking`: the token file is loaded with a non-blocking lock, retried every 100 ms | `nautilus_common::live::dst` (`common/src/live/dst.rs`), `duration_since_unix_epoch` (`core/src/time.rs`; `docs/concepts/dst.md`, "Required conditions"), as OKX's handler and clients (`adapters/okx/src/websocket/handler.rs`, `adapters/okx/src/data.rs`) | migrated | Across the session, the token file and the live clients. Tests keep tokio's own. The crate exposes no `simulation` feature and no madsim build: the seams are in place, a madsim run of the adapter is not claimed. |
| Data client tasks | Nautilus' `TaskGroup`s, session and command | `TaskGroup` (`live/src/task.rs`) | already native | |
| Quote emission filter | ours: a `subscribed` flag per instrument in `live/ic/spots.rs`, the merge kept for all | `DataClientAdapter::subscriptions_quotes` (`data/src/client.rs`) | keep: no native equivalent | That set is the data engine's, held by the adapter wrapping our `DataClient`, out of its reach. The guide asks for this split: "Update the full private cache even when emissions are filtered to active subscriptions". |
| Deal deduplication | ours: deals kept once by id across the halves of a split window | `FifoCache` (`common/src/cache/fifo.rs`) | keep: no native equivalent | `FifoCache` bounds replay deduplication of a stream; this is one request's complete set, which a bounded cache could truncate. |
| Trade event deduplication | Nautilus' `FifoCache` of the last 10,000 deal ids (`live/ic/dispatch.rs`) | `FifoCache` (`common/src/cache/fifo.rs`; guide, "Event ordering and deduplication") | already native | |
| Order identity of trade events | Nautilus' `OrderIdentity`, in the dispatch's tracked orders with their shape and bracket role | `OrderIdentity`, `OrderContext` (`live/src/execution/context.rs`; guide, "Tracked and external execution updates") | already native | The shape kept is the bracket's (legs, the position they protect), which `OrderContext` does not hold. |
| Venue leverage on the margin account | Nautilus': IC's leverage as the instruments' `margin_init` and `margin_maint` (see "Margins"); the account keeps leverage 1 | the instrument's `margin_init`/`margin_maint` (`docs/concepts/accounting.md`, "set `instrument.margin_init` and `margin_maint` so the percentage recovers the desired dollar amount"), as AX parses its venue's margin rates into them (`adapters/architect_ax/src/http/parse.rs`); `MarginAccount::set_default_leverage` / `set_leverage` (`model/src/accounts/margin.rs`) are applied to the cached account only by simulated venues (the sandbox, `adapters/sandbox/src/execution.rs`; the backtest exchange, `backtest/src/exchange.rs`) | migrated | A live execution client gets a `CacheView` (`common/src/cache/view.rs`: "so cache writes stay owned by the data and execution engines"), and `AccountState` carries no leverage (`model/src/events/account/state.rs`), so no live adapter at this commit sets the account's leverage; OKX, Bybit, Binance, Kraken, dYdX, IB and AX fill `margin_init` instead. The catalog's instruments carry the same margins, so a backtest runs IC at leverage 1. |
| Unresolved submissions | `ExecutionClient::retain_unresolved_submissions` true | the same (`common/src/clients/execution.rs`) | already native | An order sent and unanswered stays in flight until venue evidence resolves it. |

## Instruments

### Ids

`XAUUSD.ICMARKETS`: the symbol name cTrader lists, on venue `ICMARKETS`.
cTrader's `symbolId` differs per server, so it is looked up by name on
each request.

### Currency pairs

A symbol whose base and quote assets are currencies Nautilus knows (forex,
XAU) becomes a `CurrencyPair`:

- price precision `digits`;
- sizes in units of the base, cTrader's volumes being in cents of a unit:
  `stepVolume` / 100 the increment, `minVolume`, `maxVolume` and
  `lotSize` / 100 (XAUUSD: 100 oz a lot);
- margins IC's own (see "Margins");
- stamped at receipt (a symbol has no time).

Indices and shares are refused, naming the symbol.

### Margins

An instrument's `margin_init` and `margin_maint` are 1 / the leverage IC
applies to the symbol on the account, as Nautilus' AX adapter fills both
from its venue's margin rates (`adapters/architect_ax/src/http/parse.rs`,
the instruments' `margin_init`/`margin_maint`). The leverage is the lower
of two, both read from cTrader at each load of the instruments (a fill, a
data or execution client's connect):

- the account's, `ProtoOATrader.leverageInCents` (in cents: 3000 is 1:30);
- the symbol's dynamic leverage, `ProtoOASymbol.leverageId`, asked with
  `ProtoOAGetDynamicLeverageByIDReq`: tiers of a leverage (in cents too,
  measured: 20,000 for 1:200) up to a USD volume (in cents), the last tier
  applying above its volume as well. A symbol without one takes the
  account's.

cTrader's help ("Dynamic leverage", help.ctrader.com/trading-with-ctrader/
dynamic-leverage): "the account leverage acts as an upper limit. If the
symbol leverage is lower than the account leverage, the symbol leverage is
applied. If the symbol leverage is higher, the account leverage takes
precedence." On the demo (2026-10-26) the account is 1:30, XAUUSD's
leverage one tier of 1:200, EURUSD's one tier of 1:1000: both instruments
get 1 / 30. IC's own margin estimate (`ProtoOAExpectedMarginReq`, captured
then) agrees: 33.33 EUR for 1,000 EUR of EURUSD (1,000 / 30), 119.76 EUR
for 1 oz of gold (about 3,590 EUR / 30; at 1:200 it would be about 18).

- **Tiers.** The rate is the first tier's, which covers the smallest
  positions, under the account's. A position whose USD value passes the
  first tier's volume gets the next tiers' lower leverage on the excess at
  IC (cTrader's help, "Margin calculation"), so IC's margin is then above
  Nautilus' single rate. IC's demo symbols have one tier, which covers
  every size.
- **Maintenance.** cTrader states no maintenance or stop-out rate of a
  symbol: the stop-out level is the broker's, on the account's margin
  level (`ProtoOATrader` carries the stop-out strategy only), so
  `margin_maint` equals `margin_init`.
- **Where it applies.** The live clients' instruments and those
  `fill-catalog` stores come from the same load (`IcHistory::symbols`,
  `parse::margin`), so the live risk check and the backtest lock IC's
  margin. A backtest of IC instruments runs at `--leverage 1`, the default
  (backtest README, "Leverage"). A catalog filled before IC's margins
  holds margins of 1: fill it again into a new directory.
- **Nautilus' use.** Its default margin model, `LeveragedMarginModel`
  (`model/src/accounts/margin_model.rs`), locks notional / the account's
  leverage in Nautilus (1) × `margin_init` for an order, × `margin_maint`
  for a position; the risk engine denies an order whose initial margin,
  converted to the account's currency, exceeds the free balance
  (`risk/src/engine/mod.rs`, `check_margin_balance`,
  `INITIAL_MARGIN_EXCEEDS_FREE_BALANCE`).

## Quotes: tick history

### Requests

`ProtoOAGetTickDataReq` asks for the ticks of one symbol and one side, BID
or ASK, over `[fromTimestamp, toTimestamp)` in Unix ms: `from` inclusive,
`to` exclusive (demo, 2026-10-26: `to` = a tick's time leaves it out,
`from` = it takes it in, `from` = `to` answers nothing). A request spans at
most a week (Spotware's docs), so a window is asked a week at a time from
its end.

`ProtoOAGetTickDataRes.tickData` is newest first; the first tick is
absolute (ms, price × 100,000), each following one the difference in time
and price from the one before. A full page is 10,000 ticks with `hasMore`.
The next page ends 1 ms after the oldest tick of the page before, whose
ticks of that millisecond are dropped: the next page holds all of them, so
ticks sharing a millisecond across pages are neither lost nor doubled. (On
the demo no side had two ticks in one millisecond, in 10,000-tick pages.)
A full page all in one millisecond would stop the fill with an error.

### Rate

cTrader allows 5 historical requests a second per connection. Tick
requests go through the transport's rate limiter at one every 250 ms,
without burst: one every 200 ms was refused on the demo
(`BLOCKED_PAYLOAD_TYPE`, "You are being rate limited"). BID and ASK are
fetched side by side, so the fill runs at the limiter's pace: a week of
XAUUSD (727,000 ticks a side, about 150 requests) in about 38 s, of EURUSD
(210,000 and 222,000) in about 12 s. Deal and order list requests share
the limiter (see `../../live/ic/README.md`, "Reconciliation"); other
requests, trade requests included, are not limited.

### Units

- Time: the tick's millisecond, as `ts_event = ts_init` in nanoseconds.
- Price: the integer / 100,000 at the instrument's precision (`digits`);
  a price finer than that is an error, not rounded.
- Size: none in cTrader's ticks; see "Quote size".

### The merge

One quote per millisecond with a tick of either side; ticks of one side in
the same millisecond are combined, the last winning. The side without a
tick then is carried from its last tick, including the last before the
window (one request per side asks the week before it), so a resumed fill
stores the same quotes as one fill. No quote before both sides are known.
A crossed quote (ask below bid) is dropped and counted, so an order fills on
the quote before it, as `ib/` drops crossed pairs; a locked one (ask = bid)
is kept. None was crossed in the measured week; 43% of EURUSD's quotes are
locked (Raw spread 0), none of XAUUSD's.

### Quote size

Both sides get 100,000 units, or the instrument's maximum order if smaller:
XAUUSD 10,000 oz, EURUSD 100,000 EUR (one lot). Two Nautilus limits bound
it:

- The matching engine fills a market order up to the L1 size and the rest
  one tick worse (`matching_engine/mod.rs`, `apply_fills`, "Exhausted L1
  volume"; `liquidity_consumption` and `queue_position` are off, so nothing
  is used up between orders). The risk engine refuses an order above the
  instrument's maximum, so at that size no fill is limited:
  `backtest_ic_quotes.rs` fills 10,000 oz whole; run once with quotes of
  5,000, the other 5,000 filled 0.01 worse.
- The data engine adds each quote's size to the volume of the bar it
  builds, a `Quantity` that panics past 18,446,744,073
  (`aggregation.rs`, `BarBuilder::update`). A bar may hold up to
  18,446,744,073 / size quotes: XAUUSD 1.8 M, EURUSD 184,000.

Measured in the busiest hour and day of 2026-10-12 to 10-19: XAUUSD 9,405
and 155,420 quotes (736,052 in the week), EURUSD 6,088 and 72,023 (292,893).
At 100,000, hour and day bars fit both with a margin of 2.5 or more
(EURUSD's day), XAUUSD's week too. **Caveat**: an EURUSD order above one lot
fills the excess one tick (0.1 pip) worse, and an EURUSD bar of a week
(about 290,000 quotes) would panic. No size avoids both on EURUSD: a size
that never limits its orders (its maximum, 100,000,000) overflows a bar of
185 quotes.

### The fill

`fill` loads the instruments, then for each one calls `fill_catalog` once
per week from where its quotes end (or `start`), the last period taking the
remainder, so a fill holds at most two weeks of ticks in memory and a
stopped run keeps the weeks done. The `fill_catalog` rules hold
(`../README.md`, "fill_catalog.rs"):

- **Resume**: from 1 ns after the stored end, whatever `start`.
- **Backwards**: a `start` before the stored quotes is refused before any
  tick request.
- **Missing range**: a first window without ticks (before IC's history,
  or a weekend alone) is an error naming the series and the window.
- **No ticks**: a period without ticks after the first (a weekend, a
  holiday, a quiet minute) is no error; a resumed empty period records no
  coverage, and the next period asks for it again. A week always holds
  ticks, so only a short window can end in a closed market.

Coverage is the window asked for: a first window partly before IC's history
stores what IC has, silently, as for IB and OKX.

### Probe (2026-10-26)

One connection, requests one after another, before the fill existed:

- **Speed.** About 2 pages a second, bound by latency. One month of
  XAUUSD: 3.09 M BID and 3.09 M ASK ticks, 312 pages a side; of EURUSD:
  0.88 M and 0.91 M.
- **Depth.** Ticks from 2015 on both (none in January 2014); M1 bars from
  2013 on XAUUSD, 2012 on EURUSD.
- **Decision: ticks**, BID and ASK merged into quotes, not the M1 BID
  fallback: about 11 years of ticks, the whole of XAUUSD's in about 6-7 h
  one request after another; at the limiter's pace, both sides side by
  side, about 6 h at the density of the week above, less for the sparser
  early years (an estimate, not run).

## Account

`ProtoOATraderReq` answers with the account (`ProtoOATrader`); with the
asset list, it is checked (`account.rs`), at the start of `live`, before
the node is built, and at the execution client's connect:

- **Hedging only.** `accountType` must be `HEDGED`, IC's default: each
  cTrader position is one Nautilus position, its `positionId` the venue
  position id, and the venue's OMS is `Hedging`. `NETTED` and
  `SPREAD_BETTING` are refused, naming the type.
- **The configured account.** The trader's `ctidTraderAccountId` must be
  `IC_ACCOUNT_ID` (the account auth's answer is checked too, see
  "Session").
- **Currency.** The deposit asset (`depositAssetId`, in the asset list) is
  the account's base currency; it must be a currency Nautilus knows.

The account state, published at connect (`AccountState`, a margin
account, reported): one balance, its total `balance` (deposits and
realized PnL, without the open positions' PnL), locked the margin the open
positions use (the sum of their `usedMargin`, every position of the
account, of the run's instruments or not), free the rest; and that margin
as one account-wide `MarginBalance`, initial and maintenance alike (cTrader
states no maintenance margin of its own). Money is an integer scaled by
`moneyDigits`. Equity (with unrealized PnL) is not reported: cTrader serves
it only through `ProtoOAGetPositionUnrealizedPnLReq`, not used.

The Nautilus account is `ICMARKETS-DEMO` or `ICMARKETS-LIVE`, named after
the environment: the account's id and login are credential identity, never
logged. The demo account (2026-10-26): `HEDGED`, EUR, balance 1,000,000.00,
leverage 1:30, nothing open, no deal.

## Verified runs

On the demo (2026-10-26, `fill-catalog` on XAUUSD and EURUSD into a
scratch catalog): both instruments stored as above; their quotes over
2026-10-12 to 10-19 as in "Rate" and "Quote size", one file a series. IC's symbol names are `XAUUSD`,
`EURUSD`, `US500`; volumes are in cents of a unit (`lotSize` 10,000 is
XAUUSD's 100 oz, 10,000,000 EURUSD's 100,000 EUR, IC's published lots).
The details carry the commission: `USD_PER_MILLION_USD`,
`preciseTradingCommissionRate` 3,000,000,000 (30 USD per million, on
both). cTrader sent no heartbeat of its own in 25 s while ours ran.

`testdata/` holds those responses, scrubbed: the account id replaced by a
fake one, the lists trimmed to three symbols; and the XAUUSD BID and ASK
ticks of five seconds (2026-10-21 14:00:00-14:00:05 UTC); the answers to a
spot subscription and unsubscription and four spot events (2026-10-26:
the first of XAUUSD and of EURUSD, an EURUSD bid alone and an ask alone);
the answers to a trader request (the login scrubbed too), a reconcile
request and a deal list request on the demo account, which holds nothing;
and the trade events of 1 oz orders of XAUUSD on the demo (2026-10-26,
`execution_*`, `order_error_*`): a market buy with protections accepted
and filled, its position's protection order accepted, amended and
cancelled when the position was closed by its id; a limit buy accepted,
amended and cancelled; a stop loss hit; a volume below the minimum, a
cancel of an order already cancelled and a close of a position already
closed refused; the dynamic leverages of XAUUSD and EURUSD and IC's margin
estimates of 1 oz of gold and 1,000 EUR of EURUSD (2026-10-26,
`dynamic_leverage_*`, `expected_margin_*`, re-encoded from their decoded
form, every field being in the vendored `.proto`). Their `.txtpb` say what
each is. The positions, orders and deals of the reconciliation tests are
synthetic.

## Tests

- `ic/`, on the responses in `testdata/` (synthetic only where no capture
  can be had: another account, a token invalidated, malformed frames) and
  the mock cTrader peer of `testing.rs`:
  - `config.rs`: the environment required, the credential read whole and
    never in `Debug` or errors, the `.env` pair seeding the token file;
  - `token.rs`: the file seeded from `.env` (expiry unknown), mode 0600 in
    a 0700 directory, a renewed pair found by the next load, another `.env`
    pair seeding it again; no pair loaded while another session renews;
    the lock one session at a time; two writers and
    a reader at once never leaving a partial pair, the last writer's kept;
    a file that is no token file named, never its content; the expiry from
    `expiresIn` and the renewal a day before; the path under
    `XDG_STATE_HOME` or `~/.local/state`;
  - `messages.rs`: envelopes, error responses and malformed payloads;
  - `parse.rs`: XAUUSD and EURUSD to `CurrencyPair`s, at the margin
    given; an index, a missing step, a minimum finer than the step, digits
    out of range and an unlisted asset refused; ids round trip; the margin
    from the captured leverages: gold on the demo 1 / 30 (the account's
    1:30 under its 1:200), 0.05 with a tier of 1:20 under the account's
    1:30, the first tier's whatever the tiers above, the account's for a
    symbol without a leverage, a leverage missing refused naming the
    symbol, and 1,000 EUR of EURUSD locking IC's captured estimate,
    33.33 EUR;
  - `ticks.rs`: the captured ticks decoded and merged into gold quotes;
    ticks out of order refused; pages walked back keeping ticks of a
    millisecond split across pages, a window `[from, to)`, a week a
    request, an empty window, a full page of one millisecond refused, the
    last tick before; the merge (other side carried, none before both,
    seeded from before, one quote a millisecond, crossed dropped and locked
    kept, the size and its cap, a price off precision refused);
  - `websocket/`: the session's exact auth frames, the binary heartbeat,
    two requests answered in reverse order each getting its own, a venue
    error, a refused app (naming the code, never a credential), another
    account refused, malformed and unknown frames skipped,
    re-authentication after a reconnect; once cTrader ends the client's
    session, a reconnect requested, the request in flight failing and the
    next served on the new, re-authenticated connection; tick requests at
    most 5 a second, others unlimited; a trade request sent under its id,
    its answer and an event of the account coming as trade events, and one
    asked after cTrader ended the session sent on the next one;
  - `websocket/` (renewal), against a peer keeping one token pair as
    cTrader does (a renewal replaces it and ends the sessions of the old
    one): a pair of unknown expiry, or expiring within a day, renewed and
    saved before the account auth, one expiring later not; a running
    session renewing on schedule, its spots and a request in flight kept;
    an invalidated token renewed and the account authenticated again; a
    token refused at start renewed; a refused renewal naming the
    Playground, the pair kept; a token refused even renewed naming the
    Playground, never a token; two sessions on one file, starting or due
    together, renewing once; a session whose token another renewed taking
    the new pair from the file; the renewal's answer saved after cTrader
    drops the account session, without its `clientMsgId`, and at a
    disconnect; an account session dropped authenticated again; against a
    peer keeping the connection's subscriptions across a new account auth
    and refusing one held (`ALREADY_SUBSCRIBED`, as the demo), none sent
    again after a renewal, those changed during it sent once authenticated
    again;
  - `websocket/` (subscription state): a refused spot subscription marked
    failed (pending, not confirmed, the accepted one confirmed) and sent
    again on the next connection;
  - `mod.rs`: the instruments from assets, symbol list, details, the
    account's leverage and each symbol's dynamic leverage, at IC's margin
    (1 / 30 on the captured demo); Nautilus' risk engine passing a market
    buy of 1 oz of gold (about 3,590 EUR) on a margin account of 1,000 EUR
    at those margins, and denying it at margins of 1
    (`INITIAL_MARGIN_EXCEEDS_FREE_BALANCE`); an
    unlisted symbol and another venue refused; a series other than quotes
    refused before any request; a fill, against a peer serving ticks as
    cTrader does: the quotes merged and stored with the instrument over the
    window, each side's requests (the week before, the window to 1 ms past
    its end), a resume from 1 ms after the stored end with no quote twice,
    a `start` before the stored quotes refused before any tick request, a
    first window without ticks naming the series and the window, a week
    without ticks no error and a long fill stored a week at a time.
  - `websocket/` (spots): a subscription asks for timestamps and its
    events come through; a symbol subscribed twice is asked for once; after
    a reconnect the subscriptions are sent again, an unsubscribed one not;
  - `mod.rs` (account): the captured hedged account passes the check; a
    netting account and another account's trader refused, naming why;
  - `messages.rs`: tick, deal and order list requests are historical,
    others not.
- `tests/live/ic_demo.rs`, on IC's cTrader demo (gated by `NAUTILUS_IC_RUN=1`,
  demo only; it places no order): the execution client, from its factory,
  connects on the hedged account, publishes its state (1,000,000.00 EUR, no
  margin) and reconciles it over a week's lookback (nothing open, no deal,
  complete); XAUUSD and EURUSD loaded as currency pairs at IC's margins; a day of XAUUSD (2026-10-21, 149,043 quotes) stored the same in
  one fill and resumed at a time between two milliseconds, one quote a
  millisecond, none crossed, sized 10,000, covered over the day. Passed on
  2026-10-26 (about 20 s), the execution client's test too; passed again
  that day (about 18:10 UTC, 21 s) with the instruments at IC's margins, 1 / 30
  both.
- `mod.rs`, `the_demo_session_renews_its_token_and_keeps_receiving_quotes`,
  on the demo (same gate; in the library, as it forces a renewal through
  the client's test-only command): a running session subscribed to XAUUSD
  and EURUSD spots renews on demand, the token file then holding a new
  pair (mode 0600, both tokens, the refresh token replaced, the expiry
  20 to 40 days out), spots after the renewal, a request answered; then a
  fresh session authenticates from the file without renewing. It ROTATES
  the real pair. Run it with `NAUTILUS_IC_RUN=1 cargo test --lib
  the_demo_session_renews -- --nocapture`. Its first run (2026-10-26,
  13:47 UTC, an earlier handler) renewed at start, the file seeded from
  `.env`: saved, the account authenticated with the new token, `expiresIn`
  2,628,000 s (the expiry 30 days 10 hours out); the forced renewal on the
  running session then rotated the pair at cTrader without the answer
  reaching the file, so the pair was lost (the next session: account auth
  refused, renewal refused, `CH_ACCESS_TOKEN_INVALID`). That handler
  dropped a renewal out when an event ended the session, the likely cause
  (cTrader ending the account session as the token is refreshed); the
  handler now keeps it (see "Token file and renewal"). With a new pair
  from the Playground it passed on 2026-10-26: two renewals saved to the
  file (mode 0600), quotes flowing on through the renewal, and
  `tests/live/ic_demo.rs` passing 3/3 from the file. Its log showed cTrader
  answering `ALREADY_SUBSCRIBED` to the subscription sent again after the
  re-auth: subscriptions survive a re-auth on the same connection, so the
  handler no longer sends them again then.
- `tests/live/ic_live_demo.rs`, on the demo (same gate, no order; the node
  reconciles the account through the execution client at start, its cache
  in Redis under trader `HELIXTEST-101`, deleted at the end): `Live` on
  XAUUSD checks the account, then a recording strategy gets its 1-minute
  MID bars warmed up over 30 minutes from IC's data client (no catalog),
  then live quotes (at the spot's millisecond, sized 10,000, after the
  warmup's last) and the bars built from them, the first the minute after
  the last warmup bar, until SIGINT. Passed on 2026-10-26 (16:50-16:53
  UTC, 155 s): the data client answered the request over
  `[16:20:48.710, 16:50:48.710]` in 0.87 s, 3,472 quotes from 3,430 BID and
  3,442 ASK ticks; the last warmup quote at 16:50:48.457, the first live
  one at 16:50:48.808; 29 warmup bars, the last closing 16:50, 3 live bars
  from 16:51, 281 live quotes. Earlier runs on a catalog filled at the
  start (until 2026-10-26) passed with 29 warmup bars too.
- `tests/backtest/backtest_ic_quotes.rs`: a backtest on IC gold quotes written
  through `fill_catalog`; see `../../backtest/README.md`, "IC on ticks".
