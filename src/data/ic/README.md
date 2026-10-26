# IC Markets history and live data

`data/ic/` fetches IC Markets' forex and metals instruments and their
quotes, merged from cTrader's BID and ASK tick history, for `fill_catalog`,
and is the live node's data client (`IcDataClient`): live quotes from
cTrader's spot events, quote requests from the tick history. All of it goes
through Spotware's cTrader Open API: binary protobuf frames over Nautilus'
`WebSocketClient`, on `wss://demo.ctraderapi.com:5035` or
`wss://live.ctraderapi.com:5035`. `fill-catalog` routes `….ICMARKETS` ids
here (`../README.md`, "Choosing the source"), and so does `live`
(`../../live/ic/README.md`).

Nautilus has no cTrader adapter; this one follows its adapter guide
(`docs/developer_guide/adapters.md`: outer client and handler task,
`SetClient` first, `AuthTracker`). The messages are Spotware's `.proto`
files, vendored in `proto/` (the commit is in `build.rs`) and compiled with
prost; `build.rs` needs `protoc`.

Neighbouring docs: the fill algorithm (resume, coverage, writing) is in
`data/README.md`, "fill_catalog.rs"; the data client's factory, config and
place in the live node in `live/ic/README.md`.

## Layout

```
ic/
  mod.rs        IcHistory: connect, instruments, the fill
  config.rs     IcEnvironment, IcCredential and IcConnection, from IC_*
  data.rs       IcDataClient: Nautilus' DataClient on a cTrader session
  messages.rs   Spotware's protobuf messages, their payload types, the envelope
  parse.rs      cTrader symbols -> CurrencyPair, ids
  spots.rs      live quotes: spot events merged into quotes, held during a request
  ticks.rs      tick history: decoding, paging, the merge into quotes, their size
  websocket/    the cTrader connection: outer client, handler task, spot subscriptions
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
history.disconnect().await

IcDataClient::new(client_id, &connection, instrument_ids) // a DataClient; the node connects it
```

`fill` refuses a series other than quotes before any request, then fills
each instrument's quotes a week at a time (see "The fill").

## Connecting

### Credentials

In `.env`:

- `IC_ENVIRONMENT`, `demo` or `live`, required.
- `IC_CLIENT_ID` and `IC_CLIENT_SECRET`: the app registered on
  openapi.ctrader.com (Applications, Credentials).
- `IC_ACCESS_TOKEN` and `IC_ACCOUNT_ID`: from the app's Playground, the
  access token and the `ctidTraderAccountId` of the IC account. A token
  lasts about 30 days; renew it by hand in the Playground (or with the
  refresh token on the portal) and update `.env`. `.env.example` also lists
  `IC_REFRESH_TOKEN`, from the same page; the adapter does not read it:
  renewal inside the binary (`ProtoOARefreshTokenReq`) is out of this
  epic's scope. An expired token fails the start (`CH_ACCESS_TOKEN_INVALID`)
  and a token invalidated while running ends the session (see "Session").

None is ever printed: they are `SecretString`s, `Debug` redacts them,
errors name variables and cTrader's error codes only. The plaintext copies
an auth frame is built from are wiped (`zeroize`) once it is encoded; the
frame itself goes to the transport.

### Session

Application auth, then account auth. A refusal names the code and what to
check (`CH_ACCESS_TOKEN_INVALID`: renew `IC_ACCESS_TOKEN`), as does an
account other than `IC_ACCOUNT_ID`. A `ProtoHeartbeatEvent` goes out every
10 s (the transport's Ping is off). Requests carry a `clientMsgId` and get
the response that echoes it.

After a reconnect the session authenticates again, then subscribes again
to the spots of every symbol subscribed, before serving the requests that
waited; requests in flight fail. cTrader ending the session (token
invalidated) fails every request until a reconnect.

On the demo (2026-10-26) cTrader sent no heartbeat of its own in 25 s
while ours ran, and none is needed: the transport's reconnect handles a
dropped connection.

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
- margins of 1, as OKX's;
- stamped at receipt (a symbol has no time).

Indices and shares are refused, naming the symbol.

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
(210,000 and 222,000) in about 12 s. Other requests are not limited.

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
- **The merge** is the history's (`ticks::QuoteMerge`, see "The merge"):
  the other side carried, none before both are known, a crossed quote
  dropped, a locked one kept, sizes as in "Quote size" (XAUUSD 10,000 oz,
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
seconds since (`../../live/README.md`, "Start sequence").

While a request of an instrument is answered (four rate-limited tick
requests, about a second), its live quotes are held, then sent after the
response, under one lock, without those at or before the response's last
quote. So the node never gets a live quote before the warmup response,
which would panic it (nautilus_trader#5393, `../../live/README.md`, "Known
limits"), nor a quote twice. A failed request is logged; the node gets no
response, and the held quotes go out.

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
the first of XAUUSD and of EURUSD, an EURUSD bid alone and an ask alone).

## Tests

- `ic/`, on the responses in `testdata/` (synthetic only where no capture
  can be had: another account, a token invalidated, malformed frames) and
  the mock cTrader peer of `testing.rs`:
  - `config.rs`: the environment required, the credential read whole and
    never in `Debug` or errors;
  - `messages.rs`: envelopes, error responses and malformed payloads;
  - `parse.rs`: XAUUSD and EURUSD to `CurrencyPair`s; an index, a missing
    step, a minimum finer than the step, digits out of range and an
    unlisted asset refused; ids round trip;
  - `ticks.rs`: the captured ticks decoded and merged into gold quotes;
    ticks out of order refused; pages walked back keeping ticks of a
    millisecond split across pages, a window `[from, to)`, a week a
    request, an empty window, a full page of one millisecond refused, the
    last tick before; the merge (other side carried, none before both,
    seeded from before, one quote a millisecond, crossed dropped and locked
    kept, the size and its cap, a price off precision refused);
  - `websocket/`: the session's exact auth frames, the binary heartbeat,
    two requests answered in reverse order each getting its own, a venue
    error, a refused token or app (naming the code, never a credential),
    another account refused, malformed and unknown frames skipped,
    re-authentication after a reconnect, requests failing once cTrader
    ends the session; tick requests at most 5 a second, others unlimited;
  - `mod.rs`: the instruments from assets, symbol list and details; an
    unlisted symbol and another venue refused; a series other than quotes
    refused before any request; a fill, against a peer serving ticks as
    cTrader does: the quotes merged and stored with the instrument over the
    window, each side's requests (the week before, the window to 1 ms past
    its end), a resume from 1 ms after the stored end with no quote twice,
    a `start` before the stored quotes refused before any tick request, a
    first window without ticks naming the series and the window, a week
    without ticks no error and a long fill stored a week at a time.
  - `spots.rs`: the captured spot events into quotes (both sides at their
    time and receipt, sized as history; one side carried; none before both
    sides; crossed dropped; nothing when unsubscribed, the sides kept);
    quotes held by requests and released after them without those
    answered; a spot of a symbol not loaded refused;
  - `websocket/` (spots): a subscription asks for timestamps and its
    events come through; a symbol subscribed twice is asked for once; after
    a reconnect the subscriptions are sent again, an unsubscribed one not;
  - `data.rs`, the `DataClient` against the mock peer: the instruments sent
    at connect on venue `ICMARKETS`; subscribed quotes from the captured
    spot events; an unsubscription sent; a quote request answered from the
    tick history of its window, and its limit; a live quote arriving during
    a request sent after the response; after the peer closes the
    connection, the quotes subscribed again and flowing; a request without
    start and an instrument not loaded refused.
- `tests/ic_demo.rs`, on IC's cTrader demo (gated by `NAUTILUS_IC_RUN=1`,
  demo only; it places no order): XAUUSD and EURUSD loaded as currency
  pairs; a day of XAUUSD (2026-10-21, 149,043 quotes) stored the same in
  one fill and resumed at a time between two milliseconds, one quote a
  millisecond, none crossed, sized 10,000, covered over the day. Passed on
  2026-10-26 (about 20 s).
- `tests/ic_live_demo.rs`, on the demo (same gate, no order, no execution
  client): `Live` on XAUUSD fills the catalog over a 30-minute warmup,
  then a recording strategy gets its 1-minute MID bars warmed up from the
  catalog and the data client, then live quotes (at the spot's
  millisecond, sized 10,000, after the stored ones) and the bars built from
  them, the first the minute after the last warmup bar, until SIGINT.
  Passed on 2026-10-26 (12:35-12:38 UTC, about 150 s): 29 warmup bars, the
  data client asked for the 2.3 s since the fill (5 quotes), 340 live
  quotes and 3 live bars.
- `tests/backtest_ic_quotes.rs`: a backtest on IC gold quotes written
  through `fill_catalog`; see `../../backtest/README.md`, "IC on ticks".
