# IC Markets history

`data/ic/` fetches IC Markets' forex and metals instruments for
`fill_catalog` through Spotware's cTrader Open API: binary protobuf frames
over Nautilus' `WebSocketClient`, on `wss://demo.ctraderapi.com:5035` or
`wss://live.ctraderapi.com:5035`. Its quotes are not served yet: a fill
stores the instruments, then fails on the first quote series, naming it.
`fill-catalog` routes `….ICMARKETS` ids here (`../README.md`, "Choosing
the source").

Nautilus has no cTrader adapter; this one follows its adapter guide
(`docs/developer_guide/adapters.md`: outer client and handler task,
`SetClient` first, `AuthTracker`). The messages are Spotware's `.proto`
files, vendored in `proto/` (the commit is in `build.rs`) and compiled with
prost; `build.rs` needs `protoc`.

Neighbouring docs: the fill algorithm (resume, coverage, writing) is in
`data/README.md`, "fill_catalog.rs".

## Layout

```
ic/
  mod.rs        IcHistory: connect, instruments, the fill
  config.rs     IcEnvironment and IcCredential, from IC_*
  messages.rs   Spotware's protobuf messages, their payload types, the envelope
  parse.rs      cTrader symbols -> CurrencyPair, ids
  websocket/    the cTrader connection: outer client, handler task
  testing.rs    a mock cTrader peer (a local WebSocket server) for the tests
  proto/        Spotware's four .proto files, vendored
  testdata/     responses captured from IC's demo, scrubbed, text and wire forms
```

## Interface

```rust
IcHistory::connect().await -> anyhow::Result<IcHistory>   // reads IC_*, authenticates
IcHistory::serves(id: InstrumentId) -> bool               // the ICMARKETS venue
history.instruments(&ids).await -> anyhow::Result<Vec<InstrumentAny>>
history.fill(&mut catalog, series, start, end).await      // instruments, then quotes (not yet)
history.disconnect().await
```

`fill` refuses a series other than quotes before any request.

## Connecting

### Credentials

In `.env`:

- `IC_ENVIRONMENT`, `demo` or `live`, required.
- `IC_CLIENT_ID` and `IC_CLIENT_SECRET`: the app registered on
  openapi.ctrader.com (Applications, Credentials).
- `IC_ACCESS_TOKEN` and `IC_ACCOUNT_ID`: from the app's Playground, the
  access token and the `ctidTraderAccountId` of the IC account. A token
  lasts about 30 days; renew it by hand in the Playground.
  `.env.example` also lists `IC_REFRESH_TOKEN`, from the same page; the
  adapter does not read it.

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

After a reconnect the session authenticates again; requests in flight
fail. cTrader ending the session (token invalidated) fails every request
until a reconnect.

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

Not served yet. A probe on the demo (2026-10-26, one connection, requests
one after another) decided how they will be:

- **Encoding.** `ProtoOAGetTickDataRes.tickData` is newest first; the
  first tick is absolute (Unix ms, price × 100,000), each following one
  the difference in time and in price from the one before. A full page is
  10,000 ticks with `hasMore`; requests spanned a week each.
- **Speed.** About 2 pages a second, bound by latency (the limit is 5
  historical requests a second). One month of XAUUSD: 3.09 M BID and
  3.09 M ASK ticks, 312 pages and about 150 s a side; of EURUSD: 0.88 M
  and 0.91 M, about 40 s a side.
- **Depth.** Ticks from 2015 on both (none in January 2014); M1 bars from
  2013 on XAUUSD, 2012 on EURUSD.
- **Decision: ticks**, BID and ASK merged into quotes, not the M1 BID
  fallback: about 11 years of ticks, the whole of XAUUSD's in about 6-7 h.
- **Open.** Whether `toTimestamp` is inclusive: paging back to the oldest
  tick's time minus 1 ms could drop ticks sharing a millisecond across a
  page boundary.

## Verified runs

On the demo (2026-10-26, `fill-catalog` on XAUUSD and EURUSD into a
scratch catalog): both stored as above. IC's symbol names are `XAUUSD`,
`EURUSD`, `US500`; volumes are in cents of a unit (`lotSize` 10,000 is
XAUUSD's 100 oz, 10,000,000 EURUSD's 100,000 EUR, IC's published lots).
The details carry the commission: `USD_PER_MILLION_USD`,
`preciseTradingCommissionRate` 3,000,000,000 (30 USD per million, on
both). cTrader sent no heartbeat of its own in 25 s while ours ran.

`testdata/` holds those responses, scrubbed: the account id replaced by a
fake one, the lists trimmed to three symbols.

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
  - `websocket/`: the session's exact auth frames, the binary heartbeat,
    two requests answered in reverse order each getting its own, a venue
    error, a refused token or app (naming the code, never a credential),
    another account refused, malformed and unknown frames skipped,
    re-authentication after a reconnect, requests failing once cTrader
    ends the session;
  - `mod.rs`: the instruments from assets, symbol list and details; an
    unlisted symbol and another venue refused; a fill storing the
    instruments then naming the quotes; a series other than quotes refused
    before any request.
- `tests/ic_demo.rs`, on IC's cTrader demo (gated by `NAUTILUS_IC_RUN=1`,
  demo only; it places no order): XAUUSD and EURUSD loaded as currency
  pairs. Passed on 2026-10-26.
