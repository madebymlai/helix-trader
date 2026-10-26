# IC Markets live

`live/ic/mod.rs` builds IC's data client for the live node: the factory and
config of the adapter in `data/ic/` (`IcDataClient`). IC has no execution
client yet (a later slice of the IC adapter), so the node gets none for
`ICMARKETS`. The node, the start sequence and the routing between venues are
in `live/README.md`; the cTrader session, the spot events, the merge into
quotes and the quote requests in `data/ic/README.md`.

```rust
IcDataClientFactory                  // name() == "ICMARKETS", config_type() == "IcDataClientConfig"
IcDataClientConfig { connection: IcConnection, instrument_ids: Vec<InstrumentId> }
```

The factory creates an `IcDataClient` named after the client
(`ClientId("ICMARKETS")`); its `venue()` is `ICMARKETS`, so the data engine
routes IC instruments to it with no route of ours (`RoutingConfig::default()`).

## Environment and credential

`ic::clients` reads `IC_ENVIRONMENT` (`demo` or `live`, required, no
default) and the credential (`IC_CLIENT_ID`, `IC_CLIENT_SECRET`,
`IC_ACCESS_TOKEN`, `IC_ACCOUNT_ID`) into an `IcConnection` before anything
connects, as `OKX_ENVIRONMENT` is read for OKX: a missing or unknown value
fails the start naming the variable, never a value. The config carries the
connection; `Debug` redacts the credential.

The access token lasts about 30 days; renewing it is a manual step
(`data/ic/README.md`, "Credentials"). An expired token fails the start's
catalog fill, before the node is built.

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

- **No execution client.** An order on an IC instrument finds no client and
  is denied (`NoExecutionClient`); nothing reaches IC. A strategy trading IC
  instruments with IB ones is refused at the start, since IB's execution
  client is the default route and would get IC's orders.
- **Market closed over the whole warmup** (a weekend with a short warmup):
  the start's fill fails, naming the series and the window, as
  `fill-catalog` does.
- **The first spot** after a subscription is the last price, even with the
  market closed; when the warmup response already holds it, it is dropped.

## IC tests

- Unit tests in `mod.rs`: a missing or unknown `IC_ENVIRONMENT` and a missing
  credential variable refused before anything connects, an instrument of
  another venue refused, the factory creating an unconnected client on
  `ICMARKETS`, no execution client for the node.
- The data client against the mock cTrader peer: `data/ic/README.md`,
  "Tests".
- End to end, `tests/ic_live_demo.rs`: `Live` on the demo's XAUUSD, a
  recording strategy getting its warmup, live quotes and the minute bars
  built from them; no order. Skipped unless `NAUTILUS_IC_RUN=1` is set in
  the shell:

  ```sh
  NAUTILUS_IC_RUN=1 cargo test --test ic_live_demo -- --nocapture
  ```
