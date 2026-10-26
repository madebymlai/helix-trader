# OKX live

`live/okx/mod.rs` builds the OKX data and execution clients for the live
node and checks the OKX account before the node starts. It never changes the
account. The live node, its start sequence and the routing of instruments
to venues are in `live/README.md`.

The clients come from `nautilus_okx::{config, factories}`, built with
`default-features = false` (64-bit prices). OKX is one venue, so Nautilus
routes it to these clients with no route of ours.

```rust
OKXDataClientFactory::new()       // name() == "OKX"
OKXExecutionClientFactory::new()  // name() == "OKX"
```

## Environment

- **Environment from `.env`, EEA region.** Both configs take `environment`
  from `OKX_ENVIRONMENT` (`demo` or `live`, required) and set `region:
  OKXRegion::Eea`. On demo: REST on `eea.okx.com` with the
  `x-simulated-trading: 1` header, WebSockets on `wseeapap.okx.com`. An
  account opened on my.okx.com is an EEA one, and in the EEA a retail
  account trades X-Perps, not the global USDT swaps.
- **A key works only in its own region and environment.** A demo key needs
  `OKX_ENVIRONMENT=demo` and a real key `live`; OKX refuses a key in the
  other environment with code 50101, and a demo key on `www.okx.com` with
  50119.
- **Instruments.** The configs load the instrument types and families of
  the traded ids (`BTC-USD_UM_XPERP-310328.OKX`: `FUTURES`, family
  `BTC-USD_UM_XPERP`). The demo lists other contracts than the real
  account: on 2026-10-22 its BTC X-Perp was `BTC-USD_UM_XPERP-310328` (max
  leverage 10), the real one `BTC-USD_UM_XPERP-310404`.
- **Cross margin.** The exec config sets `margin_mode:
  Some(OKXMarginMode::Cross)`; the adapter's default, `None`, sends
  isolated orders.
- **Account id** is the default `OKX-001`. The factory picks `Margin` and
  `Netting` for swaps and futures.

## Setting up the account

1. Create an API key on my.okx.com under Demo Trading, with trade
   permission, and put it in `.env` as `OKX_API_KEY`, `OKX_API_SECRET`,
   `OKX_API_PASSPHRASE` (the names the adapter reads; see `.env.example`),
   with `OKX_ENVIRONMENT=demo`. The start checks they are set; the adapter
   reads them, never our code.
2. Set the position mode to one-way (net) by hand, with no open position or
   order. The start reads it through the adapter
   (`get_account_configuration`) and refuses long/short mode.
3. Set the leverage by hand on each instrument, in cross margin, to the
   run's `--leverage`. The adapter neither sets nor reads it, so the start
   reads it with our own `GET /api/v5/account/leverage-info`, signed with
   the adapter's `Credential`, and refuses another value, naming both.
4. Turn off hyphens in client order ids, which OKX refuses (Nautilus' OKX
   docs), in the strategy config's `[base]`:

   ```toml
   [base]
   strategy_id = "EMA_CROSS-001"
   order_id_tag = "001"
   use_hyphens_in_client_order_ids = false
   ```

## Warmup and bars

OKX instruments trade on bars of OKX's 1-minute candles, a composite bar
type such as `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`.
How warmup is requested is in `strategy/README.md`, "Warmup and training";
the 5 s build delay of the live hour bars is in "Bars built from quotes".
What the OKX data client does:

- **Warmup from the catalog, then the REST candles.** The node registers
  the catalog fill-catalog fills, so the data engine answers the
  `…-1-MINUTE-LAST-EXTERNAL` request from the archive's candles stored
  there and sends the rest of the window to the OKX data client, which
  pages OKX's candles up to now (`/market/candles`, then
  `/market/history-candles`), each stamped `ts_init` = its close, as the
  archive's. Its answer is not written back: the client replies with the
  payload alone, which Nautilus does not take as complete coverage (it
  logs "Catalog write-back requires an explicit completed response
  envelope").
- **Live minute bars** come from the business WebSocket's candle channel,
  confirmed candles only.
- **No quote history.** The data client does not implement
  `request_quotes`, so bars built from quotes get no warmup on OKX.

Verified on the demo, 2026-10-22, 17:08 to 18:03 UTC, `warmup = "3h"`,
`--leverage 3` (both accounts' leverage): BTC-USDT-SWAP and the X-Perp
BTC-USD_UM_XPERP-310328 each got 179 minute bars in about 0.14 s, then a
candle every minute, 0.3 to 1.5 s after its close. The minute closing
18:00 arrived at 18:00:01.4 (swap) and 18:00:00.7 (X-Perp), and the hour
bar was built at 18:00:05. No order (the EMAs were not warm); clean stop
on SIGINT.

## Known limits

- **X-Perps in `0.66.0`.** The adapter took any symbol with an underscore
  for a spread, so an X-Perp's quotes and orders took OKX's spread routes,
  which refuse it. Fixed upstream
  ([nautilus_trader#5337](https://github.com/nautechsystems/nautilus_trader/issues/5337),
  commit `4d032a3e8d`), in the pinned commit. The candle path never had the
  check; the demo run above got the X-Perp's candles. Orders on the X-Perp
  have not been run yet.
- **The warmup panic** (nautilus_trader#5393, `live/README.md`, "Known
  limits") applies to OKX as to IB: a live candle arriving before the
  warmup response panics the node. OKX answers a 3-hour warmup in well
  under a second and a candle arrives once a minute; a long warmup (720
  hours is about 430 history pages, at OKX's 20 requests per 2 s) makes it
  likely.
- **Warmup beyond about 833 hours is cut short silently.** The adapter's
  `request_bars` stops after 500 pages of 100 1-minute candles
  (`MAX_PAGES_SOFT`, `nautilus-okx` `http/client.rs`) without an error.

## OKX rules

- **Environment from `OKX_ENVIRONMENT`**, required; nothing defaults to
  demo or live.
- **Net mode, leverage by hand**: the start checks both and changes
  neither.
- **No hyphens** in the strategy's client order ids.

## OKX tests

- Unit tests in `mod.rs`: the configs (demo and EEA endpoints, cross
  margin, credentials left to the environment, instrument types and
  families), long/short mode refused, the leverage check and the parsing
  of OKX's leverage answer.
- End to end, `tests/live/okx_demo.rs`: `Live` on the demo's BTC X-Perp reads
  the real demo account and is refused at a leverage of 1000, which no
  account has, before any node is built; it only reads the account. It is
  skipped unless `NAUTILUS_OKX_RUN=1` is set in the shell:

  ```sh
  NAUTILUS_OKX_RUN=1 cargo test --test live okx_demo:: -- --nocapture
  ```
