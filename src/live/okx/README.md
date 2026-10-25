# OKX live

`nautilus_okx::{config, factories}`, with `default-features = false`
(64-bit prices).

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
  account trades X-Perps, not the global USDT swaps. A key only works in
  its own region and environment: the demo key is refused by the real
  account (code 50101) and by `www.okx.com` (50119). So a demo key needs
  `OKX_ENVIRONMENT=demo` and a real key `live`: OKX refuses a key in the
  other environment with 50101.
- **Instruments.** The configs load the instrument types and families of
  the traded ids (`BTC-USD_UM_XPERP-310328.OKX`: `FUTURES`, family
  `BTC-USD_UM_XPERP`). The demo lists other contracts than the real
  account: on 2026-10-22 its BTC X-Perp was `BTC-USD_UM_XPERP-310328` (max
  leverage 10), the real one `BTC-USD_UM_XPERP-310404`.
- **Cross margin**: the exec config sets `margin_mode:
  Some(OKXMarginMode::Cross)` (the adapter's default, `None`, sends
  isolated orders).
- **Account id**: the default `OKX-001`. The factory picks `Margin` and
  `Netting` for swaps and futures.

## Setting up the account

1. Create an API key on my.okx.com under Demo Trading, with trade
   permission, and put it in `.env` as `OKX_API_KEY`, `OKX_API_SECRET`,
   `OKX_API_PASSPHRASE` (the names the adapter reads; see `.env.example`),
   with `OKX_ENVIRONMENT=demo`. The start checks they are set; the adapter
   reads them, never our code.
2. Position mode: one-way (net). The start reads it through the adapter
   (`get_account_configuration`) and refuses long/short mode. Change it by
   hand, with no open position or order.
3. Leverage: set it by hand on each instrument, in cross margin, to the
   `--leverage` of the run. The adapter neither sets nor reads it, so the
   start reads it with our signed `GET /api/v5/account/leverage-info`
   (signed by the adapter's `Credential`) and refuses another value,
   naming both. Neither check changes the account.
4. Strategy config: OKX refuses client order ids with hyphens, so set
   `use_hyphens_in_client_order_ids = false` in the strategy's `[base]`
   (Nautilus' OKX docs):

   ```toml
   [base]
   strategy_id = "EMA_CROSS-001"
   order_id_tag = "001"
   use_hyphens_in_client_order_ids = false
   ```

## Warmup and bars

OKX instruments trade on bars of OKX's 1-minute candles, a composite bar
type such as `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`
(strategy README). The warmup requests the candles (`request_bars` of
`…-1-MINUTE-LAST-EXTERNAL`); with nothing in the catalog the data engine
sends the whole window to the OKX data client, which pages OKX's REST
candles up to now (`/market/candles`, then `/market/history-candles`),
each stamped `ts_init` = its close, as the archive's. The live minute
bars come from the business WebSocket's candle channel, confirmed candles
only. The live node builds the hour bars 5 s after the hour (strategy
README, "Bars built from quotes"). Bars built from quotes get no warmup
on OKX: its data client does not implement `request_quotes`.

Run on the demo, 2026-10-22, 17:08 to 18:03 UTC, `warmup = "3h"`,
`--leverage 3` (both accounts' leverage): BTC-USDT-SWAP and the X-Perp
BTC-USD_UM_XPERP-310328 each got 179 minute bars in about 0.14 s, then a
candle every minute, 0.3 to 1.5 s after its close. The minute closing
18:00 arrived at 18:00:01.4 (swap) and 18:00:00.7 (X-Perp), and the hour
bar was built at 18:00:05. No order (the EMAs were not warm); clean stop
on SIGINT.

## Known limits

- **X-Perps in `0.66.0`.** The adapter took any symbol with an
  underscore for a spread, so an X-Perp's quotes and orders took OKX's
  spread routes, which refuse it. Fixed upstream
  ([nautilus_trader#5337](https://github.com/nautechsystems/nautilus_trader/issues/5337),
  commit `4d032a3e8d`), in the pinned commit. The candle path never had
  the check; the demo run above got the X-Perp's candles. Orders on the
  X-Perp are not yet run.
- **nautilus_trader#5393** (the warmup panic, see "Known limits" above)
  applies to OKX as to IB: a live candle arriving before the warmup
  response panics the node. OKX answers a 3-hour warmup in well under a
  second, and a candle arrives once a minute; a long warmup (720 hours is
  about 430 history pages, at OKX's 20 requests per 2 s) makes it likely.
- **Warmup beyond about 833 hours is cut short silently.** The adapter's
  `request_bars` stops after 500 pages of 100 1-minute candles
  (`MAX_PAGES_SOFT`, `nautilus-okx` `http/client.rs`) without an error.

## OKX rules

- **Demo until switched**: `ENVIRONMENT` in `live/okx/mod.rs`.
- **Net mode, leverage by hand**: the start checks both and changes
  neither.
- **No hyphens** in the strategy's client order ids.

## OKX tests

- Unit tests in `mod.rs`: the configs (demo and EEA endpoints, cross
  margin, credentials left to the environment, instrument types and
  families), net mode refused otherwise, the leverage check and the parsing
  of OKX's leverage answer.
- End to end, `tests/okx_demo.rs`: `Live` on the demo's BTC X-Perp checks
  the real demo account, then is refused at a leverage of 1000, which no
  account has, before any node is built; it only reads the account.
  Gated: it is skipped unless `NAUTILUS_OKX_RUN=1` is set in the shell:

  ```sh
  NAUTILUS_OKX_RUN=1 cargo test --test okx_demo -- --nocapture
  ```
