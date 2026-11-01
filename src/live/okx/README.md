# OKX live

See [live](../README.md) for startup, routing, recording and recovery, and [OKX data](../../data/okx/README.md) for historical data.

## Account setup

1. Set `OKX_ENVIRONMENT` to `demo` or `live` and provide `OKX_API_KEY`, `OKX_API_SECRET` and `OKX_API_PASSPHRASE` in the environment or `.env`. Keys are specific to both region and environment. The clients use the EEA region; EEA retail accounts trade X-Perps rather than global USDT swaps.
2. Set the account to one-way (net) position mode. Startup refuses long/short mode and does not change the account.
3. Set each instrument's leverage manually in cross margin to the run's `--leverage`. Startup checks it and refuses a mismatch; it does not set leverage. Orders use cross margin.
4. Disable hyphens in client order IDs in the strategy config; OKX refuses them:

   ```toml
   [base]
   strategy_id = "EMA_CROSS-001"
   use_hyphens_in_client_order_ids = false
   ```

Demo and live can list different contracts, so select an instrument present in the chosen environment rather than assuming the same ID works in both. OKX futures and swaps use netting.

## Warmup and limits

OKX composite bars warm up from catalog minute candles and then REST candles; live minute candles arrive through WebSocket. The adapter does not write its warmup response back to the catalog and does not serve quote history, so quote-built bars cannot warm up from OKX.

A live candle arriving before the warmup response can panic the node ([upstream issue](https://github.com/nautechsystems/nautilus_trader/issues/5393)); longer warmups increase this risk. The adapter silently stops after 500 pages of 100 minute candles, cutting warmups beyond roughly 833 hours short. X-Perp candle behavior has been observed on demo; X-Perp orders have not been verified there.

## Tests

The account-check test in `tests/live/okx_demo.rs` requires `NAUTILUS_OKX_RUN=1` in the shell and reads the demo account without placing orders. Other venue tests may place real orders; inspect their gates and account before running. Run live-node tests in separate processes.

```sh
NAUTILUS_OKX_RUN=1 cargo test --test live okx_demo:: -- --nocapture
```
