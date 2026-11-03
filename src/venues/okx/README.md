# OKX

OKX serves `OKX` instruments, history and live (see the [routing table](../mod.rs)). See [data](../../data/README.md) for shared fill semantics and [live](../../live/README.md) for startup, routing, recording and recovery.

## History

`fill-catalog` uses public OKX endpoints, without a key, for swap and X-Perp definitions, external 1-minute LAST candles and funding. Other series and dated futures are refused. It fills funding before candles, so a funding failure does not add new bars.

### Archive and timestamps

Archive months and days are defined in UTC+8, not UTC. The fill uses monthly files until the first unavailable month, then daily files; publication lags the period. A missing first day is an error; a later unpublished day stops the fill and is retried on the next run. The initial archive period can start before the requested start. Windows are based on candle closes and are contiguous across periods.

OKX candle `ts_event` is the open; `ts_init` is the close, one minute later, so replay cannot see a completed bar at its open. Unconfirmed archive candles are rejected. Swap archives contain their own rows; X-Perp chain files also contain other contracts, which are filtered out.

### Funding and margins

Swap funding comes from monthly archives, followed by the public funding API. X-Perp funding comes from the API only. The API has limited history (previous observations suggest about three months, not a guarantee), so an older X-Perp start can yield less history than requested. A resume whose stored end is older than the API can reach fails rather than silently leaving a gap; no funding on a first fill also fails. Updates without a preceding interval cannot settle and are not stored. Check funding depth for the full backtest window; see [backtest](../../backtest/README.md).

Both instrument margin rates are set to 1: account leverage is chosen separately for the backtest. OKX liquidation and maintenance rules are not simulated by that choice.

Catalogs filled with the old order-book-derived quotes need a fresh directory for candles; existing quotes are not removed or extended.

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

Offline archive, funding, fill and client tests are in `src/venues/okx`. The account-check test in `tests/live/okx_demo.rs` requires `NAUTILUS_OKX_RUN=1` in the shell and reads the demo account without placing orders. Other venue tests may place real orders; inspect their gates and account before running. Run live-node tests in separate processes.

```sh
NAUTILUS_OKX_RUN=1 cargo test --test live okx_demo:: -- --nocapture
```
