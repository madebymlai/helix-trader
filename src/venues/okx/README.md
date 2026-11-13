# OKX

OKX serves `OKX` instruments, history and live (see the [routing table](../mod.rs)). See [catalog](../../catalog/README.md) for shared fill semantics and [live](../../live/README.md) for startup, routing, recording and recovery.

## History

`fill-catalog` fills swaps and X-Perps through the node with Nautilus' OKX data client, on public endpoints without a key: the strategy's source series and each instrument's funding, over what the catalog lacks, funding before the other series of an instrument. Bars are requested a day at a time. The data engine writes back only the windows the client reports complete; the fill counts a window answered without that report, writes nothing for it, and asks for it again on the next run. A series the client does not serve fails the fill. Definitions are not requested through the node, whose OKX client defines instruments with margins of 0 (see [Funding and margins](#funding-and-margins)): they are read from OKX's public API, given margins of 1 and written before the node starts. Dated futures are refused.

### Funding and margins

Funding is requested two weeks at a time from OKX's funding history, which keeps about three months (an observation, not a guarantee); older funding is absent from the catalog. The first update of each request has no preceding interval, so a backtest does not settle it. Check funding depth for the full backtest window; see [backtest](../../backtest/README.md).

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

OKX composite bars warm up from catalog minute candles and then REST candles; live minute candles arrive through WebSocket. The data engine writes the REST candles back to the catalog when the client reports their window complete. The adapter does not serve quote history, so quote-built bars cannot warm up from OKX.

A warmup needing more than 500 pages of minute candles beyond the catalog, roughly 835 hours (100 candles a page, up to 300 on a page less than two hours old), fails its request: the warmup is logged incomplete and keeps the candles fetched. X-Perp candle behavior has been observed on demo; X-Perp orders have not been verified there.

## Tests

Offline definition, request and client tests are in `src/venues/okx`; the node fill of bars and funding is tested in `tests/node_fill`. The account-check test in `tests/live/okx_demo.rs` requires `NAUTILUS_OKX_RUN=1` in the shell and reads the demo account without placing orders. Other venue tests may place real orders; inspect their gates and account before running. Run live-node tests in separate processes.

```sh
NAUTILUS_OKX_RUN=1 cargo test --test live okx_demo:: -- --nocapture
```
