# OKX history

`fill-catalog` uses public OKX endpoints, without a key, for swap and X-Perp definitions, external 1-minute LAST candles and funding. Other series and dated futures are refused. It fills funding before candles, so a funding failure does not add new bars. See [shared fill semantics](../README.md) and [live OKX](../../live/okx/README.md).

## Archive and timestamps

Archive months and days are defined in UTC+8, not UTC. The fill uses monthly files until the first unavailable month, then daily files; publication lags the period. A missing first day is an error; a later unpublished day stops the fill and is retried on the next run. The initial archive period can start before the requested start. Windows are based on candle closes and are contiguous across periods.

OKX candle `ts_event` is the open; `ts_init` is the close, one minute later, so replay cannot see a completed bar at its open. Unconfirmed archive candles are rejected. Swap archives contain their own rows; X-Perp chain files also contain other contracts, which are filtered out.

## Funding and margins

Swap funding comes from monthly archives, followed by the public funding API. X-Perp funding comes from the API only. The API has limited history (previous observations suggest about three months, not a guarantee), so an older X-Perp start can yield less history than requested. A resume whose stored end is older than the API can reach fails rather than silently leaving a gap; no funding on a first fill also fails. Updates without a preceding interval cannot settle and are not stored. Check funding depth for the full backtest window; see [backtest](../../backtest/README.md).

Both instrument margin rates are set to 1: account leverage is chosen separately for the backtest. OKX liquidation and maintenance rules are not simulated by that choice.

Catalogs filled with the old order-book-derived quotes need a fresh directory for candles; existing quotes are not removed or extended.

## Tests

Offline archive, funding and fill tests are in `src/data/okx`.
