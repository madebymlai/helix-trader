# OKX

OKX serves `OKX` instruments, history and live (see the [routing table](../mod.rs)). See [catalog](../../catalog/README.md) for shared fill semantics and [live](../../live/README.md) for startup, routing, recording and recovery.

## History

`fill-catalog` fills swaps and X-Perps through the node with Nautilus' OKX data client, on public endpoints without a key: the strategy's source series and each instrument's funding, over what the catalog lacks, funding before the other series of an instrument. Bars are requested a day at a time. The data engine writes back only the windows the client reports complete; the fill counts a window answered without that report, writes nothing for it, and asks for it again on the next run.

OKX's history serves what Nautilus' OKX data client requests (`venues::serves`, `okx::serves`): an instrument's trades (`request_trades`) and its candles (`request_bars`), `EXTERNAL` bars of the LAST price at a width OKX has a candle of, 1 second, 1, 3, 5, 15 or 30 minutes, 1, 2, 4, 6 or 12 hours, 1, 2, 3 or 5 days, a week or 1, 3, 6 or 12 months (`bar_spec_as_okx_channel`). The client asks OKX's trade-price candles whatever price a bar type names, so no other price is served, and it implements no `request_quotes` and no book request, Nautilus' defaults refusing them. A series it does not serve fails a fill before anything connects, naming why, and gets no warmup. So no rate of an OKX pair can be read, neither its BID and ASK bars nor its quotes being served, and a config listing one in `[trade] conversions` is refused at load (see [strategy](../../strategy/README.md#configuration-and-series)). Definitions are not requested through the node, whose OKX client defines instruments with margins of 0 (see [Funding and margins](#funding-and-margins)): they are read from OKX's public API, given the account's margins and written before the node starts. Writing a definition the catalog lacks reads the account's leverage for it, so it needs the key and environment of [Account setup](#account-setup); a fill whose definitions are all stored needs no key. Dated futures are refused.

### Funding and margins

Funding is requested two weeks at a time from OKX's funding history, which keeps about three months (an observation, not a guarantee); older funding is absent from the catalog. The first update of each request has no preceding interval, so a backtest does not settle it. Check funding depth for the full backtest window; see [backtest](../../backtest/README.md).

Both instrument margin rates are the account's: 1 / the leverage the account trades the instrument at in cross margin when its definition is written (a signed `GET /api/v5/account/leverage-info`, which the adapter does not make), as IC's definitions hold IC's margins. Backtests run unleveraged on them, so an OKX position locks notional / leverage, as OKX does. A definition is written once: after changing an instrument's leverage on OKX, fill a fresh catalog, whose definition holds the new margin; `live` refuses an account whose leverage differs from the catalog's. OKX liquidation and tiered maintenance rules are not simulated.

Catalogs filled with the old order-book-derived quotes need a fresh directory for candles; existing quotes are not removed or extended.

## Account setup

1. Set `OKX_ENVIRONMENT` to `demo` or `live` and provide `OKX_API_KEY`, `OKX_API_SECRET` and `OKX_API_PASSPHRASE` in the environment or `.env`. Keys are specific to both region and environment. The clients use the EEA region; EEA retail accounts trade X-Perps rather than global USDT swaps.
2. Set the account to one-way (net) position mode. Startup refuses long/short mode and does not change the account.
3. Set each instrument's leverage manually in cross margin before filling its definition, which holds it as the instrument's margin (see [Funding and margins](#funding-and-margins)). Live startup reads the definition from `--catalog`, refusing a missing one, checks the account's leverage against it and refuses a mismatch; it does not set leverage. Orders use cross margin.
4. Disable hyphens in client order IDs in the strategy config; OKX refuses them:

   ```toml
   [base]
   strategy_id = "EMA_CROSS-001"
   use_hyphens_in_client_order_ids = false
   ```

Demo and live can list different contracts, so select an instrument present in the chosen environment rather than assuming the same ID works in both. OKX futures and swaps use netting.

## Warmup and limits

OKX composite bars warm up from catalog minute candles and then REST candles; live minute candles arrive through WebSocket. The data engine writes the REST candles back to the catalog when the client reports their window complete. Quote-built bars and an instrument's quotes get no warmup (see [History](#history)): the warmup logs a warning and they start at the subscription.

A warmup needing more than 500 pages of minute candles beyond the catalog, roughly 835 hours (100 candles a page, up to 300 on a page less than two hours old), fails its request: the warmup is logged incomplete and keeps the candles fetched. X-Perp candle behavior has been observed on demo; X-Perp orders have not been verified there.

## Live data

OKX's live data streams what Nautilus' OKX data client subscribes to (`venues::streams`, `okx::streams`), which a live start checks every series the run subscribes to against before anything connects, refusing one it does not stream, naming it and why (see [live](../../live/README.md#startup-and-routing)): an instrument's quotes (`subscribe_quotes`), its trades (`subscribe_trades`) and its candles (`subscribe_bars`), the bar types its [history](#history) serves, so bars built from its quotes, which get no warmup, start at the live quotes. It streams an instrument's book too, which a run subscribes to in OKX's format (`venues::book_format`, `okx::BOOK`), the one the client takes, failing any other: its deltas (`subscribe_book_deltas`) of an L2_MBP book, each a price level, asked no depth, which the client takes as OKX's `books` channel, 400 levels deep; and its depth snapshots (`subscribe_book_depth`) of five levels a side, OKX's `books5`, the most the client subscribes to. Such a snapshot, an `OrderBookDepth`, holds the five levels a side OKX sends, where LSE's hold ten: Nautilus' depth holds the levels its venue sends, leaving out empty ones (`OrderBookDepth::new_checked`), so a feature reading depth reads at most five levels a side on OKX, and no feature declares the levels it needs. OKX's history serves no book, so a warmup requests none and a backtest replays the book live runs recorded. Nor does the client subscribe to custom data, Nautilus' default logging the subscription as not implemented.

## Tests

Offline definition, request and client tests are in `src/venues/okx`; the node fill of bars and funding is tested in `tests/node_fill`. The account-check test in `tests/live/okx_demo.rs` requires `NAUTILUS_OKX_RUN=1` in the shell and reads the demo account without placing orders. Other venue tests may place real orders; inspect their gates and account before running. Run live-node tests in separate processes.

```sh
NAUTILUS_OKX_RUN=1 cargo test --test live okx_demo:: -- --nocapture
```
