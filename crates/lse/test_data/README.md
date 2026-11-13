# Test data

What LSE answered, as it answered, but where noted.

- `export_gold_futures_tick.parquet`: `/v3/export?market=futures&symbol=GOLD&timeframe=tick&adjusted=0&format=parquet&from=2026-10-21T10:00:00Z&to=2026-10-21T10:00:01Z`, captured on 2026-11-13 with the free key: six trades, two of them alike (same time, price and size). Its SHA-256 matched LSE's `x-checksum-sha256`.
- `export_xau_usd_commodities_1m.parquet`: `/v3/export?market=commodities&symbol=XAU/USD&timeframe=1m&adjusted=0&format=parquet&from=2026-10-21T10:00:00Z&to=2026-10-21T10:05:00Z`, captured on 2026-11-13 with the free key: five 1-minute candles of gold spot, stamped at their open in milliseconds, of no volume. Its SHA-256 matched LSE's `x-checksum-sha256`.
- `export_gold_futures_4h.parquet`: `/v3/export?market=futures&symbol=GOLD&timeframe=4h&adjusted=0&format=parquet&from=2026-10-21T00:00:00Z&to=2026-10-21T12:00:00Z`, captured on 2026-11-13 with the free key: three 4-hour candles, at 00:00, 04:00 and 08:00. Its SHA-256 matched LSE's `x-checksum-sha256`.
- `refusal_symbol_unknown.json`: 404, the same export of `symbol=NOSUCH`, captured on 2026-11-13.
- `refusal_plan_required.json`: 403, the same export of `market=cme&symbol=ESZ6` on the free key, captured on 2026-11-13.
- `refusal_data_unavailable.json`: 404, GOLD's export over `[2026-11-08T03:00:00Z, 2026-11-08T03:00:02Z)`, a Sunday, captured on 2026-11-13.
- `refusal_date_not_trading.json`: 404, GOLD's export of the date `2026-11-07`, a Saturday, captured on 2026-11-13.
- `refusal_calls_exhausted.json`, `refusal_rows_exhausted.json`: 429s, not captured, which would take a minute's calls or a day's rows; their codes and messages as LSE's API documentation gives them ("Calls a minute", "Rows per export and per day", read on 2026-11-13), with the `retry_after` in seconds the documentation says the body carries. The tests send them with a `Retry-After` header of their own.
