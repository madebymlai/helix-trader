# Test data

What LSE answered, as it answered, but where noted.

- `export_gold_futures_tick.parquet`: `/v3/export?market=futures&symbol=GOLD&timeframe=tick&adjusted=0&format=parquet&from=2026-10-21T10:00:00Z&to=2026-10-21T10:00:01Z`, captured on 2026-11-13 with the free key: six trades, two of them alike (same time, price and size). Its SHA-256 matched LSE's `x-checksum-sha256`. Asked again that day, LSE sent the same bytes with `x-export-id: 15f88a2dca3f401d90c7acae13236f86`, the id the mock peer sends with every file.
- `refusal_symbol_unknown.json`: 404, the same export of `symbol=NOSUCH`, captured on 2026-11-13.
- `refusal_plan_required.json`: 403, the same export of `market=cme&symbol=ESZ6` on the free key, captured on 2026-11-13.
- `refusal_data_unavailable.json`: 404, GOLD's export over `[2026-11-08T03:00:00Z, 2026-11-08T03:00:02Z)`, a Sunday, captured on 2026-11-13.
- `refusal_date_not_trading.json`: 404, GOLD's export of the date `2026-11-07`, a Saturday, captured on 2026-11-13.
- `refusal_data_unavailable_today.json`: 404, GOLD's export over `[2026-11-13T00:00:00Z, 2026-11-13T16:00:00Z)`, captured at 16:47 UTC that day, which LSE had not loaded: it stores a day after it ends.
- `refusal_export_too_wide.json`: 413, `/v3/export?market=cme&symbol=GCZ6&data=quotes&adjusted=0&format=parquet&from=2026-11-10T00:00:00Z&to=2026-11-11T00:00:00Z`, captured on 2026-11-13 with the CME Group Lite+ key: a day of GCZ6's quotes, about 2.7 million rows, past the 2,500,000 one export may read.
- `refusal_calls_exhausted.json`, `refusal_rows_exhausted.json`, `refusal_allowance_exhausted.json`: 429s, not captured, which would take a minute's calls, a day's rows or a week's allowance; their codes and messages as LSE's API documentation gives them ("Calls a minute", "Rows per export and per day", "One download at a time", read on 2026-11-13), with the `retry_after` in seconds the documentation says the body of the first two carries. The tests send them with a `Retry-After` header of their own.
