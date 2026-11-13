# Test data

What LSE answered, as it answered, but where noted.

- `export_gold_futures_tick.parquet`: `/v3/export?market=futures&symbol=GOLD&timeframe=tick&adjusted=0&format=parquet&from=2026-10-21T10:00:00Z&to=2026-10-21T10:00:01Z`, captured on 2026-11-13 with the free key: six trades, two of them alike (same time, price and size). Its SHA-256 matched LSE's `x-checksum-sha256`.
- `refusal_symbol_unknown.json`: 404, the same export of `symbol=NOSUCH`, captured on 2026-11-13.
- `refusal_plan_required.json`: 403, the same export of `market=cme&symbol=ESZ6` on the free key, captured on 2026-11-13.
- `refusal_data_unavailable.json`: 404, GOLD's export over `[2026-11-08T03:00:00Z, 2026-11-08T03:00:02Z)`, a Sunday, captured on 2026-11-13.
- `refusal_date_not_trading.json`: 404, GOLD's export of the date `2026-11-07`, a Saturday, captured on 2026-11-13.
- `ws_*.json`, `ws_ticks_gold.jsonl`: data-ws's frames, captured on 2026-11-13 with the free key: its welcome; its authenticated frame (`ws_authenticated_futures.json`), its list of 5,407 symbols trimmed to the 33 futures it lists (those suffixed `.F`), the rest as LSE sent it; the subscription of `GOLD.F` (`ws_subscribed_gold.json`) and the first three ticks after it (16:44:55 to 16:45:00 UTC), and its unsubscription; the pong to a ping; its refusals of a 17th symbol on the key's 16 (`ws_error_limit_reached.json`), of a second connection on the key's one (`ws_error_too_many_connections.json`, which LSE then closed with 1008, "too many connections") and of a key it does not know (`ws_error_invalid_key.json`). The frames were scanned for the key before they were kept.
- `refusal_calls_exhausted.json`, `refusal_rows_exhausted.json`: 429s, not captured, which would take a minute's calls or a day's rows; their codes and messages as LSE's API documentation gives them ("Calls a minute", "Rows per export and per day", read on 2026-11-13), with the `retry_after` in seconds the documentation says the body carries. The tests send them with a `Retry-After` header of their own.
