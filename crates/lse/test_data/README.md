# Test data

What LSE answered, as it answered, but where noted.

- `export_gold_futures_tick.parquet`: `/v3/export?market=futures&symbol=GOLD&timeframe=tick&adjusted=0&format=parquet&from=2026-10-21T10:00:00Z&to=2026-10-21T10:00:01Z`, captured on 2026-11-13 with the free key: six trades, two of them alike (same time, price and size). Its SHA-256 matched LSE's `x-checksum-sha256`.
- `reference_catalogue.json`: `/v3/reference`, LSE's catalogue of its 21 reference collections, each with its `time_column`, captured on 2026-11-13.
- `reference_bond_yields_us10y.parquet`: `/v3/reference/export?dataset=bond_yields&symbol=US10Y&from=2026-11-09&to=2026-11-12&format=parquet`, captured on 2026-11-13: three daily rows of the US 10-year yield, of 9, 10 and 12 November (none of the 11th, Veterans Day), `date` a `date32`. Its SHA-256 matched LSE's `x-checksum-sha256`.
- `refusal_symbol_unknown.json`: 404, the same export of `symbol=NOSUCH`, captured on 2026-11-13.
- `refusal_plan_required.json`: 403, the same export of `market=cme&symbol=ESZ6` on the free key, captured on 2026-11-13.
- `refusal_data_unavailable.json`: 404, GOLD's export over `[2026-11-08T03:00:00Z, 2026-11-08T03:00:02Z)`, a Sunday, captured on 2026-11-13.
- `refusal_reference_data_unavailable.json`: 404, the bond yields export of US10Y over the dates `2026-11-07` to `2026-11-08`, a weekend, captured on 2026-11-13.
- `refusal_date_not_trading.json`: 404, GOLD's export of the date `2026-11-07`, a Saturday, captured on 2026-11-13.
- `refusal_calls_exhausted.json`, `refusal_rows_exhausted.json`: 429s, not captured, which would take a minute's calls or a day's rows; their codes and messages as LSE's API documentation gives them ("Calls a minute", "Rows per export and per day", read on 2026-11-13), with the `retry_after` in seconds the documentation says the body carries. The tests send them with a `Retry-After` header of their own.
