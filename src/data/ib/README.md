# Interactive Brokers history

IB fills instrument definitions and external 1-minute LAST bars from IB TRADES through the direct writer; it does not supply quotes, other bar sizes or BID/ASK/MID bars to the catalog. Some instruments, including IDEALPRO forex and spot metals, have no TRADES bars and need another source. See [shared fill semantics](../README.md) and [live IB setup](../../live/ib/README.md).

History starts or reuses the Docker paper gateway, even for a live account. Set `TWS_USERNAME` and `TWS_PASSWORD`; the exchange's Level 1 real-time subscription with Market Data API access must be shared with the paper account, even for historical requests. The fill uses a separate client ID so it can run alongside a live node.

The requested end must be at least one minute before now, checked before starting the gateway: IB can return an in-progress bar, which would otherwise be stored and skipped on resume. IB gives bar opens; the adapter stamps minute bars at their close (`ts_event` and `ts_init`). Full-session data is used for consistency with live; partial session bars can still have misleading close timestamps at coarser cadences.

The history client walks backwards with overlapping explicit-duration requests. IB's start-based segmentation can leave silent gaps because its day durations count trading sessions rather than 24-hour days (nautilus_trader#5384). Expired futures are resolved using explicit contracts with `includeExpired`; availability is limited and varies, so verify depth for the instrument. IB can return flat zero-volume bars after expiry. Its continuous future is not a substitute for per-contract history: its roll and adjustment differ and it is not directly tradable.

IB's simplified IDs use the contract exchange as venue. Configure MIC mappings explicitly when matching another source; automatic exchange-to-MIC conversion can be nondeterministic where an exchange has multiple MICs. See [instrument identity](../README.md).

## Tests

Offline tests are in `src/data/ib`. Docker paper-gateway tests are gated by `NAUTILUS_IB_RUN=1`; see [live IB](../../live/ib/README.md) for setup and operational warnings.
