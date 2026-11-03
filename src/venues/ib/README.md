# Interactive Brokers

IB serves every venue without a row of its own in the [routing table](../mod.rs), history and live, and trades GLBX's instruments live. See [data](../../data/README.md) for shared fill semantics and [live](../../live/README.md) for node startup, routing, Redis and recovery.

## History

IB fills instrument definitions and external 1-minute LAST bars from IB TRADES through the direct writer; it does not supply quotes, other bar sizes or BID/ASK/MID bars to the catalog. Some instruments, including IDEALPRO forex and spot metals, have no TRADES bars and need another source.

History starts or reuses the Docker paper gateway, even for a live account. Set `TWS_USERNAME` and `TWS_PASSWORD`; the exchange's Level 1 real-time subscription with Market Data API access must be shared with the paper account, even for historical requests. The fill uses a separate client ID so it can run alongside a live node.

The requested end must be at least one minute before now, checked before starting the gateway: IB can return an in-progress bar, which would otherwise be stored and skipped on resume. IB gives bar opens; the adapter stamps minute bars at their close (`ts_event` and `ts_init`). Full-session data is used for consistency with live; partial session bars can still have misleading close timestamps at coarser cadences.

The history client walks backwards with overlapping explicit-duration requests. IB's start-based segmentation can leave silent gaps because its day durations count trading sessions rather than 24-hour days (nautilus_trader#5384). Expired futures are resolved using explicit contracts with `includeExpired`; availability is limited and varies, so verify depth for the instrument. IB can return flat zero-volume bars after expiry. Its continuous future is not a substitute for per-contract history: its roll and adjustment differ and it is not directly tradable.

IB's simplified IDs use the contract exchange as venue. Configure MIC mappings explicitly when matching another source; automatic exchange-to-MIC conversion can be nondeterministic where an exchange has multiple MICs. See [instrument identity](../../data/README.md).

## Account and gateway

Set a bare account ID with `--account` or `NAUTILUS_IB_ACCOUNT_ID`. A paper ID beginning with `D` selects the paper gateway; another selects live. Set `TWS_USERNAME` and `TWS_PASSWORD` in the environment or `.env`. Live login requires IBKR Mobile 2FA approval within the startup timeout.

`live` starts or reuses the Dockerized gateway; it does not use a manually started gateway or TWS. Pull the image first:

```sh
docker pull ghcr.io/gnzsnz/ib-gateway:stable
```

The paper and live containers use ports 4002 and 4001 respectively and remain running for reuse. The gateway login takes over an existing session of the same IB user; close any manual gateway occupying the port. Start concurrent commands sequentially until the first gateway has logged in, to avoid a container-start race. Distinct processes need distinct client IDs. The gateway API is not read-only because even its initial execution connection requires write access. After a gateway restart, periodic checks run but the node does not repeat full startup reconciliation.

## Trading and warmup

IB is netting. **IB's execution adapter rejects reduce-only orders:** bracket exits and `close_position` use them. Consequently the EMA crossover and model strategies cannot trade live on IB; backtests on IB data and other venues are unaffected. There is no workaround here.

Live bars use full-session IB minute TRADES bars, matching history. Market orders can be priced from cached LAST bars; MID bars alone do not supply that price. Warmup ends at the request time, excluding the partial minute; the live subscription starts there. The data client does not write its historical answer back to the catalog.

A live bar arriving before a warmup response can panic the node ([upstream issue](https://github.com/nautechsystems/nautilus_trader/issues/5393)). Keep IB warmups short enough to complete before the next minute bar; long warmups are especially risky. A warmup of a day or more may silently miss bars because IB counts days as sessions when segmenting requests; this live case remains unverified. See [history](#history) for history limitations.

## Tests

Offline tests are in `src/venues/ib`. IB end-to-end tests in `tests/live/` require `NAUTILUS_IB_RUN=1` in the shell and a paper `NAUTILUS_IB_ACCOUNT_ID`; they start or reuse the Docker paper gateway. `ib_restart` also requires `NAUTILUS_IB_LIVE_ORDERS=1` and **places paper-account orders**; ensure the account and market are suitable and check the account afterwards. Run live-node tests in separate processes.

```sh
NAUTILUS_IB_RUN=1 cargo test --test live ib_gateway::
NAUTILUS_IB_RUN=1 cargo test --test live ib_minute_bars:: -- --nocapture
NAUTILUS_IB_RUN=1 NAUTILUS_IB_LIVE_ORDERS=1 cargo test --test live ib_restart:: -- --test-threads=1
```
