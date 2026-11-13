# Interactive Brokers

IB serves every venue without a row of its own in the [routing table](../mod.rs), definitions, history and live data, and is its broker; it also serves OPRA's live data, and trades GLBX's and OPRA's instruments, its clients loading those a live run reads and giving their definitions live, while their definitions in the catalog are [Databento](../databento/README.md)'s, their history [LSE](../lse/README.md)'s, and GLBX's live data LSE's. On the live node IB's data client is routed the venues whose live data is IB's, never the default route, and its execution client takes every order no other broker's takes, GLBX's among them: Nautilus' data engine makes the first data client on no venue it registers the default (`DataEngine::register_client`, `crates/data/src/engine/registry.rs:44`) and refuses another made the default after it (`set_default_client`, `registry.rs:101`), and the node builder registers its clients in its `HashMap`'s order (`crates/live/src/node/builder.rs:94`, `:642`), so with LSE's and IB's data clients, both on no venue, making IB's the default failed the build whenever LSE's came first (see [live](../../live/README.md#startup-and-routing)). See [catalog](../../catalog/README.md) for shared fill semantics and [live](../../live/README.md) for node startup, routing, Redis and recovery.

## History

IB fills instrument definitions and external 1-minute LAST bars from IB TRADES through the direct writer; it does not supply quotes, other bar sizes or BID/ASK/MID bars to the catalog. Some instruments, including IDEALPRO forex and spot metals, have no TRADES bars and need another source.

History starts or reuses the Docker paper gateway, even for a live account. Set `TWS_USERNAME` and `TWS_PASSWORD`; the exchange's Level 1 real-time subscription with Market Data API access must be shared with the paper account, even for historical requests. The fill uses a separate client ID so it can run alongside a live node.

The requested end must be at least one minute before now, checked before starting the gateway: IB can return an in-progress bar, which would otherwise be stored and skipped on resume. IB gives bar opens; the adapter stamps minute bars at their close (`ts_event` and `ts_init`). Full-session data is used for consistency with live; partial session bars can still have misleading close timestamps at coarser cadences.

The history client walks backwards with overlapping explicit-duration requests. IB's start-based segmentation can leave silent gaps because its day durations count trading sessions rather than 24-hour days (nautilus_trader#5384). Expired futures are resolved using explicit contracts with `includeExpired`; availability is limited and varies, so verify depth for the instrument. IB can return flat zero-volume bars after expiry. Its continuous future is not a substitute for per-contract history: its roll and adjustment differ and it is not directly tradable.

IB's simplified IDs use the contract exchange as venue. Configure MIC mappings explicitly when matching another source; automatic exchange-to-MIC conversion can be nondeterministic where an exchange has multiple MICs. See [instrument identity](../../catalog/README.md).

## Account and gateway

Set a bare account ID with `--account` or `NAUTILUS_IB_ACCOUNT_ID`. A paper ID beginning with `D` selects the paper gateway; another selects live. Set `TWS_USERNAME` and `TWS_PASSWORD` in the environment or `.env`. Live login requires IBKR Mobile 2FA approval within the startup timeout.

`live` starts or reuses the Dockerized gateway; it does not use a manually started gateway or TWS. Pull the image first:

```sh
docker pull ghcr.io/gnzsnz/ib-gateway:stable
```

The paper and live containers use ports 4002 and 4001 respectively and remain running for reuse. The gateway login takes over an existing session of the same IB user; close any manual gateway occupying the port. Concurrent commands share the container, each waiting for its login, but the adapter replaces a container it finds created and not yet started, which can fail the command starting it at that moment: start the second command once the first's container is running. Distinct processes need distinct client IDs. The gateway API is not read-only because even its initial execution connection requires write access. After a gateway restart, periodic checks run but the node does not repeat full startup reconciliation.

## Trading and warmup

IB is netting. Its execution adapter denies reduce-only and post-only orders, so a trade on an instrument IB trades, GLBX's and OPRA's included, sends its bracket's exits neither reduce-only nor post-only and its time close not reduce-only, in a backtest as live (`strategy::orders`).

**Such exits lose the reduce-only guarantee**, as Nautilus' IB guide warns: IB cancels no exit when another order closes the position, so an exit filling after that opens a position the other way, which no trade's exits or time close end. The time close cancels the trade's exits and closes its position without waiting for the cancels, so an exit filling before its cancel lands leaves such a position, opened by the close or the exit, whichever fills second. On the netting account a trade's time close, or an entry on the other side, also closes the other trades in the position, whose exits stay working: the EMA crossover, whose trades overlap, meets this; a model strategy's trades do not overlap.

Live bars use full-session IB minute TRADES bars, matching history. Market orders can be priced from cached LAST bars; MID bars alone do not supply that price. Warmup ends at the request time, excluding the partial minute; the live subscription starts there. The data client does not write its historical answer back to the catalog.

IB counts a duration of days as sessions, the current one included, so a multi-day request can lose the oldest session. Warmup asks in windows under a day, which IB's client asks in seconds, and holds every bar of IB's history over them. IB's client reports a failed history request as an answer with what it got, so a window failing leaves a gap in the warmup with no failure. See [history](#history) for history limitations.

## Tests

Offline tests are in `src/venues/ib`. IB end-to-end tests in `tests/live/` require `NAUTILUS_IB_RUN=1` in the shell and a paper `NAUTILUS_IB_ACCOUNT_ID`; they start or reuse the Docker paper gateway. `ib_restart` also requires `NAUTILUS_IB_LIVE_ORDERS=1` and **places paper-account orders**; ensure the account and market are suitable and check the account afterwards. Run live-node tests in separate processes.

```sh
NAUTILUS_IB_RUN=1 cargo test --test live ib_gateway::
NAUTILUS_IB_RUN=1 cargo test --test live ib_minute_bars:: -- --nocapture
NAUTILUS_IB_RUN=1 cargo test --test live ib_warmup_days:: -- --nocapture
NAUTILUS_IB_RUN=1 NAUTILUS_IB_LIVE_ORDERS=1 cargo test --test live ib_restart:: -- --test-threads=1
```
