# Running live

`helix-trader live` runs one strategy on a Nautilus live node, including for paper accounts. Run only one live module per process: Nautilus owns process-wide state and logging. Start with a paper account and review the venue restrictions before enabling orders: [IB](../venues/ib/README.md), [IC Markets](../venues/ic/README.md), [Pepperstone](../venues/pepperstone/README.md), [OKX](../venues/okx/README.md). General strategy configuration and warmup are in [strategy](../strategy/README.md); model internals are in [model](../model/README.md).

```sh
helix-trader live ema --config ema.toml
```

See `helix-trader live --help` and the maintained config for options. The command reads `.env` without overriding variables already in the environment; keep credentials out of code and git. It starts the IB gateway when needed and starts or reuses Redis. Stop with SIGINT or SIGTERM. Concurrent live processes need distinct trader IDs and, for IB, distinct client IDs; a client ID divisible by 1000 is refused. Processes trading one venue account also need distinct trader ID tags, the part after the final hyphen: Nautilus puts only the tag in client order, order list and position IDs, so `HELIX-001` and `OTHER-001` collide.

## Startup and routing

The node recovers its staging identity, validates venue configuration and accounts, prepares clients and starts Redis, then builds the node and promotes recovered staging data before strategy warmup. It registers the retrain actor of a strategy retrained (see [Retraining](#retraining)), then the strategy, and claims external orders for its instruments, then loads the cache, connects and reconciles before starting the trader. Reconciliation failure aborts startup; periodic open-order and position checks continue while running. A strategy claims the whole book for its instruments; two claims on one instrument conflict.

The instrument venue selects the broker, in the [routing table](../venues/mod.rs): OKX, IC and Pepperstone instruments route to their own clients; IB is the default for other venues. Positions follow the execution client's account mode: IB and OKX net, IC and Pepperstone hedge; backtests simulate the same modes. See the venue docs for account checks and order restrictions.

For a futures root, the live node loads the current and next contracts at startup and requests future contracts ahead of each roll. Live root bars are refused: the post-roll price is not yet known and backward-adjusting existing bars would change a running strategy's frame. Read contract bars or root ticks instead. GLBX history comes from Databento and live routing goes to IB; routing does not guarantee IB supports a particular instrument. See [instrument identity](../catalog/README.md#instrument-identity) for vendor IDs and [rolls](../rolls/README.md) for the calendar and transitions.

## Redis and reconciliation

Pull the image before the first live start; startup creates the container but does not pull it:

```sh
docker pull docker.io/library/redis:7.4.11-alpine
```

Set `DOCKER_HOST` when using a non-default Docker socket, including Podman. Redis binds port 6379 to loopback only, without a password.

`helix-redis` persists execution cache entries on the `helix-redis-data` Docker volume, keyed by trader ID across runs and venues. Market data and strategy state are not persisted there; strategy state is rebuilt by warmup. Closed entries are purged from memory, not Redis. Reconciliation can recover venue changes since the previous run, but an IB order rejected while the node was down may remain submitted if IB no longer reports it; a strategy waiting for it can stall.

To reset a trader's cache, stop its node and delete only that trader's keys, substituting its ID:

```sh
docker exec helix-redis redis-cli --scan --pattern 'trader-HELIX-001:*' \
  | xargs -r docker exec -i helix-redis redis-cli del
```

This destroys that trader's cached execution history across all venues, not its venue positions or orders, catalog, staging files, or other traders' keys. A full Redis flush destroys every trader's cache; do not use flush-on-start as a per-trader reset. Reconcile the actual account before resuming.

## Recording and recovery

The node streams market data to `--staging` outside the catalog, promotes it daily and on clean shutdown, and deletes each promoted staging file. Readers and retrains see data only through the last promotion. Built bars are stored as external bars; warmup data itself is not streamed. Catalog files accumulate. Do not consolidate them plainly: Nautilus' `consolidate_catalog` and `consolidate_data` merge a directory's files into one spanning the first start to the last end, across the slivers between them, which then count as covered and are never asked of the venue; the `_by_period` variants keep such holes.

Each trader's persistent instance ID is stored in staging. After a crash, restart with the same trader and staging directory: recovery seals complete batches and promotes them **before warmup**, so warmup write-back cannot overlap unrecovered data. An empty identity file fails startup rather than orphaning the old run. The last unflushed second can be lost. A staged interval overlapping catalog data fails promotion and remains staged; resolve the overlap before restarting. In particular, interval promotion can split OKX rows with the same timestamp across files, causing an overlap and a subsequent startup failure.

Catalog coverage is not proof of complete market data: promotion leaves small interval gaps, downtime leaves gaps, and a disconnect within one file may look covered. Warmup asks the venue for missing coverage, and certified cTrader and OKX responses can be written back; IB warmup responses are not written back. Backtests and retrains read stored rows through gaps, not a guarantee of completeness. See [catalog](../catalog/README.md) for catalog operations.

## Retraining

A model strategy requires retraining costs and balance at live startup, retrained or not: they are the costs of its dataset runs and key the model store it reads; see `live --help`.

A retrained strategy has a Nautilus actor added before the strategy, named `RETRAIN-<strategy id>`. Its own clock fires on the epoch-aligned retrain grid and launches `train` in a child process, since another Nautilus node cannot run simultaneously in the live process. Training cuts data off at the earliest last stored timestamp across required series, capped at the scheduled time. Missing series or no advance since the preceding retrain skips the run with a warning. Only one retrain per strategy runs at a time; an overlapping firing is skipped with a warning, and a finished retrain makes way for the next whether it published or kept the current model.

The retrain logs its outcome on finishing, without waiting for a decision: info when models were published or are in use, error with the reason when it kept the current model. The next decision lists the store again and selects published models only when valid at that decision time. A child still running at shutdown finishes independently. Coordinate writers: do not run concurrent training writers against the same model store.

Without `retrain` in `[model]`, a strategy has no retrain actor, timer or launch. It trades on the store as it is, picking up externally published models at the next decision; the usual bar-build delay still applies. See [model](../model/README.md#store-validity-and-versions) for validity and store contracts, [strategy](../strategy/README.md#bars-and-sessions) for bar delay, and [backtest](../backtest/README.md#model-retraining) for walk-forward operation.

A real-venue live retrain has not been verified; replay across streamed gaps and crash recovery have offline coverage, not a general guarantee of venue behavior.

## Tests

Offline live tests are under `tests/live/`. Venue end-to-end tests have explicit environment gates and may place real orders even on a demo or paper account; read the venue docs and test before enabling them. Run live-node tests in separate processes because of Nautilus process-wide state.
