# Running live

`helix-trader live` runs one strategy on a Nautilus live node, including for paper accounts. Run only one live module per process: Nautilus owns process-wide state and logging. Start with a paper account and review the venue restrictions before enabling orders: [IB](../venues/ib/README.md), [IC Markets](../venues/ic/README.md), [OKX](../venues/okx/README.md). General strategy configuration and warmup are in [strategy](../strategy/README.md); model internals are in [model](../model/README.md).

```sh
helix-trader live ema --config ema.toml
```

See `helix-trader live --help` and the maintained config for options. The command reads `.env` without overriding variables already in the environment; keep credentials out of code and git. It starts the IB gateway when needed and starts or reuses Redis. Stop with SIGINT or SIGTERM. Concurrent live processes need distinct trader IDs and, for IB, distinct client IDs; a client ID divisible by 1000 is refused.

## Startup and routing

The node recovers its staging identity, validates venue configuration and accounts, prepares clients and starts Redis, then builds the node and promotes recovered staging data before strategy warmup. It registers the retrain actor of a strategy retrained (see [Retraining](#retraining)), then the strategy, and claims external orders for its instruments, then loads the cache, connects and reconciles before starting the trader. Reconciliation failure aborts startup; periodic open-order and position checks continue while running. A strategy claims the whole book for its instruments; two claims on one instrument conflict.

The instrument venue selects the broker, in the [routing table](../venues/mod.rs): OKX and IC instruments route to their own clients; IB is the default for other venues. Positions follow the execution client's account mode: IB and OKX net, IC hedges. See the venue docs for account checks and order restrictions.

For a futures root, the live node loads the current and next contracts at startup and requests future contracts ahead of each roll. Live root bars are refused: the post-roll price is not yet known and backward-adjusting existing bars would change a running strategy's frame. Read contract bars or root ticks instead. GLBX has no live client yet; IB uses CME for these futures. See [rolls](../rolls/README.md) for the calendar and transitions.

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

The node streams market data to `--staging` outside the catalog, promotes it daily and on clean shutdown, and deletes each promoted staging file. Readers and retrains see data only through the last promotion. Built bars are stored as external bars; warmup data itself is not streamed. Catalog files accumulate and may need consolidation.

Each trader's persistent instance ID is stored in staging. After a crash, restart with the same trader and staging directory: recovery seals complete batches and promotes them **before warmup**, so warmup write-back cannot overlap unrecovered data. An empty identity file fails startup rather than orphaning the old run. The last unflushed second can be lost. A staged interval overlapping catalog data fails promotion and remains staged; resolve the overlap before restarting. In particular, interval promotion can split OKX rows with the same timestamp across files, causing an overlap and a subsequent startup failure.

Catalog coverage is not proof of complete market data: promotion leaves small interval gaps, downtime leaves gaps, and a disconnect within one file may look covered. Warmup asks the venue for missing coverage, and certified IC and OKX responses can be written back; IB warmup responses are not written back. Backtests and retrains read stored rows through gaps, not a guarantee of completeness. See [catalog](../catalog/README.md) for catalog operations.

## Retraining

A model strategy requires retraining costs and balance at live startup, retrained or not: they are the costs of the dataset runs its models are trained on, and they key the model store it reads (`live_without_its_dataset_costs_is_refused_saying_they_key_the_model_store`); see `live --help`. A retrained strategy's retrains are a Nautilus actor of the node's, added before the strategy and named after it, `RETRAIN-<strategy id>`, so that a node holds one per strategy retrained; its timer, on the actor's own clock, fires on the epoch-aligned retrain grid (`the_retrain_timer_is_the_actors_on_the_grid`), and each firing, which Nautilus hands the actor, launches `train` (`an_added_retrain_actor_trains_at_the_grid_time_its_clock_fires`) in a child process, because a second Nautilus node cannot run in the live process. At a scheduled time it cuts training data off at the earliest last stored timestamp across required series, capped at the scheduled time; missing series or no advance since the preceding retrain skips the run, with a warning. Only one retrain per strategy runs at a time: a firing while the previous retrain runs is skipped with a warning, and one that finished, its models published or the current one kept, makes way for the next (`a_retrain_due_while_one_runs_is_skipped`, `a_finished_retrain_makes_way_for_the_next`, `a_retrain_keeping_the_model_makes_way_for_the_next`). A retrain logs its outcome as it ends, waiting for no decision: as info when its models were published or are in use, as an error with the reason when it kept the current model (`a_finished_retrain_logs_its_outcome_as_it_ends`). Published models are selected only when valid at the decision time, from the strategy's next decision, which lists the store again (`a_model_published_while_live_is_used_from_the_next_decision`); a child still running at shutdown finishes independently. Coordinate writers: do not run concurrent training writers against the same model store. A strategy never retrained, without `retrain` in `[model]`, has no retrain actor, so it sets no retrain timer and launches no train; it trades on the store as it is, picking up a model `train --at` publishes at its next decision, and keeps the bar-build delay (`a_strategy_never_retrained_has_no_retrain_actor_and_a_retrained_one_has_one`, `a_live_handle_never_retrained_waits_for_composite_bars`). See [model](../model/README.md) for validity and store contracts and [backtest](../backtest/README.md) for walk-forward commands.

A real-venue live retrain has not been verified; replay across streamed gaps and crash recovery have offline coverage, not a general guarantee of venue behavior.

## Tests

Offline live tests are under `tests/live/`. Venue end-to-end tests have explicit environment gates and may place real orders even on a demo or paper account; read the venue docs and test before enabling them. Run live-node tests in separate processes because of Nautilus process-wide state.
