# Architecture

Helix Trader is a Rust workspace on NautilusTrader: the main crate, `helix-trader`, at the root, and two Nautilus adapters, `ctrader` in `crates/ctrader` for Spotware's cTrader Open API and `lse` in `crates/lse`, a data client for London Strategic Edge's market data. Nautilus owns data, simulation, execution, the portfolio and the catalog, filling it from historical requests and charging a backtest's fees; helix builds its trading system on Nautilus and takes from Nautilus everything Nautilus provides, through its plain API, adding the strategies and their model harness, the roll calendar, the venue routing table, and the commands that drive Nautilus from a strategy's config.

## Rules

Each rule names the module that owns it; that module's doc comment and README hold the details.

- Nautilus is used with no wrapper layer, in tests as in production code. Code that bypasses Nautilus for what it owns is agreed first and cites the issue that retires it, as `.pre-commit-hooks/check_workarounds.py` checks.
- The strategy config is the universe. `fill-catalog`, `backtest`, `live` and `train` each take a strategy and its TOML and read the instruments, series, warmup and cadence from it (`strategy::requirements::DataRequirements`); no command takes instruments of its own.
- The catalog is the store between the commands: `fill-catalog` asks Nautilus' data engine for the config's history, which the engine writes back (`update_catalog`), a backtest replays what it holds, and a live warmup reads it first and writes back what the venues' clients certify ([catalog](../src/catalog/README.md), [warmup](../src/strategy/README.md#warmup)).
- One process runs one strategy, which decides its own trades. The same strategy type and Nautilus engine run in backtest, paper and live; [backtest](../src/backtest/README.md) and [live](../src/live/README.md) give the differences in operation.
- Strategies sit behind Nautilus' `Strategy` trait. Backtest, live and `main` are generic over it and name no concrete strategy; a strategy is a folder, `src/strategy/<name>/`, that `build.rs` finds, and strategies share config structs and functions, never a base type ([strategy authoring](../src/strategy/README.md)).
- Venue knowledge lives in the routing table, [`src/venues/mod.rs`](../src/venues/mod.rs). A caller asks a venue's row, or a custom data type's `CustomRow`, for the part it needs (definitions, history, live data, broker), and catalog, live, backtest, order and strategy code name no venue. A broker is added as a row and a small venue module, with no new variant or branch.
- No trait stands for a venue: Nautilus' factories, configs and `RoutingConfig` are the seam, and the venues' needs differ too much for a trait to hide.
- Broker capabilities, such as position mode and order flags, are answered by the routing table, because Nautilus exposes position mode only on a built execution client and does not expose its order-flag restrictions.
- Futures roots resolve in [`src/rolls`](../src/rolls/README.md): an id the roll calendar lists as a root rolls through its contracts, and callers take its slices rather than branch on whether an id is a root.
- Models are files. `train` publishes model sets to the model store; a backtest and a live strategy read them and train nothing as they trade, and live retrains in a child process ([model scheduling](../src/model/README.md#scheduling), [live retraining](../src/live/README.md#retraining)).
- The adapter crates depend on no helix code. Helix's wiring of them lives in `src/venues`, so another cTrader broker needs no change to `crates/ctrader`.

## Process constraints

Run no two Nautilus nodes of a process at once: a node's message bus is thread-local, while logging and the Tokio runtime are process-wide. A process may run backtest nodes one after another, each disposed before the next starts, as Nautilus documents: a backtest of a model strategy builds its dataset and then runs this way, and a train over a span replays the days each of its trains lacks this way. A live node is the one node of its process while it runs, so its retrains are child processes.

Backtest integration tests run their nodes at once, one per test thread, which Nautilus does not support: each thread has its own message bus and registries, but the logger, with the logging mode and clock each backtest switches, the logging worker and the Tokio runtime are the process's, so log timestamps may reflect another test's simulated time. Live nodes also install a tracing subscriber and take SIGINT; run live test modules in separate processes.

Nautilus owns the global logger, and code logs through `log` macros. A backtest node installs Nautilus' logger from the config it is given; the live run and fill-catalog install it themselves (`init_logging`) before any node, since the live clients are set up before the node is built and a fill may run no node, and a node built afterwards keeps it. No other logger is installed. Each backtest run is a fresh `BacktestNode`, not a parameter sweep.

## Dependencies and tests

The root `Cargo.toml` is the source of truth for the Nautilus pin, one for the workspace (`[workspace.dependencies]`). Its git version supplies OKX fixes and historical request and catalog write-back support needed by warmup. Treat upgrades as deliberate changes: read release notes and run the full test suite. If an upgrade changes golden dataset rows, bump the dataset version; see [model guidance](../src/model/README.md).

Each kind of test, where it lives and what it must prove are in [testing](testing.md). `cargo test` and `cargo clippy` at the root cover every crate; `cargo test -p ctrader` and `cargo test -p lse` run an adapter's tests alone. See the [root test guidance](../README.md) for venue gates and process isolation.
