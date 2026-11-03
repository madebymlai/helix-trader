# Architecture

Helix Trader is a Rust workspace built on NautilusTrader: the main crate, `helix-trader` at the root, and the cTrader adapter, `ctrader` in `crates/ctrader`, a Nautilus adapter for Spotware's cTrader Open API. Nautilus owns data, simulation, execution and the portfolio; this repository adds what Nautilus lacks.

## Design rules

- Check Nautilus for a feature before implementing it here. Strategies use the plain Nautilus API, without a wrapper layer.
- Venue code lives in `src/venues`, one module per venue holding its history and its live clients. One routing table, at the root of `src/venues`, says which history source fills an instrument and which broker trades it live. The catalog fill machinery (`src/catalog`) and the live runner (`src/live`) name no venue: they ask the venues module. The cTrader crate names no broker and depends on no helix code: another cTrader broker is a row of the routing table and a venues module configuring the crate, as `src/venues/ic` does for IC Markets.
- One process runs one strategy, which decides its own trades. The strategy configuration defines instruments and cadence for catalog filling, backtesting and live operation.
- The same strategy type and Nautilus engine run in backtest, paper and live. See [strategy authoring](../src/strategy/README.md) and [backtesting](../src/backtest/README.md) for the differences in operation.
- Model strategies read published files rather than training in the trading process. Backtest and live use the same model store; live retrains in a child process. A strategy never retrained trains each model once and, live, has no retrain actor, so launches no train (`a_strategy_never_retrained_has_no_retrain_actor_and_a_retrained_one_has_one`); whether a strategy retrains is the schedule's alone (`model::schedule::Retrain`), so training, the backtest, its scores and live share one answer. See [model internals](../src/model/README.md) and [live operations](../src/live/README.md).

## Process constraints

Run one Nautilus node per process: its message bus is thread-local, while logging and the Tokio runtime are process-wide. Backtest integration tests run nodes on separate test threads with a shared logger and clock, so log timestamps may reflect another test's simulated time. Live nodes also install a tracing subscriber and take SIGINT; run live test modules in separate processes.

Nautilus owns the global logger. Code uses `log` macros and configures the node's logger rather than installing another logger first. Backtests use `BacktestNode`, one run per process, not parameter sweeps.

## Dependencies and tests

The root `Cargo.toml` is the source of truth for the Nautilus pin, one for the workspace (`[workspace.dependencies]`). Its git version supplies OKX fixes and historical request and catalog write-back support needed by warmup. Treat upgrades as deliberate changes: read release notes and run the full test suite. If an upgrade changes golden dataset rows, bump the dataset version; see [model guidance](../src/model/README.md).

Unit tests sit with the module they test; integration tests are under `tests/backtest/` and `tests/live/`. `cargo test` and `cargo clippy` at the root cover both crates; `cargo test -p ctrader` runs the adapter's tests alone. See the [root test guidance](../README.md) for venue gates and process isolation.
