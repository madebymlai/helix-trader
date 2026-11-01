# Architecture

Helix Trader is one Rust crate built on NautilusTrader. Nautilus owns data, simulation, execution and the portfolio; this repository adds what Nautilus lacks.

## Design rules

- Check Nautilus for a feature before implementing it here. Strategies use the plain Nautilus API, without a wrapper layer.
- One process runs one strategy, which decides its own trades. The strategy configuration defines instruments and cadence for catalog filling, backtesting and live operation.
- The same strategy type and Nautilus engine run in backtest, paper and live. See [strategy authoring](../src/strategy/README.md) and [backtesting](../src/backtest/README.md) for the differences in operation.
- Model strategies read published files rather than training in the trading process. Backtest and live use the same model store; live retrains in a child process. A strategy never retrained trains each model once and launches no train live (`a_live_handle_never_retrained_launches_no_train`); whether a strategy retrains is the schedule's alone (`model::schedule::Retrain`), so training, the backtest, its scores and live share one answer. See [model internals](../src/model/README.md) and [live operations](../src/live/README.md).

## Process constraints

Run one Nautilus node per process: its message bus is thread-local, while logging and the Tokio runtime are process-wide. Backtest integration tests run nodes on separate test threads with a shared logger and clock, so log timestamps may reflect another test's simulated time. Live nodes also install a tracing subscriber and take SIGINT; run live test modules in separate processes.

Nautilus owns the global logger. Code uses `log` macros and configures the node's logger rather than installing another logger first. Backtests use `BacktestNode`, one run per process, not parameter sweeps.

## Dependencies and tests

`Cargo.toml` is the source of truth for the Nautilus pin. Its git version supplies OKX fixes and historical request and catalog write-back support needed by warmup. Treat upgrades as deliberate changes: read release notes and run the full test suite. If an upgrade changes golden dataset rows, bump the dataset version; see [model guidance](../src/model/README.md).

Unit tests sit with the module they test; integration tests are under `tests/backtest/` and `tests/live/`. See the [root test guidance](../README.md) for venue gates and process isolation.
