# Architecture

helix-trader is one Rust crate built on NautilusTrader. Nautilus handles
data, simulation, execution and the portfolio; this repo adds only what
Nautilus lacks.

## Rules

- **Check Nautilus first.** Look for a feature in Nautilus before writing
  it, and write our own only when Nautilus lacks it.
- **Plain Nautilus API.** A strategy is a struct holding a `StrategyCore`,
  wired with `nautilus_strategy!`. It implements `DataActor` (`on_start`,
  `on_bar`) and places orders with `submit_order`, as the Nautilus docs
  describe. No wrapper layer and no custom vocabulary.
- **One strategy at a time.** The repo can hold several interchangeable
  strategies, but a process runs one, which decides all its trades
  internally. No allocator, no sleeves.
- **Orders.** The EMA strategy decides on a bar's close and sends a
  bracket: a market entry with a stop loss and a take profit. The backtest
  checks stop and limit orders against each 1-minute bar's open, high, low
  and close (or each 1-minute quote), so the order of prices within a
  minute is guessed, and simulating them well needs tick quotes.
- **One cadence.** A strategy runs on one bar cadence, which can be
  intraday.
- **The strategy config is the universe.** Instruments and cadence live in
  the strategy's TOML config, the single source for fill-catalog, backtest
  and live. No universe type, no instruments in code.
- **One simulator.** Nautilus runs both backtest and live, and the same
  strategy type runs in backtest, paper and live.
- **One crate.** All code lives under `src/`.
- **Model files, one shared store.** A strategy trading on a learned
  model reads its models from files, never trains in the trading process:
  `train` publishes each model in the model store (one directory per
  strategy, fingerprint and training settings; one file per valid-from,
  written then renamed, with its manifest and probes). Backtest and live
  read the same store, each decision using the latest model valid then,
  checked against the strategy's features and probes before use; a live
  node retrains by starting `train` as a child process. A meta model
  (meta-labeling) is a second level of the same store, under its
  primary's directory, trained by the same `train` right after the
  primary; the config types allow no third. Nothing else passes from a
  backtest to live: no lock or provenance files.

## Version

The `nautilus-*` crates come from NautilusTrader's git repository, all
pinned to the `develop` commit `04f766b4d4` (2026-10-26, crate version
`0.67.0`, unreleased), because it has the fixes after `0.66.0` that OKX
needs (nautilus_trader#5339, #5337) and the historical request groups,
`on_data_response` and catalog write-back the warmup uses. Move back to
crates.io at the next release. Their sources are under
`~/.cargo/git/checkouts/nautilus_trader-*/04f766b/crates/`. An upgrade is
one deliberate change: release notes read, full test suite green.

## Build order

Build a thin slice; each step works end to end before the next starts.

1. **Data.** Load bars into a Nautilus catalog.
2. **Backtest.** Run a strategy over the catalog, including costs.
3. **Paper live.** Run the same strategy on a live node against a paper
   account.
4. **Real strategy.**

## Nautilus v2 constraints

- **One node per process.** Run only one `BacktestNode` or `LiveNode` in a
  process: the message bus is thread-local, the logger and the Tokio
  runtime are process-wide. The backtest tests are the exception: one
  binary (`tests/backtest/`) runs them in parallel, a node per test, each
  on its own test thread; the message bus and the actor and component
  registries are thread-local, and the logger, installed by the first node
  with the default config every test uses, is shared by the others; so is
  its clock, which each backtest sets to its own simulated time, so a log
  line there may carry another test's time. A live node also takes the
  process's SIGINT and installs the `tracing` subscriber, which only one
  per process can, so the live tests (`tests/live/`) run one module per
  process (`tests/live/main.rs`).
- **Nautilus owns the logger.** Creating a node installs the global `log`
  logger, and fails if another logger (`env_logger`, a `tracing`
  subscriber) was installed first. Code logs through the `log` macros and
  configures them with `LoggerConfig`.
- **Backtests through `BacktestNode`.** A backtest is a `BacktestRunConfig`
  run by a `BacktestNode`: one run per process, no parameter sweeps.

## Layout

```
Cargo.toml    crate manifest
build.rs      compiles Spotware's cTrader protos for src/data/ic/ (needs protoc),
              names the pinned source of Nautilus' indicators for src/model/
src/          crate code, unit tests included
tests/        integration tests: full runs through Nautilus, two binaries
  backtest/   the backtests, a node per test, in parallel
  live/       the gated runs on real venues, one module per process
  common/     their fixtures, included by both
docs/         project docs
```

Unit tests live in a `#[cfg(test)] mod tests` at the bottom of the module
they test. Tests check behaviour, not implementation.
