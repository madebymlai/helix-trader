# Architecture

helix-trader is one Rust crate built on NautilusTrader. Nautilus handles
data, simulation, execution and the portfolio. This repo adds only what
Nautilus does not already provide.

## Rules

- **Check Nautilus first.** Look for it in Nautilus before writing it. Write
  our own only when Nautilus lacks it.
- **Plain Nautilus API.** A strategy is a struct holding a `StrategyCore`,
  wired with `nautilus_strategy!`. It implements `DataActor` (`on_start`,
  `on_bar`) and places orders with `submit_order`, as the Nautilus docs
  describe. No wrapper layer and no custom vocabulary.
- **One strategy at a time.** The repo can hold several interchangeable
  strategies, but a process runs one. That strategy computes the whole book
  internally and rebalances itself. No allocator, no sleeves.
- **Orders.** The EMA strategy decides on each bar's close and sends a
  market order to reach its target position. Other strategies may use
  limit or stop orders instead. The backtest checks those against each
  1-minute quote, so a price touched and left within the same minute is
  missed: simulating them well needs tick quotes.
- **One cadence.** A strategy runs on one bar cadence, and that cadence can
  be intraday.
- **The strategy config is the universe.** Instruments and cadence live in
  the strategy's TOML config, the single source for fill-catalog, backtest and
  live. No universe type, no instruments in code.
- **One simulator.** Nautilus runs both backtest and live. The same strategy
  type runs in backtest, paper and live.
- **One crate.** All code lives under `src/`.
- **No lock.** A backtest produces no lock or provenance files for live to
  consume.

## Version

The `nautilus-*` crates come from NautilusTrader's git repository, all
pinned to the `develop` commit `a51ae015c1` (2026-10-22, crate version
`0.67.0`, unreleased): it has the OKX fixes after `0.66.0` that OKX needs
(nautilus_trader#5339, #5337). Back to crates.io at the next release. Their
sources are under
`~/.cargo/git/checkouts/nautilus_trader-*/a51ae01/crates/`. Upgrades are
deliberate: one change, release notes read, full test suite green.

## Build order

Build a thin slice. Each step must work end to end before the next starts.

1. **Data.** Load bars into a Nautilus catalog.
2. **Backtest.** Run a strategy over the catalog, including costs.
3. **Paper live.** Run the same strategy on a live node against a paper
   account.
4. **Real strategy.**

## Nautilus v2 constraints

- **One node per process.** Run only one `BacktestNode` or `LiveNode` in a
  process, even across threads: the message bus is thread-local, the logger
  and the Tokio runtime are process-wide. Parallel backtests use separate
  processes; each integration test file is its own process, so one node per
  test file.
- **Nautilus owns the logger.** Creating a node installs the
  global `log` logger, and fails if another logger (`env_logger`, a
  `tracing` subscriber) was installed first. Code logs through the `log`
  macros; configuration is `LoggerConfig`.
- **Backtests through `BacktestNode`.** A backtest is a `BacktestRunConfig`
  run by a `BacktestNode`: one run per process, no parameter sweeps.
- **Continuous futures.** Nautilus stitches contracts but does not find the
  rolls. We detect the roll dates and pass them in as
  `continuous_future_transitions`.

## Layout

```
Cargo.toml    crate manifest
src/          crate code, unit tests included
tests/        integration tests: full runs through Nautilus
docs/         project docs
```

Unit tests live in a `#[cfg(test)] mod tests` at the bottom of the module
they test. Tests check behaviour, not implementation.
