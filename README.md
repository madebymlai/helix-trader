<p align="center">
  <img src="docs/assets/banner.svg" alt="helix trader" width="100%">
</p>

# Helix trader

A systematic trading system in Rust built on [NautilusTrader](https://github.com/nautechsystems/nautilus_trader). The same strategy type runs in backtest, paper and live on Interactive Brokers, OKX and IC Markets (cTrader hedging account).

## Quick start

The build requires Rust (minimum in [Cargo.toml](Cargo.toml)), clang, cmake, mold and protoc; the linker is set in [.cargo/config.toml](.cargo/config.toml). IB Gateway and the live cache also require Docker or Podman.

The maintained example uses IC Markets. Follow the [IC Markets setup](src/venues/ic/README.md) and create `.env` from [.env.example](.env.example) before filling its catalog.

```sh
cargo build --release
target/release/helix-trader fill-catalog ema --config config/ema.toml --start 2026-01-01 --end 2026-10-01
target/release/helix-trader backtest ema --config config/ema.toml --start 2026-02-01 --end 2026-10-01 --commission-rate 0.00003 --balance "10000 USD"
```

Check the config and venue requirements when changing instruments. Before running `live`, follow [live setup and the relevant venue guide](src/live/README.md), including account mode and credentials. Live trading can place real orders; use a demo account first. Each command's `--help` lists its options.

`fill-catalog` collects the configured history; `backtest` replays it; `live` runs a node until stopped; `train` publishes models for model strategies. The strategy config is the universe shared by these commands.

## Tests

```sh
cargo test
```

`cargo test` runs the harness: the strategies' own unit tests (ema, model_ema and, where it exists, gold_scalp) are behind the `strategy-tests` feature, so `cargo test --features strategy-tests` runs them too. The integration tests that run a strategy, such as the golden rows, run either way, and the strategies' code builds either way. Lint with `cargo clippy --all-targets --all-features`, which covers those tests.

Real-venue live tests are gated by `NAUTILUS_IB_RUN=1`, `NAUTILUS_OKX_RUN=1`, or `NAUTILUS_IC_RUN=1`. Run one live module per process, for example `NAUTILUS_IC_RUN=1 cargo test --test live ic_demo:: -- --nocapture`. Read the relevant [live](src/live/README.md) and venue documentation first: tests can place orders or rotate tokens.

## Documentation

- [Architecture](docs/ARCHITECTURE.md): design and process constraints.
- [Catalog](src/catalog/README.md): catalog filling; each venue's history and live clients are documented in its module under `src/venues`.
- [cTrader](crates/ctrader/README.md): the cTrader Open API adapter, a crate of the workspace, which IC Markets runs on.
- [Strategy](src/strategy/README.md): strategy authoring and configuration.
- [Model](src/model/README.md): model strategy internals.
- [Backtest](src/backtest/README.md): replay, costs and limitations.
- [Live](src/live/README.md): node operations and venue setup.
- [Rolls](src/rolls/README.md): futures calendar and continuous bars.

## Disclaimer

A personal research project, not financial advice. Trading carries risk of loss; run live only what you have backtested and paper traded.
