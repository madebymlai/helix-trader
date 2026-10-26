<p align="center">
  <img src="docs/assets/banner.svg" alt="helix trader" width="100%">
</p>

A systematic trading system in Rust, built on
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader). The same
strategy type runs in backtest, paper and live, on Interactive Brokers and
OKX and IC Markets (cTrader, a hedging account). Nautilus owns data, simulation, execution and the portfolio; this repo
adds only what Nautilus lacks.

## Commands

One binary with one subcommand per process, since Nautilus allows one node
per process. Each takes the strategy name and its TOML config.

| Command | Does |
|---|---|
| `fill-catalog` | Fills the Parquet catalog with the strategy's history: OKX's archive and API for OKX instruments, IC Markets' cTrader for IC instruments (tick quotes), Interactive Brokers for the rest. |
| `backtest` | Runs the strategy over a window of the catalog, with commissions, leverage and funding, and prints the results. |
| `live` | Runs the strategy on a live node against IB, OKX or IC Markets, until Ctrl-C, warmed up from the same catalog and the venues for the rest; IC's answers are written back to the catalog. |

## Quick start

Requires Rust 1.99+, `clang` and `mold` (the linker set in
`.cargo/config.toml`), `protoc` (for IC's cTrader messages), and Docker or
Podman for the IB Gateway and for the Redis that holds the live node's cache
(`docker pull docker.io/library/redis:7.4.11-alpine` once; see
`src/live/README.md`).

```sh
cp .env.example .env    # IB paper login, OKX API key, IC cTrader app
cargo build --release

cat > ema.toml <<'EOF'
instrument_ids = ["BTC-USDT-SWAP.OKX"]
trade_instrument_id = "BTC-USDT-SWAP.OKX"
bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
trade_size = "0.01"
stop_loss_bps = 100
take_profit_bps = 200
hold = "24h"
warmup = "720h"
EOF

target/release/helix-trader fill-catalog ema --config ema.toml \
  --start 2026-01-01 --end 2026-10-01
target/release/helix-trader backtest ema --config ema.toml \
  --start 2026-02-01 --end 2026-10-01 --commission-rate 0.0005 --balance "10000 USDT"
target/release/helix-trader live ema --config ema.toml
```

The config is the universe: instruments, cadence and warmup live there and
nowhere else. `--help` on each subcommand lists every flag.

## Tests

```sh
cargo test
```

Integration tests run full Nautilus nodes, one per test file. The tests
against real venues are skipped unless enabled: `NAUTILUS_IB_RUN=1` for the
IB paper gateway, `NAUTILUS_OKX_RUN=1` for the OKX demo account,
`NAUTILUS_IC_RUN=1` for IC's cTrader demo.

## Documentation

| Document | Covers |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design rules, the pinned Nautilus version, process-wide constraints |
| [src/data/README.md](src/data/README.md) | Filling the catalog; IB, IC and OKX history in `ib/`, `ic/` and `okx/` |
| [src/strategy/README.md](src/strategy/README.md) | Writing a strategy, its config, warmup |
| [src/backtest/README.md](src/backtest/README.md) | The backtest run, costs, fills, leverage, results |
| [src/live/README.md](src/live/README.md) | Live nodes; IB, IC and OKX clients in `ib/`, `ic/` and `okx/` |
| [docs/research/](docs/research/README.md) | Desk research on strategies, costs and brokers |

## Disclaimer

A personal research project, not financial advice. Trading carries risk of
loss; run live only what you have backtested and paper traded.
