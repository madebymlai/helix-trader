# Helix Trader 0.2.0

Released on TBD (UTC).

### Enhancements

- Added `train --from --to`, training a periodic strategy at each retrain time of a span in time order, past failures, which it lists at the end

### Breaking Changes

- Removed the holdout gate: a train publishes its models unless a fit or calibration slice has fewer rows than `min_rows`, a prediction or calibration is not finite, or a model does not reproduce its probes; judge a config's models by a backtest's scores
- Renamed `[model] holdout` and `[model.meta] holdout` to `calibration`, the last part of the window the models are calibrated on and not fitted on; rename the key, and keep `calibration` plus the target's limit shorter than the window
- Removed `min_trades` and `min_expectancy` from `[model]` and `[model.meta]`, each refused at load as unknown; drop them
- Changed `[model]` and `[model.meta]` to require `min_rows`, the fewest rows of each fit and calibration slice; set it to at least 1
- Changed the model store's version, so every model is trained again; datasets keep their keys
- Changed `backtest` of a model strategy to train nothing, refusing a store with no model to trade on at its start and naming the `train` that fills it; run `train` over the backtest's span first
- Changed `train`'s report of a model it does not train again to "already published"; match that text instead of "in use"
- Removed `WalkForward::prepare`, `Schedule::ensure_trades` and `Plan::fits`; train at each of `Training::retrain_times` with `Training::train_at`, and check a backtest's start with `WalkForward::untrained`
- Removed the `fake` learner from builds without the new `test-support` feature: a config with `learner = { kind = "fake" }` no longer loads; use `baseline`, `lightgbm` or `perpetual`
- Removed the `heartbeat` argument of `ctrader`'s `CTraderWebSocketClient::connect` and `CTraderSession::connect_to`; drop it, the session heartbeats every 10 s
- Removed the `keepalive_secs` argument of `lse`'s `LseWebSocketClient::connect`; drop it, the client pings every 25 s
- Removed `Deserialize` from `TickSeries`; read tick series through a strategy config's `[data]` series
- Removed `Launcher::in_process` outside the crate's own tests

### Internal Improvements

- Improved the tests to `docs/testing.md`'s standard: same-shape tests parametrized with `#[case]`, duplicates and tests of library logic removed, messages asserted by the facts they carry, waits bounded, expected values worked by hand, and each change checked by a mutation
- Added binary tests of the command line's refusals (`tests/backtest/cli.rs`) in place of in-process parses
- Changed the backtest fee model and the live recording to set Nautilus' `charge_commission_once` and `promote_on_close` explicitly rather than rely on their defaults
- Removed `lse`'s `LseHttpClient::with_retry`, a test hook

### Documentation Updates

- Changed the release steps to open the next release's version in `Cargo.toml` with its section, as Nautilus does

---

# Helix Trader 0.1.0

Released on 2026-11-16 (UTC).

### Breaking Changes

- Changed `ModelHandle::new` to take `&ModelBlueprint` and `&Mode`; pass references
- Removed `venues::bracket_exits_tag`: IB links a bracket's exits in its own OCA group, so they carry Nautilus' tags alone; drop its calls
- Changed `ctrader` to deny a time in force cTrader does not honor: FOK, a stop market's IOC, a market order's `AT_THE_OPEN` or `AT_THE_CLOSE`, and bracket legs other than GTC; send market orders GTC, IOC or DAY, limits GTC, GTD or IOC, stop markets GTC or GTD, and legs GTC

### Fixes

- Fixed `ctrader` failing to build without its `test-support` feature; `TokenPair` and `SpotEvents` are public whatever the features
- Fixed `ctrader` reporting an order refused at execution as cancelled: an order cancelled unfilled with its deal rejected is rejected, with the deal's status and `rejectReason`
- Fixed `ctrader` leaving a limit IOC working as a GTC order: it goes as a `MARKET_RANGE` based at its price with no slippage, filled at the price or better or rejected

### Internal Improvements

- Added Nautilus' rustfmt, taplo, markdownlint, yamllint and spelling standards, vendored from `nautilus_engineering`, and the pre-commit hooks that run them
- Added Nautilus' workspace clippy lints and a clippy hook denying warnings
- Added `clippy.toml` bans on wall-clock reads, raw tokio spawns and raw sockets, pointing to Nautilus' clock, runtime and `nautilus_network`, tests included
- Added the `check-workarounds` hook: each workaround cites the `nautilus_trader` or `helix-trader` issue that retires it
- Added CI: the hooks, clippy and the tests on every push and pull request, and a weekly run against Nautilus' `develop`, the workarounds' issues and `nautilus_engineering`
- Added `RELEASES.md` and the release workflow that tags and publishes it
- Added a pull request template: summary, type of change, migration and the Nautilus-native, tests and release-notes checklist
- Added `cargo doc` with warnings denied, `cargo-machete` and a 3-day crate release cooldown to the hooks and CI
- Added Nautilus' commit message check: a capitalized subject of 60 characters at most, a body wrapped at 79
- Added `cargo-deny` with Nautilus' policy (advisories, licenses, duplicate crates, sources) to the hooks, CI and the weekly run
- Added the offline check that internal markdown links resolve (lychee)
- Changed CI to run the tests with `cargo-nextest`, a hung test failing after 10 minutes
- Added Nautilus' convention checks to the hooks: tests, `anyhow`, tokio, error and logging use, `std::fmt` and Nautilus imports, Rust formatting, `# Errors` and `# Panics` docs, `Cargo.toml` layout, hidden characters, Unicode typography and non-Latin text
- Added actionlint and yamlfmt to the hooks
- Changed every test to `#[rstest]`, as Nautilus writes its tests
- Changed the crates to share one version, in `[workspace.package]`
- Changed the pull request checklist to run the hooks against `develop`, as CI does, so the checks of changed lines read the branch's changes
- Changed the pull request checklist to ask for tests as `docs/testing.md` sets them
- Renamed the cTrader, Databento and OKX fixture folders `testdata/` to `test_data/`, as Nautilus and the LSE crate name them
- Changed the tests that pin a library behavior helix relies on into drift tests, `drift_*`, each citing what it pins, and removed the assertions that only re-proved a library

### Documentation Updates

- Added OKX's denial of a bracket to the OKX guide, until nautilus_trader#5668
- Renamed `docs/ARCHITECTURE.md` to `docs/architecture.md`
- Added `docs/releases.md`, the release notes standard and release steps
- Changed spelling to American English
- Changed the `# Errors` and `# Panics` sections to Nautilus' wording
- Added each crate's feature flags to its README
- Added `docs/testing.md`, Nautilus' testing standard adapted to helix: its testing policy, layers and conventions, and helix's seams from the binary's commands to the live tests
- Documented the LSE book stream's panic on a non-book series and the target exits' error on a non-finite ATR

---

<!-- markdownlint-configure-file { "MD001": false, "MD025": false } -->
