# Helix Trader 0.2.0

Released on TBD (UTC).

### Enhancements

- Added `train --from --to`, training a periodic strategy at each retrain time of a span in time order, past failures, which it lists at the end
- Added a live retrain at the start of a node of the latest retrain time whose generation the store lacks, as after the node was down then
- Added the output of a live retrain's `train` to the node's Nautilus log, each line a log record, its file holding the train's whole output
- Added run figures to `train`'s report, a block per retrain time: the generation published, its directory, or the failure and its reason, the wall time, the CPU time and peak memory of the train process (Unix), the dataset days built and cached, and the rows of each fit and calibration slice of the sides, the ancestor and the meta beside `min_rows`

### Breaking Changes

- Removed the holdout gate: a train publishes its models unless a fit or calibration slice has fewer rows than `min_rows`, a prediction or calibration is not finite, or a model does not reproduce its probes; judge a config's models by a backtest's scores
- Renamed `[model] holdout` and `[model.meta] holdout` to `calibration`, the last part of the window the models are calibrated on and not fitted on; rename the key, and keep `calibration` plus the target's limit shorter than the window
- Removed `min_trades` and `min_expectancy` from `[model]` and `[model.meta]`, each refused at load as unknown; drop them
- Changed `[model]` and `[model.meta]` to require `min_rows`, the fewest rows of each fit and calibration slice; set it to at least 1
- Changed the model store's version, so every model is trained again; datasets keep their keys
- Changed `backtest` of a model strategy to train nothing, refusing a store with no model to trade on at its start and naming the `train` that fills it; run `train` over the backtest's span first
- Changed `train`'s report of a model it does not train again to "already published"; match that text instead of "in use"
- Changed `train`'s report to one block per retrain time opening `the train at <t>:`, the generation published read as `published the generation <directory>` and a failure as `failed: <reason>`, in place of a line per model; match the block's first line
- Changed `Trained` to one `Outcome` of the generation and the figures it `Reached`, `Outcome::Published` holding the generation's directory, `ModelStore::publish` returning it and `Dataset::settled` returning the days it read beside its rows; read `trained.outcome` in place of each model's
- Removed `WalkForward::prepare`, `Schedule::ensure_trades` and `Plan::fits`; train at each of `Training::retrain_times` with `Training::train_at`, and check a backtest's start with `WalkForward::untrained`
- Removed `DecidingModels::pick`; decide with `DecidingModels::decide`, or pick among a generation's pair with `DecidingModels::pick_among`
- Removed `Models::iter`, `Models::as_ref` and `Models::map`, which nothing called; read the `long`, `short` and `meta` fields
- Renamed the module `model::holdout` to `model::earnings`, what decisions took and earned on rows, there being no holdout; import `Earnings`, `Trades`, `Trade`, `MetaMargin` and `STRESS` from `model::earnings`
- Removed the `fake` learner from builds without the new `test-support` feature: a config with `learner = { kind = "fake" }` no longer loads; use `baseline`, `lightgbm` or `perpetual`
- Removed the `heartbeat` argument of `ctrader`'s `CTraderWebSocketClient::connect` and `CTraderSession::connect_to`; drop it, the session heartbeats every 10 s
- Removed the `keepalive_secs` argument of `lse`'s `LseWebSocketClient::connect`; drop it, the client pings every 25 s
- Removed `Deserialize` from `TickSeries`; read tick series through a strategy config's `[data]` series
- Removed `Launcher::in_process` outside the crate's own tests
- Changed the model store to one generation per train: a train publishes its sides, their pair's calibration and its meta together under its retrain time, a manifest written last making it visible, keyed by every training setting of the sides and the meta, and a decision uses the latest generation's sides and meta together; train again, the store's version changed
- Changed a newest generation failing its load checks to stop new entries with an error naming it and the check, read again at the next decision, instead of keeping the previous models; remove its `<valid from>/` directory and train it again, a train skipping a generation already published at its time
- Renamed `ModelSet` to `ModelStore` and `ModelBlueprint::model_set` to `model_store`, the store of a strategy's generations; publish, list, read and swap them with `ModelStore::publish`, `generations`, `read` and `swap` in place of the per-side `ModelStore`, `ModelSet::pairs`, `ModelSet::valid_froms` and `MetaLevel::swap`
- Removed `PairCalibration`, `ModelCalibration` and `ModelManifest`'s `strategy_id` and `valid_from`: a side's manifest holds no calibration, its pair's being `StoredGeneration::calibration`, and a meta's holds its `Line`
- Removed `Step` and `Plan::meta_after_sides` from the schedule; read a train's plan as `Plan::Keep` or `Plan::Train` of `Fits`
- Changed a meta model to learn from the out-of-sample picks of an ancestor pair its train fits a meta window before the retrain time, shipping beside sides fitted at that time, instead of from the decisions of the generations in the store, so a periodic strategy with a meta publishes its meta from its first train and a never-retrained one ships sides fresh at its train time; train again, the store's version changed
- Removed `Kept::Waiting` and `MetaStep`: every generation of a strategy with a meta holds one, and `ModelStore::publish` refuses one without; read the ancestor's fit as `Fits::ancestor` and its time as `ModelManifest::ancestor`
- Changed a never-retrained `train --at t` to train a generation at `t` unless one is already published there, as a periodic train does, instead of keeping the latest generation at or before `t` and reporting it "in use since"; an older generation still decides before `t`, so train a never-retrained strategy only at the time it is to trade from
- Renamed `Outcome::InUse` to `Outcome::AlreadyPublished`, holding no time as `Plan::Keep` holds none, the train's time being the generation's, and removed `Retrain::in_use`: a generation not trained again reads "already published" in both modes; match `Outcome::AlreadyPublished`
- Renamed `Kept`, of failures alone, to `Failure`, its `Failed` to `Error`, `Outcome::Kept` to `Outcome::Failed` and `Retrained::Kept` to `Retrained::Failed`; match the new names
- Changed a live retrain to train at its retrain time, its generation stamped and its data cut there as `train --at` does, once every series the strategy reads has a stored point at or after that time, instead of at once at the catalog's last stored time; expect it after the first daily promotion past the retrain time, the previous generation deciding meanwhile
- Changed a live retrain due while another waits for its data to take its place, the waiting one skipped with a warning; train a skipped time with `train --from --to` if a backtest needs it
- Changed `RetrainActor::new` to take the strategy's `ModelStore`, whose generations it lists at its start; pass the store the strategy reads

### Security

- Fixed a crash or power loss during a publish leaving a generation listed whose model files are cut short: a publish syncs its models' files to disk before it writes the manifest

### Fixes

- Fixed a meta model deciding beside the sides of another train, as when a later train's meta waited or failed
- Fixed a train whose meta failed, or that stopped between its sides' publishes, leaving its sides published without the rest of its models

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
