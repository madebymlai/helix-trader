# Helix Trader 0.2.0

Released on TBD (UTC).

### Enhancements

- Added `bar_interval` to model target ATR tables for sizing trades on bars other than the decision cadence
- Added overlapping model dataset targets while executable model entries wait for the previous trade to clear
- Added `train --from --to`, training a periodic strategy at each retrain time of a span and listing the times that failed
- Added a live retrain at a node's start of the latest retrain time whose model set the store lacks
- Added a live retrain's `train` output to the node's Nautilus log, a log record per line
- Added run figures to `train`'s report per retrain time: wall and CPU time, peak memory, dataset days and each fit and calibration slice's rows
- Added features on any list of series, any kinds, instruments and futures roots, each update reaching a feature once, in the order it comes
- Added a live warmup handing features their series' history in time order as a replay does, built bars included, ending incomplete past 2 GiB or out of order

### Breaking Changes

- Removed `Backtest::venue_oms`: `run` uses the broker's position mode and `run_dataset` hedges on every venue; omit the field from Rust struct literals
- Changed the Rust target ATR fields to `Option<TargetAtrConfig>`; convert existing `AtrArgs` with `.into()` when setting them in struct literals
- Changed LSE WebSocket option-chain subscriptions to count references; match each successful `subscribe_options` call with one `unsubscribe_options` call
- Changed `SeriesSlice` to hold `CatalogSeries`, `from` and `to`; use `series.instrument_id()` for the contract and `series.key()` for the catalog key
- Removed `FillRequest::instrument_id`; pass the `InstrumentId` to non-bar `FillData` variants and use `request.data.instrument_id()` to read the target
- Changed `fill_catalog`, its fetch callback and `IbHistory::data` to use `CatalogSeries`; pass a slice to the writer and a series reference to each fetch
- Removed the holdout gate: a train fails only on `min_rows`, a non-finite output or a failed probe; judge a config's models by a backtest's scores
- Renamed `[model] holdout` and `[model.meta] holdout` to `calibration`, the window's last part, calibrated on and not fitted on; rename the key
- Removed `min_trades` and `min_expectancy` from `[model]` and `[model.meta]`; drop them
- Changed `[model]` and `[model.meta]` to require `min_rows`, the fewest rows of each fit and calibration slice; set it to at least 1
- Changed the model store to one model set per train, its sides, their calibration and its meta published together at its retrain time; train again
- Changed `backtest` of a model strategy to train nothing, refusing a store with no model set at its start; run `train` over the backtest's span first
- Changed `train`'s report to a block per retrain time, opening `the train at <t>:`, the set published, already published or failed; match the first line
- Changed a meta model to learn from an out-of-fold pair fitted a meta window before the retrain time, so a periodic strategy's meta ships from its first train
- Changed a newest model set failing its load checks to stop new entries, naming it, until it loads; remove its directory and train it again
- Changed a never-retrained `train --at t` to train a model set at `t` unless one is published there; train it only at the time it is to trade from
- Changed live retrains to wait for a catalog interval end at or after the cutoff for each bar source and tick series, skipping one due while another runs
- Changed `RetrainActor::new` to take the strategy's `ModelStore`; pass the store the strategy reads
- Renamed the store of a strategy's model sets to `ModelStore` and `ModelBlueprint::model_set` to `model_store`, `ModelSet` now naming one train's models
- Renamed `model::holdout` to `model::earnings`; import `Earnings`, `Trades`, `Trade`, `MetaMargin` and `STRESS` from it
- Renamed `Kept` to `Failure`, its `Failed` to `Error`, and `Outcome`'s `Kept` to `Failed` and `InUse` to `AlreadyPublished`; match the new names
- Removed `Holdout`, `Refusal`, `Minimums`, `PairCalibration`, `ModelCalibration`, `Step`, `Kept::Waiting` and `Retrain::in_use`, their concepts gone
- Removed `ModelManifest`'s `holdout`, `strategy_id` and `valid_from`; read a side's pair calibration from `StoredModelSet::calibration`
- Changed `Trained` to hold one `Outcome` of the model set, `Outcome::Published` its directory; read `trained.outcome` in place of each model's
- Changed `ModelStore::publish` to return the model set's directory, and `Dataset::settled` the days it read beside its rows
- Removed `WalkForward::prepare`, `Schedule::ensure_trades`, `Plan::fits` and `Plan::meta_after_sides`; train with `Training::train_at`
- Removed `DecidingModels::pick`; decide with `DecidingModels::decide`, or pick within a model set with `DecidingModels::pick_among`
- Removed `Models::iter`, `Models::as_ref` and `Models::map`; read the `long`, `short` and `meta` fields
- Removed the `fake` learner from builds without the `test-support` feature; use `baseline`, `lightgbm` or `perpetual`
- Removed `heartbeat` from `CTraderWebSocketClient::connect` and `CTraderSession::connect_to`; drop it, the session heartbeats every 10 s
- Removed `keepalive_secs` from `LseWebSocketClient::connect`; drop it, the client pings every 25 s
- Removed `Deserialize` from `TickSeries`; read tick series through a strategy config's `[data]` series
- Removed `Launcher::in_process` outside the crate's own tests
- Removed `FeatureSetBuilder`'s `quotes`, `trades`, `bars`, `depth`, `deltas`, `over_bracket`; use `feature` on a list of series, and `bracket` for the width
- Changed `OverBracket` to be a feature's `Feature::over_bracket`, handed the width `FeatureSetBuilder::bracket` takes; return `Some(self)` from it
- Changed `Warmup`'s `feeding`, `feeding_ticks`, `feeding_books` and `feeding_custom` to take the series their indicator reads; pass them first
- Changed `NodeFill::run` to take its node's clock factory, whose clock LSE's client reads the time from; pass `|| Rc::new(RefCell::new(LiveClock::default()))`
- Changed OPRA definitions from Databento to the standard contract's multiplier of 100, Nautilus' decoder default; fill a fresh catalog for stored contracts

### Security

- Fixed a crash during a publish leaving a model set listed with truncated model files: the files are synced to disk before the manifest is written

### Fixes

- Fixed a meta model deciding beside the sides of another train, as when a later train's meta waited or failed
- Fixed a train whose meta failed, or that stopped between its sides' publishes, leaving its sides published without the rest of its models
- Fixed IB quote and trade fills losing ticks at each page boundary and the window's last second, IB leaving out a request's end second

### Internal Improvements

- Added issue templates: bug report, enhancement request and upstream tracking, each with its rules
- Changed the model feature set to supply its warmup inputs from feature declarations
- Changed walk-forward scoring to evaluate each side once per row and use the same meta verdict as trading decisions
- Added `check-release-entries`, a pre-commit hook capping each entry of the release in progress at one line of 160 characters
- Improved the LSE fill tests' run time, their mock called at 6000 calls a minute instead of 60
- Changed `cargo deny` to ignore the unmaintained `chrono`, `chrono-tz` and `humantime` advisories, transitive through Arrow, DataFusion and `object_store`
- Changed the dev and test builds to compile dependencies at `opt-level` 1, as Nautilus does, running the backtest tests 3 to 8 times faster
- Improved the slowest tests' run time: the streaming tests stop once data is staged or promoted, and the model tests skip unread trains and quotes
- Improved the tests to `docs/testing.md`'s standard: `#[case]` parametrization, duplicates removed, bounded waits and expected values worked by hand
- Added binary tests of the command line's refusals (`tests/backtest/cli.rs`) in place of in-process parses
- Changed the backtest fee model and live recording to set Nautilus' `charge_commission_once` and `promote_on_close` rather than rely on their defaults
- Removed `lse`'s `LseHttpClient::with_retry`, a test hook
- Changed the live warmup's hold of live data and the hooks feeding it to be marked `Temporary` at each site, citing helix-trader#16
- Changed the Nautilus pin to `2d844b3`, a catalog's book delta warmup opening with one snapshot at its start rather than one per window

### Documentation Updates

- Changed the live guide to distinguish a catalog interval's end from the time of its last row
- Changed the pull request template to cite upstream issues and pull requests without adding backlinks
- Changed the release steps to open the next release's version in `Cargo.toml` with its section, as Nautilus does
- Changed `docs/releases.md` to cap an entry at 160 characters, its how and why going in the pull request

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
