# Helix Trader 0.1.0

Released on TBD (UTC).

### Breaking Changes

- Changed `ModelHandle::new` to take `&ModelBlueprint` and `&Mode`; pass references

### Fixes

- Fixed `ctrader` failing to build without its `test-support` feature; `TokenPair` and `SpotEvents` are public whatever the features

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

### Documentation Updates

- Renamed `docs/ARCHITECTURE.md` to `docs/architecture.md`
- Added `docs/releases.md`, the release notes standard and release steps
- Changed spelling to American English

---

<!-- markdownlint-configure-file { "MD001": false, "MD025": false } -->
