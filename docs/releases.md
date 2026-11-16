# Releases

`RELEASES.md` holds the release notes, newest release first, written as Nautilus writes its own (`docs/developer_guide/releases.md` in `nautilus_trader`). Its top section is the release in progress, `Released on TBD (UTC).` until it ships, and each pull request into `develop` adds its entries there.

## Release notes

Each release's sections come in this order, an empty one left out:

| Section               | What it holds                                            | Entries start with                |
| --------------------- | -------------------------------------------------------- | --------------------------------- |
| Enhancements          | New features and user-visible improvements               | Added                             |
| Breaking Changes      | Changes that break a config, a command or the public API | Removed, Renamed, Changed         |
| Security              | Fixes of crashes, undefined behavior or data corruption  | Fixed                             |
| Fixes                 | Fixes of incorrect behavior                              | Fixed                             |
| Internal Improvements | Tooling, CI, refactors and tests, invisible to a run     | Added, Changed, Removed, Improved |
| Documentation Updates | Docs alone                                               | Added, Changed, Renamed, Removed  |
| Deprecations          | What is due to go, and its replacement                   | Deprecated                        |

An entry is one line: what changed, not how, code in backticks. A breaking change ends with its migration, after a semicolon.

## Shipping a release

1. On a branch from `develop`: set `[workspace.package] version` in `Cargo.toml`, give the top section of `RELEASES.md` that version and its date (`Released on 2026-11-20 (UTC).`), and merge it into `develop`.
2. Open the pull request from `develop` into `main`. Once its CI passes and it merges, the release workflow (`.github/workflows/release.yml`) tags `v<version>` on `main` and publishes a GitHub release whose notes are the top section of `RELEASES.md`, up to its `---`.
3. On a branch from `develop`: open the next release's section above it, `# Helix Trader <next version>` and `Released on TBD (UTC).`, ended by `---`.

The workflow publishes nothing while the top section is `TBD` or its version is already tagged, and fails if the top section's version is not `Cargo.toml`'s.
