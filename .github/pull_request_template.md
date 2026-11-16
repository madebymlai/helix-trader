# Pull Request

<!-- Title: a capitalized, imperative subject naming what changes, e.g. "Read OKX's leverage through
     the adapter", 60 characters at most, without Conventional Commits prefixes (`feat:`, `fix:`). -->

## Summary

<!-- What changed, why, and what it changes for a run (2-3 sentences). -->

## Related issues

<!-- `Closes #N`, `Related to #N`, and the issue each workaround this adds or drops cites. -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Improvement
- [ ] Breaking change (a config, a command or the public API)
- [ ] Documentation
- [ ] Maintenance

## Breaking change details

<!-- What breaks and how to migrate; the same line goes under Breaking Changes in RELEASES.md. -->

## Checklist

- [ ] Nautilus-native: what Nautilus provides comes from Nautilus, or each workaround was agreed, is
  marked `Temporary` and cites its issue (`docs/architecture.md`)
- [ ] `prek run --all-files` passes
- [ ] `cargo test` passes, and the strategy's unit tests (`--features strategy-tests`) if a
  strategy's folder changed
- [ ] New or changed logic is covered by tests, or no logic changed
- [ ] The gated live tests of each venue whose code changed were run (`tests/live`), or this says
  why not
- [ ] `RELEASES.md` has this change's entries in its top section (`docs/releases.md`)
- [ ] The owning module's README says what changed in its workflows or constraints
