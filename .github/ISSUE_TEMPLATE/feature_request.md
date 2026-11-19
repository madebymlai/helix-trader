---
name: Enhancement request
about: New functionality or behavior that helix does not have today
labels:
  - enhancement
---

# Enhancement Request

<!-- Title: a capitalized, imperative subject naming what helix gains, e.g. "Read OKX's leverage
     through the adapter", 60 characters at most, without Conventional Commits prefixes (`feat:`).
     Label one priority, P1 to P4, weighed against every venue, instrument, order type and setting
     a config passing validation can select; which strategies or configs use it today is no
     argument. -->

Helix builds on Nautilus and takes from Nautilus everything Nautilus provides. If Nautilus lacks
what this needs and the only path is a workaround, open an upstream tracking issue instead.

- [ ] I've searched existing issues to avoid duplicates.
- [ ] I've searched Nautilus' source at the pinned rev and its docs for its own capability.

## Problem statement

<!-- What can a run not do today, and what limits it in the current implementation? For a trading
     rule or a model feature: who trades, why, and what their behavior leaves on our tape, and
     whether our data shows it. -->

## Proposed solution

<!-- What helix should do, and what it changes for a run. -->

## Nautilus

<!-- What Nautilus provides for this at the pinned rev, by source path, and how helix uses it through
     the plain Nautilus API.
     For Nautilus issues and PRs, use redirect.github.com links to avoid upstream backlinks:
     [Upstream issue 5546](https://redirect.github.com/nautechsystems/nautilus_trader/issues/5546).
     For a PR, label it `Upstream PR N` and use `/pull/N`. -->

## Example usage

```toml
# The strategy config, or the command, that would use it
```

## Testing

<!-- The seams of `docs/testing.md` that would cover it, and the gated live tests if a venue's code
     changes. -->

## Alternatives considered

<!-- What else was explored, and why this solution is preferred. -->

## Additional context

<!-- Related issues, probe results and references. -->
