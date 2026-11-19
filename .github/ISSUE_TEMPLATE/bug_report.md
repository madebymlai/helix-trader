---
name: Bug report
about: Behavior that contradicts helix's documented or intended design
labels:
  - bug
---

# Bug Report

<!-- Title: a capitalized statement of the wrong behavior naming the affected surface, e.g.
     "IB's warmup drops a session's first bar", 60 characters at most, without Conventional
     Commits prefixes (`fix:`). Label one priority, P1 to P4, weighed against every venue,
     instrument, order type and setting a config passing validation can select; which strategies
     or configs use it today is no argument. -->

Use this template only for an issue that fits the **Bug** definition.

| Term                    | Definition                                                                                            |
| ----------------------- | ----------------------------------------------------------------------------------------------------- |
| **Bug**                 | Helix behaves against its design as its code, docs or `--help` state it: the implementation is wrong. |
| **Enhancement request** | Helix behaves as designed, but the design should change, or it lacks the behavior.                    |
| **Upstream gap**        | Nautilus lacks what helix needs, or is wrong, and helix works around it.                              |

**Note:**

- A bug in Nautilus itself is reported upstream; open an upstream tracking issue here if helix
  works around it.
- Every venue, instrument, order type and setting that a config passing validation can select is
  supported: a bug on any path such a config reaches is a bug, however unusual the combination.

## Confirmation

- [ ] I've re-read the owning module's README and the command's `--help`.
- [ ] I've searched existing issues to avoid duplicates.
- [ ] I've reproduced it on a recent `develop`, at the commit given below.
- [ ] I've checked the wrong behavior is helix's, not Nautilus' at the pinned rev.

## Expected behavior

<!-- What the code, a README or `--help` says should happen; cite where it says so. -->

## Actual behavior

<!-- What happens instead. -->

## Steps to reproduce

1.
2.
3.

<!-- The command and the strategy config, cut to what reproduces it. A failing test at the seam
     `docs/testing.md` names for the module is best; a fixture catalog beats real market data. -->

## Code snippets or logs

<!-- Relevant code, the Nautilus log or a backtrace, in code blocks. -->

## Specifications

Run this from the checkout and paste the output, then fill in the remaining fields:

```bash
echo "- Commit: $(git rev-parse --short HEAD)"
echo "- Nautilus pin: $(grep -m1 -o 'rev = "[0-9a-f]*' Cargo.toml | cut -c8-14)"
echo "- Rust: $(rustc --version)"
echo "- OS platform: $(uname -sr)"
```

- Commit:
- Nautilus pin:
- Rust:
- OS platform:
- Command (`fill-catalog`, `backtest`, `live` or `train`):
- Venue, instrument and order type (if applicable):
