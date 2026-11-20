---
name: Upstream tracking
about: A gap in Nautilus that helix works around, and what retires the workaround
labels: []
---

# Upstream Tracking

<!-- Title: "Drop X once Nautilus Y", naming the workaround and the Nautilus change that retires it,
     e.g. "Drop catalog_path once Nautilus resolves relative catalog paths".
     Label one priority, P1 to P4, weighed against every venue, instrument, order type and setting
     a config passing validation can select; which strategies or configs use it today is no
     argument. Add `tracked upstream` once a Nautilus issue or PR tracks the gap.
     Keep this body current: when the pin moves or upstream changes, restate each claim at the new
     pin in place, rather than adding a comment. -->

- [ ] I've searched existing issues here and upstream to avoid duplicates.
- [ ] I've searched Nautilus' source at the pinned rev and its docs, and it lacks this (below).
- [ ] The workaround was agreed before it was written.
- [ ] The workaround is marked `Temporary` and cites `nautilus_trader#N` when Nautilus tracks the
      gap, else this issue as `helix-trader#N` (`check-workarounds`).

## Now

<!-- What helix needs, what Nautilus lacks or gets wrong, and where in its source at the pin you
     looked (path and function). What the workaround does, where it lives, and what a run reaching
     it gets. How it was reproduced: the pin or commit, and the test or command. -->

## Blocked upstream

<!-- The Nautilus issue or PR and its state, or "No upstream issue yet" with the fix to ask for:
     a general Nautilus capability, not helix's workaround.
     For Nautilus issues and PRs, use redirect.github.com links to avoid upstream backlinks:
     [Upstream issue 5546](https://redirect.github.com/nautechsystems/nautilus_trader/issues/5546).
     For a PR, label it `Upstream PR N` and use `/pull/N`. -->

## Revisit when upstream lands

<!-- Once released and the pin passes it: the numbered steps that drop the workaround, the code and
     tests to delete or flip, the READMEs to update, what stays, and the check that proves it
     gone. -->
