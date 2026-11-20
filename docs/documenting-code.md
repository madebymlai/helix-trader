# Documenting code

Comments and doc comments follow Nautilus' standard, adapted to helix: its coding standards ("Comment conventions", "Doc comment mood" and "Terminology and phrasing", `docs/developer_guide/coding_standards.md` in `nautilus_trader`) and its Rust guide ("Box-style banner comments" and "Documentation", `rust.md`). A test's doc comment also follows [testing](testing.md#test-style).

## Comments

1. Leave one blank line above a comment block, so that it stands apart from the code above it.
2. Write in sentence case: capitalize the first letter and keep the rest lowercase, except proper nouns and acronyms.
3. Leave one space after a period, not two.
4. End a single-line comment without a period, unless it ends with a URL or an inline Markdown link, which keeps the punctuation the link needs.
5. Join the sentences of a multi-line comment with commas rather than a period per line, and end its last line with a period.
6. Keep comments concise, explaining only what is not obvious.
7. Leave out emoji.

Do not divide a module's contents with box-style banner or separator comments (`// =====`). Express structure with modules and `impl` blocks.

## Doc comments

### Coverage and tone

- Add doc comments to public items. Document private behavior with a normal comment only when non-obvious context prevents a likely misreading.
- Add module documentation to public modules and modules with a non-obvious contract, and no boilerplate to a private leaf module.
- Write in the indicative mood: "Returns the account ID", not "Return the account ID".
- End the documentation of a public field or enum variant with a period.
- Match the density of neighboring items, with no filler comments to make a bare block look uniform.
- Put important context for private fields in the type's documentation rather than documenting each field.

Document a public function when its contract is not clear from its type and name: its fallible conditions, its panic conditions, its safety obligations and its non-obvious input semantics.

### Rustdoc sections

Title a section in Title Case: `# Examples`, `# Errors`, `# Panics`, `# Safety`, `# Notes`, `# Thread Safety` or `# Feature Flags`.

Start `# Errors` with `Returns an error if`, or ``Returns `None` if`` for an `Option` return, and `# Panics` with `Panics if`. Describe the failure conditions without repeating the concrete error types of the signature, keeping the distinctions that change what a caller does, such as a retryable failure against a permanent one.

A single condition is one sentence ending with a period:

```rust
/// # Errors
///
/// Returns an error if the currency conversion fails.
```

Several conditions follow the same lead-in and a colon, `Returns an error if:`, ``Returns `None` if:`` or `Panics if:`, each condition a bullet starting with a capital letter and ending with a period, a wrapped bullet's on its last line:

```rust
/// # Panics
///
/// Panics if:
/// - The quantity is zero.
/// - The quantity exceeds the supported range.
```

Write `# Errors` only on a function that returns `Result` or `Option`, and `# Panics` only on a function that can panic: if it cannot, leave the section out rather than say it does not panic. Put `// panics-doc-ok` immediately above the doc block of a `Result`-returning function whose documented panic comes from a function it calls, and `// errors-doc-ok` there only for an error contract the signature check cannot recognize. The markers exempt the contract checks alone, not the wording or the punctuation.

### Doc examples

Annotate every fence of a doc example. Doc examples run as [testing](testing.md#running-tests) says.

| Fence         | Behavior                  | Use for                                                |
| ------------- | ------------------------- | ------------------------------------------------------ |
| `rust`        | Compiled and run.         | Self-contained examples with no external dependencies. |
| `rust,no_run` | Compiled, not run.        | Examples needing a catalog, network, or venue.         |
| `ignore`      | Neither compiled nor run. | Pseudocode. State why it cannot compile.               |

## Workarounds

A workaround, which [architecture](architecture.md) admits only once agreed, is marked where it is written, in its comment or its README paragraph: a sentence opening with `Temporary` says what Nautilus lacks and what the code does in its place, and cites the issue retiring it, `nautilus_trader#N` when Nautilus tracks the gap, else `helix-trader#N`:

```rust
// Temporary: the leverage, which the adapter does not read, through
// OKX's bare API, until Nautilus' OKX client reads it: helix-trader#21.
```

Code of `src/` building `nautilus_network`'s HTTP or WebSocket client reaches a venue past its adapter, a workaround too, its citation within the five lines above the call. `.pre-commit-hooks/check_workarounds.py --open` lists the workarounds whose cited issues are all closed, which are due to go.

## Wording

- Leave ", got" out of an error message: write ", was" for a type mismatch, ", received" for input validation, or ", found" for a search or lookup.
- Spell "hardcoded" as one word.
- Name a caught error `e`: `Err(e)` and `|e|`, not `err` or `error`.

## Checks

The pre-commit hooks check part of this standard; review checks the rest.

| Hook                                                                     | Checks                                                                                                                                                                               |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `check-docs-conventions`                                                 | The `# Errors` and `# Panics` lead-ins and bullet periods, and each section against its function's signature                                                                         |
| `check-nautilus-conventions`                                             | Box-style banner comments and ", got"                                                                                                                                                |
| `typos`                                                                  | Misspellings                                                                                                                                                                         |
| `check-unicode-typography`, `check-non-latin-text`, `check-hidden-chars` | Unicode typography, non-Latin scripts and hidden characters                                                                                                                          |
| `check-workarounds`                                                      | A comment or Markdown paragraph marking a workaround (`Temporary`, "workaround", "hack", "until ... is fixed") cites its issue, as does a direct `nautilus_network` client in `src/` |
| `cargo-doc`                                                              | Broken links in doc comments                                                                                                                                                         |
