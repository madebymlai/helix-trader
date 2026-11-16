# Testing

Tests are helix's executable specification: they document intended behavior, let a refactor proceed with confidence, and catch a regression before it reaches a venue. This standard is Nautilus' testing guide and Rust testing conventions (`docs/developer_guide/testing.md` and `rust.md`, "Testing conventions", in `nautilus_trader`), adapted to helix. The [README](../README.md#tests) says how to run the tests and open the live gates.

## Testing policy

Tests and runtime contracts form one design. A contract pushes an invariant as far down the mechanism ladder as it goes, and the test layers escalate what remains through larger input spaces and richer execution, each layer reaching inputs or states the layer below cannot. Not every module needs every layer: decide which apply before adding tests or `debug_assert!`.

Assert returned values, emitted events, protocol traffic or resulting state. A log is evidence only where logging is the contract, such as a secret never printed, and then by the records that matter rather than the whole transcript.

### Mechanism ladder

Runtime contracts, in order of preference:

1. The type system: a named type, an enum, a constructor that refuses an invalid value.
2. `check_*` from `nautilus_core::correctness` at an API boundary, so a violated precondition surfaces where it originates.
3. `debug_assert!` for an internal invariant.
4. `assert!` for a soundness-critical or always-on check.

Test layers escalate in parallel. Start at the lowest layer that proves what matters, and climb only when it stops detecting regressions or the input space outgrows hand-picked cases:

| Layer                | Trigger condition                                                                  |
| -------------------- | ---------------------------------------------------------------------------------- |
| Unit test            | A function or transition has a small, enumerable set of cases                      |
| Parametrized test    | The same shape repeats across discrete inputs: a side, a venue, an instrument kind |
| Property-based test  | An invariant must hold for a class of inputs no list of cases covers               |
| Integration test     | Modules interact through Nautilus' real engines, in a backtest or a node           |
| Fuzz test            | Untrusted or adversarial bytes cross an adapter's parser or decoder                |
| Live acceptance test | Behavior depends on a venue's contract, on its demo or paper account               |

Helix has no property or fuzz harness yet: their rows record when to add one.

### Projection rule

A module's shape decides which layers pay off. Apply the rule module by module: an adapter crate holds pure parsers and I/O-bound client loops, and each row applies to a different part.

| Module shape                        | Layers that apply                                | Helix example                                                     |
| ----------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------- |
| Pure function, crisp invariants     | Unit, parametrized, property                     | Stake and risk sizing, payoffs, the roll calendar                 |
| Pure function, no stated invariants | Unit, parametrized, property, fuzz               | Venue payload parsers, config loading                             |
| Stateful, synchronous               | Unit, parametrized, property over transitions    | The warmup's tick hold, dataset rows, features                    |
| Stateful, async                     | Unit, integration                                | The live node, a warmup through the data engine, the catalog fill |
| I/O bound, venue contract           | Integration against a mock peer, live acceptance | cTrader's and LSE's client loops                                  |

### When not to add coverage

- Add `debug_assert!` only where a test reaches it: release builds strip it, so an unexercised assertion carries no signal.
- Prefer a property test to hand-written edge cases when the invariant spans a class of inputs. Targeted unit tests stay for known venue pathologies and as reproducers of shrunk counterexamples.
- A hermetic test replays what a live acceptance test captured rather than repeating it: the live test proves the venue answers so, the hermetic one that helix handles that answer.
- Leave language and library guarantees to their owners (`Option::is_some` after `Some(..)`, `Vec::len` after `push`).

### Determinism

`clippy.toml` refuses wall-clock reads, raw tokio spawns and raw sockets in tests as in production, which keeps a test hermetic by construction: time comes from Nautilus' clock or a seam taking it, tasks from Nautilus' runtime, the network from the gated live tests alone, and files from a temp dir.

## Specifying behavior

Start from a documented contract, an agreed behavior change or a confirmed defect, and read the code for how it is implemented. Where the intended rule and the implementation disagree, settle that before the current result becomes the expectation. For each rule:

1. State the guarantee and its scope, including the config and inputs it depends on.
2. Identify the inputs with meaningfully different outcomes, failure and recovery included.
3. Choose a concrete example for each distinction a reader needs to understand the rule.
4. Identify the observation that proves each outcome and the smallest seam that produces it.

Give each example one primary test. Overlap is expected where tests at different seams prove different properties: a calculation and its visibility to a strategy need different observations, while a second test with the same setup, action and assertions needs a purpose of its own. Before replacing a test, compare inputs, assertions, entry points and lifecycle paths, and keep the checks only it makes.

Observe at the lifecycle boundary the contract names: what a strategy sees at its start needs an observation in `on_start`, which a final cache read cannot give. A failure test asserts the failure's reason and the effects it prevents, such as a strategy starting.

Script responses at the venue boundary and keep the behavior under test real. Test controls stay in test code: production gains no flag, config, hook or accessor solely to drive a test. Release clients, tasks and nodes even after a failed assertion, and bound every action with a timeout.

When required behavior changes, change the rule, its examples and the code together. An expected outcome follows the contract, never a defect: fix the defect in the same change, or record it in an issue and add the regression example with the fix.

## Helix's seams

Each kind of test observes helix at one seam, from the outermost in:

| Seam                                                                                      | Tests                                                               | What it proves                                                                                                      |
| ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `helix-trader` and its commands, run as the binary (`env!("CARGO_BIN_EXE_helix-trader")`) | `tests/backtest/`, `tests/live/streaming.rs`                        | What an operator gets from a config: a refusal, the exit status, the printed results, the catalog and store written |
| A backtest, `Backtest::run`, on a fixture catalog (`tests/common/`)                       | `tests/backtest/`                                                   | A strategy's decisions, orders, fills and P&L on Nautilus' own engines                                              |
| A Nautilus node whose data or execution clients are fakes answering as the venue does     | `tests/node_fill/`, `tests/node_warmup/`, `tests/live/streaming.rs` | What a venue is asked, and what the catalog, cache and indicators receive                                           |
| An adapter's client against a mock venue peer replaying captured payloads                 | `crates/ctrader` (`src/testing.rs`), `crates/lse/tests/`            | The wire: requests sent, payloads parsed, reconnects and refusals                                                   |
| A real venue, demo or paper, behind its gate                                              | `tests/live/`                                                       | That the venue still answers as the hermetic tests assume                                                           |
| A module's public functions                                                               | `mod tests` beside the module                                       | A calculation or transition with enumerable cases                                                                   |

A behavior that passes through Nautilus, a fill, a warmup, a route, a bar it builds, is tested through Nautilus' real engines, cache and clock, the stand-ins sitting at the venue alone. A command's own work, reading a config, refusing a run, printing its results, is tested through the binary.

Expected values are worked from outside the code, never laundered: by hand, from the venue's or Nautilus' documentation, or from a captured venue payload. The fixture catalog's levels are chosen so that a crossover's trades, times and prices follow on paper, and each test's doc comment shows the work. A snapshot holds an output too wide to work by hand, as the golden rows do (`tests/backtest/backtest_golden_rows.rs`), and changes only on purpose: the version it is keyed by is bumped, the snapshot written again, and the commit says why the output moved.

## Live tests

`tests/live/` is helix's live acceptance layer. Each module runs only with its venue's variables set and passes, saying it was skipped, without them; it may place orders or rotate tokens on a demo or paper account, so read its venue's docs before setting them. One module runs per process (`claim_process`), as a live node takes SIGINT and installs a process-wide tracing subscriber. `streaming` is hermetic and ungated: its nodes run in child processes and its backtest through the binary, so it runs beside any module and in CI. A change to a venue's code runs that venue's live tests, or its pull request says why not, and a live probe's payloads become the fixtures of the hermetic tests.

## Property-based testing and fuzzing

A property test verifies that logic holds for every valid input with `proptest`: round trips (`parse(to_string(x)) == x`), inverse operations (`(a + b) - b == a`) and orderings. Name a property `prop_*`, keep `#[rstest]` on tests inside `proptest!`, and keep its strategies beside the suite, combining ranges with explicit edge cases.

A fuzz test feeds an adapter's parser malformed or adversarial bytes; its goal is a `Result::Err`, never a panic, a hang or a leak.

## Running tests

CI runs the suite with `cargo nextest run --workspace --profile ci` and the doc examples with `cargo test --doc --workspace`, as nextest cannot run doctests; doc examples are a maintained test surface. Locally `cargo test` runs both, helix's test binaries passing under either runner; `docs/architecture.md`, "Process constraints", gives the limits of nodes sharing a process.

## Test style

- Name a test after the behavior it exercises, as a sentence (`a_backtest_whose_balance_is_not_in_a_traded_instruments_pnl_currency_is_refused`).
- Write a doc comment when it clarifies the setup, the scenario or the expected values.
- Use distinct, non-default inputs and exact expected values, and assert every stable field.
- Group the assertions after setup and actions, unless the test checks stepwise state changes.
- Use `unwrap`, `expect`, `panic!` and `assert!` freely: clarity outranks defensive handling in a test.
- A message is asserted by the facts it must carry, the instrument or the currencies, rather than its wording.

### Rust conventions

- Write ordinary tests inline in a `#[cfg(test)] mod tests`, and integration tests as the modules of one binary per directory under `tests/`, their shared fixtures in `tests/common/`.
- Mark every test `#[rstest]`, parametrize with `#[case]`, and mark an async test without cases `#[tokio::test]`.
- Keep test support behind `#[cfg(test)]` or the crate's `test-support` feature.
- Load captured payloads from files beside the code that reads them with `include_str!`.
- Reach for Nautilus' stubs and specs (`equity_aapl`, `TestOrderEventStubs`, `OrderFilledSpec`, from `nautilus_model`'s `test-support`) before fixtures of helix's own.
- Leave out Arrange, Act and Assert comments.
- Compile a strategy's own unit tests under `strategy-tests` alone.

`check-testing-conventions` checks the attribute and the comments.

## Waiting for asynchronous effects

Prefer a notification channel or another event the test owns over polling a condition: subscribe before reading the authoritative state, and read it again after every notification, so that a transition between the read and the wait cannot be missed. With no such signal, use `wait_until_async` from `nautilus_common::testing`, which stops as soon as the condition holds and bounds the wait. Use a fixed sleep only when the time window itself is under test.

## Mocks

Prefer hand-written stubs returning fixed values to mocking frameworks, and stub the boundary, never the object under test.

## Code coverage

Aim for high coverage without giving up error handling or changing the architecture solely for tests. A defensive branch that only an unexpected value reaches stays, untested, so a later change can exercise it; 100% is not the target.
