# Futures rolls

The rolls module says which dated contract stands for a futures root at
each moment. Nautilus has no continuous instruments. Its continuous futures
take a transition table from the caller and drive bars only, and its
venues and data sources (Databento, IB) take dated contracts only. This
module builds that table, in Nautilus' row shape and with its segment rule,
from a roll calendar kept in one file.

## Layout

```
rolls/
  mod.rs         TransitionTable, Transition, Slice: the public interface
  roller.rs      Roller: a strategy's tick subscriptions, rolled
  continuous.rs  ContinuousBars: Nautilus' continuous bars on roots
  calendar.rs    the roll calendar: products, contracts, roll times
  calendar.toml  the products, embedded at build time
```

## Roots and plain instruments

A root is an instrument id listed as a product `root` in `calendar.toml`.
Its symbol is the product root and its venue the data venue (`GC.GLBX`),
the form Nautilus uses for a continuous future's target (`ES.XCME` in its
docs). A root's contracts are on the same venue, named by the product's
`contract` template in the calendar (`GC{month}{y}` gives `GCZ6.GLBX`, as
Databento's raw symbols name them). Only the calendar knows the format.

Any other id is a plain instrument: `XAUUSD.ICMARKETS`, a dated contract
such as `GCZ6.GLBX`, or Databento's continuous `GC.v.0.GLBX`. It gets a
table with no transitions, so one slice on itself. Configs carry no roll
flag. Every declared instrument goes through `TransitionTable::of`, and the
calendar lookup inside it is the only place that tells roots apart.

## The table

`TransitionTable::of(instrument_id, start, end)` holds the rolls over
`[start, end]`. Each `Transition` has a time, a pre contract and a post
contract, as Nautilus' `continuous_future_transitions` rows do. The rolls
start from the contract in front at `start`. A transition is included if
its time is after `start` and no later than `end`. A roll exactly at
`start` is already done.

- `transitions()`: the rows, in time order.
- `contract_at(time)`: Nautilus' segment rule. Before the first transition
  it is the first pre contract, and from each transition's time on it is
  that transition's post contract. A time exactly at a transition belongs
  to the new contract. The rule holds outside the span too.
- `slices()`: one `Slice` (contract, from, to) per segment over the span,
  both ends included as the catalog's ranges are. Each slice ends 1 ns
  before the next transition, and the last one ends at `end`.

`Transition::after(instrument_id, time)` is the next roll after `time`,
`time` excluded, from the contract in front then (its `pre`), with no span:
none for a plain instrument. The table is built from it, and the roller
uses it to know the next roll in advance. `contracts_at(instrument_id,
time)` is what an instrument needs from `time` up to that roll: the
contract in front and the one it rolls into, or a plain instrument alone.

The transitions carry no roll prices. Nautilus' continuous bars need them,
and `ContinuousBars` reads them from the catalog (see Continuous bars).

`strategy::DataRequirements::slices(start, end)` expands every catalog
series a strategy reads through its instrument's table. The result is the
series of each contract over its slice (`SeriesSlice`), so fill-catalog
and the backtest only ever see contract ids. A contract read as itself
beside its root is one series: its slices from both tables are merged.

## The roller

A `Roller`, owned by a strategy, keeps it subscribed to the tick series it
reads (`TickSeries`: quotes, trades or book depth of an instrument, root or
plain) on the contract standing for each instrument, with the plain
Nautilus clock and subscription API, as Nautilus' own crate-private roller
of continuous bars does. The strategy starts it in `on_start` and hands it
its time events in `on_time_event`:

```rust
fn on_start(&mut self) -> anyhow::Result<()> {
    self.roller.clone().start(self)
}

fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
    if let Some(roll) = self.roller.clone().on_time_event(self, event)? {
        // roll.pre to roll.post: restart the indicators.
    }
    Ok(())
}
```

It subscribes through the strategy, so it is called on a clone; it holds
the series only and works out the contracts from the clock.

- `start` subscribes each series on the contract in front now and, for an
  instrument that rolls, sets a time alert (`ROLL:` and the instrument's
  id) for its next roll. A plain instrument is subscribed once, with no
  alert.
- `on_time_event`, on an instrument's alert, unsubscribes its series from
  the pre contract, subscribes them on the post contract, sets the alert
  of the next roll, and returns the `Transition`. Any other event returns
  `None`: it is the strategy's own. A series of the pre contract that
  another of its instruments still stands on keeps its subscription: a
  contract read as itself beside its root keeps coming after the root
  rolls off it (Nautilus keeps one subscription per series, whoever
  subscribed).

The alert is at the last nanosecond of the pre contract's slice, not at
the transition. The backtest engine delivers the data of a time before the
alerts set for that time (`BacktestEngine::run_impl`: timers at the data's
time are flushed after it), so an alert at the transition would miss the
post contract's tick stamped there. The strategy is told after the pre
contract's last tick and before the post contract's first.

Bars do not use the roller: Nautilus' continuous bars roll themselves from
the table.

Live, the node loads a root's contract in front at its start and the next
one, so the contracts the roller subscribes to are loaded; a run spans one
roll (`live/README.md`, "Rolled roots").

## Continuous bars

Bars on a root (`GC.GLBX-1-MINUTE-LAST-INTERNAL`) are Nautilus' continuous
bars: the data engine builds them from the trades of the contract standing
for the root, adjusted into one price frame, and switches contracts itself
at each transition (Nautilus' `docs/concepts/continuous_futures.md`). It
takes the table as the params of the bar request or subscription.
`ContinuousBars::read(catalog, bar_types, start, end, mode)` builds them
for the roots among the bar types' instruments, over the span:

- `continuous_future_transitions`: the table's rows, each priced from the
  catalog. `pre_price` is the pre contract's last trade in its slice,
  `post_price` the post contract's first trade in its slice, as Nautilus'
  docs take them (the catalog stores each contract over its slice only, so
  the two contracts are not priced at the same instant). The trades are
  read a day at a time from the end of the slice nearest the roll.
- `continuous_future_adjustment_mode`: the caller's `mode`, one of
  Nautilus' `ContinuousFutureAdjustmentType` (`BACKWARD_SPREAD`,
  `FORWARD_SPREAD`, `BACKWARD_RATIO`, `FORWARD_RATIO`).

`params(instrument_id)` gives the two entries of a root, none for a plain
instrument. `warmup::start` adds them to the warmup request of each bar
type on a root (`request_bars` of the bar type itself) and to its
subscription (one `subscribe_bars` per such bar type, since Nautilus reads
one continuous future per command), and the strategy reads the warmup
bars from the cache with `warmup::continuous_bars` (`strategy/README.md`,
"Bars on a root"). `Backtest::continuous_bars(mode)` reads them over the
backtest's run and warmup, the span the backtest replays.

Bars on a root must be built from its trades (internal, LAST, not
composite): the rolls are priced from trades, and Nautilus builds the
continuous bars from the contracts' trades, so other bars would never
come. `DataRequirements::slices` refuses others, naming the bar type, so
backtest and fill-catalog refuse them before anything runs.

`read` fails, before any run:

- for a root with no roll over the span: Nautilus' continuous bars need
  at least one transition, and the error names the contract to declare
  instead;
- for a roll whose pre or post contract has no trade in its slice, naming
  the contract, its slice and the roll.

Live refuses bars on a root: a live roll's prices are not known when the
subscription is made (`live/README.md`, "Rolled roots").

What follows from Nautilus' rules (`backtest_continuous_bars.rs` pins
them):

- Backward modes anchor on the span's last contract: the bars before a
  roll are shifted by the rolls after them, priced from data after the
  bar. That is lookahead in the price level, not in the moves within a
  contract; forward modes anchor on the first contract and use only past
  rolls.
- The engine switches the subscription at the transition time on a timer,
  and a backtest delivers the data of a time before its timers. The post
  contract's trade stamped exactly at the transition reaches no bar.
- A roll inside a bar keeps the pre-roll part of the bar at the old
  adjustment (Nautilus' "Mid-bar roll boundary").

## The calendar

`calendar.toml` has one `[[product]]` per product:

```toml
[[product]]
root = "GC.GLBX"
contract = "GC{month}{y}"
months = ["G", "J", "M", "Q", "Z"]
roll = "last-business-day-before-delivery-month"
days_before = 2
time = "00:00"
```

- `contract` is the symbol of each dated contract, on the root's venue, as
  a template. `{month}` is the month code, `{y}` the year's last digit,
  `{yy}` its last two digits and `{yyyy}` the year. Any other text is
  literal, so any symbol a venue uses can be declared: `GC.{month}{yy}`
  gives `GC.Z26`.

- `months` are the liquid contract months, by futures month code (F to Z).
  The front rolls through these and skips the others.
- `roll` is the rule day the front's roll out of a contract into the next
  liquid one is counted back from.
  `last-business-day-before-delivery-month` is the last weekday of the
  month before the contract's delivery month, which is COMEX metals'
  first notice day. It is the only rule so far.
- `days_before` is the number of business days before the rule day that
  the front rolls, 0 for the rule day itself. It is required, with no
  default, so each product states how far ahead of the rule it rolls.
- `time` is the time of day of the roll, in UTC.

Adding a product means adding one entry. A root listed twice, a product
with no months, a product with no `days_before` or a negative one, or a
contract template with an unknown placeholder, an unclosed brace, or no
month or year fails the calendar's parse. A unit test parses the embedded
file, so a broken calendar fails there first.

Business days are weekdays. There is no holiday list: a CME holiday
matters only when it falls on a day the roll is counted over, the rule day
or the `days_before` weekdays before it, and then the roll is a trading
day nearer the rule day than declared. None did over the span checked
against Databento.

### GC against Databento

First notice day gives GC's 2026 rule days as Jan 30, Mar 31, May 29,
Jul 31 and Nov 30. GC declares `days_before = 2`, so it rolls at 00:00 UTC
on Jan 28, Mar 27 (across the weekend), May 27, Jul 29 and Nov 26. Its
front is G, J, M, Q, Z and then the next year's G.

Databento's `GC.v.0` rolls by the previous day's volume. Its switches over
the stored span (2026-01-26 to 2026-10-23), read from the stored daily
definitions (whose expiration changes at 00:00 UTC on the switch day), are
Jan 30, Mar 30, May 29 and Jul 31: on first notice day, or a business day
before it in March. The volume crossed to the next contract a trading day
before each switch, so up to two business days before first notice day.
Rolling on first notice day would hold the expiring contract after its
volume has left. Two business days before, the roll comes before the
volume moves, and the rule can still be computed for future dates, which
live needs. A test asserts each stored switch falls after the roll,
within `days_before` + 1 (three) business days of it.
