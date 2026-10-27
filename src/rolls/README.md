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
  calendar.rs    the roll calendar: products, contracts, roll times
  calendar.toml  the products, embedded at build time
```

## Roots and plain instruments

A root is an instrument id listed as a product `root` in `calendar.toml`.
Its symbol is the product root and its venue the data venue (`GC.GLBX`),
the form Nautilus uses for a continuous future's target (`ES.XCME` in its
docs). A root's contracts are named as Databento's raw symbols name them:
the root's symbol, the month code and the year's last digit, on the same
venue (`GCZ6.GLBX`).

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
uses it to know the next roll in advance.

The transitions carry no roll prices. Nautilus' continuous bars need them,
read from the catalog at each transition, and they are added where the bar
params are built.

`strategy::DataRequirements::slices(start, end)` expands every catalog
series a strategy reads through its instrument's table. The result is the
series of each contract over its slice (`SeriesSlice`), so fill-catalog,
the backtest and live only ever see contract ids.

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
  `None`: it is the strategy's own.

The alert is at the last nanosecond of the pre contract's slice, not at
the transition. The backtest engine delivers the data of a time before the
alerts set for that time (`BacktestEngine::run_impl`: timers at the data's
time are flushed after it), so an alert at the transition would miss the
post contract's tick stamped there. The strategy is told after the pre
contract's last tick and before the post contract's first.

Bars do not use the roller: Nautilus' continuous bars roll themselves from
the table.

## The calendar

`calendar.toml` has one `[[product]]` per product:

```toml
[[product]]
root = "GC.GLBX"
months = ["G", "J", "M", "Q", "Z"]
roll = "last-business-day-before-delivery-month"
time = "00:00"
```

- `months` are the liquid contract months, by futures month code (F to Z).
  The front rolls through these and skips the others.
- `roll` is the day the front rolls out of a contract into the next liquid
  one. `last-business-day-before-delivery-month` is the last weekday of
  the month before the contract's delivery month, which is COMEX metals'
  first notice day. It is the only rule so far.
- `time` is the time of day of the roll, in UTC.

Adding a product means adding one entry. A root listed twice, or a product
with no months, fails the calendar's parse. A unit test parses the embedded
file, so a broken calendar fails there first.

Business days are weekdays. There is no holiday list: a CME holiday matters
only when it falls on the last weekday of a month, and none did over the
span checked against Databento.

### GC against Databento

The rule gives GC's 2026 rolls at 00:00 UTC on Jan 30, Mar 31, May 29,
Jul 31 and Nov 30, so its front is G, J, M, Q, Z and then the next year's G.
Databento's `GC.v.0` rolls by the previous day's volume. Its switches over
the stored span (2026-01-26 to 2026-10-23), read from the stored daily
definitions (whose expiration changes at 00:00 UTC on the switch day), are
Jan 30, Mar 30, May 29 and Jul 31. Three are on the rule's date, and March
is one business day early. The rule stays first notice day, because it can
be computed for future dates and live needs that. The test accepts each
Databento switch on the rule's date or the business day before it. On a
day where Databento has already moved before the rule (Mar 30 2026), its
`GC.v.0` data belongs to neither slice. Research masks roll days.
