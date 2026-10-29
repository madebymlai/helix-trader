# Writing a strategy

A strategy is a Nautilus strategy and nothing more. Backtest and live take
any type that implements the Nautilus `Strategy` trait and never name a
concrete strategy. A process runs one strategy at a time; `main` picks it by
name.

The Nautilus APIs below were checked against the sources of the pinned
`develop` commit in `docs/ARCHITECTURE.md` (Version), which also holds the
process-wide rules (one node per process, Nautilus owns the logger).

## Layout

Each strategy is one folder, and everything about it lives there:

```
strategy/<name>/
  mod.rs        Nautilus wiring, kept thin
  config.rs     the config: every parameter of the strategy
  book.rs       the pure core: from bars to decisions (the EMA's signal.rs
                and bracket.rs)
  ...           more pure modules as needed (features.rs, tree.rs, ...)
```

The folder exposes only the config and the constructor. `ema/` is the
reference strategy.

What most strategies need, whatever their position mode or order style,
lives in `strategy/` itself, as a config struct and functions, never a base
type or trait:

```
strategy/
  config.rs     BarsConfig: the bars read, their warmup and session
  session.rs    Session: the hours traded
  warmup.rs     the warmup calls and the node configs it needs
```

Orders stay in each strategy, on Nautilus' own API (see "Orders and
positions"): there are no shared trading helpers.

The one exception is a strategy trading on a learned model: its features
are a type, `model::features::FeatureSet`, built once and shared with
Nautilus as an indicator, and its features implement a trait,
`model::features::Feature`; it holds a type, `model::handle::ModelHandle`,
the one place it decides, which serves its models or, in a dataset run,
opens its targets (see "Features and decisions"). Its orders stay its
own. `learned/` is the example.

## Config

The strategy config is the universe: the single source of the instruments
and the bar spec, which are config values, never constants in code. Nautilus
has no universe type; `fill_catalog`, backtest and live get the instruments
and the warmup from this config, through `main`, as Nautilus types. The
instruments are those the strategy reads; which of them it trades is the
strategy's own parameter (the EMA's `trade_instrument_id`), which nothing
outside the strategy reads.

It is a plain struct holding the Nautilus `StrategyConfig` (field `base`),
the shared `strategy::BarsConfig` (field `bars`), and every parameter of the
strategy, model hyperparameters included (`ema/config.rs`,
`EmaCrossConfig`). It derives `serde::Deserialize` with
`#[serde(deny_unknown_fields)]` and is loaded from a TOML file given on the
command line; it also derives `bon::Builder` for tests, so a defaulted field
carries both `#[serde(default = ...)]` and `#[builder(default = ...)]`.

```rust
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct TrendConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    #[serde(flatten)]
    pub bars: BarsConfig,
    pub lookback: usize,
}
```

- `base` stays in each strategy: its default carries the strategy's own
  id and order tag (`EMA_CROSS-001`), and leaves `oms_type` unset (see
  "Position mode: the venue's").
- `bars` is flattened, so its keys (`instrument_ids`, `bar_spec`,
  `composite`, `warmup`, `session`) sit at the top of the TOML with the
  strategy's own. serde then buffers the keys the strategy config does not
  name and hands them to `BarsConfig`, which takes those it knows; the
  strategy config's `deny_unknown_fields` rejects any key left over, so a
  typo such as `sesion` still fails at load (serde_derive's
  `collected_deny_unknown_fields`). `BarsConfig` itself does not deny
  unknown fields: flattened, it only ever sees its own keys.
- `validate` checks the strategy's own fields after `self.bars.validate()`.

```toml
instrument_ids = ["ESH4.CME", "ESM4.CME"]
bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
warmup = "720h"
session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
```

- `StrategyConfig`, `InstrumentId` and `BarType` deserialize; `InstrumentId`
  and `BarType` from their strings, a `BarType` through its checked parser.
- `BarSpecification` deserializes from its three fields, validated by the
  checked constructor, so a bad cadence such as `step = 60, aggregation =
  "MINUTE"` fails at load. The enums parse case-insensitively.
- `SignedDuration` (`jiff`, with its `serde` feature) parses a duration
  string such as `"720h"`.
- Loading then validates what serde cannot, `BarsConfig::validate`: at
  least one instrument, all on one venue; a bar spec aggregated on time (the
  session needs the bar's length) and, when built from quotes, priced
  `BID`, `ASK` or `MID` (`LAST` fails at load); composite source bars
  Nautilus accepts (`BarType::new_composite_checked`); a positive warmup; a
  session, if any, opening before it closes; and no session together with
  `composite` (see "Session").

### Bar types

The config builds the bar types the strategy uses, one per instrument, all
at the same spec: `BarType::new(id, bar_spec, AggregationSource::Internal)`,
such as `SXR8.IBIS-1-HOUR-MID-INTERNAL`. With the optional `composite` line
the bars are built from the instrument's source bars instead, a composite
bar type (`BarType::new_composite`, its `composite_*` arguments):

```toml
instrument_ids = ["BTC-USDT-SWAP.OKX"]
bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
```

gives `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`, the hour
bars of OKX's 1-minute candles, any price type (the source's), warmed up on
those candles (see "Warmup and training"). On IB the bars are built the
same way from IB's 1-minute TRADES bars:

```toml
instrument_ids = ["SXR8.IBIS"]
bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
```

gives `SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`. Nautilus' risk
engine prices a market order from the cached external LAST bar, so the
strategy needs no quotes to trade. A bar type carries one price type, so
LAST bars carry no spread: a strategy that needs it subscribes to the
instrument's quotes itself. Instruments IB serves no TRADES bars for
(IDEALPRO forex, CMDTY spot metals) are not supported
(`data/ib/README.md`).

`BarsConfig` states what the strategy needs from data as one
`strategy::DataRequirements`: its bar types, its tick series and its
warmup, its instruments derived from the bar types and the ticks
(`DataRequirements::instrument_ids`). `fill_catalog`, backtest and live
read them from it (`main` calls `config.bars.data_requirements()`), never
from the config's fields:

```rust
pub fn data_requirements(&self) -> DataRequirements {
    DataRequirements {
        bar_types: self.bar_types(),
        ticks: self.ticks.clone(),
        warmup: self.warmup,
    }
}
```

A strategy reading ticks as they are, beside or instead of bars, declares
them as `TickSeries`, a Nautilus data type (`NautilusDataType`) of one
instrument:

```toml
ticks = [
    { data = "quotes", instrument_id = "XAUUSD.ICMARKETS" },
    { data = "trades", instrument_id = "GC.v.0.GLBX" },
    { data = "order_book_depths", instrument_id = "GC.v.0.GLBX" },
    { data = "order_book_deltas", instrument_id = "GC.v.0.GLBX" },
    { data = "Custom:Imbalance", instrument_id = "GC.v.0.GLBX" },
]
```

`data` is a name Nautilus parses (`NautilusDataType::from_str`) of one of
the series a strategy reads as they are: `quotes` (`QuoteTick`), `trades`
(`TradeTick`), `order_book_depths` (`OrderBookDepth10`),
`order_book_deltas` (`OrderBookDelta`) or `Custom:<type name>`, custom
data of the instrument. Any other data type, or a name Nautilus does not
parse, fails the config's load. Custom data is keyed by the instrument
id, in the catalog and on the message bus, as Nautilus' Databento client
keys its own: the strategy subscribes to
`DataType::new(type_name, None, Some(instrument_id))` and gets it in
`on_data`, and registers the type with Nautilus
(`nautilus_serialization::ensure_custom_data_registered`) before a
backtest reads it from the catalog.

fill-catalog asks each source for them: OKX, IC and IB fetch the declared
series and refuse those they do not serve, while Databento fetches a GLBX
instrument's MBP-10 book (depth), MBO book (deltas) and trades, whichever
are declared. No source serves custom data: fill-catalog refuses it,
naming it; it reaches the catalog otherwise (written by whatever makes
it). A backtest replays each as declared, beside the series the bars are
built from, in time order with them: one `BacktestDataConfig` per series,
deltas grouped into an event per `F_LAST` (`OrderBookDeltas`, in
`on_book_deltas`); custom data, which `BacktestDataConfig` refuses, read
from the catalog by the backtest and added to the node's engine (see
`src/backtest/README.md`, "Custom data"). The strategy subscribes to them
itself. They have no warmup of their own: a strategy reading ticks alone
gets the default cache. Live sets up the clients of their instruments, as
of every instrument of the requirements, but subscribes to none of them;
live custom data reaches the strategy as it is published on the message
bus.

An instrument may be a futures root (`GC.GLBX`), listed in the roll
calendar, rather than a dated contract. `DataRequirements::slices(start,
end)` expands each catalog series the requirements read through its
instrument's transition table (`rolls/README.md`) into `SeriesSlice`s: the
series of each contract over the slice where it stands for the root. A
plain instrument's series is one slice over the span, on itself. The
slices of one series from several tables (a root's and a contract read
beside it) are merged, so nothing is read twice. Bars on a root not built
from its trades (internal, LAST, not composite) are refused there, naming
the bar type: Nautilus builds a root's continuous bars from trades and
prices their rolls from them, so other bars would never come.
fill-catalog fills Databento's series from them, contract by contract. A
strategy reading the ticks of a root subscribes to them through a
`rolls::Roller`, which switches them to the next contract at each roll and
tells the strategy (`rolls/README.md`, "The roller"). Bars on a root are
Nautilus' continuous bars, built from the trades of each contract in turn
and adjusted across the rolls: the strategy is given the
`rolls::ContinuousBars` of its bar types (`Backtest::continuous_bars(mode)`
reads them over a backtest) and hands them to `warmup::start` (see Warmup
and training, "Bars on a root").

### Session

`Session` (`strategy/session.rs`, the `session` field of `BarsConfig`) holds a `jiff::tz::TimeZone`, read from its IANA name with
`#[serde(with = "jiff::fmt::serde::tz::required")]` because `TimeZone` has
no plain `Deserialize` (an unknown name fails at load), and `open` and
`close` as `jiff::civil::Time` (`"08:30"`).

A bar counts only if its whole span, `[ts_event - bar length, ts_event]`,
lies inside `[open, close]` on the local day of its close, in the session's
time zone, so daylight saving keeps the local hours. With hourly bars and
09:00-17:30, the bars closing 10:00 to 17:00 count. Bars outside are
ignored: they do not move the indicators, warmup included, and send no
order. The pure core applies the rule on the bar's timestamp,
never the clock, so backtest and live agree.

The `session` line is optional, and without it every bar counts, the one
spanning midnight included, as a 24/7 market such as crypto needs: no
`open`/`close` pair expresses the whole day, since `00:00`-`23:59` still
drops the bar closing at midnight. Nautilus has no session concept of its
own, and venues trading around the clock simply apply no filter. Closures
and maintenance windows still yield no bar (see "Hours without quotes").

A session is refused with `composite`: live composite bars are stamped 5 s
after their close (see "Bars built from quotes"), so the session would drop
their last bar live but not in a backtest. Bars on IB are composite, so a
strategy on IB has no session: hours without IB bars (nights, weekends,
holidays) build no bar (see "Hours without quotes").

## Wiring

`mod.rs` connects the strategy to Nautilus and decides nothing itself: a
struct holding a `StrategyCore`, `nautilus_strategy!` to generate the
`Strategy` impl, and a hand-written `DataActor` impl. `Debug` is required;
`#[derive(Debug)]` works when every field is `Debug`.

```rust
#[derive(Debug)]
pub struct Trend {
    core: StrategyCore,
    config: TrendConfig,
    book: Book,
    /// The request group of the warmup of the last start.
    warmup: UUID4,
}

impl Trend {
    pub fn new(config: TrendConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            book: Book::new(&config),
            config,
            warmup: UUID4::new(),
        })
    }
}

nautilus_strategy!(Trend);

impl DataActor for Trend {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let (bar_types, warmup) = (self.config.bars.bar_types(), self.config.bars.warmup);
        self.warmup = UUID4::new();
        // No bars on a futures root: no continuous bars (see Warmup).
        warmup::start(
            self,
            &bar_types,
            warmup,
            &ContinuousBars::default(),
            self.warmup,
        )
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if warmup::finished(self.warmup, response)? {
            log::info!("Warmup complete");
        }
        Ok(())
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, quotes) {
            self.book.update(&bar);
        }
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, bars) {
            self.book.update(&bar);
        }
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        // Feed the bar to the pure core, then submit the orders it
        // decides.
        Ok(())
    }
}
```

- The macro expects the core field to be named `core`; for another name pass
  it second, `nautilus_strategy!(Trend, strat_core)`. To override `Strategy`
  hooks, add a block: `nautilus_strategy!(Trend, { fn on_order_filled(..)
  {..} })`.
- The macro adds an inherent `config()` returning the Nautilus
  `StrategyConfig`. A field named `config` does not collide with it, but
  then `self.config` is ours and `self.config()` is Nautilus's.
- The `on_start` the runtime calls is `DataActor::on_start`. The `Strategy`
  trait also declares one (it reactivates GTD timers when
  `manage_gtd_expiry` is set); the Rust runtime does not call it, so a
  strategy that wants it calls `Strategy::on_start` from its
  `DataActor::on_start`.
- The warmup is shared as function calls, not as a hook implementation:
  `on_start` hands `warmup::start` the bar types and warmup of `bars`, the
  continuous bars of those on a futures root (none here), and a new
  request group id, stored first; the two `on_historical_*` handlers
  feed the bars `warmup::bars` returns to the strategy's own state; and
  `on_data_response` asks `warmup::finished` whether the warmup ended (see
  "Warmup and training"). Each strategy writes these four handlers itself,
  so its `DataActor` impl stays plain Nautilus.

### Orders and positions

A strategy places its orders on the factory of `self.order()` and submits
them itself; no other strategy shares the account.

```rust
let order = self.order().market(instrument_id, side, quantity,
    None, None, None, None, None, None, None);
self.submit_order(order, None, None, None)?;
```

- `market` takes `instrument_id, order_side, quantity`, then seven `Option`s:
  `time_in_force, reduce_only, quote_quantity, exec_algorithm_id,
  exec_algorithm_params, tags, client_order_id`. `limit` and the other order
  types are on the same `self.order()` factory.
- `submit_order(order, position_id, client_id, params)`: all `Option` but
  the order.
- A bracket is `self.order().bracket()`, a `bon` builder finished by
  `.call()` (`OrderFactory::bracket`, `common/src/factories/order.rs`): a
  market entry by default, its stop loss a stop market at
  `sl_trigger_price`, its take profit a limit at `tp_price`, post-only by
  default. It returns `[entry, stop loss, take profit]`: the entry `OTO`
  with the two legs linked, the legs `OUO` and reduce-only. Submit them
  together with `submit_order_list(orders, None, None, None)`.
- `close_position(&position, ..)` sends a reduce-only market order on the
  position's closing side naming its id; `cancel_orders(ids, None, None)`
  cancels orders of one instrument in one command.
- Time alerts: `self.clock().set_time_alert(name, at, None, None)` fires
  `DataActor::on_time_event` at `at` (simulated time in a backtest); a
  name used again replaces the alert, `cancel_timer(name)` drops it.
- `self.portfolio().net_position(&instrument_id)` returns a signed
  `rust_decimal::Decimal`, zero when flat; `is_net_long`, `is_net_short`,
  `is_net_flat` take the same argument.
- Order and position events arrive in `Strategy` hooks, overridden in the
  macro block: `on_order_filled(&mut self, event: &OrderFilled)`,
  `on_order_rejected(&mut self, event: OrderRejected)`,
  `on_position_opened(&mut self, event: PositionOpened)`, and so on. The
  fill hook takes a reference; most others take the event by value.

#### Position mode: the venue's

A strategy leaves `oms_type` unset in its `StrategyConfig`. Nautilus then
keeps its positions as the execution client of the venue does (the
execution engine's `resolve_oms_type_for_client`; Concepts › Execution,
"OMS configuration"): IB and OKX in net mode are netting, so trades on an
instrument add into one position at a blended price; on a hedging venue
each trade is a position of its own. The same strategy code runs on both.
Strategy and venue modes always match, so Nautilus keeps no virtual
positions. A strategy that set `HEDGING` on a netting venue could not
restart with overlapping trades: the venue reports one position at the
blended average, which differs from the cached per-trade positions, and
Nautilus refuses the start. The backtest's venue keeps positions the same
way (backtest README, "Venue").

#### The EMA crossover

The example strategy (`ema/`) trades one instrument, `trade_instrument_id`,
which must be one of `instrument_ids`; the others are read only: their bars
are subscribed and warmed up like the traded one's, and the EMA ignores
them, as a strategy reading a signal instrument would read them.

A strategy reading an instrument's book lists it in `instrument_ids` too
(CME gold, `GC.v.0.GLBX`, beside `XAUUSD.ICMARKETS`), no other config: a
backtest replays the book depth the catalog stores of it, on a simulated
venue of its own (backtest README, "Book data", "Several venues"), and
leaves its bars out if the catalog stores no source of them (gold has no
quotes). The strategy subscribes to the book itself: a backtest's
snapshots come to `subscribe_book_depth` (`on_book_depth`); live, IB's
data client offers `subscribe_book_deltas` alone. Only a managed
subscription of the kind the data arrives as keeps the cache's book
(`cache.order_book`): depth snapshots in a backtest, deltas from IB, and
`subscribe_book_at_interval` keeps it from deltas only.

- **Entry.** On each bar of the traded instrument inside the session, the
  fast EMA above the slow one is long, otherwise short; a bar changing the
  side is a cross (`signal.rs`), the first warm bar setting the side
  without one. A cross sends one bracket in its direction: a market entry
  of `trade_size`, its stop loss `stop_loss_bps` and its take profit
  `take_profit_bps` from the decision bar's close (`bracket.rs`), rounded
  to the instrument's prices.
- **Exit.** The stop loss or the take profit, the venue cancelling the
  other leg; or the hold: the entry's first fill sets a time alert `hold`
  later, named after the entry, which cancels the trade's legs still open
  and closes its position by id (`close_position`). A leg's fill cancels
  the alert. At start (a restart) each trade of the strategy on the
  traded instrument still open gets its alert again, named after its
  entry, `hold` after the entry's first fill, or at once if that has
  passed: each filled bracket entry in the cache whose position is open,
  not closed since the entry went in, and whose legs have not filled. On
  a netting venue each trade in the one position gets its own alert; a
  trade its stop loss or take profit ended gets none.
- **Overlap.** Each cross is a new trade, whatever is open. On a hedging
  venue (IC) each trade is a position of its own, closed by its own alert
  (`backtest_ema_overlap.rs`). On a netting venue (IB, OKX) trades add into
  the venue's one position: an opposite cross reduces or reverses it, and
  a trade's time exit closes the whole position, the other trades' share
  included. Nothing is done about it: the EMA is an example.
- **IB.** Nautilus' brackets have reduce-only legs, which the IB execution
  client denies (`live/ib/README.md`): the EMA cannot trade live on IB.
  Backtests on IB data are unaffected.

```toml
instrument_ids = ["XAUUSD.ICMARKETS", "EURUSD.ICMARKETS"]
trade_instrument_id = "XAUUSD.ICMARKETS"
bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
fast_period = 10
slow_period = 30
trade_size = "1"
stop_loss_bps = 50
take_profit_bps = 100
hold = "4h"
warmup = "72h"
```

Limit and stop orders backtest less faithfully: see
`docs/ARCHITECTURE.md`, "Orders", and the backtest README, "Fills".

## The pure core

All decisions live in pure modules: features, models, signals. They
take bars and state and return values, do no I/O and know no engine, so unit
tests cover them without running Nautilus.

- Decisions use only bars already received: no look-ahead.
- Time comes from `self.clock()` (`timestamp_ns()`, `utc_now()`, timers,
  alerts) and randomness is seeded from the config. Never the system clock:
  in backtest it is not simulated time.
- Log through the `log` macros.

## Bars built from quotes

Strategies trade on bars Nautilus builds: internally aggregated time bars,
from quotes (priced `BID`, `ASK` or `MID`, such as
`SXR8.IBIS-1-HOUR-MID-INTERNAL`) or, for a composite bar type, from source
bars (see "Composite bars"; the venues serve no quote history, so this is
how IB and OKX bars are built). Subscribing to one makes the data engine
aggregate the replayed data into it in a backtest and the broker's data
live, so the same bar definition runs in both.

- A bar closes at the end of its period and is stamped there (`ts_event`);
  it holds the quotes stamped in `(open, close]`.
- Quote-built bars sum the quoted sizes, not traded volume: nothing reads
  it.
- A composite bar of 1-minute source bars sees the market a minute at a
  time, in a backtest as live: its high and low are those of the minutes.

### Hours without quotes

An hour with no quote at all, or no source bar, yields no bar. IB sends
quotes, hence 1-minute bars, all through the trading day, even for illiquid
instruments, so an hour without any means the market is closed: a night, a
weekend, a holiday. By default Nautilus would
still build a bar, flat at the last close (`time_bars_build_with_no_updates
= true`), and since the session checks the time of day only, on a Saturday
the strategy would trade on Friday's last quote. The backtest and the live
node both take their data engine config from `strategy::warmup`
(`data_engine_config`, `live_data_engine_config`), which turns the flag off.
Warmup bars follow the same rule.

### Composite bars

Bars of a composite bar type are built the same way from source bars instead
of quotes: OKX's candles (backtest README, "OKX on candles") and IB's
1-minute bars. Live, a venue sends a source bar only once it has closed:
OKX's confirmed 1-minute candles arrived 0.3 to 1.5 s after their close on
2026-10-22, IB's keepUpToDate 1-minute bars 0.4 to 1.3 s after on
2026-10-26 (`tests/live/ib_minute_bars.rs`). So the live node builds time bars
5 s after their close when the strategy has a composite bar type
(`warmup::live_data_engine_config`, Nautilus' `time_bars_build_delay`);
otherwise the last minute would land in the next bar. Such a live bar is
stamped 5 s after its close, its backtest twin on the close. Bars built
from quotes keep no delay. `warmup::live_bar_build_delay` gives that
delay, for decisions to wait for it (see "Features and decisions").

## Features and decisions

A strategy trading on a learned model declares its features once, as
Nautilus indicators, and decides on them on a clock cadence or on an event
of its choosing (`model::features`, `model::decide`).

- **Declaring.** `FeatureSet::builder()` takes each feature with its data
  kind and source and a name: `.quotes(instrument_id, name, indicator)`,
  `.trades(..)`, `.bars(bar_type, ..)`, `.depth(..)`, `.deltas(..)`, and
  `.data(data_type, name, read)` for a raw feature of custom data, the last
  value `read` from a data point. `.code(include_str!("features.rs"))` adds
  the strategy's feature code to the fingerprint; `.warmup(span)` declares
  how much data the features read before a decision, which a dataset
  build replays before its first day (see "Dataset"): a strategy passes
  its own warmup, `learned/` its `warmup`; none unless given. `build()`
  refuses an empty set, two features of one name and a negative warmup.
  The names and their order are
  fixed there, and the values come in that order. A feature is a Nautilus
  `Indicator` exposing its value (`Feature`), which every Nautilus moving
  average does as it is; another indicator, or one of ours, implements
  `Feature::value`. A strategy whose primary is its own rule declares
  `.primary()`: the set's last two values are then the rule's side and
  probability, `primary_side` and `primary_probability`, given at each
  decision on a side (see "Meta-labeling"), the set having no values
  without one.
- **Feeding.** The set is shared, an `Rc<FeatureSet>`, and is one Nautilus
  indicator: `features.register(self)` in `on_start` registers it for the
  quotes and trades of each instrument and the bars of each bar type it
  reads (Nautilus' `register_indicator_for_*`). The data engine then feeds
  it each of those before the strategy's handler, replayed, live and in a
  request's response (a warmup's quotes), with no strategy code. Nautilus
  routes no depth, deltas or custom data to indicators: the strategy
  forwards them from `on_book_depth`, `on_book_deltas` and `on_data`
  (`handle_depth`, `handle_deltas`, `handle_data`). Bars a warmup builds
  reach only the cache: a strategy hands those of `warmup::bars` to
  `handle_bar`. An update reaches only the features declared on its kind
  and source; an undeclared one is a no-op, never a panic of the
  indicator trait's defaults. The set never returns an error, since
  Nautilus skips the strategy's quote, trade or bar handler after an
  indicator's error: a feature's error is logged.
- **Deciding.** The rule comes from the config, `decision = { every =
  "15m" }` or `decision = "event"` (`DecisionRule`). `decide::start(self,
  rule, delay)` in `on_start` sets a cadence's Nautilus timer
  (`DECISION_TIMER`) on an epoch-aligned time, so that it keeps the fixed
  UTC grid of the interval's multiples whatever the run start; `delay` is
  `warmup::live_bar_build_delay(&bar_types)` live and zero in a backtest,
  so that a live decision sees the composite bar its backtest twin sees.
  Each decision fires 10 ms after its time and delay
  (`DECISION_MARGIN`): a bar closing on the grid is built by the data
  engine's timer of the same time, which a backtest may run after the
  strategy's (it orders the timers of one time by name), and a live node
  as their tasks wake. `on_time_event` calls `decide::decide(self,
  &features, rule, instrument_id)`, `instrument_id` the one traded, when
  `decide::is_decision(event)`; an event strategy calls it from any
  handler. A strategy trading on its model does both through its model
  handle (see "Model handle").
- **Fresh decisions.** A decision of a cadence is skipped, logged at
  debug, unless the features read market data of the traded instrument
  made (`ts_init`) after the previous decision of the grid, an interval
  before: a closed market, a weekend's or a night's, would otherwise
  decide on its last quote, and its targets would be the spread and the
  fees. The market data is the quotes, trades, bars, depth and deltas of
  the traded instrument a feature reads (`FeatureSet::last_update`),
  whatever their kind or venue, with no calendar of sessions; not another
  instrument's, which may trade while the traded one is closed, nor
  custom data, which may come on a weekend. The data engine builds no bar
  for a period without data (see "Hours without quotes"), so a bar is
  data. The previous decision is the grid's whether the strategy ran
  then or not, so the first decision of a run is judged as any other: a
  warmup's data makes it fresh only if made within that interval. A
  decision on an event is fresh. The rule holds in every mode: a stale
  decision gives no row, no targets and no trade.
- **Rows.** `decide` snapshots the features if the decision is fresh and
  every one is initialized, and publishes them on the message bus as a
  `FeatureRow` (the strategy id, the values, the decision time), custom
  data keyed by the strategy id (`FeatureRow::data_type`), and returns
  it. Cold features give no decision and publish nothing.
- **Fingerprint.** `decide::fingerprint(&features, rule)` hashes the
  feature names in order, the strategy's feature code, the features'
  warmup, the shared indicator
  code (`model::features` itself and Nautilus' indicators, named by the
  git source `Cargo.lock` pins, through `build.rs`), the rule and
  `model::decide`'s own code; `targets::fingerprint(fingerprint, &config)`
  adds the targets and `model::targets`' code, and
  `dataset::fingerprint(&features, rule, &targets, &commission)` all of
  it and the commission of the dataset runs, as given
  (`Commission::PerOrder` or `Commission::Rate`): the targets are net of
  it, so rows and models of other costs are never read. The runs' balance
  and leverage are not in it: large enough, they deny no target.
- **Targets.** In a dataset run (`Backtest::run_dataset`, on a hedging
  venue with a large balance) each decision's targets come from real
  orders on Nautilus' simulated venue (`model::targets`), so fees, spread,
  slippage and funding are in them. The config (`TargetsConfig`, from
  TOML) names the instrument, the size of every order, and a triple
  barrier, a horizon, `horizon = "1h"`, or both. The barrier's take
  profit and stop are distances from the decision's price (the last
  quote's mid, else the last trade, else the last bar's close), each in
  one unit (`Distance`): basis points of that price, `{ bps = 20 }`, or
  multiples of an average true range, `{ atr = 1.5 }`; a barrier with an
  `atr` distance declares its ATR, Nautilus' `AverageTrueRange` (Wilder's
  average) on bars of the instrument, and only then:

  ```toml
  [barrier]
  take_profit = { bps = 20 }
  stop = { atr = 1.5 }
  limit = "2h"
  atr = { bar_type = "EURUSD.ICMARKETS-1-MINUTE-MID-INTERNAL", period = 14 }
  ```

  Each exit is on the instrument's nearest tick (Nautilus' tick grid,
  `next_bid_price` and `next_ask_price`), a tie away from the decision's
  price. The targets own the ATR: the strategy registers it with
  `register(self)` in `on_start` and subscribes to its bar type, and
  Nautilus feeds it each of those bars, as it does the features (bars a
  warmup builds reach only the cache, so they do not warm it). A
  decision at which the ATR is not yet initialized opens no target, logged
  at debug, as cold features give no decision. The strategy shares a
  `Targets` (`Targets::new(config)`, an `Rc`), calls `open(self, row)`
  with each row `decide` returns, and hands it
  `on_time_event`, `on_order_filled`, `on_order_denied`,
  `on_order_rejected`, `on_position_closed` and `on_stop`; it sends no
  orders of its own then. For each target and side, a long and a short,
  `open` sends a bracket (market entry, take-profit limit, stop, the exits
  one-updates-the-other) or a market entry, client order ids
  `D{n}-{B|H}{L|S}-{E|TP|SL}`, and sets a time alert at the limit or
  horizon that closes the decision's positions still open
  (`close_position`, tagged) and cancels its legs by their ids. A closed
  position's opening order names its decision and side, its closing order
  the exit: take profit, stop or time; the only state kept is each open
  target's position id, from its entry's fill. Once all its targets are
  closed, the decision's `TargetRow` (its `FeatureRow`, per target and
  side the realised P&L net of commission as Nautilus reports it and the
  exit, the settling time) is published on the message bus, keyed by the
  strategy id (`TargetRow::data_type`); `Outcome::sign` is the P&L's
  sign. A denied or rejected order, or an entry not filled by its time,
  leaves the decision unsettled, logged, and so do positions still open
  at the stop. A barrier hit at the same nanosecond as the time limit
  counts: Nautilus hands the venue the quote before the alert. On bars
  alone the venue fills an entry at the last close, a take profit at its
  limit and a stop at its trigger, a bar's high before its low: as
  optimistic as any bars-only backtest.
- **Dataset.** The settled rows are stored by day in the catalog
  (`model::dataset`), as Nautilus custom data, `DatasetRow`: the feature
  values, the target values in the order of
  `TargetsConfig::target_names` (per target, the barrier then the
  horizon, the long then the short, its P&L and its exit, 1 a take
  profit, -1 a stop, 0 the time; `TargetRow::values`), the settling and
  decision times. A `Dataset` (`Dataset::new(catalog, strategy_id,
  &features, rule, targets, &commission)`) stores them under the strategy
  id and the fingerprint, so another strategy, of another width, has its
  own, and a change to the features, the rule, the targets or the
  commission finds none; the
  feature names go in the data type's metadata, not in each row. The
  catalog's coverage is the day cache: `missing(first, last)` gives the
  days it lacks, from the catalog's missing intervals, and `build(first,
  last, replay)` has `replay` run the strategy once over them, from the
  features' warmup before the first's start to the last's end plus the
  longest target (`settles_within`), then `write`s them, the rows decided
  before the first day not. So the indicators read at a day's first
  decisions the data before it, as inside a longer build: a day built
  alone is stored as inside a longer build, row for row, for a feature
  whose value is a function of the data within the warmup (one keeping a
  running sum, as Nautilus' moving averages do, or an exponential
  average, may still differ in its last bits). The catalog must cover
  the data from that warmup before the first day, and the strategy's own
  warmup request before it. `write(rows, from, to)` files
  only the days the replay settled (replayed whole, `to` the longest
  target past their end), each with the day's bounds, a day without rows
  within the file of the day before it (Nautilus writes no file without
  rows), and deletes what the catalog holds over them first: Nautilus
  skips a file it already has, so a recomputed day would keep its old
  rows. The directory is never consolidated, which would lose the days'
  intervals. `read(from, to)` gives the rows back bit for bit, NaN
  included; `manifest()` the dataset's `manifest.json` beside its files:
  strategy, fingerprint, feature names, target spec and names, the days
  held. `settled(from, at, replay)` gives the rows a train at `at` sees,
  those of the decisions in `[from, at)` settled by `at`: `replay` runs
  once from the warmup before the first missing day's start to `at`, no
  further, the days
  it settled are written, and the rows of the days it did not (the last
  before `at`) come from the replay alone, so the next train replays them
  again. `read` refuses rows of other feature names or widths with a
  `SchemaMismatch`.
- **Model config.** The model section of a strategy's TOML is a
  `ModelConfig` (`model::config`): `decision`, `targets`, `label`
  (`"barrier"` or `"horizon"`: the model learns whether that target's long
  gains, net of costs), `retrain`, `window` and `holdout` (durations),
  `thresholds = { long = 0.6, short = 0.4 }`, `min_score` (a holdout AUC)
  and `learner`. `validate(&requirements)` refuses a retrain interval not
  positive, a traded instrument the strategy does not read, a barrier ATR
  on a bar type it does not read (its bars feed the ATR), a label of a
  target the config lacks, a barrier limit or horizon not shorter than
  the window, a holdout leaving no fitted row, thresholds that are not
  probabilities with the short below the long, and a minimum score
  outside [0, 1]. An optional `[model.meta]` table (`MetaConfig`) adds a
  meta model, below.
- **Training.** `Training::new(catalog, models, strategy_id, &features,
  config, &commission)` and `train_at(t, replay)` (`model::train`) train
  at `t`: the rows of the window ending at `t` settled by `t`
  (`Dataset::settled`),
  so the purge between them and the model's first use is the longest
  target; the model fitted on those before the holdout, the window's
  last part, settled by its start, the same gap; scored by the AUC of its
  predictions on the holdout (`score::auc`). At or above `min_score` the
  model is published, valid from `t`; else, and on any error, the current
  model is kept with the reason (`Kept`: below the minimum score, failed,
  or schema mismatch). A train returns what it did, `Trained`: the
  `Outcome` of each model, never an error. With a meta model the primary
  trains first, then the meta, on the rows of the one replay (see
  "Meta-labeling"); `Training::of_rule` trains a rule primary's meta
  alone.
- **Model store.** `ModelStore` (`model::store`) holds the published
  models under `<models>/<strategy id>/<fingerprint>/<settings>/` (the
  dataset's fingerprint, the commission in it), the settings a hash of the
  learner and its settings, the label, the window, the holdout and the
  minimum score: a model published at a lower minimum may score below a
  higher one, so a stricter gate lists none of it. Each model is one
  file, `<valid from>.json`, written beside its name and renamed onto
  it: its manifest (strategy, valid
  from, fingerprint, feature names, target spec, label, learner, holdout
  AUC, and up to 16 holdout rows with the model's predictions on them, a
  missing value written `"NaN"`) and the model as its adapter saved it.
  A reloaded model is checked by predicting the probes, not by its bytes.
  `valid_froms()` lists the models in time order, `read(valid_from)` gives
  one back. `StoredModel::load(feature_names, fingerprint)` loads it with
  the checks a handle makes before use (below). A meta model's store is
  `meta/<settings>` under its primary's directory (`ModelStore::meta`),
  its settings its learner, window, holdout and minimum score and the
  primary's thresholds; a rule primary's under the strategy id and
  fingerprint (`ModelStore::of_rule`). `Swap` swaps a store's models in as a
  strategy does, decision by decision: the latest valid, checked, a
  model failing a check refused and the current one kept; the handle,
  the backtest's scores and the meta's rows all use it.
- **Model handle.** A strategy trading on its model holds a `ModelHandle`
  (`model::handle`, `ModelHandle::new(strategy_id, features, &config,
  mode)`, an `Rc`), the one place it decides: `start(self, &bar_types)`
  in `on_start` registers the features and starts the cadence, live
  `warmup::live_bar_build_delay(&bar_types)` late; `decide(self)` at each
  fresh decision, on data of the targets' instrument (`new` refuses a
  cadence on features reading none of it), snapshots the features and
  publishes their row, then, in
  `Mode::Backtest { models, commission }` or `Mode::Live { models,
  commission, .. }`, takes the
  latest model of
  the store valid at the decision, predicts, and returns the `Trade`
  (side, the targets' size, the probability) that `Thresholds::side`
  makes of the probability: a buy at or above `long`, a sell at or below
  `short`. No model valid yet, a stale decision, cold features or a
  probability between the thresholds give no trade, never an error. The store is listed again
  at each decision, so a model published meanwhile is used from the first
  decision at or after its valid-from, and a model is swapped in only
  there, before predicting: no decision mixes two models. Before use a
  model is checked: its feature names in order and its fingerprint are
  the strategy's, its width theirs, and it predicts each probe to the bit
  as when fitted (parity, in the node). One failing is refused, logged as
  an error, never read again, and the current model kept. In
  `Mode::Dataset { rows }` (the replay of a train) `decide` opens the
  decision's targets instead and returns no trade, and the rows they
  settle are collected in `rows`; the strategy hands the handle its
  `on_time_event`, `on_order_filled`, `on_order_denied`,
  `on_order_rejected`, `on_position_closed` and `on_stop`, no-ops
  trading. The trades are the strategy's own orders: `learned/` sends a
  market order and closes it `hold` later.
- **Meta-labeling.** A primary picks the side, a meta model decides
  whether to take the trade (`model::meta`). The primary is a model, to
  which a `[model.meta]` table alone adds a meta model, or the
  strategy's rule: its model section is a `RuleConfig` (`decision`,
  `targets`, `retrain` and a required `[model.meta]`), its features
  declare `.primary()`, its handle is `ModelHandle::of_rule`, and it
  calls `decide_on(self, side)` only when its rule signals (`decide`
  makes no decision then). The meta table (`MetaConfig`): `window`,
  `holdout`, `threshold` (the probability from which a trade is taken),
  `min_score`, `learner`, and an optional `size = { min = 0.5, max = 2 }`,
  the trade a multiple of the targets' size, `min` at the threshold to
  `max` at a probability of 1, linear between. The meta model decides
  only when the primary signals, on the primary's features with its side
  (1 a buy, -1 a sell) and probability (missing for a rule) after them,
  and learns whether the triple-barrier trade on the primary's side won:
  the targets need a barrier, and a dataset row, which opens a long and a
  short at every decision, has either side's, so the meta's rows need no
  replay of their own. A rule primary's rows are its decisions on a side.
  A model primary's are its dataset's rows, each with the prediction of
  the primary model the strategy used then (`Swap`), out of sample by
  construction, none before the primary's first model, which the meta's
  window never starts before; a row the primary makes no trade of is
  none. Trading, the handle takes the primary's trade if the meta model
  in use predicts at least `threshold`, sized if it sizes; no meta model
  valid yet is no trade. Two levels at most, by the types: `MetaConfig`
  has no meta, so `[model.meta.meta]` is refused at load as an unknown
  field.
- **Schedule.** A model is retrained every `retrain` (`model::schedule`),
  at its multiples since the Unix epoch, whatever the run start, in a
  backtest as live: the times of a Nautilus timer named after the
  strategy (`timer(strategy_id)`, `RETRAIN-<strategy id>`, so several
  strategies can retrain in one process), that `start(clock, timer,
  every, delay)` sets from the last grid time not after now, plus the
  delay (`last_grid_time`). `times(every, from, to)` gives those a run
  over `[from, to)` uses, from the last at or before `from`, firing such
  a timer on a virtual clock; `missing(times, published)` those the
  store has no model valid from. A model is the same whatever interval
  its time came from, so the interval does not key the store.
- **Retraining backtest.** `WalkForward` (`model::walk`,
  `WalkForward::new(catalog, models, strategy_id, &features, config,
  &commission)`)
  holds a backtest's retrains and scores: `prepare(start, end, build,
  train)` has `build` build the dataset from the first retrain's window
  to `end` if it lacks a day, then `train` train at each missing retrain
  time; `scores(start, end)` scores each decision of the dataset in the
  span with the model the handle used at it, beside the baseline fitted
  at each retrain time, per retrain period and over the span (backtest
  README, "Retraining on the schedule"). With a meta model, `prepare`
  builds from the longer of the two windows and trains at each time
  either store lacks, and the meta's scores follow the primary's.
- **Live retraining.** In `Mode::Live { models, commission, launcher,
  catalog_delay }` the handle retrains too (`model::launcher`): `start`
  sets the strategy's retrain timer (`schedule::start`) on the grid of
  `retrain`, `catalog_delay` late (`Recording::catalog_delay`: the data
  the node received up to a time reaches the catalog that much later);
  each firing, handed on by the strategy's `on_time_event`, launches a
  train at its grid time through the `Launcher`, unless one is still
  running: that one is skipped, with a warning. `decide` first polls the
  running retrain: a finished one is logged, as info when it published,
  as an error with the reason when it kept the current model or failed,
  and the next may start. The model it published is used from that
  decision on (the store is listed again); live validity starts when the
  train finishes, a backtest's at its time. `Launcher::process` runs the binary's `train` as a child (one
  node per process), `--at` and the time appended to its arguments, its
  exit status the outcome, the error it printed last the reason; a child
  still running when the node stops runs to its end.
  `Launcher::in_process` trains on a thread instead, for tests. A node
  starting with models in the store trades from its first decision on
  the latest valid then.
- **Commands.** `train <strategy> --config .. --at t` (`main.rs`) trains
  at `t`: `Training::new` on the strategy's features and model section,
  `train_at` with a replay through `Backtest::run_dataset` of the strategy
  in `Mode::Dataset` on hedging venues, the costs and balance given as to
  a backtest (`--commission` or `--commission-rate`, `--balance`); it
  prints what it did when every model it trained published, and fails
  with a kept one, its reason, each model's outcome named.
  `backtest` and `live` take `--models` (default `models`), the store
  root, beside `train`'s; `live` takes the costs of its retrains'
  dataset runs (`--retrain-commission` or `--retrain-commission-rate`,
  `--retrain-balance`), needed by a strategy trading on a model, and
  passes them, its `--config`, `--catalog`, `--models` and `--leverage`
  to the `train` it starts; its handle reads the models of that
  commission. `backtest` of a strategy trading on a model
  builds its dataset and runs `train` in child processes first, with
  its `--dataset-balance`, and trades on the models of its own
  commission, each train through `Launcher::process` as
  live's: one keeping the model is a kept retrain, not a failed backtest
  (backtest README, "Retraining on the schedule").

## Warmup and training

This is the single description of warmup; the backtest and live READMEs only
say how to register the catalog that serves it.

Warmup is the history a strategy needs before it trades: indicators to
initialise and models to train. The module `strategy::warmup` is the one
place that knows Nautilus' warmup protocol for the bars it builds; no
strategy names the request params or reads the cache for its warmup. A
strategy makes the three calls shown in Wiring: `warmup::start(self,
&bar_types, warmup, &continuous, group)` in `on_start`, which requests the
warmup as the request group `group` and subscribes to the bars; `warmup::bars(self,
response)` in `on_historical_quotes` and `on_historical_bars`, which
returns the warmup bars of the response's instrument, oldest first; and
`warmup::finished(group, response)` in `on_data_response`, which tells
whether the warmup ended, complete or not.

- **What `start` requests** is chosen from the bar type alone, never the
  venue. A composite bar type is built from its source bars: `start`
  requests those (`request_bars` of `BarType::composite`, such as
  `BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL`), and the response comes to
  `on_historical_bars`. Any other bar type is built from quotes: `start`
  requests the instrument's quotes (`request_quotes`), and the response
  comes to `on_historical_quotes`. Source bars that are themselves internal
  are refused (nothing stores them to request). `request_bars` of an
  internal bar type does not build bars from quotes; only a request of
  their source with `bar_types` does.
- **The params.** Each request runs over `[now - warmup, now]`, where
  `now` is `self.clock().utc_now()`, the simulated time in backtest. The
  end is given so that the data engine trims a bar closing after `now`
  from the response: IB answers an open-ended request with the minute in
  progress, which the live stream then sends again. It carries three
  params: `bar_types`, the internal bar types to aggregate the response into
  (Nautilus' `request_bars` refuses a composite bar type and points to this
  param); `update_subscriptions = true`, so that the aggregator goes on with
  the live data and the bar spanning the warmup end and the run start comes
  out once, complete, in `on_bar` (without it that bar comes out partial, or
  as a one-update bar when the run starts on a bar's close);
  `skip_first_non_full_bar = true`, so that the oldest bar, missing the data
  before `now - warmup`, is dropped; `update_catalog = true`, so that an
  answer the data client certifies complete is written back to the
  catalog (IC's, live; a backtest registers its catalog read-only). Then
  `start` subscribes to the bar types, one `subscribe_bars` of a list per
  venue, with the param `start_ns` = `now`: the live stream starts where
  the warmup ends. Without it, a data engine with a catalog starts a
  subscription to external bars 1 ns after the catalog's last bar
  (`subscribe_command_with_prefilled_start_ns`,
  `data/src/engine/streaming.rs`), which IB's data client honours by
  sending every bar since as live (`parse_start_ns`,
  `adapters/interactive_brokers/src/data/core.rs`).
  `backtest_bars.rs`, `backtest_bars_on_close.rs` and
  `backtest_composite_warmup.rs` pin this.
- **The request group.** The requests form one group
  (`DataActor::request_group`, with the strategy's id): once every request
  is answered, Nautilus calls `on_data_response` with a payload-free
  envelope of the group, carrying the first failure reason if a request
  failed (its data received up to then delivered anyway).
  `warmup::finished` returns `Ok(true)` then, or an error naming the
  reason, which Nautilus logs; `Ok(false)` for any other response. A group
  whose requests are all answered at once, as a backtest's catalog does,
  ends inside `start`, hence the id stored before the call.
- **What `bars` does.** The bars a request builds go to the cache only;
  they are there when the response arrives. `bars` reads the internal bars
  of the response's instrument from the cache (newest first there) and
  returns them oldest first. Nautilus has no native equivalent: the
  response carries what was requested, never the bars built from it
  (`process_engine_bar` in `data/src/engine/bar.rs` publishes no
  historical bar).
- **Bars on a root.** Bars on a futures root (`GC.GLBX`) are Nautilus'
  continuous bars, given to `start` as `rolls::ContinuousBars`
  (`rolls/README.md`, "Continuous bars"). Their request is `request_bars`
  of the bar type itself, with the params above and the root's
  `continuous_future_transitions` and `continuous_future_adjustment_mode`;
  the data engine requests the trades of each contract over its segment
  and builds the adjusted bars. It reads neither
  `update_subscriptions` nor `skip_first_non_full_bar` there: the oldest
  bar is kept, partial, and the bar spanning the warmup end is not built.
  It answers with no bars, its bars in the cache only, so `bars` gets none:
  the strategy calls `warmup::continuous_bars(self, &bar_types,
  &continuous)` once `finished` returns `Ok(true)`, which returns them
  oldest first. Each such bar type is subscribed on its own, with
  `start_ns` and the same two params. `backtest_continuous_bars.rs` pins
  this.
- **Cache capacity.** The cache keeps `CacheConfig::bar_capacity` bars per
  bar type (default 10,000) and evicts the oldest beyond it, so a longer
  warmup would be cut silently. `warmup::cache_config(bar_types, warmup)`
  gives the node its cache config: the default, its bar capacity raised by
  the most bars the warmup builds of one bar type (warmup over bar length,
  plus the bar spanning the start), the default left on top as headroom.
  Past the cache's maximum of 1,000,000 it fails, naming the bar type, the
  bars needed and the capacity, and the run does not start. The backtest
  sets it on `BacktestEngineConfig::cache`, live on `LiveNodeConfig::cache`.
- **Response.** One response per request, the catalog's part and the data
  client's merged, usually before the first `on_bar`. An `Err` returned
  from `on_historical_quotes`, `on_historical_bars` or `on_data_response`
  is logged, not propagated: a failed warmup does not stop the run. An
  unanswered request never calls them, and its group never ends. Do not
  trade until warmup has actually arrived.
- **Who answers.** The data engine serves the request from a registered
  catalog and asks the data client only for the intervals the catalog
  lacks. In backtest the client answers nothing, so the catalog must be
  registered and cover the whole window, or no response comes (see the
  backtest README). Live, the node registers the same catalog, and the
  data client answers the rest (see the live README): OKX's adapter serves
  its candles up to now, IB's its 1-minute bars up to now
  (`request_bars`), IC's its quotes up to now from cTrader's tick history
  (`request_quotes`), written back. Neither OKX nor IB serves quote
  history, so on them only composite bar types get a warmup; on IC only
  bar types built from quotes do.

A model is never trained inside the strategy: `train` fits it on the
strategy's dataset and publishes it as a file in the model store, which
backtest and live share, and the strategy's model handle reads the latest
valid one at each decision (see "Features and decisions"). Its features
warm up as its indicators do: Nautilus feeds them the live and replayed
data, and the strategy hands them the warmup's bars.

## Adding a strategy

1. Create `strategy/<name>/` following the layout above: a config embedding
   `BarsConfig` (see "Config"), the wiring calling `warmup` (see "Wiring"),
   and the strategy's own decision and orders.
2. Declare it in `strategy/mod.rs`.
3. In `main.rs`, add a variant to `enum Strategy` and an arm to each `match`
   on it (fill-catalog, backtest, live, train), reading the data
   requirements from `config.bars.data_requirements()`. A strategy with
   no model refuses `train`.

## Tests

- Pure modules: unit tests in a `#[cfg(test)] mod tests` at the bottom of
  each file. `BarsConfig` and `Session` are unit tested where they live,
  `warmup::cache_config` too; a strategy's config tests cover only its own
  fields and that typos are rejected through the flattened `bars`.
- Wiring: integration tests in `tests/` run the real strategy through
  `Backtest::run` on a small fixture catalog of 1-minute quotes, one run per
  test (one node per test thread, `tests/backtest/`). They test the warmup
  module too: the fixture's prices are chosen so that the warmup bars, the
  partial oldest bar, the bar at the run start and the session each change
  the orders and the final balance. The EMA's own tests run it on fixtures
  where each cross, stop loss, take profit and hold is worked out by hand
  (`backtest_ema_*.rs`, backtest README, "Tests"). A recording strategy in
  `tests/common` checks what the EMA cannot show: every price of the bars,
  orders of a given side and size, and positions on a netting and on a
  hedging venue (`backtest_netting_venue.rs`, `backtest_hedging_venue.rs`).
