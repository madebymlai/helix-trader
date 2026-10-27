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
them as `TickSeries`, a Nautilus data type of one instrument:

```toml
ticks = [
    { data = "quotes", instrument_id = "XAUUSD.ICMARKETS" },
    { data = "trades", instrument_id = "GC.v.0.GLBX" },
    { data = "depth", instrument_id = "GC.v.0.GLBX" },
]
```

`data` is `quotes` (`QuoteTick`), `trades` (`TradeTick`) or `depth`
(`OrderBookDepth10`). fill-catalog asks each source for them: OKX, IC and
IB fetch the declared series and refuse those they do not serve, while
Databento fetches a GLBX instrument's MBP-10 book and trades whatever is
declared. A backtest replays each as declared, one `BacktestDataConfig`
per series, beside the series the bars are built from; the strategy
subscribes to them itself. They have no warmup of their own: a strategy
reading ticks alone gets the default cache. Live sets up the clients of
their instruments, as of every instrument of the requirements, but
subscribes to none of them.

An instrument may be a futures root (`GC.GLBX`), listed in the roll
calendar, rather than a dated contract. `DataRequirements::slices(start,
end)` expands each catalog series the requirements read through its
instrument's transition table (`rolls/README.md`) into `SeriesSlice`s: the
series of each contract over the slice where it stands for the root. A
plain instrument's series is one slice over the span, on itself.

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
        warmup::start(self, &bar_types, warmup, self.warmup)
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
  `on_start` hands `warmup::start` the bar types and warmup of `bars` and a
  new request group id, stored first; the two `on_historical_*` handlers
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
from quotes keep no delay.

## Warmup and training

This is the single description of warmup; the backtest and live READMEs only
say how to register the catalog that serves it.

Warmup is the history a strategy needs before it trades: indicators to
initialise and models to train. The module `strategy::warmup` is the one
place that knows Nautilus' warmup protocol for the bars it builds; no
strategy names the request params or reads the cache for its warmup. A
strategy makes the three calls shown in Wiring: `warmup::start(self,
&bar_types, warmup, group)` in `on_start`, which requests the warmup as the
request group `group` and subscribes to the bars; `warmup::bars(self,
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

A model is trained by the strategy itself, retrained on a rolling window of
the bars received in `on_bar`, every N bars as the config says. The model
lives only in memory, with no model files, so backtest and live run the
same code and nothing passes from backtest to live.

## Adding a strategy

1. Create `strategy/<name>/` following the layout above: a config embedding
   `BarsConfig` (see "Config"), the wiring calling `warmup` (see "Wiring"),
   and the strategy's own decision and orders.
2. Declare it in `strategy/mod.rs`.
3. In `main.rs`, add a variant to `enum Strategy` and an arm to each `match`
   on it (fill-catalog, backtest, live), reading the data requirements from
   `config.bars.data_requirements()`.

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
