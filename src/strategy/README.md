# Writing a strategy

A strategy is a Nautilus strategy and nothing more. Backtest and live take
any type that implements the Nautilus `Strategy` trait and never name a
concrete strategy. A process runs one strategy at a time; `main` picks it by
name.

APIs were read in the `nautilus-*` crates at `0.66.0`; the build uses the
`develop` commit in `docs/ARCHITECTURE.md` (Version). Process-wide rules
(one node per process, Nautilus owns the logger) are in
`docs/ARCHITECTURE.md`.

## Layout

Each strategy is one folder, and everything about it lives there:

```
strategy/<name>/
  mod.rs        Nautilus wiring, kept thin
  config.rs     the config: every parameter of the strategy
  book.rs       the pure core: from state to the target book
  ...           more pure modules as needed (features.rs, tree.rs, ...)
```

The folder exposes only the config and the constructor.

## Config

The strategy config is the single source of what the strategy trades: the
instruments and the bar spec. Nautilus has no universe type; `fill_catalog`,
backtest and live get the instruments and the warmup from this config,
through `main`, as Nautilus types.

It is a plain struct holding the Nautilus `StrategyConfig` plus every
parameter of the strategy, model hyperparameters included. It derives
`serde::Deserialize` and is loaded from a TOML file given on the command
line; `StrategyConfig`, `InstrumentId` and `BarType` all deserialize, and a
`BarType` is parsed with its checked parser, so a bad cadence such as
`60-MINUTE` fails at load. `bon::Builder` stays for tests.

```rust
#[derive(Debug, Clone, serde::Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct TrendConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    /// The contracts traded and stored in the catalog.
    pub instrument_ids: Vec<InstrumentId>,
    /// The bars: one internal bar type per instrument, all at the same
    /// spec, built from the quotes.
    pub bar_spec: BarSpecification,
    /// How far back the warmup history starts.
    #[serde(default = "default_warmup")]
    #[builder(default = default_warmup())]
    pub warmup: SignedDuration,
    /// The hours traded: only bars wholly inside count. Without one, as on
    /// a market open around the clock, every bar counts.
    #[serde(default)]
    pub session: Option<Session>,
}
```

```toml
instrument_ids = ["ESH4.CME", "ESM4.CME"]
bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
warmup = "720h"
session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
```

- `InstrumentId` and `BarType` deserialize from their strings.
- `BarSpecification` deserializes from its three fields, validated by the
  checked constructor, so `step = 60, aggregation = "MINUTE"` fails at load.
  The enums parse case-insensitively.
- `SignedDuration` (`jiff`, with its `serde` feature) parses a duration
  string such as `"720h"`.
- `Session` (`ema/session.rs`) holds a `jiff::tz::TimeZone`, read from its
  IANA name with `#[serde(with = "jiff::fmt::serde::tz::required")]` (an
  unknown name fails at load; `TimeZone` has no plain `Deserialize`), and
  `open` and `close` as `jiff::civil::Time` (`"08:30"`). The `session`
  line is optional: a config without it trades every bar, as a 24/7
  market such as crypto needs (no `open`/`close` pair expresses the whole
  day, since `00:00`-`23:59` still drops the bar closing at midnight).
- Loading then validates what serde cannot: at least one instrument, all on
  one venue, a bar spec aggregated on time (the session needs the bar's
  length) and, built from quotes, priced `BID`, `ASK` or `MID` (`LAST`
  fails at load), a session, if any, opening before it closes, and no
  session with `composite` (live composite bars are stamped 5 s after
  their close, so the session would drop its last bar live but not in a
  backtest; see below).

The config builds the bar types the strategy uses, one per instrument:
`BarType::new(id, bar_spec, AggregationSource::Internal)`, such as
`SXR8.IBIS-1-HOUR-MID-INTERNAL`. With the optional `composite` line the
bars are built from the instrument's source bars instead, a composite bar
type (`BarType::new_composite`, its `composite_*` arguments):

```toml
instrument_ids = ["BTC-USDT-SWAP.OKX"]
bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
```

gives `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`, the hour
bars of OKX's 1-minute candles, any price type (the source's), warmed up
on those candles (see "Warmup and training").

The config also states what the strategy needs from data, as one
`strategy::DataRequirements`: its bar types and its warmup; its
instruments derive from the bar types (`DataRequirements::instrument_ids`).
`fill_catalog`, backtest and live read them from it, never from the
config's fields:

```rust
pub fn data_requirements(&self) -> DataRequirements {
    DataRequirements {
        bar_types: self.bar_types(),
        warmup: self.warmup,
    }
}
```

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
}

impl Trend {
    pub fn new(config: TrendConfig) -> Self {
        Self {
            core: StrategyCore::new(config.base.clone()),
            book: Book::new(&config),
            config,
        }
    }
}

nautilus_strategy!(Trend);

impl DataActor for Trend {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let (bar_types, warmup) = (self.config.bar_types(), self.config.warmup);
        warmup::start(self, &bar_types, warmup)
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        self.book.warm_up(&warmup::bars(self, quotes));
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        self.book.warm_up(&warmup::bars(self, bars));
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        // Ask the pure core for the target book, then submit the orders
        // that move the current positions to it.
        Ok(())
    }
}
```

- **The macro.** It expects the core field to be named `core`; for another
  name pass it second, `nautilus_strategy!(Trend, strat_core)`. To override
  `Strategy` hooks, add a block: `nautilus_strategy!(Trend, { fn
  on_order_filled(..) {..} })`.
- **`config` vs `config()`.** The macro adds an inherent `config()` that
  returns the Nautilus `StrategyConfig`. A field named `config` does not
  collide with it, but `self.config` is then ours and `self.config()` is
  Nautilus's.
- **`on_start`.** The one the runtime calls is `DataActor::on_start`. The
  `Strategy` trait also declares one (it reactivates GTD timers); the Rust
  runtime does not call it.

### Orders and positions

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
- `self.portfolio().net_position(&instrument_id)` returns a signed
  `rust_decimal::Decimal`, zero when flat; `is_net_long`, `is_net_short`,
  `is_net_flat` take the same argument.
- Order and position events arrive in `Strategy` hooks, overridden in the
  macro block: `on_order_filled(&mut self, event: &OrderFilled)`,
  `on_order_rejected(&mut self, event: OrderRejected)`,
  `on_position_opened(&mut self, event: PositionOpened)`, and so on. The
  fill hook takes a reference; most others take the event by value.

## The pure core

All decisions live in pure modules: features, models, the target book. They
take bars and state and return values, do no I/O and know no engine, so
unit tests cover them without running Nautilus.

## Bars built from quotes

Strategies trade on bars Nautilus builds from quotes: internally aggregated
time bars priced `BID`, `ASK` or `MID`, such as
`SXR8.IBIS-1-HOUR-MID-INTERNAL`. Subscribing to one makes the data engine
aggregate the instrument's quotes into it, the replayed quotes in a backtest
and the broker's quotes live, so the same bar definition runs in both.

- **Closes.** A bar closes at the end of its period and is stamped there
  (`ts_event`); it holds the quotes stamped in `(open, close]`.
- **Hours without quotes.** An hour with no quote at all yields no bar.
  IB sends quotes all through the trading day, even for illiquid
  instruments, so an hour without any means the market is closed: a
  night, a weekend, a holiday. By default Nautilus would still build a
  bar, flat at the last close (`time_bars_build_with_no_updates = true`).
  The session checks the time of day only, so on a Saturday it would
  trade on Friday's last quote. The backtest and the live node both take
  their data engine config from `strategy::warmup` (`data_engine_config`,
  `live_data_engine_config`), which turns the flag off. Warmup bars follow
  the same rule.
- **Volume.** Quote-built bars sum the quoted sizes, not traded volume:
  nothing reads it.
- **Fidelity.** In a backtest the bars come from 1-minute quotes, so their
  high and low can be slightly narrower than live bars built from every
  quote; opens and closes match.

Bars of a composite bar type are built the same way from source bars
instead of quotes (OKX's candles; backtest README, "OKX on candles"). Live,
a venue sends a source bar only once it has closed: OKX's confirmed
1-minute candles arrived 0.3 to 1.5 s after their close on 2026-10-22. So
the live node builds time bars 5 s after their close when the strategy
has a composite bar type (`warmup::live_data_engine_config`, Nautilus'
`time_bars_build_delay`); otherwise the last minute would land in the
next bar. Such a live bar is stamped 5 s after its close, its backtest
twin on the close. Bars built from quotes keep no delay.

## Warmup and training

This is the single description of warmup; the backtest and live READMEs only
say how to register the catalog that serves it.

Warmup is the history a strategy needs before it trades: indicators to
initialise and models to train. A model is trained by the strategy itself.

The warmup of the bars Nautilus builds is the module `strategy::warmup`,
the one place that knows Nautilus' protocol for it. A strategy makes two
calls:

```rust
// In on_start: requests the warmup and subscribes to the bars.
warmup::start(self, &bar_types, self.config.warmup)?;

// In on_historical_quotes and on_historical_bars: the warmup bars of the
// response's instrument, oldest first.
let bars = warmup::bars(self, quotes);
```

- **What `start` requests.** Chosen from the bar type alone, never the
  venue. A composite bar type is built from its source bars: `start`
  requests those (`request_bars` of `BarType::composite`, such as
  `BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL`), and the response comes to
  `on_historical_bars`. Any other bar type is built from quotes: `start`
  requests the instrument's quotes (`request_quotes`), and the response
  comes to `on_historical_quotes`. Source bars that are themselves
  internal are refused (nothing stores them to request).
- **The params.** Each request runs from `now - warmup` with three params:
  `bar_types`, the internal bar types to aggregate the response into
  (Nautilus' `request_bars` refuses a composite bar type and points to this
  param); `update_subscriptions = true`, so that the aggregator goes on
  with the live data and the bar spanning the warmup end and the run start
  comes out once, complete, in `on_bar` (without it that bar comes out
  partial, or as a one-update bar when the run starts on a bar's close);
  `skip_first_non_full_bar = true`, so that the oldest bar, missing the
  data before `now - warmup`, is dropped. Then it subscribes to the bar
  types. `backtest_bars.rs`, `backtest_bars_on_close.rs` and
  `backtest_composite_warmup.rs` pin this.
- **What `bars` does.** The bars a request builds go to the cache only;
  they are there when the response arrives. `bars` reads the internal
  bars of the response's instrument from the cache (newest first there)
  and returns them oldest first. No strategy names the request params or
  reads the cache for its warmup.
- **Cache capacity.** The cache keeps `CacheConfig::bar_capacity` bars per
  bar type (default 10,000) and evicts the oldest beyond it, so a longer
  warmup would be cut silently. `warmup::cache_config(bar_types, warmup)`
  gives the node its cache config: the default, its bar capacity raised by
  the most bars the warmup builds of one bar type (warmup over bar length,
  plus the bar spanning the start), the default left on top as headroom.
  Past the cache's maximum of 1,000,000 it fails, naming the bar type, the
  bars needed and the capacity, and the run does not start. The backtest
  sets it on `BacktestEngineConfig::cache`, live on `LiveNodeConfig::cache`.
- **Response.** One response per request, usually before the first
  `on_bar`. An `Err` returned from `on_historical_quotes` or
  `on_historical_bars` is logged, not propagated: a failed warmup does not
  stop the run. An unanswered request never calls it. Do not trade until
  warmup has actually arrived.
- **Who answers.** The data engine serves the request from a registered
  catalog and asks the data client only for the intervals the catalog lacks.
  In backtest the client answers nothing, so the catalog must be registered
  and cover the whole window, or no response comes (see the backtest
  README). In live it can be a catalog, the adapter, or both (see the live
  README). OKX's adapter serves its candles up to now; it serves no quote
  history, so on OKX only composite bar types get a warmup.
- **Bound by `start`.** `self.clock().utc_now()` is the simulated time in
  backtest; `warmup::start` bounds the request with `now - warmup`.
- **`request_bars` of an internal bar type does not aggregate.** It does
  not build bars from quotes; only a request of their source with
  `bar_types` does.
- **Retrain** on a rolling window of the bars received in `on_bar`, every N
  bars as the config says.
- **No model files.** The model lives only in memory, so backtest and live
  run the same code and nothing passes from backtest to live.

## Rules

- **The config is the universe.** Instruments and bar spec are config
  values, never constants in code.
- **One cadence.** One internal bar type per instrument, all at the same
  spec, built from quotes and warmed up through `strategy::warmup`. For
  continuous futures, subscribe to the `INTERNAL` bar type with the roll
  params from `data::rolls`.
- **Whole book.** On each bar, compute the target for every instrument and
  rebalance towards it. No other strategy shares the account.
- **Orders.** The EMA strategy decides on each bar's close and sends a
  market order to reach its target position. Other strategies may use
  limit or stop orders instead. The backtest checks those against each
  1-minute quote (see Fills in the backtest README), so a price touched
  and left within the same minute is missed: simulating them well needs
  tick quotes.
- **Time and randomness from Nautilus.** Time from `self.clock()`
  (`timestamp_ns()`, `utc_now()`, timers, alerts); randomness seeded from
  the config. Never the system clock: in backtest it is not simulated time.
- **No look-ahead.** Decisions use only bars already received.
- **Trade inside the session.** A bar counts only if its whole span,
  `[ts_event - bar length, ts_event]`, lies inside `[open, close]` on the
  local day of its close, in the session's time zone, so daylight saving
  keeps the local hours. Bars outside are ignored: they do not move the
  indicators, warmup included, and get no target, so no order. The pure
  core applies the rule on the bar's timestamp, never the clock, so
  backtest and live agree. With hourly bars and 09:00-17:30, the bars
  closing 10:00 to 17:00 count. Without a session every bar counts, the
  one spanning midnight included: Nautilus has no session concept of its
  own, and venues trading around the clock simply apply no filter. Closures
  and maintenance windows still yield no bar, since an hour without quotes
  builds none (see Hours without quotes). `Session` lives in the EMA
  folder until a second strategy needs it.
- **Log through `log`** macros.

## Adding a strategy

1. Create `strategy/<name>/` following the layout above.
2. Declare it in `strategy/mod.rs`.
3. Add its name to the `--strategy` match in `main.rs`.

## Tests

- Pure modules: unit tests in a `#[cfg(test)] mod tests` at the bottom of
  each file.
- Wiring: integration tests in `tests/` run the real strategy through
  `Backtest::run` on a small fixture catalog of 1-minute quotes, one run per
  file (one node per process). The warmup module is tested through them:
  the fixture's prices are chosen so that the warmup bars, the partial
  oldest bar, the bar at the run start and the session each change the
  orders and the final balance. What the EMA cannot show, every price of
  the bars and stop orders, a recording strategy in `tests/common` checks.
  The cache size of the warmup, `warmup::cache_config`, is unit tested.
