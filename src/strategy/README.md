# Writing a strategy

A strategy is a Nautilus strategy and nothing more. Backtest and live take
any type that implements the Nautilus `Strategy` trait and never name a
concrete strategy. A process runs one strategy at a time; `main` picks it by
name.

APIs are those of the `nautilus-*` crates at `0.65.0`. Process-wide rules
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
instruments and the bar type. Nautilus has no universe type; `fill_catalog`,
backtest and live get the instruments and bar type from this config,
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
    /// The cadence: one bar type per instrument, all at the same spec.
    pub bar_spec: BarSpecification,
    /// How far back the warmup history starts.
    #[serde(default = "default_warmup")]
    #[builder(default = default_warmup())]
    pub warmup: SignedDuration,
    /// The hours traded: only bars wholly inside count.
    pub session: Session,
}
```

```toml
instrument_ids = ["ESH4.CME", "ESM4.CME"]
bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
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
  `open` and `close` as `jiff::civil::Time` (`"08:30"`).
- Loading then validates what serde cannot: at least one instrument, all on
  one venue, a bar spec the adapter can serve and aggregated on time (the
  session needs the bar's length), a session opening before it closes.

The config builds the bar types the strategy uses, one per instrument:
`BarType::new(id, bar_spec, AggregationSource::External)`.

The config also states what the strategy needs from data, as one
`strategy::DataRequirements`: its instruments and its warmup.
`fill_catalog`, backtest and live read them from it, never from the
config's fields:

```rust
pub fn data_requirements(&self) -> DataRequirements {
    DataRequirements {
        instrument_ids: self.instrument_ids.clone(),
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
        let start = self.clock().utc_now().checked_sub(self.config.warmup)?;
        for bar_type in self.config.bar_types() {
            self.request_bars(bar_type, Some(start), None, None, None, None)?;
            self.subscribe_bars(bar_type, None, None);
        }
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        self.book.warm_up(bars);
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

## Warmup and training

This is the single description of warmup; the backtest and live READMEs only
say how to register the catalog that serves it.

Warmup is the history a strategy needs before it trades: indicators to
initialise and models to train. A model is trained by the strategy itself.

- **Request.** In `on_start`:
  `request_bars(bar_type, start, end, limit, client_id, params) ->
  anyhow::Result<UUID4>`. `start` and `end` are `Option<jiff::Timestamp>`
  and must not be after the clock's now; `limit` is
  `Option<NonZeroUsize>`. Composite bar types are rejected.
- **Response.** The bars arrive in one batch in
  `on_historical_bars(&mut self, bars: &[Bar])`, before the first `on_bar`.
  An `Err` returned from it is logged, not propagated: a failed warmup does
  not stop the run, so the strategy must notice it. An empty response
  still calls it, with an empty slice; an unanswered request never does.
  Do not trade until warmup has actually arrived.
- **Who answers.** The data engine serves the request from a registered
  catalog and asks the data client only for the intervals the catalog lacks.
  In backtest the client answers nothing, so the catalog must be registered
  and cover the whole window, or no response comes (see the backtest
  README). In live it can be a catalog, the adapter, or both (see the live
  README).
- **Bound it with `start`.** `self.clock().utc_now()` is the simulated time
  in backtest. Do not rely on `limit`: the catalog path ignores it, so with
  `start: None` the response holds every bar since the epoch. Do not assume
  the slice length either.
- **Retrain** on a rolling window of the bars received in `on_bar`, every N
  bars as the config says.
- **No model files.** The model lives only in memory, so backtest and live
  run the same code and nothing passes from backtest to live.

## Rules

- **The config is the universe.** Instruments and bar type are config
  values, never constants in code.
- **One cadence.** One bar type per instrument, all at the same cadence. For
  continuous futures, subscribe to the `INTERNAL` bar type with the roll
  params from `data::rolls`.
- **Whole book.** On each bar, compute the target for every instrument and
  rebalance towards it. No other strategy shares the account.
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
  closing 10:00 to 17:00 count. `Session` lives in the EMA folder until a
  second strategy needs it.
- **Log through `log`** macros.

## Adding a strategy

1. Create `strategy/<name>/` following the layout above.
2. Declare it in `strategy/mod.rs`.
3. Add its name to the `--strategy` match in `main.rs`.

## Tests

- Pure modules: unit tests in a `#[cfg(test)] mod tests` at the bottom of
  each file.
- Wiring: one integration test file per strategy in `tests/`, running it
  through a `BacktestNode` on a small fixture catalog.
