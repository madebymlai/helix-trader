# Writing a strategy

A strategy implements Nautilus' `Strategy` trait; backtest and live do not depend on a concrete strategy. One process runs one strategy. Use [`ema/`](ema/) and the maintained [`config/ema.toml`](../../config/ema.toml) as the non-model example.

## Authoring

1. Create `strategy/<name>/` with a `Part` for strategy-specific config and rules, a constructor, thin Nautilus wiring, and pure decision modules. Make `Part::reads` answer exactly the series the strategy subscribes to and uses for indicators.
2. Register state as at least one Nautilus indicator in `DataActor::on_start`. Decide only when `self.core().indicators_initialized()?` is true; Nautilus feeds registered indicators before the strategy's handler. Keep decisions free of I/O and look-ahead, use the strategy clock rather than the system clock, and seed randomness from config.
3. Build a `Warmup` from the data requirements, feed the indicators through it, and forward historical responses and time events as needed. Use `strategy::orders::Trade` for trades and rearm open trades on start.
4. Declare the strategy in `strategy/mod.rs`, add its name to `main.rs`'s `Strategy` enum and handle it in each command's match. Non-model strategies refuse train and dataset commands. Test the pure decisions and config rules, then exercise wiring on a fixture backtest.

**Model strategies:** for features, decisions, labels, training, stores, meta-labeling or version bumps, read the [model guide](../model/README.md). `model_ema/` and [`config/model_ema.toml`](../../config/model_ema.toml) are the example. For run orchestration see [backtest](../backtest/README.md); for live operations see [live](../live/README.md).

The runtime calls `DataActor::on_start`, not `Strategy::on_start`; call the latter explicitly from the former if its behavior is needed, such as reactivating GTD timers.

## Configuration and series

`StandardConfig::load` parses and validates the strategy's TOML before a node starts. `StandardConfig` owns the common `[base]`, `[data]`, `[indicators]`, `[trade]` and, for model strategies, `[model]` tables; `Part` owns top-level strategy fields, instrument count, default base, declared indicators and additional rules. Unknown keys are rejected, except that Nautilus' `bar_spec` deserializer ignores extra keys. Parsing reports its first error; subsequent validation collects violations.

`[data].series` is exactly what `Part::reads` says the strategy reads: missing, extra and duplicate series are errors. The EMA reads only the first bars series of its traded instrument. `[trade].instruments` supplies instrument IDs and positive quantities. When no series reads a traded instrument, its quotes are added to data requirements for execution, not to `[data].series`. Consumers use `data_requirements()` rather than reconstructing requirements from config.

Declare each indicator name and argument type in `Part::INDICATORS`, build it from `config.indicators`, and register it for its input. Indicator tables not declared by the strategy are rejected. The shared config validates the declared arguments and strategy rules. A model strategy's features are declared the same way; its code hands each feature's table to the feature set rather than building it, and the TOML marks no role (see the [model guide](../model/README.md)). An argument type is `Deserialize` and `Serialize`, since a feature's arguments enter the fingerprint.

Bars are time-aggregated internal bars; `from` makes them composite from external source bars. Internal source bars are rejected because live warmup cannot request them. Tick series may be quotes, trades, book depth, book deltas or instrument-keyed custom data. Custom types must be registered before catalog replay; no fill-catalog source supplies custom data. The strategy subscribes to its own tick and book streams. Consult the [catalog guide](../catalog/README.md) for source availability and [backtest guide](../backtest/README.md) for replay.

A futures root's bars must be internal LAST bars built from its trades; orders must name a contract, not a root. Root tick subscriptions and continuous bars require roll handling; see [rolls](../rolls/README.md) for calendars and transitions.

## Bars and sessions

Internal BID, ASK and MID bars are built from quotes, LAST bars from trades. A bar stamped at close contains ticks in `(open, close]`; quote-bar volume sums quoted sizes, not trades. LAST bars carry no spread. Composite bars aggregate source bars, so their high and low reflect those source intervals. Backtest and live disable empty time bars: a period without a tick or source bar produces no bar rather than a stale flat bar.

A configured session accepts a bar only when its whole span lies within the local day's open and close in the configured IANA time zone. Outside bars neither update indicators nor trigger orders, including during warmup. Omit the session for 24/7 trading; `00:00`–`23:59` excludes the bar closing at midnight. Sessions are rejected with composite bars: composite bars are built and stamped five seconds after close to allow source bars to arrive live, in a backtest and a dataset run as live. Quote-built bars have no such delay, unless the strategy also reads composite bars: the delay is the data engine's, for every time bar it builds.

## Orders and positions

`Trade::send` rounds quantity to the instrument's size and rejects a zero result. Both price exits produce a bracket, neither produces a market entry, and a single exit is rejected. The time limit starts at send, not fill. Forward time events to `orders::on_time_event` and call `orders::arm_open_trades` on start so cached open trades regain their alerts after a restart. The alert cancels open orders and closes any remaining position; a partial exit does not end the remaining trade.

Leave `oms_type` unset so positions follow the venue. On a netting venue overlapping trades share a position and one trade's time exit closes the whole position, including the other trades' share. On a hedging venue each trade has its own position. The EMA is an example and permits overlap. Its bracket legs are reduce-only; the IB execution client denies them, so the EMA cannot trade live on IB. See [IB](../venues/ib/README.md#trading-and-warmup) and [backtest](../backtest/README.md) for execution limitations.

## Warmup

Live requests history at start and hydrates indicators before the live stream. A backtest requests no warmup: it replays only its requested interval, and indicators initialize on that replay. Start earlier if decisions are needed at a particular time. A dataset build replays the features' warmup before its first day and stores no earlier decisions; see the [model guide](../model/README.md).

`strategy::warmup::Warmup` owns the request and subscription protocol. A composite bar requests its external source bars; other bars request the quotes or trades from which they are built. It feeds the built historical bars to indicators in chronological order, keeps the subscription continuous across the request boundary, and sizes the live cache for the requested bars. Root continuous bars have a separate path; live currently refuses them, as described in [rolls](../rolls/README.md).

A completed request does not itself enable decisions: indicator initialization does. Historical callback errors are logged rather than stopping the run, and an unanswered request leaves its group unfinished. The catalog supplies available history and the live client supplies missing intervals where supported; see [live](../live/README.md) for operational details. Recursive indicators retain a decaying dependence on their initial state even after initialization; allow several periods of history when reproducibility matters.
