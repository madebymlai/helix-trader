# Backtesting

Backtest runs one strategy in a Nautilus `BacktestNode` over the configured catalog. See the [root quick start](../../README.md), command `--help`, and [architecture](../../docs/ARCHITECTURE.md) for process constraints. A commission (fixed per order or rate per fill) and starting balance are required; choose values appropriate to the venue and instrument. Seed strategy randomness.

## Replay and data

The inclusive `[start, end]` window is the entire replay: a backtest makes no warmup request. Start early enough for indicators to initialize. An off-boundary start can produce a partial first bar. Catalog series feed both the strategy's bars and the venue's fills; declared tick and custom series are replayed too. Stored book depth and funding are included where applicable. Roots expand into dated-contract slices; see [rolls](../rolls/README.md) for boundaries and continuous-bar restrictions.

The catalog is not checked for completeness before a run. Missing instruments can fail the node, but missing series may only produce a warning; partial coverage and gaps replay as quiet periods. Stored intervals are request bookkeeping, not proof of completeness. Inspect coverage before trusting results, including the earlier data needed for model dataset warmup. No data for an hour means no bar, not a synthetic flat bar.

Ordinary replay streams data in chunks. A run with custom data loads **all** series for the window at once because Nautilus' streaming node cannot retain the preloaded custom data across chunks. Large depth histories can therefore exhaust memory; reduce the window or data volume.

## Fills and costs

The default fill model adds no slippage. External LAST bars, including IB minute TRADES bars and OKX candles, drive fills without a quoted spread. A market order on a decision bar can fill at the last minute's close; the assumed open-high-low-close sequence determines which resting order fills first within a minute. This is not observed intrabar order and can be optimistic. Minute volume can limit the amount filled at the close, with the remainder one tick worse; thin or zero-volume periods are especially unreliable.

On quotes, buys fill at the ask and sells at the bid of the last quote. Resting orders are checked only on available quotes, so a crossing between quotes can be missed. A gap can leave a stale quote in use; without an earlier quote an order is rejected. Quote sizes may be nominal rather than observed depth, and liquidity is not consumed across orders. Tick quotes give more detail than minute bars but do not remove these limitations.

Fixed commission is charged on an order's first fill; a rate is charged on every fill. Neither combines a rate with a minimum, so choose the approximation carefully for IB orders. Stored OKX funding is settled on open positions; absent funding is not checked or settled. The simulated settlement price and timing can differ from OKX, including orders placed at a funding boundary. See [OKX funding](../venues/okx/README.md#funding-and-margins) for availability.

## Leverage and venues

Each simulated venue starts with the specified balance and uses its live position mode: IC Markets is hedging, other venues netting. The same balance, leverage and fee model apply to every venue in a multi-venue run.

The margin model applies both the instrument margin and `--leverage` (default 1). IC instruments already carry the venue's margin: use 1x unless deliberately modelling a further division. OKX instruments have margin 1, so set leverage to the account's per-instrument leverage. IB instruments have zero margins and lock no margin in this simulation. Orders exceeding free initial margin can be denied, but liquidation is disabled; the simulation does not reproduce venue liquidations.

## Report

PnL is realized and after commissions, including funding; open positions can leave unrealized PnL outside that figure. Costs are derived from fills and the last quote at or before each fill. After the run the report reads the replayed quotes from the catalog again, a batch at a time, so it holds no quote history in memory. Spread is the fill's distance from the quote mid, times quantity and multiplier; with no replayed quotes it is `n/a`, not zero proven spread. Cost per trade divides total commission and measured spread by fills. Gross PnL adds both back to realized PnL, so an open position's opening spread makes it too high. Inverse instruments are refused by this calculation. Max drawdown is a negative fraction; some statistics can be unavailable with insufficient observations.

## Model retraining

For model strategies, `--models` selects the shared store and `--dataset-balance` supplies the balance for target-generating dataset runs; make it large enough that target orders are not denied. Before replay, the backtest builds missing dataset days and launches training in child processes on the epoch-aligned retraining schedule, starting at the last scheduled time at or before the run. Existing models valid at a scheduled time are reused: a train there fits only the models lacking one, and none runs where every model has one (see [training](../model/README.md#training-and-acceptance)). A failed dataset build stops the backtest; training that keeps the current model reports why, continues, and will be retried on a later run. A strategy never retrained, without `retrain` in `[model]`, trains at the backtest's start alone, where a model is missing; a later backtest of the span, or from a later start, trains nothing. It then refuses to run, naming the primary or the meta, when either has no model in use at the start, since it would trade nothing; a retrained backtest whose trains were all refused still runs. Its scores cover the span as one period (see [never retrained](../model/README.md#training-and-acceptance), `a_backtest_never_retrained_with_no_primary_model_at_its_start_is_refused_naming_it`, `a_backtest_never_retrained_scores_the_span_as_one_period`).

Each decision uses the latest valid model, subject to its checks. Scores after the report compare out-of-sample predictions and economics against baselines, including meta-model scores where configured; periods without a model or enough observations have unavailable statistics. Backtest models are valid from their scheduled training time, whereas live models become valid only after training finishes, introducing some optimism. See [model guidance](../model/README.md) for datasets, validity and authoring, and [live operations](../live/README.md) for live retraining.

## Tests

Run `cargo test --test backtest` for integration coverage; unit tests live with their modules. The [root README](../../README.md) describes the full test command and live venue gates.
