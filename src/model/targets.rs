//! The targets of a strategy's decisions, read from real orders on
//! Nautilus' simulated venue.
//!
//! In a dataset run, at each decision `Targets::open` sends, for each
//! target of the config and on each side, a long and a short, the orders
//! whose outcome is the target, on a hedging margin account so that each
//! is a position of its own, each the trade a strategy trading on a model
//! of that target would place, sent through the shared order module
//! (`strategy::orders::Trade::send_as`, the one sending of both):
//!
//! - a triple barrier (`Barrier`): a bracket, a market entry, a take-profit
//!   limit and a stop, the two exits one-updates-the-other, closed at the
//!   limit after its send;
//! - a horizon: a market entry closed at the horizon after its send.
//!
//! The strategy hands its time events to the order module
//! (`orders::on_time_event`), which at a trade's time limit cancels its
//! orders still open and closes its position if still open, tagged as a
//! time close (`orders::is_time_close`). Nautilus' IB execution client denies the
//! reduce-only orders a bracket's legs and `close_position` are (see
//! `live/ib/README.md`): a model strategy cannot trade live on IB.
//!
//! The exits are `take_profit` and `stop` away from the decision's price
//! (`reference`): the mid of the instrument's last quote, else its last
//! trade, else the close of its last bar. Each distance is relative
//! (`Distance`): basis points of that price, or multiples of an average
//! true range, Nautilus' `AverageTrueRange`, of `DEFAULT_ATR`
//! unless the barrier gives its arguments, built by
//! Nautilus' `AverageTrueRange::new` as the config is validated, so that
//! an argument Nautilus refuses panics at load. Its bars are Nautilus' bars of
//! the instrument at the decision cadence, so a decision reads the bar
//! closing at its time, built from what the strategy reads of it
//! (`TargetsConfig::atr_bars`): the mids of its quotes, else the last
//! prices of its trades, else, of its source bars alone, a composite bar
//! type built from them. A backtest replays a source bar stamped at its
//! close before the timer closing the bar of the cadence at that instant,
//! so that bar holds it. The targets own the ATR, and `Targets::start`
//! registers it and subscribes to its bars, which no strategy declares. A
//! decision at which that ATR is not yet initialized opens no target, as a
//! decision on cold features is none. Each exit is on the instrument's
//! nearest tick (Nautilus' tick grid), a tie away from the decision's
//! price; an exit on the decision's price, no distance (a stop worth
//! nothing, a P&L in R of none), opens no target either.
//!
//! Each order's client order id names its decision, target, side and leg,
//! `D{n}-{B|H}{L|S}-{E|TP|SL}`: the decision's number in the run, `B` a
//! barrier and `H` a horizon, `L` long and `S` short, the entry, take
//! profit or stop. The outcome of a target is read from Nautilus' events
//! alone: a closed position's opening order names its decision and side,
//! its closing order the exit (a take profit, a stop, or a time close), and Nautilus reports its realised P&L net of commission,
//! the position the commissions it is net of. The only state of our own
//! is the rows being assembled.
//!
//! Once every target of a decision has an outcome, the decision is settled
//! and its row, a `TargetRow`, published on the message bus, custom data
//! keyed by the strategy's id (`TargetRow::data_type`). A barrier's
//! outcomes carry the distances its decision set on each side
//! (`Distances`), in price from the decision's price: what makes a
//! trade's P&L a multiple of its stop. A strategy trading on a model of
//! the barrier sets the same exits at its decisions (`Stops`), and
//! `stop_worth` makes a stop's distance money. A decision whose
//! order the risk engine denies or the venue rejects, or whose entry has
//! not filled by its time limit (the module cancels it then, and the
//! strategy hands the targets the cancel, `on_order_canceled`), is
//! unsettled: logged, and no row. So is a
//! decision whose positions are still open when the run stops.
//!
//! A quote at the same nanosecond as a time alert reaches the venue first,
//! so a barrier hit at the time limit counts as inside the window. On bars
//! alone the venue replays each bar's open, high, low and close, fills an
//! entry at the last close, a take profit at its limit and a stop at its
//! trigger: bars-only targets are as optimistic as bars-only backtests.

use std::{
    any::Any,
    cell::RefCell,
    cmp::Ordering,
    collections::BTreeMap,
    fmt::{self, Debug, Display},
    rc::Rc,
    sync::Arc,
};

use anyhow::{Context, bail, ensure};
use jiff::SignedDuration;
use nautilus_common::{
    actor::{
        DataActor, DataActorNative,
        indicators::{ActorIndicator, SharedActorIndicator},
    },
    cache::CacheApi,
    config::{ConfigError, ConfigErrorCollector, ConfigResult},
};
use nautilus_core::UnixNanos;
use nautilus_indicators::{indicator::Indicator, volatility::atr::AverageTrueRange};
use nautilus_model::{
    data::{
        Bar, BarSpecification, BarType, CustomData, CustomDataTrait, DataType, HasTsInit,
        NautilusDataType, QuoteTick, TradeTick,
    },
    enums::{AggregationSource, BarAggregation, OrderSide, PriceType},
    events::{OrderCanceled, OrderDenied, OrderRejected, PositionClosed},
    identifiers::{ClientOrderId, InstrumentId, StrategyId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    types::{Money, Price, Quantity, fixed::FIXED_PRECISION},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{
    decide::{DecisionRule, FeatureRow},
    features::Fingerprint,
};
use crate::strategy::{
    AtrArgs, DataRequirements, IndicatorArgs, TradedInstrument,
    orders::{self, Exits, OrderIds, Trade},
    warmup::Warmup,
};

/// The targets of each decision, from the strategy's config: on one
/// instrument, of one size, a triple barrier, a horizon or both. The
/// strategy's TOML has the targets alone (`TargetsTable`), on the
/// instrument and of the size of its `[trade]` table (`TargetsTable::trading`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct TargetsConfig {
    /// The instrument the targets trade.
    pub instrument_id: InstrumentId,
    /// The size of every order, rounded to the instrument's sizes.
    pub quantity: Quantity,
    /// The triple barrier, if any.
    pub barrier: Option<Barrier>,
    /// The horizon of the horizon target, if any, such as `"1h"`.
    pub horizon: Option<SignedDuration>,
}

/// The targets as a model table of a strategy's TOML has them, `targets = {
/// horizon = "1h", barrier = { .. } }`: what they trade is the strategy's
/// `[trade]`, named once.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetsTable {
    /// The triple barrier, if any.
    pub barrier: Option<Barrier>,
    /// The horizon of the horizon target, if any, such as `"1h"`.
    pub horizon: Option<SignedDuration>,
}

impl TargetsTable {
    /// The targets trading `traded`: its instrument, its size.
    #[must_use]
    pub fn trading(self, traded: &TradedInstrument) -> TargetsConfig {
        TargetsConfig {
            instrument_id: traded.instrument_id,
            quantity: traded.trade_size,
            barrier: self.barrier,
            horizon: self.horizon,
        }
    }
}

/// A triple barrier: the exits of a bracket, each a distance from the
/// decision's price, and its time limit, such as `{ take_profit = { atr =
/// 2.0 }, stop = { bps = 20 }, limit = "2h" }`, or with the ATR's
/// arguments given, `{ .., atr = { period = 20, ma_type = "WILDER" } }`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Barrier {
    /// How far the take profit is, in the side's favour.
    pub take_profit: Distance,
    /// How far the stop is, against the side.
    pub stop: Distance,
    /// How long after the decision what is still open is closed.
    pub limit: SignedDuration,
    /// The average true range the `atr` distances are multiples of, if
    /// not `DEFAULT_ATR`: only with a distance in ATRs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atr: Option<AtrArgs>,
}

/// How far an exit is from the decision's price, relative to it or to the
/// market's range, one unit to a distance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Distance {
    /// Basis points of the decision's price, `{ bps = 20 }`.
    #[serde(rename = "bps")]
    BasisPoints(Decimal),
    /// Multiples of the barrier's average true range at the decision,
    /// `{ atr = 1.5 }`.
    #[serde(rename = "atr")]
    Atr(Decimal),
}

/// The average true range of a barrier without an `atr` table: Nautilus'
/// `AverageTrueRange` of Wilder's 14 bars, its other arguments Nautilus'
/// defaults. An `atr` table gives Nautilus' arguments by their names
/// (`AtrArgs`), `{ period = 20, ma_type = "WILDER", use_previous = true,
/// value_floor = 0.0 }`; its bars are the decision cadence's (`AtrBars`).
pub const DEFAULT_ATR: AtrArgs = AtrArgs {
    period: 14,
    ma_type: None,
    use_previous: None,
    value_floor: None,
};

/// The units of the ATR's bars, longest first, each its length: those of
/// a fixed length, which a cadence, or the source bars of a composite bar
/// type, is a whole number of.
const ATR_UNITS: [(BarAggregation, SignedDuration); 5] = [
    (BarAggregation::Day, SignedDuration::from_hours(24)),
    (BarAggregation::Hour, SignedDuration::from_hours(1)),
    (BarAggregation::Minute, SignedDuration::from_mins(1)),
    (BarAggregation::Second, SignedDuration::from_secs(1)),
    (BarAggregation::Millisecond, SignedDuration::from_millis(1)),
];

/// The average true range a barrier runs: `config`, on bars of
/// `bar_type`, bars of the targets' instrument at the decision cadence,
/// so that a decision reads the bar closing at its time
/// (`TargetsConfig::atr_bars`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtrBars {
    pub bar_type: BarType,
    pub args: AtrArgs,
}

/// The target a model learns: whether its side of the barrier, or of the
/// horizon, gains after its costs, `"barrier"` or `"horizon"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Label {
    Barrier,
    Horizon,
}

/// How a target's position was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Exit {
    TakeProfit,
    Stop,
    /// At its time limit, or at the horizon.
    Time,
}

/// The outcome of a target on one side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Outcome {
    /// The position's realised P&L, net of its commissions, as Nautilus
    /// reports it.
    pub pnl: Money,
    /// The position's commissions in the P&L's currency, those its
    /// realised P&L is net of (Nautilus' `Position::commissions`).
    pub commission: Money,
    pub exit: Exit,
    /// The barrier the decision set on the side; none for a horizon.
    pub distances: Option<Distances>,
}

/// How far a barrier's exits on one side are from the decision's price,
/// in price, as the decision set them: on the instrument's ticks, the
/// basis points or ATRs resolved at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Distances {
    pub take_profit: Decimal,
    pub stop: Decimal,
}

/// The outcomes of a target on each side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Outcomes {
    pub long: Outcome,
    pub short: Outcome,
}

/// A settled decision: its features and the outcome of each of its
/// targets, published on the message bus.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TargetRow {
    /// The decision, as `decide` published it.
    pub decision: FeatureRow,
    /// The triple barrier's outcomes, if the config has one.
    pub barrier: Option<Outcomes>,
    /// The horizon target's outcomes, if the config has one.
    pub horizon: Option<Outcomes>,
    /// When the last of its positions closed.
    pub settled_at: UnixNanos,
}

/// The targets of a strategy's decisions, shared with it as an `Rc`: the
/// strategy calls `open` at each decision, and hands its order and
/// position events and its time events to the methods of their names.
pub struct Targets {
    config: TargetsConfig,
    /// The barrier's average true range, if a distance is in ATRs.
    atr: Option<Rc<BarrierAtr>>,
    state: RefCell<State>,
}

/// What the targets keep between events.
#[derive(Default)]
struct State {
    /// The number of the next decision.
    next: u64,
    /// The decisions opened and not yet settled or unsettled, by number,
    /// each with the outcomes it has.
    open: BTreeMap<u64, Decision>,
}

/// An open decision.
struct Decision {
    row: FeatureRow,
    /// The barrier set on each side, if the config has one.
    distances: BTreeMap<Side, Distances>,
    outcomes: BTreeMap<(Kind, Side), Outcome>,
}

/// A target of a decision on one side: what its orders' ids name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Target {
    decision: u64,
    kind: Kind,
    side: Side,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Kind {
    Barrier,
    Horizon,
}

/// A side of a target, and of a model: the long or the short, `"long"` or
/// `"short"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Long,
    Short,
}

/// A barrier as a decision sets it on one side (`Stops::at`): its take
/// profit and stop on the instrument's ticks, the price they are set from,
/// and what the stop is worth in money.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarrierExits {
    pub reference: Price,
    pub take_profit: Price,
    pub stop: Price,
    pub stop_worth: f64,
}

/// An order of a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Entry,
    TakeProfit,
    Stop,
}

impl TargetsConfig {
    /// # Errors
    ///
    /// Returns every violation, each Nautilus' `ConfigError` under its
    /// field of `[model]` (`model.targets.…`), as one
    /// `ConfigError::Multiple` if there are several: no target, the size, a
    /// barrier's distance, its limit or the horizon not positive, an `atr`
    /// table no distance uses, or an ATR without bars at the cadence of
    /// `rule` built from what a strategy of `requirements` reads (see
    /// `atr_bars`).
    ///
    /// # Panics
    ///
    /// Panics if Nautilus refuses an argument of the barrier's ATR as it
    /// builds it (`AverageTrueRange::new`).
    pub fn validate(
        &self,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();
        errors.check(
            self.barrier.is_some() || self.horizon.is_some(),
            ConfigError::invalid_value(
                "model.targets",
                "no target: neither a barrier nor a horizon",
            ),
        );
        errors.check(
            self.quantity.is_positive(),
            ConfigError::invalid_value("model.targets", "the target quantity must be positive"),
        );
        if let Some(barrier) = self.barrier {
            errors.check(
                barrier.take_profit.value() > Decimal::ZERO && barrier.stop.value() > Decimal::ZERO,
                ConfigError::invalid_value(
                    "model.targets.barrier",
                    "the barrier's take profit and stop must be positive",
                ),
            );
            errors.check(
                barrier.limit.is_positive(),
                ConfigError::invalid_value(
                    "model.targets.barrier.limit",
                    format!("the barrier's limit {:#} is not positive", barrier.limit),
                ),
            );
            errors.check(
                barrier.uses_atr() || barrier.atr.is_none(),
                ConfigError::invalid_value(
                    "model.targets.barrier.atr",
                    "the barrier has an ATR no distance uses",
                ),
            );
            let atr = |result: anyhow::Result<()>| {
                result.map_err(|e| {
                    ConfigError::invalid_value("model.targets.barrier.atr", format!("{e:#}"))
                })
            };
            if let Some(args) = barrier.atr_args() {
                // Nautilus checks the arguments as it builds the ATR.
                errors.collect(atr(args.build().map(drop)));
            }
            errors.collect(atr(self.atr_bars(rule, requirements).map(drop)));
        }
        if let Some(horizon) = self.horizon {
            errors.check(
                horizon.is_positive(),
                ConfigError::invalid_value(
                    "model.targets.horizon",
                    format!("the horizon {horizon:#} is not positive"),
                ),
            );
        }
        errors.into_result()
    }

    /// The barrier's average true range deciding on `rule`, for a strategy
    /// of `requirements`, if a distance is in ATRs: its period, and its
    /// bars, Nautilus' internal bars of the targets' instrument at the
    /// decision cadence, built from what the strategy reads of it, the
    /// first of:
    ///
    /// - its quotes: their mids, `XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL`
    ///   every 15 minutes;
    /// - its trades: their last prices, `..-15-MINUTE-LAST-INTERNAL`;
    /// - its source bars (`DataRequirements::source_bars`): a composite bar
    ///   type built from them, priced as they are,
    ///   `..-15-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL`; of several, the
    ///   longest whose length the cadence is a whole multiple of, the first
    ///   in their order at a tie.
    ///
    /// Quotes and trades read through bars built from them count. The step
    /// is the cadence in its largest whole unit, from days to
    /// milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus builds no bars of the cadence (a step
    /// must divide the next unit, `BarSpecification::new_checked`, so that
    /// bars close on the decisions' grid), the strategy reads neither
    /// quotes, trades nor bars of the instrument, or, on bars alone, the
    /// cadence is a whole multiple of none of them.
    pub fn atr_bars(
        &self,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<Option<AtrBars>> {
        let Some(config) = self.barrier.and_then(|barrier| barrier.atr_args()) else {
            return Ok(None);
        };
        let DecisionRule::Every(cadence) = rule;
        let nanos = cadence.as_nanos();
        let Some((aggregation, step)) = ATR_UNITS
            .into_iter()
            .find(|(_, unit)| nanos % unit.as_nanos() == 0)
            .map(|(aggregation, unit)| (aggregation, nanos / unit.as_nanos()))
        else {
            bail!(
                "the barrier's ATR has no bars of the decision cadence {cadence:#}, not whole \
                 milliseconds"
            );
        };
        let spec = |price_type| {
            usize::try_from(step)
                .map_err(anyhow::Error::from)
                .and_then(|step| BarSpecification::new_checked(step, aggregation, price_type))
                .with_context(|| {
                    format!("the barrier's ATR has no bars of the decision cadence {cadence:#}")
                })
        };
        let traded = self.instrument_id;
        let series = requirements.catalog_series();
        let reads = |data| series.contains(&(data, traded.to_string()));
        let bar_type = if reads(NautilusDataType::QuoteTick) {
            BarType::new(traded, spec(PriceType::Mid)?, AggregationSource::Internal)
        } else if reads(NautilusDataType::TradeTick) {
            BarType::new(traded, spec(PriceType::Last)?, AggregationSource::Internal)
        } else {
            let sources: Vec<BarType> = requirements
                .source_bars()
                .into_iter()
                .filter(|source| source.instrument_id() == traded)
                .collect();
            ensure!(
                !sources.is_empty(),
                "the barrier's ATR has no bars of {traded} to be built from: the strategy reads \
                 neither its quotes, its trades nor its bars"
            );
            let Some(source) = longest_dividing(&sources, cadence) else {
                let sources: Vec<String> = sources.iter().map(ToString::to_string).collect();
                bail!(
                    "the barrier's ATR has no bars of the decision cadence {cadence:#}: it is \
                     not a whole multiple of the bars of {traded} the strategy reads, {}",
                    sources.join(", ")
                );
            };
            let of = source.spec();
            BarType::new_composite_checked(
                traded,
                spec(of.price_type)?,
                AggregationSource::Internal,
                of.step.get(),
                of.aggregation,
                source.aggregation_source(),
            )?
        };
        Ok(Some(AtrBars {
            bar_type,
            args: config,
        }))
    }

    /// The config as the fingerprint reads it, its barrier's ATR `atr` as
    /// it runs: Nautilus' ATR as built, its arguments given or Nautilus'
    /// defaults, and its bars, whose source changes the rows.
    fn fingerprint_part(&self, atr: Option<AtrBars>) -> anyhow::Result<String> {
        let atr = atr
            .map(|atr| anyhow::Ok(format!("{} on bars of {}", atr.args.build()?, atr.bar_type)))
            .transpose()?;
        let barrier = self.barrier.map(|b| {
            format!(
                "take profit {} stop {} limit {} ns atr {atr:?}",
                b.take_profit,
                b.stop,
                b.limit.as_nanos()
            )
        });
        let horizon = self.horizon.map(|horizon| horizon.as_nanos());
        Ok(format!(
            "{} {} barrier {barrier:?} horizon {horizon:?} ns",
            self.instrument_id, self.quantity
        ))
    }

    /// The longest time a decision's targets stay open: the barrier's
    /// limit or the horizon. A decision is settled by then: a time close
    /// fills at once, on the venue's last quote at the alert's own time.
    #[must_use]
    pub fn settles_within(&self) -> SignedDuration {
        self.kinds()
            .map(|(_, after)| after)
            .max()
            .unwrap_or(SignedDuration::ZERO)
    }

    /// The names of the values of a row (`TargetRow::values`), in their
    /// order: for the barrier, then the horizon, the long then the short,
    /// each its P&L, its commission and its exit, and a barrier's the
    /// distances of its take profit and its stop, such as
    /// `barrier_long_pnl`, `barrier_long_commission`, `barrier_long_exit`,
    /// `barrier_long_take_profit` and `barrier_long_stop`.
    #[must_use]
    pub fn target_names(&self) -> Vec<String> {
        self.kinds()
            .flat_map(|(kind, _)| {
                Side::BOTH.into_iter().flat_map(move |side| {
                    kind.values()
                        .iter()
                        .map(move |value| format!("{}_{}_{value}", kind.name(), side.name()))
                })
            })
            .collect()
    }

    /// Where the P&L of `label`'s target on `side` is among the values of
    /// a row (`TargetRow::values`); none if the config has no such target.
    #[must_use]
    pub fn pnl_index(&self, label: Label, side: Side) -> Option<usize> {
        self.index(label, side, "pnl")
    }

    /// Where the commission of `label`'s target on `side` is among the
    /// values of a row (`TargetRow::values`); none if the config has no
    /// such target.
    #[must_use]
    pub fn commission_index(&self, label: Label, side: Side) -> Option<usize> {
        self.index(label, side, "commission")
    }

    /// Where the stop's distance of `label`'s target on `side` is among the
    /// values of a row (`TargetRow::values`); none if the config has no
    /// such target, or it is a horizon, which has no stop.
    #[must_use]
    pub fn stop_index(&self, label: Label, side: Side) -> Option<usize> {
        self.index(label, side, "stop")
    }

    /// Where `value` of `label`'s target on `side` is among the values of a
    /// row, if it has one.
    fn index(&self, label: Label, side: Side, value: &str) -> Option<usize> {
        let kind = match label {
            Label::Barrier => Kind::Barrier,
            Label::Horizon => Kind::Horizon,
        };
        let name = format!("{}_{}_{value}", kind.name(), side.name());
        self.target_names().iter().position(|of| *of == name)
    }

    /// The kinds of target of the config.
    fn kinds(&self) -> impl Iterator<Item = (Kind, SignedDuration)> {
        let barrier = self.barrier.map(|b| (Kind::Barrier, b.limit));
        let horizon = self.horizon.map(|h| (Kind::Horizon, h));
        barrier.into_iter().chain(horizon)
    }
}

impl Barrier {
    /// Whether a distance is in ATRs.
    fn uses_atr(&self) -> bool {
        [self.take_profit, self.stop]
            .iter()
            .any(|distance| matches!(distance, Distance::Atr(_)))
    }

    /// Its ATR, if a distance is in ATRs: its `atr` table's, else
    /// `DEFAULT_ATR`.
    fn atr_args(&self) -> Option<AtrArgs> {
        self.uses_atr().then(|| self.atr.unwrap_or(DEFAULT_ATR))
    }

    /// The take profit and stop of each side, the long then the short,
    /// from `reference` and, if a distance is in ATRs, the ATR's value
    /// `atr`, each on the nearest tick of `instrument`; none if an exit
    /// rounds onto `reference`, no distance (a stop's worth nothing, a P&L
    /// in R of none), so that the decision is no target, as one on a cold
    /// ATR.
    ///
    /// # Errors
    ///
    /// Returns an error if an exit is outside the instrument's prices.
    ///
    /// # Panics
    ///
    /// Panics if a distance is in ATRs and `atr` is none.
    fn exits(
        &self,
        instrument: &InstrumentAny,
        reference: Decimal,
        atr: Option<f64>,
    ) -> anyhow::Result<Option<Vec<(Side, Price, Price)>>> {
        let atr = atr
            .map(|atr| Decimal::try_from(atr).with_context(|| format!("the ATR {atr}")))
            .transpose()?;
        let take_profit = self.take_profit.length(reference, atr);
        let stop = self.stop.length(reference, atr);
        let exits = Side::BOTH
            .into_iter()
            .map(|side| {
                let favour = side.sign();
                Ok((
                    side,
                    nearest_tick(instrument, reference + favour * take_profit, favour)?,
                    nearest_tick(instrument, reference - favour * stop, -favour)?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let off_reference = exits.iter().all(|(_, take_profit, stop)| {
            take_profit.as_decimal() != reference && stop.as_decimal() != reference
        });
        Ok(off_reference.then_some(exits))
    }
}

impl Distance {
    /// The number of its unit.
    fn value(self) -> Decimal {
        match self {
            Self::BasisPoints(value) | Self::Atr(value) => value,
        }
    }

    /// The distance in prices from `reference`, the ATR being `atr`.
    fn length(self, reference: Decimal, atr: Option<Decimal>) -> Decimal {
        match self {
            Self::BasisPoints(bps) => reference * bps / Decimal::from(10_000),
            Self::Atr(multiple) => atr.expect("a validated barrier has its ATR") * multiple,
        }
    }
}

impl Display for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BasisPoints(bps) => write!(f, "{bps} bps"),
            Self::Atr(multiple) => write!(f, "{multiple} atr"),
        }
    }
}

/// Of `sources`, the longest bars whose length `cadence` is a whole
/// multiple of, the first at a tie; none if it is of none. Bars of no
/// fixed length (ticks, volumes, months) divide no cadence.
fn longest_dividing(sources: &[BarType], cadence: SignedDuration) -> Option<BarType> {
    let length = |source: &BarType| {
        let spec = source.spec();
        ATR_UNITS
            .iter()
            .any(|(aggregation, _)| *aggregation == spec.aggregation)
            .then(|| spec.timedelta())
    };
    sources
        .iter()
        .filter_map(|source| length(source).map(|length| (*source, length)))
        .filter(|(_, length)| cadence.as_nanos() % length.as_nanos() == 0)
        .fold(None, |longest, (source, length)| match longest {
            Some((_, kept)) if kept >= length => longest,
            _ => Some((source, length)),
        })
        .map(|(source, _)| source)
}

/// `price` on the nearer of `instrument`'s ticks around it, Nautilus' tick
/// grid; at a tie, the one `away` (1 up, -1 down) from the decision's
/// price, so that an exit is not nearer than its distance.
fn nearest_tick(
    instrument: &InstrumentAny,
    price: Decimal,
    away: Decimal,
) -> anyhow::Result<Price> {
    let id = instrument.id();
    let value = price
        .to_f64()
        .with_context(|| format!("the exit {price} of {id}"))?;
    let (Some(below), Some(above)) = (
        instrument.next_bid_price(value, 0),
        instrument.next_ask_price(value, 0),
    ) else {
        bail!("the exit {price} is outside the prices of {id}");
    };
    let (down, up) = (price - below.as_decimal(), above.as_decimal() - price);
    Ok(match down.cmp(&up) {
        Ordering::Less => below,
        Ordering::Greater => above,
        Ordering::Equal if away.is_sign_positive() => above,
        Ordering::Equal => below,
    })
}

/// The average true range of a barrier, registered with the strategy's
/// Nautilus indicators for the bars of its bar type.
struct BarrierAtr {
    bar_type: BarType,
    indicator: RefCell<AverageTrueRange>,
}

impl BarrierAtr {
    fn new(bars: AtrBars) -> anyhow::Result<Self> {
        Ok(Self {
            bar_type: bars.bar_type,
            indicator: RefCell::new(bars.args.build()?),
        })
    }

    /// Its value, once initialized.
    fn value(&self) -> Option<f64> {
        let indicator = self.indicator.borrow();
        indicator.initialized.then_some(indicator.value)
    }

    /// Registers it with `actor`'s Nautilus indicators for its bars and
    /// subscribes the actor to them (see `Targets::start`).
    fn start<A>(self: &Rc<Self>, actor: &mut A)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let shared = Rc::clone(self) as SharedActorIndicator;
        actor
            .core_mut()
            .register_indicator_for_bars(self.bar_type, shared);
        actor.subscribe_bars(self.bar_type, None, None);
    }
}

/// The exits a decision of a trading strategy sets on each side of the
/// barrier, as a dataset run's would: from the decision's price
/// (`reference`) and the barrier's ATR, on the instrument's ticks; and
/// its stop's worth in money (`stop_worth`), what an expected P&L in R is
/// weighed by.
pub struct Stops {
    instrument_id: InstrumentId,
    quantity: Quantity,
    barrier: Barrier,
    atr: Option<Rc<BarrierAtr>>,
}

impl Stops {
    /// The stops of the barrier of `config`, for decisions on `rule` of a
    /// strategy of `requirements`; none if `config` has no barrier.
    ///
    /// # Errors
    ///
    /// Returns an error if the barrier's ATR has no bars (see
    /// `TargetsConfig::atr_bars`).
    pub fn new(
        config: &TargetsConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<Option<Self>> {
        let Some(barrier) = config.barrier else {
            return Ok(None);
        };
        let atr = config.atr_bars(rule, requirements)?;
        Ok(Some(Self {
            instrument_id: config.instrument_id,
            quantity: config.quantity,
            barrier,
            atr: atr.map(BarrierAtr::new).transpose()?.map(Rc::new),
        }))
    }

    /// Starts the barrier's ATR, if any, on `actor`, from its `on_start`,
    /// as `Targets::start` does.
    pub fn start<A>(&self, actor: &mut A)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        if let Some(atr) = &self.atr {
            atr.start(actor);
        }
    }

    /// `warmup` building the bars of the barrier's ATR, if any, and handing
    /// them to it (`Warmup::building`): the warmup's bars reach only the
    /// cache, and live the first decision after a start then sets the stop
    /// a dataset run sets there.
    #[must_use]
    pub fn warmed(&self, warmup: Warmup) -> Warmup {
        match &self.atr {
            Some(atr) => warmup.building(atr.bar_type, Rc::clone(atr) as SharedActorIndicator),
            None => warmup,
        }
    }

    /// How long after its send a barrier trade is closed: its limit.
    #[must_use]
    pub const fn limit(&self) -> SignedDuration {
        self.barrier.limit
    }

    /// The barrier of `side` at a decision of `strategy` now; none with no price of the
    /// instrument yet, the ATR not yet initialized or an exit on the
    /// decision's price (`Barrier::exits`), as a dataset run
    /// opens no target then.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not in the cache, or an exit
    /// is outside its prices or the stop has no worth (on an inverse
    /// instrument, see `ensure_linear`).
    pub fn at<S>(&self, strategy: &S, side: Side) -> anyhow::Result<Option<BarrierExits>>
    where
        S: DataActor + DataActorNative,
    {
        let atr = match &self.atr {
            None => None,
            Some(atr) => match atr.value() {
                Some(value) => Some(value),
                None => return Ok(None),
            },
        };
        let instrument_id = self.instrument_id;
        let cache = strategy.cache();
        let instrument = cache
            .instrument(&instrument_id)
            .with_context(|| format!("instrument {instrument_id} not in cache"))?;
        let Some(reference) = reference(&cache, &instrument_id) else {
            return Ok(None);
        };
        let Some(exits) = self
            .barrier
            .exits(&instrument, reference.as_decimal(), atr)?
        else {
            return Ok(None);
        };
        let (_, take_profit, stop) = exits
            .into_iter()
            .find(|(of, _, _)| *of == side)
            .expect("a barrier has exits on both sides");
        let distance = (stop.as_decimal() - reference.as_decimal()).abs();
        let worth = stop_worth(
            &instrument,
            self.quantity,
            distance.to_f64().unwrap_or(f64::NAN),
        )?;
        Ok(Some(BarrierExits {
            reference,
            take_profit,
            stop,
            stop_worth: worth,
        }))
    }
}

impl Debug for Stops {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stops")
            .field("barrier", &self.barrier)
            .finish_non_exhaustive()
    }
}

/// What a stop `distance` in price away is worth on `quantity` of
/// `instrument`, rounded to its sizes, in its P&L currency: Nautilus'
/// `calculate_notional_value` of the distance.
///
/// # Errors
///
/// Returns an error if `instrument` is inverse (see `ensure_linear`), or
/// `quantity` or `distance` cannot be made in the instrument's sizes and
/// prices.
pub fn stop_worth(
    instrument: &InstrumentAny,
    quantity: Quantity,
    distance: f64,
) -> anyhow::Result<f64> {
    ensure_linear(instrument)?;
    let size = quantity.as_decimal();
    let quantity = instrument
        .try_make_qty_from_decimal(size, None)
        .map_err(|e| anyhow::anyhow!("ordering {size} of {}: {e}", instrument.id()))?;
    let distance = Price::new_checked(distance, FIXED_PRECISION)?;
    Ok(instrument
        .try_calculate_notional_value(quantity, distance, None)?
        .as_f64())
}

/// Checks that a barrier's stops on `instrument` have a worth
/// (`stop_worth`): Nautilus' notional value of an inverse instrument's
/// distance divides by it as a price, which is not what the stop risks.
///
/// # Errors
///
/// Returns an error if `instrument` is inverse.
pub fn ensure_linear(instrument: &InstrumentAny) -> anyhow::Result<()> {
    ensure!(
        !instrument.is_inverse(),
        "{} is inverse: a barrier's stop on it has no worth in R",
        instrument.id()
    );
    Ok(())
}

/// Nautilus' side of the ATR: it reads the bars of its bar type alone.
impl ActorIndicator for BarrierAtr {
    fn key(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn initialized(&self) -> anyhow::Result<bool> {
        Ok(self.value().is_some())
    }

    fn handle_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
        Ok(())
    }

    /// Nautilus routes a bar by instrument and specification; the ATR
    /// reads the bars of its bar type alone, of the same aggregation
    /// source.
    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        if bar.bar_type.standard() == self.bar_type.standard() {
            self.indicator.borrow_mut().handle_bar(bar);
        }
        Ok(())
    }
}

/// `fingerprint` extended with the targets of `config` deciding on `rule`
/// for a strategy of `requirements`.
///
/// # Errors
///
/// Returns an error if the barrier's ATR has no bars (see
/// `TargetsConfig::atr_bars`).
pub fn fingerprint(
    fingerprint: Fingerprint,
    config: &TargetsConfig,
    rule: DecisionRule,
    requirements: &DataRequirements,
) -> anyhow::Result<Fingerprint> {
    let atr = config.atr_bars(rule, requirements)?;
    Ok(fingerprint.with(config.fingerprint_part(atr)?.as_bytes()))
}

impl Exit {
    /// The exit as a value: 1 the take profit, -1 the stop, 0 the time.
    #[must_use]
    pub fn value(self) -> f64 {
        match self {
            Self::TakeProfit => 1.0,
            Self::Stop => -1.0,
            Self::Time => 0.0,
        }
    }
}

impl Outcome {
    /// The sign of the P&L: 1 a gain, -1 a loss, 0 neither.
    #[must_use]
    pub fn sign(&self) -> i8 {
        match self.pnl.as_decimal().cmp(&Decimal::ZERO) {
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
        }
    }
}

impl Targets {
    /// The targets of `config`, for decisions on `rule` of a strategy of
    /// `requirements`.
    ///
    /// # Errors
    ///
    /// Returns an error if `config` is invalid for `rule` and
    /// `requirements`.
    pub fn new(
        config: TargetsConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<Rc<Self>> {
        config.validate(rule, requirements)?;
        let atr = config.atr_bars(rule, requirements)?;
        Ok(Rc::new(Self {
            config,
            atr: atr.map(BarrierAtr::new).transpose()?.map(Rc::new),
            state: RefCell::new(State::default()),
        }))
    }

    /// Starts the barrier's average true range, if any, on `actor`, from
    /// its `on_start`: registers it with the actor's Nautilus indicators
    /// for its bars and subscribes the actor to them, so the data engine
    /// builds them, from the quotes, the trades or the source bars, and
    /// hands it each before the actor's own handler. Nautilus registers
    /// an indicator once per bar type and keeps one subscription per bar
    /// type and actor, so starting again, on a restart, or a strategy
    /// subscribing to the same bars, feeds it each bar once.
    pub fn start<A>(&self, actor: &mut A)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        if let Some(atr) = &self.atr {
            atr.start(actor);
        }
    }

    /// Opens the targets of the decision of `row`: for each target and
    /// side, its orders, from `strategy`, and each target's time alert.
    /// With no price of the instrument yet, the decision is unsettled;
    /// with the barrier's ATR not yet initialized, or an exit of the
    /// barrier on the decision's price (`Barrier::exits`), it opens
    /// nothing, as a decision on cold features is none.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not in the cache or an exit
    /// price cannot be made in it, before any order, or if the size cannot
    /// be made in it or Nautilus refuses an order or an alert, which
    /// unsettles the decision.
    pub fn open<S>(&self, strategy: &mut S, row: FeatureRow) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let time = row.ts_event.to_rfc3339();
        let atr = match &self.atr {
            None => None,
            Some(atr) => {
                let Some(value) = atr.value() else {
                    log::debug!(
                        "decision at {time}: the ATR of {} is not initialized; no targets",
                        atr.bar_type
                    );
                    return Ok(());
                };
                Some(value)
            }
        };
        let next_decision = || {
            let mut state = self.state.borrow_mut();
            state.next += 1;
            state.next - 1
        };
        let instrument_id = self.config.instrument_id;
        let (instrument, reference) = {
            let cache = strategy.cache();
            let instrument = cache
                .instrument(&instrument_id)
                .with_context(|| format!("instrument {instrument_id} not in cache"))?;
            (instrument, reference(&cache, &instrument_id))
        };
        let Some(reference) = reference else {
            log::error!(
                "decision {} at {time}: no price of {instrument_id} to set its targets from; \
                 unsettled",
                next_decision()
            );
            return Ok(());
        };
        let reference = reference.as_decimal();
        let exits = match self.config.barrier {
            Some(barrier) => match barrier.exits(&instrument, reference, atr)? {
                Some(exits) => exits,
                None => {
                    log::debug!(
                        "decision at {time}: an exit of the barrier is on the price {reference} \
                         of {instrument_id}; no targets"
                    );
                    return Ok(());
                }
            },
            None => vec![],
        };
        let decision = next_decision();
        self.state
            .borrow_mut()
            .open
            .insert(decision, Decision::new(row, &exits, reference));
        let sent = self.send(strategy, decision, &exits);
        if let Err(e) = &sent {
            self.unsettle(decision, format_args!("sending its orders: {e}"));
        }
        sent
    }

    /// Sends the trades of `decision`, the brackets with `exits`, each
    /// target on each side named by its ids.
    fn send<S>(
        &self,
        strategy: &mut S,
        decision: u64,
        exits: &[(Side, Price, Price)],
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let trade = |side: Side, exits| Trade {
            instrument_id: self.config.instrument_id,
            side: side.order_side(),
            quantity: self.config.quantity,
            exits,
        };
        let barriers = self.config.barrier.into_iter().flat_map(|barrier| {
            exits.iter().map(move |&(side, take_profit, stop)| {
                let exits = Exits {
                    take_profit: Some(take_profit),
                    stop_loss: Some(stop),
                    time_limit: Some(barrier.limit),
                };
                (Target::new(decision, Kind::Barrier, side), exits)
            })
        });
        let horizons = self.config.horizon.into_iter().flat_map(|horizon| {
            Side::BOTH.map(|side| {
                let exits = Exits {
                    take_profit: None,
                    stop_loss: None,
                    time_limit: Some(horizon),
                };
                (Target::new(decision, Kind::Horizon, side), exits)
            })
        });
        let trades: Vec<(Target, Exits)> = barriers.chain(horizons).collect();
        for (named, exits) in trades {
            trade(named.side, exits).send_as(strategy, named.order_ids())?;
        }
        Ok(())
    }

    /// A target's entry cancelled unfilled, at its time limit
    /// (`orders::on_time_event`), unsettles its decision.
    pub fn on_order_canceled<S>(&self, strategy: &S, event: &OrderCanceled)
    where
        S: DataActor + DataActorNative,
    {
        let entry = event.client_order_id;
        let Some((target, Leg::Entry)) = Target::parse(entry) else {
            return;
        };
        let unfilled = strategy
            .cache()
            .order(&entry)
            .is_some_and(|order| order.filled_qty().is_zero());
        if unfilled {
            self.unsettle(
                target.decision,
                format_args!("the entry {entry} has not filled by its time"),
            );
        }
    }

    /// A target's order denied by the risk engine unsettles its decision.
    pub fn on_order_denied(&self, event: &OrderDenied) {
        self.refused(event.client_order_id, "denied", event.reason);
    }

    /// A target's order rejected by the venue unsettles its decision.
    pub fn on_order_rejected(&self, event: &OrderRejected) {
        self.refused(event.client_order_id, "rejected", event.reason);
    }

    /// Records the outcome of the target whose position closed, from the
    /// event and the closing order, and publishes its decision's row,
    /// from `strategy`, once every target of it has one. A position closed
    /// by an order of no exit of its target leaves its decision unsettled.
    pub fn on_position_closed<S>(&self, strategy: &S, event: &PositionClosed)
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let Some((target, Leg::Entry)) = Target::parse(event.opening_order_id) else {
            return;
        };
        let exit = event
            .closing_order_id
            .and_then(|closing| exit(strategy, target, closing));
        let commission = |pnl: Money| {
            let position = strategy.cache().position(&event.position_id)?;
            Some(
                position
                    .commissions()
                    .into_iter()
                    .find(|commission| commission.currency == pnl.currency)
                    .unwrap_or_else(|| Money::zero(pnl.currency)),
            )
        };
        let pnl = event
            .realized_pnl
            .and_then(|pnl| Some((pnl, commission(pnl)?)));
        let (Some(exit), Some((pnl, commission))) = (exit, pnl) else {
            self.unsettle(
                target.decision,
                format_args!(
                    "the position {} of {} closed by {:?}: no exit of its target, or no \
                     realised P&L, or no such position in the cache",
                    event.position_id,
                    target.order_id(Leg::Entry),
                    event.closing_order_id
                ),
            );
            return;
        };
        let row = {
            let mut state = self.state.borrow_mut();
            let Some(decision) = state.open.get_mut(&target.decision) else {
                return;
            };
            decision.outcomes.insert(
                (target.kind, target.side),
                decision.outcome(target, pnl, commission, exit),
            );
            let barrier = decision.outcomes(Kind::Barrier);
            let horizon = decision.outcomes(Kind::Horizon);
            if barrier.is_some() != self.config.barrier.is_some()
                || horizon.is_some() != self.config.horizon.is_some()
            {
                return;
            }
            let row = TargetRow {
                decision: decision.row.clone(),
                barrier,
                horizon,
                settled_at: event.ts_event,
            };
            state.open.remove(&target.decision);
            row
        };
        let data_type = TargetRow::data_type(row.decision.strategy_id);
        let data = CustomData::new(Arc::new(row), data_type.clone());
        strategy.publish_data(&data_type, &data);
    }

    /// Reports the decisions still open, unsettled at the end of the run.
    pub fn on_stop(&self) {
        let state = self.state.borrow();
        if let (Some(first), Some(last)) = (state.open.values().next(), state.open.values().last())
        {
            log::info!(
                "{} decisions from {} to {} unsettled at the stop, their targets still open",
                state.open.len(),
                first.row.ts_event.to_rfc3339(),
                last.row.ts_event.to_rfc3339()
            );
        }
    }

    /// Unsettles the decision of `id`, a target's order refused `how`.
    fn refused(&self, id: ClientOrderId, how: &str, reason: Ustr) {
        if let Some((target, _)) = Target::parse(id) {
            self.unsettle(target.decision, format_args!("{id} {how}: {reason}"));
        }
    }

    /// Drops `decision` if open, logging `why` it is unsettled.
    fn unsettle(&self, decision: u64, why: impl Display) {
        let dropped = self.state.borrow_mut().open.remove(&decision);
        if let Some(dropped) = dropped {
            log::error!(
                "decision {decision} at {}: {why}; its targets are unsettled",
                dropped.row.ts_event.to_rfc3339()
            );
        }
    }
}

impl Debug for Targets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.borrow();
        f.debug_struct("Targets")
            .field("config", &self.config)
            .field("next", &state.next)
            .field("open", &state.open.len())
            .finish_non_exhaustive()
    }
}

impl Decision {
    /// The decision of `row`, its barrier `exits` set from `reference`.
    fn new(row: FeatureRow, exits: &[(Side, Price, Price)], reference: Decimal) -> Self {
        let distances = exits
            .iter()
            .map(|&(side, take_profit, stop)| {
                let distance = |exit: Price| (exit.as_decimal() - reference).abs();
                (
                    side,
                    Distances {
                        take_profit: distance(take_profit),
                        stop: distance(stop),
                    },
                )
            })
            .collect();
        Self {
            row,
            distances,
            outcomes: BTreeMap::new(),
        }
    }

    /// The outcome of its `target`, its position closed with `pnl`, net of
    /// `commission`, by `exit`, a barrier's with the distances it set.
    fn outcome(&self, target: Target, pnl: Money, commission: Money, exit: Exit) -> Outcome {
        let distances = match target.kind {
            Kind::Barrier => self.distances.get(&target.side).copied(),
            Kind::Horizon => None,
        };
        Outcome {
            pnl,
            commission,
            exit,
            distances,
        }
    }

    /// The outcomes of `kind`, once it has both.
    fn outcomes(&self, kind: Kind) -> Option<Outcomes> {
        Some(Outcomes {
            long: *self.outcomes.get(&(kind, Side::Long))?,
            short: *self.outcomes.get(&(kind, Side::Short))?,
        })
    }
}

impl Target {
    fn new(decision: u64, kind: Kind, side: Side) -> Self {
        Self {
            decision,
            kind,
            side,
        }
    }

    /// The client order ids of its orders, its legs' by `order_id`.
    fn order_ids(self) -> OrderIds {
        OrderIds {
            entry: self.order_id(Leg::Entry),
            take_profit: self.order_id(Leg::TakeProfit),
            stop_loss: self.order_id(Leg::Stop),
        }
    }

    /// The client order id of `leg`: `D{n}-{B|H}{L|S}-{E|TP|SL}`.
    fn order_id(self, leg: Leg) -> ClientOrderId {
        let side = match self.side {
            Side::Long => 'L',
            Side::Short => 'S',
        };
        let leg = match leg {
            Leg::Entry => "E",
            Leg::TakeProfit => "TP",
            Leg::Stop => "SL",
        };
        let id = format!("D{}-{}{side}-{leg}", self.decision, self.kind.letter());
        ClientOrderId::from(id.as_str())
    }

    /// The target and leg `id` names, if a target's.
    fn parse(id: ClientOrderId) -> Option<(Self, Leg)> {
        let (decision, rest) = id.as_str().strip_prefix('D')?.split_once('-')?;
        let (target, leg) = rest.split_once('-')?;
        let mut target = target.chars();
        let kind = Kind::of(target.next()?)?;
        let side = match target.next()? {
            'L' => Side::Long,
            'S' => Side::Short,
            _ => return None,
        };
        let leg = match leg {
            "E" => Leg::Entry,
            "TP" => Leg::TakeProfit,
            "SL" => Leg::Stop,
            _ => return None,
        };
        (target.next().is_none()).then_some(())?;
        Some((Self::new(decision.parse().ok()?, kind, side), leg))
    }
}

impl Kind {
    /// The name of the kind in the names of a row's values.
    fn name(self) -> &'static str {
        match self {
            Self::Barrier => "barrier",
            Self::Horizon => "horizon",
        }
    }

    fn letter(self) -> char {
        match self {
            Self::Barrier => 'B',
            Self::Horizon => 'H',
        }
    }

    fn of(letter: char) -> Option<Self> {
        match letter {
            'B' => Some(Self::Barrier),
            'H' => Some(Self::Horizon),
            _ => None,
        }
    }

    /// The names of the values a row has of a target of this kind, on each
    /// side.
    const fn values(self) -> &'static [&'static str] {
        match self {
            Self::Barrier => &["pnl", "commission", "exit", "take_profit", "stop"],
            Self::Horizon => &["pnl", "commission", "exit"],
        }
    }
}

impl Side {
    /// The long, then the short.
    pub const BOTH: [Self; 2] = [Self::Long, Self::Short];

    /// The name of the side, as in the names of a row's values.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }

    /// The side of the order opening it: a buy the long, a sell the short.
    #[must_use]
    pub const fn order_side(self) -> OrderSide {
        match self {
            Self::Long => OrderSide::Buy,
            Self::Short => OrderSide::Sell,
        }
    }

    /// 1 long, -1 short: the direction of the side's favour.
    fn sign(self) -> Decimal {
        match self {
            Self::Long => Decimal::ONE,
            Self::Short => -Decimal::ONE,
        }
    }
}

/// The exit of `target` that `closing` is, from its id or, closing at the
/// time, its tag; none if it is no exit of the target.
fn exit<S: DataActor + DataActorNative>(
    strategy: &S,
    target: Target,
    closing: ClientOrderId,
) -> Option<Exit> {
    match Target::parse(closing) {
        Some((of, Leg::TakeProfit)) if of == target => Some(Exit::TakeProfit),
        Some((of, Leg::Stop)) if of == target => Some(Exit::Stop),
        Some(_) => None,
        None => {
            let cache = strategy.cache();
            let order = cache.order(&closing)?;
            orders::is_time_close(&order).then_some(Exit::Time)
        }
    }
}

/// The price a decision's exits are set from: the mid of the instrument's
/// last quote, else its last trade, else the close of its last bar, of any
/// bar type.
pub(super) fn reference(cache: &CacheApi, instrument_id: &InstrumentId) -> Option<Price> {
    cache
        .price(instrument_id, PriceType::Mid)
        .or_else(|| cache.price(instrument_id, PriceType::Last))
        .or_else(|| {
            [AggregationSource::External, AggregationSource::Internal]
                .into_iter()
                .flat_map(|source| cache.bar_types(Some(instrument_id), None, source))
                .filter_map(|bar_type| cache.bar(&bar_type))
                .max_by_key(|bar| bar.ts_init)
                .map(|bar| bar.close)
        })
}

impl TargetRow {
    /// The values of the row, in the order of `TargetsConfig::target_names`
    /// of the config that made it: each P&L in its currency, each exit as
    /// `Exit::value`, each distance in price.
    #[must_use]
    pub fn values(&self) -> Vec<f64> {
        [self.barrier, self.horizon]
            .into_iter()
            .flatten()
            .flat_map(|outcomes| [outcomes.long, outcomes.short])
            .flat_map(|outcome| {
                let distances = outcome.distances.into_iter().flat_map(|distances| {
                    [distances.take_profit, distances.stop]
                        .map(|distance| distance.to_f64().unwrap_or(f64::NAN))
                });
                [
                    outcome.pnl.as_f64(),
                    outcome.commission.as_f64(),
                    outcome.exit.value(),
                ]
                .into_iter()
                .chain(distances)
            })
            .collect()
    }

    /// The data type of the rows of `strategy_id`.
    #[must_use]
    pub fn data_type(strategy_id: StrategyId) -> DataType {
        DataType::new(
            Self::type_name_static(),
            None,
            Some(strategy_id.to_string()),
        )
    }
}

impl HasTsInit for TargetRow {
    fn ts_init(&self) -> UnixNanos {
        self.settled_at
    }
}

impl CustomDataTrait for TargetRow {
    fn type_name(&self) -> &'static str {
        Self::type_name_static()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.decision.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn type_name_static() -> &'static str {
        "TargetRow"
    }
}

#[cfg(test)]
mod tests;
