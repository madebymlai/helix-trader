//! The target of a strategy's decisions, read from real orders on
//! Nautilus' simulated venue.
//!
//! In a dataset run, at each decision `Targets::open` sends, on each side,
//! a long and a short, the orders whose outcome is the target, on a hedging
//! margin account so that each is a position of its own, each the trade a
//! strategy trading on a model of that target would place, sent through the
//! shared order module (`strategy::orders::Trade::send_as`, the one sending
//! of both). The target (`Target`) has one limit and a bracket or none:
//!
//! - with one: a market entry, a take-profit limit and a stop, the two exits
//!   one-updates-the-other, closed at the limit after its send;
//! - without one, time only: a market entry closed at the limit after its
//!   send.
//!
//! The strategy hands its time events to the order module
//! (`orders::on_time_event`), which at a trade's time limit cancels its
//! orders still open and closes its position if still open, tagged as a
//! time close (`orders::is_time_close`).
//!
//! The exits (`BracketSpec`) are `take_profit` and `stop` away from the
//! decision's price (`reference`): the mid of the instrument's last quote,
//! else its last trade, else the close of its last bar. Each distance is
//! relative (`Distance`): basis points of that price, or multiples of an
//! average true range, Nautilus' `AverageTrueRange`, of `DEFAULT_ATR`
//! unless the bracket gives its arguments, built by
//! Nautilus' `AverageTrueRange::new` as the config is validated, so that
//! an argument Nautilus refuses panics at load. Its bars are Nautilus' bars of
//! the instrument at the decision cadence, so a decision reads the bar
//! closing at its time, built from what the strategy reads of it
//! (`TargetConfig::atr_bars`): the mids of its quotes, else the last
//! prices of its trades, else, of its source bars alone, a composite bar
//! type built from them. A backtest replays a source bar stamped at its
//! close before the timer closing the bar of the cadence at that instant,
//! so that bar holds it. The setter owns the ATR, and `TargetSetter::start`
//! registers it and subscribes to its bars, which no strategy declares. A
//! decision at which that ATR is not yet initialized opens no target, as a
//! decision on cold features is none. Each exit is on the instrument's
//! nearest tick (Nautilus' tick grid), a tie away from the decision's
//! price; an exit on the decision's price, no distance (a stop worth
//! nothing, a P&L in R of none), opens no target either. A time-only target
//! has no distance, so no ATR: nothing of it is built, checked or
//! subscribed to.
//!
//! A feature formed over the bracket (`model::features::OverBracket`) reads
//! its width (`BracketWidth`, of `TargetConfig::width`): the distance of a
//! bracket whose take profit and stop are as far, as the setter computes it
//! (`Distance::length`) before putting the exits on ticks, on the ATR the
//! setter runs on (`TargetSetter::sharing`), one ATR read by both; and the
//! decisions it is read at, of the target's cadence, and its time limit.
//! The handle reads a decision's features and sets its target in the one
//! decision event, so the width a feature reads then is the one the
//! target is set at.
//!
//! Each order's client order id names its decision, side and leg,
//! `D{n}-{L|S}-{E|TP|SL}`: the decision's number in the run, `L` long and
//! `S` short, the entry, take profit or stop. The outcome of a target is
//! read from Nautilus' events alone: a closed position's opening order
//! names its decision and side, its closing order the exit (a take profit,
//! a stop, or a time close), and Nautilus reports its realised P&L net of
//! commission, the position the commissions it is net of. The only state
//! of our own is the rows being assembled.
//!
//! Once both sides of a decision have an outcome, the decision is settled
//! and its row, a `TargetRow`, published on the message bus, custom data
//! keyed by the strategy's id (`TargetRow::data_type`), each outcome with the
//! unit its decision set on its side (`SideTarget::unit`). A decision whose
//! order the risk engine denies or the venue rejects, or whose entry has not
//! filled by its time limit (the module cancels it then, and the strategy
//! hands the targets the cancel, `on_order_canceled`), is unsettled: logged,
//! and no row. So is a decision whose positions are still open when the run
//! stops.
//!
//! One function sets a target, `TargetSetter::at`, for a dataset run's
//! decisions and for those of a strategy trading on a model of the target; it
//! says why it set none, which a dataset run logs and a trading decision does
//! not need. `Targets` adds only sending the orders and settling. A time-only
//! target reads no instrument on either path, so a dataset run finds one
//! missing from the cache only as it sends the orders. `Targets::start`
//! refuses a bracket on an inverse instrument; live, the setter errors at each
//! decision.
//!
//! A quote at the same nanosecond as a time alert reaches the venue first,
//! so an exit hit at the time limit counts as inside the window. On bars
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
    types::{Money, Price, Quantity},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{
    decide::{self, DecisionRule, FeatureRow},
    features::Fingerprint,
};
use crate::strategy::{
    AtrArgs, DataRequirements, IndicatorArgs, TradedInstrument,
    orders::{self, Bracket, Exits, OrderIds, Trade},
    warmup::{self, Warmup},
};

/// The target of each decision, from the strategy's config: on one
/// instrument, of one size. The strategy's TOML has the target alone
/// (`Target`), on the instrument and of the size of its `[trade]` table
/// (`Target::trading`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    /// The instrument the target trades.
    pub instrument_id: InstrumentId,
    /// The size of every order, rounded to the instrument's sizes.
    pub quantity: Quantity,
    /// How its trades end.
    pub spec: Target,
}

/// A target as a model table of a strategy's TOML has it, what it trades
/// being the strategy's `[trade]`, named once: how a decision's trades end.
/// A bracket and its time limit, such as `{ bracket = { take_profit = { atr =
/// 2.0 }, stop = { bps = 20 } }, limit = "2h" }`, or with the ATR's arguments
/// given, `{ bracket = { .., atr = { period = 20, ma_type = "WILDER" } }, .. }`;
/// a time-only target the limit alone, `{ limit = "2h" }`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct Target {
    /// Its exits, if it has them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bracket: Option<BracketSpec>,
    /// How long after the decision what is still open is closed.
    pub limit: SignedDuration,
}

/// A target's exits, each a distance from the decision's price, and the
/// arguments of the ATR its distances may be in.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BracketSpec {
    /// How far the take profit is, in the side's favour.
    pub take_profit: Distance,
    /// How far the stop is, against the side.
    pub stop: Distance,
    /// The average true range the `atr` distances are multiples of, if
    /// not `DEFAULT_ATR`: only with a distance in ATRs
    /// (`TargetConfig::validate`).
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
    /// Multiples of the target's average true range at the decision,
    /// `{ atr = 1.5 }`.
    #[serde(rename = "atr")]
    Atr(Decimal),
}

/// The average true range of a bracket without an `atr` table: Nautilus'
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

/// The units of the ATR's bars, longest first: those of a fixed length,
/// which a cadence, or the source bars of a composite bar type, is a whole
/// number of. A unit's length is Nautilus' (`BarSpecification::timedelta`).
const ATR_UNITS: [BarAggregation; 5] = [
    BarAggregation::Day,
    BarAggregation::Hour,
    BarAggregation::Minute,
    BarAggregation::Second,
    BarAggregation::Millisecond,
];

/// The average true range a target runs: `args`, on bars of `bar_type`,
/// bars of the target's instrument at the decision cadence, so that a
/// decision reads the bar closing at its time (`TargetConfig::atr_bars`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtrBars {
    pub bar_type: BarType,
    pub args: AtrArgs,
}

/// How a target's position was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Exit {
    TakeProfit,
    Stop,
    /// At its time limit.
    Time,
}

/// The outcome of a target on one side.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Outcome {
    /// The position's realised P&L, net of its commissions, as Nautilus
    /// reports it.
    pub pnl: Money,
    /// The position's commissions in the P&L's currency, those its
    /// realised P&L is net of (Nautilus' `Position::commissions`).
    pub commission: Money,
    pub exit: Exit,
    /// The unit of the side's payoff, as the decision set it
    /// (`SideTarget::unit`).
    pub unit: f64,
}

/// The outcome of a decision's target on each side.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Outcomes {
    pub long: Outcome,
    pub short: Outcome,
}

/// A settled decision: its features and the outcome of its target on each
/// side, published on the message bus.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TargetRow {
    /// The decision, as `decide` published it.
    pub decision: FeatureRow,
    /// The target's outcomes.
    pub outcomes: Outcomes,
    /// When the last of its positions closed.
    pub settled_at: UnixNanos,
}

/// The targets of a strategy's decisions, shared with it as an `Rc`: the
/// strategy calls `open` at each decision, and hands its order and
/// position events and its time events to the methods of their names.
pub struct Targets {
    config: TargetConfig,
    /// What sets each decision's target.
    setter: TargetSetter,
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
    /// The unit set on each side.
    units: PerSide<f64>,
    outcomes: BTreeMap<Side, Outcome>,
}

/// A decision's target on one side: what its orders' ids name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DecisionSide {
    decision: u64,
    side: Side,
}

/// A side of a target, and of a model: the long or the short, `"long"` or
/// `"short"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Long,
    Short,
}

/// A value for each side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerSide<T> {
    pub long: T,
    pub short: T,
}

impl<T> PerSide<T> {
    /// The value of `side`.
    #[must_use]
    pub const fn of(&self, side: Side) -> &T {
        match side {
            Side::Long => &self.long,
            Side::Short => &self.short,
        }
    }

    /// The value of `f` of each side's.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> PerSide<U> {
        PerSide {
            long: f(self.long),
            short: f(self.short),
        }
    }
}

/// A target as a decision sets it on one side (`TargetSetter::at`): the
/// price it is set from, its exits on the instrument's ticks if it has
/// them, when it ends after its send, and what the unit of its side's
/// payoff is worth in money: the stop's worth (`stop_worth`), the payoff
/// being in R, or 1 without exits, the payoff being in money.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SideTarget {
    pub reference: Price,
    pub bracket: Option<Bracket>,
    pub time_limit: SignedDuration,
    pub unit: f64,
}

/// Why a decision sets no target (`TargetSetter::at`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoTarget {
    /// The target's ATR, on bars of this type, is not yet initialized.
    ColdAtr(BarType),
    /// The instrument has no price yet to set it from.
    NoPrice,
    /// An exit is on the decision's price, this one: no distance, a stop worth
    /// nothing, a P&L in R of none.
    ExitOnPrice(Price),
}

/// An order of a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Entry,
    TakeProfit,
    Stop,
}

impl TargetConfig {
    /// # Errors
    ///
    /// Returns every violation, each Nautilus' `ConfigError` under its
    /// field of `[model]` (`model.target…`), as one
    /// `ConfigError::Multiple` if there are several: the size not positive,
    /// a distance or the limit not positive, an `atr` table no distance uses,
    /// or an ATR without bars at the cadence of `rule` built from what a
    /// strategy of `requirements` reads (see `atr_bars`).
    ///
    /// # Panics
    ///
    /// Panics if Nautilus refuses an argument of the target's ATR as it
    /// builds it (`AverageTrueRange::new`).
    pub fn validate(
        &self,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();
        let spec = self.spec;
        errors.check(
            self.quantity.is_positive(),
            ConfigError::invalid_value("model.target", "the target quantity must be positive"),
        );
        errors.check(
            spec.bracket.is_none_or(|bracket| {
                bracket.take_profit.value() > Decimal::ZERO && bracket.stop.value() > Decimal::ZERO
            }),
            ConfigError::invalid_value(
                "model.target.bracket",
                "the target's take profit and stop must be positive",
            ),
        );
        errors.check(
            spec.limit.is_positive(),
            ConfigError::invalid_value(
                "model.target.limit",
                format!("the target's limit {:#} is not positive", spec.limit),
            ),
        );
        errors.check(
            spec.bracket
                .is_none_or(|bracket| bracket.uses_atr() || bracket.atr.is_none()),
            ConfigError::invalid_value(
                "model.target.bracket.atr",
                "the target has an ATR no distance uses",
            ),
        );
        let atr = |result: anyhow::Result<()>| {
            result.map_err(|e| {
                ConfigError::invalid_value("model.target.bracket.atr", format!("{e:#}"))
            })
        };
        if let Some(args) = spec.atr_args() {
            // Nautilus checks the arguments as it builds the ATR.
            errors.collect(atr(args.build().map(drop)));
        }
        errors.collect(atr(self.atr_bars(rule, requirements).map(drop)));
        errors.into_result()
    }

    /// The target's average true range deciding on `rule`, for a strategy
    /// of `requirements`, if a distance is in ATRs: its period, and its
    /// bars, Nautilus' internal bars of the target's instrument at the
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
        let Some(config) = self.spec.atr_args() else {
            return Ok(None);
        };
        let DecisionRule::Every(cadence) = rule;
        let nanos = cadence.as_nanos();
        let Some((aggregation, step)) = ATR_UNITS
            .into_iter()
            .map(|aggregation| {
                let unit = BarSpecification::new(1, aggregation, PriceType::Last).timedelta();
                (aggregation, unit.as_nanos())
            })
            .find(|(_, unit)| nanos % unit == 0)
            .map(|(aggregation, unit)| (aggregation, nanos / unit))
        else {
            bail!(
                "the target's ATR has no bars of the decision cadence {cadence:#}, not whole \
                 milliseconds"
            );
        };
        let spec = |price_type| {
            usize::try_from(step)
                .map_err(anyhow::Error::from)
                .and_then(|step| BarSpecification::new_checked(step, aggregation, price_type))
                .with_context(|| {
                    format!("the target's ATR has no bars of the decision cadence {cadence:#}")
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
                "the target's ATR has no bars of {traded} to be built from: the strategy reads \
                 neither its quotes, its trades nor its bars"
            );
            let Some(source) = longest_dividing(&sources, cadence) else {
                let sources: Vec<String> = sources.iter().map(ToString::to_string).collect();
                bail!(
                    "the target's ATR has no bars of the decision cadence {cadence:#}: it is \
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

    /// The config as the fingerprint reads it, its ATR `atr` as it runs:
    /// Nautilus' ATR as built, its arguments given or Nautilus' defaults,
    /// and its bars, whose source changes the rows.
    fn fingerprint_part(&self, atr: Option<AtrBars>) -> anyhow::Result<String> {
        let atr = atr
            .map(|atr| anyhow::Ok(format!("{} on bars of {}", atr.args.build()?, atr.bar_type)))
            .transpose()?;
        let exits = self
            .spec
            .bracket
            .map(|bracket| format!("take profit {} stop {}", bracket.take_profit, bracket.stop));
        Ok(format!(
            "{} {} exits {exits:?} limit {} ns atr {atr:?}",
            self.instrument_id,
            self.quantity,
            self.spec.limit.as_nanos()
        ))
    }

    /// The longest time a decision's target stays open: its limit. A
    /// decision is settled by then: a time close fills at once, on the
    /// venue's last quote at the alert's own time.
    #[must_use]
    pub const fn settles_within(&self) -> SignedDuration {
        self.spec.limit
    }

    /// The width of its bracket at the decisions on `rule` of a strategy
    /// of `requirements` (see `BracketWidth`).
    ///
    /// # Errors
    ///
    /// Returns an error if the target has no bracket, its take profit and
    /// stop are not one distance, or its ATR has no bars (see `atr_bars`).
    pub fn width(
        &self,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<BracketWidth> {
        let Some(bracket) = self.spec.bracket else {
            bail!("the target has no bracket to have a width");
        };
        ensure!(
            bracket.take_profit == bracket.stop,
            "the target's take profit, {}, and stop, {}, are not one distance: its bracket has \
             no one width",
            bracket.take_profit,
            bracket.stop
        );
        let atr = self.atr_bars(rule, requirements)?;
        Ok(BracketWidth {
            distance: bracket.stop,
            atr: atr.map(TargetAtr::new).transpose()?.map(Rc::new),
            limit: self.spec.limit,
            rule,
            delay: warmup::bar_build_delay(&requirements.bar_types),
        })
    }
}

impl Target {
    /// The target trading `traded`: its instrument, its size.
    #[must_use]
    pub fn trading(self, traded: &TradedInstrument) -> TargetConfig {
        TargetConfig {
            instrument_id: traded.instrument_id,
            quantity: traded.trade_size,
            spec: self,
        }
    }

    /// Its ATR, if a distance is in ATRs: its `atr` table's, else
    /// `DEFAULT_ATR`.
    fn atr_args(&self) -> Option<AtrArgs> {
        self.bracket
            .filter(BracketSpec::uses_atr)
            .map(|bracket| bracket.atr.unwrap_or(DEFAULT_ATR))
    }
}

impl BracketSpec {
    /// Whether a distance is in ATRs.
    fn uses_atr(&self) -> bool {
        [self.take_profit, self.stop]
            .iter()
            .any(|distance| matches!(distance, Distance::Atr(_)))
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
            Self::Atr(multiple) => atr.expect("a validated target has its ATR") * multiple,
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

/// The take profit and stop of each side of `bracket`, from `reference` and,
/// if a distance is in ATRs, the ATR's value `atr`, each on the nearest tick
/// of `instrument`; none at all if an exit rounds onto `reference`, no
/// distance (a stop's worth nothing, a P&L in R of none), so that the decision
/// is no target, as one on a cold ATR.
///
/// # Errors
///
/// Returns an error if an exit is outside the instrument's prices.
///
/// # Panics
///
/// Panics if a distance is in ATRs and `atr` is none.
fn exits(
    bracket: BracketSpec,
    instrument: &InstrumentAny,
    reference: Decimal,
    atr: Option<f64>,
) -> anyhow::Result<Option<PerSide<Bracket>>> {
    let atr = atr.map(atr_decimal).transpose()?;
    let take_profit = bracket.take_profit.length(reference, atr);
    let stop = bracket.stop.length(reference, atr);
    let of = |side: Side| -> anyhow::Result<Bracket> {
        let favour = side.sign();
        Ok(Bracket {
            take_profit: nearest_tick(instrument, reference + favour * take_profit, favour)?,
            stop_loss: nearest_tick(instrument, reference - favour * stop, -favour)?,
        })
    };
    let exits = PerSide {
        long: of(Side::Long)?,
        short: of(Side::Short)?,
    };
    let off_reference = [exits.long, exits.short].iter().all(|bracket| {
        bracket.take_profit.as_decimal() != reference && bracket.stop_loss.as_decimal() != reference
    });
    Ok(off_reference.then_some(exits))
}

/// The ATR's value `atr` as the distances take it.
///
/// # Errors
///
/// Returns an error if it is no decimal (not finite).
fn atr_decimal(atr: f64) -> anyhow::Result<Decimal> {
    Decimal::try_from(atr).with_context(|| format!("the ATR {atr}"))
}

/// Of `sources`, the longest bars whose length `cadence` is a whole
/// multiple of, the first at a tie; none if it is of none. Bars of no
/// fixed length (ticks, volumes, months) divide no cadence.
fn longest_dividing(sources: &[BarType], cadence: SignedDuration) -> Option<BarType> {
    let length = |source: &BarType| {
        let spec = source.spec();
        ATR_UNITS
            .contains(&spec.aggregation)
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

/// The average true range of a target, registered with the strategy's
/// Nautilus indicators for the bars of its bar type.
struct TargetAtr {
    bars: AtrBars,
    indicator: RefCell<AverageTrueRange>,
}

impl TargetAtr {
    fn new(bars: AtrBars) -> anyhow::Result<Self> {
        Ok(Self {
            bars,
            indicator: RefCell::new(bars.args.build()?),
        })
    }

    /// Its value, once initialized.
    fn value(&self) -> Option<f64> {
        let indicator = self.indicator.borrow();
        indicator.initialized.then_some(indicator.value)
    }

    /// Registers it with `actor`'s Nautilus indicators for its bars and
    /// subscribes the actor to them (see `TargetSetter::start`).
    fn start<A>(self: &Rc<Self>, actor: &mut A)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let shared = Rc::clone(self) as SharedActorIndicator;
        let bar_type = self.bars.bar_type;
        actor
            .core_mut()
            .register_indicator_for_bars(bar_type, shared);
        actor.subscribe_bars(bar_type, None, None);
    }

    /// `warmup` building its bars and handing them to it (see
    /// `TargetSetter::warmed`).
    fn warmed(self: &Rc<Self>, warmup: Warmup) -> Warmup {
        warmup.building(self.bars.bar_type, Rc::clone(self) as SharedActorIndicator)
    }
}

/// The width of a target's bracket whose take profit and stop are one
/// distance (`TargetConfig::width`), as a feature formed over the bracket
/// reads it (`model::features::OverBracket`): the distance from the
/// decision's price as the setter computes it (`Distance::length`), before
/// the exits go on ticks. Its ATR, if the distance is in ATRs, is the one
/// the target's setter runs on (`TargetSetter::sharing`), so read at a
/// decision, from `Feature::value`, it is the width the decision's target
/// is set at. It is read at the decisions of the target's cadence
/// (`decision`).
pub struct BracketWidth {
    distance: Distance,
    atr: Option<Rc<TargetAtr>>,
    /// The target's time limit.
    limit: SignedDuration,
    rule: DecisionRule,
    /// How long after their grid times the decisions fire, before their
    /// margin: the node's bar build delay of the strategy's bar types.
    delay: SignedDuration,
}

impl BracketWidth {
    /// Whether it has a length: its ATR, if any, initialized.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.atr.as_ref().is_none_or(|atr| atr.value().is_some())
    }

    /// The width at `reference`, the decision's price, in prices; none while
    /// its ATR is not initialized.
    ///
    /// # Panics
    ///
    /// Panics if `reference` is not finite.
    #[must_use]
    pub fn length(&self, reference: f64) -> Option<f64> {
        let atr = match &self.atr {
            None => None,
            Some(atr) => Some(atr_decimal(atr.value()?).expect("the ATR is finite")),
        };
        let reference = Decimal::try_from(reference).expect("a finite reference price");
        let length = self.distance.length(reference, atr);
        Some(length.to_f64().expect("a length in f64"))
    }

    /// The target's time limit: how long after its decision a trade on it
    /// closes.
    #[must_use]
    pub const fn limit(&self) -> SignedDuration {
        self.limit
    }

    /// The grid time of the decision that first reads what is made at `ts`
    /// (`decide::decision_of`), whose width this is then.
    #[must_use]
    pub fn decision(&self, ts: UnixNanos) -> UnixNanos {
        decide::decision_of(self.rule, self.delay, ts)
    }
}

impl Debug for BracketWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BracketWidth")
            .field("distance", &self.distance)
            .field("atr", &self.atr.as_ref().map(|atr| atr.bars))
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// What sets the target of a decision on each side, of a dataset run and of
/// a trading strategy alike: its exits, if it has them, from the decision's
/// price (`reference`) and the target's ATR, on the instrument's ticks, and
/// the worth of the unit an expected P&L is weighed by and a row's trades are
/// read over.
pub struct TargetSetter {
    instrument_id: InstrumentId,
    quantity: Quantity,
    spec: Target,
    atr: Option<Rc<TargetAtr>>,
}

impl TargetSetter {
    /// The target of `config`, for decisions on `rule` of a strategy of
    /// `requirements`, on an ATR of its own.
    ///
    /// # Errors
    ///
    /// Returns an error if the target's ATR has no bars (see
    /// `TargetConfig::atr_bars`).
    pub fn new(
        config: &TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<Self> {
        Self::sharing(config, rule, requirements, None)
    }

    /// `new`, on the ATR of `width`, if given, the width of the target's
    /// bracket the strategy's features read (`TargetConfig::width`): one
    /// ATR, which `start` and `warmed` feed, read by both.
    ///
    /// # Errors
    ///
    /// As `new`, or if `width`'s ATR is not the one the target runs: of
    /// other arguments or bars, or none where the target has one or the
    /// reverse.
    pub fn sharing(
        config: &TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
        width: Option<&BracketWidth>,
    ) -> anyhow::Result<Self> {
        let bars = config.atr_bars(rule, requirements)?;
        let atr = match width {
            None => bars.map(TargetAtr::new).transpose()?.map(Rc::new),
            Some(width) => {
                let shared = width.atr.as_ref().map(|atr| atr.bars);
                ensure!(
                    shared == bars,
                    "the width the features read has the ATR {shared:?}, the target {bars:?}"
                );
                width.atr.clone()
            }
        };
        Ok(Self {
            instrument_id: config.instrument_id,
            quantity: config.quantity,
            spec: config.spec,
            atr,
        })
    }

    /// Starts the target's average true range, if any, on `actor`, from
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

    /// `warmup` building the bars of the target's ATR, if any, and handing
    /// them to it (`Warmup::building`): the warmup's bars reach only the
    /// cache, and live the first decision after a start then sets the stop
    /// a dataset run sets there.
    #[must_use]
    pub fn warmed(&self, warmup: Warmup) -> Warmup {
        match &self.atr {
            Some(atr) => atr.warmed(warmup),
            None => warmup,
        }
    }

    /// The target of each side at a decision of `strategy` now, or why none
    /// is set: the ATR not yet initialized, no price of the instrument yet or
    /// an exit on the decision's price (`NoTarget`). Without exits, the
    /// instrument itself is not read, so nothing of it can refuse the target.
    ///
    /// # Errors
    ///
    /// Returns an error if, with exits, the instrument is not in the cache,
    /// or an exit is outside its prices or a stop has no worth (on an
    /// inverse instrument, see `ensure_linear`).
    pub fn at<S>(&self, strategy: &S) -> anyhow::Result<Result<PerSide<SideTarget>, NoTarget>>
    where
        S: DataActor + DataActorNative,
    {
        let atr = match &self.atr {
            None => None,
            Some(atr) => match atr.value() {
                Some(value) => Some(value),
                None => return Ok(Err(NoTarget::ColdAtr(atr.bars.bar_type))),
            },
        };
        let instrument_id = self.instrument_id;
        let cache = strategy.cache();
        let time_limit = self.spec.limit;
        let Some(bracket) = self.spec.bracket else {
            let target = reference(&cache, &instrument_id).ok_or(NoTarget::NoPrice);
            return Ok(target.map(|reference| {
                let target = SideTarget {
                    reference,
                    bracket: None,
                    time_limit,
                    unit: 1.0,
                };
                PerSide {
                    long: target,
                    short: target,
                }
            }));
        };
        let instrument = cache
            .instrument(&instrument_id)
            .with_context(|| format!("instrument {instrument_id} not in cache"))?;
        let Some(reference) = reference(&cache, &instrument_id) else {
            return Ok(Err(NoTarget::NoPrice));
        };
        let Some(exits) = exits(bracket, &instrument, reference.as_decimal(), atr)? else {
            return Ok(Err(NoTarget::ExitOnPrice(reference)));
        };
        let target = |bracket: Bracket| -> anyhow::Result<SideTarget> {
            let distance = (bracket.stop_loss.as_decimal() - reference.as_decimal()).abs();
            let unit = stop_worth(&instrument, self.quantity, distance)?;
            Ok(SideTarget {
                reference,
                bracket: Some(bracket),
                time_limit,
                unit,
            })
        };
        Ok(Ok(PerSide {
            long: target(exits.long)?,
            short: target(exits.short)?,
        }))
    }
}

impl Debug for TargetSetter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetSetter")
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

/// What a stop `distance` in price away is worth on `quantity` of
/// `instrument`, made in its sizes, in its P&L currency: exactly quantity times
/// multiplier times distance, not rounded to the currency's precision, the
/// money made as Nautilus' `Position` makes its P&L. Nautilus' notional value
/// of the distance is rounded to that precision, and it has no call on an
/// instrument returning one unrounded, so this one product is ours, written as
/// it writes it.
///
/// # Errors
///
/// Returns an error if `instrument` is inverse (see `ensure_linear`),
/// `quantity` cannot be made in the instrument's sizes, or the worth overflows
/// or is more money than Nautilus holds.
fn stop_worth(
    instrument: &InstrumentAny,
    quantity: Quantity,
    distance: Decimal,
) -> anyhow::Result<f64> {
    ensure_linear(instrument)?;
    let size = quantity.as_decimal();
    let quantity = instrument
        .try_make_qty_from_decimal(size, None)
        .map_err(|e| anyhow::anyhow!("ordering {size} of {}: {e}", instrument.id()))?;
    let worth = quantity
        .as_decimal()
        .checked_mul(instrument.multiplier().as_decimal())
        .and_then(|value| value.checked_mul(distance))
        .with_context(|| {
            format!(
                "the worth of a stop {distance} away on {size} of {} overflows",
                instrument.id()
            )
        })?;
    Ok(Money::from_decimal_native(worth, instrument.cost_currency())?.as_f64())
}

/// Checks that a target's stops on `instrument` have a worth
/// (`stop_worth`): an inverse instrument's P&L is no multiple of a distance,
/// as Nautilus' notional value on one divides by its price. A time-only target
/// has no stop to weigh, and works on it.
///
/// # Errors
///
/// Returns an error if `instrument` is inverse.
fn ensure_linear(instrument: &InstrumentAny) -> anyhow::Result<()> {
    ensure!(
        !instrument.is_inverse(),
        "{} is inverse: a target's stop on it has no worth in R",
        instrument.id()
    );
    Ok(())
}

/// Nautilus' side of the ATR: it reads the bars of its bar type alone.
impl ActorIndicator for TargetAtr {
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
        if bar.bar_type.standard() == self.bars.bar_type.standard() {
            self.indicator.borrow_mut().handle_bar(bar);
        }
        Ok(())
    }
}

/// `fingerprint` extended with the target of `config` deciding on `rule`
/// for a strategy of `requirements`.
///
/// # Errors
///
/// Returns an error if the target's ATR has no bars (see
/// `TargetConfig::atr_bars`).
pub fn fingerprint(
    fingerprint: Fingerprint,
    config: &TargetConfig,
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
        config: TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<Rc<Self>> {
        Self::sharing(config, rule, requirements, None)
    }

    /// `new`, the setter on the ATR of `width`, if given
    /// (`TargetSetter::sharing`).
    ///
    /// # Errors
    ///
    /// As `new`, or if the setter refuses `width`.
    pub fn sharing(
        config: TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
        width: Option<&BracketWidth>,
    ) -> anyhow::Result<Rc<Self>> {
        config.validate(rule, requirements)?;
        Ok(Rc::new(Self {
            setter: TargetSetter::sharing(&config, rule, requirements, width)?,
            config,
            state: RefCell::new(State::default()),
        }))
    }

    /// Starts the targets on `actor`, from its `on_start`: refuses a bracket
    /// on an inverse instrument, then starts the target's ATR, if any
    /// (`TargetSetter::start`). The refusal is here because a failed `on_start`
    /// ends the node's run with its error, while an error at a decision is only
    /// logged by Nautilus and would leave an empty dataset. A time-only target
    /// has no stop to weigh, and works on an inverse instrument. A bracket's
    /// instrument not in the cache is not refused here: it errs at each
    /// decision (`TargetSetter::at`).
    ///
    /// # Errors
    ///
    /// Returns an error if the target has exits and its instrument in the
    /// cache is inverse (`ensure_linear`).
    pub fn start<A>(&self, actor: &mut A) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        if self.config.spec.bracket.is_some()
            && let Some(instrument) = actor.cache().instrument(&self.config.instrument_id)
        {
            ensure_linear(&instrument)?;
        }
        self.setter.start(actor);
        Ok(())
    }

    /// Opens the target of the decision of `row`, as the setter sets it
    /// (`TargetSetter::at`): on each side, its orders, from `strategy`, and its
    /// time alert. With no price of the instrument yet, the decision is
    /// unsettled, its number taken; with the target's ATR not yet initialized,
    /// or an exit of the target on the decision's price, it opens nothing and
    /// takes no number, as a decision on cold features is none.
    ///
    /// # Errors
    ///
    /// Returns an error as the setter does, before any order; or if the
    /// instrument is not in the cache, the size cannot be made in it or
    /// Nautilus refuses an order or an alert, as the orders are sent, which
    /// unsettles the decision.
    pub fn open<S>(&self, strategy: &mut S, row: FeatureRow) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let time = row.ts_event.to_rfc3339();
        let instrument_id = self.config.instrument_id;
        let next_decision = || {
            let mut state = self.state.borrow_mut();
            state.next += 1;
            state.next - 1
        };
        let targets = match self.setter.at(&*strategy)? {
            Ok(targets) => targets,
            Err(NoTarget::ColdAtr(bar_type)) => {
                log::debug!(
                    "decision at {time}: the ATR of {bar_type} is not initialized; no targets"
                );
                return Ok(());
            }
            Err(NoTarget::NoPrice) => {
                let decision = next_decision();
                log::error!(
                    "decision {decision} at {time}: no price of {instrument_id} to set its \
                     targets from; unsettled"
                );
                return Ok(());
            }
            Err(NoTarget::ExitOnPrice(reference)) => {
                log::debug!(
                    "decision at {time}: an exit of the target is on the price {reference} of \
                     {instrument_id}; no targets"
                );
                return Ok(());
            }
        };
        let decision = next_decision();
        self.state.borrow_mut().open.insert(
            decision,
            Decision::new(row, targets.map(|target| target.unit)),
        );
        let sent = self.send(strategy, decision, &targets);
        if let Err(e) = &sent {
            self.unsettle(decision, format_args!("sending its orders: {e}"));
        }
        sent
    }

    /// Sends the trades of `decision`, the long then the short, on `targets`,
    /// each side named by its ids.
    fn send<S>(
        &self,
        strategy: &mut S,
        decision: u64,
        targets: &PerSide<SideTarget>,
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        for side in Side::BOTH {
            let target = targets.of(side);
            let trade = Trade {
                instrument_id: self.config.instrument_id,
                side: side.order_side(),
                quantity: self.config.quantity,
                exits: Exits {
                    bracket: target.bracket,
                    time_limit: Some(target.time_limit),
                },
            };
            trade.send_as(strategy, DecisionSide { decision, side }.order_ids())?;
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
        let Some((target, Leg::Entry)) = DecisionSide::parse(entry) else {
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
    /// from `strategy`, once both sides have one. A position closed by an
    /// order of no exit of its target leaves its decision unsettled.
    pub fn on_position_closed<S>(&self, strategy: &S, event: &PositionClosed)
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let Some((target, Leg::Entry)) = DecisionSide::parse(event.opening_order_id) else {
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
                target.side,
                decision.outcome(target.side, pnl, commission, exit),
            );
            let Some(outcomes) = decision.outcomes() else {
                return;
            };
            let row = TargetRow {
                decision: decision.row.clone(),
                outcomes,
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
        if let Some((target, _)) = DecisionSide::parse(id) {
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
    /// The decision of `row`, `units` set on its sides.
    fn new(row: FeatureRow, units: PerSide<f64>) -> Self {
        Self {
            row,
            units,
            outcomes: BTreeMap::new(),
        }
    }

    /// The outcome of its target on `side`, its position closed with
    /// `pnl`, net of `commission`, by `exit`, with the unit it set.
    fn outcome(&self, side: Side, pnl: Money, commission: Money, exit: Exit) -> Outcome {
        Outcome {
            pnl,
            commission,
            exit,
            unit: *self.units.of(side),
        }
    }

    /// The outcomes of its target, once it has both.
    fn outcomes(&self) -> Option<Outcomes> {
        Some(Outcomes {
            long: *self.outcomes.get(&Side::Long)?,
            short: *self.outcomes.get(&Side::Short)?,
        })
    }
}

impl DecisionSide {
    /// The client order ids of its orders, its legs' by `order_id`.
    fn order_ids(self) -> OrderIds {
        OrderIds {
            entry: self.order_id(Leg::Entry),
            take_profit: self.order_id(Leg::TakeProfit),
            stop_loss: self.order_id(Leg::Stop),
        }
    }

    /// The client order id of `leg`: `D{n}-{L|S}-{E|TP|SL}`.
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
        let id = format!("D{}-{side}-{leg}", self.decision);
        ClientOrderId::from(id.as_str())
    }

    /// The decision's side and leg `id` names, if a target's.
    fn parse(id: ClientOrderId) -> Option<(Self, Leg)> {
        let (decision, rest) = id.as_str().strip_prefix('D')?.split_once('-')?;
        let (side, leg) = rest.split_once('-')?;
        let side = match side {
            "L" => Side::Long,
            "S" => Side::Short,
            _ => return None,
        };
        let leg = match leg {
            "E" => Leg::Entry,
            "TP" => Leg::TakeProfit,
            "SL" => Leg::Stop,
            _ => return None,
        };
        Some((
            Self {
                decision: decision.parse().ok()?,
                side,
            },
            leg,
        ))
    }
}

impl Side {
    /// The long, then the short.
    pub const BOTH: [Self; 2] = [Self::Long, Self::Short];

    /// The name of the side.
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
    target: DecisionSide,
    closing: ClientOrderId,
) -> Option<Exit> {
    match DecisionSide::parse(closing) {
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
fn reference(cache: &CacheApi, instrument_id: &InstrumentId) -> Option<Price> {
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
