//! The target of a strategy's decisions, read from real orders on Nautilus' simulated venue.
//!
//! In a dataset run, `Targets::open` sends a long and a short order at each decision, on a hedging
//! margin account so that each is a position of its own, through `orders::Trade::send_at` as a
//! strategy trading on a model of the target would. A `Target` has one limit and a bracket or none:
//! with one, a market entry, a take-profit limit and a stop; without, a market entry beside the ATR
//! its unit is measured in (`Target::atr`). What is open at the limit is closed after the send.
//!
//! Every target is sized at the reference stake: the config's `[trade] risk`, multiple 1, of the
//! dataset balance as a constant equity, converted into the P&L currency at the rates cached at the
//! decision (`Rates::convert`) and sized by Nautilus' fixed-risk sizer (`orders::Trade::size`). One
//! R is the stop's distance, or a time-only target's ATR. The unit, the payoff features and the
//! sent size are at that size, so a row and its payoff features are priced alike in training and
//! live. No equity is read.
//!
//! Exits are `take_profit` and `stop` away from the decision's price (`EntryQuote`), each a
//! `Distance` in basis points or in multiples of Nautilus' `AverageTrueRange`, and each on the
//! instrument's nearest tick, a tie away from the price. An exit on the price, a size that rounds
//! down to zero, or an ATR not yet initialized or nought, opens no target.
//!
//! The ATR is built by `AverageTrueRange::new` as the config is validated, so an argument Nautilus
//! refuses panics at load. Its bars are Nautilus' bars of the instrument at `bar_interval` (the
//! decision cadence if omitted), built from quote mids, else trade last prices, else a composite
//! bar type over the strategy's source bars (`TargetConfig::atr_bars`). A decision reads only bars
//! closed through its grid time, including a coincident close: a backtest replays a source bar
//! stamped at its close before the timer closing the cadence bar at that instant. Temporary: no bar
//! is registered with Nautilus, until Nautilus holds and orders updates (helix-trader#16 and
//! helix-trader#46).
//!
//! A feature formed over the bracket (`model::features::OverBracket`) reads its width
//! (`BracketWidth`) on the ATR the setter runs on (`TargetSetter::sharing`). The handle reads a
//! decision's features and sets its target in one event, so the width is the one the target is set
//! at.
//!
//! A client order id is `D{n}-{L|S}-{E|TP|SL}`: the decision's number in the run, long or short,
//! entry, take profit or stop. The outcome is read from Nautilus' events alone, so the only state
//! of our own is the rows being assembled. The commission is the run's (`backtest::Commission`),
//! handed to the setter in every mode.
//!
//! The names of the payoff features (`PayoffFeature`) are written here alone
//! (`TargetConfig::payoff_names`), in the order of the values.
//!
//! Once both sides have an outcome, the decision's `TargetRow` is published on the message bus. A
//! decision whose order the venue rejects, whose entry has not filled by its time limit, or whose
//! positions are open at the stop is unsettled: logged, with no row. An order the risk engine
//! denies fails the run (`on_order_denied`): a stake the balance cannot carry is the config's
//! fault, and a missing row would bias the training.
//!
//! `TargetSetter::at` sets a target for a dataset run and a trading strategy alike. A unit and a
//! payoff have a worth only on a linear instrument, at a commission charged in the P&L currency,
//! which is not converted. `Targets::start` refuses an inverse instrument, another commission
//! currency, and a balance the pairs do not reach the P&L currency from; live, the setter errors at
//! each decision. A decision with no rate sets no target (`NoTarget::NoRate`).
//!
//! A quote at the same nanosecond as a time alert reaches the venue first, so an exit hit at the
//! time limit counts as inside the window. On bars alone the venue replays each bar's open, high,
//! low and close: an entry fills at the last close, a take profit at its limit, a stop at its
//! trigger when the bar moves through it after its open, and at the open when the bar opens beyond
//! it (a sell stop at 100 fills at 90 on a bar opening at 90). Bars-only targets are as optimistic
//! as bars-only backtests.

use std::{
    any::Any,
    cell::RefCell,
    collections::BTreeMap,
    fmt::{Debug, Display, Write as _},
    rc::Rc,
    sync::Arc,
};

use anyhow::Context;
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
    types::{Currency, Money, Price, Quantity},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{
    dataset::DatasetVenue,
    decide::{self, DecisionRule, FeatureRow},
    features::Fingerprint,
    handle::SettledRows,
};
use crate::{
    backtest::{self, Commission},
    strategy::{
        AtrArgs, IndicatorArgs, TradedInstrument,
        conversions::{Conversions, Rates},
        orders::{self, Bracket, Exits, OrderIds, Trade, worth},
        requirements::DataRequirements,
        risk::{Risk, Stake},
        warmup::{self, Warmup},
    },
};

/// The target of each decision on one instrument, at the reference stake. A strategy's TOML has
/// the `Target` alone (`Target::trading`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    /// The instrument the target trades.
    pub instrument_id: InstrumentId,
    /// The config's `[trade] risk`: the reference stake, multiple 1, of the dataset balance.
    pub risk: Risk,
    /// The config's `[trade] conversions`, converting the dataset balance into the P&L currency;
    /// none if it lists none. Left out of a manifest when none.
    #[serde(default, skip_serializing_if = "Conversions::is_empty")]
    #[builder(default)]
    pub conversions: Conversions,
    /// How its trades end.
    pub spec: Target,
}

/// How a decision's trades end, as a strategy's TOML has it:
/// `{ bracket = { take_profit = { atr = 2.0 }, stop = { bps = 20 } }, limit = "2h" }`, or time
/// only, `{ limit = "2h", atr = { period = 14 } }`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct Target {
    /// Its exits, if it has them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bracket: Option<BracketSpec>,
    /// How long after the decision what is still open is closed.
    pub limit: SignedDuration,
    /// The ATR of a time-only target, whose worth as a stop's distance is its unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[builder(into)]
    pub atr: Option<TargetAtrConfig>,
}

/// Target-specific bar selection alongside the Nautilus indicator arguments.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetAtrConfig {
    /// Nautilus' ATR arguments; the period counts completed bars of this interval.
    #[serde(flatten)]
    pub args: AtrArgs,
    /// The fixed length of each ATR bar, or the decision cadence when omitted. A distinct interval
    /// may not close another bar within the decision timer's margin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bar_interval: Option<SignedDuration>,
}

impl From<AtrArgs> for TargetAtrConfig {
    fn from(args: AtrArgs) -> Self {
        Self {
            args,
            bar_interval: None,
        }
    }
}

/// A target's exits, each a distance from the decision's price.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BracketSpec {
    /// How far the take profit is, in the side's favor.
    pub take_profit: Distance,
    /// How far the stop is, against the side.
    pub stop: Distance,
    /// The ATR the `atr` distances are multiples of, if not `DEFAULT_ATR`: only with a distance in
    /// ATRs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atr: Option<TargetAtrConfig>,
}

/// How far an exit is from the decision's price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Distance {
    /// Basis points of the decision's price, `{ bps = 20 }`.
    #[serde(rename = "bps")]
    BasisPoints(Decimal),
    /// Multiples of the target's average true range at the decision, `{ atr = 1.5 }`.
    #[serde(rename = "atr")]
    Atr(Decimal),
}

/// The ATR of a bracket without an `atr` table: Nautilus' `AverageTrueRange` of Wilder's 14 bars,
/// its other arguments Nautilus' defaults.
pub const DEFAULT_ATR: AtrArgs = AtrArgs {
    period: 14,
    ma_type: None,
    use_previous: None,
    value_floor: None,
};

const ATR_UNITS: [BarAggregation; 5] = [
    BarAggregation::Day,
    BarAggregation::Hour,
    BarAggregation::Minute,
    BarAggregation::Second,
    BarAggregation::Millisecond,
];

/// The ATR a target runs: `args`, on bars of `bar_type` (`TargetConfig::atr_bars`).
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
    /// The position's realized P&L, net of its commissions.
    pub pnl: Money,
    /// The position's commissions in the P&L's currency (Nautilus' `Position::commissions`).
    pub commission: Money,
    pub exit: Exit,
    /// The unit of the side's payoff, as the decision set it (`SideTarget::unit`).
    pub unit: Money,
}

/// The outcome of a decision's target on each side.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Outcomes {
    pub long: Outcome,
    pub short: Outcome,
}

/// A settled decision, published on the message bus.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TargetRow {
    /// The decision, as `decide` published it.
    pub decision: FeatureRow,
    /// The payoff features of each side's target (`SideTarget::payoff`).
    pub payoff: PerSide<Vec<f64>>,
    /// The target's outcomes.
    pub outcomes: Outcomes,
    /// When the last of its positions closed.
    pub settled_at: UnixNanos,
}

/// The targets of a strategy's decisions, shared as an `Rc`: the strategy calls `open` at each
/// decision and hands its order, position and time events to the methods of their names.
pub struct Targets {
    config: TargetConfig,
    setter: TargetSetter,
    state: RefCell<State>,
}

#[derive(Default)]
struct State {
    next: u64,
    open: BTreeMap<u64, Decision>,
}

struct Decision {
    row: FeatureRow,
    payoff: PerSide<Vec<f64>>,
    units: PerSide<Money>,
    outcomes: BTreeMap<Side, Outcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DecisionSide {
    decision: u64,
    side: Side,
}

/// A side of a target, and of a model: the long or the short, `"long"` or `"short"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Long,
    Short,
}

/// A value for each side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

    /// A reference to each side's value.
    #[must_use]
    pub const fn as_ref(&self) -> PerSide<&T> {
        PerSide {
            long: &self.long,
            short: &self.short,
        }
    }
}

/// A target as a decision sets it on one side (`TargetSetter::at`): `one_r` is the price distance
/// of one R, `equity` the dataset balance in the P&L currency at the decision's rate, `unit` what
/// one R is worth on the trade's size.
#[derive(Debug, Clone, PartialEq)]
pub struct SideTarget {
    pub reference: Price,
    pub bracket: Option<Bracket>,
    pub time_limit: SignedDuration,
    pub one_r: Decimal,
    pub equity: Money,
    pub unit: Money,
    pub payoff: Vec<f64>,
}

/// A payoff feature of a side's target (`SideTarget::payoff`), money over the side's unit.
///
/// The side enters at the ask (long) or bid (short), or without a quote at the target's price.
/// `c(p)` is the commission on a fill of the side's size at `p` (`Commission::cost`). For a long of
/// take profit `up` and stop `down`:
///
/// - `TakeProfit`, the take profit's R: (up − ask − c(ask) − c(up)) / unit;
/// - `StopLoss`, the stop's nominal loss, before any overshoot: (ask − down + c(ask) + c(down)) /
///   unit;
/// - `EntryCost`, the spread plus the entry's commission: (ask − bid + c(ask)) / unit.
///
/// A short mirrors them: its take profit below, its stop above, entering at the bid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayoffFeature {
    TakeProfit,
    StopLoss,
    EntryCost,
}

/// Why a decision sets no target (`TargetSetter::at`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoTarget {
    /// The target's ATR, on bars of this type, is not yet initialized.
    ColdAtr(BarType),
    /// The target's ATR, on bars of this type, is nought.
    FlatAtr(BarType),
    /// The instrument has no price yet to set it from.
    NoPrice,
    /// An exit is on the decision's price, this one.
    ExitOnPrice(Price),
    /// The dataset balance has no rate into `pnl`, the instrument's P&L currency, at `rates`: no
    /// pair reaches it, or none is cached yet (`Rates::convert`).
    NoRate {
        balance: Money,
        pnl: Currency,
        rates: Rates,
    },
    /// The reference stake buys none of the instrument on this side: its one R is worth more than
    /// the stake on its size increment.
    NoSize { side: Side, one_r: Decimal },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Entry,
    TakeProfit,
    Stop,
}

impl TargetConfig {
    // panics-doc-ok (transitive via AverageTrueRange::new)
    /// Reports every violation of the target under its `[model]` field, as one
    /// `ConfigError::Multiple` if several.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A distance or the limit is not positive.
    /// - An `atr` table is used by no distance, is missing from a time-only target, or is beside a
    ///   bracket.
    /// - An ATR has no bars at the cadence of `rule` (see `atr_bars`).
    ///
    /// # Panics
    ///
    /// Panics if Nautilus refuses an argument of the target's ATR.
    pub fn validate(
        &self,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();
        let spec = self.spec;
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
        errors.check(
            spec.bracket.is_some() || spec.atr.is_some(),
            ConfigError::invalid_value(
                "model.target.atr",
                "the time-only target declares no ATR, `atr = { period = 14 }`: its unit, \
                 which its trades' P&L is read over, is its ATR's worth as a stop's distance",
            ),
        );
        errors.check(
            spec.bracket.is_none() || spec.atr.is_none(),
            ConfigError::invalid_value(
                "model.target.atr",
                "the target has a bracket, whose ATR is its own, `model.target.bracket.atr`",
            ),
        );
        let atr = |result: anyhow::Result<()>| {
            result.map_err(|e| ConfigError::invalid_value(spec.atr_field(), format!("{e:#}")))
        };

        if let Some(args) = spec.atr_args() {
            // Nautilus checks the arguments as it builds the ATR
            errors.collect(atr(args.args.build().map(drop)));
        }
        errors.collect(atr(self.atr_bars(rule, requirements).map(drop)));
        errors.into_result()
    }

    /// Returns the target's ATR deciding on `rule` for `requirements`, if it has one. Its bars are
    /// Nautilus' internal bars of the instrument at its ATR interval, from the first of:
    ///
    /// - Its quotes: their mids, `XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL`.
    /// - Its trades: their last prices.
    /// - Its source bars (`DataRequirements::source_bars`): a composite bar type, the longest whose
    ///   length the interval is a whole multiple of, the first at a tie.
    ///
    /// The step is the interval in its largest whole unit, from days to milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Nautilus builds no bars of the interval (a step must divide the next unit,
    ///   `BarSpecification::new_checked`).
    /// - The strategy reads neither quotes, trades nor bars of the instrument.
    /// - On bars alone, the interval is a whole multiple of none of them.
    pub fn atr_bars(
        &self,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<Option<AtrBars>> {
        let Some(config) = self.spec.atr_args() else {
            return Ok(None);
        };
        let DecisionRule::Every(default_interval) = rule;
        let cadence = config.bar_interval.unwrap_or(default_interval);
        let setting = if config.bar_interval.is_some() {
            "bar interval"
        } else {
            "decision cadence"
        };
        anyhow::ensure!(
            cadence.is_positive(),
            "the target's ATR {setting} {cadence:#} is not positive"
        );
        let nanos = cadence.as_nanos();
        if cadence != default_interval && default_interval.is_positive() {
            // Both timers have the same build delay, so the smallest positive gap from a decision
            // to a bar close is the greatest common divisor of their periods.
            let (mut gap, mut remainder) = (nanos, default_interval.as_nanos());
            while remainder != 0 {
                (gap, remainder) = (remainder, gap % remainder);
            }
            anyhow::ensure!(
                gap > decide::DECISION_MARGIN.as_nanos(),
                "the target's ATR bar interval {cadence:#} can close after a decision on \
                 cadence {default_interval:#} but within its timer margin {:#}",
                decide::DECISION_MARGIN
            );
        }
        let Some((aggregation, step)) = ATR_UNITS
            .into_iter()
            .map(|aggregation| {
                let unit = BarSpecification::new(1, aggregation, PriceType::Last).timedelta();
                (aggregation, unit.as_nanos())
            })
            .find(|(_, unit)| nanos % unit == 0)
            .map(|(aggregation, unit)| (aggregation, nanos / unit))
        else {
            anyhow::bail!(
                "the target's ATR has no bars of the {setting} {cadence:#}, not whole \
                 milliseconds"
            );
        };
        let spec = |price_type| {
            usize::try_from(step)
                .map_err(anyhow::Error::from)
                .and_then(|step| BarSpecification::new_checked(step, aggregation, price_type))
                .with_context(|| {
                    format!("the target's ATR has no bars of the {setting} {cadence:#}")
                })
        };
        let traded = self.instrument_id;
        let series = requirements.catalog_keys();
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
            anyhow::ensure!(
                !sources.is_empty(),
                "the target's ATR has no bars of {traded} to be built from: the strategy reads \
                 neither its quotes, its trades nor its bars"
            );
            let Some(source) = longest_dividing(&sources, cadence) else {
                let sources: Vec<String> = sources.iter().map(ToString::to_string).collect();
                anyhow::bail!(
                    "the target's ATR has no bars of the {setting} {cadence:#}: it is \
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
            args: config.args,
        }))
    }

    /// The config as the fingerprint reads it. Its pairs are included only if it has any, so that a
    /// config of none keeps the keys of stores made before configs could list them.
    fn fingerprint_part(&self, atr: Option<AtrBars>) -> anyhow::Result<String> {
        let atr = atr
            .map(|atr| anyhow::Ok(format!("{} on bars of {}", atr.args.build()?, atr.bar_type)))
            .transpose()?;
        let exits = self
            .spec
            .bracket
            .map(|bracket| format!("take profit {} stop {}", bracket.take_profit, bracket.stop));
        let mut part = format!(
            "{} risk {} exits {exits:?} limit {} ns atr {atr:?}",
            self.instrument_id,
            self.risk,
            self.spec.limit.as_nanos()
        );
        if !self.conversions.is_empty() {
            let pairs: Vec<String> = self
                .conversions
                .pairs()
                .iter()
                .map(ToString::to_string)
                .collect();
            write!(part, " conversions [{}]", pairs.join(", "))?;
        }
        Ok(part)
    }

    /// The names of its payoff features, in the order of a side's values.
    #[must_use]
    pub fn payoff_names(&self) -> Vec<String> {
        self.spec
            .payoff_features()
            .iter()
            .map(|feature| feature.name().to_string())
            .collect()
    }

    /// The longest time a decision's target stays open: its limit. A time close fills at once on a
    /// venue without latency, else the delay later (`dataset::DatasetVenue`).
    #[must_use]
    pub const fn settles_within(&self) -> SignedDuration {
        self.spec.limit
    }

    /// Returns the width of its bracket (see `BracketWidth`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The target has no bracket.
    /// - Its take profit and stop are not one distance.
    /// - Its ATR has no bars (see `atr_bars`).
    pub fn width(
        &self,
        rule: DecisionRule,
        requirements: &DataRequirements,
    ) -> anyhow::Result<BracketWidth> {
        let Some(bracket) = self.spec.bracket else {
            anyhow::bail!("the target has no bracket to have a width");
        };
        anyhow::ensure!(
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
    /// The target trading `traded` at `risk` through `conversions`.
    #[must_use]
    pub fn trading(
        self,
        traded: &TradedInstrument,
        risk: Risk,
        conversions: Conversions,
    ) -> TargetConfig {
        TargetConfig {
            instrument_id: traded.instrument_id,
            risk,
            conversions,
            spec: self,
        }
    }

    /// Its ATR: the bracket's `atr` table, else `DEFAULT_ATR`; time only, its own.
    fn atr_args(&self) -> Option<TargetAtrConfig> {
        match self.bracket {
            Some(bracket) => bracket
                .uses_atr()
                .then(|| bracket.atr.unwrap_or_else(|| DEFAULT_ATR.into())),
            None => self.atr,
        }
    }

    /// The field of `[model]` its ATR is declared under.
    #[must_use]
    pub const fn atr_field(&self) -> &'static str {
        match self.bracket {
            Some(_) => "model.target.bracket.atr",
            None => "model.target.atr",
        }
    }

    const fn payoff_features(&self) -> &'static [PayoffFeature] {
        match self.bracket {
            Some(_) => &[
                PayoffFeature::TakeProfit,
                PayoffFeature::StopLoss,
                PayoffFeature::EntryCost,
            ],
            None => &[PayoffFeature::EntryCost],
        }
    }
}

impl PayoffFeature {
    /// Its name, in a dataset's and a model's feature names.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::TakeProfit => "take_profit_r",
            Self::StopLoss => "stop_loss_r",
            Self::EntryCost => "entry_cost",
        }
    }
}

impl BracketSpec {
    fn uses_atr(&self) -> bool {
        [self.take_profit, self.stop]
            .iter()
            .any(|distance| matches!(distance, Distance::Atr(_)))
    }
}

impl Distance {
    fn value(self) -> Decimal {
        match self {
            Self::BasisPoints(value) | Self::Atr(value) => value,
        }
    }

    fn length(self, reference: Decimal, atr: Option<Decimal>) -> Decimal {
        match self {
            Self::BasisPoints(bps) => reference * bps / Decimal::from(10_000),
            Self::Atr(multiple) => atr.expect("a validated target has its ATR") * multiple,
        }
    }
}

impl Display for Distance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BasisPoints(bps) => write!(f, "{bps} bps"),
            Self::Atr(multiple) => write!(f, "{multiple} atr"),
        }
    }
}

// panics-doc-ok (transitive via Distance::length)
/// Returns the take profit and stop of each side of `bracket`, each on the nearest tick of
/// `instrument`. Returns none if an exit rounds onto `reference`.
///
/// # Errors
///
/// Returns an error if:
/// - `atr` is not finite.
/// - An exit is outside the instrument's prices.
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
        let favor = side.sign();
        Ok(Bracket {
            take_profit: orders::nearest_tick(instrument, reference + favor * take_profit, favor)?,
            stop_loss: orders::nearest_tick(instrument, reference - favor * stop, -favor)?,
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

/// Of `sources`, the longest bars whose length `cadence` is a whole multiple of, the first at a
/// tie. Bars of no fixed length (ticks, volumes, months) divide no cadence.
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

    fn value(&self) -> Option<f64> {
        let indicator = self.indicator.borrow();
        indicator.initialized.then_some(indicator.value)
    }

    fn warmed(self: &Rc<Self>, warmup: Warmup) -> Warmup {
        warmup.building(self.bars.bar_type, Rc::clone(self) as SharedActorIndicator)
    }
}

/// The width of a target's bracket whose take profit and stop are one distance
/// (`TargetConfig::width`), before the exits go on ticks (`Distance::length`).
pub struct BracketWidth {
    distance: Distance,
    atr: Option<Rc<TargetAtr>>,
    limit: SignedDuration,
    rule: DecisionRule,
    /// How long after their grid times decisions fire, before their margin.
    delay: SignedDuration,
}

impl BracketWidth {
    /// Whether it has a length: its ATR, if any, initialized.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.atr.as_ref().is_none_or(|atr| atr.value().is_some())
    }

    /// The width at `reference`, the decision's price, in prices; none until its ATR is ready.
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

    /// The target's time limit.
    #[must_use]
    pub const fn limit(&self) -> SignedDuration {
        self.limit
    }

    /// The grid time of the decision that first reads what is made at `ts` (`decide::decision_of`).
    #[must_use]
    pub fn decision(&self, ts: UnixNanos) -> UnixNanos {
        decide::decision_of(self.rule, self.delay, ts)
    }
}

impl Debug for BracketWidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BracketWidth))
            .field("distance", &self.distance)
            .field("atr", &self.atr.as_ref().map(|atr| atr.bars))
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// Sets the target of a decision on each side, for a dataset run and a trading strategy alike.
pub struct TargetSetter {
    instrument_id: InstrumentId,
    stake: Stake,
    balance: Money,
    rates: Rates,
    spec: Target,
    atr: Option<Rc<TargetAtr>>,
    commission: Commission,
}

impl TargetSetter {
    /// The target of `config`, for decisions on `rule` of a strategy of `requirements`, of the
    /// dataset runs on `venue`, on an ATR of its own.
    ///
    /// # Errors
    ///
    /// Returns an error if the target's ATR has no bars (see `TargetConfig::atr_bars`).
    pub fn new(
        config: &TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Self> {
        Self::sharing(config, rule, requirements, venue, None)
    }

    /// Like `new`, on the ATR of `width`, if given, which `start` and `warmed` feed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The target's ATR has no bars (see `TargetConfig::atr_bars`).
    /// - `width`'s ATR is not the one the target runs: of other arguments or bars, or none where
    ///   the target has one or the reverse.
    pub fn sharing(
        config: &TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
        width: Option<&BracketWidth>,
    ) -> anyhow::Result<Self> {
        let bars = config.atr_bars(rule, requirements)?;
        let atr = match width {
            None => bars.map(TargetAtr::new).transpose()?.map(Rc::new),
            Some(width) => {
                let shared = width.atr.as_ref().map(|atr| atr.bars);
                anyhow::ensure!(
                    shared == bars,
                    "the width the features read has the ATR {shared:?}, the target {bars:?}"
                );
                width.atr.clone()
            }
        };
        Ok(Self {
            instrument_id: config.instrument_id,
            stake: Stake::new(config.risk),
            balance: venue.balance,
            rates: config.conversions.rates(),
            spec: config.spec,
            atr,
            commission: venue.commission,
        })
    }

    /// Whether a target can be set as far as its ATR goes: none, or initialized.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.atr.as_ref().is_none_or(|atr| atr.value().is_some())
    }

    /// The bar type of the target's ATR, if any, which the strategy subscribes to.
    #[must_use]
    pub fn atr_bar_type(&self) -> Option<BarType> {
        self.atr.as_ref().map(|atr| atr.bars.bar_type)
    }

    /// `warmup` building the bars of the target's ATR, if any. The warmup's bars reach only the
    /// cache, so live the first decision after a start sets the stop a dataset run sets there.
    #[must_use]
    pub fn warmed(&self, warmup: Warmup) -> Warmup {
        match &self.atr {
            Some(atr) => atr.warmed(warmup),
            None => warmup,
        }
    }

    /// Returns the target of each side at a decision of `strategy` now, or why none is set
    /// (`NoTarget`). Each side is sized at the reference stake of the dataset balance, converted at
    /// the rates cached now (`Rates::convert`), whatever any account holds.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The instrument is not in the cache.
    /// - Its payoff has no worth (`ensure_payable`).
    /// - The balance converted overflows.
    /// - An exit or a stop is outside its prices.
    ///
    /// # Panics
    ///
    /// Panics if a time-only target has no ATR, which `TargetConfig::validate` refuses.
    pub fn at<S>(&self, strategy: &S) -> anyhow::Result<Result<PerSide<SideTarget>, NoTarget>>
    where
        S: DataActor + DataActorNative,
    {
        let atr = match &self.atr {
            None => None,
            Some(atr) => match atr.value() {
                None => return Ok(Err(NoTarget::ColdAtr(atr.bars.bar_type))),
                Some(0.0) => return Ok(Err(NoTarget::FlatAtr(atr.bars.bar_type))),
                Some(value) => Some(value),
            },
        };
        let instrument_id = self.instrument_id;
        let cache = strategy.cache();
        let instrument = cache
            .instrument(&instrument_id)
            .with_context(|| format!("instrument {instrument_id} not in cache"))?;
        self.ensure_payable(&instrument)?;
        let Some(quote) = EntryQuote::last(&cache, &instrument_id) else {
            return Ok(Err(NoTarget::NoPrice));
        };
        let reference = quote.reference;
        let exits = match self.spec.bracket {
            None => None,
            Some(bracket) => match exits(bracket, &instrument, reference.as_decimal(), atr)? {
                Some(exits) => Some(exits),
                None => return Ok(Err(NoTarget::ExitOnPrice(reference))),
            },
        };
        let pnl = instrument.cost_currency();
        let converted = self
            .rates
            .convert(&strategy.cache_ref(), [self.balance], pnl)?;
        let Some(equity) = converted else {
            return Ok(Err(NoTarget::NoRate {
                balance: self.balance,
                pnl,
                rates: self.rates,
            }));
        };
        let target = |side: Side| -> anyhow::Result<Result<SideTarget, NoTarget>> {
            let bracket = exits.map(|exits| *exits.of(side));
            let one_r = match bracket {
                Some(bracket) => (bracket.stop_loss.as_decimal() - reference.as_decimal()).abs(),
                None => atr_decimal(atr.expect("a validated time-only target has its ATR"))?,
            };

            // Its unit and payoff are set once the trade is sized
            let mut target = SideTarget {
                reference,
                bracket,
                time_limit: self.spec.limit,
                one_r,
                equity,
                unit: Money::zero(pnl),
                payoff: vec![],
            };
            let trade = target.trade(instrument_id, side);
            let quantity = trade.size(&instrument, self.stake, equity)?;
            if quantity.is_zero() {
                return Ok(Err(NoTarget::NoSize { side, one_r }));
            }
            target.unit = trade.risk(&instrument, quantity)?;
            target.payoff =
                self.payoff(&instrument, side, quote, bracket, quantity, target.unit)?;
            Ok(Ok(target))
        };
        Ok(match (target(Side::Long)?, target(Side::Short)?) {
            (Ok(long), Ok(short)) => Ok(PerSide { long, short }),
            (Err(none), _) | (_, Err(none)) => Err(none),
        })
    }

    /// Returns the payoff features of `side`'s target, in the order of the target's, over `unit`.
    /// # Errors
    ///
    /// Returns an error if:
    /// - A fill's cost cannot be computed (`Commission::cost`).
    /// - A worth overflows.
    fn payoff(
        &self,
        instrument: &InstrumentAny,
        side: Side,
        quote: EntryQuote,
        bracket: Option<Bracket>,
        quantity: Quantity,
        unit: Money,
    ) -> anyhow::Result<Vec<f64>> {
        let entry = quote.entry(side);
        let cost = |price: Price| -> anyhow::Result<Decimal> {
            Ok(self
                .commission
                .cost(quantity, price, instrument)?
                .as_decimal())
        };
        let net = |exit: Price| -> anyhow::Result<Decimal> {
            let gross = side.sign() * (exit.as_decimal() - entry.as_decimal());
            Ok(worth(instrument, quantity, gross)? - cost(entry)? - cost(exit)?)
        };
        let exit_prices = || bracket.expect("a target with the features of exits has a bracket");
        self.spec
            .payoff_features()
            .iter()
            .map(|feature| {
                let money = match feature {
                    PayoffFeature::TakeProfit => net(exit_prices().take_profit)?,
                    PayoffFeature::StopLoss => -net(exit_prices().stop_loss)?,
                    PayoffFeature::EntryCost => {
                        let spread = quote.ask.as_decimal() - quote.bid.as_decimal();
                        worth(instrument, quantity, spread)? + cost(entry)?
                    }
                };
                let money = money
                    .to_f64()
                    .with_context(|| format!("the payoff {money} in f64"))?;
                Ok(money / unit.as_f64())
            })
            .collect()
    }

    /// Checks that the payoff has a worth: the instrument is linear and the commission is charged
    /// in its P&L currency, which Nautilus nets a position's realized P&L of alone.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `instrument` is inverse.
    /// - The commission is charged in another currency.
    fn ensure_payable(&self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        orders::ensure_linear(instrument)?;
        let (charged, pnl) = (
            self.commission.currency(instrument),
            instrument.cost_currency(),
        );
        anyhow::ensure!(
            charged == pnl,
            "the commission {} is charged in {charged}, not in {pnl}, the currency of the P&L \
             of {}: a target's costs are not converted",
            self.commission.normalized(),
            instrument.id()
        );
        Ok(())
    }
}

impl Debug for TargetSetter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TargetSetter))
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

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

    /// Nautilus routes a bar by instrument and specification; the ATR reads only its bar type's,
    /// of the same aggregation source.
    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        if bar.bar_type.standard() == self.bars.bar_type.standard() {
            self.indicator.borrow_mut().handle_bar(bar);
        }
        Ok(())
    }
}

/// `fingerprint` extended with the target of `config` deciding on `rule` for a strategy of
/// `requirements`.
///
/// # Errors
///
/// Returns an error if the target's ATR has no bars (see `TargetConfig::atr_bars`).
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

impl SideTarget {
    /// The trade of the target on `side` of `instrument_id`: the one the setter sizes it by.
    #[must_use]
    pub const fn trade(&self, instrument_id: InstrumentId, side: Side) -> Trade {
        Trade::new(
            instrument_id,
            side.order_side(),
            self.reference,
            self.one_r,
            Exits {
                bracket: self.bracket,
                time_limit: Some(self.time_limit),
            },
        )
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
    /// The targets of `config`, for decisions on `rule` of a strategy of `requirements`, of a
    /// dataset run on `venue`.
    ///
    /// # Errors
    ///
    /// Returns an error if `TargetSetter::new` would.
    pub fn new(
        config: TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Rc<Self>> {
        Self::sharing(config, rule, requirements, venue, None)
    }

    /// Like `new`, the setter on the ATR of `width`, if given (`TargetSetter::sharing`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `new` would.
    /// - The setter refuses `width` (`TargetSetter::sharing`).
    pub fn sharing(
        config: TargetConfig,
        rule: DecisionRule,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
        width: Option<&BracketWidth>,
    ) -> anyhow::Result<Rc<Self>> {
        Ok(Rc::new(Self {
            setter: TargetSetter::sharing(&config, rule, requirements, venue, width)?,
            config,
            state: RefCell::new(State::default()),
        }))
    }

    /// Starts the targets on `actor`, from its `on_start`, because a failed `on_start` ends the
    /// node's run with its error, while an error at a decision is only logged by Nautilus and would
    /// leave an empty dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The target's instrument in the cache is inverse.
    /// - The commission is charged in another currency than its P&L's.
    /// - The pairs do not reach its P&L currency from the balance's.
    pub fn start<A>(&self, actor: &A) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative,
    {
        let cache = actor.cache();
        if let Some(instrument) = cache.instrument(&self.config.instrument_id) {
            self.setter.ensure_payable(&instrument)?;
            let pairs: Vec<InstrumentAny> = self
                .config
                .conversions
                .pairs()
                .iter()
                .filter_map(|pair| cache.instrument(pair))
                .collect();
            backtest::refuse_balance(self.setter.balance, &[instrument], &pairs)
                .context("the dataset balance, which the targets are sized at")?;
        }
        Ok(())
    }

    /// `warmup` building the targets' ATR bars (`TargetSetter::warmed`).
    #[must_use]
    pub fn warmed(&self, warmup: Warmup) -> Warmup {
        self.setter.warmed(warmup)
    }

    /// Whether the targets can be set as far as their ATR goes (`TargetSetter::initialized`).
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.setter.initialized()
    }

    /// The bar type of the targets' average true range, if any (`TargetSetter::atr_bar_type`).
    #[must_use]
    pub fn atr_bar_type(&self) -> Option<BarType> {
        self.setter.atr_bar_type()
    }

    /// Opens the target of the decision of `row`: its orders and time alert. With no price yet, the
    /// decision is unsettled and takes a number; with any other `NoTarget` it takes none.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The setter fails (`TargetSetter::at`), before any order.
    /// - The instrument is not in the cache, or Nautilus refuses an order or an alert, as the
    ///   orders are sent, which unsettles the decision.
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
            Err(NoTarget::FlatAtr(bar_type)) => {
                log::debug!("decision at {time}: the ATR of {bar_type} is nought; no targets");
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
            Err(NoTarget::NoRate {
                balance,
                pnl,
                rates,
            }) => {
                log::warn!(
                    "decision at {time}: the dataset balance {balance} has no rate into {pnl}, \
                     the currency of the P&L of {instrument_id}, at {rates}; no targets"
                );
                return Ok(());
            }
            Err(NoTarget::NoSize { side, one_r }) => {
                log::warn!(
                    "decision at {time}: at the reference stake of {} of {}, the {} of \
                     {instrument_id} rounds down to zero, its one R of {one_r} worth more on its \
                     size increment; no targets",
                    self.setter.stake,
                    self.setter.balance,
                    side.name()
                );
                return Ok(());
            }
        };
        let decision = next_decision();
        self.state
            .borrow_mut()
            .open
            .insert(decision, Decision::new(row, &targets));
        let sent = self.send(strategy, decision, &targets);
        if let Err(e) = &sent {
            self.unsettle(decision, format_args!("sending its orders: {e}"));
        }
        sent
    }

    fn send<S>(
        &self,
        strategy: &mut S,
        decision: u64,
        targets: &PerSide<SideTarget>,
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let stake = self.setter.stake;
        for side in Side::BOTH {
            let target = targets.of(side);
            let trade = target.trade(self.config.instrument_id, side);
            let ids = DecisionSide { decision, side }.order_ids();
            trade.send_at(strategy, stake, target.equity, ids)?;
        }
        Ok(())
    }

    /// A target's entry cancelled unfilled, at its time limit (`orders::on_time_event`), unsettles
    /// its decision.
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

    /// An order denied by the risk engine fails the dataset run. Records the denial in `rows` and
    /// shuts `strategy`'s system down, which stops Nautilus' backtest.
    pub fn on_order_denied<S>(&self, strategy: &S, event: &OrderDenied, rows: &SettledRows)
    where
        S: DataActor + DataActorNative,
    {
        let denial = format!(
            "the dataset run's order {} was denied at {}: {}; a denial fails the run, its \
             decision's row missing otherwise: its orders are sized at the reference stake of \
             {} of the dataset balance {}, which its venue must take",
            event.client_order_id,
            event.ts_event.to_rfc3339(),
            event.reason,
            self.setter.stake,
            self.setter.balance
        );
        log::error!("{denial}");
        strategy.shutdown_system(Some(denial.clone()));
        rows.fail(denial);
    }

    /// A target's order rejected by the venue unsettles its decision.
    pub fn on_order_rejected(&self, event: &OrderRejected) {
        self.refused(event.client_order_id, "rejected", event.reason);
    }

    /// Records the outcome of the target whose position closed, and publishes its decision's row
    /// once both sides have one. A position closed by no exit of its target leaves the decision
    /// unsettled.
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
                     realized P&L, or no such position in the cache",
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
                payoff: decision.payoff.clone(),
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

    fn refused(&self, id: ClientOrderId, how: &str, reason: Ustr) {
        if let Some((target, _)) = DecisionSide::parse(id) {
            self.unsettle(target.decision, format_args!("{id} {how}: {reason}"));
        }
    }

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.borrow();
        f.debug_struct(stringify!(Targets))
            .field("config", &self.config)
            .field("next", &state.next)
            .field("open", &state.open.len())
            .finish_non_exhaustive()
    }
}

impl Decision {
    fn new(row: FeatureRow, targets: &PerSide<SideTarget>) -> Self {
        Self {
            row,
            payoff: targets.as_ref().map(|target| target.payoff.clone()),
            units: targets.as_ref().map(|target| target.unit),
            outcomes: BTreeMap::new(),
        }
    }

    fn outcome(&self, side: Side, pnl: Money, commission: Money, exit: Exit) -> Outcome {
        Outcome {
            pnl,
            commission,
            exit,
            unit: *self.units.of(side),
        }
    }

    fn outcomes(&self) -> Option<Outcomes> {
        Some(Outcomes {
            long: *self.outcomes.get(&Side::Long)?,
            short: *self.outcomes.get(&Side::Short)?,
        })
    }
}

impl DecisionSide {
    fn order_ids(self) -> OrderIds {
        OrderIds {
            entry: self.order_id(Leg::Entry),
            take_profit: self.order_id(Leg::TakeProfit),
            stop_loss: self.order_id(Leg::Stop),
        }
    }

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

    fn sign(self) -> Decimal {
        match self {
            Self::Long => Decimal::ONE,
            Self::Short => -Decimal::ONE,
        }
    }
}

/// The exit of `target` that `closing` is, from its id or, closing at the time, its tag.
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

/// The prices a decision's sides enter at, and the price its exits are set from (`reference`): the
/// last quote's mid; without one, the last trade, else the close of the last bar of any bar type,
/// with no spread.
#[derive(Debug, Clone, Copy)]
struct EntryQuote {
    reference: Price,
    bid: Price,
    ask: Price,
}

impl EntryQuote {
    fn last(cache: &CacheApi, instrument_id: &InstrumentId) -> Option<Self> {
        if let Some(mid) = cache.price(instrument_id, PriceType::Mid) {
            return Some(Self {
                reference: mid,
                bid: cache.price(instrument_id, PriceType::Bid)?,
                ask: cache.price(instrument_id, PriceType::Ask)?,
            });
        }
        let reference = cache.price(instrument_id, PriceType::Last).or_else(|| {
            [AggregationSource::External, AggregationSource::Internal]
                .into_iter()
                .flat_map(|source| cache.bar_types(Some(instrument_id), None, source))
                .filter_map(|bar_type| cache.bar(&bar_type))
                .max_by_key(|bar| bar.ts_init)
                .map(|bar| bar.close)
        })?;
        Some(Self {
            reference,
            bid: reference,
            ask: reference,
        })
    }

    const fn entry(&self, side: Side) -> Price {
        match side {
            Side::Long => self.ask,
            Side::Short => self.bid,
        }
    }
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
