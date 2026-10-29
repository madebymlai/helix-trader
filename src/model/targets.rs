//! The targets of a strategy's decisions, read from real orders on
//! Nautilus' simulated venue.
//!
//! In a dataset run, at each decision `Targets::open` sends, for each
//! target of the config and on each side, a long and a short, the orders
//! whose outcome is the target, on a hedging margin account so that each
//! is a position of its own:
//!
//! - a triple barrier (`Barrier`): a bracket, a market entry, a take-profit
//!   limit and a stop, the two exits one-updates-the-other, and a time
//!   alert at the limit that closes the decision's position still open
//!   (`close_position`, tagged `TIME_TAG`) and cancels the decision's legs
//!   still open by their ids (cancelling every order of the instrument
//!   would cancel the legs of the overlapping decisions too);
//! - a horizon: a market entry closed by a time alert at the horizon.
//!
//! The exits are `take_profit` and `stop` away from the decision's price
//! (`reference`): the mid of the instrument's last quote, else its last
//! trade, else the close of its last bar. Each distance is relative
//! (`Distance`): basis points of that price, or multiples of an average
//! true range, Nautilus' `AverageTrueRange` on the bars of a bar type the
//! barrier declares, which the targets own and Nautilus feeds once
//! `Targets::register` has registered it. A decision at which that ATR is
//! not yet initialized opens no target, as a decision on cold features is
//! none. Each exit is on the instrument's nearest tick (Nautilus' tick
//! grid), a tie away from the decision's price.
//!
//! Each order's client order id names its decision, target, side and leg,
//! `D{n}-{B|H}{L|S}-{E|TP|SL}`: the decision's number in the run, `B` a
//! barrier and `H` a horizon, `L` long and `S` short, the entry, take
//! profit or stop. The outcome of a target is read from Nautilus' events
//! alone: a closed position's opening order names its decision and side,
//! its closing order the exit (a take profit, a stop, or a close tagged
//! `TIME_TAG`), and Nautilus reports its realised P&L net of commission.
//! The only state of our own besides the rows being assembled is each
//! open target's position id, from its entry's fill, which its time close
//! needs.
//!
//! Once every target of a decision has an outcome, the decision is settled
//! and its row, a `TargetRow`, published on the message bus, custom data
//! keyed by the strategy's id (`TargetRow::data_type`). A decision whose
//! order the risk engine denies or the venue rejects, or whose entry has
//! not filled by its time limit, is unsettled: logged, and no row. So is a
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
    collections::{BTreeMap, HashMap},
    fmt::{self, Debug, Display},
    rc::Rc,
    sync::Arc,
};

use anyhow::{Context, bail, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{
        DataActor, DataActorNative,
        indicators::{ActorIndicator, SharedActorIndicator},
    },
    cache::CacheApi,
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_indicators::{indicator::Indicator, volatility::atr::AverageTrueRange};
use nautilus_model::{
    data::{Bar, BarType, CustomData, CustomDataTrait, DataType, HasTsInit, QuoteTick, TradeTick},
    enums::{AggregationSource, OrderSide, PriceType},
    events::{OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::{ClientOrderId, InstrumentId, PositionId, StrategyId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    position::Position,
    types::{Money, Price, Quantity},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{decide::FeatureRow, features::Fingerprint};

/// The prefix of a target's time alert, followed by its decision and
/// target, such as `TARGET-TIME:D12-B`.
const TIME_ALERT: &str = "TARGET-TIME:";

/// The tag of the order closing a target at its time.
const TIME_TAG: &str = "TARGET-TIME";

/// The targets of each decision, from the strategy's config: on one
/// instrument, of one size, a triple barrier, a horizon or both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bon::Builder)]
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

/// A triple barrier: the exits of a bracket, each a distance from the
/// decision's price, and its time limit, such as `{ take_profit = { bps =
/// 20 }, stop = { atr = 1.5 }, limit = "2h", atr = { bar_type =
/// "...-1-MINUTE-MID-INTERNAL", period = 14 } }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Barrier {
    /// How far the take profit is, in the side's favour.
    pub take_profit: Distance,
    /// How far the stop is, against the side.
    pub stop: Distance,
    /// How long after the decision what is still open is closed.
    pub limit: SignedDuration,
    /// The average true range the `atr` distances are multiples of: there
    /// exactly when a distance is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atr: Option<AtrConfig>,
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

/// The average true range of a barrier: Nautilus' `AverageTrueRange`, of
/// its default moving average (Wilder's), on the bars of `bar_type`, the
/// targets' instrument's, over `period` bars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtrConfig {
    pub bar_type: BarType,
    pub period: usize,
}

/// The target a model learns: whether the long of the barrier, or of the
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
    pub exit: Exit,
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
    /// The position of each target whose entry filled, until it closes or
    /// its time comes.
    positions: HashMap<Target, PositionId>,
}

/// An open decision.
struct Decision {
    row: FeatureRow,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Side {
    Long,
    Short,
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
    /// Returns an error if there is no target, or the size, a barrier's
    /// distance, its limit or the horizon is not positive, or the barrier
    /// has an average true range no distance uses, or an `atr` distance
    /// without one, or one of another instrument's bars or of no bars.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.barrier.is_some() || self.horizon.is_some(),
            "no target: neither a barrier nor a horizon"
        );
        ensure!(
            self.quantity.is_positive(),
            "the target quantity must be positive"
        );
        if let Some(barrier) = self.barrier {
            ensure!(
                barrier.take_profit.value() > Decimal::ZERO && barrier.stop.value() > Decimal::ZERO,
                "the barrier's take profit and stop must be positive"
            );
            ensure!(
                barrier.limit.is_positive(),
                "the barrier's limit {:#} is not positive",
                barrier.limit
            );
            match (barrier.uses_atr(), barrier.atr) {
                (true, None) => bail!(
                    "the barrier has a distance in ATRs but no ATR: declare its bar type and \
                     period, `atr = {{ bar_type = .., period = .. }}`"
                ),
                (false, Some(_)) => bail!("the barrier has an ATR no distance uses"),
                (true, Some(atr)) => {
                    ensure!(
                        atr.bar_type.instrument_id() == self.instrument_id,
                        "the barrier's ATR is on {}, not on the targets' instrument {}",
                        atr.bar_type,
                        self.instrument_id
                    );
                    ensure!(atr.period > 0, "the barrier's ATR period must be positive");
                }
                (false, None) => {}
            }
        }
        if let Some(horizon) = self.horizon {
            ensure!(
                horizon.is_positive(),
                "the horizon {horizon:#} is not positive"
            );
        }
        Ok(())
    }

    /// The config as the fingerprint reads it.
    fn fingerprint_part(&self) -> String {
        let barrier = self.barrier.map(|b| {
            let atr = b.atr.map(|atr| format!("{} {}", atr.bar_type, atr.period));
            format!(
                "take profit {} stop {} limit {} ns atr {atr:?}",
                b.take_profit,
                b.stop,
                b.limit.as_nanos()
            )
        });
        let horizon = self.horizon.map(|horizon| horizon.as_nanos());
        format!(
            "{} {} barrier {barrier:?} horizon {horizon:?} ns",
            self.instrument_id, self.quantity
        )
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
    /// each its P&L and its exit, such as `barrier_long_pnl` and
    /// `barrier_long_exit`.
    #[must_use]
    pub fn target_names(&self) -> Vec<String> {
        self.kinds()
            .flat_map(|(kind, _)| {
                [Side::Long, Side::Short].into_iter().flat_map(move |side| {
                    ["pnl", "exit"].map(|value| format!("{}_{}_{value}", kind.name(), side.name()))
                })
            })
            .collect()
    }

    /// Where the long's P&L of `label`'s target is among the values of a
    /// row (`TargetRow::values`); none if the config has no such target.
    #[must_use]
    pub fn label_index(&self, label: Label) -> Option<usize> {
        let kind = match label {
            Label::Barrier => Kind::Barrier,
            Label::Horizon => Kind::Horizon,
        };
        // Each target has the long's P&L and exit, then the short's.
        self.kinds()
            .position(|(of, _)| of == kind)
            .map(|position| position * 4)
    }

    /// Where the P&L of `label`'s target on `side` is among the values of
    /// a row (`TargetRow::values`); none if the config has no such target.
    #[must_use]
    pub fn pnl_index(&self, label: Label, side: OrderSide) -> Option<usize> {
        let long = self.label_index(label)?;
        Some(if side == OrderSide::Buy {
            long
        } else {
            long + 2
        })
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

    /// The take profit and stop of each side, the long then the short,
    /// from `reference` and, if a distance is in ATRs, the ATR's value
    /// `atr`, each on the nearest tick of `instrument`.
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
    ) -> anyhow::Result<Vec<(Side, Price, Price)>> {
        let atr = atr
            .map(|atr| Decimal::try_from(atr).with_context(|| format!("the ATR {atr}")))
            .transpose()?;
        let take_profit = self.take_profit.length(reference, atr);
        let stop = self.stop.length(reference, atr);
        [Side::Long, Side::Short]
            .into_iter()
            .map(|side| {
                let favour = side.sign();
                Ok((
                    side,
                    nearest_tick(instrument, reference + favour * take_profit, favour)?,
                    nearest_tick(instrument, reference - favour * stop, -favour)?,
                ))
            })
            .collect()
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
    fn new(config: AtrConfig) -> Self {
        Self {
            bar_type: config.bar_type,
            indicator: RefCell::new(AverageTrueRange::new(config.period, None, None, None)),
        }
    }

    /// Its value, once initialized.
    fn value(&self) -> Option<f64> {
        let indicator = self.indicator.borrow();
        indicator.initialized.then_some(indicator.value)
    }
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

/// `fingerprint` extended with the targets of `config` and this module's
/// code, which places and reads them.
#[must_use]
pub fn fingerprint(fingerprint: Fingerprint, config: &TargetsConfig) -> Fingerprint {
    fingerprint
        .with(config.fingerprint_part().as_bytes())
        .with(include_str!("targets.rs").as_bytes())
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
    /// # Errors
    ///
    /// Returns an error if `config` is invalid.
    pub fn new(config: TargetsConfig) -> anyhow::Result<Rc<Self>> {
        config.validate()?;
        let atr = config.barrier.and_then(|barrier| barrier.atr);
        Ok(Rc::new(Self {
            config,
            atr: atr.map(|atr| Rc::new(BarrierAtr::new(atr))),
            state: RefCell::new(State::default()),
        }))
    }

    /// Registers the barrier's average true range, if any, with `actor`'s
    /// Nautilus indicators for the bars of its bar type, which `actor`
    /// subscribes to: the data engine then hands it each of those bars
    /// before the actor's own handler. Nautilus registers an indicator
    /// once per bar type, so registering again, on a restart, feeds it
    /// each bar once.
    pub fn register<A: DataActorNative>(&self, actor: &mut A) {
        if let Some(atr) = &self.atr {
            let shared = Rc::clone(atr) as SharedActorIndicator;
            actor
                .core_mut()
                .register_indicator_for_bars(atr.bar_type, shared);
        }
    }

    /// Opens the targets of the decision of `row`: for each target and
    /// side, its orders, from `strategy`, and each target's time alert.
    /// With no price of the instrument yet, the decision is unsettled;
    /// with the barrier's ATR not yet initialized, it opens nothing, as a
    /// decision on cold features is none.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not in the cache or an exit
    /// price or the size cannot be made in it, before any order, or if
    /// Nautilus refuses an order or an alert, which unsettles the decision.
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
        let decision = {
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
                "decision {decision} at {time}: no price of {instrument_id} to set its targets \
                 from; unsettled"
            );
            return Ok(());
        };
        let reference = reference.as_decimal();
        let size = self.config.quantity.as_decimal();
        let quantity = instrument
            .try_make_qty_from_decimal(size, None)
            .map_err(|e| anyhow::anyhow!("ordering {size} of {instrument_id}: {e}"))?;
        let exits = match self.config.barrier {
            Some(barrier) => barrier.exits(&instrument, reference, atr)?,
            None => vec![],
        };
        self.state
            .borrow_mut()
            .open
            .insert(decision, Decision::new(row));
        let sent = self.send(strategy, decision, quantity, &exits);
        if let Err(e) = &sent {
            self.unsettle(decision, format_args!("sending its orders: {e}"));
        }
        sent
    }

    /// Sends the orders of `decision`, `quantity` each, the brackets with
    /// `exits`, and sets the time alert of each target.
    fn send<S>(
        &self,
        strategy: &mut S,
        decision: u64,
        quantity: Quantity,
        exits: &[(Side, Price, Price)],
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let instrument_id = self.config.instrument_id;
        for &(side, take_profit, stop) in exits {
            let target = Target::new(decision, Kind::Barrier, side);
            let orders = strategy
                .order()
                .bracket()
                .instrument_id(instrument_id)
                .order_side(side.order_side())
                .quantity(quantity)
                .entry_client_order_id(target.order_id(Leg::Entry))
                .tp_client_order_id(target.order_id(Leg::TakeProfit))
                .sl_client_order_id(target.order_id(Leg::Stop))
                .tp_price(take_profit)
                .sl_trigger_price(stop)
                .call();
            strategy.submit_order_list(orders, None, None, None)?;
        }
        if self.config.horizon.is_some() {
            for side in [Side::Long, Side::Short] {
                let target = Target::new(decision, Kind::Horizon, side);
                let order = strategy.order().market(
                    instrument_id,
                    side.order_side(),
                    quantity,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(target.order_id(Leg::Entry)),
                );
                strategy.submit_order(order, None, None, None)?;
            }
        }
        let now = Timestamp::from(strategy.clock().timestamp_ns());
        for (kind, after) in self.config.kinds() {
            strategy.clock().set_time_alert(
                &time_alert(decision, kind),
                now + after,
                None,
                None,
            )?;
        }
        Ok(())
    }

    /// At a target's time alert, closes its position on each side still
    /// open and cancels its legs still open; a side whose entry has not
    /// filled leaves its decision unsettled. Another event is not the
    /// targets'.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus refuses the cancel or the close.
    pub fn on_time_event<S>(&self, strategy: &mut S, event: &TimeEvent) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let Some((decision, kind)) = parse_time_alert(&event.name) else {
            return Ok(());
        };
        for side in [Side::Long, Side::Short] {
            let target = Target::new(decision, kind, side);
            let position_id = self.state.borrow_mut().positions.remove(&target);
            let (legs, position) = {
                let cache = strategy.cache();
                let legs: Vec<ClientOrderId> = kind
                    .legs()
                    .iter()
                    .map(|&leg| target.order_id(leg))
                    .filter(|id| cache.order(id).is_some_and(|order| order.is_open()))
                    .collect();
                let position = position_id
                    .and_then(|id| cache.position(&id))
                    .filter(Position::is_open);
                (legs, position)
            };
            if position_id.is_none() && !self.has_outcome(target) {
                let entry = target.order_id(Leg::Entry);
                self.unsettle(
                    decision,
                    format_args!("the entry {entry} has not filled by its time"),
                );
            }
            if !legs.is_empty() {
                strategy.cancel_orders(legs, None, None)?;
            }
            if let Some(position) = position {
                let tags = Some(vec![Ustr::from(TIME_TAG)]);
                strategy.close_position(&position, None, tags, None, None, None, None)?;
            }
        }
        Ok(())
    }

    /// Keeps the position of a target's entry, from its fill.
    pub fn on_order_filled(&self, event: &OrderFilled) {
        if let Some((target, Leg::Entry)) = Target::parse(event.client_order_id)
            && let Some(position_id) = event.position_id
        {
            self.state
                .borrow_mut()
                .positions
                .insert(target, position_id);
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
        self.state.borrow_mut().positions.remove(&target);
        let exit = event
            .closing_order_id
            .and_then(|closing| exit(strategy, target, closing));
        let (Some(exit), Some(pnl)) = (exit, event.realized_pnl) else {
            self.unsettle(
                target.decision,
                format_args!(
                    "the position {} of {} closed by {:?}: no exit of its target, or no \
                     realised P&L",
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
            decision
                .outcomes
                .insert((target.kind, target.side), Outcome { pnl, exit });
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

    /// The decisions opened and neither settled nor unsettled yet.
    #[must_use]
    pub fn open_decisions(&self) -> usize {
        self.state.borrow().open.len()
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

    /// Whether the decision of `target`, open, has its outcome.
    fn has_outcome(&self, target: Target) -> bool {
        self.state
            .borrow()
            .open
            .get(&target.decision)
            .is_some_and(|decision| decision.outcomes.contains_key(&(target.kind, target.side)))
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
    fn new(row: FeatureRow) -> Self {
        Self {
            row,
            outcomes: BTreeMap::new(),
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

    /// The legs of a target of this kind.
    fn legs(self) -> &'static [Leg] {
        match self {
            Self::Barrier => &[Leg::Entry, Leg::TakeProfit, Leg::Stop],
            Self::Horizon => &[Leg::Entry],
        }
    }
}

impl Side {
    /// The name of the side in the names of a row's values.
    fn name(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }

    fn order_side(self) -> OrderSide {
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

/// The name of the time alert of `decision`'s target of `kind`.
fn time_alert(decision: u64, kind: Kind) -> String {
    format!("{TIME_ALERT}D{decision}-{}", kind.letter())
}

/// The decision and kind of target of the time alert `name`, if one.
fn parse_time_alert(name: &str) -> Option<(u64, Kind)> {
    let (decision, kind) = name
        .strip_prefix(TIME_ALERT)?
        .strip_prefix('D')?
        .split_once('-')?;
    let mut letters = kind.chars();
    let kind = Kind::of(letters.next()?)?;
    letters.next().is_none().then_some(())?;
    Some((decision.parse().ok()?, kind))
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
            let tagged = order
                .tags()
                .is_some_and(|tags| tags.contains(&Ustr::from(TIME_TAG)));
            tagged.then_some(Exit::Time)
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
    /// The values of the row, in the order of `TargetsConfig::target_names`
    /// of the config that made it: each P&L in its currency, each exit as
    /// `Exit::value`.
    #[must_use]
    pub fn values(&self) -> Vec<f64> {
        [self.barrier, self.horizon]
            .into_iter()
            .flatten()
            .flat_map(|outcomes| [outcomes.long, outcomes.short])
            .flat_map(|outcome| [outcome.pnl.as_f64(), outcome.exit.value()])
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
