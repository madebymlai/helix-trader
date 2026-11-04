//! Nautilus orders as cTrader trade requests, on a hedging account.
//!
//! - **A new order** (`ProtoOANewOrderReq`) carries the Nautilus client
//!   order id as its `clientOrderId`: market, limit and stop market orders,
//!   GTC, GTD (its expiry in ms), IOC and FOK; a stop's trigger `DEFAULT`
//!   or `BID_ASK` as cTrader's `TRADE`, `DOUBLE_BID_ASK` as `DOUBLE_TRADE`.
//!   A market order's time in force is cTrader's own (IOC), whatever the
//!   order's. Post-only, quote-quantity and display quantities are denied:
//!   cTrader has none.
//! - **A bracket's protections** go on its entry, absolute (`stopLoss`,
//!   `takeProfit`): cTrader moves them to the position the entry opens,
//!   as one `STOP_LOSS_TAKE_PROFIT` order. Spotware's proto says absolute
//!   protections are not supported for market orders; the demo took them
//!   on a market order and set them on its position (2026-10-26).
//! - **A trailing stop loss** (a trailing stop market) is a position's stop
//!   loss with `trailingStopLoss`: cTrader trails it at the distance it is
//!   set at. On a bracket's entry it goes relative (`relativeStopLoss`, its
//!   offset from the entry's fill), the take profit then relative too (see
//!   `EntryProtection`); on an open position, absolute, at its offset from
//!   the price that triggers it now (see `Trailing`).
//! - **An order naming a position** (`SubmitOrder.position_id`, the cTrader
//!   `positionId`): a market order on the closing side closes that much of
//!   the position (`ProtoOAClosePositionReq`), never more than it holds; any
//!   other carries the `positionId`. A reduce-only order without a position
//!   is denied: on a hedging account it would open one.
//! - **Volumes** are in cents of a unit; prices go as doubles, at the
//!   instrument's precision.
//!
//! Each request goes under a `clientMsgId` naming what it does and the
//! order (`Tag`): its answer and execution events come back under it.
//! cTrader makes a close request's `clientMsgId` the `clientOrderId` of the
//! closing order it creates (demo, 2026-10-26).

use std::{fmt, str::FromStr};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, OrderType, PositionSide, TimeInForce, TrailingOffsetType, TriggerType},
    events::OrderDeniedReason,
    identifiers::{ClientOrderId, PositionId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    types::{Price, Quantity},
};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};

use crate::common::messages::{
    ProtoOaAmendOrderReq, ProtoOaAmendPositionSltpReq, ProtoOaClosePositionReq, ProtoOaNewOrderReq,
    ProtoOaOrderTriggerMethod, ProtoOaOrderType, ProtoOaTimeInForce, ProtoOaTradeSide,
};

/// What a trade request does, and to which Nautilus order: its
/// `clientMsgId`, `<op>:<client order id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Tag {
    /// A new order.
    New(ClientOrderId),
    /// A pending order amended.
    Amend(ClientOrderId),
    /// A pending order cancelled.
    Cancel(ClientOrderId),
    /// A position closed, in part or whole, by this closing order.
    Close(ClientOrderId),
    /// A protection (stop loss or take profit) moved.
    Protect(ClientOrderId),
    /// A protection removed.
    Unprotect(ClientOrderId),
}

impl Tag {
    /// The Nautilus order the request is about.
    pub(super) fn client_order_id(self) -> ClientOrderId {
        match self {
            Self::New(id)
            | Self::Amend(id)
            | Self::Cancel(id)
            | Self::Close(id)
            | Self::Protect(id)
            | Self::Unprotect(id) => id,
        }
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self {
            Self::New(_) => "N",
            Self::Amend(_) => "A",
            Self::Cancel(_) => "X",
            Self::Close(_) => "C",
            Self::Protect(_) => "P",
            Self::Unprotect(_) => "U",
        };
        write!(f, "{op}:{}", self.client_order_id())
    }
}

impl FromStr for Tag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (op, id) = s
            .split_once(':')
            .filter(|(_, id)| !id.is_empty())
            .ok_or_else(|| format!("{s:?} is not a trade request's id"))?;
        let id = ClientOrderId::new_checked(id).map_err(|e| e.to_string())?;
        Ok(match op {
            "N" => Self::New(id),
            "A" => Self::Amend(id),
            "X" => Self::Cancel(id),
            "C" => Self::Close(id),
            "P" => Self::Protect(id),
            "U" => Self::Unprotect(id),
            _ => return Err(format!("{s:?} is not a trade request's id")),
        })
    }
}

/// A position's protection: cTrader holds at most one stop loss and one
/// take profit on a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Leg {
    StopLoss,
    TakeProfit,
}

impl Leg {
    const fn suffix(self) -> &'static str {
        match self {
            Self::StopLoss => "SL",
            Self::TakeProfit => "TP",
        }
    }

    /// The Nautilus order type of the leg: a stop market at the stop loss,
    /// a trailing stop market if it `trails`, a limit at the take profit.
    pub(super) const fn order_type(self, trails: bool) -> OrderType {
        match self {
            Self::StopLoss if trails => OrderType::TrailingStopMarket,
            Self::StopLoss => OrderType::StopMarket,
            Self::TakeProfit => OrderType::Limit,
        }
    }
}

/// The venue order id of `leg` of position `position_id`: the position's
/// id with `-SL` or `-TP`.
pub(super) fn protection_id(position_id: i64, leg: Leg) -> VenueOrderId {
    VenueOrderId::new(format!("{position_id}-{}", leg.suffix()))
}

/// The position and leg of a venue order id `protection_id` made.
pub(super) fn parse_protection_id(id: VenueOrderId) -> Option<(i64, Leg)> {
    let (position, suffix) = id.as_str().rsplit_once('-')?;
    let leg = match suffix {
        "SL" => Leg::StopLoss,
        "TP" => Leg::TakeProfit,
        _ => return None,
    };
    Some((position.parse().ok()?, leg))
}

/// The stop loss and take profit a bracket's entry carries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Protection {
    pub(super) stop_loss: Option<StopLoss>,
    pub(super) take_profit: Option<Price>,
}

/// A bracket entry's stop loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StopLoss {
    /// At this price.
    At(Price),
    /// Trailing, from its offset from the entry's fill.
    Trailing(Trailing),
}

/// The offset of a trailing stop market, as Nautilus states it: by price,
/// or in basis points of a price. cTrader takes a distance, by price, on
/// the instrument's tick, and trails the stop at the distance it is set at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Trailing {
    offset: Decimal,
    offset_type: TrailingOffsetType,
}

impl Trailing {
    /// The offset of `order`, if a trailing stop market; unchecked.
    pub(super) fn of(order: &OrderAny) -> Option<Self> {
        if order.order_type() != OrderType::TrailingStopMarket {
            return None;
        }
        Some(Self {
            offset: order.trailing_offset()?,
            offset_type: order.trailing_offset_type()?,
        })
    }

    /// The offset of `order`, a trailing stop market cTrader can hold: a
    /// positive offset by price or in basis points, no activation price
    /// (cTrader trails a stop from the moment it is set), a trigger type
    /// `trigger_method` maps, good till cancelled (a stop loss lasts as its
    /// position does), none of the flags cTrader lacks.
    ///
    /// # Errors
    ///
    /// Returns why the order is denied.
    pub(super) fn checked(order: &OrderAny) -> Result<Self, OrderDeniedReason> {
        validate(order)?;
        if order.time_in_force() != TimeInForce::Gtc {
            return Err(OrderDeniedReason::UnsupportedTimeInForce(
                order.time_in_force(),
            ));
        }
        let offset_type = order
            .trailing_offset_type()
            .ok_or(OrderDeniedReason::MissingTrailingOffsetType)?;
        let offset = order
            .trailing_offset()
            .ok_or(OrderDeniedReason::MissingTrailingOffset)?;
        match offset_type {
            TrailingOffsetType::Price | TrailingOffsetType::BasisPoints => {}
            offset_type => {
                return Err(OrderDeniedReason::UnsupportedTrailingOffsetType { offset_type });
            }
        }
        if offset <= Decimal::ZERO {
            return Err(OrderDeniedReason::ValidationFailed {
                detail: format!("trailing offset {offset} is not positive"),
            });
        }
        if order.activation_price().is_some() {
            return Err(OrderDeniedReason::ValidationFailed {
                detail: "cTrader trails a stop loss from the moment it is set: no activation \
                         price"
                    .to_owned(),
            });
        }
        trigger_method(order.trigger_type())?;
        Ok(Self {
            offset,
            offset_type,
        })
    }

    /// The distance the stop trails at, on `instrument`'s tick: the offset
    /// by price, or its basis points of `reference`, rounded to the tick.
    ///
    /// # Errors
    ///
    /// Returns a denial if an offset by price is finer than the tick, the
    /// distance is not positive, or basis points have no `reference`.
    pub(super) fn distance(
        self,
        instrument: &InstrumentAny,
        reference: Option<Price>,
    ) -> Result<Price, OrderDeniedReason> {
        let precision = instrument.price_precision();
        let distance = match self.offset_type {
            TrailingOffsetType::BasisPoints => {
                let reference = reference.ok_or_else(|| OrderDeniedReason::ValidationFailed {
                    detail: format!(
                        "no price of {} to take {} basis points of",
                        instrument.id(),
                        self.offset
                    ),
                })?;
                (reference.as_decimal() * self.offset / Decimal::from(10_000))
                    .round_dp_with_strategy(
                        u32::from(precision),
                        RoundingStrategy::MidpointAwayFromZero,
                    )
            }
            _ => self.offset.normalize(),
        };
        let invalid = |why: &str| OrderDeniedReason::ValidationFailed {
            detail: format!("trailing distance {distance} {why}"),
        };
        if distance.scale() > u32::from(precision) {
            return Err(invalid(&format!(
                "is finer than {}'s tick",
                instrument.id()
            )));
        }
        if distance <= Decimal::ZERO {
            return Err(invalid("is not positive"));
        }
        Price::from_decimal_dp(distance, precision).map_err(|e| invalid(&e.to_string()))
    }
}

/// `distance` (by price) as cTrader's relative stop loss or take profit,
/// in 1/100000 of a price unit.
fn relative(distance: Decimal) -> Result<i64, OrderDeniedReason> {
    let relative = distance * Decimal::from(100_000);
    relative
        .fract()
        .is_zero()
        .then(|| relative.to_i64())
        .flatten()
        .ok_or_else(|| OrderDeniedReason::ValidationFailed {
            detail: format!("distance {distance} is finer than cTrader's 0.00001"),
        })
}

/// A bracket's protection as its entry's request states it: absolute, or,
/// once the stop loss trails, relative, the take profit too, cTrader taking
/// no mix ("SL/TP in absolute values and in pips are not allowed
/// together", demo, 2026-11-04).
#[derive(Debug, Default)]
struct EntryProtection {
    stop_loss: Option<f64>,
    take_profit: Option<f64>,
    relative_stop_loss: Option<i64>,
    relative_take_profit: Option<i64>,
    trailing_stop_loss: Option<bool>,
}

impl EntryProtection {
    /// `protection` on an entry of `side`, of `instrument`, filling near
    /// `price`: a trailing stop loss at its distance at that price, the take
    /// profit then at its distance from that price.
    ///
    /// # Errors
    ///
    /// Returns a denial if the trailing distance cannot be taken, or a take
    /// profit beside a trailing stop loss has no price to go from or is not
    /// beyond it.
    fn of(
        protection: Protection,
        side: OrderSide,
        instrument: &InstrumentAny,
        price: Option<Price>,
    ) -> Result<Self, OrderDeniedReason> {
        let Some(StopLoss::Trailing(trailing)) = protection.stop_loss else {
            return Ok(Self {
                stop_loss: protection.stop_loss.map(|stop| match stop {
                    StopLoss::At(price) => price.as_f64(),
                    StopLoss::Trailing(_) => unreachable!("matched above"),
                }),
                take_profit: protection.take_profit.map(|p| p.as_f64()),
                ..Self::default()
            });
        };
        let distance = trailing.distance(instrument, price)?;
        let take_profit = protection
            .take_profit
            .map(|take_profit| {
                let from = price.ok_or_else(|| OrderDeniedReason::ValidationFailed {
                    detail: format!(
                        "no price of {} to set the take profit from, beside a trailing stop loss",
                        instrument.id()
                    ),
                })?;
                let gap = match side {
                    OrderSide::Sell => from.as_decimal() - take_profit.as_decimal(),
                    _ => take_profit.as_decimal() - from.as_decimal(),
                };
                if gap <= Decimal::ZERO {
                    return Err(OrderDeniedReason::ValidationFailed {
                        detail: format!("take profit {take_profit} is not beyond {from}"),
                    });
                }
                relative(gap)
            })
            .transpose()?;
        Ok(Self {
            relative_stop_loss: Some(relative(distance.as_decimal())?),
            relative_take_profit: take_profit,
            trailing_stop_loss: Some(true),
            ..Self::default()
        })
    }
}

/// How an order reaches cTrader.
#[derive(Debug, Clone, PartialEq)]
#[expect(clippy::large_enum_variant, reason = "a placement is sent at once")]
pub(super) enum Placement {
    New(ProtoOaNewOrderReq),
    Close(ProtoOaClosePositionReq),
}

/// The request placing `order`, of cTrader symbol `symbol_id`, the
/// instrument `instrument`, into `position` if it names one, with the
/// bracket's `protection` on it (see `EntryProtection`); a market entry
/// fills near `market`.
///
/// # Errors
///
/// Returns why the order is denied: a type, time in force or flag cTrader
/// lacks, a reduce-only order without a position, a closing order larger
/// than its position, a volume off the cent grid, a trailing distance
/// cTrader cannot take.
pub(super) fn placement(
    order: &OrderAny,
    symbol_id: i64,
    instrument: &InstrumentAny,
    position: Option<&Position>,
    protection: Protection,
    market: Option<Price>,
) -> Result<Placement, OrderDeniedReason> {
    validate(order)?;
    let volume = volume(order.quantity())?;
    if let Some(position) = position {
        let closing =
            position.side != PositionSide::Flat && position.is_opposite_side(order.order_side());
        if closing && order.order_type() == OrderType::Market {
            if order.quantity() > position.quantity {
                return Err(invalid_position(
                    position.id,
                    format!(
                        "{} is more than the position's {}: a close never opens the other side",
                        order.quantity(),
                        position.quantity
                    ),
                ));
            }
            if protection != Protection::default() {
                return Err(OrderDeniedReason::UnsupportedTpSl {
                    detail: "a closing order carries no protection".to_owned(),
                });
            }
            return Ok(Placement::Close(ProtoOaClosePositionReq {
                position_id: venue_position_id(position.id)?,
                volume,
                ..Default::default()
            }));
        }
    } else if order.is_reduce_only() {
        return Err(OrderDeniedReason::ValidationFailed {
            detail:
                "a reduce-only order on a cTrader hedging account names the position it reduces \
                     (close_position): without one it would open a position"
                    .to_owned(),
        });
    }
    // The price the entry fills near: its limit or stop price, else the
    // market's.
    let near = order.price().or(order.trigger_price()).or(market);
    let protection = EntryProtection::of(protection, order.order_side(), instrument, near)?;
    let mut request = ProtoOaNewOrderReq {
        symbol_id,
        trade_side: trade_side(order.order_side()) as i32,
        volume,
        client_order_id: Some(order.client_order_id().to_string()),
        position_id: position.map(|p| venue_position_id(p.id)).transpose()?,
        stop_loss: protection.stop_loss,
        take_profit: protection.take_profit,
        relative_stop_loss: protection.relative_stop_loss,
        relative_take_profit: protection.relative_take_profit,
        trailing_stop_loss: protection.trailing_stop_loss,
        ..Default::default()
    };
    match order.order_type() {
        OrderType::Market => request.set_order_type(ProtoOaOrderType::Market),
        OrderType::Limit => {
            request.set_order_type(ProtoOaOrderType::Limit);
            request.limit_price = order.price().map(|p| p.as_f64());
        }
        OrderType::StopMarket => {
            request.set_order_type(ProtoOaOrderType::Stop);
            request.stop_price = order.trigger_price().map(|p| p.as_f64());
            request.set_stop_trigger_method(trigger_method(order.trigger_type())?);
        }
        OrderType::TrailingStopMarket => {
            return Err(OrderDeniedReason::ValidationFailed {
                detail: "cTrader trails only a position's stop loss: send a trailing stop as a \
                         bracket's stop loss, or on the position it protects (position_id)"
                    .to_owned(),
            });
        }
        order_type => return Err(OrderDeniedReason::UnsupportedOrderType { order_type }),
    }
    if order.order_type() != OrderType::Market {
        let (time_in_force, expiry) = time_in_force(order)?;
        request.set_time_in_force(time_in_force);
        request.expiration_timestamp = expiry;
    }
    Ok(Placement::New(request))
}

/// The amendment of the pending `order` (cTrader order `order_id`, of
/// `instrument`) to `quantity`, `price` and `trigger_price` (the order's
/// own if none), carrying the bracket's `protection` (see
/// `EntryProtection`), from the amended price.
///
/// # Errors
///
/// Returns a denial if the volume is off the cent grid, the trigger type
/// has no cTrader method, or the trailing distance cannot be taken.
pub(super) fn amend_entry(
    order: &OrderAny,
    order_id: i64,
    instrument: &InstrumentAny,
    quantity: Quantity,
    price: Option<Price>,
    trigger_price: Option<Price>,
    protection: Protection,
) -> Result<ProtoOaAmendOrderReq, OrderDeniedReason> {
    let (price, trigger_price) = (
        price.or(order.price()),
        trigger_price.or(order.trigger_price()),
    );
    let near = price.or(trigger_price);
    let protection = EntryProtection::of(protection, order.order_side(), instrument, near)?;
    let mut request = ProtoOaAmendOrderReq {
        order_id,
        volume: Some(volume(quantity)?),
        stop_loss: protection.stop_loss,
        take_profit: protection.take_profit,
        relative_stop_loss: protection.relative_stop_loss,
        relative_take_profit: protection.relative_take_profit,
        trailing_stop_loss: protection.trailing_stop_loss,
        ..Default::default()
    };
    match order.order_type() {
        OrderType::Limit => request.limit_price = price.map(|p| p.as_f64()),
        OrderType::StopMarket => {
            request.stop_price = trigger_price.map(|p| p.as_f64());
            request.set_stop_trigger_method(trigger_method(order.trigger_type())?);
        }
        _ => {}
    }
    Ok(request)
}

/// The amendment setting the protections of cTrader position `position`:
/// the stop loss of the order `stop_loss` at its price, trailing and
/// triggered as it is if the order trails; the take profit. Both go, since
/// cTrader drops one left out.
pub(super) fn amend_position(
    position: i64,
    stop_loss: Option<(Price, &OrderAny)>,
    take_profit: Option<Price>,
) -> ProtoOaAmendPositionSltpReq {
    let trails = stop_loss.is_some_and(|(_, o)| o.order_type() == OrderType::TrailingStopMarket);
    ProtoOaAmendPositionSltpReq {
        position_id: position,
        stop_loss: stop_loss.map(|(price, _)| price.as_f64()),
        take_profit: take_profit.map(|p| p.as_f64()),
        trailing_stop_loss: trails.then_some(true),
        stop_loss_trigger_method: stop_loss
            .filter(|_| trails)
            .and_then(|(_, o)| trigger_method(o.trigger_type()).ok())
            .map(|m| m as i32),
        ..Default::default()
    }
}

/// The level at which the trailing stop `order` goes on `position`, an
/// open position of `instrument`: its offset (`trailing`) from `market`,
/// the price that triggers it now (the bid for a sell, the ask for a
/// buy), on the side that protects the position. cTrader then trails it at
/// that distance.
///
/// # Errors
///
/// Returns why the order is denied: it does not close the position, its
/// quantity is not the position's, there is no market price, or the
/// distance cannot be taken.
pub(super) fn trailing_level(
    order: &OrderAny,
    trailing: Trailing,
    position: &Position,
    instrument: &InstrumentAny,
    market: Option<Price>,
) -> Result<Price, OrderDeniedReason> {
    if position.side == PositionSide::Flat || !position.is_opposite_side(order.order_side()) {
        return Err(invalid_position(
            position.id,
            "a stop loss closes its position: it is on the other side".to_owned(),
        ));
    }
    if order.quantity() != position.quantity {
        return Err(invalid_position(
            position.id,
            format!(
                "a cTrader stop loss covers its whole position: {} is not the position's {}",
                order.quantity(),
                position.quantity
            ),
        ));
    }
    let market = market.ok_or_else(|| OrderDeniedReason::ValidationFailed {
        detail: format!(
            "no quote of {} to set a trailing stop from",
            instrument.id()
        ),
    })?;
    let distance = trailing.distance(instrument, Some(market))?;
    let level = match order.order_side() {
        OrderSide::Sell => market.as_decimal() - distance.as_decimal(),
        _ => market.as_decimal() + distance.as_decimal(),
    };
    Price::from_decimal_dp(level, instrument.price_precision()).map_err(|e| {
        OrderDeniedReason::ValidationFailed {
            detail: format!("trailing stop level {level}: {e}"),
        }
    })
}

/// The checks every order passes: the flags cTrader lacks.
fn validate(order: &OrderAny) -> Result<(), OrderDeniedReason> {
    let unsupported = |what: &str| OrderDeniedReason::ValidationFailed {
        detail: format!("cTrader has no {what} orders"),
    };
    if order.is_post_only() {
        return Err(unsupported("post-only"));
    }
    if order.is_quote_quantity() {
        return Err(unsupported("quote-quantity"));
    }
    if order.display_qty().is_some() {
        return Err(unsupported("iceberg"));
    }
    Ok(())
}

/// The volume of `quantity`, in cents of a unit.
///
/// # Errors
///
/// Returns a denial if the quantity is finer than a cent of a unit.
pub(super) fn volume(quantity: Quantity) -> Result<i64, OrderDeniedReason> {
    let cents = quantity.as_decimal() * Decimal::ONE_HUNDRED;
    (cents.fract().is_zero())
        .then(|| i64::try_from(cents).ok())
        .flatten()
        .ok_or_else(|| OrderDeniedReason::ValidationFailed {
            detail: format!("{quantity} is not a whole number of cents of a unit"),
        })
}

/// The cTrader id of a Nautilus position: on a hedging account the venue's
/// `positionId`, which a position this client did not open also carries.
pub(super) fn venue_position_id(id: PositionId) -> Result<i64, OrderDeniedReason> {
    id.as_str()
        .parse()
        .map_err(|_| invalid_position(id, "not a cTrader position".to_owned()))
}

fn invalid_position(position_id: PositionId, detail: String) -> OrderDeniedReason {
    OrderDeniedReason::InvalidPositionId {
        position_id,
        detail,
    }
}

const fn trade_side(side: OrderSide) -> ProtoOaTradeSide {
    match side {
        OrderSide::Sell => ProtoOaTradeSide::Sell,
        _ => ProtoOaTradeSide::Buy,
    }
}

/// cTrader's trigger method of a stop triggered as `trigger`: `TRADE` (a
/// buy by the ask, a sell by the bid) for `DEFAULT` and `BID_ASK`,
/// `DOUBLE_TRADE` for `DOUBLE_BID_ASK`.
pub(super) fn trigger_method(
    trigger: Option<TriggerType>,
) -> Result<ProtoOaOrderTriggerMethod, OrderDeniedReason> {
    match trigger {
        None | Some(TriggerType::Default | TriggerType::BidAsk) => {
            Ok(ProtoOaOrderTriggerMethod::Trade)
        }
        Some(TriggerType::DoubleBidAsk) => Ok(ProtoOaOrderTriggerMethod::DoubleTrade),
        Some(other) => Err(OrderDeniedReason::ValidationFailed {
            detail: format!("cTrader triggers no stop by {other}"),
        }),
    }
}

/// cTrader's time in force of a pending `order`, and its expiry in ms.
fn time_in_force(order: &OrderAny) -> Result<(ProtoOaTimeInForce, Option<i64>), OrderDeniedReason> {
    Ok(match order.time_in_force() {
        TimeInForce::Gtc => (ProtoOaTimeInForce::GoodTillCancel, None),
        TimeInForce::Ioc => (ProtoOaTimeInForce::ImmediateOrCancel, None),
        TimeInForce::Fok => (ProtoOaTimeInForce::FillOrKill, None),
        TimeInForce::Gtd => {
            let expiry = order
                .expire_time()
                .ok_or(OrderDeniedReason::MissingExpireTime)?;
            (ProtoOaTimeInForce::GoodTillDate, Some(millis(expiry)))
        }
        other => return Err(OrderDeniedReason::UnsupportedTimeInForce(other)),
    })
}

/// `time` in whole milliseconds, cTrader's signed Unix time.
pub(super) fn millis(time: UnixNanos) -> i64 {
    i64::try_from(time.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType},
        identifiers::InstrumentId,
        instruments::stubs::cfd_gold,
        orders::OrderTestBuilder,
    };

    use super::*;

    fn gold() -> InstrumentId {
        InstrumentId::from("XAUUSD.ACMEFX")
    }

    /// A gold instrument: its tick 0.01.
    fn instrument() -> InstrumentAny {
        InstrumentAny::Cfd(cfd_gold())
    }

    /// `order` placed on symbol 41, into no position.
    fn placed(order: &OrderAny, protection: Protection) -> Result<Placement, OrderDeniedReason> {
        placement(order, 41, &instrument(), None, protection, None)
    }

    fn new_order(placement: Placement) -> ProtoOaNewOrderReq {
        let Placement::New(request) = placement else {
            panic!("not a new order: {placement:?}");
        };
        request
    }

    #[test]
    fn a_tag_is_the_op_and_the_client_order_id_and_reads_back() {
        let id = ClientOrderId::from("O-20261026-150000-001-001-1");
        for tag in [
            Tag::New(id),
            Tag::Amend(id),
            Tag::Cancel(id),
            Tag::Close(id),
            Tag::Protect(id),
            Tag::Unprotect(id),
        ] {
            assert_eq!(tag.to_string().parse::<Tag>(), Ok(tag));
        }
        assert_eq!(Tag::Close(id).to_string(), "C:O-20261026-150000-001-001-1");
        assert!("PROBE-5".parse::<Tag>().is_err());
        assert!("7".parse::<Tag>().is_err());
    }

    #[test]
    fn a_protection_id_is_the_position_and_the_leg() {
        let sl = protection_id(690_004_734, Leg::StopLoss);
        assert_eq!(sl, VenueOrderId::from("690004734-SL"));
        assert_eq!(parse_protection_id(sl), Some((690_004_734, Leg::StopLoss)));
        assert_eq!(
            parse_protection_id(VenueOrderId::from("690004734-TP")),
            Some((690_004_734, Leg::TakeProfit))
        );
        assert_eq!(parse_protection_id(VenueOrderId::from("1031776265")), None);
    }

    #[test]
    fn a_market_order_of_one_ounce_is_a_volume_of_100_with_its_client_order_id() {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .build();

        let request = new_order(placed(&order, Protection::default()).unwrap());

        assert_eq!(request.symbol_id, 41);
        assert_eq!(request.order_type(), ProtoOaOrderType::Market);
        assert_eq!(request.trade_side(), ProtoOaTradeSide::Buy);
        assert_eq!(request.volume, 100);
        assert_eq!(
            request.client_order_id.as_deref(),
            Some(order.client_order_id().as_str())
        );
        assert_eq!(request.time_in_force, None);
        assert_eq!(request.position_id, None);
    }

    #[test]
    fn a_bracket_entry_carries_its_absolute_stop_loss_and_take_profit() {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(gold())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::from("3822.73"))
            .build();
        let protection = Protection {
            stop_loss: Some(StopLoss::At(Price::from("3802.73"))),
            take_profit: Some(Price::from("3842.73")),
        };

        let request = new_order(placed(&order, protection).unwrap());

        assert_eq!(request.order_type(), ProtoOaOrderType::Limit);
        assert_eq!(request.limit_price, Some(3822.73));
        assert_eq!(request.stop_loss, Some(3802.73));
        assert_eq!(request.take_profit, Some(3842.73));
        assert_eq!(request.time_in_force(), ProtoOaTimeInForce::GoodTillCancel);
    }

    #[test]
    fn a_stop_market_order_is_a_stop_triggered_as_its_trigger_type() {
        let order = OrderTestBuilder::new(OrderType::StopMarket)
            .instrument_id(gold())
            .side(OrderSide::Sell)
            .quantity(Quantity::from(2))
            .trigger_price(Price::from("3990.50"))
            .trigger_type(TriggerType::DoubleBidAsk)
            .build();

        let request = new_order(placed(&order, Protection::default()).unwrap());

        assert_eq!(request.order_type(), ProtoOaOrderType::Stop);
        assert_eq!(request.trade_side(), ProtoOaTradeSide::Sell);
        assert_eq!(request.stop_price, Some(3990.5));
        assert_eq!(
            request.stop_trigger_method(),
            ProtoOaOrderTriggerMethod::DoubleTrade
        );
    }

    #[test]
    fn order_types_and_flags_ctrader_lacks_are_denied() {
        let stop_limit = OrderTestBuilder::new(OrderType::StopLimit)
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .price(Price::from("4000.00"))
            .trigger_price(Price::from("3999.00"))
            .build();
        let post_only = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .price(Price::from("4000.00"))
            .post_only(true)
            .build();
        let day = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .price(Price::from("4000.00"))
            .time_in_force(TimeInForce::Day)
            .build();
        let reduce_only = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .reduce_only(true)
            .build();

        for order in [stop_limit, post_only, day, reduce_only] {
            assert!(placed(&order, Protection::default()).is_err(), "{order:?}");
        }
    }

    fn trailing(offset: &str, offset_type: TrailingOffsetType) -> OrderTestBuilder {
        let mut builder = OrderTestBuilder::new(OrderType::TrailingStopMarket);
        builder
            .instrument_id(gold())
            .side(OrderSide::Sell)
            .quantity(Quantity::from(1))
            .trailing_offset(offset.parse().unwrap())
            .trailing_offset_type(offset_type);
        builder
    }

    fn distance(order: &OrderAny, reference: Option<&str>) -> Result<Price, OrderDeniedReason> {
        Trailing::checked(order)?.distance(&instrument(), reference.map(Price::from))
    }

    #[test]
    fn a_trailing_offset_by_price_is_the_distance_and_basis_points_are_of_the_reference() {
        let by_price = trailing("1.00", TrailingOffsetType::Price).build();
        let by_bps = trailing("2.5", TrailingOffsetType::BasisPoints).build();

        assert_eq!(distance(&by_price, None), Ok(Price::from("1.00")));
        // 2.5 bp of 4043.26 is 1.010815, on the tick 1.01.
        assert_eq!(distance(&by_bps, Some("4043.26")), Ok(Price::from("1.01")));
        assert!(distance(&by_bps, None).is_err(), "no price to take bp of");
    }

    #[test]
    fn trailing_stops_ctrader_cannot_hold_are_denied() {
        let ticks = trailing("10", TrailingOffsetType::Ticks).build();
        let fine = trailing("1.005", TrailingOffsetType::Price).build();
        let zero = trailing("0", TrailingOffsetType::Price).build();
        let tiny = trailing("0.01", TrailingOffsetType::BasisPoints).build();
        let activated = trailing("1.00", TrailingOffsetType::Price)
            .activation_price(Price::from("4050.00"))
            .build();
        let by_last = trailing("1.00", TrailingOffsetType::Price)
            .trigger_type(TriggerType::LastPrice)
            .build();
        let for_the_day = trailing("1.00", TrailingOffsetType::Price)
            .time_in_force(TimeInForce::Day)
            .build();

        assert_eq!(
            distance(&ticks, None),
            Err(OrderDeniedReason::UnsupportedTrailingOffsetType {
                offset_type: TrailingOffsetType::Ticks
            })
        );
        for order in [fine, zero, activated, by_last, for_the_day] {
            assert!(distance(&order, None).is_err(), "{order:?}");
        }
        assert!(distance(&tiny, Some("4043.26")).is_err(), "rounds to 0");
    }

    #[test]
    fn a_trailing_bracket_leg_goes_on_its_entry_relative_and_trailing() {
        let entry = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(gold())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::from("4000.00"))
            .build();
        let leg = trailing("5", TrailingOffsetType::BasisPoints).build();
        let protection = Protection {
            stop_loss: Some(StopLoss::Trailing(Trailing::checked(&leg).unwrap())),
            take_profit: Some(Price::from("4030.50")),
        };
        let below = Protection {
            take_profit: Some(Price::from("3990.00")),
            ..protection
        };

        let request = new_order(placed(&entry, protection).unwrap());

        // 5 bp of the entry's price, 4000.00: 2.00; the take profit 30.50
        // above it.
        assert_eq!(request.relative_stop_loss, Some(200_000));
        assert_eq!(request.trailing_stop_loss, Some(true));
        assert_eq!(request.relative_take_profit, Some(3_050_000));
        assert_eq!((request.stop_loss, request.take_profit), (None, None));
        assert!(placed(&entry, below).is_err(), "a take profit below a buy");
    }

    #[test]
    fn a_trailing_stop_on_a_position_is_its_offset_from_the_market_on_the_closing_side() {
        let gold = instrument();
        let long = position(OrderSide::Buy);
        let short = position(OrderSide::Sell);
        let sell = trailing("1.00", TrailingOffsetType::Price).build();
        let buy = trailing("1.00", TrailingOffsetType::Price)
            .side(OrderSide::Buy)
            .build();
        let level = |order: &OrderAny, position: &Position, market: Option<&str>| {
            let trailing = Trailing::checked(order).unwrap();
            trailing_level(order, trailing, position, &gold, market.map(Price::from))
        };

        assert_eq!(
            level(&sell, &long, Some("4042.26")),
            Ok(Price::from("4041.26"))
        );
        assert_eq!(
            level(&buy, &short, Some("4042.31")),
            Ok(Price::from("4043.31"))
        );
        assert!(level(&buy, &long, Some("4042.31")).is_err(), "same side");
        assert!(level(&sell, &long, None).is_err(), "no quote");
    }

    /// A position of 1 oz of gold opened by a `side` fill.
    fn position(side: OrderSide) -> Position {
        let gold = instrument();
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold.id())
            .side(side)
            .quantity(Quantity::from(1))
            .build();
        let nautilus_model::events::OrderEventAny::Filled(fill) =
            nautilus_model::orders::stubs::TestOrderEventStubs::filled(
                &order,
                &gold,
                None,
                Some(PositionId::from("692481891")),
                Some(Price::from("4042.21")),
                None,
                None,
                None,
                None,
                None,
            )
        else {
            panic!("not a fill");
        };
        Position::new(&gold, fill)
    }

    #[test]
    fn an_amendment_keeps_a_trailing_stop_loss_trailing_and_its_trigger_method() {
        let stop = trailing("1.00", TrailingOffsetType::Price)
            .trigger_type(TriggerType::DoubleBidAsk)
            .build();
        let fixed = OrderTestBuilder::new(OrderType::StopMarket)
            .instrument_id(gold())
            .side(OrderSide::Sell)
            .quantity(Quantity::from(1))
            .trigger_price(Price::from("4041.00"))
            .build();

        let trailing = amend_position(
            7,
            Some((Price::from("4041.32"), &stop)),
            Some(Price::from("4093.34")),
        );
        let at = amend_position(7, Some((Price::from("4041.00"), &fixed)), None);

        assert_eq!(trailing.stop_loss, Some(4041.32));
        assert_eq!(trailing.take_profit, Some(4093.34));
        assert_eq!(trailing.trailing_stop_loss, Some(true));
        assert_eq!(
            trailing.stop_loss_trigger_method(),
            ProtoOaOrderTriggerMethod::DoubleTrade
        );
        assert_eq!(at.trailing_stop_loss, None);
        assert_eq!(
            at.stop_loss_trigger_method, None,
            "a fixed stop's as before"
        );
        assert_eq!(at.stop_loss, Some(4041.0));
    }
}
