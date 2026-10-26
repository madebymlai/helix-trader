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

use nautilus_model::{
    enums::{OrderSide, OrderType, PositionSide, TimeInForce, TriggerType},
    events::OrderDeniedReason,
    identifiers::{ClientOrderId, PositionId, VenueOrderId},
    orders::{Order, OrderAny},
    position::Position,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::data::ic::messages::{
    ProtoOaClosePositionReq, ProtoOaNewOrderReq, ProtoOaOrderTriggerMethod, ProtoOaOrderType,
    ProtoOaTimeInForce, ProtoOaTradeSide,
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
    /// a limit at the take profit.
    pub(super) const fn order_type(self) -> OrderType {
        match self {
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
    pub(super) stop_loss: Option<Price>,
    pub(super) take_profit: Option<Price>,
}

/// How an order reaches cTrader.
#[derive(Debug, Clone, PartialEq)]
#[expect(clippy::large_enum_variant, reason = "a placement is sent at once")]
pub(super) enum Placement {
    New(ProtoOaNewOrderReq),
    Close(ProtoOaClosePositionReq),
}

/// The request placing `order`, of cTrader symbol `symbol_id`, into
/// `position` if it names one, with the bracket's `protection` on it.
///
/// # Errors
///
/// Returns why the order is denied: a type, time in force or flag cTrader
/// lacks, a reduce-only order without a position, a closing order larger
/// than its position, a volume off the cent grid.
pub(super) fn placement(
    order: &OrderAny,
    symbol_id: i64,
    position: Option<&Position>,
    protection: Protection,
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
            detail: "a reduce-only order on IC's hedging account names the position it reduces \
                     (close_position): without one it would open a position"
                .to_owned(),
        });
    }
    let mut request = ProtoOaNewOrderReq {
        symbol_id,
        trade_side: trade_side(order.order_side()) as i32,
        volume,
        client_order_id: Some(order.client_order_id().to_string()),
        position_id: position.map(|p| venue_position_id(p.id)).transpose()?,
        stop_loss: protection.stop_loss.map(|p| p.as_f64()),
        take_profit: protection.take_profit.map(|p| p.as_f64()),
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
        order_type => return Err(OrderDeniedReason::UnsupportedOrderType { order_type }),
    }
    if order.order_type() != OrderType::Market {
        let (time_in_force, expiry) = time_in_force(order)?;
        request.set_time_in_force(time_in_force);
        request.expiration_timestamp = expiry;
    }
    Ok(Placement::New(request))
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
            let ms = i64::try_from(expiry.as_u64() / 1_000_000).unwrap_or(i64::MAX);
            (ProtoOaTimeInForce::GoodTillDate, Some(ms))
        }
        other => return Err(OrderDeniedReason::UnsupportedTimeInForce(other)),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType},
        identifiers::InstrumentId,
        orders::OrderTestBuilder,
    };

    use super::*;

    fn gold() -> InstrumentId {
        InstrumentId::from("XAUUSD.ICMARKETS")
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

        let request = new_order(placement(&order, 41, None, Protection::default()).unwrap());

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
            stop_loss: Some(Price::from("3802.73")),
            take_profit: Some(Price::from("3842.73")),
        };

        let request = new_order(placement(&order, 41, None, protection).unwrap());

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

        let request = new_order(placement(&order, 41, None, Protection::default()).unwrap());

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
            assert!(
                placement(&order, 41, None, Protection::default()).is_err(),
                "{order:?}"
            );
        }
    }
}
