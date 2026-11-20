//! Nautilus orders as cTrader trade requests, on a hedging account.
//!
//! - **A new order** (`ProtoOANewOrderReq`) carries the Nautilus client order id as its
//!   `clientOrderId`:
//!   - A market order GTC, IOC or DAY is a `MARKET`, whose time in force is cTrader's own (IOC).
//!   - A limit order GTC or GTD (expiry in ms) is a `LIMIT`; IOC is a `MARKET_RANGE` based at its
//!     price, with no slippage.
//!   - A stop market order GTC or GTD is a `STOP`; trigger `DEFAULT` or `BID_ASK` is cTrader's
//!     `TRADE`, `DOUBLE_BID_ASK` is `DOUBLE_TRADE`.
//!   - cTrader bounds a `MARKET_RANGE`'s slippage on the worse side alone: the demo filled one
//!     based 3.00 on the better side at the market, a buy and a sell, and refused one whose fill
//!     would be worse, whole, with its deal rejected (2026-11-16).
//!   - cTrader holds a `LIMIT` or `STOP` sent IOC or FOK as GTC, and refuses FOK on its immediate
//!     orders, so FOK, a stop's IOC, DAY on a pending order and a session's open or close are
//!     denied. Post-only, quote-quantity and display quantities are denied: cTrader has none.
//! - **A bracket's protections** go on its entry, absolute (`stopLoss`, `takeProfit`): cTrader
//!   moves them to the position the entry opens, as one `STOP_LOSS_TAKE_PROFIT` order held as long
//!   as the position, so a leg other than GTC is denied. Spotware's proto says absolute protections
//!   are not supported for market orders; the demo took them on a market order (2026-10-26) and a
//!   `MARKET_RANGE` (2026-11-16) and set them on its position.
//! - **An order naming a position** (`SubmitOrder.position_id`, the cTrader `positionId`): a market
//!   order on the closing side closes that much of the position (`ProtoOAClosePositionReq`), never
//!   more than it holds; any other carries the `positionId`. A reduce-only order without a position
//!   is denied: on a hedging account it would open one.
//! - **Volumes** are in cents of a unit; prices go as doubles, at the instrument's precision.
//!
//! Each request goes under a `clientMsgId` of its `Tag`, and its answer and execution events come
//! back under it. cTrader makes a close request's `clientMsgId` the `clientOrderId` of the closing
//! order it creates (demo, 2026-10-26).

use std::{fmt::Display, str::FromStr};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, OrderType, PositionSide, TimeInForce, TriggerType},
    events::OrderDeniedReason,
    identifiers::{ClientOrderId, PositionId, VenueOrderId},
    orders::{Order, OrderAny},
    position::Position,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::common::messages::{
    ProtoOaClosePositionReq, ProtoOaNewOrderReq, ProtoOaOrderTriggerMethod, ProtoOaOrderType,
    ProtoOaTimeInForce, ProtoOaTradeSide,
};

/// The request's `clientMsgId`, `<op>:<client order id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Tag {
    New(ClientOrderId),
    Amend(ClientOrderId),
    Cancel(ClientOrderId),
    /// A position closed by this closing order.
    Close(ClientOrderId),
    /// A protection (stop loss or take profit) moved.
    Protect(ClientOrderId),
    /// A protection removed.
    Unprotect(ClientOrderId),
}

impl Tag {
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

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

/// A position's protection: cTrader holds at most one stop loss and one take profit on a position.
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

    pub(super) const fn order_type(self) -> OrderType {
        match self {
            Self::StopLoss => OrderType::StopMarket,
            Self::TakeProfit => OrderType::Limit,
        }
    }
}

/// The venue order id of `leg` of position `position_id`: the position's id with `-SL` or `-TP`.
pub(super) fn protection_id(position_id: i64, leg: Leg) -> VenueOrderId {
    VenueOrderId::new(format!("{position_id}-{}", leg.suffix()))
}

pub(super) fn parse_protection_id(id: VenueOrderId) -> Option<(i64, Leg)> {
    let (position, suffix) = id.as_str().rsplit_once('-')?;
    let leg = match suffix {
        "SL" => Leg::StopLoss,
        "TP" => Leg::TakeProfit,
        _ => return None,
    };
    Some((position.parse().ok()?, leg))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Protection {
    pub(super) stop_loss: Option<Price>,
    pub(super) take_profit: Option<Price>,
}

#[derive(Debug, Clone, PartialEq)]
#[expect(clippy::large_enum_variant, reason = "a placement is sent at once")]
pub(super) enum Placement {
    New(ProtoOaNewOrderReq),
    Close(ProtoOaClosePositionReq),
}

/// The request placing `order` of cTrader symbol `symbol_id`, into `position` if it names one, with
/// the bracket's `protection` on it.
///
/// # Errors
///
/// Returns an error if:
/// - The order has a type, trigger or flag cTrader lacks, a time in force it does not honor for the
///   type, or is good till a date it does not name.
/// - It is reduce-only without a position.
/// - It closes `position` and is larger than it, or carries protection.
/// - `position` is not a cTrader position.
/// - Its volume is off the cent grid.
///
/// The error is why the order is denied.
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
            detail:
                "a reduce-only order on a cTrader hedging account names the position it reduces \
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
        OrderType::Market => {
            immediate(order)?;
            request.set_order_type(ProtoOaOrderType::Market);
        }

        // cTrader fills a MARKET_RANGE within its slippage of its base or better, or refuses it
        OrderType::Limit if order.time_in_force() == TimeInForce::Ioc => {
            request.set_order_type(ProtoOaOrderType::MarketRange);
            request.base_slippage_price = order.price().map(|p| p.as_f64());
            request.slippage_in_points = Some(0);
        }
        OrderType::Limit => {
            request.set_order_type(ProtoOaOrderType::Limit);
            request.limit_price = order.price().map(|p| p.as_f64());
            pending(&mut request, order)?;
        }
        OrderType::StopMarket => {
            request.set_order_type(ProtoOaOrderType::Stop);
            request.stop_price = order.trigger_price().map(|p| p.as_f64());
            request.set_stop_trigger_method(trigger_method(order.trigger_type())?);
            pending(&mut request, order)?;
        }
        order_type => return Err(OrderDeniedReason::UnsupportedOrderType { order_type }),
    }
    Ok(Placement::New(request))
}

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
/// Returns an error if the quantity is finer than a cent of a unit.
pub(super) fn volume(quantity: Quantity) -> Result<i64, OrderDeniedReason> {
    let cents = quantity.as_decimal() * Decimal::ONE_HUNDRED;
    (cents.fract().is_zero())
        .then(|| i64::try_from(cents).ok())
        .flatten()
        .ok_or_else(|| OrderDeniedReason::ValidationFailed {
            detail: format!("{quantity} is not a whole number of cents of a unit"),
        })
}

/// The venue's `positionId`, which a position this client did not open also carries.
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

/// `TRADE` (a buy by the ask, a sell by the bid) for `DEFAULT` and `BID_ASK`, `DOUBLE_TRADE` for
/// `DOUBLE_BID_ASK`.
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

/// cTrader executes a MARKET at once and cancels what it cannot fill (IOC), which honors GTC, IOC
/// and DAY. It refuses a MARKET of FOK ("Such TIF is not supported by this Order type") and of
/// MARKET_ON_OPEN ("Incorrect time of execution", demo, 2026-11-16), and has no market on close.
fn immediate(order: &OrderAny) -> Result<(), OrderDeniedReason> {
    match order.time_in_force() {
        TimeInForce::Gtc | TimeInForce::Ioc | TimeInForce::Day => Ok(()),
        other => Err(OrderDeniedReason::UnsupportedTimeInForce(other)),
    }
}

/// cTrader takes a LIMIT or STOP sent IOC or FOK and holds it GTC (demo, 2026-11-16), and has no
/// DAY nor the session's open or close.
fn pending(request: &mut ProtoOaNewOrderReq, order: &OrderAny) -> Result<(), OrderDeniedReason> {
    let (time_in_force, expiry) = match order.time_in_force() {
        TimeInForce::Gtc => (ProtoOaTimeInForce::GoodTillCancel, None),
        TimeInForce::Gtd => {
            let expiry = order
                .expire_time()
                .ok_or(OrderDeniedReason::MissingExpireTime)?;
            (ProtoOaTimeInForce::GoodTillDate, Some(millis(expiry)))
        }
        other => return Err(OrderDeniedReason::UnsupportedTimeInForce(other)),
    };
    request.set_time_in_force(time_in_force);
    request.expiration_timestamp = expiry;
    Ok(())
}

pub(super) fn millis(time: UnixNanos) -> i64 {
    i64::try_from(time.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType},
        identifiers::InstrumentId,
        orders::OrderTestBuilder,
    };
    use rstest::rstest;

    use super::*;

    fn gold() -> InstrumentId {
        InstrumentId::from("XAUUSD.ACMEFX")
    }

    fn new_order(placement: Placement) -> ProtoOaNewOrderReq {
        let Placement::New(request) = placement else {
            panic!("not a new order: {placement:?}");
        };
        request
    }

    const ORDER: &str = "O-20261026-150000-001-001-1";

    /// A `clientMsgId` of no op and client order id, as a probe's, is no tag.
    #[rstest]
    #[case::new(
        "N:O-20261026-150000-001-001-1",
        Ok(Tag::New(ClientOrderId::from(ORDER)))
    )]
    #[case::amend(
        "A:O-20261026-150000-001-001-1",
        Ok(Tag::Amend(ClientOrderId::from(ORDER)))
    )]
    #[case::cancel(
        "X:O-20261026-150000-001-001-1",
        Ok(Tag::Cancel(ClientOrderId::from(ORDER)))
    )]
    #[case::close(
        "C:O-20261026-150000-001-001-1",
        Ok(Tag::Close(ClientOrderId::from(ORDER)))
    )]
    #[case::protect(
        "P:O-20261026-150000-001-001-1",
        Ok(Tag::Protect(ClientOrderId::from(ORDER)))
    )]
    #[case::unprotect(
        "U:O-20261026-150000-001-001-1",
        Ok(Tag::Unprotect(ClientOrderId::from(ORDER)))
    )]
    #[case::no_op("PROBE-5", Err(()))]
    #[case::no_client_order_id("7", Err(()))]
    fn a_tag_is_the_op_and_the_client_order_id_and_reads_back(
        #[case] text: &str,
        #[case] expected: Result<Tag, ()>,
    ) {
        assert_eq!(text.parse::<Tag>().map_err(|_| ()), expected);
        if let Ok(tag) = expected {
            assert_eq!(tag.to_string(), text);
        }
    }

    /// A cTrader order's id is no protection's.
    #[rstest]
    #[case::stop_loss("690004734-SL", Some((690_004_734, Leg::StopLoss)))]
    #[case::take_profit("690004734-TP", Some((690_004_734, Leg::TakeProfit)))]
    #[case::an_order("1031776265", None)]
    fn a_protection_id_is_the_position_and_the_leg(
        #[case] venue_order_id: &str,
        #[case] expected: Option<(i64, Leg)>,
    ) {
        let id = VenueOrderId::from(venue_order_id);
        assert_eq!(parse_protection_id(id), expected);
        if let Some((position_id, leg)) = expected {
            assert_eq!(protection_id(position_id, leg), id);
        }
    }

    #[rstest]
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

    #[rstest]
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

    /// cTrader holds a LIMIT sent IOC as GTC (demo, 2026-11-16); its IOC execution at a price or
    /// better is MARKET_RANGE, whose base the demo filled better than on either side.
    #[rstest]
    #[case::buy(OrderSide::Buy, ProtoOaTradeSide::Buy)]
    #[case::sell(OrderSide::Sell, ProtoOaTradeSide::Sell)]
    fn a_limit_ioc_order_is_a_market_range_based_at_its_price_with_no_slippage(
        #[case] side: OrderSide,
        #[case] trade_side: ProtoOaTradeSide,
    ) {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(gold())
            .side(side)
            .quantity(Quantity::from(1))
            .price(Price::from("3869.20"))
            .time_in_force(TimeInForce::Ioc)
            .build();

        let request = new_order(placement(&order, 41, None, Protection::default()).unwrap());

        assert_eq!(request.order_type(), ProtoOaOrderType::MarketRange);
        assert_eq!(request.trade_side(), trade_side);
        assert_eq!(request.base_slippage_price, Some(3869.2));
        assert_eq!(request.slippage_in_points, Some(0));
        assert_eq!(request.limit_price, None);
        assert_eq!(request.time_in_force, None, "cTrader's own IOC");
        assert_eq!(request.volume, 100);
    }

    #[rstest]
    fn a_stop_market_order_is_a_stop_triggered_as_its_trigger_type() {
        let order = OrderTestBuilder::new(OrderType::StopMarket)
            .instrument_id(gold())
            .side(OrderSide::Sell)
            .quantity(Quantity::from(2))
            .trigger_price(Price::from("3990.50"))
            .trigger_type(TriggerType::DoubleBidAsk)
            .build();

        let request = new_order(placement(&order, 41, None, Protection::default()).unwrap());

        assert_eq!(request.symbol_id, 41);
        assert_eq!(request.order_type(), ProtoOaOrderType::Stop);
        assert_eq!(request.trade_side(), ProtoOaTradeSide::Sell);
        assert_eq!(request.volume, 200, "2 ounces, in cents");
        assert_eq!(request.stop_price, Some(3990.5));
        assert_eq!(
            request.stop_trigger_method(),
            ProtoOaOrderTriggerMethod::DoubleTrade
        );
    }

    /// The order at 4000.00 if it has a price, a GTD's expiring at `EXPIRY`.
    fn lasting(order_type: OrderType, time_in_force: TimeInForce) -> OrderAny {
        let mut builder = OrderTestBuilder::new(order_type);
        builder
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .time_in_force(time_in_force);

        match order_type {
            OrderType::Limit => builder.price(Price::from("4000.00")),
            OrderType::StopMarket => builder.trigger_price(Price::from("4000.00")),
            _ => &mut builder,
        };

        if time_in_force == TimeInForce::Gtd {
            builder.expire_time(EXPIRY);
        }
        builder.build()
    }

    /// 2026-11-16 18:00 UTC.
    const EXPIRY: UnixNanos = UnixNanos::new(1_794_852_000_000_000_000);

    /// A market order goes without a time in force (cTrader's own IOC), a pending one GTC or GTD
    /// with its expiry in ms.
    #[rstest]
    #[case::market_gtc(OrderType::Market, TimeInForce::Gtc, None, None)]
    #[case::market_ioc(OrderType::Market, TimeInForce::Ioc, None, None)]
    #[case::market_day(OrderType::Market, TimeInForce::Day, None, None)]
    #[case::limit_gtc(
        OrderType::Limit,
        TimeInForce::Gtc,
        Some(ProtoOaTimeInForce::GoodTillCancel),
        None
    )]
    #[case::limit_gtd(
        OrderType::Limit,
        TimeInForce::Gtd,
        Some(ProtoOaTimeInForce::GoodTillDate),
        Some(1_794_852_000_000)
    )]
    #[case::stop_gtc(
        OrderType::StopMarket,
        TimeInForce::Gtc,
        Some(ProtoOaTimeInForce::GoodTillCancel),
        None
    )]
    #[case::stop_gtd(
        OrderType::StopMarket,
        TimeInForce::Gtd,
        Some(ProtoOaTimeInForce::GoodTillDate),
        Some(1_794_852_000_000)
    )]
    fn a_time_in_force_ctrader_honors_is_its_own(
        #[case] order_type: OrderType,
        #[case] time_in_force: TimeInForce,
        #[case] sent: Option<ProtoOaTimeInForce>,
        #[case] expiration: Option<i64>,
    ) {
        let order = lasting(order_type, time_in_force);

        let request = new_order(placement(&order, 41, None, Protection::default()).unwrap());

        assert_eq!(request.time_in_force, sent.map(|t| t as i32));
        assert_eq!(request.expiration_timestamp, expiration);
    }

    /// The demo refused FOK on MARKET and MARKET_RANGE ("Such TIF is not supported by this Order
    /// type") and MARKET_ON_OPEN on MARKET ("Incorrect time of execution"), and held a LIMIT or
    /// STOP sent IOC or FOK as GTC (2026-11-16).
    #[rstest]
    #[case::market_fok(OrderType::Market, TimeInForce::Fok)]
    #[case::market_at_the_open(OrderType::Market, TimeInForce::AtTheOpen)]
    #[case::market_at_the_close(OrderType::Market, TimeInForce::AtTheClose)]
    #[case::limit_fok(OrderType::Limit, TimeInForce::Fok)]
    #[case::limit_day(OrderType::Limit, TimeInForce::Day)]
    #[case::limit_at_the_open(OrderType::Limit, TimeInForce::AtTheOpen)]
    #[case::limit_at_the_close(OrderType::Limit, TimeInForce::AtTheClose)]
    #[case::stop_ioc(OrderType::StopMarket, TimeInForce::Ioc)]
    #[case::stop_fok(OrderType::StopMarket, TimeInForce::Fok)]
    #[case::stop_day(OrderType::StopMarket, TimeInForce::Day)]
    #[case::stop_at_the_open(OrderType::StopMarket, TimeInForce::AtTheOpen)]
    #[case::stop_at_the_close(OrderType::StopMarket, TimeInForce::AtTheClose)]
    fn a_time_in_force_ctrader_cannot_honor_is_denied(
        #[case] order_type: OrderType,
        #[case] time_in_force: TimeInForce,
    ) {
        let order = lasting(order_type, time_in_force);

        assert_eq!(
            placement(&order, 41, None, Protection::default()),
            Err(OrderDeniedReason::UnsupportedTimeInForce(time_in_force))
        );
    }

    fn stop_limit() -> OrderAny {
        OrderTestBuilder::new(OrderType::StopLimit)
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .price(Price::from("4000.00"))
            .trigger_price(Price::from("3999.00"))
            .build()
    }

    fn post_only() -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .price(Price::from("4000.00"))
            .post_only(true)
            .build()
    }

    fn reduce_only() -> OrderAny {
        OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold())
            .quantity(Quantity::from(1))
            .reduce_only(true)
            .build()
    }

    #[derive(Debug)]
    enum Denial {
        OrderType(OrderType),
        Flag(&'static str),
    }

    /// cTrader has no stop limit nor post-only orders, and a reduce-only order on a hedging account
    /// needs the position it reduces.
    #[rstest]
    #[case::stop_limit(stop_limit(), Denial::OrderType(OrderType::StopLimit))]
    #[case::post_only(post_only(), Denial::Flag("post-only"))]
    #[case::reduce_only(reduce_only(), Denial::Flag("reduce-only"))]
    fn order_types_and_flags_ctrader_lacks_are_denied(
        #[case] order: OrderAny,
        #[case] expected: Denial,
    ) {
        let denied = placement(&order, 41, None, Protection::default());

        match (&denied, &expected) {
            (Err(OrderDeniedReason::UnsupportedOrderType { order_type }), Denial::OrderType(t)) => {
                assert_eq!(order_type, t);
            }
            (Err(OrderDeniedReason::ValidationFailed { detail }), Denial::Flag(flag)) => {
                assert!(detail.contains(flag), "{detail}");
            }
            _ => panic!("{expected:?}: {denied:?}"),
        }
    }
}
