//! cTrader's positions, orders and deals as Nautilus' reconciliation
//! reports, on a hedging account.
//!
//! - **Positions** (`ProtoOAPosition`, open): one `PositionStatusReport`
//!   each, its `positionId` the venue position id, `price` (the VWAP of
//!   its executions) the entry average, unrounded.
//! - **Stop loss and take profit**: cTrader holds them on the position
//!   (`stopLoss`, `takeProfit`, amended by position id), so each becomes an
//!   open reduce-only order of the position, its venue order id the
//!   position's with `-SL` or `-TP`: a stop market order at the stop loss,
//!   a limit order at the take profit, for the position's volume.
//! - **Pending orders** (`ProtoOAOrder`): an `OrderStatusReport` each, by
//!   `orderId`, with the position it opens into or closes, if any.
//! - **Deals** (`ProtoOADeal`, filled): a `FillReport` each, by `dealId`,
//!   of order `orderId` and position `positionId`, the commission the
//!   charge cTrader states (a negative amount) as a positive cost.
//!
//! Prices travel as IEEE-754 doubles: an order's or a deal's price, a stop
//! loss and a take profit are on the instrument's tick, so they are read at
//! its precision, and one finer is an error, not rounded. Volumes are in
//! cents of a unit. Records of a symbol the client did not load are out of
//! its scope: dropped, logged at debug.

use std::collections::HashMap;

use anyhow::{Context, bail, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{
        LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce, TriggerType,
    },
    identifiers::{AccountId, ClientOrderId, PositionId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use super::orders::{Leg, protection_id};
use crate::data::ic::messages::{
    ProtoOaDeal, ProtoOaDealStatus, ProtoOaOrder, ProtoOaOrderStatus, ProtoOaOrderTriggerMethod,
    ProtoOaOrderType, ProtoOaPosition, ProtoOaPositionStatus, ProtoOaTimeInForce, ProtoOaTradeSide,
};

/// What the reports of one collection share: the account, the loaded
/// instruments by cTrader `symbolId`, the account's currency (of
/// commissions) and the collection's start.
pub(super) struct Reports<'a> {
    pub(super) account_id: AccountId,
    pub(super) instruments: &'a HashMap<i64, InstrumentAny>,
    pub(super) currency: Currency,
    pub(super) ts_init: UnixNanos,
}

impl Reports<'_> {
    /// The instrument of `symbol_id`, or `None`, logged, if not loaded.
    fn instrument(&self, symbol_id: i64, what: &str) -> Option<&InstrumentAny> {
        let instrument = self.instruments.get(&symbol_id);
        if instrument.is_none() {
            log::debug!("IC {what} of symbol {symbol_id} skipped: not an instrument of the run");
        }
        instrument
    }

    /// The report of `position`, if open and of a loaded symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if its side, volume or times cannot be read.
    pub(super) fn position(
        &self,
        position: &ProtoOaPosition,
    ) -> anyhow::Result<Option<PositionStatusReport>> {
        let trade = &position.trade_data;
        let Some(instrument) = self.open_instrument(position) else {
            return Ok(None);
        };
        let what = || format!("IC position {}", position.position_id);
        let side = match side(trade.trade_side()) {
            OrderSide::Buy => PositionSide::Long,
            _ => PositionSide::Short,
        };
        let avg_px_open = position
            .price
            .map(|price| decimal(price).with_context(what))
            .transpose()?;
        Ok(Some(PositionStatusReport::new(
            self.account_id,
            instrument.id(),
            side,
            quantity(instrument, trade.volume).with_context(what)?,
            millis(position.utc_last_update_timestamp.or(trade.open_timestamp))
                .with_context(what)?,
            self.ts_init,
            None,
            Some(position_id(position.position_id)),
            avg_px_open,
        )))
    }

    /// The stop loss and take profit of `position`, if open and of a loaded
    /// symbol, as its open reduce-only orders.
    ///
    /// # Errors
    ///
    /// Returns an error if a price is off the instrument's tick, or the
    /// volume or times cannot be read.
    pub(super) fn protections(
        &self,
        position: &ProtoOaPosition,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let trade = &position.trade_data;
        let Some(instrument) = self.open_instrument(position) else {
            return Ok(Vec::new());
        };
        let what = || format!("IC position {}", position.position_id);
        let closing = match side(trade.trade_side()) {
            OrderSide::Buy => OrderSide::Sell,
            _ => OrderSide::Buy,
        };
        let quantity = quantity(instrument, trade.volume).with_context(what)?;
        let ts = millis(position.utc_last_update_timestamp.or(trade.open_timestamp))
            .with_context(what)?;
        let protection = |leg: Leg, order_type| {
            OrderStatusReport::new(
                self.account_id,
                instrument.id(),
                None,
                protection_id(position.position_id, leg),
                Some(closing),
                order_type,
                TimeInForce::Gtc,
                OrderStatus::Accepted,
                quantity,
                Quantity::zero(instrument.size_precision()),
                ts,
                ts,
                self.ts_init,
                None,
            )
            .with_reduce_only(true)
            .with_venue_position_id(position_id(position.position_id))
        };
        let mut reports = Vec::new();
        if let Some(stop_loss) = position.stop_loss.filter(|p| *p > 0.0) {
            let trigger = price(instrument, stop_loss).with_context(what)?;
            reports.push(
                protection(Leg::StopLoss, OrderType::StopMarket)
                    .with_trigger_price(trigger)
                    .with_trigger_type(trigger_type(position.stop_loss_trigger_method())),
            );
        }
        if let Some(take_profit) = position.take_profit.filter(|p| *p > 0.0) {
            let limit = price(instrument, take_profit).with_context(what)?;
            reports.push(protection(Leg::TakeProfit, OrderType::Limit).with_price(limit));
        }
        Ok(reports)
    }

    /// The report of `leg` of the protection order `order` (a position's
    /// `STOP_LOSS_TAKE_PROFIT` order, its stop price the stop loss, its
    /// limit price the take profit) as `status`, if the order holds the leg
    /// and is of a loaded symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if a price is off the instrument's tick, or the
    /// volume or times cannot be read.
    pub(super) fn protection_status(
        &self,
        order: &ProtoOaOrder,
        leg: Leg,
        status: OrderStatus,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let trade = &order.trade_data;
        let held = match leg {
            Leg::StopLoss => order.stop_price,
            Leg::TakeProfit => order.limit_price,
        };
        let (Some(held), Some(position)) = (held.filter(|p| *p > 0.0), order.position_id) else {
            return Ok(None);
        };
        let Some(instrument) = self.instrument(trade.symbol_id, "protection") else {
            return Ok(None);
        };
        let what = || format!("IC protection order {}", order.order_id);
        let quantity = quantity(instrument, trade.volume).with_context(what)?;
        let filled = if status == OrderStatus::Filled {
            quantity
        } else {
            Quantity::zero(instrument.size_precision())
        };
        let ts =
            millis(order.utc_last_update_timestamp.or(trade.open_timestamp)).with_context(what)?;
        let report = OrderStatusReport::new(
            self.account_id,
            instrument.id(),
            None,
            protection_id(position, leg),
            Some(side(trade.trade_side())),
            leg.order_type(),
            TimeInForce::Gtc,
            status,
            quantity,
            filled,
            ts,
            ts,
            self.ts_init,
            None,
        )
        .with_reduce_only(true)
        .with_venue_position_id(position_id(position));
        let held = price(instrument, held).with_context(what)?;
        Ok(Some(match leg {
            Leg::StopLoss => report
                .with_trigger_price(held)
                .with_trigger_type(trigger_type(order.stop_trigger_method())),
            Leg::TakeProfit => report.with_price(held),
        }))
    }

    /// The reports of `leg` of the protection order `order` filled by
    /// `deal`: its status, filled, and the fill, under the leg's venue order
    /// id.
    ///
    /// # Errors
    ///
    /// As `protection_status` and `fill`.
    pub(super) fn protection_fill(
        &self,
        order: &ProtoOaOrder,
        deal: &ProtoOaDeal,
        leg: Leg,
    ) -> anyhow::Result<Option<(OrderStatusReport, FillReport)>> {
        let Some(status) = self.protection_status(order, leg, OrderStatus::Filled)? else {
            return Ok(None);
        };
        let Some(mut fill) = self.fill(deal)? else {
            return Ok(None);
        };
        fill.venue_order_id = status.venue_order_id;
        let status = match deal.execution_price {
            Some(avg) => status.with_avg_px(decimal(avg)?),
            None => status,
        };
        Ok(Some((status, fill)))
    }

    /// The instrument of `position`, if open and loaded: cTrader also
    /// lists a position created empty for a pending order, which holds
    /// nothing.
    fn open_instrument(&self, position: &ProtoOaPosition) -> Option<&InstrumentAny> {
        if position.position_status() != ProtoOaPositionStatus::PositionStatusOpen {
            log::debug!(
                "IC position {} skipped: {}",
                position.position_id,
                position.position_status().as_str_name()
            );
            return None;
        }
        self.instrument(position.trade_data.symbol_id, "position")
    }

    /// The report of `order`, which IC holds nowhere: its request never
    /// reached IC, so it is rejected for `reason`. Its venue order id is
    /// its own, else its client order id, IC having given it none.
    pub(super) fn absent(&self, order: &OrderAny, reason: &str) -> OrderStatusReport {
        let venue = order
            .venue_order_id()
            .unwrap_or_else(|| VenueOrderId::new(order.client_order_id().as_str()));
        OrderStatusReport::new(
            self.account_id,
            order.instrument_id(),
            Some(order.client_order_id()),
            venue,
            Some(order.order_side()),
            order.order_type(),
            order.time_in_force(),
            OrderStatus::Rejected,
            order.quantity(),
            order.filled_qty(),
            self.ts_init,
            self.ts_init,
            self.ts_init,
            None,
        )
        .with_cancel_reason(reason.to_owned())
    }

    /// The report of `order`, if of a loaded symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if its type is not mapped (stop limit, whose limit
    /// cTrader states as a slippage), a price is off the instrument's tick,
    /// or its volumes or times cannot be read.
    pub(super) fn order(&self, order: &ProtoOaOrder) -> anyhow::Result<Option<OrderStatusReport>> {
        let trade = &order.trade_data;
        let Some(instrument) = self.instrument(trade.symbol_id, "order") else {
            return Ok(None);
        };
        let what = || format!("IC order {}", order.order_id);
        let quantity = quantity(instrument, trade.volume).with_context(what)?;
        let filled = match order.executed_volume.filter(|v| *v > 0) {
            Some(volume) => self::quantity(instrument, volume).with_context(what)?,
            None => Quantity::zero(instrument.size_precision()),
        };
        let status = match order.order_status() {
            ProtoOaOrderStatus::OrderStatusAccepted if filled.is_positive() => {
                OrderStatus::PartiallyFilled
            }
            ProtoOaOrderStatus::OrderStatusAccepted => OrderStatus::Accepted,
            ProtoOaOrderStatus::OrderStatusFilled => OrderStatus::Filled,
            ProtoOaOrderStatus::OrderStatusRejected => OrderStatus::Rejected,
            ProtoOaOrderStatus::OrderStatusExpired => OrderStatus::Expired,
            ProtoOaOrderStatus::OrderStatusCancelled => OrderStatus::Canceled,
        };
        let order_type = match order.order_type() {
            ProtoOaOrderType::Market | ProtoOaOrderType::MarketRange => OrderType::Market,
            ProtoOaOrderType::Limit => OrderType::Limit,
            ProtoOaOrderType::Stop => OrderType::StopMarket,
            other => bail!(
                "{}: cTrader {} orders are not mapped",
                what(),
                other.as_str_name()
            ),
        };
        let (time_in_force, expire_time) = match order.time_in_force() {
            ProtoOaTimeInForce::GoodTillDate => (
                TimeInForce::Gtd,
                Some(millis(order.expiration_timestamp).with_context(what)?),
            ),
            ProtoOaTimeInForce::GoodTillCancel => (TimeInForce::Gtc, None),
            ProtoOaTimeInForce::ImmediateOrCancel => (TimeInForce::Ioc, None),
            ProtoOaTimeInForce::FillOrKill => (TimeInForce::Fok, None),
            ProtoOaTimeInForce::MarketOnOpen => (TimeInForce::AtTheOpen, None),
        };
        let accepted = millis(trade.open_timestamp).with_context(what)?;
        let last =
            millis(order.utc_last_update_timestamp.or(trade.open_timestamp)).with_context(what)?;
        let client_order_id = order
            .client_order_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .map(ClientOrderId::new);
        let mut report = OrderStatusReport::new(
            self.account_id,
            instrument.id(),
            client_order_id,
            VenueOrderId::new(order.order_id.to_string()),
            Some(side(trade.trade_side())),
            order_type,
            time_in_force,
            status,
            quantity,
            filled,
            accepted,
            last,
            self.ts_init,
            None,
        )
        .with_reduce_only(order.closing_order.unwrap_or_default());
        if let Some(expire_time) = expire_time {
            report = report.with_expire_time(expire_time);
        }
        if let Some(position) = order.position_id.filter(|id| *id > 0) {
            report = report.with_venue_position_id(position_id(position));
        }
        match order_type {
            OrderType::Limit => {
                let limit = order
                    .limit_price
                    .context("a limit order without a limit price");
                report = report.with_price(price(instrument, limit.with_context(what)?)?);
            }
            OrderType::StopMarket => {
                let stop = order
                    .stop_price
                    .context("a stop order without a stop price");
                report = report
                    .with_trigger_price(price(instrument, stop.with_context(what)?)?)
                    .with_trigger_type(trigger_type(order.stop_trigger_method()));
            }
            _ => {}
        }
        if let Some(avg) = order.execution_price.filter(|_| filled.is_positive()) {
            report = report.with_avg_px(decimal(avg).with_context(what)?);
        }
        Ok(Some(report))
    }

    /// The fill of `deal`, if filled and of a loaded symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if its price is missing or off the instrument's
    /// tick, or its volume, commission or time cannot be read.
    pub(super) fn fill(&self, deal: &ProtoOaDeal) -> anyhow::Result<Option<FillReport>> {
        if !fills(deal) {
            return Ok(None);
        }
        let Some(instrument) = self.instrument(deal.symbol_id, "deal") else {
            return Ok(None);
        };
        let what = || format!("IC deal {}", deal.deal_id);
        let execution = deal
            .execution_price
            .context("a filled deal without an execution price")
            .with_context(what)?;
        let commission = commission(deal, self.currency).with_context(what)?;
        Ok(Some(FillReport::new(
            self.account_id,
            instrument.id(),
            VenueOrderId::new(deal.order_id.to_string()),
            TradeId::new(deal.deal_id.to_string()),
            side(deal.trade_side()),
            quantity(instrument, deal.filled_volume).with_context(what)?,
            price(instrument, execution).with_context(what)?,
            commission,
            LiquiditySide::NoLiquiditySide,
            None,
            Some(position_id(deal.position_id)),
            millis(Some(deal.execution_timestamp)).with_context(what)?,
            self.ts_init,
            None,
        )))
    }
}

/// Of `deals`, those that explain their positions: a position's deals are
/// kept when their volumes add up to the position as cTrader holds it in
/// `open` (signed volume by `positionId`), or to nothing for a position no
/// longer open; else its history starts before them, and applying them
/// would open a position that does not exist. Also returns the positions
/// whose deals were dropped.
pub(super) fn explained<'d>(
    deals: &'d [ProtoOaDeal],
    open: &HashMap<i64, i64>,
) -> (Vec<&'d ProtoOaDeal>, Vec<i64>) {
    let mut net: HashMap<i64, i64> = HashMap::new();
    for deal in deals.iter().filter(|d| fills(d)) {
        let signed = match side(deal.trade_side()) {
            OrderSide::Buy => deal.filled_volume,
            _ => -deal.filled_volume,
        };
        *net.entry(deal.position_id).or_default() += signed;
    }
    let mut unexplained: Vec<i64> = net
        .iter()
        .filter(|(position, volume)| open.get(position).copied().unwrap_or_default() != **volume)
        .map(|(position, _)| *position)
        .collect();
    unexplained.sort_unstable();
    let kept = deals
        .iter()
        .filter(|d| fills(d) && unexplained.binary_search(&d.position_id).is_err())
        .collect();
    (kept, unexplained)
}

/// The signed volume, in cents, of each open position of `positions`.
pub(super) fn open_volumes(positions: &[ProtoOaPosition]) -> HashMap<i64, i64> {
    positions
        .iter()
        .filter(|p| p.position_status() == ProtoOaPositionStatus::PositionStatusOpen)
        .map(|p| {
            let volume = p.trade_data.volume;
            let signed = match side(p.trade_data.trade_side()) {
                OrderSide::Buy => volume,
                _ => -volume,
            };
            (p.position_id, signed)
        })
        .collect()
}

/// The commission cTrader charged on `deal` (a negative amount in the
/// deposit currency `currency`, at its `moneyDigits`) as Nautilus' positive
/// cost.
///
/// # Errors
///
/// Returns an error if the amount does not fit `currency`.
pub(super) fn commission(deal: &ProtoOaDeal, currency: Currency) -> anyhow::Result<Money> {
    let charged = Decimal::new(
        deal.commission.unwrap_or_default(),
        deal.money_digits.unwrap_or(2),
    );
    Money::from_decimal(-charged, currency).map_err(|e| anyhow::anyhow!("commission: {e}"))
}

/// Whether `deal` executed some volume.
pub(super) fn fills(deal: &ProtoOaDeal) -> bool {
    matches!(
        deal.deal_status(),
        ProtoOaDealStatus::Filled | ProtoOaDealStatus::PartiallyFilled
    ) && deal.filled_volume > 0
}

pub(super) fn side(side: ProtoOaTradeSide) -> OrderSide {
    match side {
        ProtoOaTradeSide::Buy => OrderSide::Buy,
        ProtoOaTradeSide::Sell => OrderSide::Sell,
    }
}

pub(super) fn position_id(id: i64) -> PositionId {
    PositionId::new(id.to_string())
}

/// cTrader's trigger method as Nautilus': `TRADE` (a buy triggered by the
/// ask, a sell by the bid) is `BID_ASK`, `DOUBLE_TRADE` `DOUBLE_BID_ASK`;
/// the `OPPOSITE` methods have no Nautilus counterpart, and the venue holds
/// the order: `DEFAULT`.
pub(super) fn trigger_type(method: ProtoOaOrderTriggerMethod) -> TriggerType {
    match method {
        ProtoOaOrderTriggerMethod::Trade => TriggerType::BidAsk,
        ProtoOaOrderTriggerMethod::DoubleTrade => TriggerType::DoubleBidAsk,
        ProtoOaOrderTriggerMethod::Opposite | ProtoOaOrderTriggerMethod::DoubleOpposite => {
            TriggerType::Default
        }
    }
}

/// The decimal a double was written from: its shortest round-trip form.
pub(super) fn decimal(value: f64) -> anyhow::Result<Decimal> {
    ensure!(value.is_finite(), "price {value} is not finite");
    value
        .to_string()
        .parse()
        .with_context(|| format!("price {value} out of range"))
}

/// `value`, a price on `instrument`'s tick, at its precision.
pub(super) fn price(instrument: &InstrumentAny, value: f64) -> anyhow::Result<Price> {
    let precision = instrument.price_precision();
    let value = decimal(value)?.normalize();
    ensure!(
        value.scale() <= u32::from(precision),
        "price {value} is finer than {}'s {precision} decimals",
        instrument.id()
    );
    Price::from_decimal_dp(value, precision).map_err(|e| anyhow::anyhow!("price {value}: {e}"))
}

/// A volume, in cents of a unit, as a quantity of `instrument`.
pub(super) fn quantity(instrument: &InstrumentAny, cents: i64) -> anyhow::Result<Quantity> {
    ensure!(cents > 0, "volume {cents} is not positive");
    let precision = instrument.size_precision();
    let units = Decimal::new(cents, 2).normalize();
    ensure!(
        units.scale() <= u32::from(precision),
        "volume of {units} units is finer than {}'s {precision} decimals",
        instrument.id()
    );
    Quantity::from_decimal_dp(units, precision).map_err(|e| anyhow::anyhow!("volume {units}: {e}"))
}

/// A Unix time in milliseconds, which must be there.
pub(super) fn millis(ms: Option<i64>) -> anyhow::Result<UnixNanos> {
    let ms = ms.context("no time")?;
    let ms = u64::try_from(ms).with_context(|| format!("time {ms} ms before 1970"))?;
    ms.checked_mul(1_000_000)
        .map(UnixNanos::new)
        .with_context(|| format!("time {ms} ms out of range"))
}
