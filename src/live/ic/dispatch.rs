//! cTrader's trade events as Nautilus order events and reports.
//!
//! The execution client registers each order it sends (`track`), with its
//! identity and shape, before the request goes out; the trade events then
//! take one route each:
//!
//! - **Tracked**: an event of an order this client sent, by its
//!   `clientOrderId` (a close's being its request's `clientMsgId`, see
//!   `orders`), becomes the order's events: accepted, filled (each deal
//!   once, by `dealId`, at its price and volume, with its commission and
//!   `positionId`), updated, cancelled, expired, rejected. A fill before the
//!   acceptance is preceded by the acceptance it proves.
//! - **Protections**: a position's stop loss and take profit are one
//!   cTrader order, `STOP_LOSS_TAKE_PROFIT`, its stop price the stop loss
//!   and its limit price the take profit, named by the bracket entry's
//!   `clientOrderId`. Its acceptance accepts the bracket's legs (venue order
//!   ids `<positionId>-SL`, `-TP`), its replacement updates them (a price
//!   gone cancels its leg), its fill fills the leg whose price is nearer the
//!   deal's and cancels the other, its cancellation cancels them. While the
//!   entry is pending its protections live on it, and its replacement
//!   updates the legs the same way.
//! - **External**: an event of an order not tracked (placed elsewhere, or
//!   before a restart) becomes reports, which Nautilus reconciles: the
//!   order's status with the deal's fill, or the legs' of a protection.
//!   Such an order is tracked (`adopt`) once a modify or cancel of it is
//!   sent, so that a refusal reaches it.
//! - **Refusals**: an order error or an error answer, under the request's
//!   `clientMsgId` (`Tag`), rejects a new order or a close, and the legs of
//!   a bracket entry; rejects the modification of an amendment; rejects the
//!   cancel of a cancellation, of each protection of the position for a
//!   removal of protections. The reason is cTrader's code and
//!   description.
//! - **Protections asked**: a move or removal of a protection is recorded
//!   (`want`) until cTrader's protection shows it done, or it is refused:
//!   an amendment, which sets both protections, carries the other's.
//!
//! A terminal order's repeated events are suppressed; a fill with a new
//! `dealId` after it goes as a report.

use std::collections::HashMap;

use anyhow::Context;
use nautilus_common::{cache::fifo::FifoCache, messages::ExecutionReport};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::execution::context::OrderIdentity;
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus},
    events::{
        OrderAccepted, OrderCancelRejected, OrderCanceled, OrderEventAny, OrderExpired,
        OrderFilled, OrderModifyRejected, OrderRejected, OrderUpdated,
    },
    identifiers::{AccountId, ClientOrderId, TradeId, TraderId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{Currency, Price, Quantity},
};
use ustr::Ustr;

use super::{
    orders::{Leg, Protection, Tag, parse_protection_id, protection_id},
    reports::{self, Reports},
};
use crate::data::ic::{
    messages::{
        ProtoOaDeal, ProtoOaExecutionEvent, ProtoOaExecutionType, ProtoOaOrder, ProtoOaOrderStatus,
        ProtoOaOrderType,
    },
    websocket::{TradeEvent, TradeMessage},
};

/// How many deal ids the dispatch remembers, to drop a deal seen twice.
const DEALS_REMEMBERED: usize = 10_000;

/// What a trade event becomes.
#[derive(Debug, Clone)]
#[expect(
    clippy::large_enum_variant,
    reason = "each output goes to the node at once"
)]
pub(super) enum Output {
    Event(OrderEventAny),
    Report(ExecutionReport),
}

/// An order this client sent: its identity (Nautilus' `OrderIdentity`),
/// shape and state.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Tracked {
    pub(super) identity: OrderIdentity,
    pub(super) quantity: Quantity,
    /// The limit price, or a take profit's.
    pub(super) price: Option<Price>,
    /// The stop price, or a stop loss's.
    pub(super) trigger_price: Option<Price>,
    pub(super) venue_order_id: Option<VenueOrderId>,
    pub(super) accepted: bool,
    pub(super) closed: bool,
    pub(super) role: Role,
    /// The latest change of this protection asked of IC and not yet seen
    /// done, if any.
    pub(super) want: Option<Want>,
    /// Whether its own request (a new order or a close) may still wait in
    /// the session's queue.
    pub(super) queued: bool,
}

/// A change of a protection (a bracket's leg, or a position's stop loss
/// or take profit) asked of IC: until IC shows it done, each amendment of
/// the other protection carries it, since an amendment sets both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Want {
    /// Moved to this price.
    Move(Price),
    /// Removed: a cancel.
    Remove,
}

impl Want {
    /// Whether IC holding the protection at `price` (none: removed) shows
    /// this change done.
    fn done(self, price: Option<Price>) -> bool {
        match self {
            Self::Move(wanted) => price == Some(wanted),
            Self::Remove => price.is_none(),
        }
    }
}

/// What a tracked order is to a bracket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Role {
    /// An order of its own.
    Order,
    /// A bracket's entry, its legs, and the position it opened once filled.
    Entry {
        stop_loss: Option<ClientOrderId>,
        take_profit: Option<ClientOrderId>,
        position: Option<i64>,
    },
    /// A bracket's leg, of `entry`.
    Leg { entry: ClientOrderId, leg: Leg },
}

/// The trade events' state: the orders tracked, and the deals seen.
#[derive(Debug)]
pub(super) struct Dispatch {
    trader_id: TraderId,
    account_id: AccountId,
    currency: Currency,
    instruments: HashMap<i64, InstrumentAny>,
    orders: HashMap<ClientOrderId, Tracked>,
    deals: FifoCache<i64, DEALS_REMEMBERED>,
}

impl Dispatch {
    /// A dispatch for `account_id` of `trader_id`, its deposit currency
    /// `currency`, on the instruments loaded, by cTrader `symbolId`.
    pub(super) fn new(
        trader_id: TraderId,
        account_id: AccountId,
        currency: Currency,
        instruments: HashMap<i64, InstrumentAny>,
    ) -> Self {
        Self {
            trader_id,
            account_id,
            currency,
            instruments,
            orders: HashMap::new(),
            deals: FifoCache::new(),
        }
    }

    /// Tracks `order` as `role`, before its request goes out.
    pub(super) fn track(&mut self, order: &OrderAny, role: Role) {
        self.orders.insert(
            order.client_order_id(),
            Tracked {
                identity: OrderIdentity::from(order),
                quantity: order.quantity(),
                price: order.price(),
                trigger_price: order.trigger_price(),
                venue_order_id: order.venue_order_id(),
                accepted: order.venue_order_id().is_some(),
                closed: order.is_closed(),
                role,
                want: None,
                queued: true,
            },
        );
    }

    /// Tracks `order`, sent before a restart or placed elsewhere, as an
    /// order of its own, unless tracked: a refusal of a request about it
    /// then reaches it.
    pub(super) fn adopt(&mut self, order: &OrderAny) {
        let id = order.client_order_id();
        if !self.orders.contains_key(&id) {
            self.track(order, Role::Order);
            if let Some(tracked) = self.orders.get_mut(&id) {
                tracked.queued = false;
            }
        }
    }

    /// Notes that the request `tag` left the session's queue: IC holds a
    /// new order or a close from then on, if it got it.
    pub(super) fn sent(&mut self, tag: Tag) {
        if let Tag::New(id) | Tag::Close(id) = tag
            && let Some(tracked) = self.orders.get_mut(&id)
        {
            tracked.queued = false;
        }
    }

    /// The orders rejected once IC holds no order `id` over its window, a
    /// query found: `id`, and a bracket entry's open legs, now closed.
    /// `None`, nothing concluded, while its request may still wait in the
    /// session's queue (IC cannot hold it yet), for a bracket's leg (IC
    /// holds it on its entry or position, never as an order of its own), or
    /// for an order closed. An order not tracked was not sent by this
    /// session: nothing of it waits.
    pub(super) fn absent(&mut self, id: ClientOrderId) -> Option<Vec<ClientOrderId>> {
        let Some(tracked) = self.orders.get(&id) else {
            return Some(vec![id]);
        };
        if tracked.closed || tracked.queued || matches!(tracked.role, Role::Leg { .. }) {
            return None;
        }
        let mut ids = vec![id];
        ids.extend(
            self.legs(id)
                .into_iter()
                .map(|(_, leg)| leg)
                .filter(|leg| self.open(*leg).is_some()),
        );
        for id in &ids {
            if let Some(t) = self.orders.get_mut(id) {
                t.closed = true;
            }
        }
        Some(ids)
    }

    /// The tracked order `id`, open or not.
    pub(super) fn tracked(&self, id: ClientOrderId) -> Option<&Tracked> {
        self.orders.get(&id)
    }

    /// Records `want`, the latest change asked of the protection `id`.
    pub(super) fn want(&mut self, id: ClientOrderId, want: Want) {
        if let Some(tracked) = self.orders.get_mut(&id) {
            tracked.want = Some(want);
        }
    }

    /// The latest change asked of the protection `id` and not yet seen
    /// done.
    pub(super) fn wanted(&self, id: ClientOrderId) -> Option<Want> {
        self.orders.get(&id).and_then(|t| t.want)
    }

    /// The protections of the bracket `entry` as last asked: each open
    /// leg's wanted change, else its price; the stop loss and take profit
    /// an amendment carries.
    pub(super) fn protection(&self, entry: ClientOrderId) -> Protection {
        let Some(Tracked {
            role:
                Role::Entry {
                    stop_loss,
                    take_profit,
                    ..
                },
            ..
        }) = self.orders.get(&entry)
        else {
            return Protection::default();
        };
        let price = |leg: Option<ClientOrderId>, of: fn(&Tracked) -> Option<Price>| {
            let tracked = self.orders.get(&leg?).filter(|t| !t.closed)?;
            match tracked.want {
                Some(Want::Move(price)) => Some(price),
                Some(Want::Remove) => None,
                None => of(tracked),
            }
        };
        Protection {
            stop_loss: price(*stop_loss, |t| t.trigger_price),
            take_profit: price(*take_profit, |t| t.price),
        }
    }

    /// Forgets the changes of the protections of `position` that IC now
    /// holds at `(stop loss, take profit)`.
    fn settle(&mut self, position: i64, (stop_loss, take_profit): (Option<Price>, Option<Price>)) {
        for tracked in self.orders.values_mut() {
            let Some((of, leg)) = tracked.venue_order_id.and_then(parse_protection_id) else {
                continue;
            };
            let held = match leg {
                Leg::StopLoss => stop_loss,
                Leg::TakeProfit => take_profit,
            };
            if of == position && tracked.want.is_some_and(|w| w.done(held)) {
                tracked.want = None;
            }
        }
    }

    /// The outputs of `event`, received at `ts_init`.
    pub(super) fn handle(&mut self, event: TradeEvent, ts_init: UnixNanos) -> Vec<Output> {
        let id = event.client_msg_id.as_deref();
        match event.message {
            TradeMessage::Execution(execution) => self.execution(&execution, ts_init),
            TradeMessage::OrderError(error) => {
                let reason = reason(&error.error_code, error.description.as_deref());
                self.refused_by_id(id, &reason, ts_init)
            }
            TradeMessage::Refused(refused) => self.refused_by_id(id, &refused.to_string(), ts_init),
        }
    }

    fn refused_by_id(&mut self, id: Option<&str>, reason: &str, ts: UnixNanos) -> Vec<Output> {
        match id.map(str::parse::<Tag>) {
            Some(Ok(tag)) => self.refuse(tag, reason, ts),
            _ => {
                log::warn!("IC refused a request not of this client ({id:?}): {reason}");
                Vec::new()
            }
        }
    }

    /// The outputs of the request `tag` refused for `reason`, or never
    /// sent: a new order or a close rejected (a bracket entry's legs with
    /// it), an amendment's modification rejected, a cancellation's cancel
    /// rejected (a removal of protections: the cancel of each protection of
    /// the position being removed). The changes of protections it asked
    /// are forgotten.
    pub(super) fn refuse(&mut self, tag: Tag, reason: &str, ts: UnixNanos) -> Vec<Output> {
        let id = tag.client_order_id();
        let Some(tracked) = self.orders.get(&id).filter(|t| !t.closed).cloned() else {
            log::debug!("IC refused {tag} of an order no longer open: {reason}");
            return Vec::new();
        };
        let reason = Ustr::from(reason);
        match tag {
            Tag::New(_) | Tag::Close(_) => {
                let mut out = vec![self.rejected(id, &tracked, reason, ts)];
                if let Role::Entry {
                    stop_loss,
                    take_profit,
                    ..
                } = tracked.role
                {
                    for leg in [stop_loss, take_profit].into_iter().flatten() {
                        if let Some(leg_tracked) = self.open(leg) {
                            out.push(self.rejected(leg, &leg_tracked, reason, ts));
                        }
                    }
                }
                out
            }
            Tag::Amend(_) | Tag::Protect(_) => {
                if let Some(t) = self.orders.get_mut(&id) {
                    t.want = None;
                }
                vec![Output::Event(OrderEventAny::ModifyRejected(
                    OrderModifyRejected::new(
                        self.trader_id,
                        tracked.identity.strategy_id,
                        tracked.identity.instrument_id,
                        id,
                        reason,
                        UUID4::new(),
                        ts,
                        ts,
                        false,
                        tracked.venue_order_id,
                        Some(self.account_id),
                    ),
                ))]
            }
            Tag::Cancel(_) => vec![self.cancel_rejected(id, &tracked, reason, ts)],
            // One amendment removes every protection of a position being
            // cancelled.
            Tag::Unprotect(_) => {
                let position_of = |t: &Tracked| {
                    t.venue_order_id
                        .and_then(parse_protection_id)
                        .map(|(p, _)| p)
                };
                let position = position_of(&tracked);
                let others: Vec<(ClientOrderId, Tracked)> = self
                    .orders
                    .iter()
                    .filter(|(other, t)| {
                        **other != id
                            && !t.closed
                            && t.want == Some(Want::Remove)
                            && position.is_some()
                            && position_of(t) == position
                    })
                    .map(|(other, t)| (*other, t.clone()))
                    .collect();
                let mut out = vec![self.cancel_rejected(id, &tracked, reason, ts)];
                for (leg, leg_tracked) in others {
                    out.push(self.cancel_rejected(leg, &leg_tracked, reason, ts));
                }
                out
            }
        }
    }

    /// The rejection of the cancel of `id`, its wanted removal forgotten.
    fn cancel_rejected(
        &mut self,
        id: ClientOrderId,
        tracked: &Tracked,
        reason: Ustr,
        ts: UnixNanos,
    ) -> Output {
        if let Some(t) = self.orders.get_mut(&id) {
            t.want = None;
        }
        Output::Event(OrderEventAny::CancelRejected(OrderCancelRejected::new(
            self.trader_id,
            tracked.identity.strategy_id,
            tracked.identity.instrument_id,
            id,
            reason,
            UUID4::new(),
            ts,
            ts,
            false,
            tracked.venue_order_id,
            Some(self.account_id),
        )))
    }

    /// The open tracked order `id`.
    fn open(&self, id: ClientOrderId) -> Option<Tracked> {
        self.orders.get(&id).filter(|t| !t.closed).cloned()
    }

    fn execution(&mut self, event: &ProtoOaExecutionEvent, ts_init: UnixNanos) -> Vec<Output> {
        let Some(order) = &event.order else {
            log::debug!(
                "IC {} event without an order",
                event.execution_type().as_str_name()
            );
            return Vec::new();
        };
        let outputs = if order.order_type() == ProtoOaOrderType::StopLossTakeProfit {
            self.protection_event(event, order, ts_init)
        } else {
            match client_order_id(order).filter(|id| self.orders.contains_key(id)) {
                Some(id) => self.tracked_event(id, event, order, ts_init),
                None => self.external(event, order, ts_init),
            }
        };
        outputs.unwrap_or_else(|e| {
            log::error!(
                "IC {} of order {} left to reconciliation: {e:#}",
                event.execution_type().as_str_name(),
                order.order_id
            );
            Vec::new()
        })
    }

    /// Whether `deal` is new, remembering it.
    fn first_seen(&mut self, deal: &ProtoOaDeal) -> bool {
        self.deals.insert(deal.deal_id)
    }

    fn instrument(&self, symbol_id: i64) -> anyhow::Result<&InstrumentAny> {
        self.instruments
            .get(&symbol_id)
            .with_context(|| format!("symbol {symbol_id} is not an instrument of the run"))
    }

    fn tracked_event(
        &mut self,
        id: ClientOrderId,
        event: &ProtoOaExecutionEvent,
        order: &ProtoOaOrder,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<Output>> {
        let Some(tracked) = self.open(id) else {
            // Terminal: a new deal is late evidence, the rest a replay.
            return match event.deal.as_ref().filter(|d| reports::fills(d)) {
                Some(deal) if self.first_seen(deal) => self.external(event, order, ts_init),
                _ => Ok(Vec::new()),
            };
        };
        let venue = VenueOrderId::new(order.order_id.to_string());
        let ts = reports::millis(order.utc_last_update_timestamp).unwrap_or(ts_init);
        let mut out = Vec::new();
        match event.execution_type() {
            ProtoOaExecutionType::OrderAccepted => out.extend(self.accept(id, venue, ts)),
            ProtoOaExecutionType::OrderFilled | ProtoOaExecutionType::OrderPartialFill => {
                let Some(deal) = event.deal.as_ref().filter(|d| reports::fills(d)) else {
                    return Ok(out);
                };
                if !self.first_seen(deal) {
                    return Ok(out);
                }
                out.extend(self.accept(id, venue, ts));
                out.push(self.filled(id, &tracked, venue, deal, ts_init)?);
                let position = deal.position_id;
                if let Some(tracked) = self.orders.get_mut(&id) {
                    tracked.closed = order.order_status() == ProtoOaOrderStatus::OrderStatusFilled;
                    if let Role::Entry {
                        position: bound, ..
                    } = &mut tracked.role
                    {
                        *bound = Some(position);
                    }
                }
                if let Some(open) = event.position.as_ref().filter(|p| p.trade_data.volume > 0)
                    && order.closing_order == Some(true)
                {
                    out.extend(self.resize_legs(open.position_id, open.trade_data.volume, ts)?);
                }
            }
            ProtoOaExecutionType::OrderReplaced => {
                let instrument = self.instrument(order.trade_data.symbol_id)?.clone();
                let quantity = reports::quantity(&instrument, order.trade_data.volume)?;
                let price = order
                    .limit_price
                    .map(|p| reports::price(&instrument, p))
                    .transpose()?;
                let trigger = order
                    .stop_price
                    .map(|p| reports::price(&instrument, p))
                    .transpose()?;
                out.push(self.updated(id, &tracked, quantity, price, trigger, ts));
                if let Some(t) = self.orders.get_mut(&id) {
                    (t.quantity, t.price, t.trigger_price) = (quantity, price, trigger);
                }
                if let Role::Entry { .. } = tracked.role {
                    let protection = (order.stop_loss, order.take_profit);
                    out.extend(self.align_legs(id, None, protection, None, &instrument, ts)?);
                }
            }
            ProtoOaExecutionType::OrderCancelled | ProtoOaExecutionType::OrderExpired => {
                let expired = event.execution_type() == ProtoOaExecutionType::OrderExpired;
                out.push(self.closing(id, &tracked, Some(venue), expired, ts));
                out.extend(self.cancel_legs(id, ts));
            }
            ProtoOaExecutionType::OrderRejected => {
                let code = event.error_code.as_deref().unwrap_or("ORDER_REJECTED");
                out.extend(self.refuse(Tag::New(id), code, ts));
            }
            ProtoOaExecutionType::OrderCancelRejected => {
                let code = event
                    .error_code
                    .as_deref()
                    .unwrap_or("ORDER_CANCEL_REJECTED");
                out.extend(self.refuse(Tag::Cancel(id), code, ts));
            }
            other => log::debug!("IC {} of {id} ignored", other.as_str_name()),
        }
        Ok(out)
    }

    /// The acceptance of `id` as `venue`, unless already accepted.
    fn accept(&mut self, id: ClientOrderId, venue: VenueOrderId, ts: UnixNanos) -> Option<Output> {
        let tracked = self.orders.get_mut(&id)?;
        if tracked.accepted {
            return None;
        }
        tracked.accepted = true;
        tracked.venue_order_id = Some(venue);
        Some(Output::Event(OrderEventAny::Accepted(OrderAccepted::new(
            self.trader_id,
            tracked.identity.strategy_id,
            tracked.identity.instrument_id,
            id,
            venue,
            self.account_id,
            UUID4::new(),
            ts,
            ts,
            false,
        ))))
    }

    fn filled(
        &self,
        id: ClientOrderId,
        tracked: &Tracked,
        venue: VenueOrderId,
        deal: &ProtoOaDeal,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Output> {
        let instrument = self.instrument(deal.symbol_id)?;
        let what = || format!("IC deal {}", deal.deal_id);
        let price = deal
            .execution_price
            .context("a filled deal without an execution price")
            .and_then(|p| reports::price(instrument, p))
            .with_context(what)?;
        Ok(Output::Event(OrderEventAny::Filled(OrderFilled::new(
            self.trader_id,
            tracked.identity.strategy_id,
            tracked.identity.instrument_id,
            id,
            venue,
            self.account_id,
            TradeId::new(deal.deal_id.to_string()),
            tracked.identity.order_side,
            tracked.identity.order_type,
            reports::quantity(instrument, deal.filled_volume).with_context(what)?,
            price,
            instrument.quote_currency(),
            LiquiditySide::NoLiquiditySide,
            UUID4::new(),
            reports::millis(Some(deal.execution_timestamp)).with_context(what)?,
            ts_init,
            false,
            Some(reports::position_id(deal.position_id)),
            Some(reports::commission(deal, self.currency).with_context(what)?),
            None,
        ))))
    }

    fn updated(
        &self,
        id: ClientOrderId,
        tracked: &Tracked,
        quantity: Quantity,
        price: Option<Price>,
        trigger: Option<Price>,
        ts: UnixNanos,
    ) -> Output {
        Output::Event(OrderEventAny::Updated(OrderUpdated::new(
            self.trader_id,
            tracked.identity.strategy_id,
            tracked.identity.instrument_id,
            id,
            quantity,
            UUID4::new(),
            ts,
            ts,
            false,
            tracked.venue_order_id,
            Some(self.account_id),
            price,
            trigger,
            None,
            false,
        )))
    }

    fn rejected(
        &mut self,
        id: ClientOrderId,
        tracked: &Tracked,
        reason: Ustr,
        ts: UnixNanos,
    ) -> Output {
        if let Some(t) = self.orders.get_mut(&id) {
            t.closed = true;
        }
        Output::Event(OrderEventAny::Rejected(OrderRejected::new(
            self.trader_id,
            tracked.identity.strategy_id,
            tracked.identity.instrument_id,
            id,
            self.account_id,
            reason,
            UUID4::new(),
            ts,
            ts,
            false,
            false,
        )))
    }

    /// The cancellation, or expiry, of `id`, now closed, under its venue
    /// order id, else `venue`.
    fn closing(
        &mut self,
        id: ClientOrderId,
        tracked: &Tracked,
        venue: Option<VenueOrderId>,
        expired: bool,
        ts: UnixNanos,
    ) -> Output {
        if let Some(t) = self.orders.get_mut(&id) {
            t.closed = true;
        }
        let venue = tracked.venue_order_id.or(venue);
        Output::Event(if expired {
            OrderEventAny::Expired(OrderExpired::new(
                self.trader_id,
                tracked.identity.strategy_id,
                tracked.identity.instrument_id,
                id,
                UUID4::new(),
                ts,
                ts,
                false,
                venue,
                Some(self.account_id),
            ))
        } else {
            OrderEventAny::Canceled(OrderCanceled::new(
                self.trader_id,
                tracked.identity.strategy_id,
                tracked.identity.instrument_id,
                id,
                UUID4::new(),
                ts,
                ts,
                false,
                venue,
                Some(self.account_id),
                None,
            ))
        })
    }

    /// The legs of bracket `entry`, by kind.
    fn legs(&self, entry: ClientOrderId) -> Vec<(Leg, ClientOrderId)> {
        match self.orders.get(&entry).map(|t| t.role) {
            Some(Role::Entry {
                stop_loss,
                take_profit,
                ..
            }) => [(Leg::StopLoss, stop_loss), (Leg::TakeProfit, take_profit)]
                .into_iter()
                .filter_map(|(leg, id)| Some((leg, id?)))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// The cancellations of the open legs of bracket `entry`.
    fn cancel_legs(&mut self, entry: ClientOrderId, ts: UnixNanos) -> Vec<Output> {
        let mut out = Vec::new();
        for (_, leg) in self.legs(entry) {
            if let Some(tracked) = self.open(leg) {
                out.push(self.closing(leg, &tracked, None, false, ts));
            }
        }
        out
    }

    /// The bracket entry whose position is `position`.
    fn entry_of_position(&self, position: i64) -> Option<ClientOrderId> {
        self.orders.iter().find_map(|(id, t)| match t.role {
            Role::Entry {
                position: Some(p), ..
            } if p == position => Some(*id),
            _ => None,
        })
    }

    /// The updates of the accepted legs of position `position` to its
    /// remaining `volume`, after a partial close.
    fn resize_legs(
        &mut self,
        position: i64,
        volume: i64,
        ts: UnixNanos,
    ) -> anyhow::Result<Vec<Output>> {
        let Some(entry) = self.entry_of_position(position) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for (_, leg) in self.legs(entry) {
            let Some(tracked) = self.open(leg).filter(|t| t.accepted) else {
                continue;
            };
            let instrument = self
                .instruments
                .values()
                .find(|i| i.id() == tracked.identity.instrument_id);
            let instrument = instrument.context("a leg of an instrument not loaded")?;
            let quantity = reports::quantity(instrument, volume)?;
            if quantity != tracked.quantity {
                out.push(self.updated(
                    leg,
                    &tracked,
                    quantity,
                    tracked.price,
                    tracked.trigger_price,
                    ts,
                ));
                if let Some(t) = self.orders.get_mut(&leg) {
                    t.quantity = quantity;
                }
            }
        }
        Ok(out)
    }

    /// Brings the legs of bracket `entry` in line with the protection
    /// cTrader holds, `(stop loss, take profit)`: a leg whose price came is
    /// accepted (as `accept_on`'s leg, once the position holds it) or
    /// updated, a leg whose price went cancelled.
    fn align_legs(
        &mut self,
        entry: ClientOrderId,
        accept_on: Option<i64>,
        (stop_loss, take_profit): (Option<f64>, Option<f64>),
        quantity: Option<Quantity>,
        instrument: &InstrumentAny,
        ts: UnixNanos,
    ) -> anyhow::Result<Vec<Output>> {
        let mut out = Vec::new();
        for (leg, id) in self.legs(entry) {
            let Some(tracked) = self.open(id) else {
                continue;
            };
            let now = match leg {
                Leg::StopLoss => stop_loss,
                Leg::TakeProfit => take_profit,
            };
            let now = now
                .filter(|p| *p > 0.0)
                .map(|p| reports::price(instrument, p))
                .transpose()?;
            if tracked.want.is_some_and(|w| w.done(now))
                && let Some(t) = self.orders.get_mut(&id)
            {
                t.want = None;
            }
            let held = match leg {
                Leg::StopLoss => tracked.trigger_price,
                Leg::TakeProfit => tracked.price,
            };
            let Some(price) = now else {
                if tracked.accepted || accept_on.is_none() {
                    out.push(self.closing(id, &tracked, None, false, ts));
                }
                continue;
            };
            if let Some(position) = accept_on
                && !tracked.accepted
            {
                out.extend(self.accept(id, protection_id(position, leg), ts));
            }
            let quantity = quantity.unwrap_or(tracked.quantity);
            if held != Some(price) || quantity != tracked.quantity {
                let (price_now, trigger_now) = match leg {
                    Leg::StopLoss => (None, Some(price)),
                    Leg::TakeProfit => (Some(price), None),
                };
                let current = self.orders[&id].clone();
                out.push(self.updated(id, &current, quantity, price_now, trigger_now, ts));
                if let Some(t) = self.orders.get_mut(&id) {
                    t.quantity = quantity;
                    match leg {
                        Leg::StopLoss => t.trigger_price = Some(price),
                        Leg::TakeProfit => t.price = Some(price),
                    }
                }
            }
        }
        Ok(out)
    }

    /// An event of a position's `STOP_LOSS_TAKE_PROFIT` order.
    fn protection_event(
        &mut self,
        event: &ProtoOaExecutionEvent,
        order: &ProtoOaOrder,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<Output>> {
        let position = order
            .position_id
            .context("a protection order without a position")?;
        let entry = client_order_id(order)
            .filter(|id| {
                matches!(
                    self.orders.get(id).map(|t| t.role),
                    Some(Role::Entry { .. })
                )
            })
            .or_else(|| self.entry_of_position(position));
        let Some(entry) = entry else {
            return self.external_protection(event, order, ts_init);
        };
        let instrument = self.instrument(order.trade_data.symbol_id)?.clone();
        let ts = reports::millis(order.utc_last_update_timestamp).unwrap_or(ts_init);
        let prices = (order.stop_price, order.limit_price);
        match event.execution_type() {
            ProtoOaExecutionType::OrderAccepted | ProtoOaExecutionType::OrderReplaced => {
                let quantity = reports::quantity(&instrument, order.trade_data.volume)?;
                self.align_legs(
                    entry,
                    Some(position),
                    prices,
                    Some(quantity),
                    &instrument,
                    ts,
                )
            }
            ProtoOaExecutionType::OrderFilled | ProtoOaExecutionType::OrderPartialFill => {
                let Some(deal) = event.deal.as_ref().filter(|d| reports::fills(d)) else {
                    return Ok(Vec::new());
                };
                if !self.first_seen(deal) {
                    return Ok(Vec::new());
                }
                let hit = triggered(order, deal);
                let legs = self.legs(entry);
                let mut out = Vec::new();
                // The leg hit fills first, then the other is cancelled.
                for &(leg, id) in legs.iter().filter(|(leg, _)| *leg == hit) {
                    let Some(tracked) = self.open(id) else {
                        continue;
                    };
                    let venue = protection_id(position, leg);
                    out.extend(self.accept(id, venue, ts));
                    out.push(self.filled(id, &tracked, venue, deal, ts_init)?);
                    if let Some(t) = self.orders.get_mut(&id) {
                        t.closed = order.order_status() == ProtoOaOrderStatus::OrderStatusFilled;
                    }
                }
                for &(leg, id) in legs.iter().filter(|(leg, _)| *leg != hit) {
                    if let Some(tracked) = self.open(id) {
                        let venue = protection_id(position, leg);
                        out.push(self.closing(id, &tracked, Some(venue), false, ts));
                    }
                }
                Ok(out)
            }
            ProtoOaExecutionType::OrderCancelled | ProtoOaExecutionType::OrderExpired => {
                Ok(self.cancel_legs(entry, ts))
            }
            other => {
                log::debug!(
                    "IC {} of the protection of {position} ignored",
                    other.as_str_name()
                );
                Ok(Vec::new())
            }
        }
    }

    fn reports(&self, ts_init: UnixNanos) -> Reports<'_> {
        Reports {
            account_id: self.account_id,
            instruments: &self.instruments,
            currency: self.currency,
            ts_init,
        }
    }

    /// The reports of an event of an order not tracked: its status, with
    /// the deal's fill.
    fn external(
        &mut self,
        event: &ProtoOaExecutionEvent,
        order: &ProtoOaOrder,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<Output>> {
        if let Some(deal) = &event.deal
            && reports::fills(deal)
        {
            self.deals.add(deal.deal_id);
        }
        let reports = self.reports(ts_init);
        let Some(status) = reports.order(order)? else {
            return Ok(Vec::new());
        };
        let fills: Vec<_> = event
            .deal
            .iter()
            .filter_map(|deal| reports.fill(deal).transpose())
            .collect::<anyhow::Result<_>>()?;
        let report = if fills.is_empty() {
            ExecutionReport::Order(Box::new(status))
        } else {
            ExecutionReport::OrderWithFills(Box::new(status), fills)
        };
        Ok(vec![Output::Report(report)])
    }

    /// The reports of an event of a protection not tracked: the legs
    /// cTrader holds, the leg filled, or the legs cancelled.
    fn external_protection(
        &mut self,
        event: &ProtoOaExecutionEvent,
        order: &ProtoOaOrder,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<Output>> {
        let reports = self.reports(ts_init);
        let out = match event.execution_type() {
            ProtoOaExecutionType::OrderAccepted | ProtoOaExecutionType::OrderReplaced => {
                let Some(position) = &event.position else {
                    return Ok(Vec::new());
                };
                let legs = reports.protections(position)?;
                let held = |leg: Leg| {
                    let venue = protection_id(position.position_id, leg);
                    legs.iter()
                        .find(|r| r.venue_order_id == venue)
                        .and_then(|r| match leg {
                            Leg::StopLoss => r.trigger_price,
                            Leg::TakeProfit => r.price,
                        })
                };
                let prices = (held(Leg::StopLoss), held(Leg::TakeProfit));
                let out = legs
                    .into_iter()
                    .map(|r| Output::Report(ExecutionReport::Order(Box::new(r))))
                    .collect();
                self.settle(position.position_id, prices);
                out
            }
            ProtoOaExecutionType::OrderFilled | ProtoOaExecutionType::OrderPartialFill => {
                let Some(deal) = event.deal.as_ref().filter(|d| reports::fills(d)) else {
                    return Ok(Vec::new());
                };
                let hit = triggered(order, deal);
                let mut out = Vec::new();
                if let Some((status, fill)) = reports.protection_fill(order, deal, hit)? {
                    out.push(Output::Report(ExecutionReport::OrderWithFills(
                        Box::new(status),
                        vec![fill],
                    )));
                }
                let other = match hit {
                    Leg::StopLoss => Leg::TakeProfit,
                    Leg::TakeProfit => Leg::StopLoss,
                };
                out.extend(
                    reports
                        .protection_status(order, other, OrderStatus::Canceled)?
                        .map(|r| Output::Report(ExecutionReport::Order(Box::new(r)))),
                );
                self.deals.add(deal.deal_id);
                out
            }
            ProtoOaExecutionType::OrderCancelled | ProtoOaExecutionType::OrderExpired => {
                let mut out = Vec::new();
                for leg in [Leg::StopLoss, Leg::TakeProfit] {
                    out.extend(
                        reports
                            .protection_status(order, leg, OrderStatus::Canceled)?
                            .map(|r| Output::Report(ExecutionReport::Order(Box::new(r)))),
                    );
                }
                out
            }
            _ => Vec::new(),
        };
        Ok(out)
    }
}

/// The Nautilus order an order of cTrader's is: its `clientOrderId`, or for
/// a closing order the close request's tag cTrader made it.
fn client_order_id(order: &ProtoOaOrder) -> Option<ClientOrderId> {
    let raw = order
        .client_order_id
        .as_deref()
        .filter(|id| !id.is_empty())?;
    match raw.parse::<Tag>() {
        Ok(Tag::Close(id)) => Some(id),
        _ => ClientOrderId::new_checked(raw).ok(),
    }
}

/// Which leg of a protection order filled `deal`: the one set alone, or
/// the one whose price is nearer the deal's.
fn triggered(order: &ProtoOaOrder, deal: &ProtoOaDeal) -> Leg {
    let price = deal.execution_price.unwrap_or_default();
    match (
        order.stop_price.filter(|p| *p > 0.0),
        order.limit_price.filter(|p| *p > 0.0),
    ) {
        (Some(stop), Some(limit)) if (limit - price).abs() < (stop - price).abs() => {
            Leg::TakeProfit
        }
        (None, Some(_)) => Leg::TakeProfit,
        _ => Leg::StopLoss,
    }
}

/// A rejection's reason: cTrader's code, and its description if any.
fn reason(code: &str, description: Option<&str>) -> String {
    match description.filter(|d| !d.is_empty()) {
        Some(description) => format!("{code}: {description}"),
        None => code.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType},
        identifiers::InstrumentId,
        orders::OrderTestBuilder,
        types::Quantity,
    };

    use super::*;

    fn dispatch() -> Dispatch {
        Dispatch::new(
            TraderId::from("HELIX-001"),
            AccountId::from("ICMARKETS-DEMO"),
            Currency::from("EUR"),
            HashMap::new(),
        )
    }

    fn order(id: &str, kind: OrderType) -> OrderAny {
        let mut builder = OrderTestBuilder::new(kind);
        builder
            .instrument_id(InstrumentId::from("XAUUSD.ICMARKETS"))
            .client_order_id(ClientOrderId::from(id))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1));
        match kind {
            OrderType::StopMarket => builder.trigger_price(Price::from("3990.00")),
            OrderType::Limit => builder.price(Price::from("4050.00")),
            _ => &mut builder,
        };
        builder.build()
    }

    fn id(id: &str) -> ClientOrderId {
        ClientOrderId::from(id)
    }

    #[test]
    fn an_order_whose_request_may_still_wait_to_go_out_is_not_concluded_absent() {
        let mut dispatch = dispatch();
        dispatch.track(&order("O-1", OrderType::Market), Role::Order);

        assert_eq!(dispatch.absent(id("O-1")), None);
        dispatch.sent(Tag::New(id("O-1")));
        assert_eq!(dispatch.absent(id("O-1")), Some(vec![id("O-1")]));
    }

    #[test]
    fn an_order_this_session_never_sent_is_concluded_absent() {
        let mut dispatch = dispatch();

        assert_eq!(dispatch.absent(id("O-1")), Some(vec![id("O-1")]));
    }

    #[test]
    fn an_absent_entry_takes_its_legs_and_a_leg_alone_is_not_concluded() {
        let mut dispatch = dispatch();
        dispatch.track(
            &order("O-1-SL", OrderType::StopMarket),
            Role::Leg {
                entry: id("O-1"),
                leg: Leg::StopLoss,
            },
        );
        dispatch.track(
            &order("O-1-TP", OrderType::Limit),
            Role::Leg {
                entry: id("O-1"),
                leg: Leg::TakeProfit,
            },
        );
        dispatch.track(
            &order("O-1", OrderType::Market),
            Role::Entry {
                stop_loss: Some(id("O-1-SL")),
                take_profit: Some(id("O-1-TP")),
                position: None,
            },
        );
        dispatch.sent(Tag::New(id("O-1")));

        assert_eq!(dispatch.absent(id("O-1-SL")), None);
        assert_eq!(
            dispatch.absent(id("O-1")),
            Some(vec![id("O-1"), id("O-1-SL"), id("O-1-TP")])
        );
        // Concluded: closed, so not again.
        assert_eq!(dispatch.absent(id("O-1")), None);
    }
}
