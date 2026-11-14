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
//! - **Failures**: each failure of a request, under its `clientMsgId`
//!   (`Tag`), is one of Nautilus' `CommandFailure` classes (see
//!   `failure`), which takes Nautilus' policy (`fail`). A request never
//!   sent rejects a new order or a close, and the legs of a bracket entry;
//!   a modify or cancel never sent is denied, by its command. cTrader's
//!   refusal (an order error, an error answer, an order's
//!   `ORDER_REJECTED` or `ORDER_CANCEL_REJECTED`) rejects a new order or a
//!   close as well; rejects the modification of an amendment; rejects the
//!   cancel of a cancellation, of each protection of the position for a
//!   removal of protections; its reason is cTrader's code and description.
//!   A failure of unknown outcome emits nothing: the order stays in
//!   flight for its events, a query or reconciliation.
//! - **Protections asked**: a move or removal of a protection is recorded
//!   (`want`) until cTrader's protection shows it done, or it is refused:
//!   an amendment, which sets both protections, carries the other's.
//!
//! A terminal order's repeated events are suppressed; a fill with a new
//! `dealId` after it goes as a report. A deal is remembered only once its
//! outputs are built: one that cannot be (its commission not stated, see
//! `reports::commission`) emits nothing and changes nothing, and its replay
//! or reconciliation applies it once (Nautilus' `adapters.md`, "WebSocket
//! trade processing").

use std::collections::HashMap;

use anyhow::Context;
use nautilus_common::{cache::fifo::FifoCache, messages::ExecutionReport};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::execution::{context::OrderIdentity, failure::CommandFailure};
use nautilus_model::{
    enums::LiquiditySide,
    events::{
        OrderAccepted, OrderCancelRejected, OrderCanceled, OrderEventAny, OrderExpired,
        OrderFilled, OrderModifyRejected, OrderRejected, OrderUpdated,
    },
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Price, Quantity},
};
use ustr::Ustr;

use super::{
    failure,
    orders::{Leg, Protection, Tag, parse_protection_id, protection_id},
    reports::{self, Reports},
};
use crate::{
    common::messages::{
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
    Denied(Denial),
}

/// A modify or cancel of an order refused before it was sent: Nautilus'
/// `OrderModifyDenied` or `OrderCancelDenied`, caused by `command_id`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Denial {
    pub(super) command: Command,
    pub(super) strategy_id: StrategyId,
    pub(super) instrument_id: InstrumentId,
    pub(super) client_order_id: ClientOrderId,
    pub(super) venue_order_id: Option<VenueOrderId>,
    pub(super) reason: Ustr,
    pub(super) ts_event: UnixNanos,
    pub(super) command_id: UUID4,
}

/// The kind of command a denial refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Command {
    Modify,
    Cancel,
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
    /// The latest change of this protection asked of the broker and not yet
    /// seen done, if any.
    pub(super) want: Option<Want>,
    /// Whether its own request (a new order or a close) may still wait in
    /// the session's queue.
    pub(super) queued: bool,
}

/// A change of a protection (a bracket's leg, or a position's stop loss or
/// take profit) asked of the broker: until the broker shows it done, each
/// amendment of the other protection carries it, since an amendment sets
/// both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Want {
    /// Moved to this price.
    Move(Price),
    /// Removed: a cancel.
    Remove,
}

impl Want {
    /// Whether the broker holding the protection at `price` (none: removed)
    /// shows this change done.
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

    /// Notes that the request `tag` left the session's queue: the broker
    /// holds a new order or a close from then on, if it got it.
    fn sent(&mut self, tag: Tag) {
        if let Tag::New(id) | Tag::Close(id) = tag
            && let Some(tracked) = self.orders.get_mut(&id)
        {
            tracked.queued = false;
        }
    }

    /// The orders rejected once the broker holds no order `id` over its
    /// window, a query found: `id`, and a bracket entry's open legs, now
    /// closed. `None`, nothing concluded, while its request may still wait
    /// in the session's queue (the broker cannot hold it yet), for a
    /// bracket's leg (the broker holds it on its entry or position, never
    /// as an order of its own), or for an order closed. An order not
    /// tracked was not sent by this session: nothing of it waits.
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

    /// Forgets the changes of the protections of `position` that the broker
    /// now holds at `(stop loss, take profit)`.
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

    /// The outputs of `event`, received at `ts_init`: an execution event,
    /// or cTrader's refusal of a request of this client (see `failure`).
    pub(super) fn handle(&mut self, event: TradeEvent, ts_init: UnixNanos) -> Vec<Output> {
        let failure = match event.message {
            TradeMessage::Execution(execution) => return self.execution(&execution, ts_init),
            TradeMessage::OrderError(error) => {
                failure::refusal(&error.error_code, error.description.as_deref())
            }
            TradeMessage::Refused(error) => failure::answer(&error),
        };
        let id = event.client_msg_id.as_deref();
        match id.map(str::parse::<Tag>) {
            Some(Ok(tag)) => self.fail(tag, &[], failure, ts_init),
            _ => {
                let (CommandFailure::NotSent(reason)
                | CommandFailure::Ambiguous(reason)
                | CommandFailure::VenueRejected(reason)) = &failure;
                log::warn!(
                    "cTrader refused a request not of this client ({id:?}): {}",
                    interned(reason)
                );
                Vec::new()
            }
        }
    }

    /// The outputs of `failure` of the request `tag`, by Nautilus' policy
    /// for its class (`developer_guide/adapters.md`, "Naming the evidence
    /// classes"; see `failure`). `commands` are each order's modify or
    /// cancel the request carries, with its command's id: none for a new
    /// order or a close, nor from cTrader's answer, which never says a
    /// request was not sent.
    ///
    /// - `NotSent`: a new order or a close rejected, a bracket entry's legs
    ///   with it; a modify (an amendment) or a cancel (a cancellation, a
    ///   removal of protections) denied by its command, for each order
    ///   still open, its wanted change forgotten.
    /// - `VenueRejected`: a new order or a close rejected as above; an
    ///   amendment's modification rejected; a cancellation's cancel
    ///   rejected, a removal of protections' that of each protection of the
    ///   position being removed. A modified or cancelled order stays
    ///   working, its wanted change forgotten.
    /// - `Ambiguous`: nothing: the trade events, a query or reconciliation
    ///   resolve it. A wanted change stays: the broker may have made it.
    pub(super) fn fail(
        &mut self,
        tag: Tag,
        commands: &[(ClientOrderId, UUID4)],
        failure: CommandFailure,
        ts: UnixNanos,
    ) -> Vec<Output> {
        match failure {
            CommandFailure::NotSent(reason) => {
                let reason = interned(&reason);
                log::warn!("{tag} not sent: {reason}");
                match tag {
                    Tag::New(_) | Tag::Close(_) => self.rejections(tag, reason, ts),
                    Tag::Amend(_) | Tag::Protect(_) => {
                        self.denials(Command::Modify, commands, reason, ts)
                    }
                    Tag::Cancel(_) | Tag::Unprotect(_) => {
                        self.denials(Command::Cancel, commands, reason, ts)
                    }
                }
            }
            CommandFailure::VenueRejected(reason) => self.rejections(tag, interned(&reason), ts),
            CommandFailure::Ambiguous(reason) => {
                log::warn!(
                    "{tag} has an unknown outcome, left to its events, a query or \
                     reconciliation: {}",
                    interned(&reason)
                );
                Vec::new()
            }
        }
    }

    /// The outputs of what became of the request `tag`'s frame, `sending`
    /// (see `failure::sending`), for `commands` (see `fail`). Unless never
    /// sent, nothing of it waits in the session's queue any more: the
    /// broker holds a new order or a close from then on, if it got it.
    pub(super) fn sending(
        &mut self,
        tag: Tag,
        commands: &[(ClientOrderId, UUID4)],
        sending: Result<(), CommandFailure>,
        ts: UnixNanos,
    ) -> Vec<Output> {
        if !matches!(sending, Err(CommandFailure::NotSent(_))) {
            self.sent(tag);
        }
        match sending {
            Ok(()) => Vec::new(),
            Err(failure) => self.fail(tag, commands, failure, ts),
        }
    }

    /// The denials of `command` for `commands` (see `fail`), of each order
    /// still open, its wanted change forgotten.
    fn denials(
        &mut self,
        command: Command,
        commands: &[(ClientOrderId, UUID4)],
        reason: Ustr,
        ts: UnixNanos,
    ) -> Vec<Output> {
        commands
            .iter()
            .filter_map(|&(id, command_id)| {
                let tracked = self.orders.get_mut(&id).filter(|t| !t.closed)?;
                tracked.want = None;
                Some(Output::Denied(Denial {
                    command,
                    strategy_id: tracked.identity.strategy_id,
                    instrument_id: tracked.identity.instrument_id,
                    client_order_id: id,
                    venue_order_id: tracked.venue_order_id,
                    reason,
                    ts_event: ts,
                    command_id,
                }))
            })
            .collect()
    }

    /// The rejections of the request `tag` (see `fail`): a new order or a
    /// close rejected (a bracket entry's legs with it), an amendment's
    /// modification rejected, a cancellation's cancel rejected (a removal
    /// of protections: the cancel of each protection of the position being
    /// removed). The changes of protections it asked are forgotten.
    fn rejections(&mut self, tag: Tag, reason: Ustr, ts: UnixNanos) -> Vec<Output> {
        let id = tag.client_order_id();
        let Some(tracked) = self.orders.get(&id).filter(|t| !t.closed).cloned() else {
            log::debug!("{tag} of an order no longer open failed: {reason}");
            return Vec::new();
        };
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
                "cTrader {} event without an order",
                event.execution_type().as_str_name()
            );
            return Vec::new();
        };
        let outputs = if order.order_type() == ProtoOaOrderType::StopLossTakeProfit {
            self.protection_event(event, order, ts_init)
        } else {
            match reports::client_order_id(order).filter(|id| self.orders.contains_key(id)) {
                Some(id) => self.tracked_event(id, event, order, ts_init),
                None => self.external(event, order, ts_init),
            }
        };
        outputs.unwrap_or_else(|e| {
            log::error!(
                "cTrader {} of order {} left to reconciliation: {e:#}",
                event.execution_type().as_str_name(),
                order.order_id
            );
            Vec::new()
        })
    }

    /// Whether `deal` was applied already. A deal is remembered once its
    /// outputs are built, so one that fails is applied by a replay.
    fn seen(&self, deal: &ProtoOaDeal) -> bool {
        self.deals.contains(&deal.deal_id)
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
                Some(deal) if !self.seen(deal) => self.external(event, order, ts_init),
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
                if self.seen(deal) {
                    return Ok(out);
                }
                // Built before anything changes: a fill that cannot be
                // leaves the order and the deal as they were.
                let fill = self.filled(id, &tracked, venue, deal, ts_init)?;
                out.extend(self.accept(id, venue, ts));
                out.push(fill);
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
                self.deals.add(deal.deal_id);
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
                let failure = CommandFailure::venue_rejected(code);
                out.extend(self.fail(Tag::New(id), &[], failure, ts));
            }
            ProtoOaExecutionType::OrderCancelRejected => {
                let code = event
                    .error_code
                    .as_deref()
                    .unwrap_or("ORDER_CANCEL_REJECTED");
                let failure = CommandFailure::venue_rejected(code);
                out.extend(self.fail(Tag::Cancel(id), &[], failure, ts));
            }
            other => log::debug!("cTrader {} of {id} ignored", other.as_str_name()),
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
        let what = || format!("cTrader deal {}", deal.deal_id);
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
        let entry = reports::client_order_id(order)
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
                if self.seen(deal) {
                    return Ok(Vec::new());
                }
                let hit = reports::triggered(order, deal);
                let legs = self.legs(entry);
                // The fill of the leg hit, built before anything changes.
                let mut fills = Vec::new();
                for &(leg, id) in legs.iter().filter(|(leg, _)| *leg == hit) {
                    let Some(tracked) = self.open(id) else {
                        continue;
                    };
                    let venue = protection_id(position, leg);
                    fills.push((id, venue, self.filled(id, &tracked, venue, deal, ts_init)?));
                }
                let mut out = Vec::new();
                // The leg hit fills first, then the other is cancelled.
                for (id, venue, fill) in fills {
                    out.extend(self.accept(id, venue, ts));
                    out.push(fill);
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
                self.deals.add(deal.deal_id);
                Ok(out)
            }
            ProtoOaExecutionType::OrderCancelled | ProtoOaExecutionType::OrderExpired => {
                Ok(self.cancel_legs(entry, ts))
            }
            other => {
                log::debug!(
                    "cTrader {} of the protection of {position} ignored",
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
        let reports = self.reports(ts_init);
        let status = reports.order(order)?;
        let fills: Vec<_> = event
            .deal
            .iter()
            .filter_map(|deal| reports.fill(deal).transpose())
            .collect::<anyhow::Result<_>>()?;
        if let Some(deal) = event.deal.as_ref().filter(|d| reports::fills(d)) {
            self.deals.add(deal.deal_id);
        }
        let Some(status) = status else {
            return Ok(Vec::new());
        };
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
                let out = legs_closed(reports.protection_closed(order, Some(deal))?);
                self.deals.add(deal.deal_id);
                out
            }
            ProtoOaExecutionType::OrderCancelled | ProtoOaExecutionType::OrderExpired => {
                legs_closed(reports.protection_closed(order, None)?)
            }
            _ => Vec::new(),
        };
        Ok(out)
    }
}

/// The reports of a protection's legs closed (see
/// `Reports::protection_closed`), a filled leg's with its fill.
fn legs_closed(legs: Vec<(OrderStatusReport, Option<FillReport>)>) -> Vec<Output> {
    legs.into_iter()
        .map(|(status, fill)| {
            Output::Report(match fill {
                Some(fill) => ExecutionReport::OrderWithFills(Box::new(status), vec![fill]),
                None => ExecutionReport::Order(Box::new(status)),
            })
        })
        .collect()
}

/// The most characters of a reason an order event carries.
const REASON_MAX_CHARS: usize = 256;

/// The reason of a refusal whose text is empty once cleaned.
const NO_REASON: &str = "no reason given";

/// `text`, a refusal's reason, as an order event carries it, interned for
/// good: each run of control characters and whitespace one space,
/// `REASON_MAX_CHARS` characters at most, `NO_REASON` if nothing is left,
/// as Nautilus asks of a venue's text before it is logged or interned
/// (`developer_guide/adapters.md`, "Separate diagnostics from
/// strategy-facing reasons").
fn interned(text: &str) -> Ustr {
    let words: Vec<&str> = text
        .split(|c: char| c.is_control() || c.is_whitespace())
        .filter(|word| !word.is_empty())
        .collect();
    let bounded: String = words.join(" ").chars().take(REASON_MAX_CHARS).collect();
    match bounded.trim_end() {
        "" => Ustr::from(NO_REASON),
        reason => Ustr::from(reason),
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType},
        identifiers::{InstrumentId, Venue},
        orders::OrderTestBuilder,
        types::{Money, Quantity},
    };
    use prost::Message as _;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        common::{
            messages::{
                CTraderError, ProtoOaAssetListRes, ProtoOaOrderErrorEvent, ProtoOaSymbolByIdRes,
                ProtoOaSymbolsListRes,
            },
            parse,
        },
        testing::{
            ASSET_LIST_RES, EXECUTION_LIMIT_ACCEPTED, EXECUTION_LIMIT_CANCELLED,
            EXECUTION_MARKET_FILLED, EXECUTION_STOP_LOSS_FILLED, ORDER_ERROR_BAD_VOLUME,
            SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES, VENUE,
        },
    };

    /// XAUUSD's cTrader symbol id on the server the fixtures come from.
    const GOLD: i64 = 41;

    /// XAUUSD on the made-up venue, from the captured symbol, its details
    /// and the assets.
    fn gold() -> InstrumentAny {
        let light = ProtoOaSymbolsListRes::decode(SYMBOLS_LIST_RES)
            .unwrap()
            .symbol
            .into_iter()
            .find(|s| s.symbol_id == GOLD)
            .unwrap();
        let details = ProtoOaSymbolByIdRes::decode(SYMBOL_BY_ID_RES)
            .unwrap()
            .symbol
            .into_iter()
            .find(|s| s.symbol_id == GOLD)
            .unwrap();
        let assets =
            parse::assets_by_id(ProtoOaAssetListRes::decode(ASSET_LIST_RES).unwrap().asset);
        InstrumentAny::CurrencyPair(
            parse::currency_pair(
                &light,
                &details,
                Venue::from(VENUE),
                &assets,
                Decimal::new(5, 2),
                UnixNanos::default(),
            )
            .unwrap(),
        )
    }

    /// The dispatch of the account the fixtures come from, in EUR, on
    /// XAUUSD.
    fn dispatch() -> Dispatch {
        Dispatch::new(
            TraderId::from("HELIX-001"),
            AccountId::from("ACMEFX-DEMO"),
            Currency::from("EUR"),
            HashMap::from([(GOLD, gold())]),
        )
    }

    fn order(id: &str, kind: OrderType) -> OrderAny {
        let mut builder = OrderTestBuilder::new(kind);
        builder
            .instrument_id(InstrumentId::from("XAUUSD.ACMEFX"))
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

    const TS: u64 = 1_793_027_466_300_000_000;

    fn ts() -> UnixNanos {
        UnixNanos::from(TS)
    }

    /// cTrader's `message` under the request `tag`'s `clientMsgId`.
    fn answering(tag: Tag, message: TradeMessage) -> TradeEvent {
        TradeEvent {
            client_msg_id: Some(tag.to_string()),
            message,
        }
    }

    fn execution(bytes: &[u8]) -> TradeMessage {
        TradeMessage::Execution(ProtoOaExecutionEvent::decode(bytes).unwrap())
    }

    /// The captured fill `bytes`, its deal's commission not stated.
    fn without_commission(bytes: &[u8]) -> TradeMessage {
        let mut execution = ProtoOaExecutionEvent::decode(bytes).unwrap();
        execution.deal.as_mut().unwrap().commission = None;
        TradeMessage::Execution(execution)
    }

    /// cTrader's error answer `code`.
    fn refused(code: &str, description: Option<&str>) -> TradeMessage {
        TradeMessage::Refused(CTraderError::Venue {
            code: code.to_owned(),
            description: description.map(str::to_owned),
        })
    }

    /// Each output's order, and what it is.
    fn kinds(outputs: &[Output]) -> Vec<(String, &'static str)> {
        outputs
            .iter()
            .map(|output| match output {
                Output::Event(event) => (
                    event.client_order_id().to_string(),
                    match event {
                        OrderEventAny::Accepted(_) => "accepted",
                        OrderEventAny::Filled(_) => "filled",
                        OrderEventAny::Rejected(_) => "rejected",
                        OrderEventAny::ModifyRejected(_) => "modify rejected",
                        OrderEventAny::CancelRejected(_) => "cancel rejected",
                        OrderEventAny::Canceled(_) => "canceled",
                        OrderEventAny::Updated(_) => "updated",
                        _ => "other event",
                    },
                ),
                Output::Denied(denial) => (
                    denial.client_order_id.to_string(),
                    match denial.command {
                        Command::Modify => "modify denied",
                        Command::Cancel => "cancel denied",
                    },
                ),
                Output::Report(report) => (
                    match report {
                        ExecutionReport::Order(r) | ExecutionReport::OrderWithFills(r, _) => {
                            r.venue_order_id.to_string()
                        }
                        _ => String::new(),
                    },
                    "report",
                ),
            })
            .collect()
    }

    fn row(id: &str, kind: &'static str) -> (String, &'static str) {
        (id.to_owned(), kind)
    }

    /// The reason of the one rejection, of any kind, in `outputs`.
    fn reason(outputs: &[Output]) -> String {
        match outputs {
            [Output::Event(OrderEventAny::Rejected(e))] => e.reason.to_string(),
            [Output::Event(OrderEventAny::ModifyRejected(e))] => e.reason.to_string(),
            [Output::Event(OrderEventAny::CancelRejected(e))] => e.reason.to_string(),
            [Output::Denied(denial)] => denial.reason.to_string(),
            other => panic!("not one rejection: {:?}", kinds(other)),
        }
    }

    /// A limit order `id` cTrader accepted as 1031776354 (the captured
    /// one's), tracked.
    fn accepted_limit(dispatch: &mut Dispatch, id: &str) {
        dispatch.track(&order(id, OrderType::Limit), Role::Order);
        dispatch.sent(Tag::New(self::id(id)));
        let out = dispatch.handle(
            answering(Tag::New(self::id(id)), execution(EXECUTION_LIMIT_ACCEPTED)),
            ts(),
        );
        assert_eq!(kinds(&out), [row(id, "accepted")]);
    }

    /// The bracket entry `entry`, its stop loss `<entry>-SL` and take
    /// profit `<entry>-TP`, tracked.
    fn bracket(dispatch: &mut Dispatch, entry: &str) {
        let (stop_loss, take_profit) = (format!("{entry}-SL"), format!("{entry}-TP"));
        dispatch.track(
            &order(entry, OrderType::Market),
            Role::Entry {
                stop_loss: Some(id(&stop_loss)),
                take_profit: Some(id(&take_profit)),
                position: None,
            },
        );
        for (leg, kind, of) in [
            (&stop_loss, OrderType::StopMarket, Leg::StopLoss),
            (&take_profit, OrderType::Limit, Leg::TakeProfit),
        ] {
            dispatch.track(
                &order(leg, kind),
                Role::Leg {
                    entry: id(entry),
                    leg: of,
                },
            );
        }
    }

    #[test]
    fn an_order_whose_request_may_still_wait_to_go_out_is_not_concluded_absent() {
        let mut dispatch = dispatch();
        dispatch.track(&order("O-1", OrderType::Market), Role::Order);

        assert_eq!(dispatch.absent(id("O-1")), None);
        dispatch.sent(Tag::New(id("O-1")));
        assert_eq!(dispatch.absent(id("O-1")), Some(vec![id("O-1")]));
    }

    // Each wire condition's class, by the events of a request it fails.

    /// The captured refusal: cTrader's code and description.
    #[test]
    fn an_order_error_rejects_the_order_with_ctraders_code_and_description() {
        let mut dispatch = dispatch();
        dispatch.track(&order("PROBE-3", OrderType::Market), Role::Order);
        let error = ProtoOaOrderErrorEvent::decode(ORDER_ERROR_BAD_VOLUME).unwrap();

        let out = dispatch.handle(
            answering(Tag::New(id("PROBE-3")), TradeMessage::OrderError(error)),
            ts(),
        );

        assert_eq!(kinds(&out), [row("PROBE-3", "rejected")]);
        assert_eq!(
            reason(&out),
            "TRADING_BAD_VOLUME: Order volume = 0.50 is smaller than minimum allowed volume = 1.00."
        );
    }

    /// An error answer is the same evidence as an order error: the same
    /// reason, with no envelope of the client's.
    #[test]
    fn an_error_answer_rejects_with_ctraders_code_and_description() {
        let mut dispatch = dispatch();
        dispatch.track(&order("PROBE-4", OrderType::Market), Role::Order);

        let out = dispatch.handle(
            answering(
                Tag::Close(id("PROBE-4")),
                refused(
                    "POSITION_NOT_FOUND",
                    Some("Position not found with id 690004734"),
                ),
            ),
            ts(),
        );

        assert_eq!(kinds(&out), [row("PROBE-4", "rejected")]);
        assert_eq!(
            reason(&out),
            "POSITION_NOT_FOUND: Position not found with id 690004734"
        );
    }

    /// A rate limit does not say the order was not placed: it stays in
    /// flight, and its fill then applies.
    #[test]
    fn a_rate_limited_order_stays_in_flight_until_its_events_come() {
        let mut dispatch = dispatch();
        dispatch.track(&order("PROBE-1", OrderType::Market), Role::Order);
        let tag = Tag::New(id("PROBE-1"));

        let out = dispatch.handle(
            answering(tag, refused("REQUEST_FREQUENCY_EXCEEDED", None)),
            ts(),
        );
        assert!(out.is_empty(), "{:?}", kinds(&out));

        let out = dispatch.handle(answering(tag, execution(EXECUTION_MARKET_FILLED)), ts());
        assert_eq!(
            kinds(&out),
            [row("PROBE-1", "accepted"), row("PROBE-1", "filled")]
        );
    }

    /// A removal of a protection the broker may have made stays asked:
    /// the next amendment of the other protection carries it.
    #[test]
    fn a_removal_of_a_protection_with_an_unknown_outcome_stays_wanted() {
        let mut dispatch = dispatch();
        let mut stop_loss = order("O-SL", OrderType::StopMarket);
        let account = AccountId::from("ACMEFX-DEMO");
        for event in [
            nautilus_model::orders::stubs::TestOrderEventStubs::submitted(&stop_loss, account),
            nautilus_model::orders::stubs::TestOrderEventStubs::accepted(
                &stop_loss,
                account,
                VenueOrderId::from("690004734-SL"),
            ),
        ] {
            stop_loss.apply(event).unwrap();
        }
        dispatch.adopt(&stop_loss);
        dispatch.want(id("O-SL"), Want::Remove);

        let out = dispatch.handle(
            answering(
                Tag::Unprotect(id("O-SL")),
                refused("BLOCKED_PAYLOAD_TYPE", None),
            ),
            ts(),
        );

        assert!(out.is_empty(), "{:?}", kinds(&out));
        assert_eq!(dispatch.wanted(id("O-SL")), Some(Want::Remove));
    }

    /// cTrader's `ORDER_CANCEL_REJECTED`: the order stays working, and its
    /// cancellation then cancels it.
    #[test]
    fn a_cancel_rejected_by_execution_event_leaves_the_order_working() {
        let mut dispatch = dispatch();
        accepted_limit(&mut dispatch, "PROBE-2");
        let mut rejected = ProtoOaExecutionEvent::decode(EXECUTION_LIMIT_ACCEPTED).unwrap();
        rejected.set_execution_type(ProtoOaExecutionType::OrderCancelRejected);
        rejected.error_code = Some("UNABLE_TO_CANCEL_ORDER".to_owned());
        let tag = Tag::Cancel(id("PROBE-2"));

        let out = dispatch.handle(answering(tag, TradeMessage::Execution(rejected)), ts());
        assert_eq!(kinds(&out), [row("PROBE-2", "cancel rejected")]);
        assert_eq!(reason(&out), "UNABLE_TO_CANCEL_ORDER");

        let out = dispatch.handle(answering(tag, execution(EXECUTION_LIMIT_CANCELLED)), ts());
        assert_eq!(kinds(&out), [row("PROBE-2", "canceled")]);
    }

    // Each class's events, by the request it fails.

    #[test]
    fn a_new_order_not_sent_is_rejected_with_its_brackets_legs() {
        let mut dispatch = dispatch();
        bracket(&mut dispatch, "PROBE-1");

        let out = dispatch.fail(
            Tag::New(id("PROBE-1")),
            &[],
            CommandFailure::not_sent("not sent to ACMEFX: the cTrader connection was lost"),
            ts(),
        );

        assert_eq!(
            kinds(&out),
            [
                row("PROBE-1", "rejected"),
                row("PROBE-1-SL", "rejected"),
                row("PROBE-1-TP", "rejected")
            ]
        );
    }

    #[test]
    fn a_modify_not_sent_is_denied_by_its_command() {
        let mut dispatch = dispatch();
        accepted_limit(&mut dispatch, "PROBE-2");
        let command = UUID4::new();

        let out = dispatch.fail(
            Tag::Amend(id("PROBE-2")),
            &[(id("PROBE-2"), command)],
            CommandFailure::not_sent("not sent to ACMEFX: the cTrader connection was lost"),
            ts(),
        );

        let [Output::Denied(denial)] = &out[..] else {
            panic!("not one denial: {:?}", kinds(&out));
        };
        assert_eq!(denial.command, Command::Modify);
        assert_eq!(denial.client_order_id, id("PROBE-2"));
        assert_eq!(denial.command_id, command);
        assert_eq!(
            denial.reason.as_str(),
            "not sent to ACMEFX: the cTrader connection was lost"
        );
    }

    /// One amendment removes both protections of a position: neither
    /// cancel was sent.
    #[test]
    fn a_removal_of_protections_not_sent_denies_each_cancel_by_its_command() {
        let mut dispatch = dispatch();
        bracket(&mut dispatch, "PROBE-1");
        let commands = [
            (id("PROBE-1-SL"), UUID4::new()),
            (id("PROBE-1-TP"), UUID4::new()),
        ];

        let out = dispatch.fail(
            Tag::Unprotect(id("PROBE-1-SL")),
            &commands,
            CommandFailure::not_sent("not sent to ACMEFX: the cTrader connection was lost"),
            ts(),
        );

        let denied: Vec<(ClientOrderId, Command, UUID4)> = out
            .iter()
            .map(|o| match o {
                Output::Denied(d) => (d.client_order_id, d.command, d.command_id),
                _ => panic!("not a denial: {:?}", kinds(&out)),
            })
            .collect();
        assert_eq!(
            denied,
            [
                (commands[0].0, Command::Cancel, commands[0].1),
                (commands[1].0, Command::Cancel, commands[1].1)
            ]
        );
    }

    /// The broker refused the modification: the order stays working, and
    /// its cancellation then cancels it.
    #[test]
    fn a_modify_the_broker_rejects_is_rejected_and_the_order_stays_working() {
        let mut dispatch = dispatch();
        accepted_limit(&mut dispatch, "PROBE-2");

        let out = dispatch.fail(
            Tag::Amend(id("PROBE-2")),
            &[(id("PROBE-2"), UUID4::new())],
            CommandFailure::venue_rejected("TRADING_BAD_PRICES: Invalid price"),
            ts(),
        );
        assert_eq!(kinds(&out), [row("PROBE-2", "modify rejected")]);
        assert_eq!(reason(&out), "TRADING_BAD_PRICES: Invalid price");

        let out = dispatch.handle(
            answering(
                Tag::Cancel(id("PROBE-2")),
                execution(EXECUTION_LIMIT_CANCELLED),
            ),
            ts(),
        );
        assert_eq!(kinds(&out), [row("PROBE-2", "canceled")]);
    }

    #[test]
    fn a_cancel_the_broker_rejects_is_rejected_and_the_order_stays_working() {
        let mut dispatch = dispatch();
        accepted_limit(&mut dispatch, "PROBE-2");
        let tag = Tag::Cancel(id("PROBE-2"));

        let out = dispatch.fail(
            tag,
            &[(id("PROBE-2"), UUID4::new())],
            CommandFailure::venue_rejected("ORDER_NOT_FOUND: Order not found with id 1031776354"),
            ts(),
        );
        assert_eq!(kinds(&out), [row("PROBE-2", "cancel rejected")]);

        let out = dispatch.handle(answering(tag, execution(EXECUTION_LIMIT_CANCELLED)), ts());
        assert_eq!(kinds(&out), [row("PROBE-2", "canceled")]);
    }

    /// No definitive event, whatever the request: the trade events, a
    /// query or reconciliation resolve it.
    #[test]
    fn an_ambiguous_failure_emits_nothing_whatever_the_request() {
        let mut dispatch = dispatch();
        accepted_limit(&mut dispatch, "PROBE-2");
        for tag in [
            Tag::New(id("PROBE-2")),
            Tag::Amend(id("PROBE-2")),
            Tag::Cancel(id("PROBE-2")),
        ] {
            let out = dispatch.fail(
                tag,
                &[(id("PROBE-2"), UUID4::new())],
                CommandFailure::ambiguous("REQUEST_FREQUENCY_EXCEEDED"),
                ts(),
            );
            assert!(out.is_empty(), "{tag}: {:?}", kinds(&out));
        }
        let out = dispatch.handle(
            answering(
                Tag::Cancel(id("PROBE-2")),
                execution(EXECUTION_LIMIT_CANCELLED),
            ),
            ts(),
        );
        assert_eq!(kinds(&out), [row("PROBE-2", "canceled")]);
    }

    // What became of a request's frame.

    #[test]
    fn a_new_order_the_session_never_sent_is_rejected() {
        let mut dispatch = dispatch();
        dispatch.track(&order("PROBE-1", OrderType::Market), Role::Order);

        let out = dispatch.sending(
            Tag::New(id("PROBE-1")),
            &[],
            Err(CommandFailure::not_sent(
                "not sent to ACMEFX: the cTrader connection was lost",
            )),
            ts(),
        );

        assert_eq!(kinds(&out), [row("PROBE-1", "rejected")]);
    }

    /// Written, or maybe: its request waits no more, and a query finding
    /// the broker holds no such order concludes it absent.
    #[test]
    fn an_order_whose_request_was_written_or_maybe_waits_no_more() {
        for sending in [
            Ok(()),
            Err(CommandFailure::ambiguous("the ACMEFX session ended")),
        ] {
            let mut dispatch = dispatch();
            dispatch.track(&order("PROBE-1", OrderType::Market), Role::Order);

            let out = dispatch.sending(Tag::New(id("PROBE-1")), &[], sending.clone(), ts());

            assert!(out.is_empty(), "{sending:?}: {:?}", kinds(&out));
            assert_eq!(
                dispatch.absent(id("PROBE-1")),
                Some(vec![id("PROBE-1")]),
                "{sending:?}"
            );
        }
    }

    // A deal whose commission cTrader does not state (Nautilus'
    // `spec_exec_testing.md`, TC-E89).

    /// The first delivery emits nothing and touches nothing: neither the
    /// acceptance the fill proves nor the deal is spent, and the order is
    /// not closed; the repaired replay applies them once.
    #[test]
    fn a_fill_without_its_commission_is_left_and_its_repaired_replay_fills_once() {
        let mut dispatch = dispatch();
        dispatch.track(&order("PROBE-1", OrderType::Market), Role::Order);
        let tag = Tag::New(id("PROBE-1"));

        let out = dispatch.handle(
            answering(tag, without_commission(EXECUTION_MARKET_FILLED)),
            ts(),
        );
        assert!(out.is_empty(), "{:?}", kinds(&out));

        let out = dispatch.handle(answering(tag, execution(EXECUTION_MARKET_FILLED)), ts());
        assert_eq!(
            kinds(&out),
            [row("PROBE-1", "accepted"), row("PROBE-1", "filled")]
        );
        let Output::Event(OrderEventAny::Filled(fill)) = &out[1] else {
            unreachable!()
        };
        // The captured deal's charge: 0.11 EUR.
        assert_eq!(
            fill.commission,
            Some(Money::new(0.11, Currency::from("EUR")))
        );

        let out = dispatch.handle(answering(tag, execution(EXECUTION_MARKET_FILLED)), ts());
        assert!(out.is_empty(), "{:?}", kinds(&out));
    }

    /// An order not tracked: no report until the deal states its
    /// commission.
    #[test]
    fn a_deal_of_an_order_not_tracked_without_its_commission_is_reported_once_repaired() {
        let mut dispatch = dispatch();
        let event = |message| TradeEvent {
            client_msg_id: None,
            message,
        };

        let out = dispatch.handle(event(without_commission(EXECUTION_MARKET_FILLED)), ts());
        assert!(out.is_empty(), "{:?}", kinds(&out));

        let out = dispatch.handle(event(execution(EXECUTION_MARKET_FILLED)), ts());
        let [Output::Report(ExecutionReport::OrderWithFills(_, fills))] = &out[..] else {
            panic!("not one report with its fills: {:?}", kinds(&out));
        };
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].commission, Money::new(0.11, Currency::from("EUR")));
    }

    /// The captured stop loss hit of the bracket PROBE-5's position: no
    /// leg filled or accepted, until the deal states its commission.
    #[test]
    fn a_protection_hit_without_its_commission_fills_no_leg_until_repaired() {
        let mut dispatch = dispatch();
        bracket(&mut dispatch, "PROBE-5");
        let tag = Tag::New(id("PROBE-5"));

        let out = dispatch.handle(
            answering(tag, without_commission(EXECUTION_STOP_LOSS_FILLED)),
            ts(),
        );
        assert!(out.is_empty(), "{:?}", kinds(&out));

        let out = dispatch.handle(answering(tag, execution(EXECUTION_STOP_LOSS_FILLED)), ts());
        assert_eq!(
            kinds(&out),
            [
                row("PROBE-5-SL", "accepted"),
                row("PROBE-5-SL", "filled"),
                row("PROBE-5-TP", "canceled")
            ]
        );

        let out = dispatch.handle(answering(tag, execution(EXECUTION_STOP_LOSS_FILLED)), ts());
        assert!(out.is_empty(), "{:?}", kinds(&out));
    }
}
