//! IC's execution client for the live node: Nautilus' `ExecutionClient`
//! on a cTrader session.
//!
//! On connect it authenticates, checks the account (hedging,
//! `IC_ACCOUNT_ID`'s, see `data/ic/account.rs`), loads its instruments,
//! listens to the account's trade events (see `dispatch`), and publishes
//! the account state: the balance, and the margin the open positions use.
//! It reports connected once the node's cache holds the account. The venue
//! is hedging (`OmsType::Hedging`): each cTrader position is one Nautilus
//! position, its `positionId` the venue position id (see `reports`).
//!
//! Orders (see `orders`): a submission is checked, tracked, reported
//! submitted and sent; a bracket's protections go on its entry, then live
//! on the position; an order naming a position closes it, in part or
//! whole, or adds to it. A modification amends a pending order, or moves a
//! protection (`ProtoOAAmendPositionSLTPReq`, both prices, cTrader dropping
//! one left out, the other as last asked); a cancel cancels a pending
//! order, or removes a protection. A request the session never sent is rejected; one sent but
//! unanswered stays in flight (`retain_unresolved_submissions`) until its
//! events, a query or reconciliation resolve it.
//!
//! Reconciliation reads the reconcile request (open positions, their stop
//! loss and take profit, pending orders), the deal list (fills), and for a
//! single order its details or the order list.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    enums::LogLevel,
    live::{
        dst::time::{self, Duration, Instant},
        runner::{get_exec_event_sender, try_get_exec_event_sender},
    },
    logging::map_log_level_to_filter,
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    DurationNanos, Params, UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter, task::TaskGroup};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, ContingencyType, OmsType, OrderType},
    events::{AccountState, OrderDeniedReason},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Price},
};

use super::{
    dispatch::{Dispatch, Output, Role, Want},
    orders::{self, Leg, Placement, Protection, Tag, parse_protection_id},
    reports::{Reports, explained, open_volumes},
};
use crate::data::ic::{
    IcHistory,
    account::IcAccount,
    config::{IcConnection, IcCredential},
    messages::{
        ProtoOaAmendOrderReq, ProtoOaAmendPositionSltpReq, ProtoOaCancelOrderReq, ProtoOaOrder,
        ProtoOaReconcileRes, TradeRequest,
    },
    parse::IC_VENUE,
    websocket::{HEARTBEAT_INTERVAL, TradeSent},
};

/// How long connect waits for the node's cache to hold the account.
const ACCOUNT_REGISTERED_TIMEOUT: Duration = Duration::from_secs(30);

/// How long tasks get to finish at a disconnect, then to abort.
const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// How far before an order's creation a query looks for it in the order
/// list: the venue's clock and the node's may differ.
const QUERY_MARGIN_MS: i64 = 60_000;

/// IC's execution client: orders, the account state and reconciliation
/// reports of the account, on venue `ICMARKETS`.
#[derive(Debug)]
pub(super) struct IcExecutionClient {
    core: ExecutionClientCore,
    url: String,
    credential: IcCredential,
    instrument_ids: Vec<InstrumentId>,
    emitter: ExecutionEventEmitter,
    clock: &'static AtomicTime,
    session: Option<Session>,
    session_tasks: TaskGroup,
    command_tasks: TaskGroup,
}

/// A connected session, the instruments it loaded by cTrader `symbolId`,
/// the account it checked, and the orders it tracks.
#[derive(Debug)]
struct Session {
    history: Arc<IcHistory>,
    instruments: HashMap<i64, InstrumentAny>,
    symbols: HashMap<InstrumentId, i64>,
    account: IcAccount,
    dispatch: Arc<Mutex<Dispatch>>,
}

impl Session {
    fn dispatch(&self) -> MutexGuard<'_, Dispatch> {
        self.dispatch.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl IcExecutionClient {
    /// A client of `core`'s identity on the environment of `connection`,
    /// loading `instrument_ids`, IC ids, at connect; not connected. `core`
    /// is hedging, on venue `ICMARKETS`, a margin account.
    #[must_use]
    pub(super) fn new(
        core: ExecutionClientCore,
        connection: &IcConnection,
        instrument_ids: Vec<InstrumentId>,
    ) -> Self {
        let url = connection.environment.url().to_owned();
        Self::with_endpoint(core, url, connection.credential.clone(), instrument_ids)
    }

    fn with_endpoint(
        core: ExecutionClientCore,
        url: String,
        credential: IcCredential,
        instrument_ids: Vec<InstrumentId>,
    ) -> Self {
        let clock = get_atomic_clock_realtime();
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            None,
        );
        // Bound when the node's factory creates the client; `start` binds
        // it again, authoritatively.
        if let Some(sender) = try_get_exec_event_sender() {
            emitter.set_sender(sender);
        }
        Self {
            core,
            url,
            credential,
            instrument_ids,
            emitter,
            clock,
            session: None,
            session_tasks: TaskGroup::new(),
            command_tasks: TaskGroup::new(),
        }
    }

    fn session(&self) -> anyhow::Result<&Session> {
        self.session
            .as_ref()
            .with_context(|| format!("{} is not connected", self.core.client_id))
    }

    /// Connects, checks the account, loads the instruments, starts
    /// dispatching the trade events and reads the account state; the
    /// session closes again on a failure.
    async fn open_session(&self) -> anyhow::Result<(Session, AccountState)> {
        let (history, _spots) =
            IcHistory::connect_to(&self.url, self.credential.clone(), HEARTBEAT_INTERVAL).await?;
        let opened = async {
            let trades = history.trade_events()?;
            let account = history.account().await?;
            let symbols = history.symbols(&self.instrument_ids).await?;
            let open = history.reconcile().await?;
            let (balances, margins) = account.state(&open.position)?;
            Ok::<_, anyhow::Error>((trades, account, symbols, balances, margins))
        }
        .await;
        let (mut trades, account, symbols, balances, margins) = match opened {
            Ok(opened) => opened,
            Err(e) => {
                history.disconnect().await;
                return Err(e);
            }
        };
        let state = self.account_state(&account, balances, margins, true, self.now());
        let instruments: HashMap<i64, InstrumentAny> = symbols
            .into_iter()
            .map(|s| (s.symbol_id, s.instrument))
            .collect();
        let dispatch = Arc::new(Mutex::new(Dispatch::new(
            self.core.trader_id,
            self.core.account_id,
            account.currency,
            instruments.clone(),
        )));
        let stream = Arc::clone(&dispatch);
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let cancelled = self.session_tasks.cancellation_token();
        self.session_tasks.spawn(async move {
            loop {
                let event = tokio::select! {
                    biased;
                    () = cancelled.cancelled() => return,
                    event = trades.recv() => match event {
                        Some(event) => event,
                        None => return,
                    },
                };
                let outputs = stream
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .handle(event, clock.get_time_ns());
                emit(&emitter, outputs);
            }
        })?;
        let session = Session {
            history: Arc::new(history),
            symbols: instruments.iter().map(|(id, i)| (i.id(), *id)).collect(),
            instruments,
            account,
            dispatch,
        };
        Ok((session, state))
    }

    /// An account state of the account `account` checked: a margin
    /// account, its base currency the deposit currency.
    fn account_state(
        &self,
        account: &IcAccount,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> AccountState {
        AccountState::new(
            self.core.account_id,
            AccountType::Margin,
            balances,
            margins,
            reported,
            UUID4::new(),
            ts_event,
            self.now(),
            Some(account.currency),
        )
    }

    fn now(&self) -> UnixNanos {
        self.clock.get_time_ns()
    }

    /// Waits until the node's cache holds the account, as the account state
    /// goes to the node asynchronously.
    async fn await_account_registered(&self) -> anyhow::Result<()> {
        let account_id = self.core.account_id;
        let deadline = Instant::now() + ACCOUNT_REGISTERED_TIMEOUT;
        while self.core.cache().account(&account_id).is_none() {
            anyhow::ensure!(
                Instant::now() < deadline,
                "the node did not register the account {account_id} within {}s",
                ACCOUNT_REGISTERED_TIMEOUT.as_secs()
            );
            time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    fn reports<'s>(&self, session: &'s Session, ts_init: UnixNanos) -> Reports<'s> {
        Reports {
            account_id: self.core.account_id,
            instruments: &session.instruments,
            currency: session.account.currency,
            ts_init,
        }
    }

    /// The open orders of `open`: the pending orders, and the stop loss
    /// and take profit of the positions.
    fn order_reports(
        reports: &Reports<'_>,
        open: &ProtoOaReconcileRes,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let mut orders = Vec::new();
        for order in &open.order {
            orders.extend(reports.order(order)?);
        }
        for position in &open.position {
            orders.extend(reports.protections(position)?);
        }
        Ok(orders)
    }

    fn position_reports(
        reports: &Reports<'_>,
        open: &ProtoOaReconcileRes,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let mut positions = Vec::new();
        for position in &open.position {
            positions.extend(reports.position(position)?);
        }
        Ok(positions)
    }

    /// The fills of the deals over `[from, to]`.
    async fn fill_reports(
        &self,
        session: &Session,
        ts_init: UnixNanos,
        from: UnixNanos,
        to: UnixNanos,
    ) -> anyhow::Result<Vec<FillReport>> {
        let deals = session.history.deals(millis(from), millis(to)).await?;
        let reports = self.reports(session, ts_init);
        let mut fills = Vec::new();
        for deal in &deals {
            fills.extend(reports.fill(deal)?);
        }
        Ok(fills)
    }

    /// The earliest opening, in ns, of the account's positions the node's
    /// cache holds open: a restart reads the deals from there, so a
    /// position closed while the node was down gets its closing fills.
    fn earliest_cached_open(&self) -> Option<UnixNanos> {
        let cache = self.core.cache();
        cache
            .positions_open(None, None, None, Some(&self.core.account_id), None)
            .iter()
            .map(|p| p.ts_opened)
            .min()
    }

    fn spawn_command<F>(&self, description: &'static str, future: F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let cancelled = self.command_tasks.cancellation_token();
        let future = async move {
            tokio::select! {
                biased;
                () = cancelled.cancelled() => {}
                result = future => {
                    if let Err(e) = result {
                        log::error!("{description} failed: {e:#}");
                    }
                }
            }
        };
        if let Err(e) = self.command_tasks.spawn(future) {
            log::warn!("{description} skipped after shutdown began: {e}");
        }
    }

    /// Closes admission and cancels the tasks, and asks the session to
    /// close: `teardown` awaits both.
    fn begin_shutdown(&self) {
        self.session_tasks.begin_shutdown();
        self.command_tasks.begin_shutdown();
        if let Some(session) = &self.session {
            session.history.begin_shutdown();
        }
    }

    /// Drains the tasks, then closes the session, so a token renewal out
    /// at `stop` is still saved.
    async fn teardown(&mut self) -> anyhow::Result<()> {
        self.begin_shutdown();
        let (session, command) = tokio::join!(
            self.session_tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
            self.command_tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
        );
        if let Some(open) = self.session.take() {
            // The tasks are done, so this is the last handle.
            if let Ok(history) = Arc::try_unwrap(open.history) {
                history.disconnect().await;
            }
        }
        session.map_err(|e| anyhow::anyhow!("stopping the IC trade event task: {e}"))?;
        command.map_err(|e| anyhow::anyhow!("stopping the IC command tasks: {e}"))?;
        Ok(())
    }

    /// Sends `request` under `tag`; if the session never sends it, the
    /// order's command is refused (see `Dispatch::refuse`).
    fn send<Req: TradeRequest>(&self, session: &Session, request: Req, tag: Tag) {
        let sent = session.history.trade(request, tag.to_string());
        self.refuse_unsent(session, tag, sent);
    }

    fn refuse_unsent(&self, session: &Session, tag: Tag, sent: TradeSent) {
        let dispatch = Arc::clone(&session.dispatch);
        let emitter = self.emitter.clone();
        let clock = self.clock;
        self.spawn_command("IC trade request", async move {
            let refused = match sent.await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(e)) => format!("not sent to IC: {e}"),
                Err(_) => "not sent to IC: the session closed".to_owned(),
            };
            log::warn!("{tag} {refused}");
            let outputs = dispatch
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .refuse(tag, &refused, clock.get_time_ns());
            emit(&emitter, outputs);
            Ok(())
        });
    }

    /// Refuses `tag` at once: its request was never sent.
    fn refuse_now(&self, session: &Session, tag: Tag, reason: &str) {
        log::warn!("{tag} not sent: {reason}");
        let outputs = session.dispatch().refuse(tag, reason, self.now());
        emit(&self.emitter, outputs);
    }

    /// The cTrader symbol of `instrument_id`, if loaded.
    fn symbol_id(session: &Session, instrument_id: InstrumentId) -> Result<i64, OrderDeniedReason> {
        session
            .symbols
            .get(&instrument_id)
            .copied()
            .ok_or(OrderDeniedReason::InstrumentNotFound { instrument_id })
    }

    /// Places `order`, into the position `cmd_position` names if any, with
    /// `protection`: denied if it cannot be, else tracked as `role`,
    /// submitted and sent.
    fn place(
        &self,
        session: &Session,
        order: &OrderAny,
        cmd_position: Option<nautilus_model::identifiers::PositionId>,
        protection: Protection,
        role: Role,
    ) -> Result<(), OrderDeniedReason> {
        let symbol_id = Self::symbol_id(session, order.instrument_id())?;
        let position = match cmd_position {
            Some(id) => Some(
                self.core
                    .cache()
                    .position_owned(&id)
                    .ok_or(OrderDeniedReason::PositionNotFound { position_id: id })?,
            ),
            None => None,
        };
        let placement = orders::placement(order, symbol_id, position.as_ref(), protection)?;
        session.dispatch().track(order, role);
        self.emitter.emit_order_submitted(order);
        let id = order.client_order_id();
        match placement {
            Placement::New(request) => self.send(session, request, Tag::New(id)),
            Placement::Close(request) => self.send(session, request, Tag::Close(id)),
        }
        Ok(())
    }

    /// Submits the bracket of `orders`: an entry (market, limit or stop
    /// market) whose protections are its children, a stop market stop
    /// loss and a limit take profit, each for the entry's quantity on the
    /// other side.
    fn submit_bracket(
        &self,
        session: &Session,
        orders: &[OrderAny],
        cmd_position: Option<nautilus_model::identifiers::PositionId>,
    ) -> Result<(), OrderDeniedReason> {
        let unsupported = |detail: &str| OrderDeniedReason::UnsupportedOrderList {
            detail: detail.to_owned(),
        };
        let [entry] = orders
            .iter()
            .filter(|o| o.contingency_type() == Some(ContingencyType::Oto))
            .collect::<Vec<_>>()[..]
        else {
            return Err(unsupported("IC takes a bracket of one entry"));
        };
        let mut protection = Protection::default();
        let (mut stop_loss, mut take_profit) = (None, None);
        for leg in orders
            .iter()
            .filter(|o| o.client_order_id() != entry.client_order_id())
        {
            let protects = leg.parent_order_id() == Some(entry.client_order_id())
                && leg.order_side() != entry.order_side()
                && leg.quantity() == entry.quantity();
            match leg.order_type() {
                OrderType::StopMarket if protects && stop_loss.is_none() => {
                    stop_loss = Some(leg.client_order_id());
                    protection.stop_loss = leg.trigger_price();
                }
                OrderType::Limit if protects && take_profit.is_none() => {
                    take_profit = Some(leg.client_order_id());
                    protection.take_profit = leg.price();
                }
                _ => {
                    return Err(unsupported(
                        "IC protects an entry with one stop market stop loss and one limit \
                         take profit, for its quantity, on the other side",
                    ));
                }
            }
        }
        let role = Role::Entry {
            stop_loss,
            take_profit,
            position: None,
        };
        // The legs first: the entry's events may come before this returns.
        for leg in orders
            .iter()
            .filter(|o| o.client_order_id() != entry.client_order_id())
        {
            let kind = if Some(leg.client_order_id()) == stop_loss {
                Leg::StopLoss
            } else {
                Leg::TakeProfit
            };
            session.dispatch().track(
                leg,
                Role::Leg {
                    entry: entry.client_order_id(),
                    leg: kind,
                },
            );
        }
        let placed = self.place(session, entry, cmd_position, protection, role);
        if placed.is_ok() {
            for leg in orders
                .iter()
                .filter(|o| o.client_order_id() != entry.client_order_id())
            {
                self.emitter.emit_order_submitted(leg);
            }
        }
        placed
    }

    /// The order of `id` in the node's cache.
    fn cached_order(&self, id: &ClientOrderId) -> Option<OrderAny> {
        self.core.cache().order_owned(id)
    }

    /// The price of the protection `leg` of `position` as last asked: none
    /// if it is not open in the node's cache, else the dispatch's wanted
    /// change of it, else its price in the cache.
    fn asked_protection(&self, session: &Session, position: i64, leg: Leg) -> Option<Price> {
        let venue = orders::protection_id(position, leg);
        let cache = self.core.cache();
        let id = cache.client_order_id(&venue)?;
        let order = cache.order(id)?;
        if order.is_closed() {
            return None;
        }
        match session.dispatch().wanted(*id) {
            Some(Want::Move(price)) => Some(price),
            Some(Want::Remove) => None,
            None => match leg {
                Leg::StopLoss => order.trigger_price(),
                Leg::TakeProfit => order.price(),
            },
        }
    }

    /// The request setting the protections of `position` as last asked
    /// (see `asked_protection`): both prices, since cTrader drops a price
    /// left out.
    fn amend_protections(&self, session: &Session, position: i64) -> ProtoOaAmendPositionSltpReq {
        let price = |leg| self.asked_protection(session, position, leg);
        ProtoOaAmendPositionSltpReq {
            position_id: position,
            stop_loss: price(Leg::StopLoss).map(|p| p.as_f64()),
            take_profit: price(Leg::TakeProfit).map(|p| p.as_f64()),
            ..Default::default()
        }
    }

    /// The amendment of the pending order `entry` (its venue order id
    /// `venue`) to `quantity`, `price` and `trigger_price`, carrying the
    /// bracket's `protection`.
    fn amend_order(
        order: &OrderAny,
        venue: i64,
        quantity: nautilus_model::types::Quantity,
        price: Option<Price>,
        trigger_price: Option<Price>,
        protection: Protection,
    ) -> Result<ProtoOaAmendOrderReq, OrderDeniedReason> {
        let mut request = ProtoOaAmendOrderReq {
            order_id: venue,
            volume: Some(orders::volume(quantity)?),
            stop_loss: protection.stop_loss.map(|p| p.as_f64()),
            take_profit: protection.take_profit.map(|p| p.as_f64()),
            ..Default::default()
        };
        match order.order_type() {
            OrderType::Limit => request.limit_price = price.or(order.price()).map(|p| p.as_f64()),
            OrderType::StopMarket => {
                request.stop_price = trigger_price.or(order.trigger_price()).map(|p| p.as_f64());
                request.set_stop_trigger_method(orders::trigger_method(order.trigger_type())?);
            }
            _ => {}
        }
        Ok(request)
    }

    /// Modifies the order of `cmd`, or refuses it with the reason.
    fn modify(&self, session: &Session, cmd: &ModifyOrder) -> Result<(), String> {
        let id = cmd.client_order_id;
        let order = self
            .cached_order(&id)
            .ok_or_else(|| format!("{id} is not in the cache"))?;
        session.dispatch().adopt(&order);
        let venue = cmd.venue_order_id.or(order.venue_order_id());
        if let Some((position, leg)) = venue.and_then(parse_protection_id) {
            if cmd.quantity.is_some_and(|q| q != order.quantity()) {
                return Err(
                    "a protection covers its whole position: its quantity is the \
                            position's"
                        .to_owned(),
                );
            }
            let price = match leg {
                Leg::StopLoss => cmd.trigger_price,
                Leg::TakeProfit => cmd.price,
            };
            let price = price
                .or(leg_price(&order, leg))
                .ok_or("no price to move to")?;
            session.dispatch().want(id, Want::Move(price));
            let request = self.amend_protections(session, position);
            self.send(session, request, Tag::Protect(id));
            return Ok(());
        }
        let tracked_role = session.dispatch().tracked(id).map(|t| t.role);
        if let Some(Role::Leg { entry, leg }) = tracked_role {
            // The entry is pending: the protection lives on it.
            let entry_order = self
                .cached_order(&entry)
                .ok_or_else(|| format!("the entry {entry} is not in the cache"))?;
            let entry_venue = pending_venue_id(&entry_order)?;
            let price = match leg {
                Leg::StopLoss => cmd.trigger_price,
                Leg::TakeProfit => cmd.price,
            };
            let price = price
                .or(leg_price(&order, leg))
                .ok_or("no price to move to")?;
            session.dispatch().want(id, Want::Move(price));
            let protection = session.dispatch().protection(entry);
            let request = Self::amend_order(
                &entry_order,
                entry_venue,
                entry_order.quantity(),
                None,
                None,
                protection,
            )
            .map_err(|e| e.to_string())?;
            self.send(session, request, Tag::Protect(id));
            return Ok(());
        }
        let venue = pending_venue_id(&order)?;
        let protection = session.dispatch().protection(id);
        let request = Self::amend_order(
            &order,
            venue,
            cmd.quantity.unwrap_or(order.quantity()),
            cmd.price,
            cmd.trigger_price,
            protection,
        )
        .map_err(|e| e.to_string())?;
        self.send(session, request, Tag::Amend(id));
        Ok(())
    }

    /// Cancels the orders `ids`: a pending order by its id, protections by
    /// one amendment of their position each, a pending entry's by its
    /// amendment. Refuses those it cannot cancel.
    fn cancel(&self, session: &Session, ids: &[ClientOrderId]) {
        let mut protections: HashMap<i64, Vec<ClientOrderId>> = HashMap::new();
        for &id in ids {
            let outcome = (|| {
                let order = self
                    .cached_order(&id)
                    .ok_or_else(|| format!("{id} is not in the cache"))?;
                session.dispatch().adopt(&order);
                if let Some((position, _)) = order.venue_order_id().and_then(parse_protection_id) {
                    session.dispatch().want(id, Want::Remove);
                    protections.entry(position).or_default().push(id);
                    return Ok(());
                }
                // Bound first: the dispatch's lock must not outlive it.
                let tracked_role = session.dispatch().tracked(id).map(|t| t.role);
                if let Some(Role::Leg { entry, .. }) = tracked_role {
                    let entry_order = self
                        .cached_order(&entry)
                        .ok_or_else(|| format!("the entry {entry} is not in the cache"))?;
                    let venue = pending_venue_id(&entry_order)?;
                    session.dispatch().want(id, Want::Remove);
                    let protection = session.dispatch().protection(entry);
                    let request = Self::amend_order(
                        &entry_order,
                        venue,
                        entry_order.quantity(),
                        None,
                        None,
                        protection,
                    )
                    .map_err(|e| e.to_string())?;
                    self.send(session, request, Tag::Unprotect(id));
                    return Ok(());
                }
                let venue = pending_venue_id(&order)?;
                let request = ProtoOaCancelOrderReq {
                    order_id: venue,
                    ..Default::default()
                };
                self.send(session, request, Tag::Cancel(id));
                Ok::<_, String>(())
            })();
            if let Err(reason) = outcome {
                self.refuse_now(session, Tag::Cancel(id), &reason);
            }
        }
        for (position, legs) in protections {
            let request = self.amend_protections(session, position);
            self.send(session, request, Tag::Unprotect(legs[0]));
        }
    }

    /// The order `id` (venue order id `venue`, sent at the earliest at
    /// `since`) as IC holds it, open or not, with its fills; `None` if IC
    /// holds no such order since then.
    async fn find_order(
        history: &IcHistory,
        reports: &Reports<'_>,
        id: ClientOrderId,
        venue: Option<VenueOrderId>,
        since: UnixNanos,
        now: UnixNanos,
    ) -> anyhow::Result<Option<(OrderStatusReport, Vec<FillReport>)>> {
        let order_id = match venue.and_then(|v| v.as_str().parse::<i64>().ok()) {
            Some(order_id) => Some(order_id),
            None => {
                let close = Tag::Close(id).to_string();
                let orders = history
                    .orders(
                        millis(since) - QUERY_MARGIN_MS,
                        millis(now) + QUERY_MARGIN_MS,
                    )
                    .await?;
                orders
                    .iter()
                    .find(|o| is_order(o, id, &close))
                    .map(|o| o.order_id)
            }
        };
        let Some(order_id) = order_id else {
            return Ok(None);
        };
        let details = history
            .order_details(order_id)
            .await
            .with_context(|| format!("requesting IC's order {order_id}"))?;
        let Some(mut status) = reports.order(&details.order)? else {
            return Ok(None);
        };
        status.client_order_id = Some(id);
        let mut fills = Vec::new();
        for deal in &details.deal {
            fills.extend(reports.fill(deal)?);
        }
        Ok(Some((status, fills)))
    }
}

/// Whether cTrader's `order` is the Nautilus order `id`: its client order
/// id, or the tag `close` cTrader gave a closing order.
fn is_order(order: &ProtoOaOrder, id: ClientOrderId, close: &str) -> bool {
    order
        .client_order_id
        .as_deref()
        .is_some_and(|c| c == id.as_str() || c == close)
}

/// The price of `leg` an order of that leg holds.
fn leg_price(order: &OrderAny, leg: Leg) -> Option<Price> {
    match leg {
        Leg::StopLoss => order.trigger_price(),
        Leg::TakeProfit => order.price(),
    }
}

/// The cTrader id of a pending `order`, accepted under it.
fn pending_venue_id(order: &OrderAny) -> Result<i64, String> {
    order
        .venue_order_id()
        .and_then(|v| v.as_str().parse().ok())
        .ok_or_else(|| format!("{} is not yet accepted by IC", order.client_order_id()))
}

/// Sends `outputs` to the node.
fn emit(emitter: &ExecutionEventEmitter, outputs: Vec<Output>) {
    for output in outputs {
        match output {
            Output::Event(event) => emitter.send_order_event(event),
            Output::Report(report) => emitter.send_execution_report(report),
        }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for IcExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        Venue::new(IC_VENUE)
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    /// An order sent without an answer (a disconnect) stays in flight
    /// until IC's events, a query or reconciliation tell its outcome.
    fn retain_unresolved_submissions(&self) -> bool {
        true
    }

    /// Publishes an account state of `balances` and `margins` in the
    /// account's currency.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not connected.
    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        _info: Option<Params>,
    ) -> anyhow::Result<()> {
        let account = &self.session()?.account;
        let state = self.account_state(account, balances, margins, reported, ts_event);
        self.emitter.try_send_account_state(state)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        log::info!(
            "Started {}: account {}, hedging",
            self.core.client_id,
            self.core.account_id
        );
        Ok(())
    }

    /// Begins the shutdown; `disconnect`, or the next `connect`, finishes
    /// it.
    fn stop(&mut self) -> anyhow::Result<()> {
        self.core.set_stopped();
        self.core.set_disconnected();
        self.begin_shutdown();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }
        log::info!("Connecting {}", self.core.client_id);
        // The node logs a failed connect without its reason: log it here.
        let client_id = self.core.client_id;
        let failed = |e: anyhow::Error| {
            log::error!("{client_id} failed to connect: {e:#}");
            e
        };
        if self.session.is_some() || !self.session_tasks.is_open() || !self.command_tasks.is_open()
        {
            self.teardown().await.map_err(failed)?;
            self.session_tasks
                .start_generation()
                .map_err(|e| failed(anyhow::anyhow!("reopening the IC trade event task: {e}")))?;
            self.command_tasks
                .start_generation()
                .map_err(|e| failed(anyhow::anyhow!("reopening the IC command tasks: {e}")))?;
        }
        let (session, state) = self.open_session().await.map_err(failed)?;
        self.session = Some(session);
        let ready = async {
            self.emitter.try_send_account_state(state)?;
            self.await_account_registered().await
        }
        .await;
        if let Err(e) = ready {
            self.disconnect().await?;
            return Err(failed(e));
        }
        self.core.set_connected();
        log::info!("Connected {}", self.core.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.core.set_disconnected();
        let finished = self.teardown().await;
        log::info!("Disconnected {}", self.core.client_id);
        finished
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let session = self.session()?;
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;
        if order.is_closed() {
            log::warn!("{} is closed: not submitted", order.client_order_id());
            return Ok(());
        }
        if let Err(denied) = self.place(
            session,
            &order,
            cmd.position_id,
            Protection::default(),
            Role::Order,
        ) {
            self.emitter.emit_order_denied(&order, &denied.to_string());
        }
        Ok(())
    }

    /// A bracket (an entry and its protections) as one order carrying its
    /// stop loss and take profit; a list without contingencies as its
    /// orders, one by one. Any other list is denied.
    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let session = self.session()?;
        let orders: Vec<OrderAny> = {
            let cache = self.core.cache();
            cmd.order_list
                .client_order_ids
                .iter()
                .map(|id| cache.try_order_owned(id).map_err(anyhow::Error::from))
                .collect::<anyhow::Result<_>>()?
        };
        if orders.iter().all(|o| o.contingency_type().is_none()) {
            for order in &orders {
                if let Err(denied) = self.place(
                    session,
                    order,
                    cmd.position_id,
                    Protection::default(),
                    Role::Order,
                ) {
                    self.emitter.emit_order_denied(order, &denied.to_string());
                }
            }
            return Ok(());
        }
        if let Err(denied) = self.submit_bracket(session, &orders, cmd.position_id) {
            let reason = denied.to_string();
            for order in &orders {
                self.emitter.emit_order_denied(order, &reason);
            }
        }
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let session = self.session()?;
        if let Err(reason) = self.modify(session, &cmd) {
            log::warn!("modify of {} not sent: {reason}", cmd.client_order_id);
            self.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                &reason,
                self.now(),
            );
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let session = self.session()?;
        self.cancel(session, &[cmd.client_order_id]);
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let session = self.session()?;
        let ids: Vec<ClientOrderId> = self
            .core
            .cache()
            .orders_open(
                None,
                Some(&cmd.instrument_id),
                Some(&cmd.strategy_id),
                None,
                cmd.order_side,
            )
            .iter()
            .map(|order| order.client_order_id())
            .collect();
        self.cancel(session, &ids);
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let session = self.session()?;
        let ids: Vec<ClientOrderId> = cmd.cancels.iter().map(|c| c.client_order_id).collect();
        self.cancel(session, &ids);
        Ok(())
    }

    /// Sends the report of the order of `cmd` as IC holds it, open or not,
    /// with its fills; logs it if IC holds none.
    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let session = self.session()?;
        let since = self
            .cached_order(&cmd.client_order_id)
            .map_or(UnixNanos::default(), |o| o.ts_init());
        let venue = cmd
            .venue_order_id
            .filter(|v| parse_protection_id(*v).is_none());
        let history = Arc::clone(&session.history);
        let account_id = self.core.account_id;
        let instruments = session.instruments.clone();
        let currency = session.account.currency;
        let emitter = self.emitter.clone();
        let clock = self.clock;
        self.spawn_command("IC order query", async move {
            let now = clock.get_time_ns();
            let reports = Reports {
                account_id,
                instruments: &instruments,
                currency,
                ts_init: now,
            };
            let found =
                Self::find_order(&history, &reports, cmd.client_order_id, venue, since, now)
                    .await?;
            match found {
                Some((status, fills)) => emitter.send_order_with_fills(status, fills),
                None => log::warn!("IC holds no order {} since {since}", cmd.client_order_id),
            }
            Ok(())
        });
        Ok(())
    }

    /// The order of the query: open, from the reconcile request; else, by
    /// its venue order id or among the orders IC updated since the cached
    /// order was created, from its details. `None` once IC answered it
    /// holds no such order.
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let session = self.session()?;
        let open = session.history.reconcile().await?;
        let reports = self.reports(session, self.now());
        let found = Self::order_reports(&reports, &open)?.into_iter().find(|r| {
            cmd.venue_order_id == Some(r.venue_order_id)
                || (cmd.client_order_id.is_some() && cmd.client_order_id == r.client_order_id)
        });
        if found.is_some() {
            return Ok(found);
        }
        let Some(id) = cmd.client_order_id else {
            return Ok(None);
        };
        let since = self
            .cached_order(&id)
            .map(|o| o.ts_init())
            .with_context(|| format!("{id} is not in the cache: no window to look for it in"))?;
        let venue = cmd
            .venue_order_id
            .filter(|v| parse_protection_id(*v).is_none());
        let found =
            Self::find_order(&session.history, &reports, id, venue, since, self.now()).await?;
        Ok(found.map(|(status, _)| status))
    }

    /// The open orders (the pending orders, the positions' stop loss and
    /// take profit): cTrader's reconcile request lists open orders only.
    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let session = self.session()?;
        let open = session.history.reconcile().await?;
        let mut orders = Self::order_reports(&self.reports(session, cmd.ts_init), &open)?;
        orders.retain(|r| cmd.instrument_id.is_none_or(|id| r.instrument_id == id));
        log_receipt(orders.len(), "OrderStatusReport", cmd.log_receipt_level);
        Ok(orders)
    }

    /// The fills of the deals over `[start, end]` (start: the epoch, end:
    /// now).
    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let session = self.session()?;
        let from = cmd.start.unwrap_or_default();
        let to = cmd.end.unwrap_or_else(|| self.now());
        let mut fills = self.fill_reports(session, cmd.ts_init, from, to).await?;
        fills.retain(|f| {
            cmd.instrument_id.is_none_or(|id| f.instrument_id == id)
                && cmd.venue_order_id.is_none_or(|id| f.venue_order_id == id)
        });
        log_receipt(fills.len(), "FillReport", cmd.log_receipt_level);
        Ok(fills)
    }

    /// One report per open cTrader position: the reconcile request lists
    /// every open position, so an instrument without one is flat.
    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let session = self.session()?;
        let open = session.history.reconcile().await?;
        let mut positions = Self::position_reports(&self.reports(session, cmd.ts_init), &open)?;
        positions.retain(|r| cmd.instrument_id.is_none_or(|id| r.instrument_id == id));
        log_receipt(
            positions.len(),
            "PositionStatusReport",
            cmd.log_receipt_level,
        );
        Ok(positions)
    }

    /// The open positions and orders of one reconcile request, and the
    /// fills of the deals since the earliest of: `lookback_mins` ago, the
    /// opening of a position open at IC, the opening of a position the
    /// node's cache holds open. A position's deals go in only when they add
    /// up to it (see `reports::explained`); the report window says whether
    /// all did.
    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = self.now();
        let cached_open = self.earliest_cached_open();
        let session = self.session()?;
        let open = session.history.reconcile().await?;
        let reports = self.reports(session, ts_init);
        let orders = Self::order_reports(&reports, &open)?;
        let positions = Self::position_reports(&reports, &open)?;

        let lookback = lookback_mins
            .map(DurationNanos::try_from_mins)
            .transpose()?
            .map(|lookback| ts_init.saturating_sub(lookback));
        let venue_open = open
            .position
            .iter()
            .filter_map(|p| p.trade_data.open_timestamp)
            .filter_map(|ms| u64::try_from(ms).ok())
            .map(|ms| UnixNanos::new(ms.saturating_mul(1_000_000)))
            .min();
        let start = [lookback, venue_open, cached_open]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(ts_init);
        // One cutoff for the request and the report window: cTrader's, in ms.
        let start = UnixNanos::new(start.as_u64() / 1_000_000 * 1_000_000);
        let deals = session
            .history
            .deals(millis(start), millis(ts_init))
            .await?;
        let (kept, unexplained) = explained(&deals, &open_volumes(&open.position));
        if !unexplained.is_empty() {
            log::warn!(
                "IC deals of positions {unexplained:?} left out: their history starts before {start}"
            );
        }
        let mut fills = Vec::new();
        for deal in kept {
            fills.extend(reports.fill(deal)?);
        }

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.venue(),
            ts_init,
            None,
        );
        log::info!(
            "IC mass status: {} positions, {} open orders, {} fills since {start}",
            positions.len(),
            orders.len(),
            fills.len()
        );
        mass_status.add_order_reports(orders);
        mass_status.add_fill_reports(fills);
        mass_status.add_position_reports(positions);
        mass_status.set_report_window(Some(start), unexplained.is_empty());
        Ok(Some(mass_status))
    }
}

/// `nanos` in whole milliseconds.
fn millis(nanos: UnixNanos) -> i64 {
    i64::try_from(nanos.as_u64() / 1_000_000).unwrap_or(i64::MAX)
}

fn log_receipt(count: usize, kind: &str, level: LogLevel) {
    if let Some(level) = map_log_level_to_filter(level).to_level() {
        log::log!(level, "Received {count} {kind}s from IC");
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache, live::runner::replace_exec_event_sender, messages::ExecutionEvent,
    };
    use nautilus_model::{
        enums::{OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce, TriggerType},
        identifiers::{PositionId, TraderId, VenueOrderId},
        types::{Currency, Money, Price, Quantity},
    };
    use prost::Message as _;
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    use super::*;
    use crate::data::ic::{
        messages::{
            Payload, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
            ProtoOaAccountType, ProtoOaAssetListReq, ProtoOaAssetListRes,
            ProtoOaClosePositionDetail, ProtoOaDeal, ProtoOaDealListReq, ProtoOaDealListRes,
            ProtoOaDealStatus, ProtoOaErrorRes, ProtoOaOrder, ProtoOaOrderStatus,
            ProtoOaOrderTriggerMethod, ProtoOaOrderType, ProtoOaPosition, ProtoOaPositionStatus,
            ProtoOaReconcileReq, ProtoOaReconcileRes, ProtoOaRefreshTokenReq, ProtoOaSymbolByIdReq,
            ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq, ProtoOaSymbolsListRes, ProtoOaTimeInForce,
            ProtoOaTradeData, ProtoOaTradeSide, ProtoOaTraderReq, ProtoOaTraderRes,
        },
        testing::{
            self, ACCOUNT_AUTH_TOKEN_INVALID, ACCOUNT_ID, ASSET_LIST_RES, Action,
            DEAL_LIST_RES_EMPTY, RECONCILE_RES_EMPTY, Received, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES,
            TRADER_RES, accept_auth, replay, reply, token_refused,
        },
    };

    const XAUUSD: &str = "XAUUSD.ICMARKETS";
    const EURUSD: &str = "EURUSD.ICMARKETS";

    /// cTrader's symbol ids on the server the fixtures come from.
    const XAUUSD_SYMBOL_ID: i64 = 41;
    const EURUSD_SYMBOL_ID: i64 = 1;
    const US500_SYMBOL_ID: i64 = 10013;

    /// 2026-10-26T10:00:00Z, in ms.
    const T0: i64 = 1_792_922_400_000;

    /// How the peer answers trade requests and order queries.
    type Answers = Arc<dyn Fn(&ProtoMessage) -> Vec<Action> + Send + Sync>;

    /// What the peer answers to the account, the open positions and the
    /// deals: the captured responses unless a test sets others; trade
    /// requests and order queries as `trades` does.
    #[derive(Clone, Default)]
    struct Script {
        trader: Option<ProtoOaTraderRes>,
        reconcile: Option<ProtoOaReconcileRes>,
        deals: Option<Vec<ProtoOaDeal>>,
        refuse_token: bool,
        other_account: bool,
        trades: Option<Answers>,
    }

    mod order_flow;

    /// A cTrader serving `venue`, and the captured assets, symbols and
    /// details.
    fn peer(venue: Script) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
        move |_, request| {
            let payload = request.payload.as_deref().unwrap_or_default();
            if request.payload_type == ProtoOaAccountAuthReq::TYPE {
                if venue.refuse_token {
                    return vec![replay::<ProtoOaErrorRes>(
                        request,
                        ACCOUNT_AUTH_TOKEN_INVALID,
                    )];
                }
                if venue.other_account {
                    let other = ProtoOaAccountAuthRes {
                        ctid_trader_account_id: ACCOUNT_ID + 1,
                        ..Default::default()
                    };
                    return vec![reply(request, &other)];
                }
            }
            if venue.refuse_token && request.payload_type == ProtoOaRefreshTokenReq::TYPE {
                return vec![token_refused(request)];
            }
            if let Some(actions) = accept_auth(request) {
                return actions;
            }
            if let Some(trades) = venue
                .trades
                .as_ref()
                .filter(|_| order_flow::answered(request))
            {
                return trades(request);
            }
            let answer = match request.payload_type {
                t if t == ProtoOaAssetListReq::TYPE => {
                    replay::<ProtoOaAssetListRes>(request, ASSET_LIST_RES)
                }
                t if t == ProtoOaSymbolsListReq::TYPE => {
                    replay::<ProtoOaSymbolsListRes>(request, SYMBOLS_LIST_RES)
                }
                t if t == ProtoOaSymbolByIdReq::TYPE => {
                    replay::<ProtoOaSymbolByIdRes>(request, SYMBOL_BY_ID_RES)
                }
                t if t == ProtoOaTraderReq::TYPE => match &venue.trader {
                    Some(trader) => reply(request, trader),
                    None => replay::<ProtoOaTraderRes>(request, TRADER_RES),
                },
                t if t == ProtoOaReconcileReq::TYPE => match &venue.reconcile {
                    Some(open) => reply(request, open),
                    None => replay::<ProtoOaReconcileRes>(request, RECONCILE_RES_EMPTY),
                },
                // As on the demo (2026-10-26): a window not ending after
                // its start is refused.
                t if t == ProtoOaDealListReq::TYPE
                    && ProtoOaDealListReq::decode(payload)
                        .is_ok_and(|r| r.from_timestamp >= r.to_timestamp) =>
                {
                    reply(
                        request,
                        &ProtoOaErrorRes {
                            error_code: "INCORRECT_BOUNDARIES".to_owned(),
                            description: Some("Incorrect period boundaries".to_owned()),
                            ..Default::default()
                        },
                    )
                }
                t if t == ProtoOaDealListReq::TYPE => match &venue.deals {
                    Some(deals) => {
                        let asked = ProtoOaDealListReq::decode(payload).unwrap();
                        let (from, to) =
                            (asked.from_timestamp.unwrap(), asked.to_timestamp.unwrap());
                        let deal = deals
                            .iter()
                            .filter(|d| {
                                from <= d.execution_timestamp && d.execution_timestamp <= to
                            })
                            .cloned()
                            .collect();
                        reply(
                            request,
                            &ProtoOaDealListRes {
                                ctid_trader_account_id: ACCOUNT_ID,
                                deal,
                                has_more: false,
                                ..Default::default()
                            },
                        )
                    }
                    None => replay::<ProtoOaDealListRes>(request, DEAL_LIST_RES_EMPTY),
                },
                other => panic!("unexpected request {other}"),
            };
            vec![answer]
        }
    }

    type Events = mpsc::UnboundedReceiver<ExecutionEvent>;

    /// A started client of XAUUSD and EURUSD on the peer of `venue`, its
    /// cache, the node's execution events, the frames received and its
    /// token file's directory.
    fn started(
        venue: Script,
    ) -> (
        IcExecutionClient,
        Rc<RefCell<Cache>>,
        Events,
        Received,
        tempfile::TempDir,
    ) {
        let (url, received) = testing::start(peer(venue));
        let (tx, events) = mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("HELIX-001"),
            ClientId::from("ICMARKETS"),
            Venue::new(IC_VENUE),
            OmsType::Hedging,
            AccountId::from("ICMARKETS-DEMO"),
            AccountType::Margin,
            None,
            cache.clone(),
        );
        let (credential, tokens) = testing::credential();
        let mut client = IcExecutionClient::with_endpoint(
            core,
            url,
            credential,
            vec![InstrumentId::from(XAUUSD), InstrumentId::from(EURUSD)],
        );
        client.start().unwrap();
        (client, cache, events, received, tokens)
    }

    /// Plays the node: the first account state the client publishes goes
    /// into the cache, as the execution engine does; returns it.
    async fn register_account(events: &mut Events, cache: &Rc<RefCell<Cache>>) -> AccountState {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("no execution event within 10 s")
            .expect("the execution events ended");
        let ExecutionEvent::Account(state) = event else {
            panic!("not an account state: {event:?}");
        };
        cache
            .borrow_mut()
            .add_account(AccountAny::from_events(std::slice::from_ref(&state)).unwrap())
            .unwrap();
        state
    }

    /// Connects `client`, the node registering its account; returns the
    /// account state.
    async fn connect(
        client: &mut IcExecutionClient,
        events: &mut Events,
        cache: &Rc<RefCell<Cache>>,
    ) -> AccountState {
        let (connected, state) = tokio::join!(client.connect(), register_account(events, cache));
        connected.unwrap();
        state
    }

    /// Synthetic, as OpenApiModelMessages.proto defines it: an open
    /// position of XAUUSD (`volume` in cents of an ounce), opened at `T0` +
    /// `minutes`.
    fn position(id: i64, side: ProtoOaTradeSide, volume: i64, minutes: i64) -> ProtoOaPosition {
        ProtoOaPosition {
            position_id: id,
            trade_data: ProtoOaTradeData {
                symbol_id: XAUUSD_SYMBOL_ID,
                volume,
                trade_side: side as i32,
                open_timestamp: Some(T0 + minutes * 60_000),
                ..Default::default()
            },
            position_status: ProtoOaPositionStatus::PositionStatusOpen as i32,
            swap: 0,
            utc_last_update_timestamp: Some(T0 + minutes * 60_000 + 500),
            used_margin: Some(1_200_000),
            money_digits: Some(2),
            ..Default::default()
        }
    }

    /// Synthetic: two open positions on XAUUSD, a long of 100 oz with stop
    /// loss and take profit, a short of 50 oz with a stop loss triggered
    /// by the double trade method; a US500 position, out of the run; a
    /// pending EURUSD limit order.
    fn two_gold_positions() -> ProtoOaReconcileRes {
        let mut long = position(101, ProtoOaTradeSide::Buy, 10_000, 0);
        long.price = Some(4012.35);
        long.stop_loss = Some(3990.5);
        long.take_profit = Some(4050.0);
        let mut short = position(102, ProtoOaTradeSide::Sell, 5_000, 10);
        short.price = Some(4020.125);
        short.stop_loss = Some(4040.0);
        short.stop_loss_trigger_method = Some(ProtoOaOrderTriggerMethod::DoubleTrade as i32);
        let mut index = position(103, ProtoOaTradeSide::Buy, 100, 5);
        index.trade_data.symbol_id = US500_SYMBOL_ID;
        index.price = Some(6000.5);
        let limit = ProtoOaOrder {
            order_id: 201,
            trade_data: ProtoOaTradeData {
                symbol_id: EURUSD_SYMBOL_ID,
                volume: 10_000_000,
                trade_side: ProtoOaTradeSide::Buy as i32,
                open_timestamp: Some(T0 + 20 * 60_000),
                ..Default::default()
            },
            order_type: ProtoOaOrderType::Limit as i32,
            order_status: ProtoOaOrderStatus::OrderStatusAccepted as i32,
            limit_price: Some(1.08525),
            client_order_id: Some("O-20261026-100000-001-000-1".to_owned()),
            time_in_force: Some(ProtoOaTimeInForce::GoodTillCancel as i32),
            ..Default::default()
        };
        ProtoOaReconcileRes {
            ctid_trader_account_id: ACCOUNT_ID,
            position: vec![long, short, index],
            order: vec![limit],
            ..Default::default()
        }
    }

    /// Synthetic: a filled deal of XAUUSD, `volume` in cents of an ounce,
    /// at `T0` + `minutes`, charged 1.50 EUR.
    fn deal(
        id: i64,
        position: i64,
        side: ProtoOaTradeSide,
        volume: i64,
        price: f64,
        minutes: i64,
    ) -> ProtoOaDeal {
        ProtoOaDeal {
            deal_id: id,
            order_id: id + 1000,
            position_id: position,
            volume,
            filled_volume: volume,
            symbol_id: XAUUSD_SYMBOL_ID,
            create_timestamp: T0 + minutes * 60_000,
            execution_timestamp: T0 + minutes * 60_000 + 20,
            execution_price: Some(price),
            trade_side: side as i32,
            deal_status: ProtoOaDealStatus::Filled as i32,
            commission: Some(-150),
            money_digits: Some(2),
            ..Default::default()
        }
    }

    /// The deals of `two_gold_positions`: the long opened by one deal; the
    /// short opened by two of 40 oz (its VWAP 4020.125) and reduced by 30;
    /// a position opened and closed in between.
    fn gold_deals() -> Vec<ProtoOaDeal> {
        let mut reduce = deal(4, 102, ProtoOaTradeSide::Buy, 3_000, 4015.0, 30);
        reduce.close_position_detail = Some(ProtoOaClosePositionDetail {
            entry_price: 4020.125,
            closed_volume: Some(3_000),
            ..Default::default()
        });
        vec![
            deal(1, 101, ProtoOaTradeSide::Buy, 10_000, 4012.35, 0),
            deal(2, 104, ProtoOaTradeSide::Buy, 1_000, 4013.0, 2),
            deal(3, 104, ProtoOaTradeSide::Sell, 1_000, 4014.0, 3),
            deal(5, 102, ProtoOaTradeSide::Sell, 4_000, 4020.10, 10),
            deal(6, 102, ProtoOaTradeSide::Sell, 4_000, 4020.15, 11),
            reduce,
        ]
    }

    fn requests_of<M: Payload>(received: &Received) -> Vec<M> {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f)| f.payload_type == M::TYPE)
            .map(|(_, f)| M::decode(f.payload.as_deref().unwrap_or_default()).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn connecting_publishes_the_captured_account_state_and_waits_for_the_node() {
        let (mut client, cache, mut events, _, _tokens) = started(Script::default());

        let state = connect(&mut client, &mut events, &cache).await;

        // As captured: balance 100000000 at moneyDigits 2, in EUR (asset 5).
        let eur = Currency::from("EUR");
        assert_eq!(state.account_id, AccountId::from("ICMARKETS-DEMO"));
        assert_eq!(state.account_type, AccountType::Margin);
        assert_eq!(state.base_currency, Some(eur));
        assert!(state.is_reported);
        assert_eq!(
            state.balances,
            [AccountBalance::new(
                Money::new(1_000_000.0, eur),
                Money::new(0.0, eur),
                Money::new(1_000_000.0, eur),
            )]
        );
        assert_eq!(
            state.margins,
            [MarginBalance::new(
                Money::new(0.0, eur),
                Money::new(0.0, eur),
                None
            )]
        );
        assert!(client.is_connected());
        assert!(client.get_account().is_some());
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn the_margin_of_the_open_positions_is_locked() {
        let venue = Script {
            reconcile: Some(two_gold_positions()),
            ..Script::default()
        };
        let (mut client, cache, mut events, _, _tokens) = started(venue);

        let state = connect(&mut client, &mut events, &cache).await;

        // Three positions at 12,000.00 EUR each, the one out of the run too:
        // margin is the account's.
        let eur = Currency::from("EUR");
        assert_eq!(
            state.balances,
            [AccountBalance::new(
                Money::new(1_000_000.0, eur),
                Money::new(36_000.0, eur),
                Money::new(964_000.0, eur),
            )]
        );
        assert_eq!(
            state.margins,
            [MarginBalance::new(
                Money::new(36_000.0, eur),
                Money::new(36_000.0, eur),
                None
            )]
        );
        client.disconnect().await.unwrap();
    }

    /// Synthetic: the captured trader made a netting account.
    #[tokio::test]
    async fn a_netting_account_is_refused_at_connect() {
        let mut trader = ProtoOaTraderRes::decode(TRADER_RES).unwrap();
        trader.trader.set_account_type(ProtoOaAccountType::Netted);
        let venue = Script {
            trader: Some(trader),
            ..Script::default()
        };
        let (mut client, _cache, mut events, _, _tokens) = started(venue);

        let err = client.connect().await.unwrap_err().to_string();

        assert!(
            err.contains("the IC account is NETTED, not HEDGED"),
            "{err}"
        );
        assert!(!client.is_connected());
        assert!(events.try_recv().is_err(), "an account state was published");
    }

    /// The refusal is the fixture `account_auth_token_invalid`, the
    /// renewal's synthetic.
    #[tokio::test]
    async fn an_expired_token_ctrader_refuses_to_renew_is_refused_at_connect() {
        let venue = Script {
            refuse_token: true,
            ..Script::default()
        };
        let (mut client, _cache, _events, _, _tokens) = started(venue);

        let err = format!("{:#}", client.connect().await.unwrap_err());

        assert!(
            err.contains("cTrader refused to renew the access token (CH_ACCESS_TOKEN_INVALID)"),
            "{err}"
        );
        assert!(err.contains("Playground"), "{err}");
        assert!(!client.is_connected());
    }

    /// Synthetic: the account auth response of another account.
    #[tokio::test]
    async fn another_account_is_refused_at_connect() {
        let venue = Script {
            other_account: true,
            ..Script::default()
        };
        let (mut client, _cache, _events, _, _tokens) = started(venue);

        let err = format!("{:#}", client.connect().await.unwrap_err());

        assert!(err.contains("another account than IC_ACCOUNT_ID"), "{err}");
        assert!(!client.is_connected());
    }

    /// Connects on `venue` and generates the startup mass status.
    async fn mass_status(
        venue: Script,
        lookback_mins: Option<u64>,
    ) -> (ExecutionMassStatus, Received) {
        let (mut client, cache, mut events, received, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        let mass_status = client
            .generate_mass_status(lookback_mins)
            .await
            .unwrap()
            .unwrap();
        client.disconnect().await.unwrap();
        (mass_status, received)
    }

    fn gold_venue() -> Script {
        Script {
            reconcile: Some(two_gold_positions()),
            deals: Some(gold_deals()),
            ..Script::default()
        }
    }

    fn ns(ms: i64) -> UnixNanos {
        UnixNanos::new(u64::try_from(ms).unwrap() * 1_000_000)
    }

    #[tokio::test]
    async fn each_ctrader_position_is_one_position_report_by_its_position_id() {
        let (mass_status, _) = mass_status(gold_venue(), None).await;

        let reports = mass_status.position_reports();
        let gold = &reports[&InstrumentId::from(XAUUSD)];
        let positions: Vec<_> = gold
            .iter()
            .map(|r| {
                (
                    r.venue_position_id,
                    r.position_side,
                    r.quantity,
                    r.avg_px_open,
                    r.ts_last,
                )
            })
            .collect();
        assert_eq!(
            positions,
            [
                (
                    Some(PositionId::from("101")),
                    PositionSide::Long,
                    Quantity::from(100),
                    Some(Decimal::new(401_235, 2)),
                    ns(T0 + 500),
                ),
                (
                    Some(PositionId::from("102")),
                    PositionSide::Short,
                    Quantity::from(50),
                    Some(Decimal::new(4_020_125, 3)),
                    ns(T0 + 10 * 60_000 + 500),
                ),
            ]
        );
        assert_eq!(reports.len(), 1, "US500 is out of the run: {reports:?}");
        assert_eq!(mass_status.account_id, AccountId::from("ICMARKETS-DEMO"));
    }

    #[tokio::test]
    async fn stop_loss_and_take_profit_are_open_reduce_only_orders_of_their_position() {
        let (mass_status, _) = mass_status(gold_venue(), None).await;

        let orders = mass_status.order_reports();
        let long_sl = &orders[&VenueOrderId::from("101-SL")];
        assert_eq!(long_sl.order_type, OrderType::StopMarket);
        assert_eq!(long_sl.order_side, Some(OrderSide::Sell));
        assert_eq!(long_sl.quantity, Quantity::from(100));
        assert_eq!(long_sl.trigger_price, Some(Price::from("3990.50")));
        assert_eq!(long_sl.trigger_type, Some(TriggerType::BidAsk));
        assert_eq!(long_sl.venue_position_id, Some(PositionId::from("101")));
        assert!(long_sl.reduce_only);
        assert_eq!(long_sl.order_status, OrderStatus::Accepted);
        assert_eq!(long_sl.time_in_force, TimeInForce::Gtc);
        let long_tp = &orders[&VenueOrderId::from("101-TP")];
        assert_eq!(long_tp.order_type, OrderType::Limit);
        assert_eq!(long_tp.order_side, Some(OrderSide::Sell));
        assert_eq!(long_tp.price, Some(Price::from("4050.00")));
        assert_eq!(long_tp.venue_position_id, Some(PositionId::from("101")));
        assert!(long_tp.reduce_only);
        let short_sl = &orders[&VenueOrderId::from("102-SL")];
        assert_eq!(short_sl.order_side, Some(OrderSide::Buy));
        assert_eq!(short_sl.quantity, Quantity::from(50));
        assert_eq!(short_sl.trigger_price, Some(Price::from("4040.00")));
        assert_eq!(short_sl.trigger_type, Some(TriggerType::DoubleBidAsk));
        assert!(!orders.contains_key(&VenueOrderId::from("102-TP")));
        assert!(!orders.contains_key(&VenueOrderId::from("103-SL")));
    }

    #[tokio::test]
    async fn a_pending_order_is_an_open_order_report() {
        let (mass_status, _) = mass_status(gold_venue(), None).await;

        let orders = mass_status.order_reports();
        let limit = &orders[&VenueOrderId::from("201")];
        assert_eq!(limit.instrument_id, InstrumentId::from(EURUSD));
        assert_eq!(limit.order_type, OrderType::Limit);
        assert_eq!(limit.order_side, Some(OrderSide::Buy));
        assert_eq!(limit.quantity, Quantity::from(100_000));
        assert_eq!(limit.price, Some(Price::from("1.08525")));
        assert_eq!(limit.order_status, OrderStatus::Accepted);
        assert_eq!(
            limit.client_order_id,
            Some("O-20261026-100000-001-000-1".into())
        );
        assert_eq!(orders.len(), 4, "{:?}", orders.keys());
    }

    #[tokio::test]
    async fn the_deals_since_the_first_open_position_are_its_fills_by_position() {
        let (mass_status, received) = mass_status(gold_venue(), None).await;

        let fills = mass_status.fill_reports();
        let mut trades: Vec<_> = fills
            .values()
            .flatten()
            .map(|f| {
                (
                    f.trade_id.to_string(),
                    f.venue_order_id.to_string(),
                    f.venue_position_id.map(|p| p.to_string()),
                    f.order_side,
                    f.last_qty,
                    f.last_px,
                )
            })
            .collect();
        trades.sort();
        let row = |trade: &str, order: &str, position: &str, side, qty: u64, px: &str| {
            (
                trade.to_owned(),
                order.to_owned(),
                Some(position.to_owned()),
                side,
                Quantity::from(qty),
                Price::from(px),
            )
        };
        assert_eq!(
            trades,
            [
                row("1", "1001", "101", OrderSide::Buy, 100, "4012.35"),
                row("2", "1002", "104", OrderSide::Buy, 10, "4013.00"),
                row("3", "1003", "104", OrderSide::Sell, 10, "4014.00"),
                row("4", "1004", "102", OrderSide::Buy, 30, "4015.00"),
                row("5", "1005", "102", OrderSide::Sell, 40, "4020.10"),
                row("6", "1006", "102", OrderSide::Sell, 40, "4020.15"),
            ]
        );
        // cTrader's charge of -150 at moneyDigits 2, a cost to Nautilus.
        let first = &fills[&VenueOrderId::from("1001")][0];
        assert_eq!(first.commission, Money::new(1.5, Currency::from("EUR")));
        assert_eq!(first.ts_event, ns(T0 + 20));
        let asked: Vec<ProtoOaDealListReq> = requests_of(&received);
        assert_eq!(asked[0].from_timestamp, Some(T0));
        assert_eq!(mass_status.lookback_start(), Some(ns(T0)));
        assert!(mass_status.reports_complete());
    }

    #[tokio::test]
    async fn the_deals_of_a_position_opened_before_the_window_are_left_out() {
        // Position 105 closed at T0 + 5 min; its opening, before the
        // window, is not among the deals.
        let mut deals = gold_deals();
        deals.push(deal(7, 105, ProtoOaTradeSide::Sell, 2_000, 4011.0, 5));
        let venue = Script {
            deals: Some(deals),
            ..gold_venue()
        };

        let (mass_status, _) = mass_status(venue, None).await;

        let positions: Vec<String> = mass_status
            .fill_reports()
            .values()
            .flatten()
            .filter_map(|f| f.venue_position_id.map(|p| p.to_string()))
            .collect();
        assert!(!positions.contains(&"105".to_owned()), "{positions:?}");
        assert_eq!(positions.len(), 6, "{positions:?}");
        assert!(!mass_status.reports_complete());
    }

    #[tokio::test]
    async fn a_longer_lookback_reads_the_deals_from_its_start() {
        let venue = gold_venue();
        let (mut client, cache, mut events, received, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        let before = client.now();

        let mass_status = client
            .generate_mass_status(Some(60 * 24 * 365 * 10))
            .await
            .unwrap()
            .unwrap();

        let asked: Vec<ProtoOaDealListReq> = requests_of(&received);
        let from = asked[0].from_timestamp.unwrap();
        let ten_years_ms = 10 * 365 * 24 * 3_600_000;
        assert!(from < T0, "{from}");
        assert!(
            (millis(before) - ten_years_ms - from).abs() < 60_000,
            "{from}"
        );
        assert_eq!(mass_status.lookback_start(), Some(ns(from)));
        client.disconnect().await.unwrap();
    }

    /// After a restart on a persisted cache: a position the cache holds
    /// open, opened an hour before the venue's, may have closed while the
    /// node was down; its deals are read from its opening.
    #[tokio::test]
    async fn the_deals_are_read_from_the_opening_of_a_position_the_cache_holds_open() {
        use nautilus_model::{
            events::OrderEventAny,
            orders::{OrderTestBuilder, stubs::TestOrderEventStubs},
            position::Position,
        };

        let (mut client, cache, mut events, received, _tokens) = started(gold_venue());
        connect(&mut client, &mut events, &cache).await;
        let gold = client.session().unwrap().instruments[&XAUUSD_SYMBOL_ID].clone();
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(10))
            .build();
        let opened = ns(T0 - 3_600_000);
        let OrderEventAny::Filled(fill) = TestOrderEventStubs::filled(
            &order,
            &gold,
            None,
            Some(PositionId::from("99")),
            Some(Price::from("4000.00")),
            None,
            None,
            None,
            Some(opened),
            Some(AccountId::from("ICMARKETS-DEMO")),
        ) else {
            panic!("not a fill");
        };
        cache
            .borrow_mut()
            .add_position(&Position::new(&gold, fill), OmsType::Hedging)
            .unwrap();

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let asked: Vec<ProtoOaDealListReq> = requests_of(&received);
        assert_eq!(asked[0].from_timestamp, Some(T0 - 3_600_000));
        assert_eq!(mass_status.lookback_start(), Some(opened));
        client.disconnect().await.unwrap();
    }

    /// The captured reconcile and deal list answers of the demo account,
    /// with nothing open and no deal.
    #[tokio::test]
    async fn an_account_with_nothing_open_reports_nothing() {
        let (mass_status, received) = mass_status(Script::default(), None).await;

        assert!(mass_status.position_reports().is_empty());
        assert!(mass_status.order_reports().is_empty());
        assert!(mass_status.fill_reports().is_empty());
        let reconcile: Vec<ProtoOaReconcileReq> = requests_of(&received);
        assert_eq!(reconcile.last().unwrap().ctid_trader_account_id, ACCOUNT_ID);
        assert_eq!(
            reconcile.last().unwrap().return_protection_orders,
            Some(false)
        );
    }

    #[tokio::test]
    async fn the_periodic_checks_get_the_open_positions_and_orders_of_an_instrument() {
        use nautilus_common::messages::execution::{
            GenerateOrderStatusReportsBuilder, GeneratePositionStatusReportsBuilder,
        };

        let (mut client, cache, mut events, _, _tokens) = started(gold_venue());
        connect(&mut client, &mut events, &cache).await;
        let gold = InstrumentId::from(XAUUSD);

        let positions = client
            .generate_position_status_reports(
                &GeneratePositionStatusReportsBuilder::default()
                    .ts_init(client.now())
                    .instrument_id(Some(gold))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        let orders = client
            .generate_order_status_reports(
                &GenerateOrderStatusReportsBuilder::default()
                    .ts_init(client.now())
                    .open_only(true)
                    .instrument_id(Some(gold))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(positions.len(), 2);
        let mut ids: Vec<String> = orders
            .iter()
            .map(|r| r.venue_order_id.to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, ["101-SL", "101-TP", "102-SL"]);
        client.disconnect().await.unwrap();
    }
}
