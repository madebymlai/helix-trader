//! The execution client for the live node: Nautilus' `ExecutionClient` on a cTrader session.
//!
//! On connect the client:
//! - Authenticates.
//! - Checks that the credential's account is hedging (see `common::account`).
//! - Loads its instruments.
//! - Listens to the account's trade events (see `dispatch`).
//! - Publishes the account state.
//!
//! It reports connected once the node's cache holds the account. The venue is hedging
//! (`OmsType::Hedging`). Each cTrader position is one Nautilus position, and its `positionId` is
//! the venue position id (see `reports`).
//!
//! Orders (see `orders`):
//! - A submission is checked, tracked, reported submitted and sent.
//! - A bracket's protections go on its entry, then live on the position.
//! - An order naming a position closes it in part or whole, or adds to it.
//! - A modification amends a pending order or moves a protection. A protection moves through
//!   `ProtoOAAmendPositionSLTPReq`, which carries both prices. cTrader drops a price left out, and
//!   the other price is sent as last asked.
//! - A cancel cancels a pending order or removes a protection.
//!
//! A failed request takes Nautilus' policy for its `CommandFailure` class (see `failure`):
//! - A new order, or a close, that the session never sent is rejected. A modify or cancel is
//!   denied.
//! - A request that cTrader refused is rejected.
//! - A request sent but unanswered, or with an unknown outcome, stays in flight
//!   (`retain_unresolved_submissions`) until its events, a query or reconciliation resolve it.
//! - A query that finds no such order at the broker rejects it.
//!
//! Reconciliation reads:
//! - The reconcile request: open positions, their stop loss and take profit, and pending orders.
//! - The deal list, for fills.
//! - The deals that opened the positions, which is where the deal list starts.
//! - The order list: orders and protections closed over the window.
//!
//! For a single order it reads that order's details instead.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter, SocketControl, execution::failure::CommandFailure,
    task::TaskGroup,
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, ContingencyType, OmsType, OrderType, TimeInForce},
    events::{AccountState, OrderDeniedReason},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, Venue, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Price, Quantity},
};

use self::{
    dispatch::{Dispatch, Output, Role, Want},
    orders::{Leg, Placement, Protection, Tag, parse_protection_id},
    reports::{Reports, explained, open_volumes},
};
use super::{
    common::{
        account::CTraderAccount,
        messages::{
            ProtoOaAmendOrderReq, ProtoOaAmendPositionSltpReq, ProtoOaCancelOrderReq, ProtoOaDeal,
            ProtoOaOrder, ProtoOaOrderStatus, ProtoOaOrderType, ProtoOaReconcileRes, TradeRequest,
        },
    },
    config::{CTraderConnection, CTraderCredential},
    session::CTraderSession,
};

mod dispatch;
mod failure;
mod orders;
mod reports;

/// The socket's label in Nautilus' socket state events and reconnect commands.
///
/// The data client's label sits beside it under the same client id.
const SOCKET_ENDPOINT: &str = "ctrader-open-api-trading";

const ACCOUNT_REGISTERED_TIMEOUT: Duration = Duration::from_secs(30);

const TASK_GRACE: Duration = Duration::from_secs(1);
const TASK_FORCE: Duration = Duration::from_secs(2);

/// How far before an order's creation a query looks for it.
///
/// The venue's and node's clocks may differ.
const QUERY_MARGIN_MS: i64 = 60_000;

fn absence(venue: Venue) -> String {
    format!("{venue} holds no such order: its request never reached {venue}")
}

/// The execution client on the connection's venue.
#[derive(Debug)]
pub(super) struct CTraderExecutionClient {
    core: ExecutionClientCore,
    url: String,
    credential: CTraderCredential,
    instrument_ids: Vec<InstrumentId>,
    emitter: ExecutionEventEmitter,
    clock: &'static AtomicTime,
    socket: SocketControl,
    session: Option<Session>,
    session_tasks: TaskGroup,
    command_tasks: TaskGroup,
}

/// A connected session.
#[derive(Debug)]
struct Session {
    ctrader: Arc<CTraderSession>,
    instruments: HashMap<i64, InstrumentAny>,
    symbols: HashMap<InstrumentId, i64>,
    account: CTraderAccount,
    dispatch: Arc<Mutex<Dispatch>>,
}

impl Session {
    fn dispatch(&self) -> MutexGuard<'_, Dispatch> {
        self.dispatch.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl CTraderExecutionClient {
    /// `core` is hedging, with a margin account, on the connection's venue.
    #[must_use]
    pub(super) fn new(
        core: ExecutionClientCore,
        connection: &CTraderConnection,
        instrument_ids: Vec<InstrumentId>,
    ) -> Self {
        let url = connection.environment.url().to_owned();
        Self::with_endpoint(
            core,
            url,
            connection.credential.clone(),
            instrument_ids,
            get_atomic_clock_realtime(),
        )
    }

    fn with_endpoint(
        core: ExecutionClientCore,
        url: String,
        credential: CTraderCredential,
        instrument_ids: Vec<InstrumentId>,
        clock: &'static AtomicTime,
    ) -> Self {
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            None,
        );

        // Bound when the factory creates the client; `start` binds it again, authoritatively
        if let Some(sender) = try_get_exec_event_sender() {
            emitter.set_sender(sender);
        }
        let socket = SocketControl::new(core.client_id, Some(core.venue), SOCKET_ENDPOINT);
        Self {
            core,
            url,
            credential,
            instrument_ids,
            emitter,
            clock,
            socket,
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

    async fn open_session(&self) -> anyhow::Result<(Session, AccountState)> {
        let (session, _spots) = CTraderSession::connect_to(
            &self.url,
            self.core.venue,
            self.credential.clone(),
            Some(self.socket.clone()),
        )
        .await?;
        let opened = async {
            let trades = session.trade_events()?;
            let account = session.account().await?;
            let symbols = session.symbols(&self.instrument_ids).await?;
            let open = session.reconcile().await?;
            let (balances, margins) = account.state(&open.position)?;
            Ok::<_, anyhow::Error>((trades, account, symbols, balances, margins))
        }
        .await;
        let (mut trades, account, symbols, balances, margins) = match opened {
            Ok(opened) => opened,
            Err(e) => {
                session.disconnect().await;
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
            ctrader: Arc::new(session),
            symbols: instruments.iter().map(|(id, i)| (i.id(), *id)).collect(),
            instruments,
            account,
            dispatch,
        };
        Ok((session, state))
    }

    fn account_state(
        &self,
        account: &CTraderAccount,
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

    /// Returns a flat report for each position in `cached` that cTrader does not hold open.
    ///
    /// `held` says what cTrader holds open (see `open_volumes`). Only positions of a symbol the run
    /// loaded get a report. The reconcile request lists every open position.
    fn flats(
        session: &Session,
        reports: &Reports<'_>,
        cached: &[CachedOpen],
        held: &HashMap<i64, i64>,
    ) -> Vec<PositionStatusReport> {
        cached
            .iter()
            .filter(|c| !held.contains_key(&c.position))
            .filter_map(|gone| {
                let symbol = session.symbols.get(&gone.instrument_id)?;
                let instrument = session.instruments.get(symbol)?;
                Some(reports.flat(instrument, gone.position))
            })
            .collect()
    }

    /// Builds only the fills of the deals `cmd` covers.
    ///
    /// A deal outside `cmd` never fails the request (see `reports::commission`). This suits
    /// Nautilus' targeted order recovery, which asks for the fills of one order.
    async fn fill_reports(
        &self,
        session: &Session,
        cmd: &GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let from = cmd.start.unwrap_or_default();
        let to = cmd.end.unwrap_or_else(|| self.now());
        let deals = session
            .ctrader
            .deals(orders::millis(from), orders::millis(to))
            .await?;
        let asked = |deal: &&ProtoOaDeal| {
            cmd.instrument_id
                .is_none_or(|id| session.symbols.get(&id) == Some(&deal.symbol_id))
                && cmd
                    .venue_order_id
                    .is_none_or(|id| id.as_str() == deal.order_id.to_string())
        };
        let reports = self.reports(session, cmd.ts_init);
        let mut fills = Vec::new();
        for deal in deals.iter().filter(asked) {
            fills.extend(reports.fill(deal)?);
        }
        Ok(fills)
    }

    /// Lists the positions the cache holds open.
    ///
    /// A restart reads the deals from the positions' openings. So a position closed while the node
    /// was down gets its closing fills. A position with no cTrader id has no deal there.
    fn cached_open(&self) -> Vec<CachedOpen> {
        let cache = self.core.cache();
        cache
            .positions_open(None, None, None, Some(&self.core.account_id), None)
            .iter()
            .filter_map(|p| {
                Some(CachedOpen {
                    position: p.id.as_str().parse().ok()?,
                    instrument_id: p.instrument_id,
                    opened: orders::millis(p.ts_opened),
                })
            })
            .collect()
    }

    /// Returns a minute before the earliest creation of the cached open or in-flight orders.
    ///
    /// Orders the broker lists open (`listed`) are left out. A restart reads the orders from that
    /// time. So an order the broker closed while the node was down, or never answered for, gets its
    /// report. A long-lived order that the broker lists open has its report already. It would pull
    /// every start's window back to its creation.
    fn earliest_cached_order(&self, listed: &[OrderStatusReport]) -> Option<UnixNanos> {
        let cache = self.core.cache();
        let venue = Some(&self.core.venue);
        let open = cache.orders_open(venue, None, None, None, None);
        let in_flight = cache.orders_inflight(venue, None, None, None, None);
        let margin = DurationNanos::from_millis(QUERY_MARGIN_MS.unsigned_abs());
        open.iter()
            .chain(&in_flight)
            .filter(|o| {
                !listed.iter().any(|r| {
                    o.venue_order_id() == Some(r.venue_order_id)
                        || r.client_order_id == Some(o.client_order_id())
                })
            })
            .map(|o| o.ts_init().saturating_sub(margin))
            .min()
    }

    /// A position's opening is its first deal's execution. That can come a millisecond after the
    /// deal's creation, which is the time cTrader's deal list filters on. Only the positions opened
    /// within `QUERY_MARGIN_MS` of the first are asked for, one request each. A later position's
    /// opening deal was created moments before it executed, so not before the first's. Asking for
    /// every position would cost each a historical request's 250 ms, on a startup that Nautilus
    /// times out.
    async fn earliest_opening_deal(
        session: &Session,
        openings: &BTreeMap<i64, i64>,
    ) -> anyhow::Result<Option<i64>> {
        let Some(first) = openings.values().min().copied() else {
            return Ok(None);
        };
        let mut earliest = None;

        for (&position, &opened) in openings
            .iter()
            .filter(|(_, opened)| **opened <= first + QUERY_MARGIN_MS)
        {
            let deals = session.ctrader.position_deals(position, opened).await?;
            earliest = deals
                .iter()
                .map(|d| d.create_timestamp)
                .chain(earliest)
                .min();
        }
        Ok(earliest)
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

    fn begin_shutdown(&self) {
        self.session_tasks.begin_shutdown();
        self.command_tasks.begin_shutdown();
        if let Some(session) = &self.session {
            session.ctrader.begin_shutdown();
        }
    }

    async fn teardown(&mut self) -> anyhow::Result<()> {
        self.begin_shutdown();
        let (session, command) = tokio::join!(
            self.session_tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
            self.command_tasks.finish_shutdown(TASK_GRACE, TASK_FORCE),
        );

        if let Some(open) = self.session.take() {
            // The tasks are done, so this is the last handle
            if let Ok(session) = Arc::try_unwrap(open.ctrader) {
                session.disconnect().await;
            }
        }
        let venue = self.core.venue;
        session.map_err(|e| anyhow::anyhow!("stopping the {venue} trade event task: {e}"))?;
        command.map_err(|e| anyhow::anyhow!("stopping the {venue} command tasks: {e}"))?;
        Ok(())
    }

    /// A failed frame takes Nautilus' policy for its class (see `failure::sending`,
    /// `Dispatch::sending`).
    fn send<Req: TradeRequest>(
        &self,
        session: &Session,
        request: Req,
        tag: Tag,
        commands: Vec<(ClientOrderId, UUID4)>,
    ) {
        let sent = session.ctrader.trade(request, tag.to_string());
        let dispatch = Arc::clone(&session.dispatch);
        let emitter = self.emitter.clone();
        let (clock, venue) = (self.clock, self.core.venue);
        self.spawn_command("trade request", async move {
            let sending = failure::sending(sent.await, venue);
            let outputs = dispatch
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .sending(tag, &commands, sending, clock.get_time_ns());
            emit(&emitter, outputs);
            Ok(())
        });
    }

    fn fail_now(&self, session: &Session, tag: Tag, command_id: UUID4, reason: String) {
        let commands = [(tag.client_order_id(), command_id)];
        let outputs =
            session
                .dispatch()
                .fail(tag, &commands, CommandFailure::NotSent(reason), self.now());
        emit(&self.emitter, outputs);
    }

    fn symbol_id(session: &Session, instrument_id: InstrumentId) -> Result<i64, OrderDeniedReason> {
        session
            .symbols
            .get(&instrument_id)
            .copied()
            .ok_or(OrderDeniedReason::InstrumentNotFound { instrument_id })
    }

    fn place(
        &self,
        session: &Session,
        order: &OrderAny,
        cmd_position: Option<PositionId>,
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
            Placement::New(request) => self.send(session, request, Tag::New(id), Vec::new()),
            Placement::Close(request) => self.send(session, request, Tag::Close(id), Vec::new()),
        }
        Ok(())
    }

    /// Submits a bracket.
    ///
    /// The protections are the entry's children. The stop loss is a stop market order and the take
    /// profit is a limit order, each GTC.
    fn submit_bracket(
        &self,
        session: &Session,
        orders: &[OrderAny],
        cmd_position: Option<PositionId>,
    ) -> Result<(), OrderDeniedReason> {
        let unsupported = |detail: &str| OrderDeniedReason::UnsupportedOrderList {
            detail: detail.to_owned(),
        };
        let [entry] = orders
            .iter()
            .filter(|o| o.contingency_type() == Some(ContingencyType::Oto))
            .collect::<Vec<_>>()[..]
        else {
            return Err(unsupported("cTrader takes a bracket of one entry"));
        };
        let mut protection = Protection::default();
        let (mut stop_loss, mut take_profit) = (None, None);

        for leg in orders
            .iter()
            .filter(|o| o.client_order_id() != entry.client_order_id())
        {
            if leg.time_in_force() != TimeInForce::Gtc {
                return Err(OrderDeniedReason::UnsupportedTimeInForce(
                    leg.time_in_force(),
                ));
            }
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
                        "cTrader protects an entry with one stop market stop loss and one limit \
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

    fn cached_order(&self, id: &ClientOrderId) -> Option<OrderAny> {
        self.core.cache().order_owned(id)
    }

    /// Returns the protection's price.
    ///
    /// That is the change the dispatch wants, else the cached price. Returns `None` if the
    /// protection is not open in the cache.
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

    fn amend_protections(&self, session: &Session, position: i64) -> ProtoOaAmendPositionSltpReq {
        let price = |leg| self.asked_protection(session, position, leg);
        ProtoOaAmendPositionSltpReq {
            position_id: position,
            stop_loss: price(Leg::StopLoss).map(|p| p.as_f64()),
            take_profit: price(Leg::TakeProfit).map(|p| p.as_f64()),
            ..Default::default()
        }
    }

    fn amend_order(
        order: &OrderAny,
        venue: i64,
        quantity: Quantity,
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

    fn modify(&self, session: &Session, cmd: &ModifyOrder) -> Result<(), String> {
        let id = cmd.client_order_id;
        let commands = || vec![(id, cmd.command_id)];
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
            self.send(session, request, Tag::Protect(id), commands());
            return Ok(());
        }
        let tracked_role = session.dispatch().tracked(id).map(|t| t.role);
        if let Some(Role::Leg { entry, leg }) = tracked_role {
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
            self.send(session, request, Tag::Protect(id), commands());
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
        self.send(session, request, Tag::Amend(id), commands());
        Ok(())
    }

    /// Cancels pending orders and protections.
    ///
    /// A pending order is cancelled by its id. Protections are cancelled by one amendment of their
    /// position each. A pending entry's protections are cancelled by its amendment.
    fn cancel(&self, session: &Session, cancels: &[(ClientOrderId, UUID4)]) {
        let mut protections: HashMap<i64, Vec<(ClientOrderId, UUID4)>> = HashMap::new();

        for &(id, command_id) in cancels {
            let outcome = (|| {
                let order = self
                    .cached_order(&id)
                    .ok_or_else(|| format!("{id} is not in the cache"))?;
                session.dispatch().adopt(&order);
                if let Some((position, _)) = order.venue_order_id().and_then(parse_protection_id) {
                    session.dispatch().want(id, Want::Remove);
                    protections
                        .entry(position)
                        .or_default()
                        .push((id, command_id));
                    return Ok(());
                }

                // Bound first: the dispatch's lock must not outlive it
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
                    self.send(session, request, Tag::Unprotect(id), vec![(id, command_id)]);
                    return Ok(());
                }
                let venue = pending_venue_id(&order)?;
                let request = ProtoOaCancelOrderReq {
                    order_id: venue,
                    ..Default::default()
                };
                self.send(session, request, Tag::Cancel(id), vec![(id, command_id)]);
                Ok::<_, String>(())
            })();

            if let Err(reason) = outcome {
                self.fail_now(session, Tag::Cancel(id), command_id, reason);
            }
        }

        for (position, legs) in protections {
            let request = self.amend_protections(session, position);
            self.send(session, request, Tag::Unprotect(legs[0].0), legs);
        }
    }

    async fn find_order(
        ctrader: &CTraderSession,
        reports: &Reports<'_>,
        id: ClientOrderId,
        venue: Option<VenueOrderId>,
        since: UnixNanos,
        now: UnixNanos,
    ) -> anyhow::Result<Held> {
        let order_id = match venue.and_then(|v| v.as_str().parse::<i64>().ok()) {
            Some(order_id) => Some(order_id),
            None => {
                let close = Tag::Close(id).to_string();
                let orders = ctrader
                    .orders(
                        orders::millis(since) - QUERY_MARGIN_MS,
                        orders::millis(now) + QUERY_MARGIN_MS,
                    )
                    .await?;
                orders
                    .iter()
                    .find(|o| is_order(o, id, &close))
                    .map(|o| o.order_id)
            }
        };
        let Some(order_id) = order_id else {
            return Ok(Held::Nothing);
        };
        let details = ctrader
            .order_details(order_id)
            .await
            .with_context(|| format!("requesting {} order {order_id}", ctrader.venue()))?;
        let Some(mut status) = reports.order(&details.order)? else {
            return Ok(Held::Unreported);
        };
        status.client_order_id = Some(id);
        let mut fills = Vec::new();
        for deal in &details.deal {
            fills.extend(reports.fill(deal)?);
        }
        Ok(Held::Order(status, fills))
    }
}

struct CachedOpen {
    position: i64,
    instrument_id: InstrumentId,
    opened: i64,
}

#[expect(clippy::large_enum_variant, reason = "one per query, consumed at once")]
enum Held {
    Order(OrderStatusReport, Vec<FillReport>),
    Nothing,
    Unreported,
}

fn is_order(order: &ProtoOaOrder, id: ClientOrderId, close: &str) -> bool {
    order
        .client_order_id
        .as_deref()
        .is_some_and(|c| c == id.as_str() || c == close)
}

fn leg_price(order: &OrderAny, leg: Leg) -> Option<Price> {
    match leg {
        Leg::StopLoss => order.trigger_price(),
        Leg::TakeProfit => order.price(),
    }
}

fn pending_venue_id(order: &OrderAny) -> Result<i64, String> {
    order
        .venue_order_id()
        .and_then(|v| v.as_str().parse().ok())
        .ok_or_else(|| {
            format!(
                "{} is not yet accepted by {}",
                order.client_order_id(),
                order.instrument_id().venue
            )
        })
}

fn emit(emitter: &ExecutionEventEmitter, outputs: Vec<Output>) {
    for output in outputs {
        match output {
            Output::Event(event) => emitter.send_order_event(event),
            Output::Report(report) => emitter.send_execution_report(report),
        }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for CTraderExecutionClient {
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
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    /// Keeps an order in flight if it was sent without an answer (a disconnect).
    ///
    /// It stays in flight until the broker's events, a query or reconciliation tell its outcome.
    fn retain_unresolved_submissions(&self) -> bool {
        true
    }

    /// Requires fills to come from the broker.
    ///
    /// The fills are the broker's deals, with cTrader's deal id and commission. A fill inferred
    /// from the order's status would not carry them.
    fn requires_authoritative_fills(&self) -> bool {
        true
    }

    /// Publishes an account state of `balances` and `margins` in the account's currency.
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

        // The node logs a failed connect without its reason: log it here
        let (client_id, venue) = (self.core.client_id, self.core.venue);
        let failed = |e: anyhow::Error| {
            log::error!("{client_id} failed to connect: {e:#}");
            e
        };

        if self.session.is_some() || !self.session_tasks.is_open() || !self.command_tasks.is_open()
        {
            self.teardown().await.map_err(failed)?;
            self.session_tasks.start_generation().map_err(|e| {
                failed(anyhow::anyhow!(
                    "reopening the {venue} trade event task: {e}"
                ))
            })?;
            self.command_tasks
                .start_generation()
                .map_err(|e| failed(anyhow::anyhow!("reopening the {venue} command tasks: {e}")))?;
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

    /// Submits a bracket as one order that carries its stop loss and take profit.
    ///
    /// A list without contingencies is submitted as its orders. Any other list is denied.
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
            self.emitter.emit_order_modify_denied_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                &reason,
                self.now(),
                Some(cmd.command_id),
            );
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let session = self.session()?;
        self.cancel(session, &[(cmd.client_order_id, cmd.command_id)]);
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let session = self.session()?;
        let cancels: Vec<(ClientOrderId, UUID4)> = self
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
            .map(|order| (order.client_order_id(), cmd.command_id))
            .collect();
        self.cancel(session, &cancels);
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let session = self.session()?;
        let cancels: Vec<(ClientOrderId, UUID4)> = cmd
            .cancels
            .iter()
            .map(|c| (c.client_order_id, c.command_id))
            .collect();
        self.cancel(session, &cancels);
        Ok(())
    }

    /// Queries an order at the broker.
    ///
    /// If the broker's order list for the window does not hold the order, its request never reached
    /// the broker. The order is then reported rejected, with an entry's legs, unless the request
    /// may still wait in the session's queue (see `Dispatch::absent`). A protection (`-SL`, `-TP`)
    /// or a bracket's leg is not queried. cTrader holds it on its entry or position, never as an
    /// order of its own. So its absence from the order list concludes nothing.
    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let session = self.session()?;
        let id = cmd.client_order_id;
        let order = self.cached_order(&id);
        if order
            .as_ref()
            .is_some_and(|o| o.parent_order_id().is_some())
        {
            log::debug!("{id} is a bracket's leg: cTrader holds it on its entry or position");
            return Ok(());
        }
        let venue = cmd
            .venue_order_id
            .or(order.as_ref().and_then(OrderAny::venue_order_id));

        if venue.is_some_and(|v| parse_protection_id(v).is_some()) {
            log::debug!("{id} is a protection: the open-order check reads it");
            return Ok(());
        }
        let since = order
            .as_ref()
            .map_or(UnixNanos::default(), OrderAny::ts_init);

        let orders: HashMap<ClientOrderId, OrderAny> = order
            .iter()
            .flat_map(|o| o.linked_order_ids().unwrap_or_default().to_vec())
            .filter_map(|leg| self.cached_order(&leg))
            .chain(order.clone())
            .map(|o| (o.client_order_id(), o))
            .collect();
        let dispatch = Arc::clone(&session.dispatch);
        let ctrader = Arc::clone(&session.ctrader);
        let (account_id, broker) = (self.core.account_id, self.core.venue);
        let instruments = session.instruments.clone();
        let currency = session.account.currency;
        let emitter = self.emitter.clone();
        let clock = self.clock;
        self.spawn_command("order query", async move {
            let now = clock.get_time_ns();
            let reports = Reports {
                account_id,
                instruments: &instruments,
                currency,
                ts_init: now,
            };

            match Self::find_order(&ctrader, &reports, id, venue, since, now).await? {
                Held::Order(status, fills) => emitter.send_order_with_fills(status, fills),
                Held::Nothing => {
                    let absent = dispatch
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .absent(id);
                    let Some(ids) = absent else {
                        log::info!("{broker} holds no order {id} since {since} yet: not concluded");
                        return Ok(());
                    };
                    log::warn!("{broker} holds no order {id} since {since}: rejected");
                    let reason = absence(broker);
                    for order in ids.iter().filter_map(|id| orders.get(id)) {
                        emitter.send_order_status_report(reports.absent(order, &reason));
                    }
                }
                Held::Unreported => {
                    log::warn!("{broker} order {id} is of a symbol the run did not load");
                }
            }
            Ok(())
        });
        Ok(())
    }

    /// Returns the report for one order.
    ///
    /// An open order comes from the reconcile request. Any other order comes from its details,
    /// found by its venue order id or among the orders the broker updated since the cached order
    /// was created. The result is:
    /// - `None` once the broker answered that it holds no such order.
    /// - An error if the broker holds the order on a symbol the run did not load, or if an order
    ///   not open has no client order id to look for. Neither is an answer of absence.
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let session = self.session()?;
        let open = session.ctrader.reconcile().await?;
        let reports = self.reports(session, self.now());
        let found = Self::order_reports(&reports, &open)?.into_iter().find(|r| {
            cmd.venue_order_id == Some(r.venue_order_id)
                || (cmd.client_order_id.is_some() && cmd.client_order_id == r.client_order_id)
        });

        if found.is_some() {
            return Ok(found);
        }
        let id = cmd
            .client_order_id
            .context("no client order id: no window to look for an order not open in")?;
        let since = self
            .cached_order(&id)
            .map(|o| o.ts_init())
            .with_context(|| format!("{id} is not in the cache: no window to look for it in"))?;
        let venue = cmd
            .venue_order_id
            .filter(|v| parse_protection_id(*v).is_none());
        let found =
            Self::find_order(&session.ctrader, &reports, id, venue, since, self.now()).await?;

        match found {
            Held::Order(status, _) => Ok(Some(status)),
            Held::Nothing => Ok(None),
            Held::Unreported => anyhow::bail!(
                "{} holds order {id} on a symbol the run did not load: it cannot report it",
                self.core.venue
            ),
        }
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let session = self.session()?;
        let open = session.ctrader.reconcile().await?;
        let mut orders = Self::order_reports(&self.reports(session, cmd.ts_init), &open)?;
        orders.retain(|r| cmd.instrument_id.is_none_or(|id| r.instrument_id == id));
        log_receipt(
            orders.len(),
            "OrderStatusReport",
            self.core.venue,
            cmd.log_receipt_level,
        );
        Ok(orders)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let session = self.session()?;
        let fills = self.fill_reports(session, &cmd).await?;
        log_receipt(
            fills.len(),
            "FillReport",
            self.core.venue,
            cmd.log_receipt_level,
        );
        Ok(fills)
    }

    /// Generates the position reports.
    ///
    /// A position the cache held open that cTrader does not hold is reported flat, as the mass
    /// status does (see `flats`). A position with no cTrader id (see `cached_open`) has no report.
    /// Nautilus reads its absence as flat, because
    /// `ExecutionClient::provides_bulk_position_coverage` is true by default and this client leaves
    /// it as it is.
    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let cached = self.cached_open();
        let session = self.session()?;
        let open = session.ctrader.reconcile().await?;
        let reports = self.reports(session, cmd.ts_init);
        let mut positions = Self::position_reports(&reports, &open)?;
        positions.extend(Self::flats(
            session,
            &reports,
            &cached,
            &open_volumes(&open.position),
        ));
        positions.retain(|r| cmd.instrument_id.is_none_or(|id| r.instrument_id == id));
        log_receipt(
            positions.len(),
            "PositionStatusReport",
            self.core.venue,
            cmd.log_receipt_level,
        );
        Ok(positions)
    }

    /// Generates the mass status.
    ///
    /// It holds the orders closed and the fills of the deals created since the earliest of:
    /// - `lookback_mins` ago.
    /// - The creation of the deals that opened a position open at the broker or one the cache holds
    ///   open.
    /// - A minute before the creation of an order the cache holds open or in flight that cTrader
    ///   does not list open (see `earliest_cached_order`).
    ///
    /// A position's deals go in only when they add up to it (see `reports::explained`). A closed
    /// order goes in only with all its deals. The report window says whether all did, and whether
    /// every closed order could be read.
    ///
    /// A deal of a protection order closes the legs of its position, as its trade event does (see
    /// `Reports::protection_closed`). So does a protection order cancelled or expired over the
    /// window, for a position whose deals go in, except for a leg still open.
    ///
    /// A position the cache holds open that cTrader does not is reported flat. Nautilus applies
    /// bounded fills only to the positions of an instrument that has a position report.
    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = self.now();
        let cached = self.cached_open();
        let mut openings: BTreeMap<i64, i64> =
            cached.iter().map(|c| (c.position, c.opened)).collect();
        let session = self.session()?;
        let open = session.ctrader.reconcile().await?;

        // The lists' end, taken once the reconcile request is answered, so that they include any
        // deal or order made while it was on its way, for a position it no longer holds.
        let until = orders::millis(self.now());
        let reports = self.reports(session, ts_init);
        let mut orders = Self::order_reports(&reports, &open)?;
        let ordered = self.earliest_cached_order(&orders);
        let mut positions = Self::position_reports(&reports, &open)?;
        let held = open_volumes(&open.position);
        positions.extend(Self::flats(session, &reports, &cached, &held));

        let lookback = lookback_mins
            .map(DurationNanos::try_from_mins)
            .transpose()?
            .map(|lookback| ts_init.saturating_sub(lookback));

        for position in &open.position {
            if let Some(opened) = position.trade_data.open_timestamp {
                openings.insert(position.position_id, opened);
            }
        }
        let opening = Self::earliest_opening_deal(session, &openings)
            .await?
            .and_then(UnixNanos::from_millis_checked);
        let start = [lookback, opening, ordered]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(ts_init);

        // One cutoff for the request and the report window: cTrader's, in ms
        let start = start.floor(DurationNanos::from_millis(1));
        let deals = session.ctrader.deals(orders::millis(start), until).await?;
        let (kept, unexplained) = explained(&deals, &held);
        if !unexplained.is_empty() {
            log::warn!(
                "{} deals of positions {unexplained:?} left out: their history starts before \
                 {start}",
                self.core.venue
            );
        }

        let updated = session.ctrader.orders(orders::millis(start), until).await?;
        let protections: HashMap<i64, &ProtoOaOrder> = updated
            .iter()
            .filter(|o| o.order_type() == ProtoOaOrderType::StopLossTakeProfit)
            .map(|o| (o.order_id, o))
            .collect();

        // An order the node sent but never heard back about is named by its client order id, and
        // one that cannot be read leaves the history incomplete.
        let mut history = Vec::new();
        let mut history_complete = true;

        for order in updated.iter().filter(|o| {
            o.order_type() != ProtoOaOrderType::StopLossTakeProfit
                && o.order_status() != ProtoOaOrderStatus::OrderStatusAccepted
        }) {
            let reported: i64 = kept
                .iter()
                .filter(|d| d.order_id == order.order_id)
                .map(|d| d.filled_volume)
                .sum();

            if reported != order.executed_volume.unwrap_or_default() {
                continue;
            }

            match reports.order(order) {
                Ok(report) => history.extend(report),
                Err(e) => {
                    log::warn!("{} order history incomplete: {e:#}", self.core.venue);
                    history_complete = false;
                }
            }
        }

        let histories: HashSet<i64> = kept.iter().map(|d| d.position_id).collect();

        // A cancelled protection's legs come before a filled one's, so that the report of a leg
        // hit is the last for its venue order id, the one the mass status keeps.
        for protection in protections.values().filter(|o| {
            matches!(
                o.order_status(),
                ProtoOaOrderStatus::OrderStatusCancelled | ProtoOaOrderStatus::OrderStatusExpired
            ) && o.position_id.is_some_and(|p| histories.contains(&p))
        }) {
            history.extend(
                reports
                    .protection_closed(protection, None)?
                    .into_iter()
                    .map(|(leg, _)| leg),
            );
        }
        let mut fills = Vec::new();

        for deal in kept {
            match protections.get(&deal.order_id) {
                Some(protection) => {
                    for (leg, fill) in reports.protection_closed(protection, Some(deal))? {
                        history.push(leg);
                        fills.extend(fill);
                    }
                }
                None => fills.extend(reports.fill(deal)?),
            }
        }

        // An order or leg the reconcile request holds open is open
        history.retain(|closed| {
            orders
                .iter()
                .all(|o| o.venue_order_id != closed.venue_order_id)
        });

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.venue(),
            ts_init,
            None,
        );
        log::info!(
            "{} mass status: {} positions, {} open orders, {} orders and legs closed, {} fills \
             since {start}",
            self.core.venue,
            positions.len(),
            orders.len(),
            history.len(),
            fills.len()
        );
        orders.extend(history);
        mass_status.add_order_reports(orders);
        mass_status.add_fill_reports(fills);
        mass_status.add_position_reports(positions);
        mass_status.set_report_window(Some(start), unexplained.is_empty() && history_complete);
        Ok(Some(mass_status))
    }
}

fn log_receipt(count: usize, kind: &str, venue: Venue, level: LogLevel) {
    if let Some(level) = map_log_level_to_filter(level).to_level() {
        log::log!(level, "Received {count} {kind}s from {venue}");
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clients::DataClient,
        live::runner::{
            replace_data_event_sender, replace_exec_event_sender, replace_system_event_sender,
        },
        messages::{
            ExecutionEvent, SystemEvent,
            system::{SocketState, SocketStateChange},
        },
    };
    use nautilus_live::{
        SocketReconnectLookup, SocketReconnectRegistry, SocketReconnectRequestOutcome,
    };
    use nautilus_model::{
        enums::{OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce, TriggerType},
        identifiers::{PositionId, TraderId, VenueOrderId},
        types::{Currency, Money, Price, Quantity},
    };
    use prost::Message as _;
    use rstest::rstest;
    use rust_decimal::Decimal;
    use ustr::Ustr;

    use super::*;
    use crate::{
        common::messages::{
            Payload, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
            ProtoOaAccountType, ProtoOaAssetListReq, ProtoOaAssetListRes,
            ProtoOaClosePositionDetail, ProtoOaDeal, ProtoOaDealListByPositionIdReq,
            ProtoOaDealListByPositionIdRes, ProtoOaDealListReq, ProtoOaDealListRes,
            ProtoOaDealStatus, ProtoOaErrorRes, ProtoOaOrder, ProtoOaOrderListReq,
            ProtoOaOrderListRes, ProtoOaOrderStatus, ProtoOaOrderTriggerMethod, ProtoOaOrderType,
            ProtoOaPosition, ProtoOaPositionStatus, ProtoOaReconcileReq, ProtoOaReconcileRes,
            ProtoOaRefreshTokenReq, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes,
            ProtoOaSymbolsListReq, ProtoOaSymbolsListRes, ProtoOaTimeInForce, ProtoOaTradeData,
            ProtoOaTradeSide, ProtoOaTraderReq, ProtoOaTraderRes,
        },
        data::CTraderDataClient,
        testing::{
            self, ACCOUNT_AUTH_TOKEN_INVALID, ACCOUNT_ID, ASSET_LIST_RES, Action, CLIENT,
            DEAL_LIST_RES_EMPTY, RECONCILE_RES_EMPTY, Received, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES,
            TRADER_RES, VENUE, accept_auth, replay, reply, token_refused,
        },
    };

    const XAUUSD: &str = "XAUUSD.ACMEFX";
    const EURUSD: &str = "EURUSD.ACMEFX";

    const XAUUSD_SYMBOL_ID: i64 = 41;
    const EURUSD_SYMBOL_ID: i64 = 1;
    const US500_SYMBOL_ID: i64 = 10013;

    /// 2026-10-25T10:00:00Z, in ms.
    const T0: i64 = 1_792_922_400_000;

    type Answers = Arc<dyn Fn(&ProtoMessage) -> Vec<Action> + Send + Sync>;

    /// The captured responses unless a test sets others, no order.
    #[derive(Clone, Default)]
    struct Script {
        trader: Option<ProtoOaTraderRes>,
        reconcile: Option<ProtoOaReconcileRes>,
        deals: Option<Vec<ProtoOaDeal>>,
        orders: Vec<ProtoOaOrder>,
        refuse_token: bool,
        other_account: bool,
        trades: Option<Answers>,
        reconcile_takes: Option<(&'static AtomicTime, UnixNanos)>,
    }

    mod order_flow;

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
                t if t == crate::common::messages::ProtoOaGetDynamicLeverageByIdReq::TYPE => {
                    testing::dynamic_leverage_answer(request)
                }
                t if t == ProtoOaTraderReq::TYPE => match &venue.trader {
                    Some(trader) => reply(request, trader),
                    None => replay::<ProtoOaTraderRes>(request, TRADER_RES),
                },
                t if t == ProtoOaReconcileReq::TYPE => match &venue.reconcile {
                    _ if venue.reconcile_takes.is_some() => {
                        let (clock, answered) = venue.reconcile_takes.unwrap();
                        clock.set_time(answered);

                        match &venue.reconcile {
                            Some(open) => reply(request, open),
                            None => replay::<ProtoOaReconcileRes>(request, RECONCILE_RES_EMPTY),
                        }
                    }
                    Some(open) => reply(request, open),
                    None => replay::<ProtoOaReconcileRes>(request, RECONCILE_RES_EMPTY),
                },

                // As on the demo (2026-10-26): a window not ending after its start is refused
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

                // As on the demo (2026-11-12), a deal list holds the deals created over its
                // window, bounds included, and those deals may have executed after it.
                t if t == ProtoOaDealListReq::TYPE => match &venue.deals {
                    Some(deals) => {
                        let asked = ProtoOaDealListReq::decode(payload).unwrap();
                        let (from, to) =
                            (asked.from_timestamp.unwrap(), asked.to_timestamp.unwrap());
                        let deal = deals
                            .iter()
                            .filter(|d| from <= d.create_timestamp && d.create_timestamp <= to)
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

                // As on the demo (2026-11-12), an order list holds the orders last updated over
                // the window, bounds included.
                t if t == ProtoOaOrderListReq::TYPE => {
                    let asked = ProtoOaOrderListReq::decode(payload).unwrap();
                    let (from, to) = (asked.from_timestamp.unwrap(), asked.to_timestamp.unwrap());
                    let order = venue
                        .orders
                        .iter()
                        .filter(|o| {
                            o.utc_last_update_timestamp
                                .is_some_and(|t| from <= t && t <= to)
                        })
                        .cloned()
                        .collect();
                    reply(
                        request,
                        &ProtoOaOrderListRes {
                            ctid_trader_account_id: ACCOUNT_ID,
                            order,
                            has_more: false,
                            ..Default::default()
                        },
                    )
                }

                // As on the demo (2026-11-12), a position's deal list holds the deals created over
                // the window, a bound left out leaves that side unbounded, and a position the
                // account never held is not found.
                t if t == ProtoOaDealListByPositionIdReq::TYPE => {
                    let asked = ProtoOaDealListByPositionIdReq::decode(payload).unwrap();
                    let deals = venue.deals.iter().flatten();
                    let deal: Vec<ProtoOaDeal> = deals
                        .filter(|d| {
                            d.position_id == asked.position_id
                                && asked.from_timestamp.is_none_or(|t| t <= d.create_timestamp)
                                && asked.to_timestamp.is_none_or(|t| d.create_timestamp <= t)
                        })
                        .cloned()
                        .collect();
                    let held = venue
                        .deals
                        .iter()
                        .flatten()
                        .any(|d| d.position_id == asked.position_id);

                    if held {
                        reply(
                            request,
                            &ProtoOaDealListByPositionIdRes {
                                ctid_trader_account_id: ACCOUNT_ID,
                                deal,
                                has_more: false,
                                ..Default::default()
                            },
                        )
                    } else {
                        reply(
                            request,
                            &ProtoOaErrorRes {
                                error_code: "POSITION_NOT_FOUND".to_owned(),
                                description: Some("Position not found".to_owned()),
                                ..Default::default()
                            },
                        )
                    }
                }
                other => panic!("unexpected request {other}"),
            };
            vec![answer]
        }
    }

    type Events = tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>;

    fn started(
        venue: Script,
    ) -> (
        CTraderExecutionClient,
        Rc<RefCell<Cache>>,
        Events,
        Received,
        tempfile::TempDir,
    ) {
        started_at(venue, get_atomic_clock_realtime())
    }

    fn started_at(
        venue: Script,
        clock: &'static AtomicTime,
    ) -> (
        CTraderExecutionClient,
        Rc<RefCell<Cache>>,
        Events,
        Received,
        tempfile::TempDir,
    ) {
        let (url, received) = testing::start(peer(venue));
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("HELIX-001"),
            ClientId::from(CLIENT),
            Venue::new(VENUE),
            OmsType::Hedging,
            AccountId::from("ACMEFX-DEMO"),
            AccountType::Margin,
            None,
            cache.clone(),
        );
        let (credential, tokens) = testing::credential();
        let mut client = CTraderExecutionClient::with_endpoint(
            core,
            url,
            credential,
            vec![InstrumentId::from(XAUUSD), InstrumentId::from(EURUSD)],
            clock,
        );
        client.start().unwrap();
        (client, cache, events, received, tokens)
    }

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

    async fn connect(
        client: &mut CTraderExecutionClient,
        events: &mut Events,
        cache: &Rc<RefCell<Cache>>,
    ) -> AccountState {
        let (connected, state) = tokio::join!(client.connect(), register_account(events, cache));
        connected.unwrap();
        state
    }

    /// `volume` is in cents of an ounce. The position opened at `T0` + `minutes`.
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
    async fn a_brokers_data_and_trading_sockets_are_two_endpoints_each_reconnectable() {
        let (system_tx, mut system) = tokio::sync::mpsc::unbounded_channel();
        replace_system_event_sender(system_tx);
        let (data_tx, _data_events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let registry = SocketReconnectRegistry::default();
        let (mut trading, cache, mut events, trading_received, _tokens) =
            registry.scope(|| started(Script::default()));
        let (url, data_received) = testing::start(peer(Script::default()));
        let (credential, _data_tokens) = testing::credential();
        let mut data = registry.scope(|| {
            CTraderDataClient::with_endpoint(
                ClientId::from(CLIENT),
                url,
                Venue::new(VENUE),
                credential,
                vec![InstrumentId::from(XAUUSD)],
                get_atomic_clock_realtime(),
            )
        });
        connect(&mut trading, &mut events, &cache).await;
        data.connect().await.unwrap();
        let client = ClientId::from(CLIENT);
        let endpoints = ["ctrader-open-api-data", "ctrader-open-api-trading"].map(Ustr::from);

        for endpoint in endpoints {
            let SocketReconnectLookup::Handle(reconnect) = registry.get(client, endpoint) else {
                panic!("{endpoint}: {:?}", registry.get(client, endpoint));
            };
            assert_eq!(
                reconnect.request_reconnect(),
                SocketReconnectRequestOutcome::Accepted,
                "{endpoint}"
            );
        }

        let mut changes: HashMap<Ustr, Vec<SocketStateChange>> = HashMap::new();

        for _ in 0..6 {
            let event = tokio::time::timeout(Duration::from_secs(10), system.recv())
                .await
                .expect("no socket state within 10 s")
                .expect("the system events ended");
            let SystemEvent::SocketState(change) = event;
            changes.entry(change.endpoint).or_default().push(change);
        }

        for endpoint in endpoints {
            let change =
                |state| SocketStateChange::new(client, Some(Venue::new(VENUE)), endpoint, state);
            assert_eq!(
                changes[&endpoint],
                [
                    change(SocketState::Connected),
                    change(SocketState::Disconnected),
                    change(SocketState::Connected)
                ],
                "{endpoint}"
            );
        }

        for received in [&trading_received, &data_received] {
            let frames = testing::wait_until(received, |frames| {
                frames
                    .iter()
                    .any(|(c, f)| *c == 1 && f.payload_type == ProtoOaAccountAuthReq::TYPE)
            })
            .await;
            assert!(
                frames
                    .iter()
                    .any(|(c, f)| *c == 1 && f.payload_type == ProtoOaAccountAuthReq::TYPE),
                "a session did not authenticate again"
            );
        }
        trading.disconnect().await.unwrap();
        data.disconnect().await.unwrap();

        for endpoint in endpoints {
            assert!(registry.handle(client, endpoint).is_none(), "{endpoint}");
        }
    }

    #[tokio::test]
    async fn connecting_publishes_the_captured_account_state_and_waits_for_the_node() {
        let (mut client, cache, mut events, _, _tokens) = started(Script::default());

        let state = {
            let connecting = client.connect();
            tokio::pin!(connecting);

            let state = tokio::select! {
                biased;
                connected = &mut connecting => {
                    panic!("connected before the node registered the account: {connected:?}")
                }
                event = events.recv() => match event {
                    Some(ExecutionEvent::Account(state)) => state,
                    other => panic!("not an account state: {other:?}"),
                },
            };

            assert!(futures::poll!(&mut connecting).is_pending());
            cache
                .borrow_mut()
                .add_account(AccountAny::from_events(std::slice::from_ref(&state)).unwrap())
                .unwrap();
            connecting.await.unwrap();
            state
        };

        let eur = Currency::from("EUR");
        assert_eq!(state.account_id, AccountId::from("ACMEFX-DEMO"));
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

        for fact in ["ACMEFX", "NETTED", "HEDGED"] {
            assert!(err.contains(fact), "{fact}: {err}");
        }
        assert!(!client.is_connected());
        assert!(events.try_recv().is_err(), "an account state was published");
    }

    #[tokio::test]
    async fn an_expired_token_ctrader_refuses_to_renew_is_refused_at_connect() {
        let venue = Script {
            refuse_token: true,
            ..Script::default()
        };
        let (mut client, _cache, _events, _, _tokens) = started(venue);

        let err = format!("{:#}", client.connect().await.unwrap_err());

        for fact in [
            "CH_ACCESS_TOKEN_INVALID",
            "Playground",
            "openapi.ctrader.com",
            "ACME_ACCESS_TOKEN",
            "ACME_REFRESH_TOKEN",
            "acme.env",
            "the ACME guide",
        ] {
            assert!(err.contains(fact), "{fact}: {err}");
        }
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn another_account_is_refused_at_connect() {
        let venue = Script {
            other_account: true,
            ..Script::default()
        };
        let (mut client, _cache, _events, _, _tokens) = started(venue);

        let err = format!("{:#}", client.connect().await.unwrap_err());

        assert!(
            err.contains("another account than ACME_ACCOUNT_ID"),
            "{err}"
        );
        assert!(!client.is_connected());
    }

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
        assert_eq!(mass_status.account_id, AccountId::from("ACMEFX-DEMO"));
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

        // cTrader's charge of -150 at moneyDigits 2, a cost to Nautilus
        let first = &fills[&VenueOrderId::from("1001")][0];
        assert_eq!(first.commission, Money::new(1.5, Currency::from("EUR")));
        assert_eq!(first.ts_event, ns(T0 + 20));
        let asked: Vec<ProtoOaDealListReq> = requests_of(&received);
        assert_eq!(asked[0].from_timestamp, Some(T0));
        assert_eq!(mass_status.lookback_start(), Some(ns(T0)));
        assert!(mass_status.reports_complete());
    }

    /// A deal cTrader states no commission for fails the fill reports and the mass status, rather
    /// than booking it at zero or leaving it out (Nautilus' `spec_exec_testing.md`, TC-E88).
    #[tokio::test]
    async fn a_deal_without_its_commission_fails_the_fill_reports_and_the_mass_status() {
        let mut deals = gold_deals();
        deals[0].commission = None;
        let venue = Script {
            deals: Some(deals),
            ..gold_venue()
        };
        let (mut client, cache, mut events, _received, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;

        let fills = client
            .generate_fill_reports(GenerateFillReports::new(
                UUID4::new(),
                client.now(),
                None,
                None,
                Some(ns(T0)),
                None,
                None,
                None,
            ))
            .await;
        let mass_status = client.generate_mass_status(None).await;
        client.disconnect().await.unwrap();

        let fills = format!("{:#}", fills.unwrap_err());
        assert!(fills.contains("cTrader deal 1"), "{fills}");
        assert!(fills.contains("no commission"), "{fills}");
        let mass_status = format!("{:#}", mass_status.unwrap_err());
        assert!(mass_status.contains("cTrader deal 1"), "{mass_status}");
    }

    /// Nautilus' targeted order recovery asks for the fills of one order.
    ///
    /// The request names the order's instrument and has no start
    /// (`live/src/execution/reconciliation.rs`).
    #[tokio::test]
    async fn a_fill_request_fails_only_on_a_deal_it_covers_without_its_commission() {
        let mut deals = gold_deals();
        deals[1].commission = None;
        let venue = Script {
            deals: Some(deals),
            ..gold_venue()
        };
        let (mut client, cache, mut events, _received, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        let asking = |instrument: Option<&str>, order: Option<&str>| {
            GenerateFillReports::new(
                UUID4::new(),
                client.now(),
                instrument.map(InstrumentId::from),
                order.map(VenueOrderId::from),
                None,
                None,
                None,
                None,
            )
        };

        let of_order = client
            .generate_fill_reports(asking(Some(XAUUSD), Some("1001")))
            .await;
        let of_other_instrument = client
            .generate_fill_reports(asking(Some(EURUSD), None))
            .await;
        let of_deal_2 = client
            .generate_fill_reports(asking(Some(XAUUSD), Some("1002")))
            .await;
        let of_all = client.generate_fill_reports(asking(None, None)).await;
        client.disconnect().await.unwrap();

        let trades: Vec<String> = of_order
            .unwrap()
            .iter()
            .map(|f| f.trade_id.to_string())
            .collect();
        assert_eq!(trades, ["1"]);
        assert!(of_other_instrument.unwrap().is_empty());

        for covering in [of_deal_2, of_all] {
            let error = format!("{:#}", covering.unwrap_err());
            assert!(error.contains("cTrader deal 2"), "{error}");
        }
    }

    #[tokio::test]
    async fn the_deals_of_a_position_opened_before_the_window_are_left_out() {
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

    /// As the demo had it (2026-11-12, position 59798583 of another run).
    #[tokio::test]
    async fn the_protection_of_a_position_opened_before_the_window_is_left_out() {
        let mut deals = gold_deals();
        deals.push(deal(7, 105, ProtoOaTradeSide::Sell, 2_000, 4011.0, 5));
        let protection = ProtoOaOrder {
            order_id: 1_105,
            trade_data: ProtoOaTradeData {
                symbol_id: XAUUSD_SYMBOL_ID,
                volume: 2_000,
                trade_side: ProtoOaTradeSide::Sell as i32,
                open_timestamp: Some(T0 - 60 * 60_000),
                ..Default::default()
            },
            order_type: ProtoOaOrderType::StopLossTakeProfit as i32,
            order_status: ProtoOaOrderStatus::OrderStatusCancelled as i32,
            utc_last_update_timestamp: Some(T0 + 5 * 60_000 + 20),
            closing_order: Some(true),
            stop_price: Some(3990.0),
            limit_price: Some(4030.0),
            position_id: Some(105),
            ..Default::default()
        };
        let venue = Script {
            deals: Some(deals),
            orders: vec![protection],
            ..gold_venue()
        };

        let (mass_status, _) = mass_status(venue, None).await;

        let mut orders: Vec<String> = mass_status
            .order_reports()
            .keys()
            .map(ToString::to_string)
            .collect();
        orders.sort_unstable();
        assert_eq!(orders, ["101-SL", "101-TP", "102-SL", "201"]);
    }

    #[tokio::test]
    async fn a_longer_lookback_reads_the_deals_from_its_start() {
        let clock = Box::leak(Box::new(AtomicTime::new(false, ns(T0 + 3_600_000))));
        let (mut client, cache, mut events, received, _tokens) = started_at(gold_venue(), clock);
        connect(&mut client, &mut events, &cache).await;

        let mass_status = client
            .generate_mass_status(Some(60 * 24 * 365 * 10))
            .await
            .unwrap()
            .unwrap();

        let from = 1_477_566_000_000;
        let asked: Vec<ProtoOaDealListReq> = requests_of(&received);
        assert_eq!(asked[0].from_timestamp, Some(from));
        assert_eq!(mass_status.lookback_start(), Some(ns(from)));
        client.disconnect().await.unwrap();
    }

    fn hold_open(
        client: &CTraderExecutionClient,
        cache: &Rc<RefCell<Cache>>,
        position: &str,
        opened: UnixNanos,
    ) {
        use nautilus_model::{
            events::OrderEventAny,
            orders::{OrderTestBuilder, stubs::TestOrderEventStubs},
            position::Position,
        };

        let gold = client.session().unwrap().instruments[&XAUUSD_SYMBOL_ID].clone();
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(10))
            .build();
        let OrderEventAny::Filled(fill) = TestOrderEventStubs::filled(
            &order,
            &gold,
            None,
            Some(PositionId::from(position)),
            Some(Price::from("4000.00")),
            None,
            None,
            None,
            Some(opened),
            Some(AccountId::from("ACMEFX-DEMO")),
        ) else {
            panic!("not a fill");
        };
        cache
            .borrow_mut()
            .add_position(&Position::new(&gold, fill), OmsType::Hedging)
            .unwrap();
    }

    #[tokio::test]
    async fn the_deals_are_read_from_the_opening_of_a_position_the_cache_holds_open() {
        let mut deals = gold_deals();
        deals.push(deal(9, 99, ProtoOaTradeSide::Buy, 1_000, 4000.0, -60));
        let venue = Script {
            deals: Some(deals),
            ..gold_venue()
        };
        let (mut client, cache, mut events, received, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;

        hold_open(&client, &cache, "99", ns(T0 - 3_600_000 + 20));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let asked: Vec<ProtoOaDealListReq> = requests_of(&received);
        assert_eq!(asked[0].from_timestamp, Some(T0 - 3_600_000));
        assert_eq!(mass_status.lookback_start(), Some(ns(T0 - 3_600_000)));
        client.disconnect().await.unwrap();
    }

    /// The deal that opened position 59791668 on the demo (2026-11-11).
    fn opening_deal(id: i64, position: i64, created: i64) -> ProtoOaDeal {
        let mut opening = deal(id, position, ProtoOaTradeSide::Buy, 100, 4012.35, 0);
        opening.create_timestamp = created;
        opening.execution_timestamp = created + 1;
        opening
    }

    fn trade_ids(mass_status: &ExecutionMassStatus) -> Vec<String> {
        let mut trades: Vec<String> = mass_status
            .fill_reports()
            .values()
            .flatten()
            .map(|f| f.trade_id.to_string())
            .collect();
        trades.sort();
        trades
    }

    /// As on the demo (2026-11-11, position 59791668).
    #[tokio::test]
    async fn a_position_open_at_the_broker_comes_with_the_deal_that_opened_it() {
        let created = T0 + 7;
        let mut open = position(59_791_668, ProtoOaTradeSide::Buy, 100, 0);
        open.trade_data.open_timestamp = Some(created + 1);
        open.price = Some(4012.35);
        let venue = Script {
            reconcile: Some(ProtoOaReconcileRes {
                ctid_trader_account_id: ACCOUNT_ID,
                position: vec![open],
                ..Default::default()
            }),
            deals: Some(vec![opening_deal(65_945_613, 59_791_668, created)]),
            ..Script::default()
        };

        let (mass_status, _) = mass_status(venue, None).await;

        assert_eq!(trade_ids(&mass_status), ["65945613"]);
        assert!(mass_status.reports_complete());
        assert_eq!(mass_status.lookback_start(), Some(ns(created)));
    }

    /// As on the demo (2026-11-11, position 59791558).
    #[tokio::test]
    async fn a_position_the_cache_holds_open_closed_meanwhile_comes_with_its_deals() {
        let created = T0 + 7;
        let mut closing = deal(
            65_945_411,
            59_791_558,
            ProtoOaTradeSide::Sell,
            100,
            4011.0,
            1,
        );
        closing.execution_timestamp = closing.create_timestamp;
        closing.close_position_detail = Some(ProtoOaClosePositionDetail {
            entry_price: 4012.35,
            closed_volume: Some(100),
            ..Default::default()
        });
        let venue = Script {
            deals: Some(vec![opening_deal(65_945_399, 59_791_558, created), closing]),
            ..Script::default()
        };
        let (mut client, cache, mut events, _, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "59791558", ns(created + 1));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        assert_eq!(trade_ids(&mass_status), ["65945399", "65945411"]);
        assert!(mass_status.reports_complete());
        assert_eq!(mass_status.lookback_start(), Some(ns(created)));
        client.disconnect().await.unwrap();
    }

    /// Position 59793251 of the demo (2026-11-12).
    ///
    /// cTrader holds the stop loss and take profit as its protection order 76919228 under the
    /// entry's client order id.
    fn stopped_out() -> Script {
        let opening = ProtoOaDeal {
            deal_id: 65_947_597,
            order_id: 76_919_227,
            position_id: 59_793_251,
            volume: 100,
            filled_volume: 100,
            symbol_id: XAUUSD_SYMBOL_ID,
            create_timestamp: 1_794_443_791_942,
            execution_timestamp: 1_794_443_791_943,
            utc_last_update_timestamp: Some(1_794_443_791_942),
            execution_price: Some(3985.52),
            trade_side: ProtoOaTradeSide::Buy as i32,
            deal_status: ProtoOaDealStatus::Filled as i32,
            commission: Some(-4),
            money_digits: Some(2),
            ..Default::default()
        };
        let closing = ProtoOaDeal {
            deal_id: 65_947_662,
            order_id: 76_919_228,
            create_timestamp: 1_794_443_906_550,
            execution_timestamp: 1_794_443_906_550,
            utc_last_update_timestamp: Some(1_794_443_906_550),
            execution_price: Some(3983.05),
            trade_side: ProtoOaTradeSide::Sell as i32,
            close_position_detail: Some(ProtoOaClosePositionDetail {
                entry_price: 3985.52,
                closed_volume: Some(100),
                ..Default::default()
            }),
            ..opening.clone()
        };
        let entry = ProtoOaOrder {
            order_id: 76_919_227,
            trade_data: ProtoOaTradeData {
                symbol_id: XAUUSD_SYMBOL_ID,
                volume: 100,
                trade_side: ProtoOaTradeSide::Buy as i32,
                open_timestamp: Some(1_794_443_791_942),
                close_timestamp: Some(1_794_443_791_943),
                ..Default::default()
            },
            order_type: ProtoOaOrderType::Market as i32,
            order_status: ProtoOaOrderStatus::OrderStatusFilled as i32,
            execution_price: Some(3985.52),
            executed_volume: Some(100),
            utc_last_update_timestamp: Some(1_794_443_791_943),
            closing_order: Some(false),
            stop_loss: Some(3983.38),
            take_profit: Some(3987.52),
            client_order_id: Some("O-20261112-003631-104-001-1".to_owned()),
            time_in_force: Some(ProtoOaTimeInForce::ImmediateOrCancel as i32),
            position_id: Some(59_793_251),
            ..Default::default()
        };
        let protection = ProtoOaOrder {
            order_id: 76_919_228,
            trade_data: ProtoOaTradeData {
                trade_side: ProtoOaTradeSide::Sell as i32,
                open_timestamp: Some(1_794_443_791_947),
                close_timestamp: Some(1_794_443_906_550),
                ..entry.trade_data.clone()
            },
            order_type: ProtoOaOrderType::StopLossTakeProfit as i32,
            execution_price: Some(3983.05),
            utc_last_update_timestamp: Some(1_794_443_906_550),
            closing_order: Some(true),
            stop_loss: None,
            take_profit: None,
            limit_price: Some(3987.52),
            stop_price: Some(3983.38),
            time_in_force: Some(ProtoOaTimeInForce::GoodTillCancel as i32),
            stop_trigger_method: Some(ProtoOaOrderTriggerMethod::Trade as i32),
            ..entry.clone()
        };
        Script {
            deals: Some(vec![opening, closing]),
            orders: vec![entry, protection],
            ..Script::default()
        }
    }

    #[tokio::test]
    async fn a_leg_hit_while_the_node_was_down_is_filled_by_its_deal_and_the_other_cancelled() {
        let (mut client, cache, mut events, _, _tokens) = started(stopped_out());
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "59793251", ns(1_794_443_791_943));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let orders = mass_status.order_reports();
        let stop_loss = &orders[&VenueOrderId::from("59793251-SL")];
        assert_eq!(stop_loss.order_status, OrderStatus::Filled);
        assert_eq!(stop_loss.filled_qty, Quantity::from(1));
        assert_eq!(stop_loss.avg_px, Some(Decimal::new(398_305, 2)));
        let take_profit = &orders[&VenueOrderId::from("59793251-TP")];
        assert_eq!(take_profit.order_status, OrderStatus::Canceled);
        let fills = mass_status.fill_reports();
        let trades = |venue: &str| -> Vec<String> {
            fills
                .get(&VenueOrderId::from(venue))
                .into_iter()
                .flatten()
                .map(|f| f.trade_id.to_string())
                .collect()
        };
        assert_eq!(trades("59793251-SL"), ["65947662"]);
        assert_eq!(trades("76919228"), Vec::<String>::new());
        assert_eq!(trades("76919227"), ["65947597"]);
        assert!(mass_status.reports_complete());

        // Flat, because the leg's fill closes the position cTrader no longer holds, and an
        // instrument without a position report takes bounded fills on its orders only.
        let positions = mass_status.position_reports();
        let flat: Vec<_> = positions[&InstrumentId::from(XAUUSD)]
            .iter()
            .map(|r| (r.venue_position_id, r.position_side, r.quantity))
            .collect();
        assert_eq!(
            flat,
            [(
                Some(PositionId::from("59793251")),
                PositionSide::Flat,
                Quantity::from(0)
            )]
        );
        client.disconnect().await.unwrap();
    }

    /// As `stopped_out`, but a close of its own (order 76919300) came first.
    #[tokio::test]
    async fn the_legs_of_a_position_closed_otherwise_while_the_node_was_down_are_cancelled() {
        let mut venue = stopped_out();
        let deals = venue.deals.as_mut().unwrap();
        deals[1].order_id = 76_919_300;
        deals[1].execution_price = Some(3984.0);
        let protection = &mut venue.orders[1];
        protection.order_status = ProtoOaOrderStatus::OrderStatusCancelled as i32;
        protection.execution_price = None;
        protection.executed_volume = None;
        let (mut client, cache, mut events, _, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "59793251", ns(1_794_443_791_943));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let orders = mass_status.order_reports();

        for leg in ["59793251-SL", "59793251-TP"] {
            assert_eq!(
                orders[&VenueOrderId::from(leg)].order_status,
                OrderStatus::Canceled,
                "{leg}"
            );
        }
        let fills = mass_status.fill_reports();
        assert_eq!(
            fills[&VenueOrderId::from("76919300")][0]
                .trade_id
                .to_string(),
            "65947662"
        );
        client.disconnect().await.unwrap();
    }

    /// As `stopped_out`, but the protection was first set as order 76919226.
    #[rstest]
    #[case::read_before_the_hit_one(true)]
    #[case::read_after_the_hit_one(false)]
    #[tokio::test]
    async fn a_leg_hit_stays_filled_when_an_earlier_protection_of_its_position_was_cancelled(
        #[case] cancelled_first: bool,
    ) {
        let mut venue = stopped_out();
        let mut replaced = venue.orders[1].clone();
        replaced.order_id = 76_919_226;
        replaced.order_status = ProtoOaOrderStatus::OrderStatusCancelled as i32;
        replaced.execution_price = None;
        replaced.executed_volume = None;
        replaced.utc_last_update_timestamp = Some(1_794_443_800_000);
        if cancelled_first {
            venue.orders.insert(1, replaced);
        } else {
            venue.orders.push(replaced);
        }
        let (mut client, cache, mut events, _, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "59793251", ns(1_794_443_791_943));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let orders = mass_status.order_reports();
        let stop_loss = &orders[&VenueOrderId::from("59793251-SL")];
        assert_eq!(stop_loss.order_status, OrderStatus::Filled);
        assert_eq!(stop_loss.filled_qty, Quantity::from(1));
        let take_profit = &orders[&VenueOrderId::from("59793251-TP")];
        assert_eq!(take_profit.order_status, OrderStatus::Canceled);
        client.disconnect().await.unwrap();
    }

    /// As `stopped_out`, but the stop loss hit while the reconcile request was on its way.
    #[tokio::test]
    async fn a_leg_hit_while_the_reconcile_request_was_answered_comes_with_its_deal() {
        let closed: i64 = 1_794_443_906_550;
        let clock = Box::leak(Box::new(AtomicTime::new(false, ns(closed - 60_000))));
        let venue = Script {
            reconcile_takes: Some((clock, ns(closed + 5))),
            ..stopped_out()
        };
        let (mut client, cache, mut events, _, _tokens) = started_at(venue, clock);
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "59793251", ns(1_794_443_791_943));
        clock.set_time(ns(closed - 5));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        assert_eq!(trade_ids(&mass_status), ["65947597", "65947662"]);
        let orders = mass_status.order_reports();
        let stop_loss = &orders[&VenueOrderId::from("59793251-SL")];
        assert_eq!(stop_loss.order_status, OrderStatus::Filled);
        assert!(mass_status.reports_complete());
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn an_order_the_broker_lists_open_does_not_widen_the_window() {
        use nautilus_model::orders::{OrderTestBuilder, stubs::TestOrderEventStubs};

        let now = ns(T0 + 3_600_000);
        let clock = Box::leak(Box::new(AtomicTime::new(false, now)));
        let venue = Script {
            reconcile: Some(ProtoOaReconcileRes {
                ctid_trader_account_id: ACCOUNT_ID,
                order: two_gold_positions().order,
                ..Default::default()
            }),
            ..Script::default()
        };
        let (mut client, cache, mut events, _, _tokens) = started_at(venue, clock);
        connect(&mut client, &mut events, &cache).await;

        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(InstrumentId::from(EURUSD))
            .client_order_id(ClientOrderId::from("O-20261026-100000-001-000-1"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100_000))
            .price(Price::from("1.08525"))
            .build();
        let account = AccountId::from("ACMEFX-DEMO");
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        for event in [
            TestOrderEventStubs::submitted(&order, account),
            TestOrderEventStubs::accepted(&order, account, VenueOrderId::from("201")),
        ] {
            cache.borrow_mut().update_order(&event).unwrap();
        }

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        assert_eq!(mass_status.lookback_start(), Some(now));
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn only_the_opening_deals_of_the_first_opened_positions_are_asked_for() {
        let opened = [(201, 0), (202, 10), (203, 20)];
        let venue = Script {
            reconcile: Some(ProtoOaReconcileRes {
                ctid_trader_account_id: ACCOUNT_ID,
                position: opened
                    .iter()
                    .map(|&(id, minutes)| {
                        let mut open = position(id, ProtoOaTradeSide::Buy, 100, minutes);
                        open.trade_data.open_timestamp = Some(T0 + minutes * 60_000 + 20);
                        open.price = Some(4000.0);
                        open
                    })
                    .collect(),
                ..Default::default()
            }),
            deals: Some(
                opened
                    .iter()
                    .map(|&(id, minutes)| deal(id, id, ProtoOaTradeSide::Buy, 100, 4000.0, minutes))
                    .chain([deal(204, 204, ProtoOaTradeSide::Buy, 100, 4000.0, 80)])
                    .collect(),
            ),
            ..Script::default()
        };
        let (mut client, cache, mut events, received, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "204", ns(T0 + 80 * 60_000 + 20));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let asked: Vec<ProtoOaDealListByPositionIdReq> = requests_of(&received);
        let positions: Vec<i64> = asked.iter().map(|r| r.position_id).collect();
        assert_eq!(positions, [201]);
        assert_eq!(mass_status.lookback_start(), Some(ns(T0)));
        assert_eq!(trade_ids(&mass_status), ["201", "202", "203"]);
        client.disconnect().await.unwrap();
    }

    /// The demo (2026-11-12): market buy O-20261112-003352-106-001-1 of 1 oz of XAUUSD.
    ///
    /// A node sent it and died before the broker's answer.
    fn crashed() -> Script {
        let order = ProtoOaOrder {
            order_id: 76_919_114,
            trade_data: ProtoOaTradeData {
                symbol_id: XAUUSD_SYMBOL_ID,
                volume: 100,
                trade_side: ProtoOaTradeSide::Buy as i32,
                open_timestamp: Some(1_794_443_632_347),
                close_timestamp: Some(1_794_443_632_348),
                ..Default::default()
            },
            order_type: ProtoOaOrderType::Market as i32,
            order_status: ProtoOaOrderStatus::OrderStatusFilled as i32,
            execution_price: Some(3982.28),
            executed_volume: Some(100),
            utc_last_update_timestamp: Some(1_794_443_632_348),
            closing_order: Some(false),
            client_order_id: Some("O-20261112-003352-106-001-1".to_owned()),
            time_in_force: Some(ProtoOaTimeInForce::ImmediateOrCancel as i32),
            position_id: Some(59_793_188),
            ..Default::default()
        };
        let mut deal = opening_deal(65_947_517, 59_793_188, 1_794_443_632_347);
        deal.order_id = 76_919_114;
        deal.execution_price = Some(3982.28);
        let mut open = position(59_793_188, ProtoOaTradeSide::Buy, 100, 0);
        open.trade_data.open_timestamp = Some(1_794_443_632_348);
        open.utc_last_update_timestamp = Some(1_794_443_632_348);
        open.price = Some(3982.28);
        Script {
            reconcile: Some(ProtoOaReconcileRes {
                ctid_trader_account_id: ACCOUNT_ID,
                position: vec![open],
                ..Default::default()
            }),
            deals: Some(vec![deal]),
            orders: vec![order],
            ..Script::default()
        }
    }

    #[tokio::test]
    async fn an_order_in_flight_at_a_crash_and_filled_meanwhile_is_reported_by_its_client_order_id()
    {
        let (mass_status, _) = mass_status(crashed(), None).await;

        let orders = mass_status.order_reports();
        let order = &orders[&VenueOrderId::from("76919114")];
        assert_eq!(
            order.client_order_id,
            Some(ClientOrderId::from("O-20261112-003352-106-001-1"))
        );
        assert_eq!(order.order_status, OrderStatus::Filled);
        assert_eq!(order.filled_qty, Quantity::from(1));
        assert_eq!(trade_ids(&mass_status), ["65947517"]);
        assert!(mass_status.reports_complete());
    }

    /// The captured close of position 690004734 (`execution_close_filled`).
    #[tokio::test]
    async fn a_close_in_flight_at_a_crash_is_reported_as_the_order_it_was_sent_for() {
        use crate::{common::messages::ProtoOaExecutionEvent, testing::EXECUTION_CLOSE_FILLED};

        let closed = ProtoOaExecutionEvent::decode(EXECUTION_CLOSE_FILLED).unwrap();
        let (order, closing) = (closed.order.unwrap(), closed.deal.unwrap());
        let opening = opening_deal(953_555_100, 690_004_734, 1_793_027_466_284);
        let venue = Script {
            deals: Some(vec![opening, closing]),
            orders: vec![order],
            ..Script::default()
        };
        let (mut client, cache, mut events, _, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "690004734", ns(1_793_027_466_285));

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let orders = mass_status.order_reports();
        let close = &orders[&VenueOrderId::from("1031776330")];
        assert_eq!(close.client_order_id, Some(ClientOrderId::from("PROBE-1")));
        assert_eq!(close.order_status, OrderStatus::Filled);
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn an_order_in_flight_at_a_crash_and_cancelled_meanwhile_is_reported_cancelled() {
        use nautilus_model::orders::{OrderTestBuilder, stubs::TestOrderEventStubs};

        const T: i64 = 1_794_445_200_000;
        let id = ClientOrderId::from("O-20261112-010000-001-001-7");
        let cancelled = ProtoOaOrder {
            order_id: 76_919_500,
            trade_data: ProtoOaTradeData {
                symbol_id: XAUUSD_SYMBOL_ID,
                volume: 100,
                trade_side: ProtoOaTradeSide::Buy as i32,
                open_timestamp: Some(T),
                ..Default::default()
            },
            order_type: ProtoOaOrderType::Limit as i32,
            order_status: ProtoOaOrderStatus::OrderStatusCancelled as i32,
            utc_last_update_timestamp: Some(T + 5_000),
            limit_price: Some(3900.0),
            client_order_id: Some(id.to_string()),
            time_in_force: Some(ProtoOaTimeInForce::GoodTillCancel as i32),
            ..Default::default()
        };
        let venue = Script {
            orders: vec![cancelled],
            deals: Some(Vec::new()),
            ..Script::default()
        };
        let (mut client, cache, mut events, _, _tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(InstrumentId::from(XAUUSD))
            .client_order_id(id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::from("3900.00"))
            .ts_init(ns(T - 20))
            .build();
        let submitted = TestOrderEventStubs::submitted(&order, AccountId::from("ACMEFX-DEMO"));
        {
            let mut cache = cache.borrow_mut();
            cache.add_order(order, None, None, false).unwrap();
            cache.update_order(&submitted).unwrap();
        }

        let mass_status = client.generate_mass_status(None).await.unwrap().unwrap();

        let orders = mass_status.order_reports();
        let report = &orders[&VenueOrderId::from("76919500")];
        assert_eq!(report.client_order_id, Some(id));
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(mass_status.lookback_start(), Some(ns(T - 20 - 60_000)));
        client.disconnect().await.unwrap();
    }

    /// Nautilus has no order for cTrader's stop limit, so the history is incomplete.
    #[tokio::test]
    async fn an_order_closed_over_the_window_that_cannot_be_read_leaves_the_history_incomplete() {
        let mut venue = crashed();
        let mut stop_limit = venue.orders[0].clone();
        stop_limit.order_id = 76_919_120;
        stop_limit.order_type = ProtoOaOrderType::StopLimit as i32;
        stop_limit.order_status = ProtoOaOrderStatus::OrderStatusCancelled as i32;
        stop_limit.executed_volume = None;
        stop_limit.client_order_id = None;
        stop_limit.stop_price = Some(3990.0);
        venue.orders.push(stop_limit);

        let (mass_status, _) = mass_status(venue, None).await;

        assert!(!mass_status.reports_complete());
        let orders = mass_status.order_reports();
        assert!(orders.contains_key(&VenueOrderId::from("76919114")));
        assert!(!orders.contains_key(&VenueOrderId::from("76919120")));
    }

    #[tokio::test]
    async fn an_account_with_nothing_open_reports_nothing() {
        let (mass_status, _) = mass_status(Script::default(), None).await;

        assert!(mass_status.position_reports().is_empty());
        assert!(mass_status.order_reports().is_empty());
        assert!(mass_status.fill_reports().is_empty());
    }

    #[tokio::test]
    async fn the_reconcile_request_reads_protections_from_positions_not_as_orders() {
        let (_, received) = mass_status(Script::default(), None).await;

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

        let mut held: Vec<String> = positions
            .iter()
            .filter_map(|r| r.venue_position_id.map(|p| p.to_string()))
            .collect();
        held.sort();
        assert_eq!(held, ["101", "102"]);
        let mut ids: Vec<String> = orders
            .iter()
            .map(|r| r.venue_order_id.to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, ["101-SL", "101-TP", "102-SL"]);
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn the_periodic_position_check_reports_a_position_ctrader_no_longer_holds_flat() {
        use nautilus_common::messages::execution::GeneratePositionStatusReportsBuilder;

        let (mut client, cache, mut events, _, _tokens) = started(Script::default());
        connect(&mut client, &mut events, &cache).await;
        hold_open(&client, &cache, "59793251", ns(T0));

        let positions = client
            .generate_position_status_reports(
                &GeneratePositionStatusReportsBuilder::default()
                    .ts_init(client.now())
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        let reported: Vec<_> = positions
            .iter()
            .map(|r| {
                (
                    r.instrument_id,
                    r.venue_position_id,
                    r.position_side,
                    r.quantity,
                )
            })
            .collect();
        assert_eq!(
            reported,
            [(
                InstrumentId::from(XAUUSD),
                Some(PositionId::from("59793251")),
                PositionSide::Flat,
                Quantity::from(0)
            )]
        );
        client.disconnect().await.unwrap();
    }
}
