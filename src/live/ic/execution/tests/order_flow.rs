//! The order flow of `IcExecutionClient` against the mock peer, on the
//! trade events captured on the demo (`data/ic/testdata/execution_*`,
//! `order_error_*`): the peer answers each trade request with them, as the
//! demo answered the same request, mutated where a test says so. The
//! node's side is played here: orders go into the cache before they are
//! submitted, and every order event the client emits is applied to them,
//! as the execution engine does.

use nautilus_model::{
    enums::{ContingencyType, OmsType},
    events::OrderEventAny,
    identifiers::{ClientOrderId, OrderListId, StrategyId},
    orders::{OrderList, OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
};

use super::*;
use crate::data::ic::{
    messages::{
        ProtoOaAmendOrderReq, ProtoOaAmendPositionSltpReq, ProtoOaCancelOrderReq,
        ProtoOaClosePositionReq, ProtoOaExecutionEvent, ProtoOaExecutionType, ProtoOaNewOrderReq,
        ProtoOaOrderDetailsReq, ProtoOaOrderDetailsRes, ProtoOaOrderErrorEvent,
        ProtoOaOrderListReq, ProtoOaOrderListRes,
    },
    testing::{
        EXECUTION_CLOSE_ACCEPTED, EXECUTION_CLOSE_FILLED, EXECUTION_LIMIT_ACCEPTED,
        EXECUTION_LIMIT_CANCELLED, EXECUTION_LIMIT_REPLACED, EXECUTION_MARKET_ACCEPTED,
        EXECUTION_MARKET_FILLED, EXECUTION_PROTECTION_ACCEPTED, EXECUTION_PROTECTION_CANCELLED,
        EXECUTION_PROTECTION_REPLACED, EXECUTION_STOP_LOSS_ACCEPTED,
        EXECUTION_STOP_LOSS_ENTRY_FILLED, EXECUTION_STOP_LOSS_FILLED, ORDER_ERROR_BAD_VOLUME,
        ORDER_ERROR_ORDER_NOT_FOUND, ORDER_ERROR_POSITION_NOT_FOUND, event,
    },
};

/// Whether the peer's `trades` answer `request`: a trade request or an
/// order query.
pub(super) fn answered(request: &ProtoMessage) -> bool {
    [
        ProtoOaNewOrderReq::TYPE,
        ProtoOaAmendOrderReq::TYPE,
        ProtoOaAmendPositionSltpReq::TYPE,
        ProtoOaCancelOrderReq::TYPE,
        ProtoOaClosePositionReq::TYPE,
        ProtoOaOrderListReq::TYPE,
        ProtoOaOrderDetailsReq::TYPE,
    ]
    .contains(&request.payload_type)
}

fn decoded<M: Payload>(bytes: &[u8]) -> M {
    M::decode(bytes).unwrap()
}

fn execution(bytes: &[u8]) -> ProtoOaExecutionEvent {
    decoded(bytes)
}

/// `execution` under the request's `clientMsgId`.
fn answer(request: &ProtoMessage, execution: &ProtoOaExecutionEvent) -> Action {
    event(execution, request.client_msg_id.as_deref())
}

const STRATEGY: &str = "S-001";

/// A client connected to a peer answering trade requests as `trades`, and
/// the node's side of it.
struct Node {
    client: IcExecutionClient,
    cache: Rc<RefCell<Cache>>,
    events: Events,
    received: Received,
    _tokens: tempfile::TempDir,
}

impl Node {
    async fn start(trades: impl Fn(&ProtoMessage) -> Vec<Action> + Send + Sync + 'static) -> Self {
        let venue = Script {
            trades: Some(Arc::new(trades)),
            ..Script::default()
        };
        let (mut client, cache, mut events, received, tokens) = started(venue);
        connect(&mut client, &mut events, &cache).await;
        Self {
            client,
            cache,
            events,
            received,
            _tokens: tokens,
        }
    }

    /// The cached order `id`.
    fn cached(&self, id: &str) -> OrderAny {
        self.cache
            .borrow()
            .order_owned(&ClientOrderId::from(id))
            .unwrap()
    }

    fn gold(&self) -> InstrumentAny {
        self.client.session().unwrap().instruments[&XAUUSD_SYMBOL_ID].clone()
    }

    /// Adds `orders` to the cache, as the engine does before submitting.
    fn add(&self, orders: &[&OrderAny]) {
        for order in orders {
            self.cache
                .borrow_mut()
                .add_order((*order).clone(), None, None, false)
                .unwrap();
        }
    }

    fn submit(&self, order: &OrderAny, position: Option<PositionId>) {
        self.add(&[order]);
        self.client
            .submit_order(SubmitOrder::from_order(
                order,
                TraderId::from("HELIX-001"),
                Some(ClientId::from("ICMARKETS")),
                position,
                UUID4::new(),
                self.client.now(),
            ))
            .unwrap();
    }

    fn submit_list(&self, orders: &[&OrderAny]) {
        self.add(orders);
        let list = OrderList::new(
            OrderListId::from("OL-1"),
            InstrumentId::from(XAUUSD),
            StrategyId::from(STRATEGY),
            orders.iter().map(|o| o.client_order_id()).collect(),
            self.client.now(),
        );
        self.client
            .submit_order_list(SubmitOrderList::new(
                TraderId::from("HELIX-001"),
                Some(ClientId::from("ICMARKETS")),
                StrategyId::from(STRATEGY),
                list,
                orders.iter().map(|o| o.init_event().clone()).collect(),
                None,
                None,
                None,
                UUID4::new(),
                self.client.now(),
                None,
            ))
            .unwrap();
    }

    fn modify(&self, id: &str, price: Option<Price>, trigger: Option<Price>) {
        let order = self
            .cache
            .borrow()
            .order_owned(&ClientOrderId::from(id))
            .unwrap();
        self.client
            .modify_order(ModifyOrder::new(
                TraderId::from("HELIX-001"),
                Some(ClientId::from("ICMARKETS")),
                order.strategy_id(),
                order.instrument_id(),
                order.client_order_id(),
                order.venue_order_id(),
                None,
                price,
                trigger,
                UUID4::new(),
                self.client.now(),
                None,
                None,
            ))
            .unwrap();
    }

    fn cancel(&self, id: &str) {
        let order = self
            .cache
            .borrow()
            .order_owned(&ClientOrderId::from(id))
            .unwrap();
        self.client
            .cancel_order(CancelOrder::new(
                TraderId::from("HELIX-001"),
                Some(ClientId::from("ICMARKETS")),
                order.strategy_id(),
                order.instrument_id(),
                order.client_order_id(),
                order.venue_order_id(),
                UUID4::new(),
                self.client.now(),
                None,
                None,
            ))
            .unwrap();
    }

    /// The next execution event, an order event applied to the cache.
    async fn next(&mut self) -> ExecutionEvent {
        let event = tokio::time::timeout(Duration::from_secs(10), self.events.recv())
            .await
            .expect("no execution event within 10 s")
            .expect("the execution events ended");
        if let ExecutionEvent::Order(order_event) = &event {
            self.cache.borrow_mut().update_order(order_event).unwrap();
        }
        event
    }

    /// The next `n` order events, applied to the cache.
    async fn order_events(&mut self, n: usize) -> Vec<OrderEventAny> {
        let mut events = Vec::new();
        while events.len() < n {
            match self.next().await {
                ExecutionEvent::Order(event) => events.push(event),
                other => panic!("not an order event: {other:?}"),
            }
        }
        events
    }

    /// Asserts no execution event comes within 300 ms.
    async fn quiet(&mut self) {
        let next = tokio::time::timeout(Duration::from_millis(300), self.events.recv()).await;
        assert!(next.is_err(), "unexpected {next:?}");
    }

    /// Opens `id`, a Nautilus position of `quantity` oz of gold, long, in
    /// the cache, as reconciliation or a fill leaves it.
    fn open_position(&self, id: &str, quantity: u64) {
        let gold = self.gold();
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(quantity))
            .build();
        let OrderEventAny::Filled(fill) = TestOrderEventStubs::filled(
            &order,
            &gold,
            None,
            Some(PositionId::from(id)),
            Some(Price::from("4022.78")),
            None,
            None,
            None,
            None,
            Some(AccountId::from("ICMARKETS-DEMO")),
        ) else {
            panic!("not a fill");
        };
        self.cache
            .borrow_mut()
            .add_position(&Position::new(&gold, fill), OmsType::Hedging)
            .unwrap();
    }

    fn requests<M: Payload>(&self) -> Vec<M> {
        requests_of(&self.received)
    }

    /// The `clientMsgId`s of the requests of type `M`.
    fn tags<M: Payload>(&self) -> Vec<String> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f)| f.payload_type == M::TYPE)
            .filter_map(|(_, f)| f.client_msg_id.clone())
            .collect()
    }
}

fn order(kind: OrderType, id: &str, side: OrderSide, quantity: u64) -> OrderTestBuilder {
    let mut builder = OrderTestBuilder::new(kind);
    builder
        .instrument_id(InstrumentId::from(XAUUSD))
        .strategy_id(StrategyId::from(STRATEGY))
        .client_order_id(ClientOrderId::from(id))
        .side(side)
        .quantity(Quantity::from(quantity));
    builder
}

fn market(id: &str, side: OrderSide, quantity: u64) -> OrderAny {
    order(OrderType::Market, id, side, quantity).build()
}

fn kinds(events: &[OrderEventAny]) -> Vec<(String, &'static str)> {
    events
        .iter()
        .map(|e| {
            let kind = match e {
                OrderEventAny::Submitted(_) => "submitted",
                OrderEventAny::Accepted(_) => "accepted",
                OrderEventAny::Filled(_) => "filled",
                OrderEventAny::Updated(_) => "updated",
                OrderEventAny::Canceled(_) => "canceled",
                OrderEventAny::Rejected(_) => "rejected",
                OrderEventAny::ModifyRejected(_) => "modify rejected",
                OrderEventAny::CancelRejected(_) => "cancel rejected",
                OrderEventAny::Denied(_) => "denied",
                _ => "other",
            };
            (e.client_order_id().to_string(), kind)
        })
        .collect()
}

fn row(id: &str, kind: &'static str) -> (String, &'static str) {
    (id.to_owned(), kind)
}

/// The fill of `events` of order `id`.
fn fill_of(events: &[OrderEventAny], id: &str) -> nautilus_model::events::OrderFilled {
    events
        .iter()
        .find_map(|e| match e {
            OrderEventAny::Filled(f) if f.client_order_id == ClientOrderId::from(id) => {
                Some(f.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no fill of {id}: {events:?}"))
}

fn eur(amount: f64) -> Money {
    Money::new(amount, Currency::from("EUR"))
}

#[tokio::test]
async fn a_market_order_is_sent_with_its_id_and_its_fill_carries_price_commission_and_position() {
    let mut node = Node::start(|request| {
        vec![
            answer(request, &execution(EXECUTION_MARKET_ACCEPTED)),
            answer(request, &execution(EXECUTION_MARKET_FILLED)),
        ]
    })
    .await;

    node.submit(&market("PROBE-1", OrderSide::Buy, 1), None);

    let events = node.order_events(3).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-1", "submitted"),
            row("PROBE-1", "accepted"),
            row("PROBE-1", "filled")
        ]
    );
    let fill = fill_of(&events, "PROBE-1");
    assert_eq!(fill.venue_order_id, VenueOrderId::from("1031776265"));
    assert_eq!(fill.trade_id.to_string(), "953555124");
    assert_eq!(fill.last_px, Price::from("4022.78"));
    assert_eq!(fill.last_qty, Quantity::from(1));
    assert_eq!(fill.position_id, Some(PositionId::from("690004734")));
    // 30 USD per million on 4,022.78 USD is 0.12 USD: 0.11 EUR on the
    // demo's EUR account, as cTrader charged it.
    assert_eq!(fill.commission, Some(eur(0.11)));
    let requests = node.requests::<ProtoOaNewOrderReq>();
    let [request] = requests.as_slice() else {
        panic!("not one order sent");
    };
    assert_eq!(request.ctid_trader_account_id, ACCOUNT_ID);
    assert_eq!(request.symbol_id, XAUUSD_SYMBOL_ID);
    assert_eq!(request.volume, 100);
    assert_eq!(request.client_order_id.as_deref(), Some("PROBE-1"));
    assert_eq!(node.tags::<ProtoOaNewOrderReq>(), ["N:PROBE-1"]);
    let cached = node
        .cache
        .borrow()
        .order_owned(&ClientOrderId::from("PROBE-1"))
        .unwrap();
    assert_eq!(cached.status(), OrderStatus::Filled);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_limit_order_is_accepted_amended_and_cancelled() {
    let mut node = Node::start(|request| {
        let replay = match request.payload_type {
            t if t == ProtoOaNewOrderReq::TYPE => EXECUTION_LIMIT_ACCEPTED,
            t if t == ProtoOaAmendOrderReq::TYPE => EXECUTION_LIMIT_REPLACED,
            _ => EXECUTION_LIMIT_CANCELLED,
        };
        vec![answer(request, &execution(replay))]
    })
    .await;
    let limit = order(OrderType::Limit, "PROBE-2", OrderSide::Buy, 1)
        .price(Price::from("3822.73"))
        .build();

    node.submit(&limit, None);
    let accepted = node.order_events(2).await;
    node.modify("PROBE-2", Some(Price::from("3821.73")), None);
    let updated = node.order_events(1).await;
    node.cancel("PROBE-2");
    let cancelled = node.order_events(1).await;

    assert_eq!(
        kinds(&accepted),
        [row("PROBE-2", "submitted"), row("PROBE-2", "accepted")]
    );
    let OrderEventAny::Updated(update) = updated[0] else {
        panic!("not an update: {updated:?}");
    };
    assert_eq!(update.price, Some(Price::from("3821.73")));
    assert_eq!(update.quantity, Quantity::from(1));
    assert_eq!(kinds(&cancelled), [row("PROBE-2", "canceled")]);
    let sent = node.requests::<ProtoOaNewOrderReq>();
    assert_eq!(sent[0].limit_price, Some(3822.73));
    assert_eq!(sent[0].time_in_force(), ProtoOaTimeInForce::GoodTillCancel);
    let amended = node.requests::<ProtoOaAmendOrderReq>();
    assert_eq!(amended[0].order_id, 1_031_776_354);
    assert_eq!(amended[0].limit_price, Some(3821.73));
    assert_eq!(amended[0].volume, Some(100));
    assert_eq!(node.tags::<ProtoOaAmendOrderReq>(), ["A:PROBE-2"]);
    let cancel = node.requests::<ProtoOaCancelOrderReq>();
    assert_eq!(cancel[0].order_id, 1_031_776_354);
    let cached = node
        .cache
        .borrow()
        .order_owned(&ClientOrderId::from("PROBE-2"))
        .unwrap();
    assert_eq!(cached.status(), OrderStatus::Canceled);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn an_order_ic_refuses_is_rejected_with_ics_reason() {
    let mut node = Node::start(|request| {
        vec![event(
            &decoded::<ProtoOaOrderErrorEvent>(ORDER_ERROR_BAD_VOLUME),
            request.client_msg_id.as_deref(),
        )]
    })
    .await;

    node.submit(&market("PROBE-3", OrderSide::Buy, 1), None);

    let events = node.order_events(2).await;
    let OrderEventAny::Rejected(rejected) = events[1] else {
        panic!("not a rejection: {events:?}");
    };
    assert_eq!(
        rejected.reason.as_str(),
        "TRADING_BAD_VOLUME: Order volume = 0.50 is smaller than minimum allowed volume = 1.00."
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_cancel_ic_refuses_is_rejected_and_the_order_stays_open() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => {
            vec![answer(request, &execution(EXECUTION_LIMIT_ACCEPTED))]
        }
        _ => vec![event(
            &decoded::<ProtoOaOrderErrorEvent>(ORDER_ERROR_ORDER_NOT_FOUND),
            request.client_msg_id.as_deref(),
        )],
    })
    .await;
    let limit = order(OrderType::Limit, "PROBE-2", OrderSide::Buy, 1)
        .price(Price::from("3822.73"))
        .build();
    node.submit(&limit, None);
    node.order_events(2).await;

    node.cancel("PROBE-2");

    let events = node.order_events(1).await;
    let OrderEventAny::CancelRejected(rejected) = events[0] else {
        panic!("not a cancel rejection: {events:?}");
    };
    assert_eq!(
        rejected.reason.as_str(),
        "ORDER_NOT_FOUND: Order not found with id 1031776354"
    );
    let cached = node
        .cache
        .borrow()
        .order_owned(&ClientOrderId::from("PROBE-2"))
        .unwrap();
    assert_eq!(cached.status(), OrderStatus::Accepted);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_fill_before_the_acceptance_and_events_twice_make_one_acceptance_and_one_fill() {
    let mut node = Node::start(|request| {
        let filled = execution(EXECUTION_MARKET_FILLED);
        vec![
            answer(request, &filled),
            answer(request, &execution(EXECUTION_MARKET_ACCEPTED)),
            answer(request, &filled),
        ]
    })
    .await;

    node.submit(&market("PROBE-1", OrderSide::Buy, 1), None);

    let events = node.order_events(3).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-1", "submitted"),
            row("PROBE-1", "accepted"),
            row("PROBE-1", "filled")
        ]
    );
    node.quiet().await;
    node.client.disconnect().await.unwrap();
}

/// The peer never answers the order: its outcome is unknown, the order
/// stays submitted, and a query finds it filled in the order list, from
/// its details: the captured filled market order and its deal.
#[tokio::test]
async fn an_unanswered_order_stays_in_flight_until_a_query_finds_its_outcome() {
    let mut node = Node::start(|request| {
        let filled = execution(EXECUTION_MARKET_FILLED);
        match request.payload_type {
            t if t == ProtoOaOrderListReq::TYPE => vec![reply(
                request,
                &ProtoOaOrderListRes {
                    ctid_trader_account_id: ACCOUNT_ID,
                    order: vec![filled.order.clone().unwrap()],
                    has_more: false,
                    ..Default::default()
                },
            )],
            t if t == ProtoOaOrderDetailsReq::TYPE => vec![reply(
                request,
                &ProtoOaOrderDetailsRes {
                    ctid_trader_account_id: ACCOUNT_ID,
                    order: filled.order.clone().unwrap(),
                    deal: vec![filled.deal.unwrap()],
                    ..Default::default()
                },
            )],
            _ => vec![],
        }
    })
    .await;
    node.submit(&market("PROBE-1", OrderSide::Buy, 1), None);
    node.order_events(1).await;
    node.quiet().await;

    assert!(node.client.retain_unresolved_submissions());
    node.client
        .query_order(QueryOrder::new(
            TraderId::from("HELIX-001"),
            Some(ClientId::from("ICMARKETS")),
            StrategyId::from(STRATEGY),
            InstrumentId::from(XAUUSD),
            ClientOrderId::from("PROBE-1"),
            None,
            UUID4::new(),
            node.client.now(),
            None,
            None,
        ))
        .unwrap();

    let ExecutionEvent::Report(nautilus_common::messages::ExecutionReport::OrderWithFills(
        status,
        fills,
    )) = node.next().await
    else {
        panic!("not an order report with fills");
    };
    assert_eq!(status.client_order_id, Some(ClientOrderId::from("PROBE-1")));
    assert_eq!(status.venue_order_id, VenueOrderId::from("1031776265"));
    assert_eq!(status.order_status, OrderStatus::Filled);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].trade_id.to_string(), "953555124");
    let details: Vec<ProtoOaOrderDetailsReq> = node.requests();
    assert_eq!(details[0].order_id, 1_031_776_265);
    node.client.disconnect().await.unwrap();
}

/// A market entry protected by a stop loss and a take profit.
fn bracket(entry: &str, stop_loss: &str, take_profit: &str) -> [OrderAny; 3] {
    let (sl, tp) = (format!("{entry}-SL"), format!("{entry}-TP"));
    let legs = vec![
        ClientOrderId::from(sl.as_str()),
        ClientOrderId::from(tp.as_str()),
    ];
    let list = OrderListId::from("OL-1");
    let entry_order = order(OrderType::Market, entry, OrderSide::Buy, 1)
        .contingency_type(ContingencyType::Oto)
        .linked_order_ids(legs)
        .order_list_id(list)
        .build();
    let leg = |kind, id: &str, other: &str| {
        let mut builder = order(kind, id, OrderSide::Sell, 1);
        builder
            .contingency_type(ContingencyType::Oco)
            .linked_order_ids(vec![ClientOrderId::from(other)])
            .parent_order_id(ClientOrderId::from(entry))
            .order_list_id(list)
            .reduce_only(true);
        builder
    };
    let stop = leg(OrderType::StopMarket, &sl, &tp)
        .trigger_price(Price::from(stop_loss))
        .build();
    let limit = leg(OrderType::Limit, &tp, &sl)
        .price(Price::from(take_profit))
        .build();
    [entry_order, stop, limit]
}

#[tokio::test]
async fn a_bracket_entry_carries_its_protections_which_the_position_then_holds() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => vec![
            answer(request, &execution(EXECUTION_MARKET_ACCEPTED)),
            answer(request, &execution(EXECUTION_MARKET_FILLED)),
            event(&execution(EXECUTION_PROTECTION_ACCEPTED), Some("PROBE-1")),
        ],
        _ => vec![answer(request, &execution(EXECUTION_PROTECTION_REPLACED))],
    })
    .await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-1", "3992.78", "4052.78");

    node.submit_list(&[&entry, &stop_loss, &take_profit]);

    let events = node.order_events(7).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-1", "submitted"),
            row("PROBE-1-SL", "submitted"),
            row("PROBE-1-TP", "submitted"),
            row("PROBE-1", "accepted"),
            row("PROBE-1", "filled"),
            row("PROBE-1-SL", "accepted"),
            row("PROBE-1-TP", "accepted"),
        ]
    );
    let (sl, tp) = (node.cached("PROBE-1-SL"), node.cached("PROBE-1-TP"));
    assert_eq!(
        sl.venue_order_id(),
        Some(VenueOrderId::from("690004734-SL"))
    );
    assert_eq!(
        tp.venue_order_id(),
        Some(VenueOrderId::from("690004734-TP"))
    );
    let sent = node.requests::<ProtoOaNewOrderReq>();
    assert_eq!(sent.len(), 1, "the legs ride on the entry");
    assert_eq!(sent[0].order_type(), ProtoOaOrderType::Market);
    assert_eq!(sent[0].stop_loss, Some(3992.78));
    assert_eq!(sent[0].take_profit, Some(4052.78));

    // Moving the stop loss sends both prices: cTrader drops one left out.
    // The demo's answer moved the take profit too (to 4062.78): the legs
    // follow it.
    node.modify("PROBE-1-SL", None, Some(Price::from("3982.78")));

    let updated = node.order_events(2).await;
    let amended = node.requests::<ProtoOaAmendPositionSltpReq>();
    assert_eq!(amended[0].position_id, 690_004_734);
    assert_eq!(amended[0].stop_loss, Some(3982.78));
    assert_eq!(amended[0].take_profit, Some(4052.78));
    assert_eq!(node.tags::<ProtoOaAmendPositionSltpReq>(), ["P:PROBE-1-SL"]);
    let (sl, tp) = (node.cached("PROBE-1-SL"), node.cached("PROBE-1-TP"));
    assert_eq!(kinds(&updated).len(), 2);
    assert_eq!(sl.trigger_price(), Some(Price::from("3982.78")));
    assert_eq!(tp.price(), Some(Price::from("4062.78")));
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn cancelling_a_protection_keeps_the_other_on_the_position() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => vec![
            answer(request, &execution(EXECUTION_MARKET_FILLED)),
            event(&execution(EXECUTION_PROTECTION_ACCEPTED), Some("PROBE-1")),
        ],
        _ => {
            // The protection with its take profit gone.
            let mut replaced = execution(EXECUTION_PROTECTION_ACCEPTED);
            replaced.set_execution_type(ProtoOaExecutionType::OrderReplaced);
            if let Some(order) = replaced.order.as_mut() {
                order.limit_price = None;
            }
            vec![answer(request, &replaced)]
        }
    })
    .await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-1", "3992.78", "4052.78");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    node.order_events(7).await;

    node.cancel("PROBE-1-TP");

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("PROBE-1-TP", "canceled")]);
    let amended = node.requests::<ProtoOaAmendPositionSltpReq>();
    assert_eq!(amended[0].stop_loss, Some(3992.78));
    assert_eq!(amended[0].take_profit, None);
    assert_eq!(node.tags::<ProtoOaAmendPositionSltpReq>(), ["U:PROBE-1-TP"]);
    node.client.disconnect().await.unwrap();
}

/// The demo's stop loss hit (`execution_stop_loss_*`), the position also
/// protected by a take profit at 4053.51.
fn protected(bytes: &[u8]) -> ProtoOaExecutionEvent {
    let mut execution = execution(bytes);
    if let Some(order) = execution.order.as_mut() {
        order.limit_price = Some(4053.51);
    }
    execution
}

/// The bracket of the demo's stop loss hit: entry PROBE-5, its stop loss
/// at 4023.15 and take profit at 4053.51, the entry filled and its
/// protections accepted, then `last`.
async fn bracket_closed_by(last: ProtoOaExecutionEvent) -> Vec<OrderEventAny> {
    let mut node = Node::start(move |request| {
        vec![
            answer(request, &execution(EXECUTION_STOP_LOSS_ENTRY_FILLED)),
            event(&protected(EXECUTION_STOP_LOSS_ACCEPTED), Some("PROBE-5")),
            event(&last, Some("PROBE-5")),
        ]
    })
    .await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-5", "4023.15", "4053.51");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    let events = node.order_events(9).await;
    node.quiet().await;
    node.client.disconnect().await.unwrap();
    events
}

#[tokio::test]
async fn a_stop_loss_hit_fills_the_stop_loss_on_the_position_and_cancels_the_take_profit() {
    let events = bracket_closed_by(protected(EXECUTION_STOP_LOSS_FILLED)).await;

    assert_eq!(
        kinds(&events[3..]),
        [
            row("PROBE-5", "accepted"),
            row("PROBE-5", "filled"),
            row("PROBE-5-SL", "accepted"),
            row("PROBE-5-TP", "accepted"),
            row("PROBE-5-SL", "filled"),
            row("PROBE-5-TP", "canceled"),
        ]
    );
    let entry = fill_of(&events, "PROBE-5");
    let stop = fill_of(&events, "PROBE-5-SL");
    assert_eq!(entry.position_id, Some(PositionId::from("690004999")));
    assert_eq!(stop.position_id, Some(PositionId::from("690004999")));
    assert_eq!(stop.venue_order_id, VenueOrderId::from("690004999-SL"));
    assert_eq!(stop.order_side, OrderSide::Sell);
    assert_eq!(stop.last_qty, Quantity::from(1));
    assert_eq!(stop.last_px, Price::from("4023.15"));
    assert_eq!(stop.trade_id.to_string(), "953555340");
    assert_eq!(stop.commission, Some(eur(0.11)));
}

#[tokio::test]
async fn a_take_profit_hit_fills_the_take_profit_and_cancels_the_stop_loss() {
    let mut hit = protected(EXECUTION_STOP_LOSS_FILLED);
    if let Some(deal) = hit.deal.as_mut() {
        deal.execution_price = Some(4053.51);
    }

    let events = bracket_closed_by(hit).await;

    assert_eq!(
        kinds(&events[7..]),
        [row("PROBE-5-TP", "filled"), row("PROBE-5-SL", "canceled")]
    );
    assert_eq!(
        fill_of(&events, "PROBE-5-TP").last_px,
        Price::from("4053.51")
    );
}

/// The demo's close of position 690004734, by the closing order
/// `CLOSE-1`, the position left with `left` cents.
fn closed(bytes: &[u8], left: i64) -> ProtoOaExecutionEvent {
    let mut execution = execution(bytes);
    if let Some(order) = execution.order.as_mut() {
        order.client_order_id = Some("C:CLOSE-1".to_owned());
    }
    if let Some(position) = execution.position.as_mut().filter(|_| left > 0) {
        position.trade_data.volume = left;
        position.set_position_status(
            crate::data::ic::messages::ProtoOaPositionStatus::PositionStatusOpen,
        );
    }
    execution
}

#[tokio::test]
async fn closing_one_of_two_positions_closes_it_by_its_id_and_leaves_the_other() {
    let mut node = Node::start(|request| {
        vec![
            answer(request, &closed(EXECUTION_CLOSE_ACCEPTED, 0)),
            answer(request, &closed(EXECUTION_CLOSE_FILLED, 0)),
        ]
    })
    .await;
    node.open_position("690004734", 1);
    node.open_position("690004999", 1);

    node.submit(
        &market("CLOSE-1", OrderSide::Sell, 1),
        Some(PositionId::from("690004734")),
    );

    let events = node.order_events(3).await;
    assert_eq!(
        kinds(&events),
        [
            row("CLOSE-1", "submitted"),
            row("CLOSE-1", "accepted"),
            row("CLOSE-1", "filled")
        ]
    );
    let fill = fill_of(&events, "CLOSE-1");
    assert_eq!(fill.position_id, Some(PositionId::from("690004734")));
    assert_eq!(fill.last_px, Price::from("4023.56"));
    assert_eq!(fill.order_side, OrderSide::Sell);
    let closes = node.requests::<ProtoOaClosePositionReq>();
    let [close] = closes.as_slice() else {
        panic!("not one close");
    };
    assert_eq!((close.position_id, close.volume), (690_004_734, 100));
    assert_eq!(node.tags::<ProtoOaClosePositionReq>(), ["C:CLOSE-1"]);
    assert!(
        node.requests::<ProtoOaNewOrderReq>().is_empty(),
        "no order opened"
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_partial_close_closes_part_of_the_position_by_its_id() {
    let mut node = Node::start(|request| {
        vec![
            answer(request, &closed(EXECUTION_CLOSE_ACCEPTED, 100)),
            answer(request, &closed(EXECUTION_CLOSE_FILLED, 100)),
        ]
    })
    .await;
    node.open_position("690004734", 2);

    node.submit(
        &market("CLOSE-1", OrderSide::Sell, 1),
        Some(PositionId::from("690004734")),
    );

    let events = node.order_events(3).await;
    assert_eq!(fill_of(&events, "CLOSE-1").last_qty, Quantity::from(1));
    let closes = node.requests::<ProtoOaClosePositionReq>();
    let [close] = closes.as_slice() else {
        panic!("not one close");
    };
    assert_eq!((close.position_id, close.volume), (690_004_734, 100));
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_close_of_a_position_ic_no_longer_holds_is_rejected_with_ics_reason() {
    let mut node = Node::start(|request| {
        vec![event(
            &decoded::<ProtoOaOrderErrorEvent>(ORDER_ERROR_POSITION_NOT_FOUND),
            request.client_msg_id.as_deref(),
        )]
    })
    .await;
    node.open_position("690004734", 1);

    node.submit(
        &market("CLOSE-1", OrderSide::Sell, 1),
        Some(PositionId::from("690004734")),
    );

    let events = node.order_events(2).await;
    let OrderEventAny::Rejected(rejected) = &events[1] else {
        panic!("not a rejection: {events:?}");
    };
    assert_eq!(
        rejected.reason.as_str(),
        "POSITION_NOT_FOUND: Position not found with id 690004734"
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_close_larger_than_its_position_is_denied_and_nothing_is_sent() {
    let mut node = Node::start(|_| vec![]).await;
    node.open_position("690004734", 1);

    node.submit(
        &market("CLOSE-1", OrderSide::Sell, 2),
        Some(PositionId::from("690004734")),
    );

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("CLOSE-1", "denied")]);
    assert!(node.requests::<ProtoOaClosePositionReq>().is_empty());
    assert!(node.requests::<ProtoOaNewOrderReq>().is_empty());
    node.client.disconnect().await.unwrap();
}

/// A closing order's events, its protection cancelled after it: the
/// bracket's legs are cancelled with the position.
#[tokio::test]
async fn closing_a_protected_position_cancels_its_protections() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => vec![
            answer(request, &execution(EXECUTION_MARKET_FILLED)),
            event(&execution(EXECUTION_PROTECTION_ACCEPTED), Some("PROBE-1")),
        ],
        _ => vec![
            answer(request, &closed(EXECUTION_CLOSE_ACCEPTED, 0)),
            answer(request, &closed(EXECUTION_CLOSE_FILLED, 0)),
            answer(request, &execution(EXECUTION_PROTECTION_CANCELLED)),
        ],
    })
    .await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-1", "3992.78", "4052.78");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    node.order_events(7).await;
    node.open_position("690004734", 1);

    node.submit(
        &market("CLOSE-1", OrderSide::Sell, 1),
        Some(PositionId::from("690004734")),
    );

    let events = node.order_events(5).await;
    assert_eq!(
        kinds(&events),
        [
            row("CLOSE-1", "submitted"),
            row("CLOSE-1", "accepted"),
            row("CLOSE-1", "filled"),
            row("PROBE-1-SL", "canceled"),
            row("PROBE-1-TP", "canceled"),
        ]
    );
    node.client.disconnect().await.unwrap();
}
