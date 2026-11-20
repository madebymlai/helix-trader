//! The order flow of `CTraderExecutionClient` against the mock peer.
//!
//! The tests use the trade events captured on the demo (`test_data/execution_*`,
//! `order_error_*`), mutated where a test says so. They play the node's side: orders go into the
//! cache before they are submitted, and every order event the client emits is applied to them, as
//! the execution engine does.

use nautilus_common::messages::ExecutionReport;
use nautilus_model::{
    enums::{ContingencyType, OmsType},
    events::{OrderEventAny, OrderFilled},
    identifiers::{ClientOrderId, OrderListId, StrategyId},
    orders::{OrderList, OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
};

use super::*;
use crate::{
    common::messages::{
        ProtoOaAmendOrderReq, ProtoOaAmendPositionSltpReq, ProtoOaCancelOrderReq,
        ProtoOaClosePositionReq, ProtoOaExecutionEvent, ProtoOaExecutionType, ProtoOaNewOrderReq,
        ProtoOaOrderDetailsReq, ProtoOaOrderDetailsRes, ProtoOaOrderErrorEvent,
        ProtoOaOrderListReq, ProtoOaOrderListRes,
    },
    testing::{
        EXECUTION_CLOSE_ACCEPTED, EXECUTION_CLOSE_FILLED, EXECUTION_LIMIT_ACCEPTED,
        EXECUTION_LIMIT_CANCELLED, EXECUTION_LIMIT_REPLACED, EXECUTION_MARKET_ACCEPTED,
        EXECUTION_MARKET_FILLED, EXECUTION_MARKET_RANGE_ACCEPTED,
        EXECUTION_MARKET_RANGE_BRACKET_ACCEPTED, EXECUTION_MARKET_RANGE_BRACKET_FILLED,
        EXECUTION_MARKET_RANGE_BRACKET_PROTECTION_ACCEPTED,
        EXECUTION_MARKET_RANGE_BUY_BETTER_ACCEPTED, EXECUTION_MARKET_RANGE_BUY_BETTER_FILLED,
        EXECUTION_MARKET_RANGE_DEAL_REJECTED, EXECUTION_MARKET_RANGE_SELL_BETTER_ACCEPTED,
        EXECUTION_MARKET_RANGE_SELL_BETTER_FILLED, EXECUTION_PROTECTION_ACCEPTED,
        EXECUTION_PROTECTION_CANCELLED, EXECUTION_PROTECTION_REPLACED,
        EXECUTION_STOP_LOSS_ACCEPTED, EXECUTION_STOP_LOSS_ENTRY_FILLED, EXECUTION_STOP_LOSS_FILLED,
        ORDER_ERROR_BAD_VOLUME, ORDER_ERROR_ORDER_NOT_FOUND, ORDER_ERROR_POSITION_NOT_FOUND, event,
    },
};

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

/// Synthetic: a deal the broker made after the captured ones.
const LATER_DEAL: i64 = 953_555_999;

/// The captured fill `bytes` under `LATER_DEAL`.
fn later_deal(bytes: &[u8]) -> ProtoOaExecutionEvent {
    let mut execution = execution(bytes);
    if let Some(deal) = execution.deal.as_mut() {
        deal.deal_id = LATER_DEAL;
    }
    execution
}

fn answer(request: &ProtoMessage, execution: &ProtoOaExecutionEvent) -> Action {
    event(execution, request.client_msg_id.as_deref())
}

const STRATEGY: &str = "S-001";

struct Node {
    client: CTraderExecutionClient,
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

    fn cached(&self, id: &str) -> OrderAny {
        self.cache
            .borrow()
            .order_owned(&ClientOrderId::from(id))
            .unwrap()
    }

    fn gold(&self) -> InstrumentAny {
        self.client.session().unwrap().instruments[&XAUUSD_SYMBOL_ID].clone()
    }

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
                Some(ClientId::from(CLIENT)),
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
                Some(ClientId::from(CLIENT)),
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
                Some(ClientId::from(CLIENT)),
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
                Some(ClientId::from(CLIENT)),
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

    async fn quiet(&mut self) {
        let next = tokio::time::timeout(Duration::from_millis(300), self.events.recv()).await;
        assert!(next.is_err(), "unexpected {next:?}");
    }

    /// The last event the broker sent: nothing came before it.
    async fn later_deal_next(&mut self) {
        let event = self.next().await;
        let ExecutionEvent::Report(ExecutionReport::OrderWithFills(_, fills)) = &event else {
            panic!("not the report of the later deal: {event:?}");
        };
        let trades: Vec<String> = fills.iter().map(|f| f.trade_id.to_string()).collect();
        assert_eq!(trades, ["953555999"]);
    }

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
            Some(AccountId::from("ACMEFX-DEMO")),
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
                OrderEventAny::ModifyDenied(_) => "modify denied",
                OrderEventAny::CancelRejected(_) => "cancel rejected",
                OrderEventAny::CancelDenied(_) => "cancel denied",
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

fn fill_of(events: &[OrderEventAny], id: &str) -> OrderFilled {
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

    // 30 USD per million on 4,022.78 USD is 0.12 USD, or 0.11 EUR on the demo's EUR account
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
    node.client.disconnect().await.unwrap();
}

/// The captured MARKET_RANGE orders based 3.00 on the better side of the market.
///
/// They are sent as a limit IOC at that price. Each filled at the market, better than its limit.
#[rstest]
#[case::buy(
    "PROBE-IOC-1",
    OrderSide::Buy,
    "3869.20",
    EXECUTION_MARKET_RANGE_BUY_BETTER_ACCEPTED,
    EXECUTION_MARKET_RANGE_BUY_BETTER_FILLED,
    "3866.20",
    "961302364"
)]
#[case::sell(
    "PROBE-IOC-2",
    OrderSide::Sell,
    "3863.09",
    EXECUTION_MARKET_RANGE_SELL_BETTER_ACCEPTED,
    EXECUTION_MARKET_RANGE_SELL_BETTER_FILLED,
    "3866.09",
    "961302463"
)]
#[tokio::test]
async fn a_limit_ioc_order_is_a_market_range_filled_at_its_price_or_better(
    #[case] id: &str,
    #[case] side: OrderSide,
    #[case] limit: &str,
    #[case] accepted: &'static [u8],
    #[case] filled: &'static [u8],
    #[case] price: &str,
    #[case] deal: &str,
) {
    let mut node = Node::start(move |request| {
        vec![
            answer(request, &execution(accepted)),
            answer(request, &execution(filled)),
        ]
    })
    .await;
    let order = order(OrderType::Limit, id, side, 1)
        .price(Price::from(limit))
        .time_in_force(TimeInForce::Ioc)
        .build();

    node.submit(&order, None);

    let events = node.order_events(3).await;
    assert_eq!(
        kinds(&events),
        [row(id, "submitted"), row(id, "accepted"), row(id, "filled")]
    );
    let fill = fill_of(&events, id);
    assert_eq!(fill.order_type, OrderType::Limit, "{id}");
    assert_eq!(fill.last_px, Price::from(price));
    assert_eq!(fill.trade_id.to_string(), deal);
    assert_eq!(fill.commission, Some(eur(0.11)));
    let sent = node.requests::<ProtoOaNewOrderReq>();
    assert_eq!(sent[0].order_type(), ProtoOaOrderType::MarketRange);
    assert_eq!(
        sent[0].base_slippage_price,
        Some(Price::from(limit).as_f64())
    );
    assert_eq!(sent[0].slippage_in_points, Some(0));
    node.client.disconnect().await.unwrap();
}

/// The captured MARKET_RANGE buy based 5.00 under the ask.
///
/// It is sent as a limit IOC buy at that price. cTrader cancels it with its deal rejected.
#[tokio::test]
async fn a_limit_ioc_order_ctrader_cannot_fill_at_its_price_is_rejected() {
    let mut node = Node::start(|request| {
        vec![
            answer(request, &execution(EXECUTION_MARKET_RANGE_ACCEPTED)),
            answer(request, &execution(EXECUTION_MARKET_RANGE_DEAL_REJECTED)),
        ]
    })
    .await;
    let order = order(OrderType::Limit, "PROBE-REJECT-1", OrderSide::Buy, 1)
        .price(Price::from("3868.88"))
        .time_in_force(TimeInForce::Ioc)
        .build();

    node.submit(&order, None);

    let events = node.order_events(3).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-REJECT-1", "submitted"),
            row("PROBE-REJECT-1", "accepted"),
            row("PROBE-REJECT-1", "rejected")
        ]
    );
    let OrderEventAny::Rejected(rejected) = events[2] else {
        panic!("not a rejection: {events:?}");
    };
    assert_eq!(
        rejected.reason.as_str(),
        "INTERNALLY_REJECTED: Market VWAP is worse than Limit price"
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_modify_of_an_order_the_broker_has_not_accepted_is_denied_naming_the_venue() {
    let mut node = Node::start(|_| vec![]).await;
    let limit = order(OrderType::Limit, "PROBE-2", OrderSide::Buy, 1)
        .price(Price::from("3822.73"))
        .build();
    node.submit(&limit, None);
    node.order_events(1).await;

    node.modify("PROBE-2", Some(Price::from("3821.73")), None);

    let events = node.order_events(1).await;
    let OrderEventAny::ModifyDenied(denied) = &events[0] else {
        panic!("not a modify denial: {events:?}");
    };
    assert_eq!(denied.client_order_id, ClientOrderId::from("PROBE-2"));
    assert!(
        denied.reason.as_str().contains(VENUE),
        "the reason does not name {VENUE}: {}",
        denied.reason
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn an_order_the_broker_refuses_is_rejected_with_its_reason() {
    let mut node = Node::start(|request| {
        vec![event(
            &decoded::<ProtoOaOrderErrorEvent>(ORDER_ERROR_BAD_VOLUME),
            request.client_msg_id.as_deref(),
        )]
    })
    .await;

    node.submit(&market("PROBE-3", OrderSide::Buy, 1), None);

    let events = node.order_events(2).await;
    assert_eq!(
        kinds(&events),
        [row("PROBE-3", "submitted"), row("PROBE-3", "rejected")]
    );
    let OrderEventAny::Rejected(rejected) = events[1] else {
        panic!("not a rejection: {events:?}");
    };
    assert_eq!(
        rejected.reason.as_str(),
        "TRADING_BAD_VOLUME: Order volume = 0.50 is smaller than minimum allowed volume = 1.00."
    );
    node.client.disconnect().await.unwrap();
}

/// The reason is the code and the start of the description.
///
/// Each run of control characters and whitespace becomes one space. The reason has 256 characters
/// at most.
#[tokio::test]
async fn a_refusal_is_rejected_with_its_reason_cleaned_and_bounded() {
    let mut node = Node::start(|request| {
        let mut error = decoded::<ProtoOaOrderErrorEvent>(ORDER_ERROR_BAD_VOLUME);
        error.description = Some(format!(
            "Order volume\r\n\tis too small\u{7}{}",
            "x".repeat(1000)
        ));
        vec![event(&error, request.client_msg_id.as_deref())]
    })
    .await;

    node.submit(&market("PROBE-3", OrderSide::Buy, 1), None);

    let events = node.order_events(2).await;
    let OrderEventAny::Rejected(rejected) = events[1] else {
        panic!("not a rejection: {events:?}");
    };
    let start = "TRADING_BAD_VOLUME: Order volume is too small ";
    let expected = format!("{start}{}", "x".repeat(256 - start.len()));
    assert_eq!(rejected.reason.as_str(), expected);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_cancel_the_broker_refuses_is_rejected_and_the_order_stays_open() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => {
            vec![answer(request, &execution(EXECUTION_LIMIT_ACCEPTED))]
        }
        t if t == ProtoOaAmendOrderReq::TYPE => {
            vec![answer(request, &execution(EXECUTION_LIMIT_REPLACED))]
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

    // Still open: the broker's answer to a modification updates it
    node.modify("PROBE-2", Some(Price::from("3821.73")), None);
    let updated = node.order_events(1).await;
    assert_eq!(kinds(&updated), [row("PROBE-2", "updated")]);
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
            answer(request, &later_deal(EXECUTION_MARKET_FILLED)),
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
    node.later_deal_next().await;
    node.client.disconnect().await.unwrap();
}

/// The captured filled market order and its deal, found by the query in the order list.
#[tokio::test]
async fn an_unanswered_order_stays_in_flight_until_a_query_finds_its_outcome() {
    let mut node = Node::start(|request| {
        let filled = execution(EXECUTION_MARKET_FILLED);
        match request.payload_type {
            t if t == ProtoOaOrderListReq::TYPE => vec![reply(
                request,
                &ProtoOaOrderListRes {
                    ctid_trader_account_id: ACCOUNT_ID,
                    order: vec![filled.order.unwrap()],
                    has_more: false,
                    ..Default::default()
                },
            )],
            t if t == ProtoOaOrderDetailsReq::TYPE => vec![reply(
                request,
                &ProtoOaOrderDetailsRes {
                    ctid_trader_account_id: ACCOUNT_ID,
                    order: filled.order.unwrap(),
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

    assert!(node.client.retain_unresolved_submissions());
    node.client
        .query_order(QueryOrder::new(
            TraderId::from("HELIX-001"),
            Some(ClientId::from(CLIENT)),
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

    let ExecutionEvent::Report(ExecutionReport::OrderWithFills(status, fills)) = node.next().await
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

/// The captured filled market order PROBE-1 moved to US500, a symbol the run did not load.
///
/// The lookup cannot report it, and says so rather than answer that the broker holds no such
/// order.
#[tokio::test]
async fn a_lookup_of_an_order_on_a_symbol_the_run_did_not_load_fails() {
    let mut held = execution(EXECUTION_MARKET_FILLED).order.unwrap();
    held.trade_data.symbol_id = US500_SYMBOL_ID;
    let mut node = Node::start(move |request| match request.payload_type {
        t if t == ProtoOaOrderListReq::TYPE => vec![reply(
            request,
            &ProtoOaOrderListRes {
                ctid_trader_account_id: ACCOUNT_ID,
                order: vec![held.clone()],
                has_more: false,
                ..Default::default()
            },
        )],
        t if t == ProtoOaOrderDetailsReq::TYPE => vec![reply(
            request,
            &ProtoOaOrderDetailsRes {
                ctid_trader_account_id: ACCOUNT_ID,
                order: held.clone(),
                ..Default::default()
            },
        )],
        _ => vec![],
    })
    .await;
    node.add(&[&market("PROBE-1", OrderSide::Buy, 1)]);

    let lookup = node
        .client
        .generate_order_status_report(&GenerateOrderStatusReport::new(
            UUID4::new(),
            node.client.now(),
            Some(InstrumentId::from(XAUUSD)),
            Some(ClientOrderId::from("PROBE-1")),
            None,
            None,
            None,
        ))
        .await;

    let error = format!("{:#}", lookup.unwrap_err());
    assert!(error.contains(VENUE), "{error}");
    assert!(error.contains("PROBE-1"), "{error}");
    let details: Vec<ProtoOaOrderDetailsReq> = node.requests();
    assert_eq!(details[0].order_id, 1_031_776_265);
    node.client.disconnect().await.unwrap();
}

/// Its deal states no commission (Nautilus' `spec_exec_testing.md`, TC-E88).
#[tokio::test]
async fn a_lookup_of_an_order_whose_deal_states_no_commission_fails() {
    let filled = execution(EXECUTION_MARKET_FILLED);
    let mut deal = filled.deal.unwrap();
    deal.commission = None;
    let held = filled.order.unwrap();
    let mut node = Node::start(move |request| match request.payload_type {
        t if t == ProtoOaOrderListReq::TYPE => vec![reply(
            request,
            &ProtoOaOrderListRes {
                ctid_trader_account_id: ACCOUNT_ID,
                order: vec![held.clone()],
                has_more: false,
                ..Default::default()
            },
        )],
        t if t == ProtoOaOrderDetailsReq::TYPE => vec![reply(
            request,
            &ProtoOaOrderDetailsRes {
                ctid_trader_account_id: ACCOUNT_ID,
                order: held.clone(),
                deal: vec![deal.clone()],
                ..Default::default()
            },
        )],
        _ => vec![],
    })
    .await;
    node.add(&[&market("PROBE-1", OrderSide::Buy, 1)]);

    let lookup = node
        .client
        .generate_order_status_report(&GenerateOrderStatusReport::new(
            UUID4::new(),
            node.client.now(),
            Some(InstrumentId::from(XAUUSD)),
            Some(ClientOrderId::from("PROBE-1")),
            None,
            None,
            None,
        ))
        .await;

    let error = format!("{:#}", lookup.unwrap_err());
    assert!(error.contains("cTrader deal 953555124"), "{error}");
    assert!(error.contains("no commission"), "{error}");
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_lookup_of_an_order_without_its_client_order_id_fails() {
    let mut node = Node::start(|_| vec![]).await;

    let lookup = node
        .client
        .generate_order_status_report(&GenerateOrderStatusReport::new(
            UUID4::new(),
            node.client.now(),
            Some(InstrumentId::from(XAUUSD)),
            None,
            Some(VenueOrderId::from("1031776265")),
            None,
            None,
        ))
        .await;

    let error = format!("{:#}", lookup.unwrap_err());
    assert!(error.contains("no client order id"), "{error}");
    node.client.disconnect().await.unwrap();
}

fn bracket(entry: &str, stop_loss: &str, take_profit: &str) -> [OrderAny; 3] {
    let market = order(OrderType::Market, entry, OrderSide::Buy, 1);
    bracket_on(market, entry, stop_loss, take_profit)
}

fn bracket_on(
    builder: OrderTestBuilder,
    entry: &str,
    stop_loss: &str,
    take_profit: &str,
) -> [OrderAny; 3] {
    bracket_lasting(builder, entry, stop_loss, take_profit, TimeInForce::Gtc)
}

/// A GTD leg expires 2026-11-16 23:00 UTC.
fn bracket_lasting(
    mut builder: OrderTestBuilder,
    entry: &str,
    stop_loss: &str,
    take_profit: &str,
    lasting: TimeInForce,
) -> [OrderAny; 3] {
    let (sl, tp) = (format!("{entry}-SL"), format!("{entry}-TP"));
    let legs = vec![
        ClientOrderId::from(sl.as_str()),
        ClientOrderId::from(tp.as_str()),
    ];
    let list = OrderListId::from("OL-1");
    let entry_order = builder
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
            .reduce_only(true)
            .time_in_force(lasting);
        if lasting == TimeInForce::Gtd {
            builder.expire_time(UnixNanos::from(1_794_870_000_000_000_000));
        }
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

fn bracket_entry(request: &ProtoMessage) -> Vec<Action> {
    match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => vec![
            answer(request, &execution(EXECUTION_MARKET_ACCEPTED)),
            answer(request, &execution(EXECUTION_MARKET_FILLED)),
            event(&execution(EXECUTION_PROTECTION_ACCEPTED), Some("PROBE-1")),
        ],
        _ => vec![answer(request, &execution(EXECUTION_PROTECTION_REPLACED))],
    }
}

#[tokio::test]
async fn a_bracket_entry_carries_its_protections_which_the_position_then_holds() {
    let mut node = Node::start(bracket_entry).await;
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
    node.client.disconnect().await.unwrap();
}

/// The captured MARKET_RANGE entry PROBE-IOC-5 with its absolute stop loss and take profit.
///
/// cTrader set them on the position it opened, as it does for a market entry.
#[tokio::test]
async fn a_limit_ioc_bracket_entry_carries_its_protections_which_the_position_then_holds() {
    let mut node = Node::start(|request| {
        vec![
            answer(request, &execution(EXECUTION_MARKET_RANGE_BRACKET_ACCEPTED)),
            answer(request, &execution(EXECUTION_MARKET_RANGE_BRACKET_FILLED)),
            event(
                &execution(EXECUTION_MARKET_RANGE_BRACKET_PROTECTION_ACCEPTED),
                Some("PROBE-IOC-5"),
            ),
        ]
    })
    .await;
    let mut limit = order(OrderType::Limit, "PROBE-IOC-5", OrderSide::Buy, 1);
    limit
        .price(Price::from("3871.58"))
        .time_in_force(TimeInForce::Ioc);
    let [entry, stop_loss, take_profit] = bracket_on(limit, "PROBE-IOC-5", "3851.58", "3891.58");

    node.submit_list(&[&entry, &stop_loss, &take_profit]);

    let events = node.order_events(7).await;
    assert_eq!(
        kinds(&events)[3..],
        [
            row("PROBE-IOC-5", "accepted"),
            row("PROBE-IOC-5", "filled"),
            row("PROBE-IOC-5-SL", "accepted"),
            row("PROBE-IOC-5-TP", "accepted"),
        ]
    );
    assert_eq!(
        node.cached("PROBE-IOC-5-SL").venue_order_id(),
        Some(VenueOrderId::from("697002710-SL"))
    );
    let sent = node.requests::<ProtoOaNewOrderReq>();
    assert_eq!(sent[0].order_type(), ProtoOaOrderType::MarketRange);
    assert_eq!(sent[0].base_slippage_price, Some(3871.58));
    assert_eq!(sent[0].stop_loss, Some(3851.58));
    assert_eq!(sent[0].take_profit, Some(3891.58));
    node.client.disconnect().await.unwrap();
}

/// The legs are the protections of their position.
///
/// cTrader holds them as long as the position (GOOD_TILL_CANCEL).
#[rstest]
#[case::ioc(TimeInForce::Ioc)]
#[case::gtd(TimeInForce::Gtd)]
#[case::day(TimeInForce::Day)]
#[tokio::test]
async fn a_bracket_whose_legs_are_not_gtc_is_denied_and_nothing_is_sent(
    #[case] lasting: TimeInForce,
) {
    let mut node = Node::start(|_| vec![]).await;
    let [entry, stop_loss, take_profit] = bracket_lasting(
        order(OrderType::Market, "PROBE-1", OrderSide::Buy, 1),
        "PROBE-1",
        "3992.78",
        "4052.78",
        lasting,
    );

    node.submit_list(&[&entry, &stop_loss, &take_profit]);

    let events = node.order_events(3).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-1", "denied"),
            row("PROBE-1-SL", "denied"),
            row("PROBE-1-TP", "denied"),
        ]
    );
    let OrderEventAny::Denied(denied) = events[0] else {
        panic!("not a denial: {events:?}");
    };
    assert_eq!(
        denied.reason.as_str(),
        OrderDeniedReason::UnsupportedTimeInForce(lasting).to_string()
    );
    assert!(node.requests::<ProtoOaNewOrderReq>().is_empty());
    node.client.disconnect().await.unwrap();
}

/// cTrader drops a price left out. The demo's answer moved the take profit too (to 4062.78).
#[tokio::test]
async fn moving_a_positions_stop_loss_sends_both_prices_and_the_legs_follow_the_answer() {
    let mut node = Node::start(bracket_entry).await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-1", "3992.78", "4052.78");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    node.order_events(7).await;

    node.modify("PROBE-1-SL", None, Some(Price::from("3982.78")));

    let updated = node.order_events(2).await;
    let amended = node.requests::<ProtoOaAmendPositionSltpReq>();
    assert_eq!(amended[0].position_id, 690_004_734);
    assert_eq!(amended[0].stop_loss, Some(3982.78));
    assert_eq!(amended[0].take_profit, Some(4052.78));
    assert_eq!(node.tags::<ProtoOaAmendPositionSltpReq>(), ["P:PROBE-1-SL"]);
    assert_eq!(
        kinds(&updated),
        [row("PROBE-1-SL", "updated"), row("PROBE-1-TP", "updated")]
    );
    let (sl, tp) = (node.cached("PROBE-1-SL"), node.cached("PROBE-1-TP"));
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
            // The protection with its take profit gone
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

/// The demo's stop loss hit (`execution_stop_loss_*`).
///
/// A take profit at 4053.51 also protected the position.
fn protected(bytes: &[u8]) -> ProtoOaExecutionEvent {
    let mut execution = execution(bytes);
    if let Some(order) = execution.order.as_mut() {
        order.limit_price = Some(4053.51);
    }
    execution
}

/// Entry PROBE-5, with its stop loss at 4023.15 and take profit at 4053.51.
///
/// The entry is filled and accepted, then `last`. Its later deal comes after the order events.
async fn bracket_closed_by(last: ProtoOaExecutionEvent) -> Vec<OrderEventAny> {
    let mut node = Node::start(move |request| {
        vec![
            answer(request, &execution(EXECUTION_STOP_LOSS_ENTRY_FILLED)),
            event(&protected(EXECUTION_STOP_LOSS_ACCEPTED), Some("PROBE-5")),
            event(&last, Some("PROBE-5")),
            answer(request, &later_deal(EXECUTION_STOP_LOSS_ENTRY_FILLED)),
        ]
    })
    .await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-5", "4023.15", "4053.51");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    let events = node.order_events(9).await;
    node.later_deal_next().await;
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

/// The demo's close of position 690004734 by the closing order `CLOSE-1`.
fn closed(bytes: &[u8], left: i64) -> ProtoOaExecutionEvent {
    let mut execution = execution(bytes);
    if let Some(order) = execution.order.as_mut() {
        order.client_order_id = Some("C:CLOSE-1".to_owned());
    }
    if let Some(position) = execution.position.as_mut().filter(|_| left > 0) {
        position.trade_data.volume = left;
        position.set_position_status(
            crate::common::messages::ProtoOaPositionStatus::PositionStatusOpen,
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
async fn a_close_of_a_position_the_broker_no_longer_holds_is_rejected_with_its_reason() {
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
    let OrderEventAny::Denied(denied) = &events[0] else {
        panic!("not a denial: {events:?}");
    };

    // The position, then the close's 2 oz and the position's 1
    let numbers: Vec<&str> = denied
        .reason
        .as_str()
        .split(|c: char| !c.is_ascii_digit())
        .filter(|n| !n.is_empty())
        .collect();
    assert_eq!(numbers, ["690004734", "2", "1"], "{}", denied.reason);
    assert!(node.requests::<ProtoOaClosePositionReq>().is_empty());
    assert!(node.requests::<ProtoOaNewOrderReq>().is_empty());
    node.client.disconnect().await.unwrap();
}

/// The bracket's legs are cancelled with the position.
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

/// Left in the cache by reconciliation after a restart, never sent by this session.
fn reconciled_stop_loss(node: &Node, id: &str, venue: &str) {
    let mut order = order(OrderType::StopMarket, id, OrderSide::Sell, 1)
        .trigger_price(Price::from("3992.78"))
        .reduce_only(true)
        .build();
    let account = AccountId::from("ACMEFX-DEMO");
    node.add(&[&order]);
    for event in [
        TestOrderEventStubs::submitted(&order, account),
        TestOrderEventStubs::accepted(&order, account, VenueOrderId::from(venue)),
    ] {
        order.apply(event.clone()).unwrap();
        node.cache.borrow_mut().update_order(&event).unwrap();
    }
}

fn refusing(request: &ProtoMessage) -> Vec<Action> {
    vec![event(
        &decoded::<ProtoOaOrderErrorEvent>(ORDER_ERROR_POSITION_NOT_FOUND),
        request.client_msg_id.as_deref(),
    )]
}

#[rstest]
#[case::move_(
    |node: &Node| node.modify("O-RECON-SL", None, Some(Price::from("3982.78"))),
    "modify rejected",
    "P:O-RECON-SL"
)]
#[case::cancel(|node: &Node| node.cancel("O-RECON-SL"), "cancel rejected", "U:O-RECON-SL")]
#[tokio::test]
async fn a_refused_change_of_a_protection_from_before_a_restart_is_rejected(
    #[case] change: fn(&Node),
    #[case] kind: &'static str,
    #[case] tag: &str,
) {
    let mut node = Node::start(refusing).await;
    reconciled_stop_loss(&node, "O-RECON-SL", "690004734-SL");

    change(&node);

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("O-RECON-SL", kind)]);
    let reason = match &events[0] {
        OrderEventAny::ModifyRejected(rejected) => rejected.reason,
        OrderEventAny::CancelRejected(rejected) => rejected.reason,
        other => panic!("not a rejection: {other:?}"),
    };
    assert_eq!(
        reason.as_str(),
        "POSITION_NOT_FOUND: Position not found with id 690004734"
    );
    assert_eq!(node.tags::<ProtoOaAmendPositionSltpReq>(), [tag]);
    node.client.disconnect().await.unwrap();
}

/// Position 690004734 has stop loss 3992.78 and take profit 4052.78.
///
/// The broker answers no amendment.
async fn protected_position() -> Node {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => vec![
            answer(request, &execution(EXECUTION_MARKET_FILLED)),
            event(&execution(EXECUTION_PROTECTION_ACCEPTED), Some("PROBE-1")),
        ],
        _ => vec![],
    })
    .await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-1", "3992.78", "4052.78");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    node.order_events(7).await;
    node
}

async fn protection_amendments(node: &Node, n: usize) -> Vec<ProtoOaAmendPositionSltpReq> {
    testing::wait_until(&node.received, |frames| {
        frames
            .iter()
            .filter(|(_, f)| f.payload_type == ProtoOaAmendPositionSltpReq::TYPE)
            .count()
            >= n
    })
    .await;
    node.requests()
}

/// The second amendment carries the first change, still unanswered.
///
/// The take profit cancelled stays gone, and the stop loss moved stays moved.
#[rstest]
#[case::after_an_unanswered_cancel(
    |node: &Node| {
        node.cancel("PROBE-1-TP");
        node.modify("PROBE-1-SL", None, Some(Price::from("3982.78")));
    },
    None
)]
#[case::after_an_unanswered_move(
    |node: &Node| {
        node.modify("PROBE-1-SL", None, Some(Price::from("3982.78")));
        node.modify("PROBE-1-TP", Some(Price::from("4062.78")), None);
    },
    Some(4062.78)
)]
#[tokio::test]
async fn a_move_keeps_the_unanswered_change_before_it(
    #[case] changes: fn(&Node),
    #[case] take_profit: Option<f64>,
) {
    let mut node = protected_position().await;

    changes(&node);

    let amended = protection_amendments(&node, 2).await;
    assert_eq!(amended[1].stop_loss, Some(3982.78));
    assert_eq!(amended[1].take_profit, take_profit);
    node.client.disconnect().await.unwrap();
}

/// The captured limit entry PROBE-2, with its stop loss at 3802.73 and take profit at 3842.73.
#[tokio::test]
async fn a_move_after_an_unanswered_cancel_on_a_pending_entry_keeps_the_cancelled_leg_gone() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => {
            vec![answer(request, &execution(EXECUTION_LIMIT_ACCEPTED))]
        }
        _ => vec![],
    })
    .await;
    let mut limit = order(OrderType::Limit, "PROBE-2", OrderSide::Buy, 1);
    limit.price(Price::from("3822.73"));
    let [entry, stop_loss, take_profit] = bracket_on(limit, "PROBE-2", "3802.73", "3842.73");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    node.order_events(4).await;

    node.cancel("PROBE-2-TP");
    node.modify("PROBE-2-SL", None, Some(Price::from("3792.73")));

    testing::wait_until(&node.received, |frames| {
        frames
            .iter()
            .filter(|(_, f)| f.payload_type == ProtoOaAmendOrderReq::TYPE)
            .count()
            >= 2
    })
    .await;
    let amended = node.requests::<ProtoOaAmendOrderReq>();
    assert_eq!(amended[1].order_id, 1_031_776_354);
    assert_eq!(amended[1].stop_loss, Some(3792.73));
    assert_eq!(amended[1].take_profit, None);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_refused_cancel_of_both_protections_rejects_the_cancel_of_each() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaNewOrderReq::TYPE => vec![
            answer(request, &execution(EXECUTION_MARKET_FILLED)),
            event(&execution(EXECUTION_PROTECTION_ACCEPTED), Some("PROBE-1")),
        ],
        _ => refusing(request),
    })
    .await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-1", "3992.78", "4052.78");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    node.order_events(7).await;

    node.client
        .cancel_all_orders(CancelAllOrders::new(
            TraderId::from("HELIX-001"),
            Some(ClientId::from(CLIENT)),
            StrategyId::from(STRATEGY),
            InstrumentId::from(XAUUSD),
            None,
            UUID4::new(),
            node.client.now(),
            None,
            None,
        ))
        .unwrap();

    let events = node.order_events(2).await;
    let mut rejected = kinds(&events);
    rejected.sort();
    assert_eq!(
        rejected,
        [
            row("PROBE-1-SL", "cancel rejected"),
            row("PROBE-1-TP", "cancel rejected")
        ]
    );

    for event in &events {
        let OrderEventAny::CancelRejected(rejected) = event else {
            panic!("not a cancel rejection: {event:?}");
        };
        assert_eq!(
            rejected.reason.as_str(),
            "POSITION_NOT_FOUND: Position not found with id 690004734"
        );
    }
    assert_eq!(node.requests::<ProtoOaAmendPositionSltpReq>().len(), 1);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn an_unanswered_order_the_broker_holds_nowhere_is_reported_rejected_by_a_query() {
    let mut node = Node::start(|request| match request.payload_type {
        t if t == ProtoOaOrderListReq::TYPE => vec![reply(
            request,
            &ProtoOaOrderListRes {
                ctid_trader_account_id: ACCOUNT_ID,
                has_more: false,
                ..Default::default()
            },
        )],
        _ => vec![],
    })
    .await;
    node.submit(&market("PROBE-1", OrderSide::Buy, 1), None);
    node.order_events(1).await;
    node.quiet().await;

    node.client
        .query_order(QueryOrder::new(
            TraderId::from("HELIX-001"),
            Some(ClientId::from(CLIENT)),
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

    let ExecutionEvent::Report(ExecutionReport::Order(status)) = node.next().await else {
        panic!("not an order report");
    };
    assert_eq!(status.client_order_id, Some(ClientOrderId::from("PROBE-1")));
    assert_eq!(status.instrument_id, InstrumentId::from(XAUUSD));
    assert_eq!(status.order_status, OrderStatus::Rejected);
    assert_eq!(status.filled_qty, Quantity::from(0));
    let listed: Vec<ProtoOaOrderListReq> = node.requests();
    assert_eq!(listed.len(), 1);
    node.client.disconnect().await.unwrap();
}

/// A restart left the bracket in the cache while its limit entry PROBE-2 is pending at the broker.
///
/// The entry (venue order id 1031776354) is accepted, and its legs are submitted with no venue
/// order id.
fn reconciled_pending_bracket(node: &Node) {
    let mut limit = order(OrderType::Limit, "PROBE-2", OrderSide::Buy, 1);
    limit.price(Price::from("3822.73"));
    let bracket = bracket_on(limit, "PROBE-2", "3802.73", "3842.73");
    node.add(&bracket.iter().collect::<Vec<_>>());
    let account = AccountId::from("ACMEFX-DEMO");
    for mut order in bracket {
        let mut events = vec![TestOrderEventStubs::submitted(&order, account)];
        if order.parent_order_id().is_none() {
            events.push(TestOrderEventStubs::accepted(
                &order,
                account,
                VenueOrderId::from("1031776354"),
            ));
        }
        for event in events {
            order.apply(event.clone()).unwrap();
            node.cache.borrow_mut().update_order(&event).unwrap();
        }
    }
}

/// No order has a bracket leg's id, because the broker never holds a leg as an order of its own.
fn listing_nothing(request: &ProtoMessage) -> Vec<Action> {
    match request.payload_type {
        t if t == ProtoOaOrderListReq::TYPE => vec![reply(
            request,
            &ProtoOaOrderListRes {
                ctid_trader_account_id: ACCOUNT_ID,
                has_more: false,
                ..Default::default()
            },
        )],
        _ => vec![],
    }
}

impl Node {
    fn query(&self, id: &str) {
        self.client
            .query_order(QueryOrder::new(
                TraderId::from("HELIX-001"),
                Some(ClientId::from(CLIENT)),
                StrategyId::from(STRATEGY),
                InstrumentId::from(XAUUSD),
                ClientOrderId::from(id),
                None,
                UUID4::new(),
                self.client.now(),
                None,
                None,
            ))
            .unwrap();
    }

    /// A query of `UNSENT-1` reports it rejected.
    ///
    /// `UNSENT-1` is an order in the cache that this session never sent. The report is the next
    /// event.
    async fn assert_nothing_concluded(&mut self) {
        self.add(&[&market("UNSENT-1", OrderSide::Buy, 1)]);
        self.query("UNSENT-1");
        let event = self.next().await;
        let ExecutionEvent::Report(ExecutionReport::Order(status)) = &event else {
            panic!("not an order report: {event:?}");
        };
        assert_eq!(
            status.client_order_id,
            Some(ClientOrderId::from("UNSENT-1")),
            "{event:?}"
        );
    }
}

async fn next_order_report(node: &mut Node) -> (String, OrderStatus) {
    let event = node.next().await;
    let ExecutionEvent::Report(ExecutionReport::Order(status)) = &event else {
        panic!("not an order report: {event:?}");
    };
    let id = status.client_order_id.expect("no client order id");
    (id.to_string(), status.order_status)
}

#[tokio::test]
async fn an_order_this_session_never_sent_is_concluded_absent() {
    let mut node = Node::start(listing_nothing).await;
    node.add(&[&market("PROBE-1", OrderSide::Buy, 1)]);

    node.query("PROBE-1");

    assert_eq!(
        next_order_report(&mut node).await,
        ("PROBE-1".to_owned(), OrderStatus::Rejected)
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn an_absent_entry_is_concluded_once_with_its_legs() {
    let mut node = Node::start(listing_nothing).await;
    let [entry, stop_loss, take_profit] = bracket("PROBE-1", "3992.78", "4052.78");
    node.submit_list(&[&entry, &stop_loss, &take_profit]);
    node.order_events(3).await;
    testing::wait_until(&node.received, |frames| {
        frames
            .iter()
            .any(|(_, f)| f.payload_type == ProtoOaNewOrderReq::TYPE)
    })
    .await;

    node.query("PROBE-1");

    let mut rejected = Vec::new();
    for _ in 0..3 {
        rejected.push(next_order_report(&mut node).await);
    }
    rejected.sort();
    assert_eq!(
        rejected,
        [
            ("PROBE-1".to_owned(), OrderStatus::Rejected),
            ("PROBE-1-SL".to_owned(), OrderStatus::Rejected),
            ("PROBE-1-TP".to_owned(), OrderStatus::Rejected),
        ]
    );

    // Concluded: not again
    node.query("PROBE-1");
    node.assert_nothing_concluded().await;
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_query_of_a_bracket_leg_from_before_a_restart_concludes_nothing() {
    let mut node = Node::start(listing_nothing).await;
    reconciled_pending_bracket(&node);

    node.query("PROBE-2-SL");

    node.assert_nothing_concluded().await;

    // Nor was the broker asked: the only order list it got is of UNSENT-1
    assert_eq!(node.requests::<ProtoOaOrderListReq>().len(), 1);
    node.client.disconnect().await.unwrap();
}

/// A cancel of a leg the restart left with no venue order id is refused, which tracks the leg as an
/// order of its own.
#[tokio::test]
async fn a_query_of_a_bracket_leg_whose_cancel_was_refused_after_a_restart_concludes_nothing() {
    let mut node = Node::start(listing_nothing).await;
    reconciled_pending_bracket(&node);
    node.cancel("PROBE-2-SL");
    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("PROBE-2-SL", "cancel denied")]);

    node.query("PROBE-2-SL");

    node.assert_nothing_concluded().await;

    // Nor was the broker asked: the only order list it got is of UNSENT-1
    assert_eq!(node.requests::<ProtoOaOrderListReq>().len(), 1);
    node.client.disconnect().await.unwrap();
}
