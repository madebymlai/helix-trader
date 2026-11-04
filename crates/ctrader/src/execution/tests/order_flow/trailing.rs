//! Trailing stops against the mock peer, on the trade events of a trailing
//! stop loss captured on the demo (`testdata/execution_trailing_*`,
//! `trailing_sl_changed`): a bracket's, relative on its entry (PROBE-7,
//! position 692481830), and one set on an open position (PROBE-8-SL,
//! position 692481891), which then moved and was hit.

use nautilus_model::{data::QuoteTick, enums::TrailingOffsetType};

use super::*;
use crate::{
    common::messages::{ProtoOaPosition, ProtoOaPositionStatus, ProtoOaTrailingSlChangedEvent},
    testing::{
        EXECUTION_LIMIT_ACCEPTED, EXECUTION_LIMIT_REPLACED, EXECUTION_TRAILING_ENTRY_FILLED,
        EXECUTION_TRAILING_PROTECTION_ACCEPTED, EXECUTION_TRAILING_SET_ACCEPTED,
        EXECUTION_TRAILING_SET_FILLED, EXECUTION_TRAILING_SET_REPLACED,
        EXECUTION_TRAILING_STOP_LOSS_FILLED, TRAILING_SL_CHANGED,
    },
};

/// The demo's position the trailing stop was set on.
const POSITION: &str = "692481891";

fn moved() -> ProtoOaTrailingSlChangedEvent {
    decoded(TRAILING_SL_CHANGED)
}

/// A trailing stop market `id` closing a long of 1 oz, trailing `offset`
/// by `offset_type`.
fn trailing(id: &str, offset: Decimal, offset_type: TrailingOffsetType) -> OrderTestBuilder {
    let mut builder = order(OrderType::TrailingStopMarket, id, OrderSide::Sell, 1);
    builder
        .trailing_offset(offset)
        .trailing_offset_type(offset_type)
        .reduce_only(true);
    builder
}

/// A market buy of 1 oz, `entry`, protected by `<entry>-SL`, a trailing
/// stop market 1.00 behind, from 4042.00, the level the strategy expects:
/// Nautilus' bracket gives a trailing stop loss a trigger price.
fn trailing_bracket(entry: &str) -> [OrderAny; 2] {
    let sl = format!("{entry}-SL");
    let list = OrderListId::from("OL-1");
    let entry_order = order(OrderType::Market, entry, OrderSide::Buy, 1)
        .contingency_type(ContingencyType::Oto)
        .linked_order_ids(vec![ClientOrderId::from(sl.as_str())])
        .order_list_id(list)
        .build();
    let stop = trailing(&sl, Decimal::ONE, TrailingOffsetType::Price)
        .parent_order_id(ClientOrderId::from(entry))
        .order_list_id(list)
        .trigger_price(Price::from("4042.00"))
        .build();
    [entry_order, stop]
}

/// The demo's bracket entry PROBE-7 filled, its trailing stop loss accepted
/// on the position, then hit.
fn trailing_entry(request: &ProtoMessage) -> Vec<Action> {
    vec![
        answer(request, &execution(EXECUTION_TRAILING_ENTRY_FILLED)),
        event(
            &execution(EXECUTION_TRAILING_PROTECTION_ACCEPTED),
            Some("PROBE-7"),
        ),
        event(
            &execution(EXECUTION_TRAILING_STOP_LOSS_FILLED),
            Some("PROBE-7"),
        ),
    ]
}

#[tokio::test]
async fn a_brackets_trailing_stop_loss_goes_relative_on_its_entry_and_the_position_fills_it() {
    let mut node = Node::start(trailing_entry).await;
    let [entry, stop_loss] = trailing_bracket("PROBE-7");

    node.submit_list(&[&entry, &stop_loss]);

    let events = node.order_events(7).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-7", "submitted"),
            row("PROBE-7-SL", "submitted"),
            row("PROBE-7", "accepted"),
            row("PROBE-7", "filled"),
            row("PROBE-7-SL", "accepted"),
            row("PROBE-7-SL", "updated"),
            row("PROBE-7-SL", "filled"),
        ]
    );
    let sent = node.requests::<ProtoOaNewOrderReq>();
    assert_eq!(sent.len(), 1, "the stop loss rides on the entry");
    assert_eq!(sent[0].relative_stop_loss, Some(100_000));
    assert_eq!(sent[0].trailing_stop_loss, Some(true));
    assert_eq!(sent[0].stop_loss, None);
    let OrderEventAny::Updated(updated) = &events[5] else {
        panic!("not an update: {:?}", events[5]);
    };
    // cTrader's level, the fill less 1.00, replaces the strategy's.
    assert_eq!(updated.trigger_price, Some(Price::from("4042.26")));
    let stop = fill_of(&events, "PROBE-7-SL");
    assert_eq!(stop.venue_order_id, VenueOrderId::from("692481830-SL"));
    assert_eq!(stop.order_type, OrderType::TrailingStopMarket);
    assert_eq!(stop.last_px, Price::from("4042.19"));
    assert_eq!(stop.position_id, Some(PositionId::from("692481830")));
    node.client.disconnect().await.unwrap();
}

/// Puts the demo's quote of when the stop was set into the cache: bid
/// 4042.26, ask 4042.31.
fn quoted(node: &Node) {
    let quote = QuoteTick::new(
        InstrumentId::from(XAUUSD),
        Price::from("4042.26"),
        Price::from("4042.31"),
        Quantity::from(100),
        Quantity::from(100),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    node.cache.borrow_mut().add_quote(quote).unwrap();
}

/// The trailing stop PROBE-8-SL, 1.00 behind the bid, on the demo's
/// position.
fn set_on_position(node: &Node) {
    node.open_position(POSITION, 1);
    quoted(node);
    let stop = trailing("PROBE-8-SL", Decimal::ONE, TrailingOffsetType::Price).build();
    node.submit(&stop, Some(PositionId::from(POSITION)));
}

#[tokio::test]
async fn a_trailing_stop_on_a_position_goes_at_its_offset_from_the_bid_and_follows_its_moves() {
    let mut node = Node::start(|request| {
        vec![
            answer(request, &execution(EXECUTION_TRAILING_SET_ACCEPTED)),
            event(&moved(), None),
            event(&execution(EXECUTION_TRAILING_SET_FILLED), None),
        ]
    })
    .await;

    set_on_position(&node);

    let events = node.order_events(5).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-8-SL", "submitted"),
            row("PROBE-8-SL", "accepted"),
            row("PROBE-8-SL", "updated"),
            row("PROBE-8-SL", "updated"),
            row("PROBE-8-SL", "filled"),
        ]
    );
    let amended = node.requests::<ProtoOaAmendPositionSltpReq>();
    assert_eq!(amended[0].position_id, 692_481_891);
    assert_eq!(amended[0].stop_loss, Some(4041.26));
    assert_eq!(amended[0].take_profit, None);
    assert_eq!(amended[0].trailing_stop_loss, Some(true));
    assert_eq!(
        amended[0].stop_loss_trigger_method(),
        ProtoOaOrderTriggerMethod::Trade
    );
    assert_eq!(node.tags::<ProtoOaAmendPositionSltpReq>(), ["N:PROBE-8-SL"]);
    let triggers: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            OrderEventAny::Updated(u) => u.trigger_price,
            _ => None,
        })
        .collect();
    assert_eq!(
        triggers,
        [Price::from("4041.26"), Price::from("4041.27")],
        "cTrader's level, then its move"
    );
    let stop = fill_of(&events, "PROBE-8-SL");
    assert_eq!(stop.venue_order_id, VenueOrderId::from("692481891-SL"));
    assert_eq!(stop.last_px, Price::from("4041.29"));
    node.client.disconnect().await.unwrap();
}

/// The peer accepts the trailing stop, moves it, and answers its
/// amendment with `amended`.
async fn trailing_set(amended: ProtoOaExecutionEvent) -> Node {
    let mut node = Node::start(move |request| {
        if request.client_msg_id.as_deref() == Some("N:PROBE-8-SL") {
            vec![
                answer(request, &execution(EXECUTION_TRAILING_SET_ACCEPTED)),
                event(&moved(), None),
            ]
        } else {
            vec![answer(request, &amended)]
        }
    })
    .await;
    set_on_position(&node);
    node.order_events(4).await;
    node
}

#[tokio::test]
async fn moving_a_trailing_stop_loss_keeps_it_trailing_at_its_new_level() {
    let mut node = trailing_set(execution(EXECUTION_TRAILING_SET_REPLACED)).await;

    node.modify("PROBE-8-SL", None, Some(Price::from("4041.32")));

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("PROBE-8-SL", "updated")]);
    let amended = node.requests::<ProtoOaAmendPositionSltpReq>();
    assert_eq!(amended[1].stop_loss, Some(4041.32));
    assert_eq!(amended[1].trailing_stop_loss, Some(true));
    assert_eq!(
        node.tags::<ProtoOaAmendPositionSltpReq>()[1],
        "P:PROBE-8-SL"
    );
    assert_eq!(
        node.cached("PROBE-8-SL").trigger_price(),
        Some(Price::from("4041.32"))
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn cancelling_a_trailing_stop_loss_removes_it_from_the_position() {
    // The protection with its stop loss gone.
    let mut removed = execution(EXECUTION_TRAILING_SET_REPLACED);
    if let Some(order) = removed.order.as_mut() {
        (
            order.stop_price,
            order.limit_price,
            order.trailing_stop_loss,
        ) = (None, None, None);
    }
    let mut node = trailing_set(removed).await;

    node.cancel("PROBE-8-SL");

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("PROBE-8-SL", "canceled")]);
    let amended = node.requests::<ProtoOaAmendPositionSltpReq>();
    assert_eq!(amended[1].stop_loss, None);
    assert_eq!(amended[1].trailing_stop_loss, None);
    assert_eq!(
        node.tags::<ProtoOaAmendPositionSltpReq>()[1],
        "U:PROBE-8-SL"
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_trailing_stop_on_a_position_holding_a_stop_loss_is_denied_and_nothing_is_sent() {
    let mut node = trailing_set(execution(EXECUTION_TRAILING_SET_REPLACED)).await;

    let second = trailing("PROBE-9-SL", Decimal::ONE, TrailingOffsetType::Price).build();
    node.submit(&second, Some(PositionId::from(POSITION)));

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("PROBE-9-SL", "denied")]);
    let OrderEventAny::Denied(denied) = &events[0] else {
        unreachable!();
    };
    assert!(
        denied.reason.contains("has a stop loss order (PROBE-8-SL)"),
        "{}",
        denied.reason
    );
    assert_eq!(node.requests::<ProtoOaAmendPositionSltpReq>().len(), 1);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_trailing_stop_naming_no_position_and_one_without_a_quote_are_denied() {
    let mut node = Node::start(|_| vec![]).await;
    node.open_position(POSITION, 1);

    let alone = trailing("PROBE-9", Decimal::ONE, TrailingOffsetType::Price).build();
    node.submit(&alone, None);
    let unquoted = trailing("PROBE-10", Decimal::ONE, TrailingOffsetType::Price).build();
    node.submit(&unquoted, Some(PositionId::from(POSITION)));

    let events = node.order_events(2).await;
    assert_eq!(
        kinds(&events),
        [row("PROBE-9", "denied"), row("PROBE-10", "denied")]
    );
    node.quiet().await;
    assert!(node.requests::<ProtoOaNewOrderReq>().is_empty());
    assert!(node.requests::<ProtoOaAmendPositionSltpReq>().is_empty());
    node.client.disconnect().await.unwrap();
}

/// The demo's position, a long of 1 oz, its trailing stop loss at
/// 4041.26, as the reconcile request states it.
fn trailing_position() -> ProtoOaPosition {
    let mut position = execution(EXECUTION_TRAILING_SET_ACCEPTED).position.unwrap();
    position.set_position_status(ProtoOaPositionStatus::PositionStatusOpen);
    position
}

/// A move of a trailing stop loss this session did not set (set before a
/// restart) is the report of the position's stop loss at its new level,
/// which Nautilus reconciles.
#[tokio::test]
async fn a_move_of_a_trailing_stop_loss_from_before_a_restart_is_reported_at_its_new_level() {
    let venue = Script {
        reconcile: Some(ProtoOaReconcileRes {
            ctid_trader_account_id: ACCOUNT_ID,
            position: vec![trailing_position()],
            ..Default::default()
        }),
        // The move comes with the answer to the next trade request.
        trades: Some(Arc::new(|_: &ProtoMessage| vec![event(&moved(), None)])),
        ..Script::default()
    };
    let (mut client, cache, mut events, _received, _tokens) = started(venue);
    connect(&mut client, &mut events, &cache).await;
    let order = order(OrderType::Limit, "PROBE-11", OrderSide::Buy, 1)
        .price(Price::from("4000.00"))
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client
        .submit_order(SubmitOrder::from_order(
            &order,
            TraderId::from("HELIX-001"),
            Some(ClientId::from(CLIENT)),
            None,
            UUID4::new(),
            client.now(),
        ))
        .unwrap();

    let report = loop {
        let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("no execution event within 10 s")
            .expect("the execution events ended");
        if let ExecutionEvent::Report(ExecutionReport::Order(report)) = event {
            break report;
        }
    };
    assert_eq!(report.venue_order_id, VenueOrderId::from("692481891-SL"));
    assert_eq!(report.order_type, OrderType::TrailingStopMarket);
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.order_side, Some(OrderSide::Sell));
    assert_eq!(report.quantity, Quantity::from(1));
    assert_eq!(report.trigger_price, Some(Price::from("4041.27")));
    assert_eq!(report.trigger_type, Some(TriggerType::BidAsk));
    assert_eq!(report.trailing_offset, Some(Decimal::ZERO));
    assert_eq!(report.trailing_offset_type, Some(TrailingOffsetType::Price));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_position_whose_stop_loss_was_cancelled_takes_no_other() {
    // The protection with its stop loss gone.
    let mut removed = execution(EXECUTION_TRAILING_SET_REPLACED);
    if let Some(order) = removed.order.as_mut() {
        (
            order.stop_price,
            order.limit_price,
            order.trailing_stop_loss,
        ) = (None, None, None);
    }
    let mut node = trailing_set(removed).await;
    node.cancel("PROBE-8-SL");
    node.order_events(1).await;

    // Nautilus' cache keeps 692481891-SL the cancelled stop's.
    let second = trailing("PROBE-9-SL", Decimal::ONE, TrailingOffsetType::Price).build();
    node.submit(&second, Some(PositionId::from(POSITION)));

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("PROBE-9-SL", "denied")]);
    assert_eq!(node.requests::<ProtoOaAmendPositionSltpReq>().len(), 2);
    node.client.disconnect().await.unwrap();
}

/// A stop set on the position, the broker answering nothing.
async fn unanswered(venue: Script) -> Node {
    let mut node = Node::start_on(venue, |_| vec![]).await;
    set_on_position(&node);
    node.order_events(1).await;
    protection_amendments(&node, 1).await;
    node
}

#[tokio::test]
async fn a_stop_being_set_cannot_be_cancelled_and_keeps_a_second_one_out() {
    let mut node = unanswered(Script::default()).await;

    node.cancel("PROBE-8-SL");
    let second = trailing("PROBE-9-SL", Decimal::ONE, TrailingOffsetType::Price).build();
    node.submit(&second, Some(PositionId::from(POSITION)));

    let events = node.order_events(2).await;
    assert_eq!(
        kinds(&events),
        [
            row("PROBE-8-SL", "cancel denied"),
            row("PROBE-9-SL", "denied")
        ]
    );
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn an_unanswered_stop_is_rejected_by_a_query_finding_the_position_without_one() {
    let mut node = unanswered(Script::default()).await;

    node.query("PROBE-8-SL");

    let events = node.order_events(1).await;
    assert_eq!(kinds(&events), [row("PROBE-8-SL", "rejected")]);
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn an_unanswered_stop_is_accepted_by_a_query_finding_the_positions_trailing_stop() {
    let venue = Script {
        reconcile: Some(ProtoOaReconcileRes {
            ctid_trader_account_id: ACCOUNT_ID,
            position: vec![trailing_position()],
            ..Default::default()
        }),
        ..Script::default()
    };
    let mut node = unanswered(venue).await;

    node.query("PROBE-8-SL");

    let events = node.order_events(2).await;
    assert_eq!(
        kinds(&events),
        [row("PROBE-8-SL", "accepted"), row("PROBE-8-SL", "updated")]
    );
    assert_eq!(
        node.cached("PROBE-8-SL").trigger_price(),
        Some(Price::from("4041.26"))
    );
    node.client.disconnect().await.unwrap();
}

/// A take profit this session does not track (set elsewhere) on a position
/// whose trailing stop it tracks is reported, and its hit too.
#[tokio::test]
async fn a_take_profit_not_tracked_beside_a_tracked_stop_is_reported_and_its_hit_too() {
    let mut hit = execution(EXECUTION_TRAILING_SET_FILLED);
    if let Some(deal) = hit.deal.as_mut() {
        deal.execution_price = Some(4093.34);
    }
    let mut node = Node::start(move |request| {
        vec![
            answer(request, &execution(EXECUTION_TRAILING_SET_ACCEPTED)),
            // The take profit 4093.34 set elsewhere, the stop at 4041.32.
            event(&execution(EXECUTION_TRAILING_SET_REPLACED), None),
            event(&hit, None),
        ]
    })
    .await;

    set_on_position(&node);

    let (mut reports, mut events) = (Vec::new(), Vec::new());
    while !kinds(&events).contains(&row("PROBE-8-SL", "canceled")) {
        match node.next().await {
            ExecutionEvent::Order(event) => events.push(event),
            ExecutionEvent::Report(report) => reports.push(report),
            other => panic!("unexpected {other:?}"),
        }
    }
    let take_profit = VenueOrderId::from("692481891-TP");
    let accepted = reports.iter().any(|r| {
        matches!(r, ExecutionReport::Order(s)
            if s.venue_order_id == take_profit && s.order_status == OrderStatus::Accepted)
    });
    assert!(accepted, "{reports:?}");
    let filled = reports.iter().find_map(|r| match r {
        ExecutionReport::OrderWithFills(s, fills) if s.venue_order_id == take_profit => {
            Some(fills[0].last_px)
        }
        _ => None,
    });
    assert_eq!(filled, Some(Price::from("4093.34")), "{reports:?}");
    node.client.disconnect().await.unwrap();
}

/// cTrader answers the amendment of a pending entry carrying relative
/// protections with them relative only (demo, 2026-11-04): its legs stay,
/// at the levels they imply.
#[tokio::test]
async fn amending_a_pending_entry_with_a_trailing_stop_loss_keeps_its_legs() {
    let relative = |bytes: &[u8]| {
        let mut execution = execution(bytes);
        if let Some(order) = execution.order.as_mut() {
            order.stop_loss = None;
            order.relative_stop_loss = Some(100_000);
            order.trailing_stop_loss = Some(true);
        }
        execution
    };
    let mut node = Node::start(move |request| {
        let replay = match request.payload_type {
            t if t == ProtoOaNewOrderReq::TYPE => EXECUTION_LIMIT_ACCEPTED,
            _ => EXECUTION_LIMIT_REPLACED,
        };
        vec![answer(request, &relative(replay))]
    })
    .await;
    let list = OrderListId::from("OL-1");
    let entry = order(OrderType::Limit, "PROBE-2", OrderSide::Buy, 1)
        .price(Price::from("3822.73"))
        .contingency_type(ContingencyType::Oto)
        .linked_order_ids(vec![ClientOrderId::from("PROBE-2-SL")])
        .order_list_id(list)
        .build();
    let stop = trailing("PROBE-2-SL", Decimal::ONE, TrailingOffsetType::Price)
        .parent_order_id(ClientOrderId::from("PROBE-2"))
        .order_list_id(list)
        .trigger_price(Price::from("3821.73"))
        .build();
    node.submit_list(&[&entry, &stop]);
    node.order_events(3).await;

    node.modify("PROBE-2", Some(Price::from("3821.73")), None);

    let events = node.order_events(2).await;
    assert_eq!(
        kinds(&events),
        [row("PROBE-2", "updated"), row("PROBE-2-SL", "updated")]
    );
    node.quiet().await;
    // The level the relative stop loss implies at the entry's new price.
    let stop = node.cached("PROBE-2-SL");
    assert!(!stop.is_closed());
    assert_eq!(stop.trigger_price(), Some(Price::from("3820.73")));
    let amended = node.requests::<ProtoOaAmendOrderReq>();
    assert_eq!(amended[0].relative_stop_loss, Some(100_000));
    assert_eq!(amended[0].trailing_stop_loss, Some(true));
    node.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn moving_the_take_profit_of_a_pending_entry_with_a_trailing_stop_loss_updates_it() {
    let mut node = Node::start(|request| {
        let (bytes, take_profit) = match request.payload_type {
            t if t == ProtoOaNewOrderReq::TYPE => (EXECUTION_LIMIT_ACCEPTED, 3_000_000),
            // 3852.00, 29.27 above the entry.
            _ => (EXECUTION_LIMIT_REPLACED, 2_927_000),
        };
        let mut execution = execution(bytes);
        if let Some(order) = execution.order.as_mut() {
            order.limit_price = Some(3822.73);
            (order.stop_loss, order.take_profit) = (None, None);
            order.relative_stop_loss = Some(100_000);
            order.relative_take_profit = Some(take_profit);
            order.trailing_stop_loss = Some(true);
        }
        vec![answer(request, &execution)]
    })
    .await;
    let list = OrderListId::from("OL-1");
    let (sl, tp) = (
        ClientOrderId::from("PROBE-2-SL"),
        ClientOrderId::from("PROBE-2-TP"),
    );
    let entry = order(OrderType::Limit, "PROBE-2", OrderSide::Buy, 1)
        .price(Price::from("3822.73"))
        .contingency_type(ContingencyType::Oto)
        .linked_order_ids(vec![sl, tp])
        .order_list_id(list)
        .build();
    let stop = trailing("PROBE-2-SL", Decimal::ONE, TrailingOffsetType::Price)
        .parent_order_id(ClientOrderId::from("PROBE-2"))
        .order_list_id(list)
        .trigger_price(Price::from("3821.73"))
        .build();
    let limit = order(OrderType::Limit, "PROBE-2-TP", OrderSide::Sell, 1)
        .parent_order_id(ClientOrderId::from("PROBE-2"))
        .order_list_id(list)
        .reduce_only(true)
        .price(Price::from("3852.73"))
        .build();
    node.submit_list(&[&entry, &stop, &limit]);
    node.order_events(4).await;

    node.modify("PROBE-2-TP", Some(Price::from("3852.00")), None);

    // The entry's replacement updates it, as any; then the leg.
    let events = node.order_events(2).await;
    assert_eq!(
        kinds(&events),
        [row("PROBE-2", "updated"), row("PROBE-2-TP", "updated")]
    );
    assert_eq!(
        node.cached("PROBE-2-TP").price(),
        Some(Price::from("3852.00"))
    );
    let amended = node.requests::<ProtoOaAmendOrderReq>();
    assert_eq!(amended[0].relative_take_profit, Some(2_927_000));
    assert_eq!(amended[0].take_profit, None);
    node.client.disconnect().await.unwrap();
}
