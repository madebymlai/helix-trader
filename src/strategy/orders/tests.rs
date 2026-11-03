//! The module's seam: trades sent from a strategy registered without a
//! node, its orders read from the cache, its fills played into the cache
//! as the execution engine does, its time events from a virtual clock.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    timer::TimeEventCallback,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OmsType, OrderStatus, OrderType},
    events::OrderEventAny,
    identifiers::{AccountId, PositionId, StrategyId, TradeId, TraderId, VenueOrderId},
    instruments::{InstrumentAny, stubs::audusd_sim},
    orders::stubs::TestOrderEventStubs,
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

use super::*;

const STRATEGY: &str = "ORDERS-001";
const AUD: &str = "AUD/USD.SIM";

/// 2026-10-26T10:00:00Z.
const NOW: u64 = 1_792_922_400_000_000_000;
const MINUTE: u64 = 60_000_000_000;

#[derive(Debug)]
struct Probe {
    core: StrategyCore,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {}

/// A strategy `strategy` registered without a node on `cache` at `now`, and
/// its clock.
fn registered_on(
    cache: &Rc<RefCell<Cache>>,
    strategy: &str,
    now: u64,
) -> (Probe, Rc<RefCell<VirtualClock>>) {
    let mut probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(strategy)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
    };
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    clock.borrow_mut().advance_time(UnixNanos::from(now), true);
    clock
        .borrow_mut()
        .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
    let portfolio = Rc::new(RefCell::new(Portfolio::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    probe
        .core
        .register(
            TraderId::from("HELIX-001"),
            clock.clone(),
            cache.clone(),
            portfolio,
        )
        .unwrap();
    (probe, clock)
}

/// A cache holding AUD/USD, on 5 decimals and whole units.
fn cache() -> Rc<RefCell<Cache>> {
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache.borrow_mut().add_instrument(aud()).unwrap();
    cache
}

fn aud() -> InstrumentAny {
    InstrumentAny::CurrencyPair(audusd_sim())
}

/// The strategy on a cache of AUD/USD at `NOW`, and its clock.
fn registered() -> (Probe, Rc<RefCell<VirtualClock>>) {
    registered_on(&cache(), STRATEGY, NOW)
}

fn id(s: &str) -> ClientOrderId {
    ClientOrderId::from(s)
}

/// The ids of the trade of entry `entry`: `<entry>-TP`, `<entry>-SL`.
fn ids(entry: &str) -> OrderIds {
    OrderIds {
        entry: id(entry),
        take_profit: id(&format!("{entry}-TP")),
        stop_loss: id(&format!("{entry}-SL")),
    }
}

/// A buy of 100,000 AUD/USD.
fn buy(exits: Exits) -> Trade {
    Trade {
        instrument_id: InstrumentId::from(AUD),
        side: OrderSide::Buy,
        quantity: Quantity::from(100_000),
        exits,
    }
}

/// A take profit at 0.70700 and a stop loss at 0.69650, held `hours`.
fn bracket(hours: i64) -> Exits {
    Exits {
        take_profit: Some(Price::from("0.70700")),
        stop_loss: Some(Price::from("0.69650")),
        time_limit: Some(SignedDuration::from_hours(hours)),
    }
}

/// No exit but a time limit of `hours`.
fn held(hours: i64) -> Exits {
    Exits {
        take_profit: None,
        stop_loss: None,
        time_limit: Some(SignedDuration::from_hours(hours)),
    }
}

/// Plays the cached order `order` through its acceptance by the venue.
fn accept(probe: &Probe, order: &str) {
    let cache = probe.cache_rc();
    let order = cache.borrow().order_owned(&id(order)).unwrap();
    let account = AccountId::from("SIM-001");
    for event in [
        TestOrderEventStubs::submitted(&order, account),
        TestOrderEventStubs::accepted(
            &order,
            account,
            VenueOrderId::from(order.client_order_id().as_str()),
        ),
    ] {
        cache.borrow_mut().update_order(&event).unwrap();
    }
}

/// Fills the cached order `order`, accepted, whole into `position` at
/// `ts`, as `fill_of`.
fn fill(probe: &Probe, order: &str, position: &str, ts: u64, oms: OmsType) {
    fill_of(probe, order, None, position, ts, oms);
}

/// Fills `quantity` of the cached order `order`, accepted, its whole
/// quantity if none, into `position` at `ts`, as the execution engine
/// does: the order, the order's position, the position, kept as `oms`; a
/// closed position opened again as a new one under its id.
fn fill_of(
    probe: &Probe,
    order: &str,
    quantity: Option<Quantity>,
    position: &str,
    ts: u64,
    oms: OmsType,
) {
    let cache = probe.cache_rc();
    let order = cache.borrow().order_owned(&id(order)).unwrap();
    let filled = TestOrderEventStubs::filled(
        &order,
        &aud(),
        Some(TradeId::from(order.client_order_id().as_str())),
        Some(PositionId::from(position)),
        Some(Price::from("0.70000")),
        quantity,
        None,
        None,
        Some(UnixNanos::from(ts)),
        Some(AccountId::from("SIM-001")),
    );
    let OrderEventAny::Filled(event) = &filled else {
        panic!("not a fill");
    };
    let mut cache = cache.borrow_mut();
    cache.update_order(&filled).unwrap();
    let position = PositionId::from(position);
    cache
        .add_position_id(
            &position,
            &aud().id().venue,
            &event.client_order_id,
            &event.strategy_id,
        )
        .unwrap();
    match cache.position(&position).map(|p| p.is_open()) {
        Some(true) => {
            cache.update_position_from_fill(position, event).unwrap();
        }
        _ => cache
            .add_position(&Position::new(&aud(), event.clone()), oms)
            .unwrap(),
    }
}

/// Sends `trade` named after `entry` from `probe` and accepts its orders.
fn sent(probe: &mut Probe, entry: &str, trade: Trade) {
    trade.send_as(probe, ids(entry)).unwrap();
    accept(probe, entry);
    if trade.exits.take_profit.is_some() {
        accept(probe, &format!("{entry}-TP"));
        accept(probe, &format!("{entry}-SL"));
    }
}

/// Advances `clock` to `to` and hands each event fired to the module, the
/// names of those it takes as its own.
fn advance(probe: &mut Probe, clock: &Rc<RefCell<VirtualClock>>, to: u64) -> Vec<String> {
    let events = clock.borrow_mut().advance_time(UnixNanos::from(to), true);
    events
        .into_iter()
        .filter(|event| on_time_event(probe, event).unwrap())
        .map(|event| event.name.to_string())
        .collect()
}

/// The orders of the cache other than the trades' entries and legs (named
/// `T-*`): the closes, each its side, quantity, whether reduce-only and a
/// time close.
fn closes(probe: &Probe) -> Vec<(OrderSide, Quantity, bool, bool)> {
    probe
        .cache()
        .orders(None, None, None, None, None)
        .into_iter()
        .filter(|order| !order.client_order_id().as_str().starts_with("T-"))
        .map(|order| {
            (
                order.order_side(),
                order.quantity(),
                order.is_reduce_only(),
                is_time_close(&order),
            )
        })
        .collect()
}

fn status(probe: &Probe, order: &str) -> OrderStatus {
    probe.cache().order(&id(order)).unwrap().status()
}

/// When the clock holds the time alert of the trade of `entry`.
fn alert(clock: &Rc<RefCell<VirtualClock>>, entry: &str) -> Option<UnixNanos> {
    clock.borrow().next_time_ns(&time_alert(id(entry)))
}

/// The time close of a long of 100,000 on its position.
fn close() -> (OrderSide, Quantity, bool, bool) {
    (OrderSide::Sell, Quantity::from(100_000), true, true)
}

// ------------------------------------------------------------------ send

#[test]
fn a_trade_with_both_exits_is_a_bracket_its_legs_on_them() {
    let (mut probe, _) = registered();

    buy(bracket(4)).send_as(&mut probe, ids("T-1")).unwrap();

    let cache = probe.cache();
    let order = |of: &str| cache.order(&id(of)).unwrap();
    let entry = order("T-1");
    assert_eq!(
        (entry.order_type(), entry.order_side(), entry.quantity()),
        (OrderType::Market, OrderSide::Buy, Quantity::from(100_000))
    );
    let take_profit = order("T-1-TP");
    assert_eq!(
        (
            take_profit.order_type(),
            take_profit.order_side(),
            take_profit.price(),
            take_profit.is_reduce_only(),
            take_profit.parent_order_id()
        ),
        (
            OrderType::Limit,
            OrderSide::Sell,
            Some(Price::from("0.70700")),
            true,
            Some(id("T-1"))
        )
    );
    let stop_loss = order("T-1-SL");
    assert_eq!(
        (
            stop_loss.order_type(),
            stop_loss.order_side(),
            stop_loss.trigger_price(),
            stop_loss.is_reduce_only(),
            stop_loss.parent_order_id()
        ),
        (
            OrderType::StopMarket,
            OrderSide::Sell,
            Some(Price::from("0.69650")),
            true,
            Some(id("T-1"))
        )
    );
}

#[test]
fn a_trade_without_exits_is_one_market_entry() {
    let (mut probe, _) = registered();

    buy(held(1)).send(&mut probe).unwrap();

    let cache = probe.cache();
    let orders = cache.orders(None, None, None, None, None);
    let [entry] = orders.as_slice() else {
        panic!("{orders:?}")
    };
    assert_eq!(
        (
            entry.order_type(),
            entry.order_side(),
            entry.quantity(),
            entry.is_reduce_only()
        ),
        (
            OrderType::Market,
            OrderSide::Buy,
            Quantity::from(100_000),
            false
        )
    );
}

#[test]
fn a_take_profit_without_a_stop_loss_is_refused_and_nothing_sent() {
    let (mut probe, _) = registered();
    let trade = buy(Exits {
        stop_loss: None,
        ..bracket(1)
    });

    let err = trade.send(&mut probe).unwrap_err();

    assert!(err.to_string().contains("alone"), "{err}");
    assert!(
        probe
            .cache()
            .orders(None, None, None, None, None)
            .is_empty()
    );
}

/// AUD/USD trades whole units: 0.4 rounds to none.
#[test]
fn a_quantity_rounding_to_no_size_is_refused() {
    let (mut probe, _) = registered();
    let trade = Trade {
        quantity: Quantity::from("0.4"),
        ..buy(held(1))
    };

    let err = trade.send(&mut probe).unwrap_err();

    assert!(
        err.to_string().contains("ordering 0.4 of AUD/USD.SIM"),
        "{err}"
    );
}

// ------------------------------------------------------------ time limit

/// Sent at 10:00 and held 4 hours: its alert is the module's at 14:00, not
/// a nanosecond earlier.
#[test]
fn a_trades_time_limit_runs_from_its_send() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(bracket(4)));

    assert!(advance(&mut probe, &clock, NOW + 240 * MINUTE - 1).is_empty());
    assert_eq!(advance(&mut probe, &clock, NOW + 240 * MINUTE).len(), 1);
}

#[test]
fn a_trade_without_a_time_limit_has_no_alert() {
    let (mut probe, clock) = registered();
    let trade = buy(Exits {
        time_limit: None,
        ..bracket(1)
    });
    sent(&mut probe, "T-1", trade);

    assert!(advance(&mut probe, &clock, NOW + 1000 * 60 * MINUTE).is_empty());
}

#[test]
fn another_time_event_is_not_the_modules() {
    let (mut probe, clock) = registered();
    clock
        .borrow_mut()
        .set_time_alert_ns("DECIDE", UnixNanos::from(NOW + MINUTE), None, None)
        .unwrap();

    assert!(advance(&mut probe, &clock, NOW + MINUTE).is_empty());
}

/// Filled into a position of its own on a hedging venue, neither leg
/// filled: at its time the legs are cancelled and its position closed by
/// a market sell of its 100,000, a time close.
#[test]
fn at_its_time_an_open_trade_is_closed_and_its_legs_cancelled() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(bracket(4)));
    fill(&probe, "T-1", "P-1", NOW, OmsType::Hedging);

    advance(&mut probe, &clock, NOW + 240 * MINUTE);

    assert_eq!(status(&probe, "T-1-TP"), OrderStatus::PendingCancel);
    assert_eq!(status(&probe, "T-1-SL"), OrderStatus::PendingCancel);
    assert_eq!(closes(&probe), [close()]);
}

/// A dataset run purges closed orders a minute after they close (Nautilus'
/// `purge_closed_orders`), the filled entry with them: its open legs and
/// its open position still end the trade at its time.
#[test]
fn at_its_time_a_trade_its_filled_entry_purged_is_closed_and_its_legs_cancelled() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(bracket(4)));
    fill(&probe, "T-1", "P-1", NOW, OmsType::Hedging);
    probe
        .cache_rc()
        .borrow_mut()
        .purge_order(id("T-1"), false)
        .unwrap();

    advance(&mut probe, &clock, NOW + 240 * MINUTE);

    assert_eq!(status(&probe, "T-1-TP"), OrderStatus::PendingCancel);
    assert_eq!(status(&probe, "T-1-SL"), OrderStatus::PendingCancel);
    assert_eq!(closes(&probe), [close()]);
}

#[test]
fn at_its_time_an_unfilled_entry_is_cancelled_and_nothing_closed() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(held(1)));

    advance(&mut probe, &clock, NOW + 60 * MINUTE);

    assert_eq!(status(&probe, "T-1"), OrderStatus::PendingCancel);
    assert_eq!(closes(&probe), []);
}

/// On a netting venue two trades, sent at 10:00 and 10:30, add into one
/// position: the first's time closes it whole, 200,000, and the second's
/// then finds it closed.
#[test]
fn on_a_netting_venue_a_trades_time_closes_the_position_with_the_trades_it_holds() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(held(1)));
    fill(&probe, "T-1", "P-NET", NOW, OmsType::Netting);
    advance(&mut probe, &clock, NOW + 30 * MINUTE);
    sent(&mut probe, "T-2", buy(held(1)));
    fill(&probe, "T-2", "P-NET", NOW + 30 * MINUTE, OmsType::Netting);

    advance(&mut probe, &clock, NOW + 60 * MINUTE);
    let close = probe
        .cache()
        .orders(None, None, None, None, None)
        .into_iter()
        .find(is_time_close)
        .unwrap();
    assert_eq!(close.quantity(), Quantity::from(200_000));
    accept(&probe, close.client_order_id().as_str());
    fill(
        &probe,
        close.client_order_id().as_str(),
        "P-NET",
        NOW + 60 * MINUTE,
        OmsType::Netting,
    );
    advance(&mut probe, &clock, NOW + 90 * MINUTE);

    assert_eq!(closes(&probe).len(), 1);
}

/// On a netting venue the first of two trades in the position ended at
/// its stop loss: its time closes nothing, the second's closes the
/// position, its 100,000 left.
#[test]
fn a_trade_its_leg_ended_is_not_closed_at_its_time() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(bracket(1)));
    fill(&probe, "T-1", "P-NET", NOW, OmsType::Netting);
    advance(&mut probe, &clock, NOW + 30 * MINUTE);
    sent(&mut probe, "T-2", buy(bracket(1)));
    fill(&probe, "T-2", "P-NET", NOW + 30 * MINUTE, OmsType::Netting);
    fill(
        &probe,
        "T-1-SL",
        "P-NET",
        NOW + 40 * MINUTE,
        OmsType::Netting,
    );

    advance(&mut probe, &clock, NOW + 60 * MINUTE);
    assert_eq!(closes(&probe), []);
    advance(&mut probe, &clock, NOW + 90 * MINUTE);
    assert_eq!(closes(&probe), [close()]);
}

/// Its take profit filled 40,000 of its 100,000 on a hedging venue: the
/// trade is not ended, a leg closed alone ends it, so at its time the legs
/// are cancelled and its position, the 60,000 left, closed.
#[test]
fn a_trade_its_leg_partly_filled_is_closed_at_its_time() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(bracket(4)));
    fill(&probe, "T-1", "P-1", NOW, OmsType::Hedging);
    fill_of(
        &probe,
        "T-1-TP",
        Some(Quantity::from(40_000)),
        "P-1",
        NOW + 60 * MINUTE,
        OmsType::Hedging,
    );

    advance(&mut probe, &clock, NOW + 240 * MINUTE);

    assert_eq!(status(&probe, "T-1-TP"), OrderStatus::PendingCancel);
    assert_eq!(status(&probe, "T-1-SL"), OrderStatus::PendingCancel);
    assert_eq!(
        closes(&probe),
        [(OrderSide::Sell, Quantity::from(60_000), true, true)]
    );
}

/// On a netting venue the first trade's position closed at 10:20 by
/// another order and opened again by a third trade at 10:30: the first's
/// time leaves the third's position open.
#[test]
fn a_trade_whose_position_closed_and_opened_again_is_not_closed_at_its_time() {
    let (mut probe, clock) = registered();
    sent(&mut probe, "T-1", buy(held(1)));
    fill(&probe, "T-1", "P-NET", NOW, OmsType::Netting);
    let sell = Trade {
        side: OrderSide::Sell,
        exits: Exits {
            time_limit: None,
            ..held(1)
        },
        ..buy(held(1))
    };
    sent(&mut probe, "T-2", sell);
    fill(&probe, "T-2", "P-NET", NOW + 20 * MINUTE, OmsType::Netting);
    advance(&mut probe, &clock, NOW + 30 * MINUTE);
    sent(&mut probe, "T-3", buy(held(1)));
    fill(&probe, "T-3", "P-NET", NOW + 30 * MINUTE, OmsType::Netting);

    assert_eq!(
        advance(&mut probe, &clock, NOW + 60 * MINUTE),
        ["TRADE-TIME:T-1"]
    );
    assert_eq!(closes(&probe), []);
}

// --------------------------------------------------------------- restart

/// Trades sent at 05:00 and 09:40, each into a position of its own, held 4
/// hours, as a restart at 10:00 finds them in the cache.
fn before_a_restart(cache: &Rc<RefCell<Cache>>) {
    let (mut before, clock) = registered_on(cache, STRATEGY, NOW - 300 * MINUTE);
    sent(&mut before, "T-1", buy(bracket(4)));
    fill(&before, "T-1", "P-1", NOW - 300 * MINUTE, OmsType::Hedging);
    clock
        .borrow_mut()
        .advance_time(UnixNanos::from(NOW - 20 * MINUTE), true);
    sent(&mut before, "T-2", buy(held(4)));
    fill(&before, "T-2", "P-2", NOW - 19 * MINUTE, OmsType::Hedging);
}

/// The trade sent 20 minutes before the restart, filled a minute later, is
/// closed 3 hours 40 minutes after it, 4 hours after its send; the one
/// sent 5 hours before at once.
#[test]
fn a_restart_sets_each_open_trades_time_again_from_its_send() {
    let cache = cache();
    before_a_restart(&cache);
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(alert(&clock, "T-1"), Some(UnixNanos::from(NOW)));
    assert_eq!(
        alert(&clock, "T-2"),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}

/// The trade whose 4 hours are over at the restart is closed at once.
#[test]
fn a_trade_open_at_a_restart_is_closed_at_its_time() {
    let cache = cache();
    before_a_restart(&cache);
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);
    arm_open_trades(&mut probe).unwrap();

    advance(&mut probe, &clock, NOW);

    assert_eq!(closes(&probe), [close()]);
}

#[test]
fn a_restart_sets_no_time_for_another_strategys_trade() {
    let cache = cache();
    before_a_restart(&cache);
    let (mut probe, clock) = registered_on(&cache, "OTHER-001", NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(clock.borrow().timer_names(), Vec::<&str>::new());
}

#[test]
fn a_restart_sets_no_time_for_a_trade_without_a_time_limit() {
    let cache = cache();
    let (mut before, _) = registered_on(&cache, STRATEGY, NOW - 20 * MINUTE);
    let trade = buy(Exits {
        time_limit: None,
        ..bracket(1)
    });
    sent(&mut before, "T-1", trade);
    fill(&before, "T-1", "P-1", NOW - 20 * MINUTE, OmsType::Hedging);
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(clock.borrow().timer_names(), Vec::<&str>::new());
}

/// On a netting venue two trades, sent 50 and 20 minutes before the
/// restart, in one position: each its own time again.
#[test]
fn a_restart_sets_the_time_of_each_trade_in_a_netting_position_again() {
    let cache = cache();
    let (mut before, clock) = registered_on(&cache, STRATEGY, NOW - 50 * MINUTE);
    sent(&mut before, "T-1", buy(bracket(4)));
    fill(&before, "T-1", "P-NET", NOW - 50 * MINUTE, OmsType::Netting);
    clock
        .borrow_mut()
        .advance_time(UnixNanos::from(NOW - 20 * MINUTE), true);
    sent(&mut before, "T-2", buy(bracket(4)));
    fill(&before, "T-2", "P-NET", NOW - 20 * MINUTE, OmsType::Netting);
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(
        alert(&clock, "T-1"),
        Some(UnixNanos::from(NOW + 190 * MINUTE))
    );
    assert_eq!(
        alert(&clock, "T-2"),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}

/// The first of two trades in a netting position ended at its stop loss
/// before the restart: the second's time alone is set again.
#[test]
fn a_restart_sets_no_time_for_a_trade_its_leg_ended() {
    let cache = cache();
    let (mut before, clock) = registered_on(&cache, STRATEGY, NOW - 50 * MINUTE);
    sent(&mut before, "T-1", buy(bracket(4)));
    fill(&before, "T-1", "P-NET", NOW - 50 * MINUTE, OmsType::Netting);
    clock
        .borrow_mut()
        .advance_time(UnixNanos::from(NOW - 20 * MINUTE), true);
    sent(&mut before, "T-2", buy(bracket(4)));
    fill(&before, "T-2", "P-NET", NOW - 20 * MINUTE, OmsType::Netting);
    fill(
        &before,
        "T-1-SL",
        "P-NET",
        NOW - 10 * MINUTE,
        OmsType::Netting,
    );
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(alert(&clock, "T-1"), None);
    assert_eq!(
        alert(&clock, "T-2"),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}

/// A trade sent 20 minutes before the restart, its take profit since
/// filled 40,000 of its 100,000: still open, its time is set again.
#[test]
fn a_restart_sets_the_time_of_a_trade_its_leg_partly_filled_again() {
    let cache = cache();
    let (mut before, _) = registered_on(&cache, STRATEGY, NOW - 20 * MINUTE);
    sent(&mut before, "T-1", buy(bracket(4)));
    fill(&before, "T-1", "P-1", NOW - 20 * MINUTE, OmsType::Hedging);
    fill_of(
        &before,
        "T-1-TP",
        Some(Quantity::from(40_000)),
        "P-1",
        NOW - 10 * MINUTE,
        OmsType::Hedging,
    );
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(
        alert(&clock, "T-1"),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}

/// A trade sent 20 minutes before the restart, its entry still unfilled:
/// its time is set again, 4 hours after its send.
#[test]
fn a_restart_sets_the_time_of_an_unfilled_entry_again() {
    let cache = cache();
    let (mut before, _) = registered_on(&cache, STRATEGY, NOW - 20 * MINUTE);
    sent(&mut before, "T-1", buy(bracket(4)));
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(
        alert(&clock, "T-1"),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}

/// A trade sent 20 minutes before the restart, its entry filled 40,000 of
/// its 100,000, open as an order and held by a position: its time is set
/// again.
#[test]
fn a_restart_sets_the_time_of_a_partly_filled_entry_again() {
    let cache = cache();
    let (mut before, _) = registered_on(&cache, STRATEGY, NOW - 20 * MINUTE);
    sent(&mut before, "T-1", buy(bracket(4)));
    fill_of(
        &before,
        "T-1",
        Some(Quantity::from(40_000)),
        "P-1",
        NOW - 20 * MINUTE,
        OmsType::Hedging,
    );
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(
        alert(&clock, "T-1"),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}

/// The first trade's netting position closed 30 minutes before the
/// restart, inside its 4 hours, and opened again by a later trade: the
/// later trade's time alone is set again.
#[test]
fn a_restart_sets_no_time_for_a_trade_whose_position_closed_since() {
    let cache = cache();
    let (mut before, clock) = registered_on(&cache, STRATEGY, NOW - 90 * MINUTE);
    sent(&mut before, "T-1", buy(held(4)));
    fill(&before, "T-1", "P-NET", NOW - 90 * MINUTE, OmsType::Netting);
    let sell = Trade {
        side: OrderSide::Sell,
        exits: Exits {
            time_limit: None,
            ..held(1)
        },
        ..buy(held(1))
    };
    sent(&mut before, "T-2", sell);
    fill(&before, "T-2", "P-NET", NOW - 30 * MINUTE, OmsType::Netting);
    clock
        .borrow_mut()
        .advance_time(UnixNanos::from(NOW - 20 * MINUTE), true);
    sent(&mut before, "T-3", buy(held(4)));
    fill(&before, "T-3", "P-NET", NOW - 20 * MINUTE, OmsType::Netting);
    let (mut probe, clock) = registered_on(&cache, STRATEGY, NOW);

    arm_open_trades(&mut probe).unwrap();

    assert_eq!(alert(&clock, "T-1"), None);
    assert_eq!(
        alert(&clock, "T-3"),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}
