use nautilus_model::{
    enums::{OmsType, OrderType},
    events::{OrderEventAny, OrderFilled},
    identifiers::{AccountId, ClientOrderId, PositionId, VenueOrderId},
    orders::{Order, OrderAny, OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
};

use super::*;

fn market(strategy: &str, instrument: &str, closing: bool) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .instrument_id(InstrumentId::from(instrument))
        .strategy_id(StrategyId::from(strategy))
        .client_order_id(ClientOrderId::from(if closing { "CLOSE" } else { "ENTRY" }))
        .side(if closing {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        })
        .reduce_only(closing)
        .quantity(Quantity::from(10))
        .build()
}

fn fill(order: &OrderAny, instrument: &InstrumentAny, position: PositionId) -> OrderFilled {
    let OrderEventAny::Filled(fill) = TestOrderEventStubs::filled(
        order,
        instrument,
        Some(TradeId::from(order.client_order_id().as_str())),
        Some(position),
        Some(Price::from("0.70000")),
        None,
        None,
        None,
        Some(UnixNanos::from(1)),
        Some(AccountId::from("SIM-001")),
    ) else {
        unreachable!()
    };
    fill
}

#[rstest]
#[case::own(STRATEGY, AUD, true)]
#[case::another_strategy("OTHER-002", AUD, false)]
#[case::another_instrument(STRATEGY, EURUSD, false)]
fn only_an_open_position_of_the_strategy_on_its_traded_instrument_blocks_entry(
    #[case] owner: &str,
    #[case] symbol: &str,
    #[case] blocks: bool,
) {
    let models = tempfile::tempdir().unwrap();
    let (mut probe, _) = registered(at("2026-10-22T12:00:00Z"));
    let handle = sides_handle(&probe, models.path(), &config(), (Some(6), None));
    started(&handle, &mut probe);
    holding_eurusd(&probe, None);
    let instrument = probe
        .cache()
        .instrument(&InstrumentId::from(symbol))
        .unwrap();
    let position_id = PositionId::from("P-1");
    let position = Position::new(
        &instrument,
        fill(&market(owner, symbol, false), &instrument, position_id),
    );
    let cache = probe.cache_rc();
    cache
        .borrow_mut()
        .add_position(&position, OmsType::Hedging)
        .unwrap();
    assert_eq!(handle.decide(&mut probe).unwrap().is_none(), blocks);
    let close = fill(&market(owner, symbol, true), &instrument, position_id);
    cache
        .borrow_mut()
        .update_position_from_fill(position_id, &close)
        .unwrap();
    assert!(handle.decide(&mut probe).unwrap().is_some());
}

#[rstest]
#[case::entry(STRATEGY, AUD, false, true)]
#[case::close(STRATEGY, AUD, true, true)]
#[case::another_strategy("OTHER-002", AUD, false, false)]
#[case::another_instrument(STRATEGY, EURUSD, false, false)]
fn only_a_working_order_of_the_strategy_on_its_traded_instrument_blocks_entry(
    #[case] owner: &str,
    #[case] instrument: &str,
    #[case] closing: bool,
    #[case] blocks: bool,
) {
    let models = tempfile::tempdir().unwrap();
    let (mut probe, _) = registered(at("2026-10-22T12:00:00Z"));
    let handle = sides_handle(&probe, models.path(), &config(), (Some(6), None));
    started(&handle, &mut probe);
    let order = market(owner, instrument, closing);
    let cache = probe.cache_rc();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    assert_eq!(
        handle.decide(&mut probe).unwrap().is_none(),
        blocks,
        "initialized"
    );
    let account = AccountId::from("SIM-001");
    let venue_id = VenueOrderId::from("V-1");

    for event in [
        TestOrderEventStubs::submitted(&order, account),
        TestOrderEventStubs::accepted(&order, account, venue_id),
    ] {
        cache.borrow_mut().update_order(&event).unwrap();
        assert_eq!(
            handle.decide(&mut probe).unwrap().is_none(),
            blocks,
            "{event:?}"
        );
    }
    let canceled = TestOrderEventStubs::canceled(&order, account, Some(venue_id));
    cache.borrow_mut().update_order(&canceled).unwrap();
    assert!(handle.decide(&mut probe).unwrap().is_some());
}
