use std::{cell::RefCell, rc::Rc};
use rstest::rstest;

use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    messages::data::{DataCommand, RequestCommand, SubscribeCommand},
    msgbus::{self, MessagingSwitchboard, stubs::get_typed_into_message_saving_handler},
    timer::TimeEventCallback,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    accounts::{AccountAny, MarginAccount},
    enums::{AccountType, OmsType},
    events::{AccountState, OrderEventAny},
    identifiers::{AccountId, PositionId, Symbol, TradeId, TraderId, VenueOrderId},
    instruments::{
        CurrencyPair, InstrumentAny,
        stubs::{audusd_sim, btcusd_bybit},
    },
    orders::{Order, stubs::TestOrderEventStubs},
    position::Position,
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;

use super::*;
use crate::strategy::risk::Stake;

const TOML: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
    ]
    warmup = "720h"
    session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }

    [indicators.fast]
    period = 10

    [indicators.slow]
    period = 30

    [trade]
    instruments = [{ instrument_id = "ESM24.CME" }]
    risk = "0.5%"
"#;

fn error_fields(toml: &str) -> Vec<String> {
    let err = EmaCrossConfig::load(toml).unwrap_err();
    let errors = match err.downcast_ref::<ConfigError>() {
        Some(ConfigError::Multiple { errors }) => errors.clone(),
        Some(err) => vec![err.clone()],
        None => panic!("not a config error: {err}"),
    };
    errors
        .into_iter()
        .map(|e| match e {
            ConfigError::Range { field, .. }
            | ConfigError::MissingField { field }
            | ConfigError::UnsupportedField { field, .. }
            | ConfigError::InvalidReference { field, .. }
            | ConfigError::InvalidValue { field, .. } => field,
            e => panic!("unexpected config error {e:?}"),
        })
        .collect()
}

#[rstest]
fn loads_from_toml() {
    let config = EmaCrossConfig::load(TOML).unwrap();
    assert_eq!(config.stake(), Stake::new("0.5%".parse().unwrap()));
    assert_eq!(
        config.trade_instrument_id(),
        InstrumentId::from("ESM24.CME")
    );
    assert_eq!(
        config.trade_bar_type(),
        BarType::from("ESM24.CME-1-HOUR-MID-INTERNAL")
    );
}

#[rstest]
fn the_example_config_loads() {
    EmaCrossConfig::load(include_str!("../../../config/ema.toml")).unwrap();
}

#[rstest]
fn reads_its_emas_from_the_indicators() {
    let toml = TOML
        .replace("period = 10", "period = 5")
        .replace("period = 30", "period = 15");
    let config = EmaCrossConfig::load(&toml).unwrap();
    assert_eq!(
        (config.fast_ema().period, config.slow_ema().period),
        (5, 15)
    );
}

#[rstest]
fn defaults_its_base_and_leaves_the_oms_to_the_venue() {
    let config = EmaCrossConfig::load(TOML).unwrap();
    assert_eq!(
        config.base.strategy_id,
        Some(StrategyId::from("EMA_CROSS-001"))
    );
    // Nautilus tags orders with the id's tag
    assert_eq!(config.base.order_id_tag, None);
    assert_eq!(config.base.oms_type, None);
}

#[rstest]
#[case::a_period("fast_period = 5", "fast_period")]
#[case::exits("hold = \"1h\"", "hold")]
fn rejects_a_top_level_key_naming_it(#[case] line: &str, #[case] key: &str) {
    let toml = format!("{line}\n{TOML}");
    let err = EmaCrossConfig::load(&toml).unwrap_err();
    assert!(err.to_string().contains(key), "{err}");
}

#[rstest]
fn rejects_fast_period_not_below_slow() {
    let toml = TOML.replace("period = 10", "period = 30");
    assert_eq!(error_fields(&toml), ["indicators.fast.period"]);
}

#[rstest]
#[case::two(
    "instruments = [{ instrument_id = \"ESM24.CME\" }, { instrument_id = \"ESH24.CME\" }]"
)]
#[case::none("instruments = []")]
fn rejects_a_count_of_traded_instruments_other_than_one_alone(#[case] instruments: &str) {
    let toml = TOML.replace(
        "instruments = [{ instrument_id = \"ESM24.CME\" }]",
        instruments,
    );
    assert_eq!(error_fields(&toml), ["trade.instruments"]);
}

#[rstest]
fn rejects_a_trade_instrument_without_bars() {
    let toml = TOML.replace(
        "{ instrument_id = \"ESM24.CME\" }",
        "{ instrument_id = \"ESU24.CME\" }",
    );
    assert_eq!(error_fields(&toml), ["trade.instruments[0].instrument_id"]);
}

fn listing(series: &str) -> String {
    TOML.replace(
        "price_type = \"MID\" } },\n",
        &format!("price_type = \"MID\" }} }},\n        {series},\n"),
    )
}

/// Only the first bars series of the traded instrument is read, and any other series is refused
/// under its listing.
#[rstest]
#[case::a_second_bars_series_of_the_trade_instrument(
    r#"{ data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 2, aggregation = "HOUR", price_type = "MID" } }"#,
    &["ESM24.CME-2-HOUR-MID-INTERNAL"]
)]
#[case::bars_of_another_instrument(
    r#"{ data = "bars", instrument_id = "ESH24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } }"#,
    &["ESH24.CME-1-HOUR-MID-INTERNAL"]
)]
#[case::ticks_of_the_trade_instrument(
    r#"{ data = "quotes", instrument_id = "ESM24.CME" }"#,
    &["QuoteTick of ESM24.CME"]
)]
#[case::a_custom_series(
    r#"{ data = "Custom:LseRow", identifier = "US10Y", metadata = { source = "bond_yields", symbol = "US10Y" } }"#,
    &["LseRow", "US10Y"]
)]
fn rejects_a_series_it_does_not_read_as_unread(#[case] series: &str, #[case] named: &[&str]) {
    crate::venues::register_custom_data();
    let toml = listing(series);
    assert_eq!(error_fields(&toml), ["data.series[1]"]);
    let err = EmaCrossConfig::load(&toml).unwrap_err().to_string();
    for part in named {
        assert!(err.contains(part), "{part}: {err}");
    }
}

#[rstest]
fn rejects_its_bars_series_listed_twice() {
    let toml = listing(
        r#"{ data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } }"#,
    );
    let err = EmaCrossConfig::load(&toml).unwrap_err();
    let Some(ConfigError::Duplicate {
        field,
        value: Some(value),
    }) = err.downcast_ref::<ConfigError>()
    else {
        panic!("not one duplicate naming the series: {err}");
    };
    assert_eq!(field, "data.series[1]");
    assert!(value.contains("ESM24.CME-1-HOUR-MID-INTERNAL"), "{value}");
}

#[rstest]
fn reports_a_trade_instrument_without_bars_with_the_standard_violations() {
    let toml = TOML
        .replace(
            "{ instrument_id = \"ESM24.CME\" }",
            "{ instrument_id = \"ESU24.CME\" }",
        )
        .replace("\"720h\"", "\"0s\"");
    let mut fields = error_fields(&toml);
    fields.sort();
    assert_eq!(
        fields,
        ["data.warmup", "trade.instruments[0].instrument_id"]
    );
}

#[rstest]
fn reports_every_unread_series_with_the_other_violations() {
    let toml = listing(
        r#"{ data = "bars", instrument_id = "ESH24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
        { data = "trades", instrument_id = "ESM24.CME" }"#,
    )
    .replace("period = 10", "period = 30")
    .replace("\"720h\"", "\"0s\"");
    let mut fields = error_fields(&toml);
    fields.sort();
    assert_eq!(
        fields,
        [
            "data.series[1]",
            "data.series[2]",
            "data.warmup",
            "indicators.fast.period"
        ]
    );
}

fn price(s: &str) -> Decimal {
    s.parse().unwrap()
}

#[rstest]
#[case::long("620.60", OrderSide::Buy, "626.806", "617.497")]
#[case::short("600.60", OrderSide::Sell, "594.594", "603.603")]
fn a_trade_takes_profit_a_percent_and_stops_half_a_percent_from_the_close(
    #[case] close: &str,
    #[case] side: OrderSide,
    #[case] take_profit: &str,
    #[case] stop_loss: &str,
) {
    assert_eq!(
        exit_prices(price(close), side),
        (price(take_profit), price(stop_loss))
    );
}

const CONFIG: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "AUD/USD.SIM", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
    ]
    warmup = "1h"

    [indicators]
    fast = { period = 10 }
    slow = { period = 30 }

    [trade]
    instruments = [{ instrument_id = "AUD/USD.SIM" }]
    risk = "0.5%"
"#;

/// The restart, 2026-10-26T10:00:00Z.
const NOW: u64 = 1_792_922_400_000_000_000;
const MINUTE: u64 = 60_000_000_000;

fn started(cache: &Rc<RefCell<Cache>>, now: u64) -> (EmaCross, Rc<RefCell<VirtualClock>>) {
    let (mut strategy, clock) = registered(cache, now, CONFIG);
    DataActor::on_start(&mut strategy).unwrap();
    (strategy, clock)
}

fn registered(
    cache: &Rc<RefCell<Cache>>,
    now: u64,
    config: &str,
) -> (EmaCross, Rc<RefCell<VirtualClock>>) {
    let strategy = EmaCross::new(EmaCrossConfig::load(config).unwrap()).unwrap();
    register(strategy, cache, now)
}

fn register(
    mut strategy: EmaCross,
    cache: &Rc<RefCell<Cache>>,
    now: u64,
) -> (EmaCross, Rc<RefCell<VirtualClock>>) {
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
    strategy
        .core
        .register(
            TraderId::from("HELIX-001"),
            clock.clone(),
            cache.clone(),
            portfolio,
        )
        .unwrap();
    (strategy, clock)
}

fn holding(instrument: InstrumentAny) -> Rc<RefCell<Cache>> {
    let cache = Rc::new(RefCell::new(Cache::default()));
    let balance = Money::from("100000 USD");
    let state = AccountState::new(
        AccountId::from("SIM-001"),
        AccountType::Margin,
        vec![AccountBalance::new(balance, Money::from("0 USD"), balance)],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    cache.borrow_mut().add_instrument(instrument).unwrap();
    cache
        .borrow_mut()
        .add_account(AccountAny::Margin(MarginAccount::new(state, true)))
        .unwrap();
    cache
}

/// A long is decided at 0.70000 20 minutes before a restart, and its entry fills into a position.
/// The restart sets its time again, 4 hours after its send.
#[rstest]
fn a_restart_sets_the_time_of_its_open_trade_again() {
    let aud = InstrumentAny::CurrencyPair(audusd_sim());
    let cache = holding(aud.clone());
    let sent = NOW - 20 * MINUTE;
    let (mut before, _) = started(&cache, sent);
    let bar = Bar::new(
        BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL"),
        Price::from("0.70000"),
        Price::from("0.70000"),
        Price::from("0.70000"),
        Price::from("0.70000"),
        Quantity::from(1),
        UnixNanos::from(sent),
        UnixNanos::from(sent),
    );
    let (stake, rates) = (before.config.stake(), before.config.rates());
    before
        .trade(&bar, OrderSide::Buy)
        .unwrap()
        .send(&mut before, stake, rates)
        .unwrap();
    let entry = cache
        .borrow()
        .orders(None, None, None, None, None)
        .into_iter()
        .find(|order| order.parent_order_id().is_none())
        .unwrap()
        .clone();
    let account = AccountId::from("SIM-001");
    let filled = TestOrderEventStubs::filled(
        &entry,
        &aud,
        Some(TradeId::from("T-1")),
        Some(PositionId::from("P-1")),
        Some(Price::from("0.70010")),
        None,
        None,
        None,
        Some(UnixNanos::from(sent)),
        Some(account),
    );

    for event in [
        TestOrderEventStubs::submitted(&entry, account),
        TestOrderEventStubs::accepted(&entry, account, VenueOrderId::from("V-1")),
        filled.clone(),
    ] {
        cache.borrow_mut().update_order(&event).unwrap();
    }
    let OrderEventAny::Filled(fill) = filled else {
        panic!("not a fill");
    };
    cache
        .borrow_mut()
        .add_position(&Position::new(&aud, fill), OmsType::Hedging)
        .unwrap();

    let (_strategy, clock) = started(&cache, NOW);

    let clock = clock.borrow();
    let names = clock.timer_names();
    let [alert] = names.as_slice() else {
        panic!("{names:?}")
    };
    assert_eq!(
        clock.next_time_ns(alert),
        Some(UnixNanos::from(NOW + 220 * MINUTE))
    );
}

fn aud_in_quarter_ticks() -> InstrumentAny {
    InstrumentAny::CurrencyPair(
        CurrencyPair::builder()
            .instrument_id(InstrumentId::from("AUD/USD.SIM"))
            .raw_symbol(Symbol::from("AUD/USD"))
            .base_currency(Currency::AUD())
            .quote_currency(Currency::USD())
            .price_precision(5)
            .size_precision(0)
            .price_increment(Price::from("0.00025"))
            .size_increment(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

#[rstest]
fn a_trade_takes_profit_and_stops_on_the_nearest_ticks_its_one_r_to_its_stop() {
    let cache = holding(aud_in_quarter_ticks());
    let (strategy, _) = started(&cache, NOW);
    let close = Price::from("0.70010");
    let bar = Bar::new(
        BarType::from("AUD/USD.SIM-1-HOUR-MID-INTERNAL"),
        close,
        close,
        close,
        close,
        Quantity::from(1),
        UnixNanos::from(NOW),
        UnixNanos::from(NOW),
    );

    let trade = strategy.trade(&bar, OrderSide::Buy).unwrap();

    assert_eq!(
        trade,
        Trade::new(
            InstrumentId::from("AUD/USD.SIM"),
            OrderSide::Buy,
            close,
            price("0.00360"),
            Exits {
                bracket: Some(Bracket {
                    take_profit: Price::from("0.70700"),
                    stop_loss: Price::from("0.69650"),
                }),
                time_limit: Some(SignedDuration::from_hours(4)),
            },
        )
    );
}

/// Nautilus' sizer reads P&L as a multiple of a price distance. An inverse instrument's P&L is
/// not.
#[rstest]
fn trading_an_inverse_instrument_it_refuses_to_start() {
    let (handler, sent) = get_typed_into_message_saving_handler::<DataCommand>(None);
    msgbus::register_data_command_endpoint(
        MessagingSwitchboard::data_engine_queue_execute(),
        handler,
    );
    let btc = btcusd_bybit();
    let inverse = btc.id;
    let config = CONFIG.replace("AUD/USD.SIM", &inverse.to_string());
    let cache = holding(InstrumentAny::CryptoPerpetual(btc));
    let (mut strategy, _) = registered(&cache, NOW, &config);

    let err = DataActor::on_start(&mut strategy).unwrap_err();

    assert!(err.to_string().contains(&inverse.to_string()), "{err}");
    let sent = sent.get_messages();
    assert!(sent.is_empty(), "{sent:?}");
}

fn ic_aud() -> InstrumentAny {
    let mut pair = audusd_sim();
    pair.id = InstrumentId::from("AUDUSD.ICMARKETS");
    InstrumentAny::CurrencyPair(pair)
}

fn converting() -> EmaCrossConfig {
    let toml = CONFIG.replace("AUD/USD.SIM", "AUDUSD.ICMARKETS").replace(
        "risk = \"0.5%\"",
        "risk = \"0.5%\"\n    conversions = [\"EURUSD.ICMARKETS\"]",
    );
    EmaCrossConfig::load(&toml).unwrap()
}

/// Live, the strategy requests the quotes of IC's EURUSD over its warmup, which ends now. That
/// way the cache holds a rate when it sizes a trade.
#[rstest]
fn live_it_warms_up_its_conversion_pairs_quotes_then_subscribes_to_them_from_its_start() {
    let (handler, sent) = get_typed_into_message_saving_handler::<DataCommand>(None);
    msgbus::register_data_command_endpoint(
        MessagingSwitchboard::data_engine_queue_execute(),
        handler,
    );
    let eurusd = InstrumentId::from("EURUSD.ICMARKETS");
    let (mut strategy, _) = register(
        EmaCross::live(converting()).unwrap(),
        &holding(ic_aud()),
        NOW,
    );

    DataActor::on_start(&mut strategy).unwrap();

    let sent = sent.get_messages();
    let subscribed_from: Vec<Option<u64>> = sent
        .iter()
        .filter_map(|command| match command {
            DataCommand::Subscribe(SubscribeCommand::Quotes(quotes))
                if quotes.instrument_ids == [eurusd] =>
            {
                Some(quotes.params.as_ref().and_then(|p| p.get_u64("start_ns")))
            }
            _ => None,
        })
        .collect();
    assert_eq!(subscribed_from, [Some(NOW)]);
    let warmed_to: Vec<Option<UnixNanos>> = sent
        .iter()
        .filter_map(|command| match command {
            DataCommand::Request(RequestCommand::Quotes(quotes))
                if quotes.instrument_ids == [eurusd] =>
            {
                Some(quotes.end.map(UnixNanos::from))
            }
            _ => None,
        })
        .collect();
    assert_eq!(warmed_to, [Some(UnixNanos::from(NOW))]);
}
