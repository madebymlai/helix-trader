use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    timer::TimeEventCallback,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OmsType,
    events::OrderEventAny,
    identifiers::{AccountId, PositionId, Symbol, TradeId, TraderId, VenueOrderId},
    instruments::{CurrencyPair, InstrumentAny, stubs::audusd_sim},
    orders::{Order, stubs::TestOrderEventStubs},
    position::Position,
    types::{Currency, Price},
};
use nautilus_portfolio::portfolio::Portfolio;

use super::*;

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
    instruments = [{ instrument_id = "ESM24.CME", trade_size = "1" }]
"#;

/// The fields of the config errors refusing `toml`.
fn error_fields(toml: &str) -> Vec<String> {
    let err = EmaCrossConfig::load(toml).unwrap_err();
    let errors = match err.downcast_ref::<ConfigError>() {
        Some(ConfigError::Multiple { errors }) => errors.clone(),
        Some(err) => vec![err.clone()],
        None => panic!("not a config error: {err}"),
    };
    errors
        .into_iter()
        .map(|err| match err {
            ConfigError::Range { field, .. }
            | ConfigError::MissingField { field }
            | ConfigError::UnsupportedField { field, .. }
            | ConfigError::InvalidReference { field, .. }
            | ConfigError::InvalidValue { field, .. } => field,
            err => panic!("unexpected config error {err:?}"),
        })
        .collect()
}

#[test]
fn loads_from_toml() {
    let config = EmaCrossConfig::load(TOML).unwrap();
    assert_eq!(config.trade_size(), Quantity::from(1));
    assert_eq!(
        config.trade_instrument_id(),
        InstrumentId::from("ESM24.CME")
    );
    assert_eq!(
        config.trade_bar_type(),
        BarType::from("ESM24.CME-1-HOUR-MID-INTERNAL")
    );
}

#[test]
fn the_example_config_loads() {
    EmaCrossConfig::load(include_str!("../../../config/ema.toml")).unwrap();
}

#[test]
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

#[test]
fn defaults_its_base_and_leaves_the_oms_to_the_venue() {
    let config = EmaCrossConfig::load(TOML).unwrap();
    assert_eq!(
        config.base.strategy_id,
        Some(StrategyId::from("EMA_CROSS-001"))
    );
    // Its id alone: Nautilus tags its orders with the id's tag.
    assert_eq!(config.base.order_id_tag, None);
    assert_eq!(config.base.oms_type, None);
}

#[test]
fn rejects_a_period_at_the_top_level() {
    let toml = format!("fast_period = 5\n{TOML}");
    let err = EmaCrossConfig::load(&toml).unwrap_err();
    assert!(err.to_string().contains("fast_period"), "{err}");
}

#[test]
fn rejects_exits_in_the_toml() {
    let toml = format!("hold = \"1h\"\n{TOML}");
    let err = EmaCrossConfig::load(&toml).unwrap_err();
    assert!(err.to_string().contains("hold"), "{err}");
}

#[test]
fn rejects_a_missing_ema() {
    let start = TOML.find("[indicators.slow]").unwrap();
    let end = TOML.find("[trade]").unwrap();
    let toml = format!("{}{}", &TOML[..start], &TOML[end..]);
    assert_eq!(error_fields(&toml), ["indicators.slow"]);
}

#[test]
fn rejects_an_ema_of_average_true_range_arguments() {
    let toml = TOML.replacen(
        "period = 10",
        "period = 10\n        ma_type = \"SIMPLE\"",
        1,
    );
    assert_eq!(error_fields(&toml), ["indicators.fast"]);
}

#[test]
fn rejects_a_zero_period_by_nautilus_check() {
    let toml = TOML.replace("period = 10", "period = 0");
    assert_eq!(error_fields(&toml), ["indicators.fast"]);
}

#[test]
fn rejects_fast_period_not_below_slow() {
    let toml = TOML.replace("period = 10", "period = 30");
    assert_eq!(error_fields(&toml), ["indicators.fast.period"]);
}

#[test]
fn rejects_two_traded_instruments() {
    let toml = TOML.replace(
        "instruments = [{ instrument_id = \"ESM24.CME\", trade_size = \"1\" }]",
        "instruments = [{ instrument_id = \"ESM24.CME\", trade_size = \"1\" }, \
         { instrument_id = \"ESH24.CME\", trade_size = \"1\" }]",
    );
    assert_eq!(error_fields(&toml), ["trade.instruments"]);
}

/// With no instrument traded, nothing is asked of the series: the count
/// is the violation.
#[test]
fn rejects_no_traded_instrument_alone() {
    let toml = TOML.replace(
        "instruments = [{ instrument_id = \"ESM24.CME\", trade_size = \"1\" }]",
        "instruments = []",
    );
    assert_eq!(error_fields(&toml), ["trade.instruments"]);
}

#[test]
fn rejects_a_trade_instrument_without_bars() {
    let toml = TOML.replace(
        "{ instrument_id = \"ESM24.CME\", trade_size",
        "{ instrument_id = \"ESU24.CME\", trade_size",
    );
    assert_eq!(error_fields(&toml), ["trade.instruments[0].instrument_id"]);
}

/// `TOML` with `series` listed after its bars series of the traded
/// instrument.
fn listing(series: &str) -> String {
    TOML.replace(
        "price_type = \"MID\" } },\n",
        &format!("price_type = \"MID\" }} }},\n        {series},\n"),
    )
}

/// The crossover reads the first bars series of the traded instrument: a
/// second one is read by nothing.
#[test]
fn rejects_a_second_bars_series_of_the_trade_instrument_as_unread() {
    let toml = listing(
        r#"{ data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 2, aggregation = "HOUR", price_type = "MID" } }"#,
    );
    assert_eq!(error_fields(&toml), ["data.series[1]"]);
    let err = EmaCrossConfig::load(&toml).unwrap_err().to_string();
    assert!(err.contains("ESM24.CME-2-HOUR-MID-INTERNAL"), "{err}");
}

#[test]
fn rejects_bars_of_another_instrument_as_unread() {
    let toml = listing(
        r#"{ data = "bars", instrument_id = "ESH24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } }"#,
    );
    assert_eq!(error_fields(&toml), ["data.series[1]"]);
    let err = EmaCrossConfig::load(&toml).unwrap_err().to_string();
    assert!(err.contains("ESH24.CME-1-HOUR-MID-INTERNAL"), "{err}");
}

/// The crossover reads its bars series once: listed again, it is refused
/// under its second listing, naming it.
#[test]
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

/// The crossover reads bars alone, even of the instrument it trades.
#[test]
fn rejects_ticks_of_the_trade_instrument_as_unread() {
    let toml = listing(r#"{ data = "quotes", instrument_id = "ESM24.CME" }"#);
    assert_eq!(error_fields(&toml), ["data.series[1]"]);
    let err = EmaCrossConfig::load(&toml).unwrap_err().to_string();
    assert!(err.contains("QuoteTick of ESM24.CME"), "{err}");
}

/// The traded instrument without bars, its own error, is reported with
/// the standard ones.
#[test]
fn reports_a_trade_instrument_without_bars_with_the_standard_violations() {
    let toml = TOML
        .replace(
            "{ instrument_id = \"ESM24.CME\", trade_size",
            "{ instrument_id = \"ESU24.CME\", trade_size",
        )
        .replace("\"720h\"", "\"0s\"");
    let mut fields = error_fields(&toml);
    fields.sort();
    assert_eq!(
        fields,
        ["data.warmup", "trade.instruments[0].instrument_id"]
    );
}

/// Each series read by nothing is reported under its field, with the
/// standard violations and the crossover's own.
#[test]
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

/// The crossover reading the bars of the GC root, the one series of it,
/// and trading the root: no order goes to a root, which the shared trade
/// rules refuse under its field.
#[test]
fn rejects_a_futures_root_traded() {
    let toml = TOML
        .replace("ESM24.CME", "GC.GLBX")
        .replace("price_type = \"MID\"", "price_type = \"LAST\"");
    assert_eq!(error_fields(&toml), ["trade.instruments[0].instrument_id"]);
}

/// The crossover reading the MID bars of the GC root, which it must
/// trade to read them: Nautilus builds a root's bars from its contracts'
/// trades, so these would never come, refused under the series with the
/// root traded.
#[test]
fn rejects_bars_on_a_root_not_built_from_its_trades() {
    let toml = TOML.replace("ESM24.CME", "GC.GLBX");
    let mut fields = error_fields(&toml);
    fields.sort();
    assert_eq!(
        fields,
        ["data.series[0]", "trade.instruments[0].instrument_id"]
    );
    let err = EmaCrossConfig::load(&toml).unwrap_err().to_string();
    assert!(err.contains("GC.GLBX-1-HOUR-MID-INTERNAL"), "{err}");
}

/// Its bars of the instrument traded give execution its prices: no
/// quotes of it are added.
#[test]
fn requires_no_quotes_of_the_instrument_its_bars_read() {
    let config = EmaCrossConfig::load(TOML).unwrap();
    assert_eq!(config.data_requirements().ticks, []);
}

#[test]
fn reports_its_own_violations_with_the_standard_ones() {
    let toml = TOML
        .replace("period = 10", "period = 30")
        .replace("\"720h\"", "\"0s\"");
    assert_eq!(
        error_fields(&toml),
        ["data.warmup", "indicators.fast.period"]
    );
}

fn price(s: &str) -> Decimal {
    s.parse().unwrap()
}

#[test]
fn a_long_takes_profit_a_percent_above_the_close_and_stops_half_a_percent_below() {
    // 620.60 plus 1%, less 0.5%.
    assert_eq!(
        exit_prices(price("620.60"), OrderSide::Buy),
        (price("626.806"), price("617.497"))
    );
}

#[test]
fn a_short_takes_profit_a_percent_below_the_close_and_stops_half_a_percent_above() {
    // 600.60 less 1%, plus 0.5%.
    assert_eq!(
        exit_prices(price("600.60"), OrderSide::Sell),
        (price("594.594"), price("603.603"))
    );
}

/// Trades AUD/USD.
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
    instruments = [{ instrument_id = "AUD/USD.SIM", trade_size = "100000" }]
"#;

/// 2026-10-26T10:00:00Z, the time of the restart.
const NOW: u64 = 1_792_922_400_000_000_000;
const MINUTE: u64 = 60_000_000_000;

/// The strategy registered without a node on `cache` at `now`, started,
/// and its clock.
fn started(cache: &Rc<RefCell<Cache>>, now: u64) -> (EmaCross, Rc<RefCell<VirtualClock>>) {
    let mut strategy = EmaCross::new(EmaCrossConfig::load(CONFIG).unwrap()).unwrap();
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
    DataActor::on_start(&mut strategy).unwrap();
    (strategy, clock)
}

/// A long decided on a bar closing at 0.70000 20 minutes before a restart,
/// its entry filled into a position of its own: started again at the
/// restart, the strategy sets the trade's time again, 4 hours after its
/// send.
#[test]
fn a_restart_sets_the_time_of_its_open_trade_again() {
    let aud = InstrumentAny::CurrencyPair(audusd_sim());
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache.borrow_mut().add_instrument(aud.clone()).unwrap();
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
    before
        .trade(&bar, OrderSide::Buy)
        .unwrap()
        .send(&mut before)
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

/// AUD/USD of the config priced in quarters of a tenth of a pip, 0.00025 a
/// tick, finer prices than its ticks.
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

/// A long decided on a bar closing at 0.70010: its take profit, 0.707101,
/// and its stop, 0.6965995, are on the nearest of the instrument's ticks,
/// not merely at its precision (0.70710, 0.69660).
#[test]
fn a_trade_takes_profit_and_stops_on_the_nearest_ticks() {
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_instrument(aud_in_quarter_ticks())
        .unwrap();
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

    let bracket = trade.exits.bracket.unwrap();
    assert_eq!(
        (bracket.take_profit, bracket.stop_loss),
        (Price::from("0.70700"), Price::from("0.69650"))
    );
}
