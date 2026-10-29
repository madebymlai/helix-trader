use std::{cell::Cell, sync::Arc};

use nautilus_common::{actor::DataActor, cache::Cache, clock::VirtualClock};
use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    data::{BookOrder, HasTsInit, OrderBookDelta, stubs::stub_depth10},
    enums::{AggressorSide, BookAction, OrderSide, PriceType},
    identifiers::{TradeId, TraderId},
    types::{Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

use super::*;

const AUD: &str = "AUD/USD.SIM";
const GBP: &str = "GBP/USD.SIM";
const AUD_BARS: &str = "AUD/USD.SIM-1-MINUTE-MID-INTERNAL";
const AUD_BID_BARS: &str = "AUD/USD.SIM-1-MINUTE-BID-INTERNAL";

fn id(s: &str) -> InstrumentId {
    InstrumentId::from(s)
}

/// A moving average of the last `period` mids, or prices.
fn sma(period: usize) -> SimpleMovingAverage {
    SimpleMovingAverage::new(period, Some(PriceType::Mid))
}

/// A quote of `instrument` with the mid `mid`, 0.0002 wide.
fn quote(instrument: &str, mid: f64) -> QuoteTick {
    QuoteTick::new(
        id(instrument),
        Price::new(mid - 0.0001, 5),
        Price::new(mid + 0.0001, 5),
        Quantity::from(1),
        Quantity::from(1),
        UnixNanos::default(),
        UnixNanos::default(),
    )
}

fn trade(instrument: &str, price: f64) -> TradeTick {
    TradeTick::new(
        id(instrument),
        Price::new(price, 5),
        Quantity::from(1),
        AggressorSide::Buy,
        TradeId::from("T-1"),
        UnixNanos::default(),
        UnixNanos::default(),
    )
}

fn bar(bar_type: &str, close: f64) -> Bar {
    let price = Price::new(close, 5);
    Bar::new(
        BarType::from(bar_type),
        price,
        price,
        price,
        price,
        Quantity::from(1),
        UnixNanos::default(),
        UnixNanos::default(),
    )
}

/// A feature counting the depth and deltas it gets, initialized by the
/// first, its value the count.
#[derive(Default)]
struct BookCount {
    count: Rc<Cell<usize>>,
}

impl Indicator for BookCount {
    fn name(&self) -> String {
        "BookCount".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.count.get() > 0
    }

    fn initialized(&self) -> bool {
        self.count.get() > 0
    }

    fn handle_depth(&mut self, _depth: &OrderBookDepth) {
        self.count.set(self.count.get() + 1);
    }

    fn handle_deltas(&mut self, _deltas: &OrderBookDeltas) {
        self.count.set(self.count.get() + 1);
    }

    fn reset(&mut self) {
        self.count.set(0);
    }
}

impl Feature for BookCount {
    fn value(&self) -> f64 {
        self.count.get() as f64
    }
}

fn deltas(instrument: &str) -> OrderBookDeltas {
    let order = BookOrder::new(OrderSide::Buy, Price::from("1.00000"), Quantity::from(1), 1);
    let delta = OrderBookDelta::new(
        id(instrument),
        BookAction::Add,
        order,
        0,
        1,
        UnixNanos::default(),
        UnixNanos::default(),
    );
    OrderBookDeltas::new(id(instrument), vec![delta])
}

/// The custom data a raw feature reads: a level.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
struct Level {
    level: f64,
    ts: UnixNanos,
}

impl HasTsInit for Level {
    fn ts_init(&self) -> UnixNanos {
        self.ts
    }
}

impl nautilus_model::data::CustomDataTrait for Level {
    fn type_name(&self) -> &'static str {
        "Level"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn nautilus_model::data::CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn nautilus_model::data::CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
}

fn level_type(source: &str) -> DataType {
    DataType::new("Level", None, Some(source.to_string()))
}

fn level(source: &str, level: f64) -> CustomData {
    CustomData::new(
        Arc::new(Level {
            level,
            ts: UnixNanos::default(),
        }),
        level_type(source),
    )
}

fn read_level(data: &CustomData) -> f64 {
    data.data
        .as_any()
        .downcast_ref::<Level>()
        .map_or(f64::NAN, |level| level.level)
}

#[test]
fn the_names_keep_the_order_of_their_declaration() {
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), "bars", sma(2))
        .quotes(id(AUD), "quotes", sma(1))
        .trades(id(AUD), "trades", sma(1))
        .build()
        .unwrap();

    assert_eq!(features.names(), ["bars", "quotes", "trades"]);
}

#[test]
fn two_features_of_one_name_are_an_error() {
    let err = FeatureSet::builder()
        .quotes(id(AUD), "mid", sma(1))
        .quotes(id(GBP), "mid", sma(1))
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("mid"), "{err}");
}

#[test]
fn a_set_without_features_is_an_error() {
    assert!(FeatureSet::builder().build().is_err());
}

#[test]
fn a_quote_reaches_only_the_features_on_the_quotes_of_its_instrument() {
    let features = FeatureSet::builder()
        .quotes(id(AUD), "aud", sma(1))
        .quotes(id(GBP), "gbp", sma(1))
        .trades(id(AUD), "aud_trades", sma(1))
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_quote(&quote(GBP, 1.3)).unwrap();
    features.handle_trade(&trade(AUD, 0.65)).unwrap();

    let values = features.values().unwrap();
    assert!((values[0] - 0.7).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 1.3).abs() < 1e-9, "{values:?}");
    assert!((values[2] - 0.65).abs() < 1e-9, "{values:?}");
}

#[test]
fn a_bar_reaches_only_the_features_on_its_bar_type() {
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), "mid", sma(1))
        .bars(BarType::from(AUD_BID_BARS), "bid", sma(1))
        .build()
        .unwrap();

    features.handle_bar(&bar(AUD_BARS, 0.7)).unwrap();

    assert_eq!(features.values(), None);
    features.handle_bar(&bar(AUD_BID_BARS, 0.6)).unwrap();
    let values = features.values().unwrap();
    assert!((values[0] - 0.7).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 0.6).abs() < 1e-9, "{values:?}");
}

#[test]
fn depth_and_deltas_reach_only_the_features_on_their_kind_and_instrument() {
    let (depth_count, deltas_count) = (BookCount::default(), BookCount::default());
    let (on_depth, on_deltas) = (
        Rc::clone(&depth_count.count),
        Rc::clone(&deltas_count.count),
    );
    let depth = stub_depth10();
    let features = FeatureSet::builder()
        .depth(depth.instrument_id, "depth", depth_count)
        .deltas(depth.instrument_id, "deltas", deltas_count)
        .build()
        .unwrap();

    features.handle_depth(&depth);
    features.handle_deltas(&deltas(GBP));

    assert_eq!((on_depth.get(), on_deltas.get()), (1, 0));
    features.handle_deltas(&deltas(&depth.instrument_id.to_string()));
    assert_eq!((on_depth.get(), on_deltas.get()), (1, 1));
}

/// A moving average has no depth, deltas or trade handler of its own:
/// Nautilus' defaults panic.
#[test]
fn an_update_of_a_kind_no_feature_reads_is_a_no_op() {
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), "mid", sma(1))
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_trade(&trade(AUD, 0.7)).unwrap();
    features.handle_depth(&stub_depth10());
    features.handle_deltas(&deltas(AUD));
    features.handle_data(&level("SIM", 1.0));

    assert_eq!(features.values(), None);
}

/// A feature failing on every quote, after counting it.
#[derive(Default)]
struct FailingQuotes {
    count: usize,
}

impl Indicator for FailingQuotes {
    fn name(&self) -> String {
        "FailingQuotes".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.count > 0
    }

    fn initialized(&self) -> bool {
        self.count > 0
    }

    fn handle_quote(&mut self, _quote: &QuoteTick) -> anyhow::Result<()> {
        self.count += 1;
        anyhow::bail!("no price")
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

impl Feature for FailingQuotes {
    fn value(&self) -> f64 {
        self.count as f64
    }
}

#[test]
fn a_features_error_is_no_error_of_the_set_and_the_others_still_update() {
    let features = FeatureSet::builder()
        .quotes(id(AUD), "failing", FailingQuotes::default())
        .quotes(id(AUD), "mid", sma(1))
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    let values = features.values().unwrap();
    assert!((values[1] - 0.7).abs() < 1e-9, "{values:?}");
}

/// Nautilus' moving averages read the last price by default, which a
/// quote does not have: an error of the indicator.
#[test]
fn a_moving_average_of_last_prices_stays_cold_on_quotes() {
    let features = FeatureSet::builder()
        .quotes(id(AUD), "last", SimpleMovingAverage::new(1, None))
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    assert!(!features.initialized());
}

#[test]
fn the_values_wait_for_every_feature() {
    let features = FeatureSet::builder()
        .quotes(id(AUD), "mid_2", sma(2))
        .quotes(id(AUD), "mid_1", sma(1))
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    assert_eq!(features.values(), None);
    assert!(!ActorIndicator::initialized(&*features).unwrap());

    features.handle_quote(&quote(AUD, 0.8)).unwrap();
    let values = features.values().unwrap();
    assert!((values[0] - 0.75).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 0.8).abs() < 1e-9, "{values:?}");
    assert!(ActorIndicator::initialized(&*features).unwrap());
}

#[test]
fn a_raw_feature_holds_the_last_value_read_from_its_data_type() {
    let features = FeatureSet::builder()
        .data(level_type("SIM"), "level", read_level)
        .build()
        .unwrap();

    features.handle_data(&level("OTHER", 9.0));
    assert_eq!(features.values(), None);

    features.handle_data(&level("SIM", 1.5));
    features.handle_data(&level("SIM", 2.5));
    assert_eq!(features.values(), Some(vec![2.5]));
}

/// `update` made at `ns`.
macro_rules! at {
    ($update:expr, $ns:expr) => {{
        let mut update = $update;
        update.ts_init = UnixNanos::from($ns);
        update
    }};
}

#[test]
fn an_instruments_last_update_is_the_latest_a_feature_read_of_any_kind() {
    let depth = stub_depth10();
    let features = FeatureSet::builder()
        .quotes(id(AUD), "quotes", sma(1))
        .trades(id(AUD), "trades", sma(1))
        .bars(BarType::from(AUD_BARS), "bars", sma(1))
        .depth(depth.instrument_id, "depth", BookCount::default())
        .build()
        .unwrap();

    assert_eq!(features.last_update(id(AUD)), None);
    features.handle_quote(&at!(quote(AUD, 0.7), 10)).unwrap();
    features.handle_bar(&at!(bar(AUD_BARS, 0.7), 30)).unwrap();
    // Older than the bar: the latest stays.
    features.handle_trade(&at!(trade(AUD, 0.7), 20)).unwrap();
    features.handle_depth(&at!(depth.clone(), 40));

    assert_eq!(features.last_update(id(AUD)), Some(UnixNanos::from(30)));
    assert_eq!(
        features.last_update(depth.instrument_id),
        Some(UnixNanos::from(40))
    );
}

#[test]
fn an_update_no_feature_reads_and_custom_data_are_no_update_of_an_instrument() {
    let features = FeatureSet::builder()
        .quotes(id(AUD), "quotes", sma(1))
        .data(level_type("SIM"), "level", read_level)
        .build()
        .unwrap();

    features.handle_quote(&at!(quote(GBP, 1.3), 10)).unwrap();
    features.handle_trade(&at!(trade(AUD, 0.7), 20)).unwrap();
    features.handle_data(&level("SIM", 1.0));

    assert_eq!(features.last_update(id(GBP)), None);
    assert_eq!(features.last_update(id(AUD)), None);
}

#[test]
fn a_set_reads_the_instruments_of_its_market_data_features() {
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), "bars", sma(1))
        .data(level_type("SIM"), "level", read_level)
        .build()
        .unwrap();

    assert!(features.reads(id(AUD)));
    assert!(!features.reads(id(GBP)));
}

#[test]
fn a_set_declares_no_warmup_unless_given_one() {
    let features = |warmup: Option<SignedDuration>| {
        let builder = FeatureSet::builder().quotes(id(AUD), "mid", sma(1));
        match warmup {
            Some(warmup) => builder.warmup(warmup),
            None => builder,
        }
        .build()
        .unwrap()
    };

    assert_eq!(features(None).warmup(), SignedDuration::ZERO);
    assert_eq!(
        features(Some(SignedDuration::from_hours(2))).warmup(),
        SignedDuration::from_hours(2)
    );
    assert_ne!(
        features(Some(SignedDuration::from_hours(2))).fingerprint(),
        features(None).fingerprint()
    );
    assert_ne!(
        features(Some(SignedDuration::from_hours(2))).fingerprint(),
        features(Some(SignedDuration::from_hours(3))).fingerprint()
    );
}

#[test]
fn a_negative_warmup_is_an_error() {
    let err = FeatureSet::builder()
        .quotes(id(AUD), "mid", sma(1))
        .warmup(SignedDuration::from_hours(-1))
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("warmup"), "{err}");
}

fn fingerprint(names: [&str; 2], code: &'static str) -> Fingerprint {
    FeatureSet::builder()
        .quotes(id(AUD), names[0], sma(1))
        .bars(BarType::from(AUD_BARS), names[1], sma(2))
        .code(code)
        .build()
        .unwrap()
        .fingerprint()
}

#[test]
fn the_fingerprint_is_the_same_for_the_same_names_and_code() {
    assert_eq!(
        fingerprint(["a", "b"], "code"),
        fingerprint(["a", "b"], "code")
    );
}

#[test]
fn the_fingerprint_changes_with_a_name_their_order_or_the_code() {
    let base = fingerprint(["a", "b"], "code");

    assert_ne!(base, fingerprint(["a", "c"], "code"));
    assert_ne!(base, fingerprint(["b", "a"], "code"));
    assert_ne!(base, fingerprint(["a", "b"], "code 2"));
    // Names are told apart from each other, not only concatenated.
    assert_ne!(
        fingerprint(["ab", "c"], "code"),
        fingerprint(["a", "bc"], "code")
    );
}

#[test]
fn a_fingerprint_extended_with_a_part_changes_with_the_part() {
    let base = fingerprint(["a", "b"], "code");

    assert_ne!(base.with(b"rule"), base);
    assert_ne!(base.with(b"rule"), base.with(b"rule 2"));
    assert_eq!(base.with(b"rule"), base.with(b"rule"));
}

/// Registers features with a strategy without a node, as the EMA's
/// tests do.
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {}

fn registered() -> Probe {
    let mut probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(nautilus_model::identifiers::StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
    };
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let portfolio = Rc::new(RefCell::new(Portfolio::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    probe
        .core
        .register(TraderId::from("HELIX-001"), clock, cache, portfolio)
        .unwrap();
    probe
}

/// The data engine hands Nautilus' handler of the strategy each
/// update, which feeds its registered indicators first.
#[test]
fn registered_features_get_quotes_trades_and_bars_through_nautilus() {
    let mut probe = registered();
    let features = FeatureSet::builder()
        .quotes(id(AUD), "quotes", sma(1))
        .trades(id(GBP), "trades", sma(1))
        .bars(BarType::from(AUD_BARS), "bars", sma(1))
        .build()
        .unwrap();
    features.register(&mut probe);

    probe.handle_quote(&quote(AUD, 0.7));
    probe.handle_trade(&trade(GBP, 1.3));
    probe.handle_bar(&bar(AUD_BARS, 0.75));

    let values = features.values().unwrap();
    assert!((values[0] - 0.7).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 1.3).abs() < 1e-9, "{values:?}");
    assert!((values[2] - 0.75).abs() < 1e-9, "{values:?}");
}

/// Registered twice, as on a restart: each update still counts once.
#[test]
fn a_set_registered_twice_gets_each_update_once() {
    let mut probe = registered();
    let features = FeatureSet::builder()
        .quotes(id(AUD), "quotes", sma(2))
        .build()
        .unwrap();
    features.register(&mut probe);
    features.register(&mut probe);

    probe.handle_quote(&quote(AUD, 0.7));

    assert_eq!(features.values(), None);
}

#[test]
fn a_rule_primarys_side_and_probability_are_the_last_two_names() {
    let features = FeatureSet::builder()
        .quotes(id(AUD), "mid", sma(1))
        .primary()
        .build()
        .unwrap();

    assert_eq!(
        features.names(),
        ["mid", "primary_side", "primary_probability"]
    );
}

/// Warm, the set still has no values until a side is given, and none
/// after.
#[test]
fn a_rule_primarys_values_are_there_only_on_a_side() {
    let features = FeatureSet::builder()
        .quotes(id(AUD), "mid", sma(1))
        .primary()
        .build()
        .unwrap();
    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    let sold = features.on_side(OrderSide::Sell, f64::NAN, || features.values());
    let bought = features.on_side(OrderSide::Buy, 0.8, || features.values());

    assert_eq!(features.values(), None);
    let sold = sold.unwrap();
    assert!((sold[0] - 0.7).abs() < 1e-9, "{sold:?}");
    assert_eq!(sold[1], -1.0);
    assert!(sold[2].is_nan(), "{sold:?}");
    assert_eq!(bought.unwrap()[1..], [1.0, 0.8]);
}

#[test]
fn declaring_a_rule_primary_changes_the_fingerprint() {
    let plain = FeatureSet::builder()
        .quotes(id(AUD), "mid", sma(1))
        .build()
        .unwrap();
    let ruled = FeatureSet::builder()
        .quotes(id(AUD), "mid", sma(1))
        .primary()
        .build()
        .unwrap();

    assert_ne!(plain.fingerprint(), ruled.fingerprint());
}

/// The fingerprint hashes this module's code: its tests live in a
/// file of their own, so editing one changes no fingerprint.
#[test]
fn the_fingerprinted_code_holds_no_test() {
    assert!(!SHARED_CODE[0].contains("#[test]"));
}
