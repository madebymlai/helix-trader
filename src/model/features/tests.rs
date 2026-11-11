use std::sync::Arc;

use nautilus_common::{actor::DataActor, cache::Cache, clock::VirtualClock};
use nautilus_model::{
    data::{BookOrder, DataType, HasTsInit, OrderBookDelta, stubs::stub_depth10},
    enums::{AggressorSide, BookAction, OrderSide},
    identifiers::{TradeId, TraderId},
    types::{Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

use rust_decimal::Decimal;

use super::*;
use crate::{
    model::{
        decide::DecisionRule,
        targets::{BracketSpec, Distance, Target, TargetConfig},
    },
    strategy::{AtrArgs, DataRequirements, EmaArgs},
};

const AUD: &str = "AUD/USD.SIM";
const GBP: &str = "GBP/USD.SIM";
const AUD_BARS: &str = "AUD/USD.SIM-1-MINUTE-MID-INTERNAL";
const AUD_BID_BARS: &str = "AUD/USD.SIM-1-MINUTE-BID-INTERNAL";

fn id(s: &str) -> InstrumentId {
    InstrumentId::from(s)
}

/// The last mid of a quote, price of a trade or close of a bar: Nautilus'
/// exponential average of one, `[indicators.last]`.
const LAST: Declared<EmaArgs> = Declared::new("last");
/// Nautilus' exponential average of two mids, prices or closes, the mean
/// of the first two once both came, `[indicators.two]`.
const TWO: Declared<EmaArgs> = Declared::new("two");
/// The tests' count of depth and deltas, `[indicators.book]`.
const BOOK: Declared<BookCountArgs> = Declared::new("book");
/// The tests' feature failing on every quote, `[indicators.failing]`.
const FAILING: Declared<FailingQuotesArgs> = Declared::new("failing");

/// The tables of the tests' features, as `[indicators]` holds them.
fn tables() -> IndicatorsConfig {
    toml::from_str(
        r#"
        last = { period = 1, price_type = "MID" }
        two = { period = 2, price_type = "MID" }
        book = {}
        failing = {}
        "#,
    )
    .unwrap()
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
struct BookCount {
    count: usize,
}

impl Indicator for BookCount {
    fn name(&self) -> String {
        "BookCount".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.count > 0
    }

    fn initialized(&self) -> bool {
        self.count > 0
    }

    fn handle_depth(&mut self, _depth: &OrderBookDepth) {
        self.count += 1;
    }

    fn handle_deltas(&mut self, _deltas: &OrderBookDeltas) {
        self.count += 1;
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

impl Feature for BookCount {
    fn value(&self) -> f64 {
        self.count as f64
    }
}

/// The arguments of `BookCount`: none.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BookCountArgs {}

impl IndicatorArgs for BookCountArgs {
    type Indicator = BookCount;

    fn build(self) -> anyhow::Result<BookCount> {
        Ok(BookCount { count: 0 })
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

/// The data type of the levels of `instrument`, keyed by its id.
fn level_type(instrument: &str) -> DataType {
    DataType::new("Level", None, Some(instrument.to_string()))
}

/// A level of `instrument`.
fn level(instrument: &str, level: f64) -> CustomData {
    CustomData::new(
        Arc::new(Level {
            level,
            ts: UnixNanos::default(),
        }),
        level_type(instrument),
    )
}

fn read_level(data: &CustomData) -> f64 {
    data.data
        .as_any()
        .downcast_ref::<Level>()
        .map_or(f64::NAN, |level| level.level)
}

#[test]
fn a_table_feeding_one_feature_names_it_after_the_table() {
    let tables = tables();

    let features = FeatureSet::builder()
        .quotes(id(AUD), LAST, &tables)
        .build()
        .unwrap();

    assert_eq!(features.names(), ["last"]);
}

/// Bars by their bar type, ticks by their kind and instrument.
#[test]
fn a_table_feeding_several_features_names_each_after_its_series_and_the_table() {
    let tables = tables();

    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), LAST, &tables)
        .quotes(id(AUD), LAST, &tables)
        .trades(id(GBP), LAST, &tables)
        .depth(id(AUD), BOOK, &tables)
        .deltas(id(GBP), BOOK, &tables)
        .build()
        .unwrap();

    assert_eq!(
        features.names(),
        [
            "AUD/USD.SIM-1-MINUTE-MID-INTERNAL:last",
            "QuoteTick of AUD/USD.SIM:last",
            "TradeTick of GBP/USD.SIM:last",
            "OrderBookDepth of AUD/USD.SIM:book",
            "OrderBookDelta of GBP/USD.SIM:book",
        ]
    );
}

/// `last` feeds two features, `two` one.
#[test]
fn each_table_is_named_by_the_features_it_feeds_itself() {
    let tables = tables();

    let features = FeatureSet::builder()
        .quotes(id(AUD), LAST, &tables)
        .quotes(id(AUD), TWO, &tables)
        .quotes(id(GBP), LAST, &tables)
        .build()
        .unwrap();

    assert_eq!(
        features.names(),
        [
            "QuoteTick of AUD/USD.SIM:last",
            "two",
            "QuoteTick of GBP/USD.SIM:last"
        ]
    );
}

#[test]
fn the_names_keep_the_order_of_their_declaration() {
    let tables = tables();

    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), TWO, &tables)
        .data("Level", id(AUD), "level", read_level)
        .quotes(id(AUD), LAST, &tables)
        .build()
        .unwrap();

    assert_eq!(features.names(), ["two", "level", "last"]);
}

/// A table read twice on one series names two features alike.
#[test]
fn two_features_of_one_name_are_an_error() {
    let tables = tables();

    let err = FeatureSet::builder()
        .quotes(id(AUD), LAST, &tables)
        .quotes(id(AUD), LAST, &tables)
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("QuoteTick of AUD/USD.SIM:last"), "{err}");
}

#[test]
fn a_set_without_features_is_an_error() {
    assert!(FeatureSet::builder().build().is_err());
}

#[test]
fn a_quote_reaches_only_the_features_on_the_quotes_of_its_instrument() {
    let tables = tables();
    let features = FeatureSet::builder()
        .quotes(id(AUD), LAST, &tables)
        .quotes(id(GBP), LAST, &tables)
        .trades(id(AUD), LAST, &tables)
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_quote(&quote(GBP, 1.3)).unwrap();

    // The feature on AUD's trades is still cold: no quote reached it.
    assert_eq!(features.values(), None);
    features.handle_trade(&trade(AUD, 0.65)).unwrap();
    let values = features.values().unwrap();
    assert!((values[0] - 0.7).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 1.3).abs() < 1e-9, "{values:?}");
    assert!((values[2] - 0.65).abs() < 1e-9, "{values:?}");
}

#[test]
fn a_bar_reaches_only_the_features_on_its_bar_type() {
    let tables = tables();
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), LAST, &tables)
        .bars(BarType::from(AUD_BID_BARS), LAST, &tables)
        .build()
        .unwrap();

    features.handle_bar(&bar(AUD_BARS, 0.7)).unwrap();

    assert_eq!(features.values(), None);
    features.handle_bar(&bar(AUD_BID_BARS, 0.6)).unwrap();
    let values = features.values().unwrap();
    assert!((values[0] - 0.7).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 0.6).abs() < 1e-9, "{values:?}");
}

/// Each feature counts what reaches it: the deltas feature is cold until
/// deltas of its instrument come, and each counts one.
#[test]
fn depth_and_deltas_reach_only_the_features_on_their_kind_and_instrument() {
    let tables = tables();
    let depth = stub_depth10();
    let features = FeatureSet::builder()
        .depth(depth.instrument_id, BOOK, &tables)
        .deltas(depth.instrument_id, BOOK, &tables)
        .build()
        .unwrap();

    features.handle_depth(&depth);
    features.handle_deltas(&deltas(GBP));

    assert_eq!(features.values(), None);
    features.handle_deltas(&deltas(&depth.instrument_id.to_string()));
    assert_eq!(features.values(), Some(vec![1.0, 1.0]));
}

/// A moving average has no depth, deltas or trade handler of its own:
/// Nautilus' defaults panic.
#[test]
fn an_update_of_a_kind_no_feature_reads_is_a_no_op() {
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), LAST, &tables())
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_trade(&trade(AUD, 0.7)).unwrap();
    features.handle_depth(&stub_depth10());
    features.handle_deltas(&deltas(AUD));
    features.handle_data(&level(AUD, 1.0));

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

/// The arguments of `FailingQuotes`: none.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FailingQuotesArgs {}

impl IndicatorArgs for FailingQuotesArgs {
    type Indicator = FailingQuotes;

    fn build(self) -> anyhow::Result<FailingQuotes> {
        Ok(FailingQuotes::default())
    }
}

#[test]
fn a_features_error_is_no_error_of_the_set_and_the_others_still_update() {
    let tables = tables();
    let features = FeatureSet::builder()
        .quotes(id(AUD), FAILING, &tables)
        .quotes(id(AUD), LAST, &tables)
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    let values = features.values().unwrap();
    assert!((values[1] - 0.7).abs() < 1e-9, "{values:?}");
}

#[test]
fn the_values_wait_for_every_feature() {
    let tables = tables();
    let features = FeatureSet::builder()
        .quotes(id(AUD), TWO, &tables)
        .quotes(id(AUD), LAST, &tables)
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
        .data("Level", id(AUD), "level", read_level)
        .build()
        .unwrap();

    features.handle_data(&level(GBP, 9.0));
    assert_eq!(features.values(), None);

    features.handle_data(&level(AUD, 1.5));
    features.handle_data(&level(AUD, 2.5));
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
    let tables = tables();
    let features = FeatureSet::builder()
        .quotes(id(AUD), LAST, &tables)
        .trades(id(AUD), LAST, &tables)
        .bars(BarType::from(AUD_BARS), LAST, &tables)
        .depth(depth.instrument_id, BOOK, &tables)
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
        .quotes(id(AUD), LAST, &tables())
        .data("Level", id(AUD), "level", read_level)
        .build()
        .unwrap();

    features.handle_quote(&at!(quote(GBP, 1.3), 10)).unwrap();
    features.handle_trade(&at!(trade(AUD, 0.7), 20)).unwrap();
    features.handle_data(&level(AUD, 1.0));

    assert_eq!(features.last_update(id(GBP)), None);
    assert_eq!(features.last_update(id(AUD)), None);
}

/// A roller subscribes these and a live warmup requests them: each kind of
/// tick or custom data of each instrument once, in the order declared, and
/// no bars.
#[test]
fn a_sets_ticks_are_those_of_its_tick_and_raw_features_each_once() {
    let tables = tables();
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), LAST, &tables)
        .trades(id(GBP), LAST, &tables)
        .quotes(id(AUD), LAST, &tables)
        .data("Level", id(AUD), "level", read_level)
        .quotes(id(AUD), TWO, &tables)
        .depth(id(AUD), BOOK, &tables)
        .deltas(id(GBP), BOOK, &tables)
        .build()
        .unwrap();
    let tick = |data, instrument: &str| TickSeries {
        data,
        instrument_id: id(instrument),
    };

    assert_eq!(
        features.ticks(),
        [
            tick(NautilusDataType::TradeTick, GBP),
            tick(NautilusDataType::QuoteTick, AUD),
            tick(
                NautilusDataType::Custom {
                    type_name: "Level".to_string()
                },
                AUD
            ),
            tick(NautilusDataType::OrderBookDepth, AUD),
            tick(NautilusDataType::OrderBookDelta, GBP),
        ]
    );
}

/// The bars of AUD's mids read by two features, its bids by one: each bar
/// type once, in the order declared, and no ticks.
#[test]
fn a_sets_bar_types_are_those_of_its_bar_features_each_once() {
    let tables = tables();
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), LAST, &tables)
        .quotes(id(AUD), LAST, &tables)
        .bars(BarType::from(AUD_BID_BARS), LAST, &tables)
        .bars(BarType::from(AUD_BARS), TWO, &tables)
        .build()
        .unwrap();

    assert_eq!(
        features.bar_types(),
        [BarType::from(AUD_BARS), BarType::from(AUD_BID_BARS)]
    );
}

/// A set of one feature on AUD's mids, given `warmup` if any.
fn warming(warmup: Option<SignedDuration>) -> Rc<FeatureSet> {
    let builder = FeatureSet::builder().quotes(id(AUD), LAST, &tables());
    match warmup {
        Some(warmup) => builder.warmup(warmup),
        None => builder,
    }
    .build()
    .unwrap()
}

#[test]
fn a_set_declares_no_warmup_unless_given_one() {
    assert_eq!(warming(None).warmup(), SignedDuration::ZERO);
    assert_eq!(
        warming(Some(SignedDuration::from_hours(2))).warmup(),
        SignedDuration::from_hours(2)
    );
}

#[test]
fn the_fingerprint_changes_with_the_warmup() {
    assert_ne!(
        warming(Some(SignedDuration::from_hours(2))).fingerprint(),
        warming(None).fingerprint()
    );
    assert_ne!(
        warming(Some(SignedDuration::from_hours(2))).fingerprint(),
        warming(Some(SignedDuration::from_hours(3))).fingerprint()
    );
}

#[test]
fn a_negative_warmup_is_an_error() {
    let err = FeatureSet::builder()
        .quotes(id(AUD), LAST, &tables())
        .warmup(SignedDuration::from_hours(-1))
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("warmup"), "{err}");
}

/// The fingerprint of raw features named `names` and `version`: a raw
/// feature has no table, so its name and the series it reads are in it.
fn fingerprint(names: [&str; 2], version: u32) -> Fingerprint {
    FeatureSet::builder()
        .data("Level", id(AUD), names[0], read_level)
        .data("Level", id(GBP), names[1], read_level)
        .version(version)
        .build()
        .unwrap()
        .fingerprint()
}

#[test]
fn the_fingerprint_is_the_same_for_the_same_names_and_version() {
    assert_eq!(fingerprint(["a", "b"], 1), fingerprint(["a", "b"], 1));
}

#[test]
fn the_fingerprint_changes_with_a_name_their_order_or_the_version() {
    let base = fingerprint(["a", "b"], 1);

    assert_ne!(base, fingerprint(["a", "c"], 1));
    assert_ne!(base, fingerprint(["b", "a"], 1));
    assert_ne!(base, fingerprint(["a", "b"], 2));
    // Names are told apart from each other, not only concatenated.
    assert_ne!(fingerprint(["ab", "c"], 1), fingerprint(["a", "bc"], 1));
}

/// A raw feature's name is the strategy's: what it reads is in the
/// fingerprint besides, the type name and the instrument of its data.
#[test]
fn the_fingerprint_changes_with_the_custom_data_a_raw_feature_reads() {
    let reading = |type_name: &str, instrument: &str| {
        FeatureSet::builder()
            .data(type_name, id(instrument), "level", read_level)
            .build()
            .unwrap()
            .fingerprint()
    };

    assert_ne!(reading("Level", GBP), reading("Level", AUD));
    assert_ne!(reading("Depth", AUD), reading("Level", AUD));
}

/// A set without a version is of version 0, as one given it.
#[test]
fn a_set_without_a_version_is_of_version_nought() {
    let unversioned = FeatureSet::builder()
        .data("Level", id(AUD), "a", read_level)
        .data("Level", id(GBP), "b", read_level)
        .build()
        .unwrap()
        .fingerprint();

    assert_eq!(unversioned, fingerprint(["a", "b"], 0));
}

#[test]
fn a_fingerprint_extended_with_a_part_changes_with_the_part() {
    let base = fingerprint(["a", "b"], 1);

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

/// The data engine hands Nautilus' handler of the strategy each update,
/// which feeds its registered indicators first: the set is registered for
/// its bars, and not for its quotes or trades, which the strategy hands its
/// warmup (`Warmup::feeding_ticks`). Through Nautilus, the features on
/// GBP/USD's quotes and trades read neither, and the one on AUD/USD's bars
/// reads its bar.
#[test]
fn registered_features_get_their_bars_through_nautilus_and_not_their_quotes_or_trades() {
    let mut probe = registered();
    let tables = tables();
    let features = FeatureSet::builder()
        .quotes(id(GBP), LAST, &tables)
        .trades(id(GBP), LAST, &tables)
        .bars(BarType::from(AUD_BARS), LAST, &tables)
        .build()
        .unwrap();
    features.register(&mut probe);

    probe.handle_quote(&quote(GBP, 1.3));
    probe.handle_trade(&trade(GBP, 1.3));
    probe.handle_bar(&bar(AUD_BARS, 0.75));

    assert_eq!(features.last_update(id(GBP)), None);
    assert_eq!(features.last_update(id(AUD)), Some(UnixNanos::default()));
}

/// Registered twice, as on a restart: each bar still counts once, and the
/// average of two is cold after one.
#[test]
fn a_set_registered_twice_gets_each_update_once() {
    let mut probe = registered();
    let features = FeatureSet::builder()
        .bars(BarType::from(AUD_BARS), TWO, &tables())
        .build()
        .unwrap();
    features.register(&mut probe);
    features.register(&mut probe);

    probe.handle_bar(&bar(AUD_BARS, 0.75));

    assert_eq!(features.values(), None);
}

/// The width of AUD/USD's bracket `distance` a side, of an ATR of one bar
/// if in ATRs, deciding every 15 minutes on its quotes.
fn width_of(distance: Distance) -> Rc<BracketWidth> {
    let target = TargetConfig::builder()
        .instrument_id(id(AUD))
        .quantity(Quantity::from(1))
        .spec(Target {
            bracket: Some(BracketSpec {
                take_profit: distance,
                stop: distance,
                atr: matches!(distance, Distance::Atr(_)).then_some(AtrArgs {
                    period: 1,
                    ma_type: None,
                    use_previous: None,
                    value_floor: None,
                }),
            }),
            limit: SignedDuration::from_mins(5),
            atr: None,
        })
        .build();
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![crate::strategy::TickSeries {
            data: nautilus_model::data::NautilusDataType::QuoteTick,
            instrument_id: id(AUD),
        }],
        warmup: SignedDuration::from_hours(24),
    };
    let rule = DecisionRule::Every(SignedDuration::from_mins(15));
    Rc::new(target.width(rule, &requirements).unwrap())
}

/// The feature over `width` of AUD/USD's quotes.
fn over(width: &Rc<BracketWidth>) -> Rc<FeatureSet> {
    testing::mid_over(id(AUD), width).build().unwrap()
}

/// 10 bps of the mid 0.7 is 0.0007: the mid is 1000 of it.
#[test]
fn a_feature_over_the_bracket_reads_its_quotes_over_its_width() {
    let features = over(&width_of(Distance::BasisPoints(Decimal::from(10))));

    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    let values = features.values().unwrap();
    assert!((values[0] - 1000.0).abs() < 1e-9, "{values:?}");
}

/// `[indicators.also]`, a second feature over the bracket.
const ALSO: Declared<testing::MidOverWidthArgs> = Declared::new("also");

/// The tables of `over` and `also`.
fn over_tables() -> IndicatorsConfig {
    toml::from_str("over = {}\nalso = {}").unwrap()
}

/// Two features over one width read it, the one the target's setter is
/// handed (`bracket_width`).
#[test]
fn features_over_one_width_hand_it_to_the_target() {
    let width = width_of(Distance::BasisPoints(Decimal::from(10)));

    let features = testing::mid_over(id(AUD), &width)
        .over_bracket(id(AUD), ALSO, &over_tables(), &width)
        .build()
        .unwrap();

    let handed = features.bracket_width().unwrap();
    assert!(std::ptr::eq(handed, Rc::as_ptr(&width)));
}

/// The target's setter runs on the ATR of one width: features over two
/// are refused as they are declared.
#[test]
#[should_panic(expected = "two widths")]
fn features_over_two_widths_panic() {
    let in_bps = || width_of(Distance::BasisPoints(Decimal::from(10)));

    let _ = testing::mid_over(id(AUD), &in_bps()).over_bracket(
        id(AUD),
        ALSO,
        &over_tables(),
        &in_bps(),
    );
}
