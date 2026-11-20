use std::sync::Arc;

use nautilus_common::{actor::DataActor, cache::Cache, clock::VirtualClock};
use nautilus_core::Params;
use nautilus_model::{
    data::{BookOrder, CustomDataTrait, DataType, HasTsInit, OrderBookDelta, stubs::stub_depth10},
    enums::{AggressorSide, BookAction, OrderSide},
    identifiers::{StrategyId, TradeId, TraderId},
    types::{Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;
use rust_decimal::Decimal;

use super::{testing::BOOK, *};
use crate::{
    model::{
        decide::DecisionRule,
        targets::{BracketSpec, Distance, Target, TargetConfig},
    },
    strategy::{AtrArgs, EmaArgs, Series, requirements::DataRequirements},
};

const AUD: &str = "AUD/USD.SIM";
const GBP: &str = "GBP/USD.SIM";
const AUD_BARS: &str = "AUD/USD.SIM-1-MINUTE-MID-INTERNAL";
const AUD_BID_BARS: &str = "AUD/USD.SIM-1-MINUTE-BID-INTERNAL";

fn id(s: &str) -> InstrumentId {
    InstrumentId::from(s)
}

/// The `[indicators.last]` exponential average of one value: the last mid, price or close.
const LAST: Declared<EmaArgs> = Declared::new("last");
/// The `[indicators.two]` exponential average of two values. It is the mean of the first two
/// once both have come.
const TWO: Declared<EmaArgs> = Declared::new("two");
const FAILING: Declared<FailingQuotesArgs> = Declared::new("failing");

fn tables() -> IndicatorsConfig {
    toml::from_str(
        r#"
        last = { period = 1, price_type = "MID" }
        two = { period = 2, price_type = "MID" }
        book = {}
        failing = {}
        sequence = {}
        "#,
    )
    .unwrap()
}

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

impl CustomDataTrait for Level {
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

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
}

/// A custom series with no metadata, under the instrument's id. Nautilus' Databento client keys
/// it this way.
fn level_series(instrument: &str) -> CustomSeries {
    CustomSeries::new("Level", instrument.to_string(), None).unwrap()
}

fn level(instrument: &str, level: f64) -> CustomData {
    point(level_series(instrument).data_type(), level)
}

fn levels(source: &str, identifier: &str) -> CustomSeries {
    custom_series("Level", source, identifier)
}

fn custom_series(type_name: &str, source: &str, identifier: &str) -> CustomSeries {
    let metadata = Params::new([("source".to_string(), serde_json::json!(source))].into());
    CustomSeries::new(type_name, identifier.to_string(), Some(metadata)).unwrap()
}

fn point(data_type: &DataType, level: f64) -> CustomData {
    CustomData::new(
        Arc::new(Level {
            level,
            ts: UnixNanos::default(),
        }),
        data_type.clone(),
    )
}

fn read_level(data: &CustomData) -> f64 {
    data.data
        .as_any()
        .downcast_ref::<Level>()
        .map_or(f64::NAN, |level| level.level)
}

#[rstest]
fn a_table_feeding_one_feature_names_it_after_the_table() {
    let tables = tables();

    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .build()
        .unwrap();

    assert_eq!(features.names(), ["last"]);
}

#[rstest]
fn a_table_feeding_several_features_names_each_after_its_series_and_the_table() {
    let tables = tables();

    let features = FeatureSet::builder()
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], LAST, &tables)
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .feature(&[Series::trades(id(GBP))], LAST, &tables)
        .feature(&[Series::depth(id(AUD))], BOOK, &tables)
        .feature(&[Series::deltas(id(GBP))], BOOK, &tables)
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

#[rstest]
fn each_table_is_named_by_the_features_it_feeds_itself() {
    let tables = tables();

    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .feature(&[Series::quotes(id(AUD))], TWO, &tables)
        .feature(&[Series::quotes(id(GBP))], LAST, &tables)
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

#[rstest]
fn the_names_keep_the_order_of_their_declaration() {
    let tables = tables();

    let features = FeatureSet::builder()
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], TWO, &tables)
        .custom(&level_series(AUD), "level", read_level)
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .build()
        .unwrap();

    assert_eq!(features.names(), ["two", "level", "last"]);
}

#[rstest]
fn two_features_of_one_name_are_an_error() {
    let tables = tables();

    let err = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("QuoteTick of AUD/USD.SIM:last"), "{err}");
}

#[rstest]
fn a_set_without_features_is_an_error() {
    let err = FeatureSet::builder().build().unwrap_err().to_string();

    assert!(err.contains("needs a feature"), "{err}");
}

#[rstest]
fn a_quote_reaches_only_the_features_on_the_quotes_of_its_instrument() {
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .feature(&[Series::quotes(id(GBP))], LAST, &tables)
        .feature(&[Series::trades(id(AUD))], LAST, &tables)
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_quote(&quote(GBP, 1.3)).unwrap();

    // No quote reached the feature on AUD's trades
    assert_eq!(features.values(), None);
    features.handle_trade(&trade(AUD, 0.65)).unwrap();
    let values = features.values().unwrap();
    assert!((values[0] - 0.7).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 1.3).abs() < 1e-9, "{values:?}");
    assert!((values[2] - 0.65).abs() < 1e-9, "{values:?}");
}

#[rstest]
fn a_bar_reaches_only_the_features_on_its_bar_type() {
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], LAST, &tables)
        .feature(&[Series::Bars(BarType::from(AUD_BID_BARS))], LAST, &tables)
        .build()
        .unwrap();

    features.handle_bar(&bar(AUD_BARS, 0.7)).unwrap();

    assert_eq!(features.values(), None);
    features.handle_bar(&bar(AUD_BID_BARS, 0.6)).unwrap();
    let values = features.values().unwrap();
    assert!((values[0] - 0.7).abs() < 1e-9, "{values:?}");
    assert!((values[1] - 0.6).abs() < 1e-9, "{values:?}");
}

/// The deltas feature is cold until deltas for its instrument arrive. Each update counts as one.
#[rstest]
fn depth_and_deltas_reach_only_the_features_on_their_kind_and_instrument() {
    let tables = tables();
    let depth = stub_depth10();
    let features = FeatureSet::builder()
        .feature(&[Series::depth(depth.instrument_id)], BOOK, &tables)
        .feature(&[Series::deltas(depth.instrument_id)], BOOK, &tables)
        .build()
        .unwrap();

    features.handle_depth(&depth);
    features.handle_deltas(&deltas(GBP));

    assert_eq!(features.values(), None);
    features.handle_deltas(&deltas(&depth.instrument_id.to_string()));
    assert_eq!(features.values(), Some(vec![1.0, 1.0]));
}

/// A moving average has no depth, deltas or trade handler. Nautilus' defaults panic.
#[rstest]
fn an_update_of_a_kind_no_feature_reads_is_a_no_op() {
    let features = FeatureSet::builder()
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], LAST, &tables())
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_trade(&trade(AUD, 0.7)).unwrap();
    features.handle_depth(&stub_depth10());
    features.handle_deltas(&deltas(AUD));

    assert_eq!(features.values(), None);
}

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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FailingQuotesArgs {}

impl IndicatorArgs for FailingQuotesArgs {
    type Indicator = FailingQuotes;

    fn build(self) -> anyhow::Result<FailingQuotes> {
        Ok(FailingQuotes::default())
    }
}

#[rstest]
fn a_features_error_is_no_error_of_the_set_and_the_others_still_update() {
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], FAILING, &tables)
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    let values = features.values().unwrap();
    assert_eq!(values[0], 1.0, "{values:?}");
    assert!((values[1] - 0.7).abs() < 1e-9, "{values:?}");
}

#[rstest]
fn the_values_wait_for_every_feature() {
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], TWO, &tables)
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
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

/// Caches `data` the way Nautilus' data engine does before it runs the strategy's handlers.
fn cached(probe: &Probe, data: CustomData) {
    probe.cache_rc().borrow_mut().add_custom_data(data);
}

/// Cold before registration, whatever the cache holds. Then reads the latest level under
/// AUD/USD's id, counting one cached before registration. Another instrument's level is ignored.
#[rstest]
fn a_raw_feature_reads_the_latest_point_of_its_data_type_in_the_cache() {
    let mut probe = registered();
    let features = FeatureSet::builder()
        .custom(&level_series(AUD), "level", read_level)
        .build()
        .unwrap();
    cached(&probe, level(AUD, 0.5));

    let unregistered = features.values();
    features.register(&mut probe);
    let on_register = features.values();
    cached(&probe, level(GBP, 9.0));
    let after_gbp = features.values();
    cached(&probe, level(AUD, 1.5));
    cached(&probe, level(AUD, 2.5));

    assert_eq!(unregistered, None);
    assert_eq!(on_register, Some(vec![0.5]));
    assert_eq!(after_gbp, Some(vec![0.5]));
    assert_eq!(features.values(), Some(vec![2.5]));
}

/// Cold before registration, and until the cache holds a point of its data type. A point with
/// another identifier does not count. Then reads the latest point.
#[rstest]
fn a_raw_feature_of_a_custom_series_reads_the_latest_point_of_its_data_type_in_the_cache() {
    let mut probe = registered();
    let yields = levels("yields", "US10Y");
    let features = FeatureSet::builder()
        .custom(&yields, "yield", read_level)
        .build()
        .unwrap();
    let of = |identifier: &str| {
        let metadata = Params::new([("source".to_string(), serde_json::json!("yields"))].into());
        DataType::new("Level", Some(metadata), Some(identifier.to_string()))
    };

    let unregistered = features.values();
    features.register(&mut probe);
    let on_register = features.values();
    cached(&probe, point(&of("US2Y"), 9.0));
    let after_another = features.values();
    cached(&probe, point(&of("US10Y"), 1.5));
    cached(&probe, point(&of("US10Y"), 2.5));

    assert_eq!(unregistered, None);
    assert_eq!(on_register, None);
    assert_eq!(after_another, None);
    assert_eq!(features.values(), Some(vec![2.5]));
}

fn yields_type(identifier: &str) -> DataType {
    levels("yields", identifier).data_type().clone()
}

/// Returns the values of a raw feature over the levels under `identifier`, at three points.
///
/// The feature registers on 2026-11-25 at 23:00, when GCZ26 stands for the root GC. It is then
/// handed GC's roll into GCG27 at 00:00 UTC on 2026-11-26 (`rolls/README.md`). The points are:
/// - After the levels of GCZ26 and GCG27 are cached, before the roll.
/// - After the roll.
/// - After another level of GCZ26 is cached.
fn levels_across_the_roll(identifier: &str) -> [Option<Vec<f64>>; 3] {
    let start: jiff::Timestamp = "2026-11-25T23:00:00Z".parse().unwrap();
    let mut probe = registered_at(start.into());
    let features = FeatureSet::builder()
        .custom(&levels("yields", identifier), "level", read_level)
        .build()
        .unwrap();
    features.register(&mut probe);
    cached(&probe, point(&yields_type("GCZ26.GLBX"), 0.25));
    cached(&probe, point(&yields_type("GCG27.GLBX"), 0.75));
    let before_the_roll = features.values();

    let roll = Transition::after(id("GC.GLBX"), start).unwrap();
    features.roll(&roll);
    let rolled = features.values();
    cached(&probe, point(&yields_type("GCZ26.GLBX"), -0.25));

    [before_the_roll, rolled, features.values()]
}

/// A raw feature over a custom series on the root GC reads the latest point of GCZ26, the
/// contract standing for it. At the roll it moves to the latest point of GCG27 at once. No later
/// point of GCZ26 reaches it. The roll does not move GCZ26's own series.
#[rstest]
#[case::of_the_root("GC.GLBX", [0.25, 0.75, 0.75])]
#[case::of_a_contract("GCZ26.GLBX", [0.25, 0.25, -0.25])]
fn a_raw_feature_of_a_roots_custom_series_reads_the_contract_in_front_and_moves_at_its_roll(
    #[case] series: &str,
    #[case] levels: [f64; 3],
) {
    assert_eq!(
        levels_across_the_roll(series),
        levels.map(|level| Some(vec![level]))
    );
}

macro_rules! at {
    ($update:expr, $ns:expr) => {{
        let mut update = $update;
        update.ts_init = UnixNanos::from($ns);
        update
    }};
}

#[rstest]
fn an_instruments_last_update_is_the_latest_a_feature_read_of_any_kind() {
    let depth = stub_depth10();
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .feature(&[Series::trades(id(AUD))], LAST, &tables)
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], LAST, &tables)
        .feature(&[Series::depth(depth.instrument_id)], BOOK, &tables)
        .build()
        .unwrap();

    assert_eq!(features.last_update(id(AUD)), None);
    features.handle_quote(&at!(quote(AUD, 0.7), 10)).unwrap();
    features.handle_bar(&at!(bar(AUD_BARS, 0.7), 30)).unwrap();

    // Older than the bar, so the latest stays
    features.handle_trade(&at!(trade(AUD, 0.7), 20)).unwrap();
    features.handle_depth(&at!(depth.clone(), 40));

    assert_eq!(features.last_update(id(AUD)), Some(UnixNanos::from(30)));
    assert_eq!(
        features.last_update(depth.instrument_id),
        Some(UnixNanos::from(40))
    );
}

#[rstest]
fn an_update_no_feature_reads_and_custom_data_are_no_update_of_an_instrument() {
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], LAST, &tables())
        .custom(&level_series(AUD), "level", read_level)
        .build()
        .unwrap();

    let mut probe = registered();
    features.register(&mut probe);
    features.handle_quote(&at!(quote(GBP, 1.3), 10)).unwrap();
    features.handle_trade(&at!(trade(AUD, 0.7), 20)).unwrap();
    cached(&probe, level(AUD, 1.0));

    assert_eq!(features.last_update(id(GBP)), None);
    assert_eq!(features.last_update(id(AUD)), None);
}

/// Lists each kind of tick of each instrument once, in the order declared. It lists no bars and
/// no custom series under an instrument's id.
#[rstest]
fn a_sets_ticks_are_those_of_its_tick_features_each_once() {
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], LAST, &tables)
        .feature(&[Series::trades(id(GBP))], LAST, &tables)
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .custom(&level_series(AUD), "level", read_level)
        .feature(&[Series::quotes(id(AUD))], TWO, &tables)
        .feature(&[Series::depth(id(AUD))], BOOK, &tables)
        .feature(&[Series::deltas(id(GBP))], BOOK, &tables)
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
            tick(NautilusDataType::OrderBookDepth, AUD),
            tick(NautilusDataType::OrderBookDelta, GBP),
        ]
    );
}

/// Lists each custom series a raw feature reads (`model::reads`) once, in the order declared. One
/// is under an instrument's id. None is a tick series.
#[rstest]
fn a_sets_custom_series_are_those_of_its_raw_features_each_once() {
    let (us10y, us2y, aud) = (
        levels("yields", "US10Y"),
        levels("yields", "US2Y"),
        level_series(AUD),
    );
    let features = FeatureSet::builder()
        .custom(&us2y, "2y", read_level)
        .custom(&aud, "aud", read_level)
        .custom(&us10y, "10y", read_level)
        .custom(&us2y, "2y again", read_level)
        .build()
        .unwrap();

    assert_eq!(features.custom(), [us2y, aud, us10y]);
    assert_eq!(features.ticks(), []);
}

/// Lists each bar type once, in the order declared. It lists no ticks.
#[rstest]
fn a_sets_bar_types_are_those_of_its_bar_features_each_once() {
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], LAST, &tables)
        .feature(&[Series::quotes(id(AUD))], LAST, &tables)
        .feature(&[Series::Bars(BarType::from(AUD_BID_BARS))], LAST, &tables)
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], TWO, &tables)
        .build()
        .unwrap();

    assert_eq!(
        features.bar_types(),
        [BarType::from(AUD_BARS), BarType::from(AUD_BID_BARS)]
    );
}

fn warming(warmup: Option<SignedDuration>) -> Rc<FeatureSet> {
    let builder = FeatureSet::builder().feature(&[Series::quotes(id(AUD))], LAST, &tables());
    match warmup {
        Some(warmup) => builder.warmup(warmup),
        None => builder,
    }
    .build()
    .unwrap()
}

#[rstest]
fn a_set_declares_no_warmup_unless_given_one() {
    assert_eq!(warming(None).warmup(), SignedDuration::ZERO);
    assert_eq!(
        warming(Some(SignedDuration::from_hours(2))).warmup(),
        SignedDuration::from_hours(2)
    );
}

#[rstest]
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

#[rstest]
fn a_negative_warmup_is_an_error() {
    let err = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], LAST, &tables())
        .warmup(SignedDuration::from_hours(-1))
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("1h ago"), "{err}");
}

/// A raw feature has no table, so the fingerprint holds its name and the series it reads.
fn fingerprint(names: [&str; 2], version: u32) -> Fingerprint {
    FeatureSet::builder()
        .custom(&level_series(AUD), names[0], read_level)
        .custom(&level_series(GBP), names[1], read_level)
        .version(version)
        .build()
        .unwrap()
        .fingerprint()
}

#[rstest]
fn the_fingerprint_changes_with_a_name_their_order_or_the_version() {
    let base = fingerprint(["a", "b"], 1);

    assert_ne!(base, fingerprint(["a", "c"], 1));
    assert_ne!(base, fingerprint(["b", "a"], 1));
    assert_ne!(base, fingerprint(["a", "b"], 2));

    // Names are told apart, not only concatenated
    assert_ne!(fingerprint(["ab", "c"], 1), fingerprint(["a", "bc"], 1));
}

/// Another series rebuilds the dataset and the models. The fingerprint holds the data type's
/// identifier, metadata and type name.
#[rstest]
fn the_fingerprint_changes_with_the_custom_series_a_raw_feature_reads() {
    let reading = |series: CustomSeries| {
        FeatureSet::builder()
            .custom(&series, "yield", read_level)
            .build()
            .unwrap()
            .fingerprint()
    };
    let base = reading(levels("yields", "US10Y"));

    assert_eq!(base, reading(levels("yields", "US10Y")));
    assert_ne!(base, reading(levels("yields", "US2Y")));
    assert_ne!(base, reading(levels("rates", "US10Y")));
    assert_ne!(base, reading(custom_series("Depth", "yields", "US10Y")));
}

#[rstest]
fn a_set_without_a_version_is_of_version_nought() {
    let unversioned = FeatureSet::builder()
        .custom(&level_series(AUD), "a", read_level)
        .custom(&level_series(GBP), "b", read_level)
        .build()
        .unwrap()
        .fingerprint();

    assert_eq!(unversioned, fingerprint(["a", "b"], 0));
}

#[rstest]
fn a_fingerprint_extended_with_a_part_changes_with_the_part() {
    let base = fingerprint(["a", "b"], 1);

    assert_ne!(base.with(b"rule"), base);
    assert_ne!(base.with(b"rule"), base.with(b"rule 2"));
    assert_eq!(base.with(b"rule"), base.with(b"rule"));
}

#[derive(Debug)]
struct Probe {
    core: StrategyCore,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {}

fn registered() -> Probe {
    registered_at(UnixNanos::default())
}

fn registered_at(now: UnixNanos) -> Probe {
    let mut probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
    };
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    clock.borrow_mut().advance_time(now, true);
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

/// The set is registered for no indicator, so the features read nothing through Nautilus. Its
/// bars, quotes and trades come through the strategy's warmup (`Warmup::feeding`,
/// `Warmup::feeding_ticks`).
#[rstest]
fn registered_features_get_nothing_through_nautilus() {
    let mut probe = registered();
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(&[Series::quotes(id(GBP))], LAST, &tables)
        .feature(&[Series::trades(id(GBP))], LAST, &tables)
        .feature(&[Series::Bars(BarType::from(AUD_BARS))], LAST, &tables)
        .build()
        .unwrap();
    features.register(&mut probe);

    probe.handle_quote(&quote(GBP, 1.3));
    probe.handle_trade(&trade(GBP, 1.3));
    probe.handle_bar(&bar(AUD_BARS, 0.75));

    assert_eq!(features.last_update(id(GBP)), None);
    assert_eq!(features.last_update(id(AUD)), None);
}

/// Returns the width of AUD/USD's bracket, with `distance` on each side. The target decides every
/// 15 minutes, on AUD/USD's quotes.
fn width_of(distance: Distance) -> Rc<BracketWidth> {
    let target = TargetConfig::builder()
        .instrument_id(id(AUD))
        .risk("0.5%".parse().unwrap())
        .spec(Target {
            bracket: Some(BracketSpec {
                take_profit: distance,
                stop: distance,
                atr: matches!(distance, Distance::Atr(_)).then_some(
                    AtrArgs {
                        period: 1,
                        ma_type: None,
                        use_previous: None,
                        value_floor: None,
                    }
                    .into(),
                ),
            }),
            limit: SignedDuration::from_mins(5),
            atr: None,
        })
        .build();
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![crate::strategy::requirements::TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: id(AUD),
        }],
        custom: vec![],
        warmup: SignedDuration::from_hours(24),
    };
    let rule = DecisionRule::Every(SignedDuration::from_mins(15));
    Rc::new(target.width(rule, &requirements).unwrap())
}

fn over(width: &Rc<BracketWidth>) -> Rc<FeatureSet> {
    testing::mid_over(id(AUD), width).build().unwrap()
}

/// 10 bps of the mid 0.7 is 0.0007. The mid is 1000 widths.
#[rstest]
fn a_feature_over_the_bracket_reads_its_quotes_over_its_width() {
    let features = over(&width_of(Distance::BasisPoints(Decimal::from(10))));

    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    let values = features.values().unwrap();
    assert!((values[0] - 1000.0).abs() < 1e-9, "{values:?}");
}

const ALSO: Declared<testing::MidOverWidthArgs> = Declared::new("also");

fn over_tables() -> IndicatorsConfig {
    toml::from_str("over = {}\nalso = {}").unwrap()
}

#[rstest]
fn features_over_one_width_hand_it_to_the_target() {
    let width = width_of(Distance::BasisPoints(Decimal::from(10)));

    let features = testing::mid_over(id(AUD), &width)
        .feature(&[Series::quotes(id(AUD))], ALSO, &over_tables())
        .build()
        .unwrap();

    let handed = features.bracket_width().unwrap();
    assert!(std::ptr::eq(handed, Rc::as_ptr(&width)));
}

/// The target's setter runs on one width. A set given a second width is refused when built.
#[rstest]
fn a_second_width_of_the_bracket_is_refused_when_the_set_is_built() {
    let in_bps = || width_of(Distance::BasisPoints(Decimal::from(10)));

    let err = testing::mid_over(id(AUD), &in_bps())
        .bracket(&in_bps())
        .feature(&[Series::quotes(id(AUD))], ALSO, &over_tables())
        .build()
        .unwrap_err()
        .to_string();

    for part in ["bracket", "twice"] {
        assert!(err.contains(part), "{err}");
    }
}

/// A set with a feature over the bracket and no width is refused when built. The error names the
/// feature.
#[rstest]
fn a_feature_over_the_bracket_with_no_width_is_refused_when_the_set_is_built() {
    let err = FeatureSet::builder()
        .feature(&[Series::quotes(id(AUD))], ALSO, &over_tables())
        .build()
        .unwrap_err()
        .to_string();

    for part in ["also", "bracket"] {
        assert!(err.contains(part), "{err}");
    }
}

/// A width that no feature reads is refused when the set is built.
#[rstest]
fn a_width_of_the_bracket_no_feature_reads_is_refused_when_the_set_is_built() {
    let err = testing::last_mid(id(AUD))
        .bracket(&width_of(Distance::BasisPoints(Decimal::from(10))))
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("bracket"), "{err}");
}

fn ticks(data: NautilusDataType, instrument: &str) -> Series {
    Series::Ticks(TickSeries {
        data,
        instrument_id: id(instrument),
    })
}

fn quotes(instrument: &str) -> Series {
    ticks(NautilusDataType::QuoteTick, instrument)
}

fn trades(instrument: &str) -> Series {
    ticks(NautilusDataType::TradeTick, instrument)
}

fn bars(bar_type: &str) -> Series {
    Series::Bars(BarType::from(bar_type))
}

/// Records each update and each reset as a digit.
///
/// - A quote is 1, a trade 2, a bar 3, depth 4, deltas 5 and custom data 6.
/// - A reset is 9.
///
/// The indicator is initialized once an update has come since the last reset.
#[derive(Default)]
struct Sequence {
    digits: u64,
    warm: bool,
}

impl Sequence {
    fn record(&mut self, digit: u64) {
        self.digits = self.digits * 10 + digit;
        self.warm = digit != 9;
    }
}

impl Indicator for Sequence {
    fn name(&self) -> String {
        "Sequence".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.warm
    }

    fn initialized(&self) -> bool {
        self.warm
    }

    fn handle_quote(&mut self, _quote: &QuoteTick) -> anyhow::Result<()> {
        self.record(1);
        Ok(())
    }

    fn handle_trade(&mut self, _trade: &TradeTick) {
        self.record(2);
    }

    fn handle_bar(&mut self, _bar: &Bar) {
        self.record(3);
    }

    fn handle_depth(&mut self, _depth: &OrderBookDepth) {
        self.record(4);
    }

    fn handle_deltas(&mut self, _deltas: &OrderBookDeltas) {
        self.record(5);
    }

    fn reset(&mut self) {
        self.record(9);
    }
}

impl Feature for Sequence {
    fn value(&self) -> f64 {
        self.digits as f64
    }

    fn custom_handler(&mut self) -> Option<&mut dyn CustomHandler> {
        Some(self)
    }
}

impl CustomHandler for Sequence {
    fn handle_custom(&mut self, _data: &CustomData) -> anyhow::Result<()> {
        self.record(6);
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SequenceArgs {}

impl IndicatorArgs for SequenceArgs {
    type Indicator = Sequence;

    fn build(self) -> anyhow::Result<Sequence> {
        Ok(Sequence::default())
    }
}

const SEQUENCE: Declared<SequenceArgs> = Declared::new("sequence");

/// The feature reads AUD's quotes, GBP's trades, AUD's mid bars, and the stub's book depth and
/// deltas. It records 1 2 3 4 5, then 1. GBP's quote, AUD's trade, AUD's bid bars and GBP's
/// deltas do not reach it.
#[rstest]
fn a_feature_on_several_series_gets_each_of_their_updates_in_the_order_it_comes() {
    let depth = stub_depth10();
    let book = depth.instrument_id.to_string();
    let features = FeatureSet::builder()
        .feature(
            &[
                quotes(AUD),
                trades(GBP),
                bars(AUD_BARS),
                ticks(NautilusDataType::OrderBookDepth, &book),
                ticks(NautilusDataType::OrderBookDelta, &book),
            ],
            SEQUENCE,
            &tables(),
        )
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_quote(&quote(GBP, 1.3)).unwrap();
    features.handle_trade(&trade(AUD, 0.7)).unwrap();
    features.handle_trade(&trade(GBP, 1.3)).unwrap();
    features.handle_bar(&bar(AUD_BID_BARS, 0.7)).unwrap();
    features.handle_bar(&bar(AUD_BARS, 0.7)).unwrap();
    features.handle_deltas(&deltas(GBP));
    features.handle_depth(&depth);
    features.handle_deltas(&deltas(&book));
    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    assert_eq!(features.values(), Some(vec![123_451.0]));
}

/// GCZ26 stands for the root GC from 2026-11-25 until the root rolls into GCG27 at 00:00 UTC on
/// 2026-11-26 (`rolls/README.md`).
const GC: &str = "GC.GLBX";
const GCZ26: &str = "GCZ26.GLBX";
const GCG27: &str = "GCG27.GLBX";

fn before_gcs_roll() -> jiff::Timestamp {
    "2026-11-25T23:00:00Z".parse().unwrap()
}

/// A quote of GCZ26 matches both its own quote series and the root's. It reaches the feature
/// once for both.
#[rstest]
fn an_update_matching_several_of_a_features_series_reaches_it_once() {
    let mut probe = registered_at(before_gcs_roll().into());
    let features = FeatureSet::builder()
        .feature(&[quotes(GC), quotes(GCZ26)], SEQUENCE, &tables())
        .build()
        .unwrap();
    features.register(&mut probe);

    features.handle_quote(&quote(GCZ26, 4000.0)).unwrap();

    assert_eq!(features.values(), Some(vec![1.0]));
}

/// The feature reads GC's quotes and trades and AUD's quotes. It reads GCZ26's ticks until the
/// roll. The roll moves both GC series to GCG27 and resets the feature once (9). The feature is
/// cold until its next update.
#[rstest]
fn a_roll_moves_every_series_of_a_feature_on_the_root_and_resets_it_once() {
    let start = before_gcs_roll();
    let mut probe = registered_at(start.into());
    let features = FeatureSet::builder()
        .feature(&[quotes(GC), trades(GC), quotes(AUD)], SEQUENCE, &tables())
        .build()
        .unwrap();
    features.register(&mut probe);

    features.handle_quote(&quote(GCZ26, 4000.0)).unwrap();
    features.handle_trade(&trade(GCG27, 4040.0)).unwrap();
    features.handle_trade(&trade(GCZ26, 4000.0)).unwrap();
    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    let before_the_roll = features.values();
    features.roll(&Transition::after(id(GC), start).unwrap());
    let rolled = features.values();
    features.handle_quote(&quote(GCZ26, 4000.0)).unwrap();
    features.handle_trade(&trade(GCG27, 4040.0)).unwrap();
    features.handle_quote(&quote(GCG27, 4040.0)).unwrap();
    features.handle_quote(&quote(AUD, 0.7)).unwrap();

    assert_eq!(before_the_roll, Some(vec![121.0]));
    assert_eq!(rolled, None);
    assert_eq!(features.values(), Some(vec![1_219_211.0]));
}

/// Lists each kind of tick of each instrument once, and each bar type once, in the order
/// declared.
#[rstest]
fn a_sets_ticks_and_bar_types_gather_over_every_series_of_its_features_each_once() {
    let tables = tables();
    let features = FeatureSet::builder()
        .feature(
            &[bars(AUD_BARS), trades(GBP), quotes(AUD)],
            SEQUENCE,
            &tables,
        )
        .custom(&level_series(AUD), "level", read_level)
        .feature(
            &[
                quotes(AUD),
                ticks(NautilusDataType::OrderBookDepth, AUD),
                bars(AUD_BID_BARS),
                bars(AUD_BARS),
            ],
            SEQUENCE,
            &tables,
        )
        .feature(
            &[ticks(NautilusDataType::OrderBookDelta, GBP)],
            SEQUENCE,
            &tables,
        )
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
            tick(NautilusDataType::OrderBookDepth, AUD),
            tick(NautilusDataType::OrderBookDelta, GBP),
        ]
    );
    assert_eq!(
        features.bar_types(),
        [BarType::from(AUD_BARS), BarType::from(AUD_BID_BARS)]
    );
}

/// A quote of GCZ26 updates both GC and GCZ26, which are both series of the feature. GBP's trade
/// updates GBP. GBP's quote does not, because no feature reads that series.
#[rstest]
fn the_last_updates_gather_over_every_series_of_a_feature() {
    let mut probe = registered_at(before_gcs_roll().into());
    let features = FeatureSet::builder()
        .feature(
            &[quotes(GC), quotes(GCZ26), trades(GBP)],
            SEQUENCE,
            &tables(),
        )
        .build()
        .unwrap();
    features.register(&mut probe);

    features
        .handle_quote(&at!(quote(GCZ26, 4000.0), 10))
        .unwrap();
    features.handle_trade(&at!(trade(GBP, 1.3), 20)).unwrap();
    features.handle_quote(&at!(quote(GBP, 1.3), 30)).unwrap();

    assert_eq!(features.last_update(id(GC)), Some(UnixNanos::from(10)));
    assert_eq!(features.last_update(id(GCZ26)), Some(UnixNanos::from(10)));
    assert_eq!(features.last_update(id(GBP)), Some(UnixNanos::from(20)));
}

/// A feature's name lists all its series, in the order declared.
#[rstest]
fn a_table_feeding_several_features_names_each_after_its_whole_list_of_series() {
    let tables = tables();

    let features = FeatureSet::builder()
        .feature(&[quotes(AUD), trades(GBP), bars(AUD_BARS)], LAST, &tables)
        .feature(&[quotes(AUD)], LAST, &tables)
        .build()
        .unwrap();

    assert_eq!(
        features.names(),
        [
            "QuoteTick of AUD/USD.SIM, TradeTick of GBP/USD.SIM, \
             AUD/USD.SIM-1-MINUTE-MID-INTERNAL:last",
            "QuoteTick of AUD/USD.SIM:last",
        ]
    );
}

/// The fingerprint and names of a feature of each kind on one series match those from before a
/// feature could read a list.
#[rstest]
fn a_single_series_features_names_and_fingerprint_are_unchanged() {
    let tables = tables();
    let width = width_of(Distance::BasisPoints(Decimal::from(10)));
    let features = testing::mid_over(id(GBP), &width)
        .feature(&[bars(AUD_BARS)], LAST, &tables)
        .feature(&[quotes(AUD)], LAST, &tables)
        .feature(&[trades(GBP)], TWO, &tables)
        .feature(&[Series::depth(id(AUD))], BOOK, &tables)
        .feature(&[Series::deltas(id(GBP))], BOOK, &tables)
        .custom(&levels("yields", "US10Y"), "yield", read_level)
        .version(3)
        .build()
        .unwrap();

    assert_eq!(
        features.names(),
        [
            "over",
            "AUD/USD.SIM-1-MINUTE-MID-INTERNAL:last",
            "QuoteTick of AUD/USD.SIM:last",
            "two",
            "OrderBookDepth of AUD/USD.SIM:book",
            "OrderBookDelta of GBP/USD.SIM:book",
            "yield",
        ]
    );
    assert_eq!(
        features.fingerprint().to_string(),
        "96752c0120b5fa0e43aa5614edc25a11f1313a7e83def119f00d001c135e2864"
    );
}

/// Each table feeds one feature, so the name uses that feature alone.
fn listing(last: &[Series], two: &[Series]) -> Fingerprint {
    let tables = tables();
    FeatureSet::builder()
        .feature(last, LAST, &tables)
        .feature(two, TWO, &tables)
        .build()
        .unwrap()
        .fingerprint()
}

/// Adding, removing or moving a series between features rebuilds the dataset and the models.
#[rstest]
fn the_fingerprint_changes_with_a_features_list_of_series() {
    let base = listing(&[quotes(AUD), trades(AUD)], &[quotes(GBP)]);

    assert_eq!(base, listing(&[quotes(AUD), trades(AUD)], &[quotes(GBP)]));
    assert_ne!(base, listing(&[quotes(AUD)], &[quotes(GBP)]));
    assert_ne!(
        base,
        listing(&[quotes(AUD), trades(AUD), bars(AUD_BARS)], &[quotes(GBP)])
    );
    assert_ne!(base, listing(&[quotes(AUD)], &[trades(AUD), quotes(GBP)]));
}

/// Nautilus' exponential average reads no custom data (it has no `Feature::custom_handler`).
/// Declaring it on a custom series is refused. The error names the series and its table.
#[rstest]
fn a_feature_on_a_custom_series_is_refused_when_the_set_is_built() {
    let yields = levels("yields", "US10Y");

    let err = FeatureSet::builder()
        .feature(
            &[quotes(AUD), Series::Custom(yields.clone())],
            LAST,
            &tables(),
        )
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains(&yields.to_string()), "{err}");
    assert!(err.contains("last"), "{err}");
}

#[rstest]
fn a_feature_on_no_series_is_refused_when_the_set_is_built() {
    let err = FeatureSet::builder()
        .feature(&[], LAST, &tables())
        .build()
        .unwrap_err()
        .to_string();

    assert!(err.contains("last"), "{err}");
}

/// The feature reads GBP's mid over the width it is handed. 10 bps of the mid 1.3 is 1000 widths.
#[rstest]
fn a_feature_over_the_bracket_reads_its_list_of_series_over_its_width() {
    let width = width_of(Distance::BasisPoints(Decimal::from(10)));

    let features = FeatureSet::builder()
        .bracket(&width)
        .feature(&[quotes(GBP), quotes(AUD)], testing::OVER, &over_tables())
        .build()
        .unwrap();
    features.handle_quote(&quote(GBP, 1.3)).unwrap();

    let values = features.values().unwrap();
    assert!((values[0] - 1000.0).abs() < 1e-9, "{values:?}");
    assert!(std::ptr::eq(
        features.bracket_width().unwrap(),
        Rc::as_ptr(&width)
    ));
}

fn custom(series: &CustomSeries) -> Series {
    Series::Custom(series.clone())
}

/// The feature reads AUD's quotes and the yields under US10Y. It records 1 6 1 6. Yields with
/// another identifier do not reach it, nor do bonds under US10Y with other metadata.
#[rstest]
fn a_feature_on_a_custom_series_and_a_tick_series_gets_both_in_the_order_they_come() {
    let yields = levels("yields", "US10Y");
    let features = FeatureSet::builder()
        .feature(&[quotes(AUD), custom(&yields)], SEQUENCE, &tables())
        .build()
        .unwrap();

    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_custom(&point(yields.data_type(), 4.25));
    features.handle_custom(&point(levels("yields", "US02Y").data_type(), 3.9));
    features.handle_custom(&point(levels("bonds", "US10Y").data_type(), 4.3));
    features.handle_quote(&quote(AUD, 0.7)).unwrap();
    features.handle_custom(&point(yields.data_type(), 4.5));

    assert_eq!(features.values(), Some(vec![1616.0]));
}

/// The feature reads the yields under the root GC. It reads GCZ26's, not GCG27's or the root's
/// own. The roll resets it (9) and moves it to GCG27's.
#[rstest]
fn a_feature_on_a_roots_custom_series_reads_the_contract_in_front_and_moves_at_its_roll() {
    let start = before_gcs_roll();
    let mut probe = registered_at(start.into());
    let features = FeatureSet::builder()
        .feature(&[custom(&levels("yields", GC))], SEQUENCE, &tables())
        .build()
        .unwrap();
    features.register(&mut probe);

    features.handle_custom(&point(&yields_type(GCZ26), 4.25));
    features.handle_custom(&point(&yields_type(GCG27), 4.3));
    features.handle_custom(&point(&yields_type(GC), 4.2));
    let before_the_roll = features.values();
    features.roll(&Transition::after(id(GC), start).unwrap());
    let rolled = features.values();
    features.handle_custom(&point(&yields_type(GCZ26), 4.5));
    features.handle_custom(&point(&yields_type(GCG27), 4.6));

    assert_eq!(before_the_roll, Some(vec![6.0]));
    assert_eq!(rolled, None);
    assert_eq!(features.values(), Some(vec![696.0]));
}

/// Lists each custom series among a feature's series once, in the order declared. A series that
/// only a raw feature reads is left out, because a raw feature reads the cache.
#[rstest]
fn a_sets_handled_custom_series_are_its_features_not_its_raw_features() {
    let tables = tables();
    let yields = levels("yields", "US10Y");
    let features = FeatureSet::builder()
        .custom(&level_series(AUD), "level", read_level)
        .feature(&[quotes(AUD), custom(&yields)], SEQUENCE, &tables)
        .feature(&[custom(&yields)], SEQUENCE, &tables)
        .build()
        .unwrap();

    assert_eq!(features.handled_custom(), [yields]);
}

/// Lists each custom series that a raw feature or a feature reads (`model::reads`) once, in the
/// order declared. None is a tick series.
#[rstest]
fn a_sets_custom_series_gather_over_its_raw_features_and_the_series_of_its_features() {
    let tables = tables();
    let yields = levels("yields", "US10Y");
    let features = FeatureSet::builder()
        .custom(&level_series(AUD), "level", read_level)
        .feature(
            &[quotes(AUD), custom(&yields), custom(&level_series(AUD))],
            SEQUENCE,
            &tables,
        )
        .feature(&[custom(&yields)], SEQUENCE, &tables)
        .build()
        .unwrap();

    assert_eq!(features.custom(), [level_series(AUD), yields]);
    assert_eq!(
        features.ticks(),
        [TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: id(AUD),
        }]
    );
}

fn reading_custom(series: &CustomSeries) -> Fingerprint {
    FeatureSet::builder()
        .feature(&[custom(series)], SEQUENCE, &tables())
        .build()
        .unwrap()
        .fingerprint()
}

/// Another identifier, metadata or type name rebuilds the dataset and the models. The
/// fingerprint holds the series' whole data type.
#[rstest]
fn the_fingerprint_changes_with_the_custom_series_a_feature_reads() {
    let base = reading_custom(&levels("yields", "US10Y"));

    assert_eq!(base, reading_custom(&levels("yields", "US10Y")));
    assert_ne!(base, reading_custom(&levels("yields", "US02Y")));
    assert_ne!(base, reading_custom(&levels("bonds", "US10Y")));
    assert_ne!(
        base,
        reading_custom(&custom_series("Yield", "yields", "US10Y"))
    );
}
