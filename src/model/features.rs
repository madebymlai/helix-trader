//! A strategy's features: Nautilus indicators, each declared once with its
//! name, its data kind and its source, fed by Nautilus whatever the kind.
//!
//! The set is one Nautilus indicator (`ActorIndicator`), shared between
//! Nautilus and the strategy as an `Rc` (Nautilus' registration takes a
//! shared indicator whose handlers borrow it immutably, so the features sit
//! in a `RefCell`). `register` registers it with Nautilus once per
//! instrument and kind for quotes and trades, once per bar type for bars:
//! the data engine then hands it each quote, trade and bar before the
//! strategy's own handler, live, replayed, and in the responses to
//! requests (a warmup's quotes), with no strategy code. Nautilus routes no
//! depth, book deltas or custom data to indicators, so the strategy
//! forwards those from its handlers (`handle_depth`, `handle_deltas`,
//! `handle_data`); no kind takes both paths. Bars built from a warmup's
//! quotes or source bars reach only the cache, not the indicators: a
//! strategy warming bar features hands `warmup::bars` to `handle_bar`.
//!
//! Each update reaches only the features declared on its kind and source.
//! An update of a kind or source no feature reads is a no-op, so the
//! panicking defaults of Nautilus' `Indicator` trait are never called. The
//! set never returns an error: Nautilus stops handling a quote, trade or
//! bar at an indicator's error, before the strategy's handler, so an error
//! of one feature (an indicator reading the last price of a quote) is
//! logged and the update goes on.
//!
//! The names and their order are fixed when the set is built; `values`
//! snapshots the features in that order, once every one is initialized.
//! A feature exposes its current value through `Feature`, which every
//! Nautilus moving average has with no extra code. Custom data, which
//! Nautilus' `Indicator` does not handle, feeds a raw feature: the value a
//! function of the strategy reads from it.
//!
//! The fingerprint covers what makes the features what they are: their
//! names in order, the strategy's feature code (given to the builder, such
//! as `include_str!("features.rs")`) and the shared indicator code (this
//! module, and Nautilus' indicators, named by the source `Cargo.lock` pins).
//!
//! A strategy whose primary is a rule of its own declares the primary's
//! side and probability as the set's last two values (`primary`): the
//! model handle gives them at each decision on a side (`on_side`), the
//! side 1 a buy and -1 a sell, the probability missing (NaN) for a rule;
//! without a side the set has no values, so the strategy decides on
//! nothing else.

use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::BTreeSet,
    fmt::{self, Debug, Display},
    rc::Rc,
};

use anyhow::{bail, ensure};
use nautilus_common::actor::{
    DataActorNative,
    indicators::{ActorIndicator, SharedActorIndicator},
};
use nautilus_indicators::indicator::{Indicator, MovingAverage};
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, DataType, OrderBookDeltas, OrderBookDepth, QuoteTick, TradeTick,
    },
    enums::OrderSide,
    identifiers::InstrumentId,
};
use sha2::{Digest, Sha256};

/// The code every feature set runs on besides the strategy's own: this
/// module, which feeds the features, and Nautilus' indicators, named by
/// their source, a git commit (`build.rs`).
const SHARED_CODE: [&str; 2] = [
    include_str!("features.rs"),
    env!("NAUTILUS_INDICATORS_SOURCE"),
];

/// The names of a primary's side and probability, the last values of a
/// rule primary's set and of a meta model's rows.
pub const PRIMARY_FEATURES: [&str; 2] = ["primary_side", "primary_probability"];

/// The values of a primary's `side` and `probability`, in the order of
/// `PRIMARY_FEATURES`: the side 1 a buy, -1 a sell.
#[must_use]
pub fn primary_values(side: OrderSide, probability: f64) -> [f64; 2] {
    let side = if side == OrderSide::Buy { 1.0 } else { -1.0 };
    [side, probability]
}

/// A feature: a Nautilus indicator exposing its current value.
pub trait Feature: Indicator {
    /// The current value, read once the indicator is initialized.
    fn value(&self) -> f64;
}

impl<T: MovingAverage> Feature for T {
    fn value(&self) -> f64 {
        MovingAverage::value(self)
    }
}

/// How a raw feature reads its value from a custom data point.
type Read = Box<dyn Fn(&CustomData) -> f64>;

/// A declared feature: its source, and the indicator, or the raw value,
/// fed from it.
enum Slot {
    Quotes(InstrumentId, Box<dyn Feature>),
    Trades(InstrumentId, Box<dyn Feature>),
    Bars(BarType, Box<dyn Feature>),
    Depth(InstrumentId, Box<dyn Feature>),
    Deltas(InstrumentId, Box<dyn Feature>),
    Data(DataType, Read, Option<f64>),
}

/// A strategy's features, built once with `FeatureSet::builder`.
pub struct FeatureSet {
    names: Vec<String>,
    slots: RefCell<Vec<Slot>>,
    fingerprint: Fingerprint,
    /// A rule primary's side and probability, while a decision on a side
    /// is made; none if the set declares no primary.
    primary: Option<Cell<Option<[f64; 2]>>>,
}

/// Declares the features of a `FeatureSet`, each with its source and a
/// name, in the order of their values.
pub struct FeatureSetBuilder {
    names: Vec<String>,
    slots: Vec<Slot>,
    code: Vec<&'static str>,
    primary: bool,
}

/// A SHA-256 hash of what makes rows, models and decisions comparable:
/// equal fingerprints, same features and decisions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Fingerprint([u8; 32]);

impl FeatureSet {
    /// A builder with no feature declared.
    #[must_use]
    pub fn builder() -> FeatureSetBuilder {
        FeatureSetBuilder {
            names: vec![],
            slots: vec![],
            code: vec![],
            primary: false,
        }
    }

    /// The names of the features, in the order of their values.
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The value of each feature, in the order of the names, if every
    /// feature is initialized.
    #[must_use]
    /// A rule primary's set has values only on a side (`on_side`).
    pub fn values(&self) -> Option<Vec<f64>> {
        let mut values: Vec<f64> = self
            .slots
            .borrow()
            .iter()
            .map(Slot::value)
            .collect::<Option<_>>()?;
        if let Some(primary) = &self.primary {
            values.extend(primary.get()?);
        }
        Some(values)
    }

    /// Whether the set declares a rule primary's side and probability.
    #[must_use]
    pub const fn has_primary(&self) -> bool {
        self.primary.is_some()
    }

    /// Runs `decide` with the primary's `side` and `probability` as the
    /// set's last values, then takes them away.
    ///
    /// # Panics
    ///
    /// Panics if the set declares no primary.
    pub fn on_side<T>(&self, side: OrderSide, probability: f64, decide: impl FnOnce() -> T) -> T {
        let primary = self
            .primary
            .as_ref()
            .expect("a set deciding on a side declares the primary");
        primary.set(Some(primary_values(side, probability)));
        let decided = decide();
        primary.set(None);
        decided
    }

    /// Whether every feature is initialized.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.slots
            .borrow()
            .iter()
            .all(|slot| slot.value().is_some())
    }

    /// The fingerprint of the feature names in order, the strategy's
    /// feature code and the shared indicator code.
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// Registers the set with `actor`'s Nautilus indicators: for the
    /// quotes and trades of each instrument and the bars of each bar type
    /// a feature reads. Nautilus registers an indicator once per source, so
    /// a set registered again, on a restart, gets each update once.
    pub fn register<A: DataActorNative>(self: &Rc<Self>, actor: &mut A) {
        let core = actor.core_mut();
        for slot in self.slots.borrow().iter() {
            let shared = Rc::clone(self) as SharedActorIndicator;
            match slot {
                Slot::Quotes(id, _) => core.register_indicator_for_quote_ticks(*id, shared),
                Slot::Trades(id, _) => core.register_indicator_for_trade_ticks(*id, shared),
                Slot::Bars(bar_type, _) => core.register_indicator_for_bars(*bar_type, shared),
                Slot::Depth(..) | Slot::Deltas(..) | Slot::Data(..) => {}
            }
        }
    }

    /// Feeds `depth` to the features declared on its instrument's depth.
    pub fn handle_depth(&self, depth: &OrderBookDepth) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Depth(id, feature) = slot
                && *id == depth.instrument_id
            {
                feature.handle_depth(depth);
            }
        }
    }

    /// Feeds `deltas` to the features declared on its instrument's deltas.
    pub fn handle_deltas(&self, deltas: &OrderBookDeltas) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Deltas(id, feature) = slot
                && *id == deltas.instrument_id
            {
                feature.handle_deltas(deltas);
            }
        }
    }

    /// Feeds `data` to the raw features declared on its data type.
    pub fn handle_data(&self, data: &CustomData) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Data(data_type, read, value) = slot
                && *data_type == data.data_type
            {
                *value = Some(read(data));
            }
        }
    }
}

impl Slot {
    /// The feature's value, if it is initialized.
    fn value(&self) -> Option<f64> {
        match self {
            Self::Quotes(_, feature)
            | Self::Trades(_, feature)
            | Self::Bars(_, feature)
            | Self::Depth(_, feature)
            | Self::Deltas(_, feature) => feature.initialized().then(|| feature.value()),
            Self::Data(_, _, value) => *value,
        }
    }
}

impl Debug for FeatureSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureSet")
            .field("names", &self.names)
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

/// Nautilus' side of the set. Its handlers never fail: a feature's error
/// is logged, and the other features are fed.
impl ActorIndicator for FeatureSet {
    fn key(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn initialized(&self) -> anyhow::Result<bool> {
        Ok(FeatureSet::initialized(self))
    }

    fn handle_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        let mut slots = self.slots.borrow_mut();
        for (slot, name) in slots.iter_mut().zip(&self.names) {
            if let Slot::Quotes(id, feature) = slot
                && *id == quote.instrument_id
                && let Err(e) = feature.handle_quote(quote)
            {
                log::error!("feature {name} on the quote {quote}: {e}");
            }
        }
        Ok(())
    }

    fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Trades(id, feature) = slot
                && *id == trade.instrument_id
            {
                feature.handle_trade(trade);
            }
        }
        Ok(())
    }

    /// Nautilus routes a bar by instrument and specification; a feature
    /// reads the bars of its bar type alone, of the same aggregation source.
    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Bars(bar_type, feature) = slot
                && bar_type.standard() == bar.bar_type.standard()
            {
                feature.handle_bar(bar);
            }
        }
        Ok(())
    }
}

impl FeatureSetBuilder {
    /// Declares `feature`, named `name`, on the quotes of `instrument_id`.
    #[must_use]
    pub fn quotes(
        self,
        instrument_id: InstrumentId,
        name: &str,
        feature: impl Feature + 'static,
    ) -> Self {
        self.declare(name, Slot::Quotes(instrument_id, Box::new(feature)))
    }

    /// Declares `feature`, named `name`, on the trades of `instrument_id`.
    #[must_use]
    pub fn trades(
        self,
        instrument_id: InstrumentId,
        name: &str,
        feature: impl Feature + 'static,
    ) -> Self {
        self.declare(name, Slot::Trades(instrument_id, Box::new(feature)))
    }

    /// Declares `feature`, named `name`, on the bars of `bar_type`.
    #[must_use]
    pub fn bars(self, bar_type: BarType, name: &str, feature: impl Feature + 'static) -> Self {
        self.declare(name, Slot::Bars(bar_type, Box::new(feature)))
    }

    /// Declares `feature`, named `name`, on the book depth of
    /// `instrument_id`.
    #[must_use]
    pub fn depth(
        self,
        instrument_id: InstrumentId,
        name: &str,
        feature: impl Feature + 'static,
    ) -> Self {
        self.declare(name, Slot::Depth(instrument_id, Box::new(feature)))
    }

    /// Declares `feature`, named `name`, on the book deltas of
    /// `instrument_id`.
    #[must_use]
    pub fn deltas(
        self,
        instrument_id: InstrumentId,
        name: &str,
        feature: impl Feature + 'static,
    ) -> Self {
        self.declare(name, Slot::Deltas(instrument_id, Box::new(feature)))
    }

    /// Declares a raw feature, named `name`, on the custom data of
    /// `data_type`: its value is the last `read` of a data point,
    /// initialized by the first.
    #[must_use]
    pub fn data(
        self,
        data_type: DataType,
        name: &str,
        read: impl Fn(&CustomData) -> f64 + 'static,
    ) -> Self {
        self.declare(name, Slot::Data(data_type, Box::new(read), None))
    }

    /// Declares a rule primary's side and probability, `PRIMARY_FEATURES`,
    /// after the other features, whatever the order of the calls.
    #[must_use]
    pub fn primary(mut self) -> Self {
        self.primary = true;
        self
    }

    /// Adds `code`, the strategy's feature code, to the fingerprint.
    #[must_use]
    pub fn code(mut self, code: &'static str) -> Self {
        self.code.push(code);
        self
    }

    /// The feature set, shared.
    ///
    /// # Errors
    ///
    /// Returns an error if no feature is declared, or two share a name.
    pub fn build(mut self) -> anyhow::Result<Rc<FeatureSet>> {
        ensure!(!self.names.is_empty(), "a feature set needs a feature");
        if self.primary {
            self.names.extend(PRIMARY_FEATURES.map(String::from));
        }
        let mut seen = BTreeSet::new();
        if let Some(name) = self.names.iter().find(|name| !seen.insert(*name)) {
            bail!("two features are named {name}");
        }
        let fingerprint = self
            .names
            .iter()
            .map(String::as_str)
            .chain(self.code.iter().copied())
            .chain(SHARED_CODE)
            .fold(Fingerprint::default(), |hash, part| {
                hash.with(part.as_bytes())
            });
        Ok(Rc::new(FeatureSet {
            names: self.names,
            slots: RefCell::new(self.slots),
            fingerprint,
            primary: self.primary.then(Cell::default),
        }))
    }

    fn declare(mut self, name: &str, slot: Slot) -> Self {
        self.names.push(name.to_string());
        self.slots.push(slot);
        self
    }
}

impl Fingerprint {
    /// This fingerprint extended with `part`: a change to either changes
    /// the result. The part's length goes in first, so that parts are told
    /// apart, not concatenated.
    #[must_use]
    pub fn with(self, part: &[u8]) -> Self {
        let hash = Sha256::new()
            .chain_update(self.0)
            .chain_update((part.len() as u64).to_le_bytes())
            .chain_update(part)
            .finalize();
        Self(hash.into())
    }
}

impl Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, sync::Arc};

    use nautilus_common::{actor::DataActor, cache::Cache, clock::VirtualClock};
    use nautilus_core::UnixNanos;
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
}
