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
//! The set records, for each instrument, the time (`ts_init`) of the
//! latest quote, trade, bar, depth or deltas a feature read
//! (`last_update`): a decision is fresh only if data of the traded
//! instrument came since the previous one (`decide`). Custom data is of no
//! instrument.
//!
//! A set declares its warmup (`FeatureSetBuilder::warmup`), how much data
//! its features read before a decision, such as the strategy's: a dataset
//! build replays it before its first day (`dataset`).
//!
//! The fingerprint covers what makes the features what they are: their
//! names in order, the strategy's feature code (given to the builder, such
//! as `include_str!("features.rs")`), the warmup, and the shared indicator
//! code (this module, and Nautilus' indicators, named by the source
//! `Cargo.lock` pins).

use std::{
    any::Any,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Display},
    rc::Rc,
};

use anyhow::{bail, ensure};
use jiff::SignedDuration;
use nautilus_common::actor::{
    DataActorNative,
    indicators::{ActorIndicator, SharedActorIndicator},
};
use nautilus_core::UnixNanos;
use nautilus_indicators::indicator::{Indicator, MovingAverage};
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, DataType, OrderBookDeltas, OrderBookDepth, QuoteTick, TradeTick,
    },
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
    /// The time of the latest update of each instrument a feature read.
    updates: RefCell<BTreeMap<InstrumentId, UnixNanos>>,
    warmup: SignedDuration,
    fingerprint: Fingerprint,
}

/// Declares the features of a `FeatureSet`, each with its source and a
/// name, in the order of their values.
pub struct FeatureSetBuilder {
    names: Vec<String>,
    slots: Vec<Slot>,
    code: Vec<&'static str>,
    warmup: SignedDuration,
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
            warmup: SignedDuration::ZERO,
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
    pub fn values(&self) -> Option<Vec<f64>> {
        self.slots.borrow().iter().map(Slot::value).collect()
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
    /// feature code, the warmup and the shared indicator code.
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// How much data the features read before a decision: a dataset build
    /// replays this much before its first day (`dataset`).
    #[must_use]
    pub const fn warmup(&self) -> SignedDuration {
        self.warmup
    }

    /// Whether a feature reads the quotes, trades, bars, depth or deltas
    /// of `instrument_id`.
    #[must_use]
    pub fn reads(&self, instrument_id: InstrumentId) -> bool {
        self.slots
            .borrow()
            .iter()
            .any(|slot| slot.instrument_id() == Some(instrument_id))
    }

    /// The time (`ts_init`) of the latest quote, trade, bar, depth or
    /// deltas of `instrument_id` a feature read, if any: an update no
    /// feature reads, or custom data, is none.
    #[must_use]
    pub fn last_update(&self, instrument_id: InstrumentId) -> Option<UnixNanos> {
        self.updates.borrow().get(&instrument_id).copied()
    }

    /// Records an update of `instrument_id` at `ts_init`, read by a
    /// feature; an older one leaves the latest.
    fn updated(&self, instrument_id: InstrumentId, ts_init: UnixNanos) {
        let mut updates = self.updates.borrow_mut();
        let latest = updates.entry(instrument_id).or_insert(ts_init);
        *latest = (*latest).max(ts_init);
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
                self.updated(*id, depth.ts_init);
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
                self.updated(*id, deltas.ts_init);
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

    /// The instrument whose market data the feature reads; none for
    /// custom data.
    fn instrument_id(&self) -> Option<InstrumentId> {
        match self {
            Self::Quotes(id, _)
            | Self::Trades(id, _)
            | Self::Depth(id, _)
            | Self::Deltas(id, _) => Some(*id),
            Self::Bars(bar_type, _) => Some(bar_type.instrument_id()),
            Self::Data(..) => None,
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
            {
                if let Err(e) = feature.handle_quote(quote) {
                    log::error!("feature {name} on the quote {quote}: {e}");
                }
                self.updated(*id, quote.ts_init);
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
                self.updated(*id, trade.ts_init);
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
                self.updated(bar_type.instrument_id(), bar.ts_init);
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

    /// Adds `code`, the strategy's feature code, to the fingerprint.
    #[must_use]
    pub fn code(mut self, code: &'static str) -> Self {
        self.code.push(code);
        self
    }

    /// Declares how much data the features read before a decision, such
    /// as the strategy's warmup: a dataset build replays this much before
    /// its first day, so that the day's first decisions are those of a
    /// longer build. None unless given.
    #[must_use]
    pub const fn warmup(mut self, warmup: SignedDuration) -> Self {
        self.warmup = warmup;
        self
    }

    /// The feature set, shared.
    ///
    /// # Errors
    ///
    /// Returns an error if no feature is declared, two share a name, or
    /// the warmup is negative.
    pub fn build(self) -> anyhow::Result<Rc<FeatureSet>> {
        ensure!(!self.names.is_empty(), "a feature set needs a feature");
        let mut seen = BTreeSet::new();
        if let Some(name) = self.names.iter().find(|name| !seen.insert(*name)) {
            bail!("two features are named {name}");
        }
        ensure!(
            !self.warmup.is_negative(),
            "the features' warmup {:#} is negative",
            self.warmup
        );
        let warmup = format!("warmup {} ns", self.warmup.as_nanos());
        let fingerprint = self
            .names
            .iter()
            .map(String::as_str)
            .chain(self.code.iter().copied())
            .chain([warmup.as_str()])
            .chain(SHARED_CODE)
            .fold(Fingerprint::default(), |hash, part| {
                hash.with(part.as_bytes())
            });
        Ok(Rc::new(FeatureSet {
            names: self.names,
            slots: RefCell::new(self.slots),
            updates: RefCell::default(),
            warmup: self.warmup,
            fingerprint,
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
}
