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
//! `Cargo.lock` pins). The tests of a hashed module live in its own
//! `tests.rs`, so editing one changes no fingerprint.
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
    /// The time of the latest update of each instrument a feature read.
    updates: RefCell<BTreeMap<InstrumentId, UnixNanos>>,
    warmup: SignedDuration,
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
    warmup: SignedDuration,
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
            warmup: SignedDuration::ZERO,
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
    pub fn build(mut self) -> anyhow::Result<Rc<FeatureSet>> {
        ensure!(!self.names.is_empty(), "a feature set needs a feature");
        if self.primary {
            self.names.extend(PRIMARY_FEATURES.map(String::from));
        }
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
mod tests;
