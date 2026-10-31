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
//! `handle_data`); no kind takes both paths. A live warmup's quotes and
//! trades reach the set as registered, its depth and deltas through the
//! warmup (`BookIndicator`, `Warmup::on_historical_book_depth` and
//! `on_historical_book_deltas`), its custom data too
//! (`CustomDataIndicator`, `Warmup::on_historical_data`). Bars built from
//! a warmup's quotes, trades or source bars reach only the cache, not the
//! indicators: the strategy's warmup hands them to `handle_bar`
//! (`Warmup::on_historical`, the warmup a model strategy's `ModelHandle`
//! builds).
//!
//! A feature on the ticks or custom data of a futures root (`GC.GLBX`, see
//! `rolls`) reads the contract standing for it: `register` sets it from
//! the clock (`rolls::front`), and the set is registered with Nautilus for
//! that contract's quotes and trades, its depth, deltas and custom data
//! (keyed by the contract's id) matched to it. The set's tick series
//! (`ticks`), custom data among them, are what a `rolls::Roller`
//! subscribes to and a live warmup requests, the model handle's; at the
//! roller's transition, `roll` moves the root's features to the post
//! contract, registered too, and resets them (Nautilus' `Indicator::reset`,
//! a raw feature to no value): their windows never mix two contracts,
//! which trade at different prices, and the set is not initialized until
//! they refill, so decisions wait. Other instruments' features are
//! untouched.
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
//! Nautilus moving average has with no extra code. Custom data of an
//! instrument, which Nautilus' `Indicator` does not handle, feeds a raw
//! feature: the value a function of the strategy reads from it.
//!
//! The set records, for each instrument, the time (`ts_init`) of the
//! latest quote, trade, bar, depth or deltas a feature read
//! (`last_update`), a root's under the root: a decision is fresh only if
//! data of the traded instrument came since the previous one (`decide`).
//! Custom data is no update of its instrument.
//!
//! A set declares its warmup (`FeatureSetBuilder::warmup`), how much data
//! its features read before a decision, such as the strategy's: a dataset
//! build replays it before its first day (`dataset`).
//!
//! The fingerprint covers what makes the features what they are: their
//! names in order, the warmup, and the version of the strategy's feature
//! code (`FeatureSetBuilder::version`), bumped by hand when what it
//! computes changes. The code shared by every set, this module and
//! Nautilus' indicators, is versioned with the dataset (`dataset::VERSION`).
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
    DataActor, DataActorNative,
    indicators::{ActorIndicator, SharedActorIndicator},
};
use nautilus_core::UnixNanos;
use nautilus_indicators::indicator::{Indicator, MovingAverage};
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, DataType, NautilusDataType, OrderBookDeltas, OrderBookDepth,
        QuoteTick, TradeTick,
    },
    enums::OrderSide,
    identifiers::InstrumentId,
};
use sha2::{Digest, Sha256};

use crate::{
    rolls::{Transition, front},
    strategy::{
        TickSeries,
        warmup::{BookIndicator, CustomDataIndicator},
    },
};

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
    Quotes(Reading, Box<dyn Feature>),
    Trades(Reading, Box<dyn Feature>),
    Bars(BarType, Box<dyn Feature>),
    Depth(Reading, Box<dyn Feature>),
    Deltas(Reading, Box<dyn Feature>),
    /// The custom data of a type name, of the instrument read.
    Data(Reading, String, Read, Option<f64>),
}

/// The instrument whose ticks a feature reads, a futures root or a plain
/// instrument, and the contract whose ticks it reads: the one standing for
/// the root, from `register` (`rolls::front`) and each roll (`roll`); the
/// instrument itself if it does not roll.
#[derive(Debug, Clone, Copy)]
struct Reading {
    instrument_id: InstrumentId,
    contract: InstrumentId,
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
    version: u32,
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
            version: 0,
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
    /// feature version and the warmup.
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

    /// The tick series the features read as they are, quotes, trades,
    /// depth, deltas and custom data, each of its declared instrument, a
    /// root or plain, each once, in order: what a `rolls::Roller`
    /// subscribes to and a live warmup requests. Bars are not among them.
    #[must_use]
    pub fn ticks(&self) -> Vec<TickSeries> {
        let mut ticks: Vec<TickSeries> = Vec::new();
        for tick in self.slots.borrow().iter().filter_map(Slot::tick_series) {
            if !ticks.contains(&tick) {
                ticks.push(tick);
            }
        }
        ticks
    }

    /// The bar types the features read, each once, in order: the bars
    /// beside `ticks`.
    #[must_use]
    pub fn bar_types(&self) -> Vec<BarType> {
        let mut bar_types: Vec<BarType> = Vec::new();
        for slot in self.slots.borrow().iter() {
            if let Slot::Bars(bar_type, _) = slot
                && !bar_types.contains(bar_type)
            {
                bar_types.push(*bar_type);
            }
        }
        bar_types
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

    /// Registers the set with `actor`'s Nautilus indicators, from its
    /// `on_start`: each tick or raw feature reads the contract standing for
    /// its instrument now (`rolls::front`), and the set is registered for the
    /// quotes and trades of each such contract and the bars of each bar
    /// type a feature reads. Nautilus registers an indicator once per
    /// source, so a set registered again, on a restart, gets each update
    /// once.
    pub fn register<A: DataActor + DataActorNative>(self: &Rc<Self>, actor: &mut A) {
        let now = actor.clock().utc_now();
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Some(reading) = slot.reading_mut() {
                reading.contract = front(reading.instrument_id, now);
            }
        }
        self.register_contracts(actor);
    }

    /// Moves the features reading a root rolled by `roll` (those on its
    /// pre contract that are not the contract itself, read as a plain
    /// instrument), tick and raw features alike, to its post contract,
    /// registered with Nautilus, and resets them (`Indicator::reset`, a raw
    /// feature to no value): they are cold until they refill on the post
    /// contract, so the set is not initialized and decisions wait. The
    /// other features are untouched. From the roller's transition
    /// (`rolls::Roller::on_time_event`).
    pub fn roll<A: DataActorNative>(self: &Rc<Self>, actor: &mut A, roll: &Transition) {
        for slot in self.slots.borrow_mut().iter_mut() {
            slot.roll(roll);
        }
        self.register_contracts(actor);
    }

    /// Registers the set with `actor`'s Nautilus indicators for the quotes
    /// and trades of the contract each such feature reads and the bars of
    /// each bar type.
    fn register_contracts<A: DataActorNative>(self: &Rc<Self>, actor: &mut A) {
        let core = actor.core_mut();
        for slot in self.slots.borrow().iter() {
            let shared = Rc::clone(self) as SharedActorIndicator;
            match slot {
                Slot::Quotes(reading, _) => {
                    core.register_indicator_for_quote_ticks(reading.contract, shared);
                }
                Slot::Trades(reading, _) => {
                    core.register_indicator_for_trade_ticks(reading.contract, shared);
                }
                Slot::Bars(bar_type, _) => core.register_indicator_for_bars(*bar_type, shared),
                Slot::Depth(..) | Slot::Deltas(..) | Slot::Data(..) => {}
            }
        }
    }
}

/// The strategy's side of the set for books, which Nautilus routes to no
/// indicator: the strategy forwards its live depth and deltas, and its
/// warmup those of its responses (`Warmup::feeding_books`).
impl BookIndicator for FeatureSet {
    /// Feeds `depth` to the features declared on its instrument's depth,
    /// or on the depth of the root it stands for.
    fn handle_depth(&self, depth: &OrderBookDepth) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Depth(reading, feature) = slot
                && reading.contract == depth.instrument_id
            {
                feature.handle_depth(depth);
                self.updated(reading.instrument_id, depth.ts_init);
            }
        }
    }

    /// Feeds `deltas` to the features declared on its instrument's deltas,
    /// or on the deltas of the root it stands for.
    fn handle_deltas(&self, deltas: &OrderBookDeltas) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Deltas(reading, feature) = slot
                && reading.contract == deltas.instrument_id
            {
                feature.handle_deltas(deltas);
                self.updated(reading.instrument_id, deltas.ts_init);
            }
        }
    }
}

/// The strategy's side of the set for custom data, which Nautilus routes to
/// no indicator: the strategy forwards its live custom data, and its warmup
/// that of its responses (`Warmup::feeding_custom_data`).
impl CustomDataIndicator for FeatureSet {
    /// Feeds `data` to the raw features declared on its data type.
    fn handle_data(&self, data: &CustomData) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Data(reading, type_name, read, value) = slot
                && DataType::new(type_name, None, Some(reading.contract.to_string()))
                    == data.data_type
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
            Self::Data(.., value) => *value,
        }
    }

    /// What a tick feature or a raw feature reads; none for bars.
    fn reading_mut(&mut self) -> Option<&mut Reading> {
        match self {
            Self::Quotes(reading, _)
            | Self::Trades(reading, _)
            | Self::Depth(reading, _)
            | Self::Deltas(reading, _)
            | Self::Data(reading, ..) => Some(reading),
            Self::Bars(..) => None,
        }
    }

    /// Moves the feature to `roll`'s post contract and resets it, if it
    /// reads the root on the pre contract (see `FeatureSet::roll`).
    fn roll(&mut self, roll: &Transition) {
        let Some(reading) = self.reading_mut() else {
            return;
        };
        if reading.contract == roll.pre && reading.instrument_id != roll.pre {
            reading.contract = roll.post;
            self.reset();
        }
    }

    /// Makes the feature cold: an indicator reset (`Indicator::reset`), a
    /// raw feature without a value.
    fn reset(&mut self) {
        match self {
            Self::Quotes(_, feature)
            | Self::Trades(_, feature)
            | Self::Bars(_, feature)
            | Self::Depth(_, feature)
            | Self::Deltas(_, feature) => feature.reset(),
            Self::Data(.., value) => *value = None,
        }
    }

    /// The tick series a tick feature or a raw feature reads, of its
    /// declared instrument; none for bars.
    fn tick_series(&self) -> Option<TickSeries> {
        let (data, reading) = match self {
            Self::Quotes(reading, _) => (NautilusDataType::QuoteTick, reading),
            Self::Trades(reading, _) => (NautilusDataType::TradeTick, reading),
            Self::Depth(reading, _) => (NautilusDataType::OrderBookDepth, reading),
            Self::Deltas(reading, _) => (NautilusDataType::OrderBookDelta, reading),
            Self::Data(reading, type_name, ..) => (
                NautilusDataType::Custom {
                    type_name: type_name.clone(),
                },
                reading,
            ),
            Self::Bars(..) => return None,
        };
        Some(TickSeries {
            data,
            instrument_id: reading.instrument_id,
        })
    }
}

impl Reading {
    /// `instrument_id` read, on itself until `register` sets the contract
    /// standing for it.
    const fn of(instrument_id: InstrumentId) -> Self {
        Self {
            instrument_id,
            contract: instrument_id,
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
            if let Slot::Quotes(reading, feature) = slot
                && reading.contract == quote.instrument_id
            {
                if let Err(e) = feature.handle_quote(quote) {
                    log::error!("feature {name} on the quote {quote}: {e}");
                }
                self.updated(reading.instrument_id, quote.ts_init);
            }
        }
        Ok(())
    }

    fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Trades(reading, feature) = slot
                && reading.contract == trade.instrument_id
            {
                feature.handle_trade(trade);
                self.updated(reading.instrument_id, trade.ts_init);
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
        self.declare(
            name,
            Slot::Quotes(Reading::of(instrument_id), Box::new(feature)),
        )
    }

    /// Declares `feature`, named `name`, on the trades of `instrument_id`.
    #[must_use]
    pub fn trades(
        self,
        instrument_id: InstrumentId,
        name: &str,
        feature: impl Feature + 'static,
    ) -> Self {
        self.declare(
            name,
            Slot::Trades(Reading::of(instrument_id), Box::new(feature)),
        )
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
        self.declare(
            name,
            Slot::Depth(Reading::of(instrument_id), Box::new(feature)),
        )
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
        self.declare(
            name,
            Slot::Deltas(Reading::of(instrument_id), Box::new(feature)),
        )
    }

    /// Declares a raw feature, named `name`, on the custom data
    /// `type_name` of `instrument_id` (see `strategy::TickSeries`): its
    /// value is the last `read` of a data point, initialized by the first.
    #[must_use]
    pub fn data(
        self,
        type_name: &str,
        instrument_id: InstrumentId,
        name: &str,
        read: impl Fn(&CustomData) -> f64 + 'static,
    ) -> Self {
        self.declare(
            name,
            Slot::Data(
                Reading::of(instrument_id),
                type_name.to_string(),
                Box::new(read),
                None,
            ),
        )
    }

    /// Declares a rule primary's side and probability, `PRIMARY_FEATURES`,
    /// after the other features, whatever the order of the calls.
    #[must_use]
    pub fn primary(mut self) -> Self {
        self.primary = true;
        self
    }

    /// Sets the version of the strategy's feature code, in the
    /// fingerprint: bumped by hand whenever what the features compute
    /// changes, which then rebuilds the strategy's dataset and models. 0
    /// unless given.
    #[must_use]
    pub const fn version(mut self, version: u32) -> Self {
        self.version = version;
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
        let version = format!("version {}", self.version);
        let warmup = format!("warmup {} ns", self.warmup.as_nanos());
        let fingerprint = self
            .names
            .iter()
            .map(String::as_str)
            .chain([version.as_str(), warmup.as_str()])
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
