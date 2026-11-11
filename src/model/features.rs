//! A strategy's features: Nautilus indicators, each declared once with its
//! data kind and its source, fed by Nautilus whatever the kind.
//!
//! A feature is built of a declared indicator's table in `[indicators]`
//! (`strategy::Declared`, `IndicatorsConfig`), never by hand: the builder
//! takes the declaration and the tables, builds the indicator, names the
//! feature and carries the table's arguments into the fingerprint. A
//! feature is named after its table (`fast`) when the table feeds that
//! feature alone, and after the series it reads and its table
//! (`XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL:fast`, `QuoteTick of
//! XAUUSD.ICMARKETS:fast`) when the table feeds several. A raw feature of
//! custom data has no table, and is named by the strategy.
//!
//! The set is one Nautilus indicator (`ActorIndicator`), shared between
//! Nautilus, the strategy's warmup and the strategy as an `Rc` (Nautilus'
//! registration takes a shared indicator whose handlers borrow it
//! immutably, so the features sit in a `RefCell`). `register` registers it
//! with Nautilus once per bar type for bars: the data engine then hands it
//! each bar before the strategy's own handler, live and replayed, with no
//! strategy code. Its quotes and trades come through the warmup instead
//! (`Warmup::feeding_ticks`, the warmup a model strategy's `ModelHandle`
//! builds): Nautilus hands a registered indicator each live tick as it
//! comes, while a live warmup's older windows are still being answered, so
//! the warmup hands the set the quotes and trades of its responses and the
//! live ones the strategy hands it, these held until it ends
//! (`strategy::warmup`'s `hold`, which goes once Nautilus holds them, the
//! set then registered for quotes and trades again); in a replay, as they
//! come. Nautilus routes no depth, book deltas or custom data to
//! indicators either: a live warmup hands the set its own (`BookIndicator`,
//! `Warmup::on_historical_book_depth` and `on_historical_book_deltas`;
//! `CustomDataIndicator`, `Warmup::on_historical_data`), and no strategy
//! hands it live or replayed ones, none declaring a feature on them, which
//! would read a live warmup's alone. Bars built from a warmup's quotes,
//! trades or source bars reach only the cache, not the indicators: the
//! strategy's warmup hands them to `handle_bar` (`Warmup::on_data_response`).
//!
//! A feature on the ticks or custom data of a futures root (`GC.GLBX`, see
//! `rolls`) reads the contract standing for it: `register` sets it from
//! the clock (`rolls::front`), and that contract's quotes, trades, depth,
//! deltas and custom data (keyed by the contract's id) are matched to it.
//! The set's tick series (`ticks`), custom data among them, are what a
//! `rolls::Roller` subscribes to and a live warmup requests, the model
//! handle's; at the roller's transition, `roll` moves the root's features
//! to the post contract and resets them (Nautilus' `Indicator::reset`, a
//! raw feature to no value): their windows never mix two contracts, which
//! trade at different prices, and the set is not initialized until they
//! refill, so decisions wait. Other instruments' features are untouched.
//!
//! Each update reaches only the features declared on its kind and source.
//! An update of a kind or source no feature reads is a no-op, so the
//! panicking defaults of Nautilus' `Indicator` trait are never called. The
//! set never returns an error: Nautilus stops handling a bar at an
//! indicator's error, before the strategy's handler, and the warmup its
//! hand-over of ticks, so an error of one feature (an indicator reading the
//! last price of a quote) is logged and the update goes on.
//!
//! The names and their order are fixed when the set is built; `values`
//! snapshots the features in that order, once every one is initialized. A
//! side model reads them, then its side's payoff features, which the target
//! names (`side_names`, `side_row`); a meta model reads them, then the side
//! picked and its probability (`meta_names`, `meta_row`).
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
//! A feature over the target's bracket (`OverBracket`, declared with
//! `FeatureSetBuilder::over_bracket`) reads quotes, and is handed the
//! bracket's width (`targets::BracketWidth`) as it is built: the width at
//! the decision its value is for, the decision its quotes are read at, and
//! the target's time limit. The set's features read one width, which the
//! model handle has the target's setter run on (`bracket_width`), so the
//! width's ATR is the target's, with no series of the strategy's. The width
//! is the target's, in the fingerprint of a dataset beside the set's
//! (`targets::fingerprint`).
//!
//! The fingerprint covers what makes the features what they are: their
//! names in order, the series each reads (which the name of a feature
//! named after its table alone, or of a raw feature, does not tell), the
//! arguments of each table they are built from by value
//! (`IndicatorsConfig::canonical_args`: parsed, so however the TOML spells
//! them), the warmup, and the version of the strategy's feature code
//! (`FeatureSetBuilder::version`), bumped by hand when what it computes
//! changes. An indicator the strategy builds of `[indicators]` for its own
//! logic is not in it. The code shared by every set, this module and
//! Nautilus' indicators, is versioned with the dataset
//! (`dataset::VERSION`).

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

use super::targets::BracketWidth;
use crate::{
    rolls::{Transition, front},
    strategy::{
        Declaration, Declared, IndicatorArgs, IndicatorsConfig, TickSeries,
        warmup::{BookIndicator, CustomDataIndicator},
    },
};

/// The names of a side model's features: the strategy's `names`, then the
/// target's `payoff` names (`targets::TargetConfig::payoff_names`), in the
/// order of `side_row`.
#[must_use]
pub fn side_names(names: &[String], payoff: &[String]) -> Vec<String> {
    names.iter().chain(payoff).cloned().collect()
}

/// A side model's row: the decision's `features`, then its side's `payoff`
/// features (`targets::SideTarget::payoff`), in the order of `side_names`.
#[must_use]
pub fn side_row(features: &[f64], payoff: &[f64]) -> Vec<f64> {
    features.iter().chain(payoff).copied().collect()
}

/// The names of the side picked and its probability, the last values of
/// a meta model's rows.
pub const PICKED_FEATURES: [&str; 2] = ["picked_side", "picked_probability"];

/// The values of the `side` picked and its `probability`, in the order of
/// `PICKED_FEATURES`: the side 1 a buy, -1 a sell.
fn picked_values(side: OrderSide, probability: f64) -> [f64; 2] {
    let side = if side == OrderSide::Buy { 1.0 } else { -1.0 };
    [side, probability]
}

/// The names of a meta model's features, of its sides' `names`: those,
/// then the side picked and its probability (`PICKED_FEATURES`).
#[must_use]
pub fn meta_names(names: &[String]) -> Vec<String> {
    names
        .iter()
        .cloned()
        .chain(PICKED_FEATURES.map(String::from))
        .collect()
}

/// A meta model's row: the sides' `features`, then the `side` picked and
/// its `probability`, in the order of `meta_names`.
#[must_use]
pub fn meta_row(features: &[f64], side: OrderSide, probability: f64) -> Vec<f64> {
    features
        .iter()
        .copied()
        .chain(picked_values(side, probability))
        .collect()
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

/// A feature formed over the target's bracket: its value at a decision is
/// over the bracket's width there, which it reads from the width it is
/// handed as it is built (`FeatureSetBuilder::over_bracket`).
pub trait OverBracket: Feature {
    /// Reads its values over `width` from now on.
    fn over(&mut self, width: Rc<BracketWidth>);
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
    /// The width the features over the bracket read, if any.
    width: Option<Rc<BracketWidth>>,
    /// The time of the latest update of each instrument a feature read.
    updates: RefCell<BTreeMap<InstrumentId, UnixNanos>>,
    warmup: SignedDuration,
    fingerprint: Fingerprint,
}

/// Declares the features of a `FeatureSet`, each with its source, in the
/// order of their values: a feature of a declared indicator's table, or a
/// raw feature of custom data. An indicator is never handed in built:
///
/// ```
/// # use helix_trader::{model::features::FeatureSet, strategy::{Declared, EmaArgs, IndicatorsConfig}};
/// # use nautilus_model::identifiers::InstrumentId;
/// const MID: Declared<EmaArgs> = Declared::new("mid");
/// let indicators: IndicatorsConfig =
///     toml::from_str(r#"mid = { period = 1, price_type = "MID" }"#).unwrap();
///
/// let features = FeatureSet::builder()
///     .quotes(InstrumentId::from("AUD/USD.SIM"), MID, &indicators)
///     .build()
///     .unwrap();
///
/// assert_eq!(features.names(), ["mid"]);
/// ```
///
/// ```compile_fail
/// # use helix_trader::model::features::FeatureSet;
/// # use nautilus_indicators::average::ema::ExponentialMovingAverage;
/// # use nautilus_model::identifiers::InstrumentId;
/// let features = FeatureSet::builder()
///     .quotes(
///         InstrumentId::from("AUD/USD.SIM"),
///         "mid",
///         ExponentialMovingAverage::new(1, None),
///     )
///     .build();
/// ```
pub struct FeatureSetBuilder {
    names: Vec<Name>,
    slots: Vec<Slot>,
    /// The width the features over the bracket read, if any.
    width: Option<Rc<BracketWidth>>,
    /// The arguments of each table a feature is built from, by its name,
    /// in canonical form (`IndicatorsConfig::canonical_args`).
    arguments: BTreeMap<&'static str, String>,
    version: u32,
    warmup: SignedDuration,
}

/// How a declared feature is named (see `FeatureSetBuilder::build`).
enum Name {
    /// After the `[indicators]` table it is built from.
    Table(&'static str),
    /// As the strategy gives it: a raw feature's.
    Given(String),
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
            width: None,
            arguments: BTreeMap::new(),
            version: 0,
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

    /// The fingerprint of the feature names in order, the arguments of
    /// their tables, the strategy's feature version and the warmup.
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
    /// bars of each bar type a feature reads (`register_contracts`). Nautilus
    /// registers an indicator once per source, so a set registered again, on
    /// a restart, gets each bar once.
    pub fn register<A: DataActor + DataActorNative>(self: &Rc<Self>, actor: &mut A) {
        let now = actor.clock().utc_now();
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Some(reading) = slot.reading_mut() {
                reading.contract = front(reading.instrument_id, now);
            }
        }
        self.register_contracts(actor);
    }

    /// The width of the target's bracket its features read, if any: the
    /// one the target's setter is to run on (`TargetSetter::sharing`).
    pub(super) fn bracket_width(&self) -> Option<&BracketWidth> {
        self.width.as_deref()
    }

    /// Moves the features reading a root rolled by `roll` (those on its
    /// pre contract that are not the contract itself, read as a plain
    /// instrument), tick and raw features alike, to its post contract, and
    /// resets them (`Indicator::reset`, a raw
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

    /// Registers the set with `actor`'s Nautilus indicators for the bars of
    /// each bar type a feature reads. Not for quotes and trades: the warmup
    /// hands it those (`Warmup::feeding_ticks`), holding the live ones while
    /// it is pending, which Nautilus does not do for an indicator.
    fn register_contracts<A: DataActorNative>(self: &Rc<Self>, actor: &mut A) {
        let core = actor.core_mut();
        for slot in self.slots.borrow().iter() {
            let shared = Rc::clone(self) as SharedActorIndicator;
            match slot {
                Slot::Bars(bar_type, _) => core.register_indicator_for_bars(*bar_type, shared),
                Slot::Quotes(..)
                | Slot::Trades(..)
                | Slot::Depth(..)
                | Slot::Deltas(..)
                | Slot::Data(..) => {}
            }
        }
    }
}

/// The warmup's side of the set for books, which Nautilus routes to no
/// indicator: a live warmup hands it those of its responses
/// (`Warmup::feeding_books`); no strategy hands it live ones (see the
/// module).
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

/// The warmup's side of the set for custom data, which Nautilus routes to
/// no indicator: a live warmup hands it that of its responses
/// (`Warmup::feeding_custom_data`); no strategy hands it live data (see the
/// module).
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

    /// The series the feature reads, as a feature's name and the
    /// fingerprint have it: the bar type of bars, the tick series of
    /// ticks (`QuoteTick of XAUUSD.ICMARKETS`), a root's as the root.
    fn series(&self) -> String {
        match self {
            Self::Bars(bar_type, _) => bar_type.to_string(),
            slot => slot
                .tick_series()
                .expect("a feature not on bars reads ticks")
                .to_string(),
        }
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

/// Nautilus' side of the set, for its bars, and the warmup's, for its quotes
/// and trades (`Warmup::feeding_ticks`). Its handlers never fail: a
/// feature's error is logged, and the other features are fed.
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
    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the quotes of `instrument_id`.
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused,
    /// which the load refuses of an indicator the strategy declares
    /// (`Part::INDICATORS`).
    #[must_use]
    pub fn quotes<A>(
        self,
        instrument_id: InstrumentId,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: Feature + 'static,
    {
        self.table(declared, indicators, |feature| {
            Slot::Quotes(Reading::of(instrument_id), feature)
        })
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the trades of `instrument_id`.
    ///
    /// # Panics
    ///
    /// As `quotes`.
    #[must_use]
    pub fn trades<A>(
        self,
        instrument_id: InstrumentId,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: Feature + 'static,
    {
        self.table(declared, indicators, |feature| {
            Slot::Trades(Reading::of(instrument_id), feature)
        })
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the bars of `bar_type`.
    ///
    /// # Panics
    ///
    /// As `quotes`.
    #[must_use]
    pub fn bars<A>(
        self,
        bar_type: BarType,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: Feature + 'static,
    {
        self.table(declared, indicators, |feature| {
            Slot::Bars(bar_type, feature)
        })
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the book depth of `instrument_id`.
    ///
    /// # Panics
    ///
    /// As `quotes`.
    #[must_use]
    pub fn depth<A>(
        self,
        instrument_id: InstrumentId,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: Feature + 'static,
    {
        self.table(declared, indicators, |feature| {
            Slot::Depth(Reading::of(instrument_id), feature)
        })
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the book deltas of `instrument_id`.
    ///
    /// # Panics
    ///
    /// As `quotes`.
    #[must_use]
    pub fn deltas<A>(
        self,
        instrument_id: InstrumentId,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: Feature + 'static,
    {
        self.table(declared, indicators, |feature| {
            Slot::Deltas(Reading::of(instrument_id), feature)
        })
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the quotes of `instrument_id`, over the bracket of
    /// `width`, which it is handed (`OverBracket::over`) and the target's
    /// setter runs on (`FeatureSet::bracket_width`).
    ///
    /// # Panics
    ///
    /// As `quotes`, or if a feature declared before reads another width:
    /// the setter runs on one.
    #[must_use]
    pub fn over_bracket<A>(
        mut self,
        instrument_id: InstrumentId,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
        width: &Rc<BracketWidth>,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: OverBracket + 'static,
    {
        assert!(
            self.width
                .as_ref()
                .is_none_or(|read| Rc::ptr_eq(read, width)),
            "features over two widths of the target's bracket: its setter runs on one"
        );
        self.width = Some(Rc::clone(width));
        let table = declared.name();
        self.arguments
            .insert(table, indicators.canonical_args(declared));
        let mut feature = indicators.build(declared);
        feature.over(Rc::clone(width));
        self.declare(
            Name::Table(table),
            Slot::Quotes(Reading::of(instrument_id), Box::new(feature)),
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
            Name::Given(name.to_string()),
            Slot::Data(
                Reading::of(instrument_id),
                type_name.to_string(),
                Box::new(read),
                None,
            ),
        )
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

    /// The feature set, shared. A feature of a table is named after the
    /// table if the table feeds it alone, `<series>:<table>` if it feeds
    /// several, the series the feature's (see the module for both forms);
    /// a raw feature as given. The fingerprint hashes, each part
    /// told apart (`Fingerprint::with`), the names in order, then
    /// `series <series>` of each feature in that order, then
    /// `indicators.<table> <arguments>` of each table a feature is built
    /// from, by table name, the arguments in canonical form
    /// (`IndicatorsConfig::canonical_args`), then `version <n>` and
    /// `warmup <ns> ns`.
    ///
    /// # Errors
    ///
    /// Returns an error if no feature is declared, two share a name, or
    /// the warmup is negative.
    pub fn build(self) -> anyhow::Result<Rc<FeatureSet>> {
        ensure!(!self.names.is_empty(), "a feature set needs a feature");
        let names: Vec<String> = self
            .names
            .iter()
            .zip(&self.slots)
            .map(|(name, slot)| match name {
                Name::Table(table) if self.feeds_several(table) => {
                    format!("{}:{table}", slot.series())
                }
                Name::Table(table) => (*table).to_string(),
                Name::Given(name) => name.clone(),
            })
            .collect();
        let mut seen = BTreeSet::new();
        if let Some(name) = names.iter().find(|name| !seen.insert(*name)) {
            bail!("two features are named {name}");
        }
        ensure!(
            !self.warmup.is_negative(),
            "the features' warmup {:#} is negative",
            self.warmup
        );
        let series = self
            .slots
            .iter()
            .map(|slot| format!("series {}", slot.series()));
        let arguments = self
            .arguments
            .iter()
            .map(|(table, args)| format!("indicators.{table} {args}"));
        let version = format!("version {}", self.version);
        let warmup = format!("warmup {} ns", self.warmup.as_nanos());
        let fingerprint = names
            .iter()
            .cloned()
            .chain(series)
            .chain(arguments)
            .chain([version, warmup])
            .fold(Fingerprint::default(), |hash, part| {
                hash.with(part.as_bytes())
            });
        Ok(Rc::new(FeatureSet {
            names,
            slots: RefCell::new(self.slots),
            width: self.width,
            updates: RefCell::default(),
            warmup: self.warmup,
            fingerprint,
        }))
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, read from the source `slot` gives it, its table's
    /// arguments recorded for the fingerprint.
    fn table<A>(
        mut self,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
        slot: impl FnOnce(Box<dyn Feature>) -> Slot,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: Feature + 'static,
    {
        let table = declared.name();
        self.arguments
            .insert(table, indicators.canonical_args(declared));
        let feature = Box::new(indicators.build(declared));
        self.declare(Name::Table(table), slot(feature))
    }

    /// Whether `table` feeds more than one of the features.
    fn feeds_several(&self, table: &str) -> bool {
        self.names
            .iter()
            .filter(|name| matches!(name, Name::Table(of) if *of == table))
            .nth(1)
            .is_some()
    }

    fn declare(mut self, name: Name, slot: Slot) -> Self {
        self.names.push(name);
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
pub(crate) mod testing;
#[cfg(test)]
mod tests;
