//! A strategy's features: Nautilus indicators, each declared once on the list of series it reads
//! (`FeatureSetBuilder::feature`): any number of bar types, tick series (quotes, trades, depth or
//! deltas of an instrument or futures root) and custom series (`Series`), in any combination.
//!
//! A feature is built from a declared indicator's table in `[indicators]` (`strategy::Declared`,
//! `IndicatorsConfig`), never by hand. It is named after its table (`fast`) when the table feeds
//! it alone, else after its series and its table: the whole list in the order declared, apart by
//! `, ` (`QuoteTick of XAUUSD.ICMARKETS, TradeTick of XAUUSD.ICMARKETS:fast`). A raw feature has
//! no table and is named by the strategy.
//!
//! The set is one Nautilus indicator (`ActorIndicator`), shared as an `Rc` between the strategy
//! and its warmup, its features in a `RefCell` as the handlers borrow it immutably. It is
//! registered with none of Nautilus' indicators of the strategy: the warmup hands it every bar,
//! quote and trade (`Warmup::feeding`, `Warmup::feeding_ticks`), because Nautilus hands a
//! registered indicator each live tick as it comes, and each live bar once its request completes,
//! while a live warmup's older windows are still being answered. The warmup orders its responses
//! across series and holds the live updates until it ends. Temporary: `strategy::warmup`'s `hold`
//! and `merge` do this, until Nautilus holds them (helix-trader#16) and orders a warmup's history
//! across data types (helix-trader#46), when the set is registered for quotes, trades and bars
//! again.
//!
//! Nautilus routes depth and deltas to the actor's handlers alone, and custom data to `on_data`
//! and `on_historical_data` alone, so the strategy hands them to the warmup (`model_data_hooks!`),
//! which hands them to the set (`BookIndicator`, `CustomIndicator`). A feature reading a custom
//! series gives its own handler (`Feature::custom_handler`). Bars built from a warmup's quotes,
//! trades or source bars reach only the cache, so the warmup reads them there and hands them to
//! `handle_bar` (`Warmup::on_data_response`).
//!
//! A raw feature reads a custom series of a whole Nautilus data type (`FeatureSetBuilder::custom`)
//! from the cache of the actor the set is registered with (`register`), with no indicator. Its
//! value is `read` of the latest point of the data type (`Cache::custom_data`), cold until the
//! cache holds one. The data engine caches each point it processes before the strategy's
//! handlers run, but not a request's response, so a live warmup adds its own
//! (`Warmup::on_historical_data`).
//!
//! A feature on a futures root (`GC.GLBX`, see `rolls`) reads the contract standing for it:
//! `register` sets it from the clock (`rolls::front`), and that contract's data, custom data keyed
//! by its id (`CustomSeries::on`), is matched to it. `roll` moves the root's features to the post
//! contract and resets them (`Indicator::reset`), so their windows never mix two contracts and
//! decisions wait until they refill. A raw feature reads the post contract's latest cached point.
//!
//! `ticks`, `bar_types` and `custom` gather the series of all features, each once, in the order
//! declared: the model handle subscribes to them (`strategy::subscriptions`), a live warmup
//! requests them and `[data] series` is checked against them (`model::reads`).
//!
//! An update reaches only the features declared on a series of its kind and source, once however
//! many of a feature's series it matches. An update no feature reads is a no-op, so the panicking
//! defaults of Nautilus' `Indicator` are never called. The set never returns an error, because
//! Nautilus stops handling a bar at an indicator's error, before the strategy's handler, and the
//! warmup its hand-over of ticks: an error of one feature is logged and the update goes on.
//!
//! The names and their order are fixed when the set is built, and `values` snapshots them once
//! every feature is initialized. A side model reads them, then its side's payoff features
//! (`side_names`, `side_row`); a meta model, then the side picked and the decision's level and
//! side call (`meta_names`, `meta_row`).
//!
//! The set records for each instrument the `ts_init` of the latest update a feature read, a root's
//! under the root (`last_update`): a decision is fresh only if data of the traded instrument came
//! since the previous one (`decide`). Custom data is no update of any instrument.
//!
//! The warmup (`FeatureSetBuilder::warmup`) is how much data the features read before a decision:
//! a dataset build replays it before its first day (`dataset`).
//!
//! A feature over the target's bracket (`Feature::over_bracket`, an `OverBracket`) is declared as
//! any other and handed the width (`targets::BracketWidth`) the builder takes once
//! (`FeatureSetBuilder::bracket`). The set refuses such a feature with no width given, a width no
//! feature reads and a second width. A feature reading a width it was never handed panics, never
//! reading NaN. The model handle has the target's setter run on the one width (`bracket_width`),
//! so its ATR is the target's, with no series of the strategy's. The width is in the fingerprint
//! of a dataset beside the set's (`targets::fingerprint`).
//!
//! The fingerprint covers the names in order, the series each reads (which a name from its table
//! alone, or of a raw feature, does not tell), the whole data type of each custom series, the
//! arguments of each table by value (`IndicatorsConfig::canonical_args`, so however the TOML
//! spells them), the warmup, and the version of the strategy's feature code
//! (`FeatureSetBuilder::version`), bumped by hand when what it computes changes. An indicator the
//! strategy builds of `[indicators]` for its own logic is not in it. The code shared by every set,
//! this module and Nautilus' indicators, is versioned with the dataset (`dataset::VERSION`).

use std::{
    any::Any,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display},
    rc::Rc,
};

use jiff::SignedDuration;
use nautilus_common::{
    actor::{
        DataActor, DataActorNative,
        indicators::{ActorIndicator, SharedActorIndicator},
    },
    cache::Cache,
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

use super::{calibration::Expected, targets::BracketWidth};
use crate::{
    rolls::{Transition, front},
    strategy::{
        Declaration, Declared, IndicatorArgs, IndicatorsConfig, Series,
        requirements::{CustomSeries, TickSeries},
        warmup::{BookIndicator, CustomIndicator, Warmup},
    },
};

/// The names of a side model's features: `names`, then the target's `payoff` names.
#[must_use]
pub fn side_names(names: &[String], payoff: &[String]) -> Vec<String> {
    names.iter().chain(payoff).cloned().collect()
}

/// A side model's row: the decision's `features`, then its side's `payoff` features.
#[must_use]
pub fn side_row(features: &[f64], payoff: &[f64]) -> Vec<f64> {
    features.iter().chain(payoff).copied().collect()
}

/// The names of the last values of a meta model's rows.
pub const PICKED_FEATURES: [&str; 3] = ["picked_side", "level", "side_call"];

fn picked_values(side: OrderSide, expected: Expected) -> [f64; 3] {
    let side = if side == OrderSide::Buy { 1.0 } else { -1.0 };
    [side, expected.level, expected.side_call]
}

/// The names of a meta model's features: `names`, then `PICKED_FEATURES`.
#[must_use]
pub fn meta_names(names: &[String]) -> Vec<String> {
    names
        .iter()
        .cloned()
        .chain(PICKED_FEATURES.map(String::from))
        .collect()
}

/// A meta model's row: the sides' `features`, then the `side` picked (1 a buy, -1 a sell), and the
/// level and side call of `expected`.
#[must_use]
pub fn meta_row(features: &[f64], side: OrderSide, expected: Expected) -> Vec<f64> {
    features
        .iter()
        .copied()
        .chain(picked_values(side, expected))
        .collect()
}

/// A Nautilus indicator exposing its current value.
pub trait Feature: Indicator {
    /// The current value, read once initialized.
    fn value(&self) -> f64;

    /// Its handler of custom data, none unless given. A feature on a custom series with none is
    /// refused at build.
    fn custom_handler(&mut self) -> Option<&mut dyn CustomHandler> {
        None
    }

    /// How it reads the target's bracket, none unless it is formed over it.
    fn over_bracket(&mut self) -> Option<&mut dyn OverBracket> {
        None
    }
}

/// How a feature reads custom data (`Feature::custom_handler`).
pub trait CustomHandler {
    /// Reads `data`, a point of one of the feature's custom series, in the order it comes.
    ///
    /// # Errors
    ///
    /// Returns an error if the feature cannot read it, a point of a type it does not know, say. The
    /// set logs it.
    fn handle_custom(&mut self, data: &CustomData) -> anyhow::Result<()>;
}

impl<T: MovingAverage> Feature for T {
    fn value(&self) -> f64 {
        MovingAverage::value(self)
    }
}

/// How a feature formed over the target's bracket reads it (`Feature::over_bracket`): its value at
/// a decision is over the bracket's width there. Reading a width it was never handed, which only a
/// feature outside a set can, it panics.
pub trait OverBracket {
    /// Reads its values over `width` from now on.
    fn over(&mut self, width: Rc<BracketWidth>);
}

type Read = Box<dyn Fn(&CustomData) -> f64>;

enum Slot {
    Indicator(Vec<Source>, Box<dyn Feature>),
    /// A raw feature: a custom series read from the cache by its data type.
    Custom(CustomSeries, Option<Reading>, Read),
}

/// Bars match by their standard bar type (`BarType::standard`), ticks of one kind by the contract
/// read, a custom series by its data type as read (`read_type`).
enum Source {
    Bars(BarType),
    Ticks(NautilusDataType, Reading),
    Custom(CustomSeries, Option<Reading>),
}

/// The instrument or root read, and the contract standing for it, else the instrument itself.
#[derive(Debug, Clone, Copy)]
struct Reading {
    instrument_id: InstrumentId,
    contract: InstrumentId,
}

/// A strategy's features, built once with `FeatureSet::builder`.
pub struct FeatureSet {
    names: Vec<String>,
    slots: RefCell<Vec<Slot>>,
    width: Option<Rc<BracketWidth>>,
    updates: RefCell<BTreeMap<InstrumentId, UnixNanos>>,
    /// Set by `register`.
    cache: RefCell<Option<Rc<RefCell<Cache>>>>,
    warmup: SignedDuration,
    fingerprint: Fingerprint,
}

/// Declares the features of a `FeatureSet`, in the order of their values. An indicator is never
/// handed in built:
///
/// ```
/// # use helix_trader::{model::features::FeatureSet, strategy::{Declared, EmaArgs, IndicatorsConfig, Series}};
/// # use nautilus_model::identifiers::InstrumentId;
/// const MID: Declared<EmaArgs> = Declared::new("mid");
/// let indicators: IndicatorsConfig =
///     toml::from_str(r#"mid = { period = 1, price_type = "MID" }"#).unwrap();
///
/// let features = FeatureSet::builder()
///     .feature(&[Series::quotes(InstrumentId::from("AUD/USD.SIM"))], MID, &indicators)
///     .build()
///     .unwrap();
///
/// assert_eq!(features.names(), ["mid"]);
/// ```
///
/// ```compile_fail
/// # use helix_trader::{model::features::FeatureSet, strategy::Series};
/// # use nautilus_indicators::average::ema::ExponentialMovingAverage;
/// # use nautilus_model::identifiers::InstrumentId;
/// let features = FeatureSet::builder()
///     .feature(
///         &[Series::quotes(InstrumentId::from("AUD/USD.SIM"))],
///         "mid",
///         ExponentialMovingAverage::new(1, None),
///     )
///     .build();
/// ```
pub struct FeatureSetBuilder {
    names: Vec<Name>,
    slots: Vec<Slot>,
    width: Option<Rc<BracketWidth>>,
    arguments: BTreeMap<&'static str, String>,
    refused: Vec<String>,
    version: u32,
    warmup: SignedDuration,
}

enum Name {
    Table(&'static str),
    Given(String),
}

/// A SHA-256 hash of what makes rows, models and decisions comparable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Fingerprint([u8; 32]);

impl FeatureSet {
    /// Returns a builder with no feature declared.
    #[must_use]
    pub fn builder() -> FeatureSetBuilder {
        FeatureSetBuilder {
            names: vec![],
            slots: vec![],
            width: None,
            arguments: BTreeMap::new(),
            refused: vec![],
            version: 0,
            warmup: SignedDuration::ZERO,
        }
    }

    /// The names of the features, in the order of their values.
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The value of each feature, in the order of the names, if every feature is initialized.
    #[must_use]
    pub fn values(&self) -> Option<Vec<f64>> {
        let cache = self.cache.borrow();
        let cache = cache.as_ref().map(|cache| cache.borrow());
        self.slots
            .borrow()
            .iter()
            .map(|slot| slot.value(cache.as_deref()))
            .collect()
    }

    #[must_use]
    pub fn initialized(&self) -> bool {
        self.values().is_some()
    }

    /// The fingerprint of the set (see the module).
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// How much data the features read before a decision.
    #[must_use]
    pub const fn warmup(&self) -> SignedDuration {
        self.warmup
    }

    /// The tick series the features read, each once, in the order declared. Bars and custom series
    /// are not among them.
    #[must_use]
    pub fn ticks(&self) -> Vec<TickSeries> {
        let mut ticks: Vec<TickSeries> = Vec::new();

        for slot in self.slots.borrow().iter() {
            for tick in slot.sources().iter().filter_map(Source::tick_series) {
                if !ticks.contains(&tick) {
                    ticks.push(tick);
                }
            }
        }
        ticks
    }

    /// The bar types the features read, each once, in the order declared.
    #[must_use]
    pub fn bar_types(&self) -> Vec<BarType> {
        let mut bar_types: Vec<BarType> = Vec::new();

        for slot in self.slots.borrow().iter() {
            for source in slot.sources() {
                if let Source::Bars(bar_type) = source
                    && !bar_types.contains(bar_type)
                {
                    bar_types.push(*bar_type);
                }
            }
        }
        bar_types
    }

    /// The custom series the features read, raw features' and among a feature's series, each once,
    /// in the order declared.
    #[must_use]
    pub fn custom(&self) -> Vec<CustomSeries> {
        let mut custom: Vec<CustomSeries> = Vec::new();

        for slot in self.slots.borrow().iter() {
            let read = match slot {
                Slot::Custom(series, ..) => vec![series],
                Slot::Indicator(sources, _) => sources
                    .iter()
                    .filter_map(|source| match source {
                        Source::Custom(series, _) => Some(series),
                        Source::Bars(_) | Source::Ticks(..) => None,
                    })
                    .collect(),
            };

            for series in read {
                if !custom.contains(series) {
                    custom.push(series.clone());
                }
            }
        }
        custom
    }

    /// The custom series among the features' series, which the set hands their custom handlers. A
    /// raw feature's are not among them.
    #[must_use]
    pub fn handled_custom(&self) -> Vec<CustomSeries> {
        let mut custom: Vec<CustomSeries> = Vec::new();

        for slot in self.slots.borrow().iter() {
            for source in slot.sources() {
                if let Source::Custom(series, _) = source
                    && !custom.contains(series)
                {
                    custom.push(series.clone());
                }
            }
        }
        custom
    }

    /// The `ts_init` of the latest update of `instrument_id` a feature read, if any.
    #[must_use]
    pub fn last_update(&self, instrument_id: InstrumentId) -> Option<UnixNanos> {
        self.updates.borrow().get(&instrument_id).copied()
    }

    fn updated(&self, instrument_id: InstrumentId, ts_init: UnixNanos) {
        let mut updates = self.updates.borrow_mut();
        let latest = updates.entry(instrument_id).or_insert(ts_init);
        *latest = (*latest).max(ts_init);
    }

    /// Whether any of `sources` reads the tick, recording the update under the instrument of each
    /// that does, a root's under the root.
    fn reads_ticks(
        &self,
        sources: &[Source],
        data: &NautilusDataType,
        contract: InstrumentId,
        ts_init: UnixNanos,
    ) -> bool {
        let mut read = false;

        for source in sources {
            if let Source::Ticks(of, reading) = source
                && of == data
                && reading.contract == contract
            {
                self.updated(reading.instrument_id, ts_init);
                read = true;
            }
        }
        read
    }

    fn reads_bar(&self, sources: &[Source], bar: &Bar) -> bool {
        let read = sources.iter().any(|source| match source {
            Source::Bars(bar_type) => bar_type.standard() == bar.bar_type.standard(),
            Source::Ticks(..) | Source::Custom(..) => false,
        });

        if read {
            self.updated(bar.bar_type.instrument_id(), bar.ts_init);
        }
        read
    }

    pub(super) fn warmed(self: &Rc<Self>, warmup: Warmup) -> Warmup {
        let ticks = self.ticks();
        warmup
            .feeding(&self.bar_types(), Rc::clone(self) as SharedActorIndicator)
            .feeding_ticks(&ticks, Rc::clone(self) as SharedActorIndicator)
            .feeding_books(&ticks, Rc::clone(self) as Rc<dyn BookIndicator>)
            .feeding_custom(
                &self.handled_custom(),
                Rc::clone(self) as Rc<dyn CustomIndicator>,
            )
    }

    /// Registers the set on `actor`, from its `on_start`. A set registered again on a restart gets
    /// each update once, as the actor's indicators are not registered with. Temporary until
    /// Nautilus holds and orders them: helix-trader#16 and helix-trader#46.
    pub fn register<A: DataActor + DataActorNative>(self: &Rc<Self>, actor: &mut A) {
        *self.cache.borrow_mut() = Some(actor.cache_rc());
        let now = actor.clock().utc_now();

        for slot in self.slots.borrow_mut().iter_mut() {
            for reading in slot.readings_mut() {
                reading.contract = front(reading.instrument_id, now);
            }
        }
    }

    pub(super) fn bracket_width(&self) -> Option<&BracketWidth> {
        self.width.as_deref()
    }

    /// Moves the features reading the root `roll` rolls, those on its pre contract that are not the
    /// contract itself, to the post contract. Called from the roller's transition
    /// (`rolls::Roller::on_time_event`).
    pub fn roll(&self, roll: &Transition) {
        for slot in self.slots.borrow_mut().iter_mut() {
            slot.roll(roll);
        }
    }
}

/// The warmup's side of the set for books (`Warmup::feeding_books`).
impl BookIndicator for FeatureSet {
    fn handle_depth(&self, depth: &OrderBookDepth) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Indicator(sources, feature) = slot
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::OrderBookDepth,
                    depth.instrument_id,
                    depth.ts_init,
                )
            {
                feature.handle_depth(depth);
            }
        }
    }

    fn handle_deltas(&self, deltas: &OrderBookDeltas) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Indicator(sources, feature) = slot
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::OrderBookDelta,
                    deltas.instrument_id,
                    deltas.ts_init,
                )
            {
                feature.handle_deltas(deltas);
            }
        }
    }
}

/// The warmup's side of the set for custom data (`Warmup::feeding_custom`).
impl CustomIndicator for FeatureSet {
    fn handle_custom(&self, data: &CustomData) {
        let mut slots = self.slots.borrow_mut();
        for (slot, name) in slots.iter_mut().zip(&self.names) {
            if let Slot::Indicator(sources, feature) = slot
                && sources.iter().any(|source| source.reads_custom(data))
                && let Err(e) = feature
                    .custom_handler()
                    .expect("a feature on a custom series has a custom handler, checked at build")
                    .handle_custom(data)
            {
                log::error!("feature {name} on the custom data {}: {e}", data.data_type);
            }
        }
    }
}

impl Slot {
    fn value(&self, cache: Option<&Cache>) -> Option<f64> {
        match self {
            Self::Indicator(_, feature) => feature.initialized().then(|| feature.value()),
            Self::Custom(series, reading, read) => cache?
                .custom_data(&read_type(series, reading.as_ref()))
                .map(&**read),
        }
    }

    fn sources(&self) -> &[Source] {
        match self {
            Self::Indicator(sources, _) => sources,
            Self::Custom(..) => &[],
        }
    }

    fn readings_mut(&mut self) -> Vec<&mut Reading> {
        match self {
            Self::Indicator(sources, _) => sources
                .iter_mut()
                .filter_map(|source| match source {
                    Source::Ticks(_, reading) => Some(reading),
                    Source::Custom(_, reading) => reading.as_mut(),
                    Source::Bars(_) => None,
                })
                .collect(),
            Self::Custom(_, reading, _) => reading.iter_mut().collect(),
        }
    }

    fn roll(&mut self, roll: &Transition) {
        let mut moved = false;

        for reading in self.readings_mut() {
            if reading.contract == roll.pre && reading.instrument_id != roll.pre {
                reading.contract = roll.post;
                moved = true;
            }
        }

        if moved {
            self.reset();
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Indicator(_, feature) => feature.reset(),
            Self::Custom(..) => {}
        }
    }

    /// The series as a feature's name and the fingerprint have it.
    fn series(&self) -> String {
        match self {
            Self::Indicator(sources, _) => sources
                .iter()
                .map(Source::series)
                .collect::<Vec<_>>()
                .join(", "),
            Self::Custom(series, ..) => series.to_string(),
        }
    }
}

impl Source {
    fn of(series: &Series) -> Self {
        match series {
            Series::Bars(bar_type) => Self::Bars(*bar_type),
            Series::Ticks(ticks) => {
                Self::Ticks(ticks.data.clone(), Reading::of(ticks.instrument_id))
            }
            Series::Custom(custom) => Self::Custom(custom.clone(), custom.root().map(Reading::of)),
        }
    }

    fn tick_series(&self) -> Option<TickSeries> {
        match self {
            Self::Ticks(data, reading) => Some(TickSeries {
                data: data.clone(),
                instrument_id: reading.instrument_id,
            }),
            Self::Bars(_) | Self::Custom(..) => None,
        }
    }

    fn reads_custom(&self, data: &CustomData) -> bool {
        match self {
            Self::Custom(series, reading) => read_type(series, reading.as_ref()) == data.data_type,
            Self::Bars(_) | Self::Ticks(..) => false,
        }
    }

    fn series(&self) -> String {
        match self {
            Self::Bars(bar_type) => bar_type.to_string(),
            Self::Ticks(..) => self
                .tick_series()
                .expect("a tick source has a tick series")
                .to_string(),
            Self::Custom(series, _) => series.to_string(),
        }
    }
}

fn read_type(series: &CustomSeries, reading: Option<&Reading>) -> DataType {
    reading.map_or_else(
        || series.data_type().clone(),
        |reading| series.on(reading.contract).data_type().clone(),
    )
}

impl Reading {
    const fn of(instrument_id: InstrumentId) -> Self {
        Self {
            instrument_id,
            contract: instrument_id,
        }
    }
}

impl Debug for FeatureSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(FeatureSet))
            .field("names", &self.names)
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

/// The warmup's side of the set, for its bars (`Warmup::feeding`), quotes and trades
/// (`Warmup::feeding_ticks`).
impl ActorIndicator for FeatureSet {
    fn key(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn initialized(&self) -> anyhow::Result<bool> {
        Ok(Self::initialized(self))
    }

    fn handle_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        let mut slots = self.slots.borrow_mut();
        for (slot, name) in slots.iter_mut().zip(&self.names) {
            if let Slot::Indicator(sources, feature) = slot
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::QuoteTick,
                    quote.instrument_id,
                    quote.ts_init,
                )
                && let Err(e) = feature.handle_quote(quote)
            {
                log::error!("feature {name} on the quote {quote}: {e}");
            }
        }
        Ok(())
    }

    fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Indicator(sources, feature) = slot
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::TradeTick,
                    trade.instrument_id,
                    trade.ts_init,
                )
            {
                feature.handle_trade(trade);
            }
        }
        Ok(())
    }

    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Slot::Indicator(sources, feature) = slot
                && self.reads_bar(sources, bar)
            {
                feature.handle_bar(bar);
            }
        }
        Ok(())
    }
}

impl FeatureSetBuilder {
    /// Declares the feature of `declared`, built from its table in `indicators`, on `series`.
    /// `build` refuses a custom series if the feature has no custom handler, as it does an empty
    /// list. The load refuses a missing, unparsable or refused table of an indicator the strategy
    /// declares (`Part::INDICATORS`).
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - Its table is missing.
    /// - Its table does not parse or is refused.
    #[must_use]
    pub fn feature<A>(
        mut self,
        series: &[Series],
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: Feature + 'static,
    {
        let feature = Box::new(self.built(declared, indicators));
        self.indicator(declared.name(), series, feature)
    }

    /// Takes `width`, which the features over the bracket read and the target's setter runs on
    /// (`FeatureSet::bracket_width`). `build` refuses a second width, a width no feature reads,
    /// and a feature over the bracket with none.
    #[must_use]
    pub fn bracket(mut self, width: &Rc<BracketWidth>) -> Self {
        if self.width.is_some() {
            self.refused.push(
                "the set is given the width of the target's bracket twice: the target's setter \
                 runs on one"
                    .to_string(),
            );
        }
        self.width = Some(Rc::clone(width));
        self
    }

    /// Declares a raw feature, named `name`, on the custom series `series`: its value is `read` of
    /// the latest point of its data type the cache holds. Any series not on a root stands for
    /// itself, and no roll moves it.
    #[must_use]
    pub fn custom(
        self,
        series: &CustomSeries,
        name: &str,
        read: impl Fn(&CustomData) -> f64 + 'static,
    ) -> Self {
        self.declare(
            Name::Given(name.to_string()),
            Slot::Custom(
                series.clone(),
                series.root().map(Reading::of),
                Box::new(read),
            ),
        )
    }

    /// Sets the version of the strategy's feature code, in the fingerprint, bumped by hand when
    /// what the features compute changes, which rebuilds the strategy's dataset and models. 0
    /// unless given.
    #[must_use]
    pub const fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Declares how much data the features read before a decision: a dataset build replays this
    /// much before its first day. Zero unless given.
    #[must_use]
    pub const fn warmup(mut self, warmup: SignedDuration) -> Self {
        self.warmup = warmup;
        self
    }

    /// Returns the feature set, shared. Features are named as the module says. The fingerprint
    /// hashes, each part told apart (`Fingerprint::with`), the names in order, `series <series>`
    /// of each feature, `indicators.<table> <arguments>` of each table by name
    /// (`IndicatorsConfig::canonical_args`), then `version <n>` and `warmup <ns> ns`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No feature is declared.
    /// - A feature is declared on no series, or on a custom series it has no custom handler for
    ///   (see `feature`).
    /// - Two features share a name.
    /// - A feature over the target's bracket is given no width.
    /// - A width is given twice or read by no feature (see `bracket`).
    /// - The warmup is negative.
    pub fn build(self) -> anyhow::Result<Rc<FeatureSet>> {
        anyhow::ensure!(!self.names.is_empty(), "a feature set needs a feature");
        if let Some(refused) = self.refused.first() {
            anyhow::bail!("{refused}");
        }
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
            anyhow::bail!("two features are named {name}");
        }
        let mut slots = self.slots;
        let mut over = slots
            .iter_mut()
            .zip(&names)
            .filter_map(|(slot, name)| match slot {
                Slot::Indicator(_, feature) => feature.over_bracket().map(|over| (over, name)),
                Slot::Custom(..) => None,
            })
            .peekable();

        match &self.width {
            Some(width) => {
                anyhow::ensure!(
                    over.peek().is_some(),
                    "the set is given a width of the target's bracket, and no feature is formed \
                     over the bracket (`Feature::over_bracket`)"
                );

                for (feature, _) in over {
                    feature.over(Rc::clone(width));
                }
            }
            None => {
                if let Some((_, name)) = over.next() {
                    anyhow::bail!(
                        "the feature {name} is formed over the target's bracket, and the set is \
                         given no width of it (`FeatureSetBuilder::bracket`)"
                    );
                }
            }
        }
        anyhow::ensure!(
            !self.warmup.is_negative(),
            "the features' warmup {:#} is negative",
            self.warmup
        );
        let series = slots.iter().map(|slot| format!("series {}", slot.series()));
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
            slots: RefCell::new(slots),
            width: self.width,
            updates: RefCell::default(),
            cache: RefCell::default(),
            warmup: self.warmup,
            fingerprint,
        }))
    }

    /// Builds the indicator, recording its table's arguments for the fingerprint.
    fn built<A>(&mut self, declared: Declared<A>, indicators: &IndicatorsConfig) -> A::Indicator
    where
        A: IndicatorArgs,
    {
        self.arguments
            .insert(declared.name(), indicators.canonical_args(declared));
        indicators.build(declared)
    }

    /// Refuses no series, or a custom series without a custom handler (see `feature`).
    fn indicator(
        mut self,
        table: &'static str,
        series: &[Series],
        mut feature: Box<dyn Feature>,
    ) -> Self {
        if series.is_empty() {
            self.refused.push(format!(
                "the feature of [indicators.{table}] reads no series"
            ));
        }

        if feature.custom_handler().is_none() {
            for series in series {
                if let Series::Custom(custom) = series {
                    self.refused.push(format!(
                        "the feature of [indicators.{table}] is declared on the custom series \
                         {custom}, which it does not read: it has no custom handler \
                         (`Feature::custom_handler`)"
                    ));
                }
            }
        }
        let sources = series.iter().map(Source::of).collect();
        self.declare(Name::Table(table), Slot::Indicator(sources, feature))
    }

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
    /// Extends the fingerprint with `part`, its length first, so that parts are told apart, not
    /// concatenated.
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
