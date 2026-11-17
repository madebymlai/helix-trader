//! A strategy's features: Nautilus indicators, each declared once on the
//! list of series it reads (`FeatureSetBuilder::feature`), fed by Nautilus
//! whatever their kinds.
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
//! (`strategy::warmup`'s `hold`, which goes once Nautilus holds them,
//! helix-trader#16; the set then registered for quotes and trades again);
//! in a replay, as they come. Nautilus routes no depth or book deltas to
//! indicators at all, only to the actor's handlers: the strategy hands the
//! warmup those of its responses and its live and replayed ones from them
//! (`model_data_hooks!`), which hands them to the set (`BookIndicator`), the
//! live ones held while a live warmup is pending, as quotes and trades are.
//! Bars built from a warmup's quotes, trades or source bars reach only the
//! cache, not the indicators: the strategy's warmup hands them to `handle_bar`
//! (`Warmup::on_data_response`).
//!
//! A raw feature reads custom data, which Nautilus' `Indicator` does not
//! handle: a custom series, of a whole Nautilus data type, its type name,
//! identifier and metadata (`FeatureSetBuilder::custom`), an instrument's
//! custom data under its id among them, which a model strategy reads, its
//! `[data] series` listing it (`custom`, `model::reads`). It
//! reads Nautilus' cache, which the set takes from the actor it is
//! registered with (`register`): the data engine caches each point it
//! processes, live or replayed, before the strategy's handlers run, one
//! history per data type, and the feature's value is a function of the
//! strategy (`read`) of the latest point of its data type
//! (`Cache::custom_data`), read as the set's values are. The data engine
//! does not cache a request's response, so a live warmup adds its own
//! (`Warmup::on_historical_data`). A raw feature is cold until the cache
//! holds a point of its data type.
//!
//! A feature on the ticks of a futures root (`GC.GLBX`, see `rolls`), or
//! on a custom series under the root's id, reads the contract standing
//! for it: `register` sets it from the clock (`rolls::front`), and that
//! contract's quotes, trades, depth, deltas and custom data (keyed by the
//! contract's id, `CustomSeries::on`) are matched to it.
//! The set's tick series (`ticks`) are what the model handle's
//! subscriptions subscribe to (`strategy::subscriptions`) and a live
//! warmup requests; at the roller's
//! transition, `roll` moves the root's features
//! to the post contract and resets them (Nautilus' `Indicator::reset`):
//! their windows never mix two contracts, which trade at different prices,
//! and the set is not initialized until they refill, so decisions wait. A
//! raw feature has no window: it reads the latest point of the post
//! contract the cache holds, if any. Other instruments' features, and the
//! raw features of a custom series of no root, are untouched.
//!
//! Each update reaches only the features declared on a series of its kind
//! and source, once however many of a feature's series it matches, in the
//! order it comes. A roll resets a feature once, however many of its
//! series are on the rolled root, its series on other instruments
//! untouched. An update of a kind or source no feature reads is a no-op,
//! so the panicking defaults of Nautilus' `Indicator` trait are never
//! called. The set never returns an error: Nautilus stops handling a bar at
//! an indicator's error, before the strategy's handler, and the warmup its
//! hand-over of ticks, so an error of one feature (an indicator reading the
//! last price of a quote) is logged and the update goes on.
//!
//! The names and their order are fixed when the set is built; `values`
//! snapshots the features in that order, once every one is initialized. A
//! side model reads them, then its side's payoff features, which the target
//! names (`side_names`, `side_row`); a meta model reads them, then the side
//! picked and the decision's level and side call (`meta_names`,
//! `meta_row`).
//! A feature exposes its current value through `Feature`, which every
//! Nautilus moving average has with no extra code; a raw feature reads its
//! value from the cache instead (see above).
//!
//! The set records, for each instrument, the time (`ts_init`) of the
//! latest quote, trade, bar, depth or deltas a feature read
//! (`last_update`), a root's under the root: a decision is fresh only if
//! data of the traded instrument came since the previous one (`decide`).
//! Custom data is no update of any instrument.
//!
//! A set declares its warmup (`FeatureSetBuilder::warmup`), how much data
//! its features read before a decision, such as the strategy's: a dataset
//! build replays it before its first day (`dataset`).
//!
//! A feature over the target's bracket (`OverBracket`, declared with
//! `FeatureSetBuilder::feature_over_bracket`) reads its list of series, and
//! is handed the bracket's width (`targets::BracketWidth`) as it is built:
//! the width at the decision its value is for, the decision its series are
//! read at, and the target's time limit. The set's features read one
//! width, which the model handle has the target's setter run on
//! (`bracket_width`), so the width's ATR is the target's, with no series of
//! the strategy's. The width is the target's, in the fingerprint of a
//! dataset beside the set's (`targets::fingerprint`).
//!
//! The fingerprint covers what makes the features what they are: their
//! names in order, the series each reads (which the name of a feature
//! named after its table alone, or of a raw feature, does not tell), the
//! whole data type of a raw feature's custom series, the arguments of
//! each table they are built from by value
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
        Bar, BarType, CustomData, NautilusDataType, OrderBookDeltas, OrderBookDepth, QuoteTick,
        TradeTick,
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
        warmup::BookIndicator,
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

/// The names of the side picked and the decision's calibrated level and
/// side call, the last values of a meta model's rows.
pub const PICKED_FEATURES: [&str; 3] = ["picked_side", "level", "side_call"];

/// The values of the `side` picked and the decision's level and side call
/// of `expected`, in the order of `PICKED_FEATURES`: the side 1 a buy, -1 a
/// sell.
fn picked_values(side: OrderSide, expected: Expected) -> [f64; 3] {
    let side = if side == OrderSide::Buy { 1.0 } else { -1.0 };
    [side, expected.level, expected.side_call]
}

/// The names of a meta model's features, of its sides' `names`: those,
/// then the side picked and the decision's level and side call
/// (`PICKED_FEATURES`).
#[must_use]
pub fn meta_names(names: &[String]) -> Vec<String> {
    names
        .iter()
        .cloned()
        .chain(PICKED_FEATURES.map(String::from))
        .collect()
}

/// A meta model's row: the sides' `features`, then the `side` picked and
/// the decision's level and side call of `expected`, in the order of
/// `meta_names`.
#[must_use]
pub fn meta_row(features: &[f64], side: OrderSide, expected: Expected) -> Vec<f64> {
    features
        .iter()
        .copied()
        .chain(picked_values(side, expected))
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

/// A declared feature: the indicator fed from its sources, or how a raw
/// feature reads its value.
enum Slot {
    /// An indicator, handed each update of any of its sources once, its
    /// sources in the order declared.
    Indicator(Vec<Source>, Box<dyn Feature>),
    /// A custom series, read from the cache by its data type: on a root,
    /// with the root it reads, by the data type of the contract read
    /// (`CustomSeries::on`).
    Custom(CustomSeries, Option<Reading>, Read),
}

/// A series an indicator feature reads: bars, matched by their standard
/// bar type (`BarType::standard`), or the ticks of one kind (quotes,
/// trades, depth or deltas, as `TickSeries` has them) of an instrument or
/// root, matched by the contract read.
enum Source {
    Bars(BarType),
    Ticks(NautilusDataType, Reading),
}

/// The instrument whose ticks a feature reads, or the root its custom
/// series is under, a futures root or a plain instrument, and the
/// contract it reads them of: the one
/// standing for the root, from `register` (`rolls::front`) and each roll
/// (`roll`); the instrument itself if it does not roll.
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
    /// The cache of the actor the set is registered with, which its raw
    /// features read (`register`).
    cache: RefCell<Option<Rc<RefCell<Cache>>>>,
    warmup: SignedDuration,
    fingerprint: Fingerprint,
}

/// Declares the features of a `FeatureSet`, each with its source, in the
/// order of their values: a feature of a declared indicator's table, or a
/// raw feature of custom data. An indicator is never handed in built:
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
    /// The width the features over the bracket read, if any.
    width: Option<Rc<BracketWidth>>,
    /// The arguments of each table a feature is built from, by its name,
    /// in canonical form (`IndicatorsConfig::canonical_args`).
    arguments: BTreeMap<&'static str, String>,
    /// Why a declaration is refused, which `build` returns.
    refused: Vec<String>,
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

    /// The value of each feature, in the order of the names, if every
    /// feature is initialized: a raw feature's read from the cache now.
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

    /// Whether every feature is initialized.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.values().is_some()
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
    /// depth and deltas, each of its declared instrument, a root or plain,
    /// each once, in the order declared over every series of every
    /// feature: what a strategy's
    /// subscriptions subscribe to (`strategy::subscriptions`) and a live
    /// warmup requests. Bars and custom series (`custom`) are not among
    /// them.
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

    /// The bar types the features read, each once, in order as `ticks`
    /// are: the bars beside them.
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

    /// The custom series the features read, each once, in order, beside
    /// the `ticks`: series a model strategy reads (`model::reads`).
    #[must_use]
    pub fn custom(&self) -> Vec<CustomSeries> {
        let mut custom: Vec<CustomSeries> = Vec::new();
        for slot in self.slots.borrow().iter() {
            if let Slot::Custom(series, ..) = slot
                && !custom.contains(series)
            {
                custom.push(series.clone());
            }
        }
        custom
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

    /// Whether any of `sources` reads a tick of `data` of `contract`: the
    /// update, at `ts_init`, recorded under the instrument of each that
    /// does, a root's under the root.
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

    /// Whether any of `sources` reads `bar`, of its standard bar type: the
    /// bar recorded under its instrument.
    fn reads_bar(&self, sources: &[Source], bar: &Bar) -> bool {
        let read = sources.iter().any(|source| match source {
            Source::Bars(bar_type) => bar_type.standard() == bar.bar_type.standard(),
            Source::Ticks(..) => false,
        });

        if read {
            self.updated(bar.bar_type.instrument_id(), bar.ts_init);
        }
        read
    }

    /// Registers the set with `actor`'s Nautilus indicators, from its
    /// `on_start`: each tick feature, or raw feature of a custom series on
    /// a root, reads the contract standing for its instrument or root now
    /// (`rolls::front`), the
    /// raw features from `actor`'s cache, and the set is registered for the
    /// bars of each bar type a feature reads (`register_contracts`).
    /// Nautilus registers an indicator once per source, so a set registered
    /// again, on a restart, gets each bar once.
    pub fn register<A: DataActor + DataActorNative>(self: &Rc<Self>, actor: &mut A) {
        *self.cache.borrow_mut() = Some(actor.cache_rc());
        let now = actor.clock().utc_now();

        for slot in self.slots.borrow_mut().iter_mut() {
            for reading in slot.readings_mut() {
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
    /// instrument), tick and raw features alike, a custom series' on the
    /// root among them, to its post contract, and
    /// resets the tick features (`Indicator::reset`): they are cold until
    /// they refill on the post contract, so the set is not initialized and
    /// decisions wait; a raw feature reads the post contract's latest point
    /// the cache holds. The other features are untouched. From the roller's transition
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
            for source in slot.sources() {
                if let Source::Bars(bar_type) = source {
                    let shared = Rc::clone(self) as SharedActorIndicator;
                    core.register_indicator_for_bars(*bar_type, shared);
                }
            }
        }
    }
}

/// The warmup's side of the set for books, which Nautilus routes to no
/// indicator: the warmup hands it those of its responses and the live and
/// replayed ones the strategy hands it (`Warmup::feeding_books`, see the
/// module).
impl BookIndicator for FeatureSet {
    /// Feeds `depth` once to each feature declared on its instrument's
    /// depth, or on the depth of the root it stands for.
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

    /// Feeds `deltas` once to each feature declared on its instrument's
    /// deltas, or on the deltas of the root it stands for.
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

impl Slot {
    /// The feature's value, if it is initialized: a raw feature's read of
    /// the latest point of its data type in `cache`, none before the set is
    /// registered with one.
    fn value(&self, cache: Option<&Cache>) -> Option<f64> {
        match self {
            Self::Indicator(_, feature) => feature.initialized().then(|| feature.value()),
            Self::Custom(series, reading, read) => {
                let on = reading.map(|reading| series.on(reading.contract));
                cache?
                    .custom_data(on.as_ref().unwrap_or(series).data_type())
                    .map(&**read)
            }
        }
    }

    /// The sources of an indicator feature; none of a raw feature.
    fn sources(&self) -> &[Source] {
        match self {
            Self::Indicator(sources, _) => sources,
            Self::Custom(..) => &[],
        }
    }

    /// What the feature reads of an instrument or root: each tick source
    /// of an indicator feature, or a raw feature's custom series on a
    /// root; none of bars or another custom series.
    fn readings_mut(&mut self) -> Vec<&mut Reading> {
        match self {
            Self::Indicator(sources, _) => sources
                .iter_mut()
                .filter_map(|source| match source {
                    Source::Ticks(_, reading) => Some(reading),
                    Source::Bars(_) => None,
                })
                .collect(),
            Self::Custom(_, reading, _) => reading.iter_mut().collect(),
        }
    }

    /// Moves each of the feature's readings of the root `roll` rolls, on
    /// the pre contract, to the post contract, and resets the feature once
    /// if any moved (see `FeatureSet::roll`).
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

    /// Makes the feature cold: an indicator reset (`Indicator::reset`); a
    /// raw feature holds nothing to reset, reading the cache.
    fn reset(&mut self) {
        match self {
            Self::Indicator(_, feature) => feature.reset(),
            Self::Custom(..) => {}
        }
    }

    /// The series the feature reads, as a feature's name and the
    /// fingerprint have it: its sources in the order declared, each as
    /// `Source::series` writes it, apart by `, `; a custom series by its
    /// data type as Nautilus writes it, its topic of its type name,
    /// metadata and identifier (see `CustomSeries`).
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
    /// The source of `series`, read on itself until `register` sets the
    /// contract standing for a root; none of a custom series, which an
    /// indicator feature does not read.
    fn of(series: &Series) -> Option<Self> {
        match series {
            Series::Bars(bar_type) => Some(Self::Bars(*bar_type)),
            Series::Ticks(ticks) => Some(Self::Ticks(
                ticks.data.clone(),
                Reading::of(ticks.instrument_id),
            )),
            Series::Custom(_) => None,
        }
    }

    /// The tick series of a tick source, of its declared instrument; none
    /// of bars.
    fn tick_series(&self) -> Option<TickSeries> {
        match self {
            Self::Ticks(data, reading) => Some(TickSeries {
                data: data.clone(),
                instrument_id: reading.instrument_id,
            }),
            Self::Bars(_) => None,
        }
    }

    /// The source's series: the bar type of bars, the tick series of
    /// ticks (`QuoteTick of XAUUSD.ICMARKETS`), a root's as the root.
    fn series(&self) -> String {
        match self {
            Self::Bars(bar_type) => bar_type.to_string(),
            Self::Ticks(..) => self
                .tick_series()
                .expect("a tick source has a tick series")
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(FeatureSet))
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

    /// Nautilus routes a bar by instrument and specification; a feature
    /// reads the bars of its bar types alone, of the same aggregation
    /// source.
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
    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on `series`, any number of bar types and tick series
    /// (quotes, trades, depth or deltas of an instrument or futures root)
    /// in any combination: it is handed each update of any of them once,
    /// through Nautilus' handler of the update's kind, in the order it
    /// comes. A series on a root reads the contract standing for the root,
    /// from `register` and each roll. A custom series among them is
    /// refused by `build`, as an empty list is: a raw feature reads custom
    /// data (`custom`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused,
    /// which the load refuses of an indicator the strategy declares
    /// (`Part::INDICATORS`).
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
        let table = declared.name();
        let feature = self.built(declared, indicators);
        self.indicator(table, series, Box::new(feature))
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the quotes of `instrument_id` (see `feature`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused (see
    /// `feature`).
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
        self.feature(
            &[ticks(NautilusDataType::QuoteTick, instrument_id)],
            declared,
            indicators,
        )
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the trades of `instrument_id` (see `feature`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused (see
    /// `feature`).
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
        self.feature(
            &[ticks(NautilusDataType::TradeTick, instrument_id)],
            declared,
            indicators,
        )
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the bars of `bar_type` (see `feature`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused (see
    /// `feature`).
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
        self.feature(&[Series::Bars(bar_type)], declared, indicators)
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the book depth of `instrument_id` (see `feature`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused (see
    /// `feature`).
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
        self.feature(
            &[ticks(NautilusDataType::OrderBookDepth, instrument_id)],
            declared,
            indicators,
        )
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the book deltas of `instrument_id` (see `feature`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused (see
    /// `feature`).
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
        self.feature(
            &[ticks(NautilusDataType::OrderBookDelta, instrument_id)],
            declared,
            indicators,
        )
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on `series` (see `feature`), over the bracket of
    /// `width`, which it is handed (`OverBracket::over`) and the target's
    /// setter runs on (`FeatureSet::bracket_width`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused (see
    /// `feature`), or if a feature declared before reads another width:
    /// the setter runs on one.
    #[must_use]
    pub fn feature_over_bracket<A>(
        mut self,
        series: &[Series],
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
        let mut feature = self.built(declared, indicators);
        feature.over(Rc::clone(width));
        self.indicator(table, series, Box::new(feature))
    }

    /// Declares the feature of `declared`, built of its table in
    /// `indicators`, on the quotes of `instrument_id`, over the bracket of
    /// `width` (see `feature_over_bracket`).
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused, or if
    /// a feature declared before reads another width (see
    /// `feature_over_bracket`).
    #[must_use]
    pub fn over_bracket<A>(
        self,
        instrument_id: InstrumentId,
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
        width: &Rc<BracketWidth>,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: OverBracket + 'static,
    {
        self.feature_over_bracket(
            &[ticks(NautilusDataType::QuoteTick, instrument_id)],
            declared,
            indicators,
            width,
        )
    }

    /// Declares a raw feature, named `name`, on `series`, a custom series
    /// (see `strategy::requirements::CustomSeries`), an instrument's
    /// custom data under its id among them: its value is `read` of the
    /// latest point of its data type the cache holds, cold until it holds
    /// one. On a root, it reads the series of the contract standing for
    /// the root, from `register` and each roll; any other series stands
    /// for itself, a contract's among them, and no roll moves it.
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
    /// several, the series the feature's whole list, apart by `, ` (see
    /// the module for both forms);
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
    /// Returns an error if no feature is declared, a feature is declared on
    /// no series or on a custom series (see `feature`), two share a name,
    /// or the warmup is negative.
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
        anyhow::ensure!(
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
            cache: RefCell::default(),
            warmup: self.warmup,
            fingerprint,
        }))
    }

    /// The indicator of `declared`, built of its table in `indicators`,
    /// its table's arguments recorded for the fingerprint.
    fn built<A>(&mut self, declared: Declared<A>, indicators: &IndicatorsConfig) -> A::Indicator
    where
        A: IndicatorArgs,
    {
        self.arguments
            .insert(declared.name(), indicators.canonical_args(declared));
        indicators.build(declared)
    }

    /// Declares `feature`, of `table`, on `series`, refusing a custom
    /// series among them or none at all (see `feature`).
    fn indicator(
        mut self,
        table: &'static str,
        series: &[Series],
        feature: Box<dyn Feature>,
    ) -> Self {
        if series.is_empty() {
            self.refused.push(format!(
                "the feature of [indicators.{table}] reads no series"
            ));
        }
        let mut sources = Vec::with_capacity(series.len());
        for series in series {
            match Source::of(series) {
                Some(source) => sources.push(source),
                None => self.refused.push(format!(
                    "the feature of [indicators.{table}] is declared on the custom series \
                     {series}, which only a raw feature reads"
                )),
            }
        }
        self.declare(Name::Table(table), Slot::Indicator(sources, feature))
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

/// The `data` ticks of `instrument_id`, as a series.
fn ticks(data: NautilusDataType, instrument_id: InstrumentId) -> Series {
    Series::Ticks(TickSeries {
        data,
        instrument_id,
    })
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
