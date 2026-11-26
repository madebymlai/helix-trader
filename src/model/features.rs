//! Declares a strategy's features as Nautilus indicators.
//!
//! Each feature is declared once, on the list of series it reads (`FeatureSetBuilder::feature`).
//! The series can be any number of bar types, tick series (quotes, trades, depth or deltas of an
//! instrument or futures root) and custom series (`Series`), in any combination.
//!
//! A feature is built from a declared indicator's table in `[indicators]` (`strategy::Declared`,
//! `IndicatorsConfig`), never by hand. A raw feature has no table and is named by the strategy.
//! Other features are named as follows:
//! - If the table feeds the feature alone, the feature is named after its table (`fast`).
//! - Otherwise it is named after its series and its table. The whole list of series comes in the
//!   order declared, separated by `, ` (`QuoteTick of XAUUSD.ICMARKETS, TradeTick of
//!   XAUUSD.ICMARKETS:fast`).
//!
//! The set is one Nautilus indicator (`ActorIndicator`). The strategy and its warmup share it as an
//! `Rc`. Its features sit in a `RefCell`, because the handlers borrow the set immutably.
//!
//! The set is registered with none of Nautilus' indicators of the strategy. Instead the warmup
//! hands it every bar, quote and trade (`Warmup::feeding`, `Warmup::feeding_ticks`). Nautilus hands
//! a registered indicator each live tick as it comes, and each live bar once its request
//! completes. That happens while a live warmup's older windows are still being answered. The warmup
//! orders its responses across series and holds the live updates until it ends.
//!
//! Temporary: `strategy::warmup`'s `hold` and `merge` do this, until Nautilus holds them
//! (helix-trader#16) and orders a warmup's history across data types (helix-trader#46). Then the
//! set is registered for quotes, trades and bars again.
//!
//! Nautilus routes depth and deltas to the actor's handlers alone. It routes custom data to
//! `on_data` and `on_historical_data` alone. So the strategy hands them to the warmup
//! (`model_data_hooks!`), and the warmup hands them to the set (`BookIndicator`,
//! `CustomIndicator`). A feature that reads a custom series supplies its own handler
//! (`Feature::custom_handler`).
//!
//! Bars built from a warmup's quotes, trades or source bars reach only the cache. So the warmup
//! reads them there and hands them to `handle_bar` (`Warmup::on_data_response`).
//!
//! A raw feature reads a custom series of a whole Nautilus data type (`FeatureSetBuilder::custom`),
//! with no indicator. It reads from the cache of the actor the set is registered with (`register`).
//! Its value is `read` applied to the latest point of the data type (`Cache::custom_data`), and it
//! is cold until the cache holds one. The data engine caches each point it processes before the
//! strategy's handlers run, but not a request's response. So a live warmup adds its own
//! (`Warmup::on_historical_data`).
//!
//! A feature on a futures root (`GC.GLBX`, see `rolls`) reads the contract standing for the root.
//! `register` sets that contract from the clock (`rolls::front`). The contract's data is matched to
//! the feature, including custom data keyed by its id (`CustomSeries::on`). `roll` moves the root's
//! features to the post contract and resets them (`Indicator::reset`). So their windows never mix
//! two contracts, and decisions wait until the windows refill. A raw feature reads the post
//! contract's latest cached point.
//!
//! A set may declare one event filter (`FeatureSetBuilder::event_filter`, an `EventFilter`), on a
//! list of series and from its table, as a feature is declared. It says whether a decision is an
//! event, the only decisions the strategy makes (`event`, read by `decide`). A filter of time alone
//! reads no series. The filter reads its series, warms, rolls and is fingerprinted as a feature
//! does. It is no feature: its name and value are in neither `names` nor `values`. The same
//! indicator can be declared as a feature too, which the model then reads.
//!
//! `ticks`, `bar_types` and `custom` gather the series of all features and the event filter, each
//! once, in the order declared. Three things use them:
//! - The model handle subscribes to them (`strategy::subscriptions`).
//! - A live warmup requests them.
//! - `[data] series` is checked against them (`model::reads`).
//!
//! An update reaches only the features, and the event filter, declared on a series of its kind and
//! source. It reaches each once, however many of its series it matches. An update none reads is a
//! no-op, so the panicking defaults of Nautilus' `Indicator` are never called.
//!
//! The set never returns an error. Nautilus stops handling a bar at an indicator's error, before
//! the strategy's handler, and the warmup stops its hand-over of ticks. So the set logs one
//! feature's error and the update goes on.
//!
//! The names and their order are fixed when the set is built. `values` snapshots them once every
//! feature is initialized.
//! - A side model reads them, then its side's payoff features (`side_names`, `side_row`).
//! - A meta model reads them, then the side picked and the decision's level and side call
//!   (`meta_names`, `meta_row`).
//!
//! The set records the `ts_init` of the latest update a feature or the event filter read, for each
//! instrument (`last_update`). A root's is recorded under the root. A decision is fresh only if
//! data of the traded instrument came since the previous decision (`decide`). Custom data is no
//! update of any instrument.
//!
//! The warmup (`FeatureSetBuilder::warmup`) is how much data the features read before a decision.
//! A dataset build replays it before its first day (`dataset`).
//!
//! A feature over the target's bracket (`Feature::over_bracket`, an `OverBracket`) is declared
//! like any other. It is handed the width (`targets::BracketWidth`) that the builder takes once
//! (`FeatureSetBuilder::bracket`). The event filter may be formed over it too
//! (`EventFilter::over_bracket`). The set refuses:
//! - Such a feature or filter with no width given.
//! - A width that no feature or filter reads.
//! - A second width.
//!
//! A feature that reads a width it was never handed panics, and never reads NaN. The model handle
//! has the target's setter run on the one width (`bracket_width`), so its ATR is the target's, with
//! no series of the strategy's. The dataset's fingerprint includes the width beside the set's
//! (`targets::fingerprint`).
//!
//! The fingerprint covers:
//! - The names, in order.
//! - The series each feature reads, which a name from its table alone, or a raw feature's name,
//!   does not tell.
//! - The whole data type of each custom series.
//! - The arguments of each table by value (`IndicatorsConfig::canonical_args`), however the TOML
//!   spells them.
//! - The event filter's table and series, if any.
//! - The warmup.
//! - The version of the strategy's feature code (`FeatureSetBuilder::version`), which is bumped by
//!   hand when what the code computes changes.
//!
//! It does not cover an indicator that the strategy builds from `[indicators]` for its own logic.
//! The code shared by every set, which is this module and Nautilus' indicators, is versioned with
//! the dataset (`dataset::VERSION`).

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

/// A meta model's row: the sides' `features`, then the `side` picked (1 a buy, -1 a sell), then the
/// level and side call from `expected`.
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

    /// Returns the handler for custom data, or `None` if the feature has none. A feature on a
    /// custom series with no handler is refused at build.
    fn custom_handler(&mut self) -> Option<&mut dyn CustomHandler> {
        None
    }

    /// Returns how it reads the target's bracket, or `None` unless it is formed over it.
    fn over_bracket(&mut self) -> Option<&mut dyn OverBracket> {
        None
    }
}

/// A Nautilus indicator that says whether a decision is an event, the only decisions the strategy
/// makes (`FeatureSetBuilder::event_filter`).
///
/// Its answer is a function of the data it read and the grid alone:
/// - An edge event (a cross, a sweep) keeps the `ts_init` of its last event and answers whether it
///   lies after `previous` and not after `now`. Live, the filter may have read data past `now`,
///   by at most the callback's delay, before the decision's callback ran: an event there is the
///   next decision's, and it replaces an earlier event after `previous`, so the decision at `now`
///   is no event live while a backtest marks it.
/// - A session answers from `now`.
/// - A regime answers from its own state.
pub trait EventFilter: Indicator {
    /// Returns whether the decision at `now` is an event, `previous` being the grid point before
    /// it. It is asked once initialized.
    fn is_event(&self, previous: UnixNanos, now: UnixNanos) -> bool;

    /// Returns the handler for custom data, or `None` if the filter has none. A filter on a custom
    /// series with no handler is refused at build.
    fn custom_handler(&mut self) -> Option<&mut dyn CustomHandler> {
        None
    }

    /// Returns how it reads the target's bracket, or `None` unless it is formed over it.
    fn over_bracket(&mut self) -> Option<&mut dyn OverBracket> {
        None
    }
}

/// Why a decision is no event (`FeatureSet::event`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoEvent {
    /// The event filter is not initialized.
    Cold,
    /// The event filter marks no event.
    Unmarked,
}

impl Display for NoEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cold => write!(f, "the event filter is not initialized"),
            Self::Unmarked => write!(f, "the event filter marks no event"),
        }
    }
}

/// How a feature or an event filter reads custom data (`Feature::custom_handler`,
/// `EventFilter::custom_handler`).
pub trait CustomHandler {
    /// Reads `data`, a point of one of the feature's custom series, in the order it comes.
    ///
    /// # Errors
    ///
    /// Returns an error if the feature cannot read the point, such as a point of a type it does not
    /// know. The set logs the error.
    fn handle_custom(&mut self, data: &CustomData) -> anyhow::Result<()>;
}

impl<T: MovingAverage> Feature for T {
    fn value(&self) -> f64 {
        MovingAverage::value(self)
    }
}

/// How a feature formed over the target's bracket reads it (`Feature::over_bracket`).
///
/// Its value at a decision is over the bracket's width there. Reading a width it was never handed
/// panics, which only a feature outside a set can do.
pub trait OverBracket {
    /// Reads its values over `width` from now on.
    fn over(&mut self, width: Rc<BracketWidth>);
}

type Read = Box<dyn Fn(&CustomData) -> f64>;

enum Slot {
    Feature(Vec<Source>, Box<dyn Feature>),
    /// A raw feature: a custom series read from the cache by its data type.
    Custom(CustomSeries, Option<Reading>, Read),
    EventFilter(Vec<Source>, Box<dyn EventFilter>),
}

/// How an update matches a source.
///
/// - Bars match by their standard bar type (`BarType::standard`).
/// - Ticks of one kind match by the contract read.
/// - A custom series matches by its data type as read (`read_type`).
enum Source {
    Bars(BarType),
    Ticks(NautilusDataType, Reading),
    Custom(CustomSeries, Option<Reading>),
}

/// The instrument or root read, and the contract standing for it, or the instrument itself if none.
#[derive(Debug, Clone, Copy)]
struct Reading {
    instrument_id: InstrumentId,
    contract: InstrumentId,
}

/// A strategy's features, built once with `FeatureSet::builder`.
pub struct FeatureSet {
    names: Vec<String>,
    slots: RefCell<Vec<Slot>>,
    /// What each slot is in a log: `feature <name>`, or the event filter and its table.
    labels: Vec<String>,
    width: Option<Rc<BracketWidth>>,
    updates: RefCell<BTreeMap<InstrumentId, UnixNanos>>,
    /// Set by `register`.
    cache: RefCell<Option<Rc<RefCell<Cache>>>>,
    warmup: SignedDuration,
    fingerprint: Fingerprint,
}

/// Declares the features of a `FeatureSet`, in the order of their values.
///
/// An indicator is never handed in built:
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
    /// The event filter, by its table. It names no feature.
    EventFilter(&'static str),
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
            .filter(|slot| !matches!(slot, Slot::EventFilter(..)))
            .map(|slot| slot.value(cache.as_deref()))
            .collect()
    }

    /// Returns whether the decision at `now` is an event, `previous` being the grid point before
    /// it.
    ///
    /// Without an event filter every decision is one.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision is no event, saying why: the event filter is cold, or marks
    /// no event.
    pub fn event(&self, previous: UnixNanos, now: UnixNanos) -> Result<(), NoEvent> {
        let slots = self.slots.borrow();
        let Some(filter) = slots.iter().find_map(|slot| match slot {
            Slot::EventFilter(_, filter) => Some(filter),
            Slot::Feature(..) | Slot::Custom(..) => None,
        }) else {
            return Ok(());
        };

        if !filter.initialized() {
            Err(NoEvent::Cold)
        } else if filter.is_event(previous, now) {
            Ok(())
        } else {
            Err(NoEvent::Unmarked)
        }
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

    /// The tick series the features and the event filter read, each once, in the order declared.
    /// Bars and custom series are not among them.
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

    /// The bar types the features and the event filter read, each once, in the order declared.
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

    /// The custom series the features and the event filter read, each once, in the order declared.
    /// It covers the series of raw features and the custom series among a feature's or the event
    /// filter's series.
    #[must_use]
    pub fn custom(&self) -> Vec<CustomSeries> {
        let mut custom: Vec<CustomSeries> = Vec::new();

        for slot in self.slots.borrow().iter() {
            let read = match slot {
                Slot::Custom(series, ..) => vec![series],
                Slot::Feature(sources, _) | Slot::EventFilter(sources, _) => sources
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

    /// The custom series among the series of the features and the event filter, which the set hands
    /// to their custom handlers. The series of raw features are not among them.
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

    /// The `ts_init` of the latest update of `instrument_id` that a feature read, if any.
    #[must_use]
    pub fn last_update(&self, instrument_id: InstrumentId) -> Option<UnixNanos> {
        self.updates.borrow().get(&instrument_id).copied()
    }

    fn updated(&self, instrument_id: InstrumentId, ts_init: UnixNanos) {
        let mut updates = self.updates.borrow_mut();
        let latest = updates.entry(instrument_id).or_insert(ts_init);
        *latest = (*latest).max(ts_init);
    }

    /// Returns whether any of `sources` reads the tick.
    ///
    /// It records the update under the instrument of each source that does, or under the root for a
    /// root.
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

    /// Registers the set on `actor`, from its `on_start`.
    ///
    /// A set registered again on a restart gets each update once, because the actor's indicators
    /// are not registered with. Temporary until Nautilus holds and orders them: helix-trader#16 and
    /// helix-trader#46.
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

    /// Moves the features that read the root `roll` rolls to the post contract.
    ///
    /// Only features on its pre contract that are not the contract itself move. The roller calls it
    /// on a transition (`rolls::Roller::on_time_event`).
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
            if let Some((sources, indicator)) = slot.indicator_mut()
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::OrderBookDepth,
                    depth.instrument_id,
                    depth.ts_init,
                )
            {
                indicator.handle_depth(depth);
            }
        }
    }

    fn handle_deltas(&self, deltas: &OrderBookDeltas) {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Some((sources, indicator)) = slot.indicator_mut()
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::OrderBookDelta,
                    deltas.instrument_id,
                    deltas.ts_init,
                )
            {
                indicator.handle_deltas(deltas);
            }
        }
    }
}

/// The warmup's side of the set for custom data (`Warmup::feeding_custom`).
impl CustomIndicator for FeatureSet {
    fn handle_custom(&self, data: &CustomData) {
        let mut slots = self.slots.borrow_mut();
        for (slot, label) in slots.iter_mut().zip(&self.labels) {
            if slot
                .sources()
                .iter()
                .any(|source| source.reads_custom(data))
                && let Err(e) = slot
                    .custom_handler()
                    .expect("a reader of a custom series has a custom handler, checked at build")
                    .handle_custom(data)
            {
                log::error!("{label} on the custom data {}: {e}", data.data_type);
            }
        }
    }
}

impl Slot {
    /// # Panics
    ///
    /// Panics if the slot is the event filter, which has no value.
    fn value(&self, cache: Option<&Cache>) -> Option<f64> {
        match self {
            Self::Feature(_, feature) => feature.initialized().then(|| feature.value()),
            Self::Custom(series, reading, read) => cache?
                .custom_data(&read_type(series, reading.as_ref()))
                .map(&**read),
            Self::EventFilter(..) => unreachable!("the event filter has no value"),
        }
    }

    fn sources(&self) -> &[Source] {
        match self {
            Self::Feature(sources, _) | Self::EventFilter(sources, _) => sources,
            Self::Custom(..) => &[],
        }
    }

    /// The series and indicator of a feature or the event filter. A raw feature has none.
    fn indicator_mut(&mut self) -> Option<(&[Source], &mut dyn Indicator)> {
        match self {
            Self::Feature(sources, feature) => Some((sources, feature.as_mut())),
            Self::EventFilter(sources, filter) => Some((sources, filter.as_mut())),
            Self::Custom(..) => None,
        }
    }

    fn custom_handler(&mut self) -> Option<&mut dyn CustomHandler> {
        match self {
            Self::Feature(_, feature) => feature.custom_handler(),
            Self::EventFilter(_, filter) => filter.custom_handler(),
            Self::Custom(..) => None,
        }
    }

    fn over_bracket(&mut self) -> Option<&mut dyn OverBracket> {
        match self {
            Self::Feature(_, feature) => feature.over_bracket(),
            Self::EventFilter(_, filter) => filter.over_bracket(),
            Self::Custom(..) => None,
        }
    }

    fn readings_mut(&mut self) -> Vec<&mut Reading> {
        match self {
            Self::Feature(sources, _) | Self::EventFilter(sources, _) => sources
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
        if let Some((_, indicator)) = self.indicator_mut() {
            indicator.reset();
        }
    }

    /// The series as they appear in a feature's name and the fingerprint.
    fn series(&self) -> String {
        match self {
            Self::Feature(sources, _) | Self::EventFilter(sources, _) => sources
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

/// The warmup's side of the set, for bars (`Warmup::feeding`), quotes and trades
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
        for (slot, label) in slots.iter_mut().zip(&self.labels) {
            if let Some((sources, indicator)) = slot.indicator_mut()
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::QuoteTick,
                    quote.instrument_id,
                    quote.ts_init,
                )
                && let Err(e) = indicator.handle_quote(quote)
            {
                log::error!("{label} on the quote {quote}: {e}");
            }
        }
        Ok(())
    }

    fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Some((sources, indicator)) = slot.indicator_mut()
                && self.reads_ticks(
                    sources,
                    &NautilusDataType::TradeTick,
                    trade.instrument_id,
                    trade.ts_init,
                )
            {
                indicator.handle_trade(trade);
            }
        }
        Ok(())
    }

    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        for slot in self.slots.borrow_mut().iter_mut() {
            if let Some((sources, indicator)) = slot.indicator_mut()
                && self.reads_bar(sources, bar)
            {
                indicator.handle_bar(bar);
            }
        }
        Ok(())
    }
}

impl FeatureSetBuilder {
    /// Declares the feature of `declared`, built from its table in `indicators`, on `series`.
    ///
    /// `build` refuses an empty list of series, and a custom series if the feature has no custom
    /// handler. The load refuses a missing, unparsable or refused table for an indicator the
    /// strategy declares (`Part::INDICATORS`).
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

    /// Declares the event filter of `declared`, built from its table in `indicators`, on `series`.
    ///
    /// A filter of time alone reads no series. `build` refuses a second event filter, and a custom
    /// series if the filter has no custom handler. The load refuses its table as a feature's
    /// (`feature`).
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - Its table is missing.
    /// - Its table does not parse or is refused.
    #[must_use]
    pub fn event_filter<A>(
        mut self,
        series: &[Series],
        declared: Declared<A>,
        indicators: &IndicatorsConfig,
    ) -> Self
    where
        A: IndicatorArgs,
        A::Indicator: EventFilter + 'static,
    {
        let table = declared.name();
        let declared_first = self.names.iter().find_map(|name| match name {
            Name::EventFilter(first) => Some(*first),
            Name::Table(_) | Name::Given(_) => None,
        });

        if let Some(first) = declared_first {
            self.refused.push(format!(
                "the set declares the event filter of [indicators.{table}] beside that of \
                 [indicators.{first}]: a strategy decides on one event filter"
            ));
        }
        let mut filter: Box<dyn EventFilter> = Box::new(self.built(declared, indicators));
        if filter.custom_handler().is_none() {
            self.refuse_unread_custom(
                &format!("the event filter of [indicators.{table}]"),
                series,
                "EventFilter::custom_handler",
            );
        }
        let sources = series.iter().map(Source::of).collect();
        self.declare(Name::EventFilter(table), Slot::EventFilter(sources, filter))
    }

    /// Takes the bracket `width` (`FeatureSet::bracket_width`).
    ///
    /// The features over the bracket read it, and the target's setter runs on it. `build` refuses:
    /// - A second width.
    /// - A width no feature reads.
    /// - A feature over the bracket with no width.
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

    /// Declares a raw feature, named `name`, on the custom series `series`.
    ///
    /// Its value is `read` applied to the latest point of its data type that the cache holds. A
    /// series not on a root stands for itself, and no roll moves it.
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

    /// Sets the version of the strategy's feature code, in the fingerprint.
    ///
    /// It is bumped by hand when what the features compute changes, which rebuilds the strategy's
    /// dataset and models. It is 0 unless given.
    #[must_use]
    pub const fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Declares how much data the features read before a decision.
    ///
    /// A dataset build replays this much before its first day. It is zero unless given.
    #[must_use]
    pub const fn warmup(mut self, warmup: SignedDuration) -> Self {
        self.warmup = warmup;
        self
    }

    /// Returns the feature set, shared.
    ///
    /// Features are named as the module says. The fingerprint hashes these parts, each told apart
    /// (`Fingerprint::with`):
    /// - The names in order.
    /// - `series <series>` of each feature.
    /// - `event filter indicators.<table> series <series>`, with an event filter.
    /// - `indicators.<table> <arguments>` of each table by name
    ///   (`IndicatorsConfig::canonical_args`), the event filter's among them.
    /// - `version <n>`.
    /// - `warmup <ns> ns`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No feature is declared.
    /// - A feature is declared on no series, or on a custom series it has no custom handler for
    ///   (see `feature`).
    /// - A second event filter is declared, or one on a custom series it has no custom handler for
    ///   (see `event_filter`).
    /// - Two features share a name.
    /// - A feature or the event filter over the target's bracket is given no width.
    /// - A width is given twice or read by no feature or event filter (see `bracket`).
    /// - The warmup is negative.
    pub fn build(self) -> anyhow::Result<Rc<FeatureSet>> {
        anyhow::ensure!(
            self.names
                .iter()
                .any(|name| !matches!(name, Name::EventFilter(_))),
            "a feature set needs a feature"
        );

        if let Some(refused) = self.refused.first() {
            anyhow::bail!("{refused}");
        }
        let (mut names, mut labels, mut filtering) = (vec![], vec![], None);
        for (name, slot) in self.names.iter().zip(&self.slots) {
            let feature = match name {
                Name::Table(table) if self.feeds_several(table) => {
                    format!("{}:{table}", slot.series())
                }
                Name::Table(table) => (*table).to_string(),
                Name::Given(name) => name.clone(),
                Name::EventFilter(table) => {
                    labels.push(format!("event filter of [indicators.{table}]"));
                    filtering = Some(format!(
                        "event filter indicators.{table} series {}",
                        slot.series()
                    ));
                    continue;
                }
            };
            labels.push(format!("feature {feature}"));
            names.push(feature);
        }
        let mut seen = BTreeSet::new();
        if let Some(name) = names.iter().find(|name| !seen.insert(*name)) {
            anyhow::bail!("two features are named {name}");
        }
        let mut slots = self.slots;
        let mut over = slots
            .iter_mut()
            .zip(&labels)
            .filter_map(|(slot, label)| slot.over_bracket().map(|over| (over, label)))
            .peekable();

        match &self.width {
            Some(width) => {
                anyhow::ensure!(
                    over.peek().is_some(),
                    "the set is given a width of the target's bracket, and no feature or event \
                     filter is formed over the bracket (`Feature::over_bracket`, \
                     `EventFilter::over_bracket`)"
                );

                for (reader, _) in over {
                    reader.over(Rc::clone(width));
                }
            }
            None => {
                if let Some((_, label)) = over.next() {
                    anyhow::bail!(
                        "the {label} is formed over the target's bracket, and the set is given no \
                         width of it (`FeatureSetBuilder::bracket`)"
                    );
                }
            }
        }
        anyhow::ensure!(
            !self.warmup.is_negative(),
            "the features' warmup {:#} is negative",
            self.warmup
        );
        let series = slots
            .iter()
            .filter(|slot| !matches!(slot, Slot::EventFilter(..)))
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
            .chain(filtering)
            .chain(arguments)
            .chain([version, warmup])
            .fold(Fingerprint::default(), |hash, part| {
                hash.with(part.as_bytes())
            });
        Ok(Rc::new(FeatureSet {
            names,
            slots: RefCell::new(slots),
            labels,
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

    /// Refuses an empty list of series or a custom series without a custom handler (see `feature`).
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
            self.refuse_unread_custom(
                &format!("the feature of [indicators.{table}]"),
                series,
                "Feature::custom_handler",
            );
        }
        let sources = series.iter().map(Source::of).collect();
        self.declare(Name::Table(table), Slot::Feature(sources, feature))
    }

    /// Refuses each custom series among `series` of `reader`, which has no custom `handler`.
    fn refuse_unread_custom(&mut self, reader: &str, series: &[Series], handler: &str) {
        for series in series {
            if let Series::Custom(custom) = series {
                self.refused.push(format!(
                    "{reader} is declared on the custom series {custom}, which it does not read: \
                     it has no custom handler (`{handler}`)"
                ));
            }
        }
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
