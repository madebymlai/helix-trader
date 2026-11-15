//! The data requirements of a strategy (`DataRequirements`), stated by its
//! config: the bar types, the tick series (`TickSeries`) and the custom
//! series (`CustomSeries`) it reads, and
//! the catalog series its bars are built from and its ticks are
//! (`CatalogSeries`, keyed in the catalog by `CatalogSeries::key`), each
//! over the slices its contracts stand for its instruments (`SeriesSlice`).
//! Filling the catalog, backtesting and going live all read them from here.

use std::{fmt, str::FromStr};

use anyhow::{anyhow, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::{ClosedInterval, Params, UnixNanos, correctness::check_valid_string_utf8};
use nautilus_model::{
    data::{BarType, DataType, NautilusDataType},
    enums::PriceType,
    identifiers::InstrumentId,
};
use nautilus_persistence::common::{
    coverage::merge_closed_intervals, paths::urisafe_instrument_id,
};
use serde::{Deserialize, Deserializer, de};

use crate::rolls::{Slice, Transition, TransitionTable, front};

/// What a strategy needs from data, stated by its config: filling the
/// catalog, backtesting and going live all read it from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRequirements {
    /// The bar types the strategy reads and warms up on, internal bars
    /// Nautilus builds and, of a pair it converts through read as bars, the
    /// pair's own (an IB pair's 1-minute BID and ASK bars,
    /// `venues::rate_series`, `StandardConfig::data_requirements`): their
    /// instruments are those it
    /// reads (see `instrument_ids`), and a live node sizes its cache to hold
    /// their warmup (see `warmup::Warmup::cache_config`).
    pub bar_types: Vec<BarType>,
    /// The tick series the strategy reads as they are, beside or instead
    /// of bars, book deltas and custom data among them (see
    /// `TickSeries`), those of its harness among them (a traded
    /// instrument's quotes when no series prices it,
    /// `StandardConfig::data_requirements`), and the quotes of a
    /// `[trade] conversions` pair read as quotes (a cTrader pair's,
    /// `venues::rate_series`): fetched by fill-catalog from their
    /// instrument's source, which must serve them (`venues::serves`; no
    /// source serves an instrument's custom data), and replayed in a
    /// backtest as declared. Their instruments are read too: live sets up
    /// their clients, which load them, but subscribes to none of the ticks:
    /// the strategy subscribes to each from its start through its
    /// subscriptions (`strategy::subscriptions`, a model strategy's
    /// handle's), a live start
    /// refusing one its venue's live data does not stream
    /// (`venues::streams`), and warms up on each its venue's history serves
    /// (`warmup::Warmup`), and on custom data its venue's history does not
    /// serve, from the catalog alone.
    pub ticks: Vec<TickSeries>,
    /// The custom series the strategy reads (see `CustomSeries`), a
    /// reference series such as LSE's daily bond yields among them, each
    /// as its config gives it in `[data] series`: routed by its type's row
    /// alone, whatever its identifier (`venues::serves_custom`), fetched by
    /// fill-catalog from its type's history source, with no definition,
    /// and replayed in a backtest over the run's window, the strategy's
    /// subscriptions subscribing to each from its start
    /// (`strategy::subscriptions`), each by its data type unchanged; a
    /// live run reading any its type's live data does not stream is
    /// refused (`venues::streams_custom`), none streaming any yet. They
    /// bring no instrument. Custom data of an instrument written with an
    /// `instrument_id` is a tick series still, keyed by the instrument and
    /// rolled with it.
    pub custom: Vec<CustomSeries>,
    /// How far back before a live start the warmup history starts: a
    /// backtest requests none.
    pub warmup: SignedDuration,
}

/// A series a strategy reads as it is: the data of one Nautilus data type
/// (`NautilusDataType`) of one instrument,
/// `{ data = "quotes", instrument_id = "XAUUSD.ICMARKETS" }` in a TOML
/// config. `data` is a name Nautilus parses (`NautilusDataType::from_str`)
/// of quotes (`"quotes"`), trades (`"trades"`), book depth snapshots
/// (`"order_book_depths"`), book deltas (`"order_book_deltas"`) or custom
/// data (`"Custom:<type name>"`): custom data of the instrument, keyed in
/// the catalog and on the message bus by the instrument id, as Nautilus'
/// Databento client keys its own (`DataType::new(type_name, None,
/// Some(instrument_id))`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickSeries {
    #[serde(deserialize_with = "series_data")]
    pub data: NautilusDataType,
    pub instrument_id: InstrumentId,
}

/// The data of a tick series: a Nautilus data type by any name Nautilus
/// parses, one of those a strategy reads as they are (see `TickSeries`).
fn series_data<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NautilusDataType, D::Error> {
    tick_data(&String::deserialize(deserializer)?).map_err(de::Error::custom)
}

/// The Nautilus data type `name` parses to, if a strategy reads it as it
/// is (see `TickSeries`), or why not.
pub(super) fn tick_data(name: &str) -> Result<NautilusDataType, String> {
    let data = NautilusDataType::from_str(name).map_err(|e| e.to_string())?;
    match data {
        NautilusDataType::QuoteTick
        | NautilusDataType::TradeTick
        | NautilusDataType::OrderBookDepth
        | NautilusDataType::OrderBookDelta
        | NautilusDataType::Custom { .. } => Ok(data),
        _ => Err(format!(
            "{name} is not a series a strategy reads as it is: \
             quotes, trades, order_book_depths, order_book_deltas or Custom:<type name>"
        )),
    }
}

/// A custom series: the custom data of one Nautilus data type
/// (`DataType`), its type's name, the identifier keying it in the catalog
/// and on the message bus, and its metadata, if any, saying what its
/// source asks for, `{ data = "Custom:LseRow", identifier = "US10Y",
/// metadata = { source = "bond_yields", symbol = "US10Y" } }` in `[data]
/// series`. Built only through `new`, which holds it to what makes it one
/// series of the catalog. Its data type is the one Nautilus subscribes,
/// caches, requests and stores it by, as the config names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomSeries(DataType);

impl CustomSeries {
    /// The series of `type_name` under `identifier`, of `metadata`: empty
    /// metadata is none, as Nautilus' data type of either has one topic,
    /// which keys its points in the cache and on the message bus.
    ///
    /// # Errors
    ///
    /// Returns Nautilus' error if `metadata` holds the key it reserves for
    /// the identifier (`DataType::try_new`), or an error naming
    /// `identifier` if it holds a backslash, or if its catalog folder
    /// (`catalog_folder`) is empty or blank, which Nautilus' correctness
    /// check refuses (`check_valid_string_utf8`): an empty folder is its
    /// type's own, which no series keys.
    pub fn new(
        type_name: &str,
        identifier: String,
        metadata: Option<Params>,
    ) -> anyhow::Result<Self> {
        let metadata = metadata.filter(|metadata| !metadata.is_empty());
        let folder = urisafe_instrument_id(&identifier);
        ensure!(
            !identifier.contains('\\'),
            "the identifier {identifier:?} holds a backslash, which Nautilus' catalog writes as a \
             folder separator and reads back as a character, so its series would be stored where \
             no read finds it"
        );
        check_valid_string_utf8(&folder, "identifier").map_err(|e| {
            anyhow!("the identifier {identifier:?} keys the catalog folder {folder:?}: {e}")
        })?;
        Ok(Self(DataType::try_new(
            type_name,
            metadata,
            Some(identifier),
        )?))
    }

    /// Its data type as the config names it.
    #[must_use]
    pub fn data_type(&self) -> &DataType {
        &self.0
    }

    /// Its type's name, which routes it (`venues::serves_custom`).
    #[must_use]
    pub fn type_name(&self) -> &str {
        self.0.type_name()
    }

    /// The identifier keying it in the catalog.
    #[must_use]
    pub fn identifier(&self) -> &str {
        self.0
            .identifier()
            .expect("a custom series is built with an identifier")
    }

    /// The folder Nautilus' catalog stores it under, below its type's
    /// folder, and reads it back from (`ParquetDataCatalog::
    /// make_path_custom_data`, `filter_identifier_files`): its identifier
    /// made URI-safe (`urisafe_instrument_id`: `/` removed, `^` made `_`).
    /// Of an identifier holding a backslash alone, the write would make it
    /// a folder separator and the read keep it, which `new` refuses.
    #[must_use]
    pub fn catalog_folder(&self) -> String {
        urisafe_instrument_id(self.identifier())
    }

    /// The root of the roll calendar its identifier names, if it names
    /// one (`GC.GLBX`, see `rolls`): the series then rolls, read on the
    /// contract standing for the root, as every series on a root is. Any
    /// other identifier stands for itself: a contract's (`GCZ26.GLBX`), a
    /// plain instrument's, or one naming no instrument (`US10Y`).
    #[must_use]
    pub fn root(&self) -> Option<InstrumentId> {
        InstrumentId::from_str(self.identifier())
            .ok()
            .filter(|&id| Transition::after(id, Timestamp::UNIX_EPOCH).is_some())
    }

    /// The series read at `time`: on a root, that of the contract standing
    /// for it then (`rolls::front`); itself otherwise.
    #[must_use]
    pub fn at(&self, time: Timestamp) -> Self {
        self.root()
            .map_or_else(|| self.clone(), |root| self.on(front(root, time)))
    }

    /// The series of `contract`: its type and metadata under the
    /// contract's id, which keys it in the catalog, on the message bus
    /// and in the cache, as Nautilus' Databento client keys an
    /// instrument's custom data. The metadata passed `new` already, and a
    /// contract of the roll calendar keys a folder of its own.
    pub(crate) fn on(&self, contract: InstrumentId) -> Self {
        Self(DataType::new(
            self.type_name(),
            self.0.metadata().cloned(),
            Some(contract.to_string()),
        ))
    }

    /// Its slices over `[start, end]`: on a root, the series of each
    /// contract over the slice it stands for the root, in time order
    /// (`rolls::TransitionTable::slices`); itself over the window
    /// otherwise.
    #[must_use]
    pub fn slices(&self, start: Timestamp, end: Timestamp) -> Vec<CustomSlice> {
        let Some(root) = self.root() else {
            return vec![CustomSlice {
                series: self.clone(),
                from: start,
                to: end,
            }];
        };
        TransitionTable::of(root, start, end)
            .slices()
            .into_iter()
            .map(|slice| CustomSlice {
                series: self.on(slice.instrument_id),
                from: slice.from,
                to: slice.to,
            })
            .collect()
    }
}

/// A custom series over the part of a window it is read in, both ends
/// included, as the catalog's ranges are: on a root, a contract's series
/// over the slice the contract stands for the root (see
/// `CustomSeries::slices`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomSlice {
    /// The series read, a root's on the contract.
    pub series: CustomSeries,
    pub from: Timestamp,
    pub to: Timestamp,
}

/// The series by its data type as Nautilus writes it, its topic of its
/// type's name, metadata and identifier
/// (`LseRow.source=bond_yields.symbol=US10Y.identifier=US10Y`).
impl fmt::Display for CustomSeries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl DataRequirements {
    /// The instruments read, whose history the catalog stores: those of
    /// the bar types, then those of the ticks, each once, in order.
    #[must_use]
    pub fn instrument_ids(&self) -> Vec<InstrumentId> {
        let bars = self.bar_types.iter().map(BarType::instrument_id);
        let ticks = self.ticks.iter().map(|tick| tick.instrument_id);
        let mut ids: Vec<InstrumentId> = Vec::new();
        for id in bars.chain(ticks) {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }

    /// These requirements on `instrument_ids` alone: their bar types and
    /// their ticks.
    #[must_use]
    pub fn of_instruments(&self, instrument_ids: &[InstrumentId]) -> Self {
        Self {
            bar_types: self
                .bar_types
                .iter()
                .filter(|bar_type| instrument_ids.contains(&bar_type.instrument_id()))
                .copied()
                .collect(),
            ticks: self
                .ticks
                .iter()
                .filter(|tick| instrument_ids.contains(&tick.instrument_id))
                .cloned()
                .collect(),
            custom: vec![],
            warmup: self.warmup,
        }
    }

    /// The keys in the catalog of the series read (`catalog_series`):
    /// those the bars are built from (see the free `catalog_keys`) and the
    /// ticks, each once, sorted as the free `catalog_keys` sorts.
    #[must_use]
    pub fn catalog_keys(&self) -> Vec<(NautilusDataType, String)> {
        self.catalog_series()
            .into_iter()
            .map(|source| source.key())
            .collect()
    }

    /// The external bars read, stored as they are: the source bars of the
    /// composite bar types (see `catalog_series`), each once, sorted as
    /// `catalog_series` sorts.
    #[must_use]
    pub fn source_bars(&self) -> Vec<BarType> {
        self.catalog_series()
            .into_iter()
            .filter_map(|source| match source {
                CatalogSeries::Bars(bar_type) => Some(bar_type),
                CatalogSeries::Ticks(_) => None,
            })
            .collect()
    }

    /// The catalog series read over `[start, end]`, contract by contract:
    /// each of `catalog_series`, in its order, split into the slices of its
    /// instrument's transition table (see `rolls::TransitionTable`), in
    /// time order, on the slice's contract. A series of a plain instrument
    /// is one slice over the span on that instrument. The slices of one
    /// series from several tables (a root's and its contract's own) are
    /// merged where they overlap or touch, so nothing is read twice. Bars
    /// on a root are built from its contracts' trades, as the config's
    /// load requires (see `DataConfig`).
    #[must_use]
    pub fn slices(&self, start: Timestamp, end: Timestamp) -> Vec<SeriesSlice> {
        let slices = self.catalog_series().into_iter().flat_map(|source| {
            TransitionTable::of(source.instrument_id(), start, end)
                .slices()
                .into_iter()
                .map(move |slice| SeriesSlice {
                    series: source.on(slice.instrument_id).key(),
                    slice,
                })
        });
        SeriesSlice::merged(slices)
    }

    /// The contracts standing for the instruments read over `[start, end]`,
    /// each with its slices of their transition tables (see
    /// `rolls::TransitionTable`), merged where they overlap or touch: those
    /// of a root and of a contract read beside it are one. A plain
    /// instrument is one slice over the span on itself. In the order of
    /// `instrument_ids`, then time.
    #[must_use]
    pub fn contract_slices(&self, start: Timestamp, end: Timestamp) -> Vec<Slice> {
        let keyed = self.instrument_ids().into_iter().flat_map(|id| {
            TransitionTable::of(id, start, end)
                .slices()
                .into_iter()
                .map(|slice| (slice.instrument_id, slice.from, slice.to))
        });
        merged(keyed)
            .into_iter()
            .map(|(instrument_id, from, to)| Slice {
                instrument_id,
                from,
                to,
            })
            .collect()
    }

    /// The custom series read over `[start, end]`, contract by contract:
    /// the slices of each (`CustomSeries::slices`), in the order listed,
    /// then time, those of one series merged where they overlap or touch,
    /// so a contract's series listed beside its root's is read once.
    #[must_use]
    pub fn custom_slices(&self, start: Timestamp, end: Timestamp) -> Vec<CustomSlice> {
        let keyed = self.custom.iter().flat_map(|series| {
            series
                .slices(start, end)
                .into_iter()
                .map(|slice| (slice.series, slice.from, slice.to))
        });
        merged(keyed)
            .into_iter()
            .map(|(series, from, to)| CustomSlice { series, from, to })
            .collect()
    }

    /// The catalog series read: those the bars are built from
    /// (`bar_sources`) and the ticks, each once, sorted as the free
    /// `catalog_keys` sorts their keys.
    pub(crate) fn catalog_series(&self) -> Vec<CatalogSeries> {
        let ticks = self.ticks.iter().cloned().map(CatalogSeries::Ticks);
        sorted_sources(bar_sources(&self.bar_types).chain(ticks))
    }
}

/// The windows `[from, to]` of `keyed`, those of one key merged where they
/// overlap or touch (Nautilus' `merge_closed_intervals`), each key's in
/// time order, the keys in the order they first come. A key is of one
/// contract.
fn merged<K: PartialEq + Clone>(
    keyed: impl IntoIterator<Item = (K, Timestamp, Timestamp)>,
) -> Vec<(K, Timestamp, Timestamp)> {
    let mut groups: Vec<(K, Vec<ClosedInterval>)> = Vec::new();
    for (key, from, to) in keyed {
        let interval = ClosedInterval {
            start: UnixNanos::from(from).as_u64(),
            end: UnixNanos::from(to).as_u64(),
        };
        match groups.iter_mut().find(|(of, _)| *of == key) {
            Some((_, intervals)) => intervals.push(interval),
            None => groups.push((key, vec![interval])),
        }
    }
    groups
        .into_iter()
        .flat_map(|(key, intervals)| {
            merge_closed_intervals(&intervals)
                .into_iter()
                .map(move |interval| {
                    let from = UnixNanos::from(interval.start).into();
                    let to = UnixNanos::from(interval.end).into();
                    (key.clone(), from, to)
                })
        })
        .collect()
}

/// A catalog series over the slice its contract stands for its root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesSlice {
    /// The series of the slice's contract: its Nautilus data type and
    /// catalog identifier, as `catalog_keys` gives them.
    pub series: (NautilusDataType, String),
    pub slice: Slice,
}

impl SeriesSlice {
    /// `slices`, those of one series merged where they overlap or touch,
    /// each series' in time order, the series in the order they first
    /// come: no part of a series is read twice.
    #[must_use]
    pub fn merged(slices: impl IntoIterator<Item = Self>) -> Vec<Self> {
        let keyed = slices.into_iter().map(|of| {
            (
                (of.series, of.slice.instrument_id),
                of.slice.from,
                of.slice.to,
            )
        });
        merged(keyed)
            .into_iter()
            .map(|((series, instrument_id), from, to)| Self {
                series,
                slice: Slice {
                    instrument_id,
                    from,
                    to,
                },
            })
            .collect()
    }
}

/// The ticks by their Nautilus data type and instrument,
/// `QuoteTick of XAUUSD.ICMARKETS`.
impl fmt::Display for TickSeries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} of {}", self.data, self.instrument_id)
    }
}

impl TickSeries {
    /// The catalog series of these ticks: their Nautilus data type, keyed
    /// by the instrument id (custom data too: see `TickSeries`).
    #[must_use]
    pub fn catalog_key(&self) -> (NautilusDataType, String) {
        (self.data.clone(), self.instrument_id.to_string())
    }
}

/// The keys in the catalog of the series the bars of `bar_types` are built
/// from, as Nautilus' data engine builds them: a key is a Nautilus data
/// type and its catalog identifier, the instrument id for ticks and the bar
/// type for bars. External bars are their own series; a composite bar type is built
/// from its source bars (`BarType::composite`), and internal bars from the
/// trades of their instrument if priced LAST, from its quotes otherwise.
/// Each series comes once, sorted by instrument, then identifier.
#[must_use]
pub fn catalog_keys(bar_types: &[BarType]) -> Vec<(NautilusDataType, String)> {
    sorted_sources(bar_sources(bar_types))
        .iter()
        .map(CatalogSeries::key)
        .collect()
}

/// What a catalog series holds: external bars, or the ticks of an
/// instrument. A venue's history serves some (`venues::serves`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CatalogSeries {
    Bars(BarType),
    Ticks(TickSeries),
}

/// The bars by their bar type, the ticks as `TickSeries` names them.
impl fmt::Display for CatalogSeries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bars(bar_type) => write!(f, "{bar_type}"),
            Self::Ticks(tick) => write!(f, "{tick}"),
        }
    }
}

impl CatalogSeries {
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::Bars(bar_type) => bar_type.instrument_id(),
            Self::Ticks(tick) => tick.instrument_id,
        }
    }

    /// The same series on `instrument_id`.
    fn on(&self, instrument_id: InstrumentId) -> Self {
        match self {
            Self::Bars(bar_type) => Self::Bars(BarType::new(
                instrument_id,
                bar_type.spec(),
                bar_type.aggregation_source(),
            )),
            Self::Ticks(tick) => Self::Ticks(TickSeries {
                data: tick.data.clone(),
                instrument_id,
            }),
        }
    }

    /// Its key in the catalog: its Nautilus data type and catalog
    /// identifier.
    pub(crate) fn key(&self) -> (NautilusDataType, String) {
        match self {
            Self::Bars(bar_type) => (NautilusDataType::Bar, bar_type.to_string()),
            Self::Ticks(tick) => tick.catalog_key(),
        }
    }
}

/// The catalog series each of `bar_types` is built from (see
/// `catalog_keys`).
pub(super) fn bar_sources(bar_types: &[BarType]) -> impl Iterator<Item = CatalogSeries> {
    bar_types.iter().map(|bar_type| {
        let source = if bar_type.is_composite() {
            bar_type.composite()
        } else {
            *bar_type
        };
        let instrument_id = source.instrument_id();
        if source.is_externally_aggregated() {
            CatalogSeries::Bars(source)
        } else if source.spec().price_type == PriceType::Last {
            CatalogSeries::Ticks(TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id,
            })
        } else {
            CatalogSeries::Ticks(TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id,
            })
        }
    })
}

/// `sources`, each once, sorted by instrument, then identifier, in the
/// order given otherwise.
fn sorted_sources(sources: impl Iterator<Item = CatalogSeries>) -> Vec<CatalogSeries> {
    let mut unique: Vec<CatalogSeries> = Vec::new();
    for source in sources {
        if !unique.contains(&source) {
            unique.push(source);
        }
    }
    unique.sort_by_cached_key(|source| (source.instrument_id(), source.key().1));
    unique
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_instruments_are_those_of_the_bar_types_each_once_in_order() {
        let requirements = DataRequirements {
            bar_types: vec![
                BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL"),
                BarType::from("EUNL.IBIS-1-HOUR-MID-INTERNAL"),
                BarType::from("SXR8.IBIS-1-DAY-MID-INTERNAL"),
            ],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::from_hours(720),
        };
        assert_eq!(
            requirements.instrument_ids(),
            [
                InstrumentId::from("SXR8.IBIS"),
                InstrumentId::from("EUNL.IBIS")
            ]
        );
    }

    #[test]
    fn requirements_on_some_instruments_keep_their_bar_types_and_the_warmup() {
        let requirements = DataRequirements {
            bar_types: vec![
                BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL"),
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL"),
            ],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::from_hours(720),
        };
        let okx = requirements.of_instruments(&[InstrumentId::from("BTC-USDT-SWAP.OKX")]);
        assert_eq!(
            okx,
            DataRequirements {
                bar_types: vec![BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL")],
                ticks: vec![],
                custom: vec![],
                warmup: SignedDuration::from_hours(720),
            }
        );
    }

    fn tick(data: NautilusDataType, instrument_id: &str) -> TickSeries {
        TickSeries {
            data,
            instrument_id: InstrumentId::from(instrument_id),
        }
    }

    /// Hour bars of IC's gold, and IC's gold quotes, CME gold's trades and
    /// book depth as ticks.
    fn gold() -> DataRequirements {
        DataRequirements {
            bar_types: vec![BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
            ticks: vec![
                tick(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
                tick(NautilusDataType::TradeTick, "GC.v.0.GLBX"),
                tick(NautilusDataType::OrderBookDepth, "GC.v.0.GLBX"),
            ],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    #[test]
    fn the_instruments_of_ticks_follow_those_of_the_bar_types_each_once() {
        assert_eq!(
            gold().instrument_ids(),
            [
                InstrumentId::from("XAUUSD.ICMARKETS"),
                InstrumentId::from("GC.v.0.GLBX")
            ]
        );
    }

    #[test]
    fn requirements_without_bar_types_read_the_instruments_of_their_ticks() {
        let requirements = DataRequirements {
            bar_types: vec![],
            ..gold()
        };
        assert_eq!(
            requirements.instrument_ids(),
            [
                InstrumentId::from("XAUUSD.ICMARKETS"),
                InstrumentId::from("GC.v.0.GLBX")
            ]
        );
    }

    #[test]
    fn requirements_on_some_instruments_keep_their_ticks() {
        let glbx = gold().of_instruments(&[InstrumentId::from("GC.v.0.GLBX")]);
        assert_eq!(
            glbx,
            DataRequirements {
                bar_types: vec![],
                ticks: vec![
                    tick(NautilusDataType::TradeTick, "GC.v.0.GLBX"),
                    tick(NautilusDataType::OrderBookDepth, "GC.v.0.GLBX"),
                ],
                custom: vec![],
                warmup: SignedDuration::ZERO,
            }
        );
    }

    #[test]
    fn the_series_of_requirements_are_the_sources_of_their_bars_and_their_ticks_each_once() {
        assert_eq!(
            gold().catalog_keys(),
            [
                (NautilusDataType::TradeTick, "GC.v.0.GLBX".to_string()),
                (NautilusDataType::OrderBookDepth, "GC.v.0.GLBX".to_string()),
                (NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS".to_string()),
            ]
        );
    }

    #[test]
    fn a_tick_series_loads_from_toml() {
        #[derive(Deserialize)]
        struct Config {
            ticks: Vec<TickSeries>,
        }
        let config: Config = toml::from_str(
            r#"ticks = [
                { data = "quotes", instrument_id = "XAUUSD.ICMARKETS" },
                { data = "trades", instrument_id = "GC.v.0.GLBX" },
                { data = "order_book_depths", instrument_id = "GC.v.0.GLBX" },
            ]"#,
        )
        .unwrap();
        assert_eq!(config.ticks, gold().ticks);
    }

    #[test]
    fn rejects_an_unknown_tick_data_type() {
        let err = toml::from_str::<TickSeries>(
            r#"data = "bars"
            instrument_id = "GC.v.0.GLBX""#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("bars"), "{err}");
    }

    #[test]
    fn book_deltas_and_custom_data_load_from_toml_as_nautilus_data_types() {
        #[derive(Deserialize)]
        struct Config {
            ticks: Vec<TickSeries>,
        }
        let config: Config = toml::from_str(
            r#"ticks = [
                { data = "order_book_deltas", instrument_id = "GC.v.0.GLBX" },
                { data = "Custom:Imbalance", instrument_id = "GC.v.0.GLBX" },
            ]"#,
        )
        .unwrap();
        assert_eq!(
            config.ticks,
            [
                tick(NautilusDataType::OrderBookDelta, "GC.v.0.GLBX"),
                tick(
                    NautilusDataType::Custom {
                        type_name: "Imbalance".to_string()
                    },
                    "GC.v.0.GLBX"
                ),
            ]
        );
    }

    #[test]
    fn book_deltas_and_custom_data_are_series_of_the_requirements_keyed_by_their_instrument() {
        let imbalance = NautilusDataType::Custom {
            type_name: "Imbalance".to_string(),
        };
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                tick(NautilusDataType::OrderBookDelta, "GC.v.0.GLBX"),
                tick(imbalance.clone(), "GC.v.0.GLBX"),
            ],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        assert_eq!(
            requirements.instrument_ids(),
            [InstrumentId::from("GC.v.0.GLBX")]
        );
        assert_eq!(
            requirements.catalog_keys(),
            [
                (NautilusDataType::OrderBookDelta, "GC.v.0.GLBX".to_string()),
                (imbalance, "GC.v.0.GLBX".to_string()),
            ]
        );
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    /// One nanosecond before `time`.
    fn before(time: &str) -> Timestamp {
        at(time) - SignedDuration::from_nanos(1)
    }

    fn series_slice(
        data_type: NautilusDataType,
        identifier: &str,
        instrument_id: &str,
        from: Timestamp,
        to: Timestamp,
    ) -> SeriesSlice {
        SeriesSlice {
            series: (data_type, identifier.to_string()),
            slice: Slice {
                instrument_id: InstrumentId::from(instrument_id),
                from,
                to,
            },
        }
    }

    #[test]
    fn the_series_of_plain_instruments_are_one_slice_each_over_the_span() {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
            ticks: vec![
                tick(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
                tick(NautilusDataType::TradeTick, "GCZ26.GLBX"),
            ],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(
            requirements.slices(start, end),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ26.GLBX",
                    "GCZ26.GLBX",
                    start,
                    end
                ),
                series_slice(
                    NautilusDataType::QuoteTick,
                    "XAUUSD.ICMARKETS",
                    "XAUUSD.ICMARKETS",
                    start,
                    end
                ),
            ]
        );
    }

    #[test]
    fn the_ticks_of_a_root_are_its_contracts_ticks_over_their_slices() {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                tick(NautilusDataType::TradeTick, "GC.GLBX"),
                tick(NautilusDataType::OrderBookDepth, "GC.GLBX"),
            ],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        let roll = "2026-07-29T00:00Z";
        assert_eq!(
            requirements.slices(start, end),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCQ26.GLBX",
                    "GCQ26.GLBX",
                    start,
                    before(roll)
                ),
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ26.GLBX",
                    "GCZ26.GLBX",
                    at(roll),
                    end
                ),
                series_slice(
                    NautilusDataType::OrderBookDepth,
                    "GCQ26.GLBX",
                    "GCQ26.GLBX",
                    start,
                    before(roll)
                ),
                series_slice(
                    NautilusDataType::OrderBookDepth,
                    "GCZ26.GLBX",
                    "GCZ26.GLBX",
                    at(roll),
                    end
                ),
            ]
        );
    }

    #[test]
    fn the_bars_of_a_root_are_built_from_its_contracts_trades_over_their_slices() {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("GC.GLBX-1-HOUR-LAST-INTERNAL")],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::from_hours(24),
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        let roll = "2026-07-29T00:00Z";
        assert_eq!(
            requirements.slices(start, end),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCQ26.GLBX",
                    "GCQ26.GLBX",
                    start,
                    before(roll)
                ),
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ26.GLBX",
                    "GCZ26.GLBX",
                    at(roll),
                    end
                ),
            ]
        );
    }

    /// The slices of `bar_type` over July and August 2026, across GC's
    /// roll of 29 July.
    fn bar_slices(bar_type: &str) -> Vec<SeriesSlice> {
        DataRequirements {
            bar_types: vec![BarType::from(bar_type)],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
        .slices(at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"))
    }

    #[test]
    fn bars_on_a_contract_need_not_be_built_from_trades() {
        assert_eq!(
            bar_slices("GCZ26.GLBX-1-MINUTE-MID-INTERNAL"),
            [series_slice(
                NautilusDataType::QuoteTick,
                "GCZ26.GLBX",
                "GCZ26.GLBX",
                at("2026-07-01T00:00Z"),
                at("2026-08-31T00:00Z")
            )]
        );
    }

    /// GCQ26.GLBX stands for GC.GLBX up to the roll of 29 July and is read
    /// as itself over the whole span.
    #[test]
    fn a_contract_read_beside_its_root_is_one_slice_of_each_series_over_both() {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                tick(NautilusDataType::TradeTick, "GC.GLBX"),
                tick(NautilusDataType::TradeTick, "GCQ26.GLBX"),
            ],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(
            requirements.slices(start, end),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCQ26.GLBX",
                    "GCQ26.GLBX",
                    start,
                    end
                ),
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ26.GLBX",
                    "GCZ26.GLBX",
                    at("2026-07-29T00:00Z"),
                    end
                ),
            ]
        );
    }

    #[test]
    fn the_contracts_of_a_root_and_of_a_contract_beside_it_are_one_slice_each() {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("GC.GLBX-1-HOUR-LAST-INTERNAL")],
            ticks: vec![tick(NautilusDataType::OrderBookDepth, "GCQ26.GLBX")],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        let slice = |id: &str, from, to| Slice {
            instrument_id: InstrumentId::from(id),
            from,
            to,
        };
        assert_eq!(
            requirements.contract_slices(start, end),
            [
                slice("GCQ26.GLBX", start, end),
                slice("GCZ26.GLBX", at("2026-07-29T00:00Z"), end),
            ]
        );
    }

    /// The imbalances under `identifier`, a custom series of no metadata.
    fn imbalances(identifier: &str) -> CustomSeries {
        CustomSeries::new("Imbalance", identifier.to_string(), None).unwrap()
    }

    /// LSE's 10-year US yields under `identifier`, a custom series of
    /// metadata.
    fn yields(identifier: &str) -> CustomSeries {
        let metadata = Params::new(
            [
                ("source".to_string(), serde_json::json!("bond_yields")),
                ("symbol".to_string(), serde_json::json!("US10Y")),
            ]
            .into(),
        );
        CustomSeries::new("LseRow", identifier.to_string(), Some(metadata)).unwrap()
    }

    fn custom_slice(series: CustomSeries, from: Timestamp, to: Timestamp) -> CustomSlice {
        CustomSlice { series, from, to }
    }

    /// An identifier naming a root of the roll calendar rolls; one naming a
    /// contract, an instrument the calendar does not roll, or no
    /// instrument at all, stands for itself.
    #[test]
    fn a_custom_series_has_a_root_only_under_a_root_s_identifier() {
        let roots = ["GC.GLBX", "GCZ26.GLBX", "XAUUSD.ICMARKETS", "US10Y"]
            .map(|identifier| imbalances(identifier).root());

        assert_eq!(
            roots,
            [Some(InstrumentId::from("GC.GLBX")), None, None, None]
        );
    }

    /// GC rolls from GCQ26 into GCZ26 at 00:00 UTC on 29 July 2026
    /// (`rolls/README.md`): its series is GCQ26's up to the last nanosecond
    /// before, GCZ26's from the roll on, its type and metadata kept, the
    /// contract's id its identifier.
    #[test]
    fn a_custom_series_on_a_root_is_at_each_time_the_series_of_the_contract_in_front() {
        let series = yields("GC.GLBX");

        let read = [before("2026-07-29T00:00Z"), at("2026-07-29T00:00Z")]
            .map(|time| series.at(time).data_type().clone());

        assert_eq!(
            read,
            [
                yields("GCQ26.GLBX").data_type().clone(),
                yields("GCZ26.GLBX").data_type().clone(),
            ]
        );
    }

    /// A contract's series and one of no instrument are themselves at any
    /// time.
    #[test]
    fn a_custom_series_of_no_root_is_itself_at_any_time() {
        for series in [yields("GCQ26.GLBX"), yields("US10Y")] {
            assert_eq!(series.at(at("2026-07-29T00:00Z")), series);
        }
    }

    /// Over July and August 2026, across GC's roll of 29 July: GCQ26's
    /// series up to 1 ns before the roll, GCZ26's from it.
    #[test]
    fn the_slices_of_a_custom_series_on_a_root_are_its_contracts_series_over_their_slices() {
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        let roll = "2026-07-29T00:00Z";

        assert_eq!(
            yields("GC.GLBX").slices(start, end),
            [
                custom_slice(yields("GCQ26.GLBX"), start, before(roll)),
                custom_slice(yields("GCZ26.GLBX"), at(roll), end),
            ]
        );
    }

    #[test]
    fn a_custom_series_of_no_root_is_one_slice_over_the_window() {
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));

        for series in [yields("GCQ26.GLBX"), yields("US10Y")] {
            assert_eq!(
                series.slices(start, end),
                [custom_slice(series.clone(), start, end)]
            );
        }
    }

    /// GCQ26's imbalances listed beside GC's are read once over the whole
    /// window, the root's GCQ26 slice within it; the yields under US10Y
    /// over the window too.
    #[test]
    fn a_contract_s_custom_series_listed_beside_its_root_s_is_one_slice_over_both() {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![],
            custom: vec![
                imbalances("GC.GLBX"),
                imbalances("GCQ26.GLBX"),
                yields("US10Y"),
            ],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));

        assert_eq!(
            requirements.custom_slices(start, end),
            [
                custom_slice(imbalances("GCQ26.GLBX"), start, end),
                custom_slice(imbalances("GCZ26.GLBX"), at("2026-07-29T00:00Z"), end),
                custom_slice(yields("US10Y"), start, end),
            ]
        );
    }

    /// The series of each bar type, as `(data type, identifier)` strings.
    fn series_of(bar_types: &[&str]) -> Vec<(NautilusDataType, String)> {
        let bar_types: Vec<BarType> = bar_types.iter().map(|&s| BarType::from(s)).collect();
        catalog_keys(&bar_types)
    }

    #[test]
    fn bars_built_from_quotes_need_the_quotes_of_their_instrument() {
        assert_eq!(
            series_of(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]),
            [(NautilusDataType::QuoteTick, "SXR8.IBIS".to_string())]
        );
    }

    #[test]
    fn bars_built_from_trades_need_the_trades_of_their_instrument() {
        assert_eq!(
            series_of(&["BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL"]),
            [(NautilusDataType::TradeTick, "BTC-USDT-SWAP.OKX".to_string())]
        );
    }

    #[test]
    fn composite_bars_need_their_source_bars() {
        assert_eq!(
            series_of(&["BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"]),
            [(
                NautilusDataType::Bar,
                "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL".to_string()
            )]
        );
    }

    #[test]
    fn composite_bars_of_internal_source_bars_need_what_those_are_built_from() {
        assert_eq!(
            series_of(&["SXR8.IBIS-1-HOUR-MID-INTERNAL@1-MINUTE-INTERNAL"]),
            [(NautilusDataType::QuoteTick, "SXR8.IBIS".to_string())]
        );
    }

    #[test]
    fn external_bars_are_their_own_series() {
        assert_eq!(
            series_of(&["BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL"]),
            [(
                NautilusDataType::Bar,
                "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL".to_string()
            )]
        );
    }

    #[test]
    fn each_series_comes_once_in_the_order_of_its_instrument() {
        assert_eq!(
            series_of(&[
                "SXR8.IBIS-1-MINUTE-MID-INTERNAL",
                "EUNL.IBIS-1-HOUR-MID-INTERNAL",
                "SXR8.IBIS-1-HOUR-BID-INTERNAL",
                "EUNL.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            ]),
            [
                (NautilusDataType::QuoteTick, "EUNL.IBIS".to_string()),
                (
                    NautilusDataType::Bar,
                    "EUNL.IBIS-1-MINUTE-LAST-EXTERNAL".to_string()
                ),
                (NautilusDataType::QuoteTick, "SXR8.IBIS".to_string()),
            ]
        );
    }
}
