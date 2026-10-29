//! Strategies: interchangeable, one runs at a time.
//!
//! Build step 5. The port is the Nautilus `Strategy` trait
//! (`nautilus_trading`): backtest and live are generic over it and never
//! name a concrete strategy. `main` picks one by name and builds it.
//!
//! Each strategy is its own submodule and exposes only its constructor and
//! config. Inside, `mod.rs` is the thin Nautilus wiring: a struct holding a
//! `StrategyCore`, wired with `nautilus_strategy!`, implementing `DataActor`
//! (`on_start`, `on_bar`) and placing orders with Nautilus' own API. Pure
//! modules beside it (the EMA's `signal.rs` and `bracket.rs`) turn bars
//! into decisions, tested without an engine.
//!
//! What most strategies share is a config struct and functions, never a
//! base type or trait: `BarsConfig`, embedded in each strategy config with
//! `#[serde(flatten)]`, holds the bars read, their warmup and their
//! `Session`. The exception is a strategy trading on a learned model,
//! which holds the harness' types (`model::features::FeatureSet`,
//! `model::handle::ModelHandle`); `learned` is the example.
//!
//! Strategies trade on bars Nautilus builds, from source bars (IB's
//! 1-minute bars, OKX's candles) or from quotes; `warmup` asks for their
//! history and hands it back, so that no strategy knows how. The catalog
//! stores what their bars are built from (see `catalog_series`).

mod config;
pub mod ema;
#[cfg(gold_scalp)]
pub mod gold_scalp;
pub mod learned;
mod session;
pub mod warmup;

pub use config::{BarsConfig, CompositeSource};
pub use session::Session;

use std::str::FromStr;

use anyhow::ensure;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::{ClosedInterval, UnixNanos};
use nautilus_model::{
    data::{BarType, NautilusDataType},
    enums::PriceType,
    identifiers::InstrumentId,
};
use nautilus_persistence::common::coverage::merge_closed_intervals;
use serde::{Deserialize, Deserializer, de};

use crate::rolls::{Slice, Transition, TransitionTable};

/// What a strategy needs from data, stated by its config: filling the
/// catalog, backtesting and going live all read it from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRequirements {
    /// The internal bar types the strategy reads and warms up on: their
    /// instruments are those it reads (see `instrument_ids`), and the node
    /// sizes its cache to hold their warmup (see `warmup::cache_config`).
    pub bar_types: Vec<BarType>,
    /// The tick series the strategy reads as they are, beside or instead
    /// of bars, book deltas and custom data among them (see
    /// `TickSeries`): fetched by fill-catalog from their instrument's
    /// source (Databento fetches a GLBX instrument's book and trades
    /// whatever is declared; no source serves custom data) and replayed in
    /// a backtest as declared, with no warmup of their own. Their
    /// instruments are read too: live sets up their clients but subscribes
    /// to none of the ticks.
    pub ticks: Vec<TickSeries>,
    /// How far back before the run the warmup history starts.
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
    let name = String::deserialize(deserializer)?;
    let data = NautilusDataType::from_str(&name).map_err(de::Error::custom)?;
    match data {
        NautilusDataType::QuoteTick
        | NautilusDataType::TradeTick
        | NautilusDataType::OrderBookDepth
        | NautilusDataType::OrderBookDelta
        | NautilusDataType::Custom { .. } => Ok(data),
        _ => Err(de::Error::custom(format!(
            "{name} is not a series a strategy reads as it is: \
             quotes, trades, order_book_depths, order_book_deltas or Custom:<type name>"
        ))),
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
            warmup: self.warmup,
        }
    }

    /// The catalog series read: those the bars are built from (see
    /// `catalog_series`) and the ticks, each once, sorted as
    /// `catalog_series` sorts.
    #[must_use]
    pub fn catalog_series(&self) -> Vec<(NautilusDataType, String)> {
        self.sources()
            .into_iter()
            .map(|source| source.series())
            .collect()
    }

    /// The catalog series read over `[start, end]`, contract by contract:
    /// each of `catalog_series`, in its order, split into the slices of its
    /// instrument's transition table (see `rolls::TransitionTable`), in
    /// time order, on the slice's contract. A series of a plain instrument
    /// is one slice over the span on that instrument. The slices of one
    /// series from several tables (a root's and its contract's own) are
    /// merged where they overlap or touch, so nothing is read twice.
    ///
    /// # Errors
    ///
    /// Returns an error naming a bar type on a root that is not built from
    /// its trades (internal, LAST, not composite): Nautilus builds its
    /// continuous bars from the contracts' trades and prices their rolls
    /// from them (see `rolls::ContinuousBars`), so such bars would never
    /// come.
    pub fn slices(&self, start: Timestamp, end: Timestamp) -> anyhow::Result<Vec<SeriesSlice>> {
        for bar_type in &self.bar_types {
            let instrument_id = bar_type.instrument_id();
            ensure!(
                Transition::after(instrument_id, start).is_none()
                    || (bar_type.is_internally_aggregated()
                        && !bar_type.is_composite()
                        && bar_type.spec().price_type == PriceType::Last),
                "bars {bar_type} on the root {instrument_id} must be built from its trades \
                 (internal, LAST, not composite): its continuous bars are built and their \
                 rolls priced from trades"
            );
        }
        let slices = self.sources().into_iter().flat_map(|source| {
            TransitionTable::of(source.instrument_id(), start, end)
                .slices()
                .into_iter()
                .map(move |slice| SeriesSlice {
                    series: source.on(slice.instrument_id).series(),
                    slice,
                })
        });
        Ok(SeriesSlice::merged(slices))
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
                .map(|slice| (slice.instrument_id, slice))
        });
        merged(keyed).into_iter().map(|(_, slice)| slice).collect()
    }

    /// The sources of the bars and the ticks, as `catalog_series` sorts
    /// their series.
    fn sources(&self) -> Vec<Source> {
        let ticks = self.ticks.iter().cloned().map(Source::Ticks);
        sorted_sources(bar_sources(&self.bar_types).chain(ticks))
    }
}

/// The slices of `keyed`, those of one key merged where they overlap or
/// touch (Nautilus' `merge_closed_intervals`), each key's in time order,
/// the keys in the order they first come. A key is of one contract.
fn merged<K: PartialEq + Clone>(keyed: impl IntoIterator<Item = (K, Slice)>) -> Vec<(K, Slice)> {
    let mut groups: Vec<(K, InstrumentId, Vec<ClosedInterval>)> = Vec::new();
    for (key, slice) in keyed {
        let interval = ClosedInterval {
            start: UnixNanos::from(slice.from).as_u64(),
            end: UnixNanos::from(slice.to).as_u64(),
        };
        match groups.iter_mut().find(|(of, _, _)| *of == key) {
            Some((_, _, intervals)) => intervals.push(interval),
            None => groups.push((key, slice.instrument_id, vec![interval])),
        }
    }
    groups
        .into_iter()
        .flat_map(|(key, instrument_id, intervals)| {
            merge_closed_intervals(&intervals)
                .into_iter()
                .map(move |interval| {
                    let slice = Slice {
                        instrument_id,
                        from: UnixNanos::from(interval.start).into(),
                        to: UnixNanos::from(interval.end).into(),
                    };
                    (key.clone(), slice)
                })
        })
        .collect()
}

/// A catalog series over the slice its contract stands for its root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesSlice {
    /// The series of the slice's contract: its Nautilus data type and
    /// catalog identifier, as `catalog_series` gives them.
    pub series: (NautilusDataType, String),
    pub slice: Slice,
}

impl SeriesSlice {
    /// `slices`, those of one series merged where they overlap or touch,
    /// each series' in time order, the series in the order they first
    /// come: no part of a series is read twice.
    #[must_use]
    pub fn merged(slices: impl IntoIterator<Item = Self>) -> Vec<Self> {
        merged(slices.into_iter().map(|of| (of.series, of.slice)))
            .into_iter()
            .map(|(series, slice)| Self { series, slice })
            .collect()
    }
}

impl TickSeries {
    /// The catalog series of these ticks: their Nautilus data type, keyed
    /// by the instrument id (custom data too: see `TickSeries`).
    #[must_use]
    pub fn catalog_series(&self) -> (NautilusDataType, String) {
        (self.data.clone(), self.instrument_id.to_string())
    }
}

/// The catalog series the bars of `bar_types` are built from, as Nautilus'
/// data engine builds them: a series is a Nautilus data type and its
/// catalog identifier, the instrument id for ticks and the bar type for
/// bars. External bars are their own series; a composite bar type is built
/// from its source bars (`BarType::composite`), and internal bars from the
/// trades of their instrument if priced LAST, from its quotes otherwise.
/// Each series comes once, sorted by instrument, then identifier.
#[must_use]
pub fn catalog_series(bar_types: &[BarType]) -> Vec<(NautilusDataType, String)> {
    sorted_sources(bar_sources(bar_types))
        .iter()
        .map(Source::series)
        .collect()
}

/// What a catalog series holds: external bars, or the ticks of an
/// instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    Bars(BarType),
    Ticks(TickSeries),
}

impl Source {
    fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::Bars(bar_type) => bar_type.instrument_id(),
            Self::Ticks(tick) => tick.instrument_id,
        }
    }

    /// The same source on `instrument_id`.
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

    /// The catalog series: its Nautilus data type and catalog identifier.
    fn series(&self) -> (NautilusDataType, String) {
        match self {
            Self::Bars(bar_type) => (NautilusDataType::Bar, bar_type.to_string()),
            Self::Ticks(tick) => tick.catalog_series(),
        }
    }
}

/// The source each of `bar_types` is built from (see `catalog_series`).
fn bar_sources(bar_types: &[BarType]) -> impl Iterator<Item = Source> {
    bar_types.iter().map(|bar_type| {
        let source = if bar_type.is_composite() {
            bar_type.composite()
        } else {
            *bar_type
        };
        let instrument_id = source.instrument_id();
        if source.is_externally_aggregated() {
            Source::Bars(source)
        } else if source.spec().price_type == PriceType::Last {
            Source::Ticks(TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id,
            })
        } else {
            Source::Ticks(TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id,
            })
        }
    })
}

/// `sources`, each once, sorted by instrument, then identifier, in the
/// order given otherwise.
fn sorted_sources(sources: impl Iterator<Item = Source>) -> Vec<Source> {
    let mut unique: Vec<Source> = Vec::new();
    for source in sources {
        if !unique.contains(&source) {
            unique.push(source);
        }
    }
    unique.sort_by_cached_key(|source| (source.instrument_id(), source.series().1));
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
            warmup: SignedDuration::from_hours(720),
        };
        let okx = requirements.of_instruments(&[InstrumentId::from("BTC-USDT-SWAP.OKX")]);
        assert_eq!(
            okx,
            DataRequirements {
                bar_types: vec![BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL")],
                ticks: vec![],
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
                warmup: SignedDuration::ZERO,
            }
        );
    }

    #[test]
    fn the_series_of_requirements_are_the_sources_of_their_bars_and_their_ticks_each_once() {
        assert_eq!(
            gold().catalog_series(),
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
    fn rejects_a_name_nautilus_does_not_parse() {
        let err = toml::from_str::<TickSeries>(
            r#"data = "depth"
            instrument_id = "GC.v.0.GLBX""#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("depth"), "{err}");
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
            warmup: SignedDuration::ZERO,
        };
        assert_eq!(
            requirements.instrument_ids(),
            [InstrumentId::from("GC.v.0.GLBX")]
        );
        assert_eq!(
            requirements.catalog_series(),
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
                tick(NautilusDataType::TradeTick, "GCZ6.GLBX"),
            ],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(
            requirements.slices(start, end).unwrap(),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ6.GLBX",
                    "GCZ6.GLBX",
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
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        let roll = "2026-07-29T00:00Z";
        assert_eq!(
            requirements.slices(start, end).unwrap(),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCQ6.GLBX",
                    "GCQ6.GLBX",
                    start,
                    before(roll)
                ),
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ6.GLBX",
                    "GCZ6.GLBX",
                    at(roll),
                    end
                ),
                series_slice(
                    NautilusDataType::OrderBookDepth,
                    "GCQ6.GLBX",
                    "GCQ6.GLBX",
                    start,
                    before(roll)
                ),
                series_slice(
                    NautilusDataType::OrderBookDepth,
                    "GCZ6.GLBX",
                    "GCZ6.GLBX",
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
            warmup: SignedDuration::from_hours(24),
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        let roll = "2026-07-29T00:00Z";
        assert_eq!(
            requirements.slices(start, end).unwrap(),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCQ6.GLBX",
                    "GCQ6.GLBX",
                    start,
                    before(roll)
                ),
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ6.GLBX",
                    "GCZ6.GLBX",
                    at(roll),
                    end
                ),
            ]
        );
    }

    /// The slices of `bar_type` over July and August 2026, across GC's
    /// roll of 29 July.
    fn bar_slices(bar_type: &str) -> anyhow::Result<Vec<SeriesSlice>> {
        DataRequirements {
            bar_types: vec![BarType::from(bar_type)],
            ticks: vec![],
            warmup: SignedDuration::ZERO,
        }
        .slices(at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"))
    }

    #[test]
    fn bars_on_a_root_not_built_from_its_trades_are_refused_naming_the_bar_type() {
        for bar_type in [
            "GC.GLBX-1-MINUTE-MID-INTERNAL",
            "GC.GLBX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            "GC.GLBX-1-MINUTE-LAST-EXTERNAL",
        ] {
            let err = bar_slices(bar_type).unwrap_err().to_string();
            assert!(err.contains(bar_type), "{err}");
        }
    }

    #[test]
    fn bars_on_a_contract_need_not_be_built_from_trades() {
        assert_eq!(
            bar_slices("GCZ6.GLBX-1-MINUTE-MID-INTERNAL").unwrap(),
            [series_slice(
                NautilusDataType::QuoteTick,
                "GCZ6.GLBX",
                "GCZ6.GLBX",
                at("2026-07-01T00:00Z"),
                at("2026-08-31T00:00Z")
            )]
        );
    }

    /// GCQ6.GLBX stands for GC.GLBX up to the roll of 29 July and is read
    /// as itself over the whole span.
    #[test]
    fn a_contract_read_beside_its_root_is_one_slice_of_each_series_over_both() {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                tick(NautilusDataType::TradeTick, "GC.GLBX"),
                tick(NautilusDataType::TradeTick, "GCQ6.GLBX"),
            ],
            warmup: SignedDuration::ZERO,
        };
        let (start, end) = (at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(
            requirements.slices(start, end).unwrap(),
            [
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCQ6.GLBX",
                    "GCQ6.GLBX",
                    start,
                    end
                ),
                series_slice(
                    NautilusDataType::TradeTick,
                    "GCZ6.GLBX",
                    "GCZ6.GLBX",
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
            ticks: vec![tick(NautilusDataType::OrderBookDepth, "GCQ6.GLBX")],
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
                slice("GCQ6.GLBX", start, end),
                slice("GCZ6.GLBX", at("2026-07-29T00:00Z"), end),
            ]
        );
    }

    /// The series of each bar type, as `(data type, identifier)` strings.
    fn series_of(bar_types: &[&str]) -> Vec<(NautilusDataType, String)> {
        let bar_types: Vec<BarType> = bar_types.iter().map(|&s| BarType::from(s)).collect();
        catalog_series(&bar_types)
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
