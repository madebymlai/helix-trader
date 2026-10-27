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
//! `Session`.
//!
//! Strategies trade on bars Nautilus builds, from source bars (IB's
//! 1-minute bars, OKX's candles) or from quotes; `warmup` asks for their
//! history and hands it back, so that no strategy knows how. The catalog
//! stores what their bars are built from (see `catalog_series`).

mod config;
pub mod ema;
#[cfg(gold_scalp)]
pub mod gold_scalp;
mod session;
pub mod warmup;

pub use config::{BarsConfig, CompositeSource};
pub use session::Session;

use jiff::SignedDuration;
use nautilus_model::{
    data::{BarType, NautilusDataType},
    enums::PriceType,
    identifiers::InstrumentId,
};
use serde::Deserialize;

/// What a strategy needs from data, stated by its config: filling the
/// catalog, backtesting and going live all read it from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRequirements {
    /// The internal bar types the strategy reads and warms up on: their
    /// instruments are those it reads (see `instrument_ids`), and the node
    /// sizes its cache to hold their warmup (see `warmup::cache_config`).
    pub bar_types: Vec<BarType>,
    /// The tick series the strategy reads as they are, beside or instead
    /// of bars: fetched by fill-catalog from their instrument's source
    /// (Databento fetches a GLBX instrument's book and trades whatever is
    /// declared) and replayed in a backtest as declared, with no warmup of
    /// their own. Their instruments are read too: live sets up their
    /// clients but subscribes to none of the ticks.
    pub ticks: Vec<TickSeries>,
    /// How far back before the run the warmup history starts.
    pub warmup: SignedDuration,
}

/// A tick series a strategy reads: the ticks of one Nautilus data type of
/// one instrument, `{ data = "quotes", instrument_id = "XAUUSD.ICMARKETS" }`
/// in a TOML config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickSeries {
    pub data: TickData,
    pub instrument_id: InstrumentId,
}

/// The Nautilus data type of a tick series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TickData {
    /// `QuoteTick`s.
    Quotes,
    /// `TradeTick`s.
    Trades,
    /// `OrderBookDepth10` snapshots.
    Depth,
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
                .copied()
                .collect(),
            warmup: self.warmup,
        }
    }

    /// The catalog series read: those the bars are built from (see
    /// `catalog_series`) and the ticks, each once, sorted as
    /// `catalog_series` sorts.
    #[must_use]
    pub fn catalog_series(&self) -> Vec<(NautilusDataType, String)> {
        let ticks = self
            .ticks
            .iter()
            .map(|tick| (tick.instrument_id, tick.catalog_series()));
        sorted_series(bar_sources(&self.bar_types).chain(ticks))
    }
}

impl TickSeries {
    /// The catalog series of these ticks: their Nautilus data type, keyed
    /// by the instrument id.
    #[must_use]
    pub fn catalog_series(&self) -> (NautilusDataType, String) {
        let data_type = match self.data {
            TickData::Quotes => NautilusDataType::QuoteTick,
            TickData::Trades => NautilusDataType::TradeTick,
            TickData::Depth => NautilusDataType::OrderBookDepth,
        };
        (data_type, self.instrument_id.to_string())
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
    sorted_series(bar_sources(bar_types))
}

/// The series each of `bar_types` is built from (see `catalog_series`),
/// with its instrument.
fn bar_sources(
    bar_types: &[BarType],
) -> impl Iterator<Item = (InstrumentId, (NautilusDataType, String))> {
    bar_types.iter().map(|bar_type| {
        let source = if bar_type.is_composite() {
            bar_type.composite()
        } else {
            *bar_type
        };
        let instrument_id = source.instrument_id();
        let series = if source.is_externally_aggregated() {
            (NautilusDataType::Bar, source.to_string())
        } else if source.spec().price_type == PriceType::Last {
            (NautilusDataType::TradeTick, instrument_id.to_string())
        } else {
            (NautilusDataType::QuoteTick, instrument_id.to_string())
        };
        (instrument_id, series)
    })
}

/// `series`, each with its instrument, each once, sorted by instrument,
/// then identifier, in the order given otherwise.
fn sorted_series(
    series: impl Iterator<Item = (InstrumentId, (NautilusDataType, String))>,
) -> Vec<(NautilusDataType, String)> {
    let mut unique: Vec<(InstrumentId, (NautilusDataType, String))> = Vec::new();
    for entry in series {
        if !unique.contains(&entry) {
            unique.push(entry);
        }
    }
    unique.sort_by(|(a, (_, a_id)), (b, (_, b_id))| (a, a_id).cmp(&(b, b_id)));
    unique.into_iter().map(|(_, series)| series).collect()
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

    fn tick(data: TickData, instrument_id: &str) -> TickSeries {
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
                tick(TickData::Quotes, "XAUUSD.ICMARKETS"),
                tick(TickData::Trades, "GC.v.0.GLBX"),
                tick(TickData::Depth, "GC.v.0.GLBX"),
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
                    tick(TickData::Trades, "GC.v.0.GLBX"),
                    tick(TickData::Depth, "GC.v.0.GLBX"),
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
                { data = "depth", instrument_id = "GC.v.0.GLBX" },
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
