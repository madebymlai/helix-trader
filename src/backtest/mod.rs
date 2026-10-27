//! Backtests a strategy over the catalog, costs included.
//!
//! Build step 2: one run per process, through a Nautilus `BacktestNode`.
//! Generic over the strategy: any Nautilus strategy runs, none is named here.

mod costs;
mod coverage;
mod node;
mod report;

use std::{collections::BTreeSet, fmt::Debug, path::PathBuf};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActorNative, component::Component, logging::logger::LoggerConfig,
};
use nautilus_execution::models::fee::FeeModelAny;
use nautilus_model::{
    data::{BarType, NautilusDataType},
    enums::OmsType,
    identifiers::{InstrumentId, Venue},
    types::Money,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::data_path_prefix,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

pub use self::{
    costs::{commission_per_order, commission_rate},
    report::{Outcome, report},
};
use crate::strategy::{TickSeries, catalog_series};

/// One backtest: a window of the catalog, the bar types whose source data
/// is replayed, the tick series replayed as they are, and the costs. Every
/// field is required.
#[derive(Debug, Clone)]
pub struct Backtest {
    /// The catalog directory: instruments, the series the bars are built
    /// from, the instruments' book and funding are read from it, and it
    /// serves the strategy's warmup requests.
    pub catalog: PathBuf,
    /// The internal bar types the strategy trades and warms up on. Their
    /// instruments are those the strategy reads, on one venue or several,
    /// each a simulated venue of the run. The series Nautilus builds the
    /// bars from (see `strategy::catalog_series`: the quotes of their
    /// instrument, or the source bars of a composite bar type) are
    /// replayed, as is the book of each instrument the catalog stores one
    /// of (see `series`). The engine's cache is sized to hold their warmup
    /// bars, which reach the strategy only through it.
    pub bar_types: Vec<BarType>,
    /// The tick series the strategy reads as they are: each replayed as
    /// declared, beside the series of the bar types. Their instruments
    /// are read too, each a simulated venue of the run as well.
    pub ticks: Vec<TickSeries>,
    pub start: Timestamp,
    pub end: Timestamp,
    /// The strategy's warmup: the catalog must cover the series from
    /// `start - warmup`, so that the warmup request is answered.
    pub warmup: SignedDuration,
    /// The commission: `commission_per_order` or `commission_rate`.
    pub fee_model: FeeModelAny,
    /// The starting balance of the account of each venue.
    pub balance: Money,
    /// The leverage of each venue's account on every instrument, positive:
    /// a position locks its value / leverage times the instrument's margin
    /// (Nautilus' default margin model). 1 is no leverage.
    pub leverage: Decimal,
    /// How each simulated venue keeps positions: as the real venue does,
    /// `venue_oms_type`. A strategy leaving its `oms_type` unset follows
    /// it, as it follows the execution client's live.
    pub venue_oms: fn(Venue) -> OmsType,
    /// The Nautilus logger: the node installs it, so nothing else may.
    pub logging: LoggerConfig,
}

/// The venues whose accounts hold each trade as a position of its own:
/// IC Markets, whose cTrader account is hedging (`live/ic`).
const HEDGING_VENUES: &[&str] = &["ICMARKETS"];

/// How the accounts of `venue` keep positions, as the live execution
/// client declares it: `Hedging` for the venues in `HEDGING_VENUES`,
/// `Netting`, one position per instrument, for the others: IB's venues
/// (Nautilus' IB execution client nets) and OKX, whose derivatives
/// Nautilus' OKX client nets, in the net position mode `live` requires.
#[must_use]
pub fn venue_oms_type(venue: Venue) -> OmsType {
    if HEDGING_VENUES.contains(&venue.as_str()) {
        OmsType::Hedging
    } else {
        OmsType::Netting
    }
}

impl Backtest {
    /// Runs `strategy` over the window on a `BacktestNode` and returns the
    /// Nautilus result with the costs of the run's fills.
    ///
    /// # Errors
    ///
    /// Returns an error if the inputs are inconsistent (no instruments, a
    /// negative warmup, `start` not before
    /// `end`, a leverage not positive), if the cache cannot hold the warmup bars of a bar type (see
    /// `warmup::cache_config`), if the catalog misses an instrument or part of a series
    /// over `[start - 1 ns - warmup, end]`, if the node fails to build
    /// or run, or if a fill of an instrument with quotes has none before
    /// it.
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<Outcome>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::run_config(self)?;
        coverage::check(self)?;
        node::run(config, strategy)
    }

    /// When the engine starts: 1 ns before `start`. A minute's quote is
    /// stamped 1 ns before its close, and the engine skips data before its
    /// start, so a run starting on a bar's close would otherwise miss the
    /// quote that closes that bar, and reject an order sent on it. The
    /// strategy's warmup ends here too.
    fn engine_start(&self) -> anyhow::Result<Timestamp> {
        self.start
            .checked_sub(SignedDuration::from_nanos(1))
            .with_context(|| format!("no engine start before {}", self.start))
    }

    /// The instruments of the bar types and of the ticks, each once.
    fn instruments(&self) -> BTreeSet<InstrumentId> {
        let ticks = self.ticks.iter().map(|tick| tick.instrument_id);
        self.bar_types
            .iter()
            .map(BarType::instrument_id)
            .chain(ticks)
            .collect()
    }

    /// The catalog series the run replays: those the bars are built from
    /// (see `strategy::catalog_series`), then the ticks as declared, then
    /// the book depth of each instrument of the bar types whose catalog
    /// stores one, then the funding of each instrument whose catalog
    /// stores funding, which the venue settles on open positions; the book
    /// and the funding keyed by the instrument id; each series once. An
    /// instrument read for its book alone, none of the series its bars
    /// are built from stored (CME gold's book without its quotes), has its
    /// book replayed and its bars left out: no bar is built of it. An
    /// instrument of the ticks alone has no book replayed but a declared
    /// one.
    ///
    /// # Errors
    ///
    /// Returns an error if what the catalog stores cannot be read.
    fn series(&self) -> anyhow::Result<Vec<(NautilusDataType, String)>> {
        let catalog = ParquetDataCatalog::new(&self.catalog, None, None, None, None);
        let stored = |(data_type, identifier): &(NautilusDataType, String)| {
            catalog
                .query_last_timestamp(&data_type.clone().into(), Some(identifier))
                .map(|last| last.is_some())
                .with_context(|| {
                    format!(
                        "reading the stored range of the {} of {identifier}",
                        data_path_prefix(data_type)
                    )
                })
        };
        let bar_instruments: BTreeSet<InstrumentId> =
            self.bar_types.iter().map(BarType::instrument_id).collect();
        let mut books = Vec::new();
        for id in bar_instruments {
            let book = (NautilusDataType::OrderBookDepth, id.to_string());
            if stored(&book)? {
                books.push(book);
            }
        }
        let mut funding = Vec::new();
        for id in self.instruments() {
            let rate = (NautilusDataType::FundingRateUpdate, id.to_string());
            if stored(&rate)? {
                funding.push(rate);
            }
        }
        let mut built = Vec::new();
        for bar_type in &self.bar_types {
            let id = bar_type.instrument_id().to_string();
            let has_book = books.iter().any(|(_, book_of)| *book_of == id);
            let mut has_source = false;
            for source in catalog_series(&[*bar_type]) {
                has_source |= stored(&source)?;
            }
            if has_source || !has_book {
                built.push(*bar_type);
            }
        }
        let ticks = self.ticks.iter().map(TickSeries::catalog_series);
        let mut series: Vec<(NautilusDataType, String)> = Vec::new();
        for entry in catalog_series(&built)
            .into_iter()
            .chain(ticks)
            .chain(books)
            .chain(funding)
        {
            if !series.contains(&entry) {
                series.push(entry);
            }
        }
        Ok(series)
    }
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_common::logging::logger::LoggerConfig;
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{FundingRateUpdate, QuoteTick, stubs::stub_depth10},
        types::{Money, Price, Quantity},
    };

    use super::*;

    const SWAP: &str = "BTC-USDT-SWAP.OKX";
    const XPERP: &str = "BTC-USD_UM_XPERP-310404.OKX";

    /// A backtest on the composite hour bars of `instruments`.
    fn backtest(catalog: &std::path::Path, instruments: &[&str]) -> Backtest {
        Backtest {
            catalog: catalog.to_path_buf(),
            bar_types: instruments
                .iter()
                .map(|id| {
                    BarType::from(format!("{id}-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL").as_str())
                })
                .collect(),
            start: "2026-09-01T00:00:00Z".parse().unwrap(),
            end: "2026-09-30T00:00:00Z".parse().unwrap(),
            ticks: vec![],
            warmup: SignedDuration::ZERO,
            fee_model: commission_rate(Decimal::ZERO).unwrap(),
            balance: Money::from("10000 USDT"),
            leverage: Decimal::ONE,
            venue_oms: venue_oms_type,
            logging: LoggerConfig::default(),
        }
    }

    #[test]
    fn ib_venues_net_positions() {
        for venue in ["IBIS", "CME", "NASDAQ", "LSEETF", "IDEALPRO"] {
            assert_eq!(
                venue_oms_type(Venue::from(venue)),
                OmsType::Netting,
                "{venue}"
            );
        }
    }

    #[test]
    fn okx_nets_positions() {
        assert_eq!(venue_oms_type(Venue::from("OKX")), OmsType::Netting);
    }

    #[test]
    fn ic_markets_hedges_positions() {
        assert_eq!(venue_oms_type(Venue::from("ICMARKETS")), OmsType::Hedging);
    }

    #[test]
    fn the_funding_a_catalog_stores_of_a_traded_instrument_is_replayed_and_none_without() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let ts = UnixNanos::from(1_788_192_000_000_000_000);
        let funding = FundingRateUpdate::new(
            InstrumentId::from(SWAP),
            Decimal::new(1, 4),
            Some(480),
            None,
            ts,
            ts,
        );
        catalog
            .write_to_parquet(&[funding], None, None, None)
            .unwrap();
        let backtest = backtest(dir.path(), &[XPERP, SWAP]);
        let mut expected = catalog_series(&backtest.bar_types);
        expected.push((NautilusDataType::FundingRateUpdate, SWAP.to_string()));
        assert_eq!(backtest.series().unwrap(), expected);
    }

    const GOLD: &str = "GC.v.0.GLBX";

    /// A catalog holding a book depth snapshot of CME gold, and its quotes
    /// if `quoted`; a backtest on its MID hour bars.
    fn gold(dir: &std::path::Path, quoted: bool) -> Backtest {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        let id = InstrumentId::from(GOLD);
        let ts = UnixNanos::from(1_788_192_000_000_000_000);
        let mut depth = stub_depth10();
        (depth.instrument_id, depth.ts_event, depth.ts_init) = (id, ts, ts);
        catalog
            .write_to_parquet(&[depth], None, None, None)
            .unwrap();
        if quoted {
            let quote = QuoteTick::new(
                id,
                Price::from("4000.0"),
                Price::from("4000.1"),
                Quantity::from(1),
                Quantity::from(1),
                ts,
                ts,
            );
            catalog
                .write_to_parquet(&[quote], None, None, None)
                .unwrap();
        }
        Backtest {
            bar_types: vec![BarType::from(
                format!("{GOLD}-1-HOUR-MID-INTERNAL").as_str(),
            )],
            ..backtest(dir, &[])
        }
    }

    #[test]
    fn an_instrument_read_for_its_book_alone_replays_its_book_and_no_bar_source() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = gold(dir.path(), false);
        assert_eq!(
            backtest.series().unwrap(),
            [(NautilusDataType::OrderBookDepth, GOLD.to_string())]
        );
    }

    #[test]
    fn the_book_of_an_instrument_is_replayed_beside_the_source_of_its_bars() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = gold(dir.path(), true);
        assert_eq!(
            backtest.series().unwrap(),
            [
                (NautilusDataType::QuoteTick, GOLD.to_string()),
                (NautilusDataType::OrderBookDepth, GOLD.to_string()),
            ]
        );
    }

    #[test]
    fn a_catalog_without_funding_replays_the_series_the_bars_are_built_from() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = backtest(dir.path(), &[XPERP, SWAP]);
        assert_eq!(
            backtest.series().unwrap(),
            catalog_series(&backtest.bar_types)
        );
    }
}
