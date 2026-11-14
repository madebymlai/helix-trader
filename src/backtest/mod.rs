//! Backtests a strategy over the catalog, costs included.
//!
//! Build step 2: each run through a Nautilus `BacktestNode` of its own,
//! disposed at its end; a process runs them one after another, never two
//! at once.
//! Generic over the strategy: any Nautilus strategy runs, none is named here.

mod costs;
mod node;
mod report;

use std::{
    collections::BTreeSet,
    fmt::Debug,
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActorNative, component::Component, logging::logger::LoggerConfig,
};
use nautilus_model::{
    data::{BarType, DataType, NautilusDataType},
    enums::{ContinuousFutureAdjustmentType, OmsType},
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::Money,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::data_path_prefix,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

pub use self::{
    costs::Commission,
    report::{Outcome, report},
};
use crate::{
    rolls::ContinuousBars,
    strategy::{DataRequirements, SeriesSlice, TickSeries},
};

/// One backtest: a window of the catalog, the bar types whose source data
/// is replayed, the tick series replayed as they are, and the costs. Every
/// field is required.
#[derive(Debug, Clone)]
pub struct Backtest {
    /// The catalog directory: instruments, the series the bars are built
    /// from, the instruments' book and funding are read from it, over the
    /// window alone. It serves no request: the strategy warms up on the
    /// replay.
    pub catalog: PathBuf,
    /// The internal bar types the strategy trades on. Their instruments
    /// are those the strategy reads, on one venue or several, each a
    /// simulated venue of the run. The series Nautilus builds the
    /// bars from (see `strategy::catalog_series`: the quotes of their
    /// instrument, or the source bars of a composite bar type) are
    /// replayed, as is the book of each instrument the catalog stores one
    /// of (see `series`).
    pub bar_types: Vec<BarType>,
    /// The tick series the strategy reads as they are: each replayed as
    /// declared, beside the series of the bar types, a root's as its
    /// contracts' over their slices (see `series`). Their instruments
    /// are read too, each a simulated venue of the run as well.
    pub ticks: Vec<TickSeries>,
    /// The custom data of no instrument the strategy reads (see
    /// `strategy::DataRequirements::custom`): each series replayed over the
    /// window, beside the others, each row when it is known (`lse::LseRow`:
    /// a date's at its end). The run registers the types the venues'
    /// sources serve with Nautilus, which decodes custom data by its type's
    /// name (`venues::register_custom_data`).
    pub custom: Vec<DataType>,
    pub start: Timestamp,
    pub end: Timestamp,
    /// The commission, which the fee model of each venue is made from
    /// (`Commission::fee_model`).
    pub commission: Commission,
    /// The starting balance of the account of each venue a broker trades;
    /// a venue only read, which no broker trades (`venues::has_broker`),
    /// starts with nothing in its currency, so that it adds no money.
    pub balance: Money,
    /// The leverage of each venue's account on every instrument in a
    /// backtest (`run`), positive: a position locks its value / leverage
    /// times the instrument's margin (Nautilus' default margin model). 1 is
    /// no leverage. A dataset run's accounts (`run_dataset`) margin
    /// nothing, whatever it is.
    pub leverage: Decimal,
    /// How each simulated venue keeps positions: a normal run passes the
    /// real venue's, `venues::oms_type`, and a dataset run hedging on every
    /// venue. A strategy leaving its `oms_type` unset follows it, as it
    /// follows the execution client's live.
    pub venue_oms: fn(Venue) -> OmsType,
    /// The Nautilus logger: the node installs it, so nothing else may.
    pub logging: LoggerConfig,
}

impl Backtest {
    /// Runs `strategy` over the window on a `BacktestNode` and returns the
    /// Nautilus result with the costs of the run's fills.
    ///
    /// # Errors
    ///
    /// Returns an error if the inputs are inconsistent (no instruments,
    /// `start` not before `end`, a leverage not positive), if the node
    /// fails to build or run (Nautilus' own errors: none of a data
    /// config's instruments in the catalog, say; a series without data it
    /// only warns of), or if a fill of an instrument with quotes has none before it.
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<Outcome>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::run_config(self)?;
        node::run(config, node::custom_data(self)?, strategy)
    }

    /// Runs `strategy` over the window on a `BacktestNode` as `run` does,
    /// for what the strategy does alone, as a dataset run builds the
    /// training table (`model::targets`): each venue's account is frozen at
    /// the balance, whatever the run's trades lose, and margins nothing, so
    /// that Nautilus' risk engine denies no order for its margin, its
    /// other checks kept (`node::dataset_run_config`); the execution engine
    /// purges the closed orders and positions from the cache a minute after
    /// they close, so that the run takes time in proportion to its days,
    /// and there is no result to read from it.
    ///
    /// # Errors
    ///
    /// Returns an error as `run` does, but for the costs of the fills.
    pub fn run_dataset<S>(&self, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::dataset_run_config(self)?;
        node::run_dataset(config, node::custom_data(self)?, strategy)
    }

    /// Refuses the run unless its balance is in the P&L currency of each
    /// instrument of `traded`, those the strategy trades, as its catalog
    /// defines them (`traded_instruments`, `refuse_balance`): a trade sent
    /// at a stake is sized on the equity in
    /// its P&L currency alone, which is not converted
    /// (`strategy::orders::Trade::send`), so on a balance in another it
    /// would send nothing.
    ///
    /// # Errors
    ///
    /// Returns an error as `traded_instruments` and `refuse_balance` do.
    pub fn refuse_balance(&self, traded: &[InstrumentId]) -> anyhow::Result<()> {
        refuse_balance(self.balance, &traded_instruments(&self.catalog, traded)?)
    }

    /// The continuous bars of the bar types on roots over the run,
    /// adjusted in `mode`, their roll prices read from the catalog (see
    /// `rolls::ContinuousBars::read`): the strategy passes them to
    /// `warmup::subscribe`.
    ///
    /// # Errors
    ///
    /// Returns an error as `ContinuousBars::read` does: a root without a
    /// roll over `[start, end]`, a roll without a trade to price it. A bar
    /// type on a root not built from trades is refused by the config's
    /// load (see `strategy::DataConfig`).
    pub fn continuous_bars(
        &self,
        mode: ContinuousFutureAdjustmentType,
    ) -> anyhow::Result<ContinuousBars> {
        ContinuousBars::read(&self.catalog, &self.bar_types, self.start, self.end, mode)
    }

    /// The instruments the strategy reads: those of the bar types and of
    /// the ticks, roots or plain instruments, each once.
    fn declared(&self) -> BTreeSet<InstrumentId> {
        self.requirements().instrument_ids().into_iter().collect()
    }

    /// The bar types, the ticks and the custom data as data requirements,
    /// which expand the first two into the contracts standing for their
    /// instruments. A backtest requests no warmup: none.
    fn requirements(&self) -> DataRequirements {
        DataRequirements {
            bar_types: self.bar_types.clone(),
            ticks: self.ticks.clone(),
            custom: self.custom.clone(),
            warmup: SignedDuration::ZERO,
        }
    }

    /// The catalog series the run replays, each over the slice of
    /// `[start, end]` its contract stands for its instrument (see
    /// `strategy::DataRequirements::slices`), the slices of one series
    /// merged (`SeriesSlice::merged`): those the bars are built from (see
    /// `strategy::catalog_series`) and the ticks as declared, then the
    /// book depth of each contract of the instruments of the bar types
    /// whose catalog stores one, then the funding of each contract whose
    /// catalog stores funding, which the venue settles on open positions;
    /// the book and the funding keyed by the contract's id, over its
    /// slices (see `strategy::DataRequirements::contract_slices`); no part
    /// of a series twice. A plain instrument is one slice on itself over the
    /// whole span. An instrument read for its book alone, none of the
    /// series its bars are built from stored (CME gold's book without its
    /// quotes), has its book replayed and its bars left out: no bar is
    /// built of it. An instrument of the ticks alone has no book replayed
    /// but a declared one.
    ///
    /// # Errors
    ///
    /// Returns an error if what the catalog stores cannot be read.
    fn series(&self) -> anyhow::Result<Vec<SeriesSlice>> {
        let (from, to) = (self.start, self.end);
        let catalog = ParquetDataCatalog::new(&self.catalog, None, None, None, None);
        let stored = |series: &SeriesSlice| {
            let (data_type, identifier) = &series.series;
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
        let requirements = self.requirements();
        // The series of `data_type` of each contract of `ids`.
        let of = |data_type: NautilusDataType, ids: &[InstrumentId]| -> Vec<SeriesSlice> {
            requirements
                .of_instruments(ids)
                .contract_slices(from, to)
                .into_iter()
                .map(|slice| SeriesSlice {
                    series: (data_type.clone(), slice.instrument_id.to_string()),
                    slice,
                })
                .collect()
        };
        let read = |bar_types: Vec<BarType>, ticks: Vec<TickSeries>| {
            DataRequirements {
                bar_types,
                ticks,
                custom: vec![],
                warmup: SignedDuration::ZERO,
            }
            .slices(from, to)
        };
        let bar_instruments: Vec<InstrumentId> =
            self.bar_types.iter().map(BarType::instrument_id).collect();
        let mut books = Vec::new();
        for book in of(NautilusDataType::OrderBookDepth, &bar_instruments) {
            if stored(&book)? {
                books.push(book);
            }
        }
        let mut funding = Vec::new();
        for rate in of(
            NautilusDataType::FundingRateUpdate,
            &requirements.instrument_ids(),
        ) {
            if stored(&rate)? {
                funding.push(rate);
            }
        }
        let mut built = Vec::new();
        for bar_type in &self.bar_types {
            let has_book = of(
                NautilusDataType::OrderBookDepth,
                &[bar_type.instrument_id()],
            )
            .iter()
            .any(|book| books.iter().any(|stored| stored.series == book.series));
            let mut has_source = false;
            for source in read(vec![*bar_type], vec![]) {
                has_source |= stored(&source)?;
            }
            if has_source || !has_book {
                built.push(*bar_type);
            }
        }
        Ok(SeriesSlice::merged(
            read(built, self.ticks.clone())
                .into_iter()
                .chain(books)
                .chain(funding),
        ))
    }
}

/// The instruments of `traded`, those a strategy trades, as the catalog at
/// `catalog` defines them. The catalog is opened through Nautilus'
/// `ParquetDataCatalog::from_uri`, whose error a missing directory is,
/// where `ParquetDataCatalog::new` panics.
///
/// # Errors
///
/// Returns an error if the catalog cannot be opened or read, or an
/// instrument of `traded` is not in it.
pub fn traded_instruments(
    catalog: &Path,
    traded: &[InstrumentId],
) -> anyhow::Result<Vec<InstrumentAny>> {
    let ids: Vec<String> = traded.iter().map(ToString::to_string).collect();
    let defined = ParquetDataCatalog::from_uri(&catalog.to_string_lossy(), None, None, None, None)
        .with_context(|| format!("opening the catalog {}", catalog.display()))?
        .instruments(Some(&ids), None, None)
        .with_context(|| format!("reading {} from the catalog", ids.join(", ")))?;
    traded
        .iter()
        .map(|&id| {
            defined
                .iter()
                .find(|instrument| instrument.id() == id)
                .cloned()
                .with_context(|| {
                    format!(
                        "the traded instrument {id} is not in the catalog {}",
                        catalog.display()
                    )
                })
        })
        .collect()
}

/// Refuses `balance` unless it is in the P&L currency
/// (`Instrument::cost_currency`) of each instrument of `traded`: the
/// equity a trade is sized on, by Nautilus' fixed-risk sizer, which reads
/// it as given, is not converted.
///
/// # Errors
///
/// Returns an error if the P&L of an instrument of `traded` is in another
/// currency than the balance.
pub fn refuse_balance(balance: Money, traded: &[InstrumentAny]) -> anyhow::Result<()> {
    for instrument in traded {
        let pnl = instrument.cost_currency();
        ensure!(
            pnl == balance.currency,
            "the balance {balance} is in {}, the P&L of {}, which the strategy trades, in \
             {pnl}: a trade is sized on the equity in its P&L currency, which is not converted",
            balance.currency,
            instrument.id()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_common::logging::logger::LoggerConfig;
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{FundingRateUpdate, QuoteTick, stubs::stub_depth10},
        types::{Money, Price, Quantity},
    };

    use super::*;
    use crate::rolls::Slice;

    const SWAP: &str = "BTC-USDT-SWAP.OKX";
    const XPERP: &str = "BTC-USD_UM_XPERP-310404.OKX";
    /// The source bars of the composite hour bars of `backtest`, sorted by
    /// instrument: `BTC-USDT` before `BTC-USD_`.
    const SWAP_MINUTE_BARS: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";
    const XPERP_MINUTE_BARS: &str = "BTC-USD_UM_XPERP-310404.OKX-1-MINUTE-LAST-EXTERNAL";

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
            custom: vec![],
            commission: Commission::Rate(Decimal::ZERO),
            balance: Money::from("10000 USDT"),
            leverage: Decimal::ONE,
            venue_oms: crate::venues::oms_type,
            logging: LoggerConfig::default(),
        }
    }

    /// Each of `series`, of plain instruments, over the whole span of
    /// `backtest`: from its start to its end.
    fn whole(backtest: &Backtest, series: Vec<(NautilusDataType, String)>) -> Vec<SeriesSlice> {
        series
            .into_iter()
            .map(|(data_type, identifier)| {
                let instrument_id = if data_type == NautilusDataType::Bar {
                    BarType::from(identifier.as_str()).instrument_id()
                } else {
                    InstrumentId::from(identifier.as_str())
                };
                SeriesSlice {
                    series: (data_type, identifier),
                    slice: Slice {
                        instrument_id,
                        from: backtest.start,
                        to: backtest.end,
                    },
                }
            })
            .collect()
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
        let expected = vec![
            (NautilusDataType::Bar, SWAP_MINUTE_BARS.to_string()),
            (NautilusDataType::Bar, XPERP_MINUTE_BARS.to_string()),
            (NautilusDataType::FundingRateUpdate, SWAP.to_string()),
        ];
        assert_eq!(backtest.series().unwrap(), whole(&backtest, expected));
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
            whole(
                &backtest,
                vec![(NautilusDataType::OrderBookDepth, GOLD.to_string())]
            )
        );
    }

    #[test]
    fn the_book_of_an_instrument_is_replayed_beside_the_source_of_its_bars() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = gold(dir.path(), true);
        assert_eq!(
            backtest.series().unwrap(),
            whole(
                &backtest,
                vec![
                    (NautilusDataType::QuoteTick, GOLD.to_string()),
                    (NautilusDataType::OrderBookDepth, GOLD.to_string()),
                ]
            )
        );
    }

    /// GC.GLBX rolls from GCQ26.GLBX to GCZ26.GLBX at 00:00 UTC on 29 July
    /// 2026; its bars are built from the trades of each contract.
    #[test]
    fn the_trades_of_a_contract_read_beside_the_bars_of_its_root_are_replayed_once() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![BarType::from("GC.GLBX-1-HOUR-LAST-INTERNAL")],
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from("GCQ26.GLBX"),
            }],
            start: "2026-07-20T00:00:00Z".parse().unwrap(),
            end: "2026-08-10T00:00:00Z".parse().unwrap(),
            ..backtest(dir.path(), &[])
        };
        let roll: Timestamp = "2026-07-29T00:00:00Z".parse().unwrap();
        let trades = |id: &str, from, to| SeriesSlice {
            series: (NautilusDataType::TradeTick, id.to_string()),
            slice: Slice {
                instrument_id: InstrumentId::from(id),
                from,
                to,
            },
        };
        let from = backtest.start;
        assert_eq!(
            backtest.series().unwrap(),
            [
                trades("GCQ26.GLBX", from, backtest.end),
                trades("GCZ26.GLBX", roll, backtest.end),
            ]
        );
    }

    /// GC.GLBX's bars over July and August 2026, a book of August's
    /// contract stored, and its book read as ticks: GC.GLBX rolls from
    /// GCQ26.GLBX to GCZ26.GLBX at 00:00 UTC on 29 July 2026.
    #[test]
    fn the_book_of_a_contract_read_beside_the_bars_of_its_root_is_replayed_once() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let ts = UnixNanos::from(1_785_000_000_000_000_000);
        let mut depth = stub_depth10();
        (depth.instrument_id, depth.ts_event, depth.ts_init) =
            (InstrumentId::from("GCQ26.GLBX"), ts, ts);
        catalog
            .write_to_parquet(&[depth], None, None, None)
            .unwrap();
        let backtest = Backtest {
            bar_types: vec![BarType::from("GC.GLBX-1-HOUR-LAST-INTERNAL")],
            ticks: vec![TickSeries {
                data: NautilusDataType::OrderBookDepth,
                instrument_id: InstrumentId::from("GCQ26.GLBX"),
            }],
            start: "2026-07-20T00:00:00Z".parse().unwrap(),
            end: "2026-08-10T00:00:00Z".parse().unwrap(),
            ..backtest(dir.path(), &[])
        };
        // No trades stored: the bars are left out, read for the book alone.
        assert_eq!(
            backtest.series().unwrap(),
            [SeriesSlice {
                series: (NautilusDataType::OrderBookDepth, "GCQ26.GLBX".to_string()),
                slice: Slice {
                    instrument_id: InstrumentId::from("GCQ26.GLBX"),
                    from: backtest.start,
                    to: backtest.end,
                },
            }]
        );
    }

    #[test]
    fn a_catalog_without_funding_replays_the_series_the_bars_are_built_from() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = backtest(dir.path(), &[XPERP, SWAP]);
        let expected = vec![
            (NautilusDataType::Bar, SWAP_MINUTE_BARS.to_string()),
            (NautilusDataType::Bar, XPERP_MINUTE_BARS.to_string()),
        ];
        assert_eq!(backtest.series().unwrap(), whole(&backtest, expected));
    }
}
