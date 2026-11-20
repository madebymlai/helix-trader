//! Backtests a strategy over the catalog, costs included.
//!
//! Build step 2: each run goes through a Nautilus `BacktestNode` of its own, disposed at its end. A
//! process runs them one after another, never two at once. Generic over the strategy: any Nautilus
//! strategy runs, none is named here.

mod costs;
mod latency;
mod node;
mod report;

use std::{collections::BTreeSet, fmt::Debug, path::PathBuf};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActorNative, component::Component, logging::logger::LoggerConfig,
};
use nautilus_model::{
    data::{BarType, NautilusDataType},
    enums::ContinuousFutureAdjustmentType,
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::Money,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::data_path_prefix,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};

pub use self::{
    costs::Commission,
    latency::Latency,
    report::{Outcome, report},
};
use crate::{
    rolls::ContinuousBars,
    strategy::{
        TradeConfig, conversions,
        requirements::{CatalogSeries, CustomSeries, DataRequirements, SeriesSlice, TickSeries},
    },
    venues,
};

/// One backtest: a window of the catalog, the bar types whose source data is replayed, the tick
/// series replayed as they are, and the costs. Every field is required.
#[derive(Debug, Clone)]
pub struct Backtest {
    /// The catalog directory: instruments, the series the bars are built from, and the instruments'
    /// book and funding, read over the window alone. It serves no request: the strategy warms up on
    /// the replay.
    pub catalog: PathBuf,
    /// The internal bar types the strategy trades on, and the external bars of a pair it converts
    /// through, an IB pair's BID and ASK bars
    /// (`strategy::requirements::DataRequirements::bar_types`). Their instruments are those the
    /// strategy reads, on one venue or several, each a simulated venue of the run. Replayed are the
    /// series Nautilus builds the bars from (see `strategy::requirements::catalog_keys`: the quotes
    /// of their instrument, the source bars of a composite bar type, or external bars themselves)
    /// and the book of each instrument the catalog stores one of (see `series`).
    pub bar_types: Vec<BarType>,
    /// The tick series the strategy reads as they are: each replayed as declared beside the series
    /// of the bar types, a root's as its contracts' over their slices (see `series`). Their
    /// instruments are read too, each a simulated venue of the run.
    pub ticks: Vec<TickSeries>,
    /// The custom series the strategy reads (see
    /// `strategy::requirements::DataRequirements::custom`): each replayed over its slices of the
    /// window by its data type, a root's as its contracts' series over their slices
    /// (`DataRequirements::custom_slices`), each row when it is known (`lse::LseRow`: a date's at
    /// its end). Its type's history source fills it, or it is the catalog's, written into it from
    /// outside helix (`venues::serves_custom`). They bring no instrument. Nautilus decodes custom
    /// data by its type's name, each type registered as the process starts
    /// (`venues::register_custom_data`).
    pub custom: Vec<CustomSeries>,
    pub start: Timestamp,
    pub end: Timestamp,
    /// The commission the fee model of each venue is made from (`Commission::fee_model`).
    pub commission: Commission,
    /// The latency of each venue, if any, which its latency model is made from (`Latency::model`):
    /// each order command reaches the venue that long after it is sent. With none, each reaches it
    /// as it is sent.
    pub latency: Option<Latency>,
    /// The FX pairs the strategy converts through and does not trade
    /// (`TradeConfig::untraded_pairs`), read through the bar types or the ticks. They decide no
    /// venue's money (see `balance`). A pair it trades is one of its instruments, funding its venue
    /// as any traded instrument.
    pub pairs: Vec<InstrumentId>,
    /// The starting balance of the account of each venue a broker trades. A venue only read, which
    /// no broker trades (`venues::has_broker`), or read for the pairs alone, starts with nothing in
    /// its currency, so that it adds no money (`funds`).
    pub balance: Money,
    /// The Nautilus logger: the node installs it, so nothing else may.
    pub logging: LoggerConfig,
}

impl Backtest {
    /// Runs `strategy` over the window on a `BacktestNode` and returns the Nautilus result with the
    /// costs of the run's fills. Each simulated venue uses its broker's position mode, or the venue
    /// default if no broker trades it (`venues::oms_type`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The inputs are inconsistent (no instruments, `start` not before `end`).
    /// - The node fails to build or run (Nautilus' own errors: none of a data config's instruments
    ///   in the catalog, say; a series without data it only warns of).
    /// - A fill of an instrument with quotes has none before it.
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<Outcome>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::run_config(self)?;
        node::run(config, node::custom_data(self)?, strategy)
    }

    /// Runs `strategy` over the window on a `BacktestNode` as `run` does, for what the strategy
    /// does alone, as a dataset run builds the training table (`model::targets`). Every venue
    /// hedges so the long and short targets settle separately. Each account is frozen at the
    /// balance, whatever the run's trades lose, and margins nothing, so that Nautilus' risk engine
    /// denies no order for its margin, its other checks kept (`node::dataset_run_config`). The
    /// execution engine purges the closed orders and positions from the cache a minute after they
    /// close, so the run takes time in proportion to its days, and there is no result to read from
    /// it.
    ///
    /// # Errors
    ///
    /// Returns an error if the inputs are inconsistent or the node fails to build or run, as `run`
    /// does but for the costs of the fills.
    pub fn run_dataset<S>(&self, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::dataset_run_config(self)?;
        node::run_dataset(config, node::custom_data(self)?, strategy)
    }

    /// Refuses the run unless its balance's currency is the P&L currency of each instrument `trade`
    /// trades whose broker's account has no base currency, which its simulated account would not
    /// book the trades' losses in otherwise (`refuse_unheld_pnl_currency`), and reaches the P&L
    /// currency of each through the pairs it converts through (`[trade] conversions`), all as its
    /// catalog defines them (`catalog::instruments`, `refuse_balance`). A trade sent at a stake is
    /// sized on the account's equity converted into its P&L currency at the pairs' rates
    /// (`strategy::orders::Trade::send`), so on a balance they do not convert it would send
    /// nothing. A pair defined without a base currency, which gives no rate, is refused whatever
    /// the balance. A dataset run's balance is checked by `refuse_balance` alone.
    ///
    /// # Errors
    ///
    /// Returns an error if `catalog::instruments`, `refuse_unheld_pnl_currency` or `refuse_balance`
    /// fails.
    pub fn refuse_balance(&self, trade: &TradeConfig) -> anyhow::Result<()> {
        let traded = crate::catalog::instruments(&self.catalog, &trade.instrument_ids())
            .context("reading the instruments traded")?;
        let pairs = crate::catalog::instruments(&self.catalog, trade.conversions.pairs())
            .context("reading the pairs of [trade] conversions")?;
        refuse_unheld_pnl_currency(self.balance, &traded)?;
        refuse_balance(self.balance, &traded, &pairs)
    }

    /// The continuous bars of the bar types on roots over the run, adjusted in `mode`, their roll
    /// prices read from the catalog (see `rolls::ContinuousBars::read`). The strategy subscribes to
    /// them through `strategy::subscriptions::Subscriptions::continuous`.
    ///
    /// # Errors
    ///
    /// Returns an error if `ContinuousBars::read` fails: a root without a roll over `[start, end]`,
    /// a roll without a trade to price it. A bar type on a root not built from trades is refused by
    /// the config's load (see `strategy::DataConfig`).
    pub fn continuous_bars(
        &self,
        mode: ContinuousFutureAdjustmentType,
    ) -> anyhow::Result<ContinuousBars> {
        ContinuousBars::read(&self.catalog, &self.bar_types, self.start, self.end, mode)
    }

    /// Whether the simulated `venue` starts with the balance: a broker trades it
    /// (`venues::has_broker`) and the strategy reads an instrument on it besides the pairs it does
    /// not trade (`pairs`). EURUSD traded on IC funds IC, while IDEALPRO's EUR/USD for gold on GLBX
    /// leaves IDEALPRO with nothing, as a venue only read.
    fn funds(&self, venue: Venue) -> bool {
        venues::has_broker(venue)
            && self
                .declared()
                .iter()
                .any(|id| id.venue == venue && !self.pairs.contains(id))
    }

    /// The instruments the strategy reads: those of the bar types and of the ticks, roots or plain
    /// instruments, each once.
    fn declared(&self) -> BTreeSet<InstrumentId> {
        self.requirements().instrument_ids().into_iter().collect()
    }

    /// The bar types, the ticks and the custom series as data requirements, which expand the first
    /// two into the contracts standing for their instruments. A backtest requests no warmup.
    fn requirements(&self) -> DataRequirements {
        DataRequirements {
            bar_types: self.bar_types.clone(),
            ticks: self.ticks.clone(),
            custom: self.custom.clone(),
            warmup: SignedDuration::ZERO,
        }
    }

    /// The catalog series the run replays, each over the slice of `[start, end]` its contract
    /// stands for its instrument (see `strategy::requirements::DataRequirements::slices`), the
    /// slices of one series merged (`SeriesSlice::merged`), so no part of a series is replayed
    /// twice. They are:
    /// - Those the bars are built from (see `strategy::requirements::catalog_keys`) and the ticks
    ///   as declared.
    /// - The book depth of each contract of the instruments of the bar types whose catalog stores
    ///   one.
    /// - The funding of each contract whose catalog stores funding, which the venue settles on open
    ///   positions.
    ///
    /// The book and the funding are keyed by the contract's id, over its slices (see
    /// `strategy::requirements::DataRequirements::contract_slices`). A plain instrument is one
    /// slice on itself over the whole span. An instrument read for its book alone, none of the
    /// series its bars are built from stored (CME gold's book without its quotes), has its book
    /// replayed and no bar built of it. An instrument of the ticks alone has no book replayed but a
    /// declared one.
    ///
    /// # Errors
    ///
    /// Returns an error if what the catalog stores cannot be read.
    fn series(&self) -> anyhow::Result<Vec<SeriesSlice>> {
        let (from, to) = (self.start, self.end);
        let catalog = ParquetDataCatalog::new(&self.catalog, None, None, None, None);
        let stored = |series: &SeriesSlice| {
            let (data_type, identifier) = &series.series.key();
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

        // The series of `data_type` of each contract of `ids`
        let of = |data_type: NautilusDataType, ids: &[InstrumentId]| -> Vec<SeriesSlice> {
            requirements
                .of_instruments(ids)
                .contract_slices(from, to)
                .into_iter()
                .map(|slice| SeriesSlice {
                    series: CatalogSeries::Ticks(TickSeries {
                        data: data_type.clone(),
                        instrument_id: slice.instrument_id,
                    }),
                    from: slice.from,
                    to: slice.to,
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

/// Refuses `balance` unless its currency reaches the P&L currency (`Instrument::cost_currency`) of
/// each instrument of `traded` through `pairs`, the definitions of the pairs a strategy converts
/// through (`conversions::reaches`, Nautilus' own path search). The equity a trade is sized on, by
/// Nautilus' fixed-risk sizer, which reads it as given, is converted into its P&L currency at the
/// pairs' rates, and a currency they do not reach is not converted. Without pairs, the balance is
/// in each P&L currency or refused.
///
/// # Errors
///
/// Returns an error if:
/// - A pair of `pairs`, which it names, is defined without a base currency, which gives no rate
///   (`conversions::reaches`).
/// - The pairs do not reach the P&L currency of an instrument of `traded` from the balance's,
///   naming both currencies.
pub fn refuse_balance(
    balance: Money,
    traded: &[InstrumentAny],
    pairs: &[InstrumentAny],
) -> anyhow::Result<()> {
    for instrument in traded {
        let pnl = instrument.cost_currency();
        if conversions::reaches(pairs, balance.currency, pnl)? {
            continue;
        }
        let through = if pairs.is_empty() {
            "no pair".to_string()
        } else {
            let ids: Vec<String> = pairs.iter().map(|pair| pair.id().to_string()).collect();
            format!("none of the pairs [{}]", ids.join(", "))
        };
        anyhow::bail!(
            "the balance {balance} is in {}, which {through} converts into {pnl}, the P&L \
             currency of {}, which the strategy trades: a trade is sized on the equity \
             converted into its P&L currency through the pairs of [trade] conversions",
            balance.currency,
            instrument.id()
        );
    }
    Ok(())
}

/// Refuses `balance` for a backtest's simulated accounts unless its currency is the P&L currency
/// (`Instrument::cost_currency`) of each instrument of `traded` whose venue's broker keeps an
/// account without a base currency (`venues::has_base_currency`). Nautilus books such an account in
/// each currency apart and never funds one currency from another: a fill's loss or commission in a
/// currency the account does not hold is not booked, while a gain is
/// (`AccountsManager::update_balance_multi_currency` in Nautilus' portfolio crate, its branches
/// "intentionally asymmetric until cross-currency equity tracking is implemented"), so a run on a
/// balance in another currency would count the trades' wins and lose their losses. A dataset run,
/// whose accounts are frozen and book nothing, and a live run, whose broker books its own account,
/// are not refused.
///
/// # Errors
///
/// Returns an error if the balance's currency is not the P&L currency of such an instrument, naming
/// the instrument, its venue, the balance's currency, the P&L currency and the Nautilus limitation.
fn refuse_unheld_pnl_currency(balance: Money, traded: &[InstrumentAny]) -> anyhow::Result<()> {
    for instrument in traded {
        let (pnl, venue) = (instrument.cost_currency(), instrument.id().venue);
        if balance.currency == pnl || venues::has_base_currency(venue) {
            continue;
        }
        anyhow::bail!(
            "the balance {balance} is in {}, not in {pnl}, the P&L currency of {}, which the \
             strategy trades on {venue}, whose broker's account has no base currency: Nautilus \
             books a simulated account without one in each currency apart and books no loss or \
             commission in a currency it does not hold, while it books gains \
             (AccountsManager::update_balance_multi_currency in Nautilus' portfolio crate, \
             intentionally asymmetric until cross-currency equity tracking is implemented), so \
             the backtest would drop the {pnl} losses; backtest it on a balance in {pnl}",
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
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;

    const SWAP: &str = "BTC-USDT-SWAP.OKX";
    const XPERP: &str = "BTC-USD_UM_XPERP-310404.OKX";
    /// The source bars of the composite hour bars of `backtest`, sorted by instrument: `BTC-USDT`
    /// before `BTC-USD_`.
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
            latency: None,
            pairs: vec![],
            balance: Money::from("10000 USDT"),
            logging: LoggerConfig::default(),
        }
    }

    /// Each of `series`, of plain instruments, over the whole span of `backtest`: from its start to
    /// its end.
    fn whole(backtest: &Backtest, series: Vec<(NautilusDataType, String)>) -> Vec<SeriesSlice> {
        series
            .into_iter()
            .map(|(data_type, identifier)| {
                let series = if data_type == NautilusDataType::Bar {
                    CatalogSeries::Bars(BarType::from(identifier.as_str()))
                } else {
                    CatalogSeries::Ticks(TickSeries {
                        data: data_type,
                        instrument_id: InstrumentId::from(identifier.as_str()),
                    })
                };
                SeriesSlice {
                    series,
                    from: backtest.start,
                    to: backtest.end,
                }
            })
            .collect()
    }

    /// The bars of the X-Perp's and the swap's minutes, and the swap's funding if the catalog
    /// stores it.
    #[rstest]
    #[case::with_funding(true)]
    #[case::without_funding(false)]
    fn the_funding_a_catalog_stores_of_a_traded_instrument_is_replayed_and_none_without(
        #[case] funded: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let ts = UnixNanos::from(1_788_192_000_000_000_000);

        if funded {
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
        }
        let backtest = backtest(dir.path(), &[XPERP, SWAP]);
        let mut expected = vec![
            (NautilusDataType::Bar, SWAP_MINUTE_BARS.to_string()),
            (NautilusDataType::Bar, XPERP_MINUTE_BARS.to_string()),
        ];

        if funded {
            expected.push((NautilusDataType::FundingRateUpdate, SWAP.to_string()));
        }
        assert_eq!(backtest.series().unwrap(), whole(&backtest, expected));
    }

    const GOLD: &str = "GC.v.0.GLBX";

    /// A catalog holding a book depth snapshot of CME gold, and its quotes if `quoted`; a backtest
    /// on its MID hour bars.
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

    /// Gold read for its book alone replays its book and no bar source; with its quotes stored, its
    /// book is replayed beside them, the source of its MID bars.
    #[rstest]
    #[case::alone(false, vec![(NautilusDataType::OrderBookDepth, GOLD.to_string())])]
    #[case::beside_the_source_of_its_bars(
        true,
        vec![
            (NautilusDataType::QuoteTick, GOLD.to_string()),
            (NautilusDataType::OrderBookDepth, GOLD.to_string()),
        ]
    )]
    fn an_instrument_read_for_its_book_replays_it_beside_any_source_of_its_bars(
        #[case] quoted: bool,
        #[case] replayed: Vec<(NautilusDataType, String)>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let backtest = gold(dir.path(), quoted);
        assert_eq!(backtest.series().unwrap(), whole(&backtest, replayed));
    }

    /// GC.GLBX rolls from GCQ26.GLBX to GCZ26.GLBX at 00:00 UTC on 29 July 2026; its bars are built
    /// from the trades of each contract.
    #[rstest]
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
            series: CatalogSeries::Ticks(TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from(id),
            }),
            from,
            to,
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

    /// GC.GLBX's bars over July and August 2026, a book of August's contract stored, and its book
    /// read as ticks: GC.GLBX rolls from GCQ26.GLBX to GCZ26.GLBX at 00:00 UTC on 29 July 2026.
    #[rstest]
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

        // No trades stored: the bars are left out, read for the book alone
        assert_eq!(
            backtest.series().unwrap(),
            [SeriesSlice {
                series: CatalogSeries::Ticks(TickSeries {
                    data: NautilusDataType::OrderBookDepth,
                    instrument_id: InstrumentId::from("GCQ26.GLBX")
                }),
                from: backtest.start,
                to: backtest.end,
            }]
        );
    }

    /// A futures contract in USD on `id`'s venue, as Databento defines GLBX's.
    fn future(id: &str) -> InstrumentAny {
        let mut future = nautilus_model::instruments::stubs::futures_contract_es(None, None);
        future.id = InstrumentId::from(id);
        InstrumentAny::FuturesContract(future)
    }

    /// Gold in USD as a cTrader broker defines it, a CFD, on `id`'s venue.
    fn cfd(id: &str) -> InstrumentAny {
        let mut gold = nautilus_model::instruments::stubs::cfd_gold();
        gold.id = InstrumentId::from(id);
        InstrumentAny::Cfd(gold)
    }

    /// The rule is the venues table's alone: a balance in the contract's P&L currency is accepted
    /// on a broker without a base currency, any balance on a broker whose account has one, a
    /// cTrader broker's, as Nautilus books it in that base.
    #[rstest]
    #[case::in_the_pnl_currency_of_glbx("100000 USD", future("GCZ6.GLBX"))]
    #[case::in_the_pnl_currency_of_a_venue_of_no_row("100000 USD", future("GCZ6.IDEALPRO"))]
    #[case::on_ic("100000 EUR", cfd("XAUUSD.ICMARKETS"))]
    #[case::on_pepperstone("100000 EUR", cfd("XAUUSD.PEPPERSTONE"))]
    fn a_balance_in_the_pnl_currency_or_on_a_broker_with_a_base_currency_is_accepted(
        #[case] balance: &str,
        #[case] instrument: InstrumentAny,
    ) {
        refuse_unheld_pnl_currency(Money::from(balance), &[instrument]).unwrap();
    }

    /// Every instrument traded is checked, one of them refusing the balance.
    #[rstest]
    fn a_balance_is_refused_for_any_instrument_traded_it_is_not_the_pnl_currency_of() {
        let traded = [cfd("XAUUSD.ICMARKETS"), future("GCZ6.GLBX")];

        let err = refuse_unheld_pnl_currency(Money::from("100000 EUR"), &traded).unwrap_err();

        assert!(err.to_string().contains("GCZ6.GLBX"), "{err}");
    }
}
