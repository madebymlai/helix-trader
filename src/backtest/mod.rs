//! Backtests a strategy over the catalog, costs included.
//!
//! This is build step 2. Each run goes through its own Nautilus `BacktestNode`, which is disposed
//! of at the end of the run. A process runs them one after another, never two at once.

mod costs;
mod latency;
mod node;
mod report;
mod trades;

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
    trades::Trades,
};
use crate::{
    rolls::ContinuousBars,
    strategy::{
        TradeConfig, conversions,
        requirements::{CatalogSeries, CustomSeries, DataRequirements, SeriesSlice, TickSeries},
    },
    venues,
};

/// One backtest, over a window of the catalog and with its costs.
///
/// Every field is required.
#[derive(Debug, Clone)]
pub struct Backtest {
    /// The catalog directory. It serves no request, because the strategy warms up on the replay.
    pub catalog: PathBuf,
    /// The internal bar types the strategy trades on, and the external bars of each pair it
    /// converts through (`strategy::requirements::DataRequirements::bar_types`).
    ///
    /// Each of their instruments is a simulated venue. The run replays the series the bars are
    /// built from (`strategy::requirements::catalog_keys`) and the stored books (see `series`).
    pub bar_types: Vec<BarType>,
    /// The tick series the strategy reads as they are (see `series`).
    ///
    /// Each of their instruments is a simulated venue.
    pub ticks: Vec<TickSeries>,
    /// The custom series the strategy reads (`strategy::requirements::DataRequirements::custom`).
    ///
    /// The run replays each row when it is known (`lse::LseRow`: a date's row is known at its end).
    /// The history source of the series' type fills the series, or the catalog does when it is
    /// written from outside helix (`venues::serves_custom`). Custom series bring no instrument.
    /// Nautilus decodes custom data by its type's name, which is registered as the process starts
    /// (`venues::register_custom_data`).
    pub custom: Vec<CustomSeries>,
    pub start: Timestamp,
    pub end: Timestamp,
    /// The commission that each venue's fee model charges (`Commission::fee_model`).
    pub commission: Commission,
    /// The latency of each venue (`Latency::model`).
    ///
    /// With none, a command reaches the venue as it is sent.
    pub latency: Option<Latency>,
    /// The FX pairs the strategy converts through and does not trade
    /// (`TradeConfig::untraded_pairs`).
    ///
    /// They decide no venue's money (see `balance`).
    pub pairs: Vec<InstrumentId>,
    /// The starting balance of each venue that a broker trades (`funds`).
    ///
    /// Any other venue starts with nothing.
    pub balance: Money,
    /// The Nautilus logger. The node installs it, so nothing else may.
    pub logging: LoggerConfig,
}

impl Backtest {
    /// Runs `strategy` over the window and returns the Nautilus result with the costs of the fills.
    ///
    /// Each venue uses its broker's position mode, or the venue default (`venues::oms_type`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The inputs are inconsistent (no instruments, `start` not before `end`).
    /// - The node fails to build or run.
    /// - A fill of an instrument with quotes has none before it.
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<Outcome>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::run_config(self)?;
        node::run(config, node::custom_data(self)?, strategy)
    }

    /// Runs `strategy` as `run` does, for what it does alone.
    ///
    /// A dataset run builds the training table this way (`model::targets`). Every venue hedges, so
    /// the long and short targets settle separately. Each account is frozen at the balance and
    /// holds no margin, so Nautilus' risk engine denies no order for its margin
    /// (`node::dataset_run_config`). The run purges the cache as it goes, so there is no result to
    /// read.
    ///
    /// # Errors
    ///
    /// Returns an error if the inputs are inconsistent or the node fails to build or run.
    pub fn run_dataset<S>(&self, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::dataset_run_config(self)?;
        node::run_dataset(config, node::custom_data(self)?, strategy)
    }

    /// Refuses the run unless its balance and commission suit the instruments `trade` trades.
    ///
    /// Three checks apply:
    /// - For each instrument whose broker's account has no base currency, the balance's currency
    ///   must be the instrument's P&L currency (`refuse_unheld_pnl_currency`).
    /// - The account of each instrument must book the commission (`refuse_unbooked_commission`).
    /// - The pairs of `[trade] conversions` must convert the balance's currency into the P&L
    ///   currency of each instrument (`refuse_balance`).
    ///
    /// A trade sent at a stake is sized on the equity converted into its P&L currency
    /// (`strategy::orders::Trade::send`). On a balance the pairs do not convert, it would send
    /// nothing. A dataset run's balance is checked by `refuse_balance` alone.
    ///
    /// # Errors
    ///
    /// Returns an error if `catalog::instruments`, `refuse_unheld_pnl_currency`,
    /// `refuse_unbooked_commission` or `refuse_balance` fails.
    pub fn refuse_accounts(&self, trade: &TradeConfig) -> anyhow::Result<()> {
        let traded = crate::catalog::instruments(&self.catalog, &trade.instrument_ids())
            .context("reading the instruments traded")?;
        let pairs = crate::catalog::instruments(&self.catalog, trade.conversions.pairs())
            .context("reading the pairs of [trade] conversions")?;
        refuse_unheld_pnl_currency(self.balance, &traded)?;
        refuse_unbooked_commission(self.balance, self.commission, &traded, &pairs)?;
        refuse_balance(self.balance, &traded, &pairs)
    }

    /// Returns the continuous bars of the bar types on roots over the run, adjusted in `mode`.
    ///
    /// The strategy subscribes to them through
    /// `strategy::subscriptions::Subscriptions::continuous`.
    ///
    /// # Errors
    ///
    /// Returns an error if `ContinuousBars::read` fails. It fails when a root has no roll over
    /// `[start, end]`, or a roll has no trade to price it. The config's load
    /// (`strategy::DataConfig`) refuses a bar type on a root not built from trades.
    pub fn continuous_bars(
        &self,
        mode: ContinuousFutureAdjustmentType,
    ) -> anyhow::Result<ContinuousBars> {
        ContinuousBars::read(&self.catalog, &self.bar_types, self.start, self.end, mode)
    }

    /// Returns whether the simulated `venue` starts with the balance.
    ///
    /// It does when a broker trades the venue (`venues::has_broker`) and the strategy reads an
    /// instrument on it besides the untraded `pairs`. For example, IDEALPRO's EUR/USD for gold on
    /// GLBX leaves IDEALPRO with nothing.
    fn funds(&self, venue: Venue) -> bool {
        venues::has_broker(venue)
            && self
                .declared()
                .iter()
                .any(|id| id.venue == venue && !self.pairs.contains(id))
    }

    fn declared(&self) -> BTreeSet<InstrumentId> {
        self.requirements().instrument_ids().into_iter().collect()
    }

    /// A backtest requests no warmup.
    fn requirements(&self) -> DataRequirements {
        DataRequirements {
            bar_types: self.bar_types.clone(),
            ticks: self.ticks.clone(),
            custom: self.custom.clone(),
            warmup: SignedDuration::ZERO,
        }
    }

    /// Returns the catalog series the run replays.
    ///
    /// Each series covers the slice of `[start, end]` in which its contract stands for its
    /// instrument (`strategy::requirements::DataRequirements::slices`). The slices are merged
    /// (`SeriesSlice::merged`) so no part is replayed twice. The series are:
    /// - The series the bars are built from (`strategy::requirements::catalog_keys`), and the
    ///   ticks.
    /// - The book depth of each contract of the bar types' instruments whose catalog stores one.
    /// - The funding of each contract whose catalog stores it, which the venue settles.
    ///
    /// An instrument with no bar source series stored (CME gold's book without its quotes) has its
    /// book replayed and no bar built. An instrument read through ticks alone has no book
    /// replayed.
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

/// Refuses `balance` unless `pairs` convert its currency into the P&L currency of each instrument
/// in `traded`.
///
/// The P&L currency is `Instrument::cost_currency`, and `conversions::reaches` finds the path with
/// Nautilus' own path search. Nautilus' fixed-risk sizer reads the equity as given, so it does not
/// convert a currency the pairs do not reach.
///
/// # Errors
///
/// Returns an error if:
/// - A pair in `pairs` is defined without a base currency, which gives no rate.
/// - The pairs do not reach the P&L currency of an instrument in `traded` from the balance's
///   currency.
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
/// (`Instrument::cost_currency`) of each such instrument in `traded`.
///
/// An instrument qualifies when the broker of its venue keeps an account without a base currency
/// (`venues::has_base_currency`). Nautilus books such an account in each currency apart. It does
/// not book a fill's loss or commission in a currency the account does not hold, but it books a
/// gain (`AccountsManager::update_balance_multi_currency` in Nautilus' portfolio crate, whose
/// branches are "intentionally asymmetric until cross-currency equity tracking is implemented").
/// A dataset run has frozen accounts and a live run has a broker that books its own account, so
/// neither is refused.
///
/// # Errors
///
/// Returns an error if the balance's currency is not the P&L currency of such an instrument.
fn refuse_unheld_pnl_currency(balance: Money, traded: &[InstrumentAny]) -> anyhow::Result<()> {
    for instrument in traded {
        let (pnl, venue) = (instrument.cost_currency(), instrument.id().venue);
        if balance.currency == pnl || venues::has_base_currency(venue) {
            continue;
        }
        anyhow::bail!(
            "the balance {balance} is not in {pnl}, the P&L currency of {}, and the account on \
             {venue} has no base currency, so Nautilus would drop the {pnl} losses; backtest it on \
             a balance in {pnl}",
            instrument.id()
        );
    }
    Ok(())
}

/// Refuses `commission` for a backtest's simulated accounts unless the account of each instrument
/// in `traded` books it.
///
/// Nautilus books nothing of a fill whose commission (`Commission::currency`) the account cannot
/// book, its P&L included (`AccountsManager::update_balances` in Nautilus' portfolio crate):
/// - An account with a base currency (`venues::has_base_currency`), the balance's, converts a
///   commission in another currency, even a zero one, at a rate on the instrument's venue
///   (`update_balance_single_currency`). The rates are those of `pairs`, which the run replays,
///   all on that venue: the config refuses a pair off it (`Conversions::validate`).
/// - An account without one holds the balance's currency alone, and deducts no commission other
///   than zero in another (`update_balance_multi_currency`).
///
/// A dataset run has frozen accounts and a live run has a broker that books its own account, so
/// neither is refused.
///
/// # Errors
///
/// Returns an error if:
/// - The account of an instrument in `traded` cannot book the commission.
/// - `conversions::reaches` fails.
fn refuse_unbooked_commission(
    balance: Money,
    commission: Commission,
    traded: &[InstrumentAny],
    pairs: &[InstrumentAny],
) -> anyhow::Result<()> {
    for instrument in traded {
        let (id, charged) = (instrument.id(), commission.currency(instrument));
        let venue = id.venue;

        if venues::has_base_currency(venue) {
            if !conversions::reaches(pairs, charged, balance.currency)? {
                anyhow::bail!(
                    "the commission {commission} of {id} is in {charged}, which no pair of [trade] \
                     conversions on {venue} converts into {0}, the balance's currency, so Nautilus \
                     would book none of its fills; add a pair of {venue} converting {charged} into \
                     {0} to [trade] conversions",
                    balance.currency
                );
            }
        } else if charged != balance.currency && !commission.is_zero() {
            anyhow::bail!(
                "the commission {commission} of {id} is in {charged}, not in {0}, the balance's \
                 currency, and the account on {venue} has no base currency, so Nautilus would book \
                 none of its fills; backtest it with a commission in {0}",
                balance.currency
            );
        }
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

    // The source bars of the composite hour bars, sorted by instrument with `BTC-USDT` first
    const SWAP_MINUTE_BARS: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";
    const XPERP_MINUTE_BARS: &str = "BTC-USD_UM_XPERP-310404.OKX-1-MINUTE-LAST-EXTERNAL";

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

    fn future(id: &str) -> InstrumentAny {
        let mut future = nautilus_model::instruments::stubs::futures_contract_es(None, None);
        future.id = InstrumentId::from(id);
        InstrumentAny::FuturesContract(future)
    }

    fn cfd(id: &str) -> InstrumentAny {
        let mut gold = nautilus_model::instruments::stubs::cfd_gold();
        gold.id = InstrumentId::from(id);
        InstrumentAny::Cfd(gold)
    }

    fn pair(id: &str) -> InstrumentAny {
        let id = InstrumentId::from(id);
        InstrumentAny::CurrencyPair(nautilus_model::instruments::stubs::default_fx_ccy(
            id.symbol,
            Some(id.venue),
        ))
    }

    fn per_order(amount: &str) -> Commission {
        Commission::PerOrder(Money::from(amount))
    }

    #[rstest]
    #[case::in_the_pnl_currency_of_glbx("100000 USD", per_order("3 USD"), future("GCZ6.GLBX"), &[])]
    #[case::at_a_rate_on_glbx("100000 USD", Commission::Rate(Decimal::ONE), future("GCZ6.GLBX"), &[])]
    #[case::in_the_pnl_currency_of_a_venue_of_no_row(
        "100000 USD",
        per_order("3 USD"),
        future("GCZ6.IDEALPRO"),
        &[]
    )]
    #[case::free_in_another_currency_on_glbx(
        "100000 USD",
        per_order("0 EUR"),
        future("GCZ6.GLBX"),
        &[]
    )]
    #[case::on_ic_through_its_pair(
        "100000 EUR",
        per_order("3 EUR"),
        cfd("XAUUSD.ICMARKETS"),
        &["EUR/USD.ICMARKETS"]
    )]
    #[case::on_ic_charged_in_the_pnl_currency(
        "100000 EUR",
        per_order("3 USD"),
        cfd("XAUUSD.ICMARKETS"),
        &["EUR/USD.ICMARKETS"]
    )]
    #[case::on_ic_charged_in_a_third_currency(
        "100000 USD",
        Commission::PerQuantity(Money::from("0.10 EUR")),
        cfd("XAUUSD.ICMARKETS"),
        &["EUR/USD.ICMARKETS"]
    )]
    #[case::on_pepperstone_at_a_rate(
        "100000 EUR",
        Commission::Rate(Decimal::ONE),
        cfd("XAUUSD.PEPPERSTONE"),
        &["EUR/USD.PEPPERSTONE"]
    )]
    fn a_commission_its_account_books_is_accepted(
        #[case] balance: &str,
        #[case] commission: Commission,
        #[case] instrument: InstrumentAny,
        #[case] pairs: &[&str],
    ) {
        let pairs: Vec<InstrumentAny> = pairs.iter().map(|id| pair(id)).collect();

        refuse_unbooked_commission(Money::from(balance), commission, &[instrument], &pairs)
            .unwrap();
    }

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

    #[rstest]
    fn a_balance_is_refused_for_any_instrument_traded_it_is_not_the_pnl_currency_of() {
        let traded = [cfd("XAUUSD.ICMARKETS"), future("GCZ6.GLBX")];

        let err = refuse_unheld_pnl_currency(Money::from("100000 EUR"), &traded).unwrap_err();

        assert!(err.to_string().contains("GCZ6.GLBX"), "{err}");
    }

    #[rstest]
    #[case::on_ic_with_no_pair(cfd("XAUUSD.ICMARKETS"), "3 EUR", &[])]
    #[case::free_on_ic_with_no_pair(cfd("XAUUSD.ICMARKETS"), "0 EUR", &[])]
    #[case::on_glbx(future("GCZ6.GLBX"), "3 EUR", &[])]
    fn a_commission_its_account_cannot_book_is_refused(
        #[case] instrument: InstrumentAny,
        #[case] commission: &str,
        #[case] pairs: &[&str],
    ) {
        let pairs: Vec<InstrumentAny> = pairs.iter().map(|id| pair(id)).collect();
        let id = instrument.id();

        let err = refuse_unbooked_commission(
            Money::from("100000 USD"),
            per_order(commission),
            &[instrument],
            &pairs,
        )
        .unwrap_err();

        let err = err.to_string();
        assert!(
            [id.to_string().as_str(), "commission", "EUR", "USD"]
                .iter()
                .all(|named| err.contains(named)),
            "{err}"
        );
    }
}
