//! Runs a backtest over the catalog.
//!
//! This module builds a Nautilus `BacktestRunConfig` from three parts: the venue
//! (`BacktestVenueConfig`), the catalog data (`BacktestDataConfig`, one per series the run
//! replays) and the engine config. It runs the config on a `BacktestNode` in three steps: `build`,
//! add the strategy through `get_engine_mut`, then `run`. The run keeps the engine, so the report
//! reads the fills from its cache. The run then disposes of the engine.
//!
//! `BacktestDataConfig` refuses custom data. So the run reads custom data from the catalog itself
//! (`custom_data`) and adds it to the engine through `add_data` before `run`. The engine replays
//! it in time order with the rest. A streaming run clears the engine's data after each chunk,
//! which drops what was added before. So a run with custom data loads all its data at once, with
//! no `chunk_size`.

use std::{collections::BTreeSet, fmt::Debug, sync::Arc};

use anyhow::Context;
use nautilus_analysis::statistics::max_drawdown::MaxDrawdown;
use nautilus_backtest::{
    config::{BacktestDataConfig, BacktestEngineConfig, BacktestRunConfig, BacktestVenueConfig},
    node::BacktestNode,
    result::BacktestResult,
};
use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_core::{Params, UnixNanos};
use nautilus_execution::engine::config::ExecutionEngineConfig;
use nautilus_model::{
    data::{Data, DataBatch, NautilusDataType, QuoteTick},
    enums::{AccountType, BookType, OmsType},
    identifiers::Venue,
    types::Money,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::CatalogQuery,
    config::DataCatalogConfig,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

use super::{Backtest, Latency, report::Outcome};
use crate::{
    catalog::catalog_config,
    strategy::{
        requirements::{CatalogSeries, CustomSlice, SeriesSlice},
        warmup,
    },
    venues,
};

const RUN_ID: &str = "backtest";

/// The number of data points loaded and replayed at a time (Nautilus' streaming mode).
///
/// A day of CME gold's book depth is 7.4 million snapshots, which is beyond 16 GB loaded at once.
const CHUNK_SIZE: usize = 100_000;

/// How often a dataset run purges closed orders and positions from the cache, in minutes.
///
/// This is also how long after closing they are purged (live keeps them a week: `live::node`). The
/// run reads targets from events, never from the cache. Closed orders and positions kept in the
/// cache slow the run more than in proportion to its days. 16 days of quotes every 5 s and targets
/// every 15 minutes took 70 s against 25 s purged, and 2 days took 4.2 s against 3.1 s. The timer
/// also releases every command due at a latency (`Latency::dataset_delay`).
pub(super) const DATASET_PURGE_MINS: u32 = 1;

/// The leverage of a dataset run's accounts, which is the largest value a `Decimal` holds.
///
/// Nautilus' default margin model computes an order's margin as the notional divided by the
/// leverage, times the instrument's margin (`LeveragedMarginModel`,
/// model/src/accounts/margin_model.rs). At this leverage, every order's margin rounds to nothing.
/// The risk engine's `check_margin` (risk/src/engine/mod.rs) then denies no order, and its other
/// checks stay. A dataset run opens a long and a short at every decision, which no account trades.
/// The strategy's backtest and the live venue check that an account can margin a trade. Nautilus
/// has no margin-free model for a `BacktestVenueConfig` (`MarginModelAny`).
const DATASET_LEVERAGE: Decimal = Decimal::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Run {
    /// Each venue's account is unleveraged, and its balance moves with the run's P&L.
    Backtest,
    /// A dataset run (`Backtest::run_dataset`).
    ///
    /// Each account is frozen at the run's balance and holds no margin (`DATASET_LEVERAGE`). The
    /// run purges closed orders and positions every `DATASET_PURGE_MINS`, that long after they
    /// close.
    Dataset,
}

/// Returns the run config of `backtest`.
///
/// The config has one simulated venue per venue of the instruments. Each venue keeps positions as
/// `venues::oms_type` specifies. Each venue has the run's balance, or nothing if no broker trades
/// it or the run reads its pairs alone on it (`Backtest::funds`).
///
/// The data is `Backtest::series` over `[start, end]`, the only data replayed. No catalog is
/// registered in the engine, and the strategy warms up on the replay.
///
/// The data engine builds time bars as late after their close as a live node does
/// (`warmup::data_engine_config`). The run streams the data `CHUNK_SIZE` points at a time, unless
/// it replays a custom series (see `custom_data`), which it loads at once. Build and run errors
/// are raised, not only logged. The engine outlives the run.
pub(super) fn run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    config(backtest, Run::Backtest)
}

/// Returns the run config of a dataset run of `backtest` (`Run::Dataset`).
pub(super) fn dataset_run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    config(backtest, Run::Dataset)
}

fn config(backtest: &Backtest, run: Run) -> anyhow::Result<BacktestRunConfig> {
    // Nautilus checks only the engine start against the end, so a run starting on its end gets
    // through.
    let (start, end) = (backtest.start, backtest.end);
    anyhow::ensure!(
        start < end,
        "backtest start {start} must be before its end {end}"
    );

    let venues: BTreeSet<Venue> = backtest.declared().iter().map(|id| id.venue).collect();
    anyhow::ensure!(!venues.is_empty(), "no instruments");
    let catalog = catalog_config(&backtest.catalog)?;
    let venues = venues
        .into_iter()
        .map(|venue| venue_config(backtest, venue, run))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let data = backtest
        .series()?
        .into_iter()
        .map(|series| data_config(&catalog, series))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let exec_engine = match run {
        Run::Backtest => None,
        Run::Dataset => Some(
            ExecutionEngineConfig::builder()
                .purge_closed_orders_interval_mins(DATASET_PURGE_MINS)
                .purge_closed_orders_buffer_mins(DATASET_PURGE_MINS)
                .purge_closed_positions_interval_mins(DATASET_PURGE_MINS)
                .purge_closed_positions_buffer_mins(DATASET_PURGE_MINS)
                .build()?,
        ),
    };
    let engine_config = BacktestEngineConfig::builder()
        .data_engine(warmup::data_engine_config(&backtest.bar_types))
        .maybe_exec_engine(exec_engine)
        .logging(backtest.logging.clone())
        .build();
    Ok(BacktestRunConfig::builder()
        .id(RUN_ID.to_string())
        .venues(venues)
        .data(data)
        .engine(engine_config)
        .start(UnixNanos::from(start))
        .end(UnixNanos::from(end))
        .maybe_chunk_size(backtest.custom.is_empty().then_some(CHUNK_SIZE))
        .raise_exception(true)
        .dispose_on_completion(false)
        .build()?)
}

/// Returns the simulated `venue` of `backtest` for `run`.
///
/// The venue is a margin account that fills on the top of the book on Nautilus' default fill
/// model:
/// - A market order fills at the quote, or on bars at the last price, with no slippage.
/// - A limit order fills whenever touched.
/// - At a latency, an order fills at the first settlement point after its arrival.
///
/// The account holds the run's balance if `Backtest::funds`, else nothing. A backtest's account
/// has the balance's currency as its base if its broker's account has one
/// (`venues::has_base_currency`, a cTrader broker's). Nautilus then books margins, commissions and
/// P&L in that currency, as the live account does. Otherwise the account has no base currency, and
/// each currency is held apart.
///
/// A backtest's account is unleveraged. A position locks its instrument's margin, which holds the
/// account's (IC's definitions the broker's, OKX's the account's leverage: `venues::okx`; IB's lock
/// nothing).
///
/// A dataset run's account is frozen (Nautilus' `frozen_account`). The simulated exchange adjusts
/// none of its balances (`adjust_account_balance`, backtest/src/exchange.rs). The portfolio locks
/// no margin of an account that does not calculate its state (portfolio/src/portfolio.rs). So
/// trades that lose leave its balance whole. It has no base currency on any venue. Its trades are
/// sized at a fixed equity (`strategy::orders::Trade::send_at`), so a base would add conversions
/// nothing reads.
fn venue_config(
    backtest: &Backtest,
    venue: Venue,
    run: Run,
) -> anyhow::Result<BacktestVenueConfig> {
    let balance = if backtest.funds(venue) {
        backtest.balance
    } else {
        Money::zero(backtest.balance.currency)
    };
    let (oms_type, leverage) = match run {
        Run::Backtest => (venues::oms_type(venue), Decimal::ONE),
        Run::Dataset => (OmsType::Hedging, DATASET_LEVERAGE),
    };
    let base_currency = (run == Run::Backtest && venues::has_base_currency(venue))
        .then_some(backtest.balance.currency);
    Ok(BacktestVenueConfig::builder()
        .name(venue.as_str())
        .oms_type(oms_type)
        .account_type(AccountType::Margin)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec![balance.to_string()])
        .maybe_base_currency(base_currency)
        .default_leverage(leverage)
        .frozen_account(run == Run::Dataset)
        .fee_model(backtest.commission.fee_model()?)
        .maybe_latency_model(backtest.latency.map(Latency::model))
        .build()?)
}

/// Returns the `BacktestDataConfig` that loads `series` from `catalog` over its slice.
///
/// Both ends of the slice are included, so a contract of a root is replayed only while it stands
/// for the root.
fn data_config(
    catalog: &DataCatalogConfig,
    series: SeriesSlice,
) -> anyhow::Result<BacktestDataConfig> {
    let SeriesSlice { series, from, to } = series;
    let builder = BacktestDataConfig::builder()
        .catalog(catalog.clone())
        .instrument_id(series.instrument_id())
        .start_time(UnixNanos::from(from))
        .end_time(UnixNanos::from(to))
        // One stream over the series' files, as the catalog queries, because Nautilus' default
        // opens a prefetching stream per file before the first chunk, which is tens of GB over
        // months of MBP-10.
        .optimize_file_loading(true);
    let config = match series {
        CatalogSeries::Bars(bar) => builder
            .data_type(NautilusDataType::Bar)
            .bar_spec(bar.spec())
            .build(),
        CatalogSeries::Ticks(tick) => builder.data_type(tick.data).build(),
    };
    Ok(config?)
}

/// Returns the custom series of `backtest` (`Backtest::custom`), read from its catalog.
///
/// The read covers the slices of the run's window, with both ends included. It returns the data
/// the node cannot load (see `run_config`). The series of a root is read contract by contract
/// (`DataRequirements::custom_slices`).
///
/// A slice in which the catalog holds nothing gets a warning, as `BacktestNode` warns for any
/// other series.
///
/// Each type must be registered before the run, as Nautilus decodes it by name
/// (`nautilus_serialization::ensure_custom_data_registered`). helix registers its own types as the
/// process starts (`venues::register_custom_data`).
///
/// # Errors
///
/// Returns an error if:
/// - The series cannot be worked out.
/// - The catalog cannot be read or a stored type is not registered. The error names the series.
/// - A row of a custom series is stored under its identifier as another data type. The error names
///   both. Nautilus publishes each row on its stored data type's topic, so the strategy would get
///   none of the series, or part of it, and fill-catalog, which reads the catalog by identifier,
///   would find nothing missing.
pub(super) fn custom_data(backtest: &Backtest) -> anyhow::Result<Vec<Data>> {
    let mut catalog = ParquetDataCatalog::builder()
        .uri(&backtest.catalog)
        .build()
        .with_context(|| format!("opening the catalog {}", backtest.catalog.display()))?;
    let mut data = Vec::new();
    let custom = backtest
        .requirements()
        .custom_slices(backtest.start, backtest.end);

    for CustomSlice { series, from, to } in &custom {
        let data_type = series.data_type();
        let read = catalog
            .query_custom_data_dynamic(
                series.type_name(),
                Some(&[series.identifier().to_string()]),
                Some(UnixNanos::from(*from)),
                Some(UnixNanos::from(*to)),
                None,
                None,
                true,
            )
            .with_context(|| format!("reading the {data_type}"))?;

        if read.is_empty() {
            // Warns as `BacktestNode::run_oneshot` does
            log::warn!("No data found for config: {data_type:?}");
            continue;
        }

        // The engine publishes each row on the topic of its stored data type
        if let Some(stored) = read.iter().find_map(|row| match row {
            Data::Custom(custom) if custom.data_type != *data_type => Some(&custom.data_type),
            _ => None,
        }) {
            anyhow::bail!(
                "the catalog stores the rows of {data_type} under its identifier as \
                 {stored}, of other metadata: the run would replay them under that data \
                 type, which its strategy does not read; name the data type they were \
                 filled as, or fill them under another identifier"
            );
        }
        data.extend(read);
    }
    Ok(data)
}

/// Runs `config` with `strategy` on a new `BacktestNode` and returns the result.
///
/// The node's engine gets `custom` added. The result holds the return statistics with
/// `MaxDrawdown` (registered with the portfolio before the run), and the costs of the fills in the
/// engine's cache. The costs are measured on the quotes the run replayed, which the run reads
/// again from the catalog (`replayed_quotes`; the other series are not read).
pub(super) fn run<S>(
    config: BacktestRunConfig,
    custom: Vec<Data>,
    strategy: S,
) -> anyhow::Result<Outcome>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    on_node(config, custom, strategy, |node| {
        let results = node.run()?;
        let [result] = <[BacktestResult; 1]>::try_from(results)
            .map_err(|results| anyhow::anyhow!("{} results from one run", results.len()))?;
        let config = &node.configs()[0];
        let quotes = config
            .data()
            .iter()
            .filter(|data| *data.data_type() == NautilusDataType::QuoteTick)
            .map(|data| replayed_quotes(data, config.start(), config.end()));
        let engine = node.get_engine(RUN_ID).context("no engine for the run")?;
        Outcome::new(result, &engine.kernel().cache.borrow(), quotes)
    })
}

/// Returns the quotes `data` replays in a run from `start` to `end`, in time order.
///
/// The quotes are read a batch at a time (`query_batch_session`), on the query the node streams
/// `data` by (`dispatch_batch_query`, `backtest/src/node.rs`). `BacktestNode::load_data_config`
/// would collect them into memory at once. An error opening the catalog or reading a batch is an
/// item.
fn replayed_quotes(
    data: &BacktestDataConfig,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> Box<dyn Iterator<Item = anyhow::Result<QuoteTick>>> {
    let query = CatalogQuery::new(data.data_type().clone())
        .with_identifiers(data.query_identifiers())
        .with_range(
            start.into_iter().chain(data.start_time()).max(),
            end.into_iter().chain(data.end_time()).min(),
        )
        .with_where_clause(data.filter_expr().map(str::to_string))
        .with_params(Some(Params::new(
            [(
                "optimize_file_loading".to_string(),
                data.optimize_file_loading().into(),
            )]
            .into(),
        )));
    let session = BacktestNode::load_catalog(data)
        .and_then(|mut catalog| catalog.query_batch_session(&query, None));
    let mut session = match session {
        Ok(session) => session,
        Err(e) => return Box::new(std::iter::once(Err(e))),
    };
    let batches = std::iter::from_fn(move || session.next_batch().transpose());
    Box::new(batches.flat_map(|batch| match batch {
        Ok(DataBatch::Quote(quotes)) => quotes.iter().copied().map(Ok).collect(),
        Ok(_) => vec![Err(anyhow::anyhow!("a quote query yielded other data"))],
        Err(e) => vec![Err(e)],
    }))
}

/// Runs `config` as `run` does, for what the strategy does alone.
///
/// A dataset run purges its cache as it goes, so it has no result to read.
pub(super) fn run_dataset<S>(
    config: BacktestRunConfig,
    custom: Vec<Data>,
    strategy: S,
) -> anyhow::Result<()>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    on_node(config, custom, strategy, |node| node.run().map(drop))
}

/// Runs `run` on a new `BacktestNode` of `config`, then disposes of the node whatever came of it.
///
/// Nautilus requires the disposal before the next node starts on the thread
/// (`concepts/architecture.md`, "Processes and threads"). A model backtest goes on to further
/// nodes after a failed dataset run.
fn on_node<S, T>(
    config: BacktestRunConfig,
    custom: Vec<Data>,
    strategy: S,
    run: impl FnOnce(&mut BacktestNode) -> anyhow::Result<T>,
) -> anyhow::Result<T>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    let mut node = BacktestNode::new(vec![config])?;
    let outcome = build(&mut node, custom, strategy).and_then(|()| run(&mut node));
    node.dispose();
    outcome
}

fn build<S>(node: &mut BacktestNode, custom: Vec<Data>, strategy: S) -> anyhow::Result<()>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    node.build()?;
    let engine = node
        .get_engine_mut(RUN_ID)
        .context("no engine for the run")?;
    engine
        .kernel_mut()
        .portfolio
        .borrow_mut()
        .register_statistic(Arc::new(MaxDrawdown::new()));
    engine.add_strategy(strategy)?;
    if !custom.is_empty() {
        engine.add_data(custom, None, false, true)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use jiff::{SignedDuration, Timestamp};
    use nautilus_common::{
        logging::logger::LoggerConfig,
        messages::execution::{CancelAllOrders, TradingCommand},
    };
    use nautilus_core::UUID4;
    use nautilus_execution::models::{
        fee::FeeModelAny,
        latency::{LatencyModel, LatencyModelAny},
    };
    use nautilus_model::{
        data::BarType,
        identifiers::{InstrumentId, StrategyId, TraderId},
        types::{Currency, Money},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{backtest::Commission, strategy::requirements::CustomSeries};

    fn backtest(catalog: &Path) -> Backtest {
        Backtest {
            catalog: catalog.to_path_buf(),
            start: "2026-07-01T00:00:00Z".parse().unwrap(),
            end: "2026-10-01T00:00:00Z".parse().unwrap(),
            bar_types: vec![
                BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL"),
                BarType::from("EUNL.IBIS-1-HOUR-MID-INTERNAL"),
            ],
            ticks: vec![],
            custom: vec![],
            commission: Commission::PerOrder(Money::from("3 EUR")),
            latency: None,
            pairs: vec![],
            balance: Money::from("10000 EUR"),
            logging: LoggerConfig {
                print_config: true,
                ..LoggerConfig::default()
            },
        }
    }

    #[rstest]
    fn one_margin_venue_named_after_the_instruments() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let [venue] = config.venues() else {
            panic!("{:?}", config.venues())
        };
        assert_eq!(venue.name(), "IBIS");
        assert_eq!(venue.account_type(), AccountType::Margin);
        assert_eq!(venue.book_type(), BookType::L1_MBP);
    }

    #[rstest]
    fn each_series_is_streamed_from_its_files_as_one_query() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(!config.data().is_empty());
        assert!(
            config
                .data()
                .iter()
                .all(BacktestDataConfig::optimize_file_loading)
        );
    }

    #[rstest]
    fn a_backtests_venues_are_unleveraged_under_nautilus_default_margin_model() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let venue = &config.venues()[0];
        assert!(venue.margin_model().is_none(), "{:?}", venue.margin_model());
        assert_eq!(venue.default_leverage(), Some(Decimal::ONE));
    }

    /// A cTrader venue's account has a base currency live, and IB's has none
    /// (`venues::has_base_currency`). A dataset run's account has none on any venue.
    #[rstest]
    fn a_backtests_venue_has_the_balances_currency_as_its_base_if_its_broker_has_one() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![
                BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL"),
                BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL"),
            ],
            ..backtest(dir.path())
        };
        let bases = |config: BacktestRunConfig| -> Vec<(String, Option<Currency>)> {
            config
                .venues()
                .iter()
                .map(|venue| (venue.name().to_string(), venue.base_currency()))
                .collect()
        };

        assert_eq!(
            bases(run_config(&backtest).unwrap()),
            [
                ("IBIS".to_string(), None),
                ("ICMARKETS".to_string(), Some(Currency::EUR())),
            ]
        );
        assert_eq!(
            bases(dataset_run_config(&backtest).unwrap()),
            [("IBIS".to_string(), None), ("ICMARKETS".to_string(), None)]
        );
    }

    /// Returns each venue's base, submit, modify and cancel latencies, in nanoseconds.
    fn latencies(config: &BacktestRunConfig) -> Vec<Option<[u64; 4]>> {
        let command = TradingCommand::CancelAllOrders(CancelAllOrders::new(
            TraderId::from("TESTER-001"),
            None,
            StrategyId::from("S-001"),
            InstrumentId::from("SXR8.IBIS"),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ));
        let now = UnixNanos::default();
        config
            .venues()
            .iter()
            .map(|venue| {
                venue.latency_model().map(|model: &LatencyModelAny| {
                    [
                        model.get_base_latency(),
                        model.get_insert_latency(&command, now).unwrap(),
                        model.get_update_latency(&command, now).unwrap(),
                        model.get_delete_latency(&command, now).unwrap(),
                    ]
                    .map(|latency| latency.as_u64())
                })
            })
            .collect()
    }

    #[rstest]
    #[case::at_150_ms(Some(SignedDuration::from_millis(150)), Some([150_000_000; 4]))]
    #[case::without_a_latency(None, None)]
    fn a_backtest_at_a_latency_delays_every_command_by_it_on_every_venue(
        #[case] latency: Option<SignedDuration>,
        #[case] delayed: Option<[u64; 4]>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![
                BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL"),
                BarType::from("GC.v.0.GLBX-1-MINUTE-MID-INTERNAL"),
            ],
            latency: latency.map(|latency| Latency::try_from(latency).unwrap()),
            ..backtest(dir.path())
        };

        assert_eq!(latencies(&run_config(&backtest).unwrap()), [delayed; 2]);
        assert_eq!(
            latencies(&dataset_run_config(&backtest).unwrap()),
            [delayed; 2]
        );
    }

    /// Pins two defaults of Nautilus' `BacktestVenueConfig` builder, bar execution and the
    /// rejection of a stop in the market, both on (`crates/backtest/src/config.rs:489-491`,
    /// `:468-470`).
    /// `venue_config` sets neither, and the backtest and model guides rely on both.
    #[rstest]
    fn drift_the_venue_matches_on_bars_and_rejects_a_stop_in_the_market_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let venue = &config.venues()[0];
        assert!(venue.bar_execution());
        assert!(venue.reject_stop_orders());
    }

    /// Pins Nautilus' documented defaults (`docs/concepts/backtesting/bar-execution.md`,
    /// `docs/concepts/options.md`), which `venue_config` keeps.
    #[rstest]
    fn the_venue_replays_bars_in_a_fixed_order_and_consumes_no_liquidity() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let venue = &config.venues()[0];
        assert!(!venue.bar_adaptive_high_low_ordering());
        assert!(!venue.liquidity_consumption());
    }

    fn quoted(config: &BacktestRunConfig, catalog: &Path) -> Vec<String> {
        config
            .data()
            .iter()
            .map(|data| {
                assert_eq!(data.data_type(), &NautilusDataType::QuoteTick);
                assert_eq!(data.catalog().path(), catalog.to_str().unwrap());
                data.instrument_id().unwrap().to_string()
            })
            .collect()
    }

    #[rstest]
    fn only_the_quotes_of_every_instrument_come_from_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert_eq!(quoted(&config, dir.path()), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    #[rstest]
    fn an_instrument_of_two_bar_types_has_its_quotes_loaded_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut backtest = backtest(dir.path());
        backtest
            .bar_types
            .push(BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL"));
        let config = run_config(&backtest).unwrap();
        assert_eq!(quoted(&config, dir.path()), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    #[rstest]
    fn a_composite_bar_type_has_its_source_bars_loaded_by_bar_type() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![BarType::from(
                "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            )],
            ..backtest(dir.path())
        };
        let config = run_config(&backtest).unwrap();
        let [data] = config.data() else {
            panic!("{:?}", config.data())
        };
        assert_eq!(data.data_type(), &NautilusDataType::Bar);
        assert_eq!(
            data.query_identifiers().unwrap(),
            ["SXR8.IBIS-1-MINUTE-LAST-EXTERNAL"]
        );
        assert_eq!(
            data.get_instrument_ids().unwrap(),
            [InstrumentId::from("SXR8.IBIS")]
        );
    }

    #[rstest]
    fn a_run_on_a_composite_bar_type_builds_its_bars_as_late_as_a_live_node() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![BarType::from(
                "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            )],
            ..backtest(dir.path())
        };
        let delay = |config: BacktestRunConfig| {
            config
                .engine()
                .data_engine
                .as_ref()
                .unwrap()
                .time_bars_build_delay
        };
        assert_eq!(delay(run_config(&backtest).unwrap()), 5_000_000);
        assert_eq!(delay(dataset_run_config(&backtest).unwrap()), 5_000_000);
    }

    #[rstest]
    fn no_catalog_is_registered_to_serve_requests() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().catalogs.is_empty());
        assert_eq!(config.engine().cache, None);
    }

    #[rstest]
    fn the_node_installs_the_logger_as_configured() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().logging.print_config);
    }

    #[rstest]
    fn a_trading_run_keeps_its_closed_orders_and_positions() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().exec_engine.is_none());
    }

    #[rstest]
    fn a_dataset_run_purges_its_closed_orders_and_positions_every_minute() {
        let dir = tempfile::tempdir().unwrap();
        let config = dataset_run_config(&backtest(dir.path())).unwrap();
        let exec = config.engine().exec_engine.clone().unwrap();
        let purge = [
            exec.purge_closed_orders_interval_mins,
            exec.purge_closed_orders_buffer_mins,
            exec.purge_closed_positions_interval_mins,
            exec.purge_closed_positions_buffer_mins,
        ];
        assert_eq!(purge, [Some(1); 4]);

        let trading = run_config(&backtest(dir.path())).unwrap();
        assert_eq!(config.venues().len(), trading.venues().len());
        assert_eq!(config.data().len(), trading.data().len());
    }

    #[rstest]
    fn the_data_is_streamed_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert_eq!(config.chunk_size(), Some(100_000));
    }

    #[rstest]
    fn a_run_of_a_custom_series_loads_the_other_series_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            custom: vec![CustomSeries::new("Imbalance", "SXR8.IBIS".to_string(), None).unwrap()],
            ..backtest(dir.path())
        };
        let config = run_config(&backtest).unwrap();
        assert_eq!(config.chunk_size(), None);
        assert_eq!(quoted(&config, dir.path()), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    /// `BacktestNode` warns when it reads nothing from a series, and runs on (`run_oneshot`).
    #[rstest]
    fn an_empty_read_of_a_custom_series_logs_nautilus_no_data_warning_and_reads_on() {
        let name = "an_empty_read_of_a_custom_series_logs_nautilus_no_data_warning_and_reads_on";
        if !crate::log_capture::alone(module_path!(), name) {
            return;
        }
        crate::log_capture::capture(log::LevelFilter::Warn);
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            custom: vec![CustomSeries::new("Signal", "GOLD-SIGNAL".to_string(), None).unwrap()],
            ..backtest(dir.path())
        };

        let read = custom_data(&backtest).unwrap();

        assert!(read.is_empty(), "{read:?}");
        assert_eq!(
            crate::log_capture::lines(),
            [
                "WARN No data found for config: DataType(type_name=Signal, metadata=None, \
                 identifier=Some(\"GOLD-SIGNAL\"))"
            ]
        );
    }

    #[rstest]
    fn runs_and_reads_each_series_from_the_start_to_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let ns = |ts: &str| UnixNanos::from(ts.parse::<Timestamp>().unwrap());
        let (start, end) = (ns("2026-07-01T00:00:00Z"), ns("2026-10-01T00:00:00Z"));
        assert_eq!(config.start(), Some(start));
        assert_eq!(config.end(), Some(end));
        assert!(!config.data().is_empty());
        for data in config.data() {
            assert_eq!(
                (data.start_time(), data.end_time()),
                (Some(start), Some(end))
            );
        }
    }

    #[rstest]
    fn raises_errors_and_keeps_the_engine_after_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.raise_exception());
        assert!(!config.dispose_on_completion());
    }

    #[rstest]
    #[case::on_the_end("2026-10-01T00:00:00Z")]
    #[case::after_the_end("2026-10-01T19:00:00Z")]
    fn rejects_a_start_on_or_after_the_end_naming_both(#[case] start: &str) {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            start: start.parse().unwrap(),
            ..backtest(dir.path())
        };
        let err = run_config(&backtest).unwrap_err().to_string();
        assert!(
            err.contains(start) && err.contains("2026-10-01T00:00:00Z"),
            "{err}"
        );
    }

    #[rstest]
    fn rejects_no_instruments() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![],
            ..backtest(dir.path())
        };
        let err = run_config(&backtest).unwrap_err();
        assert!(err.to_string().contains("no instruments"), "{err}");
    }

    #[rstest]
    #[case::trading(run_config, OmsType::Netting, Decimal::ONE)]
    #[case::dataset(dataset_run_config, OmsType::Hedging, Decimal::MAX)]
    fn the_run_sets_each_simulated_venues_position_mode(
        #[case] configure: fn(&Backtest) -> anyhow::Result<BacktestRunConfig>,
        #[case] glbx_mode: OmsType,
        #[case] leverage: Decimal,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![
                BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL"),
                BarType::from("GC.v.0.GLBX-1-MINUTE-MID-INTERNAL"),
            ],
            ..backtest(dir.path())
        };

        let config = configure(&backtest).unwrap();

        let venues: Vec<_> = config
            .venues()
            .iter()
            .map(|venue| (venue.name().to_string(), venue.oms_type()))
            .collect();
        assert_eq!(
            venues,
            [
                ("GLBX".to_string(), glbx_mode),
                ("ICMARKETS".to_string(), OmsType::Hedging),
            ]
        );

        for venue in config.venues() {
            assert_eq!(venue.starting_balances(), ["10000.00 EUR"]);
            assert_eq!(venue.default_leverage(), Some(leverage));
            assert_eq!(venue.account_type(), AccountType::Margin);
            assert!(matches!(venue.fee_model(), Some(FeeModelAny::Fixed(_))));
        }
    }

    #[rstest]
    fn a_venue_no_broker_trades_starts_with_nothing_in_the_run_s_currency() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL")],
            ticks: vec![crate::strategy::requirements::TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from("GOLD-FUTURES.LSEDGE"),
            }],
            ..backtest(dir.path())
        };
        let venues = |config: BacktestRunConfig| -> Vec<(String, OmsType, Vec<String>)> {
            config
                .venues()
                .iter()
                .map(|venue| {
                    (
                        venue.name().to_string(),
                        venue.oms_type(),
                        venue.starting_balances().to_vec(),
                    )
                })
                .collect()
        };

        let normal = venues(run_config(&backtest).unwrap());
        let dataset = venues(dataset_run_config(&backtest).unwrap());

        let venue =
            |name: &str, oms, balance: &str| (name.to_string(), oms, vec![balance.to_string()]);
        assert_eq!(
            normal,
            [
                venue("ICMARKETS", OmsType::Hedging, "10000.00 EUR"),
                venue("LSEDGE", OmsType::Unspecified, "0.00 EUR"),
            ]
        );
        assert_eq!(
            dataset,
            [
                venue("ICMARKETS", OmsType::Hedging, "10000.00 EUR"),
                venue("LSEDGE", OmsType::Hedging, "0.00 EUR"),
            ]
        );
    }
}
