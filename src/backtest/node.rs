//! Backtest run over the catalog.
//!
//! Builds a Nautilus `BacktestRunConfig` (`nautilus_backtest::config`): the
//! venue (`BacktestVenueConfig`), the catalog data (`BacktestDataConfig`, one
//! per series the run replays) and the engine
//! config, then runs it on a `BacktestNode`: `build`, the strategy
//! added through `get_engine_mut`, `run`, which returns the `BacktestResult`.
//! The engine is kept after the run, so that the report reads the fills
//! from its cache, then disposed.
//!
//! Custom data is the exception: `BacktestDataConfig` refuses it, so the
//! run reads it from the catalog itself (`custom_data`) and adds it to the
//! node's engine before `run`, through the engine's own `add_data`, which
//! replays it in time order with the rest, routed to its subscribers by
//! its data type. A streaming run clears the engine's data after each
//! chunk, dropping what was added before, so a run with custom data loads
//! all its data at once (no `chunk_size`).

use std::{collections::BTreeSet, fmt::Debug, str::FromStr, sync::Arc};

use anyhow::{Context, anyhow, ensure};
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
    enums::{AccountType, BookType},
    identifiers::{InstrumentId, Venue},
    types::Money,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::CatalogQuery,
    config::DataCatalogConfig,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

use super::{Backtest, report::Outcome};
use crate::{
    catalog::catalog_config,
    strategy::{SeriesSlice, warmup},
    venues,
};

/// The id of the one run of the node.
const RUN_ID: &str = "backtest";

/// How many data points the node loads and replays at a time (Nautilus'
/// streaming mode): a day of CME gold's book depth is 7.4 million
/// snapshots, beyond 16 GB loaded at once.
const CHUNK_SIZE: usize = 100_000;

/// How often a dataset run's execution engine purges the closed orders and
/// positions from the cache, and how long after closing one is purged, in
/// minutes (live keeps a week: `live::node`). Its targets are read from
/// events as they come, never from the cache afterwards. Kept, they slow
/// the run more than in proportion to its days: 16 days of quotes every
/// 5 s and targets every 15 minutes took 70 s against 25 s purged, 2 days
/// 4.2 s against 3.1 s.
const DATASET_PURGE_MINS: u32 = 1;

/// The leverage of a dataset run's accounts: the largest a `Decimal` holds,
/// so that under Nautilus' default margin model, the notional / leverage
/// times the instrument's margin (`LeveragedMarginModel`,
/// model/src/accounts/margin_model.rs), every order's margin rounds to
/// nothing, and the risk engine, which checks an order's initial margin
/// against the free balance (`check_margin`, risk/src/engine/mod.rs),
/// denies none for it while keeping its other checks: a size below the
/// instrument's minimum is still denied. A dataset run opens a long and a
/// short at every decision, at all hours, which no account trades; whether
/// an account can margin a trade is checked by the strategy's backtest and
/// the live venue. Nautilus has no margin-free model for a
/// `BacktestVenueConfig`, whose margin model is the standard or the
/// leveraged one (`MarginModelAny`).
const DATASET_LEVERAGE: Decimal = Decimal::MAX;

/// What a run is for, which its node is configured by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Run {
    /// A backtest of the strategy: each venue's account at the run's
    /// leverage, its balance moving with the run's P&L.
    Backtest,
    /// A dataset run (`Backtest::run_dataset`): each venue's account
    /// frozen at the run's balance and margining nothing
    /// (`DATASET_LEVERAGE`), its execution engine purging closed orders
    /// and positions `DATASET_PURGE_MINS` after they close, every
    /// `DATASET_PURGE_MINS`.
    Dataset,
}

/// The run config of `backtest`: one simulated venue per venue of the
/// instruments, named after it, keeping positions as `backtest.venue_oms`
/// says of it, each with the run's balance, or nothing if no broker trades
/// it, its leverage (Nautilus' default margin model: the notional /
/// leverage times the instrument's margin) and fee model, which fills
/// orders on the replayed data and settles funding, the series of the
/// window from the catalog (the only data replayed: the data engine builds
/// the strategy's bars from them and keeps the books the strategy
/// subscribes to, the venue settles the funding; see `Backtest::series` and
/// `data_config`), each over its slice of `[start, end]`, the window the
/// engine runs, `start` before `end`. No catalog is registered in the
/// engine: it serves no request, and the strategy warms up on the replay.
/// The cache is Nautilus' default, the data engine builds no bar for an
/// hour without quotes and builds time bars as late after their close as a
/// live node does, by the backtest's bar types
/// (`warmup::data_engine_config`). The data is streamed `CHUNK_SIZE` points
/// at a time, unless the run replays custom data, which
/// `BacktestDataConfig` refuses: then the custom series are left out (see
/// `custom_data`) and the data is loaded at once. Build and run errors are
/// raised, not only logged; the engine outlives the run (see `run`).
pub(super) fn run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    config(backtest, Run::Backtest)
}

/// The run config of a dataset run of `backtest`: that of `run_config` as
/// a dataset run's (`Run::Dataset`), its accounts frozen and margining
/// nothing, its cache purged as it goes.
pub(super) fn dataset_run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    config(backtest, Run::Dataset)
}

/// The run config of `backtest` for `run` (see `run_config`).
fn config(backtest: &Backtest, run: Run) -> anyhow::Result<BacktestRunConfig> {
    // Nautilus only checks the engine start against the end, which lets a
    // run starting on its end through.
    let (start, end) = (backtest.start, backtest.end);
    ensure!(
        start < end,
        "backtest start {start} must be before its end {end}"
    );
    // A root's contracts are on its venue.
    let venues: BTreeSet<Venue> = backtest.declared().iter().map(|id| id.venue).collect();
    ensure!(!venues.is_empty(), "no instruments");
    let catalog = catalog_config(&backtest.catalog)?;
    let venues = venues
        .into_iter()
        .map(|venue| venue_config(backtest, venue, run))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let (custom, series): (Vec<SeriesSlice>, Vec<SeriesSlice>) =
        backtest.series()?.into_iter().partition(is_custom);
    let data = series
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
        .maybe_chunk_size((custom.is_empty() && backtest.custom.is_empty()).then_some(CHUNK_SIZE))
        .raise_exception(true)
        .dispose_on_completion(false)
        .build()?)
}

/// The simulated `venue` of `backtest` for `run`: a margin account keeping
/// positions as `backtest.venue_oms` says, with the run's balance if a
/// broker trades the venue (`venues::has_broker`), else nothing in its
/// currency, and the fee model of its commission, filling on the top of
/// the book on Nautilus' default fill model: a market order fills at the
/// quote (a buy at the ask, a sell at the bid), or on bars at the last
/// price, with no slippage, and a limit order whenever touched. A
/// backtest's account is at the run's leverage. A dataset run's is frozen,
/// Nautilus' `frozen_account`: the simulated exchange adjusts none of its
/// balances (`adjust_account_balance`, backtest/src/exchange.rs) and the
/// portfolio locks no margin of an account that does not calculate its
/// state (portfolio/src/portfolio.rs), so trades that lose leave its
/// balance whole; and it is at `DATASET_LEVERAGE`, margining nothing.
fn venue_config(
    backtest: &Backtest,
    venue: Venue,
    run: Run,
) -> anyhow::Result<BacktestVenueConfig> {
    let balance = if venues::has_broker(venue) {
        backtest.balance
    } else {
        Money::zero(backtest.balance.currency)
    };
    let leverage = match run {
        Run::Backtest => backtest.leverage,
        Run::Dataset => DATASET_LEVERAGE,
    };
    Ok(BacktestVenueConfig::builder()
        .name(venue.as_str())
        .oms_type((backtest.venue_oms)(venue))
        .account_type(AccountType::Margin)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec![balance.to_string()])
        .default_leverage(leverage)
        .frozen_account(run == Run::Dataset)
        .fee_model(backtest.commission.fee_model()?)
        .build()?)
}

/// The `BacktestDataConfig` loading `series` from `catalog` over its
/// slice, both ends included: bars by their bar type, any other data by
/// its instrument id, as the catalog keys them. The node replays the slice
/// within the run, so a contract of a root is replayed only while it
/// stands for the root.
fn data_config(
    catalog: &DataCatalogConfig,
    series: SeriesSlice,
) -> anyhow::Result<BacktestDataConfig> {
    let SeriesSlice {
        series: (data_type, identifier),
        slice,
    } = series;
    let builder = BacktestDataConfig::builder()
        .catalog(catalog.clone())
        .data_type(data_type.clone())
        .start_time(UnixNanos::from(slice.from))
        .end_time(UnixNanos::from(slice.to))
        // One stream over the series' files, as the catalog itself queries:
        // Nautilus' default for a backtest config opens a prefetching stream
        // per file, all before the first chunk, which over months of MBP-10
        // files takes tens of GB.
        .optimize_file_loading(true);
    let config = if data_type == NautilusDataType::Bar {
        builder.bar_types(vec![identifier]).build()
    } else {
        builder
            .instrument_id(InstrumentId::from_str(&identifier)?)
            .build()
    };
    Ok(config?)
}

/// Whether `series` is of custom data, which `BacktestDataConfig` refuses.
fn is_custom(series: &SeriesSlice) -> bool {
    matches!(series.series.0, NautilusDataType::Custom { .. })
}

/// The custom data `backtest` replays, read from its catalog: that of its
/// instruments (see `Backtest::series`), each series over its slice, and
/// that of no instrument (`Backtest::custom`), each keyed by its data
/// type's identifier, over the run's window, both ends included: the data
/// the node cannot load (see `run_config`). Nautilus decodes each type by
/// its name, so each must be registered with Nautilus before the run
/// (`nautilus_serialization::ensure_custom_data_registered`): the run
/// registers those the venues' sources serve, LSE's rows
/// (`venues::register_custom_data`), and the code declaring another type's
/// series registers it, as the tests do.
///
/// # Errors
///
/// Returns an error if the series cannot be worked out, or if the catalog
/// cannot be read or a stored type is not registered, naming the series.
pub(super) fn custom_data(backtest: &Backtest) -> anyhow::Result<Vec<Data>> {
    venues::register_custom_data();
    let mut catalog = ParquetDataCatalog::new(&backtest.catalog, None, None, None, None);
    let mut data = Vec::new();
    for data_type in &backtest.custom {
        let identifiers = data_type.identifier().map(|id| vec![id.to_string()]);
        let read = catalog
            .query_custom_data_dynamic(
                data_type.type_name(),
                identifiers.as_deref(),
                Some(UnixNanos::from(backtest.start)),
                Some(UnixNanos::from(backtest.end)),
                None,
                None,
                true,
            )
            .with_context(|| format!("reading the {data_type}"))?;
        data.extend(read);
    }
    for series in backtest.series()?.into_iter().filter(is_custom) {
        let SeriesSlice {
            series: (data_type, identifier),
            slice,
        } = series;
        let NautilusDataType::Custom { type_name } = &data_type else {
            unreachable!("a custom series");
        };
        let read = catalog
            .query_custom_data_dynamic(
                type_name,
                Some(std::slice::from_ref(&identifier)),
                Some(UnixNanos::from(slice.from)),
                Some(UnixNanos::from(slice.to)),
                None,
                None,
                true,
            )
            .with_context(|| format!("reading the {data_type} of {identifier}"))?;
        data.extend(read);
    }
    Ok(data)
}

/// Runs `config` with `strategy` on a new `BacktestNode`, `custom` (see
/// `custom_data`) added to its engine, and returns its
/// one result, its return statistics with `MaxDrawdown` (registered with
/// the portfolio before the run), with the costs of the fills in the
/// engine's cache, each on the quotes the run replayed, if any, read
/// again from the catalog a batch at a time (see `replayed_quotes`; the
/// other series are not read).
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
            .map_err(|results| anyhow!("{} results from one run", results.len()))?;
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

/// The quotes `data` replays in a run from `start` to `end`, in time
/// order, read from its catalog a batch at a time by the query the node
/// streams them by (Nautilus' `BacktestNode::load_data_config` collects
/// the same query into memory, all at once). An error opening the catalog
/// or reading a batch is an item.
///
/// It stands in for Nautilus: the query is built here as Nautilus' private
/// `dispatch_batch_query` builds it (`backtest/src/node.rs`), no public
/// call streaming a data config. Once Nautilus has one, this is that call
/// and a match on quotes: helix-trader issue #15 (<https://github.com/madebymlai/helix-trader/issues/15>).
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
        Err(err) => return Box::new(std::iter::once(Err(err))),
    };
    let batches = std::iter::from_fn(move || session.next_batch().transpose());
    Box::new(batches.flat_map(|batch| match batch {
        Ok(DataBatch::Quote(quotes)) => quotes.iter().copied().map(Ok).collect(),
        Ok(_) => vec![Err(anyhow!("a quote query yielded other data"))],
        Err(err) => vec![Err(err)],
    }))
}

/// Runs `config` with `strategy` on a new `BacktestNode`, `custom` added
/// to its engine, as `run` does, for what the strategy does alone: a
/// dataset run, whose cache is purged as it goes, has no result to read
/// from it.
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

/// What `run` makes of a new `BacktestNode` of `config`, built (see
/// `build`), the node then disposed whatever came of it, as Nautilus
/// requires of a node before the next starts on the thread
/// (`concepts/architecture.md`, "Processes and threads"): a model
/// backtest goes on to further nodes after a failed dataset run.
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

/// Builds `node`, registers `MaxDrawdown` with the portfolio, adds
/// `strategy` and `custom` to its engine.
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

    use jiff::Timestamp;
    use nautilus_common::logging::logger::LoggerConfig;
    use nautilus_execution::models::fee::FeeModelAny;
    use nautilus_model::{data::BarType, enums::OmsType, types::Money};
    use rust_decimal::Decimal;

    use super::*;
    use crate::backtest::Commission;

    /// A backtest on the catalog `catalog`.
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
            balance: Money::from("10000 EUR"),
            leverage: Decimal::ONE,
            venue_oms: |_| OmsType::Netting,
            logging: LoggerConfig {
                print_config: true,
                ..LoggerConfig::default()
            },
        }
    }

    #[test]
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

    #[test]
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

    #[test]
    fn the_venue_keeps_positions_as_the_backtest_says() {
        let dir = tempfile::tempdir().unwrap();
        let netting = run_config(&backtest(dir.path())).unwrap();
        let hedging = Backtest {
            venue_oms: |_| OmsType::Hedging,
            ..backtest(dir.path())
        };
        let hedging = run_config(&hedging).unwrap();
        assert_eq!(netting.venues()[0].oms_type(), OmsType::Netting);
        assert_eq!(hedging.venues()[0].oms_type(), OmsType::Hedging);
    }

    #[test]
    fn margins_follow_nautilus_default_model_at_the_leverage_of_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            leverage: Decimal::TEN,
            ..backtest(dir.path())
        };
        let config = run_config(&backtest).unwrap();
        let venue = &config.venues()[0];
        assert!(venue.margin_model().is_none(), "{:?}", venue.margin_model());
        assert_eq!(venue.default_leverage(), Some(Decimal::TEN));
    }

    #[test]
    fn rejects_a_leverage_not_positive() {
        let dir = tempfile::tempdir().unwrap();
        for leverage in [Decimal::ZERO, Decimal::NEGATIVE_ONE] {
            let backtest = Backtest {
                leverage,
                ..backtest(dir.path())
            };
            let err = run_config(&backtest).unwrap_err();
            assert!(err.to_string().contains("default_leverage"), "{err}");
        }
    }

    /// The venue matches as Nautilus does by default, which the backtest
    /// and model guides rely on: resting orders match on external bars,
    /// replayed open, high, low, close, without consuming liquidity, and a
    /// stop whose trigger is in the market is rejected.
    #[test]
    fn the_venue_keeps_nautilus_default_matching() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let venue = &config.venues()[0];
        assert!(venue.bar_execution());
        assert!(!venue.bar_adaptive_high_low_ordering());
        assert!(!venue.liquidity_consumption());
        assert!(venue.reject_stop_orders());
    }

    /// The instrument of each data config, all quotes from `catalog`.
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

    #[test]
    fn only_the_quotes_of_every_instrument_come_from_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert_eq!(quoted(&config, dir.path()), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    #[test]
    fn an_instrument_of_two_bar_types_has_its_quotes_loaded_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut backtest = backtest(dir.path());
        backtest
            .bar_types
            .push(BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL"));
        let config = run_config(&backtest).unwrap();
        assert_eq!(quoted(&config, dir.path()), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    #[test]
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
        assert_eq!(data.instrument_id(), None);
    }

    /// 5 s after their close, as a live node builds them, in a trading run
    /// as in a dataset run.
    #[test]
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

    /// Its data comes from the data configs alone: no catalog serves a
    /// request, and the cache keeps Nautilus' default.
    #[test]
    fn no_catalog_is_registered_to_serve_requests() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().catalogs.is_empty());
        assert_eq!(config.engine().cache, None);
    }

    #[test]
    fn the_node_installs_the_logger_as_configured() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().logging.print_config);
    }

    #[test]
    fn a_trading_run_keeps_its_closed_orders_and_positions() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().exec_engine.is_none());
    }

    /// Every minute, those closed a minute or more before.
    #[test]
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
        // The rest of the run is a trading run's.
        let trading = run_config(&backtest(dir.path())).unwrap();
        assert_eq!(config.venues().len(), trading.venues().len());
        assert_eq!(config.data().len(), trading.data().len());
    }

    #[test]
    fn the_data_is_streamed_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert_eq!(config.chunk_size(), Some(100_000));
    }

    #[test]
    fn a_run_of_custom_data_loads_the_other_series_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            ticks: vec![crate::strategy::TickSeries {
                data: NautilusDataType::Custom {
                    type_name: "Imbalance".to_string(),
                },
                instrument_id: InstrumentId::from("SXR8.IBIS"),
            }],
            ..backtest(dir.path())
        };
        let config = run_config(&backtest).unwrap();
        assert_eq!(config.chunk_size(), None);
        assert_eq!(quoted(&config, dir.path()), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    #[test]
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

    #[test]
    fn raises_errors_and_keeps_the_engine_after_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.raise_exception());
        assert!(!config.dispose_on_completion());
    }

    #[test]
    fn rejects_no_instruments() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![],
            ..backtest(dir.path())
        };
        let err = run_config(&backtest).unwrap_err();
        assert!(err.to_string().contains("no instruments"), "{err}");
    }

    #[test]
    fn instruments_on_two_venues_get_a_venue_each_as_it_keeps_positions() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![
                BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL"),
                BarType::from("GC.v.0.GLBX-1-MINUTE-MID-INTERNAL"),
            ],
            venue_oms: crate::venues::oms_type,
            ..backtest(dir.path())
        };

        let config = run_config(&backtest).unwrap();

        let venues: Vec<_> = config
            .venues()
            .iter()
            .map(|venue| (venue.name().to_string(), venue.oms_type()))
            .collect();
        assert_eq!(
            venues,
            [
                ("GLBX".to_string(), OmsType::Netting),
                ("ICMARKETS".to_string(), OmsType::Hedging),
            ]
        );
        for venue in config.venues() {
            assert_eq!(venue.starting_balances(), ["10000.00 EUR"]);
            assert_eq!(venue.default_leverage(), Some(Decimal::ONE));
            assert_eq!(venue.account_type(), AccountType::Margin);
            assert!(matches!(venue.fee_model(), Some(FeeModelAny::Fixed(_))));
        }
    }

    /// IC's gold traded, LSE's read: LSEDGE, which no broker trades, starts
    /// with nothing in the run's currency, keeping positions as Nautilus'
    /// venue default in a normal run, and hedging in a dataset run, as
    /// every venue there.
    #[test]
    fn a_venue_no_broker_trades_starts_with_nothing_in_the_run_s_currency() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL")],
            ticks: vec![crate::strategy::TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from("GOLD-FUTURES.LSEDGE"),
            }],
            venue_oms: crate::venues::oms_type,
            ..backtest(dir.path())
        };
        let dataset = Backtest {
            venue_oms: |_| OmsType::Hedging,
            ..backtest.clone()
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
        let dataset = venues(dataset_run_config(&dataset).unwrap());

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
