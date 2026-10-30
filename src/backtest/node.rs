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
use nautilus_core::UnixNanos;
use nautilus_execution::engine::config::ExecutionEngineConfig;
use nautilus_model::{
    data::{Data, NautilusDataType},
    enums::{AccountType, BookType},
    identifiers::{InstrumentId, Venue},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{Backtest, costs, report::Outcome};
use crate::{
    data::catalog_path,
    strategy::{SeriesSlice, warmup},
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

/// The run config of `backtest`: one simulated venue per venue of the
/// instruments, named after it, keeping positions as `backtest.venue_oms`
/// says of it, each with the run's balance, leverage (Nautilus' default
/// margin model: the notional / leverage times the instrument's margin)
/// and fee model, which fills orders on the replayed data and settles
/// funding, the series of the window from the catalog (the only data
/// replayed: the data engine builds the strategy's bars from them and
/// keeps the books the strategy subscribes to, the venue settles the
/// funding; see `Backtest::series` and `data_config`), each over its
/// slice of `[start, end]`, the window the engine runs, `start` before
/// `end`. No catalog is registered in the engine: it serves no request,
/// and the strategy warms up on the replay. The cache is Nautilus'
/// default, the data engine builds no bar for an hour without quotes
/// (`warmup::data_engine_config`). The data is streamed `CHUNK_SIZE`
/// points at a time, unless the run replays custom data, which
/// `BacktestDataConfig` refuses: then the custom series are left out
/// (see `custom_data`) and the data is loaded at once. Build and run errors are raised, not only logged; the engine
/// outlives the run (see `run`).
pub(super) fn run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    config(backtest, None)
}

/// The run config of a dataset run of `backtest`: that of `run_config`,
/// its execution engine purging closed orders and positions
/// `DATASET_PURGE_MINS` after they close, every `DATASET_PURGE_MINS`.
pub(super) fn dataset_run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    let exec_engine = ExecutionEngineConfig::builder()
        .purge_closed_orders_interval_mins(DATASET_PURGE_MINS)
        .purge_closed_orders_buffer_mins(DATASET_PURGE_MINS)
        .purge_closed_positions_interval_mins(DATASET_PURGE_MINS)
        .purge_closed_positions_buffer_mins(DATASET_PURGE_MINS)
        .build()?;
    config(backtest, Some(exec_engine))
}

/// The run config of `backtest` (see `run_config`), with `exec_engine`, or
/// Nautilus' default execution engine.
fn config(
    backtest: &Backtest,
    exec_engine: Option<ExecutionEngineConfig>,
) -> anyhow::Result<BacktestRunConfig> {
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
    let catalog = catalog_path(&backtest.catalog)?;
    let venues = venues
        .into_iter()
        .map(|venue| venue_config(backtest, venue))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let (custom, series): (Vec<SeriesSlice>, Vec<SeriesSlice>) =
        backtest.series()?.into_iter().partition(is_custom);
    let data = series
        .into_iter()
        .map(|series| data_config(&catalog, series))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let engine_config = BacktestEngineConfig::builder()
        .data_engine(warmup::data_engine_config())
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
        .maybe_chunk_size(custom.is_empty().then_some(CHUNK_SIZE))
        .raise_exception(true)
        .dispose_on_completion(false)
        .build()?)
}

/// The simulated `venue` of `backtest`: a margin account keeping positions
/// as `backtest.venue_oms` says, with the run's balance, leverage, fee
/// model and fill model, filling on the top of the book.
fn venue_config(backtest: &Backtest, venue: Venue) -> anyhow::Result<BacktestVenueConfig> {
    Ok(BacktestVenueConfig::builder()
        .name(venue.as_str())
        .oms_type((backtest.venue_oms)(venue))
        .account_type(AccountType::Margin)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec![backtest.balance.to_string()])
        .default_leverage(backtest.leverage)
        .fee_model(backtest.fee_model.clone())
        .fill_model(costs::fill_model()?)
        .build()?)
}

/// The `BacktestDataConfig` loading `series` from `catalog` over its
/// slice, both ends included: bars by their bar type, any other data by
/// its instrument id, as the catalog keys them. The node replays the slice
/// within the run, so a contract of a root is replayed only while it
/// stands for the root.
fn data_config(catalog: &str, series: SeriesSlice) -> anyhow::Result<BacktestDataConfig> {
    let SeriesSlice {
        series: (data_type, identifier),
        slice,
    } = series;
    let builder = BacktestDataConfig::builder()
        .catalog_path(catalog.to_string())
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

/// The custom data `backtest` replays (see `Backtest::series`), read from
/// its catalog, each series over its slice, both ends included: the data
/// the node cannot load (see `run_config`). Nautilus decodes each type by
/// its name, so the strategy registers its custom types with Nautilus
/// before (`nautilus_serialization::ensure_custom_data_registered`).
///
/// # Errors
///
/// Returns an error if the series cannot be worked out, or if the catalog
/// cannot be read or a stored type is not registered, naming the series.
pub(super) fn custom_data(backtest: &Backtest) -> anyhow::Result<Vec<Data>> {
    let mut catalog = ParquetDataCatalog::new(&backtest.catalog, None, None, None, None);
    let mut data = Vec::new();
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
/// engine's cache, each on the quotes the run replayed, if any, loaded
/// again as the node loads them (the other series are not).
pub(super) fn run<S>(
    config: BacktestRunConfig,
    custom: Vec<Data>,
    strategy: S,
) -> anyhow::Result<Outcome>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    let mut node = build(config, custom, strategy)?;
    let results = node.run()?;
    let [result] = <[BacktestResult; 1]>::try_from(results)
        .map_err(|results| anyhow!("{} results from one run", results.len()))?;
    let config = &node.configs()[0];
    let mut quotes = Vec::new();
    let quoted = config
        .data()
        .iter()
        .filter(|data| *data.data_type() == NautilusDataType::QuoteTick);
    for data in quoted {
        let replayed = BacktestNode::load_data_config(data, config.start(), config.end())?;
        quotes.extend(replayed.into_iter().filter_map(|data| match data {
            Data::Quote(quote) => Some(quote),
            _ => None,
        }));
    }
    let engine = node.get_engine(RUN_ID).context("no engine for the run")?;
    let outcome = Outcome::new(result, &engine.kernel().cache.borrow(), &quotes);
    node.dispose();
    outcome
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
    let mut node = build(config, custom, strategy)?;
    node.run()?;
    node.dispose();
    Ok(())
}

/// A built `BacktestNode` of `config`, `MaxDrawdown` registered with the
/// portfolio, `strategy` added and `custom` added to its engine.
fn build<S>(
    config: BacktestRunConfig,
    custom: Vec<Data>,
    strategy: S,
) -> anyhow::Result<BacktestNode>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    let mut node = BacktestNode::new(vec![config])?;
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
    Ok(node)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use jiff::Timestamp;
    use nautilus_common::logging::logger::LoggerConfig;
    use nautilus_execution::models::fee::FeeModelAny;
    use nautilus_model::{data::BarType, enums::OmsType, types::Money};
    use rust_decimal::Decimal;

    use super::*;

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
            fee_model: costs::commission_per_order(Money::from("3 EUR")).unwrap(),
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

    #[test]
    fn orders_fill_on_quotes_only_at_the_quoted_price() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let venue = &config.venues()[0];
        assert!(venue.bar_execution());
        // Nautilus' default: external bars replay open, high, low, close.
        assert!(!venue.bar_adaptive_high_low_ordering());
        assert!(!venue.liquidity_consumption());
    }

    /// The instrument of each data config, all quotes from `catalog`.
    fn quoted(config: &BacktestRunConfig, catalog: &Path) -> Vec<String> {
        config
            .data()
            .iter()
            .map(|data| {
                assert_eq!(data.data_type(), &NautilusDataType::QuoteTick);
                assert_eq!(data.catalog_path(), catalog.to_str().unwrap());
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

    #[test]
    fn a_relative_catalog_is_made_absolute() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            catalog: PathBuf::from("catalog"),
            ..backtest(dir.path())
        };
        let config = run_config(&backtest).unwrap();
        let absolute = std::env::current_dir().unwrap().join("catalog");
        let absolute = absolute.to_str().unwrap();
        assert_eq!(config.data()[0].catalog_path(), absolute);
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
    fn runs_and_reads_each_series_from_the_start_to_the_end_and_raises_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let ns = |ts: &str| UnixNanos::from(ts.parse::<Timestamp>().unwrap());
        let (start, end) = (ns("2026-07-01T00:00:00Z"), ns("2026-10-01T00:00:00Z"));
        assert_eq!(config.start(), Some(start));
        assert_eq!(config.end(), Some(end));
        for data in config.data() {
            assert_eq!(
                (data.start_time(), data.end_time()),
                (Some(start), Some(end))
            );
        }
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
        assert!(run_config(&backtest).is_err());
    }

    #[test]
    fn instruments_on_two_venues_get_a_venue_each_as_it_keeps_positions() {
        let dir = tempfile::tempdir().unwrap();
        let backtest = Backtest {
            bar_types: vec![
                BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL"),
                BarType::from("GC.v.0.GLBX-1-MINUTE-MID-INTERNAL"),
            ],
            venue_oms: crate::backtest::venue_oms_type,
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

    #[test]
    fn rejects_a_start_after_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let b = backtest(dir.path());
        let backtest = Backtest {
            start: b.end,
            end: b.start,
            ..b
        };
        assert!(run_config(&backtest).is_err());
    }
}
