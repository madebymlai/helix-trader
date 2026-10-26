//! Backtest run over the catalog.
//!
//! Builds a Nautilus `BacktestRunConfig` (`nautilus_backtest::config`): the
//! venue (`BacktestVenueConfig`), the catalog data (`BacktestDataConfig`, one
//! per series the run replays) and the engine
//! config, then runs it on a `BacktestNode`: `build`, the strategy
//! added through `get_engine_mut`, `run`, which returns the `BacktestResult`.
//! The engine is kept after the run, so that the report reads the fills
//! from its cache, then disposed.

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
use nautilus_model::{
    data::{Data, NautilusDataType},
    enums::{AccountType, BookType},
    identifiers::{InstrumentId, Venue},
};
use nautilus_persistence::config::DataCatalogConfig;
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{Backtest, costs, report::Outcome};
use crate::{data::catalog_path, strategy::warmup};

/// The id of the one run of the node.
const RUN_ID: &str = "backtest";

/// How many data points the node loads and replays at a time (Nautilus'
/// streaming mode): a day of CME gold's book depth is 7.4 million
/// snapshots, beyond 16 GB loaded at once.
const CHUNK_SIZE: usize = 100_000;

/// The run config of `backtest`: one simulated venue per venue of the
/// instruments, named after it, keeping positions as `backtest.venue_oms`
/// says of it, each with the run's balance, leverage (Nautilus' default
/// margin model: the notional / leverage times the instrument's margin)
/// and fee model, which fills orders on the replayed data and settles
/// funding, the series of the window from the catalog (the only data
/// replayed: the data engine builds the strategy's bars from them and
/// keeps the books the strategy subscribes to, the venue settles the
/// funding; see `Backtest::series` and `data_config`), and the catalog registered in the
/// engine, read-only, so that it serves the strategy's warmup requests and
/// takes no write-back, with a cache
/// holding the warmup bars (`warmup::cache_config`) and a data engine
/// building no bar for an hour without quotes
/// (`warmup::data_engine_config`). The engine starts
/// 1 ns before `start` (see `Backtest::engine_start`), which must be before
/// `end`. The data is streamed `CHUNK_SIZE` points at a time. Build and
/// run errors are raised, not only logged; the engine outlives the run
/// (see `run`).
pub(super) fn run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    // Nautilus only checks the engine start against the end, which lets a
    // run starting on its end through.
    let (start, end) = (backtest.start, backtest.end);
    ensure!(
        start < end,
        "backtest start {start} must be before its end {end}"
    );
    let venues: BTreeSet<Venue> = backtest.instruments().iter().map(|id| id.venue).collect();
    ensure!(!venues.is_empty(), "no instruments");
    let catalog = catalog_path(&backtest.catalog)?;
    let venues = venues
        .into_iter()
        .map(|venue| venue_config(backtest, venue))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let data = backtest
        .series()?
        .into_iter()
        .map(|(data_type, identifier)| data_config(&catalog, data_type, &identifier))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let cache = warmup::cache_config(&backtest.bar_types, backtest.warmup)?;
    let engine_config = BacktestEngineConfig::builder()
        .cache(cache)
        .data_engine(warmup::data_engine_config())
        .catalogs(vec![
            DataCatalogConfig::new(catalog, None, None).with_read_only(true),
        ])
        .logging(backtest.logging.clone())
        .build();
    Ok(BacktestRunConfig::builder()
        .id(RUN_ID.to_string())
        .venues(venues)
        .data(data)
        .engine(engine_config)
        .start(UnixNanos::from(backtest.engine_start()?))
        .end(UnixNanos::from(backtest.end))
        .chunk_size(CHUNK_SIZE)
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

/// The `BacktestDataConfig` loading the series of `data_type` and
/// `identifier` from `catalog`: bars by their bar type, any other data by
/// its instrument id, as the catalog keys them.
fn data_config(
    catalog: &str,
    data_type: NautilusDataType,
    identifier: &str,
) -> anyhow::Result<BacktestDataConfig> {
    let builder = BacktestDataConfig::builder()
        .catalog_path(catalog.to_string())
        .data_type(data_type.clone());
    let config = if data_type == NautilusDataType::Bar {
        builder.bar_types(vec![identifier.to_string()]).build()
    } else {
        builder
            .instrument_id(InstrumentId::from_str(identifier)?)
            .build()
    };
    Ok(config?)
}

/// Runs `config` with `strategy` on a new `BacktestNode` and returns its
/// one result, its return statistics with `MaxDrawdown` (registered with
/// the portfolio before the run), with the costs of the fills in the
/// engine's cache, each on the quotes the run replayed, if any, loaded
/// again as the node loads them (the other series are not).
pub(super) fn run<S>(config: BacktestRunConfig, strategy: S) -> anyhow::Result<Outcome>
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
            warmup: jiff::SignedDuration::from_hours(720),
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
        assert_eq!(config.engine().catalogs[0].path(), absolute);
    }

    #[test]
    fn the_catalog_is_registered_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().catalogs[0].read_only());
    }

    #[test]
    fn the_node_installs_the_logger_as_configured() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert!(config.engine().logging.print_config);
    }

    #[test]
    fn the_data_is_streamed_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        assert_eq!(config.chunk_size(), Some(100_000));
    }

    #[test]
    fn runs_from_one_nanosecond_before_the_start_to_the_end_and_raises_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = run_config(&backtest(dir.path())).unwrap();
        let ns = |ts: &str| UnixNanos::from(ts.parse::<Timestamp>().unwrap());
        assert_eq!(config.start(), Some(ns("2026-06-30T23:59:59.999999999Z")));
        assert_eq!(config.end(), Some(ns("2026-10-01T00:00:00Z")));
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
