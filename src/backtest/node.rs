//! Backtest run over the catalog.
//!
//! Builds a Nautilus `BacktestRunConfig` (`nautilus_backtest::config`): the
//! venue (`BacktestVenueConfig`), the catalog data (`BacktestDataConfig`, one
//! for the bars and one per instrument for its quotes) and the engine
//! config, then runs it on a `BacktestNode`: `build`, the strategy
//! added through `get_engine_mut`, `run`, which returns the `BacktestResult`.

use std::fmt::Debug;

use anyhow::{Context, anyhow, ensure};
use nautilus_backtest::{
    config::{BacktestDataConfig, BacktestEngineConfig, BacktestRunConfig, BacktestVenueConfig},
    node::BacktestNode,
    result::BacktestResult,
};
use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_model::{
    data::{BarType, NautilusDataType},
    enums::{AccountType, BookType, OmsType},
    identifiers::Venue,
};
use nautilus_persistence::config::DataCatalogConfig;
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{Backtest, costs};
use crate::data::to_unix_nanos;

/// The id of the one run of the node.
const RUN_ID: &str = "backtest";

/// The run config of `backtest`: one venue named after the bar types'
/// venue, which fills orders on the quotes alone (bars reach the strategy
/// only), the bars and the instruments' quotes of the window from the
/// catalog, and the catalog registered in the engine so that it serves the
/// strategy's warmup requests. The engine starts 1 ns before `start` (see
/// `Backtest::engine_start`). Build and run errors are raised, not only
/// logged.
pub(super) fn run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    let venue = venue(&backtest.bar_types)?;
    // Absolute: the node's DataFusion reader finds no object store for a
    // relative path ("file://catalog/...").
    let catalog = std::path::absolute(&backtest.catalog)?;
    let catalog = catalog
        .to_str()
        .with_context(|| format!("catalog path {} is not UTF-8", catalog.display()))?
        .to_string();
    let venue_config = BacktestVenueConfig::builder()
        .name(venue.as_str())
        .oms_type(OmsType::Netting)
        .account_type(AccountType::Margin)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec![backtest.balance.to_string()])
        .fee_model(costs::fee_model(backtest.commission)?)
        .fill_model(costs::fill_model()?)
        .bar_execution(false)
        .build()?;
    let bars = BacktestDataConfig::builder()
        .data_type(NautilusDataType::Bar)
        .catalog_path(catalog.clone())
        .bar_types(backtest.bar_types.iter().map(ToString::to_string).collect())
        .build()?;
    let mut data = vec![bars];
    for instrument_id in backtest.instrument_ids() {
        data.push(
            BacktestDataConfig::builder()
                .data_type(NautilusDataType::QuoteTick)
                .catalog_path(catalog.clone())
                .instrument_id(instrument_id)
                .build()?,
        );
    }
    let engine_config = BacktestEngineConfig::builder()
        .catalogs(vec![DataCatalogConfig::new(catalog, None, None)])
        .logging(backtest.logging.clone())
        .build();
    Ok(BacktestRunConfig::builder()
        .id(RUN_ID.to_string())
        .venues(vec![venue_config])
        .data(data)
        .engine(engine_config)
        .start(to_unix_nanos(backtest.engine_start()?)?)
        .end(to_unix_nanos(backtest.end)?)
        .raise_exception(true)
        .build()?)
}

/// Runs `config` with `strategy` on a new `BacktestNode` and returns its
/// one result.
pub(super) fn run<S>(config: BacktestRunConfig, strategy: S) -> anyhow::Result<BacktestResult>
where
    S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    let mut node = BacktestNode::new(vec![config])?;
    node.build()?;
    node.get_engine_mut(RUN_ID)
        .context("no engine for the run")?
        .add_strategy(strategy)?;
    let results = node.run()?;
    let [result] = <[BacktestResult; 1]>::try_from(results)
        .map_err(|results| anyhow!("{} results from one run", results.len()))?;
    Ok(result)
}

/// The one venue of `bar_types`.
fn venue(bar_types: &[BarType]) -> anyhow::Result<Venue> {
    let first = bar_types.first().context("no bar types")?;
    let venue = first.instrument_id().venue;
    ensure!(
        bar_types.iter().all(|bt| bt.instrument_id().venue == venue),
        "bar types span more than one venue"
    );
    Ok(venue)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use jiff::Timestamp;
    use nautilus_common::logging::logger::LoggerConfig;
    use nautilus_model::types::Money;

    use super::*;

    fn backtest() -> Backtest {
        Backtest {
            catalog: PathBuf::from("/data/catalog"),
            bar_types: vec![
                BarType::from("SXR8.IBIS-1-HOUR-LAST-EXTERNAL"),
                BarType::from("EUNL.IBIS-1-HOUR-LAST-EXTERNAL"),
            ],
            start: "2026-07-01T00:00:00Z".parse().unwrap(),
            end: "2026-10-01T00:00:00Z".parse().unwrap(),
            warmup: jiff::SignedDuration::from_hours(720),
            commission: Money::from("3 EUR"),
            balance: Money::from("10000 EUR"),
            logging: LoggerConfig {
                print_config: true,
                ..LoggerConfig::default()
            },
        }
    }

    #[test]
    fn one_netting_margin_venue_named_after_the_bar_types() {
        let config = run_config(&backtest()).unwrap();
        let [venue] = config.venues() else {
            panic!("{:?}", config.venues())
        };
        assert_eq!(venue.name(), "IBIS");
        assert_eq!(venue.oms_type(), OmsType::Netting);
        assert_eq!(venue.account_type(), AccountType::Margin);
        assert_eq!(venue.book_type(), BookType::L1_MBP);
    }

    #[test]
    fn the_account_starts_with_the_balance() {
        let config = run_config(&backtest()).unwrap();
        let balances = config.venues()[0].starting_balances();
        let parsed: Vec<Money> = balances.iter().map(|b| b.parse().unwrap()).collect();
        assert_eq!(parsed, vec![Money::from("10000 EUR")]);
    }

    #[test]
    fn orders_fill_on_quotes_only_at_the_quoted_price() {
        let config = run_config(&backtest()).unwrap();
        let venue = &config.venues()[0];
        assert!(!venue.bar_execution());
        assert!(!venue.liquidity_consumption());
        assert_eq!(
            venue.fill_model().unwrap().to_string(),
            "DefaultFillModel(prob_fill_on_limit=1, prob_slippage=0)"
        );
    }

    #[test]
    fn the_bars_of_every_bar_type_come_from_the_catalog() {
        let config = run_config(&backtest()).unwrap();
        let data = &config.data()[0];
        assert_eq!(data.data_type(), &NautilusDataType::Bar);
        assert_eq!(data.catalog_path(), "/data/catalog");
        assert_eq!(
            data.bar_types().unwrap(),
            [
                "SXR8.IBIS-1-HOUR-LAST-EXTERNAL",
                "EUNL.IBIS-1-HOUR-LAST-EXTERNAL"
            ]
        );
    }

    /// The instrument of each quote data config, after the bars' one.
    fn quoted(config: &BacktestRunConfig) -> Vec<String> {
        config.data()[1..]
            .iter()
            .map(|data| {
                assert_eq!(data.data_type(), &NautilusDataType::QuoteTick);
                assert_eq!(data.catalog_path(), "/data/catalog");
                data.instrument_id().unwrap().to_string()
            })
            .collect()
    }

    #[test]
    fn the_quotes_of_every_instrument_come_from_the_catalog() {
        let config = run_config(&backtest()).unwrap();
        assert_eq!(quoted(&config), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    #[test]
    fn an_instrument_of_several_bar_types_has_its_quotes_loaded_once() {
        let mut backtest = backtest();
        backtest
            .bar_types
            .push(BarType::from("SXR8.IBIS-1-DAY-LAST-EXTERNAL"));
        let config = run_config(&backtest).unwrap();
        assert_eq!(quoted(&config), ["EUNL.IBIS", "SXR8.IBIS"]);
    }

    #[test]
    fn the_catalog_is_registered_for_warmup() {
        let config = run_config(&backtest()).unwrap();
        let catalogs = &config.engine().catalogs;
        assert_eq!(catalogs.len(), 1);
        assert_eq!(catalogs[0].path(), "/data/catalog");
    }

    #[test]
    fn a_relative_catalog_is_made_absolute() {
        let backtest = Backtest {
            catalog: PathBuf::from("catalog"),
            ..backtest()
        };
        let config = run_config(&backtest).unwrap();
        let absolute = std::env::current_dir().unwrap().join("catalog");
        let absolute = absolute.to_str().unwrap();
        assert_eq!(config.data()[0].catalog_path(), absolute);
        assert_eq!(config.engine().catalogs[0].path(), absolute);
    }

    #[test]
    fn the_node_installs_the_logger_as_configured() {
        let config = run_config(&backtest()).unwrap();
        assert!(config.engine().logging.print_config);
    }

    #[test]
    fn runs_from_one_nanosecond_before_the_start_to_the_end_and_raises_errors() {
        let config = run_config(&backtest()).unwrap();
        let ns = |ts: &str| to_unix_nanos(ts.parse::<Timestamp>().unwrap()).unwrap();
        assert_eq!(config.start(), Some(ns("2026-06-30T23:59:59.999999999Z")));
        assert_eq!(config.end(), Some(ns("2026-10-01T00:00:00Z")));
        assert!(config.raise_exception());
    }

    #[test]
    fn rejects_no_bar_types() {
        let backtest = Backtest {
            bar_types: vec![],
            ..backtest()
        };
        assert!(run_config(&backtest).is_err());
    }

    #[test]
    fn rejects_bar_types_on_two_venues() {
        let mut backtest = backtest();
        backtest
            .bar_types
            .push(BarType::from("CSPX.LSEETF-1-HOUR-LAST-EXTERNAL"));
        let err = run_config(&backtest).unwrap_err();
        assert!(err.to_string().contains("venue"), "{err}");
    }

    #[test]
    fn rejects_a_negative_commission() {
        let backtest = Backtest {
            commission: Money::from("-3 EUR"),
            ..backtest()
        };
        assert!(run_config(&backtest).is_err());
    }

    #[test]
    fn rejects_a_start_after_the_end() {
        let b = backtest();
        let backtest = Backtest {
            start: b.end,
            end: b.start,
            ..b
        };
        assert!(run_config(&backtest).is_err());
    }
}
