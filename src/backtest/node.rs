//! Backtest run over the catalog.
//!
//! Builds a Nautilus `BacktestRunConfig` (`nautilus_backtest::config`): the
//! venue (`BacktestVenueConfig`), the catalog data (`BacktestDataConfig`) and
//! the engine config, then runs it on a `BacktestNode`: `build`, the strategy
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
/// venue, the bars of the window from the catalog, and the catalog
/// registered in the engine so that it serves the strategy's warmup
/// requests. Build and run errors are raised, not only logged.
pub(super) fn run_config(backtest: &Backtest) -> anyhow::Result<BacktestRunConfig> {
    let venue = venue(&backtest.bar_types)?;
    let catalog = backtest
        .catalog
        .to_str()
        .with_context(|| format!("catalog path {} is not UTF-8", backtest.catalog.display()))?
        .to_string();
    let venue_config = BacktestVenueConfig::builder()
        .name(venue.as_str())
        .oms_type(OmsType::Netting)
        .account_type(AccountType::Margin)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec![backtest.balance.to_string()])
        .fee_model(costs::fee_model(backtest.commission)?)
        .fill_model(costs::fill_model()?)
        .bar_adaptive_high_low_ordering(true)
        .build()?;
    let data_config = BacktestDataConfig::builder()
        .data_type(NautilusDataType::Bar)
        .catalog_path(catalog.clone())
        .bar_types(backtest.bar_types.iter().map(ToString::to_string).collect())
        .build()?;
    let engine_config = BacktestEngineConfig::builder()
        .catalogs(vec![DataCatalogConfig::new(catalog, None, None)])
        .build();
    Ok(BacktestRunConfig::builder()
        .id(RUN_ID.to_string())
        .venues(vec![venue_config])
        .data(vec![data_config])
        .engine(engine_config)
        .start(to_unix_nanos(backtest.start)?)
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
        assert!(venue.bar_adaptive_high_low_ordering());
    }

    #[test]
    fn the_account_starts_with_the_balance() {
        let config = run_config(&backtest()).unwrap();
        let balances = config.venues()[0].starting_balances();
        let parsed: Vec<Money> = balances.iter().map(|b| b.parse().unwrap()).collect();
        assert_eq!(parsed, vec![Money::from("10000 EUR")]);
    }

    #[test]
    fn the_bars_of_every_bar_type_come_from_the_catalog() {
        let config = run_config(&backtest()).unwrap();
        let [data] = config.data() else {
            panic!("{:?}", config.data())
        };
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

    #[test]
    fn the_catalog_is_registered_for_warmup() {
        let config = run_config(&backtest()).unwrap();
        let catalogs = &config.engine().catalogs;
        assert_eq!(catalogs.len(), 1);
        assert_eq!(catalogs[0].path(), "/data/catalog");
    }

    #[test]
    fn runs_over_the_window_and_raises_errors() {
        let config = run_config(&backtest()).unwrap();
        let ns = |ts: &str| to_unix_nanos(ts.parse::<Timestamp>().unwrap()).unwrap();
        assert_eq!(config.start(), Some(ns("2026-07-01T00:00:00Z")));
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
