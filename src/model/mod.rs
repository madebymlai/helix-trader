//! The model harness: what a strategy trading on a learned model shares
//! with every other, in backtest, live and the training table alike.
//!
//! `features` is the strategy's feature set, Nautilus indicators fed by
//! Nautilus; `decide` is when the strategy decides and the feature row it
//! publishes at each decision; `targets` is the outcome of each decision,
//! read from real orders on the simulated venue of a dataset run;
//! `dataset` is the settled rows of a strategy stored by day in the
//! catalog, its coverage the cache of the days built;
//! `learner` is the model port, its learners fitting and loading the
//! models that predict; `calibration` turns their predictions into win
//! rates; `payoff` is what a side's trade is expected to make and the
//! side a decision trades;
//! `config` is the model section of a strategy's config; `train` trains
//! at a time, fitting a model of each side traded on a window of the
//! dataset and publishing in the `store` each whose `holdout`, what its
//! decisions would have earned there and its `score`, meets the minimums;
//! `handle` is
//! what the strategy holds and decides through, trading on the latest
//! model of the store valid at each decision, or opening the decision's
//! targets in a dataset run; `schedule` is when a model is retrained, on
//! a fixed UTC grid; `launcher` starts a live strategy's retrains, one
//! at a time, in a child process; `walk` is a backtest retraining on the
//! schedule before its run and scoring its models out of sample after.
//! `meta` is meta-labeling: a meta model deciding whether to take the
//! trade of the primary, the strategy's rule or its model, trained on
//! the primary's out-of-sample predictions.
//!
//! `ModelStrategy` is what a strategy trading on a model gives the
//! harness: the binary's backtest, live, train and dataset runs are
//! generic over it.

pub mod calibration;
pub mod config;
pub mod dataset;
pub mod decide;
pub mod features;
pub mod handle;
pub mod holdout;
pub mod launcher;
pub mod learner;
pub mod meta;
pub mod payoff;
pub mod schedule;
pub mod score;
pub mod store;
pub mod targets;
pub mod train;
pub mod walk;

use std::{fmt::Debug, path::Path, rc::Rc};

use anyhow::ensure;
use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use self::{
    dataset::{Dataset, DatasetVenue},
    features::FeatureSet,
    handle::Mode,
    train::Training,
    walk::WalkForward,
};
use crate::strategy::{Part, Series, StandardConfig, catalog_series, warmup::Warmup};

/// A strategy trading on a model, as the binary runs it: built of its
/// config in a `Mode`, deciding on the features of that config, its
/// dataset, its training and its walk forward built of them and of the
/// config's model (`StandardConfig::model`). A Nautilus strategy, as a
/// node runs it.
///
/// The features are built of the config once on each of those paths
/// (`features`), and checked against its `[data]` there: a run starts only
/// if they read every series `[data]` lists but those the barrier's ATR
/// reads, and none it does not list.
pub trait ModelStrategy:
    Strategy + StrategyNative + DataActorNative + Component + Debug + Sized + 'static
{
    /// Its own part of its config, which trades on a model
    /// (`Part::TAKES_MODEL`).
    type Params: Part;

    /// The strategy of `config`, deciding on `features`, those of `config`
    /// checked against its `[data]` (`features`), its model handle in
    /// `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the handle cannot be built in `mode`, or if
    /// Nautilus refuses the base.
    fn with_features(
        config: StandardConfig<Self::Params>,
        features: Rc<FeatureSet>,
        mode: Mode,
    ) -> anyhow::Result<Self>;

    /// Its warmup, its model handle's: a live node sizes its cache from
    /// it.
    fn warmup(&self) -> Rc<Warmup>;

    /// The features its model decides on, as its code builds them of
    /// `config`; `features` checks them.
    ///
    /// # Errors
    ///
    /// Returns an error if they cannot be built of `config`.
    fn build_features(config: &StandardConfig<Self::Params>) -> anyhow::Result<Rc<FeatureSet>>;

    /// The strategy of `config`, deciding on its features (`features`),
    /// its model handle in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error as `features` and `with_features` do.
    fn new(config: StandardConfig<Self::Params>, mode: Mode) -> anyhow::Result<Self> {
        let features = Self::features(&config)?;
        Self::with_features(config, features, mode)
    }

    /// The features its model decides on, of `config` (`build_features`),
    /// checked against `[data]`.
    ///
    /// # Errors
    ///
    /// Returns an error as `build_features` does, or naming each series
    /// the features read that `[data]` does not list, and each `[data]`
    /// lists that neither they nor the barrier's ATR read.
    fn features(config: &StandardConfig<Self::Params>) -> anyhow::Result<Rc<FeatureSet>> {
        let features = Self::build_features(config)?;
        check_series(&features, config)?;
        Ok(features)
    }

    /// The dataset of the strategy `config` describes, run on `venue`, in
    /// the catalog at `catalog`; its replay runs the strategy in
    /// `Mode::Dataset`.
    ///
    /// # Errors
    ///
    /// Returns an error as `features` and `Dataset::new` do.
    fn dataset(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Dataset> {
        let (features, model) = (Self::features(config)?, config.model());
        Dataset::new(
            catalog,
            config.strategy_id(),
            &features,
            model.decision,
            model.targets,
            &config.data_requirements(),
            venue,
        )
    }

    /// The training of the model of the strategy `config` describes, on
    /// its dataset, run on `venue`, in the catalog at `catalog`, and its
    /// models in the store under `models`.
    ///
    /// # Errors
    ///
    /// Returns an error as `features` and `Training::new` do.
    fn training(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        models: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Training> {
        let features = Self::features(config)?;
        Training::new(
            catalog,
            models,
            config.strategy_id(),
            &features,
            config.model(),
            &config.data_requirements(),
            venue,
        )
    }

    /// The backtests of the model of the strategy `config` describes, on
    /// its dataset, run on `venue`, in the catalog at `catalog`, and its
    /// models in the store under `models`: their retrains and their
    /// scores.
    ///
    /// # Errors
    ///
    /// Returns an error as `features` and `WalkForward::new` do.
    fn walk_forward(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        models: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<WalkForward> {
        let features = Self::features(config)?;
        WalkForward::new(
            catalog,
            models,
            config.strategy_id(),
            &features,
            config.model(),
            &config.data_requirements(),
            venue,
        )
    }
}

/// Checks that `features`, of `config`, read the series of its `[data]`:
/// each it lists, but one the barrier's ATR reads (its bars, or the ticks
/// they are built from, `catalog_series`), and none it does not list.
///
/// # Errors
///
/// Returns an error naming each series read and not listed, then each
/// listed and read by neither.
fn check_series<P: Part>(features: &FeatureSet, config: &StandardConfig<P>) -> anyhow::Result<()> {
    let read: Vec<Series> = features
        .bar_types()
        .into_iter()
        .map(Series::Bars)
        .chain(features.ticks().into_iter().map(Series::Ticks))
        .collect();
    let model = config.model();
    let atr = model
        .targets
        .atr_bars(model.decision, &config.data_requirements())?;
    let atr_reads = |series: &Series| {
        atr.as_ref().is_some_and(|atr| match series {
            Series::Bars(bar_type) => *bar_type == atr.bar_type,
            Series::Ticks(ticks) => {
                catalog_series(&[atr.bar_type]).contains(&ticks.catalog_series())
            }
        })
    };
    let listed = &config.data.series;
    let unlisted = named(read.iter().filter(|series| !listed.contains(series)));
    let unread = named(
        listed
            .iter()
            .filter(|series| !read.contains(series) && !atr_reads(series)),
    );
    let mismatches: Vec<String> = [
        unlisted
            .map(|series| format!("the features read {series}, which [data] series does not list")),
        unread.map(|series| {
            format!(
                "[data] series lists {series}, which neither the features nor the barrier's ATR \
                 read"
            )
        }),
    ]
    .into_iter()
    .flatten()
    .collect();
    ensure!(
        mismatches.is_empty(),
        "the series of [data] are not those the features read: {}",
        mismatches.join("; ")
    );
    Ok(())
}

/// `series` named in a list, if any.
fn named<'a>(series: impl Iterator<Item = &'a Series>) -> Option<String> {
    let names: Vec<String> = series.map(ToString::to_string).collect();
    (!names.is_empty()).then(|| names.join(", "))
}

#[cfg(test)]
mod tests {
    use nautilus_common::actor::DataActor;
    use nautilus_indicators::average::sma::SimpleMovingAverage;
    use nautilus_model::{
        data::NautilusDataType, enums::PriceType, identifiers::StrategyId, types::Money,
    };
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };
    use rust_decimal::Decimal;
    use serde::Deserialize;

    use super::*;
    use crate::{
        model::{dataset::Commission, handle::SettledRows},
        strategy::Series,
    };

    /// A strategy trading on a model whose features read the series its
    /// own `features` lists, an average of each.
    #[derive(Debug)]
    struct Probe {
        core: StrategyCore,
        warmup: Rc<Warmup>,
    }

    nautilus_strategy!(Probe);

    impl DataActor for Probe {}

    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ProbeParams {
        features: Vec<Series>,
    }

    impl Part for ProbeParams {
        const INSTRUMENTS: usize = 1;
        const TAKES_MODEL: bool = true;

        fn default_base() -> StrategyConfig {
            StrategyConfig {
                strategy_id: Some(StrategyId::from("PROBE-001")),
                ..Default::default()
            }
        }
    }

    impl ModelStrategy for Probe {
        type Params = ProbeParams;

        fn with_features(
            config: StandardConfig<ProbeParams>,
            _features: Rc<FeatureSet>,
            _mode: Mode,
        ) -> anyhow::Result<Self> {
            let warmup = Rc::new(Warmup::replayed(&config.data_requirements()));
            Ok(Self {
                core: StrategyCore::new_checked(config.base)?,
                warmup,
            })
        }

        fn warmup(&self) -> Rc<Warmup> {
            Rc::clone(&self.warmup)
        }

        fn build_features(config: &StandardConfig<ProbeParams>) -> anyhow::Result<Rc<FeatureSet>> {
            let average = || SimpleMovingAverage::new(2, Some(PriceType::Mid));
            let builder = config.own.features.iter().enumerate().fold(
                FeatureSet::builder(),
                |builder, (i, series)| {
                    let name = i.to_string();
                    match series {
                        Series::Bars(bar_type) => builder.bars(*bar_type, &name, average()),
                        Series::Ticks(ticks) => match &ticks.data {
                            NautilusDataType::QuoteTick => {
                                builder.quotes(ticks.instrument_id, &name, average())
                            }
                            NautilusDataType::TradeTick => {
                                builder.trades(ticks.instrument_id, &name, average())
                            }
                            NautilusDataType::Custom { type_name } => {
                                builder.data(type_name, ticks.instrument_id, &name, |_| 0.0)
                            }
                            data => panic!("the probe reads no {data}"),
                        },
                    }
                },
            );
            builder.build()
        }
    }

    const GOLD_BARS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } }"#;
    const GC_TRADES: &str = r#"{ data = "trades", instrument_id = "GC.GLBX" }"#;

    /// The probe trading gold, its features reading `features`, its
    /// `[data]` listing `series`, deciding hourly on the `targets`.
    fn config(features: &[&str], series: &[&str], targets: &str) -> StandardConfig<ProbeParams> {
        let label = if targets.contains("barrier") {
            "barrier"
        } else {
            "horizon"
        };
        StandardConfig::load(&format!(
            r#"
            features = [{}]

            [data]
            series = [{}]
            warmup = "14h"

            [trade]
            instruments = [{{ instrument_id = "XAUUSD.ICMARKETS", trade_size = "1" }}]

            [model]
            decision = {{ every = "1h" }}
            targets = {targets}
            label = "{label}"
            retrain = "24h"
            window = "48h"
            holdout = "12h"
            margins = {{ long = 0.5, short = 0.5 }}
            min_pnl = 0.5
            min_trades = 30
            learner = {{ kind = "baseline" }}
            "#,
            features.join(", "),
            series.join(", "),
        ))
        .unwrap()
    }

    const HORIZON: &str = r#"{ horizon = "30m" }"#;
    /// A barrier in ATRs: Nautilus' ATR of 14 hourly bars of gold.
    const BARRIER: &str =
        r#"{ barrier = { take_profit = { atr = 2 }, stop = { atr = 1 }, limit = "45m" } }"#;

    /// The message of the error refusing the features of `config`.
    fn refused(config: &StandardConfig<ProbeParams>) -> String {
        format!("{:#}", Probe::features(config).unwrap_err())
    }

    #[test]
    fn features_reading_every_series_of_data_and_no_other_are_built() {
        let config = config(&[GOLD_BARS, GC_TRADES], &[GC_TRADES, GOLD_BARS], HORIZON);

        let features = Probe::features(&config).unwrap();

        assert_eq!(features.names().len(), 2);
    }

    #[test]
    fn a_series_of_data_no_feature_reads_is_refused_naming_it() {
        let config = config(&[GOLD_BARS], &[GOLD_BARS, GC_TRADES], HORIZON);

        let err = refused(&config);

        assert!(
            err.contains(
                "TradeTick of GC.GLBX, which neither the features nor the barrier's ATR read"
            ),
            "{err}"
        );
    }

    #[test]
    fn a_series_the_features_read_that_data_does_not_list_is_refused_naming_it() {
        let config = config(&[GOLD_BARS, GC_TRADES], &[GOLD_BARS], HORIZON);

        let err = refused(&config);

        assert!(
            err.contains("TradeTick of GC.GLBX, which [data] series does not list"),
            "{err}"
        );
    }

    /// Hourly bars of silver the features read and `[data]` does not list,
    /// the trades of GC `[data]` lists and no feature reads: both named.
    #[test]
    fn both_mismatches_are_refused_at_once() {
        let silver_bars = GOLD_BARS
            .replace("XAUUSD", "XAGUSD")
            .replace("MINUTE", "HOUR");
        let config = config(&[GOLD_BARS, &silver_bars], &[GOLD_BARS, GC_TRADES], HORIZON);

        let err = refused(&config);

        assert!(
            err.contains("XAGUSD.ICMARKETS-1-HOUR-MID-INTERNAL")
                && err.contains("TradeTick of GC.GLBX"),
            "{err}"
        );
    }

    /// The features read GC's trades alone; the barrier's ATR is built of
    /// the quotes of the gold traded, which `[data]` lists for it.
    #[test]
    fn ticks_the_barrier_atr_reads_alone_are_read() {
        let gold_quotes = r#"{ data = "quotes", instrument_id = "XAUUSD.ICMARKETS" }"#;

        Probe::features(&config(&[GC_TRADES], &[GC_TRADES, gold_quotes], BARRIER)).unwrap();
        let err = refused(&config(&[GC_TRADES], &[GC_TRADES, gold_quotes], HORIZON));
        assert!(err.contains("QuoteTick of XAUUSD.ICMARKETS"), "{err}");
    }

    /// Deciding hourly, the barrier's ATR runs on gold's hourly bars of
    /// mids, which `[data]` lists and no feature reads.
    #[test]
    fn bars_the_barrier_atr_runs_on_alone_are_read() {
        let gold_hours = GOLD_BARS.replace("MINUTE", "HOUR");

        Probe::features(&config(&[GC_TRADES], &[GC_TRADES, &gold_hours], BARRIER)).unwrap();
        let err = refused(&config(&[GC_TRADES], &[GC_TRADES, &gold_hours], HORIZON));
        assert!(
            err.contains("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL"),
            "{err}"
        );
    }

    /// Every run builds the features of the config and checks them first:
    /// a backtest's walk forward and run, a live node's strategy, a
    /// training and a dataset.
    #[test]
    fn no_strategy_dataset_training_or_walk_forward_is_built_of_mismatched_series() {
        let config = config(&[GOLD_BARS], &[GOLD_BARS, GC_TRADES], HORIZON);
        let dir = tempfile::tempdir().unwrap();
        let venue = DatasetVenue {
            commission: Commission::Rate("0.00003".to_string()),
            balance: Money::from("10000000 USD"),
            leverage: Decimal::ONE,
        };
        let mode = Mode::Dataset {
            rows: SettledRows::default(),
        };
        let (catalog, models) = (dir.path().join("catalog"), dir.path().join("models"));

        for (run, err) in [
            ("strategy", Probe::new(config.clone(), mode).err()),
            ("dataset", Probe::dataset(&config, &catalog, &venue).err()),
            (
                "training",
                Probe::training(&config, &catalog, &models, &venue).err(),
            ),
            (
                "walk forward",
                Probe::walk_forward(&config, &catalog, &models, &venue).err(),
            ),
        ] {
            let err = format!("{:#}", err.unwrap_or_else(|| panic!("{run} built")));
            assert!(err.contains("TradeTick of GC.GLBX"), "{run}: {err}");
        }
    }
}
