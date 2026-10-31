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

use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use self::{
    config::ModelConfig,
    dataset::{Dataset, DatasetVenue},
    features::FeatureSet,
    handle::Mode,
    train::Training,
    walk::WalkForward,
};
use crate::strategy::{Part, StandardConfig, warmup::Warmup};

/// A strategy trading on a model, as the binary runs it: built of its
/// config in a `Mode`, the features and the model section of that config
/// the harness reads, its dataset, its training and its walk forward built
/// of them. A Nautilus strategy, as a node runs it.
pub trait ModelStrategy:
    Strategy + StrategyNative + DataActorNative + Component + Debug + Sized + 'static
{
    /// Its own part of its config.
    type Params: Part;

    /// The strategy of `config`, its model handle in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the handle cannot be built in `mode`, or if
    /// Nautilus refuses the base.
    fn new(config: StandardConfig<Self::Params>, mode: Mode) -> anyhow::Result<Self>;

    /// Its warmup, its model handle's: a live node sizes its cache from
    /// it.
    fn warmup(&self) -> Rc<Warmup>;

    /// The features its model decides on, of `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if they cannot be built of `config`.
    fn features(config: &StandardConfig<Self::Params>) -> anyhow::Result<Rc<FeatureSet>>;

    /// Its model, of `config`.
    fn model(config: &StandardConfig<Self::Params>) -> ModelConfig;

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
        let (features, model) = (Self::features(config)?, Self::model(config));
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
            Self::model(config),
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
            Self::model(config),
            &config.data_requirements(),
            venue,
        )
    }
}
