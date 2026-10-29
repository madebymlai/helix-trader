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
//! models that predict;
//! `config` is the model section of a strategy's config; `train` trains
//! at a time, fitting on a window of the dataset and publishing in the
//! `store` the model whose holdout `score` passes the gate; `handle` is
//! what the strategy holds and decides through, trading on the latest
//! model of the store valid at each decision, or opening the decision's
//! targets in a dataset run; `schedule` is when a model is retrained, on
//! a fixed UTC grid; `launcher` starts a live strategy's retrains, one
//! at a time, in a child process; `walk` is a backtest retraining on the
//! schedule before its run and scoring its models out of sample after.
//! `meta` is meta-labeling: a meta model deciding whether to take the
//! trade of the primary, the strategy's rule or its model, trained on
//! the primary's out-of-sample predictions.

pub mod config;
pub mod dataset;
pub mod decide;
pub mod features;
pub mod handle;
pub mod launcher;
pub mod learner;
pub mod meta;
pub mod schedule;
pub mod score;
pub mod store;
pub mod targets;
pub mod train;
pub mod walk;
