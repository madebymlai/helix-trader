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
//! a fixed UTC grid; `walk` is a backtest retraining on that schedule
//! before its run and scoring its models out of sample after.

pub mod config;
pub mod dataset;
pub mod decide;
pub mod features;
pub mod handle;
pub mod learner;
pub mod schedule;
pub mod score;
pub mod store;
pub mod targets;
pub mod train;
pub mod walk;
