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
//! `store` the model whose holdout `score` passes the gate.

pub mod config;
pub mod dataset;
pub mod decide;
pub mod features;
pub mod learner;
pub mod score;
pub mod store;
pub mod targets;
pub mod train;
