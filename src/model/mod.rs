//! The model harness: what a strategy trading on a learned model shares
//! with every other, in backtest, live and the training table alike.
//!
//! `features` is the strategy's feature set, Nautilus indicators fed by
//! Nautilus; `decide` is when the strategy decides and the feature row it
//! publishes at each decision; `learner` is the model port, its learners
//! fitting and loading the models that predict.

pub mod decide;
pub mod features;
pub mod learner;
