//! Backtests a strategy over the catalog, costs included.
//!
//! Build step 2: one run per process, through a Nautilus `BacktestNode`.
//! Generic over the strategy: any Nautilus strategy runs, none is named here.

mod costs;
mod coverage;
mod node;
mod report;

use std::{fmt::Debug, path::PathBuf};

use jiff::{SignedDuration, Timestamp};
use nautilus_backtest::result::BacktestResult;
use nautilus_common::{
    actor::DataActorNative, component::Component, logging::logger::LoggerConfig,
};
use nautilus_model::{data::BarType, types::Money};
use nautilus_trading::strategy::{Strategy, StrategyNative};

pub use self::report::report;

/// One backtest: a window of the catalog, the bar types replayed and the
/// costs. Every field is required.
#[derive(Debug, Clone)]
pub struct Backtest {
    /// The catalog directory: bars and instruments are read from it, and it
    /// serves the strategy's warmup requests.
    pub catalog: PathBuf,
    /// The bar types replayed, all on one venue; the instruments and the
    /// venue derive from them.
    pub bar_types: Vec<BarType>,
    pub start: Timestamp,
    pub end: Timestamp,
    /// The strategy's warmup: the catalog must cover the bar types from
    /// `start - warmup`, so that the warmup request is answered.
    pub warmup: SignedDuration,
    /// Charged once per order, on its first fill.
    pub commission: Money,
    /// The starting balance of the account.
    pub balance: Money,
    /// The Nautilus logger: the node installs it, so nothing else may.
    pub logging: LoggerConfig,
}

impl Backtest {
    /// Runs `strategy` over the window on a `BacktestNode` and returns the
    /// Nautilus result.
    ///
    /// # Errors
    ///
    /// Returns an error if the inputs are inconsistent (no bar types, several
    /// venues, a negative commission or warmup, `start` not before `end`),
    /// if the catalog misses an instrument or part of a bar type over
    /// `[start - warmup, end]`, or if the node fails to build or run.
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<BacktestResult>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::run_config(self)?;
        coverage::check(self)?;
        node::run(config, strategy)
    }
}
