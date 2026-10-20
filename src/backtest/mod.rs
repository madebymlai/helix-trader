//! Backtests a strategy over the catalog, costs included.
//!
//! Build step 2: one run per process, through a Nautilus `BacktestNode`.
//! Generic over the strategy: any Nautilus strategy runs, none is named here.

mod costs;
mod coverage;
mod node;
mod report;

use std::{collections::BTreeSet, fmt::Debug, path::PathBuf};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_backtest::result::BacktestResult;
use nautilus_common::{
    actor::DataActorNative, component::Component, logging::logger::LoggerConfig,
};
use nautilus_model::{data::BarType, identifiers::InstrumentId, types::Money};
use nautilus_trading::strategy::{Strategy, StrategyNative};

pub use self::report::report;

/// One backtest: a window of the catalog, the instruments whose quotes are
/// replayed and the costs. Every field is required.
#[derive(Debug, Clone)]
pub struct Backtest {
    /// The catalog directory: instruments and quotes are read from it, and
    /// it serves the strategy's warmup requests.
    pub catalog: PathBuf,
    /// The internal bar types the strategy trades and warms up on. Their
    /// instruments, all on one venue, which the venue of the run derives
    /// from, are those whose quotes are replayed: Nautilus builds the bars
    /// from these quotes. The engine's cache is sized to hold their warmup
    /// bars, which reach the strategy only through it.
    pub bar_types: Vec<BarType>,
    pub start: Timestamp,
    pub end: Timestamp,
    /// The strategy's warmup: the catalog must cover the quotes from
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
    /// Returns an error if the inputs are inconsistent (no instruments,
    /// several venues, a negative commission or warmup, `start` not before
    /// `end`), if the cache cannot hold the warmup bars of a bar type (see
    /// `warmup::cache_config`), if the catalog misses an instrument or part of its quotes
    /// over `[start - 1 ns - warmup, end]`, or if the node fails to build
    /// or run.
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<BacktestResult>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let config = node::run_config(self)?;
        coverage::check(self)?;
        node::run(config, strategy)
    }

    /// When the engine starts: 1 ns before `start`. A minute's quote is
    /// stamped 1 ns before its close, and the engine skips data before its
    /// start, so a run starting on a bar's close would otherwise miss the
    /// quote that closes that bar, and reject an order sent on it. The
    /// strategy's warmup ends here too.
    fn engine_start(&self) -> anyhow::Result<Timestamp> {
        self.start
            .checked_sub(SignedDuration::from_nanos(1))
            .with_context(|| format!("no engine start before {}", self.start))
    }

    /// The instruments of the bar types, each once.
    fn instruments(&self) -> BTreeSet<InstrumentId> {
        self.bar_types.iter().map(BarType::instrument_id).collect()
    }
}
