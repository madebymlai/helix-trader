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
use nautilus_common::{
    actor::DataActorNative, component::Component, logging::logger::LoggerConfig,
};
use nautilus_execution::models::fee::FeeModelAny;
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::InstrumentId,
    types::Money,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

pub use self::{
    costs::{commission_per_order, commission_rate},
    report::{Outcome, report},
};
use crate::strategy::catalog_series;

/// One backtest: a window of the catalog, the bar types whose source data
/// is replayed and the costs. Every field is required.
#[derive(Debug, Clone)]
pub struct Backtest {
    /// The catalog directory: instruments and the series the bars are
    /// built from are read from it, and it serves the strategy's warmup
    /// requests.
    pub catalog: PathBuf,
    /// The internal bar types the strategy trades and warms up on. Their
    /// instruments are all on one venue, which the venue of the run derives
    /// from. The series Nautilus builds the bars from (see
    /// `strategy::catalog_series`: the quotes of their instrument, or the
    /// source bars of a composite bar type) are those replayed. The
    /// engine's cache is sized to hold their warmup bars, which reach the
    /// strategy only through it.
    pub bar_types: Vec<BarType>,
    pub start: Timestamp,
    pub end: Timestamp,
    /// The strategy's warmup: the catalog must cover the series from
    /// `start - warmup`, so that the warmup request is answered.
    pub warmup: SignedDuration,
    /// The commission: `commission_per_order` or `commission_rate`.
    pub fee_model: FeeModelAny,
    /// The starting balance of the account.
    pub balance: Money,
    /// The account's leverage on every instrument, positive: a position
    /// locks its value / leverage times the instrument's margin (Nautilus'
    /// default margin model). 1 is no leverage.
    pub leverage: Decimal,
    /// The Nautilus logger: the node installs it, so nothing else may.
    pub logging: LoggerConfig,
}

impl Backtest {
    /// Runs `strategy` over the window on a `BacktestNode` and returns the
    /// Nautilus result with the costs of the run's fills.
    ///
    /// # Errors
    ///
    /// Returns an error if the inputs are inconsistent (no instruments,
    /// several venues, a negative warmup, `start` not before
    /// `end`, a leverage not positive), if the cache cannot hold the warmup bars of a bar type (see
    /// `warmup::cache_config`), if the catalog misses an instrument or part of a series
    /// over `[start - 1 ns - warmup, end]`, if the node fails to build
    /// or run, or if a fill of an instrument with quotes has none before
    /// it.
    pub fn run<S>(&self, strategy: S) -> anyhow::Result<Outcome>
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

    /// The catalog series the bars are built from (see
    /// `strategy::catalog_series`).
    fn series(&self) -> Vec<(NautilusDataType, String)> {
        catalog_series(&self.bar_types)
    }
}
