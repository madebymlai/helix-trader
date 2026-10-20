//! Strategies: interchangeable, one runs at a time.
//!
//! Build step 5. The port is the Nautilus `Strategy` trait
//! (`nautilus_trading`): backtest and live are generic over it and never
//! name a concrete strategy. `main` picks one by name and builds it.
//!
//! Each strategy is its own submodule and exposes only its constructor and
//! config. Inside, `mod.rs` is the thin Nautilus wiring: a struct holding a
//! `StrategyCore`, wired with `nautilus_strategy!`, implementing `DataActor`
//! (`on_start`, `on_bar`) and placing orders with `submit_order`. `book.rs`
//! is the pure core that turns bars into the target book, tested without an
//! engine.

pub mod ema;

use jiff::SignedDuration;
use nautilus_model::identifiers::InstrumentId;

/// What a strategy needs from data, stated by its config: filling the
/// catalog, backtesting and going live all read it from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRequirements {
    /// The instruments traded, whose history the catalog stores.
    pub instrument_ids: Vec<InstrumentId>,
    /// How far back before the run the warmup history starts.
    pub warmup: SignedDuration,
}
