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
//!
//! Strategies trade on bars Nautilus builds from quotes; `warmup` asks for
//! their history and hands it back, so that no strategy knows how.

pub mod ema;
pub mod warmup;

use jiff::SignedDuration;
use nautilus_model::{data::BarType, identifiers::InstrumentId};

/// What a strategy needs from data, stated by its config: filling the
/// catalog, backtesting and going live all read it from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRequirements {
    /// The internal bar types the strategy trades and warms up on: their
    /// instruments are those traded (see `instrument_ids`), and the node
    /// sizes its cache to hold their warmup (see `warmup::cache_config`).
    pub bar_types: Vec<BarType>,
    /// How far back before the run the warmup history starts.
    pub warmup: SignedDuration,
}

impl DataRequirements {
    /// The instruments traded, whose history the catalog stores: those of
    /// the bar types, each once, in the order of the bar types.
    #[must_use]
    pub fn instrument_ids(&self) -> Vec<InstrumentId> {
        let mut ids: Vec<InstrumentId> = Vec::new();
        for bar_type in &self.bar_types {
            let id = bar_type.instrument_id();
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_instruments_are_those_of_the_bar_types_each_once_in_order() {
        let requirements = DataRequirements {
            bar_types: vec![
                BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL"),
                BarType::from("EUNL.IBIS-1-HOUR-MID-INTERNAL"),
                BarType::from("SXR8.IBIS-1-DAY-MID-INTERNAL"),
            ],
            warmup: SignedDuration::from_hours(720),
        };
        assert_eq!(
            requirements.instrument_ids(),
            [
                InstrumentId::from("SXR8.IBIS"),
                InstrumentId::from("EUNL.IBIS")
            ]
        );
    }
}
