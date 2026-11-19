//! helix-trader: strategies on NautilusTrader, from data to live, one at a time.
//!
//! Nautilus provides the engines, the adapters and the catalog; this crate
//! adds the strategies and their model harness, the roll calendar, the
//! venue routing table and the commands that drive Nautilus from a
//! strategy's config.

pub mod backtest;
pub mod catalog;
pub mod live;
#[cfg(test)]
mod log_capture;
pub mod model;
pub mod rolls;
pub mod strategy;
mod time_range;
pub mod venues;
