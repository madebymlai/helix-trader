//! helix-trader: strategies on NautilusTrader, from data to live, one at a time.
//!
//! Nautilus provides the engines and the adapters; this crate adds the
//! catalog filling, the roll dates, the costs and the strategies.

// Tests may read the wall clock and spawn on tokio (clippy.toml).
#![cfg_attr(test, allow(clippy::disallowed_methods))]

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
