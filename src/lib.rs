//! helix-trader: strategies on NautilusTrader, from data to live, one at a time.
//!
//! Nautilus provides the engines and the adapters; this crate adds the
//! catalog filling, the roll dates, the costs and the strategies.

pub mod backtest;
pub mod data;
pub mod live;
pub mod strategy;
