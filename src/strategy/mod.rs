//! Holds the strategies, which are interchangeable: one runs at a time.
//!
//! This is build step 5. The port is the Nautilus `Strategy` trait (`nautilus_trading`). Backtest
//! and live are generic over it and never name a concrete strategy. `main` does not name one
//! either. It picks a strategy by name (`StrategyName`) and hands it a `Visitor` for its
//! subcommand.
//!
//! Each strategy is a folder `strategy/<name>/mod.rs`. The build script (`build.rs`) finds it.
//! The folder name is the module name and the command-line name, and no other file names it.
//!
//! The `mod.rs` is thin Nautilus wiring: a `StrategyCore`, `nautilus_strategy!`, `DataActor`
//! hooks and Nautilus' own order API. It exposes:
//! - the constructor
//! - the config
//! - `visit`, which hands the strategy to a visitor by its kind (`PlainStrategy` or
//!   `model::ModelStrategy`)
//!
//! Pure modules beside it turn bars into decisions and are tested without an engine. The EMA's
//! `signal.rs` is one. Trades go through `orders`.
//!
//! Strategies share a config struct and functions, never a base type. `StandardConfig` holds the
//! tables that every strategy's TOML has beside the strategy's own. The strategy's own is its
//! `Part`, which answers the series it reads. A strategy trading on a learned model holds the
//! harness' types (`model::features::FeatureSet`, `model::handle::ModelHandle`) and implements
//! `model::ModelStrategy`. Its part builds the features (`model::ModelPart`). `model_ema` is the
//! example.
//!
//! Strategies trade on bars that Nautilus builds from source bars (IB's 1-minute bars, OKX's
//! candles) or from ticks (quotes, or trades for LAST bars). Each strategy holds:
//! - a `warmup::Warmup`, which asks live for the history of its data requirements, including the
//!   quotes of its FX conversion pairs where their venue serves it
//! - a `subscriptions::Subscriptions`, which subscribes from where the warmup ends
//!
//! A backtest requests no warmup. A `rolls::Roller` tells the strategy of each roll. It is held
//! where a root's ticks need rolling (see `README.md`, "Configuration and series"). The catalog
//! stores what the bars are built from (`requirements::catalog_keys`).

pub mod conversions;
mod indicators;
mod kind;
pub mod orders;
pub mod requirements;
pub mod risk;
mod session;
mod standard;
pub mod subscriptions;
pub mod warmup;

// Includes one module per strategy and `StrategyName` (see `build.rs`)
include!(concat!(env!("OUT_DIR"), "/strategies.rs"));

pub use indicators::{
    AtrArgs, Declaration, Declared, EmaArgs, IndicatorArgs, IndicatorsConfig, SmaArgs,
};
pub use kind::{PlainStrategy, Visitor};
pub use session::Session;
pub use standard::{DataConfig, Part, Series, StandardConfig, TradeConfig, TradedInstrument};
