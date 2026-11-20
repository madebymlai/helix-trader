//! Strategies: interchangeable, one runs at a time.
//!
//! Build step 5. The port is the Nautilus `Strategy` trait (`nautilus_trading`): backtest and
//! live are generic over it and never name a concrete strategy, nor does `main`, which picks one
//! by name (`StrategyName`) and hands it a `Visitor` of its subcommand.
//!
//! Each strategy is a folder `strategy/<name>/mod.rs`, found by the build script (`build.rs`):
//! the folder's name is its module and command-line name, and no other file names it. Its
//! `mod.rs` exposes its constructor, its config and `visit`, which hands it to a visitor by its
//! kind (`PlainStrategy` or `model::ModelStrategy`), and is the thin Nautilus wiring: a
//! `StrategyCore`, `nautilus_strategy!`, `DataActor` hooks and Nautilus' own order API. Pure
//! modules beside it (the EMA's `signal.rs`) turn bars into decisions, tested without an
//! engine; trades go through `orders`.
//!
//! Strategies share a config struct and functions, never a base type. `StandardConfig` holds
//! the tables every strategy's TOML has beside the strategy's own (its `Part`, which answers
//! the series it reads). A strategy trading on a learned model holds the harness' types
//! (`model::features::FeatureSet`, `model::handle::ModelHandle`) and implements
//! `model::ModelStrategy`, its part building its features (`model::ModelPart`); `model_ema` is
//! the example.
//!
//! Strategies trade on bars Nautilus builds from source bars (IB's 1-minute bars, OKX's
//! candles) or from ticks (quotes, or trades for LAST bars). Each holds a `warmup::Warmup`,
//! which asks live for the history of its data requirements, including the quotes of its FX
//! conversion pairs where their venue serves it, and a `subscriptions::Subscriptions`, which
//! subscribes from where the warmup ends. A backtest requests no warmup. A `rolls::Roller`,
//! held where a root's ticks need rolling (see `README.md`, "Configuration and series"), tells
//! it of each roll. The catalog stores what the bars are built from
//! (`requirements::catalog_keys`).

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

// The strategies, a module each, and `StrategyName` (see `build.rs`)
include!(concat!(env!("OUT_DIR"), "/strategies.rs"));

pub use indicators::{AtrArgs, Declaration, Declared, EmaArgs, IndicatorArgs, IndicatorsConfig};
pub use kind::{PlainStrategy, Visitor};
pub use session::Session;
pub use standard::{DataConfig, Part, Series, StandardConfig, TradeConfig, TradedInstrument};
