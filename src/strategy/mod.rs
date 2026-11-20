//! Strategies: interchangeable, one runs at a time.
//!
//! Build step 5. The port is the Nautilus `Strategy` trait (`nautilus_trading`): backtest and live
//! are generic over it and never name a concrete strategy, nor does `main`, which picks one by name
//! (`StrategyName`) and hands it a `Visitor` of its subcommand.
//!
//! Each strategy is a folder of its own, `strategy/<name>/mod.rs`, found by the build script
//! (`build.rs`): the folder's name is the strategy's module and command-line name, and no other
//! file names it. Its `mod.rs` exposes its constructor, its config and `visit`, which hands the
//! strategy to a visitor by its kind (`PlainStrategy` or `model::ModelStrategy`). Inside, `mod.rs`
//! is the thin Nautilus wiring: a struct holding a `StrategyCore`, wired with `nautilus_strategy!`,
//! implementing `DataActor` (`on_start`, `on_bar`) and placing orders with Nautilus' own API. Pure
//! modules beside it (the EMA's `signal.rs`) turn bars into decisions, tested without an engine;
//! their trades go through the shared order module (`orders`): a Nautilus bracket or a market
//! entry, closed at its time limit, set again at a restart.
//!
//! Most strategies share a config struct and functions, never a base type. `StandardConfig`, the
//! tables every strategy's TOML has, holds the series read, their warmup and their `Session`, the
//! indicators read (`IndicatorsConfig`), the instruments traded and the model of a strategy trading
//! on one, beside the strategy's own table (its `Part`, which answers the series it reads, those
//! its series list). The exception is a strategy trading on a learned model, which holds the
//! harness' types (`model::features::FeatureSet`, `model::handle::ModelHandle`) and implements
//! `model::ModelStrategy`, its part building its features (`model::ModelPart`); `model_ema` is the
//! example.
//!
//! Strategies trade on bars Nautilus builds, from source bars (IB's 1-minute bars, OKX's candles)
//! or from ticks (quotes, or trades for LAST bars). A `warmup::Warmup`, which each strategy holds,
//! asks for their history live and hands it back, so that no strategy knows how, a backtest
//! requesting none. The warmup asks the history of the ticks of its data requirements too, the
//! quotes of the FX pairs its equity converts through among them, where their venue serves it. A
//! `subscriptions::Subscriptions`, which each strategy holds too (a model strategy's handle),
//! subscribes to the bars, the ticks and the custom series from where the warmup ends. A
//! `rolls::Roller`, held where a root's ticks need rolling (see `README.md`, "Configuration and
//! series"), tells it of each roll of a root, at which it moves the root's ticks. The catalog
//! stores what their bars are built from (see `requirements::catalog_keys`).

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
