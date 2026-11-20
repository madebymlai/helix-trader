//! The kinds of strategy the command line runs.
//!
//! `StrategyName::visit` hands each strategy to the visitor method for its kind.

use std::{fmt::Debug, rc::Rc};

use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{Part, StandardConfig, warmup::Warmup};
use crate::model::ModelStrategy;

/// Handles a strategy for the command line, by its kind.
///
/// The `visit` function in a strategy's folder calls the method for its kind with its type:
///
/// ```ignore
/// pub fn visit<V: Visitor>(visitor: V) -> V::Output {
///     visitor.model::<ModelEma>()
/// }
/// ```
pub trait Visitor {
    type Output;

    /// Works on a strategy that trades on no model.
    fn plain<S: PlainStrategy>(self) -> Self::Output;

    /// Works on a strategy that trades on a model.
    fn model<S: ModelStrategy>(self) -> Self::Output;
}

/// A strategy that trades on no model and is built from its config.
pub trait PlainStrategy:
    Strategy + StrategyNative + DataActorNative + Component + Debug + Sized + 'static
{
    type Params: Part;

    /// Creates the strategy as a backtest runs it. It requests no warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    fn backtest(config: StandardConfig<Self::Params>) -> anyhow::Result<Self>;

    /// Creates the strategy as a live node runs it. It requests its warmup at start.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    fn live(config: StandardConfig<Self::Params>) -> anyhow::Result<Self>;

    /// Returns the warmup. A live node sizes its cache from it.
    fn warmup(&self) -> Rc<Warmup>;
}
