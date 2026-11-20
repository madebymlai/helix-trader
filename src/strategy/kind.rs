//! The kinds of strategy the command line runs, and the visitor each is handed to by its kind
//! (`StrategyName::visit`).

use std::{fmt::Debug, rc::Rc};

use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{Part, StandardConfig, warmup::Warmup};
use crate::model::ModelStrategy;

/// What the command line does with a strategy, by its kind.
///
/// A strategy's folder calls the method of its kind with its type, in its `visit`:
///
/// ```ignore
/// pub fn visit<V: Visitor>(visitor: V) -> V::Output {
///     visitor.model::<ModelEma>()
/// }
/// ```
pub trait Visitor {
    type Output;

    /// Works on a strategy trading on no model.
    fn plain<S: PlainStrategy>(self) -> Self::Output;

    /// Works on a strategy trading on a model.
    fn model<S: ModelStrategy>(self) -> Self::Output;
}

/// A strategy trading on no model, built of its config.
pub trait PlainStrategy:
    Strategy + StrategyNative + DataActorNative + Component + Debug + Sized + 'static
{
    type Params: Part;

    /// Creates the strategy as a backtest runs it, requesting no warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    fn backtest(config: StandardConfig<Self::Params>) -> anyhow::Result<Self>;

    /// Creates the strategy as a live node runs it, requesting its warmup at start.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    fn live(config: StandardConfig<Self::Params>) -> anyhow::Result<Self>;

    /// Returns the warmup, which a live node sizes its cache from.
    fn warmup(&self) -> Rc<Warmup>;
}
