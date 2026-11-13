//! The kinds of strategy the command line runs, and the visitor each
//! strategy is handed to by its kind (`StrategyName::visit`).

use std::{fmt::Debug, rc::Rc};

use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use super::{Part, StandardConfig, warmup::Warmup};
use crate::model::ModelStrategy;

/// What the command line does with a strategy, by its kind: each method is
/// generic over the strategy, and the strategy's folder calls the one of
/// its kind with its type, in its `visit`:
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

/// A strategy trading on no model: built of its config, as a backtest runs
/// it or as a live node does.
pub trait PlainStrategy:
    Strategy + StrategyNative + DataActorNative + Component + Debug + Sized + 'static
{
    /// Its own part of its config.
    type Params: Part;

    /// The strategy as a backtest runs it: it requests no warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    fn backtest(config: StandardConfig<Self::Params>) -> anyhow::Result<Self>;

    /// The strategy as a live node runs it: it requests its warmup at
    /// start.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    fn live(config: StandardConfig<Self::Params>) -> anyhow::Result<Self>;

    /// Its warmup: a live node sizes its cache from it.
    fn warmup(&self) -> Rc<Warmup>;
}
