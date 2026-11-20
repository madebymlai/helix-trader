//! Makes a strategy's decision in one place.
//!
//! `DecidingModels` lists the model sets in the strategy's model store, swaps them in, picks a side
//! and applies the meta's margin. It sees no strategy, cache or clock, so the handle and the
//! meta's examples decide through the same code.
//!
//! The caller resolves what a decision reads:
//! - The time.
//! - The decision's features.
//! - Each side's row (`SideRow`): the features, then the side's payoff features, in R at the
//!   reference stake.
//!
//! All rows come from `SideRow::new`. The handle builds them from the target it sets
//! (`targets::TargetSetter`). The meta's examples and a train build them from each dataset row
//! (`dataset::DatasetRow::sides`).
//!
//! `new` lists the model sets once and fails on a read error. `list` lists them again, and the
//! caller decides what a failure means. The handle logs it. The meta's examples fail. A failed
//! listing keeps the previous list.
//!
//! A model set is swapped in only inside a decision, before it predicts (`store::Swap`). So no
//! decision mixes sets: its sides and its meta come from one train. If a model set fails its
//! checks, the decision fails with an error naming the model set and the check. The model set is
//! read again at the next decision.
//!
//! `decide` picks among the sides with the evaluation in `pick_among`. A train's meta also learns
//! from that evaluation, and its out-of-fold pair is in no store.
//! - Each side's `E'` (`store::Pair::expect`) is held to its margin in R. `payoff::pick` picks
//!   among the sides by `E'`.
//! - The side picked is taken if the meta model expects a net R of at least its margin
//!   (`earnings::MetaMargin`). The meta model reads the features with that side and the decision's
//!   `C'` and `D'` (`features::meta_row`).
//! - A model set of a strategy with a meta holds a meta model (`store::ModelStore::load`).
//! - An expected R that is not finite clears no margin.

use anyhow::Context;
use jiff::Timestamp;

use super::{
    calibration::Expected,
    earnings::MetaMargin,
    features::{meta_row, side_row},
    payoff,
    store::{ModelSet, ModelStore, Pair, Swap},
    targets::{PerSide, Side},
};
use crate::strategy::risk::R;

/// The strategy's model sets as last listed, and the one swapped in so far.
#[derive(Debug)]
pub struct DecidingModels {
    models: ModelStore,
    /// The time each model set is valid from.
    listed: Vec<Timestamp>,
    model_sets: Swap<ModelSet>,
}

/// The row a decision reads for a side (`features::side_row`).
#[derive(Debug, Clone, PartialEq)]
pub struct SideRow {
    pub features: Vec<f64>,
}

impl SideRow {
    /// Builds the row from a decision's `features` and its target's `payoff` features.
    #[must_use]
    pub fn new(features: &[f64], payoff: &[f64]) -> Self {
        Self {
            features: side_row(features, payoff),
        }
    }
}

/// The side a decision picks, before any meta.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Picked {
    pub side: Side,
    /// The net R the pair expects from the trade (its `E'`).
    pub expected_r: R,
    pub expected: Expected,
}

#[derive(Debug)]
struct SideEvaluation {
    expected: Expected,
    picked: Option<Picked>,
}

#[derive(Debug)]
pub(super) struct RowEvaluation {
    pub expected: Expected,
    pub pick: Option<EvaluatedPick>,
}

#[derive(Debug)]
pub(super) struct EvaluatedPick {
    pub picked: Picked,
    pub meta: Option<MetaVerdict>,
}

/// Keeps a declined estimate too, which may be nonfinite, for scoring.
#[derive(Debug, Clone, Copy)]
pub(super) enum MetaVerdict {
    Accepted(R),
    Declined(f64),
}

impl EvaluatedPick {
    fn taken(self) -> Option<Taken> {
        let meta_expected_r = match self.meta {
            Some(MetaVerdict::Declined(_)) => return None,
            Some(MetaVerdict::Accepted(expected)) => Some(expected),
            None => None,
        };
        Some(Taken {
            side: self.picked.side,
            expected_r: self.picked.expected_r,
            expected: self.picked.expected,
            meta_expected_r,
        })
    }
}

/// A decision to trade the side picked. The meta takes it, if there is one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Taken {
    pub side: Side,
    /// The pair's `E'`.
    pub expected_r: R,
    pub expected: Expected,
    /// The meta model's expected R, if one took the trade.
    pub meta_expected_r: Option<R>,
}

impl DecidingModels {
    /// Lists the model sets in `models`.
    ///
    /// # Errors
    ///
    /// Returns an error if the model sets cannot be listed.
    pub fn new(models: &ModelStore) -> anyhow::Result<Self> {
        Ok(Self {
            listed: models.model_sets()?,
            model_sets: models.swap(),
            models: models.clone(),
        })
    }

    /// Lists the model sets again. A failure keeps the previous list.
    ///
    /// # Errors
    ///
    /// Returns an error if the model sets cannot be listed.
    pub fn list(&mut self) -> anyhow::Result<()> {
        self.listed = self.models.model_sets()?;
        Ok(())
    }

    /// Decides the trade at `now` with the latest model set valid then.
    ///
    /// Returns `None` if:
    /// - No model set is valid yet.
    /// - No side is picked.
    /// - The meta's margin is not met.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The latest model set valid then fails its checks (`Swap::at`).
    /// - A model set with a meta holds no meta model.
    /// - A model fails to predict.
    pub fn decide(
        &mut self,
        now: Timestamp,
        features: &[f64],
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<Option<Taken>> {
        let Some((_, model_set)) = self.model_sets.at(now, &self.listed)? else {
            return Ok(None);
        };
        let row = Self::evaluate(&self.models, model_set, features, sides)?;
        Ok(row.pick.and_then(EvaluatedPick::taken))
    }

    pub(super) fn evaluate(
        models: &ModelStore,
        model_set: &ModelSet,
        features: &[f64],
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<RowEvaluation> {
        let SideEvaluation { expected, picked } =
            Self::evaluate_sides(model_set.pair(), margins(models), sides)?;
        let Some(picked) = picked else {
            return Ok(RowEvaluation {
                expected,
                pick: None,
            });
        };
        let meta = if let Some(level) = models.meta() {
            let model = model_set
                .meta()
                .context("the model set holds no meta model, and the strategy has a meta")?;
            let estimate =
                model.predict(&meta_row(features, picked.side.order_side(), expected))?;
            let margin = MetaMargin(level.config().margin);
            Some(match R::finite(estimate).filter(|&r| margin.takes(r)) {
                Some(r) => MetaVerdict::Accepted(r),
                None => MetaVerdict::Declined(estimate),
            })
        } else {
            None
        };
        Ok(RowEvaluation {
            expected,
            pick: Some(EvaluatedPick { picked, meta }),
        })
    }

    /// Picks the side with the greatest finite `E'` that clears its margin in `margins`.
    ///
    /// Returns `None` if no side clears its margin or two tie (`payoff::pick`).
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn pick_among(
        pair: &Pair,
        margins: impl IntoIterator<Item = (Side, R)>,
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<Option<Picked>> {
        Ok(Self::evaluate_sides(pair, margins, sides)?.picked)
    }

    fn evaluate_sides(
        pair: &Pair,
        margins: impl IntoIterator<Item = (Side, R)>,
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<SideEvaluation> {
        let expected = pair.expect(sides)?;
        let picked = payoff::pick(margins.into_iter().filter_map(|(side, margin)| {
            let expected_r = R::finite(expected.of(side))?;
            let picked = Picked {
                side,
                expected_r,
                expected,
            };
            Some((picked, expected_r, margin))
        }));
        Ok(SideEvaluation { expected, picked })
    }
}

/// Lists the sides `models` trade, the long first, each with its margin.
pub(super) fn margins(models: &ModelStore) -> impl Iterator<Item = (Side, R)> + '_ {
    models
        .sides()
        .map(|traded| (traded.side(), traded.margin()))
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
