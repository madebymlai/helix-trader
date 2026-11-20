//! A strategy's decision, made in one place: `DecidingModels` lists the model sets of its model
//! store, swaps them in, picks a side and applies the meta's margin. It sees no strategy, cache or
//! clock, so the handle and the meta's examples decide through the same code.
//!
//! A caller resolves what a decision reads: the time, the decision's features and each side's row
//! (`SideRow`, the features then the side's payoff features, in R at the reference stake). The
//! handle builds the rows from the target it sets (`targets::TargetSetter`), the meta's examples
//! and a train's from each dataset row (`dataset::DatasetRow::sides`), all through
//! `SideRow::new`.
//!
//! `new` lists the model sets once and fails on a read error. `list` lists them again, and the
//! caller decides what a failure means: the handle logs it, the meta's examples fail. A failed
//! listing keeps the previous list.
//!
//! A model set is swapped in only inside a decision, before it predicts (`store::Swap`), so no
//! decision mixes sets: its sides and its meta come from one train. A model set failing its checks
//! fails the decision with an error naming it and the check, and is read again at the next.
//!
//! `decide` picks among the sides by the evaluation of `pick_among`, which a train's meta also
//! learns from, its out-of-fold pair being in no store. Each side's `E'` (`store::Pair::expect`)
//! is held to its margin in R and picked on by `payoff::pick`. The side picked is taken if the
//! meta model, on the features with that side and the decision's `C'` and `D'`
//! (`features::meta_row`), expects a net R of at least its margin (`earnings::MetaMargin`). A
//! model set of a strategy with a meta holds a meta model (`store::ModelStore::load`). An expected
//! R that is not finite clears no margin.

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

/// A strategy's model sets as last listed, and the one swapped in so far.
#[derive(Debug)]
pub struct DecidingModels {
    models: ModelStore,
    /// When the model sets are valid from.
    listed: Vec<Timestamp>,
    model_sets: Swap<ModelSet>,
}

/// What a decision reads of a side (`features::side_row`).
#[derive(Debug, Clone, PartialEq)]
pub struct SideRow {
    pub features: Vec<f64>,
}

impl SideRow {
    /// Builds the row of a decision's `features` and its target's `payoff` features.
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
    /// The net R the pair expects of its trade, its `E'`.
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

/// Keeps even a declined (possibly nonfinite) estimate for scoring.
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

/// A decision to trade: the side picked, taken by the meta if there is one.
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
    /// Lists the model sets of `models`.
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

    /// Decides the trade at `now` by the latest model set valid then. Returns `None` if no model
    /// set is valid yet, no side is picked or the meta's margin is not met.
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

    /// Picks the side with the greatest finite `E'` that clears its margin of `margins`. Returns
    /// `None` if none clears it or two tie (`payoff::pick`).
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

/// The sides `models` trade, the long first, each with its margin.
pub(super) fn margins(models: &ModelStore) -> impl Iterator<Item = (Side, R)> + '_ {
    models
        .sides()
        .map(|traded| (traded.side(), traded.margin()))
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
