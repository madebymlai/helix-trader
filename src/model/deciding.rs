//! A strategy's decision, made in one place: `DecidingModels` lists the model sets of its model
//! store, swaps them in, picks a side, and applies the meta's margin. It sees no strategy, cache or
//! clock, so the model handle and the meta's examples decide through the same code, and the meta
//! learns from the decisions the handle would make.
//!
//! A caller resolves first what a decision reads: the time, the decision's features, which a meta
//! reads, and each side's row (`SideRow`), the row its model reads, those features then the side's
//! payoff features, each in R of the side's trade at the reference stake. The handle builds them
//! from the target it sets at the decision (`targets::TargetSetter`), the meta's examples and a
//! train's from each dataset row (`dataset::DatasetRow::sides`), each through `SideRow::new`, so a
//! side's row is the same whoever builds it.
//!
//! `new` lists the model sets once and fails on a read error, so no decision runs unlisted. `list`
//! lists them again, when the caller chooses, and what a failure means is the caller's: the handle
//! lists at each decision and logs a failure; the meta's examples decide on the store as it is when
//! made, and fail if it cannot be read. A failed listing keeps the previous list, so a model set
//! already listed can still swap in once its valid-from passes.
//!
//! A model set is swapped in only inside a decision, before it predicts (`store::Swap`), so no
//! decision mixes two, and its sides and its meta decide together: no decision uses a long and a
//! short of different trains, nor a meta beside sides of another train. A model set failing its
//! checks fails the decision with an error naming it and the check: no side is picked until it
//! loads, and it is read again at the next.
//!
//! `decide` picks among the sides of the model set valid at the time, by the same pair evaluation
//! as `pick_among`, which a train's meta also learns from, its out-of-fold pair being in no store.
//! That is the pair's calibrated expectation of the decision, its level `C'` and side call `D'`
//! (`store::Pair::expect`), each side's `E'` held to its margin in R and picked on by
//! `payoff::pick`. `decide` takes the side picked if the model set's meta model, on the features
//! with that side and the decision's `C'` and `D'` (`features::meta_row`), expects of its trade a
//! net R, its calibrated expected R, of at least its margin in R (`earnings::MetaMargin`); the
//! trade is as the target sets it, whatever the meta expects. A model set of a strategy with a meta
//! holds a meta model (`store::ModelStore::load`). An expected R that is not finite, of a model
//! predicting NaN say, clears no margin.

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

/// A strategy's models as they decide: its model store's model sets as last listed, and the model
/// set swapped in so far.
#[derive(Debug)]
pub struct DecidingModels {
    /// The model store: each side traded, the long first, with its margin.
    models: ModelStore,
    /// When the model sets are valid from, as last listed.
    listed: Vec<Timestamp>,
    /// The model sets, swapped in decision by decision.
    model_sets: Swap<ModelSet>,
}

/// What a decision reads of a side: the row its model reads, the decision's features then the
/// side's payoff features (`features::side_row`).
#[derive(Debug, Clone, PartialEq)]
pub struct SideRow {
    pub features: Vec<f64>,
}

impl SideRow {
    /// The row of a side of a decision of `features` and its target's `payoff` features.
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
    /// The decision's calibrated level and side call.
    pub expected: Expected,
}

/// The pair's calibrated expectation and the pick made from it, before any meta.
#[derive(Debug)]
struct SideEvaluation {
    expected: Expected,
    picked: Option<Picked>,
}

/// One row evaluated by one model set, after model selection.
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
    /// The net R the pair expects of the trade, its `E'`.
    pub expected_r: R,
    /// The decision's calibrated level and side call.
    pub expected: Expected,
    /// The meta model's calibrated expected R of the trade, if one took it.
    pub meta_expected_r: Option<R>,
}

impl DecidingModels {
    /// The models of `models`, its model sets listed, none swapped in yet.
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

    /// Lists the model sets again, for the decisions from now. A failure keeps the previous list.
    ///
    /// # Errors
    ///
    /// Returns an error if the model sets cannot be listed.
    pub fn list(&mut self) -> anyhow::Result<()> {
        self.listed = self.models.model_sets()?;
        Ok(())
    }

    /// The trade decided at `now` on the decision's `features` and each side's row `sides`, by the
    /// latest model set valid then, swapped in: the side picked (`pick_among`), if there is no
    /// meta. With one, the side is taken if the model set's meta model, on the features with that
    /// side and the decision's level and side call, expects of it a net R, its calibrated expected
    /// R, of at least its margin, at the meta model's expected R. None if no model set is valid
    /// yet, no side is picked or the margin is not met.
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

    /// Evaluates the sides and applies the same set's meta only to their pick.
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

    /// The side picked by `pair` on each side's row `sides`, each side traded held to its margin of
    /// `margins`: of the sides whose expected R, their `E'` of the pair's calibrated expectation
    /// (`Pair::expect`), is finite and clears their margin, the one expected the most; none if none
    /// clears it, or two tie (`payoff::pick`).
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

    /// Scores each fitted side and picks from the same joint calibration.
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
