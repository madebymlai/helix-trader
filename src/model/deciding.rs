//! A strategy's decision, made in one place: `DecidingModels` lists the stores
//! of its model set, swaps the models in, picks a side, and applies the meta's
//! margin. It sees no strategy, cache or clock, so the model
//! handle and the meta's examples decide through the same code, and the meta
//! learns from the decisions the handle would make.
//!
//! A caller resolves first what a decision reads: the time, the decision's
//! features, which a meta reads, and each side's row (`SideRow`), the row its
//! model reads, those features then the side's payoff features, each in R of
//! the side's trade at the reference stake. The
//! handle builds them from the target it sets at the
//! decision (`targets::TargetSetter`), the meta's examples and a train's from
//! each dataset row (`dataset::DatasetRow::sides`), each through
//! `SideRow::new`, so a side's row is the same whoever builds it.
//!
//! `new` lists every store once and fails on a read error, so no decision
//! runs unlisted. `list` lists them again, when the caller chooses, and what a
//! failure means is the caller's: the handle lists at each decision and logs
//! a failure; the meta's examples decide on the stores as they are when made,
//! and fail if they cannot be read. A failed listing keeps the previous lists,
//! all of them, so a decision never mixes a fresh list with a stale one, and a
//! model already listed can still swap in once its valid-from passes.
//!
//! A model is swapped in only inside a decision, before it predicts
//! (`store::Swap`), so no decision mixes two models. The sides are swapped
//! in as a pair (`store::Pair`), the sides one train published, valid from
//! one time, so no decision uses a long and a short of different trains, and
//! a side's model with no other beside it is never used. `pick` picks among
//! the sides of the pair valid at the time, by `pick_among`, the rule for a
//! pair already in hand, which a train's gate also judges its pair by, being
//! in no store yet: the pair's calibrated expectation of the decision, its
//! level `C'` and side call `D'` (`store::Pair::expect`), each side's `E'`
//! held to its margin in R and picked on by `payoff::pick`. `decide` takes
//! the side picked if the meta model valid then, on the features with that
//! side and the decision's `C'` and `D'` (`features::meta_row`), expects of
//! its trade a net R, its calibrated expected R, of at least its margin in R
//! (`holdout::MetaMargin`), the trade as the target sets it whatever the meta
//! expects. The meta's model is read only once a side is picked, and no meta
//! model valid yet is no trade. An expected R that is not finite, of a
//! model predicting NaN say, clears no margin.

use jiff::Timestamp;

use super::{
    calibration::Expected,
    features::{meta_row, side_row},
    holdout::MetaMargin,
    payoff,
    schedule::Models,
    store::{LoadedModel, ModelSet, Pair, Swap},
    targets::{PerSide, Side},
};
use crate::strategy::risk::R;

/// A strategy's models as they decide: its model set's stores as last listed,
/// and the sides' pair and the meta's model swapped in so far.
#[derive(Debug)]
pub struct DecidingModels {
    /// The model set: each side traded, the long first, with its margin.
    models: ModelSet,
    /// When the models of each store are valid from, as last listed.
    listed: Models<Vec<Timestamp>>,
    /// The sides' pairs, swapped in decision by decision.
    pairs: Swap<Pair>,
    meta: Option<MetaSwap>,
}

/// The meta level: its margin and its models, swapped in decision by
/// decision.
#[derive(Debug)]
struct MetaSwap {
    margin: MetaMargin,
    swap: Swap<LoadedModel>,
}

/// What a decision reads of a side: the row its model reads, the decision's
/// features then the side's payoff features (`features::side_row`).
#[derive(Debug, Clone, PartialEq)]
pub struct SideRow {
    pub features: Vec<f64>,
}

impl SideRow {
    /// The row of a side of a decision of `features` and its target's
    /// `payoff` features.
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
    /// The models of `models`, every store listed, none swapped in yet.
    ///
    /// # Errors
    ///
    /// Returns an error if a store cannot be read.
    pub fn new(models: &ModelSet) -> anyhow::Result<Self> {
        Ok(Self {
            listed: models.valid_froms()?,
            pairs: models.pairs(),
            meta: models.meta().map(|level| MetaSwap {
                margin: MetaMargin(level.config().margin),
                swap: level.swap(),
            }),
            models: models.clone(),
        })
    }

    /// Lists every store again, for the decisions from now. A failure keeps
    /// the previous lists, all of them.
    ///
    /// # Errors
    ///
    /// Returns an error if a store cannot be read.
    pub fn list(&mut self) -> anyhow::Result<()> {
        self.listed = self.models.valid_froms()?;
        Ok(())
    }

    /// The side picked at `now` on each side's row `sides`, among the sides
    /// of the latest pair valid then, swapped in (`pick_among`); none without
    /// a pair valid, no side clearing its margin, or two tied.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn pick(
        &mut self,
        now: Timestamp,
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<Option<Picked>> {
        let Some((_, pair)) = self.pairs.at(now, &self.listed.pairs()) else {
            return Ok(None);
        };
        let margins = self
            .models
            .sides()
            .map(|traded| (traded.side(), traded.margin()));
        Self::pick_among(pair, margins, sides)
    }

    /// The trade decided at `now` on the decision's `features` and each side's
    /// row `sides`: the side picked (`pick`), if there is no meta; with one,
    /// taken if the meta model valid then, swapped in, on the features with
    /// that side and the decision's level and side call, expects of it a net
    /// R, its calibrated expected R, of at least its margin, at the meta
    /// model's expected R. None if no side is picked, no meta model is valid
    /// yet or the margin is not met.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn decide(
        &mut self,
        now: Timestamp,
        features: &[f64],
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<Option<Taken>> {
        let Some(picked) = self.pick(now, sides)? else {
            return Ok(None);
        };
        let taken = |meta_expected_r| Taken {
            side: picked.side,
            expected_r: picked.expected_r,
            expected: picked.expected,
            meta_expected_r,
        };
        let (Some(meta), Some(valid_froms)) = (&mut self.meta, &self.listed.meta) else {
            return Ok(Some(taken(None)));
        };
        let Some((_, model)) = meta.swap.at(now, valid_froms) else {
            return Ok(None);
        };
        let expected_r = model.predict(&meta_row(
            features,
            picked.side.order_side(),
            picked.expected,
        ))?;
        Ok(R::finite(expected_r)
            .filter(|&expected_r| meta.margin.takes(expected_r))
            .map(|expected_r| taken(Some(expected_r))))
    }

    /// The side picked by `pair` on each side's row `sides`, each side
    /// traded held to its margin of `margins`: of the sides whose expected
    /// R, their `E'` of the pair's calibrated expectation (`Pair::expect`),
    /// is finite and clears their margin, the one expected the most; none if
    /// none clears it, or two tie (`payoff::pick`).
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn pick_among(
        pair: &Pair,
        margins: impl IntoIterator<Item = (Side, R)>,
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<Option<Picked>> {
        let expected = pair.expect(sides)?;
        Ok(payoff::pick(margins.into_iter().filter_map(
            |(side, margin)| {
                let expected_r = R::finite(expected.of(side))?;
                let picked = Picked {
                    side,
                    expected_r,
                    expected,
                };
                Some((picked, expected_r, margin))
            },
        )))
    }
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
