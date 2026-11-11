//! A strategy's decision, made in one place: `DecidingModels` lists the stores
//! of its model set, swaps the models in, picks a side, and applies the meta's
//! threshold and size. It sees no strategy, cache or clock, so the model
//! handle and the meta's examples decide through the same code, and the meta
//! learns from the decisions the handle would make.
//!
//! A caller resolves first what a decision reads: the time, the decision's
//! features, which a meta reads, and each side's row (`SideRow`), the row its
//! model reads, those features then the side's payoff features, with its
//! unit, the worth of a stop with exits, one without, by which its expected
//! R makes its expected net P&L (`pick_among`). The handle builds them from the target
//! it sets at the decision (`targets::TargetSetter`), the meta's examples and
//! a train's from each dataset row (`dataset::DatasetRow::sides`), each
//! through `SideRow::new`, so a side's row is the same whoever builds it.
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
//! times its row's unit its expected net P&L, picked on by `payoff::pick`.
//! `decide` takes the side picked if the meta model valid then predicts at
//! least its threshold (`holdout::Threshold`) on the features with that side
//! and the decision's `C'` and `D'` (`features::meta_row`), sized from its
//! probability if the meta sizes (`MetaConfig::size`). The meta's model is
//! read only once a side is picked, and no meta model valid yet is no trade.

use jiff::Timestamp;
use nautilus_model::types::{Quantity, fixed::FIXED_PRECISION};

use super::{
    calibration::Expected,
    config::MetaConfig,
    features::{meta_row, side_row},
    holdout::Threshold,
    payoff,
    schedule::Models,
    store::{LoadedModel, ModelSet, Pair, Swap},
    targets::{PerSide, Side},
};

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

/// The meta level: its config and its models, swapped in decision by decision.
#[derive(Debug)]
struct MetaSwap {
    config: MetaConfig,
    swap: Swap<LoadedModel>,
}

/// What a decision reads of a side: the row its model reads, the decision's
/// features then the side's payoff features (`features::side_row`), and the
/// worth of the unit its payoff is weighed by.
#[derive(Debug, Clone, PartialEq)]
pub struct SideRow {
    pub features: Vec<f64>,
    pub unit: f64,
}

impl SideRow {
    /// The row of a side of a decision of `features`, its target's `payoff`
    /// features and its unit worth `unit`.
    #[must_use]
    pub fn new(features: &[f64], payoff: &[f64], unit: f64) -> Self {
        Self {
            features: side_row(features, payoff),
            unit,
        }
    }
}

/// The side a decision picks, before any meta.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Picked {
    pub side: Side,
    /// The net P&L the pair expects of its trade, in money: its `E'` times
    /// its row's unit.
    pub expected_pnl: f64,
    /// The decision's calibrated level and side call.
    pub expected: Expected,
}

/// A decision to trade: the side picked, taken by the meta if there is one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Taken {
    pub side: Side,
    /// The target's size, or, with a meta model sizing, its multiple of it.
    pub quantity: Quantity,
    /// The net P&L the pair expects of the trade, in money.
    pub expected_pnl: f64,
    /// The decision's calibrated level and side call.
    pub expected: Expected,
    /// The meta model's calibrated probability, if one took the trade.
    pub meta_probability: Option<f64>,
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
                config: level.config().clone(),
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
    /// row `sides`, of `size`: the side picked (`pick`), of that size, if there
    /// is no meta; with one, taken if the meta model valid then, swapped in,
    /// predicts at least its threshold on the features with that side and the
    /// decision's level and side call, at the meta model's probability and
    /// sized from it if the meta sizes. None if no side is picked, no meta
    /// model is valid yet or the threshold is not met.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict, or the size is no
    /// quantity.
    pub fn decide(
        &mut self,
        now: Timestamp,
        features: &[f64],
        sides: &PerSide<SideRow>,
        size: Quantity,
    ) -> anyhow::Result<Option<Taken>> {
        let Some(picked) = self.pick(now, sides)? else {
            return Ok(None);
        };
        let taken = |quantity, meta_probability| Taken {
            side: picked.side,
            quantity,
            expected_pnl: picked.expected_pnl,
            expected: picked.expected,
            meta_probability,
        };
        let (Some(meta), Some(valid_froms)) = (&mut self.meta, &self.listed.meta) else {
            return Ok(Some(taken(size, None)));
        };
        let Some((_, model)) = meta.swap.at(now, valid_froms) else {
            return Ok(None);
        };
        let probability = model.predict(&meta_row(
            features,
            picked.side.order_side(),
            picked.expected,
        ))?;
        if !Threshold(meta.config.threshold).takes(probability) {
            return Ok(None);
        }
        Ok(Some(taken(
            sized(&meta.config, size, probability)?,
            Some(probability),
        )))
    }

    /// The side picked by `pair` on each side's row `sides`, each side
    /// traded held to its margin of `margins`: of the sides whose expected
    /// net P&L, their `E'` of the pair's calibrated expectation
    /// (`Pair::expect`) times their row's unit, clears their margin, the one
    /// expected the most; none if none clears it, or two tie
    /// (`payoff::pick`).
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn pick_among(
        pair: &Pair,
        margins: impl IntoIterator<Item = (Side, f64)>,
        sides: &PerSide<SideRow>,
    ) -> anyhow::Result<Option<Picked>> {
        let expected = pair.expect(sides)?;
        Ok(payoff::pick(margins.into_iter().map(|(side, margin)| {
            let expected_pnl = expected.of(side) * sides.of(side).unit;
            let picked = Picked {
                side,
                expected_pnl,
                expected,
            };
            (picked, expected_pnl, margin)
        })))
    }
}

/// `size` as `meta` sizes a trade taken at `probability`: untouched if it does
/// not size, which keeps the size's precision.
fn sized(meta: &MetaConfig, size: Quantity, probability: f64) -> anyhow::Result<Quantity> {
    if meta.size.is_none() {
        return Ok(size);
    }
    let sized = size.as_decimal() * meta.size(probability);
    Quantity::from_decimal_dp(sized, FIXED_PRECISION)
        .map_err(|e| anyhow::anyhow!("sizing {sized}: {e}"))
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
