//! A strategy's decision, made in one place: `DecidingModels` lists the stores
//! of its model set, swaps each model in, picks a side, and applies the meta's
//! threshold and size. It sees no strategy, cache or clock, so the model
//! handle and the meta's examples decide through the same code, and the meta
//! learns from the decisions the handle would make.
//!
//! A caller resolves first what a decision reads: the time, the feature row
//! and each side's unit, the worth of a stop with exits, one without
//! (`store::SideModel::expected_pnl`). The handle reads them from the target
//! it sets at the decision (`targets::TargetSetter`), the meta's examples from
//! each row (`dataset::DatasetRow::units`).
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
//! (`store::Swap`), so no decision mixes two models. `pick` picks among the
//! sides with a model valid at the time, by `pick_among`, the rule for models
//! already in hand, on `payoff::pick`, which a train's gate also judges its
//! candidates by, being in no store yet. `decide` takes the side picked if the
//! meta model valid then predicts at least its threshold (`holdout::Threshold`)
//! on the row with that side and its probability (`features::meta_row`), sized
//! from that probability if the meta sizes (`MetaConfig::size`). The meta's
//! model is read only once a side is picked, and no meta model valid yet is no
//! trade.

use jiff::Timestamp;
use nautilus_model::types::{Quantity, fixed::FIXED_PRECISION};

use super::{
    config::MetaConfig,
    features::meta_row,
    holdout::Threshold,
    payoff,
    schedule::Models,
    store::{LoadedModel, ModelSet, SideModel, Swap},
    targets::{PerSide, Side},
};

/// A strategy's models as they decide: its model set's stores as last listed,
/// and the model of each swapped in so far.
#[derive(Debug)]
pub struct DecidingModels {
    models: ModelSet,
    /// When the models of each store are valid from, as last listed.
    listed: Models<Vec<Timestamp>>,
    /// Each side traded, the long first.
    sides: Vec<SideSwap>,
    meta: Option<MetaSwap>,
}

/// A side traded: its margin and its models, swapped in decision by decision.
#[derive(Debug)]
struct SideSwap {
    side: Side,
    margin: f64,
    swap: Swap<SideModel>,
}

/// The meta level: its config and its models, swapped in decision by decision.
#[derive(Debug)]
struct MetaSwap {
    config: MetaConfig,
    swap: Swap<LoadedModel>,
}

/// A side's model in hand, for `DecidingModels::pick_among`, with what the pick
/// needs of it: its side and its margin.
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    pub side: Side,
    pub margin: f64,
    pub model: &'a SideModel,
}

/// The side a decision picks, before any meta.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Picked {
    pub side: Side,
    /// The calibrated probability its model gives its trade.
    pub probability: f64,
    /// The net P&L its model expects of its trade, in money.
    pub expected_pnl: f64,
}

/// A decision to trade: the side picked, taken by the meta if there is one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Taken {
    pub side: Side,
    /// The target's size, or, with a meta model sizing, its multiple of it.
    pub quantity: Quantity,
    /// The calibrated probability of the model deciding: the meta model's,
    /// if any, else the side's.
    pub probability: f64,
    /// The net P&L the side's model expects of the trade, in money.
    pub expected_pnl: f64,
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
            sides: models
                .sides()
                .map(|level| SideSwap {
                    side: level.side(),
                    margin: level.margin(),
                    swap: level.swap(),
                })
                .collect(),
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

    /// The side picked at `now` on `row`, the units being `units`, among the
    /// latest model of each side valid then, swapped in (`pick_among`); none
    /// without a model valid, no side clearing its margin, or two tied.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn pick(
        &mut self,
        now: Timestamp,
        row: &[f64],
        units: PerSide<f64>,
    ) -> anyhow::Result<Option<Picked>> {
        let listed = &self.listed;
        let in_use = self.sides.iter_mut().filter_map(|traded| {
            let (_, model) = traded.swap.at(now, listed.side(traded.side)?)?;
            Some(Candidate {
                side: traded.side,
                margin: traded.margin,
                model,
            })
        });
        Self::pick_among(in_use, row, units)
    }

    /// The trade decided at `now` on `row`, the units being `units`, of `size`:
    /// the side picked (`pick`), of that size, if there is no meta; with one,
    /// taken if the meta model valid then, swapped in, predicts at least its
    /// threshold on the row with that side and its probability, at the meta
    /// model's probability and sized from it if the meta sizes. None if no side
    /// is picked, no meta model is valid yet or the threshold is not met.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict, or the size is no
    /// quantity.
    pub fn decide(
        &mut self,
        now: Timestamp,
        row: &[f64],
        units: PerSide<f64>,
        size: Quantity,
    ) -> anyhow::Result<Option<Taken>> {
        let Some(picked) = self.pick(now, row, units)? else {
            return Ok(None);
        };
        let taken = |quantity, probability| Taken {
            side: picked.side,
            quantity,
            probability,
            expected_pnl: picked.expected_pnl,
        };
        let (Some(meta), Some(valid_froms)) = (&mut self.meta, &self.listed.meta) else {
            return Ok(Some(taken(size, picked.probability)));
        };
        let Some((_, model)) = meta.swap.at(now, valid_froms) else {
            return Ok(None);
        };
        let probability =
            model.predict(&meta_row(row, picked.side.order_side(), picked.probability))?;
        if !Threshold(meta.config.threshold).takes(probability) {
            return Ok(None);
        }
        Ok(Some(taken(
            sized(&meta.config, size, probability)?,
            probability,
        )))
    }

    /// The side picked among `candidates` on `row`, the units being `units`:
    /// of those whose expected net P&L at their calibrated probability and
    /// their side's unit clears their margin, the one expected the most; none
    /// if none clears it, or two tie (`payoff::pick`).
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn pick_among<'a>(
        candidates: impl IntoIterator<Item = Candidate<'a>>,
        row: &[f64],
        units: PerSide<f64>,
    ) -> anyhow::Result<Option<Picked>> {
        let sides = candidates
            .into_iter()
            .map(
                |Candidate {
                     side,
                     margin,
                     model,
                 }| {
                    let probability = model.predict(row)?;
                    let expected_pnl = model.expected_pnl(probability, *units.of(side));
                    let picked = Picked {
                        side,
                        probability,
                        expected_pnl,
                    };
                    Ok((picked, expected_pnl, margin))
                },
            )
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(payoff::pick(sides))
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
