//! Meta-labeling: a primary picks the side of a trade, and a meta model
//! decides whether to take it, and optionally its size.
//!
//! The primary is a model, the side models of a `ModelConfig` with a meta
//! section, whose side is the one its decision rule picks, on the expected
//! net P&L of each side's calibrated probability (`payoff`). The meta model
//! decides only when the primary signals, on the primary's features with
//! its side and the chosen side's probability after them
//! (`PRIMARY_FEATURES`), and learns whether the triple-barrier trade on
//! the primary's side wins after its costs (`TargetsConfig::pnl_index`):
//! a dataset row opens a long and a short at every decision, so the row
//! has the trade of either side, and the meta's rows need no replay of
//! their own.
//!
//! The primary's rows are its dataset's, each decided by the models of
//! each side in use at it as a strategy would have used them
//! (`store::Swap`), of the strategy's model set (`store::ModelSet`) as its
//! stores are when the rows are made: the latest valid then, trained on
//! rows settled by its valid-from, so every decision is out of sample; the
//! worth of each side's stop is the row's own (`payoff::Realised::unit`).
//! A row before the primary's first model has no decision: the meta's
//! rows start there at the earliest, and with no primary model before a
//! train the meta waits (`schedule::Schedule::plan`). Never retrained,
//! with neither in use, one train fits the primary as of the meta's window
//! before it and the meta at it, on that primary's decisions over the
//! window: the meta filters the very primary it learned. A row the primary
//! makes no trade of is no row of the meta; a long-only primary gives it
//! buys alone.
//!
//! Two levels at most: a meta section has no meta of its own (`config`).

use anyhow::Context;
use jiff::Timestamp;
use nautilus_model::enums::OrderSide;

use super::{
    config::ModelConfig,
    dataset::DatasetRow,
    features::meta_row,
    holdout::Trade,
    payoff::{self, Realised},
    store::ModelSet,
    targets::{Label, Side, TargetsConfig},
    train::Example,
};

/// How a strategy's meta model makes its rows of the primary's: where the
/// trades it learns are among a row's targets. Its config, feature names
/// and store are its model set's meta level (`store::MetaLevel`).
#[derive(Debug)]
pub struct Meta {
    /// Where the barrier's P&L and commission of a long and of a short are
    /// among a row's target values.
    trades: [(usize, usize); 2],
}

impl Meta {
    /// How the meta model of `config` makes its rows; none if `config` has
    /// no meta section.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets have no barrier.
    pub fn of(config: &ModelConfig) -> anyhow::Result<Option<Self>> {
        config
            .meta
            .as_ref()
            .map(|_| {
                Ok(Self {
                    trades: trades(&config.targets)?,
                })
            })
            .transpose()
    }

    /// The meta's examples of the primary's dataset `rows`, in time order:
    /// each row the primary makes a trade of, its features with the
    /// primary's side and probability, its label whether the barrier's
    /// trade on that side won. The primary's decision on a row is that of
    /// the models of each side of `models` in use then, as its stores list
    /// them now, none before the first, the rows' trades read by
    /// `realised`.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary's store cannot be read, a model fails
    /// to predict, or a row's stop has no worth.
    pub(super) fn examples(
        &self,
        models: &ModelSet,
        rows: &[&DatasetRow],
        realised: &Realised,
    ) -> anyhow::Result<Vec<Example>> {
        let mut swaps = models
            .sides()
            .map(|level| {
                let valid_froms = level.store().valid_froms()?;
                Ok((level.side(), level.margin(), valid_froms, level.swap()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut examples = vec![];
        for row in rows {
            let now = Timestamp::from(row.ts_event);
            let mut decided = vec![];
            for (side, margin, valid_froms, swap) in &mut swaps {
                let Some((_, model)) = swap.at(now, valid_froms) else {
                    continue;
                };
                let probability = model.predict(&row.features)?;
                let payoff = model.payoff().context("a primary's model has its payoff")?;
                let expected = payoff.expected(probability) * realised.unit(row, *side)?;
                decided.push(((*side, probability), expected, *margin));
            }
            let Some((side, probability)) = payoff::pick(decided) else {
                continue;
            };
            let side = side.order_side();
            examples.push(Example {
                features: meta_row(&row.features, side, probability),
                ..self.example(row, side)
            });
        }
        Ok(examples)
    }

    /// The example of `row` on the barrier's trade on `side`: whether it
    /// won, and the trade.
    fn example(&self, row: &DatasetRow, side: OrderSide) -> Example {
        let [long, short] = self.trades;
        let (pnl, commission) = if side == OrderSide::Buy { long } else { short };
        let trade = Trade {
            pnl: row.targets[pnl],
            commission: row.targets[commission],
        };
        Example::of(row, trade.pnl > 0.0, trade)
    }
}

/// Where the barrier's P&L and commission of a long and of a short are
/// among the values of a row of `targets`.
fn trades(targets: &TargetsConfig) -> anyhow::Result<[(usize, usize); 2]> {
    Side::BOTH
        .map(|side| {
            let pnl = targets.pnl_index(Label::Barrier, side)?;
            Some((pnl, targets.commission_index(Label::Barrier, side)?))
        })
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .and_then(|trades| trades.try_into().ok())
        .context("the meta model learns whether the primary's barrier trade wins, and the targets have no barrier")
}
