//! Meta-labeling: a primary picks the side of a trade, and a meta model
//! decides whether to take it, and optionally its size.
//!
//! The primary is a model, the side models of a `ModelConfig` with a meta
//! section, whose side is the one its decision rule picks, on the expected
//! net P&L of each side's calibrated probability
//! (`store::SideModel::expected_pnl`, `payoff::pick`). The meta model
//! decides only when the primary signals, on the primary's features with
//! its side and the chosen side's probability after them
//! (`PRIMARY_FEATURES`), and learns whether the primary's trade, the target
//! of its label on its side, a barrier's or a horizon's, wins after its
//! costs, read as the side models read their trades (`payoff::Realised`,
//! on the side models' label): a dataset row
//! opens a long and a short at every decision, so the row has the trade of
//! either side, and the meta's rows need no replay of their own.
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

use jiff::Timestamp;

use super::{
    dataset::DatasetRow,
    features::meta_row,
    holdout::Example,
    payoff::{self, Realised},
    store::ModelSet,
};

/// The meta's examples of the primary's dataset `rows`, in time order:
/// each row the primary makes a trade of, its features with the primary's
/// side and probability, its label whether the trade on that side, of the
/// label `realised` reads, won, with the trade and its unit. The primary's
/// decision on a row is that of the models of each side of `models` in use
/// then, as its stores list them now, none before the first, the rows'
/// trades read by `realised`.
///
/// # Errors
///
/// Returns an error if a primary's store cannot be read, a model fails to
/// predict, or a row's stop has no worth.
pub(super) fn examples(
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
            let expected = model.expected_pnl(probability, realised.unit(row, *side)?);
            decided.push(((*side, probability), expected, *margin));
        }
        let Some((side, probability)) = payoff::pick(decided) else {
            continue;
        };
        examples.push(Example {
            features: meta_row(&row.features, side.order_side(), probability),
            ..Example::of_side(row, side, realised)?
        });
    }
    Ok(examples)
}
