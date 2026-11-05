//! Meta-labeling: the side models pick the side of a trade, and a meta
//! model decides whether to take it, and optionally its size.
//!
//! The side picked is the one the decision picks (`deciding`) among the
//! side models of a `ModelConfig` with a meta section, on the expected net
//! P&L of each side's calibrated probability
//! (`store::SideModel::expected_pnl`, `payoff::pick`). The meta model
//! decides only when a side is picked, on the sides' features with the
//! side picked and its probability after them (`PICKED_FEATURES`), and
//! learns whether the trade of the side picked, the target on that side,
//! with exits or time only, wins after its costs, read as the side models
//! read their trades (`payoff::Realised`): a dataset row opens a long and
//! a short at every decision, so the row has the trade of either side, and
//! the meta's rows need no replay of their own.
//!
//! The sides' rows are their dataset's, each decided by the models of
//! each side in use at it through the code a strategy decides through
//! (`deciding::DecidingModels`), of the strategy's model set
//! (`store::ModelSet`) as its stores are when the rows are made, listed
//! once, a read error failing them: the latest valid then, trained on
//! rows settled by its valid-from, so every decision is out of sample; the
//! worth of each side's stop is the row's own (`payoff::Realised::unit`).
//! A row before the sides' first model has no decision: the meta's rows
//! start there at the earliest, and with no side's model before a train
//! the meta waits (`schedule::Schedule::plan`). Never retrained, with
//! neither in use, one train fits the sides as of the meta's window before
//! it and the meta at it, on those sides' decisions over the window: the
//! meta filters the very side models it learned from. A row no side is
//! picked on is no row of the meta; a long-only strategy gives it buys
//! alone.
//!
//! Two levels at most: a meta section has no meta of its own (`config`).

use jiff::Timestamp;

use super::{
    dataset::DatasetRow, deciding::DecidingModels, features::meta_row, holdout::Example,
    payoff::Realised, store::ModelSet,
};

/// The meta's examples of the dataset `rows` of the sides, in time order:
/// each row a side is picked on, its features with that side and its
/// probability, its label whether the trade on that side, as `realised` reads
/// it, won, with the trade and its unit. The side picked on a row is the one
/// `DecidingModels` picks, on the row's units, among the models of `models` in
/// use then, as its stores list them now, none before the first.
///
/// # Errors
///
/// Returns an error if a store cannot be read, a model fails to predict, or a
/// row's stop has no worth.
pub(super) fn examples(
    models: &ModelSet,
    rows: &[&DatasetRow],
    realised: &Realised,
) -> anyhow::Result<Vec<Example>> {
    let mut models = DecidingModels::new(models)?;
    let mut examples = vec![];
    for row in rows {
        let Some(picked) = models.pick(
            Timestamp::from(row.ts_event),
            &row.features,
            realised.units(row)?,
        )?
        else {
            continue;
        };
        examples.push(Example {
            features: meta_row(&row.features, picked.side.order_side(), picked.probability),
            ..Example::of_side(row, picked.side, realised)?
        });
    }
    Ok(examples)
}
