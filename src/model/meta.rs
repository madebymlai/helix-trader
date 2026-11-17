//! Meta-labeling: the side models pick the side of a trade, and a meta
//! model decides whether to take it, the trade as the target sets it,
//! staked by the strategy.
//!
//! The side picked is the one the decision picks (`deciding`) among the
//! side models of a `ModelConfig` with a meta section, on the expected R of
//! each side, its calibrated expected R of the pair (`store::Pair::expect`,
//! `payoff::pick`). The meta model decides only
//! when a side is picked, on the decision's features, the strategy's
//! without either side's payoff features, with the side picked and the
//! decision's calibrated level and side call after them (`PICKED_FEATURES`),
//! and learns, as a regression, the realized net R of the trade of the side
//! picked, the target on that side, with exits or time only, its net P&L
//! over its unit, read from its row as the side models read their trades
//! (`dataset::DatasetRow`): a dataset row opens a long and a short at every
//! decision, so the row has the trade of either side, and the meta's rows
//! need no replay of their own. It takes the trade when the net R it
//! expects of it, its calibrated expected R, clears its margin, in R
//! (`holdout::MetaMargin`).
//!
//! The sides' rows are their dataset's, each decided by the pair of the
//! sides in use at it through the code a strategy decides through
//! (`deciding::DecidingModels`): a retrained strategy's those of the
//! generations of its model store (`store::ModelStore`) as the store is when
//! the rows are made, listed once, a read error or a generation failing its
//! checks failing them, the latest valid then, trained on rows settled by
//! its valid-from, so every decision is out of sample; each side's row, its
//! payoff features, is the row's own (`dataset::DatasetRow::sides`). A row
//! before the first generation has no decision: the meta's rows start there
//! at the earliest, and with no generation before a train the meta waits
//! (`schedule::Schedule::plan`). Never retrained, one train fits the sides
//! as of the meta's window before it and the meta at it, on those sides'
//! decisions over the window, picked among the pair in hand
//! (`DecidingModels::pick_among`): the meta filters the very side models it
//! learned from, in the one generation the train publishes. A row no side
//! is picked on is no row of the meta; a long-only strategy gives it buys
//! alone.
//!
//! Two levels at most: a meta section has no meta of its own (`config`).

use jiff::Timestamp;

use super::{
    dataset::DatasetRow,
    deciding::{Picked, SideRow},
    features::meta_row,
    holdout::Example,
    targets::PerSide,
};

/// The meta's examples of the dataset `rows` of the sides, in time order:
/// each row a side is picked on, its features with that side and the
/// decision's level and side call, with the trade on that side and its unit,
/// whose realized R the meta learns. The side picked on a row is the one
/// `pick` picks at its time on each side's row (`DatasetRow::sides`): the
/// sides of the generation in use then (`DecidingModels::pick`), or of a pair
/// in hand (`DecidingModels::pick_among`).
///
/// # Errors
///
/// Returns an error if `pick` fails.
pub(super) fn examples(
    rows: &[&DatasetRow],
    mut pick: impl FnMut(Timestamp, &PerSide<SideRow>) -> anyhow::Result<Option<Picked>>,
) -> anyhow::Result<Vec<Example>> {
    let mut examples = vec![];

    for row in rows {
        let Some(picked) = pick(Timestamp::from(row.ts_event), &row.sides())? else {
            continue;
        };
        examples.push(Example {
            features: meta_row(&row.features, picked.side.order_side(), picked.expected),
            ..Example::of_side(row, picked.side)
        });
    }
    Ok(examples)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use jiff::SignedDuration;
    use nautilus_core::UnixNanos;
    use rstest::rstest;

    use super::*;
    use crate::model::{
        config::ModelConfig,
        deciding::{
            DecidingModels,
            testing::{at, both_sides, features, generation_of, meta, model_store},
        },
        store::ModelStore,
        targets::{Exit, Side},
    };

    /// Both sides traded from an expected R of nought, their trades taken
    /// by `meta`.
    fn config() -> ModelConfig {
        ModelConfig {
            meta: Some(meta()),
            ..both_sides()
        }
    }

    /// The models of `config` under `models`.
    fn models(models: &Path) -> ModelStore {
        model_store(models, &features(), &config())
    }

    /// Publishes under `models` the generations of `config`: from 06:00 a
    /// long of 0.4R and a short of -0.4R, from 17:00 the reverse.
    fn publish_generations(models: &Path) {
        for (valid_from, long, short) in [
            ("2026-01-06T06:00:00Z", 9, 1),
            ("2026-01-06T17:00:00Z", 1, 9),
        ] {
            self::models(models)
                .publish(&generation_of(
                    &features(),
                    &config(),
                    valid_from,
                    [(Side::Long, long), (Side::Short, short)],
                    None,
                ))
                .unwrap();
        }
    }

    /// The row of the decision at `time` on a mid of 0.7, settled an hour
    /// later, each side stopped: the long's stop worth 2 USD, the short's 4.
    fn row(time: &str) -> DatasetRow {
        let ts = UnixNanos::from(at(time));
        DatasetRow {
            features: vec![0.7],
            long_payoff: vec![0.07],
            long_pnl: -2.2,
            long_commission: 0.2,
            long_exit: Exit::Stop.value(),
            long_unit: 2.0,
            short_payoff: vec![0.08],
            short_pnl: -4.2,
            short_commission: 0.2,
            short_exit: Exit::Stop.value(),
            short_unit: 4.0,
            settled_at: UnixNanos::from(at(time) + SignedDuration::from_hours(1)),
            ts_event: ts,
            ts_init: ts,
        }
    }

    /// A meta example carries the unit of its trade, the worth of the stop
    /// of the side picked: the row at 05:00, before the first generation,
    /// is no example; at 08:00 the long of the generation of 06:00 is
    /// picked, a buy carrying the long's 2 USD, and at 19:00 the short of
    /// the generation of 17:00, a sell carrying the short's 4 USD.
    #[rstest]
    fn the_metas_examples_carry_the_unit_of_their_sides_trade() {
        let dir = tempfile::tempdir().unwrap();
        publish_generations(dir.path());
        let rows = [
            row("2026-01-06T05:00:00Z"),
            row("2026-01-06T08:00:00Z"),
            row("2026-01-06T19:00:00Z"),
        ];
        let mut generations = DecidingModels::new(&models(dir.path())).unwrap();

        let examples = examples(&rows.iter().collect::<Vec<_>>(), |now, sides| {
            generations.pick(now, sides)
        })
        .unwrap();

        let sides_and_units: Vec<(f64, f64)> = examples
            .iter()
            .map(|example| (example.features[1], example.trade.unit))
            .collect();
        assert_eq!(sides_and_units, [(1.0, 2.0), (-1.0, 4.0)]);
    }

    /// The examples fail on a pick failing, a generation failing its checks
    /// say, so a train never learns silently from rows skipped.
    #[rstest]
    fn a_pick_failing_fails_the_metas_examples() {
        let rows = [row("2026-01-06T08:00:00Z")];

        let err = examples(&rows.iter().collect::<Vec<_>>(), |_, _| {
            anyhow::bail!("the generation fails its checks")
        })
        .unwrap_err();

        assert!(format!("{err:#}").contains("fails its checks"), "{err:#}");
    }
}
