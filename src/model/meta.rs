//! Meta-labeling: the side models pick the side of a trade, and a meta model decides whether to
//! take it, the trade as the target sets it, staked by the strategy.
//!
//! The side picked is the one the decision picks (`deciding`) among the side models of a
//! `ModelConfig` with a meta section, on the expected R of each side, its calibrated expected R of
//! the pair (`store::Pair::expect`, `payoff::pick`). The meta model decides only when a side is
//! picked, on the decision's features, the strategy's without either side's payoff features, with
//! the side picked and the decision's calibrated level and side call after them
//! (`PICKED_FEATURES`).
//!
//! It learns, as a regression, the realized net R of the trade of the side picked: the target on
//! that side, with exits or time only, its net P&L over its unit, read from its row as the side
//! models read their trades (`dataset::DatasetRow`). A dataset row opens a long and a short at
//! every decision, so the row has the trade of either side, and the meta's rows need no replay of
//! their own. It takes the trade when the net R it expects of it, its calibrated expected R, clears
//! its margin, in R (`earnings::MetaMargin`).
//!
//! The meta's rows are those of its window back from its train, each decided through the code a
//! strategy decides through (`deciding::DecidingModels::pick_among`) by the out-of-fold pair the
//! train fits as of the window's start (`train`), out of sample for it, each row after its time.
//! Each side's row, its payoff features, is the row's own (`dataset::DatasetRow::sides`). The
//! out-of-fold pair is in no store: the meta learns from nothing a store holds, and decides beside
//! the sides its train fits as of its own time, in the one model set the train publishes. A row no
//! side is picked on is no row of the meta; a long-only strategy gives it buys alone.
//!
//! Two levels at most: a meta section has no meta of its own (`config`).

use super::{
    dataset::DatasetRow,
    deciding::{Picked, SideRow},
    earnings::Example,
    features::meta_row,
    targets::PerSide,
};

/// The meta's examples of the dataset `rows` of the sides, in time order: each row a side is picked
/// on, its features with that side and the decision's level and side call, with the trade on that
/// side and its unit, whose realized R the meta learns. The side picked on a row is the one `pick`
/// picks on each side's row (`DatasetRow::sides`), of a pair in hand
/// (`DecidingModels::pick_among`).
///
/// # Errors
///
/// Returns an error if `pick` fails.
pub(super) fn examples(
    rows: &[&DatasetRow],
    mut pick: impl FnMut(&PerSide<SideRow>) -> anyhow::Result<Option<Picked>>,
) -> anyhow::Result<Vec<Example>> {
    let mut examples = vec![];

    for row in rows {
        let Some(picked) = pick(&row.sides())? else {
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
    use jiff::SignedDuration;
    use nautilus_core::UnixNanos;
    use rstest::rstest;

    use super::*;
    use crate::{
        model::{
            calibration::Expected,
            deciding::testing::at,
            targets::{Exit, Side},
        },
        strategy::risk::R,
    };

    /// The row of the decision at `time` on a mid of 0.7, settled an hour later, each side stopped:
    /// the long's stop worth 2 USD, the short's 4.
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

    /// The side `side` picked, expecting 0.4R of a level of 0.4 and no side call.
    fn picked(side: Side) -> Picked {
        Picked {
            side,
            expected_r: R::new(0.4),
            expected: Expected {
                level: 0.4,
                side_call: 0.0,
            },
        }
    }

    /// A meta example carries the unit of its trade, the worth of the stop of the side picked: the
    /// row at 05:00, no side picked, is no example; at 08:00 the long is picked, a buy carrying the
    /// long's 2 USD, and at 19:00 the short, a sell carrying the short's 4 USD.
    #[rstest]
    fn the_metas_examples_carry_the_unit_of_their_sides_trade() {
        let rows = [
            row("2026-01-06T05:00:00Z"),
            row("2026-01-06T08:00:00Z"),
            row("2026-01-06T19:00:00Z"),
        ];
        let mut picks = [None, Some(picked(Side::Long)), Some(picked(Side::Short))].into_iter();

        let examples = examples(&rows.iter().collect::<Vec<_>>(), |_| {
            Ok(picks.next().unwrap())
        })
        .unwrap();

        let sides_and_units: Vec<(f64, f64)> = examples
            .iter()
            .map(|example| (example.features[1], example.trade.unit))
            .collect();
        assert_eq!(sides_and_units, [(1.0, 2.0), (-1.0, 4.0)]);
    }

    /// The examples fail on a pick failing, a model failing to predict say, so a train never learns
    /// silently from rows skipped.
    #[rstest]
    fn a_pick_failing_fails_the_metas_examples() {
        let rows = [row("2026-01-06T08:00:00Z")];

        let err = examples(&rows.iter().collect::<Vec<_>>(), |_| {
            anyhow::bail!("the model fails to predict")
        })
        .unwrap_err();

        assert!(format!("{err:#}").contains("fails to predict"), "{err:#}");
    }
}
