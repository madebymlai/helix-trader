//! Meta-labeling. The side models pick the side of a trade, and a meta model decides whether to
//! take it.
//!
//! `deciding` picks the side among the side models by each side's calibrated expected R
//! (`store::Pair::expect`, `payoff::pick`). The meta decides only when a side is picked. Its
//! input is the strategy's features without the payoff features of either side, then the side
//! picked and the decision's calibrated level and side call (`PICKED_FEATURES`).
//!
//! The meta learns the realized net R of the trade on the side picked, as a regression. That R is
//! the trade's net P&L over its unit. The meta reads both from its row, as the side models do
//! (`dataset::DatasetRow`). A row opens a long and a short at every decision, so the meta needs no
//! replay of its own. It takes the trade when its calibrated expected R clears its margin
//! (`earnings::MetaMargin`).
//!
//! The meta's rows come from its window, which reaches back from its train. Each row is decided
//! through the code a strategy decides through (`deciding::DecidingModels::pick_among`). The
//! decider is the out-of-fold pair that the train fits as of the window's start (`train`), so the
//! rows are out of sample. No store holds the out-of-fold pair. The meta decides beside the sides
//! that its train fits as of its own time, in the one model set the train publishes. A row with no
//! side picked is not a row of the meta.
//!
//! There are two levels at most: a meta section has no meta of its own (`config`).

use super::{
    dataset::DatasetRow,
    deciding::{Picked, SideRow},
    earnings::Example,
    features::meta_row,
    targets::PerSide,
};

/// Returns the meta's examples from the dataset `rows`, in time order.
///
/// Each row on which `pick` picks a side gives one example. It holds the row's features with that
/// side and the decision's level and side call, and the trade on that side with its unit.
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

    /// The decision at `time` on a mid of 0.7, settled an hour later. The long's stop is worth 2
    /// USD and the short's 4.
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

    /// The side picked, expecting 0.4R from a level of 0.4 and no side call.
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

    /// No side is picked at 05:00. The long at 08:00 carries its 2 USD, the short at 19:00 its 4.
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

    /// A failing pick fails the examples, so a train never learns from silently skipped rows.
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
