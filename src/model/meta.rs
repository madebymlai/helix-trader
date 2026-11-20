//! Meta-labeling: the side models pick the side of a trade, and a meta model decides whether to
//! take it.
//!
//! The side is the one `deciding` picks among the side models on each side's calibrated expected R
//! (`store::Pair::expect`, `payoff::pick`). The meta decides only when a side is picked, on the
//! strategy's features without either side's payoff features, then the side picked and the
//! decision's calibrated level and side call (`PICKED_FEATURES`).
//!
//! It learns, as a regression, the realized net R of the trade on the side picked: its net P&L
//! over its unit, read from its row as the side models read theirs (`dataset::DatasetRow`). A row
//! opens a long and a short at every decision, so the meta needs no replay of its own. It takes the
//! trade when its calibrated expected R clears its margin (`earnings::MetaMargin`).
//!
//! Its rows are those of its window back from its train, each decided through the code a strategy
//! decides through (`deciding::DecidingModels::pick_among`) by the out-of-fold pair the train fits
//! as of the window's start (`train`), so out of sample. The out-of-fold pair is in no store: the
//! meta decides beside the sides its train fits as of its own time, in the one model set the train
//! publishes. A row no side is picked on is no row of the meta.
//!
//! Two levels at most: a meta section has no meta of its own (`config`).

use super::{
    dataset::DatasetRow,
    deciding::{Picked, SideRow},
    earnings::Example,
    features::meta_row,
    targets::PerSide,
};

/// The meta's examples of the dataset `rows`, in time order: for each row `pick` picks a side on,
/// its features with that side and the decision's level and side call, and the trade on that side
/// with its unit.
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

    /// The decision at `time` on a mid of 0.7, settled an hour later: the long's stop worth 2 USD,
    /// the short's 4.
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

    /// The side picked, expecting 0.4R of a level of 0.4 and no side call.
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

    /// No side picked at 05:00; the long at 08:00 carries its 2 USD, the short at 19:00 its 4.
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
