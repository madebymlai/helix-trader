//! The dataset store (`model::dataset`) on a fixture catalog: a probe
//! deciding every hour on the last mid of IC's gold opens the targets of
//! each decision through `Backtest::run_dataset`, and records the target
//! rows published for it; a dataset build replays the days it lacks
//! through it and stores the rows in the catalog. A second build of the
//! same span replays no day, and the rows read back are those the replay
//! settled over the days built. One node per thread: the second build
//! runs none.

use crate::common::{
    dataset::{STRATEGY, features, replay, rule, targets, walk},
    ic,
};
use helix_trader::model::dataset::{Dataset, DatasetRow};
use jiff::{SignedDuration, Timestamp, civil::Date};
use nautilus_model::identifiers::StrategyId;

/// Two days, the 21st and 22nd of October 2026, quotes from 23:00 the day
/// before (the engine starts 1 ns before the run) to 03:00 of the 23rd:
/// the replay runs to 02:00, the end of the 22nd plus the bracket's two
/// hours, and every decision of the two days settles by then.
#[test]
fn a_second_build_of_the_same_span_replays_no_day_and_rows_read_back_as_written() {
    let (first, last) = (Date::constant(2026, 10, 21), Date::constant(2026, 10, 22));
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(51),
    );
    let dataset = Dataset::new(
        catalog.path(),
        StrategyId::from(STRATEGY),
        &features(),
        rule(),
        targets(),
    )
    .unwrap();
    let (mut replayed, mut settled) = (vec![], vec![]);

    let built = dataset
        .build(first, last, |from, to| {
            replayed.push((from, to));
            settled = replay(&catalog, from, to)?;
            Ok(settled.clone())
        })
        .unwrap();
    let rebuilt = dataset
        .build(first, last, |_, _| panic!("the second build replayed"))
        .unwrap();

    assert_eq!(built, [first, last]);
    assert_eq!(
        replayed,
        [(
            ic::at("00:00:00"),
            ic::at("00:00:00") + SignedDuration::from_hours(50)
        )]
    );
    assert!(rebuilt.is_empty());
    let end = ic::at("00:00:00") + SignedDuration::from_hours(48);
    let mut written: Vec<DatasetRow> = settled
        .iter()
        .filter(|row| Timestamp::from(row.decision.ts_event) < end)
        .map(|row| {
            DatasetRow::new(
                row.decision.features.clone(),
                row.values(),
                row.settled_at,
                row.decision.ts_event,
                row.decision.ts_init,
            )
        })
        .collect();
    written.sort_by_key(|row| row.ts_init);
    // A decision every hour of the two days, each settled.
    assert_eq!(written.len(), 48);
    assert_eq!(dataset.read(ic::at("00:00:00"), end).unwrap(), written);
}
