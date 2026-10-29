//! The dataset store (`model::dataset`) on a fixture catalog: a probe
//! deciding every hour on the last mid of IC's gold opens the targets of
//! each decision through `Backtest::run_dataset`, and records the target
//! rows published for it; a dataset build replays the days it lacks
//! through it and stores the rows in the catalog. A second build of the
//! same span replays no day, and the rows read back are those the replay
//! settled over the days built. One node per thread: the second build
//! runs none.

use crate::common::{
    dataset::{Lcg, STRATEGY, features, replay, rule, targets, walk},
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

/// `hh:mm:ss` UTC on `date`.
fn on(date: Date, time: &str) -> Timestamp {
    format!("{date}T{time}Z").parse().unwrap()
}

/// IC's gold as `walk` from 23:00 on Wednesday 21 October 2026 to 06:00
/// on Tuesday 27th, without quotes over the weekend, from 21:00 on Friday
/// 23rd to 22:00 on Sunday 25th.
fn weekend_walk() -> tempfile::TempDir {
    let (from, to) = (
        on(Date::constant(2026, 10, 21), "23:00:00"),
        on(Date::constant(2026, 10, 27), "06:00:00"),
    );
    let (close, open) = (
        on(Date::constant(2026, 10, 23), "21:00:00"),
        on(Date::constant(2026, 10, 25), "22:00:00"),
    );
    let mut walk = Lcg(7);
    let mut bid = 4000.0_f64;
    let mut quotes = vec![];
    let mut time = from;
    while time < to {
        if time < close || time >= open {
            bid = ((bid + (walk.next() - 0.5) * 0.6) * 100.0).round() / 100.0;
            let (b, a) = (format!("{bid:.2}"), format!("{:.2}", bid + 0.2));
            quotes.push(ic::quote(time, &b, &a));
        }
        time += SignedDuration::from_secs(5);
    }
    ic::catalog_of(quotes, from, to)
}

/// The rows of Friday 23 October 2026 stored by a build from Thursday
/// 22nd to `last`, on a catalog of its own: on a thread of its own, one
/// node per thread.
fn friday_stored_by_a_build_to(last: Date) -> Vec<DatasetRow> {
    std::thread::spawn(move || {
        let catalog = weekend_walk();
        let dataset = Dataset::new(
            catalog.path(),
            StrategyId::from(STRATEGY),
            &features(),
            rule(),
            targets(),
        )
        .unwrap();
        dataset
            .build(Date::constant(2026, 10, 22), last, |from, to| {
                replay(&catalog, from, to)
            })
            .unwrap();
        let friday = Date::constant(2026, 10, 23);
        dataset
            .read(on(friday, "00:00:00"), on(friday, "23:59:59"))
            .unwrap()
    })
    .join()
    .unwrap()
}

/// A build cut on Friday replays to 02:00 on Saturday, one cut on Monday
/// through the weekend to 02:00 on Tuesday. The decisions after Friday's
/// last quote, 21:00, still settle at their time limits: the venue fills
/// the time close, a market order, on the last quote it has, as it fills
/// any market order between quotes, so neither build waits for Sunday's
/// open, and both store the same Friday.
#[test]
fn a_day_is_stored_the_same_by_a_build_cut_before_a_weekend_and_one_cut_after() {
    let cut_before = friday_stored_by_a_build_to(Date::constant(2026, 10, 23));
    let cut_after = friday_stored_by_a_build_to(Date::constant(2026, 10, 26));

    assert_eq!(cut_before.len(), 24, "{cut_before:#?}");
    assert_eq!(cut_before, cut_after);
}
