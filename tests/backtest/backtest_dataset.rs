//! The dataset store (`model::dataset`) on a fixture catalog: a probe
//! deciding every hour on the last mid of IC's gold opens the targets of
//! each decision through `Backtest::run_dataset`, and records the target
//! rows published for it; a dataset build replays the days it lacks
//! through it and stores the rows in the catalog. A second build of the
//! same span replays no day, and the rows read back are those the replay
//! settled over the days built. One node per thread: the second build
//! runs none.

use std::{collections::VecDeque, rc::Rc};

use crate::common::{
    dataset::{Lcg, STRATEGY, commission, features, replay, replay_on, rule, targets, walk},
    ic,
};
use helix_trader::model::{
    dataset::{Dataset, DatasetRow},
    features::{Feature, FeatureSet},
};
use jiff::{SignedDuration, Timestamp, civil::Date};
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::QuoteTick,
    identifiers::{InstrumentId, StrategyId},
};

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
        &commission(),
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

/// The mean of the last `period` mids, summed from the oldest each time:
/// a function of its window alone, to the bit. Nautilus' moving averages
/// keep a running sum, whose rounding depends on the quotes before the
/// window too.
struct WindowMean {
    period: usize,
    mids: VecDeque<f64>,
}

impl Indicator for WindowMean {
    fn name(&self) -> String {
        "WindowMean".to_string()
    }

    fn has_inputs(&self) -> bool {
        !self.mids.is_empty()
    }

    fn initialized(&self) -> bool {
        self.mids.len() == self.period
    }

    fn handle_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if self.mids.len() == self.period {
            self.mids.pop_front();
        }
        self.mids
            .push_back((quote.bid_price.as_f64() + quote.ask_price.as_f64()) / 2.0);
        Ok(())
    }

    fn reset(&mut self) {
        self.mids.clear();
    }
}

impl Feature for WindowMean {
    fn value(&self) -> f64 {
        self.mids.iter().sum::<f64>() / self.period as f64
    }
}

/// The mean of the last 1440 mids of IC's gold, two hours of its quotes,
/// with a warmup of 3 hours.
fn two_hour_mean() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(
            InstrumentId::from(ic::INSTRUMENT_ID),
            "mean_2h",
            WindowMean {
                period: 1440,
                mids: VecDeque::new(),
            },
        )
        .warmup(SignedDuration::from_hours(3))
        .build()
        .unwrap()
}

/// The rows of Thursday 22 October 2026 stored by a build of the two-hour
/// mean from `first` to the 22nd, on a catalog of its own from 20:00 on
/// the 20th: on a thread of its own, one node per thread.
fn thursday_stored_by_a_build_from(first: Date) -> Vec<DatasetRow> {
    std::thread::spawn(move || {
        let thursday = Date::constant(2026, 10, 22);
        let catalog = walk(
            on(Date::constant(2026, 10, 20), "20:00:00"),
            on(Date::constant(2026, 10, 23), "03:00:00"),
        );
        let dataset = Dataset::new(
            catalog.path(),
            StrategyId::from(STRATEGY),
            &two_hour_mean(),
            rule(),
            targets(),
            &commission(),
        )
        .unwrap();
        dataset
            .build(first, thursday, |from, to| {
                replay_on(&catalog, two_hour_mean(), from, to)
            })
            .unwrap();
        dataset
            .read(on(thursday, "00:00:00"), on(thursday, "23:59:59"))
            .unwrap()
    })
    .join()
    .unwrap()
}

/// The mean at Thursday's first decisions reads Wednesday's quotes: a
/// build of Thursday alone replays from 21:00 on Wednesday, the warmup
/// before it, as a build from Wednesday replays it, and stores none of
/// Wednesday's rows.
#[test]
fn a_day_built_alone_is_stored_as_inside_a_longer_build() {
    let alone = thursday_stored_by_a_build_from(Date::constant(2026, 10, 22));
    let inside = thursday_stored_by_a_build_from(Date::constant(2026, 10, 21));

    assert_eq!(alone.len(), 24, "{alone:#?}");
    assert_eq!(alone, inside);
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

/// The rows from `from` to `to` stored by a build from Thursday 22 October
/// 2026 to `last` on `weekend_walk`, on a catalog of its own: on a thread
/// of its own, one node per thread.
fn stored_by_a_build_to(last: Date, from: Timestamp, to: Timestamp) -> Vec<DatasetRow> {
    std::thread::spawn(move || {
        let catalog = weekend_walk();
        let dataset = Dataset::new(
            catalog.path(),
            StrategyId::from(STRATEGY),
            &features(),
            rule(),
            targets(),
            &commission(),
        )
        .unwrap();
        dataset
            .build(Date::constant(2026, 10, 22), last, |from, to| {
                replay(&catalog, from, to)
            })
            .unwrap();
        dataset.read(from, to).unwrap()
    })
    .join()
    .unwrap()
}

/// The rows of Friday 23 October 2026 stored by a build to `last`.
fn friday_stored_by_a_build_to(last: Date) -> Vec<DatasetRow> {
    let friday = Date::constant(2026, 10, 23);
    stored_by_a_build_to(last, on(friday, "00:00:00"), on(friday, "23:59:59"))
}

/// A build cut on Friday replays to 02:00 on Saturday, one cut on Monday
/// through the weekend to 02:00 on Tuesday. The decision at 21:00, on
/// Friday's last quote, still settles at its time limits: the venue fills
/// the time close, a market order, on the last quote it has, as it fills
/// any market order between quotes, so neither build waits for Sunday's
/// open, and both store the same Friday. The decisions at 22:00 and 23:00,
/// with no quote since the one before, store no row.
#[test]
fn a_day_is_stored_the_same_by_a_build_cut_before_a_weekend_and_one_cut_after() {
    let cut_before = friday_stored_by_a_build_to(Date::constant(2026, 10, 23));
    let cut_after = friday_stored_by_a_build_to(Date::constant(2026, 10, 26));

    assert_eq!(cut_before.len(), 22, "{cut_before:#?}");
    assert_eq!(cut_before, cut_after);
}

/// From Friday's close at 21:00 to Sunday's open at 22:00 no quote comes:
/// the decision at 21:00 reads the quotes of the hour before it, the next
/// stale one is skipped, and so is every decision to Sunday's open, the
/// first since a quote.
#[test]
fn a_closed_market_stores_no_row() {
    let (friday, sunday) = (Date::constant(2026, 10, 23), Date::constant(2026, 10, 25));

    let rows = stored_by_a_build_to(
        Date::constant(2026, 10, 26),
        on(friday, "20:30:00"),
        on(sunday, "23:30:00"),
    );

    let decided: Vec<Timestamp> = rows
        .iter()
        .map(|row| Timestamp::from(row.ts_event))
        .collect();
    let margin = SignedDuration::from_millis(10);
    assert_eq!(
        decided,
        [
            on(friday, "21:00:00") + margin,
            on(sunday, "22:00:00") + margin,
            on(sunday, "23:00:00") + margin,
        ]
    );
}
