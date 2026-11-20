//! Tests the dataset store (`model::dataset`) on a fixture catalog. A probe decides every hour on
//! the last mid of IC's gold. It opens targets through `Backtest::run_dataset` and records the
//! rows published. A dataset build replays the days it lacks and stores the rows. A second build of
//! the same span replays no day, and the rows read back are those the replay settled. When a
//! target's ATR warms over exactly the warmup, it opens the target of a day's first decision.

use std::{collections::VecDeque, path::PathBuf, rc::Rc};

use helix_trader::{
    model::{
        config::ModelConfig,
        dataset::DatasetRow,
        features::{Feature, FeatureSet},
        targets::{BracketSpec, Distance, Target, TargetConfig},
    },
    strategy::{
        Declared, IndicatorArgs, Series,
        requirements::{DataRequirements, TickSeries},
    },
};
use jiff::{SignedDuration, Timestamp, civil::Date};
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::{NautilusDataType, QuoteTick},
    identifiers::InstrumentId,
};
use rstest::rstest;
use rust_decimal::Decimal;
use tempfile::TempDir;

use crate::common::{
    dataset::{
        Lcg, MID, blueprint, config, features, mid_tables, replay, replay_of, replay_on, target,
        walk,
    },
    features::tables,
    ic,
};

/// Quotes from 23:00 on 20 October 2026 to 03:00 on the 23rd.
///
/// The replay runs to 02:00, which is the end of the 22nd plus the bracket's two hours, so every
/// decision settles.
#[rstest]
fn a_second_build_of_the_same_span_replays_no_day_and_rows_read_back_as_written() {
    let (first, last) = (Date::constant(2026, 10, 21), Date::constant(2026, 10, 22));
    let catalog = walk(
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(51),
    );
    let dataset = blueprint(features(), config())
        .dataset(catalog.path())
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
        .map(DatasetRow::of)
        .collect();
    written.sort_by_key(|row| row.ts_init);

    assert_eq!(written.len(), 48);
    assert_eq!(dataset.read(ic::at("00:00:00"), end).unwrap(), written);
}

/// The mean of the last `period` mids.
///
/// It sums from the oldest mid each time, so it depends only on its window, unlike Nautilus'
/// running-sum averages.
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowMeanArgs {
    period: usize,
}

impl IndicatorArgs for WindowMeanArgs {
    type Indicator = WindowMean;

    fn build(self) -> anyhow::Result<WindowMean> {
        Ok(WindowMean {
            period: self.period,
            mids: VecDeque::new(),
        })
    }
}

const MEAN_2H: Declared<WindowMeanArgs> = Declared::new("mean_2h");

/// Returns a feature over two hours of quotes, with a 3-hour warmup.
fn two_hour_mean() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .feature(
            &[Series::quotes(InstrumentId::from(ic::INSTRUMENT_ID))],
            MEAN_2H,
            &tables("mean_2h = { period = 1440 }"),
        )
        .warmup(SignedDuration::from_hours(3))
        .build()
        .unwrap()
}

fn thursday_stored_by_a_build_from(catalog: &TempDir, first: Date) -> Vec<DatasetRow> {
    on_thread(|| {
        let thursday = Date::constant(2026, 10, 22);
        let dataset = blueprint(two_hour_mean(), config())
            .dataset(catalog.path())
            .unwrap();
        dataset
            .build(first, thursday, |from, to| {
                replay_on(catalog, two_hour_mean(), from, to)
            })
            .unwrap();
        dataset
            .read(on(thursday, "00:00:00"), on(thursday, "23:59:59"))
            .unwrap()
    })
}

#[rstest]
fn a_day_built_alone_is_stored_as_inside_a_longer_build() {
    let catalog = walk(
        on(Date::constant(2026, 10, 20), "20:00:00"),
        on(Date::constant(2026, 10, 23), "03:00:00"),
    );
    let copy = copy_of(&catalog);

    let alone = thursday_stored_by_a_build_from(&catalog, Date::constant(2026, 10, 22));
    let inside = thursday_stored_by_a_build_from(&copy, Date::constant(2026, 10, 21));

    assert_eq!(alone.len(), 24, "{alone:#?}");
    assert_eq!(alone, inside);
}

fn on_thread<T: Send>(run: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| scope.spawn(run).join().unwrap())
}

fn copy_of(catalog: &TempDir) -> TempDir {
    let copy = tempfile::tempdir().unwrap();
    let mut dirs = vec![PathBuf::new()];
    while let Some(dir) = dirs.pop() {
        std::fs::create_dir_all(copy.path().join(&dir)).unwrap();
        for entry in std::fs::read_dir(catalog.path().join(&dir)).unwrap() {
            let entry = entry.unwrap();
            let path = dir.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                dirs.push(path);
            } else {
                std::fs::copy(catalog.path().join(&path), copy.path().join(&path)).unwrap();
            }
        }
    }
    copy
}

fn on(date: Date, time: &str) -> Timestamp {
    format!("{date}T{time}Z").parse().unwrap()
}

/// There are no quotes from 21:00 on Friday 23rd to 22:00 on Sunday 25th.
fn weekend_walk() -> TempDir {
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

fn stored_by_a_build_to(
    catalog: &TempDir,
    last: Date,
    from: Timestamp,
    to: Timestamp,
) -> Vec<DatasetRow> {
    on_thread(|| {
        let dataset = blueprint(features(), config())
            .dataset(catalog.path())
            .unwrap();
        dataset
            .build(Date::constant(2026, 10, 22), last, |from, to| {
                replay(catalog, from, to)
            })
            .unwrap();
        dataset.read(from, to).unwrap()
    })
}

fn friday_stored_by_a_build_to(catalog: &TempDir, last: Date) -> Vec<DatasetRow> {
    let friday = Date::constant(2026, 10, 23);
    stored_by_a_build_to(
        catalog,
        last,
        on(friday, "00:00:00"),
        on(friday, "23:59:59"),
    )
}

/// The decision at 21:00, on Friday's last quote, settles at its time limit, as the venue fills the
/// time close on the last quote it has. So a build cut on Friday, which replays to 02:00 on
/// Saturday, and one cut on Monday store the same Friday. The decisions at 22:00 and 23:00 have no
/// quote since, so they store no row.
#[rstest]
fn a_day_is_stored_the_same_by_a_build_cut_before_a_weekend_and_one_cut_after() {
    let catalog = weekend_walk();
    let copy = copy_of(&catalog);

    let cut_before = friday_stored_by_a_build_to(&catalog, Date::constant(2026, 10, 23));
    let cut_after = friday_stored_by_a_build_to(&copy, Date::constant(2026, 10, 26));

    assert_eq!(cut_before.len(), 22, "{cut_before:#?}");
    assert_eq!(cut_before, cut_after);
}

#[rstest]
fn a_closed_market_stores_no_row() {
    let (friday, sunday) = (Date::constant(2026, 10, 23), Date::constant(2026, 10, 25));

    let rows = stored_by_a_build_to(
        &weekend_walk(),
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

/// The bracket is 1.5 ATRs away on both sides and closes after 45 minutes. A reference stake of 100
/// USD buys whole ounces on the walk's stops of some dollars, which 4 USD would not.
fn atr_config() -> ModelConfig {
    let bracket = Target::builder()
        .bracket(BracketSpec {
            take_profit: Distance::Atr(Decimal::new(15, 1)),
            stop: Distance::Atr(Decimal::new(15, 1)),
            atr: None,
        })
        .limit(SignedDuration::from_mins(45))
        .build();
    ModelConfig {
        target: TargetConfig {
            spec: bracket,
            risk: "0.001%".parse().unwrap(),
            ..target()
        },
        ..config()
    }
}

fn mid_warming_for(warmup: SignedDuration) -> Rc<FeatureSet> {
    FeatureSet::builder()
        .feature(
            &[Series::quotes(InstrumentId::from(ic::INSTRUMENT_ID))],
            MID,
            &mid_tables(),
        )
        .warmup(warmup)
        .build()
        .unwrap()
}

fn thursday_decided_after_a_warmup_of(warmup: SignedDuration) -> Vec<Timestamp> {
    std::thread::spawn(move || {
        let thursday = Date::constant(2026, 10, 22);
        let catalog = walk(
            on(Date::constant(2026, 10, 21), "09:00:02.500"),
            on(Date::constant(2026, 10, 23), "03:00:00"),
        );
        let config = atr_config();
        let dataset = blueprint(mid_warming_for(warmup), config.clone())
            .dataset(catalog.path())
            .unwrap();
        dataset
            .build(thursday, thursday, |from, to| {
                replay_of(&catalog, mid_warming_for(warmup), &config, from, to)
            })
            .unwrap();
        dataset
            .read(on(thursday, "00:00:00"), on(thursday, "23:59:59"))
            .unwrap()
            .iter()
            .map(|row| Timestamp::from(row.ts_event))
            .collect()
    })
    .join()
    .unwrap()
}

fn reading_quotes(warmup: SignedDuration) -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(ic::INSTRUMENT_ID),
        }],
        custom: vec![],
        warmup,
    }
}

/// A target's ATR of 14 hourly bars warms over exactly a warmup of 14 hours. A build of a day alone
/// replays from 10:00 the day before, so the 14th hourly bar closes at midnight, 10 ms before the
/// day's first decision. The quotes are 2.5 s off the hour, because one at 10:00 would close a 15th
/// bar and hide a warmup one bar short.
#[rstest]
fn a_target_atr_warming_over_exactly_the_warmup_opens_the_day_s_first_decision() {
    let (thursday, margin) = (
        Date::constant(2026, 10, 22),
        SignedDuration::from_millis(10),
    );
    let (warmup, shorter) = (
        SignedDuration::from_hours(14),
        SignedDuration::from_hours(13),
    );
    assert!(atr_config().validate(&reading_quotes(warmup)).is_ok());
    assert!(atr_config().validate(&reading_quotes(shorter)).is_err());

    let decided = thursday_decided_after_a_warmup_of(warmup);
    let decided_shorter = thursday_decided_after_a_warmup_of(shorter);

    assert_eq!(decided.first(), Some(&(on(thursday, "00:00:00") + margin)));
    assert_eq!(decided.len(), 24, "{decided:#?}");
    assert_eq!(
        decided_shorter.first(),
        Some(&(on(thursday, "01:00:00") + margin))
    );
}
