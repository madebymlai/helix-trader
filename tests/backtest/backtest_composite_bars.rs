//! Tests a composite bar type: hour bars that Nautilus builds from external minute bars.
//! `fill_catalog` stores the minute bars as the strategy's bar types ask
//! (`strategy::requirements::catalog_keys`). The backtest replays them, and builds each hour bar
//! `BUILT_AFTER` its close, as live does. A model strategy decides once the hour bar closing on
//! its grid is built.
//!
//! The k-th minute bar after 06:00 opens at 600 + 0.01 x (37 k mod 101). The fill stores minute
//! bars from 06:00 to 15:00, and the runs start at 10:30.

use std::path::Path;

use helix_trader::{
    catalog::fill_catalog,
    model::{
        config::ModelConfig,
        dataset::DatasetVenue,
        features::FeatureSet,
        handle::{Mode, SettledRows},
        targets::TargetConfig,
    },
    strategy::{
        Declared, Series,
        requirements::{CatalogSeries, DataRequirements, catalog_keys},
    },
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, Data, NautilusDataType},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rstest::rstest;

use crate::common::{
    self, dataset,
    features::{SmaArgs, tables},
    instrument, ohlc,
    recorder::Recorder,
    ts,
};

const COMPOSITE: &str = "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";
const MINUTE_BARS: &str = "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL";

/// How long a live node waits after the close for the last minute.
const BUILT_AFTER: SignedDuration = SignedDuration::from_secs(5);

fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

fn minute_bar(close: Timestamp) -> Bar {
    let k = close.duration_since(at("06:00")).as_mins();
    let open = 600.0 + 0.01 * ((37 * k) % 101) as f64;
    let stamp = UnixNanos::from(close);
    Bar::new(
        BarType::from(MINUTE_BARS),
        Price::new(open, 2),
        Price::new(open + 0.30, 2),
        Price::new(open - 0.20, 2),
        Price::new(open + 0.10, 2),
        Quantity::from(1),
        stamp,
        stamp,
    )
}

fn minute_bars(from: Timestamp, to: Timestamp) -> Vec<Bar> {
    let minute = SignedDuration::from_mins(1);
    let mut close = from;
    let mut bars = vec![];

    while close <= to {
        if close > at("06:00") {
            bars.push(minute_bar(close));
        }
        close += minute;
    }
    bars
}

/// Returns the hour bar worked out from the minute bars closing in `(close − 1 h, close]`, stamped
/// `BUILT_AFTER` the close.
fn hour_bar(close: Timestamp) -> common::Ohlc {
    let minutes = minute_bars(close - SignedDuration::from_mins(59), close);
    let high = minutes.iter().map(|bar| bar.high).max().unwrap();
    let low = minutes.iter().map(|bar| bar.low).min().unwrap();
    let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
    (close + BUILT_AFTER, first.open, high, low, last.close)
}

fn fill(dir: &Path) {
    let mut catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let series = [CatalogSeries::Bars(BarType::from(COMPOSITE).composite())];
    let fill = fill_catalog(
        &mut catalog,
        vec![instrument()],
        &series,
        at("06:00"),
        at("15:00"),
        async |_, from, to| Ok(minute_bars(from, to).into_iter().map(Data::Bar).collect()),
    );

    // The node runs its own runtime, and this one is gone before it starts
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();
}

#[rstest]
fn hour_bars_built_from_stored_minute_bars_have_the_prices_of_their_minutes() {
    let dir = tempfile::tempdir().unwrap();
    let bar_types = [BarType::from(COMPOSITE)];
    assert_eq!(
        catalog_keys(&bar_types),
        [(NautilusDataType::Bar, MINUTE_BARS.to_string())],
        "the minute bars are what the hour bars are built from"
    );
    fill(dir.path());

    let (recorder, record) = Recorder::new(BarType::from(COMPOSITE));
    let mut backtest = common::backtest(dir.path(), at("10:30"), at("15:01"), "0 EUR");
    backtest.bar_types = bar_types.to_vec();
    backtest.run(recorder).unwrap();

    let record = record.borrow();
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();

    // The bar closing 11:00 lacks the minutes before 10:30, and the last is built at 15:00:05,
    // before the run's end.
    let stamps: Vec<Timestamp> = replay.iter().map(|bar| bar.0).collect();
    assert_eq!(
        stamps,
        ["11:00", "12:00", "13:00", "14:00", "15:00"].map(|close| at(close) + BUILT_AFTER)
    );
    let whole: Vec<_> = ["12:00", "13:00", "14:00", "15:00"]
        .map(|close| hour_bar(at(close)))
        .to_vec();
    assert_eq!(replay[1..], whole);
}

const CLOSE: Declared<SmaArgs> = Declared::new("close");

/// A dataset run from 10:30 to 15:00 on a hedging venue decides 10 ms after each hour bar is built.
/// The hour closing 15:00 is built after the run's end.
#[rstest]
fn a_decision_on_hour_bars_reads_the_bar_closing_on_its_grid() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path());
    let hour_bars = BarType::from(COMPOSITE);
    let features = FeatureSet::builder()
        .feature(
            &[Series::Bars(hour_bars)],
            CLOSE,
            &tables("close = { period = 1 }"),
        )
        .build()
        .unwrap();
    let requirements = DataRequirements {
        bar_types: vec![hour_bars],
        ticks: vec![],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let mut backtest = common::backtest(dir.path(), at("10:30"), at("15:00"), "0 EUR");
    let config = ModelConfig {
        target: TargetConfig {
            instrument_id: InstrumentId::from(common::INSTRUMENT_ID),
            ..dataset::target()
        },
        dataset: DatasetVenue {
            commission: backtest.commission,
            balance: backtest.balance,
            latency: None,
        },
        ..dataset::config()
    };
    let (probe, record) = dataset::probe_reading(
        features,
        &config,
        &requirements,
        &Mode::Dataset {
            rows: SettledRows::default(),
        },
    );
    backtest.bar_types = requirements.bar_types;

    backtest.run_dataset(probe).unwrap();

    let decisions: Vec<(Timestamp, f64)> = record
        .borrow()
        .rows
        .iter()
        .map(|row| (Timestamp::from(row.ts_event), row.features[0]))
        .collect();
    let late = BUILT_AFTER + SignedDuration::from_millis(10);
    let expected: Vec<(Timestamp, f64)> = ["11:00", "12:00", "13:00", "14:00"]
        .map(|hour| (at(hour) + late, hour_bar(at(hour)).4.as_f64()))
        .to_vec();
    assert_eq!(decisions, expected);
}
