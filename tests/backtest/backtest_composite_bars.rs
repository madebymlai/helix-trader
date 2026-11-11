//! A strategy on a composite bar type, hour bars Nautilus builds from
//! external minute bars: `fill_catalog` stores the minute bars a source
//! returns, as the strategy's bar types ask (`strategy::catalog_series`),
//! and the backtest replays them, so that every hour bar has the open,
//! high, low and close of its minute bars, built `BUILT_AFTER` its close
//! as live. A model strategy deciding on them decides once the hour bar
//! closing on its grid is built. One node per thread, so one run per test.

use std::path::Path;

use crate::common::{
    self, dataset,
    features::{SmaArgs, tables},
    instrument, ohlc,
    recorder::Recorder,
    ts,
};
use helix_trader::{
    catalog::fill_catalog,
    model::{
        config::ModelConfig,
        features::FeatureSet,
        handle::{Mode, SettledRows},
        targets::TargetConfig,
    },
    strategy::{DataRequirements, Declared, catalog_series},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, Data, NautilusDataType},
    enums::OmsType,
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const COMPOSITE: &str = "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";
const MINUTE_BARS: &str = "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL";

/// How long after its close the node builds an hour bar of minute bars,
/// and stamps it: 5 s, as a live node waits for the last minute.
const BUILT_AFTER: SignedDuration = SignedDuration::from_secs(5);

/// `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// The minute bar closing at `close`, stamped at its close: the k-th minute
/// after 06:00 opens at 600 plus 0.01 times `37 k mod 101`, with its own
/// high, low and close around it.
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

/// The minute bars closing in `[from, to]`.
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

/// The hour bar closing at `close`, worked out here from its minute bars,
/// those closing in `(close − 1 h, close]`, stamped `BUILT_AFTER` its
/// close.
fn hour_bar(close: Timestamp) -> common::Ohlc {
    let minutes = minute_bars(close - SignedDuration::from_mins(59), close);
    let high = minutes.iter().map(|bar| bar.high).max().unwrap();
    let low = minutes.iter().map(|bar| bar.low).min().unwrap();
    let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
    (close + BUILT_AFTER, first.open, high, low, last.close)
}

/// Fills the catalog in `dir` with the instrument and the minute bars the
/// hour bars are built from, over 06:00 to 15:00, as `fill-catalog` does.
fn fill(dir: &Path) {
    let mut catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let series = catalog_series(&[BarType::from(COMPOSITE)]);
    let fill = fill_catalog(
        &mut catalog,
        vec![instrument()],
        &series,
        at("06:00"),
        at("15:00"),
        async |_, _, from, to| Ok(minute_bars(from, to).into_iter().map(Data::Bar).collect()),
    );
    // The node runs its own runtime: this one is gone before it starts.
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();
}

#[test]
fn hour_bars_built_from_stored_minute_bars_have_the_prices_of_their_minutes() {
    let dir = tempfile::tempdir().unwrap();
    let bar_types = [BarType::from(COMPOSITE)];
    assert_eq!(
        catalog_series(&bar_types),
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
    // The run starts at 10:30: the bar closing 11:00 lacks the minutes
    // before it; those closing 12:00 to 15:00 are whole, the last built
    // at 15:00:05, before the run's end.
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

/// The close of a bar, Nautilus' simple average of one,
/// `[indicators.close]`.
const CLOSE: Declared<SmaArgs> = Declared::new("close");

/// A model strategy deciding every hour on the close of the hour bars, in
/// a dataset run from 10:30 to 15:00 on a hedging venue, opening the
/// targets of `dataset::config` on the instrument. A live node builds the
/// hour bars `BUILT_AFTER` their close, so that the minute closing on the
/// hour is in, and decides 10 ms after that; the backtest does both as
/// live, so each decision reads the hour bar closing on its grid, whose
/// close is that minute's. The hour closing 15:00 is built after the
/// run's end.
#[test]
fn a_decision_on_hour_bars_reads_the_bar_closing_on_its_grid() {
    let dir = tempfile::tempdir().unwrap();
    fill(dir.path());
    let hour_bars = BarType::from(COMPOSITE);
    let features = FeatureSet::builder()
        .bars(hour_bars, CLOSE, &tables("close = { period = 1 }"))
        .build()
        .unwrap();
    let requirements = DataRequirements {
        bar_types: vec![hour_bars],
        ticks: vec![],
        warmup: SignedDuration::ZERO,
    };
    let config = ModelConfig {
        target: TargetConfig {
            instrument_id: InstrumentId::from(common::INSTRUMENT_ID),
            ..dataset::target()
        },
        ..dataset::config()
    };
    let mut backtest = common::backtest(dir.path(), at("10:30"), at("15:00"), "0 EUR");
    let (probe, record) = dataset::probe_reading(
        features,
        &config,
        &requirements,
        Mode::Dataset {
            rows: SettledRows::default(),
            commission: backtest.commission,
        },
    );
    backtest.bar_types = requirements.bar_types.clone();
    backtest.venue_oms = |_| OmsType::Hedging;

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
