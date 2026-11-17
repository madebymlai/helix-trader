//! A model strategy's features on a futures root's ticks (`common::rolls`:
//! GC.GLBX, rolling from GCQ26.GLBX to GCZ26.GLBX at 00:00 UTC on 29 July
//! 2026), trading IC's gold, whose quotes the harness reads: a dataset run
//! from 23:00 to 01:00 across the roll, deciding every 15 minutes, its
//! model handle rolling the root's trades. The features read the pre
//! contract, are reset at the roll, stay cold until their window refills
//! on the post contract, with no row, then read the post contract alone.
//! One node per thread, so one run per test.

use std::rc::Rc;

use helix_trader::{
    model::{
        config::ModelConfig,
        decide::DecisionRule,
        features::FeatureSet,
        handle::{Mode, SettledRows},
    },
    strategy::{
        Declared, IndicatorsConfig, Series,
        requirements::{DataRequirements, TickSeries},
    },
};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::{
    data::{NautilusDataType, QuoteTick, TradeTick},
    identifiers::InstrumentId,
};
use rstest::rstest;

use crate::common::{
    dataset::{self, probe_reading},
    features::{SmaArgs, tables},
    ic,
    rolls::{self, POST, PRE, ROOT, quote, trade, tuesday, wednesday},
};

/// The contracts' trades: August's at 22:50 (before the run), 23:10, 23:20
/// and 23:50 in its slice, 00:10 after it; December's at 23:40, before its
/// slice, then 00:05, 00:20 and 00:35.
fn trades() -> Vec<TradeTick> {
    vec![
        trade(PRE, tuesday("22:50:00"), "3989.00"),
        trade(PRE, tuesday("23:10:00"), "3990.00"),
        trade(PRE, tuesday("23:20:00"), "3991.00"),
        trade(PRE, tuesday("23:50:00"), "3993.00"),
        trade(PRE, wednesday("00:10:00"), "3994.00"),
        trade(POST, tuesday("23:40:00"), "4040.00"),
        trade(POST, wednesday("00:05:00"), "4041.00"),
        trade(POST, wednesday("00:20:00"), "4042.00"),
        trade(POST, wednesday("00:35:00"), "4043.00"),
    ]
}

/// Both contracts' trades and IC's gold quoted at 30 s past every minute
/// from 22:00 Tuesday to 02:00 Wednesday, its mid 4000.10.
fn catalog() -> tempfile::TempDir {
    catalog_of(&trades(), &[])
}

/// The contracts' `trades` and `quotes`, and IC's gold quoted at 30 s past
/// every minute from 22:00 Tuesday to 02:00 Wednesday, its mid 4000.10.
fn catalog_of(trades: &[TradeTick], quotes: &[QuoteTick]) -> tempfile::TempDir {
    let catalog = rolls::catalog_of_ticks(trades, quotes);
    let (from, to) = (tuesday("22:00:00"), wednesday("02:00:00"));
    let mut quotes = vec![];
    let mut time = from + SignedDuration::from_secs(30);
    while time < to {
        quotes.push(ic::quote(time, "4000.00", "4000.20"));
        time += SignedDuration::from_mins(1);
    }
    ic::store(catalog.path(), quotes, from, to);
    catalog
}

/// The mean of the last two trades, `[indicators.root_2_trades]`.
const ROOT_2_TRADES: Declared<SmaArgs> = Declared::new("root_2_trades");
/// The mean of the last 60 mids, `[indicators.gold_60_mids]`.
const GOLD_60_MIDS: Declared<SmaArgs> = Declared::new("gold_60_mids");

/// The tables of `ROOT_2_TRADES` and `GOLD_60_MIDS`, as `[indicators]`
/// holds them.
fn root_tables() -> IndicatorsConfig {
    tables(
        r#"
        root_2_trades = { period = 2 }
        gold_60_mids = { period = 60, price_type = "MID" }
        "#,
    )
}

/// The mean of the root's last two trades.
fn root_trades() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .trades(InstrumentId::from(ROOT), ROOT_2_TRADES, &root_tables())
        .build()
        .unwrap()
}

/// The `data` ticks of `id`.
fn tick(data: NautilusDataType, id: &str) -> TickSeries {
    TickSeries {
        data,
        instrument_id: InstrumentId::from(id),
    }
}

/// What the strategy reads: the root's trades, and the quotes of IC's gold,
/// which the harness reads for the instrument it trades.
fn requirements() -> DataRequirements {
    requirements_of(vec![tick(NautilusDataType::TradeTick, ROOT)])
}

/// What the strategy reads: the root's `ticks`, and the quotes of IC's
/// gold, which the harness reads for the instrument it trades.
fn requirements_of(mut ticks: Vec<TickSeries>) -> DataRequirements {
    ticks.push(tick(NautilusDataType::QuoteTick, ic::INSTRUMENT_ID));
    DataRequirements {
        bar_types: vec![],
        ticks,
        custom: vec![],
        warmup: SignedDuration::ZERO,
    }
}

/// The probe's decisions and targets (`common::dataset`), deciding every
/// 15 minutes.
fn config() -> ModelConfig {
    ModelConfig {
        decision: DecisionRule::Every(SignedDuration::from_mins(15)),
        ..dataset::config()
    }
}

/// The time of each row the dataset run of the probe deciding on
/// `features` published over 23:00 to 01:00, and its values.
fn rows(features: Rc<FeatureSet>) -> Vec<(Timestamp, Vec<f64>)> {
    rows_of(features, &catalog(), &requirements())
}

/// The rows of `rows`, of the probe reading `requirements` of `catalog`.
fn rows_of(
    features: Rc<FeatureSet>,
    catalog: &tempfile::TempDir,
    requirements: &DataRequirements,
) -> Vec<(Timestamp, Vec<f64>)> {
    let requirements = requirements.clone();
    let (probe, record) = probe_reading(
        features,
        &config(),
        &requirements,
        &Mode::Dataset {
            rows: SettledRows::default(),
        },
    );
    let backtest = helix_trader::backtest::Backtest {
        ticks: requirements.ticks,
        ..dataset::backtest(catalog.path(), tuesday("23:00:00"), wednesday("01:00:00"))
    };
    backtest.run_dataset(probe).unwrap();
    let record = record.take();
    record
        .rows
        .into_iter()
        .map(|row| (Timestamp::from(row.ts_event), row.features))
        .collect()
}

/// A decision's time: `time` and 10 ms.
fn decided(time: Timestamp) -> Timestamp {
    time + SignedDuration::from_millis(10)
}

fn assert_rows(got: &[(Timestamp, Vec<f64>)], expected: &[(Timestamp, Vec<f64>)]) {
    let times: Vec<Timestamp> = got.iter().map(|(time, _)| *time).collect();
    let expected_times: Vec<Timestamp> = expected.iter().map(|(time, _)| *time).collect();
    assert_eq!(times, expected_times, "{got:?}");
    for ((time, values), (_, want)) in got.iter().zip(expected) {
        assert_eq!(values.len(), want.len(), "at {time}: {values:?}");
        for (value, want) in values.iter().zip(want) {
            assert!(
                (value - want).abs() < 1e-9,
                "at {time}: {values:?}, {want:?}"
            );
        }
    }
}

/// The run replays August's trades of 23:10, 23:20 and 23:50 and
/// December's of 00:05, 00:20 and 00:35. The decision of 23:15 has one
/// trade; those of 23:30 and 23:45 the mean of 3990 and 3991. The roll's
/// alert, at its last nanosecond before 00:00, resets the feature: the
/// decision of 00:00, which would have read 3991 and 3993, has no trade,
/// and that of 00:15, which would have mixed 3993 and 4041, one. Those of
/// 00:30 and 00:45 read December's alone: the mean of 4041 and 4042, then
/// of 4042 and 4043. No feature reads IC's gold, which the run trades:
/// its quotes, the harness's, make each decision fresh.
#[rstest]
fn features_on_a_root_read_each_contract_in_turn_reset_at_the_roll() {
    let rows = rows(root_trades());

    assert_rows(
        &rows,
        &[
            (decided(tuesday("23:30:00")), vec![3990.5]),
            (decided(tuesday("23:45:00")), vec![3990.5]),
            (decided(wednesday("00:30:00")), vec![4041.5]),
            (decided(wednesday("00:45:00")), vec![4042.5]),
        ],
    );
}

/// The root's feature beside the mean of IC's gold's last 60 mids, warm
/// from its 60th quote, at 23:59:30. The roll resets the root's feature
/// alone: the gold's, reset too, would stay cold until 00:59:30, after the
/// run's last decision; untouched, it gives the rows of 00:30 and 00:45,
/// a mid of 4000.10.
#[rstest]
fn a_roll_leaves_the_features_of_other_instruments_untouched() {
    let tables = root_tables();
    let features = FeatureSet::builder()
        .trades(InstrumentId::from(ROOT), ROOT_2_TRADES, &tables)
        .quotes(InstrumentId::from(ic::INSTRUMENT_ID), GOLD_60_MIDS, &tables)
        .build()
        .unwrap();

    let rows = rows(features);

    assert_rows(
        &rows,
        &[
            (decided(wednesday("00:30:00")), vec![4041.5, 4000.1]),
            (decided(wednesday("00:45:00")), vec![4042.5, 4000.1]),
        ],
    );
}

/// The mean of the last two quote mids or trade prices,
/// `[indicators.root_2_ticks]`.
const ROOT_2_TICKS: Declared<SmaArgs> = Declared::new("root_2_ticks");

/// August's quotes, mid 3990, 3992 and 3993 at 23:05, 23:20 and 23:40, and
/// its trades, 3991 and 3994 at 23:10 and 23:50; then December's trades,
/// 4041 and 4043 at 00:05 and 00:20, and its quotes, mid 4042 and 4044 at
/// 00:10 and 00:35. August's quote of 00:25 and December's of 23:55 are
/// outside their slices, not replayed. The decision of 23:15 reads the
/// mean of the quote of 23:05 and the trade of 23:10, 3990.5; that of
/// 23:30 of that trade and the quote of 23:20, 3991.5; that of 23:45 of
/// the quotes of 23:20 and 23:40, 3992.5. The roll resets the feature: the
/// decision of 00:00 has no tick. Then 00:15 reads the trade of 00:05 and
/// the quote of 00:10, 4041.5; 00:30 that quote and the trade of 00:20,
/// 4042.5; 00:45 that trade and the quote of 00:35, 4043.5. Quotes alone,
/// trades alone, or one after the other would each read other means.
#[rstest]
fn a_feature_on_a_roots_quotes_and_trades_reads_them_in_time_order_across_the_roll() {
    let trades = [
        trade(PRE, tuesday("23:10:00"), "3991.00"),
        trade(PRE, tuesday("23:50:00"), "3994.00"),
        trade(POST, wednesday("00:05:00"), "4041.00"),
        trade(POST, wednesday("00:20:00"), "4043.00"),
    ];
    let quotes = [
        quote(PRE, tuesday("23:05:00"), "3990.00"),
        quote(PRE, tuesday("23:20:00"), "3992.00"),
        quote(PRE, tuesday("23:40:00"), "3993.00"),
        quote(PRE, wednesday("00:25:00"), "3996.00"),
        quote(POST, tuesday("23:55:00"), "4039.00"),
        quote(POST, wednesday("00:10:00"), "4042.00"),
        quote(POST, wednesday("00:35:00"), "4044.00"),
    ];
    let tables = tables(r#"root_2_ticks = { period = 2, price_type = "MID" }"#);
    let features = FeatureSet::builder()
        .feature(
            &[
                Series::Ticks(tick(NautilusDataType::QuoteTick, ROOT)),
                Series::Ticks(tick(NautilusDataType::TradeTick, ROOT)),
            ],
            ROOT_2_TICKS,
            &tables,
        )
        .build()
        .unwrap();
    let requirements = requirements_of(vec![
        tick(NautilusDataType::QuoteTick, ROOT),
        tick(NautilusDataType::TradeTick, ROOT),
    ]);

    let rows = rows_of(features, &catalog_of(&trades, &quotes), &requirements);

    assert_rows(
        &rows,
        &[
            (decided(tuesday("23:15:00")), vec![3990.5]),
            (decided(tuesday("23:30:00")), vec![3991.5]),
            (decided(tuesday("23:45:00")), vec![3992.5]),
            (decided(wednesday("00:15:00")), vec![4041.5]),
            (decided(wednesday("00:30:00")), vec![4042.5]),
            (decided(wednesday("00:45:00")), vec![4043.5]),
        ],
    );
}
