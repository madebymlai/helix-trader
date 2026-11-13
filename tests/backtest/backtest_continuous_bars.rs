//! Bars on a rolled root (`common::rolls`: GC.GLBX, rolling from GCQ26.GLBX
//! to GCZ26.GLBX at 00:00 UTC on 29 July 2026): Nautilus' continuous bars,
//! fed the transition table with its roll prices read from the catalog,
//! August's last trade of its slice (3992.00, 1 ns before the roll) and
//! December's first (4041.00, at the roll), a spread of 49.00. 30-minute
//! LAST bars over a run from 23:00 to 01:00. One node per thread, so one
//! run per test.

use crate::common::{
    Ohlc, ohlc,
    recorder::Recorder,
    rolls::{self, POST, tuesday, wednesday},
};
use helix_trader::{
    backtest::{Backtest, Commission},
    venues,
};
use jiff::Timestamp;
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_model::{
    data::BarType,
    enums::ContinuousFutureAdjustmentType,
    identifiers::InstrumentId,
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;
use tempfile::TempDir;

const BARS: &str = "GC.GLBX-30-MINUTE-LAST-INTERNAL";

/// A backtest of the bars on the root over `[23:00 Tuesday, 01:00
/// Wednesday]` on `catalog`.
fn backtest(catalog: &TempDir) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![BarType::from(BARS)],
        ticks: vec![],
        custom: vec![],
        start: tuesday("23:00:00"),
        end: wednesday("01:00:00"),
        commission: Commission::Rate(Decimal::ZERO),
        balance: Money::from("1000000 USD"),
        leverage: Decimal::ONE,
        venue_oms: venues::oms_type,
        logging: LoggerConfig::default(),
    }
}

/// A flat bar closing at `close`, at `price`.
fn flat(close: Timestamp, price: &str) -> Ohlc {
    let price = Price::from(price);
    (close, price, price, price, price)
}

/// What a recorder of the root's bars got: its bars and their volumes.
struct Got {
    bars: Vec<Ohlc>,
    volumes: Vec<Quantity>,
}

/// Runs a recorder of the root's bars in `mode` through `backtest`.
fn run(backtest: &Backtest, mode: ContinuousFutureAdjustmentType) -> Got {
    let continuous = backtest.continuous_bars(mode).unwrap();
    let (recorder, record) = Recorder::continuous(BarType::from(BARS), continuous);
    backtest.run(recorder).unwrap();
    let record = record.borrow();
    Got {
        bars: record.bars.iter().map(ohlc).collect(),
        volumes: record.bars.iter().map(|bar| bar.volume).collect(),
    }
}

#[test]
fn a_contract_is_plain_and_has_no_params() {
    let catalog = rolls::catalog();
    let mut backtest = backtest(&catalog);
    backtest.bar_types = vec![BarType::from("GCZ26.GLBX-30-MINUTE-LAST-INTERNAL")];

    let continuous = backtest
        .continuous_bars(ContinuousFutureAdjustmentType::BackwardSpread)
        .unwrap();

    assert_eq!(continuous.params(InstrumentId::from(POST)), None);
}

#[test]
fn backward_spread_bars_shift_the_pre_roll_bars_by_the_spread() {
    let catalog = rolls::catalog();

    let got = run(
        &backtest(&catalog),
        ContinuousFutureAdjustmentType::BackwardSpread,
    );

    // August's trades plus 49.00, December's as they are; those before
    // 23:00 are not replayed.
    assert_eq!(
        got.bars,
        [
            flat(tuesday("23:30:00"), "4040.00"),
            flat(wednesday("00:00:00"), "4041.00"),
            flat(wednesday("01:00:00"), "4042.00"),
        ]
    );
    // December's trade at the roll (4041.00, its post price) reaches no
    // bar: the engine rolls the subscription on a timer at the roll, and
    // the backtest delivers the trade first, to August's subscription.
    assert_eq!(got.volumes, [Quantity::from(1); 3]);
}

#[test]
fn forward_spread_bars_shift_the_post_roll_bars_by_the_spread() {
    let catalog = rolls::catalog();

    let got = run(
        &backtest(&catalog),
        ContinuousFutureAdjustmentType::ForwardSpread,
    );

    // August's trades as they are, December's less 49.00.
    assert_eq!(
        got.bars,
        [
            flat(tuesday("23:30:00"), "3991.00"),
            flat(wednesday("00:00:00"), "3992.00"),
            flat(wednesday("01:00:00"), "3993.00"),
        ]
    );
}

#[test]
fn forward_ratio_bars_scale_the_post_roll_bars_by_the_ratio() {
    let catalog = rolls::catalog();

    let got = run(
        &backtest(&catalog),
        ContinuousFutureAdjustmentType::ForwardRatio,
    );

    // August's trades as they are, December's times 3992.00 / 4041.00:
    // 4042.00 × 3992 / 4041 = 3992.98788, to the cent 3992.99.
    assert_eq!(
        got.bars,
        [
            flat(tuesday("23:30:00"), "3991.00"),
            flat(wednesday("00:00:00"), "3992.00"),
            flat(wednesday("01:00:00"), "3992.99"),
        ]
    );
}

#[test]
fn a_roll_without_a_trade_of_the_post_contract_in_its_slice_is_refused() {
    let catalog = rolls::catalog_storing(false);

    let err = backtest(&catalog)
        .continuous_bars(ContinuousFutureAdjustmentType::BackwardSpread)
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(err.contains(POST), "{err}");
}
