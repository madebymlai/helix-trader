//! Bars on a rolled root (`common::rolls`: GC.GLBX, rolling from GCQ6.GLBX
//! to GCZ6.GLBX at 00:00 UTC on 31 July 2026): Nautilus' continuous bars,
//! fed the transition table with its roll prices read from the catalog,
//! August's last trade of its slice (3992.00, 1 ns before the roll) and
//! December's first (4041.00, at the roll), a spread of 49.00. 30-minute
//! LAST bars over a run from 23:00 to 01:00 after a 30-minute warmup. One
//! node per thread, so one run per test.

use crate::common::{
    Ohlc, ohlc,
    recorder::Recorder,
    rolls::{self, POST, ROOT, friday, thursday},
};
use helix_trader::backtest::{Backtest, commission_rate, venue_oms_type};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_model::{
    data::BarType,
    enums::ContinuousFutureAdjustmentType,
    identifiers::InstrumentId,
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;
use serde_json::json;
use tempfile::TempDir;

const BARS: &str = "GC.GLBX-30-MINUTE-LAST-INTERNAL";

const WARMUP: SignedDuration = SignedDuration::from_mins(30);

/// A backtest of the bars on the root over `[23:00 Thursday, 01:00
/// Friday]` on `catalog`, after `WARMUP`.
fn backtest(catalog: &TempDir) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![BarType::from(BARS)],
        ticks: vec![],
        start: thursday("23:00:00"),
        end: friday("01:00:00"),
        warmup: WARMUP,
        fee_model: commission_rate(Decimal::ZERO).unwrap(),
        balance: Money::from("1000000 USD"),
        leverage: Decimal::ONE,
        venue_oms: venue_oms_type,
        logging: LoggerConfig::default(),
    }
}

/// A flat bar closing at `close`, at `price`.
fn flat(close: Timestamp, price: &str) -> Ohlc {
    let price = Price::from(price);
    (close, price, price, price, price)
}

/// What a recorder of the root's bars got: its warmup bars, its bars and
/// their volumes.
struct Got {
    warmup: Vec<Ohlc>,
    bars: Vec<Ohlc>,
    volumes: Vec<Quantity>,
}

/// Runs a recorder of the root's bars in `mode` through `backtest`.
fn run(backtest: &Backtest, mode: ContinuousFutureAdjustmentType) -> Got {
    let continuous = backtest.continuous_bars(mode).unwrap();
    let (recorder, record) = Recorder::continuous(BarType::from(BARS), WARMUP, continuous);
    backtest.run(recorder).unwrap();
    let record = record.borrow();
    assert_eq!(record.warmup_end, Some(Ok(())));
    Got {
        warmup: record.warmup.iter().map(ohlc).collect(),
        bars: record.bars.iter().map(ohlc).collect(),
        volumes: record.bars.iter().map(|bar| bar.volume).collect(),
    }
}

#[test]
fn the_params_of_the_roots_bars_carry_the_roll_priced_from_the_catalog() {
    let catalog = rolls::catalog();

    let continuous = backtest(&catalog)
        .continuous_bars(ContinuousFutureAdjustmentType::ForwardRatio)
        .unwrap();

    assert_eq!(
        continuous.params(InstrumentId::from(ROOT)),
        Some([
            (
                "continuous_future_transitions".to_string(),
                json!([{
                    "transition_time_ns": 1_785_456_000_000_000_000_u64,
                    "pre_instrument_id": "GCQ6.GLBX",
                    "post_instrument_id": "GCZ6.GLBX",
                    "pre_price": "3992.00",
                    "post_price": "4041.00",
                }])
            ),
            (
                "continuous_future_adjustment_mode".to_string(),
                json!("FORWARD_RATIO")
            ),
        ])
    );
}

#[test]
fn a_contract_is_plain_and_has_no_params() {
    let catalog = rolls::catalog();
    let mut backtest = backtest(&catalog);
    backtest.bar_types = vec![BarType::from("GCZ6.GLBX-30-MINUTE-LAST-INTERNAL")];

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

    // August's trades plus 49.00, December's as they are. The warmup,
    // from 22:29:59.999999999 to 22:59:59.999999999: the bar closing at
    // 22:30 holds August's 3990.00, built on the trade of 22:45, whose bar
    // the warmup does not close (a continuous request leaves out
    // `update_subscriptions` and `skip_first_non_full_bar`).
    assert_eq!(got.warmup, [flat(thursday("22:30:00"), "4039.00")]);
    assert_eq!(
        got.bars,
        [
            flat(thursday("23:30:00"), "4040.00"),
            flat(friday("00:00:00"), "4041.00"),
            flat(friday("01:00:00"), "4042.00"),
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
    assert_eq!(got.warmup, [flat(thursday("22:30:00"), "3990.00")]);
    assert_eq!(
        got.bars,
        [
            flat(thursday("23:30:00"), "3991.00"),
            flat(friday("00:00:00"), "3992.00"),
            flat(friday("01:00:00"), "3993.00"),
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

    assert!(
        err.contains("pricing the roll of GCQ6.GLBX to GCZ6.GLBX at 2026-07-31T00:00:00Z"),
        "{err}"
    );
    assert!(
        err.contains(
            "no trade of GCZ6.GLBX in its slice [2026-07-31T00:00:00Z, 2026-07-31T01:00:00Z]"
        ),
        "{err}"
    );
}
