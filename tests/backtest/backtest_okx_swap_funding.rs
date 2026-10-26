//! Funding on an OKX swap, settled by Nautilus' simulated venue: the
//! catalog stores BTC-USDT-SWAP.OKX's minute bars and its funding rates as
//! `fill-catalog` stores them (`ts_event` = `ts_init` = the funding time,
//! an 8-hour interval), so the backtest replays the funding with the bars.
//! A recorder on the hour bars buys 2 contracts (0.02 BTC) on the first
//! one and holds them across two funding times: at each, the venue makes a
//! `FundingSettlement` on the position, priced at the last bar's close (no
//! mark price: the mid of the book the bars set), which adjusts the
//! position and the balance. A long pays a positive rate and receives a
//! negative one. One node per thread, so one run per test; the refused
//! window builds no node.

use std::path::Path;

use crate::common::{recorder::Recorder, ts};
use helix_trader::backtest::{Backtest, commission_rate, venue_oms_type};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, FundingRateUpdate},
    enums::{OrderSide, PositionAdjustmentType},
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

const INSTRUMENT_ID: &str = "BTC-USDT-SWAP.OKX";
const MINUTE_BARS: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";
const HOUR_BARS: &str = "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

/// `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// BTC-USDT-SWAP as OKX defines it: price tick 0.1, lot 0.01 contracts,
/// 0.01 BTC a contract, with initial and maintenance margins of 1.
fn instrument() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(
        CryptoPerpetual::builder()
            .instrument_id(InstrumentId::from(INSTRUMENT_ID))
            .raw_symbol(Symbol::from("BTC-USDT-SWAP"))
            .base_currency(Currency::BTC())
            .quote_currency(Currency::USDT())
            .settlement_currency(Currency::USDT())
            .is_inverse(false)
            .price_precision(1)
            .size_precision(2)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from("0.01"))
            .multiplier(Quantity::from("0.01"))
            .lot_size(Quantity::from("0.01"))
            .min_quantity(Quantity::from("0.01"))
            .margin_init(Decimal::ONE)
            .margin_maint(Decimal::ONE)
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The flat minute bars opening in `[from, to)`: at 100,000 before noon,
/// 101,000 from noon, 500 contracts, stamped at the open (`ts_event`) and
/// the close (`ts_init`).
fn minute_bars(from: Timestamp, to: Timestamp) -> Vec<Bar> {
    let mut open = from;
    let mut bars = vec![];
    while open < to {
        let price = Price::from(if open < at("12:00") {
            "100000.0"
        } else {
            "101000.0"
        });
        bars.push(Bar::new(
            BarType::from(MINUTE_BARS),
            price,
            price,
            price,
            price,
            Quantity::from("500.00"),
            UnixNanos::from(open),
            UnixNanos::from(open + SignedDuration::from_mins(1)),
        ));
        open += SignedDuration::from_mins(1);
    }
    bars
}

/// The funding rate `rate` at `time`, of an 8-hour interval.
fn funding(time: Timestamp, rate: &str) -> FundingRateUpdate {
    let ts = UnixNanos::from(time);
    FundingRateUpdate::new(
        InstrumentId::from(INSTRUMENT_ID),
        rate.parse().unwrap(),
        Some(480),
        None,
        ts,
        ts,
    )
}

/// A catalog of the instrument, its minute bars stored over `[06:00,
/// bars_to]` and its funding at 08:00 (+0.01%) and 16:00 (-0.02%) stored
/// over `[06:00, funding_to]`.
fn catalog(dir: &Path, bars_to: &str, funding_to: &str) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    let from = Some(UnixNanos::from(at("06:00")));
    catalog
        .write_to_parquet(
            &minute_bars(at("06:00"), at(bars_to)),
            from,
            Some(UnixNanos::from(at(bars_to))),
            None,
        )
        .unwrap();
    catalog
        .write_to_parquet(
            &[
                funding(at("08:00"), "0.0001"),
                funding(at("16:00"), "-0.0002"),
            ],
            from,
            Some(UnixNanos::from(at(funding_to))),
            None,
        )
        .unwrap();
}

/// A run on the hour bars from 07:00 to `end`, no commission.
fn backtest(dir: &Path, end: &str) -> Backtest {
    Backtest {
        catalog: dir.to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start: at("07:00"),
        end: at(end),
        warmup: SignedDuration::ZERO,
        fee_model: commission_rate(Decimal::ZERO).unwrap(),
        balance: Money::from("10000 USDT"),
        leverage: Decimal::ONE,
        oms_type: venue_oms_type(Venue::from("OKX")),
        logging: LoggerConfig::default(),
    }
}

#[test]
fn a_held_swap_pays_and_receives_funding_on_its_notional_at_each_funding_time() {
    let dir = tempfile::tempdir().unwrap();
    catalog(dir.path(), "17:00", "17:00");
    let (recorder, record) = Recorder::trading_without_warmup(
        BarType::from(HOUR_BARS),
        vec![(OrderSide::Buy, Quantity::from("2.00"))],
    );

    let outcome = backtest(dir.path(), "17:00").run(recorder).unwrap();

    let record = record.borrow();
    assert_eq!(
        record.fills,
        [(
            OrderSide::Buy,
            Price::from("100000.0"),
            UnixNanos::from(at("07:00"))
        )]
    );
    // 0.02 BTC long: at 08:00, worth 2,000 USDT, it pays 0.01% (0.2); at
    // 16:00, worth 2,020 USDT, it receives 0.02% (0.404).
    let settled: Vec<_> = record
        .adjustments
        .iter()
        .map(|adjustment| {
            assert_eq!(adjustment.adjustment_type, PositionAdjustmentType::Funding);
            let reason = adjustment.reason.unwrap();
            assert!(reason.starts_with("funding_settlement:"), "{reason}");
            (
                Timestamp::from(adjustment.ts_event),
                adjustment.pnl_change.unwrap(),
            )
        })
        .collect();
    assert_eq!(
        settled,
        [
            (at("08:00"), Money::from("-0.2 USDT")),
            (at("16:00"), Money::from("0.404 USDT")),
        ]
    );
    let summary = &outcome.result.summary;
    assert_eq!(
        summary["account.OKX.balance.USDT.total"],
        "10000.20400000 USDT"
    );
}

#[test]
fn a_window_the_stored_funding_does_not_cover_is_refused_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    catalog(dir.path(), "18:00", "17:00");
    let (recorder, _) = Recorder::without_warmup(BarType::from(HOUR_BARS));

    let err = backtest(dir.path(), "18:00")
        .run(recorder)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains(&format!("funding_rates of {INSTRUMENT_ID}")),
        "{err}"
    );
    let missing = format!(
        "[{}, {}] is missing",
        at("17:00") + SignedDuration::from_nanos(1),
        at("18:00")
    );
    assert!(err.contains(&missing), "{err}");
}
