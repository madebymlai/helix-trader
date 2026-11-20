//! Funding on an OKX swap, settled by Nautilus' simulated venue. The catalog stores the minute bars
//! and funding rates of BTC-USDT-SWAP.OKX as `fill-catalog` stores them (`ts_event` = `ts_init` =
//! the funding time, an 8-hour interval), so the backtest replays the funding with the bars.
//!
//! A recorder on the hour bars buys 2 contracts (0.02 BTC) and holds them across two funding times.
//! At each, the venue makes a `FundingSettlement` on the position, priced at the last bar's close
//! (no mark price: the mid of the book the bars set). The settlement adjusts the position and the
//! balance. A long pays a positive rate and receives a negative one.
//!
//! The price is 100,000 before noon and 101,000 from noon, and the run starts at 06:30. The funding
//! rate is +0.01% at 08:00 and -0.02% at 16:00. The first hour bar closes at 07:00 and is built 5 s
//! after, as live, so the buy fills there.

use std::path::Path;

use helix_trader::backtest::{Backtest, Commission};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, FundingRateUpdate},
    enums::{OrderSide, PositionAdjustmentType},
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rstest::rstest;
use rust_decimal::Decimal;

use crate::common::{recorder::Recorder, ts};

const INSTRUMENT_ID: &str = "BTC-USDT-SWAP.OKX";
const MINUTE_BARS: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";
const HOUR_BARS: &str = "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

/// Returns the time `hh:mm` UTC on Wednesday 7 January 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-01-07T{hh_mm}:00Z"))
}

/// Returns BTC-USDT-SWAP as OKX defines it.
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

fn backtest(dir: &Path, end: &str) -> Backtest {
    Backtest {
        catalog: dir.to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start: at("06:30"),
        end: at(end),
        ticks: vec![],
        custom: vec![],
        commission: Commission::Rate(Decimal::ZERO),
        latency: None,
        pairs: vec![],
        balance: Money::from("10000 USDT"),
        logging: LoggerConfig::default(),
    }
}

#[rstest]
fn a_held_swap_pays_and_receives_funding_on_its_notional_at_each_funding_time() {
    let dir = tempfile::tempdir().unwrap();
    catalog(dir.path(), "17:00", "17:00");
    let (recorder, record) = Recorder::trading(
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
            UnixNanos::from(at("07:00") + SignedDuration::from_secs(5))
        )]
    );

    // The long's notional is 2,000 USDT at 08:00 and 2,020 at 16:00
    let settled: Vec<_> = record
        .adjustments
        .iter()
        .map(|adjustment| {
            assert_eq!(adjustment.adjustment_type, PositionAdjustmentType::Funding);
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
