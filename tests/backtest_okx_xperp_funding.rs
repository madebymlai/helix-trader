//! Funding on an OKX X-Perp, settled by Nautilus' simulated venue: the
//! catalog stores BTC-USD_UM_XPERP-310404.OKX's minute bars and its funding
//! rates as `fill-catalog` stores those of OKX's API (`ts_event` =
//! `ts_init` = the funding time, an 8-hour interval), so the backtest
//! replays the funding with the bars. A recorder on the hour bars buys
//! 1,000 contracts (0.1 BTC) on the first one and holds them across two
//! funding times, at each of which the venue settles funding on the
//! position in USD, priced at the last bar's close. OKX keeps about 3
//! months of an X-Perp's funding, so a run starting before the stored
//! funding is refused. One node per process, so one run per file; the
//! refused window builds no node.

mod common;

use std::path::Path;

use common::{recorder::Recorder, ts};
use helix_trader::backtest::{Backtest, commission_rate, venue_oms_type};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, FundingRateUpdate},
    enums::{OrderSide, PositionAdjustmentType},
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoFuture, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

const INSTRUMENT_ID: &str = "BTC-USD_UM_XPERP-310404.OKX";
const MINUTE_BARS: &str = "BTC-USD_UM_XPERP-310404.OKX-1-MINUTE-LAST-EXTERNAL";
const HOUR_BARS: &str = "BTC-USD_UM_XPERP-310404.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL";

/// `hh:mm` UTC on Wednesday 7 October 2026.
fn at(hh_mm: &str) -> Timestamp {
    ts(&format!("2026-10-07T{hh_mm}:00Z"))
}

/// BTC-USD_UM_XPERP-310404 as Nautilus' OKX adapter parses OKX's
/// definition, with initial and maintenance margins of 1.
fn instrument() -> InstrumentAny {
    InstrumentAny::CryptoFuture(
        CryptoFuture::builder()
            .instrument_id(InstrumentId::from(INSTRUMENT_ID))
            .raw_symbol(Symbol::from("BTC-USD_UM_XPERP-310404"))
            .underlying(Currency::get_or_create_crypto("BTC-USD"))
            .quote_currency(Currency::USD())
            .settlement_currency(Currency::USD())
            .is_inverse(false)
            .activation_ns(UnixNanos::from(ts("2026-03-30T04:30:00.428Z")))
            .expiration_ns(UnixNanos::from(ts("2031-04-04T08:00:00Z")))
            .price_precision(1)
            .size_precision(0)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from("1"))
            .multiplier(Quantity::from("0.0001"))
            .lot_size(Quantity::from("1"))
            .min_quantity(Quantity::from("1"))
            .margin_init(Decimal::ONE)
            .margin_maint(Decimal::ONE)
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The flat minute bars opening in `[from, to)`: at 100,000 before noon,
/// 101,000 from noon, 5,000 contracts, stamped at the open (`ts_event`)
/// and the close (`ts_init`).
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
            Quantity::from("5000"),
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
/// 17:00]` and its funding at 08:00 (+0.01%) and 16:00 (-0.02%) stored over
/// `[funding_from, 17:00]`.
fn catalog(dir: &Path, funding_from: Timestamp) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    let to = Some(UnixNanos::from(at("17:00")));
    catalog
        .write_to_parquet(
            &minute_bars(at("06:00"), at("17:00")),
            Some(UnixNanos::from(at("06:00"))),
            to,
            None,
        )
        .unwrap();
    catalog
        .write_to_parquet(
            &[
                funding(at("08:00"), "0.0001"),
                funding(at("16:00"), "-0.0002"),
            ],
            Some(UnixNanos::from(funding_from)),
            to,
            None,
        )
        .unwrap();
}

/// A run on the hour bars from 07:00 to 17:00, no commission.
fn backtest(dir: &Path) -> Backtest {
    Backtest {
        catalog: dir.to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start: at("07:00"),
        end: at("17:00"),
        warmup: SignedDuration::ZERO,
        fee_model: commission_rate(Decimal::ZERO).unwrap(),
        balance: Money::from("100000 USD"),
        leverage: Decimal::ONE,
        venue_oms: venue_oms_type,
        logging: LoggerConfig::default(),
    }
}

#[test]
fn a_held_x_perp_pays_and_receives_funding_in_usd_at_each_funding_time() {
    let dir = tempfile::tempdir().unwrap();
    catalog(dir.path(), at("06:00"));
    let (recorder, record) = Recorder::trading_without_warmup(
        BarType::from(HOUR_BARS),
        vec![(OrderSide::Buy, Quantity::from("1000"))],
    );

    let outcome = backtest(dir.path()).run(recorder).unwrap();

    let record = record.borrow();
    assert_eq!(
        record.fills,
        [(
            OrderSide::Buy,
            Price::from("100000.0"),
            UnixNanos::from(at("07:00"))
        )]
    );
    // 0.1 BTC long: at 08:00, worth 10,000 USD, it pays 0.01% (1.00); at
    // 16:00, worth 10,100 USD, it receives 0.02% (2.02).
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
            (at("08:00"), Money::from("-1.00 USD")),
            (at("16:00"), Money::from("2.02 USD")),
        ]
    );
    assert_eq!(
        outcome.result.summary["account.OKX.balance.USD.total"],
        "100001.02 USD"
    );
}

#[test]
fn a_run_starting_before_the_stored_funding_is_refused_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    catalog(dir.path(), at("07:30"));
    let (recorder, _) = Recorder::without_warmup(BarType::from(HOUR_BARS));

    let err = backtest(dir.path()).run(recorder).unwrap_err().to_string();

    assert!(
        err.contains(&format!("funding_rates of {INSTRUMENT_ID}")),
        "{err}"
    );
    let nanosecond = SignedDuration::from_nanos(1);
    let missing = format!(
        "[{}, {}] is missing",
        at("07:00") - nanosecond,
        at("07:30") - nanosecond
    );
    assert!(err.contains(&missing), "{err}");
}
