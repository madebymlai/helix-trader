//! IC Markets' gold on its tick quotes: XAUUSD.ICMARKETS, a `CurrencyPair`
//! of ounces quoted in USD as `fill-catalog` loads it from IC (IC's margin
//! on the demo, 1:30, at most 10,000 oz an order), and quotes as fill-catalog stores them:
//! stamped at their tick's millisecond, at irregular times, both sides
//! sized 10,000 (the instrument's maximum order), written through
//! `fill_catalog`.

use helix_trader::{
    backtest::{Backtest, commission_rate, venue_oms_type},
    data::fill_catalog,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, Data, NautilusDataType, QuoteTick},
    identifiers::{InstrumentId, Symbol},
    instruments::{CurrencyPair, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
use tempfile::TempDir;

use super::ts;

pub const INSTRUMENT_ID: &str = "XAUUSD.ICMARKETS";
pub const HOUR_BARS: &str = "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL";

/// `hh:mm:ss.fff` UTC on Wednesday 21 October 2026.
pub fn at(time: &str) -> Timestamp {
    ts(&format!("2026-10-21T{time}Z"))
}

/// IC's margin of XAUUSD on the demo account: 1 / 30, the account's 1:30
/// under the symbol's 1:200 (`src/data/ic/README.md`, "Margins").
pub fn margin() -> Decimal {
    Decimal::ONE / Decimal::from(30)
}

/// XAUUSD as IC defines it (`src/data/ic/README.md`, "Currency pairs").
pub fn instrument() -> InstrumentAny {
    InstrumentAny::CurrencyPair(
        CurrencyPair::builder()
            .instrument_id(InstrumentId::from(INSTRUMENT_ID))
            .raw_symbol(Symbol::from("XAUUSD"))
            .base_currency(Currency::from("XAU"))
            .quote_currency(Currency::USD())
            .price_precision(2)
            .size_precision(0)
            .price_increment(Price::from("0.01"))
            .size_increment(Quantity::from(1))
            .lot_size(Quantity::from(100))
            .min_quantity(Quantity::from(1))
            .max_quantity(Quantity::from(10_000))
            .margin_init(margin())
            .margin_maint(margin())
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// An IC quote at `time`, sized as IC's fill sizes XAUUSD's.
pub fn quote(time: Timestamp, bid: &str, ask: &str) -> QuoteTick {
    let size = Quantity::from(10_000);
    QuoteTick::new(
        InstrumentId::from(INSTRUMENT_ID),
        Price::from(bid),
        Price::from(ask),
        size,
        size,
        time.into(),
        time.into(),
    )
}

/// Quotes from 08:00 to 12:00, two a minute at :07.250 and :37.750, the
/// bid stepping through 4000.00 to 4000.90 by the minute, the ask 0.20
/// above; the last of the hours closing 10:00 and 11:00 set apart.
pub fn quotes() -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = at("08:00:07.250");
    while time < at("12:00:00") {
        let bid = Decimal::new(400_000 + 10 * (time.as_second() / 60 % 10), 2);
        let ask = bid + Decimal::new(20, 2);
        quotes.push(match time {
            t if t == at("09:59:37.750") => quote(t, "4005.90", "4006.10"),
            t if t == at("10:59:37.750") => quote(t, "4011.90", "4012.15"),
            t => quote(t, &bid.to_string(), &ask.to_string()),
        });
        let step = if time.subsec_millisecond() == 250 {
            30_500
        } else {
            29_500
        };
        time += SignedDuration::from_millis(step);
    }
    quotes
}

/// A catalog of the instrument and its quotes from 08:00 to 12:00, stored
/// over that window as fill-catalog's node writes them back, written
/// through `fill_catalog`.
pub fn catalog() -> TempDir {
    catalog_of(quotes(), at("08:00:00"), at("12:00:00"))
}

/// A catalog of the instrument and `quotes`, stored over `[from, to]` as
/// fill-catalog's node writes them back, written through `fill_catalog`.
pub fn catalog_of(quotes: Vec<QuoteTick>, from: Timestamp, to: Timestamp) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let series = [(NautilusDataType::QuoteTick, INSTRUMENT_ID.to_owned())];
    let data: Vec<Data> = quotes.into_iter().map(Data::Quote).collect();
    let fill = fill_catalog(
        &mut catalog,
        vec![instrument()],
        &series,
        from,
        to,
        async |_, _, _, _| Ok(data.clone()),
    );
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();
    dir
}

/// A backtest of the hour bars over `[09:00, 12:00]` on the venue as IC
/// keeps positions (`venue_oms_type`: hedging), charged IC's Raw
/// commission of 30 USD per million USD traded, unleveraged: IC's leverage
/// is in the instrument's margin.
pub fn backtest(catalog: &TempDir) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start: at("09:00:00"),
        end: at("12:00:00"),
        ticks: vec![],
        fee_model: commission_rate(Decimal::new(3, 5)).unwrap(),
        balance: Money::from("2000000 USD"),
        leverage: Decimal::ONE,
        venue_oms: venue_oms_type,
        logging: LoggerConfig::default(),
    }
}
