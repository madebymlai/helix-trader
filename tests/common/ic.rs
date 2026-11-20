//! IC Markets' gold, XAUUSD.ICMARKETS, and its FX pairs on tick quotes as `fill-catalog` stores
//! them.
//!
//! The gold is a `Cfd` in ounces quoted in USD, with IC's demo margin (1:30) and a 10,000 oz
//! maximum order. Quotes are stamped at their tick's millisecond, at irregular times, both sides
//! sized 10,000.

use std::path::Path;

use helix_trader::{
    backtest::{Backtest, Commission},
    catalog::fill_catalog,
    strategy::requirements::{CatalogSeries, TickSeries},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, Data, NautilusDataType, QuoteTick},
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{Cfd, Instrument, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
use tempfile::TempDir;

use super::ts;

pub(crate) const INSTRUMENT_ID: &str = "XAUUSD.ICMARKETS";
pub(crate) const HOUR_BARS: &str = "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL";

pub(crate) const EURUSD: &str = "EURUSD.ICMARKETS";

pub(crate) const USDJPY: &str = "USDJPY.ICMARKETS";

/// `hh:mm:ss.fff` UTC on Wednesday 21 October 2026.
pub(crate) fn at(time: &str) -> Timestamp {
    ts(&format!("2026-10-21T{time}Z"))
}

/// 1 / 30: the account's 1:30 under the symbol's 1:200 (`src/venues/ic/README.md`).
pub(crate) fn margin() -> Decimal {
    Decimal::ONE / Decimal::from(30)
}

/// XAUUSD as IC defines it (`crates/ctrader/README.md`).
pub(crate) fn instrument() -> InstrumentAny {
    InstrumentAny::Cfd(
        Cfd::builder()
            .instrument_id(InstrumentId::from(INSTRUMENT_ID))
            .raw_symbol(Symbol::from("XAUUSD"))
            .asset_class(AssetClass::Commodity)
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

/// A forex symbol as the cTrader adapter defines it (`crates/ctrader/README.md`), at IC's margin.
/// Sizes, by the thousand, are this fixture's.
pub(crate) fn pair(
    id: &str,
    base: Currency,
    quote: Currency,
    price_precision: u8,
) -> InstrumentAny {
    let instrument_id = InstrumentId::from(id);
    InstrumentAny::Cfd(
        Cfd::builder()
            .instrument_id(instrument_id)
            .raw_symbol(instrument_id.symbol)
            .asset_class(AssetClass::FX)
            .base_currency(base)
            .quote_currency(quote)
            .price_precision(price_precision)
            .size_precision(0)
            .price_increment(
                Price::from_decimal_dp(
                    Decimal::new(1, u32::from(price_precision)),
                    price_precision,
                )
                .unwrap(),
            )
            .size_increment(Quantity::from(1_000))
            .lot_size(Quantity::from(100_000))
            .min_quantity(Quantity::from(1_000))
            .max_quantity(Quantity::from(10_000_000))
            .margin_init(margin())
            .margin_maint(margin())
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// One quote a minute at :15, 08:00 to 12:00.
pub(crate) fn pair_quotes(id: &str, bid: &str, ask: &str) -> Vec<QuoteTick> {
    pair_quotes_over(id, bid, ask, at("08:00:00"), at("12:00:00"))
}

/// One quote a minute at :15 from `from` to `to`.
pub(crate) fn pair_quotes_over(
    id: &str,
    bid: &str,
    ask: &str,
    from: Timestamp,
    to: Timestamp,
) -> Vec<QuoteTick> {
    let size = Quantity::from(10_000_000);
    let mut quotes = vec![];
    let mut time = from + SignedDuration::from_secs(15);
    while time < to {
        quotes.push(QuoteTick::new(
            InstrumentId::from(id),
            Price::from(bid),
            Price::from(ask),
            size,
            size,
            time.into(),
            time.into(),
        ));
        time += SignedDuration::from_mins(1);
    }
    quotes
}

pub(crate) fn store_pair(dir: &Path, pair: InstrumentAny, quotes: Vec<QuoteTick>) {
    store_defined(dir, pair, quotes, at("08:00:00"), at("12:00:00"));
}

pub(crate) fn quote(time: Timestamp, bid: &str, ask: &str) -> QuoteTick {
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

/// Bottom price of each even hour of `edge_quotes`, and the rise over it.
pub(crate) const EDGE_BOTTOM: f64 = 4000.0;
pub(crate) const EDGE_RISE: f64 = 4.0;

/// Quotes over `[from, to)` every 45 s, ask 0.20 above bid: the bid rises from 4000.00 by 0.05 a
/// quote to 4003.95 on even hours, and falls from 4004.00 to 4000.05 on odd hours.
pub(crate) fn edge_quotes(from: Timestamp, to: Timestamp) -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = from;
    while time < to {
        let seconds = time.as_second();
        let step = (seconds % 3600 / 45) as f64 * 0.05;
        let bid = if seconds / 3600 % 2 == 0 {
            EDGE_BOTTOM + step
        } else {
            EDGE_BOTTOM + EDGE_RISE - step
        };
        quotes.push(quote(
            time,
            &format!("{bid:.2}"),
            &format!("{:.2}", bid + 0.2),
        ));
        time += SignedDuration::from_secs(45);
    }
    quotes
}

/// Quotes 08:00 to 12:00 at :07.250 and :37.750, bid stepping 4000.00 to 4000.90 by the minute,
/// ask 0.20 above, the last quote of the hours closing 10:00 and 11:00 set apart.
pub(crate) fn quotes() -> Vec<QuoteTick> {
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

pub(crate) fn catalog() -> TempDir {
    catalog_of(quotes(), at("08:00:00"), at("12:00:00"))
}

pub(crate) fn catalog_of(quotes: Vec<QuoteTick>, from: Timestamp, to: Timestamp) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    store(dir.path(), quotes, from, to);
    dir
}

/// `defined` replaces the gold's definition.
pub(crate) fn catalog_defining(
    defined: InstrumentAny,
    quotes: Vec<QuoteTick>,
    from: Timestamp,
    to: Timestamp,
) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    store_defined(dir.path(), defined, quotes, from, to);
    dir
}

pub(crate) fn store(dir: &Path, quotes: Vec<QuoteTick>, from: Timestamp, to: Timestamp) {
    store_defined(dir, instrument(), quotes, from, to);
}

pub(crate) fn store_defined(
    dir: &Path,
    defined: InstrumentAny,
    quotes: Vec<QuoteTick>,
    from: Timestamp,
    to: Timestamp,
) {
    let mut catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let series = [CatalogSeries::Ticks(TickSeries {
        data: NautilusDataType::QuoteTick,
        instrument_id: defined.id(),
    })];
    let data: Vec<Data> = quotes.into_iter().map(Data::Quote).collect();
    let fill = fill_catalog(
        &mut catalog,
        vec![defined],
        &series,
        from,
        to,
        async |_, _, _| Ok(data.clone()),
    );
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fill)
        .unwrap();
}

/// Hour bars over `[09:00, 12:00]` on a hedging venue (`venues::oms_type`), charged IC's Raw
/// commission of 30 USD per million traded. IC's leverage is in the instrument's margin.
pub(crate) fn backtest(catalog: &TempDir) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![BarType::from(HOUR_BARS)],
        start: at("09:00:00"),
        end: at("12:00:00"),
        ticks: vec![],
        custom: vec![],
        commission: Commission::Rate(Decimal::new(3, 5)),
        latency: None,
        pairs: vec![],
        balance: Money::from("2000000 USD"),
        logging: LoggerConfig::default(),
    }
}
