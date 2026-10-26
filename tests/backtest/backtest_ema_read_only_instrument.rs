//! An instrument the EMA crossover reads but does not trade: its bars are
//! subscribed and warmed up, and their crosses send no order. SXR8.IBIS
//! traded on `common::trades`' time exit, EUNL.IBIS read, its levels
//! turning every hour. One node per thread, so one run per test.

use crate::common::{self, INSTRUMENT_ID, MID_BAR_TYPE, trades, ts};
use helix_trader::{
    backtest::Backtest,
    strategy::{
        BarsConfig,
        ema::{EmaCross, EmaCrossConfig},
    },
};
use nautilus_model::{
    data::{BarType, QuoteTick},
    identifiers::{InstrumentId, Symbol},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

const READ: &str = "EUNL.IBIS";

/// EUNL.IBIS, an ETF on the same venue.
fn read_instrument() -> InstrumentAny {
    let InstrumentAny::Equity(mut equity) = common::instrument() else {
        unreachable!("the fixture's instrument is an equity")
    };
    equity.id = InstrumentId::from(READ);
    equity.raw_symbol = Symbol::from("EUNL");
    equity.isin = Some("IE00B4L5Y983".into());
    InstrumentAny::Equity(equity)
}

/// EUNL's quotes over the window of the trades: its hour bars alternate
/// between 90.60 and 110.60, from 10:00, so that from its first warm bar
/// each one crosses its slow EMA.
fn read_quotes(levels: &[f64]) -> Vec<QuoteTick> {
    let quotes = common::stepped_quotes(trades::at("09:00"), trades::end(levels), |close| {
        let hours = close.duration_since(ts("2026-01-07T00:00:00Z")).as_hours();
        Some(if hours % 2 == 0 { 90.0 } else { 110.0 })
    });
    quotes
        .into_iter()
        .map(|quote| QuoteTick {
            instrument_id: InstrumentId::from(READ),
            ..quote
        })
        .collect()
}

#[test]
fn an_instrument_read_but_not_traded_sends_no_order() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let store = ParquetDataCatalog::new(catalog.path(), None, None, None, None);
    let read = read_instrument();
    store.write_instruments(vec![read.clone()]).unwrap();
    store
        .write_to_parquet(
            &read_quotes(&levels),
            Some(trades::at("09:00").into()),
            Some(trades::end(&levels).into()),
            None,
        )
        .unwrap();
    let read_bars = BarType::from(format!("{READ}-1-HOUR-MID-INTERNAL").as_str());
    let base = common::ema_config(
        1,
        2,
        trades::WARMUP,
        Some(common::session()),
        trades::time_exit(),
    );
    let config = EmaCrossConfig {
        bars: BarsConfig {
            instrument_ids: vec![InstrumentId::from(INSTRUMENT_ID), read.id()],
            ..base.bars.clone()
        },
        ..base
    };
    let backtest = Backtest {
        bar_types: vec![BarType::from(MID_BAR_TYPE), read_bars],
        ..trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
    };

    let result = backtest.run(EmaCross::new(config).unwrap()).unwrap().result;

    // Only SXR8's trade: its 4 orders and balance (backtest_warmup.rs).
    // Each EUNL bar from 13:00 crosses, so trading it would have added a
    // bracket an hour.
    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), trades::TIME_EXIT_BALANCE);
}
