//! A backtest the catalog cannot serve is refused before the node is built:
//! a warmup reaching before the stored history would get no answer and the
//! run would go on with cold indicators; without quotes no order could
//! fill.

mod common;

use common::{BAR_TYPE, BARS_BEFORE_START, INSTRUMENT_ID};
use jiff::SignedDuration;

#[test]
fn a_warmup_longer_than_the_stored_history_is_refused_naming_the_bar_type() {
    let catalog = common::catalog();
    let mut backtest = common::backtest(catalog.path(), "3 EUR");
    backtest.warmup = SignedDuration::from_hours(BARS_BEFORE_START + 24);

    let err = backtest.run(common::ema_cross()).unwrap_err().to_string();

    assert!(err.contains(BAR_TYPE), "{err}");
    // The engine starts 1 ns before the run start, the warmup before it.
    let warmup_start = backtest.start - SignedDuration::from_nanos(1) - backtest.warmup;
    assert!(err.contains(&format!("[{warmup_start}, ")), "{err}");
}

#[test]
fn a_catalog_without_quotes_is_refused_naming_the_instrument() {
    let catalog = common::catalog_without_quotes();
    let backtest = common::backtest(catalog.path(), "3 EUR");

    let err = backtest.run(common::ema_cross()).unwrap_err().to_string();

    assert!(err.contains(&format!("quotes of {INSTRUMENT_ID}")), "{err}");
}
