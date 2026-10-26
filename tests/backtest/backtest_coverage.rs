//! A backtest the catalog cannot serve is refused before the node is built:
//! a warmup reaching before the stored quotes would get no answer and the
//! run would go on with cold indicators; without quotes no bar is built and
//! no order could fill.

use crate::common::{self, INSTRUMENT_ID, wave};
use jiff::SignedDuration;

#[test]
fn a_warmup_longer_than_the_stored_quotes_is_refused_naming_the_missing_interval() {
    let catalog = wave::catalog();
    let mut backtest = wave::backtest(catalog.path(), "3 EUR");
    backtest.warmup = SignedDuration::from_hours(96);

    let err = backtest.run(wave::ema_cross()).unwrap_err().to_string();

    assert!(err.contains(&format!("quotes of {INSTRUMENT_ID}")), "{err}");
    // The engine starts 1 ns before the run start, the warmup before it;
    // the quotes are stored from Monday 5 January 2026, midnight.
    let warmup_start = backtest.start - SignedDuration::from_nanos(1) - backtest.warmup;
    let missing = format!("[{warmup_start}, 2026-01-04T23:59:59.999999999Z] is missing");
    assert!(err.contains(&missing), "{err}");
}

#[test]
fn a_catalog_without_quotes_is_refused_naming_the_instrument() {
    let catalog = common::catalog_without_quotes();
    let backtest = wave::backtest(catalog.path(), "3 EUR");

    let err = backtest.run(wave::ema_cross()).unwrap_err().to_string();

    assert!(err.contains(&format!("quotes of {INSTRUMENT_ID}")), "{err}");
}
