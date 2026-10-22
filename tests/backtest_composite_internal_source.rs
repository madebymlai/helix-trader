//! A composite bar type built from internal source bars has no warmup to
//! request: its source bars are built, not stored. The run fails at the
//! strategy's start, naming the source bars. One node per process, so one
//! run per file.

mod common;

use common::{recorder::Recorder, ts};
use jiff::SignedDuration;
use nautilus_model::data::BarType;

const HOUR_BARS: &str = "SXR8.IBIS-1-HOUR-MID-INTERNAL@1-MINUTE-INTERNAL";

#[test]
fn a_composite_bar_type_of_internal_source_bars_has_no_warmup() {
    let (from, to) = (ts("2026-01-07T08:00:00Z"), ts("2026-01-07T12:00:00Z"));
    let dir = common::catalog(&common::minute_quotes(from, to, |_| Some(600.0)), from, to);
    let warmup = SignedDuration::from_hours(2);
    let (recorder, _) = Recorder::new(BarType::from(HOUR_BARS), warmup, None);
    let mut backtest =
        common::backtest(dir.path(), ts("2026-01-07T10:30:00Z"), to, warmup, "0 EUR");
    backtest.bar_types = vec![BarType::from(HOUR_BARS)];

    let err = format!("{:#}", backtest.run(recorder).unwrap_err());

    assert!(err.contains("SXR8.IBIS-1-MINUTE-MID-INTERNAL"), "{err}");
}
