//! A warmup of more bars than Nautilus' cache holds by default (10,000 per
//! bar type): the bars a warmup request builds reach the strategy only
//! through the cache, so the run sizes it to hold them all, or refuses to
//! run when the cache cannot. One node per thread, so one run per test:
//! the refused run builds no node.

use crate::common::{self, minute_quotes, recorder::Recorder, ts};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::data::BarType;

#[test]
fn a_warmup_longer_than_the_default_cache_gets_every_bar() {
    // 1-minute MID bars over eight days of warmup: 11,519 bars. A quote a
    // minute from Monday 5 January 2026 to the run's end, 01:30 on Tuesday
    // 13 January; the run starts at 00:30.
    let warmup = SignedDuration::from_hours(8 * 24);
    let (first, start, end) = (
        ts("2026-01-05T00:00:00Z"),
        ts("2026-01-13T00:30:00Z"),
        ts("2026-01-13T01:30:00Z"),
    );
    let quotes = minute_quotes(first, end, |_| Some(600.0));
    let catalog = common::catalog(&quotes, first, end);
    let bar_type = BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL");
    let (recorder, record) = Recorder::new(bar_type, warmup, None);
    let mut backtest = common::backtest(catalog.path(), start, end, warmup, "0 EUR");
    backtest.bar_types = vec![bar_type];

    backtest.run(recorder).unwrap();

    // The warmup starts at the quote closing 00:30 on 5 January: the bar it
    // alone makes is partial, dropped; the bar closing 00:30 on 13 January,
    // closed by the quote of the run start, comes in the run. So the
    // warmup bars close every minute from 00:31 on 5 January to 00:29 on
    // 13 January.
    let record = record.borrow();
    let closes: Vec<Timestamp> = record.warmup.iter().map(|b| b.ts_event.into()).collect();
    assert_eq!(closes.len(), 8 * 24 * 60 - 1);
    assert_eq!(closes.first(), Some(&ts("2026-01-05T00:31:00Z")));
    assert_eq!(closes.last(), Some(&ts("2026-01-13T00:29:00Z")));
}

#[test]
fn a_warmup_the_cache_cannot_hold_is_refused_before_the_run() {
    // 1-second bars over twelve days: 1,036,801 bars, beyond the
    // 1,000,000 a cache holds at most.
    let warmup = SignedDuration::from_hours(12 * 24);
    let catalog = common::catalog_without_quotes();
    let bar_type = BarType::from("SXR8.IBIS-1-SECOND-MID-INTERNAL");
    let (recorder, _) = Recorder::new(bar_type, warmup, None);
    let (start, end) = (ts("2026-01-13T00:30:00Z"), ts("2026-01-13T01:30:00Z"));
    let mut backtest = common::backtest(catalog.path(), start, end, warmup, "0 EUR");
    backtest.bar_types = vec![bar_type];

    let err = format!("{:#}", backtest.run(recorder).unwrap_err());

    for part in ["SXR8.IBIS-1-SECOND-MID-INTERNAL", "1036801 bars", "1000000"] {
        assert!(err.contains(part), "{err}");
    }
}
