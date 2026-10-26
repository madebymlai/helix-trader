//! No session, as on a market open around the clock: every bar counts,
//! the one spanning midnight included. One node per process, so one run
//! per file.

mod common;

use common::{stepped_quotes, ts};
use jiff::{SignedDuration, Timestamp};

/// Quoted from Wednesday 7 January 2026, 20:00 UTC, to Thursday 01:30,
/// all outside Xetra's session; the hour bars close at their level + 0.60:
/// up to 22:00 (the warmup): 600.60; 23:00: 620.60; Thursday 00:00, from
/// 23:00 across midnight: 580.60; from 01:00: 600.60.
fn level(close: Timestamp) -> Option<f64> {
    let hour = |at: &str| ts(&format!("2026-01-{at}:00:00Z"));
    Some(match close {
        c if c <= hour("07T22") => 600.0,
        c if c == hour("07T23") => 620.0,
        c if c == hour("08T00") => 580.0,
        _ => 600.0,
    })
}

#[test]
fn without_a_session_every_bar_counts_across_midnight() {
    let (from, start, end) = (
        ts("2026-01-07T20:00:00Z"),
        ts("2026-01-07T22:30:00Z"),
        ts("2026-01-08T01:30:00Z"),
    );
    let warmup = SignedDuration::from_hours(2);
    let catalog = common::catalog(&stepped_quotes(from, end, level), from, end);

    let result = common::backtest(catalog.path(), start, end, warmup, "0 EUR")
        .run(common::ema_cross(1, 2, warmup, None))
        .unwrap()
        .result;

    // The slow EMA after the warmup, the partial 21:00 bar dropped, is
    // 600.60. 23:00: 620.60 above it, long: buy 10 at 620.65, the EMA going
    // to 613.93. 00:00: 580.60 below, short: close the long (sell 10) and
    // sell 10 at 580.55, the EMA going to 591.71. 01:00: 600.60 above,
    // long: close the short (buy 10) and buy 10 at 600.65. The 10 shares
    // bought lose 40.10 each, the 10 sold short 20.10. Had the bar closing
    // at midnight been dropped, as a session of 00:00 to 23:59 drops it,
    // 01:00 would be below the EMA of 613.93: a single crossing, three
    // orders.
    assert_eq!(result.total_orders, 5);
    assert_eq!(common::balance(&result), "9398.00 EUR");
}
