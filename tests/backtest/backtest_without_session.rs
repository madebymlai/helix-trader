//! No session, as on a market open around the clock: every bar counts,
//! the one spanning midnight included. One node per thread, so one run
//! per test.

use crate::common::{self, Exits, stepped_quotes, ts};
use jiff::{SignedDuration, Timestamp};

/// Quoted from Wednesday 7 January 2026, 19:00 UTC, to Thursday 02:00,
/// all outside Xetra's session; the hour bars close at their level + 0.60:
/// up to 20:00: 600.60; 21:00: 610.60; 22:00: 600.60 (initialized); 23:00:
/// 620.60; Thursday 00:00, from 23:00 across midnight: 600.60; 01:00:
/// 610.60; 02:00: 615.60.
fn level(close: Timestamp) -> Option<f64> {
    let hour = |at: &str| ts(&format!("2026-01-{at}:00:00Z"));
    Some(match close {
        c if c <= hour("07T20") => 600.0,
        c if c == hour("07T21") => 610.0,
        c if c == hour("07T22") => 600.0,
        c if c == hour("07T23") => 620.0,
        c if c == hour("08T00") => 600.0,
        c if c == hour("08T01") => 610.0,
        _ => 615.0,
    })
}

#[test]
fn without_a_session_every_bar_counts_across_midnight() {
    let (from, start, end) = (
        ts("2026-01-07T19:00:00Z"),
        ts("2026-01-07T20:00:00Z"),
        ts("2026-01-08T02:00:00Z"),
    );
    let catalog = common::catalog(&stepped_quotes(from, end, level), from, end);
    let exits = Exits::after(SignedDuration::from_mins(30));

    let result = common::backtest(catalog.path(), start, end, "0 EUR")
        .run(common::ema_cross(1, 2, None, exits))
        .unwrap()
        .result;

    // From 20:00 the run replays the bars closing 21:00 and 22:00: the
    // crossover, initialized at 22:00, is short: 600.60 below the slow
    // EMA of 605.60. Each trade is held 30 minutes and closed at
    // the 30th minute quote of the next bar:
    //
    // - 23:00: 620.60 above 605.60, cross up: buy 10 at 620.65, the EMA
    //   going to 615.60; closed at 23:30 at the bid 600.25: −204.00;
    // - 00:00: 600.60 below 615.60, cross down: sell 10 at 600.55, the EMA
    //   going to 605.60; closed at 00:30 at the ask 610.35: −98.00;
    // - 01:00: 610.60 above 605.60, cross up: buy 10 at 610.65; closed at
    //   01:30 at the bid 615.25: +46.00.
    //
    // Had the bar closing at midnight been dropped, as a session of 00:00
    // to 23:59 drops it, 01:00 would be below the EMA of 615.60, a cross
    // down: a sell instead of a buy.
    assert_eq!(result.total_orders, 12);
    assert_eq!(common::balance(&result), "9744.00 EUR");
}
