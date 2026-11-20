//! No session, as on a market open around the clock: every bar counts, the one spanning midnight
//! included.

use jiff::Timestamp;
use rstest::rstest;

use crate::common::{self, stepped_quotes, ts};

/// Quoted from Wednesday 7 January 2026, 19:00 UTC, to Thursday 02:00, all outside Xetra's session;
/// the hour bars close at level + 0.60.
fn level(close: Timestamp) -> f64 {
    let hour = |at: &str| ts(&format!("2026-01-{at}:00:00Z"));
    match close {
        c if c <= hour("07T20") => 600.0,
        c if c == hour("07T21") => 610.0,
        c if c == hour("07T22") => 600.0,
        c if c == hour("07T23") => 620.0,
        c if c == hour("08T00") => 600.0,
        c if c == hour("08T01") => 610.0,
        _ => 615.0,
    }
}

#[rstest]
fn without_a_session_every_bar_counts_across_midnight() {
    let (from, start, end) = (
        ts("2026-01-07T19:00:00Z"),
        ts("2026-01-07T20:00:00Z"),
        ts("2026-01-08T02:00:00Z"),
    );
    let catalog = common::catalog(
        &stepped_quotes(from, end, |close| Some(level(close))),
        from,
        end,
    );

    let result = common::backtest(catalog.path(), start, end, "0 EUR")
        .run(common::ema_cross(1, 2, None))
        .unwrap()
        .result;

    // The run replays the bars closing 21:00 and 22:00 from 20:00: the crossover, initialized at
    // 22:00, is short (600.60 below the slow EMA of 605.60). Each trade but the last ends at its
    // stop, 50 bps away, on the first quote of the next bar, which jumps past it:
    //
    // - 23:00: 620.60 above 605.60, cross up: buy 10 at 620.65, stop 617.50, EMA to 615.60;
    //   stopped at 23:01 at the bid 600.00 + 0.01 - 0.05 = 599.96: -206.90
    // - 00:00: 600.60 below 615.60, cross down: sell 10 at 600.55, stop 603.60, EMA to 605.60;
    //   stopped at 00:01 at the ask 610.00 + 0.01 + 0.05 = 610.06: -95.10
    // - 01:00: 610.60 above 605.60, cross up: buy 10 at 610.65, stop 607.55, take profit 616.71;
    //   02:00 at 615.60 touches neither, the run ends long
    //
    // Dropping the bar closing at midnight, as a session of 00:00 to 23:59 does, would leave 01:00
    // below the EMA of 615.60: a sell instead of a buy.
    assert_eq!(result.total_orders, 9);
    assert_eq!(common::balance(&result), "9698.00 EUR");
}
