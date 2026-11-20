//! Tests that a run with no session, as on a market open around the clock, counts every bar,
//! including the one closing at midnight.
//!
//! Quotes run from Wednesday 7 January 2026, 19:00 UTC, to Thursday 02:00, all outside Xetra's
//! session. The run starts at 20:00. The crossover initializes short at 22:00, then trades three
//! brackets of 10 units: crosses up at 23:00, down at 00:00 and up at 01:00. The first two end at
//! their stops, -206.90 and -95.10, and the last ends long, so 9 orders and 9698.00 EUR.
//!
//! Dropping the bar closing at midnight, as a session of 00:00 to 23:59 does, would leave 01:00
//! below the EMA of 615.60, which gives a sell instead of a buy.

use jiff::Timestamp;
use rstest::rstest;

use crate::common::{self, stepped_quotes, ts};

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

    assert_eq!(result.total_orders, 9);
    assert_eq!(common::balance(&result), "9698.00 EUR");
}
