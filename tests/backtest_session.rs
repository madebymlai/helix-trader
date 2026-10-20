//! The session on internal bars: the bars outside it, quoted or flat for
//! want of quotes overnight, neither move the EMAs nor trade. One node per
//! process, so one run per file.

mod common;

use common::{stepped_quotes, ts};
use jiff::{SignedDuration, Timestamp};

/// Quoted from 07:00 to 17:00 UTC each day, nothing overnight; the hour
/// bars close at their level + 0.60:
///
/// - Wednesday 7 January 2026, 10:00 to 14:00 (the warmup): 600.60;
///   15:00: 590.60; 16:00: 700.60; 17:00, out of the session: 500.60;
/// - overnight, out of the session: flat bars at 500.60, no quotes;
/// - Thursday 08:00, out of the session: 500.60; 09:00: 650.60.
fn level(close: Timestamp) -> Option<f64> {
    let (wednesday, thursday) = ("2026-01-07T", "2026-01-08T");
    let hour = |day: &str, hh: &str| ts(&format!("{day}{hh}:00:00Z"));
    match close {
        c if c <= hour(wednesday, "14") => Some(600.0),
        c if c == hour(wednesday, "15") => Some(590.0),
        c if c == hour(wednesday, "16") => Some(700.0),
        c if c == hour(wednesday, "17") => Some(500.0),
        c if c < hour(thursday, "08") => None,
        c if c == hour(thursday, "08") => Some(500.0),
        _ => Some(650.0),
    }
}

#[test]
fn bars_outside_the_session_quoted_or_flat_neither_move_the_emas_nor_trade() {
    let (from, start, end) = (
        ts("2026-01-07T08:00:00Z"),
        ts("2026-01-07T14:30:00Z"),
        ts("2026-01-08T09:30:00Z"),
    );
    let warmup = SignedDuration::from_hours(6);
    let catalog = common::catalog(&stepped_quotes(from, end, level), from, end);

    let result = common::backtest(catalog.path(), start, end, warmup, "0 EUR")
        .run(common::shortest_ema_cross(warmup))
        .unwrap()
        .result;

    // The slow EMA after the warmup is 600.60. 15:00: 590.60 below it,
    // short: sell 10 at 590.55, the EMA going to 593.93. 16:00: 700.60
    // above, long: buy 20 at 700.65, the EMA going to 665.04. Thursday
    // 09:00: 650.60 below, short: sell 20 at 650.55. The 10 shares sold
    // short lose 110.10 each, the 10 bought 50.10. Had the bars outside the
    // session counted, the slow EMA would be near 500.60 on Thursday 09:00:
    // still long, no third order.
    assert_eq!(result.total_orders, 3);
    assert_eq!(common::balance(&result), "8398.00 EUR");
}
