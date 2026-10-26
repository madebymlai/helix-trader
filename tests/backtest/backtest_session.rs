//! The session on internal bars: the bars outside it neither move the EMAs
//! nor trade; overnight, without quotes, there are no bars at all. One node
//! per thread, so one run per test.

use crate::common::{self, Exits, stepped_quotes, ts};
use jiff::{SignedDuration, Timestamp};

/// Quoted from 07:00 to 17:00 UTC each day, nothing overnight; the hour
/// bars close at their level + 0.60:
///
/// - Wednesday 7 January 2026, 09:00 to 13:00: 600.60; 14:00: 590.60 (the
///   warmup); 15:00: 610.60; 16:00: 600.60; 17:00, out of the session:
///   620.60;
/// - overnight, out of the session: no quotes, so no bars;
/// - Thursday 08:00, out of the session: 640.60; 09:00: 610.60; 10:00:
///   615.60.
fn level(close: Timestamp) -> Option<f64> {
    let (wednesday, thursday) = ("2026-01-07T", "2026-01-08T");
    let hour = |day: &str, hh: &str| ts(&format!("{day}{hh}:00:00Z"));
    match close {
        c if c <= hour(wednesday, "13") => Some(600.0),
        c if c == hour(wednesday, "14") => Some(590.0),
        c if c == hour(wednesday, "15") => Some(610.0),
        c if c == hour(wednesday, "16") => Some(600.0),
        c if c == hour(wednesday, "17") => Some(620.0),
        c if c < hour(thursday, "08") => None,
        c if c == hour(thursday, "08") => Some(640.0),
        c if c == hour(thursday, "09") => Some(610.0),
        _ => Some(615.0),
    }
}

#[test]
fn bars_outside_the_session_neither_move_the_emas_nor_trade() {
    let (from, start, end) = (
        ts("2026-01-07T08:00:00Z"),
        ts("2026-01-07T14:30:00Z"),
        ts("2026-01-08T10:00:00Z"),
    );
    let warmup = SignedDuration::from_hours(6);
    let catalog = common::catalog(&stepped_quotes(from, end, level), from, end);
    let exits = Exits::after(SignedDuration::from_mins(30));

    let result = common::backtest(catalog.path(), start, end, warmup, "0 EUR")
        .run(common::shortest_ema_cross(warmup, exits))
        .unwrap()
        .result;

    // The warmup, the partial 09:00 bar dropped, ends short, 590.60 below
    // the slow EMA of 600.60, which goes to 593.93. Each trade is held 30
    // minutes and closed at the 30th minute quote of the next bar:
    //
    // - 15:00: 610.60 above 593.93, cross up: buy 10 at 610.65, the EMA
    //   going to 605.04; closed at 15:30 at the bid 600.25: −104.00;
    // - 16:00: 600.60 below 605.04, cross down: sell 10 at 600.55, the EMA
    //   going to 602.08; closed at 16:30 at the ask 620.35 (the 17:00 bar,
    //   out of the session, still quoted): −198.00;
    // - Thursday 09:00: 610.60 above 602.08, cross up: buy 10 at 610.65;
    //   closed at 09:30 at the bid 615.25: +46.00.
    //
    // Three trades of 4 orders. Had the bars outside the session counted,
    // 17:00 (620.60) would cross up, Thursday 08:00 (640.60) stay long and
    // 09:00 (610.60, below the EMA of 631.87) cross down: four trades.
    assert_eq!(result.total_orders, 12);
    assert_eq!(common::balance(&result), "9744.00 EUR");
}
