//! The session on internal bars: the bars outside it neither move the EMAs
//! nor trade; overnight, without quotes, there are no bars at all. One node
//! per thread, so one run per test.

use crate::common::{self, stepped_quotes, ts};
use jiff::Timestamp;

/// Quoted from 07:00 to 17:00 UTC each day, nothing overnight; the hour
/// bars close at their level + 0.60:
///
/// - Wednesday 7 January 2026, 09:00 to 13:00: 600.60; 14:00: 590.60 (the
///   crossover still short); 15:00: 610.60; 16:00: 600.60; 17:00, out of
///   the session: 620.60;
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
        ts("2026-01-07T09:00:00Z"),
        ts("2026-01-08T10:00:00Z"),
    );
    let catalog = common::catalog(&stepped_quotes(from, end, level), from, end);

    let result = common::backtest(catalog.path(), start, end, "0 EUR")
        .run(common::shortest_ema_cross())
        .unwrap()
        .result;

    // From 09:00 the run replays the bars closing 10:00 to 14:00: the
    // crossover, initialized at 11:00, is short there and stays short to
    // 14:00, 590.60 below the slow EMA of 600.60, which goes to 593.93.
    // Each trade but the last ends at its stop loss, 50 bps away, on the
    // first quote of the next bar, which jumps past it:
    //
    // - 15:00: 610.60 above 593.93, cross up: buy 10 at 610.65, its stop
    //   loss at 607.55, the EMA going to 605.04; stopped at 15:01 at the
    //   bid 600.00 + 0.01 − 0.05 = 599.96: −106.90;
    // - 16:00: 600.60 below 605.04, cross down: sell 10 at 600.55, its stop
    //   loss at 603.60, the EMA going to 602.08; stopped at 16:01 at the
    //   ask 620.00 + 0.01 + 0.05 = 620.06 (the 17:00 bar, out of the
    //   session, still quoted): −195.10;
    // - Thursday 09:00: 610.60 above 602.08, cross up: buy 10 at 610.65,
    //   its stop loss at 607.55 and take profit at 616.71; 10:00 at 615.60
    //   stays long, its bids, 614.96 to 615.55, touching neither, and the
    //   run ends with the long open.
    //
    // Three brackets of 3 orders. Had the bars outside the session
    // counted, 17:00 (620.60) would cross up, Thursday 08:00 (640.60) stay
    // long and 09:00 (610.60, below the EMA of 631.88) cross down: four
    // trades.
    assert_eq!(result.total_orders, 9);
    assert_eq!(common::balance(&result), "9698.00 EUR");
}
