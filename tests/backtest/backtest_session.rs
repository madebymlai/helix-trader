//! The session on internal bars: the bars outside it neither move the EMAs nor trade; overnight,
//! without quotes, there are no bars at all.

use jiff::Timestamp;
use rstest::rstest;

use crate::common::{self, stepped_quotes, ts};

/// Quoted 07:00 to 17:00 UTC each day, nothing overnight, so no bars then; the hour bars close at
/// level + 0.60.
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

#[rstest]
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

    // The run replays the bars closing 10:00 to 14:00 from 09:00: the crossover, initialized at
    // 11:00, is short to 14:00 (590.60 below the slow EMA of 600.60, which goes to 593.93). Each
    // trade but the last ends at its stop, 50 bps away, on the first quote of the next bar, which
    // jumps past it:
    //
    // - 15:00: 610.60 above 593.93, cross up: buy 10 at 610.65, stop 607.55, EMA to 605.04;
    //   stopped at 15:01 at the bid 600.00 + 0.01 - 0.05 = 599.96: -106.90
    // - 16:00: 600.60 below 605.04, cross down: sell 10 at 600.55, stop 603.60, EMA to 602.08;
    //   stopped at 16:01 at the ask 620.00 + 0.01 + 0.05 = 620.06: -195.10
    // - Thursday 09:00: 610.60 above 602.08, cross up: buy 10 at 610.65, stop 607.55, take profit
    //   616.71; 10:00 at 615.60 touches neither, the run ends long
    //
    // Three brackets of 3 orders. Counting the bars outside the session, 17:00 (620.60) would
    // cross up, Thursday 08:00 (640.60) stay long and 09:00 (610.60, below the EMA of 631.88)
    // cross down: four trades.
    assert_eq!(result.total_orders, 9);
    assert_eq!(common::balance(&result), "9698.00 EUR");
}
