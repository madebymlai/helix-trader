//! Tests that bars outside the session neither move the EMAs nor trade.
//!
//! Quotes run 07:00 to 17:00 UTC each day from Wednesday 7 January 2026, with none overnight, so
//! there are no bars then. The run starts at 09:00 and ends Thursday 10:00. The shortest crossover
//! initializes short at 11:00, then trades three brackets of 10 units: crosses up at 15:00, down at
//! 16:00 and up on Thursday 09:00. The first two end at their stops, -106.90 and -195.10, and the
//! last ends long, so 9 orders and 9698.00 EUR.
//!
//! Counting the bars outside the session would give four trades, the second a cross at 17:00
//! (620.60) and the last a cross down on Thursday 09:00 (610.60, below the EMA of 631.88).

use jiff::Timestamp;
use rstest::rstest;

use crate::common::{self, stepped_quotes, ts};

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

    assert_eq!(result.total_orders, 9);
    assert_eq!(common::balance(&result), "9698.00 EUR");
}
