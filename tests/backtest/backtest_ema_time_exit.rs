//! A trade of the EMA crossover that neither its stop loss nor its take
//! profit ends is closed `hold` after its entry filled, on a time alert,
//! at the market then (`common::trades`, `TIME_EXIT`, held 90 minutes
//! instead of an hour). One node per thread, so one run per test.

use crate::common::{self, Exits, trades};
use jiff::SignedDuration;

#[test]
fn a_trade_is_closed_its_hold_after_the_entry_fill() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let exits = Exits {
        hold: SignedDuration::from_mins(90),
        ..trades::time_exit()
    };

    let result = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(trades::ema_cross(exits))
        .unwrap()
        .result;

    // The entry buys 10 at 620.65 at 13:00; 90 minutes later, at 14:30,
    // the close sells 10 at the bid of the quote closing 14:30, the 30th
    // minute of the 15:00 bar: 624.20 + 0.30 − 0.05 = 624.45. 3.80 a share
    // on 10. Held an hour, it would have sold at 624.55 (10039.00 EUR);
    // never closed, the balance would still be 10000.00.
    assert_eq!(result.total_orders, 4);
    assert_eq!(common::balance(&result), "10038.00 EUR");
}
