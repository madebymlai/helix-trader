//! Inconsistent inputs are refused before the node is built, though the
//! catalog could serve the run. No test here builds a node, so several
//! share the file.

use crate::common::trades;

#[test]
fn a_start_on_or_after_the_end_is_refused() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let end = trades::end(&levels);
    for start in [end, trades::at("19:00")] {
        let backtest = trades::backtest(catalog.path(), start, &levels, "0 EUR");

        let run = backtest.run(trades::ema_cross());

        assert!(run.is_err(), "a run from {start} to {end}");
    }
}
