//! Inconsistent inputs are refused before the node is built, though the
//! catalog could serve the run. No test here builds a node, so several
//! share the file.

use crate::common::trades;

#[test]
fn a_start_on_or_after_the_end_is_refused() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let end = trades::end(&levels);
    for start in [end, trades::at("16:00")] {
        let backtest = trades::backtest(catalog.path(), start, &levels, "0 EUR");

        let err = backtest
            .run(trades::ema_cross(trades::time_exit()))
            .unwrap_err();

        let err = format!("{err:#}");
        let message = format!("backtest start {start} must be before its end {end}");
        assert!(err.contains(message.as_str()), "{err}");
    }
}
