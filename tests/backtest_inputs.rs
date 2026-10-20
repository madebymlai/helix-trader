//! Inconsistent inputs are refused before the node is built, though the
//! catalog could serve the run. No test here builds a node, so several
//! share the file.

mod common;

use common::steps;
use jiff::SignedDuration;

#[test]
fn a_negative_warmup_is_refused() {
    let catalog = steps::catalog();
    let mut backtest = steps::backtest(catalog.path(), steps::at("12:30"));
    backtest.warmup = SignedDuration::from_hours(-1);

    let err = backtest
        .run(common::shortest_ema_cross(steps::WARMUP))
        .unwrap_err();

    let err = format!("{err:#}");
    assert!(err.contains("negative warmup"), "{err}");
}

#[test]
fn a_start_on_or_after_the_end_is_refused() {
    let catalog = steps::catalog();
    for start in [steps::end(), steps::at("16:00")] {
        let backtest = steps::backtest(catalog.path(), start);

        let err = backtest
            .run(common::shortest_ema_cross(steps::WARMUP))
            .unwrap_err();

        let err = format!("{err:#}");
        let message = format!(
            "backtest start {start} must be before its end {}",
            steps::end()
        );
        assert!(err.contains(&message), "{err}");
    }
}
