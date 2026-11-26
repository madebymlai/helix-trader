//! Defines the features and the event filter of the `model_cross` strategy.
//!
//! The features are those of `model_ema`: the fast and slow `ExponentialMovingAverage` of the
//! closes of each bar type the strategy reads, built from `[indicators.fast]` and
//! `[indicators.slow]`. A feature is named after its table when one bar type is read (`fast`).
//! When several are read, it is named after the bar type and the table
//! (`XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL:fast`).
//!
//! The event filter is a cross of the simple averages of the `MovingAverageConvergenceDivergence`
//! of `[indicators.cross]` on the bars of the traded instrument (`cross::Cross`), so the strategy
//! decides only at the grid ticks after a cross.

use std::rc::Rc;

use crate::{
    model::features::FeatureSet,
    strategy::{Declared, EmaArgs, Series},
};

use super::{ModelCrossConfig, cross::CrossArgs};

pub(super) const FAST: Declared<EmaArgs> = Declared::new("fast");
pub(super) const SLOW: Declared<EmaArgs> = Declared::new("slow");
pub(super) const CROSS: Declared<CrossArgs> = Declared::new("cross");

/// The version of these features and the event filter, in the fingerprint of the strategy's
/// dataset and models.
///
/// Bumped when what they compute changes and their names and tables do not, such as the kind of
/// average or what makes a cross. The periods, the price read and the bar types are in the
/// fingerprint by value.
const VERSION: u32 = 1;

// panics-doc-ok (transitive via FeatureSetBuilder::feature and FeatureSetBuilder::event_filter)
/// Builds the averages of each bar type in turn, the fast then the slow, and the cross on the bars
/// of the traded instrument.
///
/// The strategy's warmup warms them. A dataset build replays that warmup before its first day.
///
/// # Errors
///
/// Returns an error if the traded instrument has not exactly one bar type among those it reads.
///
/// # Panics
///
/// Panics if `[indicators]` does not build the averages and the cross, which the load refuses.
pub(super) fn features(config: &ModelCrossConfig) -> anyhow::Result<Rc<FeatureSet>> {
    let traded = config.trade.instruments[0].instrument_id;
    let crossed: Vec<Series> = config
        .data
        .bar_types()
        .filter(|bar_type| bar_type.instrument_id() == traded)
        .map(Series::Bars)
        .collect();
    anyhow::ensure!(
        crossed.len() == 1,
        "the cross reads the bars of the traded instrument {traded}, and data.series lists {} \
         bars series of it: list one",
        crossed.len()
    );
    config
        .data
        .bar_types()
        .flat_map(|bar_type| [FAST, SLOW].map(|average| (bar_type, average)))
        .fold(
            FeatureSet::builder()
                .version(VERSION)
                .warmup(config.data.warmup),
            |builder, (bar_type, average)| {
                builder.feature(&[Series::Bars(bar_type)], average, &config.indicators)
            },
        )
        .event_filter(&crossed, CROSS, &config.indicators)
        .build()
}

#[cfg(all(test, feature = "strategy-tests"))]
mod tests {
    use nautilus_common::actor::indicators::ActorIndicator;
    use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_MINUTE};
    use nautilus_model::data::{Bar, BarType};
    use rstest::rstest;

    use super::*;
    use crate::{
        model::{ModelPart, features::NoEvent},
        strategy::model_cross::ModelCrossParams,
    };

    const GOLD: &str = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
    const GOLD_BARS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#;

    /// Test config:
    /// - gold's minute bars
    /// - features of a fast average of 2 bars and a slow average of 3
    /// - a cross of simple averages of the same periods
    /// - a warmup of 3 hours
    /// - a time-only target
    /// - a unit of the ATR of 3 hourly bars
    const CONFIG: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },
        ]
        warmup = "3h"

        [indicators.fast]
        period = 2

        [indicators.slow]
        period = 3

        [indicators.cross]
        fast_period = 2
        slow_period = 3

        [trade]
        instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
        risk = "0.5%"

        [model]
        dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
        decision = { every = "1h" }
        target = { limit = "30m", atr = { period = 3 } }
        retrain = "24h"
        window = "48h"
        calibration = "12h"
        margins = { long = "0.5R", short = "0.5R" }
        min_rows = 30
        learner = { kind = "baseline" }
    "#;

    fn features_of(toml: &str) -> Rc<FeatureSet> {
        ModelCrossParams::features(&ModelCrossConfig::load(toml).unwrap()).unwrap()
    }

    /// A bar of gold closing at `close`, at the end of minute `minute`.
    fn bar(minute: u64, close: &str) -> Bar {
        let ts = UnixNanos::from(minute * NANOSECONDS_IN_MINUTE);
        Bar::new(
            BarType::from(GOLD),
            close.into(),
            close.into(),
            close.into(),
            close.into(),
            1.into(),
            ts,
            ts,
        )
    }

    fn fed(closes: &[&str]) -> Rc<FeatureSet> {
        let features = features_of(CONFIG);
        for (minute, close) in (1..).zip(closes) {
            features.handle_bar(&bar(minute, close)).unwrap();
        }
        features
    }

    /// Two closes warm the fast average and not the slow, of three bars.
    #[rstest]
    fn a_cross_whose_slow_average_is_cold_marks_no_decision() {
        let features = fed(&["11.00", "12.00"]);

        assert_eq!(
            features.event(
                UnixNanos::default(),
                UnixNanos::from(3 * NANOSECONDS_IN_MINUTE)
            ),
            Err(NoEvent::Cold)
        );
    }

    /// Closes of 11, 12, 13 then 14. The cross's fast average is the mean of the last two closes,
    /// 12.5 after the third bar and 13.5 after the fourth, and its slow the mean of the last three,
    /// 12 then 13. The fast is above the slow after the third bar and the fourth.
    #[rstest]
    fn averages_that_do_not_cross_mark_no_decision() {
        let features = fed(&["11.00", "12.00", "13.00", "14.00"]);

        assert_eq!(
            features.event(
                UnixNanos::from(3 * NANOSECONDS_IN_MINUTE),
                UnixNanos::from(5 * NANOSECONDS_IN_MINUTE)
            ),
            Err(NoEvent::Unmarked)
        );
    }

    /// Closes of 11, 12, 13 then 10. After the third bar the fast average, (12 + 13) / 2 = 12.5, is
    /// above the slow, (11 + 12 + 13) / 3 = 12. The fourth brings the fast to (13 + 10) / 2 = 11.5
    /// and the slow to (12 + 13 + 10) / 3 = 11.67: the fast is below, a cross at the fourth bar,
    /// minute 4. It is an event for a decision whose previous grid point is before minute 4, and
    /// none for one whose previous grid point is at it. Nor is it one for a decision before minute
    /// 4, whose callback a live clock ran after the bar came: it is the next decision's.
    #[rstest]
    #[case::between_the_grid_points(3 * NANOSECONDS_IN_MINUTE + 1, 5 * NANOSECONDS_IN_MINUTE, Ok(()))]
    #[case::at_the_decision(3 * NANOSECONDS_IN_MINUTE + 1, 4 * NANOSECONDS_IN_MINUTE, Ok(()))]
    #[case::at_the_previous_grid_point(4 * NANOSECONDS_IN_MINUTE, 5 * NANOSECONDS_IN_MINUTE, Err(NoEvent::Unmarked))]
    #[case::after_the_decision(3 * NANOSECONDS_IN_MINUTE + 1, 4 * NANOSECONDS_IN_MINUTE - 1, Err(NoEvent::Unmarked))]
    fn a_cross_is_an_event_for_the_decision_after_it(
        #[case] previous: u64,
        #[case] now: u64,
        #[case] event: Result<(), NoEvent>,
    ) {
        let features = fed(&["11.00", "12.00", "13.00", "10.00"]);

        assert_eq!(
            features.event(UnixNanos::from(previous), UnixNanos::from(now)),
            event
        );
    }

    /// Closes of 13, 10, 11 then 12. After the third bar the fast average, (10 + 11) / 2 = 10.5,
    /// is below the slow, (13 + 10 + 11) / 3 = 11.33. The fourth brings the fast to 11.5 and the
    /// slow to 11, above: a cross at the fourth bar. The averages read those closes alone, so bars
    /// before them, here three at 100, leave the cross as it is, whatever bar a replay starts at.
    #[rstest]
    #[case::from_the_first_of_the_closes(&[])]
    #[case::after_earlier_bars(&["100.00", "100.00", "100.00"])]
    fn a_cross_is_the_same_whatever_came_before_its_averages_bars(#[case] earlier: &[&str]) {
        let closes: Vec<&str> = [earlier, &["13.00", "10.00", "11.00", "12.00"]].concat();
        let fourth = closes.len() as u64;
        let features = fed(&closes);

        assert_eq!(
            features.event(
                UnixNanos::from((fourth - 1) * NANOSECONDS_IN_MINUTE),
                UnixNanos::from((fourth + 1) * NANOSECONDS_IN_MINUTE)
            ),
            Ok(())
        );
    }

    /// Closes of 10, 10 and 12.2, then three of 10.2 and one of 10.3. After the three closes of
    /// 10.2 both averages are 10.2, so the fast is not above the slow, and 10.3 brings it above, to
    /// 10.25 over 10.23: a cross at the last bar. Nautilus' averages keep running sums, which after
    /// those closes round to slightly different values depending on the bar a replay started at;
    /// the cross is the same whichever.
    #[rstest]
    #[case::from_the_first_close(&["10.00", "10.00", "12.20"])]
    #[case::from_the_second_close(&["10.00", "12.20"])]
    fn a_cross_after_equal_averages_is_the_same_whatever_bar_a_replay_starts_at(
        #[case] earlier: &[&str],
    ) {
        let closes: Vec<&str> = [earlier, &["10.20", "10.20", "10.20", "10.30"]].concat();
        let last = closes.len() as u64;
        let features = fed(&closes);

        assert_eq!(
            features.event(
                UnixNanos::from((last - 1) * NANOSECONDS_IN_MINUTE),
                UnixNanos::from(last * NANOSECONDS_IN_MINUTE)
            ),
            Ok(())
        );
    }

    /// The cross is no feature: the names and values are the averages'.
    #[rstest]
    fn the_features_are_the_averages_alone() {
        let features = fed(&["11.00", "12.00", "13.00"]);

        assert_eq!(features.names(), ["fast", "slow"]);
        let values = features.values().unwrap();
        assert!((values[0] - 12.5).abs() < 1e-9, "{values:?}");
        assert!((values[1] - 12.0).abs() < 1e-9, "{values:?}");
    }

    /// The cross reads the bars of the traded instrument, gold. Silver's bars alone, or gold's of
    /// two bar types, leave it no one bar type to read, and the load refuses them under the
    /// series.
    #[rstest]
    #[case::of_another_instrument_alone(CONFIG.replace("XAUUSD.ICMARKETS\", bar_spec", "XAGUSD.ICMARKETS\", bar_spec"), 0)]
    #[case::of_two_bar_types_of_the_traded_instrument(
        CONFIG.replace(GOLD_BARS, &format!("{GOLD_BARS}\n{}", GOLD_BARS.replace("step = 1,", "step = 5,"))),
        2
    )]
    fn a_config_without_one_bars_series_of_the_traded_instrument_is_refused(
        #[case] toml: String,
        #[case] listed: usize,
    ) {
        let err = ModelCrossConfig::load(&toml).unwrap_err();

        let message = format!("{err:#}");
        assert!(
            message.contains("data.series")
                && message.contains("XAUUSD.ICMARKETS")
                && message.contains(&format!("lists {listed} bars series")),
            "{message}"
        );
    }

    /// Averages of one period never cross, so the load refuses them under the cross's table.
    #[rstest]
    fn a_cross_whose_fast_period_is_not_below_its_slow_is_refused_under_its_table() {
        let toml = CONFIG.replace("fast_period = 2", "fast_period = 3");

        let message = format!("{:#}", ModelCrossConfig::load(&toml).unwrap_err());

        assert!(
            message.contains("indicators.cross") && message.contains("fast period 3"),
            "{message}"
        );
    }

    /// An average of no closes has no value, so the load refuses a fast period of zero under the
    /// cross's table rather than Nautilus panicking on it.
    #[rstest]
    fn a_cross_whose_fast_period_is_zero_is_refused_under_its_table() {
        let toml = CONFIG.replace("fast_period = 2", "fast_period = 0");

        let message = format!("{:#}", ModelCrossConfig::load(&toml).unwrap_err());

        assert!(
            message.contains("indicators.cross") && message.contains("fast period 0"),
            "{message}"
        );
    }

    /// A slow period above Nautilus' maximum is refused under the cross's table, not panicked on.
    #[rstest]
    fn a_cross_whose_slow_period_nautilus_refuses_is_refused_under_its_table() {
        let toml = CONFIG.replace("slow_period = 3", "slow_period = 16777217");

        let message = format!("{:#}", ModelCrossConfig::load(&toml).unwrap_err());

        assert!(message.contains("indicators.cross"), "{message}");
    }

    /// A changed period of the cross changes the fingerprint with no version bump.
    #[rstest]
    fn a_changed_period_of_the_cross_changes_the_fingerprint() {
        let changed = CONFIG.replace("slow_period = 3", "slow_period = 4");

        assert_ne!(
            features_of(&changed).fingerprint(),
            features_of(CONFIG).fingerprint()
        );
    }
}
